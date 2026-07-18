"""CLI subcommands for triggering and inspecting scripthut workflows.

The command surface mirrors ``gh``: noun first, then verb
(``scripthut workflow run``, ``scripthut run watch``).  Two transports
share a single ``Client`` interface:

* ``LocalClient`` — boots a ``Runtime`` in-process (same backend
  connections, storage, and ``RunManager`` the web server uses) and
  calls into it directly.  No server required.
* ``RemoteClient`` — calls a running scripthut server's ``/api/v1``
  endpoints over httpx.

Server resolution order: ``--server`` argument → ``SCRIPTHUT_SERVER``
env var → ``settings.cli_server`` in scripthut.yaml.  If none of those
are set, commands talk to a local daemon (a detached ``scripthut``
server on ``settings.server_host:server_port``), starting one first if
allowed by ``settings.cli_autostart`` — see ``_ensure_local_server``.
``--server local`` forces the in-process ``LocalClient`` instead.

When the resolved server sits behind Cloudflare Access, the CLI sends
auth headers built from (per field, highest wins): ``--cf-client-id`` /
``--cf-client-secret`` / ``--cf-access-token`` flags →
``SCRIPTHUT_CF_CLIENT_ID`` / ``SCRIPTHUT_CF_CLIENT_SECRET`` /
``SCRIPTHUT_CF_ACCESS_TOKEN`` env vars → ``settings.cli_auth`` in
scripthut.yaml.  As a final fallback for the JWT, if
``--cloudflared-app`` (or its env / config equivalent) is set, the CLI
shells out to ``cloudflared access token --app=<url>`` at invocation.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import httpx

from scripthut.config import ConfigError, load_config, set_global_setting
from scripthut.config_schema import (
    PBSBackendConfig,
    ScriptHutConfig,
    SlurmBackendConfig,
    Stack,
)
from scripthut.runs.models import Run, RunItemStatus, RunStatus
from scripthut.runtime import Runtime, init_runtime, shutdown_runtime
from scripthut.ssh.client import SSHClient
from scripthut.stacks import StackManager, StackState, StackStatus

logger = logging.getLogger(__name__)


TERMINAL_RUN_STATES = {
    RunStatus.COMPLETED.value,
    RunStatus.FAILED.value,
    RunStatus.CANCELLED.value,
}


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _summary_from_run(run: Run) -> dict[str, Any]:
    """Compact summary matching the API response shape."""
    counts: dict[str, int] = {}
    for item in run.items:
        counts[item.status.value] = counts.get(item.status.value, 0) + 1
    completed, total = run.progress
    submitted_count = sum(
        1 for item in run.items if item.status != RunItemStatus.PENDING
    )
    return {
        "id": run.id,
        "workflow_name": run.workflow_name,
        "backend_name": run.backend_name,
        "created_at": run.created_at.isoformat(),
        "status": run.status.value,
        "task_count": total,
        "completed_count": completed,
        "submitted_count": submitted_count,
        "status_counts": counts,
    }


def _print_run_submitted(summary: dict[str, Any], remote_base: str | None) -> None:
    """Human-readable confirmation after a successful run submission."""
    submitted = summary.get("submitted_count", 0)
    total = summary.get("task_count", 0)
    pending = total - submitted
    print(f"Submitted run {summary['id']} (workflow '{summary['workflow_name']}')")
    print(f"  backend: {summary['backend_name']}")
    print(f"  tasks: {submitted}/{total} dispatched, {pending} pending")
    counts = summary.get("status_counts", {})
    if counts:
        parts = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        print(f"  status: {parts}")
    if remote_base is None and pending > 0:
        print(
            f"  note: {pending} task(s) still pending. "
            "Start `scripthut` to monitor and submit further waves."
        )


def _format_run_table(runs: list[dict[str, Any]]) -> str:
    if not runs:
        return "No runs found."
    lines = [
        f"{'ID':<10} {'STATUS':<10} {'PROGRESS':<10} "
        f"{'WORKFLOW':<28} {'BACKEND':<14} CREATED"
    ]
    for r in runs:
        progress = f"{r['completed_count']}/{r['task_count']}"
        lines.append(
            f"{r['id']:<10} {r['status']:<10} {progress:<10} "
            f"{r['workflow_name'][:28]:<28} {r['backend_name'][:14]:<14} "
            f"{r['created_at']}"
        )
    return "\n".join(lines)


def _format_run_view(detail: dict[str, Any]) -> str:
    lines = [
        f"Run {detail['id']} ({detail['workflow_name']})",
        f"  status:   {detail['status']}",
        f"  backend:  {detail['backend_name']}",
        f"  created:  {detail['created_at']}",
        f"  tasks:    {detail['completed_count']}/{detail['task_count']} complete",
    ]
    counts = detail.get("status_counts", {})
    if counts:
        parts = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        lines.append(f"  by state: {parts}")
    if detail.get("commit_hash"):
        lines.append(f"  commit:   {detail['commit_hash']}")
    items = detail.get("items", [])
    if items:
        lines.append("")
        lines.append(
            f"  {'TASK':<22} {'STATUS':<10} {'JOB':<14} {'CPU%':<6} {'MEM':<10}"
        )
        for it in items:
            task = it["task"]
            job_id = it.get("job_id") or "-"
            cpu = it.get("cpu_efficiency")
            cpu_str = f"{cpu:.0f}" if isinstance(cpu, (int, float)) else "-"
            mem = it.get("max_rss") or "-"
            lines.append(
                f"  {task['id'][:22]:<22} {it['status']:<10} "
                f"{job_id:<14} {cpu_str:<6} {mem:<10}"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------


class LocalClient:
    """Run scripthut commands against an in-process ``Runtime``.

    Boot is expensive (SSH connections, storage scan), so callers should
    create one client per command invocation, not per call.  Always use
    via ``async with`` to guarantee ``shutdown_runtime`` runs.
    """

    def __init__(self, config_path: Path | None = None, *, restore_runs: bool = False):
        self._config_path = config_path
        self._restore_runs = restore_runs
        self._runtime: Runtime | None = None

    async def __aenter__(self) -> LocalClient:
        config = load_config(self._config_path)
        # Don't restore by default — the CLI is short-lived and restoring
        # would re-trigger process_run on every active run.
        self._runtime = await init_runtime(config, restore_runs=self._restore_runs)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._runtime is not None:
            await shutdown_runtime(self._runtime)
            self._runtime = None

    @property
    def runtime(self) -> Runtime:
        if self._runtime is None:
            raise RuntimeError("LocalClient not entered")
        return self._runtime

    async def list_backends(self) -> dict[str, Any]:
        config = self.runtime.config
        return {
            "backends": [
                {
                    "name": bs.name,
                    "type": bs.backend_type,
                    "connected": bs.status.connected,
                    "max_concurrent": getattr(
                        config.get_backend(bs.name), "max_concurrent", None,
                    ),
                }
                for bs in self.runtime.backends.values()
            ]
        }

    async def list_sources(self) -> dict[str, Any]:
        from scripthut.config_schema import GitSourceConfig, PathSourceConfig
        config = self.runtime.config
        out: list[dict[str, Any]] = []
        for s in config.sources:
            base: dict[str, Any] = {
                "name": s.name, "type": s.type,
                "description": getattr(s, "description", ""),
                "max_concurrent": getattr(s, "max_concurrent", None),
            }
            if isinstance(s, GitSourceConfig):
                base.update({"url": s.url, "branch": s.branch})
            elif isinstance(s, PathSourceConfig):
                base.update({"path": s.path, "backend": s.backend})
            out.append(base)
        return {"sources": out}

    async def view_source(self, name: str) -> dict[str, Any]:
        """Source metadata. Workflow discovery is server-only in local mode.

        Discovery needs a synced clone (git) or SSH to the backend (path);
        the CLI's LocalClient doesn't carry a source manager, so we
        return ``workflows=[]`` with a clear ``discover_error`` and let
        callers fall back to ``--server <url>`` for the real view.
        """
        from scripthut.config_schema import GitSourceConfig, PathSourceConfig
        config = self.runtime.config
        source = config.get_source(name)
        if source is None:
            raise RuntimeError(f"Source '{name}' not found")
        base: dict[str, Any] = {
            "name": source.name, "type": source.type,
            "description": getattr(source, "description", ""),
            "max_concurrent": getattr(source, "max_concurrent", None),
        }
        if isinstance(source, GitSourceConfig):
            base.update({"url": source.url, "branch": source.branch})
        elif isinstance(source, PathSourceConfig):
            base.update({"path": source.path, "backend": source.backend})
        return {
            **base,
            "workflows": [],
            "discover_error": (
                "Workflow discovery is only available via a running scripthut "
                "server. Re-run with --server <url>."
            ),
        }

    async def list_runs(self, limit: int | None = None) -> dict[str, Any]:
        # CLI doesn't restore by default; pull straight from storage so
        # `list runs` reflects what's on disk.
        rm = self.runtime.run_manager
        all_runs = rm.storage.load_all_runs() if rm.storage else {}
        runs = sorted(
            (r for r in all_runs.values() if r.workflow_name != "_default"),
            key=lambda r: r.created_at,
            reverse=True,
        )
        if limit is not None and limit > 0:
            runs = runs[:limit]
        return {"runs": [_summary_from_run(r) for r in runs]}

    async def run_task(
        self, task_dict: dict, backend: str, run_name: str | None = None,
    ) -> dict[str, Any]:
        from scripthut.runs.models import TaskDefinition

        task = TaskDefinition.from_dict(task_dict)
        run = await self.runtime.run_manager.create_adhoc_run(
            task, backend, run_name=run_name,
        )
        return _summary_from_run(run)

    async def run_source_workflow(
        self, source: str, workflow: str, *, backend: str | None = None,
        branch: str | None = None,
    ) -> dict[str, Any]:
        """Local-mode source-workflow submission is not supported.

        Submitting needs the workflow JSON, which lives in a synced git
        clone or behind SSH on the source's backend — the CLI's
        ``LocalClient`` doesn't carry a source manager to fetch either.
        Run a scripthut server and use ``--server <url>``; the remote
        client's ``/api/v1/sources/{name}/run`` handles refresh + submit.
        """
        raise RuntimeError(
            "Source workflow submission is not available in local CLI mode. "
            "Run a scripthut server and re-run with `--server <url>` "
            "(or set `settings.cli_server` in scripthut.yaml)."
        )

    async def sync_source(self, name: str) -> dict[str, Any]:
        raise RuntimeError(
            "Source sync is not available in local CLI mode. "
            "Run a scripthut server and re-run with `--server <url>`."
        )

    async def sync_all_sources(self) -> dict[str, Any]:
        raise RuntimeError(
            "Source sync is not available in local CLI mode. "
            "Run a scripthut server and re-run with `--server <url>`."
        )

    async def view_run(self, run_id: str) -> dict[str, Any]:
        # Local mode reads from storage rather than in-memory state since
        # we don't restore on startup.
        rm = self.runtime.run_manager
        all_runs = rm.storage.load_all_runs() if rm.storage else {}
        run = all_runs.get(run_id)
        if run is None:
            raise RuntimeError(f"Run '{run_id}' not found")
        summary = _summary_from_run(run)
        summary["items"] = [item.to_dict() for item in run.items]
        summary["log_dir"] = run.log_dir
        summary["account"] = run.account
        summary["commit_hash"] = run.commit_hash
        summary["git_repo"] = run.git_repo
        summary["git_branch"] = run.git_branch
        return summary

    async def cancel_run(self, run_id: str) -> dict[str, Any]:
        # cancel_run requires the run to be in-memory; load it first.
        rm = self.runtime.run_manager
        if run_id not in rm.runs and rm.storage is not None:
            all_runs = rm.storage.load_all_runs()
            if run_id in all_runs:
                rm.runs[run_id] = all_runs[run_id]
        cancelled = await rm.cancel_run(run_id)
        if not cancelled:
            raise RuntimeError(f"Run '{run_id}' not found")
        return {"run_id": run_id, "cancelled": True}

    async def rerun(self, run_id: str) -> dict[str, Any]:
        rm = self.runtime.run_manager
        if run_id not in rm.runs and rm.storage is not None:
            all_runs = rm.storage.load_all_runs()
            if run_id in all_runs:
                rm.runs[run_id] = all_runs[run_id]
        run = await rm.rerun_in_place(run_id)
        return _summary_from_run(run)

    async def fetch_logs(
        self, run_id: str, task_id: str, log_type: str = "output",
        tail: int | None = None,
    ) -> dict[str, Any]:
        rm = self.runtime.run_manager
        if run_id not in rm.runs and rm.storage is not None:
            all_runs = rm.storage.load_all_runs()
            if run_id in all_runs:
                rm.runs[run_id] = all_runs[run_id]
        content, error = await rm.fetch_log_file(
            run_id, task_id, log_type=log_type, tail_lines=tail,
        )
        if error and content is None:
            raise RuntimeError(error)
        return {
            "run_id": run_id,
            "task_id": task_id,
            "type": log_type,
            "content": content or "",
        }


class RemoteAuth:
    """Auth headers to send with every ``RemoteClient`` request.

    Holds Cloudflare Access credentials: either a service-token pair
    (``cf_client_id`` + ``cf_client_secret``) or a user JWT
    (``cf_access_token``). Both are optional — an instance with no
    credentials populated yields no headers, which is the same as not
    using auth at all.
    """

    def __init__(
        self,
        *,
        cf_client_id: str | None = None,
        cf_client_secret: str | None = None,
        cf_access_token: str | None = None,
    ):
        self.cf_client_id = cf_client_id
        self.cf_client_secret = cf_client_secret
        self.cf_access_token = cf_access_token

    def headers(self) -> dict[str, str]:
        h: dict[str, str] = {}
        # Service token: both halves must be present to be useful.
        if self.cf_client_id and self.cf_client_secret:
            h["CF-Access-Client-Id"] = self.cf_client_id
            h["CF-Access-Client-Secret"] = self.cf_client_secret
        # User JWT — `cf-access-token` header works for API calls; Cloudflare
        # also accepts the `CF_Authorization` cookie, but the header form is
        # what `cloudflared access curl` emits and is documented for service
        # auth, so we standardize on it.
        if self.cf_access_token:
            h["cf-access-token"] = self.cf_access_token
        return h

    @property
    def has_credentials(self) -> bool:
        return bool(
            (self.cf_client_id and self.cf_client_secret) or self.cf_access_token
        )


class RemoteClient:
    """Run scripthut commands against a running server's ``/api/v1``."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 60.0,
        auth: RemoteAuth | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.auth = auth
        self._client = httpx.AsyncClient(
            base_url=f"{self.base_url}/api/v1",
            timeout=timeout,
            headers=auth.headers() if auth is not None else {},
        )

    async def __aenter__(self) -> RemoteClient:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self._client.aclose()

    async def _get(self, path: str, **params: Any) -> dict[str, Any]:
        params = {k: v for k, v in params.items() if v is not None}
        resp = await self._client.get(path, params=params)
        return self._handle(resp)

    async def _post(self, path: str, **params: Any) -> dict[str, Any]:
        params = {k: v for k, v in params.items() if v is not None}
        resp = await self._client.post(path, params=params)
        return self._handle(resp)

    @staticmethod
    def _handle(resp: httpx.Response) -> dict[str, Any]:
        if resp.status_code >= 400:
            try:
                detail = resp.json().get("detail", resp.text)
            except Exception:
                detail = resp.text
            raise RuntimeError(f"HTTP {resp.status_code}: {detail}")
        return resp.json()

    async def list_backends(self) -> dict[str, Any]:
        return await self._get("/backends")

    async def list_sources(self) -> dict[str, Any]:
        return await self._get("/sources")

    async def view_source(self, name: str) -> dict[str, Any]:
        return await self._get(f"/sources/{name}")

    async def sync_source(self, name: str) -> dict[str, Any]:
        return await self._post(f"/sources/{name}/sync")

    async def sync_all_sources(self) -> dict[str, Any]:
        return await self._post("/sources/sync")

    async def list_runs(self, limit: int | None = None) -> dict[str, Any]:
        return await self._get("/runs", limit=limit)

    async def run_task(
        self, task_dict: dict, backend: str, run_name: str | None = None,
    ) -> dict[str, Any]:
        # Server endpoint takes a JSON body via POST; the rest of the
        # RemoteClient uses query params so we need the raw httpx call here.
        body: dict[str, Any] = {"task": task_dict, "backend": backend}
        if run_name is not None:
            body["run_name"] = run_name
        if self._client is None:
            raise RuntimeError("RemoteClient not entered")
        resp = await self._client.post("/tasks/run", json=body)
        resp.raise_for_status()
        return resp.json()

    async def run_source_workflow(
        self, source: str, workflow: str, *, backend: str | None = None,
        branch: str | None = None,
    ) -> dict[str, Any]:
        return await self._post(
            f"/sources/{source}/run", workflow=workflow, backend=backend,
            branch=branch,
        )

    async def view_run(self, run_id: str) -> dict[str, Any]:
        return await self._get(f"/runs/{run_id}")

    async def cancel_run(self, run_id: str) -> dict[str, Any]:
        return await self._post(f"/runs/{run_id}/cancel")

    async def rerun(self, run_id: str) -> dict[str, Any]:
        return await self._post(f"/runs/{run_id}/rerun")

    async def fetch_logs(
        self, run_id: str, task_id: str, log_type: str = "output",
        tail: int | None = None,
    ) -> dict[str, Any]:
        return await self._get(
            f"/runs/{run_id}/tasks/{task_id}/logs", type=log_type, tail=tail,
        )


# ---------------------------------------------------------------------------
# Server resolution + client factory
# ---------------------------------------------------------------------------


def _fetch_cloudflared_token(app_url: str) -> str:
    """Shell out to ``cloudflared access token --app=<app_url>`` and return its JWT.

    Bubbles up a clear error if cloudflared is missing or the cached login
    has expired — those are the two common failure modes, and the user has
    different remedies for each (install cloudflared vs. re-run
    ``cloudflared access login``).
    """
    import shutil
    import subprocess

    if shutil.which("cloudflared") is None:
        raise RuntimeError(
            "cloudflared not found on PATH — install it "
            "(https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/) "
            "or pass --cf-access-token / --cf-client-id+--cf-client-secret instead."
        )
    try:
        proc = subprocess.run(
            ["cloudflared", "access", "token", f"--app={app_url}"],
            check=True, capture_output=True, text=True, timeout=30,
        )
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        raise RuntimeError(
            f"cloudflared access token failed: {stderr or e.returncode}. "
            f"Try `cloudflared access login {app_url}` first."
        ) from e
    except subprocess.TimeoutExpired as e:
        raise RuntimeError("cloudflared access token timed out after 30s") from e
    token = proc.stdout.strip()
    if not token:
        raise RuntimeError(
            f"cloudflared returned an empty token for {app_url}. "
            f"Try `cloudflared access login {app_url}` first."
        )
    return token


class AuthResolution:
    """What ``_resolve_auth_with_source`` decided + how it got there.

    ``auth`` is what gets sent on the wire; the rest is bookkeeping that
    the ``status`` command turns into human-readable diagnostics.
    ``method`` is one of: ``"none"``, ``"service-token"``, ``"jwt"``.
    ``source`` describes where the winning credential came from
    (``"flag"`` / ``"env"`` / ``"config"`` / ``"cloudflared"``).
    ``cloudflared_app`` is set when a cloudflared fallback was configured
    (whether or not the token was actually fetched — see
    ``fetch_cloudflared``).
    """

    def __init__(
        self,
        *,
        auth: RemoteAuth | None,
        method: str,
        source: str | None,
        cloudflared_app: str | None = None,
        cloudflared_error: str | None = None,
    ):
        self.auth = auth
        self.method = method
        self.source = source
        self.cloudflared_app = cloudflared_app
        self.cloudflared_error = cloudflared_error


def _resolve_auth_with_source(
    args: argparse.Namespace, *, fetch_cloudflared: bool = True,
) -> AuthResolution:
    """Resolve auth credentials and remember where each one came from.

    Per-field precedence (highest wins): CLI flags → env vars →
    ``settings.cli_auth`` from scripthut.yaml → cloudflared shell-out.
    The cloudflared fallback only runs when no explicit token is
    supplied; pass ``fetch_cloudflared=False`` to suppress it for a
    "describe only, no side effects" view (used by ``scripthut status``
    so it stays fast and doesn't fail when offline).
    """
    cfg_auth = None
    try:
        config = load_config(getattr(args, "config", None))
        cfg_auth = config.settings.cli_auth
    except Exception:
        # No config / unreadable config is fine — the user might be running
        # purely off flags or env vars.
        pass

    def pick(
        flag: str | None, env_key: str, cfg_attr: str,
    ) -> tuple[str | None, str | None]:
        if flag:
            return flag, "flag"
        env = os.environ.get(env_key)
        if env:
            return env, "env"
        v = getattr(cfg_auth, cfg_attr, None) if cfg_auth else None
        return (v, "config") if v else (None, None)

    client_id, src_id = pick(
        getattr(args, "cf_client_id", None),
        "SCRIPTHUT_CF_CLIENT_ID",
        "cf_client_id",
    )
    client_secret, src_secret = pick(
        getattr(args, "cf_client_secret", None),
        "SCRIPTHUT_CF_CLIENT_SECRET",
        "cf_client_secret",
    )
    token, src_token = pick(
        getattr(args, "cf_access_token", None),
        "SCRIPTHUT_CF_ACCESS_TOKEN",
        "cf_access_token",
    )

    # Track the cloudflared app URL even when we don't actually shell out,
    # so status can report "would use cloudflared (from <source>)".
    cloudflared_app: str | None = None
    cf_source: str | None = None
    if getattr(args, "cloudflared_app", None):
        cloudflared_app, cf_source = args.cloudflared_app, "flag"
    elif os.environ.get("SCRIPTHUT_CLOUDFLARED_APP"):
        cloudflared_app, cf_source = (
            os.environ["SCRIPTHUT_CLOUDFLARED_APP"],
            "env",
        )
    elif cfg_auth and getattr(cfg_auth, "cloudflared_app", None):
        cloudflared_app, cf_source = cfg_auth.cloudflared_app, "config"

    cloudflared_error: str | None = None
    if token is None and cloudflared_app and fetch_cloudflared:
        try:
            token = _fetch_cloudflared_token(cloudflared_app)
            src_token = "cloudflared"
        except RuntimeError as e:
            cloudflared_error = str(e)

    auth = RemoteAuth(
        cf_client_id=client_id,
        cf_client_secret=client_secret,
        cf_access_token=token,
    )
    if not auth.has_credentials:
        # No headers will be sent. Still report a cloudflared-app source
        # if one was configured, so status can explain "configured but
        # nothing fetched".
        if cloudflared_app and not fetch_cloudflared:
            return AuthResolution(
                auth=None, method="jwt-pending", source=cf_source,
                cloudflared_app=cloudflared_app,
            )
        return AuthResolution(
            auth=None, method="none", source=None,
            cloudflared_app=cloudflared_app,
            cloudflared_error=cloudflared_error,
        )
    # When both methods are configured RemoteAuth.headers() emits both;
    # we describe whichever has its full credential pair (service token
    # needs id+secret) first.
    if client_id and client_secret:
        return AuthResolution(
            auth=auth, method="service-token", source=src_id,
            cloudflared_app=cloudflared_app,
        )
    return AuthResolution(
        auth=auth, method="jwt", source=src_token,
        cloudflared_app=cloudflared_app,
    )


def _resolve_auth(args: argparse.Namespace) -> RemoteAuth | None:
    """Backwards-compatible wrapper around ``_resolve_auth_with_source``.

    Existing call sites (``_make_client``) only care about the
    ``RemoteAuth`` itself; the source attribution is for ``status``.
    """
    return _resolve_auth_with_source(args).auth


def _resolve_server_with_source(
    args: argparse.Namespace,
) -> tuple[str | None, str]:
    """Like ``_resolve_server`` but also reports where the value came from.

    Returns ``(url, source)`` where ``source`` is one of: ``"flag"``,
    ``"env"``, ``"config"``, ``"local-keyword"`` (user passed
    ``--server local``), or ``"none"`` (nothing configured anywhere).
    """
    explicit = getattr(args, "server", None)
    if explicit == "local":
        return None, "local-keyword"
    if explicit:
        return explicit, "flag"
    env = os.environ.get("SCRIPTHUT_SERVER")
    if env:
        return env, "env"
    # Read config's cli_server; failures are non-fatal (user might not have
    # a config in CWD when targeting --server local).
    try:
        config = load_config(getattr(args, "config", None))
    except Exception:
        return None, "none"
    if config.settings.cli_server:
        return config.settings.cli_server, "config"
    return None, "none"


def _resolve_server(args: argparse.Namespace) -> str | None:
    """Backwards-compatible wrapper around ``_resolve_server_with_source``.

    ``--server local`` forces local mode (returns ``None``) even when env
    or config points at a remote URL — useful for one-off debugging.
    """
    return _resolve_server_with_source(args)[0]


def _confirm_stderr(question: str, default_yes: bool = True) -> bool:
    """Yes/no prompt on stderr (keeps ``--json | jq`` pipelines clean).

    Mirrors ``scripthut.setup.aws_ec2._confirm`` — stdlib only, no
    click/prompt_toolkit dependency. EOF (ctrl-d, closed stdin) counts
    as "no".
    """
    suffix = "[Y/n]" if default_yes else "[y/N]"
    while True:
        print(f"{question} {suffix}: ", end="", file=sys.stderr, flush=True)
        try:
            answer = input().strip().lower()
        except EOFError:
            print(file=sys.stderr)
            return False
        if not answer:
            return default_yes
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        print("Please answer y or n.", file=sys.stderr)


def _ensure_local_server(args: argparse.Namespace) -> str:
    """Return the base URL of a local daemon, starting one if allowed.

    Called when nothing resolves a server. A running daemon (or a
    foreground ``scripthut`` server) on ``settings.server_host:port`` is
    always used as-is; otherwise ``settings.cli_autostart`` decides
    whether to prompt (``ask``, TTY only), spawn silently (``always``),
    or fail with guidance (``never``, or ``ask`` off a TTY).

    Raises RuntimeError (rendered cleanly by ``main()``) when autostart
    is declined, disabled, or the daemon fails to come up.
    """
    from scripthut import daemon

    # A daemon with no backends is useless — let the friendly
    # "No configuration found" FileNotFoundError propagate instead.
    config = load_config(getattr(args, "config", None))
    host, port = daemon.resolve_host_port(config)
    url = f"http://{host}:{port}"

    if daemon.ping(host, port) is not None:
        return url

    mode = config.settings.cli_autostart
    interactive = sys.stdin.isatty()

    if mode == "never" or (mode == "ask" and not interactive):
        raise RuntimeError(
            f"No server configured and no local daemon running at {url}.\n"
            f"  - start one:          scripthut daemon start\n"
            f"  - run in-process:     add --server local\n"
            f"  - point at a server:  --server <url> / SCRIPTHUT_SERVER / settings.cli_server\n"
            f"  - always autostart:   set settings.cli_autostart: always"
            + ("" if mode == "never" else "\n  (non-interactive session: not prompting)")
        )

    if mode == "ask":
        wants = _confirm_stderr(
            f"No scripthut server running at {url}. "
            f"Start a local daemon (background web admin)?",
            default_yes=True,
        )
        if _confirm_stderr("Remember this choice in the global config?", default_yes=False):
            try:
                saved_to = set_global_setting(
                    "cli_autostart", "always" if wants else "never",
                )
                print(f"Saved settings.cli_autostart to {saved_to}", file=sys.stderr)
            except RuntimeError as e:
                print(f"Warning: {e}", file=sys.stderr)
        if not wants:
            raise RuntimeError(
                "Not starting a daemon. Use --server local for in-process "
                "mode, or `scripthut daemon start` to launch one explicitly."
            )

    # mode == "always", or the user said yes.
    info = daemon.start_daemon(config, getattr(args, "config", None))
    print(
        f"Started local daemon (pid {info.pid}) at {info.url}; "
        f"logs: {daemon.logfile_path(config)}",
        file=sys.stderr,
    )
    return info.url


def _make_client(args: argparse.Namespace) -> Any:
    """Build the right Client subclass for this invocation."""
    server, source = _resolve_server_with_source(args)
    if server:
        return RemoteClient(server, auth=_resolve_auth(args))
    if source == "local-keyword":
        # Explicit --server local: in-process Runtime, no daemon involved.
        return LocalClient(getattr(args, "config", None))
    # Nothing configured: route through the local daemon. No auth — the
    # daemon is on localhost, and resolving auth could shell out to
    # cloudflared when settings.cli_auth.cloudflared_app is set.
    return RemoteClient(_ensure_local_server(args), auth=None)


# ---------------------------------------------------------------------------
# `stack` subcommands
# ---------------------------------------------------------------------------


def _select_ssh_backends(
    config, stack: Stack, backend_filter: str | None,
) -> list[SlurmBackendConfig | PBSBackendConfig]:
    """Pick which SSH-capable backends a stack operation should run against.

    - If ``--backend`` is given, only that one (must exist and be SSH-based).
    - Else if ``stack.backends`` lists names, those (each must be SSH-based).
    - Else every Slurm/PBS backend in the config.
    """
    ssh_types = (SlurmBackendConfig, PBSBackendConfig)

    if backend_filter:
        b = config.get_backend(backend_filter)
        if b is None:
            raise RuntimeError(f"Backend '{backend_filter}' not found in config")
        if not isinstance(b, ssh_types):
            raise RuntimeError(
                f"Backend '{backend_filter}' ({b.type}) doesn't support stacks "
                f"(SSH-based only for now)"
            )
        return [b]

    if stack.backends:
        out = []
        for name in stack.backends:
            b = config.get_backend(name)
            if b is None:
                raise RuntimeError(
                    f"Stack '{stack.name}' references backend '{name}', "
                    f"which is not in the config"
                )
            if not isinstance(b, ssh_types):
                # Silently skip non-SSH; surfacing this per-call would be noisy
                continue
            out.append(b)
        return out

    return [b for b in config.backends if isinstance(b, ssh_types)]


def _ssh_client_for(backend_cfg: SlurmBackendConfig | PBSBackendConfig) -> SSHClient:
    """Build an SSHClient from a backend's SSH config (CLI-only; no shared pool)."""
    return SSHClient(
        host=backend_cfg.ssh.host,
        user=backend_cfg.ssh.user,
        key_path=backend_cfg.ssh.key_path_resolved,
        port=backend_cfg.ssh.port,
        cert_path=backend_cfg.ssh.cert_path_resolved,
        known_hosts=backend_cfg.ssh.known_hosts_resolved,
    )


def _format_size(n: int | None) -> str:
    if n is None:
        return "-"
    size = float(n)
    for unit in ("B", "K", "M", "G", "T"):
        if size < 1024 or unit == "T":
            return f"{size:.0f}{unit}" if unit == "B" else f"{size:.1f}{unit}"
        size /= 1024
    return f"{size}"


def _format_age(dt) -> str:
    if dt is None:
        return "-"
    from datetime import datetime, timezone
    delta = datetime.now(timezone.utc) - dt
    s = int(delta.total_seconds())
    if s < 60:
        return f"{s}s ago"
    if s < 3600:
        return f"{s // 60}m ago"
    if s < 86400:
        return f"{s // 3600}h ago"
    return f"{s // 86400}d ago"


def _print_status_table(statuses: list[tuple[str, str, StackStatus | None, str | None]]) -> None:
    """Render ``(stack_name, backend_name, status, error_or_skip)`` rows."""
    print(f"{'STACK':<20} {'BACKEND':<20} {'STATE':<12} {'HASH':<14} "
          f"{'BUILT':<12} {'SIZE':<8} NOTE")
    for name, backend, status, note in statuses:
        if status is None:
            print(f"{name[:20]:<20} {backend[:20]:<20} {'-':<12} {'-':<14} "
                  f"{'-':<12} {'-':<8} {note or ''}")
            continue
        print(
            f"{name[:20]:<20} {backend[:20]:<20} "
            f"{status.state.value:<12} {status.hash:<14} "
            f"{_format_age(status.last_built):<12} "
            f"{_format_size(status.size_bytes):<8} "
            f"{status.error or ''}"
        )


def _scheduler_kind(backend_cfg) -> str | None:
    """Map a backend config to the scheduler tag StackManager understands."""
    if isinstance(backend_cfg, SlurmBackendConfig):
        return "slurm"
    if isinstance(backend_cfg, PBSBackendConfig):
        return "pbs"
    return None


async def _run_per_backend(
    config,
    stacks: list[Stack],
    backend_filter: str | None,
    fn,
) -> list[tuple[str, str, StackStatus | None, str | None]]:
    """Iterate (stack × selected backend), open an SSH client, call ``fn``.

    ``fn`` is ``async (stack, backend_name, ssh, scheduler) -> StackStatus``.
    ``scheduler`` is "slurm"/"pbs"/None so the install path can decide
    whether to wrap prep with srun/qsub.
    """
    results: list[tuple[str, str, StackStatus | None, str | None]] = []
    for stack in stacks:
        try:
            backends = _select_ssh_backends(config, stack, backend_filter)
        except RuntimeError as e:
            results.append((stack.name, backend_filter or "-", None, str(e)))
            continue
        if not backends:
            results.append((stack.name, "-", None, "no SSH backends matched"))
            continue
        for backend_cfg in backends:
            ssh = _ssh_client_for(backend_cfg)
            try:
                await ssh.connect()
            except Exception as e:
                results.append((stack.name, backend_cfg.name, None, f"connect: {e}"))
                continue
            try:
                status = await fn(
                    stack, backend_cfg.name, ssh, _scheduler_kind(backend_cfg),
                )
                results.append((stack.name, backend_cfg.name, status, None))
            except Exception as e:
                results.append((stack.name, backend_cfg.name, None, str(e)))
            finally:
                await ssh.disconnect()
    return results


# ---------------------------------------------------------------------------
# `agent` subcommand — emit a prompt teaching a coding agent how to use the CLI
# ---------------------------------------------------------------------------


def _render_agent_prompt(config: ScriptHutConfig | None) -> str:
    """Compose a markdown briefing for a coding agent.

    The output mixes a static reference (CLI patterns the agent should
    use) with a live inventory of the current context (backends,
    stacks, workflows the user has actually configured) so the agent's
    suggestions reference real names. ``config=None`` produces a
    "no config found" variant that still teaches the CLI shape.
    """
    out: list[str] = []
    out.append("# ScriptHut Agent Brief\n")
    out.append(
        "ScriptHut submits compute jobs to remote backends (Slurm, PBS, "
        "AWS Batch, AWS EC2). Interact with it via the `scripthut` CLI. "
        "This briefing tells you what's available in the current project "
        "context and the minimum set of commands you need to be useful.\n"
    )

    out.append("## What's available here\n")
    if config is None:
        out.append(
            "_No `scripthut.yaml` was discovered in the current directory "
            "or in `~/.config/scripthut/`. Ask the user to create one or "
            "run `scripthut backend list` to confirm._\n"
        )
    else:
        # ---- Backends -----------------------------------------------------
        out.append("### Backends")
        if not config.backends:
            out.append("_No backends configured. Submitting work won't work until one is added._\n")
        else:
            for b in config.backends:
                bits = [f"`{b.name}` ({b.type})"]
                # Each backend type has different surfacing fields; keep
                # the prompt readable rather than dumping the whole model.
                if isinstance(b, (SlurmBackendConfig, PBSBackendConfig)):
                    bits.append(f"ssh `{b.ssh.user}@{b.ssh.host}`")
                    if b.account:
                        bits.append(f"account `{b.account}`")
                if isinstance(b, SlurmBackendConfig) and b.partition_map:
                    pairs = ", ".join(
                        f"{k}→{v}" for k, v in b.partition_map.items()
                    )
                    bits.append(f"partition map: {pairs}")
                out.append(f"- {' · '.join(bits)}")
            out.append("")

        # ---- Stacks -------------------------------------------------------
        out.append("### Stacks (reusable software environments)")
        if not config.stacks:
            out.append("_No stacks configured._\n")
        else:
            for s in config.stacks:
                where = ", ".join(s.backends) if s.backends else "all SSH backends"
                inputs = ", ".join(
                    f"`{k}={v}`" for k, v in s.inputs.items()
                )
                bits = [f"`{s.name}` — backends: {where}"]
                if inputs:
                    bits.append(f"inputs: {inputs}")
                out.append(f"- {' · '.join(bits)}")
            out.append(
                "\nRun `scripthut stack check <name> --json` before "
                "submitting a task that needs one — install with "
                "`scripthut stack install <name>` if it's not `ready`."
            )
            out.append("")

        # ---- Sources ------------------------------------------------------
        out.append("### Sources (workflow JSON catalogs)")
        if not config.sources:
            out.append(
                "_No sources configured. Source workflows live in a git "
                "repo or backend path; ask the user to add one or use the "
                "ad-hoc `task run` modes below._\n"
            )
        else:
            from scripthut.config_schema import (
                GitSourceConfig as _Git,
                PathSourceConfig as _Path,
            )
            for src in config.sources:
                bits = [f"`{src.name}` ({src.type})"]
                if isinstance(src, _Git):
                    bits.append(f"git `{src.url}` branch `{src.branch}`")
                    default_be = getattr(src, "backend", None)
                    if default_be:
                        bits.append(f"default backend `{default_be}`")
                elif isinstance(src, _Path):
                    bits.append(f"path `{src.path}` on `{src.backend}`")
                if getattr(src, "description", None):
                    bits.append(src.description)
                out.append(f"- {' · '.join(bits)}")
            out.append(
                "\nList workflows in a source: "
                "`scripthut source view <name> --json`. Each git source is "
                "pinned to a branch above — `scripthut workflow run` "
                "re-syncs the source on the server before submitting, so "
                "the run executes against **latest HEAD on that branch**. "
                "Pass `--branch <name>` to run from a different branch "
                "instead (workflow file and repo config are read at that "
                "branch's tip). "
                "If the user just pushed a *new* workflow file (not just "
                "edits to existing ones), run `scripthut source sync "
                "<name>` first so the workflow list picks it up."
            )
            out.append("")

    # ---------- static reference ------------------------------------------

    out.append("## Submitting work — pick the smallest tool that fits\n")
    out.append(
        "There are three input modes to `scripthut task run`. Pick the one "
        "that matches what you're trying to do — they're mutually exclusive.\n"
    )

    out.append("### A) `--inline-script <local-file>` (your file → the backend)\n")
    out.append(
        "Use this when you wrote a script locally and want to run it on the "
        "backend **without staging files first**. ScriptHut reads the local "
        "file, base64-embeds it into the task command, and the backend "
        "decodes and runs it. No git, no scp.\n"
        "\n"
        "```bash\n"
        "# Wrote /tmp/probe.py locally, want it to run on mercury-nb:\n"
        "scripthut task run --inline-script /tmp/probe.py \\\n"
        "  --backend mercury-nb --partition standard \\\n"
        "  --cpus 1 --memory 1G --time 0:05:00 --json\n"
        "```\n"
        "If your file has no `#!` line, `#!/bin/bash` is prepended for you. "
        "Best for files up to a few hundred KB; over that, fall back to a "
        "workflow with a git repo.\n"
    )

    out.append("### B) Positional command (one-liner)\n")
    out.append(
        "```bash\n"
        "scripthut task run \"python -c 'print(2 + 2)'\" \\\n"
        "  --backend mercury-nb --partition standard \\\n"
        "  --cpus 1 --memory 1G --time 0:05:00 --json\n"
        "```\n"
        "Good for genuine one-liners. If you find yourself quoting a multi-"
        "line script, switch to `--inline-script`.\n"
    )

    out.append("### C) `--from-stdin` (you've built a full TaskDefinition JSON)\n")
    out.append(
        "```bash\n"
        "scripthut task run --from-stdin --backend mercury-nb --json <<'JSON'\n"
        "{\n"
        '  "id": "exp-42",\n'
        '  "name": "experiment 42",\n'
        '  "command": "python train.py --lr 1e-3",\n'
        '  "cpus": 8,\n'
        '  "memory": "32G",\n'
        '  "time_limit": "4:00:00",\n'
        '  "partition": "gpu",\n'
        '  "gres": "gpu:1",\n'
        '  "working_dir": "/scratch/me/repo"\n'
        "}\n"
        "JSON\n"
        "```\n"
        "CLI flags layer on top of stdin/file bodies, so you can pipe a "
        "template and override single fields per submission.\n"
    )

    out.append("### Source workflow (a JSON file in a configured source)\n")
    out.append(
        "```bash\n"
        "scripthut workflow run <workflow.json> --source <name> "
        "--backend <backend> --json\n"
        "```\n"
        "- `--backend` is **required for git sources** unless the source "
        "config has its own `backend:` field (see the Sources inventory "
        "above).\n"
        "- The server re-syncs the source's git clone before submitting, "
        "so the run executes against **latest HEAD on the source's "
        "configured branch**. The user doesn't have to sync first to pick "
        "up edits to existing workflow files.\n"
        "- `--branch <name>` runs from a different branch of a git source "
        "(fetched on demand; the workflow file and the repo's "
        "`scripthut.yaml` are read at that branch's tip).\n"
        "- If the user pushed a *new* workflow file and `source view` "
        "doesn't show it yet, run `scripthut source sync <name>` to "
        "refresh the file list.\n"
        "- Prefer a source workflow over ad-hoc when one already does "
        "what you need — it carries the source's resolved env, partition "
        "map, and stack assumptions.\n"
    )

    out.append("### TaskDefinition shape\n")
    out.append(
        "- **Required**: `id` (string), `name` (string), `command` (bash).\n"
        "- **Resources**: `cpus` (int), `memory` (str, e.g. `\"4G\"`), "
        "`time_limit` (str, e.g. `\"1:00:00\"`), `partition` (str), "
        "`gres` (str, e.g. `\"gpu:1\"`), `working_dir` (str — absolute path "
        "on the *backend*, not your local CWD).\n"
        "- **Behavior**: `dependencies` (list of other task ids), "
        "`env` (list of `{set: {KEY: VAL}}` rules), `image` (container "
        "URI for AWS Batch/EC2).\n"
    )

    out.append("## Resource sizing — default small, escalate deliberately\n")
    out.append(
        "Cluster allocations are charged by reservation, not actual use, "
        "and oversized requests get queued behind tighter ones. Start "
        "conservative:\n"
        "\n"
        "| Task type           | cpus | memory | time     |\n"
        "|---------------------|------|--------|----------|\n"
        "| Probe / sanity check| 1    | 1G     | 0:05:00  |\n"
        "| Small Python script | 1–2  | 2G–4G  | 0:30:00  |\n"
        "| Single-GPU train    | 4–8  | 16G–32G| 1:00:00–4:00:00 |\n"
        "| Multi-GPU train     | 8–16 | 64G+   | size for the job |\n"
        "\n"
        "Bump these only when an earlier run actually hit OOM or timed out. "
        "Check the backend's partition list (see the inventory above) for "
        "the max wall-clock you can request on each partition.\n"
    )

    out.append("## Inspecting state\n")
    out.append(
        "```bash\n"
        "scripthut status                            # server reachable + auth OK?\n"
        "scripthut backend list --json               # backend connectivity/health\n"
        "scripthut source list --json                # sources configured on the server\n"
        "scripthut source view <name> --json         # workflow files in one source\n"
        "scripthut source sync [<name>] --json       # re-clone/re-glob + refresh workflow list\n"
        "scripthut stack check [<name>] --json       # stacks ready / missing / installing\n"
        "scripthut run list --json --limit 10        # recent runs\n"
        "scripthut run view <id> --json              # one run, item statuses + counts\n"
        "scripthut run logs <id> <task_id> --tail 100         # stdout (task output)\n"
        "scripthut run logs <id> <task_id> --error --tail 100 # stderr (task errors)\n"
        "scripthut run logs <id> <task_id> -f                 # tail a task live until terminal\n"
        "```\n"
    )

    out.append("## Exit codes\n")
    out.append(
        "- `0` — command succeeded.\n"
        "- `1` — error (bad arguments, unknown name, backend unreachable).\n"
        "- `2` — `run watch --exit-status` only: the run terminated FAILED/CANCELLED.\n"
    )

    out.append("## Editing scripthut.yaml (when the user wants config changes)\n")
    out.append(
        "ScriptHut reads two YAMLs and merges them. Put new entries in the "
        "right file — the loader rejects sections that belong in the other "
        "layer, so a mis-targeted edit fails loudly rather than silently "
        "doing nothing.\n"
        "\n"
        "- **User-global** at `~/.config/scripthut/scripthut.yaml`. "
        "Carries infrastructure: `backends`, `sources`, `settings`, "
        "`pricing`. Edit this when the user adds a cluster, a git/path "
        "source, or changes CLI defaults (`cli_server`, `cli_auth`).\n"
        "- **Project-local** at `./scripthut.yaml` (or any ancestor of "
        "the CWD). Carries per-project knobs: `stacks`, `workflows`, "
        "`env`, `env_groups`. Trying to put `backends`/`sources`/"
        "`settings`/`pricing` here raises a `ConfigError` at load time.\n"
        "\n"
        "**Merge semantics** (project-local overlays global):\n"
        "- `env` — concatenated, global first, then project. Project "
        "rules see the env that global rules set up.\n"
        "- `env_groups` — dict-merged, project keys win on collision.\n"
        "- `stacks` / `workflows` — by-name override, project wins.\n"
        "\n"
        "**env rule shape** — one entry in the `env:` list (or inside "
        "an `env_groups:` value). Combine keys freely on one rule:\n"
        "```yaml\n"
        "env:\n"
        "  - set:                       # set / overwrite vars\n"
        "      WANDB_PROJECT: balke-jmp\n"
        "      OMP_NUM_THREADS: \"4\"\n"
        "  - if:                         # AND across keys, list value = OR.\n"
        "      SCRIPTHUT_BACKEND_TYPE: slurm\n"
        "    set:\n"
        "      SLURM_EXPORT_ENV: ALL\n"
        "  - include: [cuda]             # inline an env_group at this position\n"
        "  - append:                     # join with ':' (PATH-style vars)\n"
        "      PYTHONPATH: ${PWD}/src\n"
        "  - init: |                     # raw bash; concatenated into extra_init\n"
        "      module load python/3.13\n"
        "```\n"
        "All `${name}` references expand against the env-resolved-so-far. "
        "Per-task `set:` always wins; resolver order is "
        "server → backend → repo-project → workflow-doc → task.\n"
        "\n"
        "**A source repo's `scripthut.yaml` also applies server-side.** "
        "If the source repo has a `scripthut.yaml` at its root, its "
        "`env` / `env_groups` are pulled in by the server when running "
        "any workflow from that source — sitting *above* the server's "
        "config / backend layer and *below* the workflow file's own "
        "inline rules. So a Julia repo can define `env_groups: "
        "{julia-1.12: [...]}` once at the root, and every workflow JSON "
        "in `.hut/workflows/` can just `env: [{include: [julia-1.12]}]` "
        "without duplicating the module-load text. The repo file is "
        "subject to the same project-local validation: `backends:` / "
        "`sources:` / `settings:` / `pricing:` are rejected (a repo must "
        "never redefine infrastructure server-side).\n"
        "\n"
        "**Repo-defined stacks are visible to the CLI with `--source`.** "
        "`scripthut stack list --source <name>` (and `check` / `install` "
        "/ `delete`) fetch the repo's `scripthut.yaml` via the server "
        "and overlay its `stacks:` on top of the operator's local config "
        "— source wins on name collision. Use this when an operator "
        "needs to install a stack defined by the repo without first "
        "cloning the repo locally.\n"
        "\n"
        "**Edit discipline** (this matters more than the schema):\n"
        "- **Read the file first** (don't `Write` blind). Make a minimal "
        "diff — preserve every other entry, comment, and ordering choice "
        "the user made. Never overwrite the whole YAML.\n"
        "- **One concept per edit.** \"Add a CUDA env group\" is one edit; "
        "incidental reformatting belongs in a separate one.\n"
        "- **Hot-reload, don't restart.** Stacks / workflows / env / "
        "env_groups changes hot-reload via the server's Settings page "
        "(Save & Reload). Backend changes still require a server "
        "restart. The CLI re-reads YAML on every invocation, so no "
        "reload step is needed there.\n"
        "- **Show the user the change** (the diff) before saving anything "
        "non-obvious.\n"
    )

    out.append("## Stacks — define once, install once, reference everywhere\n")
    out.append(
        "A **stack** is a content-hashed, cache-on-first-build software "
        "environment (Python venv, Julia depot, conda env, raw bash). "
        "Each stack has: a unique `name`, a `prep:` bash script that "
        "builds it, and an `init:` bash snippet tasks run to *enter* it "
        "(e.g. `source ~/.julia-1.12/install/activate`). Scripthut runs "
        "`prep` once per (backend × input hash) and caches the result; "
        "every task that references the stack just sources the `init:` "
        "text — fast, deterministic, shared across runs.\n"
        "\n"
        "### Where to define a stack\n"
        "- **Server-global** in `~/.config/scripthut/scripthut.yaml` "
        "under `stacks:` — shared across every project on this server.\n"
        "- **Per-repo** in `<repo>/scripthut.yaml` under `stacks:` — the "
        "stack travels with the repo and is automatically picked up by "
        "every run of that source's workflows (v0.7.0+). Same name in "
        "both places → the repo wins (project-local overrides global).\n"
        "\n"
        "```yaml\n"
        "stacks:\n"
        "  - name: julia-1.12\n"
        "    backends: [mercury-nb]            # optional: which backends "
        "this stack is built for; empty = all SSH backends\n"
        "    inputs: {julia_version: \"1.12.0\"} # hashed into the cache "
        "key — changing this triggers a rebuild\n"
        "    prep: |                            # bash, runs ONCE per "
        "(backend × hash)\n"
        "      curl -sSL https://install.julialang.org | sh -s -- --yes "
        "--default-channel 1.12\n"
        "    init: |                            # bash, runs on EVERY "
        "task that references this stack\n"
        "      export PATH=$HOME/.juliaup/bin:$PATH\n"
        "      export JULIA_NUM_THREADS=$(nproc)\n"
        "```\n"
        "\n"
        "### How to install (operator step — not automatic)\n"
        "Scripthut does NOT auto-install a stack at task submit time. "
        "The operator installs once before running any task that "
        "references it. The CLI has **two modes** for these commands:\n"
        "\n"
        "- **Local mode** — no `--server` / `SCRIPTHUT_SERVER` /"
        " `settings.cli_server`. The CLI reads the local "
        "`scripthut.yaml`, opens SSH to the backend itself, and runs "
        "`prep` synchronously. Iterates over every backend the stack "
        "is configured for when `--backend` is omitted. **Blocking**: "
        "the command sits until prep finishes.\n"
        "- **Server mode** — a server URL is resolvable. `check` / "
        "`delete` are still single-call server-side ops; **`install` "
        "v0.9.0+ submits a workflow run** and returns the run_id "
        "immediately. The actual prep runs as a normal scheduled job "
        "on the backend (sbatch on Slurm, etc.) and shows up in "
        "`scripthut run list` alongside everything else. Monitor it "
        "with `scripthut run view <id>`, tail with `scripthut run "
        "logs <id> install-XXXXXX -f`, or block until done with "
        "`scripthut run watch <id> --exit-status`. **`--backend` is "
        "required** in server mode.\n"
        "\n"
        "```bash\n"
        "# Server mode (cli_server / SCRIPTHUT_SERVER configured) —\n"
        "# install becomes a normal run:\n"
        "scripthut stack check <name> --backend <b>           # READY / "
        "MISSING / INSTALLING for one stack on one backend\n"
        "scripthut stack install <name> --backend <b>         # "
        "submits a run; prints the run_id and hint\n"
        "scripthut stack install <name> --backend <b> --watch # submit "
        "AND block until done (returns non-zero on failure)\n"
        "scripthut stack install <name> --backend <b> --json | jq -r .id "
        "# capture the run_id for scripts/CI\n"
        "scripthut stack install <name> --source <src> --backend <b>  "
        "# build a repo-defined stack — server re-syncs the source's "
        "git HEAD first, then submits prep as a run\n"
        "scripthut stack install <name> --backend <b> --rebuild   "
        "# wipe and re-run prep even when the input hash matches\n"
        "scripthut stack delete <name> --backend <b>          # rm -rf "
        "the cache (every hash for that name on that backend)\n"
        "```\n"
        "The first time a stack is installed on a backend, `prep:` runs "
        "as a normal job on that backend (so cluster modules, GPUs, etc. "
        "are available). Subsequent invocations of the same stack on "
        "the same backend find a `.ready` sentinel and exit 0 "
        "immediately — the install still appears in `run list` as a "
        "successful run, so the operator sees \"I tried to install "
        "julia, it was already ready.\"\n"
        "\n"
        "**Long installs are not a problem anymore.** Because server-"
        "mode install is a queued run rather than a blocking HTTP call, "
        "Cloudflare-Access timeouts don't matter — the CLI submits "
        "and exits; the run finishes server-side at its own pace. The "
        "agent should `scripthut run view $RUN_ID --json` or `run "
        "watch $RUN_ID --exit-status` to track it.\n"
        "\n"
        "### How a task uses a stack (the v0.7.1 way)\n"
        "Tasks reference stacks via the **`stacks:`** field on any env "
        "rule. The resolver expands each reference to a synthetic env "
        "rule carrying the stack's `init:` text, then continues with "
        "the rule's own `set:` / `append:` / `init:` as usual.\n"
        "\n"
        "```json\n"
        "{\n"
        "  \"id\": \"train\", \"name\": \"train\", "
        "\"command\": \"julia train.jl\",\n"
        "  \"env\": [\n"
        "    {\"stacks\": [\"julia-1.12\"]},\n"
        "    {\"set\": {\"JULIA_NUM_THREADS\": \"8\"}}\n"
        "  ]\n"
        "}\n"
        "```\n"
        "Or, in YAML for an env_group that several workflows include:\n"
        "```yaml\n"
        "env_groups:\n"
        "  julia:\n"
        "    - stacks: [julia-1.12]\n"
        "    - set: {JULIA_NUM_THREADS: \"8\"}\n"
        "```\n"
        "Then any workflow file in the repo can use it with "
        "`env: [{include: [julia]}]`.\n"
        "\n"
        "**Multiple stacks per rule are allowed**: `stacks: [julia-1.12, "
        "cuda-12]` expands to both inits in order. Combine with `if:` "
        "to conditionally enable a stack: `{if: {SCRIPTHUT_BACKEND: "
        "mercury-nb}, stacks: [cuda-12]}` only fires on that backend.\n"
        "\n"
        "### Critical gotchas — read these before suggesting `stacks:`\n"
        "- **Unknown stack name → ValueError at submit time, run fails.** "
        "There is no silent-skip. If `stacks: [julia-1.13]` is typoed or "
        "the stack isn't configured anywhere, the run fails with "
        "`stack 'julia-1.13' referenced from … but not defined`. Catch "
        "typos by running `scripthut stack check` and `scripthut source "
        "view <name>` (the latter lists what's in the repo).\n"
        "- **No auto-install at submit.** A task that references a stack "
        "whose `init:` would source a path that doesn't exist on the "
        "backend will fail inside the user's script (not at scripthut's "
        "layer) — the stack must be `READY` first. Always run "
        "`scripthut stack check <name>` before suggesting a run that "
        "uses it; install if not ready.\n"
        "- **Stacks are NOT a `TaskDefinition` field.** The wiring "
        "happens through env rules — there is no `task.stack` or "
        "`workflow.stacks` shortcut. The author writes `env: [{stacks: "
        "[…]}]` (per-task, per-workflow, or via an env_group). This is "
        "intentional: same composition rules as the rest of the env "
        "system, no separate code path.\n"
    )

    out.append("## Emitting structured outputs (plots, summaries, tables)\n")
    out.append(
        "Tasks can emit files that scripthut renders in the run-detail "
        "UI — much more useful than scraping logs for results. Three "
        "env vars are exported by the script wrapper *before* the "
        "user's command runs; the agent can use them when constructing "
        "tasks that produce results worth showing.\n"
        "\n"
        "- **`$SCRIPTHUT_OUTPUT_DIR`** — directory the task writes any "
        "file to. Markdown (`.md`), images (`.png` / `.jpg` / `.svg` / "
        "`.gif` / `.webp`), and \"other\" files surface in a per-task "
        "**Outputs** subtab under the Details panel. Markdown is "
        "rendered (server-side sanitized) with `tables` / "
        "`fenced_code` extensions; images embed inline; other files "
        "appear as download links.\n"
        "- **`$SCRIPTHUT_TASK_SUMMARY`** — convenience path "
        "(`$SCRIPTHUT_OUTPUT_DIR/task-summary.md`) that renders "
        "prominently at the top of the per-task Outputs panel. Use it "
        "for the task's headline markdown.\n"
        "- **`$SCRIPTHUT_RUN_SUMMARY`** — a *separate* markdown file "
        "the task writes for the run-wide **Summary** panel. Each "
        "task's contribution becomes one section, concatenated in "
        "submission order with task-name headings at the top of the "
        "run-detail page.\n"
        "\n"
        "**Example task command** the agent can synthesize:\n"
        "```bash\n"
        "python train.py --output-plot=\"$SCRIPTHUT_OUTPUT_DIR/loss.png\"\n"
        "cat > \"$SCRIPTHUT_TASK_SUMMARY\" <<'MD'\n"
        "## Loss curve\n"
        "![](loss.png)\n"
        "\n"
        "| epoch | loss |\n"
        "|---|---|\n"
        "| 10  | 0.5 |\n"
        "| 100 | 0.04 |\n"
        "MD\n"
        "echo \"- **${SCRIPTHUT_TASK_NAME:-task}**: final loss 0.04\" "
        "> \"$SCRIPTHUT_RUN_SUMMARY\"\n"
        "```\n"
        "\n"
        "**Behaviors worth knowing before suggesting `$SCRIPTHUT_OUTPUT_DIR`:**\n"
        "- **SSH-only (v0.11.0):** Slurm + PBS backends only. AWS "
        "Batch and EC2 (API-only mode) silently ignore the exports — "
        "tasks still run, they just don't produce a panel. Don't "
        "tell the user \"this will show up in the UI\" when the run "
        "is on Batch / EC2.\n"
        "- **Collected post-completion**, not streamed. The Outputs "
        "subtab and Run Summary panel populate when the item leaves "
        "SETTLING → COMPLETED. During the run the user sees the live "
        "stdout/stderr in the existing Output tab; structured "
        "renderings only appear after.\n"
        "- **Relative `<img src=\"rel.png\">` in markdown is "
        "rewritten** to point at the per-task file endpoint, so "
        "authoring `![](plot.png)` next to where you wrote "
        "`plot.png` Just Works. Absolute URLs (`http(s)://`, `/`) "
        "pass through unchanged.\n"
        "- **Markdown is sanitized** with bleach (script tags + event "
        "handlers stripped) — safe for any markdown the user (or the "
        "agent) authors. Allowed tags: headings, paragraphs, lists, "
        "tables, code blocks, blockquotes, `<img>`, links.\n"
        "- **Size limits**: 5 MB per file, 200 files per task. "
        "Oversize files are dropped from the listing (the file still "
        "exists on the backend; it just doesn't surface). Markdown "
        "files over 1 MB show as a download link rather than being "
        "rendered inline. If your run will produce a plot per "
        "iteration of a 10k-step training loop, write one summary "
        "image, not 10k.\n"
        "- **The directories are `mkdir -p`'d for you** before the "
        "command runs, so `python -c \"...; plt.savefig('$SCRIPTHUT_OUTPUT_DIR/foo.png')\"` "
        "works without any preflight.\n"
        "- **Empty tasks are first-class** — a task that writes "
        "nothing to either path simply doesn't get an Outputs subtab "
        "or contribute to the Run Summary card. Don't add no-op "
        "`touch` calls.\n"
        "\n"
        "**When to use which surface:**\n"
        "- Per-task results that someone reviewing *that specific "
        "task* would want — diagnostic plots, hyperparameter tables, "
        "convergence curves, error analyses → "
        "`$SCRIPTHUT_TASK_SUMMARY` + files in `$SCRIPTHUT_OUTPUT_DIR`.\n"
        "- Run-wide TL;DR for someone scanning the run-list "
        "page — best-of-sweep, final test loss, total epochs run, "
        "one-line per task → `$SCRIPTHUT_RUN_SUMMARY`.\n"
        "- Don't duplicate. If a single sentence belongs in both, "
        "write it in `$SCRIPTHUT_RUN_SUMMARY` and link from "
        "`$SCRIPTHUT_TASK_SUMMARY`.\n"
    )

    out.append("## Gotchas\n")
    out.append(
        "- **`working_dir` is a path on the backend**, not your local "
        "filesystem. If you need files on the backend, either use a stack, "
        "use a workflow that clones a git repo, or use `--inline-script` "
        "for self-contained code.\n"
        "- **Partition names are remapped per backend** via `partition_map` "
        "(see the inventory above). Use the *logical* name (e.g. `gpu`, "
        "`standard`) from the project YAML, not the cluster's raw name.\n"
        "- **Stacks need to be `ready` before tasks that use them can run.** "
        "Always `stack check` first; install if needed; only then submit.\n"
        "- **Paths in YAML are resolved relative to that YAML file**, not "
        "your CWD. Use absolute paths in TaskDefinition fields you build.\n"
        "- **The `--json` shape from `task run` matches `workflow run`** — "
        "capture `id` and pipe it to `run view` / `run watch`.\n"
        "- **In local mode, status doesn't refresh on its own.** Re-run "
        "`scripthut run view <id> --json` to poll. Against a running server "
        "(`SCRIPTHUT_SERVER` set), live tracking works.\n"
    )

    out.append("## Typical agent loop — verify, then submit\n")
    out.append(
        "Always do the verify phase first; submitting blind to a backend "
        "the user thought was down or to a partition that doesn't exist on "
        "this cluster is a bad time.\n"
        "\n"
        "**Verify** (cheap, read-only):\n"
        "1. `scripthut status` — confirm the server is reachable and your "
        "auth is working. If this fails, nothing below will work either; "
        "stop and report the diagnostic to the user.\n"
        "2. `scripthut agent prompt` — re-read whenever the user's project "
        "context changes (new source, new backend, branch swap).\n"
        "3. `scripthut backend list --json` — confirm targets are connected.\n"
        "4. `scripthut source list --json` and "
        "`scripthut source view <name> --json` if you might trigger an "
        "existing source workflow instead of ad-hoc — almost always better, "
        "because the workflow carries the resolved env, partition map, and "
        "stack assumptions.\n"
        "5. If the user just pushed a *new* workflow file (one not yet in "
        "`source view`'s output), run `scripthut source sync <name>` so "
        "the file list picks it up. Edits to *existing* files are picked "
        "up automatically on `workflow run`.\n"
        "6. If using a stack: `scripthut stack check <name> --json`; install "
        "with `scripthut stack install <name>` if not `ready`.\n"
        "7. `scripthut task run ... --dry-run` to print the assembled "
        "TaskDefinition. Show it to the user for anything non-trivial.\n"
        "\n"
        "**Submit + track**:\n"
        "8. Submit with `--json` and capture `id`. For a source workflow:\n"
        "   ```bash\n"
        "   RUN_ID=$(scripthut workflow run train.json \\\n"
        "     --source <name> --backend <backend> --json | jq -r .id)\n"
        "   ```\n"
        "   For an ad-hoc task: same shape, `task run` instead.\n"
        "9. **Status**: `scripthut run view $RUN_ID --json` to see counts "
        "and per-item statuses (`SUBMITTED`/`RUNNING`/`COMPLETED`/`FAILED`). "
        "Re-poll until the top-level `status` is terminal. Against a "
        "running server, `scripthut run watch $RUN_ID --exit-status` "
        "blocks until done and exits 2 on failure.\n"
        "10. **Output / logs** while running or after:\n"
        "    - `scripthut run logs $RUN_ID <task_id> -f` — tail stdout "
        "live until the task is terminal (best while running).\n"
        "    - `scripthut run logs $RUN_ID <task_id> --tail 200` — last "
        "200 lines of stdout (after the fact).\n"
        "    - `scripthut run logs $RUN_ID <task_id> --error --tail 200` "
        "— stderr; use this first when a task ends in `FAILED`.\n"
        "11. On failure: read stderr (step 10), then check whether the "
        "issue is a stack (`stack check`), a resource limit (OOM/TIMEOUT "
        "shows up in stderr or `run view`), or the workflow content "
        "itself. Bump the relevant field and resubmit a *new* run rather "
        "than rerunning the same one.\n"
    )

    return "\n".join(out)


async def _cmd_agent_prompt(args: argparse.Namespace) -> int:
    try:
        config: ScriptHutConfig | None = load_config(getattr(args, "config", None))
    except (FileNotFoundError, ConfigError):
        config = None
    print(_render_agent_prompt(config))
    return 0


async def _cmd_agent_install(args: argparse.Namespace) -> int:
    from scripthut import agent_skill

    if args.user:
        claude_dir = Path.home() / ".claude"
    else:
        claude_dir = Path.cwd() / ".claude"

    results = agent_skill.install_assets(claude_dir, force=args.force)

    skipped = False
    for r in results:
        try:
            shown: Path | str = r.path.relative_to(Path.cwd())
        except ValueError:
            shown = r.path
        print(f"  {r.action:<9} {shown}")
        if r.action == "skipped":
            skipped = True

    if skipped:
        sys.stdout.flush()
        print(
            "\nSkipped files exist but weren't written by scripthut; "
            "re-run with --force to overwrite.",
            file=sys.stderr,
        )
        return 1
    print(
        "\nInstalled. Claude Code picks these up automatically: the "
        "`scripthut` skill loads when a task involves submitting or "
        "monitoring compute, and `/scripthut:debug <run-id>` diagnoses a "
        "failed run."
    )
    return 0


# ---------------------------------------------------------------------------
# `status` subcommand — diagnose CLI connectivity to the server
# ---------------------------------------------------------------------------


def _gather_status(args: argparse.Namespace) -> dict[str, Any]:
    """Collect everything ``scripthut status`` reports on, as a plain dict.

    Returns a dict (used directly for ``--json`` output and consumed by
    ``_render_status`` for the human view). Pure config/path inspection
    only — the HTTP probes happen separately in ``_probe_server`` so we
    can stay synchronous here and unit-test without mocking httpx.
    """
    from scripthut import __version__  # type: ignore

    server, server_source = _resolve_server_with_source(args)
    # Describe-only: don't shell out to cloudflared just to render status.
    auth_res = _resolve_auth_with_source(args, fetch_cloudflared=False)

    # Surface which config files were loaded — the most common "why is
    # nothing being picked up?" failure mode is having the wrong file.
    explicit_cfg = getattr(args, "config", None)
    config_paths: dict[str, str | None] = {}
    if explicit_cfg:
        config_paths["explicit"] = str(explicit_cfg)
    else:
        from scripthut.config import discover_global_config, discover_project_config
        g = discover_global_config()
        p = discover_project_config()
        config_paths["global"] = str(g) if g else None
        config_paths["project"] = str(p) if p else None

    config: ScriptHutConfig | None
    config_error: str | None = None
    try:
        config = load_config(explicit_cfg)
    except (FileNotFoundError, ConfigError) as e:
        config = None
        config_error = str(e)

    install_path: str | None
    try:
        import scripthut as _pkg
        install_path = (
            str(Path(_pkg.__file__).parent) if _pkg.__file__ else None
        )
    except Exception:
        install_path = None

    return {
        "version": __version__,
        "install_path": install_path,
        "config_paths": config_paths,
        "config_error": config_error,
        "server": server,
        "server_source": server_source,
        "auth": {
            "method": auth_res.method,
            "source": auth_res.source,
            "cloudflared_app": auth_res.cloudflared_app,
        },
        # Local-mode sources/backends from the loaded config; in remote
        # mode these are replaced by what the server reports.
        "local_sources": (
            _local_source_summaries(config) if config is not None else []
        ),
        "local_backends": (
            [{"name": b.name, "type": b.backend_type} for b in config.backends]
            if config is not None else []
        ),
    }


def _local_source_summaries(config: ScriptHutConfig) -> list[dict[str, Any]]:
    """Compact dicts for the status renderer's local-mode source listing."""
    from scripthut.config_schema import GitSourceConfig, PathSourceConfig
    out: list[dict[str, Any]] = []
    for s in config.sources:
        base: dict[str, Any] = {
            "name": s.name, "type": s.type,
            "description": getattr(s, "description", ""),
        }
        if isinstance(s, GitSourceConfig):
            base.update({"url": s.url, "branch": s.branch})
        elif isinstance(s, PathSourceConfig):
            base.update({"path": s.path, "backend": s.backend})
        out.append(base)
    return out


async def _probe_server(
    server: str, auth: RemoteAuth | None, *, timeout: float = 5.0,
) -> dict[str, Any]:
    """Hit ``/api/v1/health`` and ``/api/v1/sources`` (both authenticated).

    Both probes send any configured auth headers. ``/health`` mainly
    answers "did the server respond at all?" — a connection error means
    the host/network is the problem, a 302 means Cloudflare Access
    rejected the credentials. ``/sources`` is the data-fetching probe
    and additionally tells us auth is good *and* the API surface is
    reachable.

    The two probes used to differ on auth (health unauthenticated), but
    that left a ⚠ even when the auth path worked, because a CF-Access
    server 302s every unauthenticated request. Now both probes carry
    the same headers; the renderer distinguishes "server unreachable"
    (connection error on /health) from "auth rejected at the edge"
    (302 on either) from "auth OK" (200).
    """
    headers = auth.headers() if auth is not None else {}
    base = server.rstrip("/") + "/api/v1"
    result: dict[str, Any] = {
        "health": {},
        "authorized": {},
    }

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
        t0 = asyncio.get_event_loop().time()
        try:
            r = await client.get(f"{base}/health", headers=headers)
            elapsed_ms = int((asyncio.get_event_loop().time() - t0) * 1000)
            result["health"] = {
                "status_code": r.status_code,
                "elapsed_ms": elapsed_ms,
                "redirect_location": r.headers.get("location"),
            }
        except httpx.RequestError as e:
            result["health"] = {"error": f"{type(e).__name__}: {e}"}

        # Authorization — with headers; if creds work this is a real test
        # that we can talk to /api/v1.
        try:
            r = await client.get(f"{base}/sources", headers=headers)
            result["authorized"] = {
                "status_code": r.status_code,
                "redirect_location": r.headers.get("location"),
            }
            if r.status_code == 200:
                try:
                    result["server_data"] = r.json()
                except Exception:
                    pass
        except httpx.RequestError as e:
            result["authorized"] = {"error": f"{type(e).__name__}: {e}"}

        # Backends — only meaningful if /sources succeeded.
        if result["authorized"].get("status_code") == 200:
            try:
                r = await client.get(f"{base}/backends", headers=headers)
                if r.status_code == 200:
                    result["server_data_backends"] = r.json()
            except httpx.RequestError:
                pass

    return result


def _render_status(data: dict[str, Any], probe: dict[str, Any] | None) -> str:
    """Format ``_gather_status`` + ``_probe_server`` output for humans."""
    lines: list[str] = []
    lines.append(f"scripthut {data['version']}")
    if data.get("install_path"):
        lines.append(f"  installed at {data['install_path']}")
    lines.append("")

    cfg_paths = data["config_paths"]
    if "explicit" in cfg_paths:
        lines.append(f"Config:        {cfg_paths['explicit']}  (--config)")
    else:
        g = cfg_paths.get("global") or "(none)"
        p = cfg_paths.get("project") or "(none)"
        lines.append(f"Config:        global={g}")
        lines.append(f"               project={p}")
    if data.get("config_error"):
        lines.append(f"               ! {data['config_error']}")

    server = data["server"]
    src = data["server_source"]
    if server:
        lines.append(f"Server:        {server}  (from {_describe_source(src)})")
    elif src == "local-keyword":
        lines.append("Server:        local mode (--server local)")
    else:
        lines.append(
            "Server:        local daemon "
            "(no --server / SCRIPTHUT_SERVER / settings.cli_server set)"
        )

    ld = data.get("local_daemon")
    if ld is not None:
        if ld.get("error"):
            lines.append(f"Local daemon:  ?  {ld['error']}")
        elif ld.get("running"):
            lines.append(
                f"Local daemon:  ✓  running (pid {ld['pid']}) at {ld['url']}"
            )
            lines.append(f"               log: {ld['log_path']}")
        elif ld.get("foreign"):
            lines.append(
                "Local daemon:  ✗  the configured port is serving something "
                "that isn't scripthut"
            )
        else:
            lines.append(
                f"Local daemon:  ○  not running "
                f"(autostart: {ld.get('autostart')})"
            )

    auth = data["auth"]
    if not server:
        lines.append("Auth:          n/a (local mode)")
    elif auth["method"] == "none":
        lines.append("Auth:          (none — server reachable only if unprotected)")
    elif auth["method"] == "jwt-pending":
        lines.append(
            f"Auth:          cloudflared (app={auth['cloudflared_app']})  "
            f"(from {_describe_source(auth['source'])})  [not fetched]"
        )
    else:
        lines.append(
            f"Auth:          {auth['method']}  "
            f"(from {_describe_source(auth['source'])})"
        )

    if probe is not None:
        h = probe["health"]
        if "error" in h:
            lines.append(f"Reachable:     ✗  {h['error']}")
        else:
            code = h["status_code"]
            ms = h.get("elapsed_ms")
            ms_str = f" in {ms} ms" if ms is not None else ""
            marker = "✓" if code == 200 else ("⚠" if code == 302 else "✗")
            lines.append(f"Reachable:     {marker}  GET /api/v1/health → {code}{ms_str}")
            if code == 302 and h.get("redirect_location"):
                # Surface the Access subdomain — sometimes the *team*
                # is what's wrong (e.g. typo'd server URL).
                loc = h["redirect_location"]
                lines.append(f"               → redirected to {loc[:80]}")

        a = probe["authorized"]
        if "error" in a:
            lines.append(f"Authorized:    ✗  {a['error']}")
        else:
            code = a["status_code"]
            marker = "✓" if code == 200 else "✗"
            lines.append(f"Authorized:    {marker}  GET /api/v1/sources → {code}")
            if code != 200:
                lines.append(_authorization_hint(code, a.get("redirect_location"), auth))

    # Sources + backends
    if probe and probe.get("server_data"):
        sd = probe["server_data"]
        sources = sd.get("sources", [])
        if sources:
            lines.append(f"Sources:       {len(sources)} configured on server")
            for s in sources:
                lines.append(f"  - {_format_source_line(s)}")
        else:
            lines.append("Sources:       none configured on server")
    elif not server and data["local_sources"]:
        lines.append(
            f"Sources:       {len(data['local_sources'])} configured locally"
        )
        for s in data["local_sources"]:
            lines.append(f"  - {_format_source_line(s)}")

    if probe and probe.get("server_data_backends"):
        bs = probe["server_data_backends"].get("backends", [])
        if bs:
            parts = [
                f"{b['name']} ({'connected' if b.get('connected') else 'down'})"
                for b in bs
            ]
            lines.append(f"Backends:      {', '.join(parts)}")
    elif not server and data["local_backends"]:
        names = ", ".join(b["name"] for b in data["local_backends"])
        lines.append(f"Backends:      {names}  (local config; not probed)")

    return "\n".join(lines)


def _format_source_line(s: dict[str, Any]) -> str:
    """One-line representation of a source for status output."""
    desc = f"  — {s['description']}" if s.get("description") else ""
    if s.get("type") == "git":
        loc = f"{s.get('url', '?')}@{s.get('branch', 'main')}"
    else:
        loc = f"[{s.get('backend', '?')}] {s.get('path', '?')}"
    return f"{s['name']:<20} ({s.get('type', '?')})  {loc}{desc}"


def _describe_source(src: str | None) -> str:
    """Turn an internal source tag into a human label."""
    return {
        "flag": "CLI flag",
        "env": "env var",
        "config": "settings.cli_server / cli_auth",
        "cloudflared": "cloudflared",
        "local-keyword": "--server local",
        "none": "nothing configured",
    }.get(src or "", src or "?")


def _authorization_hint(
    code: int, redirect_location: str | None, auth: dict[str, Any],
) -> str:
    """Suggest a remediation for an authorization failure."""
    if code == 302 and redirect_location and "cloudflareaccess.com" in redirect_location:
        if auth["method"] in ("none", "jwt-pending"):
            if auth.get("cloudflared_app"):
                return (
                    "               → no token sent. Try: "
                    f"cloudflared access login {auth['cloudflared_app']}"
                )
            return (
                "               → server is behind Cloudflare Access but no "
                "credentials are configured. See README §Talking to a remote server."
            )
        return (
            "               → token rejected by Cloudflare. Likely expired — "
            "re-run `cloudflared access login <server>`."
        )
    if code in (401, 403):
        return "               → credentials rejected by the scripthut server."
    return f"               → unexpected status {code}."


async def _cmd_status(args: argparse.Namespace) -> int:
    data = _gather_status(args)

    # In daemon mode (nothing configured), report whether a local daemon
    # is up — informational only, never triggers a spawn.
    if data["server"] is None and data["server_source"] == "none" and not getattr(
        args, "quick", False,
    ):
        from scripthut import daemon

        cfg: ScriptHutConfig | None
        try:
            cfg = load_config(getattr(args, "config", None))
        except Exception:
            cfg = None
        try:
            local_daemon = daemon.daemon_status(cfg)
        except Exception as e:
            local_daemon = {"error": str(e)}
        local_daemon["autostart"] = (
            cfg.settings.cli_autostart if cfg is not None else "ask"
        )
        data["local_daemon"] = local_daemon

    probe: dict[str, Any] | None = None
    if data["server"] and not getattr(args, "quick", False):
        # Re-resolve auth with cloudflared fetch enabled so the probe is
        # realistic; if it errors, surface it in the auth section.
        try:
            auth_res = _resolve_auth_with_source(args, fetch_cloudflared=True)
            real_auth = auth_res.auth
            if auth_res.cloudflared_error:
                data["auth"]["cloudflared_error"] = auth_res.cloudflared_error
        except Exception as e:
            real_auth = None
            data["auth"]["cloudflared_error"] = str(e)
        probe = await _probe_server(data["server"], real_auth)

    if getattr(args, "json", False):
        out = {**data, "probe": probe}
        print(json.dumps(out, indent=2))
        return 0

    print(_render_status(data, probe))
    # Exit non-zero on failure so this composes in scripts; in --quick
    # mode we don't probe and so can't fail the check.
    if probe is not None:
        h = probe["health"]
        a = probe["authorized"]
        if "error" in h or "error" in a:
            return 2
        if a.get("status_code") != 200:
            return 2
    return 0


_INLINE_SCRIPT_SIZE_WARN = 200 * 1024  # bytes; warn above this (base64 inflates ~1.33x)


def _build_inline_script_command(script_path: Path) -> str:
    """Return a one-liner that decodes a local script and runs it on the backend.

    Layout: base64-encode the file's bytes, embed in a shell snippet that
    writes them to ``mktemp``, ``chmod +x``, executes, and propagates the
    exit code (cleaning up the temp file in all cases). If the file
    doesn't start with ``#!``, ``#!/bin/bash`` is prepended so it's
    self-executable regardless of how the OS invokes scripts without a
    shebang.

    Trade-off: very large scripts inflate the sbatch command line.
    Slurm typically accepts ~64KB scripts comfortably; over a few
    hundred KB you should use git or a real file-push instead.
    """
    import base64

    if not script_path.exists():
        raise RuntimeError(f"--inline-script: file not found: {script_path}")
    # Normalize line endings to LF: the target is always a Linux backend's
    # bash, which fails noisily on \r in shebangs ("/usr/bin/env: 'python3\r'").
    # Doing this in the script-reader instead of relying on the user's
    # editor settings makes Windows -> Linux submissions just work.
    data = script_path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    if not data.startswith(b"#!"):
        data = b"#!/bin/bash\n" + data
    if len(data) > _INLINE_SCRIPT_SIZE_WARN:
        logger.warning(
            f"--inline-script: {script_path} is {len(data) // 1024} KB — "
            f"large inline scripts inflate the submission payload; consider "
            f"staging via git or a shared filesystem if it grows further."
        )
    encoded = base64.b64encode(data).decode("ascii")
    # Use a single-quoted blob so $-expansion doesn't run on the base64;
    # mktemp + trap for cleanup. The trap fires on EXIT so the temp file
    # is removed regardless of which exit path the script takes.
    return (
        f'__SH_TMP=$(mktemp /tmp/scripthut-script-XXXXXX) && '
        f"trap 'rm -f \"$__SH_TMP\"' EXIT && "
        f"echo '{encoded}' | base64 -d > \"$__SH_TMP\" && "
        f'chmod +x "$__SH_TMP" && "$__SH_TMP"'
    )


def _build_adhoc_task_dict(args: argparse.Namespace) -> dict:
    """Assemble a TaskDefinition-shaped dict from CLI args / stdin / file.

    Input sources (mutually exclusive — at most one of these):

    - positional ``command`` string
    - ``--from-stdin``: TaskDefinition JSON on stdin
    - ``--from-file <path>``: TaskDefinition JSON in a local file
    - ``--inline-script <path>``: local script file, base64-bootstrapped
      so the backend can execute it without any prior file staging

    Per-field flags (``--cpus``, ``--memory``, …) are layered on top of
    whichever source provided the base, so an agent can pipe a JSON
    template and tweak individual fields per submission. ``id`` and
    ``name`` default to a short hash so two ad-hoc runs don't collide.
    """
    import hashlib
    import time

    # At most one source. Empty positional is the "not given" sentinel
    # because argparse `nargs="?"` yields None when absent.
    sources_given = [
        bool(args.command),
        bool(args.from_stdin),
        bool(args.from_file),
        bool(getattr(args, "inline_script", None)),
    ]
    if sum(sources_given) > 1:
        raise RuntimeError(
            "Specify exactly one of: positional command, --from-stdin, "
            "--from-file, --inline-script"
        )

    base: dict = {}
    if args.from_file:
        path = Path(args.from_file).expanduser()
        base = json.loads(path.read_text())
    elif args.from_stdin:
        base = json.loads(sys.stdin.read())
    elif getattr(args, "inline_script", None):
        script_path = Path(args.inline_script).expanduser()
        base = {"command": _build_inline_script_command(script_path)}
    elif args.command:
        base = {"command": args.command}
    else:
        raise RuntimeError(
            "Provide a command argument, --from-stdin, --from-file, "
            "or --inline-script"
        )

    # Per-flag overrides (only set if the user passed the flag).
    for key, attr in (
        ("name", "name"),
        ("id", "id"),
        ("cpus", "cpus"),
        ("memory", "memory"),
        ("time_limit", "time_limit"),
        ("partition", "partition"),
        ("working_dir", "working_dir"),
        ("gres", "gres"),
        ("image", "image"),
    ):
        val = getattr(args, attr, None)
        if val is not None:
            base[key] = val

    # --env KEY=VAL stays simple here: collected into a set: {...} EnvRule.
    if args.env:
        env_rules = list(base.get("env", []))
        env_kv: dict = {}
        for item in args.env:
            if "=" not in item:
                raise RuntimeError(
                    f"--env value '{item}' must be KEY=VALUE"
                )
            k, _, v = item.partition("=")
            env_kv[k.strip()] = v
        if env_kv:
            env_rules.append({"set": env_kv})
        base["env"] = env_rules

    # Defaults for the two required fields if not yet present.
    if "command" not in base:
        raise RuntimeError(
            "Task has no 'command' — provide one as a positional arg, "
            "via --from-file/--from-stdin, or in the JSON body"
        )
    if "id" not in base:
        # 8-char id hashed over (command + time + cryptographic randomness).
        # Time alone isn't enough — Windows' time.time_ns() resolution is
        # coarse (~15 ms), so back-to-back calls with the same command
        # would collide. os.urandom guarantees uniqueness regardless of
        # clock resolution.
        import os
        seed = (
            f"{base['command']}|{time.time_ns()}".encode()
            + os.urandom(8)
        )
        base["id"] = "adhoc-" + hashlib.sha256(seed).hexdigest()[:8]
    if "name" not in base:
        base["name"] = base["id"]

    return base


async def _cmd_task_run(args: argparse.Namespace) -> int:
    task_dict = _build_adhoc_task_dict(args)

    if args.dry_run:
        # Print the assembled TaskDefinition and exit without hitting any
        # backend — lets agents verify the payload before committing.
        print(json.dumps({"task": task_dict, "backend": args.backend}, indent=2))
        return 0

    async with _make_client(args) as client:
        summary = await client.run_task(
            task_dict, backend=args.backend, run_name=args.run_name,
        )

    if args.json:
        print(json.dumps(summary, indent=2))
        return 0
    print(
        f"Run {summary['id']} submitted to {args.backend} "
        f"(task '{task_dict['id']}')."
    )
    print(f"  scripthut run view {summary['id']}")
    return 0


async def _overlay_source_stacks(
    args: argparse.Namespace, config: ScriptHutConfig,
) -> ScriptHutConfig:
    """Fetch ``/api/v1/sources/{name}/config`` and overlay its stacks.

    When ``args.source`` is unset, this is a no-op. When set, the source
    must be configured on the resolved server; its project-local
    ``scripthut.yaml`` ``stacks:`` are merged into ``config.stacks``
    with the source winning on name collision (matching the convention
    in ``_merge_configs``, where project-local overrides global).

    The flag exists so an operator can manage stacks defined by a
    source repo without first cloning the repo locally — they just need
    a server URL + auth credentials.
    """
    from scripthut.config_schema import Stack

    source_name = getattr(args, "source", None)
    if not source_name:
        return config
    server = _resolve_server(args)
    if not server:
        raise RuntimeError(
            "--source requires a server URL. Pass --server <url>, set "
            "SCRIPTHUT_SERVER, or configure settings.cli_server in "
            "scripthut.yaml."
        )
    auth = _resolve_auth(args)
    base = server.rstrip("/") + "/api/v1"
    headers = auth.headers() if auth is not None else {}
    async with httpx.AsyncClient(timeout=15.0, headers=headers) as client:
        r = await client.get(f"{base}/sources/{source_name}/config")
    if r.status_code == 404:
        raise RuntimeError(
            f"Source '{source_name}' not found on the server at {server}"
        )
    if r.status_code == 422:
        try:
            detail = r.json().get("detail", r.text)
        except Exception:
            detail = r.text
        raise RuntimeError(
            f"Source '{source_name}' project config is invalid: {detail}"
        )
    if r.status_code >= 400:
        raise RuntimeError(
            f"GET /api/v1/sources/{source_name}/config returned "
            f"HTTP {r.status_code}: {r.text}"
        )
    data = r.json()
    if not data.get("config_present", False):
        # No scripthut.yaml at the source root — nothing to overlay; the
        # operator's local config.stacks are used as-is.
        return config
    raw_stacks = data.get("stacks", []) or []
    if not raw_stacks:
        return config
    try:
        overlay_stacks = [Stack.model_validate(s) for s in raw_stacks]
    except Exception as e:
        raise RuntimeError(
            f"Source '{source_name}' stacks failed schema validation: {e}"
        ) from e
    by_name = {s.name: s for s in config.stacks}
    for s in overlay_stacks:
        by_name[s.name] = s
    return config.model_copy(update={"stacks": list(by_name.values())})


async def _remote_stack_call(
    args: argparse.Namespace, server: str, method: str, path: str,
    *, params: dict[str, Any] | None = None,
    json_body: Any | None = None,
    timeout: float = 1800.0,
    use_auth: bool = True,
) -> dict[str, Any]:
    """Hit a ``/api/v1/stacks/...`` endpoint with auth + sensible error mapping.

    Long default timeout because the install path blocks on ``prep``;
    Cloudflare-Access tunnels generally hold while data is flowing
    (prep emits output). Each per-backend call is independent — a slow
    one doesn't block the others if the CLI iterates.

    ``use_auth=False`` skips auth resolution entirely — used when the
    target is the local daemon, where resolving auth could needlessly
    shell out to cloudflared.
    """
    auth = _resolve_auth(args) if use_auth else None
    base = server.rstrip("/") + "/api/v1"
    headers = auth.headers() if auth is not None else {}
    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
        r = await client.request(
            method, f"{base}{path}",
            params={k: v for k, v in (params or {}).items() if v is not None},
            json=json_body,
        )
    if r.status_code == 404:
        try:
            detail = r.json().get("detail", r.text)
        except Exception:
            detail = r.text
        raise RuntimeError(f"Not found: {detail}")
    if r.status_code == 422:
        try:
            detail = r.json().get("detail", r.text)
        except Exception:
            detail = r.text
        raise RuntimeError(f"Server rejected the request: {detail}")
    if r.status_code == 503:
        try:
            detail = r.json().get("detail", r.text)
        except Exception:
            detail = r.text
        raise RuntimeError(
            f"Server cannot service this request right now: {detail}. "
            f"Check `scripthut status` and the backend's connectivity."
        )
    if r.status_code >= 400:
        raise RuntimeError(
            f"{method} {path} returned HTTP {r.status_code}: {r.text}"
        )
    return r.json()


def _require_remote_backend(args: argparse.Namespace) -> str:
    """``stack install/check/delete`` against a server need an explicit backend.

    The CLI doesn't iterate in remote mode for v1: a server-side install
    is a synchronous, potentially-long operation, and silently fanning
    out across every connected backend would surprise the user with N
    concurrent prep jobs they didn't expect. Be explicit.
    """
    backend = getattr(args, "backend", None)
    if not backend:
        raise RuntimeError(
            "--backend is required when targeting a server. The local "
            "CLI iterates over every configured backend, but in "
            "server mode each backend is one separate install call; "
            "pick one (`scripthut backend list --json` to see what's "
            "available) and re-run."
        )
    return backend


def _render_remote_stack_result(
    name: str, data: dict[str, Any], *, as_json: bool, note: str | None = None,
) -> None:
    """Print the JSON or a tiny one-line summary for a server-side stack op."""
    if as_json:
        payload = {"stack": name, "backend": data.get("backend"), "status": data}
        if note is not None:
            payload["note"] = note
        print(json.dumps(payload, indent=2))
        return
    marker = "✓" if data.get("state") == "ready" else (
        "…" if data.get("state") == "installing" else "✗"
    )
    line = f"  {marker} {name:<20} backend={data.get('backend')}  state={data.get('state')}"
    if data.get("hash"):
        line += f"  hash={data['hash'][:10]}"
    print(line)
    if data.get("error"):
        print(f"      → {data['error']}")


async def _cmd_stack_list(args: argparse.Namespace) -> int:
    config = load_config(getattr(args, "config", None))
    config = await _overlay_source_stacks(args, config)
    if not config.stacks:
        print("No stacks configured.")
        return 0
    if args.json:
        print(json.dumps([s.model_dump(mode="json") for s in config.stacks], indent=2))
        return 0
    print(f"{'NAME':<20} {'BACKENDS':<30} INPUTS")
    for s in config.stacks:
        be = ",".join(s.backends) if s.backends else "(all SSH)"
        inputs = ",".join(f"{k}={v}" for k, v in s.inputs.items()) or "-"
        print(f"{s.name[:20]:<20} {be[:30]:<30} {inputs}")
    return 0


async def _cmd_stack_check(args: argparse.Namespace) -> int:
    server, source = _resolve_server_with_source(args)
    via_daemon = False
    if server is None and source == "none":
        server = _ensure_local_server(args)
        via_daemon = True
    if server:
        if not args.name:
            print(
                "Stack name is required when targeting a server "
                "(server-side check is single-stack per call).",
                file=sys.stderr,
            )
            return 2
        backend = _require_remote_backend(args)
        data = await _remote_stack_call(
            args, server, "GET", f"/stacks/{args.name}/check",
            params={"backend": backend, "source": getattr(args, "source", None)},
            use_auth=not via_daemon,
        )
        _render_remote_stack_result(args.name, data, as_json=args.json)
        return 0 if data.get("state") == "ready" else 1

    config = load_config(getattr(args, "config", None))
    config = await _overlay_source_stacks(args, config)
    if args.name:
        stack = config.get_stack(args.name)
        if stack is None:
            print(f"Stack '{args.name}' not found", file=sys.stderr)
            return 2
        stacks = [stack]
    else:
        stacks = list(config.stacks)
    if not stacks:
        print("No stacks configured.")
        return 0

    mgr = StackManager()

    async def do_check(stack, backend_name, ssh, scheduler):
        # check is read-only; the scheduler kind is irrelevant here.
        return await mgr.check(stack, backend_name, ssh)

    results = await _run_per_backend(
        config, stacks, args.backend, do_check,
    )
    if args.json:
        print(json.dumps([
            {
                "stack": name,
                "backend": be,
                "status": (
                    {
                        "state": s.state.value,
                        "hash": s.hash,
                        "path": s.path,
                        "last_built": s.last_built.isoformat() if s.last_built else None,
                        "size_bytes": s.size_bytes,
                        "error": s.error,
                    } if s else None
                ),
                "note": note,
            }
            for name, be, s, note in results
        ], indent=2))
        return 0
    _print_status_table(results)
    # Exit non-zero if any stack is missing/installing — useful for CI gates.
    bad = any(s is None or s.state != StackState.READY for _, _, s, _ in results)
    return 1 if bad else 0


async def _cmd_stack_install(args: argparse.Namespace) -> int:
    server, source = _resolve_server_with_source(args)
    via_daemon = False
    if server is None and source == "none":
        server = _ensure_local_server(args)
        via_daemon = True
    if server:
        backend = _require_remote_backend(args)
        data = await _remote_stack_call(
            args, server, "POST", f"/stacks/{args.name}/install",
            params={
                "backend": backend,
                "source": getattr(args, "source", None),
                "rebuild": "true" if args.rebuild else "false",
            },
            use_auth=not via_daemon,
            # Submission is now non-blocking — the synthesized install
            # workflow is queued and the run summary returns right away.
            # Keep the timeout short so a stuck network surfaces quickly.
            timeout=60.0,
        )
        run_id = data.get("id")
        if args.json:
            print(json.dumps(data, indent=2))
        else:
            print(
                f"Install submitted as run {run_id} "
                f"(workflow '{data.get('workflow_name')}' on {data.get('backend_name')})."
            )
            if not args.watch:
                print(
                    f"  scripthut run watch {run_id} --exit-status   "
                    f"# wait until done\n"
                    f"  scripthut run logs {run_id} install-{(data.get('id') or '')[:0]}  "
                    f"# live output"
                )
        if not args.watch:
            return 0
        # `--watch` mode: block until the run terminates, exit 1 on failure.
        # We reuse `_cmd_run_watch` so the polling cadence and output
        # match the standalone `scripthut run watch` command.
        import copy
        watch_args = copy.copy(args)
        watch_args.id = run_id
        watch_args.exit_status = True
        watch_args.interval = getattr(args, "interval", 2.0)
        return await _cmd_run_watch(watch_args)

    config = load_config(getattr(args, "config", None))
    config = await _overlay_source_stacks(args, config)
    stack = config.get_stack(args.name)
    if stack is None:
        print(f"Stack '{args.name}' not found", file=sys.stderr)
        return 2

    mgr = StackManager()

    async def do_install(stack, backend_name, ssh, scheduler):
        return await mgr.install(
            stack, backend_name, ssh,
            rebuild=args.rebuild, scheduler=scheduler,
        )

    results = await _run_per_backend(config, [stack], args.backend, do_install)
    _print_status_table(results)
    failed = any(
        s is None or s.state != StackState.READY
        for _, _, s, _ in results
    )
    return 1 if failed else 0


async def _cmd_stack_delete(args: argparse.Namespace) -> int:
    server, source = _resolve_server_with_source(args)
    via_daemon = False
    if server is None and source == "none":
        server = _ensure_local_server(args)
        via_daemon = True
    if server:
        backend = _require_remote_backend(args)
        data = await _remote_stack_call(
            args, server, "DELETE", f"/stacks/{args.name}",
            params={"backend": backend, "source": getattr(args, "source", None)},
            use_auth=not via_daemon,
        )
        if data.get("deleted"):
            print(f"  ✓ {args.name} deleted on backend {data.get('backend')}")
            return 0
        print(f"  ✗ {args.name} delete failed: {data}", file=sys.stderr)
        return 1

    config = load_config(getattr(args, "config", None))
    config = await _overlay_source_stacks(args, config)
    stack = config.get_stack(args.name)
    if stack is None:
        print(f"Stack '{args.name}' not found", file=sys.stderr)
        return 2

    mgr = StackManager()

    async def do_delete(stack, backend_name, ssh, scheduler):
        # delete is rm -rf over SSH; scheduler kind doesn't matter.
        await mgr.delete(stack, backend_name, ssh)
        return await mgr.check(stack, backend_name, ssh)

    results = await _run_per_backend(config, [stack], args.backend, do_delete)
    _print_status_table(results)
    failed = any(note is not None for _, _, _, note in results)
    return 1 if failed else 0


# ---------------------------------------------------------------------------
# `disk` subcommands — remote disk usage (Phase 1: report only)
# ---------------------------------------------------------------------------


def _print_disk_result(backend: str, result: dict[str, Any]) -> None:
    """Render one backend's scan result (API dict shape) as a table."""
    from datetime import datetime

    entries = result.get("entries", [])
    if entries:
        print(f"{'BACKEND':<14} {'KIND':<7} {'NAME':<36} {'CLASS':<11} "
              f"{'SIZE':<8} {'AGE':<10} RUNS")
        for e in entries:
            name = e.get("detail") or e["path"].rsplit("/", 1)[-1]
            mtime = None
            if e.get("mtime"):
                try:
                    mtime = datetime.fromisoformat(e["mtime"])
                except ValueError:
                    pass
            run_ids = e.get("run_ids") or []
            runs = ",".join(run_ids[:2]) + (
                f" +{len(run_ids) - 2}" if len(run_ids) > 2 else ""
            ) or "-"
            print(
                f"{backend[:14]:<14} {e['kind']:<7} {name[:36]:<36} "
                f"{e['classification']:<11} {_format_size(e['size_bytes']):<8} "
                f"{_format_age(mtime):<10} {runs}"
            )
    else:
        print(f"{backend}: no scripthut directories found")

    parts = [f"{len(entries)} entries, {_format_size(result.get('total_bytes'))} total"]
    by_class = result.get("totals_by_class") or {}
    for cls in ("orphaned", "active"):
        if cls in by_class:
            count, size = by_class[cls]
            parts.append(f"{cls} {_format_size(size)} in {count}")
    if result.get("disk_avail_bytes") is not None:
        parts.append(f"disk {_format_size(result['disk_avail_bytes'])} free")
    scanned = result.get("scanned_at", "")[:19].replace("T", " ")
    print(f"{backend}: {' · '.join(parts)} (scanned {scanned})")
    for err in result.get("errors", []):
        print(f"  ! {err}", file=sys.stderr)


def _render_disk_response(data: dict[str, Any], *, as_json: bool) -> int:
    """Render a GET /api/v1/disk response; 0 unless nothing was ever scanned."""
    if as_json:
        print(json.dumps(data, indent=2))
        return 0
    backends = data.get("backends", {})
    if not backends:
        print("No SSH backends on the server.")
        return 0
    for i, name in enumerate(sorted(backends)):
        if i:
            print()
        info = backends[name]
        if info.get("scanning"):
            print(f"{name}: scan in progress — re-run to see the result")
        elif info.get("result"):
            _print_disk_result(name, info["result"])
        else:
            print(f"{name}: never scanned — run `scripthut disk scan`")
    return 0


async def _disk_scan_local(args: argparse.Namespace) -> int:
    """Scan over direct SSH without a server (mirrors stack local mode)."""
    from scripthut.disk.scan import build_scan_spec
    from scripthut.disk.service import (
        DiskScanService,
        compute_current_stack_hashes,
        gather_project_stacks,
    )
    from scripthut.runs.storage import RunStorageManager

    config = load_config(getattr(args, "config", None))
    ssh_types = (SlurmBackendConfig, PBSBackendConfig)
    backends = [b for b in config.backends if isinstance(b, ssh_types)]
    backend_filter = getattr(args, "backend", None)
    if backend_filter:
        backends = [b for b in backends if b.name == backend_filter]
        if not backends:
            print(
                f"Backend '{backend_filter}' not found or not SSH-based",
                file=sys.stderr,
            )
            return 2
    if not backends:
        print("No SSH backends configured.")
        return 0

    storage = RunStorageManager(config.settings.data_dir_resolved / "workflows")
    runs = list(storage.load_all_runs().values())
    svc = DiskScanService()

    results: dict[str, Any] = {}
    failures = 0
    for b in backends:
        ssh = _ssh_client_for(b)
        try:
            await ssh.connect()
        except Exception as e:
            print(f"{b.name}: connect failed: {e}", file=sys.stderr)
            failures += 1
            continue
        try:
            # Project-declared stacks (per-source scripthut.yaml env dirs);
            # git sources only resolve where the server's sources cache
            # exists locally, path sources over this backend's SSH.
            stacks, gather_errors = await gather_project_stacks(
                config, b.name, ssh=ssh,
            )
            spec = build_scan_spec(config, b.name, b.clone_dir, extra_stacks=stacks)
            result = await svc.scan_backend(
                spec=spec, ssh=ssh, runs=runs,
                current_stack_hashes=compute_current_stack_hashes(config, stacks),
                extra_errors=gather_errors,
            )
            results[b.name] = result
        finally:
            await ssh.disconnect()

    if args.json:
        print(json.dumps(
            {name: r.to_dict() for name, r in results.items()}, indent=2,
        ))
    else:
        for i, name in enumerate(sorted(results)):
            if i:
                print()
            _print_disk_result(name, results[name].to_dict())
    return 1 if failures else 0


async def _cmd_disk_status(args: argparse.Namespace) -> int:
    server, source = _resolve_server_with_source(args)
    via_daemon = False
    if server is None and source == "none":
        server = _ensure_local_server(args)
        via_daemon = True
    if server:
        data = await _remote_stack_call(
            args, server, "GET", "/disk",
            params={"backend": getattr(args, "backend", None)},
            use_auth=not via_daemon, timeout=60.0,
        )
        return _render_disk_response(data, as_json=args.json)
    # --server local: there is no cache without a server, so just scan.
    return await _disk_scan_local(args)


async def _wait_disk_idle(
    args: argparse.Namespace,
    server: str,
    targets: list[str],
    *,
    backend_filter: str | None,
    via_daemon: bool,
    what: str = "scans",
) -> dict[str, Any] | None:
    """Poll GET /disk until no target is scanning or cleaning.

    Returns the final response, or None on timeout (15 min — comfortably
    above the server's own per-operation timeouts, so a hang here means
    something is truly wedged).
    """
    import time

    deadline = time.monotonic() + 900
    while True:
        await asyncio.sleep(getattr(args, "interval", 2.0))
        data = await _remote_stack_call(
            args, server, "GET", "/disk", params={"backend": backend_filter},
            use_auth=not via_daemon, timeout=60.0,
        )
        pending = [
            b for b in targets
            if data.get("backends", {}).get(b, {}).get("scanning")
            or data.get("backends", {}).get(b, {}).get("cleaning")
        ]
        if not pending:
            return data
        if time.monotonic() > deadline:
            print(f"Timed out waiting for {what}: {', '.join(pending)}",
                  file=sys.stderr)
            return None


async def _cmd_disk_scan(args: argparse.Namespace) -> int:
    server, source = _resolve_server_with_source(args)
    via_daemon = False
    if server is None and source == "none":
        server = _ensure_local_server(args)
        via_daemon = True
    if server is None:
        return await _disk_scan_local(args)

    backend_filter = getattr(args, "backend", None)
    data = await _remote_stack_call(
        args, server, "GET", "/disk", params={"backend": backend_filter},
        use_auth=not via_daemon, timeout=60.0,
    )
    targets = sorted(data.get("backends", {}))
    if not targets:
        print("No SSH backends on the server.")
        return 0
    for b in targets:
        resp = await _remote_stack_call(
            args, server, "POST", "/disk/scan", params={"backend": b},
            use_auth=not via_daemon, timeout=60.0,
        )
        if resp.get("status") == "already_running":
            print(f"{b}: a scan is already running; waiting for it",
                  file=sys.stderr)

    data = await _wait_disk_idle(
        args, server, targets, backend_filter=backend_filter,
        via_daemon=via_daemon,
    )
    if data is None:
        return 1
    return _render_disk_response(data, as_json=args.json)


def _print_cleanup_plan(backend: str, plan: dict[str, Any]) -> None:
    """Render a cleanup plan dict (API shape) as a table."""
    entries = plan.get("entries", [])
    if not entries:
        print(f"{backend}: nothing to clean")
        return
    print(f"{'KIND':<7} {'NAME':<36} {'CLASS':<11} {'SIZE':<8} {'ACTION':<18} NOTE")
    for pe in entries:
        e = pe["entry"]
        name = e.get("detail") or e["path"].rsplit("/", 1)[-1]
        note = pe.get("reason") or "; ".join(pe.get("warnings") or [])
        print(
            f"{e['kind']:<7} {name[:36]:<36} {e['classification']:<11} "
            f"{_format_size(e['size_bytes']):<8} {pe['action']:<18} {note}"
        )
    counts = plan.get("counts", {})
    n = counts.get("delete", 0) + counts.get("check", 0)
    print(
        f"{backend}: would delete {n} entries "
        f"(~{_format_size(plan.get('delete_bytes'))}); "
        f"{counts.get('skip', 0)} skipped"
    )


def _print_cleanup_report(backend: str, report: dict[str, Any]) -> None:
    """Render a cleanup report dict (API shape)."""
    marks = {"deleted": "✓", "skipped": "-", "failed": "✗"}
    for o in report.get("outcomes", []):
        line = f"  {marks.get(o['outcome'], '?')} {o['outcome']:<8} {o['path']}"
        if o.get("reason"):
            line += f" ({o['reason']})"
        print(line)
    for err in report.get("errors", []):
        print(f"  ! {err}", file=sys.stderr)
    c = report.get("counts", {})
    prefix = "at least " if report.get("freed_is_lower_bound") else ""
    print(
        f"{backend}: deleted {c.get('deleted', 0)} · "
        f"freed {prefix}{_format_size(report.get('freed_bytes'))} · "
        f"skipped {c.get('skipped', 0)} · failed {c.get('failed', 0)}"
    )


async def _disk_clean_local(args: argparse.Namespace) -> int:
    """Guardrailed cleanup over direct SSH without a server."""
    from datetime import datetime, timezone

    from scripthut.disk.classify import build_run_references
    from scripthut.disk.cleanup import plan_cleanup
    from scripthut.disk.scan import build_scan_spec
    from scripthut.disk.service import (
        DiskScanService,
        compute_current_stack_hashes,
        execute_cleanup,
        gather_project_stacks,
    )
    from scripthut.runs.storage import RunStorageManager

    config = load_config(getattr(args, "config", None))
    ssh_types = (SlurmBackendConfig, PBSBackendConfig)
    backends = [b for b in config.backends if isinstance(b, ssh_types)]
    backend_filter = getattr(args, "backend", None)
    if backend_filter:
        backends = [b for b in backends if b.name == backend_filter]
        if not backends:
            print(
                f"Backend '{backend_filter}' not found or not SSH-based",
                file=sys.stderr,
            )
            return 2
    if not backends:
        print("No SSH backends configured.")
        return 0
    print(
        "note: local mode sees only persisted runs — prefer server mode "
        "when a scripthut server is running",
        file=sys.stderr,
    )

    storage = RunStorageManager(config.settings.data_dir_resolved / "workflows")
    runs = list(storage.load_all_runs().values())
    svc = DiskScanService()
    paths = getattr(args, "paths", None)

    rc = 0
    json_out: dict[str, Any] = {}
    for b in backends:
        ssh = _ssh_client_for(b)
        try:
            await ssh.connect()
        except Exception as e:
            print(f"{b.name}: connect failed: {e}", file=sys.stderr)
            rc = 1
            continue
        try:
            stacks, gather_errors = await gather_project_stacks(
                config, b.name, ssh=ssh,
            )
            for err in gather_errors:
                print(f"{b.name}: {err}", file=sys.stderr)
            hashes = compute_current_stack_hashes(config, stacks)
            spec = build_scan_spec(config, b.name, b.clone_dir, extra_stacks=stacks)
            result = await svc.scan_backend(
                spec=spec, ssh=ssh, runs=runs, current_stack_hashes=hashes,
                extra_errors=gather_errors,
            )
            refs = build_run_references(
                runs, b.name, spec.clone_dirs, result.home_dir,
            )
            plan = plan_cleanup(
                result, refs, spec=spec, current_stack_hashes=hashes,
                planned_at=datetime.now(timezone.utc),
                paths=paths,
                allow_referenced=frozenset(paths or []),
            )
            if plan.errors:
                for err in plan.errors:
                    print(f"{b.name}: {err}", file=sys.stderr)
                rc = 2
                continue
            if not args.json:
                _print_cleanup_plan(b.name, plan.to_dict())
            json_out[b.name] = {"plan": plan.to_dict()}
            if args.dry_run or not plan.to_delete:
                continue
            if not args.yes and not _confirm_stderr(
                f"Delete {len(plan.to_delete)} entries "
                f"(~{_format_size(plan.delete_bytes)}) on {b.name}?",
                default_yes=False,
            ):
                print(f"{b.name}: skipped (not confirmed)", file=sys.stderr)
                continue
            report = await execute_cleanup(plan, ssh)
            fresh = await svc.scan_backend(
                spec=spec, ssh=ssh, runs=runs, current_stack_hashes=hashes,
            )
            if not args.json:
                _print_cleanup_report(b.name, report.to_dict())
                print()
                _print_disk_result(b.name, fresh.to_dict())
            json_out[b.name].update(
                report=report.to_dict(), result=fresh.to_dict(),
            )
            if report.counts.get("failed"):
                rc = 1
        finally:
            await ssh.disconnect()
    if args.json:
        print(json.dumps(json_out, indent=2))
    return rc


async def _cmd_disk_clean(args: argparse.Namespace) -> int:
    if getattr(args, "paths", None) and not getattr(args, "backend", None):
        print("--path requires --backend", file=sys.stderr)
        return 2
    server, source = _resolve_server_with_source(args)
    via_daemon = False
    if server is None and source == "none":
        server = _ensure_local_server(args)
        via_daemon = True
    if server is None:
        return await _disk_clean_local(args)

    backend_filter = getattr(args, "backend", None)
    data = await _remote_stack_call(
        args, server, "GET", "/disk", params={"backend": backend_filter},
        use_auth=not via_daemon, timeout=60.0,
    )
    targets = sorted(data.get("backends", {}))
    if not targets:
        print("No SSH backends on the server.")
        return 0

    # Always plan from a fresh scan so the confirmation reflects reality.
    for b in targets:
        await _remote_stack_call(
            args, server, "POST", "/disk/scan", params={"backend": b},
            use_auth=not via_daemon, timeout=60.0,
        )
    if await _wait_disk_idle(
        args, server, targets, backend_filter=backend_filter,
        via_daemon=via_daemon,
    ) is None:
        return 1

    rc = 0
    json_out: dict[str, Any] = {}
    for i, b in enumerate(targets):
        if i and not args.json:
            print()
        plan_resp = await _remote_stack_call(
            args, server, "POST", "/disk/clean",
            json_body={
                "backend": b,
                "paths": getattr(args, "paths", None),
                "allow_referenced": getattr(args, "paths", None) or [],
                "dry_run": True,
            },
            use_auth=not via_daemon, timeout=60.0,
        )
        plan = plan_resp.get("plan", {})
        if not args.json:
            _print_cleanup_plan(b, plan)
        json_out[b] = {"plan": plan}
        # The exec request sends exactly the paths shown to the user, so
        # the confirmation is pinned; server-side re-classification can
        # only shrink that set.
        to_delete = [
            pe["entry"]["path"]
            for pe in plan.get("entries", [])
            if pe.get("action") != "skip"
        ]
        if args.dry_run or not to_delete:
            continue
        if not args.yes and not _confirm_stderr(
            f"Delete {len(to_delete)} entries "
            f"(~{_format_size(plan.get('delete_bytes'))}) on {b}?",
            default_yes=False,
        ):
            print(f"{b}: skipped (not confirmed)", file=sys.stderr)
            continue
        exec_resp = await _remote_stack_call(
            args, server, "POST", "/disk/clean",
            json_body={
                "backend": b,
                "paths": to_delete,
                "allow_referenced": getattr(args, "paths", None) or [],
                "dry_run": False,
            },
            use_auth=not via_daemon, timeout=60.0,
        )
        if exec_resp.get("status") != "started":
            print(f"{b}: {exec_resp.get('status')}", file=sys.stderr)
            continue
        final = await _wait_disk_idle(
            args, server, [b], backend_filter=backend_filter,
            via_daemon=via_daemon, what="cleanup",
        )
        if final is None:
            rc = 1
            continue
        info = final.get("backends", {}).get(b, {})
        report = info.get("last_cleanup") or {}
        if not args.json:
            _print_cleanup_report(b, report)
            if info.get("result"):
                print()
                _print_disk_result(b, info["result"])
        json_out[b].update(report=report, result=info.get("result"))
        if report.get("counts", {}).get("failed"):
            rc = 1
    if args.json:
        print(json.dumps(json_out, indent=2))
    return rc


# ---------------------------------------------------------------------------
# `daemon` subcommands — manage the local background scripthut server
# ---------------------------------------------------------------------------


def _load_config_or_none(args: argparse.Namespace) -> ScriptHutConfig | None:
    """Config if loadable, else None — stop/status/logs must work without one."""
    try:
        return load_config(getattr(args, "config", None))
    except (FileNotFoundError, ConfigError):
        return None


async def _cmd_daemon_start(args: argparse.Namespace) -> int:
    from scripthut import daemon

    # A daemon needs a config (backends to talk to) — propagate the
    # friendly FileNotFoundError when there is none.
    config = load_config(getattr(args, "config", None))
    host, port = daemon.resolve_host_port(config)
    already = daemon.ping(host, port) is not None
    info = daemon.start_daemon(config, getattr(args, "config", None))
    if already:
        print(f"Already running (pid {info.pid}) at {info.url}")
    else:
        print(f"Started local daemon (pid {info.pid}) at {info.url}")
        print(f"  logs: {daemon.logfile_path(config)}")
    return 0


async def _cmd_daemon_stop(args: argparse.Namespace) -> int:
    from scripthut import daemon

    print(daemon.stop_daemon(_load_config_or_none(args)))
    return 0


async def _cmd_daemon_status(args: argparse.Namespace) -> int:
    from scripthut import daemon

    status = daemon.daemon_status(_load_config_or_none(args))
    if args.json:
        print(json.dumps(status, indent=2))
        return 0 if status["running"] else 3
    if status["running"]:
        line = f"● running (pid {status['pid']}) at {status['url']}"
        if status.get("version"):
            line += f"  v{status['version']}"
        print(line)
        if status.get("uptime_s") is not None:
            print(f"  up {int(status['uptime_s'] // 60)} min")
    elif status["foreign"]:
        print(
            "✗ the configured port is serving something that isn't "
            "scripthut — change settings.server_port or stop that process"
        )
    else:
        print("○ not running")
    if status.get("stale_pidfile"):
        print(f"  ! stale pidfile: {status['pidfile']}")
    print(f"  log: {status['log_path']}")
    return 0 if status["running"] else 3


async def _cmd_daemon_logs(args: argparse.Namespace) -> int:
    from scripthut import daemon

    path = daemon.logfile_path(_load_config_or_none(args))
    if not path.exists():
        print(f"No daemon log at {path}", file=sys.stderr)
        return 2
    print(f"# {path}", file=sys.stderr)
    for line in path.read_text(errors="replace").splitlines()[-args.tail:]:
        print(line)
    return 0


# ---------------------------------------------------------------------------
# `project` subcommands
# ---------------------------------------------------------------------------


async def _cmd_source_list(args: argparse.Namespace) -> int:
    async with _make_client(args) as client:
        data = await client.list_sources()
    if args.json:
        print(json.dumps(data, indent=2))
        return 0
    sources = data.get("sources", [])
    if not sources:
        print("No sources configured.")
        return 0
    print("Sources:")
    for s in sources:
        desc = f" — {s['description']}" if s.get("description") else ""
        # Git sources show url@branch; path sources show backend:path.
        if s.get("type") == "git":
            loc = f"{s.get('url', '?')}@{s.get('branch', 'main')}"
        else:
            loc = f"[{s.get('backend', '?')}] {s.get('path', '?')}"
        print(f"  {s['name']:<20} ({s.get('type', '?')})  {loc}{desc}")
    return 0


async def _cmd_source_sync(args: argparse.Namespace) -> int:
    """Re-clone (git) or re-glob (path) one or all sources, refresh cache.

    No name argument syncs every configured source; per-source errors
    are reported but don't fail the overall command (exit code 0). When
    a name is given and *that* source errors, exit code 2 so it composes
    in scripts.
    """
    async with _make_client(args) as client:
        if args.name:
            data = await client.sync_source(args.name)
            entries = [data]
        else:
            data = await client.sync_all_sources()
            entries = data.get("sources", [])

    if args.json:
        print(json.dumps(data, indent=2))
    elif not entries:
        print("No sources configured.")
    else:
        any_failed = False
        for r in entries:
            marker = "✗" if r.get("error") else "✓"
            wfs = r.get("workflows", []) or []
            wf_str = f"{len(wfs)} workflow{'s' if len(wfs) != 1 else ''}"
            line = f"  {marker} {r['name']:<20} ({r.get('type', '?')})  {wf_str}"
            if r.get("type") == "git" and r.get("last_commit"):
                line += f"  @ {r['last_commit'][:8]}"
            print(line)
            if r.get("error"):
                any_failed = True
                print(f"      → {r['error']}")
        if args.name and any_failed:
            return 2
    return 0


async def _cmd_source_view(args: argparse.Namespace) -> int:
    async with _make_client(args) as client:
        data = await client.view_source(args.name)
    if args.json:
        print(json.dumps(data, indent=2))
        return 0
    print(f"Source '{data['name']}' (type: {data.get('type', '?')})")
    if data.get("type") == "git":
        print(f"  url:    {data.get('url', '?')}")
        print(f"  branch: {data.get('branch', '?')}")
    else:
        print(f"  backend: {data.get('backend', '?')}")
        print(f"  path:    {data.get('path', '?')}")
    if data.get("description"):
        print(f"  description: {data['description']}")
    if data.get("max_concurrent") is not None:
        print(f"  max_concurrent: {data['max_concurrent']}")
    err = data.get("discover_error")
    workflows = data.get("workflows", [])
    if err:
        print(f"  workflows: <{err}>")
    elif workflows:
        print(f"  workflows ({len(workflows)}):")
        for w in workflows:
            print(f"    {w}")
    else:
        print("  workflows: none discovered")
    return 0


async def _cmd_workflow_run(args: argparse.Namespace) -> int:
    async with _make_client(args) as client:
        summary = await client.run_source_workflow(
            args.source, args.name, backend=args.backend,
            branch=getattr(args, "branch", None),
        )
    # base_url covers the daemon path too, where _resolve_server is None
    # but a server is in fact processing the run.
    _print_run_submitted(summary, remote_base=getattr(client, "base_url", None))
    return 0


# ---------------------------------------------------------------------------
# `backend` subcommands
# ---------------------------------------------------------------------------


async def _cmd_backend_list(args: argparse.Namespace) -> int:
    async with _make_client(args) as client:
        data = await client.list_backends()
    if args.json:
        print(json.dumps(data, indent=2))
        return 0
    backends = data.get("backends", [])
    if not backends:
        print("No backends configured.")
        return 0
    print(f"{'NAME':<20} {'TYPE':<8} {'STATUS':<10} MAX_CONCURRENT")
    for b in backends:
        status = "connected" if b.get("connected") else "down"
        max_c = b.get("max_concurrent")
        max_str = str(max_c) if max_c is not None else "-"
        print(f"{b['name'][:20]:<20} {b['type']:<8} {status:<10} {max_str}")
    return 0


# ---------------------------------------------------------------------------
# `run` subcommands
# ---------------------------------------------------------------------------


async def _cmd_run_list(args: argparse.Namespace) -> int:
    async with _make_client(args) as client:
        data = await client.list_runs(limit=args.limit)
    if args.json:
        print(json.dumps(data, indent=2))
        return 0
    print(_format_run_table(data.get("runs", [])))
    return 0


async def _cmd_run_view(args: argparse.Namespace) -> int:
    async with _make_client(args) as client:
        data = await client.view_run(args.id)
    if args.json:
        print(json.dumps(data, indent=2))
        return 0
    print(_format_run_view(data))
    return 0


def _clear_lines(n: int) -> None:
    """Move cursor up n lines and erase to end of screen."""
    if n > 0:
        sys.stdout.write(f"\033[{n}A\033[J")
        sys.stdout.flush()


async def _cmd_run_watch(args: argparse.Namespace) -> int:
    """Poll a run's status until it reaches a terminal state."""
    last_lines = 0
    final_status: str | None = None
    async with _make_client(args) as client:
        while True:
            try:
                data = await client.view_run(args.id)
            except RuntimeError as e:
                _clear_lines(last_lines)
                print(f"Error: {e}", file=sys.stderr)
                return 1
            block = _format_run_view(data)
            _clear_lines(last_lines)
            sys.stdout.write(block + "\n")
            sys.stdout.flush()
            last_lines = block.count("\n") + 1
            final_status = data.get("status")
            if final_status in TERMINAL_RUN_STATES:
                break
            await asyncio.sleep(args.interval)
    if args.exit_status and final_status != RunStatus.COMPLETED.value:
        return 1
    return 0


async def _cmd_run_cancel(args: argparse.Namespace) -> int:
    async with _make_client(args) as client:
        await client.cancel_run(args.id)
    print(f"Cancelled run {args.id}")
    return 0


async def _cmd_run_rerun(args: argparse.Namespace) -> int:
    async with _make_client(args) as client:
        summary = await client.rerun(args.id)
    _print_run_submitted(summary, remote_base=getattr(client, "base_url", None))
    return 0


async def _cmd_run_logs(args: argparse.Namespace) -> int:
    """Print or tail logs for a single task in a run."""
    log_type = "error" if args.error else "output"

    async with _make_client(args) as client:
        if not args.follow:
            data = await client.fetch_logs(
                args.id, args.task, log_type=log_type, tail=args.tail,
            )
            sys.stdout.write(data["content"])
            if data["content"] and not data["content"].endswith("\n"):
                sys.stdout.write("\n")
            return 0

        # --follow: poll, print only the delta, exit when the task is terminal.
        seen_len = 0
        terminal_item_states = {
            RunItemStatus.COMPLETED.value,
            RunItemStatus.FAILED.value,
            RunItemStatus.DEP_FAILED.value,
        }
        while True:
            try:
                data = await client.fetch_logs(
                    args.id, args.task, log_type=log_type,
                )
            except RuntimeError as e:
                # Task may not have started yet — wait and retry rather than fail
                if "not been submitted" in str(e):
                    await asyncio.sleep(args.interval)
                    continue
                raise
            content = data.get("content", "")
            if len(content) > seen_len:
                sys.stdout.write(content[seen_len:])
                sys.stdout.flush()
                seen_len = len(content)

            run = await client.view_run(args.id)
            item = next(
                (it for it in run.get("items", []) if it["task"]["id"] == args.task),
                None,
            )
            if item is None:
                break
            if item["status"] in terminal_item_states:
                break
            await asyncio.sleep(args.interval)
    return 0


# ---------------------------------------------------------------------------
# Argparse wiring
# ---------------------------------------------------------------------------


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--server",
        help=(
            "URL of a running scripthut server. "
            "Overrides SCRIPTHUT_SERVER and settings.cli_server. "
            "Pass 'local' to force local mode."
        ),
    )
    parser.add_argument(
        "--config", "-c", type=Path,
        help="Path to scripthut.yaml (used for local mode and cli_server lookup)",
    )
    # Cloudflare Access auth — only used when talking to a remote server.
    # Each flag has a matching SCRIPTHUT_CF_* env var and a settings.cli_auth
    # field in scripthut.yaml; see _resolve_auth.
    parser.add_argument(
        "--cf-client-id", dest="cf_client_id",
        help=(
            "Cloudflare Access service-token client ID. "
            "Env: SCRIPTHUT_CF_CLIENT_ID. Pair with --cf-client-secret."
        ),
    )
    parser.add_argument(
        "--cf-client-secret", dest="cf_client_secret",
        help=(
            "Cloudflare Access service-token client secret. "
            "Env: SCRIPTHUT_CF_CLIENT_SECRET."
        ),
    )
    parser.add_argument(
        "--cf-access-token", dest="cf_access_token",
        help=(
            "Cloudflare Access user JWT (e.g. `cloudflared access token "
            "--app=<url>`). Env: SCRIPTHUT_CF_ACCESS_TOKEN."
        ),
    )
    parser.add_argument(
        "--cloudflared-app", dest="cloudflared_app",
        help=(
            "App URL passed to `cloudflared access token` to fetch a fresh "
            "JWT at invocation time. Ignored if a token is supplied "
            "explicitly. Env: SCRIPTHUT_CLOUDFLARED_APP."
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the gh-style ``scripthut`` parser (workflow/run noun groups)."""
    parser = argparse.ArgumentParser(
        prog="scripthut",
        description="ScriptHut CLI — manage workflows and runs",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # ----- workflow ---------------------------------------------------------
    p_wf = sub.add_parser("workflow", help="Run source workflows")
    wf_sub = p_wf.add_subparsers(dest="wf_cmd", required=True)

    p_wf_run = wf_sub.add_parser(
        "run",
        help="Submit a workflow file from a configured source for execution",
    )
    p_wf_run.add_argument("name", help="Workflow filename within the source")
    p_wf_run.add_argument(
        "--source", required=True, help="Name of the source the workflow lives in",
    )
    p_wf_run.add_argument(
        "--backend",
        help="Override the source's default backend",
    )
    p_wf_run.add_argument(
        "--branch",
        help=(
            "Run from this git branch instead of the source's configured "
            "one (git sources only)"
        ),
    )
    _add_common(p_wf_run)
    p_wf_run.set_defaults(handler=_cmd_workflow_run)

    # ----- run --------------------------------------------------------------
    p_run = sub.add_parser("run", help="Inspect and control runs")
    run_sub = p_run.add_subparsers(dest="run_cmd", required=True)

    p_run_list = run_sub.add_parser("list", help="List recent runs")
    p_run_list.add_argument("--limit", type=int, default=20)
    p_run_list.add_argument("--json", action="store_true")
    _add_common(p_run_list)
    p_run_list.set_defaults(handler=_cmd_run_list)

    p_run_view = run_sub.add_parser("view", help="Show details for a single run")
    p_run_view.add_argument("id")
    p_run_view.add_argument("--json", action="store_true")
    _add_common(p_run_view)
    p_run_view.set_defaults(handler=_cmd_run_view)

    p_run_watch = run_sub.add_parser(
        "watch", help="Watch a run until it completes (polls server)",
    )
    p_run_watch.add_argument("id")
    p_run_watch.add_argument(
        "--interval", type=float, default=5.0,
        help="Seconds between polls (default: 5)",
    )
    p_run_watch.add_argument(
        "--exit-status", action="store_true",
        help="Exit non-zero if the run finishes with a non-success status",
    )
    _add_common(p_run_watch)
    p_run_watch.set_defaults(handler=_cmd_run_watch)

    p_run_cancel = run_sub.add_parser("cancel", help="Cancel a running run")
    p_run_cancel.add_argument("id")
    _add_common(p_run_cancel)
    p_run_cancel.set_defaults(handler=_cmd_run_cancel)

    p_run_rerun = run_sub.add_parser(
        "rerun", help="Reset and re-submit a previous run in place",
    )
    p_run_rerun.add_argument("id")
    _add_common(p_run_rerun)
    p_run_rerun.set_defaults(handler=_cmd_run_rerun)

    p_run_logs = run_sub.add_parser("logs", help="Show stdout/stderr for a task in a run")
    p_run_logs.add_argument("id")
    p_run_logs.add_argument("task")
    p_run_logs.add_argument("--error", action="store_true", help="Show stderr instead of stdout")
    p_run_logs.add_argument("--tail", type=int, help="Show only the last N lines")
    p_run_logs.add_argument(
        "--follow", "-f", action="store_true", help="Tail the log until task ends",
    )
    p_run_logs.add_argument(
        "--interval", type=float, default=2.0, help="Polling interval for --follow",
    )
    _add_common(p_run_logs)
    p_run_logs.set_defaults(handler=_cmd_run_logs)

    # ----- backend ----------------------------------------------------------
    p_be = sub.add_parser("backend", help="Inspect configured backends")
    be_sub = p_be.add_subparsers(dest="be_cmd", required=True)

    p_be_list = be_sub.add_parser("list", help="List configured backends")
    p_be_list.add_argument("--json", action="store_true")
    _add_common(p_be_list)
    p_be_list.set_defaults(handler=_cmd_backend_list)

    # ----- agent ------------------------------------------------------------
    p_ag = sub.add_parser(
        "agent",
        help="Helpers for coding agents (prompt generation, etc.)",
    )
    ag_sub = p_ag.add_subparsers(dest="agent_cmd", required=True)

    p_ag_prompt = ag_sub.add_parser(
        "prompt",
        help="Print a markdown briefing teaching an agent how to use this scripthut",
    )
    _add_common(p_ag_prompt)
    p_ag_prompt.set_defaults(handler=_cmd_agent_prompt)

    p_ag_install = ag_sub.add_parser(
        "install",
        help=(
            "Install a Claude Code skill and /scripthut:debug command "
            "into ./.claude (or ~/.claude with --user)"
        ),
    )
    p_ag_install.add_argument(
        "--user",
        action="store_true",
        help="Install into ~/.claude (all projects) instead of ./.claude",
    )
    p_ag_install.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing files even if scripthut didn't write them",
    )
    p_ag_install.set_defaults(handler=_cmd_agent_install)

    # ----- task -------------------------------------------------------------
    p_tk = sub.add_parser(
        "task",
        help="Submit one-off ad-hoc tasks (no workflow / git repo needed)",
    )
    tk_sub = p_tk.add_subparsers(dest="task_cmd", required=True)

    p_tk_run = tk_sub.add_parser(
        "run",
        help="Submit a single task described inline (great for coding agents)",
    )
    p_tk_run.add_argument(
        "command", nargs="?",
        help="Bash command to run. Omit when using --from-stdin or --from-file.",
    )
    p_tk_run.add_argument(
        "--backend", required=True,
        help="Backend to submit to (must be configured)",
    )
    p_tk_run.add_argument(
        "--name", default=None,
        help="Human-readable label (default: derived from --id)",
    )
    p_tk_run.add_argument(
        "--id", dest="id", default=None,
        help="Task id (default: 'adhoc-<8-char-hash>')",
    )
    p_tk_run.add_argument(
        "--run-name", default=None,
        help="Override the synthetic '_adhoc/<id>' workflow label",
    )
    p_tk_run.add_argument("--cpus", type=int, default=None)
    p_tk_run.add_argument("--memory", default=None, help="e.g. '4G'")
    p_tk_run.add_argument(
        "--time", dest="time_limit", default=None,
        help="Wall-clock limit, e.g. '1:00:00'",
    )
    p_tk_run.add_argument("--partition", default=None)
    p_tk_run.add_argument(
        "--gres", default=None,
        help="Slurm-style generic resources, e.g. 'gpu:1'",
    )
    p_tk_run.add_argument("--working-dir", dest="working_dir", default=None)
    p_tk_run.add_argument(
        "--image", default=None,
        help="Container image (AWS Batch / EC2 backends)",
    )
    p_tk_run.add_argument(
        "--env", action="append", default=[],
        help="KEY=VALUE env var (repeatable)",
    )
    p_tk_run.add_argument(
        "--from-stdin", action="store_true",
        help="Read a TaskDefinition JSON body from stdin (other flags override)",
    )
    p_tk_run.add_argument(
        "--from-file", default=None,
        help="Read a TaskDefinition JSON body from this file (other flags override)",
    )
    p_tk_run.add_argument(
        "--inline-script", dest="inline_script", default=None,
        help=(
            "Local script file to run on the backend. Contents are "
            "base64-embedded into the task command so the backend can "
            "execute it without any prior file staging. Mutually "
            "exclusive with positional command / --from-stdin / --from-file."
        ),
    )
    p_tk_run.add_argument(
        "--dry-run", action="store_true",
        help="Print the assembled task + backend as JSON without submitting",
    )
    p_tk_run.add_argument("--json", action="store_true")
    _add_common(p_tk_run)
    p_tk_run.set_defaults(handler=_cmd_task_run)

    # ----- stack ------------------------------------------------------------
    p_st = sub.add_parser("stack", help="Manage reusable software stacks")
    st_sub = p_st.add_subparsers(dest="stack_cmd", required=True)

    _SOURCE_OVERLAY_HELP = (
        "Overlay stacks from a source repo's scripthut.yaml (fetched from "
        "the configured server). Source wins on name collision; requires "
        "--server / SCRIPTHUT_SERVER."
    )

    p_st_list = st_sub.add_parser("list", help="List configured stacks")
    p_st_list.add_argument("--json", action="store_true")
    p_st_list.add_argument("--source", help=_SOURCE_OVERLAY_HELP)
    _add_common(p_st_list)
    p_st_list.set_defaults(handler=_cmd_stack_list)

    p_st_check = st_sub.add_parser(
        "check",
        help="Show per-backend state of one or all stacks (state/hash/age/size)",
    )
    p_st_check.add_argument("name", nargs="?", help="Stack name (default: all)")
    p_st_check.add_argument("--backend", help="Limit to a single backend")
    p_st_check.add_argument("--json", action="store_true")
    p_st_check.add_argument("--source", help=_SOURCE_OVERLAY_HELP)
    _add_common(p_st_check)
    p_st_check.set_defaults(handler=_cmd_stack_check)

    p_st_install = st_sub.add_parser(
        "install",
        help="Build a stack on each configured backend (idempotent)",
    )
    p_st_install.add_argument("name")
    p_st_install.add_argument("--backend", help="Install on a single backend only")
    p_st_install.add_argument(
        "--rebuild",
        action="store_true",
        help="Force a rebuild even when the input hash matches the cached build",
    )
    p_st_install.add_argument("--source", help=_SOURCE_OVERLAY_HELP)
    p_st_install.add_argument(
        "--json", action="store_true",
        help="Print the submitted run summary as JSON (server mode only).",
    )
    p_st_install.add_argument(
        "--watch", action="store_true",
        help=(
            "After submitting (server mode only), block until the install "
            "run terminates and exit non-zero on failure — equivalent to "
            "piping through `scripthut run watch --exit-status`."
        ),
    )
    p_st_install.add_argument(
        "--interval", type=float, default=2.0,
        help="Polling interval (seconds) for --watch.",
    )
    _add_common(p_st_install)
    p_st_install.set_defaults(handler=_cmd_stack_install)

    p_st_delete = st_sub.add_parser(
        "delete",
        help="Remove a stack's cache on each backend (all hashes for that name)",
    )
    p_st_delete.add_argument("name")
    p_st_delete.add_argument("--backend", help="Delete on a single backend only")
    p_st_delete.add_argument("--source", help=_SOURCE_OVERLAY_HELP)
    _add_common(p_st_delete)
    p_st_delete.set_defaults(handler=_cmd_stack_delete)

    # ----- disk -------------------------------------------------------------
    p_disk = sub.add_parser(
        "disk",
        help="Show what scripthut has left on each backend's filesystem",
    )
    # Bare `scripthut disk` = `disk status`; flags on the group parser make
    # `scripthut disk --json` work without naming the subcommand.
    p_disk.add_argument("--backend", help="Limit to a single backend")
    p_disk.add_argument("--json", action="store_true")
    _add_common(p_disk)
    p_disk.set_defaults(handler=_cmd_disk_status)
    disk_sub = p_disk.add_subparsers(dest="disk_cmd", required=False)

    p_disk_status = disk_sub.add_parser(
        "status",
        help="Show the server's cached scan results (never triggers a scan)",
    )
    p_disk_status.add_argument("--backend", help="Limit to a single backend")
    p_disk_status.add_argument("--json", action="store_true")
    _add_common(p_disk_status)
    p_disk_status.set_defaults(handler=_cmd_disk_status)

    p_disk_scan = disk_sub.add_parser(
        "scan",
        help="Run a fresh scan (sizes + classification) and print the result",
    )
    p_disk_scan.add_argument("--backend", help="Scan a single backend only")
    p_disk_scan.add_argument("--json", action="store_true")
    p_disk_scan.add_argument(
        "--interval", type=float, default=2.0,
        help="Polling interval (seconds) while waiting for a server-side scan.",
    )
    _add_common(p_disk_scan)
    p_disk_scan.set_defaults(handler=_cmd_disk_scan)

    p_disk_clean = disk_sub.add_parser(
        "clean",
        help=(
            "Delete orphaned scripthut artifacts (guardrailed; scans first, "
            "shows the plan, asks before deleting)"
        ),
    )
    p_disk_clean.add_argument("--backend", help="Clean a single backend only")
    p_disk_clean.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be deleted and stop",
    )
    p_disk_clean.add_argument(
        "--yes", action="store_true",
        help="Skip the confirmation prompt",
    )
    p_disk_clean.add_argument("--json", action="store_true")
    p_disk_clean.add_argument(
        "--path", action="append", dest="paths", metavar="PATH",
        help=(
            "Delete this specific scanned entry (repeatable; requires "
            "--backend; may target entries still referenced by finished runs)"
        ),
    )
    p_disk_clean.add_argument(
        "--interval", type=float, default=2.0,
        help="Polling interval (seconds) while waiting for the server.",
    )
    _add_common(p_disk_clean)
    p_disk_clean.set_defaults(handler=_cmd_disk_clean)

    # ----- project ----------------------------------------------------------
    p_src = sub.add_parser("source", help="Inspect configured sources")
    src_sub = p_src.add_subparsers(dest="src_cmd", required=True)

    p_src_list = src_sub.add_parser("list", help="List configured sources")
    p_src_list.add_argument("--json", action="store_true")
    _add_common(p_src_list)
    p_src_list.set_defaults(handler=_cmd_source_list)

    p_src_view = src_sub.add_parser(
        "view", help="Show source metadata and discovered workflow files",
    )
    p_src_view.add_argument("name")
    p_src_view.add_argument("--json", action="store_true")
    _add_common(p_src_view)
    p_src_view.set_defaults(handler=_cmd_source_view)

    p_src_sync = src_sub.add_parser(
        "sync",
        help="Re-clone/re-glob a source (or all) and refresh workflow cache",
    )
    p_src_sync.add_argument(
        "name",
        nargs="?",
        help="Source name; omit to sync every configured source",
    )
    p_src_sync.add_argument("--json", action="store_true")
    _add_common(p_src_sync)
    p_src_sync.set_defaults(handler=_cmd_source_sync)

    # ----- daemon -----------------------------------------------------------
    # Deliberately no _add_common: --server and the CF auth flags are
    # meaningless for managing a localhost process; only --config matters.
    p_dm = sub.add_parser(
        "daemon", help="Manage the local background scripthut server",
    )
    dm_sub = p_dm.add_subparsers(dest="daemon_cmd", required=True)

    p_dm_start = dm_sub.add_parser(
        "start",
        help=(
            "Start the local daemon (detached; idempotent). For a "
            "foreground server just run `scripthut` with no subcommand."
        ),
    )
    p_dm_start.add_argument("--config", "-c", type=Path, help="Path to scripthut.yaml")
    p_dm_start.set_defaults(handler=_cmd_daemon_start)

    p_dm_stop = dm_sub.add_parser(
        "stop", help="Stop the local daemon (SIGTERM, then SIGKILL)",
    )
    p_dm_stop.add_argument("--config", "-c", type=Path, help="Path to scripthut.yaml")
    p_dm_stop.set_defaults(handler=_cmd_daemon_stop)

    p_dm_status = dm_sub.add_parser(
        "status", help="Show local daemon state (exit 0 running, 3 not)",
    )
    p_dm_status.add_argument("--config", "-c", type=Path, help="Path to scripthut.yaml")
    p_dm_status.add_argument("--json", action="store_true")
    p_dm_status.set_defaults(handler=_cmd_daemon_status)

    p_dm_logs = dm_sub.add_parser(
        "logs", help="Print the daemon log path and its last lines",
    )
    p_dm_logs.add_argument("--config", "-c", type=Path, help="Path to scripthut.yaml")
    p_dm_logs.add_argument("--tail", type=int, default=100, help="Lines to print (default 100)")
    p_dm_logs.set_defaults(handler=_cmd_daemon_logs)

    # ----- status -----------------------------------------------------------
    p_status = sub.add_parser(
        "status",
        help="Show resolved server/auth/config and probe remote reachability",
    )
    p_status.add_argument(
        "--quick",
        action="store_true",
        help="Skip the HTTP probes; just print resolved config (no network).",
    )
    p_status.add_argument("--json", action="store_true")
    _add_common(p_status)
    p_status.set_defaults(handler=_cmd_status)

    return parser


def main(argv: list[str]) -> int:
    """Entrypoint dispatched from ``scripthut.main._dispatch_subcommand``."""
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = args.handler

    try:
        return asyncio.run(handler(args))
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    except httpx.RequestError as e:
        # Connection refused / DNS / timeout — render cleanly instead of
        # a traceback, with the target so the user knows what was probed.
        try:
            target = f"\n  target: {e.request.url}"
        except RuntimeError:
            target = ""
        print(
            f"Error: could not reach the server "
            f"({type(e).__name__}: {e}).{target}\n"
            f"  Is it running? Try `scripthut status` or "
            f"`scripthut daemon status`.",
            file=sys.stderr,
        )
        return 1
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
