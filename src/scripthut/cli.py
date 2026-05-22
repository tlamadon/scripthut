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
are set, commands run locally.
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

from scripthut.config import ConfigError, load_config
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

    async def list_workflows(self) -> dict[str, Any]:
        config = self.runtime.config
        return {
            "workflows": [
                {
                    "name": wf.name,
                    "backend": wf.backend,
                    "description": wf.description,
                    "max_concurrent": wf.max_concurrent,
                    "has_git": wf.git is not None,
                }
                for wf in config.workflows
            ],
        }

    async def list_projects(self) -> dict[str, Any]:
        config = self.runtime.config
        return {
            "projects": [
                {
                    "name": p.name,
                    "backend": p.backend,
                    "path": p.path,
                    "description": p.description,
                    "max_concurrent": p.max_concurrent,
                }
                for p in config.projects
            ],
        }

    async def view_project(self, name: str) -> dict[str, Any]:
        config = self.runtime.config
        project = config.get_project(name)
        if project is None:
            raise RuntimeError(f"Project '{name}' not found")
        workflows: list[str] = []
        discover_error: str | None = None
        try:
            workflows = await self.runtime.run_manager.discover_workflows(name)
        except ValueError as e:
            discover_error = str(e)
        except Exception as e:
            discover_error = str(e)
        return {
            "name": project.name,
            "backend": project.backend,
            "path": project.path,
            "description": project.description,
            "max_concurrent": project.max_concurrent,
            "workflows": workflows,
            "discover_error": discover_error,
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

    async def run_workflow(
        self, name: str, *, backend: str | None = None,
    ) -> dict[str, Any]:
        run = await self.runtime.run_manager.create_run(name, backend=backend)
        return _summary_from_run(run)

    async def run_task(
        self, task_dict: dict, backend: str, run_name: str | None = None,
    ) -> dict[str, Any]:
        from scripthut.runs.models import TaskDefinition

        task = TaskDefinition.from_dict(task_dict)
        run = await self.runtime.run_manager.create_adhoc_run(
            task, backend, run_name=run_name,
        )
        return _summary_from_run(run)

    async def run_project_workflow(
        self, project: str, workflow: str, *, backend: str | None = None,
    ) -> dict[str, Any]:
        run = await self.runtime.run_manager.create_run_from_project(
            project, workflow, backend=backend,
        )
        return _summary_from_run(run)

    async def view_workflow(
        self, name: str, *, backend: str | None = None,
    ) -> dict[str, Any]:
        from scripthut.api import _serialize_dry_run

        result = await self.runtime.run_manager.dry_run(name, backend=backend)
        return _serialize_dry_run(result)

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

    async def rerun(self, run_id: str, mode: str = "new") -> dict[str, Any]:
        rm = self.runtime.run_manager
        if run_id not in rm.runs and rm.storage is not None:
            all_runs = rm.storage.load_all_runs()
            if run_id in all_runs:
                rm.runs[run_id] = all_runs[run_id]
        if mode == "in_place":
            run = await rm.rerun_in_place(run_id)
        else:
            run = await rm.rerun_from(run_id)
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


class RemoteClient:
    """Run scripthut commands against a running server's ``/api/v1``."""

    def __init__(self, base_url: str, *, timeout: float = 60.0):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=f"{self.base_url}/api/v1", timeout=timeout,
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

    async def list_workflows(self) -> dict[str, Any]:
        return await self._get("/workflows")

    async def list_projects(self) -> dict[str, Any]:
        return await self._get("/projects")

    async def view_project(self, name: str) -> dict[str, Any]:
        return await self._get(f"/projects/{name}")

    async def list_runs(self, limit: int | None = None) -> dict[str, Any]:
        return await self._get("/runs", limit=limit)

    async def run_workflow(
        self, name: str, *, backend: str | None = None,
    ) -> dict[str, Any]:
        return await self._post(f"/workflows/{name}/run", backend=backend)

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

    async def run_project_workflow(
        self, project: str, workflow: str, *, backend: str | None = None,
    ) -> dict[str, Any]:
        return await self._post(
            f"/projects/{project}/run", workflow=workflow, backend=backend,
        )

    async def view_workflow(
        self, name: str, *, backend: str | None = None,
    ) -> dict[str, Any]:
        return await self._get(f"/workflows/{name}/dry-run", backend=backend)

    async def view_run(self, run_id: str) -> dict[str, Any]:
        return await self._get(f"/runs/{run_id}")

    async def cancel_run(self, run_id: str) -> dict[str, Any]:
        return await self._post(f"/runs/{run_id}/cancel")

    async def rerun(self, run_id: str, mode: str = "new") -> dict[str, Any]:
        return await self._post(f"/runs/{run_id}/rerun", mode=mode)

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


def _resolve_server(args: argparse.Namespace) -> str | None:
    """Determine which server URL (if any) to target for the command.

    Order: ``--server`` arg > ``SCRIPTHUT_SERVER`` env > config's
    ``settings.cli_server``.  Returns ``None`` for local mode.

    A bare ``--server local`` forces local mode even when env/config
    sets a remote URL — useful for one-off debugging.
    """
    explicit = getattr(args, "server", None)
    if explicit == "local":
        return None
    if explicit:
        return explicit
    env = os.environ.get("SCRIPTHUT_SERVER")
    if env:
        return env
    # Read config's cli_server; failures are non-fatal (user might not have
    # a config in CWD when targeting --server local).
    try:
        config = load_config(getattr(args, "config", None))
    except Exception:
        return None
    return config.settings.cli_server


def _make_client(args: argparse.Namespace) -> Any:
    """Build the right Client subclass for this invocation."""
    server = _resolve_server(args)
    if server:
        return RemoteClient(server)
    return LocalClient(getattr(args, "config", None))


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


async def _run_per_backend(
    config,
    stacks: list[Stack],
    backend_filter: str | None,
    fn,
) -> list[tuple[str, str, StackStatus | None, str | None]]:
    """Iterate (stack × selected backend), open an SSH client, call ``fn``."""
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
                status = await fn(stack, backend_cfg.name, ssh)
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

        # ---- Workflows ----------------------------------------------------
        out.append("### Workflows")
        if not config.workflows:
            out.append("_No workflows configured (use `scripthut task run` for one-off work)._\n")
        else:
            for w in config.workflows:
                desc = f" — {w.description}" if w.description else ""
                out.append(f"- `{w.name}` (backend: `{w.backend}`){desc}")
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

    out.append("### Configured workflow (when one matches the task)\n")
    out.append(
        "```bash\n"
        "scripthut workflow run <name> --json\n"
        "```\n"
        "Prefer a configured workflow over ad-hoc when one already does what "
        "you need — it carries the project's resolved env, partition map, "
        "and stack assumptions.\n"
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
        "scripthut backend list --json               # backend connectivity/health\n"
        "scripthut stack check [<name>] --json       # stacks ready / missing / installing\n"
        "scripthut run list --json --limit 10        # recent runs\n"
        "scripthut run view <id> --json              # one run with item statuses\n"
        "scripthut run logs <id> <task_id> --tail 100         # stdout\n"
        "scripthut run logs <id> <task_id> --error --tail 100 # stderr\n"
        "scripthut workflow view <name> --json       # preview a workflow's tasks\n"
        "```\n"
    )

    out.append("## Exit codes\n")
    out.append(
        "- `0` — command succeeded.\n"
        "- `1` — error (bad arguments, unknown name, backend unreachable).\n"
        "- `2` — `run watch --exit-status` only: the run terminated FAILED/CANCELLED.\n"
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
        "1. `scripthut agent prompt` — re-read whenever the user's project "
        "context changes.\n"
        "2. `scripthut backend list --json` — confirm targets are connected.\n"
        "3. `scripthut workflow view <name> --json` if you might use an "
        "existing workflow instead of ad-hoc — saves work.\n"
        "4. If using a stack: `scripthut stack check <name> --json`; install "
        "with `scripthut stack install <name>` if not `ready`.\n"
        "5. `scripthut task run ... --dry-run` to print the assembled "
        "TaskDefinition. Show it to the user for anything non-trivial.\n"
        "\n"
        "**Submit + track**:\n"
        "6. Submit with `--json`, capture `id`:\n"
        "   ```bash\n"
        "   RUN_ID=$(scripthut task run ... --json | jq -r .id)\n"
        "   ```\n"
        "7. Poll: `scripthut run view $RUN_ID --json` until the run is "
        "terminal. Against a running server, `scripthut run watch $RUN_ID "
        "--exit-status` blocks until done.\n"
        "8. On failure: `scripthut run logs $RUN_ID <task_id> --error "
        "--tail 200`. If the issue is a stack, re-run `stack check`. "
        "If it's a resource issue (OOM, TIMEOUT), bump the relevant field "
        "and resubmit a new run.\n"
    )

    return "\n".join(out)


async def _cmd_agent_prompt(args: argparse.Namespace) -> int:
    try:
        config: ScriptHutConfig | None = load_config(getattr(args, "config", None))
    except (FileNotFoundError, ConfigError):
        config = None
    print(_render_agent_prompt(config))
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


async def _cmd_stack_list(args: argparse.Namespace) -> int:
    config = load_config(getattr(args, "config", None))
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
    config = load_config(getattr(args, "config", None))
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
    results = await _run_per_backend(
        config, stacks, args.backend, mgr.check,
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
    config = load_config(getattr(args, "config", None))
    stack = config.get_stack(args.name)
    if stack is None:
        print(f"Stack '{args.name}' not found", file=sys.stderr)
        return 2

    mgr = StackManager()

    async def do_install(stack, backend_name, ssh):
        return await mgr.install(stack, backend_name, ssh, rebuild=args.rebuild)

    results = await _run_per_backend(config, [stack], args.backend, do_install)
    _print_status_table(results)
    failed = any(
        s is None or s.state != StackState.READY
        for _, _, s, _ in results
    )
    return 1 if failed else 0


async def _cmd_stack_delete(args: argparse.Namespace) -> int:
    config = load_config(getattr(args, "config", None))
    stack = config.get_stack(args.name)
    if stack is None:
        print(f"Stack '{args.name}' not found", file=sys.stderr)
        return 2

    mgr = StackManager()

    async def do_delete(stack, backend_name, ssh):
        await mgr.delete(stack, backend_name, ssh)
        # Re-check to reflect the new state in the table.
        return await mgr.check(stack, backend_name, ssh)

    results = await _run_per_backend(config, [stack], args.backend, do_delete)
    _print_status_table(results)
    failed = any(note is not None for _, _, _, note in results)
    return 1 if failed else 0


# ---------------------------------------------------------------------------
# `workflow` subcommands
# ---------------------------------------------------------------------------


async def _cmd_workflow_list(args: argparse.Namespace) -> int:
    async with _make_client(args) as client:
        data = await client.list_workflows()
    if args.json:
        print(json.dumps(data, indent=2))
        return 0
    workflows = data.get("workflows", [])
    if not workflows:
        print("No workflows configured.  See `scripthut project list` for git projects.")
        return 0
    print("Workflows:")
    for wf in workflows:
        git = " (git)" if wf.get("has_git") else ""
        desc = f" — {wf['description']}" if wf.get("description") else ""
        print(f"  {wf['name']}  [{wf['backend']}]{git}{desc}")
    return 0


# ---------------------------------------------------------------------------
# `project` subcommands
# ---------------------------------------------------------------------------


async def _cmd_project_list(args: argparse.Namespace) -> int:
    async with _make_client(args) as client:
        data = await client.list_projects()
    if args.json:
        print(json.dumps(data, indent=2))
        return 0
    projects = data.get("projects", [])
    if not projects:
        print("No projects configured.")
        return 0
    print("Projects:")
    for p in projects:
        desc = f" — {p['description']}" if p.get("description") else ""
        print(f"  {p['name']}  [{p['backend']}]  {p['path']}{desc}")
    return 0


async def _cmd_project_view(args: argparse.Namespace) -> int:
    async with _make_client(args) as client:
        data = await client.view_project(args.name)
    if args.json:
        print(json.dumps(data, indent=2))
        return 0
    print(f"Project '{data['name']}' on backend '{data['backend']}'")
    print(f"  path: {data['path']}")
    if data.get("description"):
        print(f"  description: {data['description']}")
    if data.get("max_concurrent") is not None:
        print(f"  max_concurrent: {data['max_concurrent']}")
    err = data.get("discover_error")
    workflows = data.get("workflows", [])
    if err:
        print(f"  workflows: <discovery failed: {err}>")
    elif workflows:
        print(f"  workflows ({len(workflows)}):")
        for w in workflows:
            print(f"    {w}")
    else:
        print("  workflows: none discovered")
    return 0


async def _cmd_workflow_view(args: argparse.Namespace) -> int:
    async with _make_client(args) as client:
        data = await client.view_workflow(args.name, backend=args.backend)
    if args.json:
        print(json.dumps(data, indent=2))
        return 0
    wf = data["workflow"]
    print(f"Workflow '{wf['name']}' on backend '{data['backend_name']}'")
    print(f"  tasks: {data['task_count']}, max_concurrent: {data['max_concurrent']}")
    if data.get("commit_hash"):
        print(f"  commit: {data['commit_hash']}")
    for entry in data["tasks"]:
        task = entry["task"]
        deps = ", ".join(task.get("dependencies", [])) or "-"
        print(
            f"  - {task['id']}: {task['name']}  "
            f"({task['cpus']}cpu, {task['memory']}, {task['time_limit']}, deps={deps})"
        )
    return 0


async def _cmd_workflow_run(args: argparse.Namespace) -> int:
    server = _resolve_server(args)
    async with _make_client(args) as client:
        if args.project:
            summary = await client.run_project_workflow(
                args.project, args.name, backend=args.backend,
            )
        else:
            summary = await client.run_workflow(args.name, backend=args.backend)
    _print_run_submitted(summary, remote_base=server)
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
    server = _resolve_server(args)
    mode = "in_place" if args.in_place else "new"
    async with _make_client(args) as client:
        summary = await client.rerun(args.id, mode=mode)
    _print_run_submitted(summary, remote_base=server)
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


CLI_SUBCOMMANDS = {"workflow", "run", "backend", "project"}


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


def build_parser() -> argparse.ArgumentParser:
    """Build the gh-style ``scripthut`` parser (workflow/run noun groups)."""
    parser = argparse.ArgumentParser(
        prog="scripthut",
        description="ScriptHut CLI — manage workflows and runs",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # ----- workflow ---------------------------------------------------------
    p_wf = sub.add_parser("workflow", help="Manage workflows")
    wf_sub = p_wf.add_subparsers(dest="wf_cmd", required=True)

    p_wf_list = wf_sub.add_parser("list", help="List configured workflows and projects")
    p_wf_list.add_argument("--json", action="store_true")
    _add_common(p_wf_list)
    p_wf_list.set_defaults(handler=_cmd_workflow_list)

    p_wf_view = wf_sub.add_parser("view", help="Preview a workflow's tasks (dry run)")
    p_wf_view.add_argument("name")
    p_wf_view.add_argument(
        "--backend",
        help="Override the workflow's configured backend",
    )
    p_wf_view.add_argument("--json", action="store_true")
    _add_common(p_wf_view)
    p_wf_view.set_defaults(handler=_cmd_workflow_view)

    p_wf_run = wf_sub.add_parser("run", help="Submit a workflow for execution")
    p_wf_run.add_argument("name", help="Workflow name (or sflow.json path with --project)")
    p_wf_run.add_argument("--project", help="Trigger an sflow.json from this project")
    p_wf_run.add_argument(
        "--backend",
        help="Override the workflow's (or project's) configured backend",
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

    p_run_rerun = run_sub.add_parser("rerun", help="Re-execute a previous run")
    p_run_rerun.add_argument("id")
    p_run_rerun.add_argument(
        "--in-place", action="store_true",
        help="Reset and re-submit the same run instead of creating a new one",
    )
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

    p_st_list = st_sub.add_parser("list", help="List configured stacks")
    p_st_list.add_argument("--json", action="store_true")
    _add_common(p_st_list)
    p_st_list.set_defaults(handler=_cmd_stack_list)

    p_st_check = st_sub.add_parser(
        "check",
        help="Show per-backend state of one or all stacks (state/hash/age/size)",
    )
    p_st_check.add_argument("name", nargs="?", help="Stack name (default: all)")
    p_st_check.add_argument("--backend", help="Limit to a single backend")
    p_st_check.add_argument("--json", action="store_true")
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
    _add_common(p_st_install)
    p_st_install.set_defaults(handler=_cmd_stack_install)

    p_st_delete = st_sub.add_parser(
        "delete",
        help="Remove a stack's cache on each backend (all hashes for that name)",
    )
    p_st_delete.add_argument("name")
    p_st_delete.add_argument("--backend", help="Delete on a single backend only")
    _add_common(p_st_delete)
    p_st_delete.set_defaults(handler=_cmd_stack_delete)

    # ----- project ----------------------------------------------------------
    p_pr = sub.add_parser("project", help="Inspect git projects")
    pr_sub = p_pr.add_subparsers(dest="pr_cmd", required=True)

    p_pr_list = pr_sub.add_parser("list", help="List configured projects")
    p_pr_list.add_argument("--json", action="store_true")
    _add_common(p_pr_list)
    p_pr_list.set_defaults(handler=_cmd_project_list)

    p_pr_view = pr_sub.add_parser(
        "view", help="Show project metadata and discovered sflow.json files",
    )
    p_pr_view.add_argument("name")
    p_pr_view.add_argument("--json", action="store_true")
    _add_common(p_pr_view)
    p_pr_view.set_defaults(handler=_cmd_project_view)

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
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
