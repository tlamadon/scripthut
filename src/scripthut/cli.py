"""CLI for ScriptFlow, backend observation and local server management."""
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
from scripthut.config_schema import ScriptHutConfig
from scripthut.runtime import Runtime, init_runtime, shutdown_runtime
logger = logging.getLogger(__name__)

def _emit_json(payload: Any) -> int:
    """Print ``payload`` as indented JSON. Returns 0 for use as a tail call."""
    print(json.dumps(payload, indent=2, default=str))
    return 0

class LocalClient:
    """Inspect sources and backend connections in a short-lived local runtime."""

    def __init__(self, config_path: Path | None = None):
        self._config_path = config_path
        self._runtime: Runtime | None = None

    async def __aenter__(self) -> LocalClient:
        config = load_config(self._config_path)
        self._runtime = await init_runtime(config)
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
            }
            if isinstance(s, GitSourceConfig):
                base.update({"url": s.url, "branch": s.branch})
                if s.local_path is not None:
                    base["local_path"] = str(s.local_path_resolved)
            elif isinstance(s, PathSourceConfig):
                base.update({"path": s.path, "backend": s.backend})
            out.append(base)
        return {"sources": out}

    async def view_source(self, name: str) -> dict[str, Any]:
        """Return configured source metadata."""
        from scripthut.config_schema import GitSourceConfig, PathSourceConfig
        config = self.runtime.config
        source = config.get_source(name)
        if source is None:
            raise RuntimeError(f"Source '{name}' not found")
        base: dict[str, Any] = {
            "name": source.name, "type": source.type,
            "description": getattr(source, "description", ""),
        }
        if isinstance(source, GitSourceConfig):
            base.update({"url": source.url, "branch": source.branch})
            if source.local_path is not None:
                base["local_path"] = str(source.local_path_resolved)
        elif isinstance(source, PathSourceConfig):
            base.update({"path": source.path, "backend": source.backend})
        return base

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

async def _cmd_agent_prompt(args: argparse.Namespace) -> int:
    try:
        config: ScriptHutConfig | None = load_config(getattr(args, "config", None))
    except (FileNotFoundError, ConfigError):
        config = None
    prompt = _render_agent_prompt(config)
    if args.json:
        # The briefing is markdown; JSON mode just wraps it so a caller
        # piping every command through jq doesn't hit a parse error.
        return _emit_json({"prompt": prompt})
    print(prompt)
    return 0

async def _cmd_agent_install(args: argparse.Namespace) -> int:
    from scripthut import agent_skill

    if args.user:
        claude_dir = Path.home() / ".claude"
    else:
        claude_dir = Path.cwd() / ".claude"

    results = agent_skill.install_assets(claude_dir, force=args.force)
    skipped = any(r.action == "skipped" for r in results)

    if args.json:
        _emit_json({
            "claude_dir": str(claude_dir),
            "files": [{"path": str(r.path), "action": r.action} for r in results],
            "skipped": skipped,
        })
        return 1 if skipped else 0

    for r in results:
        try:
            shown: Path | str = r.path.relative_to(Path.cwd())
        except ValueError:
            shown = r.path
        print(f"  {r.action:<9} {shown}")

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
            [{"name": b.name, "type": b.type} for b in config.backends]
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
            if s.local_path is not None:
                base["local_path"] = str(s.local_path_resolved)
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

def _source_location(s: dict[str, Any]) -> str:
    """Where a source's code comes from, for one-line listings.

    A git source with ``local_path`` shows the local repo it is read and
    pushed from — that, not the (optional) remote, is what a run uses.
    """
    if s.get("type") != "git":
        return f"[{s.get('backend', '?')}] {s.get('path', '?')}"
    branch = s.get("branch", "main")
    if s.get("local_path"):
        return f"{s['local_path']}@{branch} (local)"
    return f"{s.get('url', '?')}@{branch}"

def _format_source_line(s: dict[str, Any]) -> str:
    """One-line representation of a source for status output."""
    desc = f"  — {s['description']}" if s.get("description") else ""
    loc = _source_location(s)
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
    if args.json:
        return _emit_json({
            "running": True,
            "already_running": already,
            "pid": info.pid,
            "url": info.url,
            "log_path": str(daemon.logfile_path(config)),
        })
    if already:
        print(f"Already running (pid {info.pid}) at {info.url}")
    else:
        print(f"Started local daemon (pid {info.pid}) at {info.url}")
        print(f"  logs: {daemon.logfile_path(config)}")
    return 0

async def _cmd_daemon_stop(args: argparse.Namespace) -> int:
    from scripthut import daemon

    message = daemon.stop_daemon(_load_config_or_none(args))
    if args.json:
        return _emit_json({"running": False, "message": message})
    print(message)
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
        if args.json:
            _emit_json({"log_path": str(path), "exists": False, "lines": []})
            return 2
        print(f"No daemon log at {path}", file=sys.stderr)
        return 2
    lines = path.read_text(errors="replace").splitlines()[-args.tail:]
    if args.json:
        return _emit_json({"log_path": str(path), "exists": True, "lines": lines})
    print(f"# {path}", file=sys.stderr)
    for line in lines:
        print(line)
    return 0

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
        loc = _source_location(s)
        print(f"  {s['name']:<20} ({s.get('type', '?')})  {loc}{desc}")
    return 0

async def _cmd_source_sync(args: argparse.Namespace) -> int:
    """Synchronize configured git sources; report individual errors."""
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
            line = f"  {marker} {r['name']:<20} ({r.get('type', '?')})"
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
        if data.get("local_path"):
            print(f"  local_path: {data['local_path']}  (pushed to the backend)")
        print(f"  url:    {data.get('url') or '<none>'}")
        print(f"  branch: {data.get('branch', '?')}")
    else:
        print(f"  backend: {data.get('backend', '?')}")
        print(f"  path:    {data.get('path', '?')}")
    if data.get("description"):
        print(f"  description: {data['description']}")
    for warn in data.get("warnings") or []:
        print(f"  warning: {warn}")
    return 0

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
    print(f"{'NAME':<20} {'TYPE':<8} {'STATUS':<10}")
    for b in backends:
        status = "connected" if b.get("connected") else "down"
        print(f"{b['name'][:20]:<20} {b['type']:<8} {status:<10}")
    return 0

def _add_common(parser: argparse.ArgumentParser) -> None:
    # --json lives here so *every* subcommand accepts it. Agents are told
    # (by `scripthut agent prompt` and the installed skill) that the flag is
    # universal; a subcommand that omitted it died with "unrecognized
    # arguments: --json" mid-pipeline. Handlers must honour it — see
    # test_every_subcommand_accepts_json.
    parser.add_argument(
        "--json", action="store_true",
        help="Emit machine-readable JSON on stdout instead of a table.",
    )
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

def _add_daemon_common(parser: argparse.ArgumentParser) -> None:
    """Flags shared by ``daemon`` subcommands.

    Not ``_add_common``: --server and the CF auth flags are meaningless for
    managing a localhost process. --json still is, so it lives here too.
    """
    parser.add_argument("--config", "-c", type=Path, help="Path to scripthut.yaml")
    parser.add_argument(
        "--json", action="store_true",
        help="Emit machine-readable JSON on stdout instead of a table.",
    )

def build_parser() -> argparse.ArgumentParser:
    """Build the ScriptHut command groups."""
    parser = argparse.ArgumentParser(
        prog="scripthut",
        description="ScriptHut CLI — ScriptFlow and cluster observation",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    from scripthut.scriptflow_cli import add_parser as add_scriptflow_parser

    add_scriptflow_parser(sub, _add_daemon_common)

    # ----- backend ----------------------------------------------------------
    p_be = sub.add_parser("backend", help="Inspect configured backends")
    be_sub = p_be.add_subparsers(dest="be_cmd", required=True)

    p_be_list = be_sub.add_parser("list", help="List configured backends")
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
    p_ag_install.add_argument(
        "--json", action="store_true",
        help="Emit machine-readable JSON on stdout instead of a table.",
    )
    p_ag_install.set_defaults(handler=_cmd_agent_install)

    # ----- project ----------------------------------------------------------
    p_src = sub.add_parser("source", help="Inspect configured sources")
    src_sub = p_src.add_subparsers(dest="src_cmd", required=True)

    p_src_list = src_sub.add_parser("list", help="List configured sources")
    _add_common(p_src_list)
    p_src_list.set_defaults(handler=_cmd_source_list)

    p_src_view = src_sub.add_parser(
        "view", help="Show source metadata",
    )
    p_src_view.add_argument("name")
    _add_common(p_src_view)
    p_src_view.set_defaults(handler=_cmd_source_view)

    p_src_sync = src_sub.add_parser(
        "sync",
        help="Synchronize a configured git source (or all)",
    )
    p_src_sync.add_argument(
        "name",
        nargs="?",
        help="Source name; omit to sync every configured source",
    )
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
    _add_daemon_common(p_dm_start)
    p_dm_start.set_defaults(handler=_cmd_daemon_start)

    p_dm_stop = dm_sub.add_parser(
        "stop", help="Stop the local daemon (SIGTERM, then SIGKILL)",
    )
    _add_daemon_common(p_dm_stop)
    p_dm_stop.set_defaults(handler=_cmd_daemon_stop)

    p_dm_status = dm_sub.add_parser(
        "status", help="Show local daemon state (exit 0 running, 3 not)",
    )
    _add_daemon_common(p_dm_status)
    p_dm_status.set_defaults(handler=_cmd_daemon_status)

    p_dm_logs = dm_sub.add_parser(
        "logs", help="Print the daemon log path and its last lines",
    )
    _add_daemon_common(p_dm_logs)
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
    except (RuntimeError, ConfigError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130


def _render_agent_prompt(config: ScriptHutConfig) -> str:
    from scripthut.agent_skill import render_skill
    inventory = {"backends": [b.name for b in config.backends], "sources": [s.name for s in config.sources]}
    return render_skill() + "\nConfigured inventory:\n" + json.dumps(inventory, indent=2)
