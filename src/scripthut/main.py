"""FastAPI application for ScriptHut."""

import argparse
import asyncio
import json
import logging
import os
import re
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncssh
import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse

from scripthut import __version__
from scripthut.config import find_config_file, load_config, load_yaml_config, set_config
from scripthut.config_schema import GitSourceConfig, PathSourceConfig, ScriptHutConfig
from scripthut.models import ConnectionStatus, HPCJob
from scripthut.runtime import BackendState, Runtime, init_runtime, shutdown_runtime
from scripthut.sources.git import GitSourceManager, SourceStatus
from scripthut.terminal import TerminalManager

logger = logging.getLogger(__name__)
@dataclass
class AppState:
    """Application state container."""

    config: ScriptHutConfig | None = None
    config_error: str | None = None
    backends: dict[str, BackendState] = field(default_factory=dict)
    source_manager: GitSourceManager | None = None
    source_statuses: dict[str, SourceStatus] = field(default_factory=dict)
    path_sources: list[PathSourceConfig] = field(default_factory=list)
    terminal_manager: TerminalManager = field(default_factory=TerminalManager)
    disabled_sources: set[str] = field(default_factory=set)  # Source names toggled off
    filter_enabled: bool = False
    filter_user: str | None = None
    live_view: bool = True
    # Startup readiness: a phase string ("connecting backends", ...) while
    # background initialization runs; None once the server is fully ready.
    # The default is None so CLI/local-runtime contexts that never run the
    # web lifespan behave as before.
    startup_phase: str | None = None
    _startup_task: asyncio.Task[None] | None = None
    _polling_task: asyncio.Task[None] | None = None
    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)
    # SSE: global poll event for dashboard/runs pages
    _poll_event: asyncio.Event = field(default_factory=asyncio.Event)
    _force_poll_event: asyncio.Event = field(default_factory=asyncio.Event)
    _last_poll_time: float = 0.0  # time.monotonic() of last poll completion

    @property
    def seconds_until_next_poll(self) -> int:
        """Seconds remaining until the next squeue poll."""
        if self._last_poll_time == 0.0:
            interval = self.config.settings.poll_interval if self.config else 60
            return interval
        interval = self.config.settings.poll_interval if self.config else 60
        elapsed = time.monotonic() - self._last_poll_time
        remaining = max(0, interval - elapsed)
        return int(remaining)

    def notify_poll(self) -> None:
        """Wake all global SSE listeners (dashboard, runs list)."""
        old = self._poll_event
        self._poll_event = asyncio.Event()
        old.set()

    async def wait_for_poll(self, timeout: float = 65.0) -> bool:
        """Wait for the next squeue poll to complete."""
        try:
            await asyncio.wait_for(self._poll_event.wait(), timeout=timeout)
            return True
        except TimeoutError:
            return False

    @property
    def all_jobs(self) -> list[tuple[str, HPCJob]]:
        """Get all jobs from all backends with backend name."""
        jobs = []
        for backend_name, backend_state in self.backends.items():
            for job in backend_state.jobs:
                jobs.append((backend_name, job))
        return jobs

    def get_filtered_jobs(self) -> list[tuple[str, HPCJob]]:
        """Get jobs - filtering now happens server-side during polling."""
        return self.all_jobs

    @property
    def any_connected(self) -> bool:
        """Check if any backend is connected."""
        return any(c.status.connected for c in self.backends.values())


state = AppState()
_config_path: Path | None = None

async def poll_backend(backend_state: BackendState, filter_user: str | None = None) -> None:
    """Poll jobs for a single backend."""
    if not backend_state.enabled:
        return
    if backend_state.backend is None:
        return

    # Attempt to reconnect if the SSH connection is down (SSH-based backends only)
    if backend_state.ssh_client is not None and not backend_state.ssh_client.is_connected:
        # Skip if still in backoff period
        now = time.monotonic()
        if now < backend_state._reconnect_after:
            return

        try:
            await backend_state.ssh_client.connect()
            logger.info(f"Reconnected to backend '{backend_state.name}'")
            backend_state._reconnect_delay = 0.0
            backend_state._reconnect_after = 0.0
        except asyncssh.PermissionDenied as e:
            # Auth failure — back off aggressively to avoid flooding the server
            delay = min(max(backend_state._reconnect_delay * 2, 60.0), 600.0)
            backend_state._reconnect_delay = delay
            backend_state._reconnect_after = now + delay
            logger.warning(
                f"Auth failed for '{backend_state.name}', next retry in {delay:.0f}s: {e}"
            )
            backend_state.status = ConnectionStatus(
                connected=False,
                host=backend_state.status.host,
                last_poll=backend_state.status.last_poll,
                error=f"{e} (retry in {delay:.0f}s)",
            )
            return
        except Exception as e:
            backend_state.status = ConnectionStatus(
                connected=False,
                host=backend_state.status.host,
                last_poll=backend_state.status.last_poll,
                error=str(e),
            )
            return

    start_time = time.perf_counter()
    try:
        jobs = await backend_state.backend.get_jobs(user=filter_user)
        cluster_info = await backend_state.backend.get_cluster_info(user=filter_user)
        backend_state.jobs = jobs
        backend_state.status = ConnectionStatus(
            connected=True, host=backend_state.status.host,
            last_poll=datetime.now(UTC), job_count=len(jobs), cluster_info=cluster_info,
            last_poll_duration_ms=int((time.perf_counter() - start_time) * 1000),
        )
    except Exception as exc:
        backend_state.status.connected = False
        backend_state.status.error = str(exc)


async def poll_jobs() -> None:
    """Background task to poll all backends periodically."""
    if state.config is None:
        return

    interval = state.config.settings.poll_interval
    logger.info(f"Starting job polling (interval: {interval}s)")
    poll_count = 0

    while not state._shutdown_event.is_set():
        filter_user = state.filter_user if state.filter_enabled else None

        tasks = [
            poll_backend(backend_state, filter_user=filter_user)
            for backend_state in state.backends.values()
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            total_jobs = sum(len(c.jobs) for c in state.backends.values())
            logger.info(f"Polled {total_jobs} jobs from {len(tasks)} backends (filter_user={filter_user})")

        await scriptflow_service.poll()

        # Record poll time and notify SSE listeners
        state._last_poll_time = time.monotonic()
        state.notify_poll()

        try:
            # Sleep until next interval, but wake early on shutdown or force-poll
            done, pending = await asyncio.wait(
                [
                    asyncio.ensure_future(state._shutdown_event.wait()),
                    asyncio.ensure_future(state._force_poll_event.wait()),
                ],
                timeout=interval,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if state._shutdown_event.is_set():
                break
            # Reset force-poll flag for next cycle
            state._force_poll_event.clear()
            # Cancel any pending futures from the wait set
            for f in done | pending:
                f.cancel()
        except asyncio.CancelledError:
            break

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager - handles startup and shutdown."""
    try:
        config = load_config(_config_path)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        state.config_error = str(e)
        yield
        return

    set_config(config)
    state.config = config
    logger.info(f"Loaded configuration with {len(config.backends)} backends, {len(config.sources)} sources")

    # Initialize filter settings
    state.filter_user = config.settings.filter_user
    state.filter_enabled = config.settings.filter_user is not None

    # Initialize source manager
    state.source_manager = GitSourceManager(config.settings.sources_cache_dir_resolved)
    for source in config.sources:
        if isinstance(source, GitSourceConfig):
            state.source_manager.add_source(source)
        elif isinstance(source, PathSourceConfig):
            state.path_sources.append(source)

    # Everything slow — SSH connections — happens in a
    # background task so uvicorn binds the socket immediately. Until the
    # task clears state.startup_phase, the gate middleware answers with a
    # "starting…" page / 503 instead of connection-refused.
    state.startup_phase = "connecting backends"
    state._startup_task = asyncio.create_task(_finish_startup(config))

    yield

    # Shutdown
    logger.info("Shutting down...")
    state._shutdown_event.set()

    # If init is still in flight, stop it before tearing anything down.
    if state._startup_task and not state._startup_task.done():
        state._startup_task.cancel()
        try:
            await state._startup_task
        except asyncio.CancelledError:
            pass

    # Wake all SSE listeners so they can exit cleanly
    state.notify_poll()
    if state._polling_task:
        state._polling_task.cancel()
        try:
            await state._polling_task
        except asyncio.CancelledError:
            pass

    if state.config:
        await shutdown_runtime(Runtime(config=state.config, backends=state.backends))


async def _finish_startup(config: ScriptHutConfig) -> None:
    """Background half of startup: backends and polling.

    Runs after uvicorn is already accepting connections; progress is
    reported through ``state.startup_phase`` (shown by the gate middleware
    and ``/api/v1/health``) and cleared when the server is fully ready.
    """
    try:
        # Initialize shared backend connections
        def on_phase(phase: str) -> None:
            state.startup_phase = phase

        runtime = await init_runtime(config, on_phase=on_phase)
        state.backends.update(runtime.backends)
        # Start background polling
        state._polling_task = asyncio.create_task(poll_jobs())
        logger.info("Startup complete — server ready")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        # Fatal init failure: surface it the same way a broken config is
        # surfaced (homepage banner + API 503s carry the message).
        logger.exception("Startup initialization failed")
        state.config_error = f"Startup failed: {e}"
    finally:
        state.startup_phase = None

@dataclass
class ReloadReport:
    """Summary of which config sections were hot-reloaded vs require a restart."""

    reloaded: list[str] = field(default_factory=list)
    requires_restart: list[str] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    messages: list[str] = field(default_factory=list)

def _diff_named(
    old_items: list[Any], new_items: list[Any]
) -> tuple[set[str], set[str], set[str]]:
    """Return (added, removed, modified) names for two lists of named pydantic models."""
    old_map = {item.name: item.model_dump() for item in old_items}
    new_map = {item.name: item.model_dump() for item in new_items}
    added = set(new_map) - set(old_map)
    removed = set(old_map) - set(new_map)
    modified = {k for k in old_map.keys() & new_map.keys() if old_map[k] != new_map[k]}
    return added, removed, modified

def reload_runtime_config(new_config: ScriptHutConfig) -> ReloadReport:
    """Apply a freshly parsed config to the running server, in-process.

    Hot-reloadable: environments, sources, most settings.
    Backend changes are detected and reported but NOT applied (SSH connections
    and the polling task are kept as-is to avoid losing in-flight job state).
    Pricing service changes also require a restart.
    """
    report = ReloadReport()

    if state.config is None:
        # No prior config (config_error path). A full restart is the only
        # safe way to bring up SSH backends, the polling loop, and the run
        # manager — those are wired up in lifespan(), not here.
        raise RuntimeError(
            "Cannot hot-reload: server started without a valid config. Restart required."
        )

    old = state.config
    added_b, removed_b, modified_b = _diff_named(old.backends, new_config.backends)
    if added_b or removed_b or modified_b:
        report.requires_restart.append("backends")
        if added_b:
            report.messages.append(f"Backends added (restart required): {sorted(added_b)}")
        if removed_b:
            report.messages.append(f"Backends removed (restart required): {sorted(removed_b)}")
        if modified_b:
            report.messages.append(f"Backends modified (restart required): {sorted(modified_b)}")

    if old.settings.poll_interval != new_config.settings.poll_interval:
        report.messages.append(
            f"poll_interval changed ({old.settings.poll_interval} → "
            f"{new_config.settings.poll_interval}); restart required for new interval"
        )

    state.config = new_config
    set_config(new_config)

    state.filter_user = new_config.settings.filter_user
    state.filter_enabled = new_config.settings.filter_user is not None

    report.reloaded.extend(["env rules", "settings"])
    report.counts["env_rules"] = len(new_config.env)

    previously_disabled = set(state.disabled_sources)
    state.source_manager = GitSourceManager(new_config.settings.sources_cache_dir_resolved)
    state.path_sources = []
    state.source_statuses = {}
    new_source_names = {s.name for s in new_config.sources}
    state.disabled_sources = previously_disabled & new_source_names
    for source in new_config.sources:
        if isinstance(source, GitSourceConfig):
            state.source_manager.add_source(source)
        elif isinstance(source, PathSourceConfig):
            state.path_sources.append(source)
    report.reloaded.append("sources")
    report.counts["sources"] = len(new_config.sources)

    state.config_error = None
    return report

# Create FastAPI app
app = FastAPI(
    title="ScriptHut",
    description="ScriptFlow controllers and cluster observation",
    version=__version__,
    lifespan=lifespan,
)

# Versioned JSON API for CLI and external clients
from scripthut.api import make_api_router  # noqa: E402

app.include_router(make_api_router(state))

# Templates. These ship inside the package, so resolve them relative to this
# module rather than the source checkout layout.
templates_path = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_path))
templates.env.globals["scripthut_version"] = __version__

from scripthut.scriptflow_routes import make_scriptflow_router  # noqa: E402
from scripthut.scriptflow_server import ScriptFlowService  # noqa: E402

scriptflow_service = ScriptFlowService(state)
app.include_router(make_scriptflow_router(scriptflow_service, templates))


_STARTING_PAGE = """\
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="2">
  <title>ScriptHut — starting…</title>
  <style>
    body {{ margin: 0; min-height: 100vh; display: flex; align-items: center;
           justify-content: center; background: #0f172a; color: #e2e8f0;
           font-family: ui-sans-serif, system-ui, sans-serif; }}
    .card {{ text-align: center; }}
    .spinner {{ width: 2.5rem; height: 2.5rem; margin: 0 auto 1.25rem;
               border: 3px solid #334155; border-top-color: #38bdf8;
               border-radius: 50%; animation: spin 0.8s linear infinite; }}
    @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
    h1 {{ font-size: 1.1rem; font-weight: 600; margin: 0 0 0.4rem; }}
    p {{ color: #94a3b8; font-size: 0.85rem; margin: 0; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="spinner"></div>
    <h1>ScriptHut is starting</h1>
    <p>{phase}&hellip; this page refreshes automatically.</p>
  </div>
</body>
</html>
"""

_GATE_EXEMPT_PATHS = ("/ping", "/health", "/api/v1/health")


@app.middleware("http")
async def _startup_gate(request: Request, call_next):  # type: ignore[no-untyped-def]
    """While background init runs, answer instead of exposing half-built state.

    Uvicorn accepts connections as soon as the (fast) lifespan yields; the
    slow initialization continues in ``_finish_startup``. Until it clears
    ``state.startup_phase``, HTML routes get an auto-refreshing starting
    page and API routes get a structured 503 — both far better signals
    than the connection-refused a restarting container shows otherwise.
    """
    phase = state.startup_phase
    if phase is None or request.url.path in _GATE_EXEMPT_PATHS:
        return await call_next(request)
    if request.url.path.startswith("/api/"):
        return JSONResponse(
            status_code=503,
            headers={"Retry-After": "2"},
            content={
                "detail": f"Server is starting ({phase}); retry shortly",
                "starting": phase,
            },
        )
    return HTMLResponse(
        status_code=503,
        headers={"Retry-After": "2"},
        content=_STARTING_PAGE.format(phase=phase),
    )

@app.post("/poll")
async def force_poll() -> dict[str, str]:
    """Trigger an immediate poll of all backends."""
    state._force_poll_event.set()
    return {"status": "polling"}

@app.get("/ping")
async def ping() -> dict[str, Any]:
    """Simple ping endpoint - no dependencies. Used by the CLI daemon probe."""
    return {"status": "pong", "pid": os.getpid(), "version": __version__}

@app.get("/health")
async def health() -> dict[str, Any]:
    """Health check endpoint."""
    backend_statuses = {
        name: {
            "connected": cs.status.connected,
            "host": cs.status.host,
            "job_count": len(cs.jobs),
            "last_poll": cs.status.last_poll.isoformat() if cs.status.last_poll else None,
            "error": cs.status.error,
        }
        for name, cs in state.backends.items()
    }

    source_statuses = {
        name: {
            "cloned": ss.cloned,
            "branch": ss.branch,
            "last_commit": ss.last_commit,
            "error": ss.error,
        }
        for name, ss in state.source_statuses.items()
    }

    return {
        "status": "ok" if state.any_connected else "degraded",
        "backends": backend_statuses,
        "sources": source_statuses,
        "total_jobs": sum(len(c.jobs) for c in state.backends.values()),
    }

@app.post("/filter/toggle", response_class=HTMLResponse)
async def toggle_filter(request: Request) -> HTMLResponse:
    """Toggle the user filter on/off and trigger immediate refresh."""
    state.filter_enabled = not state.filter_enabled

    filter_user = state.filter_user if state.filter_enabled else None
    tasks = [
        poll_backend(backend_state, filter_user=filter_user)
        for backend_state in state.backends.values()
    ]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    return await jobs_partial(request)

@app.post("/live-view/toggle", response_class=HTMLResponse)
async def toggle_live_view(request: Request) -> HTMLResponse:
    """Toggle live view (hide old completed jobs)."""
    state.live_view = not state.live_view
    return await jobs_partial(request)

@app.post("/backends/{name}/toggle")
async def toggle_backend(name: str) -> dict[str, Any]:
    """Enable or disable a backend for polling."""
    backend_state = state.backends.get(name)
    if not backend_state:
        return {"error": f"Backend '{name}' not found"}
    backend_state.enabled = not backend_state.enabled
    return {"name": name, "enabled": backend_state.enabled}

@app.get("/backends/{name}/log", response_class=HTMLResponse)
async def backend_log_page(request: Request, name: str) -> HTMLResponse:
    """Show SSH command log for a backend."""
    backend_state = state.backends.get(name)
    if not backend_state:
        return HTMLResponse("Backend not found", status_code=404)
    return templates.TemplateResponse("backend_log.html", {
        "request": request,
        "backend_name": name,
        "backend_type": backend_state.backend_type,
        "entries": backend_state.command_log.get_entries(),
    })

@app.post("/backends/{name}/log/clear")
async def clear_backend_log(name: str) -> dict[str, str]:
    """Clear the SSH command log for a backend."""
    backend_state = state.backends.get(name)
    if not backend_state:
        return {"error": "Backend not found"}
    backend_state.command_log.clear()
    return {"status": "cleared"}

@app.get("/filter/status")
async def filter_status() -> dict[str, Any]:
    """Get current filter status."""
    return {
        "enabled": state.filter_enabled,
        "user": state.filter_user,
    }

@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, message: str = "", message_type: str = "success") -> HTMLResponse:
    """Settings page showing the YAML config file."""
    config_path = find_config_file(_config_path)
    editable = False
    config_content = ""

    if config_path is not None:
        config_path = config_path.resolve()
        try:
            config_content = config_path.read_text()
            editable = os.access(config_path, os.W_OK)
        except OSError as e:
            config_content = f"# Error reading config: {e}"
    else:
        config_content = "# No configuration file found.\n# Create scripthut.yaml to get started."

    sections = _section_summary()

    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "config_content": config_content,
            "config_path": str(config_path) if config_path else "No config file",
            "editable": editable,
            "message": message,
            "message_type": message_type,
            "sections": sections,
            "can_hot_reload": state.config is not None,
        },
    )

def _section_summary() -> dict[str, dict[str, Any]]:
    """Build a per-section summary of the loaded config for the settings page.

    Each entry is `{names: [...], hot_reload: bool, label: str}`. Used to show
    the user which sections will hot-reload vs require a restart.
    """
    cfg = state.config
    if cfg is None:
        return {}
    return {
        "env_rules": {
            "label": "Env rules",
            "names": [f"rule #{i}" for i, _ in enumerate(cfg.env)],
            "hot_reload": True,
        },
        "sources": {
            "label": "Sources",
            "names": [s.name for s in cfg.sources],
            "hot_reload": True,
        },
        "backends": {
            "label": "Backends",
            "names": [b.name for b in cfg.backends],
            "hot_reload": False,
        },

    }

@app.post("/settings", response_class=HTMLResponse)
async def settings_save(request: Request) -> HTMLResponse:
    """Save the YAML config file and optionally restart or hot-reload."""
    form = await request.form()
    config_content = form.get("config_content", "")
    action = form.get("action", "save")

    config_path = find_config_file(_config_path)
    if config_path is None:
        return await settings_page(request, message="No config file to save to.", message_type="error")

    config_path = config_path.resolve()

    import yaml
    try:
        raw = yaml.safe_load(config_content)
        if raw is None:
            raw = {}
        new_config = ScriptHutConfig.model_validate(raw)
    except Exception as e:
        return await settings_page(
            request,
            message=f"Invalid configuration: {e}",
            message_type="error",
        )

    try:
        config_path.write_text(config_content)
    except OSError as e:
        return await settings_page(request, message=f"Failed to save: {e}", message_type="error")

    if action == "save_restart":
        message = "Configuration saved. Restarting server..."
        asyncio.get_event_loop().call_later(0.5, _restart_server)
        return await settings_page(request, message=message, message_type="warning")

    if action == "save_reload":
        try:
            report = reload_runtime_config(new_config)
        except Exception as e:
            return await settings_page(
                request,
                message=f"Saved, but hot-reload failed: {e}. Restart to apply.",
                message_type="error",
            )
        parts = [f"Reloaded {', '.join(report.reloaded)}."]
        if report.requires_restart:
            parts.append(
                f"Restart required for: {', '.join(report.requires_restart)}."
            )
        parts.extend(report.messages)
        msg_type = "warning" if report.requires_restart else "success"
        return await settings_page(request, message=" ".join(parts), message_type=msg_type)

    return await settings_page(request, message="Configuration saved successfully.", message_type="success")

@app.post("/admin/reload")
async def admin_reload() -> JSONResponse:
    """Re-read scripthut.yaml from disk and hot-reload safe sections.

    Reloads environments, sources, and most settings
    in-process. Backend connection changes still require a server restart.
    """
    try:
        path = find_config_file(_config_path)
        if path is None:
            return JSONResponse(
                {"ok": False, "error": "No config file found"}, status_code=404
            )
        new_config = load_yaml_config(path)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)

    try:
        report = reload_runtime_config(new_config)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=409)

    return JSONResponse(
        {
            "ok": True,
            "reloaded": report.reloaded,
            "requires_restart": report.requires_restart,
            "counts": report.counts,
            "messages": report.messages,
        }
    )

def _restart_server() -> None:
    """Restart the server process by re-executing the current command."""
    import sys
    os.execv(sys.executable, [sys.executable] + sys.argv)

@app.get("/terminal/{backend_name}", response_class=HTMLResponse)
async def terminal_page(request: Request, backend_name: str) -> HTMLResponse:
    """Serve the browser terminal page for a backend."""
    backend_state = state.backends.get(backend_name)
    if not backend_state:
        return HTMLResponse("Backend not found", status_code=404)

    return templates.TemplateResponse(
        "terminal.html",
        {
            "request": request,
            "backend_name": backend_name,
            "backend_type": backend_state.backend_type,
            "host": backend_state.status.host,
        },
    )

@app.websocket("/ws/terminal/{backend_name}")
async def terminal_ws(websocket: WebSocket, backend_name: str) -> None:
    """WebSocket endpoint for interactive terminal sessions.

    After accepting, expects an init JSON message:
      {"type": "init", "cols": 80, "rows": 24,
       "session_type": "headnode" | "job",
       "node": "...", "job_id": "..."}  (node/job_id for job sessions)
    """
    await websocket.accept()

    backend_state = state.backends.get(backend_name)
    if not backend_state or not backend_state.ssh_client:
        await websocket.send_json(
            {"type": "error", "message": f"Backend '{backend_name}' not found or disconnected"}
        )
        await websocket.close()
        return

    # Peek at the init message to determine session type and build command
    try:
        init_data = await asyncio.wait_for(websocket.receive_json(), timeout=10)
    except Exception:
        await websocket.close()
        return

    session_type = init_data.get("session_type", "headnode")
    command: str | None = None
    session_node: str | None = None
    session_job_id: str | None = None
    session_label: str | None = None

    node = init_data.get("node", "")
    job_id = init_data.get("job_id", "")
    tmux_session = init_data.get("session") or (f"sh-{job_id}" if job_id else "")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", tmux_session):
        await websocket.send_json({"type": "error", "message": "An existing compute host and tmux session are required"})
        await websocket.close()
        return
    command = f'ssh -tt {node} "tmux attach -t {tmux_session}"'
    session_type = "job"
    session_node = node
    session_job_id = job_id or None
    session_label = f"{backend_name} - {tmux_session} on {node}"

    session = state.terminal_manager.create_session(
        backend_name, session_type,
        node=session_node, job_id=session_job_id, label=session_label,
    )
    session.websocket = websocket
    logger.info(f"Terminal session {session.id} ({session_type}) for backend '{backend_name}'")

    try:
        cols = init_data.get("cols", 80)
        rows = init_data.get("rows", 24)

        process = await backend_state.ssh_client.create_interactive_session(
            command=command,
            term_size=(cols, rows),
        )

        async def ws_to_ssh() -> None:
            try:
                while True:
                    raw = await websocket.receive_text()
                    msg = json.loads(raw)
                    if msg["type"] == "input":
                        process.stdin.write(msg["data"].encode())
                    elif msg["type"] == "resize":
                        process.change_terminal_size(
                            msg.get("cols", 80), msg.get("rows", 24)
                        )
            except (WebSocketDisconnect, Exception):
                process.close()

        async def ssh_to_ws() -> None:
            try:
                while not process.is_closing():
                    data = await process.stdout.read(4096)
                    if not data:
                        break
                    text = data.decode("utf-8", errors="replace")
                    await websocket.send_json({"type": "output", "data": text})
            except Exception:
                pass
            finally:
                exit_code = process.returncode if process.returncode is not None else -1
                try:
                    await websocket.send_json({"type": "exit", "code": exit_code})
                    await websocket.close()
                except Exception:
                    pass

        done, pending = await asyncio.wait(
            [asyncio.create_task(ws_to_ssh()), asyncio.create_task(ssh_to_ws())],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()

    except Exception as e:
        logger.error(f"Terminal session {session.id} failed: {e}")
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
            await websocket.close()
        except Exception:
            pass
    finally:
        state.terminal_manager.remove_session(session.id)
        logger.info(f"Terminal session {session.id} closed")

@app.delete("/terminal/sessions/{session_id}")
async def close_terminal_session(session_id: str) -> dict[str, str]:
    """Close a terminal session by closing its WebSocket connection."""
    closed = await state.terminal_manager.close_session(session_id)
    if not closed:
        return JSONResponse(status_code=404, content={"error": "Session not found"})
    return {"status": "ok"}

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="ScriptHut - ScriptFlow and cluster observation",
        epilog="Commands: " + ", ".join(sorted(_CLI_SUBCOMMANDS)) + ". Use COMMAND --help for details.",
    )
    parser.add_argument(
        "--version",
        "-V",
        action="version",
        version=f"scripthut {__version__}",
    )
    parser.add_argument(
        "--config",
        "-c",
        type=Path,
        help="Path to configuration file (default: ./scripthut.yaml)",
    )
    parser.add_argument(
        "--host",
        type=str,
        help="Host to bind the server to (overrides config)",
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        help="Port to bind the server to (overrides config)",
    )
    return parser.parse_args()

def _dispatch_subcommand(name: str, argv_rest: list[str]) -> int:
    if name in _CLI_SUBCOMMANDS:
        from scripthut.cli import main as cli_main
        return cli_main([name, *argv_rest])
    raise SystemExit(f"Unknown subcommand: {name}")

def run() -> None:
    """Run the application with uvicorn (or dispatch to a subcommand)."""
    import sys
    if len(sys.argv) > 1 and sys.argv[1] in _SUBCOMMANDS:
        sys.exit(_dispatch_subcommand(sys.argv[1], sys.argv[2:]))

    global _config_path

    args = parse_args()
    _config_path = args.config

    try:
        config = load_config(_config_path)
        host = args.host or config.settings.server_host
        port = args.port or config.settings.server_port
    except Exception as e:
        logger.warning(f"Failed to load config: {e}")
        logger.warning("Starting server with defaults — config error will be shown on homepage")
        host = args.host or "127.0.0.1"
        port = args.port or 8000

    url = f"http://{host}:{port}"
    line1 = f"  ScriptHut v{__version__}"
    line2 = f"  → {url}"
    width = max(len(line1) + 4, len(line2) + 4, 40)
    print()
    print(f"  ╔{'═' * (width - 4)}╗")
    print(f"  ║{line1:<{width - 4}}║")
    print(f"  ║{line2:<{width - 4}}║")
    print(f"  ╚{'═' * (width - 4)}╝")
    print()

    uvicorn.run(
        "scripthut.main:app",
        host=host,
        port=port,
        reload=False,
    )


_CLI_SUBCOMMANDS = frozenset({"backend", "source", "agent", "status", "daemon", "scriptflow"})
_SUBCOMMANDS = _CLI_SUBCOMMANDS


@app.get("/", response_class=HTMLResponse)
async def overview(request: Request):
    if state.config_error:
        return templates.TemplateResponse("config_error.html", {"request": request, "config_error": state.config_error})
    from fastapi.responses import RedirectResponse
    return RedirectResponse("/scriptflow")


@app.get("/backends", response_class=HTMLResponse)
async def backends_page(request: Request):
    return templates.TemplateResponse("backends.html", {"request": request, "state": state})


@app.get("/jobs", response_class=HTMLResponse)
async def jobs_partial(request: Request):
    return templates.TemplateResponse("jobs.html", {"request": request, "jobs": state.all_jobs})


@app.get("/jobs/stream")
async def jobs_stream(request: Request):
    async def events():
        while not state._shutdown_event.is_set():
            if await request.is_disconnected():
                break
            if await state.wait_for_poll():
                yield {"event": "jobs", "data": templates.get_template("jobs.html").render(jobs=state.all_jobs)}
    return EventSourceResponse(events())


@app.get("/sources", response_class=HTMLResponse)
async def sources_page(request: Request):
    from scripthut.scriptflow_sources import source_rows

    rows = await source_rows(state.config, state.backends) if state.config else []
    return templates.TemplateResponse("sources.html", {"request": request, "sources": rows})


@app.get("/terminals", response_class=HTMLResponse)
async def terminals_page(request: Request):
    return templates.TemplateResponse("terminals.html", {"request": request, "sessions": list(state.terminal_manager.sessions.values()), "backends": state.backends})


if __name__ == "__main__":
    run()
