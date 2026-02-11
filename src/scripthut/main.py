"""FastAPI application for ScriptHut."""

import argparse
import asyncio
import json
import logging
import time

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, AsyncGenerator

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse

from scripthut.backends.slurm import JobStats, SlurmBackend
from scripthut.config import load_config, set_config
from scripthut.config_schema import ScriptHutConfig, SlurmBackendConfig
from scripthut.models import ConnectionStatus, JobState, SlurmJob
from scripthut.runs import Run, RunManager
from scripthut.runs.models import RunItemStatus
from scripthut.runs.storage import RunStorageManager
from scripthut.sources.git import GitSourceManager, SourceStatus
from scripthut.ssh.client import SSHClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class BackendState:
    """State for a single backend."""

    name: str
    backend_type: str
    ssh_client: SSHClient | None = None
    backend: SlurmBackend | None = None
    jobs: list[SlurmJob] = field(default_factory=list)
    status: ConnectionStatus = field(
        default_factory=lambda: ConnectionStatus(connected=False, host="")
    )


@dataclass
class AppState:
    """Application state container."""

    config: ScriptHutConfig | None = None
    backends: dict[str, BackendState] = field(default_factory=dict)
    source_manager: GitSourceManager | None = None
    source_statuses: dict[str, SourceStatus] = field(default_factory=dict)
    run_manager: RunManager | None = None
    run_storage: RunStorageManager | None = None
    filter_enabled: bool = False
    filter_user: str | None = None
    live_view: bool = True
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
        except asyncio.TimeoutError:
            return False

    @property
    def all_jobs(self) -> list[tuple[str, SlurmJob]]:
        """Get all jobs from all backends with backend name."""
        jobs = []
        for backend_name, backend_state in self.backends.items():
            for job in backend_state.jobs:
                jobs.append((backend_name, job))
        return jobs

    def get_filtered_jobs(self) -> list[tuple[str, SlurmJob]]:
        """Get jobs - filtering now happens server-side during polling."""
        return self.all_jobs

    @property
    def any_connected(self) -> bool:
        """Check if any backend is connected."""
        return any(c.status.connected for c in self.backends.values())


state = AppState()

# CLI argument for config path (set by run())
_config_path: Path | None = None


async def init_backend(backend_config: SlurmBackendConfig) -> BackendState:
    """Initialize a Slurm backend connection."""
    backend_state = BackendState(
        name=backend_config.name,
        backend_type="slurm",
        status=ConnectionStatus(connected=False, host=backend_config.ssh.host),
    )

    ssh_client = SSHClient(
        host=backend_config.ssh.host,
        user=backend_config.ssh.user,
        key_path=backend_config.ssh.key_path_resolved,
        port=backend_config.ssh.port,
        cert_path=backend_config.ssh.cert_path_resolved,
        known_hosts=backend_config.ssh.known_hosts_resolved,
    )

    try:
        await ssh_client.connect()
        backend_state.ssh_client = ssh_client
        backend_state.backend = SlurmBackend(ssh_client)
        backend_state.status = ConnectionStatus(
            connected=True,
            host=backend_config.ssh.host,
        )
        logger.info(f"Connected to backend '{backend_config.name}' ({backend_config.ssh.host})")
    except Exception as e:
        logger.error(f"Failed to connect to backend '{backend_config.name}': {e}")
        backend_state.status = ConnectionStatus(
            connected=False,
            host=backend_config.ssh.host,
            error=str(e),
        )

    return backend_state


async def poll_backend(backend_state: BackendState, filter_user: str | None = None) -> None:
    """Poll jobs for a single backend."""
    if backend_state.backend is None:
        return

    start_time = time.perf_counter()
    try:
        jobs = await backend_state.backend.get_jobs(user=filter_user)
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        backend_state.jobs = jobs
        backend_state.status = ConnectionStatus(
            connected=True,
            host=backend_state.status.host,
            last_poll=datetime.now(),
            last_poll_duration_ms=duration_ms,
            job_count=len(jobs),
        )
        logger.debug(f"Polled {len(jobs)} jobs from '{backend_state.name}' in {duration_ms}ms")

        # Collect slurm_job_ids missing utilization stats across all runs
        sacct_ids: list[str] = []
        if state.run_manager:
            for run in state.run_manager.runs.values():
                if run.backend_name != backend_state.name:
                    continue
                for item in run.items:
                    if (item.slurm_job_id
                        and item.status in (RunItemStatus.COMPLETED, RunItemStatus.FAILED)
                        and item.cpu_efficiency is None):
                        sacct_ids.append(item.slurm_job_id)

        # Query sacct for resource utilization
        job_stats: dict[str, JobStats] = {}
        if sacct_ids:
            try:
                job_stats = await backend_state.backend.get_job_stats(
                    sacct_ids, user=filter_user,
                )
            except Exception as e:
                logger.warning(f"sacct stats fetch failed for '{backend_state.name}': {e}")

        # Write stats back to RunItems
        if state.run_manager and job_stats:
            for run in state.run_manager.runs.values():
                if run.backend_name != backend_state.name:
                    continue
                for item in run.items:
                    if item.slurm_job_id and item.slurm_job_id in job_stats:
                        s = job_stats[item.slurm_job_id]
                        item.cpu_efficiency = s.cpu_efficiency
                        item.max_rss = s.max_rss
                        if s.start_time:
                            item.started_at = s.start_time
                        if s.end_time:
                            item.finished_at = s.end_time
                        state.run_manager._persist_run(run)

        # Handle external jobs (not in any active run)
        if state.run_manager and state.run_storage:
            known_slurm_ids: set[str] = set()
            for run in state.run_manager.runs.values():
                for item in run.items:
                    if item.slurm_job_id:
                        known_slurm_ids.add(item.slurm_job_id)

            slurm_state_to_run_status = {
                JobState.PENDING: "submitted",
                JobState.RUNNING: "running",
                JobState.COMPLETING: "running",
                JobState.COMPLETED: "completed",
                JobState.CANCELLED: "failed",
                JobState.FAILED: "failed",
                JobState.TIMEOUT: "failed",
                JobState.NODE_FAIL: "failed",
                JobState.PREEMPTED: "failed",
                JobState.BOOT_FAIL: "failed",
                JobState.DEADLINE: "failed",
                JobState.OUT_OF_MEMORY: "failed",
            }

            for slurm_job in jobs:
                if slurm_job.job_id not in known_slurm_ids:
                    run_status = slurm_state_to_run_status.get(slurm_job.state, "running")
                    stats = job_stats.get(slurm_job.job_id)
                    state.run_storage.add_external_job(
                        backend_name=backend_state.name,
                        slurm_job_id=slurm_job.job_id,
                        name=slurm_job.name,
                        user=slurm_job.user,
                        state=run_status,
                        partition=slurm_job.partition,
                        cpus=slurm_job.cpus,
                        memory=slurm_job.memory,
                        submit_time=slurm_job.submit_time,
                        start_time=slurm_job.start_time,
                        cpu_efficiency=stats.cpu_efficiency if stats else None,
                        max_rss=stats.max_rss if stats else None,
                    )

    except Exception as e:
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        logger.error(f"Job polling failed for '{backend_state.name}': {e}")
        backend_state.status = ConnectionStatus(
            connected=False,
            host=backend_state.status.host,
            last_poll=backend_state.status.last_poll,
            last_poll_duration_ms=duration_ms,
            error=str(e),
        )


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
            if backend_state.backend is not None
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            total_jobs = sum(len(c.jobs) for c in state.backends.values())
            logger.info(f"Polled {total_jobs} jobs from {len(tasks)} backends (filter_user={filter_user})")

        # Update run statuses based on polled jobs
        if state.run_manager and state.run_manager.get_active_runs():
            backend_jobs: dict[str, list[tuple[str, JobState]]] = {}
            for name, bs in state.backends.items():
                backend_jobs[name] = [(job.job_id, job.state) for job in bs.jobs]
            await state.run_manager.update_all_runs(backend_jobs)

        # Save dirty runs
        if state.run_manager:
            state.run_manager.save_dirty()

        # Record poll time and notify SSE listeners
        state._last_poll_time = time.monotonic()
        state.notify_poll()

        # Cleanup old runs once per hour
        poll_count += 1
        if poll_count >= 60 and state.run_storage:
            state.run_storage.cleanup_old_runs()
            poll_count = 0

        try:
            # Sleep until next interval, but wake early on shutdown or force-poll
            done, _ = await asyncio.wait(
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
            for f in done:
                f.cancel()
        except asyncio.CancelledError:
            break


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager - handles startup and shutdown."""
    config = load_config(_config_path)
    set_config(config)
    state.config = config

    logger.info(f"Loaded configuration with {len(config.backends)} backends, {len(config.sources)} sources")

    # Initialize filter settings
    state.filter_user = config.settings.filter_user
    state.filter_enabled = config.settings.filter_user is not None

    # Initialize source manager
    state.source_manager = GitSourceManager(config.settings.sources_cache_dir_resolved)
    for source in config.sources:
        state.source_manager.add_source(source)

    if config.sources:
        asyncio.create_task(sync_sources_background())

    # Initialize backends
    for backend_config in config.slurm_backends:
        backend_state = await init_backend(backend_config)
        state.backends[backend_config.name] = backend_state

    for ecs_config in config.ecs_backends:
        logger.warning(f"ECS backend '{ecs_config.name}' configured but ECS backend not yet implemented")

    # Initialize run storage and manager
    state.run_storage = RunStorageManager()
    ssh_clients = {
        name: cs.ssh_client
        for name, cs in state.backends.items()
        if cs.ssh_client is not None
    }
    state.run_manager = RunManager(config, ssh_clients, storage=state.run_storage)
    logger.info(f"Initialized run manager with {len(config.workflows)} workflows")

    # Restore runs from storage
    restored_count = await state.run_manager.restore_from_storage()
    if restored_count > 0:
        logger.info(f"Restored {restored_count} runs from storage")

    # Start background polling
    state._polling_task = asyncio.create_task(poll_jobs())

    yield

    # Shutdown
    logger.info("Shutting down...")
    state._shutdown_event.set()

    # Wake all SSE listeners so they can exit cleanly
    state.notify_poll()
    if state.run_manager:
        for run_id in list(state.run_manager._run_events.keys()):
            state.run_manager.notify_run(run_id)

    if state._polling_task:
        state._polling_task.cancel()
        try:
            await state._polling_task
        except asyncio.CancelledError:
            pass

    # Save any unsaved run changes before disconnecting
    if state.run_manager:
        state.run_manager.save_dirty()
        logger.info("Saved run data on shutdown")

    # Disconnect all backends
    for backend_state in state.backends.values():
        if backend_state.ssh_client:
            await backend_state.ssh_client.disconnect()


async def sync_sources_background() -> None:
    """Sync git sources in the background."""
    if state.source_manager is None:
        return

    logger.info("Syncing git sources...")
    try:
        state.source_statuses = await state.source_manager.sync_all()
        synced = sum(1 for s in state.source_statuses.values() if s.cloned)
        logger.info(f"Synced {synced}/{len(state.source_statuses)} sources")
    except Exception as e:
        logger.error(f"Failed to sync sources: {e}")


# Create FastAPI app
app = FastAPI(
    title="ScriptHut",
    description="Remote job management for Slurm, ECS, and AWS Batch",
    version="0.1.0",
    lifespan=lifespan,
)

# Templates
templates_path = Path(__file__).parent.parent.parent / "templates"
templates = Jinja2Templates(directory=str(templates_path))


def _backend_job_counts() -> dict[str, int]:
    """Count active (pending/submitted/running) jobs per backend from active runs."""
    counts: dict[str, int] = {}
    if state.run_manager:
        for run in state.run_manager.runs.values():
            for item in run.items:
                if item.status not in (RunItemStatus.COMPLETED, RunItemStatus.FAILED, RunItemStatus.DEP_FAILED):
                    counts[run.backend_name] = counts.get(run.backend_name, 0) + 1
    return counts


@dataclass
class JobView:
    """Lightweight view model for the jobs dashboard table."""
    slurm_job_id: str | None
    name: str
    user: str
    backend_name: str
    state: str
    source: str  # "run" or "external"
    partition: str
    cpus: int
    memory: str
    time_used: str
    time_limit: str
    submit_time: datetime | None
    start_time: datetime | None
    finish_time: datetime | None
    run_id: str | None
    task_id: str | None
    error: str | None
    cpu_efficiency: float | None
    max_rss: str | None
    workflow_name: str | None

    @property
    def state_class(self) -> str:
        """Return CSS class for state styling."""
        state_classes = {
            "running": "text-green-600",
            "pending": "text-gray-500",
            "submitted": "text-yellow-600",
            "completed": "text-blue-600",
            "failed": "text-red-600",
            "dep_failed": "text-orange-600",
        }
        return state_classes.get(self.state, "text-gray-500")

    @property
    def is_terminal(self) -> bool:
        """Check if job is in a terminal state."""
        return self.state in ("completed", "failed", "dep_failed")


def _collect_all_job_views() -> list[JobView]:
    """Collect all RunItems from all runs into JobViews for the dashboard."""
    views: list[JobView] = []
    if state.run_manager is None:
        return views

    user = state.filter_user or "unknown"

    # Active runs (managed by RunManager)
    for run in state.run_manager.runs.values():
        for item in run.items:
            views.append(JobView(
                slurm_job_id=item.slurm_job_id,
                name=item.task.name,
                user=user,
                backend_name=run.backend_name,
                state=item.status.value,
                source="run" if run.workflow_name != "_default" else "external",
                partition=item.task.partition,
                cpus=item.task.cpus,
                memory=item.task.memory,
                time_used="",
                time_limit=item.task.time_limit,
                submit_time=item.submitted_at,
                start_time=item.started_at,
                finish_time=item.finished_at,
                run_id=run.id,
                task_id=item.task.id,
                error=item.error,
                cpu_efficiency=item.cpu_efficiency,
                max_rss=item.max_rss,
                workflow_name=run.workflow_name,
            ))

    # Weekly default runs (external jobs from storage)
    if state.run_storage:
        for wf_name in state.run_storage.list_workflows():
            if not wf_name.startswith("_default_"):
                continue
            for run in state.run_storage.load_runs_for_workflow(wf_name):
                for item in run.items:
                    views.append(JobView(
                        slurm_job_id=item.slurm_job_id,
                        name=item.task.name,
                        user=user,
                        backend_name=run.backend_name,
                        state=item.status.value,
                        source="external",
                        partition=item.task.partition,
                        cpus=item.task.cpus,
                        memory=item.task.memory,
                        time_used="",
                        time_limit=item.task.time_limit,
                        submit_time=item.submitted_at,
                        start_time=item.started_at,
                        finish_time=item.finished_at,
                        run_id=run.id,
                        task_id=item.task.id,
                        error=item.error,
                        cpu_efficiency=item.cpu_efficiency,
                        max_rss=item.max_rss,
                        workflow_name="_default",
                    ))

    views.sort(key=lambda v: v.submit_time or datetime.min, reverse=True)
    return views


def _apply_job_filters(job_views: list[JobView]) -> list[JobView]:
    """Apply active filters (user filter, live view) to job views."""
    if state.filter_enabled and state.filter_user:
        job_views = [j for j in job_views if j.user == state.filter_user]
    if state.live_view:
        cutoff = datetime.now() - timedelta(hours=12)
        job_views = [
            j for j in job_views
            if not (j.is_terminal and j.finish_time and j.finish_time < cutoff)
        ]
    return job_views


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    """Main page with unified job list."""
    job_views = _apply_job_filters(_collect_all_job_views())

    poll_interval = state.config.settings.poll_interval if state.config else 60

    return templates.TemplateResponse(
        "base.html",
        {
            "request": request,
            "job_views": job_views,
            "backends": state.backends,
            "backend_job_counts": _backend_job_counts(),
            "sources": state.source_statuses,
            "status": ConnectionStatus(
                connected=state.any_connected,
                host=", ".join(c.status.host for c in state.backends.values() if c.status.connected),
                last_poll=max(
                    (c.status.last_poll for c in state.backends.values() if c.status.last_poll),
                    default=None,
                ),
            ),
            "poll_interval": poll_interval,
            "poll_remaining": state.seconds_until_next_poll,
            "filter_enabled": state.filter_enabled,
            "filter_user": state.filter_user,
            "live_view": state.live_view,
        },
    )


@app.get("/jobs", response_class=HTMLResponse)
async def jobs_partial(request: Request) -> HTMLResponse:
    """HTMX partial for unified job table."""
    job_views = _apply_job_filters(_collect_all_job_views())

    return templates.TemplateResponse(
        "jobs.html",
        {
            "request": request,
            "job_views": job_views,
            "backends": state.backends,
            "status": ConnectionStatus(
                connected=state.any_connected,
                host=", ".join(c.status.host for c in state.backends.values() if c.status.connected),
            ),
            "filter_enabled": state.filter_enabled,
            "filter_user": state.filter_user,
            "live_view": state.live_view,
        },
    )


@app.get("/jobs/stream")
async def jobs_stream(request: Request) -> EventSourceResponse:
    """SSE endpoint for live job updates on the dashboard."""

    async def event_generator() -> AsyncGenerator[dict[str, Any], None]:
        while True:
            changed = await state.wait_for_poll()
            if state._shutdown_event.is_set() or await request.is_disconnected():
                break

            if changed:
                job_views = _apply_job_filters(_collect_all_job_views())

                html = templates.get_template("jobs.html").render(
                    {
                        "request": request,
                        "job_views": job_views,
                        "backends": state.backends,
                        "status": ConnectionStatus(
                            connected=state.any_connected,
                            host=", ".join(c.status.host for c in state.backends.values() if c.status.connected),
                        ),
                        "filter_enabled": state.filter_enabled,
                        "filter_user": state.filter_user,
                        "live_view": state.live_view,
                    }
                )
                yield {"event": "jobs-update", "data": html}

                backends_html = templates.get_template("backends_status.html").render(
                    {"request": request, "backends": state.backends, "backend_job_counts": _backend_job_counts()}
                )
                yield {"event": "backends-update", "data": backends_html}
            else:
                yield {"comment": "keepalive"}

    return EventSourceResponse(event_generator())


@app.get("/runs/stream")
async def runs_stream(request: Request) -> EventSourceResponse:
    """SSE endpoint for live run list updates."""

    async def event_generator() -> AsyncGenerator[dict[str, Any], None]:
        while True:
            changed = await state.wait_for_poll()
            if state._shutdown_event.is_set() or await request.is_disconnected():
                break

            if changed:
                runs = state.run_manager.get_all_runs() if state.run_manager else []
                html = templates.get_template("runs_list.html").render(
                    {"request": request, "runs": runs}
                )
                yield {"event": "runs-update", "data": html}
            else:
                yield {"comment": "keepalive"}

    return EventSourceResponse(event_generator())


@app.post("/poll")
async def force_poll() -> dict[str, str]:
    """Trigger an immediate poll of all backends."""
    state._force_poll_event.set()
    return {"status": "polling"}


@app.get("/ping")
async def ping() -> dict[str, str]:
    """Simple ping endpoint for debugging - no dependencies."""
    return {"status": "pong"}


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


@app.get("/sources")
async def list_sources() -> dict[str, Any]:
    """List all configured sources and their statuses."""
    return {
        name: {
            "cloned": ss.cloned,
            "path": str(ss.path),
            "branch": ss.branch,
            "last_commit": ss.last_commit,
            "error": ss.error,
        }
        for name, ss in state.source_statuses.items()
    }


@app.post("/sources/{name}/sync")
async def sync_source(name: str) -> dict[str, Any]:
    """Trigger a sync for a specific source."""
    if state.source_manager is None:
        return {"error": "Source manager not initialized"}

    try:
        status = await state.source_manager.sync_source(name)
        state.source_statuses[name] = status
        return {
            "name": name,
            "cloned": status.cloned,
            "last_commit": status.last_commit,
            "error": status.error,
        }
    except ValueError as e:
        return {"error": str(e)}


@app.post("/filter/toggle", response_class=HTMLResponse)
async def toggle_filter(request: Request) -> HTMLResponse:
    """Toggle the user filter on/off and trigger immediate refresh."""
    state.filter_enabled = not state.filter_enabled

    filter_user = state.filter_user if state.filter_enabled else None
    tasks = [
        poll_backend(backend_state, filter_user=filter_user)
        for backend_state in state.backends.values()
        if backend_state.backend is not None
    ]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    return await jobs_partial(request)


@app.post("/live-view/toggle", response_class=HTMLResponse)
async def toggle_live_view(request: Request) -> HTMLResponse:
    """Toggle live view (hide old completed jobs)."""
    state.live_view = not state.live_view
    return await jobs_partial(request)


@app.get("/filter/status")
async def filter_status() -> dict[str, Any]:
    """Get current filter status."""
    return {
        "enabled": state.filter_enabled,
        "user": state.filter_user,
    }


@app.post("/jobs/{slurm_job_id}/cancel", response_class=HTMLResponse)
async def cancel_external_job(request: Request, slurm_job_id: str) -> HTMLResponse:
    """Cancel an external Slurm job via scancel."""
    for bs in state.backends.values():
        for job in bs.jobs:
            if job.job_id == slurm_job_id:
                if bs.ssh_client:
                    await bs.ssh_client.run_command(f"scancel {slurm_job_id}")
                # Update in-memory state so the UI reflects the change immediately
                job.state = JobState.CANCELLED
                # Update storage
                if state.run_storage:
                    state.run_storage.add_external_job(
                        backend_name=bs.name,
                        slurm_job_id=slurm_job_id,
                        name=job.name,
                        user=job.user,
                        state="failed",
                        submit_time=job.submit_time,
                        start_time=job.start_time,
                        finish_time=datetime.now(),
                    )
                    state.run_storage.save_if_dirty(
                        state.run_manager.runs if state.run_manager else {}
                    )
                return await jobs_partial(request)
    return await jobs_partial(request)


@app.delete("/jobs/{slurm_job_id}", response_class=HTMLResponse)
async def delete_external_job(request: Request, slurm_job_id: str) -> HTMLResponse:
    """Remove a completed external job from history."""
    if state.run_storage:
        state.run_storage.remove_external_job(slurm_job_id)
    return await jobs_partial(request)


# Workflows and Runs Routes


@app.get("/workflows")
async def list_workflows() -> dict[str, Any]:
    """List all configured workflows."""
    if state.config is None:
        return {"workflows": []}

    return {
        "workflows": [
            {
                "name": wf.name,
                "backend": wf.backend,
                "description": wf.description,
                "max_concurrent": wf.max_concurrent,
                "has_git": wf.git is not None,
            }
            for wf in state.config.workflows
        ]
    }


@app.post("/workflows/{name}/run", response_model=None)
async def run_workflow(name: str):
    """Trigger a workflow to create a new run."""
    if state.run_manager is None:
        return JSONResponse(
            status_code=500,
            content={"error": "Run manager not initialized"},
        )

    try:
        run = await state.run_manager.create_run(name)
        return {
            "run_id": run.id,
            "workflow_name": run.workflow_name,
            "backend_name": run.backend_name,
            "task_count": len(run.items),
            "status": run.status.value,
        }
    except Exception as e:
        logger.error(f"Failed to create run for workflow '{name}': {e}")
        return JSONResponse(
            status_code=422,
            content={"error": str(e)},
        )


@app.get("/workflows/{name}/dry-run", response_class=HTMLResponse)
async def dry_run_workflow(request: Request, name: str) -> HTMLResponse:
    """Dry run a workflow - show what would be submitted without creating a run."""
    if state.run_manager is None:
        return templates.TemplateResponse(
            "dry_run.html",
            {"request": request, "error": "Run manager not initialized", "result": None},
        )

    try:
        result = await state.run_manager.dry_run(name)
        return templates.TemplateResponse(
            "dry_run.html",
            {"request": request, "result": result, "error": None},
        )
    except ValueError as e:
        return templates.TemplateResponse(
            "dry_run.html",
            {"request": request, "error": str(e), "result": None},
        )

# Project Routes


@app.get("/projects/{name}/workflows")
async def list_project_workflows(name: str) -> dict[str, Any]:
    """Discover sflow.json files in a project repo."""
    if state.run_manager is None:
        return {"error": "Run manager not initialized"}

    try:
        paths = await state.run_manager.discover_workflows(name)
        return {"project": name, "workflows": paths}
    except ValueError as e:
        return {"error": str(e)}


@app.post("/projects/{name}/run", response_model=None)
async def run_project_workflow(name: str, workflow: str):
    """Run a sflow.json workflow from a project."""
    if state.run_manager is None:
        return JSONResponse(
            status_code=500,
            content={"error": "Run manager not initialized"},
        )

    try:
        run = await state.run_manager.create_run_from_project(
            name, workflow
        )
        return {
            "run_id": run.id,
            "workflow_name": run.workflow_name,
            "backend_name": run.backend_name,
            "task_count": len(run.items),
            "status": run.status.value,
        }
    except Exception as e:
        logger.error(f"Failed to create run for project '{name}': {e}")
        return JSONResponse(
            status_code=422,
            content={"error": str(e)},
        )


@app.get("/runs", response_class=HTMLResponse)
async def runs_page(request: Request) -> HTMLResponse:
    """Page listing all runs."""
    runs = state.run_manager.get_all_runs() if state.run_manager else []
    workflows = state.config.workflows if state.config else []
    projects = state.config.projects if state.config else []

    # Discover workflows for each project
    project_workflows: dict[str, list[str]] = {}
    if state.run_manager:
        for project in projects:
            try:
                paths = await state.run_manager.discover_workflows(
                    project.name
                )
                project_workflows[project.name] = paths
            except ValueError:
                project_workflows[project.name] = []

    return templates.TemplateResponse(
        "runs.html",
        {
            "request": request,
            "runs": runs,
            "workflows": workflows,
            "projects": projects,
            "project_workflows": project_workflows,
        },
    )


@app.get("/runs/list")
async def list_runs() -> dict[str, Any]:
    """List all runs (JSON)."""
    if state.run_manager is None:
        return {"runs": []}

    return {
        "runs": [
            {
                "id": r.id,
                "workflow_name": r.workflow_name,
                "backend_name": r.backend_name,
                "created_at": r.created_at.isoformat(),
                "status": r.status.value,
                "progress": r.progress,
                "task_count": len(r.items),
            }
            for r in state.run_manager.get_all_runs()
        ]
    }


def _compute_gantt_data(run: Run) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Compute layout data for Gantt chart bars."""
    now = datetime.now()

    time_origin = run.created_at

    # Determine the end of the time axis:
    # - If any task is still running/submitted, use `now` so the chart stays live.
    # - Otherwise, use the latest timestamp across all items so finished runs
    #   don't show a long blank tail stretching to the current time.
    has_active = any(
        item.status in (RunItemStatus.RUNNING, RunItemStatus.SUBMITTED)
        for item in run.items
    )
    time_end = now if has_active else time_origin

    for item in run.items:
        for ts in (item.finished_at, item.started_at, item.submitted_at):
            if ts and ts > time_end:
                time_end = ts

    total_span = (time_end - time_origin).total_seconds()
    if total_span <= 0:
        total_span = 1

    markers: list[dict[str, Any]] = []
    nice_intervals = [1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 7200, 14400, 28800, 86400]
    interval = nice_intervals[-1]
    for ni in nice_intervals:
        if total_span / ni <= 10:
            interval = ni
            break

    t = 0.0
    while t <= total_span:
        pct = (t / total_span) * 100
        if t < 60:
            label = f"+{int(t)}s"
        elif t < 3600:
            label = f"+{int(t // 60)}m"
        else:
            h = int(t // 3600)
            m = int((t % 3600) // 60)
            label = f"+{h}h{m:02d}m" if m else f"+{h}h"
        markers.append({"pct": pct, "label": label})
        t += interval

    gantt_items: list[dict[str, Any]] = []
    for item in run.items:
        entry: dict[str, Any] = {
            "task_id": item.task.id,
            "name": item.task.name,
            "status": item.status.value,
            "deps": item.task.dependencies,
            "has_bar": False,
            "wait_left": 0,
            "wait_width": 0,
            "run_left": 0,
            "run_width": 0,
            "bar_start": 0,
            "bar_end": 0,
        }

        if item.submitted_at:
            submitted_offset = max(0, (item.submitted_at - time_origin).total_seconds())

            if item.started_at:
                started_offset = max(submitted_offset, (item.started_at - time_origin).total_seconds())
                entry["wait_left"] = (submitted_offset / total_span) * 100
                entry["wait_width"] = ((started_offset - submitted_offset) / total_span) * 100

                end_ts = item.finished_at or now
                end_offset = max(started_offset, (end_ts - time_origin).total_seconds())
                entry["run_left"] = (started_offset / total_span) * 100
                entry["run_width"] = ((end_offset - started_offset) / total_span) * 100
                entry["has_bar"] = True
                entry["bar_start"] = entry["wait_left"]
                entry["bar_end"] = entry["run_left"] + entry["run_width"]
            else:
                wait_end_ts = item.finished_at or now
                wait_end = (wait_end_ts - time_origin).total_seconds()
                entry["wait_left"] = (submitted_offset / total_span) * 100
                entry["wait_width"] = ((wait_end - submitted_offset) / total_span) * 100
                entry["has_bar"] = True
                entry["bar_start"] = entry["wait_left"]
                entry["bar_end"] = entry["wait_left"] + entry["wait_width"]

        gantt_items.append(entry)

    return gantt_items, markers


@app.get("/runs/{run_id}", response_class=HTMLResponse)
async def run_detail_page(request: Request, run_id: str) -> HTMLResponse:
    """Page showing run detail."""
    run = state.run_manager.get_run(run_id) if state.run_manager else None

    if run is None:
        return templates.TemplateResponse(
            "run_detail.html",
            {"request": request, "run": None, "error": "Run not found", "gantt_items": [], "markers": []},
        )

    gantt_items, markers = _compute_gantt_data(run)

    return templates.TemplateResponse(
        "run_detail.html",
        {
            "request": request,
            "run": run,
            "error": None,
            "gantt_items": gantt_items,
            "markers": markers,
        },
    )


@app.get("/runs/{run_id}/info", response_class=HTMLResponse)
async def run_info_partial(request: Request, run_id: str) -> HTMLResponse:
    """HTMX partial for run info (progress bar, status, counts)."""
    run = state.run_manager.get_run(run_id) if state.run_manager else None

    return templates.TemplateResponse(
        "run_info.html",
        {"request": request, "run": run},
    )


@app.get("/runs/{run_id}/items", response_class=HTMLResponse)
async def run_items_partial(request: Request, run_id: str) -> HTMLResponse:
    """HTMX partial for run items table."""
    run = state.run_manager.get_run(run_id) if state.run_manager else None

    return templates.TemplateResponse(
        "run_items.html",
        {"request": request, "run": run},
    )


@app.get("/runs/{run_id}/gantt", response_class=HTMLResponse)
async def run_gantt_partial(request: Request, run_id: str) -> HTMLResponse:
    """HTMX partial for run Gantt chart."""
    run = state.run_manager.get_run(run_id) if state.run_manager else None

    gantt_items: list[dict[str, Any]] = []
    markers: list[dict[str, Any]] = []
    if run:
        gantt_items, markers = _compute_gantt_data(run)

    return templates.TemplateResponse(
        "run_gantt.html",
        {"request": request, "run": run, "gantt_items": gantt_items, "markers": markers},
    )


@app.get("/runs/{run_id}/events")
async def run_events(request: Request, run_id: str) -> EventSourceResponse:
    """SSE stream for real-time run updates."""

    async def event_generator():
        if not state.run_manager:
            return

        while True:
            changed = await state.run_manager.wait_for_update(run_id)

            if state._shutdown_event.is_set():
                return

            run = state.run_manager.get_run(run_id)
            if run is None:
                return

            if changed:
                info_html = templates.get_template("run_info.html").render(
                    {"request": request, "run": run}
                )
                items_html = templates.get_template("run_items.html").render(
                    {"request": request, "run": run}
                )
                gantt_items, markers = _compute_gantt_data(run)
                gantt_html = templates.get_template("run_gantt.html").render(
                    {"request": request, "run": run, "gantt_items": gantt_items, "markers": markers}
                )
                yield {"event": "info-update", "data": info_html}
                yield {"event": "items-update", "data": items_html}
                yield {"event": "gantt-update", "data": gantt_html}
                sidebar_html = templates.get_template("run_sidebar.html").render(
                    {"request": request, "run": run}
                )
                yield {"event": "sidebar-update", "data": sidebar_html}
            else:
                yield {"comment": "keepalive"}

    return EventSourceResponse(event_generator())


@app.post("/runs/{run_id}/cancel")
async def cancel_run(run_id: str) -> dict[str, Any]:
    """Cancel all pending and running items in a run."""
    if state.run_manager is None:
        return {"error": "Run manager not initialized"}

    success = await state.run_manager.cancel_run(run_id)
    if success:
        return {"status": "cancelled", "run_id": run_id}
    else:
        return {"error": "Run not found"}


@app.delete("/runs/{run_id}")
async def delete_run(run_id: str) -> Response:
    """Delete a terminal run (completed, failed, or cancelled)."""
    if state.run_manager is None:
        return Response(status_code=400)

    success = state.run_manager.delete_run(run_id)
    if success:
        return Response(status_code=200)
    else:
        return Response(status_code=404)


@app.get("/runs/{run_id}/tasks/{task_id}/script", response_class=HTMLResponse)
async def view_task_script(request: Request, run_id: str, task_id: str) -> HTMLResponse:
    """View the sbatch script used to submit a task."""
    if state.run_manager is None:
        return templates.TemplateResponse(
            "log_viewer.html",
            {"request": request, "title": "Script", "content": None, "error": "Run manager not initialized"},
        )

    run = state.run_manager.get_run(run_id)
    if run is None:
        return templates.TemplateResponse(
            "log_viewer.html",
            {"request": request, "title": "Script", "content": None, "error": f"Run '{run_id}' not found"},
        )

    item = run.get_item_by_task_id(task_id)
    if item is None:
        return templates.TemplateResponse(
            "log_viewer.html",
            {"request": request, "title": "Script", "content": None, "error": f"Task '{task_id}' not found"},
        )

    script = item.sbatch_script
    if script is None:
        log_dir = run.log_dir
        backend_state = state.backends.get(run.backend_name)
        if backend_state and backend_state.ssh_client and log_dir.startswith("~"):
            try:
                stdout, _, _ = await backend_state.ssh_client.run_command("echo $HOME")
                log_dir = log_dir.replace("~", stdout.strip(), 1)
            except Exception:
                pass
        env_vars, extra_init = state.run_manager._resolve_environment(item.task)
        script = item.task.to_sbatch_script(
            run.id, log_dir, account=run.account, login_shell=run.login_shell,
            env_vars=env_vars, extra_init=extra_init,
        )

    return templates.TemplateResponse(
        "log_viewer.html",
        {
            "request": request,
            "title": f"Script: {item.task.name}",
            "content": script,
            "error": None,
            "run_id": run_id,
            "task_id": task_id,
            "task_name": item.task.name,
            "content_type": "script",
            "run_items": run.items,
        },
    )


@app.get("/runs/{run_id}/tasks/{task_id}/detail/{detail_type}")
async def get_task_detail(
    run_id: str, task_id: str, detail_type: str, tail: int | None = None
) -> JSONResponse:
    """Get task detail content as JSON for inline display in the run detail page."""
    if state.run_manager is None:
        return JSONResponse({"error": "Run manager not initialized", "content": None, "task_name": "", "path": None})

    run = state.run_manager.get_run(run_id)
    if run is None:
        return JSONResponse({"error": f"Run '{run_id}' not found", "content": None, "task_name": "", "path": None})

    item = run.get_item_by_task_id(task_id)
    if item is None:
        return JSONResponse({"error": f"Task '{task_id}' not found", "content": None, "task_name": "", "path": None})

    content: str | None = None
    error: str | None = None
    path: str | None = None

    if detail_type in ("output", "error"):
        content, error = await state.run_manager.fetch_log_file(
            run_id, task_id, detail_type, tail_lines=tail
        )
        if detail_type == "output":
            path = item.task.get_output_path(run.id, run.log_dir)
        else:
            path = item.task.get_error_path(run.id, run.log_dir)
    elif detail_type == "script":
        content = item.sbatch_script
        if content is None:
            log_dir = run.log_dir
            backend_state = state.backends.get(run.backend_name)
            if backend_state and backend_state.ssh_client and log_dir.startswith("~"):
                try:
                    stdout, _, _ = await backend_state.ssh_client.run_command("echo $HOME")
                    log_dir = log_dir.replace("~", stdout.strip(), 1)
                except Exception:
                    pass
            env_vars, extra_init = state.run_manager._resolve_environment(item.task)
            content = item.task.to_sbatch_script(
                run.id, log_dir, account=run.account, login_shell=run.login_shell,
                env_vars=env_vars, extra_init=extra_init,
            )
    elif detail_type == "json":
        task_data: dict[str, Any] = {
            "id": item.task.id,
            "name": item.task.name,
            "command": item.task.command,
            "working_dir": item.task.working_dir,
            "partition": item.task.partition,
            "cpus": item.task.cpus,
            "memory": item.task.memory,
            "time_limit": item.task.time_limit,
            "dependencies": item.task.dependencies,
            "environment": item.task.environment,
            "generates_source": item.task.generates_source,
            "output_file": item.task.output_file,
            "error_file": item.task.error_file,
        }
        item_data: dict[str, Any] = {
            "task": task_data,
            "status": item.status.value,
            "slurm_job_id": item.slurm_job_id,
            "submitted_at": item.submitted_at.isoformat() if item.submitted_at else None,
            "started_at": item.started_at.isoformat() if item.started_at else None,
            "finished_at": item.finished_at.isoformat() if item.finished_at else None,
            "error": item.error,
            "cpu_efficiency": item.cpu_efficiency,
            "max_rss": item.max_rss,
        }
        content = json.dumps(item_data, indent=2)
    else:
        error = f"Unknown detail type: {detail_type}"

    return JSONResponse({
        "content": content,
        "error": error,
        "task_name": item.task.name,
        "task_id": task_id,
        "path": path,
    })


@app.get("/runs/{run_id}/tasks/{task_id}/json", response_class=HTMLResponse)
async def view_task_json(request: Request, run_id: str, task_id: str) -> HTMLResponse:
    """View the JSON definition of a task."""
    if state.run_manager is None:
        return templates.TemplateResponse(
            "log_viewer.html",
            {"request": request, "title": "JSON", "content": None, "error": "Run manager not initialized"},
        )

    run = state.run_manager.get_run(run_id)
    if run is None:
        return templates.TemplateResponse(
            "log_viewer.html",
            {"request": request, "title": "JSON", "content": None, "error": f"Run '{run_id}' not found"},
        )

    item = run.get_item_by_task_id(task_id)
    if item is None:
        return templates.TemplateResponse(
            "log_viewer.html",
            {"request": request, "title": "JSON", "content": None, "error": f"Task '{task_id}' not found"},
        )

    task_data: dict[str, Any] = {
        "id": item.task.id,
        "name": item.task.name,
        "command": item.task.command,
        "working_dir": item.task.working_dir,
        "partition": item.task.partition,
        "cpus": item.task.cpus,
        "memory": item.task.memory,
        "time_limit": item.task.time_limit,
        "dependencies": item.task.dependencies,
        "environment": item.task.environment,
        "generates_source": item.task.generates_source,
        "output_file": item.task.output_file,
        "error_file": item.task.error_file,
    }
    item_data: dict[str, Any] = {
        "task": task_data,
        "status": item.status.value,
        "slurm_job_id": item.slurm_job_id,
        "submitted_at": item.submitted_at.isoformat() if item.submitted_at else None,
        "started_at": item.started_at.isoformat() if item.started_at else None,
        "finished_at": item.finished_at.isoformat() if item.finished_at else None,
        "error": item.error,
        "cpu_efficiency": item.cpu_efficiency,
        "max_rss": item.max_rss,
    }
    content = json.dumps(item_data, indent=2)

    return templates.TemplateResponse(
        "log_viewer.html",
        {
            "request": request,
            "title": f"JSON: {item.task.name}",
            "content": content,
            "error": None,
            "run_id": run_id,
            "task_id": task_id,
            "task_name": item.task.name,
            "content_type": "json",
            "run_items": run.items,
        },
    )


@app.get("/runs/{run_id}/tasks/{task_id}/logs/{log_type}", response_class=HTMLResponse)
async def view_task_log(
    request: Request,
    run_id: str,
    task_id: str,
    log_type: str,
    tail: int | None = None,
) -> HTMLResponse:
    """View a log file for a task (fetched over SSH)."""
    if state.run_manager is None:
        return templates.TemplateResponse(
            "log_viewer.html",
            {"request": request, "title": "Log", "content": None, "error": "Run manager not initialized"},
        )

    content, error = await state.run_manager.fetch_log_file(
        run_id, task_id, log_type, tail_lines=tail
    )

    run = state.run_manager.get_run(run_id)
    task_name = ""
    log_path = ""
    if run:
        item = run.get_item_by_task_id(task_id)
        if item:
            task_name = item.task.name
            if log_type == "output":
                log_path = item.task.get_output_path(run.id, run.log_dir)
            else:
                log_path = item.task.get_error_path(run.id, run.log_dir)

    title = f"{'Output' if log_type == 'output' else 'Error'} Log: {task_name}"

    return templates.TemplateResponse(
        "log_viewer.html",
        {
            "request": request,
            "title": title,
            "content": content,
            "error": error,
            "run_id": run_id,
            "task_id": task_id,
            "task_name": task_name,
            "log_type": log_type,
            "log_path": log_path,
            "content_type": "log",
            "tail": tail,
            "run_items": run.items if run else [],
        },
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="ScriptHut - Remote job management",
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


def run() -> None:
    """Run the application with uvicorn."""
    global _config_path

    args = parse_args()
    _config_path = args.config

    config = load_config(_config_path)

    host = args.host or config.settings.server_host
    port = args.port or config.settings.server_port

    uvicorn.run(
        "scripthut.main:app",
        host=host,
        port=port,
        reload=False,
    )


if __name__ == "__main__":
    run()
