"""FastAPI application for ScriptHut."""

import argparse
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncGenerator

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse

from scripthut.backends.slurm import SlurmBackend
from scripthut.config import load_config, set_config
from scripthut.config_schema import ScriptHutConfig, SlurmClusterConfig
from scripthut.history import JobHistoryManager
from scripthut.models import ConnectionStatus, JobState, SlurmJob
from scripthut.queues import Queue, QueueManager
from scripthut.sources.git import GitSourceManager, SourceStatus
from scripthut.ssh.client import SSHClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class ClusterState:
    """State for a single cluster."""

    name: str
    cluster_type: str
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
    clusters: dict[str, ClusterState] = field(default_factory=dict)
    source_manager: GitSourceManager | None = None
    source_statuses: dict[str, SourceStatus] = field(default_factory=dict)
    queue_manager: QueueManager | None = None
    history_manager: JobHistoryManager | None = None
    filter_enabled: bool = False
    filter_user: str | None = None
    _polling_task: asyncio.Task[None] | None = None
    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def all_jobs(self) -> list[tuple[str, SlurmJob]]:
        """Get all jobs from all clusters with cluster name."""
        jobs = []
        for cluster_name, cluster_state in self.clusters.items():
            for job in cluster_state.jobs:
                jobs.append((cluster_name, job))
        return jobs

    def get_filtered_jobs(self) -> list[tuple[str, SlurmJob]]:
        """Get jobs - filtering now happens server-side during polling."""
        # Server-side filtering in poll_cluster makes this more efficient
        # Jobs are already filtered when filter_enabled is True
        return self.all_jobs

    @property
    def any_connected(self) -> bool:
        """Check if any cluster is connected."""
        return any(c.status.connected for c in self.clusters.values())


state = AppState()

# CLI argument for config path (set by run())
_config_path: Path | None = None


async def init_cluster(cluster_config: SlurmClusterConfig) -> ClusterState:
    """Initialize a Slurm cluster connection."""
    cluster_state = ClusterState(
        name=cluster_config.name,
        cluster_type="slurm",
        status=ConnectionStatus(connected=False, host=cluster_config.ssh.host),
    )

    ssh_client = SSHClient(
        host=cluster_config.ssh.host,
        user=cluster_config.ssh.user,
        key_path=cluster_config.ssh.key_path_resolved,
        port=cluster_config.ssh.port,
        cert_path=cluster_config.ssh.cert_path_resolved,
        known_hosts=cluster_config.ssh.known_hosts_resolved,
    )

    try:
        await ssh_client.connect()
        cluster_state.ssh_client = ssh_client
        cluster_state.backend = SlurmBackend(ssh_client)
        cluster_state.status = ConnectionStatus(
            connected=True,
            host=cluster_config.ssh.host,
        )
        logger.info(f"Connected to cluster '{cluster_config.name}' ({cluster_config.ssh.host})")
    except Exception as e:
        logger.error(f"Failed to connect to cluster '{cluster_config.name}': {e}")
        cluster_state.status = ConnectionStatus(
            connected=False,
            host=cluster_config.ssh.host,
            error=str(e),
        )

    return cluster_state


async def poll_cluster(cluster_state: ClusterState, filter_user: str | None = None) -> None:
    """Poll jobs for a single cluster.

    Args:
        cluster_state: The cluster to poll.
        filter_user: If provided, only fetch jobs for this user (server-side filter).
    """
    if cluster_state.backend is None:
        return

    start_time = time.perf_counter()
    try:
        # Filter at the Slurm level for better performance
        jobs = await cluster_state.backend.get_jobs(user=filter_user)
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        cluster_state.jobs = jobs
        cluster_state.status = ConnectionStatus(
            connected=True,
            host=cluster_state.status.host,
            last_poll=datetime.now(),
            last_poll_duration_ms=duration_ms,
            job_count=len(jobs),
        )
        logger.debug(f"Polled {len(jobs)} jobs from '{cluster_state.name}' in {duration_ms}ms")

        # Update job history with polled jobs
        if state.history_manager:
            state.history_manager.update_from_slurm_poll(jobs, cluster_state.name)
    except Exception as e:
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        logger.error(f"Job polling failed for '{cluster_state.name}': {e}")
        cluster_state.status = ConnectionStatus(
            connected=False,
            host=cluster_state.status.host,
            last_poll=cluster_state.status.last_poll,
            last_poll_duration_ms=duration_ms,
            error=str(e),
        )


async def poll_jobs() -> None:
    """Background task to poll all clusters periodically."""
    if state.config is None:
        return

    interval = state.config.settings.poll_interval
    logger.info(f"Starting job polling (interval: {interval}s)")
    poll_count = 0

    while not state._shutdown_event.is_set():
        # Determine if we should filter by user (server-side for performance)
        filter_user = state.filter_user if state.filter_enabled else None

        # Poll all clusters in parallel
        tasks = [
            poll_cluster(cluster_state, filter_user=filter_user)
            for cluster_state in state.clusters.values()
            if cluster_state.backend is not None
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            total_jobs = sum(len(c.jobs) for c in state.clusters.values())
            logger.info(f"Polled {total_jobs} jobs from {len(tasks)} clusters (filter_user={filter_user})")

        # Update queue statuses based on polled jobs
        if state.queue_manager and state.queue_manager.get_active_queues():
            cluster_jobs: dict[str, list[tuple[str, JobState]]] = {}
            for name, cs in state.clusters.items():
                cluster_jobs[name] = [(job.job_id, job.state) for job in cs.jobs]
            await state.queue_manager.update_all_queues(cluster_jobs)

        # Save job history after each poll
        if state.history_manager:
            state.history_manager.save_if_dirty()

        # Cleanup old jobs once per hour (every ~60 polls at 60s interval)
        poll_count += 1
        if poll_count >= 60 and state.history_manager:
            state.history_manager.cleanup_old_jobs()
            poll_count = 0

        try:
            await asyncio.wait_for(
                state._shutdown_event.wait(),
                timeout=interval,
            )
            break  # Shutdown requested
        except asyncio.TimeoutError:
            pass  # Continue polling


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager - handles startup and shutdown."""
    # Load configuration
    config = load_config(_config_path)
    set_config(config)
    state.config = config

    logger.info(f"Loaded configuration with {len(config.clusters)} clusters, {len(config.sources)} sources")

    # Initialize filter settings
    state.filter_user = config.settings.filter_user
    state.filter_enabled = config.settings.filter_user is not None

    # Initialize source manager
    state.source_manager = GitSourceManager(config.settings.sources_cache_dir_resolved)
    for source in config.sources:
        state.source_manager.add_source(source)

    # Sync sources in background (don't block startup)
    if config.sources:
        asyncio.create_task(sync_sources_background())

    # Initialize clusters
    for cluster_config in config.slurm_clusters:
        cluster_state = await init_cluster(cluster_config)
        state.clusters[cluster_config.name] = cluster_state

    # TODO: Initialize ECS clusters when implemented
    for ecs_config in config.ecs_clusters:
        logger.warning(f"ECS cluster '{ecs_config.name}' configured but ECS backend not yet implemented")

    # Initialize job history manager
    state.history_manager = JobHistoryManager()
    state.history_manager.cleanup_old_jobs()  # Clean up on startup
    logger.info("Initialized job history manager")

    # Initialize queue manager
    ssh_clients = {
        name: cs.ssh_client
        for name, cs in state.clusters.items()
        if cs.ssh_client is not None
    }
    state.queue_manager = QueueManager(config, ssh_clients, history_manager=state.history_manager)
    logger.info(f"Initialized queue manager with {len(config.task_sources)} task sources")

    # Restore queues from history
    restored_count = state.queue_manager.restore_from_history()
    if restored_count > 0:
        logger.info(f"Restored {restored_count} queues from history")

    # Start background polling
    state._polling_task = asyncio.create_task(poll_jobs())

    yield

    # Shutdown
    logger.info("Shutting down...")
    state._shutdown_event.set()

    if state._polling_task:
        state._polling_task.cancel()
        try:
            await state._polling_task
        except asyncio.CancelledError:
            pass

    # Disconnect all clusters
    for cluster_state in state.clusters.values():
        if cluster_state.ssh_client:
            await cluster_state.ssh_client.disconnect()


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


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    """Main page with unified job list."""
    # Get unified jobs from history
    unified_jobs = []
    if state.history_manager:
        unified_jobs = state.history_manager.get_all_jobs()
        # Apply user filter if enabled
        if state.filter_enabled and state.filter_user:
            unified_jobs = [j for j in unified_jobs if j.user == state.filter_user]

    poll_interval = state.config.settings.poll_interval if state.config else 60

    return templates.TemplateResponse(
        "base.html",
        {
            "request": request,
            "unified_jobs": unified_jobs,
            "clusters": state.clusters,
            "sources": state.source_statuses,
            "status": ConnectionStatus(
                connected=state.any_connected,
                host=", ".join(c.status.host for c in state.clusters.values() if c.status.connected),
                last_poll=max(
                    (c.status.last_poll for c in state.clusters.values() if c.status.last_poll),
                    default=None,
                ),
            ),
            "poll_interval": poll_interval,
            "filter_enabled": state.filter_enabled,
            "filter_user": state.filter_user,
        },
    )


@app.get("/jobs", response_class=HTMLResponse)
async def jobs_partial(request: Request) -> HTMLResponse:
    """HTMX partial for unified job table."""
    unified_jobs = []
    if state.history_manager:
        unified_jobs = state.history_manager.get_all_jobs()
        if state.filter_enabled and state.filter_user:
            unified_jobs = [j for j in unified_jobs if j.user == state.filter_user]

    return templates.TemplateResponse(
        "jobs.html",
        {
            "request": request,
            "unified_jobs": unified_jobs,
            "clusters": state.clusters,
            "status": ConnectionStatus(
                connected=state.any_connected,
                host=", ".join(c.status.host for c in state.clusters.values() if c.status.connected),
            ),
            "filter_enabled": state.filter_enabled,
            "filter_user": state.filter_user,
        },
    )


@app.get("/jobs/stream")
async def jobs_stream(request: Request) -> EventSourceResponse:
    """SSE endpoint for live job updates."""

    async def event_generator() -> AsyncGenerator[dict[str, Any], None]:
        last_count = -1
        while True:
            if await request.is_disconnected():
                break

            # Send update when job count changes
            current_count = sum(len(c.jobs) for c in state.clusters.values())
            if current_count != last_count:
                last_count = current_count
                yield {
                    "event": "jobs-updated",
                    "data": str(current_count),
                }

            await asyncio.sleep(1)

    return EventSourceResponse(event_generator())


@app.get("/ping")
async def ping() -> dict[str, str]:
    """Simple ping endpoint for debugging - no dependencies."""
    return {"status": "pong"}


@app.get("/health")
async def health() -> dict[str, Any]:
    """Health check endpoint."""
    cluster_statuses = {
        name: {
            "connected": cs.status.connected,
            "host": cs.status.host,
            "job_count": len(cs.jobs),
            "last_poll": cs.status.last_poll.isoformat() if cs.status.last_poll else None,
            "error": cs.status.error,
        }
        for name, cs in state.clusters.items()
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
        "clusters": cluster_statuses,
        "sources": source_statuses,
        "total_jobs": sum(len(c.jobs) for c in state.clusters.values()),
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

    # Trigger immediate poll with new filter setting
    filter_user = state.filter_user if state.filter_enabled else None
    tasks = [
        poll_cluster(cluster_state, filter_user=filter_user)
        for cluster_state in state.clusters.values()
        if cluster_state.backend is not None
    ]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    # Return the updated jobs partial
    return await jobs_partial(request)


@app.get("/filter/status")
async def filter_status() -> dict[str, Any]:
    """Get current filter status."""
    return {
        "enabled": state.filter_enabled,
        "user": state.filter_user,
    }


# Task Sources and Queues Routes


@app.get("/task-sources")
async def list_task_sources() -> dict[str, Any]:
    """List all configured task sources."""
    if state.config is None:
        return {"task_sources": []}

    return {
        "task_sources": [
            {
                "name": ts.name,
                "cluster": ts.cluster,
                "description": ts.description,
                "max_concurrent": ts.max_concurrent,
            }
            for ts in state.config.task_sources
        ]
    }


@app.post("/task-sources/{name}/run")
async def run_task_source(name: str) -> dict[str, Any]:
    """Trigger a task source to create a new queue."""
    if state.queue_manager is None:
        return {"error": "Queue manager not initialized"}

    try:
        queue = await state.queue_manager.create_queue(name)
        return {
            "queue_id": queue.id,
            "source_name": queue.source_name,
            "cluster_name": queue.cluster_name,
            "task_count": len(queue.items),
            "status": queue.status.value,
        }
    except ValueError as e:
        return {"error": str(e)}


@app.get("/task-sources/{name}/dry-run", response_class=HTMLResponse)
async def dry_run_task_source(request: Request, name: str) -> HTMLResponse:
    """Dry run a task source - show what would be submitted without creating a queue."""
    if state.queue_manager is None:
        return templates.TemplateResponse(
            "dry_run.html",
            {"request": request, "error": "Queue manager not initialized", "result": None},
        )

    try:
        result = await state.queue_manager.dry_run(name)
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
    if state.queue_manager is None:
        return {"error": "Queue manager not initialized"}

    try:
        paths = await state.queue_manager.discover_workflows(name)
        return {"project": name, "workflows": paths}
    except ValueError as e:
        return {"error": str(e)}


@app.post("/projects/{name}/run")
async def run_project_workflow(name: str, workflow: str) -> dict[str, Any]:
    """Run a sflow.json workflow from a project."""
    if state.queue_manager is None:
        return {"error": "Queue manager not initialized"}

    try:
        queue = await state.queue_manager.create_queue_from_project(
            name, workflow
        )
        return {
            "queue_id": queue.id,
            "source_name": queue.source_name,
            "cluster_name": queue.cluster_name,
            "task_count": len(queue.items),
            "status": queue.status.value,
        }
    except ValueError as e:
        return {"error": str(e)}


@app.get("/queues", response_class=HTMLResponse)
async def queues_page(request: Request) -> HTMLResponse:
    """Page listing all queues."""
    queues = state.queue_manager.get_all_queues() if state.queue_manager else []
    task_sources = state.config.task_sources if state.config else []
    projects = state.config.projects if state.config else []

    # Discover workflows for each project
    project_workflows: dict[str, list[str]] = {}
    if state.queue_manager:
        for project in projects:
            try:
                paths = await state.queue_manager.discover_workflows(
                    project.name
                )
                project_workflows[project.name] = paths
            except ValueError:
                project_workflows[project.name] = []

    return templates.TemplateResponse(
        "queues.html",
        {
            "request": request,
            "queues": queues,
            "task_sources": task_sources,
            "projects": projects,
            "project_workflows": project_workflows,
        },
    )


@app.get("/queues/list")
async def list_queues() -> dict[str, Any]:
    """List all queues (JSON)."""
    if state.queue_manager is None:
        return {"queues": []}

    return {
        "queues": [
            {
                "id": q.id,
                "source_name": q.source_name,
                "cluster_name": q.cluster_name,
                "created_at": q.created_at.isoformat(),
                "status": q.status.value,
                "progress": q.progress,
                "task_count": len(q.items),
            }
            for q in state.queue_manager.get_all_queues()
        ]
    }


@app.get("/queues/{queue_id}", response_class=HTMLResponse)
async def queue_detail_page(request: Request, queue_id: str) -> HTMLResponse:
    """Page showing queue detail."""
    queue = state.queue_manager.get_queue(queue_id) if state.queue_manager else None

    if queue is None:
        return templates.TemplateResponse(
            "queue_detail.html",
            {"request": request, "queue": None, "error": "Queue not found"},
        )

    return templates.TemplateResponse(
        "queue_detail.html",
        {"request": request, "queue": queue, "error": None},
    )


@app.get("/queues/{queue_id}/info", response_class=HTMLResponse)
async def queue_info_partial(request: Request, queue_id: str) -> HTMLResponse:
    """HTMX partial for queue info (progress bar, status, counts)."""
    queue = state.queue_manager.get_queue(queue_id) if state.queue_manager else None

    return templates.TemplateResponse(
        "queue_info.html",
        {"request": request, "queue": queue},
    )


@app.get("/queues/{queue_id}/items", response_class=HTMLResponse)
async def queue_items_partial(request: Request, queue_id: str) -> HTMLResponse:
    """HTMX partial for queue items table."""
    queue = state.queue_manager.get_queue(queue_id) if state.queue_manager else None

    return templates.TemplateResponse(
        "queue_items.html",
        {"request": request, "queue": queue},
    )


@app.post("/queues/{queue_id}/cancel")
async def cancel_queue(queue_id: str) -> dict[str, Any]:
    """Cancel all pending and running items in a queue."""
    if state.queue_manager is None:
        return {"error": "Queue manager not initialized"}

    success = await state.queue_manager.cancel_queue(queue_id)
    if success:
        return {"status": "cancelled", "queue_id": queue_id}
    else:
        return {"error": "Queue not found"}


@app.get("/queues/{queue_id}/tasks/{task_id}/script", response_class=HTMLResponse)
async def view_task_script(request: Request, queue_id: str, task_id: str) -> HTMLResponse:
    """View the sbatch script used to submit a task."""
    if state.queue_manager is None:
        return templates.TemplateResponse(
            "log_viewer.html",
            {"request": request, "title": "Script", "content": None, "error": "Queue manager not initialized"},
        )

    queue = state.queue_manager.get_queue(queue_id)
    if queue is None:
        return templates.TemplateResponse(
            "log_viewer.html",
            {"request": request, "title": "Script", "content": None, "error": f"Queue '{queue_id}' not found"},
        )

    item = queue.get_item_by_task_id(task_id)
    if item is None:
        return templates.TemplateResponse(
            "log_viewer.html",
            {"request": request, "title": "Script", "content": None, "error": f"Task '{task_id}' not found"},
        )

    script = item.sbatch_script
    if script is None:
        # Generate the script if not stored (task hasn't been submitted yet)
        script = item.task.to_sbatch_script(queue.id, queue.log_dir)

    return templates.TemplateResponse(
        "log_viewer.html",
        {
            "request": request,
            "title": f"Script: {item.task.name}",
            "content": script,
            "error": None,
            "queue_id": queue_id,
            "task_id": task_id,
            "task_name": item.task.name,
            "content_type": "script",
        },
    )


@app.get("/queues/{queue_id}/tasks/{task_id}/logs/{log_type}", response_class=HTMLResponse)
async def view_task_log(
    request: Request,
    queue_id: str,
    task_id: str,
    log_type: str,
    tail: int | None = None,
) -> HTMLResponse:
    """View a log file for a task (fetched over SSH).

    Args:
        queue_id: The queue ID.
        task_id: The task ID.
        log_type: "output" for stdout, "error" for stderr.
        tail: If provided, only show last N lines.
    """
    if state.queue_manager is None:
        return templates.TemplateResponse(
            "log_viewer.html",
            {"request": request, "title": "Log", "content": None, "error": "Queue manager not initialized"},
        )

    content, error = await state.queue_manager.fetch_log_file(
        queue_id, task_id, log_type, tail_lines=tail
    )

    # Get task info for display
    queue = state.queue_manager.get_queue(queue_id)
    task_name = ""
    log_path = ""
    if queue:
        item = queue.get_item_by_task_id(task_id)
        if item:
            task_name = item.task.name
            if log_type == "output":
                log_path = item.task.get_output_path(queue.id, queue.log_dir)
            else:
                log_path = item.task.get_error_path(queue.id, queue.log_dir)

    title = f"{'Output' if log_type == 'output' else 'Error'} Log: {task_name}"

    return templates.TemplateResponse(
        "log_viewer.html",
        {
            "request": request,
            "title": title,
            "content": content,
            "error": error,
            "queue_id": queue_id,
            "task_id": task_id,
            "task_name": task_name,
            "log_type": log_type,
            "log_path": log_path,
            "content_type": "log",
            "tail": tail,
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

    # Load config to get server settings
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
