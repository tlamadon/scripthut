"""Versioned JSON API exposed under ``/api/v1`` for CLI and external clients.

These endpoints are the contract the CLI depends on.  Unlike the HTML
routes in ``main.py``, they always return JSON with a stable shape and
should not be reshaped to suit template rendering.

The router is built via ``make_api_router(state)`` so it can capture the
running ``AppState`` without forcing ``main.py`` and ``api.py`` into a
mutual import cycle.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, HTTPException

from scripthut.config_schema import (
    BatchBackendConfig,
    EC2BackendConfig,
    ECSBackendConfig,
    PBSBackendConfig,
    SlurmBackendConfig,
)
from scripthut.runs.models import Run, RunItemStatus

if TYPE_CHECKING:
    from scripthut.main import AppState
    from scripthut.runs.manager import RunManager

logger = logging.getLogger(__name__)


def _backend_kind(cfg: Any) -> str:
    """Map a BackendConfig instance to its short type name."""
    if isinstance(cfg, SlurmBackendConfig):
        return "slurm"
    if isinstance(cfg, PBSBackendConfig):
        return "pbs"
    if isinstance(cfg, BatchBackendConfig):
        return "batch"
    if isinstance(cfg, EC2BackendConfig):
        return "ec2"
    if isinstance(cfg, ECSBackendConfig):
        return "ecs"
    return "unknown"


def _run_summary(run: Run) -> dict[str, Any]:
    """Compact run representation used by list / submit responses."""
    completed, total = run.progress
    counts: dict[str, int] = {}
    for item in run.items:
        counts[item.status.value] = counts.get(item.status.value, 0) + 1
    submitted_count = sum(
        1 for item in run.items
        if item.status not in (RunItemStatus.PENDING,)
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


def make_api_router(state: AppState) -> APIRouter:
    """Build the ``/api/v1`` router bound to the given app state.

    Closure-based DI keeps ``api.py`` independent of ``main.py``'s import
    graph (no circular import) while still letting routes reach the live
    ``RunManager`` and storage held by the running server.
    """
    router = APIRouter(prefix="/api/v1")

    def _require_manager() -> RunManager:
        if state.run_manager is None:
            raise HTTPException(
                status_code=503, detail="Run manager not initialized"
            )
        return state.run_manager

    @router.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "config_loaded": state.config is not None,
            "config_error": state.config_error,
            "backends": [
                {"name": name, "type": bs.backend_type, "connected": bs.status.connected}
                for name, bs in state.backends.items()
            ],
        }

    @router.get("/backends")
    async def list_backends() -> dict[str, Any]:
        """List configured backends and their current connection status."""
        if state.config is None:
            return {"backends": []}
        result = []
        for cfg in state.config.backends:
            bs = state.backends.get(cfg.name)
            connected = bs.status.connected if bs else False
            backend_type = bs.backend_type if bs else _backend_kind(cfg)
            result.append({
                "name": cfg.name,
                "type": backend_type,
                "connected": connected,
                "max_concurrent": getattr(cfg, "max_concurrent", None),
            })
        return {"backends": result}

    def _source_summary(src: Any) -> dict[str, Any]:
        """Compact dict for a GitSourceConfig or PathSourceConfig.

        Hides type-specific fields under a discriminator so callers can
        format both variants without sniffing the class.
        """
        from scripthut.config_schema import GitSourceConfig, PathSourceConfig
        common: dict[str, Any] = {
            "name": src.name,
            "type": src.type,
            "description": getattr(src, "description", ""),
            "max_concurrent": getattr(src, "max_concurrent", None),
        }
        if isinstance(src, GitSourceConfig):
            common.update({"url": src.url, "branch": src.branch})
        elif isinstance(src, PathSourceConfig):
            common.update({"path": src.path, "backend": src.backend})
        return common

    @router.get("/sources")
    async def list_sources() -> dict[str, Any]:
        if state.config is None:
            return {"sources": []}
        return {"sources": [_source_summary(s) for s in state.config.sources]}

    @router.get("/sources/{name}")
    async def view_source(name: str) -> dict[str, Any]:
        """Source metadata plus the workflow files discovered for it.

        Git sources need to have been synced (cloned) for workflows to
        appear; if discovery fails, metadata is still returned and
        ``discover_error`` carries the reason.
        """
        if state.config is None:
            raise HTTPException(status_code=503, detail="Config not loaded")
        source = state.config.get_source(name)
        if source is None:
            raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        workflows: list[str] = []
        discover_error: str | None = None
        try:
            cached = state.source_workflows.get(name, [])
            workflows = [wf.filename for wf in cached]
        except Exception as e:
            logger.warning(f"workflow discovery failed for source '{name}': {e}")
            discover_error = str(e)
        return {
            **_source_summary(source),
            "workflows": workflows,
            "discover_error": discover_error,
        }

    @router.post("/tasks/run")
    async def run_adhoc_task(payload: dict) -> dict[str, Any]:
        """Submit a single ad-hoc task as a one-item run.

        ``payload`` must contain a ``task`` object matching the
        TaskDefinition JSON shape (id, name, command, plus any optional
        fields) and a ``backend`` name. Optional ``run_name`` overrides
        the synthetic ``_adhoc/<id>`` label used otherwise.
        """
        from scripthut.runs.models import TaskDefinition

        rm = _require_manager()
        task_dict = payload.get("task")
        backend = payload.get("backend")
        run_name = payload.get("run_name")
        if not isinstance(task_dict, dict) or not backend:
            raise HTTPException(
                status_code=422,
                detail="payload must contain 'task' (dict) and 'backend' (str)",
            )
        try:
            task = TaskDefinition.from_dict(task_dict)
        except (KeyError, ValueError) as e:
            raise HTTPException(status_code=422, detail=f"invalid task: {e}")
        try:
            run = await rm.create_adhoc_run(task, backend, run_name=run_name)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        except Exception as e:
            logger.error(f"Failed to create adhoc run: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        state.notify_poll()
        return _run_summary(run)

    @router.post("/sources/{name}/run")
    async def run_source_workflow_v1(
        name: str, workflow: str, backend: str | None = None,
    ) -> dict[str, Any]:
        """Submit a source workflow as a new run.

        ``workflow`` is the filename within the source (e.g. ``train.json``).
        ``backend`` falls back to the source's own ``backend`` field when
        omitted — required because ``create_run_from_source`` needs one.
        """
        if state.config is None:
            raise HTTPException(status_code=503, detail="Config not loaded")
        source = state.config.get_source(name)
        if source is None:
            raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        effective_backend = backend or getattr(source, "backend", None)
        if not effective_backend:
            raise HTTPException(
                status_code=422,
                detail=(
                    "No backend specified and source has no default backend. "
                    "Pass ?backend=<name>."
                ),
            )
        # Fetch the workflow JSON. Git sources need a refresh first so we
        # don't run against a stale clone.
        wf = None
        if state.source_manager and name in getattr(
            state.source_manager, "_sources", {},
        ):
            try:
                await state.source_manager.sync_source(name)
                workflows = state.source_manager.discover_workflows(name)
                state.source_workflows[name] = workflows
            except Exception as e:
                logger.warning(f"Failed to refresh source '{name}' before run: {e}")
        wf = next(
            (w for w in state.source_workflows.get(name, []) if w.filename == workflow),
            None,
        )
        if wf is None:
            raise HTTPException(
                status_code=404,
                detail=f"Workflow '{workflow}' not found in source '{name}'",
            )
        rm = _require_manager()
        try:
            run = await rm.create_run_from_source(
                name, workflow, wf.tasks_json, backend=effective_backend,
            )
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        except Exception as e:
            logger.error(f"Failed to create run for source '{name}': {e}")
            raise HTTPException(status_code=500, detail=str(e))
        state.notify_poll()
        return _run_summary(run)

    @router.get("/sources/{name}/workflows")
    async def list_source_workflows_v1(name: str) -> dict[str, Any]:
        if state.config is None or state.config.get_source(name) is None:
            raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        cached = state.source_workflows.get(name, [])
        return {
            "source": name,
            "workflows": [wf.filename for wf in cached],
        }

    async def _sync_one_source(name: str) -> dict[str, Any]:
        """Re-sync a single source and refresh its workflow cache.

        Git sources re-clone via ``source_manager``; path sources re-glob
        over SSH using ``main._discover_path_source_workflows`` (lazy
        imported to avoid a circular dep at module load).
        """
        from scripthut.config_schema import GitSourceConfig, PathSourceConfig
        if state.config is None:
            raise HTTPException(status_code=503, detail="Config not loaded")
        source = state.config.get_source(name)
        if source is None:
            raise HTTPException(
                status_code=404, detail=f"Source '{name}' not found",
            )

        result: dict[str, Any] = {
            "name": name, "type": source.type, "error": None,
        }
        if isinstance(source, GitSourceConfig):
            if state.source_manager is None:
                raise HTTPException(
                    status_code=503, detail="Source manager not initialized",
                )
            try:
                status = await state.source_manager.sync_source(name)
                state.source_statuses[name] = status
                result["cloned"] = status.cloned
                result["last_commit"] = status.last_commit
                if status.error:
                    result["error"] = status.error
                workflows = state.source_manager.discover_workflows(name)
                state.source_workflows[name] = workflows
                result["workflows"] = [wf.filename for wf in workflows]
            except Exception as e:
                logger.exception(f"Sync failed for git source '{name}'")
                result["error"] = str(e)
                result["workflows"] = [
                    wf.filename for wf in state.source_workflows.get(name, [])
                ]
        elif isinstance(source, PathSourceConfig):
            try:
                from scripthut.main import _discover_path_source_workflows
                workflows = await _discover_path_source_workflows(source)
                state.source_workflows[name] = workflows
                result["workflows"] = [wf.filename for wf in workflows]
            except Exception as e:
                logger.exception(f"Discovery failed for path source '{name}'")
                result["error"] = str(e)
                result["workflows"] = [
                    wf.filename for wf in state.source_workflows.get(name, [])
                ]
        return result

    @router.post("/sources/{name}/sync")
    async def sync_one_source_v1(name: str) -> dict[str, Any]:
        """Re-sync a single source and refresh its workflow cache.

        Returns the refreshed metadata (cloned status, commit, workflow
        filenames) plus an ``error`` field that is ``null`` on success.
        4xx is reserved for "source/config not found"; an individual
        sync failure surfaces as a 200 with ``error`` populated so
        callers can distinguish "doesn't exist" from "exists but broken".
        """
        return await _sync_one_source(name)

    @router.post("/sources/sync")
    async def sync_all_sources_v1() -> dict[str, Any]:
        """Re-sync every configured source. Errors are per-entry, not fatal."""
        if state.config is None:
            return {"sources": []}
        names = [s.name for s in state.config.sources]
        results = [await _sync_one_source(n) for n in names]
        return {"sources": results}

    @router.get("/runs")
    async def list_runs(limit: int | None = None, workflow: str | None = None) -> dict[str, Any]:
        rm = _require_manager()
        runs = rm.get_all_runs()
        if workflow:
            runs = [r for r in runs if r.workflow_name == workflow]
        runs = sorted(runs, key=lambda r: r.created_at, reverse=True)
        if limit is not None and limit > 0:
            runs = runs[:limit]
        return {"runs": [_run_summary(r) for r in runs]}

    @router.get("/runs/{run_id}")
    async def get_run(run_id: str) -> dict[str, Any]:
        rm = _require_manager()
        run = rm.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
        summary = _run_summary(run)
        summary["items"] = [item.to_dict() for item in run.items]
        summary["log_dir"] = run.log_dir
        summary["account"] = run.account
        summary["commit_hash"] = run.commit_hash
        summary["git_repo"] = run.git_repo
        summary["git_branch"] = run.git_branch
        return summary

    @router.post("/runs/{run_id}/cancel")
    async def cancel_run(run_id: str) -> dict[str, Any]:
        rm = _require_manager()
        cancelled = await rm.cancel_run(run_id)
        if not cancelled:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
        return {"run_id": run_id, "cancelled": True}

    @router.post("/runs/{run_id}/rerun")
    async def rerun_run(run_id: str, mode: str = "in_place") -> dict[str, Any]:
        rm = _require_manager()
        try:
            run = await rm.rerun_in_place(run_id)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(f"Failed to rerun run '{run_id}': {e}")
            raise HTTPException(status_code=422, detail=str(e))
        state.notify_poll()
        return _run_summary(run)

    @router.get("/runs/{run_id}/tasks/{task_id}/logs")
    async def get_task_logs(
        run_id: str,
        task_id: str,
        type: str = "output",
        tail: int | None = None,
    ) -> dict[str, Any]:
        rm = _require_manager()
        if type not in ("output", "error"):
            raise HTTPException(
                status_code=422, detail="type must be 'output' or 'error'"
            )
        content, error = await rm.fetch_log_file(
            run_id, task_id, log_type=type, tail_lines=tail,
        )
        if error and content is None:
            # Distinguish "not found" from "task not yet submitted" by message
            status = 404 if "not found" in error else 422
            raise HTTPException(status_code=status, detail=error)
        return {
            "run_id": run_id,
            "task_id": task_id,
            "type": type,
            "content": content or "",
        }

    return router


__all__ = ["make_api_router"]
