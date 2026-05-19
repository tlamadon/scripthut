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


def _serialize_dry_run(result: dict[str, Any]) -> dict[str, Any]:
    """Convert a ``RunManager.dry_run`` dict into a fully JSON-safe payload.

    The manager returns a mix of dataclasses (``TaskDefinition``) and a
    Pydantic ``WorkflowConfig``, both of which need explicit serialization
    before they can be returned over HTTP.
    """
    workflow = result["workflow"]
    return {
        "workflow": {
            "name": workflow.name,
            "backend": workflow.backend,
            "description": workflow.description,
            "max_concurrent": workflow.max_concurrent,
            "has_git": workflow.git is not None,
        },
        "backend_name": result["backend_name"],
        "task_count": result["task_count"],
        "max_concurrent": result["max_concurrent"],
        "account": result["account"],
        "commit_hash": result["commit_hash"],
        "tasks": [
            {
                "task": entry["task"].to_dict(),
                "submit_script": entry["submit_script"],
                "output_path": entry["output_path"],
                "error_path": entry["error_path"],
            }
            for entry in result["tasks"]
        ],
        "raw_output": result["raw_output"],
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

    @router.get("/workflows")
    async def list_workflows() -> dict[str, Any]:
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
            ],
        }

    @router.get("/projects")
    async def list_projects() -> dict[str, Any]:
        if state.config is None:
            return {"projects": []}
        return {
            "projects": [
                {
                    "name": p.name,
                    "backend": p.backend,
                    "path": p.path,
                    "description": p.description,
                    "max_concurrent": p.max_concurrent,
                }
                for p in state.config.projects
            ],
        }

    @router.get("/projects/{name}")
    async def view_project(name: str) -> dict[str, Any]:
        """Project metadata plus the sflow.json files discovered on the backend.

        Discovery requires the project's backend to be reachable; if it isn't,
        the metadata is still returned and ``discover_error`` carries the
        reason so the caller can show partial results.
        """
        if state.config is None:
            raise HTTPException(status_code=503, detail="Config not loaded")
        project = state.config.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        rm = _require_manager()
        workflows: list[str] = []
        discover_error: str | None = None
        try:
            workflows = await rm.discover_workflows(name)
        except ValueError as e:
            discover_error = str(e)
        except Exception as e:
            logger.warning(f"discover_workflows failed for project '{name}': {e}")
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

    @router.post("/workflows/{name}/run")
    async def run_workflow(name: str, backend: str | None = None) -> dict[str, Any]:
        rm = _require_manager()
        try:
            run = await rm.create_run(name, backend=backend)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        except Exception as e:
            logger.error(f"Failed to create run for workflow '{name}': {e}")
            raise HTTPException(status_code=500, detail=str(e))
        state.notify_poll()
        return _run_summary(run)

    @router.get("/workflows/{name}/dry-run")
    async def dry_run_workflow(name: str, backend: str | None = None) -> dict[str, Any]:
        rm = _require_manager()
        try:
            result = await rm.dry_run(name, backend=backend)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        except Exception as e:
            logger.error(f"Dry run failed for workflow '{name}': {e}")
            raise HTTPException(status_code=500, detail=str(e))
        return _serialize_dry_run(result)

    @router.post("/projects/{name}/run")
    async def run_project_workflow(
        name: str, workflow: str, backend: str | None = None,
    ) -> dict[str, Any]:
        rm = _require_manager()
        try:
            run = await rm.create_run_from_project(name, workflow, backend=backend)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        except Exception as e:
            logger.error(f"Failed to create run for project '{name}': {e}")
            raise HTTPException(status_code=500, detail=str(e))
        state.notify_poll()
        return _run_summary(run)

    @router.get("/projects/{name}/workflows")
    async def list_project_workflows(name: str) -> dict[str, Any]:
        rm = _require_manager()
        try:
            paths = await rm.discover_workflows(name)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        return {"project": name, "workflows": paths}

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
    async def rerun_run(run_id: str, mode: str = "new") -> dict[str, Any]:
        rm = _require_manager()
        if mode not in ("new", "in_place"):
            raise HTTPException(
                status_code=422, detail="mode must be 'new' or 'in_place'"
            )
        try:
            if mode == "in_place":
                run = await rm.rerun_in_place(run_id)
            else:
                run = await rm.rerun_from(run_id)
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
