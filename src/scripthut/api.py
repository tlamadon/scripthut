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
from pydantic import BaseModel, Field

from scripthut.config_schema import (
    BatchBackendConfig,
    EC2BackendConfig,
    ECSBackendConfig,
    LocalBackendConfig,
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
    if isinstance(cfg, LocalBackendConfig):
        return "local"
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


class DiskCleanRequest(BaseModel):
    """Body of ``POST /api/v1/disk/clean``.

    A JSON body (not query params) because ``paths`` is a variable-
    length list of absolute remote paths — spaces, unicode, possibly
    hundreds of entries — which query strings encode fragilely and
    proxies cap in length.
    """

    backend: str
    paths: list[str] | None = Field(
        default=None,
        description="Explicit scanned paths to delete; null = all orphaned",
    )
    allow_referenced: list[str] = Field(
        default_factory=list,
        description="Paths that may be deleted even while referenced by a terminal run",
    )
    dry_run: bool = False


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
        # isinstance (not a bare getattr) so MagicMock states in tests and
        # states without the field read as "not starting".
        phase = getattr(state, "startup_phase", None)
        return {
            "status": "ok",
            "config_loaded": state.config is not None,
            "config_error": state.config_error,
            "starting": phase if isinstance(phase, str) else None,
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

    @router.post("/tasks/probe")
    async def probe_tasks_v1(payload: dict) -> dict[str, Any]:
        """Dry-run cache probe: hit/miss per task, executing nothing.

        Takes the same task JSON a run submission would — either a single
        ``task`` object (the ``/tasks/run`` shape) or a workflow document
        under ``tasks`` (wrapped ``{"tasks": [...], "env": [...]}`` or a
        bare list) — plus a ``backend`` name. Computes each task's cache
        key exactly as a submission would and answers hit/miss, returning
        the cached outputs' content hashes on a hit. Guaranteed
        side-effect-free: no run records, no cache mutations, no restores.

        Optional ``commit_hash`` feeds ``cache_scope="commit"`` tasks'
        keys (defaults to null, matching ad-hoc submissions); optional
        ``workflow_name`` matches the env-resolution context of a
        specific workflow's submission.
        """
        from scripthut.runs.models import TaskDefinition

        rm = _require_manager()
        backend = payload.get("backend")
        if not backend or not isinstance(backend, str):
            raise HTTPException(
                status_code=422, detail="payload must contain 'backend' (str)",
            )
        task_dict = payload.get("task")
        tasks_doc = payload.get("tasks")
        if (task_dict is None) == (tasks_doc is None):
            raise HTTPException(
                status_code=422,
                detail="payload must contain exactly one of 'task' (dict) "
                       "or 'tasks' (workflow document)",
            )
        try:
            if task_dict is not None:
                if not isinstance(task_dict, dict):
                    raise ValueError("'task' must be a dict")
                tasks = [TaskDefinition.from_dict(task_dict)]
                doc_env: list = []
                doc_env_groups: dict = {}
            else:
                if not isinstance(tasks_doc, (dict, list)):
                    raise ValueError("'tasks' must be a list or dict")
                tasks, doc_env, doc_env_groups = TaskDefinition.parse_document(
                    tasks_doc
                )
        except (KeyError, ValueError) as e:
            raise HTTPException(status_code=422, detail=f"invalid task(s): {e}")
        try:
            results = await rm.probe_tasks(
                tasks, backend,
                workflow_name=payload.get("workflow_name") or "_probe",
                commit_hash=payload.get("commit_hash"),
                doc_env=doc_env,
                doc_env_groups=doc_env_groups,
            )
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        from scripthut.runs.manager import probe_summary

        return {
            "backend": backend,
            "cache_enabled": rm.cache_manager.enabled,
            "results": results,
            "summary": probe_summary(results),
        }

    @router.post("/sources/{name}/run")
    async def run_source_workflow_v1(
        name: str, workflow: str, backend: str | None = None,
        branch: str | None = None,
    ) -> dict[str, Any]:
        """Submit a source workflow as a new run.

        ``workflow`` is the filename within the source (e.g. ``train.json``).
        ``backend`` falls back to the source's own ``backend`` field when
        omitted — required because ``create_run_from_source`` needs one.
        ``branch`` (git sources only) runs the workflow from that branch
        instead of the source's configured one: the branch is fetched
        into the server's clone on demand and both the workflow JSON and
        the ``scripthut.yaml`` overlay are read at its tip.
        """
        from scripthut.config_schema import GitSourceConfig
        from scripthut.sources.git import is_safe_branch_name

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

        # A branch equal to the configured one is a no-op override; drop
        # it so the normal cached-discovery path below handles the run.
        if branch is not None and branch == getattr(source, "branch", None):
            branch = None

        # Fetch the workflow JSON. Git sources need a refresh first so we
        # don't run against a stale clone; a branch override instead
        # fetches that branch and discovers workflows at its tip (the
        # cached default-branch discovery is left untouched).
        wf = None
        if branch is not None:
            if not isinstance(source, GitSourceConfig):
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Source '{name}' is not a git source; ?branch= "
                        "only applies to git sources."
                    ),
                )
            if not is_safe_branch_name(branch):
                raise HTTPException(
                    status_code=422, detail=f"Invalid branch name: {branch!r}",
                )
            sm = state.source_manager
            if sm is None or name not in getattr(sm, "_sources", {}):
                raise HTTPException(
                    status_code=503,
                    detail=f"Source manager not available for '{name}'",
                )
            try:
                commit = await sm.fetch_branch(name, branch)
                branch_workflows = await sm.discover_workflows_at(name, commit)
            except ValueError as e:
                raise HTTPException(status_code=422, detail=str(e))
            wf = next(
                (w for w in branch_workflows if w.filename == workflow), None,
            )
            if wf is None:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"Workflow '{workflow}' not found in source '{name}' "
                        f"on branch '{branch}'"
                    ),
                )
        else:
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
                branch=branch,
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

    @router.get("/sources/{name}/config")
    async def view_source_project_config(name: str) -> dict[str, Any]:
        """Return the source repo's ``scripthut.yaml`` as JSON.

        Carries ``env`` / ``env_groups`` / ``stacks`` — the sections a
        project-local YAML is allowed to define. The same file is
        merged server-side into runs of this source's workflows
        (precedence: server → backend → repo-project → workflow-doc →
        task); this endpoint exists so the CLI can also surface its
        ``stacks`` to operators via ``scripthut stack ... --source <name>``.

        ``config_present=False`` means the file is absent at the source
        root or unreadable — both are non-error cases. 422 is reserved
        for an actively-broken project YAML (forbidden sections, bad
        YAML, schema mismatch) so the operator sees the diagnostic.
        """
        from scripthut.config_schema import PathSourceConfig

        if state.config is None:
            raise HTTPException(status_code=503, detail="Config not loaded")
        source = state.config.get_source(name)
        if source is None:
            raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        rm = _require_manager()

        # Path sources require the backend's SSH client to read the file;
        # without it, treat as "no overlay readable" rather than 5xx, so
        # the operator's `stack list --source` still works against the
        # rest of the data. Soft fail is consistent with the helper.
        ssh_client = None
        if isinstance(source, PathSourceConfig):
            ssh_client = rm.get_ssh_client(source.backend)

        try:
            project_cfg = await rm._load_source_project_config(
                source, ssh_client=ssh_client,
            )
        except ValueError as e:
            # Forbidden section / malformed YAML / schema error — actively
            # broken config, surface to the caller.
            raise HTTPException(status_code=422, detail=str(e)) from e

        if project_cfg is None:
            return {
                "source": name,
                "config_present": False,
                "env": [],
                "env_groups": {},
                "stacks": [],
            }

        return {
            "source": name,
            "config_present": True,
            "env": [
                r.model_dump(mode="json", by_alias=True, exclude_defaults=True)
                for r in project_cfg.env
            ],
            "env_groups": {
                name_: [
                    r.model_dump(mode="json", by_alias=True, exclude_defaults=True)
                    for r in rules
                ]
                for name_, rules in project_cfg.env_groups.items()
            },
            "stacks": [
                s.model_dump(mode="json", exclude_defaults=True)
                for s in project_cfg.stacks
            ],
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
        items = []
        for item in run.items:
            d = item.to_dict()
            # Terminal items carry their assembled task manifest so a
            # consumer of run results doesn't need a second request per
            # task; the dedicated manifest endpoint below returns the
            # same document standalone.
            if item.status in (RunItemStatus.COMPLETED, RunItemStatus.FAILED):
                d["manifest"] = rm.get_task_manifest(run, item)
            items.append(d)
        summary["items"] = items
        summary["log_dir"] = run.log_dir
        summary["account"] = run.account
        summary["commit_hash"] = run.commit_hash
        summary["git_repo"] = run.git_repo
        summary["git_branch"] = run.git_branch
        return summary

    @router.get("/runs/{run_id}/tasks/{task_id}/manifest")
    async def get_task_manifest_v1(run_id: str, task_id: str) -> dict[str, Any]:
        """The versioned per-task manifest (see scripthut.runs.manifest).

        Available once the task is terminal; a 409 before that keeps
        consumers from adopting hashes that aren't final yet.
        """
        rm = _require_manager()
        run = rm.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
        item = run.get_item_by_task_id(task_id)
        if item is None:
            raise HTTPException(
                status_code=404,
                detail=f"Task '{task_id}' not found in run '{run_id}'",
            )
        if item.status not in (
            RunItemStatus.COMPLETED,
            RunItemStatus.FAILED,
            RunItemStatus.DEP_FAILED,
        ):
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Task '{task_id}' is {item.status.value} — the manifest "
                    "is only final once the task is terminal"
                ),
            )
        return rm.get_task_manifest(run, item)

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

    # ----- stacks -----------------------------------------------------------
    #
    # Server-mediated stack install/check/delete. The CLI's local-SSH path
    # remains the default when no server is configured; when a remote
    # server IS configured, these endpoints let a thin client manage
    # stacks without holding SSH keys to the cluster. Symmetric with
    # workflow run (also server-mediated when --source is set).

    async def _resolve_stack(
        name: str, source: str | None,
    ) -> Any:
        """Find a Stack by name, optionally overlaying a source repo's project YAML.

        Resolution: server-global stacks are the base; ``source`` (when
        set) is re-synced and its project ``scripthut.yaml`` stacks are
        merged on top (source wins on collision, matching the CLI's
        ``_overlay_source_stacks``). 404 if the name resolves to nothing
        in either layer.
        """
        if state.config is None:
            raise HTTPException(status_code=503, detail="Config not loaded")
        rm = _require_manager()

        merged: dict[str, Any] = {s.name: s for s in state.config.stacks}

        if source is not None:
            source_cfg = state.config.get_source(source)
            if source_cfg is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Source '{source}' not found",
                )
            # Re-sync git sources so the stack definition reflects the
            # latest HEAD on the source's configured branch — same
            # guarantee `workflow run` gives.
            if state.source_manager and source in getattr(
                state.source_manager, "_sources", {},
            ):
                try:
                    await state.source_manager.sync_source(source)
                except Exception as e:
                    logger.warning(
                        f"Failed to sync source '{source}' before stack op: {e}"
                    )
            from scripthut.config_schema import PathSourceConfig
            ssh_for_cfg = None
            if isinstance(source_cfg, PathSourceConfig):
                ssh_for_cfg = rm.get_ssh_client(source_cfg.backend)
            try:
                project_cfg = await rm._load_source_project_config(
                    source_cfg, ssh_client=ssh_for_cfg,
                )
            except ValueError as e:
                # Forbidden section / malformed YAML — explicit error so
                # the operator sees the diagnostic.
                raise HTTPException(status_code=422, detail=str(e)) from e
            if project_cfg is not None:
                for s in project_cfg.stacks:
                    merged[s.name] = s

        stack = merged.get(name)
        if stack is None:
            where = f" in source '{source}' or server config" if source else " in the server config"
            raise HTTPException(
                status_code=404,
                detail=f"Stack '{name}' not found{where}",
            )
        return stack

    def _scheduler_for_backend(backend_name: str) -> str | None:
        """Scheduler tag StackManager.install uses to decide how to run prep.

        Slurm wraps prep with ``srun`` so the build lands on a worker;
        PBS / API-only backends run prep inline over SSH (PBS-side
        builds are not wired yet). Inlined here rather than imported
        from cli.py to keep api.py from depending on the CLI surface.
        """
        from scripthut.config_schema import (
            PBSBackendConfig,
            SlurmBackendConfig,
        )
        if state.config is None:
            return None
        cfg = state.config.get_backend(backend_name)
        if isinstance(cfg, SlurmBackendConfig):
            return "slurm"
        if isinstance(cfg, PBSBackendConfig):
            return "pbs"
        return None

    def _stack_status_dict(status: Any) -> dict[str, Any]:
        """Shape a StackStatus for JSON return."""
        return {
            "name": status.name,
            "backend": status.backend,
            "state": status.state.value,
            "hash": status.hash,
            "path": status.path,
            "last_built": status.last_built.isoformat() if status.last_built else None,
            "size_bytes": status.size_bytes,
            "error": status.error,
        }

    @router.get("/stacks/{name}/check")
    async def check_stack_v1(
        name: str, backend: str, source: str | None = None,
    ) -> dict[str, Any]:
        """Report the installed state of a stack on one backend (server-side).

        Source overlay (when ``source`` is set) means the same name can
        resolve to different prep/inputs than the server-global default,
        so the hash this returns may differ from a hashless ``stack list``
        view — that's the whole point of letting a repo redefine a
        stack.
        """
        from scripthut.stacks import StackManager

        stack = await _resolve_stack(name, source)
        rm = _require_manager()
        ssh = rm.get_ssh_client(backend)
        if ssh is None:
            raise HTTPException(
                status_code=503,
                detail=f"Backend '{backend}' has no SSH connection on the server",
            )
        mgr = StackManager()
        try:
            status = await mgr.check(stack, backend, ssh)
        except Exception as e:
            logger.exception(f"stack check '{name}' on '{backend}' failed")
            raise HTTPException(status_code=500, detail=str(e)) from e
        return _stack_status_dict(status)

    @router.post("/stacks/{name}/install")
    async def install_stack_v1(
        name: str, backend: str, source: str | None = None,
        rebuild: bool = False,
    ) -> dict[str, Any]:
        """Submit a stack install as a workflow run; return the run summary.

        The endpoint no longer blocks: install is synthesized as a
        single-task workflow, submitted through the same scheduler path
        as every other run, and the run summary returned immediately.
        Caller monitors via the existing ``/runs/{id}`` endpoints —
        ``run view`` for status, ``run logs -f`` for live output, ``run
        watch --exit-status`` for "wait until done".

        Idempotency lives in the task command: if the stack's content
        hash already has a ``.ready`` sentinel and ``rebuild`` is False,
        the task exits 0 immediately. A "nothing to do" install is still
        a visible run — operators see "I tried to install julia, it was
        already there" rather than guessing whether the request landed.
        """
        stack = await _resolve_stack(name, source)
        rm = _require_manager()
        try:
            run = await rm.create_run_from_stack(
                stack, backend, rebuild=rebuild, source_name=source,
            )
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e
        except Exception as e:
            logger.exception(
                f"stack install submission for '{name}' on '{backend}' failed"
            )
            raise HTTPException(status_code=500, detail=str(e)) from e
        state.notify_poll()
        return _run_summary(run)

    @router.delete("/stacks/{name}")
    async def delete_stack_v1(
        name: str, backend: str, source: str | None = None,
    ) -> dict[str, Any]:
        """Remove every cached build of this stack on one backend.

        Source overlay matters when the repo's project YAML defines a
        stack with a different ``cache_dir`` than the server-global —
        the delete operates on the resolved stack's cache path, not on
        every directory that happens to share the stack name.
        """
        from scripthut.stacks import StackManager

        stack = await _resolve_stack(name, source)
        rm = _require_manager()
        ssh = rm.get_ssh_client(backend)
        if ssh is None:
            raise HTTPException(
                status_code=503,
                detail=f"Backend '{backend}' has no SSH connection on the server",
            )
        mgr = StackManager()
        try:
            await mgr.delete(stack, backend, ssh)
        except Exception as e:
            logger.exception(f"stack delete '{name}' on '{backend}' failed")
            raise HTTPException(status_code=500, detail=str(e)) from e
        return {"name": name, "backend": backend, "deleted": True}

    def _disk_backend_names() -> list[str]:
        """SSH-based backends — the only ones with a scannable filesystem."""
        return [
            name
            for name, bs in state.backends.items()
            if bs.backend_type in ("slurm", "pbs")
        ]

    def _disk_status(name: str) -> dict[str, Any]:
        result = state.disk_service.get_cached(name)
        report = state.disk_service.get_last_cleanup(name)
        return {
            "scanning": state.disk_service.is_scanning(name),
            "cleaning": state.disk_service.is_cleaning(name),
            "result": result.to_dict() if result else None,
            "last_cleanup": report.to_dict() if report else None,
        }

    @router.get("/disk")
    async def get_disk_v1(backend: str | None = None) -> dict[str, Any]:
        """Cached disk-scan results per backend. Never triggers a scan."""
        if backend is not None:
            if backend not in state.backends:
                raise HTTPException(
                    status_code=404, detail=f"Unknown backend '{backend}'"
                )
            names = [backend]
        else:
            names = _disk_backend_names()
        return {"backends": {n: _disk_status(n) for n in names}}

    @router.post("/disk/scan")
    async def scan_disk_v1(backend: str) -> dict[str, Any]:
        """Kick off a background disk scan; poll GET /disk for the result.

        Non-blocking on purpose: a ``du`` over a large shared filesystem
        can exceed the request limits of proxies like Cloudflare Access,
        so the only shape that works everywhere is start-then-poll.
        """
        from scripthut.disk.service import start_scan_for_backend

        if state.config is None:
            raise HTTPException(status_code=503, detail="No configuration loaded")
        backend_state = state.backends.get(backend)
        if backend_state is None:
            raise HTTPException(
                status_code=404, detail=f"Unknown backend '{backend}'"
            )
        rm = _require_manager()
        ssh = rm.get_ssh_client(backend)
        if ssh is None:
            raise HTTPException(
                status_code=503,
                detail=f"Backend '{backend}' has no SSH connection on the server",
            )
        started = await start_scan_for_backend(
            state.disk_service,
            config=state.config,
            backend_name=backend,
            clone_dir=backend_state.clone_dir,
            ssh=ssh,
            run_manager=state.run_manager,
            run_storage=state.run_storage,
        )
        return {
            "backend": backend,
            "status": "started" if started else "already_running",
        }

    @router.post("/disk/clean")
    async def clean_disk_v1(req: DiskCleanRequest) -> dict[str, Any]:
        """Delete scanned artifacts (guardrailed); poll GET /disk for the result.

        Bulk mode (``paths=null``) deletes every currently-orphaned
        entry from the last scan. Explicit paths must all be present in
        that scan — the server never deletes a path it didn't scan —
        and referenced entries additionally need per-path opt-in via
        ``allow_referenced``. ``dry_run`` returns the plan without
        touching the backend (synchronous, no SSH).
        """
        from scripthut.disk.service import (
            plan_cleanup_for_backend,
            start_clean_for_backend,
        )

        if state.config is None:
            raise HTTPException(status_code=503, detail="No configuration loaded")
        backend_state = state.backends.get(req.backend)
        if backend_state is None:
            raise HTTPException(
                status_code=404, detail=f"Unknown backend '{req.backend}'"
            )
        rm = _require_manager()
        # Fetched before the dry-run branch so dry-run and exec classify
        # path-source project stacks identically; dry-run tolerates None
        # (path-source overlays are then skipped, git ones still read).
        ssh = rm.get_ssh_client(req.backend)

        if req.dry_run:
            plan = await plan_cleanup_for_backend(
                state.disk_service,
                config=state.config,
                backend_name=req.backend,
                clone_dir=backend_state.clone_dir,
                run_manager=state.run_manager,
                run_storage=state.run_storage,
                paths=req.paths,
                allow_referenced=frozenset(req.allow_referenced),
                ssh=ssh,
            )
            if plan is None:
                raise HTTPException(
                    status_code=409,
                    detail=f"No disk scan cached for '{req.backend}' — "
                    "POST /api/v1/disk/scan first",
                )
            if plan.errors:
                raise HTTPException(status_code=400, detail="; ".join(plan.errors))
            return {"backend": req.backend, "dry_run": True, "plan": plan.to_dict()}

        if ssh is None:
            raise HTTPException(
                status_code=503,
                detail=f"Backend '{req.backend}' has no SSH connection on the server",
            )
        status, plan = await start_clean_for_backend(
            state.disk_service,
            config=state.config,
            backend_name=req.backend,
            clone_dir=backend_state.clone_dir,
            ssh=ssh,
            run_manager=state.run_manager,
            run_storage=state.run_storage,
            paths=req.paths,
            allow_referenced=frozenset(req.allow_referenced),
        )
        if status == "no_scan":
            raise HTTPException(
                status_code=409,
                detail=f"No disk scan cached for '{req.backend}' — "
                "POST /api/v1/disk/scan first",
            )
        if status == "invalid":
            raise HTTPException(
                status_code=400,
                detail="; ".join(plan.errors if plan else ["invalid request"]),
            )
        body: dict[str, Any] = {"backend": req.backend, "status": status}
        if plan is not None:
            counts = plan.counts
            body["planned"] = {
                "delete": counts["delete"],
                "check": counts["check"],
                "skip": counts["skip"],
                "bytes": plan.delete_bytes,
            }
        return body

    return router


__all__ = ["make_api_router"]
