"""Shared backend and source API."""
from __future__ import annotations
import logging
from typing import Any, TYPE_CHECKING
from fastapi import APIRouter, HTTPException
if TYPE_CHECKING:
    from scripthut.main import AppState
logger = logging.getLogger(__name__)


def make_api_router(state: AppState) -> APIRouter:
    router = APIRouter(prefix="/api/v1")
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
            backend_type = bs.backend_type if bs else cfg.type
            result.append({
                "name": cfg.name,
                "type": backend_type,
                "connected": connected,
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
        }
        if isinstance(src, GitSourceConfig):
            common.update({"url": src.url, "branch": src.branch})
            if src.local_path is not None:
                common["local_path"] = str(src.local_path_resolved)
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
        """Return source metadata and the latest synchronization status."""
        if state.config is None:
            raise HTTPException(status_code=503, detail="Config not loaded")
        source = state.config.get_source(name)
        if source is None:
            raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        status = state.source_statuses.get(name)
        return {
            **_source_summary(source),
            "warnings": list(status.warnings) if status else [],
            "dirty": bool(status.dirty) if status else False,
        }


    async def _sync_one_source(name: str) -> dict[str, Any]:
        """Synchronize one configured git repository."""
        from scripthut.config_schema import GitSourceConfig
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
            except Exception as e:
                logger.exception(f"Sync failed for git source '{name}'")
                result["error"] = str(e)
        return result


    @router.post("/sources/{name}/sync")
    async def sync_one_source_v1(name: str) -> dict[str, Any]:
        """Synchronize a source; report synchronization errors in the response."""
        return await _sync_one_source(name)


    @router.post("/sources/sync")
    async def sync_all_sources_v1() -> dict[str, Any]:
        """Re-sync every configured source. Errors are per-entry, not fatal."""
        if state.config is None:
            return {"sources": []}
        names = [s.name for s in state.config.sources]
        results = [await _sync_one_source(n) for n in names]
        return {"sources": results}


    @router.get("/backends/{name}/jobs")
    async def backend_jobs(name: str):
        from dataclasses import asdict
        backend = state.backends.get(name)
        if backend is None:
            raise HTTPException(status_code=404, detail="Backend not found")
        return {"jobs": [asdict(job) for job in backend.jobs]}

    return router
