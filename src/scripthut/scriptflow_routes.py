"""ScriptFlow JSON routes and passive web views."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from sse_starlette.sse import EventSourceResponse

from scripthut.scriptflow import CachedRun, ObservationError, RecordReader
from scripthut.scriptflow_cli import cancel
from scripthut.scriptflow_server import ScriptFlowService

LOG_CAP = 262144


def make_scriptflow_router(service: ScriptFlowService, templates: Any) -> APIRouter:
    router = APIRouter()
    api = "/api/v1/scriptflow/runs"

    async def get(reference: str) -> CachedRun:
        try:
            return await service.store.get(reference)
        except ObservationError as exc:
            raise HTTPException(
                404 if exc.kind in ("not_found", "invalid_reference") else 503, str(exc)
            ) from exc

    async def listing() -> list[dict[str, Any]]:
        try:
            return [cached.projection() for cached in await service.store.list()]
        except (ObservationError, OSError) as exc:
            raise HTTPException(503, str(exc)) from exc

    @router.get(api)
    async def list_runs() -> list[dict[str, Any]]:
        return await listing()

    @router.get(api + "/{reference}")
    async def view(reference: str) -> dict[str, Any]:
        return (await get(reference)).projection()

    @router.post(api + "/{reference}/refresh")
    async def refresh(reference: str) -> dict[str, Any]:
        await get(reference)
        return (await service.refresh(reference)).projection()

    @router.post(api + "/{reference}/cancel")
    async def request_cancel(reference: str) -> dict[str, Any]:
        async with service.lock(reference):
            cached = await get(reference)
            await service.fetch(cached)
            if cached.last_fetch_failed:
                data = cached.projection()
                data["cancel_result"] = {
                    "status": "unreachable",
                    "message": "Record unreadable; no cancellation written.",
                }
                return data
            data, _ = await cancel(
                RecordReader(service.transport(cached)), cached, service.store, None
            )
            return data

    @router.get(api + "/{reference}/logs")
    async def logs(
        reference: str,
        task: str,
        attempt: int = Query(1, ge=1),
        tail: int = Query(200, ge=0, le=10000),
    ) -> dict[str, Any]:
        async with service.lock(reference):
            cached = await get(reference)
            await service.fetch(cached)
            if cached.last_fetch_failed:
                raise HTTPException(503, cached.fetch_error)
            try:
                content, number = await RecordReader(service.transport(cached)).logs(
                    cached, task, attempt, tail, max_bytes=LOG_CAP + 1
                )
            except ObservationError as exc:
                raise HTTPException(
                    422 if exc.kind == "unsafe_path" else 404 if exc.kind == "not_found" else 503,
                    str(exc),
                ) from exc
            encoded = content.encode()
            return {
                "content": encoded[:LOG_CAP].decode(errors="ignore"),
                "attempt": number,
                "truncated": len(encoded) > LOG_CAP,
                "max_bytes": LOG_CAP,
            }

    @router.get("/scriptflow/stream")
    async def stream(request: Request) -> EventSourceResponse:
        async def events() -> Any:
            while not await request.is_disconnected():
                if await service.state.wait_for_poll():
                    yield {"event": "refresh", "data": "updated"}

        return EventSourceResponse(events())

    @router.get("/scriptflow", response_class=HTMLResponse)
    async def page(request: Request) -> Any:
        return templates.TemplateResponse(
            request=request,
            name="scriptflow.html",
            context={"request": request, "runs": await listing(), "run": None},
        )

    @router.get("/scriptflow/{reference}", response_class=HTMLResponse)
    async def run_page(request: Request, reference: str) -> Any:
        run = (await get(reference)).projection()
        return templates.TemplateResponse(
            request=request,
            name="scriptflow.html",
            context={"request": request, "runs": [], "run": run, "urlquote": quote},
        )

    return router
