"""ScriptFlow JSON routes and passive web views."""

from __future__ import annotations

from collections import Counter
from typing import Any
from urllib.parse import quote, urlencode

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from sse_starlette.sse import EventSourceResponse

from scripthut.scriptflow import CachedRun, ObservationError, RecordReader, Snapshot
from scripthut.scriptflow_cli import cancel
from scripthut.scriptflow_server import ScriptFlowService
from scripthut.scriptflow_web import (
    cached_view,
    configure_templates,
    runs_context,
    task_page,
    timeline,
)

LOG_CAP = 262144


def make_scriptflow_router(service: ScriptFlowService, templates: Any) -> APIRouter:
    router = APIRouter()
    configure_templates(templates)
    api = "/api/v1/scriptflow/runs"

    async def get(reference: str) -> CachedRun:
        try:
            return await service.store.get(reference)
        except ObservationError as exc:
            raise HTTPException(
                404 if exc.kind in ("not_found", "invalid_reference") else 503, str(exc)
            ) from exc

    async def listing(summary: bool) -> list[dict[str, Any]]:
        try:
            result = []
            for cached in await service.store.list():
                data = cached.projection()
                if summary and data["workflow"] is not None:
                    tasks = data["workflow"].pop("tasks")
                    data["task_counts"] = dict(Counter(task["state"] for task in tasks.values()))
                    data["task_count"] = len(tasks)
                result.append(data)
            return result
        except (ObservationError, OSError) as exc:
            raise HTTPException(503, str(exc)) from exc

    @router.get(api)
    async def list_runs(summary: bool = True) -> list[dict[str, Any]]:
        return await listing(summary)

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

    async def context(request: Request) -> dict[str, Any]:
        data = await runs_context(service)
        filters = {
            key: request.query_params.get(key, "") for key in ("state", "backend", "source", "flow")
        }
        data["runs"] = [
            run
            for run in data["runs"]
            if all(
                not value
                or value.lower()
                in str(
                    getattr(
                        run,
                        {
                            "state": "workflow_status",
                            "backend": "backend_name",
                            "source": "source_name",
                            "flow": "workflow_name",
                        }[key],
                    )
                ).lower()
                for key, value in filters.items()
            )
        ]
        return dict(data, filters=filters, filter_query=urlencode(filters))

    async def detail(
        reference: str, page: int = 1, task_state: str = "", search: str = ""
    ) -> dict[str, Any]:
        run = await cached_view(await get(reference), service)
        paging = task_page(run, page, task_state, search)
        return dict(run=run, urlquote=quote, **paging, **timeline(run))

    @router.get("/runs/stream")
    async def runs_stream(request: Request) -> EventSourceResponse:
        async def events() -> Any:
            while not await request.is_disconnected():
                if await service.state.wait_for_poll():
                    yield dict(
                        event="runs-update",
                        data=templates.get_template("runs_list.html").render(
                            request=request, **await context(request)
                        ),
                    )

        return EventSourceResponse(events())

    @router.get("/runs", response_class=HTMLResponse)
    @router.get("/scriptflow", response_class=HTMLResponse)
    async def page(request: Request) -> Any:
        return templates.TemplateResponse(
            request=request, name="runs.html", context=await context(request)
        )

    @router.get("/runs/{reference}", response_class=HTMLResponse)
    @router.get("/scriptflow/{reference}", response_class=HTMLResponse)
    async def run_page(
        request: Request,
        reference: str,
        page: int = Query(1, ge=1),
        task_state: str = "",
        search: str = Query("", max_length=200),
    ) -> Any:
        return templates.TemplateResponse(
            request=request,
            name="run_detail.html",
            context=await detail(reference, page, task_state, search),
        )

    @router.get("/runs/{reference}/events")
    async def run_events(
        request: Request,
        reference: str,
        page: int = Query(1, ge=1),
        task_state: str = "",
        search: str = Query("", max_length=200),
    ) -> EventSourceResponse:
        await get(reference)

        async def events() -> Any:
            while not await request.is_disconnected():
                if await service.state.wait_for_poll():
                    data = await detail(reference, page, task_state, search)
                    for event, name in [
                        ("info", "run_info"),
                        ("paging", "run_task_paging"),
                        ("items", "run_items"),
                        ("gantt", "run_gantt"),
                        ("sidebar", "run_sidebar"),
                    ]:
                        yield dict(
                            event=event + "-update",
                            data=templates.get_template(name + ".html").render(
                                request=request, **data
                            ),
                        )

        return EventSourceResponse(events())

    @router.get("/runs/{reference}/logs", response_class=HTMLResponse)
    async def log_page(
        request: Request, reference: str, task: str, attempt: int = Query(1, ge=1)
    ) -> Any:
        cached = await get(reference)
        record = cached.snapshot
        try:
            snapshot = Snapshot.model_validate(record)
            task_id = RecordReader.select_task(snapshot, task)
            if attempt > len(snapshot.tasks[task_id].attempts):
                raise HTTPException(404, "Attempt not found")
        except (ObservationError, ValueError, AttributeError, TypeError):
            raise HTTPException(404, "Task not found")
        run = await cached_view(cached, service)
        run.items = [item for item in run.items if item.task.id == task_id]
        data = dict(run=run)
        data.update(
            title=f"{task} · Attempt {attempt}",
            task_name=task,
            attempt=attempt,
            log_url=api
            + "/"
            + quote(reference)
            + "/logs?"
            + urlencode(dict(task=task_id, attempt=attempt)),
        )
        return templates.TemplateResponse(request=request, name="log_viewer.html", context=data)

    return router
