"""Cached dashboard pages and explicit source launches."""

from argparse import Namespace
from types import SimpleNamespace
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from sse_starlette.sse import EventSourceResponse

from scripthut.scriptflow import ObservationError
from scripthut.scriptflow_cli import handle_controller
from scripthut.scriptflow_web import configure_templates, dashboard_context


def make_web_router(state: Any, service: Any, templates: Any) -> APIRouter:
    router = APIRouter()
    configure_templates(templates)

    def page(request: Request, name: str, **context: Any) -> Any:
        return templates.TemplateResponse(request=request, name=name, context=context)

    async def overview_context() -> dict[str, Any]:
        return await dashboard_context(state, service)

    @router.get("/", response_class=HTMLResponse)
    async def overview(request: Request) -> Any:
        if state.config_error:
            return page(request, "config_error.html", config_error=state.config_error)
        return page(request, "overview.html", **await overview_context())

    @router.get("/backends", response_class=HTMLResponse)
    async def backends(request: Request) -> Any:
        return page(request, "backends.html", **await dashboard_context(state, service))

    @router.get("/jobs", response_class=HTMLResponse)
    async def jobs(request: Request) -> Any:
        return page(request, "jobs.html", **await dashboard_context(state, service))

    @router.get("/sources", response_class=HTMLResponse)
    async def sources(request: Request) -> Any:
        configs = {s.name: s for s in state.config.sources} if state.config else {}
        rows = state.web_sources or [
            dict(
                config=s,
                workdir=getattr(s, "working_directory", None),
                flow_dir=getattr(s, "flow_dir", "."),
                flows=[],
                error=None,
                notice="Awaiting source observation",
            )
            for s in configs.values()
        ]
        workflows = {
            name: sorted(
                {
                    flow
                    for row in rows
                    if row["config"].name == name
                    for flow in row.get("flows", [])
                }
            )
            for name in configs
        }
        return page(
            request,
            "sources.html",
            source_rows=rows,
            source_workflows=workflows,
            source_configs=configs,
            source_statuses=state.source_statuses,
            disabled_sources=set(),
            backend_names=list(state.backends),
        )

    @router.post("/sources/launch")
    async def launch(request: Request) -> Any:
        if state.config is None:
            raise HTTPException(503, "Configuration unavailable")
        form = await request.form()
        required = ("source", "flow", "run_id", "backend")
        if any(not isinstance(form.get(key), str) or not form[key] for key in required):
            raise HTTPException(422, "source, flow, run_id and backend are required")
        args = Namespace(
            scriptflow_cmd="launch",
            **{key: form[key] for key in required},
            branch=None,
            commit=None,
            snapshot=False,
            workdir=None,
            runs_dir=None,
            cpus=None,
            memory=None,
            time_limit=None,
            partition=None,
            force_pattern=None,
            resubmit_ambiguous=False,
        )
        try:
            result = await handle_controller(state.config, service.store, args)
        except (ObservationError, ValueError, OSError, RuntimeError) as exc:
            raise HTTPException(409, str(exc)) from exc
        state.notify_poll()
        return result

    @router.get("/terminals", response_class=HTMLResponse)
    async def terminals(request: Request) -> Any:
        backends = [
            SimpleNamespace(name=name, host=b.status.host, connected=b.status.connected)
            for name, b in state.backends.items()
        ]
        return page(
            request,
            "terminals.html",
            sessions=list(state.terminal_manager.sessions.values()),
            backends=backends,
        )

    def disk_context() -> dict[str, Any]:
        return dict(
            disk_backends=[
                state.disk_usage.get(
                    name,
                    dict(name=name, rows=[], observed_at=None, error="Awaiting disk observation"),
                )
                for name in state.backends
            ]
        )

    @router.get("/disk", response_class=HTMLResponse)
    async def disk(request: Request) -> Any:
        return page(request, "disk.html", **disk_context())

    async def events(request: Request, kind: str) -> Any:
        while not await request.is_disconnected():
            if not await state.wait_for_poll():
                continue
            if kind == "overview":
                fragments = [("overview-update", "overview_cards.html", await overview_context())]
            elif kind == "disk":
                fragments = [("disk-update", "disk_backend.html", disk_context())]
            else:
                context = await dashboard_context(state, service)
                fragments = [
                    ("jobs-update", "jobs.html", context),
                    ("backends-update", "backends_status.html", context),
                ]
            for event, template, context in fragments:
                yield dict(
                    event=event,
                    data=templates.get_template(template).render(request=request, **context),
                )

    @router.get("/overview/stream")
    async def overview_stream(request: Request) -> EventSourceResponse:
        return EventSourceResponse(events(request, "overview"))

    @router.get("/jobs/stream")
    async def jobs_stream(request: Request) -> EventSourceResponse:
        return EventSourceResponse(events(request, "jobs"))

    @router.get("/disk/stream")
    async def disk_stream(request: Request) -> EventSourceResponse:
        return EventSourceResponse(events(request, "disk"))

    return router
