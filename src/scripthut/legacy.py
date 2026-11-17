"""Startup route selection for the legacy execution mode."""

from typing import Any


def legacy_route(path: str) -> bool:
    prefixes = (
        "/runs",
        "/api/v1/runs",
        "/api/v1/tasks",
        "/terminal/interactive",
        "/disk",
        "/api/v1/disk",
    )
    if any(path == prefix or path.startswith(prefix + "/") for prefix in prefixes):
        return True
    if "/sources/" in path and (
        "/workflows" in path or path.endswith(("/run", "/agent", "/dry-run"))
    ):
        return True
    if path.startswith("/api/v1/stacks/") and path.endswith("/install"):
        return True
    return path.startswith("/jobs/")


def configure_routes(app: Any, enabled: bool) -> None:
    if not hasattr(app.state, "all_routes"):
        app.state.all_routes = list(app.router.routes)
    app.router.routes[:] = [
        route
        for route in app.state.all_routes
        if enabled or not legacy_route(getattr(route, "path", ""))
    ]
    app.openapi_schema = None
