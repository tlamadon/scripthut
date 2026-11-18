"""Regression checks from the live interface review."""

import copy
import shutil
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripthut import main
from scripthut.models import ConnectionStatus
from scripthut.runtime import BackendState
from scripthut.scriptflow import Reference
from scripthut.scriptflow_routes import make_scriptflow_router
from scripthut.scriptflow_server import ScriptFlowService
from scripthut.scriptflow_sources import source_rows
from scripthut.scriptflow_web_observation import refresh_web_cache
from tests.test_scriptflow import fresh_record
from tests.test_scriptflow_launch import setup  # noqa: F401
from tests.test_scriptflow_sources import checkout  # noqa: F401
from tests.test_web_restoration import dashboard, save  # noqa: F401


@pytest.mark.parametrize("cached_repository", [False, True])
async def test_sources_not_delivered_on_two_backends(checkout, dashboard, cached_repository):  # noqa: F811
    c = checkout
    client, state, _ = dashboard
    if cached_repository:
        directory = c.config.settings.sources_cache_dir_resolved / c.source.name
        directory.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(c.repo, directory)
    else:
        c.source.local_path = c.repo
    state.config.sources = [c.source]
    state.config.settings.sources_cache_dir = c.config.settings.sources_cache_dir
    connections = {
        name: SimpleNamespace(ssh_client=c.shell, enabled=True) for name in ("alpha", "beta")
    }
    state.web_sources = await source_rows(state.config, connections)
    assert all(
        row["status"] == "not_delivered" and row["error"] is None for row in state.web_sources
    )
    assert all(row["flows"] == ["first", "second"] for row in state.web_sources)
    html = client.get("/sources").text
    assert html.count("Not delivered to alpha, beta yet") == 1
    assert "2 flows" in html and "Error:" not in html and "fatal:" not in html
    assert "first" in html and "second" in html
    assert not c.directory.exists()
    broken = SimpleNamespace(run_command=AsyncMock(return_value=("", "connection failed", 255)))
    rows = await source_rows(
        state.config, {"alpha": SimpleNamespace(ssh_client=broken, enabled=True)}
    )
    assert rows[0]["error"] == "connection failed" and rows[0]["status"] != "not_delivered"


async def test_disk_missing_and_log_folder_labels(checkout):  # noqa: F811
    c = checkout
    state = main.AppState(config=c.config)
    state.backends["cluster"] = BackendState(
        name="cluster",
        backend_type="slurm",
        ssh_client=c.shell,
        status=ConnectionStatus(True, "cluster.example.edu"),
    )
    service = ScriptFlowService(state)
    await refresh_web_cache(state, service)
    rows = state.disk_usage["cluster"]["rows"]
    root = next(row for row in rows if row["path"] == str(c.directory))
    assert root["error"] == "not present on this backend"
    assert all("du:" not in (row["error"] or "") for row in rows)
    assert not c.directory.exists()
    for name in ("log", "results"):
        (c.directory / name).mkdir(parents=True)
        (c.directory / name / "file").write_text("example")
    cached = await service.store.register(
        Reference.create("cluster", str(c.directory / ".sf/runs/example"))
    )
    cached.snapshot = fresh_record()
    cached.snapshot["tasks"]["first"]["outputs"] = [str(c.directory / "results/file")]
    await service.store.save(cached)
    await refresh_web_cache(state, service)
    rows = {row["path"]: row for row in state.disk_usage["cluster"]["rows"]}
    assert rows[str(c.directory / "log")]["kind"] == "folder"
    assert rows[str(c.directory / "results")]["kind"] == "results folder"


def large_record():
    raw = fresh_record()
    template = raw["tasks"]["first"]
    raw["tasks"] = {}
    for index in range(3000):
        task = copy.deepcopy(template)
        task.update(
            task_id=f"task-{index:04d}",
            name=f"analysis-{index:04d}",
            state="running" if index % 2 else "completed",
        )
        raw["tasks"][task["task_id"]] = task
    return raw


def test_large_run_pages_and_summary_are_bounded(dashboard):  # noqa: F811
    client, _, service = dashboard
    cached, _ = save(service, large_record())
    base = "/runs/" + cached.reference.id
    first = client.get(base)
    assert first.status_code == 200
    assert len(first.content) < 256 * 1024
    assert "Tasks 1–20 of 3000" in first.text and "running (1500)" in first.text
    assert "analysis-0019" in first.text and "analysis-0020" not in first.text
    second = client.get(base + "?page=2")
    assert "analysis-0020" in second.text and "analysis-0000" not in second.text
    filtered = client.get(base + "?task_state=running&search=analysis-29")
    assert "50 matching" in filtered.text and "analysis-2901" in filtered.text
    assert "analysis-2900" not in filtered.text
    assert "task_state=running" in filtered.text and "search=analysis-29" in filtered.text
    assert client.get(base + "?page=0").status_code == 422
    log = client.get(base + "/logs?task=task-2999")
    assert log.status_code == 200 and len(log.content) < 32 * 1024
    assert "analysis-2999" in log.text and "analysis-0000" not in log.text
    summary = client.get("/api/v1/scriptflow/runs")
    assert len(summary.content) < 8 * 1024
    assert summary.json()[0]["task_counts"] == {"completed": 1500, "running": 1500}
    assert "tasks" not in summary.json()[0]["workflow"]
    assert (
        len(client.get("/api/v1/scriptflow/runs?summary=false").json()[0]["workflow"]["tasks"])
        == 3000
    )


async def test_large_run_sse_preserves_page_and_filters(dashboard):  # noqa: F811
    _, state, service = dashboard
    cached, _ = save(service, large_record())
    state.wait_for_poll = AsyncMock(return_value=True)
    router = make_scriptflow_router(service, main.templates)
    endpoint = next(
        route.endpoint for route in router.routes if route.path == "/runs/{reference}/events"
    )
    request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
    response = await endpoint(
        request, cached.reference.id, page=2, task_state="running", search="analysis-29"
    )
    fragments = [await anext(response.body_iterator) for _ in range(5)]
    assert sum(len(event["data"].encode()) for event in fragments) < 256 * 1024
    assert "analysis-2941" in "".join(event["data"] for event in fragments)
    assert "analysis-2901" not in "".join(event["data"] for event in fragments)
    assert state.wait_for_poll.await_count == 1
    await response.body_iterator.aclose()


@pytest.mark.parametrize("minutes,display", [(0.4, "0.4 min"), (120, "2.0 hours")])
def test_missing_resources_and_cpu_units(dashboard, minutes, display):  # noqa: F811
    client, _, service = dashboard
    raw = fresh_record("completed")
    finish = datetime.now(UTC)
    raw["tasks"]["first"]["attempts"] = [
        dict(
            attempt_id="one",
            state="completed",
            started_at=(finish - timedelta(minutes=minutes)).isoformat(),
            finished_at=finish.isoformat(),
        )
    ]
    cached, _ = save(service, raw)
    html = client.get("/runs/" + cached.reference.id).text
    assert "None" not in html and "unknown, unknown" not in html
    assert "— CPU, —, —" in html
    assert "CPU time" in html and "CPU Hours" not in html and display in html
    assert "assuming 1" in html


@pytest.mark.parametrize("managed", [False, True])
def test_manual_controller_label(dashboard, managed):  # noqa: F811
    client, _, service = dashboard
    cached, path = save(service, fresh_record())
    if managed:
        cached.launch_key = "1" * 32
        path.write_text(cached.model_dump_json())
    for url in ("/", "/runs/" + cached.reference.id):
        html = client.get(url).text
        if managed:
            assert "not managed by ScriptHut" not in html and "unknown" in html
        else:
            assert "not managed by ScriptHut" in html
            assert "unknown / not managed" not in html
