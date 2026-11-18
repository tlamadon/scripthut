"""The original dashboard reads cached ScriptFlow observations."""

import shlex
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripthut import main
from scripthut.backends.base import ClusterInfo, PartitionInfo, QuotaInfo
from scripthut.config_schema import GlobalSettings, ScriptHutConfig
from scripthut.models import ConnectionStatus, HPCJob, JobState
from scripthut.runtime import BackendState
from scripthut.scriptflow import CachedRun, Reference
from scripthut.scriptflow_activity import build_activity_grid, build_hourly_usage
from scripthut.scriptflow_routes import make_scriptflow_router
from scripthut.scriptflow_server import ScriptFlowService
from scripthut.scriptflow_web import run_view
from scripthut.scriptflow_web_observation import refresh_web_cache
from scripthut.web_routes import make_web_router
from tests.test_scriptflow import fresh_record
from tests.test_scriptflow_launch import setup  # noqa: F401
from tests.test_scriptflow_sources import checkout, tree  # noqa: F401


@pytest.fixture
def dashboard(tmp_path):
    state = main.AppState(config=ScriptHutConfig(settings=GlobalSettings(data_dir=tmp_path)))
    service = ScriptFlowService(state)
    app = FastAPI()
    app.include_router(make_web_router(state, service, main.templates))
    app.include_router(make_scriptflow_router(service, main.templates))
    with TestClient(app) as client:
        yield client, state, service


def save(service, raw=None, problem=None):
    cached = CachedRun(
        reference=Reference.create("cluster", "/data/project/.sf/runs/example"),
        snapshot=raw,
        problem=problem,
        last_successful_fetch=datetime.now(UTC),
    )
    if raw is None:
        cached.launch_key = "1" * 32
        cached.problem = "missing"
    service.store.directory.mkdir(parents=True, exist_ok=True)
    path = service.store.directory / (cached.reference.id + ".json")
    path.write_text(cached.model_dump_json())
    return cached, path


@pytest.mark.parametrize(
    "state_name,problem",
    [
        ("empty", None),
        ("not started", None),
        ("running", None),
        ("completed", None),
        ("failed", None),
        ("cancelled", None),
        ("interrupted", None),
        ("running", "stale"),
        ("running", "unreachable"),
        ("running", "unsupported_version"),
    ],
)
def test_pages_are_cached(dashboard, state_name, problem):
    client, state, service = dashboard
    transport = SimpleNamespace(
        run_command=AsyncMock(side_effect=AssertionError("Page contacted backend"))
    )
    state.backends["cluster"] = BackendState(
        name="cluster",
        backend_type="slurm",
        ssh_client=transport,
        backend=None,
        status=ConnectionStatus(connected=False, host="cluster.example.edu"),
    )
    cached = None
    if state_name != "empty":
        cached, path = save(
            service, fresh_record(state_name) if state_name != "not started" else None, problem
        )
        before = path.read_bytes()
    for url in (
        "/",
        "/runs",
        "/scriptflow",
        "/backends",
        "/jobs",
        "/sources",
        "/terminals",
        "/disk",
    ):
        response = client.get(url)
        assert response.status_code == 200, (url, response.text)
        if url in ("/", "/runs", "/scriptflow") and cached:
            assert state_name in response.text
    if cached:
        for prefix in ("/runs/", "/scriptflow/"):
            response = client.get(prefix + cached.reference.id)
            assert response.status_code == 200
            assert state_name in response.text
            if problem:
                assert problem in response.text
        assert path.read_bytes() == before
    transport.run_command.assert_not_called()
    assert client.get("/").url.path == "/"
    for url in (
        "/api/v1/runs",
        "/api/v1/tasks/run",
        "/disk/cleanup",
        "/terminal/interactive/cluster",
    ):
        assert client.get(url).status_code == 404


def test_cpu_hours_and_buckets():
    now = datetime(2026, 11, 18, 1, 30).astimezone()
    start = now - timedelta(hours=2)
    raw = fresh_record()
    raw["heartbeat"] = now.isoformat()
    task = raw["tasks"]["first"]
    task["cpus"] = 4
    task["memory"] = 2.5
    task["walltime"] = 30
    task["partition"] = "short"
    task["attempts"] = [
        dict(
            attempt_id="one",
            state="running",
            started_at=start.isoformat(),
            submitted_at=(start - timedelta(minutes=20)).isoformat(),
        )
    ]
    cached = CachedRun(
        reference=Reference.create("cluster", "/data/project/.sf/runs/example"), snapshot=raw
    )
    run = run_view(cached, now)
    assert run.total_cpu_hours == 8 and not run.approximate
    grid = build_activity_grid(run.intervals, now=now)
    assert [d.cpu_hours for d in grid.days[-2:]] == [2, 6]
    hourly = build_hourly_usage(run.intervals, now=now)
    assert [h.total_cpu_hours for h in hourly.hours[-3:]] == [2, 4, 2]
    assert hourly.totals == {"manual": 8}
    del task["cpus"]
    del task["attempts"][0]["started_at"]
    task["attempts"][0]["submitted_at"] = start.isoformat()
    run = run_view(cached, now)
    assert run.total_cpu_hours == 2 and run.approximate
    task["attempts"][0].update(
        state="completed", finished_at=(now - timedelta(hours=1)).isoformat()
    )
    assert run_view(cached, now).total_cpu_hours == 1


def test_backend_status_and_jobs(dashboard):
    client, state, _ = dashboard
    partition = PartitionInfo("short", "up", 12, 20, 0, 32, 2, "02:00:00", 65536, "fast", True)
    cluster = ClusterInfo(
        [partition], {"Resources": 1}, QuotaInfo(account="example-account", fair_share=0.75)
    )
    job = HPCJob(
        "123",
        "controller",
        "alice",
        JobState.RUNNING,
        "short",
        "00:10:00",
        "node-1",
        4,
        "8G",
        datetime.now(UTC),
        datetime.now(UTC) - timedelta(minutes=10),
    )
    state.backends["cluster"] = BackendState(
        name="cluster",
        backend_type="slurm",
        ssh_client=None,
        backend=None,
        jobs=[job],
        status=ConnectionStatus(True, "cluster.example.edu", cluster_info=cluster),
    )
    html = client.get("/backends").text
    for text in (
        "short",
        "20",
        "32",
        "64",
        "02:00:00",
        "fast",
        "Resources",
        "example-account",
        "123",
    ):
        assert text in html
    assert "controller" in client.get("/jobs").text
    assert "example-account" in client.get("/").text
    assert "/cancel" not in html and "/delete" not in html
    state.backends["cluster"].status.connected = False
    assert "Disconnected" in client.get("/").text


async def test_overview_sse_is_passive(dashboard):
    _, state, service = dashboard
    save(service, fresh_record())
    state.wait_for_poll = AsyncMock(return_value=True)
    router = make_web_router(state, service, main.templates)
    endpoint = next(r.endpoint for r in router.routes if r.path == "/overview/stream")
    request = SimpleNamespace(
        is_disconnected=AsyncMock(return_value=False), url=SimpleNamespace(path="/")
    )
    response = await endpoint(request)
    event = await anext(response.body_iterator)
    assert event["event"] == "overview-update"
    assert "analysis" in event["data"] and "manual" in event["data"]
    assert state.wait_for_poll.await_count == 1
    await response.body_iterator.aclose()


def test_source_launch_repeats_existing_handler(checkout, monkeypatch):  # noqa: F811
    c = checkout
    from scripthut.scriptflow_launch import ControllerBackend

    monkeypatch.setattr("scripthut.ssh.config._ssh_client_for", lambda settings: c.shell)
    submit = AsyncMock(return_value="123")
    monkeypatch.setattr(ControllerBackend, "submit_job", submit)
    state = main.AppState(config=c.config)
    service = ScriptFlowService(state)
    app = FastAPI()
    app.include_router(make_web_router(state, service, main.templates))
    with TestClient(app) as client:
        form = dict(source="example", flow="first", backend="cluster", run_id="web-example")
        first = client.post("/sources/launch", data=form)
        assert first.status_code == 200, first.text
        assert first.json()["job_id"] == "123"
        assert client.post("/sources/launch", data=form).json()["job_id"] == "123"
        assert submit.await_count == 1
        form["flow"] = "second"
        assert client.post("/sources/launch", data=form).status_code == 409
        assert submit.await_count == 1


@pytest.mark.parametrize(
    "reason",
    [
        "/data/project: tracked files have uncommitted changes",
        "/data/project: running ScriptFlow run",
        "/data/project: stale running run; use scriptflow cleanup",
        "/data/project: cannot fast-forward; push or reconcile commits",
        "/data/project: different source repository",
        "/data/project: flow_dir escapes repository",
        "Submission is ambiguous; no resubmission",
    ],
)
def test_launch_refusals_verbatim(dashboard, monkeypatch, reason):
    client, _, _ = dashboard
    handler = AsyncMock(side_effect=ValueError(reason))
    monkeypatch.setattr("scripthut.web_routes.handle_controller", handler)
    response = client.post(
        "/sources/launch",
        data=dict(source="example", flow="analysis", backend="cluster", run_id="web-example"),
    )
    assert response.status_code == 409 and response.json() == {"detail": reason}


async def test_disk_poll_is_read_only(checkout):  # noqa: F811
    c = checkout
    root = c.directory
    (root / "results").mkdir(parents=True)
    (root / "results" / "output.dat").write_bytes(b"output")
    (root / ".sf/runs/example").mkdir(parents=True)
    (root / ".sf/runs/example/run.json").write_text("{}")
    images = root.parent / "images"
    images.mkdir()
    (images / "image.sif").write_bytes(b"image")
    c.config.get_backend("cluster").image_dir = str(images)
    state = main.AppState(config=c.config)
    state.backends["cluster"] = BackendState(
        name="cluster",
        backend_type="slurm",
        backend=None,
        ssh_client=c.shell,
        status=ConnectionStatus(True, "cluster.example.edu"),
    )
    service = ScriptFlowService(state)
    before = tree(root.parent)
    await refresh_web_cache(state, service)
    assert tree(root.parent) == before
    rows = state.disk_usage["cluster"]["rows"]
    assert {"working directory", "results folder", "run records", "images"} <= {
        r["kind"] for r in rows
    }
    assert all(row["size"] is not None for row in rows)
    for command in c.shell.commands:
        assert shlex.split(command)[0] in ("git", "find", "du")
        assert not any(
            word in shlex.split(command) for word in ("rm", "delete", "cleanup", "mkdir", "touch")
        )
    app = FastAPI()
    app.include_router(make_web_router(state, service, main.templates))
    count = len(c.shell.commands)
    with TestClient(app) as client:
        html = client.get("/disk").text
        assert "output.dat" not in html and str(root / "results") in html
        assert "Read-only" in html and "/disk/cleanup" not in html and "/disk/delete" not in html
    assert len(c.shell.commands) == count


def test_resources_attempts_source_images_logs_and_filters(dashboard):
    client, state, service = dashboard
    raw = fresh_record()
    raw["tasks"]["first"].update(cpus=8, memory=4.5, walltime=60, partition="short")
    raw["tasks"]["first"]["attempts"][0].update(started_at=datetime.now(UTC).isoformat())
    cached, path = save(service, raw)
    cached.source = dict(name="example", branch="main", short_commit="123abcdef", detached=False)
    cached.images = [
        dict(
            name="app",
            ref="registry.example.org/app@sha256:" + "a" * 64,
            path="/data/images/app.sif",
        )
    ]
    path.write_text(cached.model_dump_json())
    response = client.get("/runs/" + cached.reference.id)
    for text in (
        "8 CPU",
        "4.5",
        "short",
        "Attempt 1",
        "Attempt 2",
        "exit 7",
        "Timeline",
        "123abcdef",
        "/data/images/app.sif",
        "Branch: main",
    ):
        assert text in response.text
    assert "analysis" in client.get("/runs?source=example&state=running").text
    assert "analysis" not in client.get("/runs?source=other").text
    assert client.get("/runs/" + cached.reference.id + "/logs?task=first").status_code == 200
    assert client.get("/runs/" + cached.reference.id + "/logs?task=missing").status_code == 404
    assert (
        client.get("/runs/" + cached.reference.id + "/logs?task=first&attempt=8").status_code == 404
    )
    assert cached.snapshot == raw


def test_source_page_uses_cached_flows(dashboard):
    from scripthut.config_schema import GitSourceConfig

    client, state, _ = dashboard
    source = GitSourceConfig(
        name="example",
        url="https://git.example.org/org/repo.git",
        workdir="/data/project",
        flow_dir="flows",
    )
    state.config.sources = [source]
    state.web_sources = [
        dict(
            config=source,
            backend="cluster",
            workdir="/data/project",
            flow_dir="flows",
            flows=["analysis"],
            error=None,
        )
    ]
    html = client.get("/sources").text
    assert "analysis" in html and "/data/project" in html and "flows" in html
    assert 'action="/sources/launch"' in html and 'name="run_id"' in html


def test_settings_and_terminal_layout(tmp_path, monkeypatch):
    config_path = tmp_path / "scripthut.yaml"
    config_path.write_text("settings:\n  server_host: 127.0.0.1\n  server_port: 8000\n")
    monkeypatch.setattr(main, "_config_path", config_path)
    monkeypatch.setattr(main.state, "startup_phase", None)
    monkeypatch.setattr(main.state, "config", ScriptHutConfig())
    backend = BackendState(
        name="cluster", backend_type="slurm", status=ConnectionStatus(True, "cluster.example.edu")
    )
    monkeypatch.setattr(main.state, "backends", {"cluster": backend})
    client = TestClient(main.app)
    html = client.get("/settings").text
    assert "bg-gray-100" in html and "server_host" in html
    terminal = client.get("/terminal/cluster?node=compute-1&session=existing").text
    assert "xterm" in terminal and "const session =" in terminal
    assert "/terminal/interactive" not in terminal and "Submitting job" not in terminal
    assert config_path.read_text() == "settings:\n  server_host: 127.0.0.1\n  server_port: 8000\n"
