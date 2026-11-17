"""ScriptFlow server projection without legacy execution."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.testclient import TestClient

from scripthut.config_schema import GlobalSettings, ScriptHutConfig
from scripthut.scriptflow import CachedRun, Reference
from scripthut.scriptflow_routes import LOG_CAP, make_scriptflow_router
from scripthut.scriptflow_server import CombinedRead, ScriptFlowService
from tests.test_scriptflow import RUN_DIRECTORY, FakeTransport, fresh_record
from tests.test_scriptflow_launch import LocalTransport


@pytest.fixture
def service(tmp_path):
    transport = FakeTransport(json.dumps(fresh_record()))
    state = SimpleNamespace(
        config=ScriptHutConfig(settings=GlobalSettings(data_dir=tmp_path)),
        backends={"cluster": SimpleNamespace(ssh_client=transport, enabled=True)},
        wait_for_poll=AsyncMock(),
    )
    return ScriptFlowService(state), transport


async def register(service, state="running"):
    cached = await service.store.register(Reference.create("cluster", RUN_DIRECTORY))
    cached.snapshot = fresh_record(state)
    await service.store.save(cached)
    return cached


async def test_poll_once_and_terminal_on_demand(service):
    service, transport = service
    cached = await register(service)
    await service.poll()
    assert len(transport.commands) == 1
    cached.snapshot = fresh_record("completed")
    await service.store.save(cached)
    await service.poll()
    assert len(transport.commands) == 1
    await service.refresh(cached.reference.id)
    assert len(transport.commands) == 2


async def test_failure_changes_only_observation(service):
    service, transport = service
    cached = await register(service)
    cached.controller = {"state": "running", "observed_at": "earlier"}
    cached.cancel_present = True
    await service.store.save(cached)
    before = cached.model_dump()
    transport.error = OSError("offline")
    after = (await service.refresh(cached.reference.id)).model_dump()
    for key in ("last_fetch_failed", "last_fetch_at", "problem", "fetch_error"):
        before.pop(key)
        after.pop(key)
    assert before == after


@pytest.mark.parametrize(
    "queue,accounting,code,expected",
    [
        ("RUNNING", "RUNNING", 0, "running"),
        ("", "COMPLETED", 0, "ended"),
        ("", "", 0, "unknown"),
        ("", "COMPLETED", 1, "unknown"),
    ],
)
async def test_combined_one_command(tmp_path, queue, accounting, code, expected):
    bins = tmp_path / "bin"
    bins.mkdir()
    for name, output in (("squeue", queue), ("sacct", accounting)):
        path = bins / name
        path.write_text(f"#!/bin/sh\nprintf '%s\\n' '{output}'\nexit {code}\n")
        path.chmod(0o755)
    local = LocalTransport()
    calls = []

    async def run(command, timeout=30):
        calls.append(command)
        return await local.run_command(f'export PATH="{bins}:$PATH"\n' + command)

    transport = SimpleNamespace(run_command=run)
    combined = CombinedRead(transport, "1234")
    assert await combined.run_command("printf 'record'") == ("record", "", 0)
    assert len(calls) == 1 and combined.controller["state"] == expected


@pytest.fixture
def web(service):
    service, transport = service
    app = FastAPI()
    templates = Jinja2Templates(
        directory=str(Path(__file__).parents[1] / "src/scripthut/templates")
    )
    templates.env.globals["scripthut_version"] = "test"
    app.include_router(make_scriptflow_router(service, templates))
    with TestClient(app) as client:
        yield client, service, transport


def seed(service, raw=None, problem=None):
    cached = CachedRun(
        reference=Reference.create("cluster", RUN_DIRECTORY),
        snapshot=raw or fresh_record(),
        problem=problem,
    )
    service.store.directory.mkdir(parents=True, exist_ok=True)
    (service.store.directory / f"{cached.reference.id}.json").write_text(cached.model_dump_json())
    return cached.reference.id


def test_api_read_refresh_unknown_and_cancel(web):
    client, service, transport = web
    reference = seed(service)
    root = f"/api/v1/scriptflow/runs/{reference}"
    assert client.get("/api/v1/scriptflow/runs").status_code == 200
    assert client.get(root).json()["workflow"]["state"] == "running"
    assert not transport.commands
    assert client.get("/api/v1/scriptflow/runs/" + "0" * 32).status_code == 404
    assert client.post(root + "/refresh").json()["observation"]["status"] == "fresh"
    original = transport.run_command

    async def run(command, timeout=30):
        if "scripthut-cancel" in command:
            transport.commands.append(command)
            return "requested", "", 0
        return await original(command, timeout)

    transport.run_command = run
    assert client.post(root + "/cancel").json()["cancel_result"]["status"] == "requested"
    transport.content = json.dumps(fresh_record("completed"))
    transport.commands.clear()
    assert client.post(root + "/cancel").json()["cancel_result"]["status"] == "terminal"
    assert all("scripthut-cancel" not in command for command in transport.commands)
    transport.error = OSError("offline")
    assert client.post(root + "/refresh").json()["observation"]["status"] == "unreachable"
    assert client.post(root + "/cancel").json()["cancel_result"]["status"] == "unreachable"
    assert client.get(root + "/logs?task=first").status_code == 503


def test_logs_cap_and_containment(web):
    client, service, transport = web
    reference = seed(service)
    original = transport.run_command

    async def run(command, timeout=30):
        if "run.json" in command:
            return await original(command, timeout)
        assert f"head -c {LOG_CAP + 1}" in command
        return "x" * (LOG_CAP + 1), "", 0

    transport.run_command = run
    root = f"/api/v1/scriptflow/runs/{reference}/logs?task=first&attempt=1&tail=10"
    result = client.get(root)
    assert result.status_code == 200
    assert result.json()["truncated"] and len(result.json()["content"]) == LOG_CAP
    raw = fresh_record()
    raw["tasks"]["first"]["attempts"][0]["log"] = "/outside/log"
    transport.content = json.dumps(raw)
    assert client.get(root).status_code == 422
    assert client.get(root + "&tail=-1").status_code == 422


@pytest.mark.parametrize(
    "state,problem",
    [
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
def test_templates(web, state, problem):
    client, service, transport = web
    raw = fresh_record(state, cleanup=True)
    reference = seed(service, raw, problem)
    response = client.get(f"/scriptflow/{reference}")
    assert response.status_code == 200
    assert state in response.text and "Cleanup pending: True" in response.text
    if problem:
        assert problem in response.text
    assert client.get("/scriptflow").status_code == 200
    assert not transport.commands


def test_not_started_template(web):
    client, service, _ = web
    reference = seed(service)
    path = service.store.directory / f"{reference}.json"
    cached = CachedRun.model_validate_json(path.read_text())
    cached.snapshot = None
    cached.launch_key = "1" * 32
    cached.problem = "missing"
    path.write_text(cached.model_dump_json())
    assert "not started" in client.get(f"/scriptflow/{reference}").text


async def test_launched_terminal_poll_policy(service, monkeypatch):
    from tests.legacy_guards import RunManager
    from tests.legacy_guards import RunStorageManager

    service, transport = service

    def forbidden(*args, **kwargs):
        pytest.fail("Legacy manager/storage invoked")

    monkeypatch.setattr(RunManager, "__init__", forbidden)
    monkeypatch.setattr(RunStorageManager, "__init__", forbidden)
    cached = await register(service, "completed")
    cached.launch_key = "1" * 32
    cached.controller = {"state": "running", "observed_at": "earlier"}
    await service.store.save(cached)
    await service.poll()
    assert len(transport.commands) == 1
    cached.controller["state"] = "ended"
    await service.store.save(cached)
    await service.poll()
    assert len(transport.commands) == 1


async def test_sse_waits_without_fetching(service):
    service, transport = service
    router = make_scriptflow_router(service, None)
    endpoint = next(route.endpoint for route in router.routes if route.path == "/scriptflow/stream")
    request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
    response = await endpoint(request)
    event = await anext(response.body_iterator)
    assert event["event"] == "refresh"
    assert service.state.wait_for_poll.await_count == 1
    assert not transport.commands
    await response.body_iterator.aclose()
