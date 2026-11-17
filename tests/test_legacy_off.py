"""Opt-out startup leaves legacy execution and files untouched."""

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from scripthut import main, runtime
from scripthut.config_schema import GlobalSettings, ScriptHutConfig, SlurmBackendConfig, SSHConfig
from scripthut.legacy import configure_routes
from scripthut.runtime import BackendState, ConnectionStatus
from scripthut.scriptflow_server import ScriptFlowService


def settings(tmp_path):
    return ScriptHutConfig(
        settings=GlobalSettings(data_dir=tmp_path, legacy_runs_enabled=False),
        backends=[
            SlurmBackendConfig(
                name="cluster", ssh=SSHConfig(host="hpc-cluster.example.edu", user="alice")
            )
        ],
    )


def test_startup_routes_and_legacy_files_untouched(tmp_path, monkeypatch):
    config = settings(tmp_path)
    directory = tmp_path / "workflows"
    directory.mkdir()
    old = directory / "existing.json"
    old.write_bytes(b'{"legacy":"do not load or modify"}')
    before = old.read_bytes(), old.stat().st_mtime_ns

    def forbidden(*args, **kwargs):
        pytest.fail("Legacy manager/storage constructed")

    monkeypatch.setattr(runtime, "RunManager", forbidden)
    monkeypatch.setattr(runtime, "RunStorageManager", forbidden)
    driver = SimpleNamespace(get_jobs=AsyncMock(side_effect=AssertionError("scheduler query")))
    ssh = SimpleNamespace(
        is_connected=True,
        disconnect=AsyncMock(),
        run_command=AsyncMock(side_effect=AssertionError("remote command")),
    )
    backend = BackendState(
        name="cluster",
        backend_type="slurm",
        backend=driver,
        ssh_client=ssh,
        status=ConnectionStatus(connected=True, host="hpc-cluster.example.edu"),
    )
    monkeypatch.setattr(runtime, "init_backend", AsyncMock(return_value=backend))
    state = main.AppState()
    monkeypatch.setattr(main, "state", state)
    monkeypatch.setattr(main, "load_config", lambda *args, **kwargs: config)
    monkeypatch.setattr(main, "scriptflow_service", ScriptFlowService(state))
    from fastapi import FastAPI

    from scripthut.scriptflow_routes import make_scriptflow_router

    app = FastAPI(lifespan=main.lifespan)
    app.include_router(make_scriptflow_router(main.scriptflow_service, main.templates))

    @app.get("/runs")
    async def legacy():
        raise AssertionError("legacy route executed")

    try:
        with TestClient(app) as client:
            for _ in range(100):
                if state.startup_phase is None:
                    break
                time.sleep(0.01)
            assert state.startup_phase is None
            assert state.run_manager is None and state.run_storage is None
            assert all(getattr(route, "path", "") != "/runs" for route in app.routes)
            assert client.get("/runs").status_code == 404
            page = client.get("/scriptflow")
            assert page.status_code == 200 and 'href="/runs"' not in page.text
            assert client.get("/api/v1/scriptflow/runs").json() == []
            assert not ssh.run_command.called and not driver.get_jobs.called
        assert (old.read_bytes(), old.stat().st_mtime_ns) == before
        assert ssh.disconnect.await_count == 1
    finally:
        main.templates.env.globals["legacy_runs_enabled"] = True


async def test_off_poll_never_touches_legacy_manager(tmp_path, monkeypatch):
    state = main.AppState(config=settings(tmp_path))

    class Forbidden:
        def __getattribute__(self, name):
            raise AssertionError("legacy access: " + name)

    state.run_manager = Forbidden()
    state.run_storage = Forbidden()
    monkeypatch.setattr(main, "state", state)

    async def refresh():
        state._shutdown_event.set()

    service = SimpleNamespace(poll=AsyncMock(side_effect=refresh))
    monkeypatch.setattr(main, "scriptflow_service", service)
    await main.poll_jobs()
    assert service.poll.await_count == 1


def test_route_filter_restores_default():
    from fastapi import FastAPI

    app = FastAPI()

    @app.get("/api/v1/runs")
    def run():
        return {}

    original = list(app.routes)
    configure_routes(app, False)
    assert all(getattr(r, "path", "") != "/api/v1/runs" for r in app.routes)
    configure_routes(app, True)
    assert app.routes == original
