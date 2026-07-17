"""Tests for the fast-bind startup path.

The lifespan yields (and uvicorn binds) before backends/storage are
initialized; ``state.startup_phase`` marks the in-flight phase and the
gate middleware answers for the not-yet-ready app:

- HTML routes → 503 with a self-contained auto-refreshing starting page
- ``/api/...`` → structured JSON 503 carrying the phase
- ``/ping`` and the health endpoints stay live throughout
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

import scripthut.main as main_module
from scripthut.config_schema import (
    GlobalSettings,
    ScriptHutConfig,
    SlurmBackendConfig,
    SSHConfig,
)
from scripthut.runtime import BackendState, ConnectionStatus, init_runtime


@pytest.fixture
def starting_client():
    """TestClient over the real app with startup_phase set (and restored)."""
    main_module.state.startup_phase = "connecting backends"
    try:
        yield TestClient(main_module.app)
    finally:
        main_module.state.startup_phase = None


class TestStartupGateMiddleware:
    def test_html_route_gets_starting_page(self, starting_client):
        resp = starting_client.get("/")
        assert resp.status_code == 503
        assert resp.headers["retry-after"] == "2"
        assert "ScriptHut is starting" in resp.text
        assert "connecting backends" in resp.text
        # Self-refreshing so the user lands on the dashboard when ready.
        assert 'http-equiv="refresh"' in resp.text

    def test_api_route_gets_structured_503(self, starting_client):
        resp = starting_client.get("/api/v1/runs")
        assert resp.status_code == 503
        body = resp.json()
        assert body["starting"] == "connecting backends"
        assert "Server is starting" in body["detail"]

    def test_ping_stays_live(self, starting_client):
        resp = starting_client.get("/ping")
        assert resp.status_code == 200
        assert resp.json()["status"] == "pong"

    def test_api_health_stays_live_and_reports_phase(self, starting_client):
        resp = starting_client.get("/api/v1/health")
        assert resp.status_code == 200
        assert resp.json()["starting"] == "connecting backends"

    def test_gate_open_when_ready(self):
        main_module.state.startup_phase = None
        client = TestClient(main_module.app)
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200
        assert resp.json()["starting"] is None


class TestInitRuntimePhases:
    @pytest.mark.asyncio
    async def test_reports_phases_and_gathers_ssh_backends(self, tmp_path: Path):
        config = ScriptHutConfig(
            backends=[
                SlurmBackendConfig(
                    name="a", ssh=SSHConfig(host="a.example", user="u"),
                ),
                SlurmBackendConfig(
                    name="b", ssh=SSHConfig(host="b.example", user="u"),
                ),
            ],
            settings=GlobalSettings(data_dir=tmp_path),
        )

        def fake_state(cfg):
            return BackendState(
                name=cfg.name,
                backend_type="slurm",
                ssh_client=None,
                backend=None,
                status=ConnectionStatus(connected=False, host=cfg.ssh.host),
                clone_dir=cfg.clone_dir,
            )

        phases: list[str] = []
        with patch(
            "scripthut.runtime.init_backend",
            new=AsyncMock(side_effect=lambda c: fake_state(c)),
        ) as init_mock:
            runtime = await init_runtime(config, on_phase=phases.append)

        assert phases == ["connecting backends", "restoring runs"]
        assert list(runtime.backends) == ["a", "b"]
        assert init_mock.await_count == 2
