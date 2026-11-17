"""Tests for the versioned JSON API exposed under ``/api/v1``."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripthut.api import make_api_router
from scripthut.config_schema import GitSourceConfig, PathSourceConfig

# -- fixtures ----------------------------------------------------------------




def _make_state(
    sources: list | None = None,
    source_workflows: dict | None = None,
):
    """Build a minimal AppState-like object that satisfies the router.

    ``sources`` covers both GitSourceConfig and PathSourceConfig — the
    fixture mirrors what ``state.config.sources`` looks like on a live
    server. ``source_workflows`` is the cached discovery result keyed by
    source name; defaults to empty dict.
    """
    state = MagicMock()
    state.config_error = None
    state.backends = {}
    state.source_manager = None
    state.source_workflows = source_workflows or {}
    if sources is not None:
        s_list = sources
        state.config = MagicMock()
        state.config.sources = s_list
        state.config.get_source = lambda name: next(
            (s for s in s_list if s.name == name), None,
        )
    else:
        state.config = None
    state.notify_poll = MagicMock()
    return state


def _client(state) -> TestClient:
    app = FastAPI()
    app.include_router(make_api_router(state))
    return TestClient(app)


def test_list_sources_returns_configured_sources():
    sources = [
        PathSourceConfig(name="local", backend="cluster", path="/r/s", description="d"),
        GitSourceConfig(name="git", url="git@h:o/r.git", branch="main"),
    ]
    state = _make_state(sources=sources)

    resp = _client(state).get("/api/v1/sources")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data["sources"]) == 2
    by_name = {s["name"]: s for s in data["sources"]}
    assert by_name["local"]["type"] == "path"
    assert by_name["local"]["path"] == "/r/s"
    assert by_name["local"]["description"] == "d"
    assert by_name["git"]["type"] == "git"
    assert by_name["git"]["url"] == "git@h:o/r.git"






def test_view_source_unknown_returns_404():
    state = _make_state(sources=[])

    resp = _client(state).get("/api/v1/sources/missing")

    assert resp.status_code == 404


# -- /backends ----------------------------------------------------------------


def test_list_backends_returns_configured_with_status():
    from scripthut.config_schema import SlurmBackendConfig, SSHConfig

    slurm = SlurmBackendConfig(
        name="cluster-a",
        ssh=SSHConfig(host="login.example.com", user="me"),
        max_concurrent=100,
    )
    state = _make_state()
    state.config = MagicMock()
    state.config.backends = [slurm]
    bs = MagicMock()
    bs.status.connected = True
    bs.backend_type = "slurm"
    state.backends = {"cluster-a": bs}

    resp = _client(state).get("/api/v1/backends")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body["backends"]) == 1
    entry = body["backends"][0]
    assert entry["name"] == "cluster-a"
    assert entry["type"] == "slurm"
    assert entry["connected"] is True


def test_list_backends_no_config_returns_empty():
    state = _make_state()
    state.config = None
    resp = _client(state).get("/api/v1/backends")
    assert resp.json() == {"backends": []}


# -- /sources/{name}/run -----------------------------------------------------












# -- /sources/{name}/config --------------------------------------------------










# -- /sources/{name}/sync ----------------------------------------------------




def test_sync_unknown_source_returns_404():
    state = _make_state(sources=[])
    resp = _client(state).post("/api/v1/sources/missing/sync")
    assert resp.status_code == 404


def test_sync_git_source_failure_returns_200_with_error_field():
    """Per-source failures are reported in-band, not as a 5xx.

    A 5xx would make it look like the server itself is broken; we want
    callers to see "this one source failed, others may still work."
    """
    sources = [GitSourceConfig(name="git", url="git@h:o/r.git", branch="main")]
    sm = MagicMock()
    sm.sync_source = AsyncMock(side_effect=RuntimeError("clone failed"))
    state = _make_state(sources=sources)
    state.source_manager = sm
    state.source_statuses = {}

    resp = _client(state).post("/api/v1/sources/git/sync")

    assert resp.status_code == 200
    body = resp.json()
    assert "clone failed" in body["error"]


# -- /sources/sync (all) ------------------------------------------------------


def test_sync_all_sources_iterates_every_configured_source():
    from scripthut.sources.git import SourceStatus
    sources = [
        GitSourceConfig(name="a", url="git@h:o/a.git", branch="main"),
        GitSourceConfig(name="b", url="git@h:o/b.git", branch="main"),
    ]
    sm = MagicMock()

    async def fake_sync(n: str) -> SourceStatus:
        return SourceStatus(
            name=n, path=Path(f"/tmp/{n}"), cloned=True, branch="main",
            last_commit=f"{n}-sha",
        )

    sm.sync_source = AsyncMock(side_effect=fake_sync)
    state = _make_state(sources=sources)
    state.source_manager = sm
    state.source_statuses = {}

    resp = _client(state).post("/api/v1/sources/sync")

    assert resp.status_code == 200
    body = resp.json()
    names = [r["name"] for r in body["sources"]]
    assert names == ["a", "b"]


def test_sync_all_sources_empty_config_returns_empty_list():
    state = _make_state()
    state.config = None
    resp = _client(state).post("/api/v1/sources/sync")
    assert resp.status_code == 200
    assert resp.json() == {"sources": []}


# -- /runs --------------------------------------------------------------------








# -- /runs/{run_id}/rerun -----------------------------------------------------




# -- /runs/{run_id}/tasks/{task_id}/logs --------------------------------------












# -- /stacks/{name}/check ----------------------------------------------------
















# -- /stacks/{name}/install --------------------------------------------------














# -- /stacks/{name} (DELETE) -------------------------------------------------






# -- /health ------------------------------------------------------------------


def test_health_reports_state():
    state = _make_state()
    resp = _client(state).get("/api/v1/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "config_loaded" in body
