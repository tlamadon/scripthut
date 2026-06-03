"""Tests for the versioned JSON API exposed under ``/api/v1``."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripthut.api import make_api_router
from scripthut.config_schema import GitSourceConfig, PathSourceConfig
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition

# -- fixtures ----------------------------------------------------------------


def _make_run(
    run_id: str = "run123",
    workflow_name: str = "demo",
    backend_name: str = "test-cluster",
    statuses: list[RunItemStatus] | None = None,
) -> Run:
    statuses = statuses or [RunItemStatus.SUBMITTED, RunItemStatus.PENDING]
    items = [
        RunItem(
            task=TaskDefinition(id=f"t{i}", name=f"task-{i}", command="true"),
            status=status,
        )
        for i, status in enumerate(statuses)
    ]
    return Run(
        id=run_id,
        workflow_name=workflow_name,
        backend_name=backend_name,
        created_at=datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC),
        items=items,
        max_concurrent=4,
    )


def _make_state(
    run_manager: MagicMock | None = None,
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
    state.run_manager = run_manager
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
    state = _make_state(run_manager=MagicMock(), sources=sources)

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


def test_view_source_returns_metadata_and_workflows():
    sources = [PathSourceConfig(name="src", backend="cluster", path="/r/s")]
    from scripthut.sources.git import SourceWorkflow
    wfs = [
        SourceWorkflow(
            name="train", source_name="src", filename="train.json",
            tasks_json="[]",
        ),
    ]
    state = _make_state(
        run_manager=MagicMock(), sources=sources,
        source_workflows={"src": wfs},
    )

    resp = _client(state).get("/api/v1/sources/src")

    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "src"
    assert body["type"] == "path"
    assert body["workflows"] == ["train.json"]
    assert body["discover_error"] is None


def test_view_source_returns_metadata_when_no_workflows_cached():
    """No cached workflows is normal pre-sync, not an error."""
    sources = [GitSourceConfig(name="src", url="git@h:o/r.git", branch="main")]
    state = _make_state(run_manager=MagicMock(), sources=sources)

    resp = _client(state).get("/api/v1/sources/src")

    assert resp.status_code == 200
    body = resp.json()
    assert body["workflows"] == []
    assert body["discover_error"] is None


def test_view_source_unknown_returns_404():
    state = _make_state(run_manager=MagicMock(), sources=[])

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
    state = _make_state(run_manager=MagicMock())
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
    assert entry["max_concurrent"] == 100


def test_list_backends_no_config_returns_empty():
    state = _make_state()
    state.config = None
    resp = _client(state).get("/api/v1/backends")
    assert resp.json() == {"backends": []}


# -- /sources/{name}/run -----------------------------------------------------


def test_run_source_workflow_uses_source_default_backend():
    """Path source carries its own ``backend``; the call should pick it up
    when the request doesn't override.
    """
    from scripthut.sources.git import SourceWorkflow
    sources = [PathSourceConfig(name="src", backend="cluster", path="/r/s")]
    wfs = [
        SourceWorkflow(
            name="train", source_name="src", filename="train.json",
            tasks_json='[]',
        ),
    ]
    run = _make_run(workflow_name="src/train")
    rm = MagicMock()
    rm.create_run_from_source = AsyncMock(return_value=run)
    state = _make_state(
        run_manager=rm, sources=sources, source_workflows={"src": wfs},
    )

    resp = _client(state).post("/api/v1/sources/src/run?workflow=train.json")

    assert resp.status_code == 200
    rm.create_run_from_source.assert_awaited_once_with(
        "src", "train.json", "[]", backend="cluster",
    )


def test_run_source_workflow_passes_backend_override():
    from scripthut.sources.git import SourceWorkflow
    sources = [PathSourceConfig(name="src", backend="cluster", path="/r/s")]
    wfs = [
        SourceWorkflow(
            name="train", source_name="src", filename="train.json",
            tasks_json="[]",
        ),
    ]
    run = _make_run(workflow_name="src/train")
    rm = MagicMock()
    rm.create_run_from_source = AsyncMock(return_value=run)
    state = _make_state(
        run_manager=rm, sources=sources, source_workflows={"src": wfs},
    )

    resp = _client(state).post(
        "/api/v1/sources/src/run?workflow=train.json&backend=cluster-b"
    )

    assert resp.status_code == 200
    rm.create_run_from_source.assert_awaited_once_with(
        "src", "train.json", "[]", backend="cluster-b",
    )


def test_run_source_workflow_unknown_source_returns_404():
    state = _make_state(run_manager=MagicMock(), sources=[])
    resp = _client(state).post("/api/v1/sources/missing/run?workflow=x.json")
    assert resp.status_code == 404


def test_run_source_workflow_missing_workflow_returns_404():
    sources = [PathSourceConfig(name="src", backend="cluster", path="/r/s")]
    state = _make_state(
        run_manager=MagicMock(), sources=sources, source_workflows={"src": []},
    )
    resp = _client(state).post("/api/v1/sources/src/run?workflow=missing.json")
    assert resp.status_code == 404


def test_run_source_workflow_git_source_without_backend_errors():
    """Git sources don't carry a backend; without one in the query, 422.

    Path sources have a default; git sources don't (their backend field
    is deprecated/excluded), so the request must specify one.
    """
    sources = [GitSourceConfig(name="git", url="git@h:o/r.git", branch="main")]
    state = _make_state(run_manager=MagicMock(), sources=sources)

    resp = _client(state).post("/api/v1/sources/git/run?workflow=train.json")

    assert resp.status_code == 422


# -- /sources/{name}/config --------------------------------------------------


def test_source_config_returns_env_groups_and_stacks():
    """Happy path: project YAML present, returned as JSON for the CLI."""
    from scripthut.config_schema import ScriptHutConfig
    sources = [GitSourceConfig(name="src", url="git@h:o/r.git", branch="main")]
    project_cfg = ScriptHutConfig.model_validate({
        "env_groups": {"cuda": [{"set": {"CUDA_VISIBLE_DEVICES": "0"}}]},
        "stacks": [{"name": "py-ml", "prep": "echo prep"}],
    })
    rm = MagicMock()
    rm._load_source_project_config = AsyncMock(return_value=project_cfg)
    state = _make_state(run_manager=rm, sources=sources)

    resp = _client(state).get("/api/v1/sources/src/config")

    assert resp.status_code == 200
    body = resp.json()
    assert body["config_present"] is True
    assert body["env_groups"]["cuda"][0]["set"]["CUDA_VISIBLE_DEVICES"] == "0"
    assert body["stacks"][0]["name"] == "py-ml"


def test_source_config_missing_file_returns_empty_shape():
    """No scripthut.yaml at the source root — still 200, config_present=False."""
    sources = [GitSourceConfig(name="src", url="git@h:o/r.git", branch="main")]
    rm = MagicMock()
    rm._load_source_project_config = AsyncMock(return_value=None)
    state = _make_state(run_manager=rm, sources=sources)

    resp = _client(state).get("/api/v1/sources/src/config")

    assert resp.status_code == 200
    body = resp.json()
    assert body["config_present"] is False
    assert body["env"] == []
    assert body["env_groups"] == {}
    assert body["stacks"] == []


def test_source_config_unknown_source_returns_404():
    state = _make_state(run_manager=MagicMock(), sources=[])
    resp = _client(state).get("/api/v1/sources/missing/config")
    assert resp.status_code == 404


def test_source_config_forbidden_section_returns_422():
    """An actively-broken project YAML (`backends:` in a project-local
    file) must surface to the operator, not silently disappear.
    """
    sources = [GitSourceConfig(name="src", url="git@h:o/r.git", branch="main")]
    rm = MagicMock()
    rm._load_source_project_config = AsyncMock(
        side_effect=ValueError(
            "source 'src'/scripthut.yaml contains fields that belong in "
            "the user-global config: backends."
        ),
    )
    state = _make_state(run_manager=rm, sources=sources)

    resp = _client(state).get("/api/v1/sources/src/config")

    assert resp.status_code == 422
    assert "backends" in resp.json()["detail"]


# -- /sources/{name}/sync ----------------------------------------------------


def test_sync_git_source_refreshes_status_and_workflows():
    """Syncing a git source re-clones and re-discovers workflows."""
    from scripthut.sources.git import SourceStatus, SourceWorkflow
    sources = [GitSourceConfig(name="git", url="git@h:o/r.git", branch="main")]
    sm = MagicMock()
    sm.sync_source = AsyncMock(return_value=SourceStatus(
        name="git", path=Path("/tmp/clone"), cloned=True, branch="main",
        last_commit="deadbeef1234",
    ))
    sm.discover_workflows = MagicMock(return_value=[
        SourceWorkflow(name="train", source_name="git", filename="train.json",
                       tasks_json="[]"),
    ])
    state = _make_state(run_manager=MagicMock(), sources=sources)
    state.source_manager = sm
    state.source_statuses = {}

    resp = _client(state).post("/api/v1/sources/git/sync")

    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "git"
    assert body["type"] == "git"
    assert body["cloned"] is True
    assert body["last_commit"] == "deadbeef1234"
    assert body["workflows"] == ["train.json"]
    assert body["error"] is None
    # Cache must be refreshed for subsequent /run calls.
    assert state.source_workflows["git"][0].filename == "train.json"


def test_sync_unknown_source_returns_404():
    state = _make_state(run_manager=MagicMock(), sources=[])
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
    state = _make_state(run_manager=MagicMock(), sources=sources)
    state.source_manager = sm
    state.source_statuses = {}

    resp = _client(state).post("/api/v1/sources/git/sync")

    assert resp.status_code == 200
    body = resp.json()
    assert "clone failed" in body["error"]


# -- /sources/sync (all) ------------------------------------------------------


def test_sync_all_sources_iterates_every_configured_source():
    from scripthut.sources.git import SourceStatus, SourceWorkflow
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
    sm.discover_workflows = MagicMock(return_value=[
        SourceWorkflow(name="t", source_name="x", filename="t.json",
                       tasks_json="[]"),
    ])
    state = _make_state(run_manager=MagicMock(), sources=sources)
    state.source_manager = sm
    state.source_statuses = {}

    resp = _client(state).post("/api/v1/sources/sync")

    assert resp.status_code == 200
    body = resp.json()
    names = [r["name"] for r in body["sources"]]
    assert names == ["a", "b"]


def test_sync_all_sources_empty_config_returns_empty_list():
    state = _make_state(run_manager=MagicMock())
    state.config = None
    resp = _client(state).post("/api/v1/sources/sync")
    assert resp.status_code == 200
    assert resp.json() == {"sources": []}


# -- /runs --------------------------------------------------------------------


def test_list_runs_filters_and_limits():
    run_a = _make_run(run_id="a", workflow_name="alpha")
    run_b = _make_run(run_id="b", workflow_name="beta")
    rm = MagicMock()
    rm.get_all_runs = MagicMock(return_value=[run_a, run_b])
    state = _make_state(run_manager=rm)

    resp = _client(state).get("/api/v1/runs?workflow=alpha&limit=10")

    assert resp.status_code == 200
    runs = resp.json()["runs"]
    assert [r["id"] for r in runs] == ["a"]


def test_get_run_includes_items():
    run = _make_run()
    rm = MagicMock()
    rm.get_run = MagicMock(return_value=run)
    state = _make_state(run_manager=rm)

    resp = _client(state).get("/api/v1/runs/run123")

    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == "run123"
    assert len(body["items"]) == 2
    assert body["items"][0]["task"]["id"] == "t0"


def test_get_run_missing_returns_404():
    rm = MagicMock()
    rm.get_run = MagicMock(return_value=None)
    state = _make_state(run_manager=rm)

    resp = _client(state).get("/api/v1/runs/missing")

    assert resp.status_code == 404


# -- /runs/{run_id}/rerun -----------------------------------------------------


def test_rerun_resets_run_in_place():
    run = _make_run(run_id="old", workflow_name="demo")
    rm = MagicMock()
    rm.rerun_in_place = AsyncMock(return_value=run)
    state = _make_state(run_manager=rm)

    resp = _client(state).post("/api/v1/runs/old/rerun")

    assert resp.status_code == 200
    rm.rerun_in_place.assert_awaited_once_with("old")
    state.notify_poll.assert_called_once()


# -- /runs/{run_id}/tasks/{task_id}/logs --------------------------------------


def test_get_task_logs_returns_content():
    rm = MagicMock()
    rm.fetch_log_file = AsyncMock(return_value=("hello world", None))
    state = _make_state(run_manager=rm)

    resp = _client(state).get("/api/v1/runs/r1/tasks/t1/logs")

    assert resp.status_code == 200
    body = resp.json()
    assert body["content"] == "hello world"
    assert body["type"] == "output"
    rm.fetch_log_file.assert_awaited_once_with(
        "r1", "t1", log_type="output", tail_lines=None,
    )


def test_get_task_logs_passes_type_and_tail():
    rm = MagicMock()
    rm.fetch_log_file = AsyncMock(return_value=("err output", None))
    state = _make_state(run_manager=rm)

    resp = _client(state).get("/api/v1/runs/r1/tasks/t1/logs?type=error&tail=20")

    assert resp.status_code == 200
    rm.fetch_log_file.assert_awaited_once_with(
        "r1", "t1", log_type="error", tail_lines=20,
    )


def test_get_task_logs_invalid_type_returns_422():
    rm = MagicMock()
    state = _make_state(run_manager=rm)

    resp = _client(state).get("/api/v1/runs/r1/tasks/t1/logs?type=garbage")

    assert resp.status_code == 422


def test_get_task_logs_run_not_found_returns_404():
    rm = MagicMock()
    rm.fetch_log_file = AsyncMock(return_value=(None, "Run 'missing' not found"))
    state = _make_state(run_manager=rm)

    resp = _client(state).get("/api/v1/runs/missing/tasks/t1/logs")

    assert resp.status_code == 404


def test_get_task_logs_task_not_submitted_returns_422():
    rm = MagicMock()
    rm.fetch_log_file = AsyncMock(return_value=(None, "Task has not been submitted yet"))
    state = _make_state(run_manager=rm)

    resp = _client(state).get("/api/v1/runs/r1/tasks/t1/logs")

    assert resp.status_code == 422


# -- /health ------------------------------------------------------------------


def test_health_reports_state():
    state = _make_state(run_manager=MagicMock())
    resp = _client(state).get("/api/v1/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "config_loaded" in body
