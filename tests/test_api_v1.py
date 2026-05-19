"""Tests for the versioned JSON API exposed under ``/api/v1``."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripthut.api import make_api_router
from scripthut.config_schema import ProjectConfig, WorkflowConfig
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
    workflows: list[WorkflowConfig] | None = None,
    projects: list[ProjectConfig] | None = None,
):
    """Build a minimal AppState-like object that satisfies the router."""
    state = MagicMock()
    state.run_manager = run_manager
    state.config_error = None
    state.backends = {}
    if workflows is not None or projects is not None:
        wf_list = workflows or []
        pr_list = projects or []
        state.config = MagicMock()
        state.config.workflows = wf_list
        state.config.projects = pr_list
        state.config.get_workflow = lambda name: next(
            (w for w in wf_list if w.name == name), None,
        )
        state.config.get_project = lambda name: next(
            (p for p in pr_list if p.name == name), None,
        )
    else:
        state.config = None
    state.notify_poll = MagicMock()
    return state


def _client(state) -> TestClient:
    app = FastAPI()
    app.include_router(make_api_router(state))
    return TestClient(app)


# -- /workflows --------------------------------------------------------------


def test_list_workflows_returns_configured_entries():
    workflows = [
        WorkflowConfig(name="demo", backend="cluster", command="echo []", description="d"),
    ]
    state = _make_state(run_manager=MagicMock(), workflows=workflows)

    resp = _client(state).get("/api/v1/workflows")

    assert resp.status_code == 200
    data = resp.json()
    assert data["workflows"][0]["name"] == "demo"
    assert data["workflows"][0]["has_git"] is False
    # /workflows no longer leaks projects — see /projects instead
    assert "projects" not in data


def test_list_projects_returns_configured_projects():
    projects = [
        ProjectConfig(name="proj", backend="cluster", path="~/proj", description="d"),
    ]
    state = _make_state(run_manager=MagicMock(), projects=projects)

    resp = _client(state).get("/api/v1/projects")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data["projects"]) == 1
    assert data["projects"][0]["name"] == "proj"
    assert data["projects"][0]["path"] == "~/proj"


def test_view_project_returns_metadata_and_workflows():
    projects = [ProjectConfig(name="proj", backend="cluster", path="~/proj")]
    rm = MagicMock()
    rm.discover_workflows = AsyncMock(
        return_value=["a/sflow.json", "b/sflow.json"],
    )
    state = _make_state(run_manager=rm, projects=projects)

    resp = _client(state).get("/api/v1/projects/proj")

    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "proj"
    assert body["workflows"] == ["a/sflow.json", "b/sflow.json"]
    assert body["discover_error"] is None
    rm.discover_workflows.assert_awaited_once_with("proj")


def test_view_project_returns_metadata_when_discovery_fails():
    """Discovery failure shouldn't blank out the metadata view."""
    projects = [ProjectConfig(name="proj", backend="cluster", path="~/proj")]
    rm = MagicMock()
    rm.discover_workflows = AsyncMock(
        side_effect=ValueError("No SSH connection to backend 'cluster'"),
    )
    state = _make_state(run_manager=rm, projects=projects)

    resp = _client(state).get("/api/v1/projects/proj")

    assert resp.status_code == 200
    body = resp.json()
    assert body["workflows"] == []
    assert "No SSH connection" in body["discover_error"]


def test_view_project_unknown_returns_404():
    state = _make_state(run_manager=MagicMock(), projects=[])

    resp = _client(state).get("/api/v1/projects/missing")

    assert resp.status_code == 404


def test_list_workflows_no_config_returns_empty():
    state = _make_state(run_manager=MagicMock(), workflows=None)
    state.config = None

    resp = _client(state).get("/api/v1/workflows")

    assert resp.status_code == 200
    assert resp.json() == {"workflows": []}


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


# -- /workflows/{name}/run ---------------------------------------------------


def test_run_workflow_returns_summary():
    run = _make_run(
        statuses=[RunItemStatus.SUBMITTED, RunItemStatus.PENDING, RunItemStatus.PENDING],
    )
    rm = MagicMock()
    rm.create_run = AsyncMock(return_value=run)
    state = _make_state(run_manager=rm)

    resp = _client(state).post("/api/v1/workflows/demo/run")

    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == "run123"
    assert body["workflow_name"] == "demo"
    assert body["task_count"] == 3
    assert body["submitted_count"] == 1
    assert body["status_counts"] == {"submitted": 1, "pending": 2}
    rm.create_run.assert_awaited_once_with("demo", backend=None)
    state.notify_poll.assert_called_once()


def test_run_workflow_passes_backend_override():
    run = _make_run()
    rm = MagicMock()
    rm.create_run = AsyncMock(return_value=run)
    state = _make_state(run_manager=rm)

    resp = _client(state).post("/api/v1/workflows/demo/run?backend=cluster-b")

    assert resp.status_code == 200
    rm.create_run.assert_awaited_once_with("demo", backend="cluster-b")


def test_run_workflow_unknown_name_returns_422():
    rm = MagicMock()
    rm.create_run = AsyncMock(side_effect=ValueError("Workflow 'nope' not found"))
    state = _make_state(run_manager=rm)

    resp = _client(state).post("/api/v1/workflows/nope/run")

    assert resp.status_code == 422
    assert "not found" in resp.json()["detail"]


def test_run_workflow_no_manager_returns_503():
    state = _make_state(run_manager=None)

    resp = _client(state).post("/api/v1/workflows/demo/run")

    assert resp.status_code == 503


# -- /workflows/{name}/dry-run -----------------------------------------------


def test_dry_run_serializes_tasks():
    rm = MagicMock()
    workflow = WorkflowConfig(name="demo", backend="cluster", command="echo []")
    rm.dry_run = AsyncMock(
        return_value={
            "workflow": workflow,
            "backend_name": "cluster",
            "task_count": 1,
            "max_concurrent": 4,
            "account": None,
            "commit_hash": "abc123",
            "tasks": [
                {
                    "task": TaskDefinition(id="t0", name="hello", command="echo hi"),
                    "submit_script": "#!/bin/bash\necho hi",
                    "output_path": "/tmp/out",
                    "error_path": "/tmp/err",
                }
            ],
            "raw_output": "[]",
        }
    )
    state = _make_state(run_manager=rm)

    resp = _client(state).get("/api/v1/workflows/demo/dry-run")

    assert resp.status_code == 200
    body = resp.json()
    assert body["task_count"] == 1
    assert body["tasks"][0]["task"]["id"] == "t0"
    assert body["tasks"][0]["submit_script"].startswith("#!/bin/bash")
    assert body["workflow"]["name"] == "demo"
    rm.dry_run.assert_awaited_once_with("demo", backend=None)


def test_dry_run_passes_backend_override():
    rm = MagicMock()
    workflow = WorkflowConfig(name="demo", backend="cluster", command="echo []")
    rm.dry_run = AsyncMock(return_value={
        "workflow": workflow, "backend_name": "cluster-b",
        "task_count": 0, "max_concurrent": 4, "account": None,
        "commit_hash": None, "tasks": [], "raw_output": "[]",
    })
    state = _make_state(run_manager=rm)

    _client(state).get("/api/v1/workflows/demo/dry-run?backend=cluster-b")

    rm.dry_run.assert_awaited_once_with("demo", backend="cluster-b")


# -- /projects/{name}/run ----------------------------------------------------


def test_run_project_workflow_passes_workflow_query():
    run = _make_run(workflow_name="proj/sub")
    rm = MagicMock()
    rm.create_run_from_project = AsyncMock(return_value=run)
    state = _make_state(run_manager=rm)

    resp = _client(state).post("/api/v1/projects/proj/run?workflow=sub/sflow.json")

    assert resp.status_code == 200
    rm.create_run_from_project.assert_awaited_once_with(
        "proj", "sub/sflow.json", backend=None,
    )


def test_run_project_workflow_passes_backend_override():
    run = _make_run(workflow_name="proj/sub")
    rm = MagicMock()
    rm.create_run_from_project = AsyncMock(return_value=run)
    state = _make_state(run_manager=rm)

    resp = _client(state).post(
        "/api/v1/projects/proj/run?workflow=sub/sflow.json&backend=cluster-b"
    )

    assert resp.status_code == 200
    rm.create_run_from_project.assert_awaited_once_with(
        "proj", "sub/sflow.json", backend="cluster-b",
    )


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


def test_rerun_default_mode_creates_new_run():
    run = _make_run(run_id="new1", workflow_name="demo")
    rm = MagicMock()
    rm.rerun_from = AsyncMock(return_value=run)
    rm.rerun_in_place = AsyncMock(return_value=run)
    state = _make_state(run_manager=rm)

    resp = _client(state).post("/api/v1/runs/old/rerun")

    assert resp.status_code == 200
    rm.rerun_from.assert_awaited_once_with("old")
    rm.rerun_in_place.assert_not_awaited()
    state.notify_poll.assert_called_once()


def test_rerun_in_place_mode_resets_run():
    run = _make_run(run_id="old", workflow_name="demo")
    rm = MagicMock()
    rm.rerun_from = AsyncMock(return_value=run)
    rm.rerun_in_place = AsyncMock(return_value=run)
    state = _make_state(run_manager=rm)

    resp = _client(state).post("/api/v1/runs/old/rerun?mode=in_place")

    assert resp.status_code == 200
    rm.rerun_in_place.assert_awaited_once_with("old")
    rm.rerun_from.assert_not_awaited()


def test_rerun_invalid_mode_returns_422():
    rm = MagicMock()
    state = _make_state(run_manager=rm)

    resp = _client(state).post("/api/v1/runs/old/rerun?mode=bogus")

    assert resp.status_code == 422


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
