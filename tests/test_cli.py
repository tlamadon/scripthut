"""Tests for the gh-style CLI (workflow/run noun groups)."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from scripthut import cli
from scripthut.config_schema import ProjectConfig, WorkflowConfig
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition

# -- helpers -----------------------------------------------------------------


def _make_run(
    run_id: str = "abc12345",
    workflow_name: str = "demo",
    statuses: list[RunItemStatus] | None = None,
) -> Run:
    statuses = statuses or [RunItemStatus.SUBMITTED, RunItemStatus.PENDING]
    items = [
        RunItem(
            task=TaskDefinition(id=f"t{i}", name=f"task-{i}", command="true"),
            status=s,
        )
        for i, s in enumerate(statuses)
    ]
    return Run(
        id=run_id,
        workflow_name=workflow_name,
        backend_name="cluster",
        created_at=datetime(2026, 5, 15, 12, 0, tzinfo=UTC),
        items=items,
        max_concurrent=2,
    )


def _ns(**kwargs) -> argparse.Namespace:
    """Build an argparse Namespace with sensible defaults for CLI handlers."""
    defaults = dict(server=None, config=None, json=False)
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _async_ctx(client: MagicMock) -> MagicMock:
    """Make a MagicMock satisfy the `async with _make_client(args)` protocol."""
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    return client


# -- parser tests ------------------------------------------------------------


def test_parser_workflow_run_requires_name():
    parser = cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["workflow", "run"])


def test_parser_workflow_run_accepts_project_and_server():
    parser = cli.build_parser()
    args = parser.parse_args(
        ["workflow", "run", "wf-name", "--project", "proj", "--server", "http://s"]
    )
    assert args.cmd == "workflow"
    assert args.wf_cmd == "run"
    assert args.name == "wf-name"
    assert args.project == "proj"
    assert args.server == "http://s"
    assert args.handler is cli._cmd_workflow_run


def test_parser_run_view_dispatches_to_handler():
    parser = cli.build_parser()
    args = parser.parse_args(["run", "view", "abc123"])
    assert args.handler is cli._cmd_run_view
    assert args.id == "abc123"


def test_parser_run_logs_flags():
    parser = cli.build_parser()
    args = parser.parse_args(
        ["run", "logs", "abc", "task-1", "--error", "--follow", "--tail", "50"]
    )
    assert args.id == "abc"
    assert args.task == "task-1"
    assert args.error is True
    assert args.follow is True
    assert args.tail == 50


# -- _resolve_server ---------------------------------------------------------


def test_resolve_server_explicit_arg_wins(monkeypatch):
    monkeypatch.setenv("SCRIPTHUT_SERVER", "http://from-env")
    args = _ns(server="http://from-arg")
    assert cli._resolve_server(args) == "http://from-arg"


def test_resolve_server_local_keyword_forces_local(monkeypatch):
    monkeypatch.setenv("SCRIPTHUT_SERVER", "http://from-env")
    args = _ns(server="local")
    assert cli._resolve_server(args) is None


def test_resolve_server_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("SCRIPTHUT_SERVER", "http://from-env")
    args = _ns()
    assert cli._resolve_server(args) == "http://from-env"


def test_resolve_server_falls_back_to_config(monkeypatch):
    monkeypatch.delenv("SCRIPTHUT_SERVER", raising=False)
    fake_config = MagicMock()
    fake_config.settings.cli_server = "http://from-config"
    args = _ns()
    with patch.object(cli, "load_config", return_value=fake_config):
        assert cli._resolve_server(args) == "http://from-config"


def test_resolve_server_returns_none_when_nothing_set(monkeypatch):
    monkeypatch.delenv("SCRIPTHUT_SERVER", raising=False)
    fake_config = MagicMock()
    fake_config.settings.cli_server = None
    args = _ns()
    with patch.object(cli, "load_config", return_value=fake_config):
        assert cli._resolve_server(args) is None


# -- LocalClient -------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_client_run_workflow_calls_manager():
    run = _make_run(
        statuses=[RunItemStatus.SUBMITTED, RunItemStatus.PENDING, RunItemStatus.PENDING],
    )
    runtime = MagicMock()
    runtime.run_manager.create_run = AsyncMock(return_value=run)

    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = runtime

    summary = await client.run_workflow("demo")

    assert summary["id"] == "abc12345"
    assert summary["task_count"] == 3
    assert summary["submitted_count"] == 1
    assert summary["status_counts"] == {"submitted": 1, "pending": 2}
    runtime.run_manager.create_run.assert_awaited_once_with("demo", backend=None)


@pytest.mark.asyncio
async def test_local_client_list_workflows_uses_config():
    runtime = MagicMock()
    runtime.config.workflows = [
        WorkflowConfig(name="alpha", backend="cluster", command="echo []"),
    ]
    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = runtime

    data = await client.list_workflows()

    assert data["workflows"][0]["name"] == "alpha"
    assert "projects" not in data


@pytest.mark.asyncio
async def test_local_client_list_projects_uses_config():
    runtime = MagicMock()
    runtime.config.projects = [
        ProjectConfig(name="proj", backend="cluster", path="~/proj"),
    ]
    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = runtime

    data = await client.list_projects()

    assert data["projects"][0]["name"] == "proj"


@pytest.mark.asyncio
async def test_local_client_view_project_discovers_workflows():
    runtime = MagicMock()
    runtime.config.get_project = MagicMock(
        return_value=ProjectConfig(name="proj", backend="cluster", path="~/proj"),
    )
    runtime.run_manager.discover_workflows = AsyncMock(
        return_value=["a/sflow.json", "b/sflow.json"],
    )
    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = runtime

    data = await client.view_project("proj")

    assert data["name"] == "proj"
    assert data["workflows"] == ["a/sflow.json", "b/sflow.json"]
    assert data["discover_error"] is None


@pytest.mark.asyncio
async def test_local_client_view_project_swallows_discovery_error():
    runtime = MagicMock()
    runtime.config.get_project = MagicMock(
        return_value=ProjectConfig(name="proj", backend="cluster", path="~/proj"),
    )
    runtime.run_manager.discover_workflows = AsyncMock(
        side_effect=ValueError("No SSH connection"),
    )
    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = runtime

    data = await client.view_project("proj")

    assert data["workflows"] == []
    assert "No SSH connection" in data["discover_error"]


@pytest.mark.asyncio
async def test_local_client_view_project_unknown_raises():
    runtime = MagicMock()
    runtime.config.get_project = MagicMock(return_value=None)
    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = runtime

    with pytest.raises(RuntimeError, match="not found"):
        await client.view_project("missing")


@pytest.mark.asyncio
async def test_local_client_list_runs_filters_default_workflow():
    """`_default` runs (external job bins) shouldn't appear in `list runs`."""
    real_run = _make_run(run_id="real", workflow_name="alpha")
    default_run = _make_run(run_id="def", workflow_name="_default")

    runtime = MagicMock()
    runtime.run_manager.storage.load_all_runs = MagicMock(
        return_value={"real": real_run, "def": default_run}
    )
    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = runtime

    data = await client.list_runs()

    ids = [r["id"] for r in data["runs"]]
    assert ids == ["real"]


@pytest.mark.asyncio
async def test_local_client_view_run_loads_from_storage():
    run = _make_run()
    runtime = MagicMock()
    runtime.run_manager.storage.load_all_runs = MagicMock(return_value={"abc12345": run})

    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = runtime

    detail = await client.view_run("abc12345")

    assert detail["id"] == "abc12345"
    assert len(detail["items"]) == 2
    assert detail["items"][0]["task"]["id"] == "t0"


@pytest.mark.asyncio
async def test_local_client_view_run_missing_raises():
    runtime = MagicMock()
    runtime.run_manager.storage.load_all_runs = MagicMock(return_value={})

    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = runtime

    with pytest.raises(RuntimeError, match="not found"):
        await client.view_run("missing")


# -- RemoteClient ------------------------------------------------------------


def _mock_remote(handler) -> cli.RemoteClient:
    """Build a RemoteClient backed by an httpx.MockTransport."""
    transport = httpx.MockTransport(handler)
    client = cli.RemoteClient("http://localhost:8000")
    client._client = httpx.AsyncClient(
        base_url="http://localhost:8000/api/v1", transport=transport,
    )
    return client


@pytest.mark.asyncio
async def test_remote_client_run_workflow_posts_and_parses():
    received: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received["url"] = str(request.url)
        received["method"] = request.method
        return httpx.Response(
            200,
            json={
                "id": "abc12345", "workflow_name": "demo", "backend_name": "cluster",
                "created_at": "2026-05-15T12:00:00+00:00", "status": "running",
                "task_count": 2, "completed_count": 0, "submitted_count": 1,
                "status_counts": {"submitted": 1, "pending": 1},
            },
        )

    client = _mock_remote(handler)
    summary = await client.run_workflow("demo")
    await client._client.aclose()

    assert received["method"] == "POST"
    assert received["url"].endswith("/api/v1/workflows/demo/run")
    assert summary["id"] == "abc12345"


@pytest.mark.asyncio
async def test_remote_client_run_project_passes_workflow_query():
    received: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received["url"] = str(request.url)
        return httpx.Response(200, json={"id": "x"})

    client = _mock_remote(handler)
    await client.run_project_workflow("proj", "sub/sflow.json")
    await client._client.aclose()

    assert "workflow=sub" in received["url"]
    assert "/api/v1/projects/proj/run" in received["url"]


@pytest.mark.asyncio
async def test_remote_client_rerun_passes_mode():
    received: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received["url"] = str(request.url)
        return httpx.Response(200, json={"id": "y"})

    client = _mock_remote(handler)
    await client.rerun("abc", mode="in_place")
    await client._client.aclose()

    assert "mode=in_place" in received["url"]


@pytest.mark.asyncio
async def test_remote_client_fetch_logs_passes_type_and_tail():
    received: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received["url"] = str(request.url)
        return httpx.Response(
            200,
            json={"run_id": "r", "task_id": "t", "type": "error", "content": "boom"},
        )

    client = _mock_remote(handler)
    data = await client.fetch_logs("r", "t", log_type="error", tail=50)
    await client._client.aclose()

    assert "type=error" in received["url"]
    assert "tail=50" in received["url"]
    assert data["content"] == "boom"


@pytest.mark.asyncio
async def test_remote_client_raises_runtime_error_on_4xx():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, json={"detail": "Workflow not found"})

    client = _mock_remote(handler)
    with pytest.raises(RuntimeError, match="HTTP 422.*Workflow not found"):
        await client.run_workflow("nope")
    await client._client.aclose()


# -- subcommand dispatchers --------------------------------------------------


@pytest.mark.asyncio
async def test_workflow_run_local_uses_local_client(capsys):
    run = _make_run(statuses=[RunItemStatus.SUBMITTED])
    fake = _async_ctx(MagicMock())
    fake.run_workflow = AsyncMock(return_value=cli._summary_from_run(run))

    args = _ns(name="demo", project=None, backend=None)
    with patch.object(cli, "_make_client", return_value=fake):
        with patch.object(cli, "_resolve_server", return_value=None):
            rc = await cli._cmd_workflow_run(args)

    fake.run_workflow.assert_awaited_once_with("demo", backend=None)
    out = capsys.readouterr().out
    assert "Submitted run abc12345" in out
    assert rc == 0


@pytest.mark.asyncio
async def test_workflow_run_forwards_backend_override():
    run = _make_run(statuses=[RunItemStatus.SUBMITTED])
    fake = _async_ctx(MagicMock())
    fake.run_workflow = AsyncMock(return_value=cli._summary_from_run(run))

    args = _ns(name="demo", project=None, backend="cluster-b")
    with patch.object(cli, "_make_client", return_value=fake):
        with patch.object(cli, "_resolve_server", return_value=None):
            await cli._cmd_workflow_run(args)

    fake.run_workflow.assert_awaited_once_with("demo", backend="cluster-b")


@pytest.mark.asyncio
async def test_workflow_run_with_project_calls_project_endpoint():
    run = _make_run(workflow_name="proj/sub", statuses=[RunItemStatus.SUBMITTED])
    fake = _async_ctx(MagicMock())
    fake.run_project_workflow = AsyncMock(return_value=cli._summary_from_run(run))

    args = _ns(name="sub/sflow.json", project="proj", backend=None)
    with patch.object(cli, "_make_client", return_value=fake):
        with patch.object(cli, "_resolve_server", return_value=None):
            rc = await cli._cmd_workflow_run(args)

    fake.run_project_workflow.assert_awaited_once_with(
        "proj", "sub/sflow.json", backend=None,
    )
    assert rc == 0


@pytest.mark.asyncio
async def test_project_list_prints_table(capsys):
    fake = _async_ctx(MagicMock())
    fake.list_projects = AsyncMock(return_value={
        "projects": [
            {"name": "proj-a", "backend": "cluster", "path": "~/a", "description": ""},
            {"name": "proj-b", "backend": "cluster", "path": "~/b", "description": "demo"},
        ]
    })

    args = _ns()
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_project_list(args)

    out = capsys.readouterr().out
    assert "proj-a" in out
    assert "proj-b" in out
    assert "demo" in out
    assert rc == 0


@pytest.mark.asyncio
async def test_project_view_lists_discovered_workflows(capsys):
    fake = _async_ctx(MagicMock())
    fake.view_project = AsyncMock(return_value={
        "name": "proj", "backend": "cluster", "path": "~/proj",
        "description": "", "max_concurrent": 4,
        "workflows": ["a/sflow.json", "b/sflow.json"],
        "discover_error": None,
    })

    args = _ns(name="proj")
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_project_view(args)

    out = capsys.readouterr().out
    assert "proj" in out
    assert "a/sflow.json" in out
    assert "b/sflow.json" in out
    assert "workflows (2)" in out
    fake.view_project.assert_awaited_once_with("proj")
    assert rc == 0


@pytest.mark.asyncio
async def test_project_view_surfaces_discover_error(capsys):
    fake = _async_ctx(MagicMock())
    fake.view_project = AsyncMock(return_value={
        "name": "proj", "backend": "cluster", "path": "~/proj",
        "description": "", "max_concurrent": None,
        "workflows": [],
        "discover_error": "No SSH connection",
    })

    args = _ns(name="proj")
    with patch.object(cli, "_make_client", return_value=fake):
        await cli._cmd_project_view(args)

    out = capsys.readouterr().out
    assert "discovery failed" in out
    assert "No SSH connection" in out


@pytest.mark.asyncio
async def test_backend_list_prints_table(capsys):
    fake = _async_ctx(MagicMock())
    fake.list_backends = AsyncMock(return_value={
        "backends": [
            {"name": "cluster-a", "type": "slurm", "connected": True, "max_concurrent": 100},
            {"name": "cluster-b", "type": "pbs", "connected": False, "max_concurrent": None},
        ]
    })

    args = _ns()
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_backend_list(args)

    out = capsys.readouterr().out
    assert "cluster-a" in out
    assert "slurm" in out
    assert "connected" in out
    assert "cluster-b" in out
    assert "down" in out
    assert rc == 0


@pytest.mark.asyncio
async def test_backend_list_json_emits_raw():
    fake = _async_ctx(MagicMock())
    fake.list_backends = AsyncMock(return_value={"backends": []})

    args = _ns(json=True)
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_backend_list(args)

    assert rc == 0
    fake.list_backends.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_run_view_prints_table(capsys):
    fake = _async_ctx(MagicMock())
    fake.view_run = AsyncMock(return_value={
        "id": "abc12345", "workflow_name": "demo", "backend_name": "cluster",
        "created_at": "2026-05-15T12:00:00+00:00", "status": "running",
        "task_count": 2, "completed_count": 0, "submitted_count": 1,
        "status_counts": {"submitted": 1, "pending": 1},
        "items": [
            {"task": {"id": "t0", "name": "n0"}, "status": "submitted",
             "job_id": "12345", "cpu_efficiency": None, "max_rss": None},
        ],
    })

    args = _ns(id="abc12345")
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_run_view(args)

    out = capsys.readouterr().out
    assert "Run abc12345" in out
    assert "t0" in out
    assert rc == 0


@pytest.mark.asyncio
async def test_run_watch_polls_until_terminal(capsys):
    """Watch should keep polling until status hits a terminal state."""
    statuses = ["running", "running", "completed"]
    call_count = {"n": 0}

    async def fake_view(run_id):
        idx = min(call_count["n"], len(statuses) - 1)
        call_count["n"] += 1
        return {
            "id": run_id, "workflow_name": "demo", "backend_name": "cluster",
            "created_at": "2026-05-15T12:00:00+00:00",
            "status": statuses[idx], "task_count": 1, "completed_count": 1,
            "submitted_count": 1, "status_counts": {"completed": 1}, "items": [],
        }

    fake = _async_ctx(MagicMock())
    fake.view_run = fake_view

    args = _ns(id="abc", interval=0.0, exit_status=False)
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_run_watch(args)

    assert call_count["n"] == 3
    assert rc == 0


@pytest.mark.asyncio
async def test_run_watch_exit_status_returns_nonzero_on_failure():
    fake = _async_ctx(MagicMock())
    fake.view_run = AsyncMock(return_value={
        "id": "abc", "workflow_name": "demo", "backend_name": "cluster",
        "created_at": "2026-05-15T12:00:00+00:00",
        "status": "failed", "task_count": 1, "completed_count": 1,
        "submitted_count": 1, "status_counts": {"failed": 1}, "items": [],
    })

    args = _ns(id="abc", interval=0.0, exit_status=True)
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_run_watch(args)

    assert rc == 1


@pytest.mark.asyncio
async def test_run_cancel_calls_client(capsys):
    fake = _async_ctx(MagicMock())
    fake.cancel_run = AsyncMock(return_value={"run_id": "abc", "cancelled": True})

    args = _ns(id="abc")
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_run_cancel(args)

    fake.cancel_run.assert_awaited_once_with("abc")
    assert "Cancelled run abc" in capsys.readouterr().out
    assert rc == 0


@pytest.mark.asyncio
async def test_run_rerun_default_mode_is_new(capsys):
    run = _make_run(statuses=[RunItemStatus.SUBMITTED])
    fake = _async_ctx(MagicMock())
    fake.rerun = AsyncMock(return_value=cli._summary_from_run(run))

    args = _ns(id="old", in_place=False)
    with patch.object(cli, "_make_client", return_value=fake):
        with patch.object(cli, "_resolve_server", return_value=None):
            rc = await cli._cmd_run_rerun(args)

    fake.rerun.assert_awaited_once_with("old", mode="new")
    assert rc == 0


@pytest.mark.asyncio
async def test_run_rerun_in_place_passes_mode():
    run = _make_run(statuses=[RunItemStatus.SUBMITTED])
    fake = _async_ctx(MagicMock())
    fake.rerun = AsyncMock(return_value=cli._summary_from_run(run))

    args = _ns(id="old", in_place=True)
    with patch.object(cli, "_make_client", return_value=fake):
        with patch.object(cli, "_resolve_server", return_value=None):
            await cli._cmd_run_rerun(args)

    fake.rerun.assert_awaited_once_with("old", mode="in_place")


@pytest.mark.asyncio
async def test_run_logs_one_shot_writes_content(capsys):
    fake = _async_ctx(MagicMock())
    fake.fetch_logs = AsyncMock(
        return_value={"run_id": "r", "task_id": "t", "type": "output", "content": "hello"}
    )

    args = _ns(id="r", task="t", error=False, tail=None, follow=False, interval=2.0)
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_run_logs(args)

    fake.fetch_logs.assert_awaited_once_with("r", "t", log_type="output", tail=None)
    out = capsys.readouterr().out
    assert "hello" in out
    assert rc == 0


@pytest.mark.asyncio
async def test_run_logs_error_flag_uses_stderr_type():
    fake = _async_ctx(MagicMock())
    fake.fetch_logs = AsyncMock(
        return_value={"run_id": "r", "task_id": "t", "type": "error", "content": "oops"}
    )

    args = _ns(id="r", task="t", error=True, tail=10, follow=False, interval=2.0)
    with patch.object(cli, "_make_client", return_value=fake):
        await cli._cmd_run_logs(args)

    fake.fetch_logs.assert_awaited_once_with("r", "t", log_type="error", tail=10)


@pytest.mark.asyncio
async def test_run_logs_follow_streams_until_task_terminal(capsys):
    """--follow polls until the task status reaches a terminal state."""
    contents = ["line1\n", "line1\nline2\n", "line1\nline2\nfinal\n"]
    item_states = ["running", "running", "completed"]
    n = {"i": 0}

    async def fake_fetch(run_id, task_id, log_type="output", tail=None):
        idx = min(n["i"], len(contents) - 1)
        return {"run_id": run_id, "task_id": task_id, "type": log_type, "content": contents[idx]}

    async def fake_view(run_id):
        idx = min(n["i"], len(item_states) - 1)
        n["i"] += 1
        return {
            "id": run_id, "workflow_name": "demo", "backend_name": "cluster",
            "created_at": "2026-05-15T12:00:00+00:00", "status": "running",
            "task_count": 1, "completed_count": 0, "submitted_count": 1,
            "status_counts": {}, "items": [{"task": {"id": "t"}, "status": item_states[idx]}],
        }

    fake = _async_ctx(MagicMock())
    fake.fetch_logs = fake_fetch
    fake.view_run = fake_view

    args = _ns(id="r", task="t", error=False, tail=None, follow=True, interval=0.0)
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_run_logs(args)

    out = capsys.readouterr().out
    assert "line1" in out
    assert "line2" in out
    assert "final" in out
    assert rc == 0
