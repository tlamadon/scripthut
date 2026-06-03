"""Tests for the gh-style CLI (workflow/run noun groups)."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from scripthut import cli
from scripthut.config_schema import GitSourceConfig, PathSourceConfig
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
    defaults = dict(
        server=None, config=None, json=False,
        cf_client_id=None, cf_client_secret=None,
        cf_access_token=None, cloudflared_app=None,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _async_ctx(client: MagicMock) -> MagicMock:
    """Make a MagicMock satisfy the `async with _make_client(args)` protocol."""
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    return client


# -- parser tests ------------------------------------------------------------


def test_main_dispatch_allowlist_covers_all_cli_subcommands():
    """`main._CLI_SUBCOMMANDS` must list every top-level cli.py subcommand.

    Drift here is invisible at startup but fatal at runtime — the dropped
    subcommand falls through to the web-server argparse and dies with
    'unrecognized arguments'. Introspect the actual parser so this test
    fails the moment someone adds a noun without updating main.py.
    """
    from scripthut import main as main_mod

    parser = cli.build_parser()
    sub_action = next(
        a for a in parser._actions if isinstance(a, argparse._SubParsersAction)
    )
    declared = set(sub_action.choices.keys())
    missing = declared - main_mod._CLI_SUBCOMMANDS
    assert not missing, (
        f"cli.py defines top-level subcommand(s) {sorted(missing)} that are "
        "not in main._CLI_SUBCOMMANDS — they will be unreachable from the "
        "`scripthut` entry point."
    )


def test_parser_workflow_run_requires_name():
    parser = cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["workflow", "run"])


def test_parser_workflow_run_accepts_source_and_server():
    parser = cli.build_parser()
    args = parser.parse_args(
        ["workflow", "run", "wf-name", "--source", "src", "--server", "http://s"]
    )
    assert args.cmd == "workflow"
    assert args.wf_cmd == "run"
    assert args.name == "wf-name"
    assert args.source == "src"
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


# -- _resolve_auth -----------------------------------------------------------


def _empty_cf_env(monkeypatch):
    for k in (
        "SCRIPTHUT_CF_CLIENT_ID",
        "SCRIPTHUT_CF_CLIENT_SECRET",
        "SCRIPTHUT_CF_ACCESS_TOKEN",
        "SCRIPTHUT_CLOUDFLARED_APP",
    ):
        monkeypatch.delenv(k, raising=False)


def test_resolve_auth_returns_none_when_nothing_configured(monkeypatch):
    _empty_cf_env(monkeypatch)
    args = _ns()
    with patch.object(cli, "load_config", side_effect=FileNotFoundError):
        assert cli._resolve_auth(args) is None


def test_resolve_auth_service_token_from_flags(monkeypatch):
    _empty_cf_env(monkeypatch)
    args = _ns(cf_client_id="ID", cf_client_secret="SECRET")
    with patch.object(cli, "load_config", side_effect=FileNotFoundError):
        auth = cli._resolve_auth(args)
    assert auth is not None
    assert auth.headers() == {
        "CF-Access-Client-Id": "ID",
        "CF-Access-Client-Secret": "SECRET",
    }


def test_resolve_auth_token_from_env(monkeypatch):
    _empty_cf_env(monkeypatch)
    monkeypatch.setenv("SCRIPTHUT_CF_ACCESS_TOKEN", "JWT123")
    args = _ns()
    with patch.object(cli, "load_config", side_effect=FileNotFoundError):
        auth = cli._resolve_auth(args)
    assert auth is not None
    assert auth.headers() == {"cf-access-token": "JWT123"}


def test_resolve_auth_flag_overrides_env_and_config(monkeypatch):
    _empty_cf_env(monkeypatch)
    monkeypatch.setenv("SCRIPTHUT_CF_ACCESS_TOKEN", "env-jwt")
    fake_config = MagicMock()
    fake_config.settings.cli_auth = MagicMock(
        cf_client_id=None, cf_client_secret=None,
        cf_access_token="config-jwt", cloudflared_app=None,
    )
    args = _ns(cf_access_token="flag-jwt")
    with patch.object(cli, "load_config", return_value=fake_config):
        auth = cli._resolve_auth(args)
    assert auth.headers()["cf-access-token"] == "flag-jwt"


def test_resolve_auth_invokes_cloudflared_when_no_token(monkeypatch):
    _empty_cf_env(monkeypatch)
    args = _ns(cloudflared_app="https://scripthut.example.com")
    with patch.object(cli, "load_config", side_effect=FileNotFoundError), \
         patch.object(cli, "_fetch_cloudflared_token", return_value="cf-jwt") as fetch:
        auth = cli._resolve_auth(args)
    fetch.assert_called_once_with("https://scripthut.example.com")
    assert auth.headers() == {"cf-access-token": "cf-jwt"}


def test_resolve_auth_skips_cloudflared_when_explicit_token_present(monkeypatch):
    _empty_cf_env(monkeypatch)
    args = _ns(
        cf_access_token="already-have-jwt",
        cloudflared_app="https://scripthut.example.com",
    )
    with patch.object(cli, "load_config", side_effect=FileNotFoundError), \
         patch.object(cli, "_fetch_cloudflared_token") as fetch:
        auth = cli._resolve_auth(args)
    fetch.assert_not_called()
    assert auth.headers()["cf-access-token"] == "already-have-jwt"


def test_remote_client_attaches_auth_headers(monkeypatch):
    """End-to-end: a RemoteClient built with auth sends the right headers."""
    seen: dict[str, str] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen.update(dict(req.headers))
        return httpx.Response(200, json={"ok": True})

    auth = cli.RemoteAuth(cf_client_id="id1", cf_client_secret="sec1")
    client = cli.RemoteClient("http://localhost:8000", auth=auth)
    # Swap transport so we don't actually hit the network.
    client._client = httpx.AsyncClient(
        base_url="http://localhost:8000/api/v1",
        transport=httpx.MockTransport(handler),
        headers=auth.headers(),
    )

    import asyncio
    asyncio.run(client._get("/backends"))
    assert seen.get("cf-access-client-id") == "id1"
    assert seen.get("cf-access-client-secret") == "sec1"


# -- LocalClient -------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_client_list_sources_returns_summaries():
    runtime = MagicMock()
    runtime.config.sources = [
        PathSourceConfig(name="local-src", backend="cluster", path="/r/src"),
        GitSourceConfig(name="git-src", url="git@host:o/r.git", branch="main"),
    ]
    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = runtime

    data = await client.list_sources()

    by_name = {s["name"]: s for s in data["sources"]}
    assert by_name["local-src"]["type"] == "path"
    assert by_name["local-src"]["backend"] == "cluster"
    assert by_name["git-src"]["type"] == "git"
    assert by_name["git-src"]["url"] == "git@host:o/r.git"


@pytest.mark.asyncio
async def test_local_client_view_source_returns_metadata():
    runtime = MagicMock()
    runtime.config.get_source = MagicMock(
        return_value=PathSourceConfig(
            name="src", backend="cluster", path="/r/src", description="My repo",
        ),
    )
    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = runtime

    data = await client.view_source("src")

    assert data["name"] == "src"
    assert data["type"] == "path"
    assert data["description"] == "My repo"
    # Local mode degrades workflow discovery — it carries a clear hint.
    assert data["workflows"] == []
    assert "--server" in (data["discover_error"] or "")


@pytest.mark.asyncio
async def test_local_client_view_source_unknown_raises():
    runtime = MagicMock()
    runtime.config.get_source = MagicMock(return_value=None)
    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = runtime

    with pytest.raises(RuntimeError, match="not found"):
        await client.view_source("missing")


@pytest.mark.asyncio
async def test_local_client_run_source_workflow_is_unsupported():
    """Local CLI mode can't fetch the workflow JSON, so this is degraded.

    The error message must direct the user to ``--server <url>`` — that's
    the supported path until source discovery is added to ``Runtime``.
    """
    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = MagicMock()
    with pytest.raises(RuntimeError, match="--server"):
        await client.run_source_workflow("src", "wf.json")


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
async def test_remote_client_run_source_passes_workflow_query():
    received: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received["url"] = str(request.url)
        return httpx.Response(200, json={"id": "x"})

    client = _mock_remote(handler)
    await client.run_source_workflow("src", "train.json")
    await client._client.aclose()

    assert "workflow=train.json" in received["url"]
    assert "/api/v1/sources/src/run" in received["url"]


@pytest.mark.asyncio
async def test_remote_client_rerun_posts_to_endpoint():
    received: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received["url"] = str(request.url)
        received["method"] = request.method
        return httpx.Response(200, json={"id": "y"})

    client = _mock_remote(handler)
    await client.rerun("abc")
    await client._client.aclose()

    assert received["method"] == "POST"
    assert received["url"].endswith("/runs/abc/rerun")


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
        await client.run_source_workflow("src", "missing.json")
    await client._client.aclose()


# -- subcommand dispatchers --------------------------------------------------


@pytest.mark.asyncio
async def test_workflow_run_forwards_backend_override():
    run = _make_run(workflow_name="src/train", statuses=[RunItemStatus.SUBMITTED])
    fake = _async_ctx(MagicMock())
    fake.run_source_workflow = AsyncMock(return_value=cli._summary_from_run(run))

    args = _ns(name="train.json", source="src", backend="cluster-b")
    with patch.object(cli, "_make_client", return_value=fake):
        with patch.object(cli, "_resolve_server", return_value=None):
            await cli._cmd_workflow_run(args)

    fake.run_source_workflow.assert_awaited_once_with(
        "src", "train.json", backend="cluster-b",
    )


@pytest.mark.asyncio
async def test_workflow_run_with_source_calls_source_endpoint():
    run = _make_run(workflow_name="src/train", statuses=[RunItemStatus.SUBMITTED])
    fake = _async_ctx(MagicMock())
    fake.run_source_workflow = AsyncMock(return_value=cli._summary_from_run(run))

    args = _ns(name="train.json", source="src", backend=None)
    with patch.object(cli, "_make_client", return_value=fake):
        with patch.object(cli, "_resolve_server", return_value=None):
            rc = await cli._cmd_workflow_run(args)

    fake.run_source_workflow.assert_awaited_once_with(
        "src", "train.json", backend=None,
    )
    assert rc == 0


@pytest.mark.asyncio
async def test_source_list_prints_table(capsys):
    fake = _async_ctx(MagicMock())
    fake.list_sources = AsyncMock(return_value={
        "sources": [
            {"name": "src-a", "type": "path", "backend": "cluster",
             "path": "/r/a", "description": ""},
            {"name": "src-b", "type": "git",
             "url": "git@h:o/r.git", "branch": "main", "description": "demo"},
        ]
    })

    args = _ns()
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_source_list(args)

    out = capsys.readouterr().out
    assert "src-a" in out
    assert "src-b" in out
    assert "demo" in out
    assert rc == 0


@pytest.mark.asyncio
async def test_source_view_lists_discovered_workflows(capsys):
    fake = _async_ctx(MagicMock())
    fake.view_source = AsyncMock(return_value={
        "name": "src", "type": "path", "backend": "cluster", "path": "/r/src",
        "description": "", "max_concurrent": 4,
        "workflows": ["train.json", "eval.json"],
        "discover_error": None,
    })

    args = _ns(name="src")
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_source_view(args)

    out = capsys.readouterr().out
    assert "src" in out
    assert "train.json" in out
    assert "eval.json" in out
    assert "workflows (2)" in out
    fake.view_source.assert_awaited_once_with("src")
    assert rc == 0


@pytest.mark.asyncio
async def test_source_view_surfaces_discover_error(capsys):
    fake = _async_ctx(MagicMock())
    fake.view_source = AsyncMock(return_value={
        "name": "src", "type": "git", "url": "git@h:o/r.git", "branch": "main",
        "description": "", "max_concurrent": None,
        "workflows": [],
        "discover_error": "Source not synced",
    })

    args = _ns(name="src")
    with patch.object(cli, "_make_client", return_value=fake):
        await cli._cmd_source_view(args)

    out = capsys.readouterr().out
    assert "Source not synced" in out


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
async def test_run_rerun_calls_client(capsys):
    run = _make_run(statuses=[RunItemStatus.SUBMITTED])
    fake = _async_ctx(MagicMock())
    fake.rerun = AsyncMock(return_value=cli._summary_from_run(run))

    args = _ns(id="old")
    with patch.object(cli, "_make_client", return_value=fake):
        with patch.object(cli, "_resolve_server", return_value=None):
            rc = await cli._cmd_run_rerun(args)

    fake.rerun.assert_awaited_once_with("old")
    assert rc == 0


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


# -- status command ----------------------------------------------------------


def _status_ns(**kwargs) -> argparse.Namespace:
    """Defaults for ``scripthut status`` invocations."""
    defaults = dict(
        config=None, server=None, json=False, quick=False,
        cf_client_id=None, cf_client_secret=None,
        cf_access_token=None, cloudflared_app=None,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_status_local_mode_reports_no_server(monkeypatch):
    """No --server / env / config → local mode, no probe attempted."""
    monkeypatch.delenv("SCRIPTHUT_SERVER", raising=False)
    monkeypatch.delenv("SCRIPTHUT_CF_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("SCRIPTHUT_CLOUDFLARED_APP", raising=False)
    # Force load_config to look "empty" (no projects/backends).
    fake_cfg = MagicMock()
    fake_cfg.projects = []
    fake_cfg.backends = []
    fake_cfg.settings.cli_server = None
    fake_cfg.settings.cli_auth = None
    with patch.object(cli, "load_config", return_value=fake_cfg), \
         patch("scripthut.config.discover_global_config", return_value=None), \
         patch("scripthut.config.discover_project_config", return_value=None):
        data = cli._gather_status(_status_ns())

    assert data["server"] is None
    assert data["server_source"] == "none"
    assert data["auth"]["method"] == "none"


def test_status_reports_source_attribution(monkeypatch):
    """A --server flag wins over env, and the source string says so."""
    monkeypatch.setenv("SCRIPTHUT_SERVER", "http://from-env")
    fake_cfg = MagicMock()
    fake_cfg.projects = []
    fake_cfg.backends = []
    fake_cfg.settings.cli_server = "http://from-config"
    fake_cfg.settings.cli_auth = None
    with patch.object(cli, "load_config", return_value=fake_cfg):
        data = cli._gather_status(_status_ns(server="http://from-flag"))

    assert data["server"] == "http://from-flag"
    assert data["server_source"] == "flag"


def test_status_describes_cloudflared_app_without_fetching(monkeypatch):
    """Status must NOT shell out to cloudflared just to describe config.

    The fetch is slow and can fail offline — status should stay fast and
    report intent ("would use cloudflared") rather than result.
    """
    monkeypatch.delenv("SCRIPTHUT_SERVER", raising=False)
    monkeypatch.delenv("SCRIPTHUT_CF_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("SCRIPTHUT_CLOUDFLARED_APP", "https://srv.example")
    fake_cfg = MagicMock()
    fake_cfg.projects = []
    fake_cfg.backends = []
    fake_cfg.settings.cli_server = None
    fake_cfg.settings.cli_auth = None
    called = MagicMock(side_effect=AssertionError("cloudflared was invoked!"))
    with patch.object(cli, "load_config", return_value=fake_cfg), \
         patch.object(cli, "_fetch_cloudflared_token", called):
        data = cli._gather_status(_status_ns())

    assert data["auth"]["cloudflared_app"] == "https://srv.example"
    assert data["auth"]["method"] == "jwt-pending"
    called.assert_not_called()


@pytest.mark.asyncio
async def test_status_probe_ok():
    """Both probes return 200 → reachable + authorized."""

    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path.endswith("/health"):
            return httpx.Response(200, json={"ok": True})
        if req.url.path.endswith("/sources"):
            return httpx.Response(200, json={"sources": [
                {"name": "s1", "type": "path", "backend": "b1",
                 "path": "/p", "description": ""},
            ]})
        if req.url.path.endswith("/backends"):
            return httpx.Response(200, json={"backends": [
                {"name": "b1", "type": "slurm", "connected": True},
            ]})
        return httpx.Response(404)

    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient
    with patch("httpx.AsyncClient", lambda *a, **kw: real_client(
        transport=transport, timeout=kw.get("timeout", 5.0),
        follow_redirects=kw.get("follow_redirects", False),
    )):
        probe = await cli._probe_server("http://srv.example", auth=None)

    assert probe["health"]["status_code"] == 200
    assert probe["authorized"]["status_code"] == 200
    assert probe["server_data"]["sources"][0]["name"] == "s1"
    assert probe["server_data_backends"]["backends"][0]["name"] == "b1"


@pytest.mark.asyncio
async def test_status_probe_auth_failure_distinguished_from_unreachable():
    """A 302 to cloudflareaccess.com on /sources means server up, auth bad."""

    def handler(req: httpx.Request) -> httpx.Response:
        # /health is also Access-protected here, so it returns 302 unauthenticated.
        return httpx.Response(
            302,
            headers={"location": "https://team.cloudflareaccess.com/cdn-cgi/access/login/..."},
        )

    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient
    with patch("httpx.AsyncClient", lambda *a, **kw: real_client(
        transport=transport, timeout=kw.get("timeout", 5.0),
        follow_redirects=kw.get("follow_redirects", False),
    )):
        probe = await cli._probe_server("http://srv.example", auth=None)

    assert probe["health"]["status_code"] == 302
    assert probe["authorized"]["status_code"] == 302
    assert "cloudflareaccess.com" in probe["authorized"]["redirect_location"]


@pytest.mark.asyncio
async def test_status_probe_unreachable_returns_error_field():
    """Connection errors land in an `error` field, not status_code."""

    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("nope")

    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient
    with patch("httpx.AsyncClient", lambda *a, **kw: real_client(
        transport=transport, timeout=kw.get("timeout", 5.0),
        follow_redirects=kw.get("follow_redirects", False),
    )):
        probe = await cli._probe_server("http://srv.example", auth=None)

    assert "error" in probe["health"]
    assert "error" in probe["authorized"]


def test_status_renders_local_mode():
    data = {
        "version": "0.5.7",
        "install_path": "/x/y/scripthut",
        "config_paths": {"global": "/u/scripthut.yaml", "project": None},
        "config_error": None,
        "server": None,
        "server_source": "none",
        "auth": {"method": "none", "source": None, "cloudflared_app": None},
        "local_sources": [
            {"name": "demo", "type": "path", "backend": "slurm",
             "path": "/repos/demo", "description": ""},
        ],
        "local_backends": [{"name": "slurm", "type": "slurm"}],
    }
    out = cli._render_status(data, probe=None)
    assert "local mode" in out
    assert "demo" in out
    assert "/u/scripthut.yaml" in out
    # No probe lines when offline.
    assert "Reachable" not in out


def test_status_renders_remote_authorized():
    data = {
        "version": "0.5.7",
        "install_path": "/x/y/scripthut",
        "config_paths": {"global": None, "project": None},
        "config_error": None,
        "server": "https://srv.example",
        "server_source": "config",
        "auth": {
            "method": "jwt", "source": "cloudflared",
            "cloudflared_app": "https://srv.example",
        },
        "local_sources": [],
        "local_backends": [],
    }
    probe = {
        "health": {"status_code": 200, "elapsed_ms": 80, "redirect_location": None},
        "authorized": {"status_code": 200, "redirect_location": None},
        "server_data": {"sources": [
            {"name": "demo", "type": "path", "backend": "slurm",
             "path": "/r/d", "description": ""},
        ]},
        "server_data_backends": {"backends": [
            {"name": "slurm", "type": "slurm", "connected": True},
        ]},
    }
    out = cli._render_status(data, probe=probe)
    assert "https://srv.example" in out
    assert "Reachable:     ✓" in out  # check mark
    assert "Authorized:    ✓" in out
    assert "demo" in out
    assert "slurm (connected)" in out


def test_status_renders_auth_failure_with_hint():
    data = {
        "version": "0.5.7",
        "install_path": None,
        "config_paths": {"global": None, "project": None},
        "config_error": None,
        "server": "https://srv.example",
        "server_source": "env",
        "auth": {
            "method": "jwt-pending", "source": "config",
            "cloudflared_app": "https://srv.example",
        },
        "local_sources": [], "local_backends": [],
    }
    probe = {
        "health": {"status_code": 302, "elapsed_ms": 50,
                   "redirect_location": "https://team.cloudflareaccess.com/.."},
        "authorized": {"status_code": 302,
                       "redirect_location": "https://team.cloudflareaccess.com/.."},
    }
    out = cli._render_status(data, probe=probe)
    # Hint should mention the cloudflared login command, populated with the app URL.
    assert "cloudflared access login https://srv.example" in out


@pytest.mark.asyncio
async def test_cmd_status_exit_code_zero_when_authorized(monkeypatch, capsys):
    """A successful probe exits 0."""
    monkeypatch.delenv("SCRIPTHUT_SERVER", raising=False)
    fake_cfg = MagicMock()
    fake_cfg.projects = []
    fake_cfg.backends = []
    fake_cfg.settings.cli_server = None
    fake_cfg.settings.cli_auth = None

    async def fake_probe(server, auth, **kw):
        return {
            "health": {"status_code": 200, "elapsed_ms": 5,
                       "redirect_location": None},
            "authorized": {"status_code": 200, "redirect_location": None},
            "server_data": {"sources": []},
            "server_data_backends": {"backends": []},
        }

    with patch.object(cli, "load_config", return_value=fake_cfg), \
         patch.object(cli, "_probe_server", side_effect=fake_probe):
        rc = await cli._cmd_status(_status_ns(server="https://srv.example"))

    assert rc == 0


@pytest.mark.asyncio
async def test_cmd_status_exit_code_nonzero_on_auth_failure(monkeypatch):
    """A 302 on /sources exits non-zero so the command composes in scripts."""
    monkeypatch.delenv("SCRIPTHUT_SERVER", raising=False)
    fake_cfg = MagicMock()
    fake_cfg.projects = []
    fake_cfg.backends = []
    fake_cfg.settings.cli_server = None
    fake_cfg.settings.cli_auth = None

    async def fake_probe(server, auth, **kw):
        return {
            "health": {"status_code": 200, "elapsed_ms": 5,
                       "redirect_location": None},
            "authorized": {"status_code": 302,
                           "redirect_location": "https://cf/login"},
        }

    with patch.object(cli, "load_config", return_value=fake_cfg), \
         patch.object(cli, "_probe_server", side_effect=fake_probe):
        rc = await cli._cmd_status(_status_ns(server="https://srv.example"))

    assert rc == 2


@pytest.mark.asyncio
async def test_cmd_status_quick_mode_skips_probe(monkeypatch, capsys):
    """--quick means no HTTP call, even with a remote configured."""
    monkeypatch.delenv("SCRIPTHUT_SERVER", raising=False)
    fake_cfg = MagicMock()
    fake_cfg.projects = []
    fake_cfg.backends = []
    fake_cfg.settings.cli_server = None
    fake_cfg.settings.cli_auth = None

    probe_called = MagicMock(side_effect=AssertionError("probe was called"))
    with patch.object(cli, "load_config", return_value=fake_cfg), \
         patch.object(cli, "_probe_server", probe_called):
        rc = await cli._cmd_status(
            _status_ns(server="https://srv.example", quick=True),
        )

    assert rc == 0
    probe_called.assert_not_called()


@pytest.mark.asyncio
async def test_cmd_status_json_includes_probe(monkeypatch, capsys):
    """--json output includes the probe results so scripts can parse them."""
    monkeypatch.delenv("SCRIPTHUT_SERVER", raising=False)
    fake_cfg = MagicMock()
    fake_cfg.projects = []
    fake_cfg.backends = []
    fake_cfg.settings.cli_server = None
    fake_cfg.settings.cli_auth = None

    async def fake_probe(server, auth, **kw):
        return {
            "health": {"status_code": 200, "elapsed_ms": 5,
                       "redirect_location": None},
            "authorized": {"status_code": 200, "redirect_location": None},
            "server_data": {"sources": []},
            "server_data_backends": {"backends": []},
        }

    with patch.object(cli, "load_config", return_value=fake_cfg), \
         patch.object(cli, "_probe_server", side_effect=fake_probe):
        rc = await cli._cmd_status(
            _status_ns(server="https://srv.example", json=True),
        )

    out = capsys.readouterr().out
    payload = __import__("json").loads(out)
    assert payload["server"] == "https://srv.example"
    assert payload["probe"]["health"]["status_code"] == 200
    assert rc == 0
