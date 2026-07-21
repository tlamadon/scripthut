"""Tests for the gh-style CLI (workflow/run noun groups)."""

from __future__ import annotations

import argparse
import json
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


def _leaf_parsers(
    parser: argparse.ArgumentParser, path: str = "scripthut",
) -> list[tuple[str, argparse.ArgumentParser]]:
    """Every runnable (handler-bearing) parser in the tree, with its path."""
    subs = [
        a for a in parser._actions if isinstance(a, argparse._SubParsersAction)
    ]
    leaves: list[tuple[str, argparse.ArgumentParser]] = []
    if not subs:
        return [(path, parser)]
    seen: set[int] = set()
    for action in subs:
        for name, sp in action.choices.items():
            if id(sp) in seen:  # aliases point at one parser
                continue
            seen.add(id(sp))
            leaves.extend(_leaf_parsers(sp, f"{path} {name}"))
    return leaves


def test_every_subcommand_accepts_json():
    """`--json` must be universal.

    `scripthut agent prompt` and the installed Claude skill both tell agents
    that every command takes --json; a subcommand that omits it dies with
    "unrecognized arguments: --json" in the middle of a pipeline.
    """
    missing = [
        path for path, sp in _leaf_parsers(cli.build_parser())
        if not any("--json" in a.option_strings for a in sp._actions)
    ]
    assert not missing, f"subcommands missing --json: {missing}"


def test_every_handler_honours_json():
    """Accepting --json and ignoring it is the same trap, one step later.

    Smoke check: each handler's source must mention json. `run manifest` is
    exempt in spirit only — its output is JSON unconditionally, and its
    docstring says so, which satisfies the check.
    """
    import inspect

    deaf = []
    for path, sp in _leaf_parsers(cli.build_parser()):
        handler = sp.get_default("handler")
        assert handler is not None, f"{path} has no handler"
        if "json" not in inspect.getsource(handler).lower():
            deaf.append(path)
    assert not deaf, f"handlers that ignore --json: {deaf}"


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
        "src", "train.json", backend="cluster-b", branch=None,
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
        "src", "train.json", backend=None, branch=None,
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
async def test_source_sync_single_name_calls_sync_source(capsys):
    """`scripthut source sync <name>` hits the per-source endpoint."""
    fake = _async_ctx(MagicMock())
    fake.sync_source = AsyncMock(return_value={
        "name": "src", "type": "git", "cloned": True,
        "last_commit": "deadbeef0123", "workflows": ["train.json"], "error": None,
    })

    args = _ns(name="src")
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_source_sync(args)

    fake.sync_source.assert_awaited_once_with("src")
    out = capsys.readouterr().out
    assert "src" in out
    assert "1 workflow" in out
    assert "deadbeef" in out
    assert rc == 0


@pytest.mark.asyncio
async def test_source_sync_no_name_syncs_all(capsys):
    fake = _async_ctx(MagicMock())
    fake.sync_all_sources = AsyncMock(return_value={
        "sources": [
            {"name": "a", "type": "git", "workflows": ["t.json"], "error": None,
             "cloned": True, "last_commit": "abc12345"},
            {"name": "b", "type": "path", "workflows": [], "error": None},
        ]
    })

    args = _ns(name=None)
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_source_sync(args)

    fake.sync_all_sources.assert_awaited_once_with()
    out = capsys.readouterr().out
    assert "a" in out
    assert "b" in out
    assert rc == 0


@pytest.mark.asyncio
async def test_source_sync_single_failure_returns_nonzero(capsys):
    """A specifically requested source failing should be a hard error."""
    fake = _async_ctx(MagicMock())
    fake.sync_source = AsyncMock(return_value={
        "name": "src", "type": "git", "workflows": [],
        "error": "git clone failed: permission denied",
    })

    args = _ns(name="src")
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_source_sync(args)

    out = capsys.readouterr().out
    assert "permission denied" in out
    # Exit non-zero so scripts can detect the failure.
    assert rc == 2


@pytest.mark.asyncio
async def test_source_sync_all_with_one_failure_still_returns_zero(capsys):
    """Per-source failures in a sync-all are reported but not fatal.

    Otherwise one broken source on a server would make the whole batch
    sync look like a failure — better to report per-entry and let the
    user act on what they see.
    """
    fake = _async_ctx(MagicMock())
    fake.sync_all_sources = AsyncMock(return_value={
        "sources": [
            {"name": "ok", "type": "git", "workflows": ["t.json"], "error": None,
             "cloned": True},
            {"name": "bad", "type": "git", "workflows": [], "error": "boom"},
        ]
    })

    args = _ns(name=None)
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_source_sync(args)

    out = capsys.readouterr().out
    assert "ok" in out and "bad" in out
    assert "boom" in out
    assert rc == 0


# -- _overlay_source_stacks --------------------------------------------------


class _MockResponse:
    """Minimal stand-in for the bits of httpx.Response we use."""

    def __init__(self, status_code: int, json_body: dict | None = None,
                 text: str = ""):
        self.status_code = status_code
        self._json = json_body or {}
        self.text = text

    def json(self):
        return self._json


def _fake_async_client_returning(resp: _MockResponse):
    """Build an `httpx.AsyncClient`-compatible context manager that
    yields a stub whose `.get()` always returns `resp`.
    """
    class _AsyncClient:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return None

        async def get(self, url):
            return resp

    return _AsyncClient


@pytest.mark.asyncio
async def test_overlay_source_stacks_noop_without_source_flag():
    """Default (no --source) must not touch config and must not call any
    server endpoint — otherwise the existing local-mode stack workflow
    suddenly grows a server dependency.
    """
    from scripthut.config_schema import ScriptHutConfig, Stack
    cfg = ScriptHutConfig(stacks=[Stack(name="local", prep="echo")])
    args = _ns()  # no source attribute
    out = await cli._overlay_source_stacks(args, cfg)
    assert out is cfg


@pytest.mark.asyncio
async def test_overlay_source_stacks_requires_server():
    """Setting --source without a resolvable server should fail loudly
    rather than silently falling back to local mode.
    """
    from scripthut.config_schema import ScriptHutConfig
    cfg = ScriptHutConfig()
    args = _ns(source="balke-jmp")
    with patch.object(cli, "_resolve_server", return_value=None):
        with pytest.raises(RuntimeError, match="requires a server URL"):
            await cli._overlay_source_stacks(args, cfg)


@pytest.mark.asyncio
async def test_overlay_source_stacks_fetches_and_merges():
    """Happy path: stacks from /api/v1/sources/{name}/config land on
    config.stacks alongside any local entries.
    """
    from scripthut.config_schema import ScriptHutConfig, Stack
    cfg = ScriptHutConfig(stacks=[Stack(name="from-local", prep="echo")])
    args = _ns(source="src")

    resp = _MockResponse(200, json_body={
        "source": "src", "config_present": True,
        "env": [], "env_groups": {},
        "stacks": [{"name": "from-repo", "prep": "echo repo"}],
    })
    fake_client = _fake_async_client_returning(resp)
    with patch.object(cli, "_resolve_server", return_value="https://srv"), \
         patch.object(cli, "_resolve_auth", return_value=None), \
         patch("scripthut.cli.httpx.AsyncClient", fake_client):
        out = await cli._overlay_source_stacks(args, cfg)

    names = {s.name for s in out.stacks}
    assert names == {"from-local", "from-repo"}


@pytest.mark.asyncio
async def test_overlay_source_stacks_source_wins_on_collision():
    """Same stack name in both local config and the repo's project YAML
    — repo wins, mirroring `_merge_configs`'s "project-local overrides".
    """
    from scripthut.config_schema import ScriptHutConfig, Stack
    cfg = ScriptHutConfig(stacks=[
        Stack(name="julia", prep="echo from-local"),
    ])
    args = _ns(source="src")

    resp = _MockResponse(200, json_body={
        "source": "src", "config_present": True,
        "env": [], "env_groups": {},
        "stacks": [{"name": "julia", "prep": "echo from-repo"}],
    })
    fake_client = _fake_async_client_returning(resp)
    with patch.object(cli, "_resolve_server", return_value="https://srv"), \
         patch.object(cli, "_resolve_auth", return_value=None), \
         patch("scripthut.cli.httpx.AsyncClient", fake_client):
        out = await cli._overlay_source_stacks(args, cfg)

    by_name = {s.name: s for s in out.stacks}
    assert "from-repo" in by_name["julia"].prep


@pytest.mark.asyncio
async def test_overlay_source_stacks_missing_config_is_noop():
    """`config_present=False` on the server means there's no
    scripthut.yaml in the repo — the CLI should leave local stacks alone.
    """
    from scripthut.config_schema import ScriptHutConfig, Stack
    cfg = ScriptHutConfig(stacks=[Stack(name="local", prep="echo")])
    args = _ns(source="src")

    resp = _MockResponse(200, json_body={
        "source": "src", "config_present": False,
        "env": [], "env_groups": {}, "stacks": [],
    })
    fake_client = _fake_async_client_returning(resp)
    with patch.object(cli, "_resolve_server", return_value="https://srv"), \
         patch.object(cli, "_resolve_auth", return_value=None), \
         patch("scripthut.cli.httpx.AsyncClient", fake_client):
        out = await cli._overlay_source_stacks(args, cfg)

    assert [s.name for s in out.stacks] == ["local"]


@pytest.mark.asyncio
async def test_overlay_source_stacks_unknown_source_raises():
    """404 from the server should be a clear "unknown source" error,
    not a generic HTTP error or silent skip.
    """
    from scripthut.config_schema import ScriptHutConfig
    cfg = ScriptHutConfig()
    args = _ns(source="missing")

    resp = _MockResponse(404, text="Not found")
    fake_client = _fake_async_client_returning(resp)
    with patch.object(cli, "_resolve_server", return_value="https://srv"), \
         patch.object(cli, "_resolve_auth", return_value=None), \
         patch("scripthut.cli.httpx.AsyncClient", fake_client):
        with pytest.raises(RuntimeError, match="not found on the server"):
            await cli._overlay_source_stacks(args, cfg)


@pytest.mark.asyncio
async def test_overlay_source_stacks_broken_repo_yaml_raises():
    """422 from the server (forbidden section, malformed YAML) must
    surface the server's detail message so the operator can fix the repo.
    """
    from scripthut.config_schema import ScriptHutConfig
    cfg = ScriptHutConfig()
    args = _ns(source="src")

    resp = _MockResponse(422, json_body={
        "detail": "source 'src'/scripthut.yaml contains forbidden section: backends",
    })
    fake_client = _fake_async_client_returning(resp)
    with patch.object(cli, "_resolve_server", return_value="https://srv"), \
         patch.object(cli, "_resolve_auth", return_value=None), \
         patch("scripthut.cli.httpx.AsyncClient", fake_client):
        with pytest.raises(RuntimeError, match="backends"):
            await cli._overlay_source_stacks(args, cfg)


# -- stack list --source -----------------------------------------------------


@pytest.mark.asyncio
async def test_stack_list_with_source_flag_overlays(capsys):
    """End-to-end: `stack list --source <name>` overlays repo stacks
    and prints them alongside local ones.
    """
    from scripthut.config_schema import ScriptHutConfig, Stack
    local_cfg = ScriptHutConfig(stacks=[Stack(name="local", prep="echo")])
    repo_cfg = ScriptHutConfig(stacks=[Stack(name="from-repo", prep="echo")])

    args = _ns(source="src")
    with patch.object(cli, "load_config", return_value=local_cfg), \
         patch.object(cli, "_overlay_source_stacks",
                      AsyncMock(return_value=repo_cfg.model_copy(
                          update={"stacks": list(local_cfg.stacks)
                                  + list(repo_cfg.stacks)},
                      ))):
        rc = await cli._cmd_stack_list(args)

    out = capsys.readouterr().out
    assert "from-repo" in out
    assert "local" in out
    assert rc == 0


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


# -- --json output shapes ----------------------------------------------------


@pytest.mark.asyncio
async def test_workflow_run_json_emits_run_summary(capsys):
    """`workflow run --json | jq -r .id` is the documented submit-and-capture
    recipe — the output must be a single parseable document."""
    run = _make_run(statuses=[RunItemStatus.SUBMITTED])
    fake = _async_ctx(MagicMock())
    fake.run_source_workflow = AsyncMock(return_value=cli._summary_from_run(run))

    args = _ns(name="train.json", source="src", backend=None, json=True)
    with patch.object(cli, "_make_client", return_value=fake):
        with patch.object(cli, "_resolve_server", return_value=None):
            rc = await cli._cmd_workflow_run(args)

    payload = json.loads(capsys.readouterr().out)
    assert payload["id"] == "abc12345"
    assert payload["backend_name"] == "cluster"
    assert rc == 0


@pytest.mark.asyncio
async def test_run_rerun_json_emits_run_summary(capsys):
    run = _make_run(statuses=[RunItemStatus.SUBMITTED])
    fake = _async_ctx(MagicMock())
    fake.rerun = AsyncMock(return_value=cli._summary_from_run(run))

    args = _ns(id="old", json=True)
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_run_rerun(args)

    assert json.loads(capsys.readouterr().out)["id"] == "abc12345"
    assert rc == 0


@pytest.mark.asyncio
async def test_run_cancel_json_emits_ack(capsys):
    fake = _async_ctx(MagicMock())
    fake.cancel_run = AsyncMock(return_value={"run_id": "abc", "cancelled": True})

    args = _ns(id="abc", json=True)
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_run_cancel(args)

    assert json.loads(capsys.readouterr().out) == {"id": "abc", "cancelled": True}
    assert rc == 0


@pytest.mark.asyncio
async def test_run_logs_json_wraps_content(capsys):
    fake = _async_ctx(MagicMock())
    fake.fetch_logs = AsyncMock(
        return_value={"run_id": "r", "task_id": "t", "type": "output", "content": "hello"}
    )

    args = _ns(id="r", task="t", error=False, tail=None, follow=False,
               interval=2.0, json=True)
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_run_logs(args)

    payload = json.loads(capsys.readouterr().out)
    assert payload["content"] == "hello"
    assert payload["task"] == "t"
    assert rc == 0


@pytest.mark.asyncio
async def test_run_watch_json_prints_only_final_state(capsys):
    """The live redraw is ANSI cursor trickery — under --json it must be
    suppressed so stdout holds exactly one JSON document."""
    states = ["running", "completed"]
    n = {"i": 0}

    async def fake_view(run_id):
        idx = min(n["i"], len(states) - 1)
        n["i"] += 1
        return {
            "id": run_id, "workflow_name": "demo", "backend_name": "cluster",
            "created_at": "2026-05-15T12:00:00+00:00", "status": states[idx],
            "task_count": 1, "completed_count": idx, "submitted_count": 1,
            "status_counts": {}, "items": [],
        }

    fake = _async_ctx(MagicMock())
    fake.view_run = fake_view

    args = _ns(id="abc", interval=0.0, exit_status=True, json=True)
    with patch.object(cli, "_make_client", return_value=fake):
        rc = await cli._cmd_run_watch(args)

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "completed"
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


# -- stack remote routing -----------------------------------------------------


def _stack_args(**kwargs) -> argparse.Namespace:
    """Defaults for stack-command argparse namespaces."""
    defaults = dict(
        server=None, config=None, json=False,
        source=None, backend=None, name=None, rebuild=False,
        watch=False, interval=2.0,
        cf_client_id=None, cf_client_secret=None,
        cf_access_token=None, cloudflared_app=None,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


@pytest.mark.asyncio
async def test_stack_install_remote_submits_run_and_prints_id(capsys):
    """v0.9.0: ``stack install`` against a server submits a workflow
    run and prints the run_id + a hint, returning 0 on submission.
    """
    call_log: dict = {}

    async def fake_remote_call(args, server, method, path, *,
                               params=None, timeout=60.0, use_auth=True):
        call_log["method"] = method
        call_log["path"] = path
        call_log["params"] = params
        return {
            "id": "abc12345",
            "workflow_name": "_stack/src/foo",
            "backend_name": "cluster",
            "created_at": "2026-01-01T00:00:00+00:00",
            "status": "submitted",
            "task_count": 1, "completed_count": 0, "submitted_count": 1,
            "status_counts": {},
        }

    args = _stack_args(name="foo", backend="cluster", source="src", rebuild=True)
    with patch.object(cli, "_resolve_server_with_source", return_value=("https://srv", "flag")), \
         patch.object(cli, "_remote_stack_call", side_effect=fake_remote_call):
        rc = await cli._cmd_stack_install(args)

    assert call_log["method"] == "POST"
    assert call_log["path"] == "/stacks/foo/install"
    assert call_log["params"]["rebuild"] == "true"
    out = capsys.readouterr().out
    assert "abc12345" in out
    # Default (no --watch) prints the hint to wait via run watch.
    assert "scripthut run watch abc12345" in out
    assert rc == 0


@pytest.mark.asyncio
async def test_stack_install_remote_watch_blocks_until_done(capsys):
    """``--watch`` after submission should re-enter the existing run-watch
    polling so the same UX as `run watch --exit-status` works.
    """

    async def fake_remote_call(*a, **kw):
        return {
            "id": "ready1", "workflow_name": "_stack/foo",
            "backend_name": "cluster",
            "created_at": "2026-01-01T00:00:00+00:00",
            "status": "submitted",
            "task_count": 1, "completed_count": 0, "submitted_count": 1,
            "status_counts": {},
        }

    watch_calls: list = []

    async def fake_watch(watch_args):
        watch_calls.append(watch_args)
        return 0

    args = _stack_args(name="foo", backend="cluster", watch=True)
    with patch.object(cli, "_resolve_server_with_source", return_value=("https://srv", "flag")), \
         patch.object(cli, "_remote_stack_call", side_effect=fake_remote_call), \
         patch.object(cli, "_cmd_run_watch", side_effect=fake_watch):
        rc = await cli._cmd_stack_install(args)

    assert rc == 0
    assert len(watch_calls) == 1
    # The watch handler must see the submitted run's id and
    # exit_status=True so a failed install returns non-zero.
    assert watch_calls[0].id == "ready1"
    assert watch_calls[0].exit_status is True


@pytest.mark.asyncio
async def test_stack_install_remote_json_flag_prints_run_summary(capsys):
    """``--json`` outputs the submitted-run summary verbatim so scripts
    can `jq -r .id` to capture the run_id, matching `task run --json`.
    """
    async def fake_remote_call(*a, **kw):
        return {
            "id": "jsonok", "workflow_name": "_stack/foo",
            "backend_name": "cluster",
            "created_at": "2026-01-01T00:00:00+00:00",
            "status": "submitted",
            "task_count": 1, "completed_count": 0, "submitted_count": 1,
            "status_counts": {},
        }
    args = _stack_args(name="foo", backend="cluster", json=True)
    with patch.object(cli, "_resolve_server_with_source", return_value=("https://srv", "flag")), \
         patch.object(cli, "_remote_stack_call", side_effect=fake_remote_call):
        rc = await cli._cmd_stack_install(args)
    out = capsys.readouterr().out
    payload = __import__("json").loads(out)
    assert payload["id"] == "jsonok"
    assert rc == 0


@pytest.mark.asyncio
async def test_stack_check_remote_dispatches_to_api(capsys):
    call_log: dict = {}

    async def fake_remote_call(args, server, method, path, *,
                               params=None, timeout=1800.0, use_auth=True):
        call_log["path"] = path
        return {"name": "foo", "backend": "cluster", "state": "ready",
                "hash": "abc12345", "path": "/c/foo/abc12345",
                "last_built": None, "size_bytes": None, "error": None}

    args = _stack_args(name="foo", backend="cluster")
    with patch.object(cli, "_resolve_server_with_source", return_value=("https://srv", "flag")), \
         patch.object(cli, "_remote_stack_call", side_effect=fake_remote_call):
        rc = await cli._cmd_stack_check(args)

    assert call_log["path"] == "/stacks/foo/check"
    assert rc == 0


@pytest.mark.asyncio
async def test_stack_check_remote_missing_state_returns_one():
    """``state != "ready"`` translates to a non-zero exit code so CI
    gates can rely on it.
    """
    async def fake_remote_call(*a, **kw):
        return {"name": "foo", "backend": "cluster", "state": "missing",
                "hash": "", "path": "", "last_built": None,
                "size_bytes": None, "error": None}

    args = _stack_args(name="foo", backend="cluster")
    with patch.object(cli, "_resolve_server_with_source", return_value=("https://srv", "flag")), \
         patch.object(cli, "_remote_stack_call", side_effect=fake_remote_call):
        rc = await cli._cmd_stack_check(args)
    assert rc == 1


@pytest.mark.asyncio
async def test_stack_delete_remote_dispatches_to_api(capsys):
    call_log: dict = {}

    async def fake_remote_call(args, server, method, path, *,
                               params=None, timeout=1800.0, use_auth=True):
        call_log["method"] = method
        call_log["path"] = path
        return {"name": "foo", "backend": "cluster", "deleted": True}

    args = _stack_args(name="foo", backend="cluster")
    with patch.object(cli, "_resolve_server_with_source", return_value=("https://srv", "flag")), \
         patch.object(cli, "_remote_stack_call", side_effect=fake_remote_call):
        rc = await cli._cmd_stack_delete(args)

    assert call_log["method"] == "DELETE"
    assert call_log["path"] == "/stacks/foo"
    assert rc == 0


@pytest.mark.asyncio
async def test_stack_install_remote_requires_backend(capsys):
    """``--backend`` is required when in server mode. v1 doesn't iterate
    over backends across separate API calls — the operator picks one.
    The error message must point at the right fix.
    """
    args = _stack_args(name="foo", backend=None)
    with patch.object(cli, "_resolve_server_with_source", return_value=("https://srv", "flag")):
        with pytest.raises(RuntimeError, match="--backend is required"):
            await cli._cmd_stack_install(args)


@pytest.mark.asyncio
async def test_stack_check_remote_requires_name(capsys):
    """``stack check`` without a name works locally (checks every stack)
    but in server mode the URL needs a name — error rather than guess.
    """
    args = _stack_args(name=None, backend="cluster")
    with patch.object(cli, "_resolve_server_with_source", return_value=("https://srv", "flag")):
        rc = await cli._cmd_stack_check(args)
    assert rc == 2


@pytest.mark.asyncio
async def test_stack_install_local_path_unchanged_when_no_server(monkeypatch):
    """``--server local`` → the in-process local-SSH path, which is what
    the existing tests already cover. This just confirms the dispatch
    picks the right branch. (Without the local keyword, no-server now
    routes through the local daemon — see the _ensure_local_server tests.)
    """
    monkeypatch.delenv("SCRIPTHUT_SERVER", raising=False)
    args = _stack_args(name="foo", backend="cluster", server="local")
    fake_cfg = MagicMock()
    fake_cfg.get_stack.return_value = None  # short-circuits with "not found"
    fake_cfg.settings.cli_server = None
    remote_called = AsyncMock(side_effect=AssertionError("remote called!"))
    with patch.object(cli, "load_config", return_value=fake_cfg), \
         patch.object(cli, "_overlay_source_stacks",
                      AsyncMock(return_value=fake_cfg)), \
         patch.object(cli, "_remote_stack_call", remote_called):
        rc = await cli._cmd_stack_install(args)
    # Stack not in local config → 2; the important assertion is that
    # _remote_stack_call was never invoked.
    assert rc == 2
    remote_called.assert_not_called()


# -- local daemon autostart ---------------------------------------------------


def _daemon_cfg(tmp_path, autostart="ask", port=8123):
    from scripthut.config_schema import GlobalSettings, ScriptHutConfig
    return ScriptHutConfig(settings=GlobalSettings(
        data_dir=tmp_path, server_host="127.0.0.1", server_port=port,
        cli_autostart=autostart,
    ))


def _tty(monkeypatch, is_tty: bool):
    import sys as _sys
    monkeypatch.setattr(_sys, "stdin", MagicMock(isatty=lambda: is_tty))


def _answers(monkeypatch, *replies):
    it = iter(replies)
    monkeypatch.setattr("builtins.input", lambda: next(it))


def test_parser_daemon_subcommands_dispatch():
    parser = cli.build_parser()
    assert parser.parse_args(["daemon", "start"]).handler is cli._cmd_daemon_start
    assert parser.parse_args(["daemon", "stop"]).handler is cli._cmd_daemon_stop
    assert parser.parse_args(["daemon", "status", "--json"]).handler is cli._cmd_daemon_status
    logs = parser.parse_args(["daemon", "logs", "--tail", "7"])
    assert logs.handler is cli._cmd_daemon_logs
    assert logs.tail == 7


def test_ensure_local_server_uses_running_daemon(tmp_path, monkeypatch):
    """A live daemon is used as-is: no prompt, no spawn, whatever the mode."""
    cfg = _daemon_cfg(tmp_path, autostart="never")
    monkeypatch.setattr(
        "builtins.input",
        lambda: (_ for _ in ()).throw(AssertionError("prompted!")),
    )
    with patch.object(cli, "load_config", return_value=cfg), \
         patch("scripthut.daemon.ping", return_value={"status": "pong", "pid": 1}), \
         patch("scripthut.daemon.start_daemon",
               side_effect=AssertionError("spawned!")):
        url = cli._ensure_local_server(_ns())
    assert url == "http://127.0.0.1:8123"


def test_ensure_local_server_never_mode_fails_with_guidance(tmp_path, monkeypatch):
    cfg = _daemon_cfg(tmp_path, autostart="never")
    _tty(monkeypatch, True)
    with patch.object(cli, "load_config", return_value=cfg), \
         patch("scripthut.daemon.ping", return_value=None):
        with pytest.raises(RuntimeError) as exc:
            cli._ensure_local_server(_ns())
    msg = str(exc.value)
    assert "--server local" in msg
    assert "scripthut daemon start" in msg
    assert "not prompting" not in msg


def test_ensure_local_server_ask_non_tty_fails_without_prompt(tmp_path, monkeypatch):
    cfg = _daemon_cfg(tmp_path, autostart="ask")
    _tty(monkeypatch, False)
    monkeypatch.setattr(
        "builtins.input",
        lambda: (_ for _ in ()).throw(AssertionError("prompted!")),
    )
    with patch.object(cli, "load_config", return_value=cfg), \
         patch("scripthut.daemon.ping", return_value=None), \
         patch("scripthut.daemon.start_daemon",
               side_effect=AssertionError("spawned!")):
        with pytest.raises(RuntimeError, match="not prompting"):
            cli._ensure_local_server(_ns())


def test_ensure_local_server_ask_yes_spawns(tmp_path, monkeypatch):
    from scripthut.daemon import DaemonInfo
    cfg = _daemon_cfg(tmp_path, autostart="ask")
    _tty(monkeypatch, True)
    _answers(monkeypatch, "y", "n")  # start? yes; remember? no
    info = DaemonInfo(pid=99, host="127.0.0.1", port=8123, started_at="")
    remember = MagicMock()
    with patch.object(cli, "load_config", return_value=cfg), \
         patch("scripthut.daemon.ping", return_value=None), \
         patch("scripthut.daemon.start_daemon", return_value=info) as start, \
         patch.object(cli, "set_global_setting", remember):
        url = cli._ensure_local_server(_ns())
    assert url == "http://127.0.0.1:8123"
    start.assert_called_once()
    remember.assert_not_called()


def test_ensure_local_server_ask_yes_remembers_always(tmp_path, monkeypatch):
    from scripthut.daemon import DaemonInfo
    cfg = _daemon_cfg(tmp_path, autostart="ask")
    _tty(monkeypatch, True)
    _answers(monkeypatch, "y", "y")  # start? yes; remember? yes
    info = DaemonInfo(pid=99, host="127.0.0.1", port=8123, started_at="")
    remember = MagicMock(return_value=tmp_path / "scripthut.yaml")
    with patch.object(cli, "load_config", return_value=cfg), \
         patch("scripthut.daemon.ping", return_value=None), \
         patch("scripthut.daemon.start_daemon", return_value=info), \
         patch.object(cli, "set_global_setting", remember):
        cli._ensure_local_server(_ns())
    remember.assert_called_once_with("cli_autostart", "always")


def test_ensure_local_server_ask_no_remembers_never_and_fails(tmp_path, monkeypatch):
    cfg = _daemon_cfg(tmp_path, autostart="ask")
    _tty(monkeypatch, True)
    _answers(monkeypatch, "n", "y")  # start? no; remember? yes
    remember = MagicMock(return_value=tmp_path / "scripthut.yaml")
    with patch.object(cli, "load_config", return_value=cfg), \
         patch("scripthut.daemon.ping", return_value=None), \
         patch("scripthut.daemon.start_daemon",
               side_effect=AssertionError("spawned!")), \
         patch.object(cli, "set_global_setting", remember):
        with pytest.raises(RuntimeError, match="--server local"):
            cli._ensure_local_server(_ns())
    remember.assert_called_once_with("cli_autostart", "never")


def test_ensure_local_server_always_spawns_without_prompt(tmp_path, monkeypatch):
    from scripthut.daemon import DaemonInfo
    cfg = _daemon_cfg(tmp_path, autostart="always")
    _tty(monkeypatch, False)  # even non-interactive: always is an explicit opt-in
    monkeypatch.setattr(
        "builtins.input",
        lambda: (_ for _ in ()).throw(AssertionError("prompted!")),
    )
    info = DaemonInfo(pid=99, host="127.0.0.1", port=8123, started_at="")
    with patch.object(cli, "load_config", return_value=cfg), \
         patch("scripthut.daemon.ping", return_value=None), \
         patch("scripthut.daemon.start_daemon", return_value=info) as start:
        url = cli._ensure_local_server(_ns())
    assert url == "http://127.0.0.1:8123"
    start.assert_called_once()


def test_make_client_three_way_split(monkeypatch):
    """flag/env/config URL → RemoteClient with auth; --server local →
    LocalClient; nothing → RemoteClient at the daemon URL with NO auth
    resolution (it could shell out to cloudflared)."""
    args = _ns()

    with patch.object(cli, "_resolve_server_with_source",
                      return_value=("https://srv", "flag")), \
         patch.object(cli, "_resolve_auth", return_value=None) as auth:
        client = cli._make_client(args)
    assert isinstance(client, cli.RemoteClient)
    auth.assert_called_once()

    with patch.object(cli, "_resolve_server_with_source",
                      return_value=(None, "local-keyword")):
        client = cli._make_client(args)
    assert isinstance(client, cli.LocalClient)

    with patch.object(cli, "_resolve_server_with_source",
                      return_value=(None, "none")), \
         patch.object(cli, "_ensure_local_server",
                      return_value="http://127.0.0.1:8123"), \
         patch.object(cli, "_resolve_auth",
                      side_effect=AssertionError("auth resolved!")):
        client = cli._make_client(args)
    assert isinstance(client, cli.RemoteClient)
    assert client.base_url == "http://127.0.0.1:8123"
    assert client.auth is None


def test_main_renders_connection_errors_cleanly(capsys):
    """httpx.RequestError must become a friendly message, not a traceback."""
    async def boom(args):
        raise httpx.ConnectError(
            "[Errno 61] Connection refused",
            request=httpx.Request("GET", "http://127.0.0.1:8123/api/v1/runs"),
        )

    fake_parser = MagicMock()
    fake_parser.parse_args.return_value = argparse.Namespace(handler=boom)
    with patch.object(cli, "build_parser", return_value=fake_parser):
        rc = cli.main(["run", "list"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "could not reach the server" in err
    assert "http://127.0.0.1:8123/api/v1/runs" in err
    assert "daemon status" in err


# -- disk subcommands --------------------------------------------------------


def _disk_result_dict(**overrides):
    base = {
        "backend": "hpc",
        "scanned_at": "2026-07-17T10:00:00+00:00",
        "duration_ms": 1200,
        "home_dir": "/home/alice",
        "disk_total_bytes": 100 * 1024**3,
        "disk_avail_bytes": 41 * 1024**3,
        "total_bytes": 3 * 1024**3,
        "totals_by_class": {"orphaned": [2, 2 * 1024**3], "active": [1, 1024**3]},
        "totals_by_kind": {"clone": [3, 3 * 1024**3]},
        "entries": [
            {
                "path": "/home/alice/scripthut-repos/a1b2c3d4e5f6",
                "kind": "clone",
                "size_bytes": 1024**3,
                "mtime": "2026-07-17T08:00:00+00:00",
                "classification": "active",
                "run_ids": ["r1", "r2", "r3"],
                "detail": None,
                "ready": None,
            },
            {
                "path": "/home/alice/scripthut-repos/ffffffffffff",
                "kind": "clone",
                "size_bytes": None,
                "mtime": None,
                "classification": "orphaned",
                "run_ids": [],
                "detail": None,
                "ready": None,
            },
        ],
        "errors": ["scan exited 1: boom"],
    }
    base.update(overrides)
    return base


def test_parser_disk_defaults_to_status():
    parser = cli.build_parser()
    args = parser.parse_args(["disk"])
    assert args.handler is cli._cmd_disk_status
    assert args.backend is None
    assert args.json is False


def test_parser_disk_scan_flags():
    parser = cli.build_parser()
    args = parser.parse_args(["disk", "scan", "--backend", "hpc", "--json"])
    assert args.handler is cli._cmd_disk_scan
    assert args.backend == "hpc"
    assert args.json is True


def test_print_disk_result_table(capsys):
    cli._print_disk_result("hpc", _disk_result_dict())
    captured = capsys.readouterr()
    out = captured.out
    assert "KIND" in out and "CLASS" in out
    assert "a1b2c3d4e5f6" in out
    assert "active" in out and "orphaned" in out
    # three run ids truncate to two plus a count
    assert "r1,r2 +1" in out
    # unsized entry renders a dash
    assert "ffffffffffff" in out
    # summary line
    assert "orphaned 2.0G in 2" in out
    assert "disk 41.0G free" in out
    # errors go to stderr
    assert "scan exited 1" in captured.err


def test_render_disk_response_states(capsys):
    data = {
        "backends": {
            "a": {"scanning": True, "result": None},
            "b": {"scanning": False, "result": None},
            "c": {"scanning": False, "result": _disk_result_dict(backend="c")},
        }
    }
    rc = cli._render_disk_response(data, as_json=False)
    assert rc == 0
    out = capsys.readouterr().out
    assert "a: scan in progress" in out
    assert "b: never scanned" in out
    assert "c:" in out


def test_render_disk_response_json(capsys):
    import json as _json

    data = {"backends": {"hpc": {"scanning": False, "result": None}}}
    cli._render_disk_response(data, as_json=True)
    parsed = _json.loads(capsys.readouterr().out)
    assert parsed == data


# -- disk clean --------------------------------------------------------------

CLEAN_PLAN = {
    "backend": "hpc",
    "planned_at": "2026-07-17T12:00:00+00:00",
    "entries": [
        {
            "entry": {
                "path": "/h/r/aaaaaaaaaaaa", "kind": "clone", "size_bytes": 1024,
                "mtime": None, "classification": "orphaned", "run_ids": [],
                "detail": None, "ready": None,
            },
            "action": "delete", "reason": None,
            "warnings": ["may contain logs of forgotten runs (not archived)"],
        },
        {
            "entry": {
                "path": "/h/r/agent-11111111", "kind": "agent", "size_bytes": 512,
                "mtime": None, "classification": "orphaned", "run_ids": [],
                "detail": None, "ready": None,
            },
            "action": "skip", "reason": "workspace has uncommitted changes",
            "warnings": [],
        },
    ],
    "counts": {"delete": 1, "check": 0, "skip": 1},
    "delete_bytes": 1024,
    "errors": [],
}

CLEAN_REPORT = {
    "backend": "hpc",
    "outcomes": [
        {"path": "/h/r/aaaaaaaaaaaa", "kind": "clone", "size_bytes": 1024,
         "outcome": "deleted", "reason": None},
        {"path": "/h/r/agent-11111111", "kind": "agent", "size_bytes": 512,
         "outcome": "skipped", "reason": "workspace has uncommitted changes"},
    ],
    "errors": [],
    "counts": {"deleted": 1, "skipped": 1, "failed": 0},
    "freed_bytes": 1024,
    "freed_is_lower_bound": False,
}


def _fake_disk_server():
    """(calls, fake _remote_stack_call) recording every HTTP interaction."""
    calls = []

    async def fake(args, server, method, path, **kw):
        calls.append((method, path, kw.get("json_body")))
        if method == "GET" and path == "/disk":
            return {"backends": {"hpc": {
                "scanning": False, "cleaning": False,
                "result": None, "last_cleanup": CLEAN_REPORT,
            }}}
        if path == "/disk/scan":
            return {"backend": "hpc", "status": "started"}
        if path == "/disk/clean":
            body = kw.get("json_body") or {}
            if body.get("dry_run"):
                return {"backend": "hpc", "dry_run": True, "plan": CLEAN_PLAN}
            return {"backend": "hpc", "status": "started"}
        raise AssertionError(f"unexpected call {method} {path}")

    return calls, fake


def _clean_args(*extra):
    return cli.build_parser().parse_args(
        ["disk", "clean", "--server", "http://x", "--interval", "0.01", *extra]
    )


def _exec_calls(calls):
    return [
        c for c in calls
        if c[1] == "/disk/clean" and not (c[2] or {}).get("dry_run")
    ]


def test_parser_disk_clean_flags():
    args = cli.build_parser().parse_args(
        ["disk", "clean", "--backend", "hpc", "--path", "/x", "--yes", "--dry-run"]
    )
    assert args.handler is cli._cmd_disk_clean
    assert args.paths == ["/x"]
    assert args.yes is True and args.dry_run is True


async def test_disk_clean_path_requires_backend(capsys):
    args = _clean_args("--path", "/x")
    assert await cli._cmd_disk_clean(args) == 2
    assert "--path requires --backend" in capsys.readouterr().err


async def test_disk_clean_dry_run_never_execs(capsys):
    calls, fake = _fake_disk_server()
    with patch.object(cli, "_remote_stack_call", new=fake):
        rc = await cli._cmd_disk_clean(_clean_args("--dry-run"))
    assert rc == 0
    assert _exec_calls(calls) == []
    out = capsys.readouterr().out
    assert "would delete 1 entries" in out
    assert "uncommitted changes" in out  # skip reason shown in the plan


async def test_disk_clean_declined_confirm_no_exec(capsys):
    calls, fake = _fake_disk_server()
    with patch.object(cli, "_remote_stack_call", new=fake), \
         patch.object(cli, "_confirm_stderr", return_value=False):
        rc = await cli._cmd_disk_clean(_clean_args())
    assert rc == 0
    assert _exec_calls(calls) == []
    assert "not confirmed" in capsys.readouterr().err


async def test_disk_clean_yes_sends_plan_paths(capsys):
    calls, fake = _fake_disk_server()
    with patch.object(cli, "_remote_stack_call", new=fake):
        rc = await cli._cmd_disk_clean(_clean_args("--yes"))
    assert rc == 0
    execs = _exec_calls(calls)
    assert len(execs) == 1
    # exec sends exactly the non-skip paths the user was shown
    assert execs[0][2]["paths"] == ["/h/r/aaaaaaaaaaaa"]
    out = capsys.readouterr().out
    assert "deleted 1" in out and "freed 1.0K" in out


def test_print_cleanup_plan_table(capsys):
    cli._print_cleanup_plan("hpc", CLEAN_PLAN)
    out = capsys.readouterr().out
    assert "ACTION" in out
    assert "aaaaaaaaaaaa" in out and "delete" in out
    assert "agent-11111111" in out and "skip" in out
    assert "would delete 1 entries (~1.0K); 1 skipped" in out


def test_print_cleanup_report_lines(capsys):
    report = dict(CLEAN_REPORT)
    report["errors"] = ["delete script failed: boom"]
    report["freed_is_lower_bound"] = True
    cli._print_cleanup_report("hpc", report)
    captured = capsys.readouterr()
    assert "✓ deleted" in captured.out
    assert "- skipped" in captured.out
    assert "freed at least 1.0K" in captured.out
    assert "delete script failed: boom" in captured.err


def test_status_lists_local_backends_from_real_config(monkeypatch):
    """Regression: local_backends must read BackendConfig.type (the config
    field), not .backend_type (a BackendState attribute) — a real config
    object raises AttributeError on the latter, which MagicMock-based
    tests silently tolerated."""
    from scripthut.config_schema import (
        LocalBackendConfig,
        ScriptHutConfig,
        SlurmBackendConfig,
        SSHConfig,
    )

    monkeypatch.delenv("SCRIPTHUT_SERVER", raising=False)
    monkeypatch.delenv("SCRIPTHUT_CF_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("SCRIPTHUT_CLOUDFLARED_APP", raising=False)
    real_cfg = ScriptHutConfig(backends=[
        SlurmBackendConfig(
            name="cluster", type="slurm", ssh=SSHConfig(host="h", user="u"),
        ),
        LocalBackendConfig(name="laptop"),
    ])
    with patch.object(cli, "load_config", return_value=real_cfg), \
         patch("scripthut.config.discover_global_config", return_value=None), \
         patch("scripthut.config.discover_project_config", return_value=None):
        data = cli._gather_status(_status_ns())

    assert data["local_backends"] == [
        {"name": "cluster", "type": "slurm"},
        {"name": "laptop", "type": "local"},
    ]
