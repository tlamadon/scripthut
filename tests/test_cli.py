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

# -- helpers -----------------------------------------------------------------




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
    assert data["name"] == "src"
    assert "discover_error" not in data


@pytest.mark.asyncio
async def test_local_client_view_source_unknown_raises():
    runtime = MagicMock()
    runtime.config.get_source = MagicMock(return_value=None)
    client = cli.LocalClient.__new__(cli.LocalClient)
    client._runtime = runtime

    with pytest.raises(RuntimeError, match="not found"):
        await client.view_source("missing")










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
async def test_remote_client_raises_runtime_error_on_4xx():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, json={"detail": "Source not found"})

    client = _mock_remote(handler)
    with pytest.raises(RuntimeError, match="HTTP 422.*Source not found"):
        await client.view_source("missing")
    await client._client.aclose()


# -- subcommand dispatchers --------------------------------------------------










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
    assert "deadbeef" in out
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
















# -- stack list --source -----------------------------------------------------




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


























# -- --json output shapes ----------------------------------------------------












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




def _clean_args(*extra):
    return cli.build_parser().parse_args(
        ["disk", "clean", "--server", "http://x", "--interval", "0.01", *extra]
    )


def _exec_calls(calls):
    return [
        c for c in calls
        if c[1] == "/disk/clean" and not (c[2] or {}).get("dry_run")
    ]
