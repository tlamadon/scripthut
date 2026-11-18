"""Plain-config startup and terminal attachment without legacy execution."""

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from scripthut import main
from scripthut.config_schema import GlobalSettings, ScriptHutConfig


def test_plain_startup_preserves_old_data(tmp_path, monkeypatch):
    old = tmp_path / "workflows" / "old.json"
    old.parent.mkdir()
    old.write_bytes(b'not a readable legacy record')
    ledger = tmp_path / "usage.jsonl"
    ledger.write_bytes(b'old usage\n')
    before = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in (old, ledger)}
    config = ScriptHutConfig(settings=GlobalSettings(data_dir=tmp_path))
    monkeypatch.setattr(main, "load_config", lambda _: config)
    monkeypatch.setattr(main.state, "backends", {})
    monkeypatch.setattr(main.state, "config_error", None)
    import asyncio
    monkeypatch.setattr(main.state, "_shutdown_event", asyncio.Event())
    with TestClient(main.app) as client:
        for _ in range(100):
            if main.state.startup_phase is None:
                break
            time.sleep(.01)
        assert main.state.startup_phase is None
        assert client.get("/").url.path == "/"
        for path in ("/runs", "/disk", "/scriptflow", "/backends", "/sources", "/terminals", "/api/v1/scriptflow/runs"):
            assert client.get(path).status_code == 200
        for path in ("/api/v1/runs", "/disk/cleanup", "/api/v1/tasks/run"):
            assert client.get(path).status_code == 404
    assert before == {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in before}


def test_terminal_only_attaches_existing_session(monkeypatch):
    ssh = SimpleNamespace(create_interactive_session=AsyncMock(side_effect=RuntimeError("test relay closed")))
    backend = SimpleNamespace(ssh_client=ssh)
    monkeypatch.setattr(main.state, "backends", {"cluster": backend})
    with TestClient(main.app).websocket_connect("/ws/terminal/cluster") as ws:
        ws.send_json({"type": "init", "node": "compute-1", "session": "sf-example"})
        assert ws.receive_json()["type"] == "error"
    assert ssh.create_interactive_session.call_args.kwargs["command"] == 'ssh -tt compute-1 "tmux attach -t sf-example"'
    ssh.create_interactive_session.reset_mock()
    with TestClient(main.app).websocket_connect("/ws/terminal/cluster") as ws:
        ws.send_json({"type": "init", "node": "compute-1;false", "session": "sf-example"})
        assert ws.receive_json()["type"] == "error"
    ssh.create_interactive_session.assert_not_called()
