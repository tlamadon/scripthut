"""Tests for the local-daemon lifecycle mechanics (scripthut.daemon)."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from scripthut import daemon
from scripthut.config_schema import GlobalSettings, ScriptHutConfig

# -- helpers -----------------------------------------------------------------


def _cfg(tmp_path: Path, **settings) -> ScriptHutConfig:
    defaults = dict(data_dir=tmp_path, server_host="127.0.0.1", server_port=8123)
    defaults.update(settings)
    return ScriptHutConfig(settings=GlobalSettings(**defaults))


def _pong(pid: int = 4242) -> dict:
    return {"status": "pong", "pid": pid, "version": "0.0.0-test"}


def _response(status_code: int, json_body=None, text: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    if json_body is None:
        resp.json.side_effect = ValueError("not json")
    else:
        resp.json.return_value = json_body
    resp.text = text
    return resp


# -- pidfile -----------------------------------------------------------------


def test_pidfile_round_trip(tmp_path):
    path = tmp_path / "daemon.json"
    info = daemon.DaemonInfo(pid=42, host="127.0.0.1", port=8123, started_at="2026-07-15T00:00:00+00:00")
    daemon.write_pidfile(path, info)
    assert daemon.read_pidfile(path) == info
    assert info.url == "http://127.0.0.1:8123"


def test_read_pidfile_missing_returns_none(tmp_path):
    assert daemon.read_pidfile(tmp_path / "nope.json") is None


def test_read_pidfile_corrupt_returns_none(tmp_path):
    path = tmp_path / "daemon.json"
    path.write_text("{not json")
    assert daemon.read_pidfile(path) is None
    path.write_text('{"pid": "abc", "host": 1}')  # wrong types / missing keys
    assert daemon.read_pidfile(path) is None


def test_paths_derive_from_data_dir(tmp_path):
    cfg = _cfg(tmp_path)
    assert daemon.pidfile_path(cfg) == tmp_path / "daemon" / "daemon.json"
    assert daemon.logfile_path(cfg) == tmp_path / "daemon" / "daemon.log"


# -- pid_alive ---------------------------------------------------------------


def test_pid_alive_own_process():
    assert daemon.pid_alive(os.getpid()) is True


def test_pid_alive_dead_process():
    # Spawn a child, let it exit, and reap it — its pid is then gone.
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    assert daemon.pid_alive(proc.pid) is False


# -- ping --------------------------------------------------------------------


def test_ping_pong_returns_payload():
    with patch.object(daemon.httpx, "get", return_value=_response(200, _pong())):
        payload = daemon.ping("127.0.0.1", 8123)
    assert payload is not None and payload["pid"] == 4242


def test_ping_nothing_listening_returns_none():
    with patch.object(daemon.httpx, "get", side_effect=httpx.ConnectError("refused")):
        assert daemon.ping("127.0.0.1", 8123) is None


def test_ping_foreign_process_raises():
    # e.g. `python -m http.server` answers /ping with a 404 HTML page
    with patch.object(daemon.httpx, "get", return_value=_response(404, None, "<html>")):
        with pytest.raises(daemon.ForeignServerError, match="isn't scripthut"):
            daemon.ping("127.0.0.1", 8123)


def test_ping_wrong_json_shape_raises():
    with patch.object(daemon.httpx, "get", return_value=_response(200, {"hello": "world"})):
        with pytest.raises(daemon.ForeignServerError):
            daemon.ping("127.0.0.1", 8123)


# -- spawn_daemon ------------------------------------------------------------


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="spawn_daemon is unsupported on Windows (raises DaemonError)",
)
def test_spawn_daemon_argv_and_detachment(tmp_path):
    logfile = tmp_path / "d" / "daemon.log"
    with patch.object(daemon.subprocess, "Popen") as popen:
        daemon.spawn_daemon("127.0.0.1", 8123, config_path=None, logfile=logfile)
    cmd = popen.call_args.args[0]
    kwargs = popen.call_args.kwargs
    assert cmd == [sys.executable, "-m", "scripthut", "--host", "127.0.0.1", "--port", "8123"]
    assert kwargs["stdin"] == subprocess.DEVNULL
    assert kwargs["stderr"] == subprocess.STDOUT
    assert kwargs["start_new_session"] is True
    assert kwargs["cwd"] == Path.home()
    # A start banner is written to the (created) logfile.
    assert "--- daemon started" in logfile.read_text()


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="spawn_daemon is unsupported on Windows (raises DaemonError)",
)
def test_spawn_daemon_passes_config_path(tmp_path):
    cfg_file = tmp_path / "scripthut.yaml"
    cfg_file.write_text("settings: {}\n")
    with patch.object(daemon.subprocess, "Popen") as popen:
        daemon.spawn_daemon(
            "127.0.0.1", 8123, config_path=cfg_file, logfile=tmp_path / "daemon.log",
        )
    cmd = popen.call_args.args[0]
    assert cmd[-2:] == ["--config", str(cfg_file.resolve())]


@pytest.mark.skipif(sys.platform != "win32", reason="Windows-only guard")
def test_spawn_daemon_refuses_on_windows(tmp_path):
    with pytest.raises(daemon.DaemonError, match="not supported on Windows"):
        daemon.spawn_daemon(
            "127.0.0.1", 8123, config_path=None, logfile=tmp_path / "daemon.log",
        )


# -- wait_ready --------------------------------------------------------------


def test_wait_ready_returns_once_healthy(monkeypatch):
    monkeypatch.setattr(daemon.time, "sleep", lambda s: None)
    monkeypatch.setattr(daemon, "ping", MagicMock(side_effect=[None, None, _pong()]))
    monkeypatch.setattr(daemon, "_api_ready", lambda *a, **k: True)
    payload = daemon.wait_ready("127.0.0.1", 8123, timeout=5.0)
    assert payload["pid"] == 4242


def test_wait_ready_child_death_reports_log_tail(tmp_path, monkeypatch):
    monkeypatch.setattr(daemon, "ping", lambda *a, **k: None)
    logfile = tmp_path / "daemon.log"
    logfile.write_text("boom line 1\nboom line 2\n")
    proc = MagicMock()
    proc.poll.return_value = 1
    proc.returncode = 1
    with pytest.raises(daemon.DaemonError, match="exited with code 1") as exc:
        daemon.wait_ready("127.0.0.1", 8123, proc=proc, logfile=logfile, timeout=5.0)
    assert "boom line 2" in str(exc.value)
    assert str(logfile) in str(exc.value)


def test_wait_ready_timeout_names_logfile(tmp_path, monkeypatch):
    monkeypatch.setattr(daemon, "ping", lambda *a, **k: None)
    logfile = tmp_path / "daemon.log"
    with pytest.raises(daemon.DaemonError, match="did not become ready"):
        daemon.wait_ready("127.0.0.1", 8123, logfile=logfile, timeout=0.0)


def test_wait_ready_survives_lost_spawn_race(monkeypatch):
    """Child exited (lost the port-bind race) but /ping answers from the
    winner — that's success, not an error."""
    monkeypatch.setattr(daemon, "ping", lambda *a, **k: _pong(pid=777))
    monkeypatch.setattr(daemon, "_api_ready", lambda *a, **k: True)
    proc = MagicMock()
    proc.poll.return_value = 1
    payload = daemon.wait_ready("127.0.0.1", 8123, proc=proc, timeout=5.0)
    assert payload["pid"] == 777


# -- start_daemon ------------------------------------------------------------


def test_start_daemon_idempotent_when_already_running(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(daemon, "ping", lambda *a, **k: _pong(pid=4242))
    monkeypatch.setattr(
        daemon, "spawn_daemon",
        MagicMock(side_effect=AssertionError("must not spawn")),
    )
    info = daemon.start_daemon(cfg)
    assert info.pid == 4242
    assert info.url == "http://127.0.0.1:8123"
    # Pidfile reconciled from the server's self-reported pid.
    assert daemon.read_pidfile(daemon.pidfile_path(cfg)).pid == 4242


def test_start_daemon_records_race_winner_pid(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(daemon, "ping", lambda *a, **k: None)
    child = MagicMock()
    child.pid = 555
    monkeypatch.setattr(daemon, "spawn_daemon", MagicMock(return_value=child))
    monkeypatch.setattr(daemon, "wait_ready", MagicMock(return_value=_pong(pid=777)))
    info = daemon.start_daemon(cfg)
    assert info.pid == 777  # the winner's pid from /ping, not our child's
    assert daemon.read_pidfile(daemon.pidfile_path(cfg)).pid == 777


# -- stop_daemon -------------------------------------------------------------


def _write_pidfile(cfg, pid=123):
    daemon.write_pidfile(
        daemon.pidfile_path(cfg),
        daemon.DaemonInfo(pid=pid, host="127.0.0.1", port=8123, started_at=""),
    )


def test_stop_daemon_no_pidfile(tmp_path):
    assert "no pidfile" in daemon.stop_daemon(_cfg(tmp_path))


def test_stop_daemon_removes_stale_pidfile(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _write_pidfile(cfg, pid=123)
    monkeypatch.setattr(daemon, "pid_alive", lambda pid: False)
    result = daemon.stop_daemon(cfg)
    assert "stale" in result
    assert not daemon.pidfile_path(cfg).exists()


def test_stop_daemon_sigterm_then_exit(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _write_pidfile(cfg, pid=123)
    alive = iter([True, False])  # alive at identity check, dead after TERM
    monkeypatch.setattr(daemon, "pid_alive", lambda pid: next(alive))
    monkeypatch.setattr(daemon, "ping", lambda *a, **k: _pong(pid=123))
    kills: list = []
    monkeypatch.setattr(daemon.os, "kill", lambda pid, sig: kills.append((pid, sig)))
    result = daemon.stop_daemon(cfg)
    assert kills == [(123, signal.SIGTERM)]
    assert "stopped pid 123" in result
    assert not daemon.pidfile_path(cfg).exists()


def test_stop_daemon_sigkill_fallback(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _write_pidfile(cfg, pid=123)
    monkeypatch.setattr(daemon, "pid_alive", lambda pid: True)
    monkeypatch.setattr(daemon, "ping", lambda *a, **k: _pong(pid=123))
    monkeypatch.setattr(daemon, "STOP_TERM_TIMEOUT", 0.0)
    kills: list = []
    monkeypatch.setattr(daemon.os, "kill", lambda pid, sig: kills.append((pid, sig)))
    result = daemon.stop_daemon(cfg)
    # No SIGKILL on Windows — stop_daemon hard-kills with SIGTERM there.
    hard_kill = getattr(signal, "SIGKILL", signal.SIGTERM)
    assert kills == [(123, signal.SIGTERM), (123, hard_kill)]
    assert "SIGKILL" in result


def test_stop_daemon_refuses_unconfirmed_pid(tmp_path, monkeypatch):
    """Pid reuse protection: alive pid that neither /ping nor ps confirms
    as scripthut must not be killed."""
    cfg = _cfg(tmp_path)
    _write_pidfile(cfg, pid=123)
    monkeypatch.setattr(daemon, "pid_alive", lambda pid: True)
    monkeypatch.setattr(daemon, "ping", lambda *a, **k: _pong(pid=999))  # someone else
    monkeypatch.setattr(daemon, "_looks_like_scripthut", lambda pid: False)
    monkeypatch.setattr(
        daemon.os, "kill",
        lambda pid, sig: (_ for _ in ()).throw(AssertionError("killed!")),
    )
    with pytest.raises(daemon.DaemonError, match="refusing"):
        daemon.stop_daemon(cfg)
    assert daemon.pidfile_path(cfg).exists()


# -- daemon_status -----------------------------------------------------------


def test_daemon_status_running(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _write_pidfile(cfg, pid=4242)
    monkeypatch.setattr(daemon, "ping", lambda *a, **k: _pong(pid=4242))
    status = daemon.daemon_status(cfg)
    assert status["running"] is True
    assert status["pid"] == 4242
    assert status["url"] == "http://127.0.0.1:8123"
    assert status["foreign"] is False
    assert status["stale_pidfile"] is False


def test_daemon_status_not_running_flags_stale_pidfile(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    _write_pidfile(cfg, pid=4242)
    monkeypatch.setattr(daemon, "ping", lambda *a, **k: None)
    status = daemon.daemon_status(cfg)
    assert status["running"] is False
    assert status["stale_pidfile"] is True


def test_daemon_status_foreign_port(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    monkeypatch.setattr(
        daemon, "ping",
        MagicMock(side_effect=daemon.ForeignServerError("nope")),
    )
    status = daemon.daemon_status(cfg)
    assert status["running"] is False
    assert status["foreign"] is True


# -- /ping endpoint ----------------------------------------------------------


def test_ping_endpoint_reports_pid_and_version():
    """The daemon probe relies on /ping carrying the server's identity."""
    from scripthut import __version__
    from scripthut.main import ping as ping_endpoint

    payload = asyncio.run(ping_endpoint())
    assert payload["status"] == "pong"
    assert payload["pid"] == os.getpid()
    assert payload["version"] == __version__
