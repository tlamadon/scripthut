"""Local daemon lifecycle: probe, spawn, readiness, pidfile, stop, status.

The "daemon" is a detached ``scripthut`` server (the same FastAPI app the
foreground ``scripthut`` command runs) that the CLI talks to over HTTP when
no remote server is configured. This module is purely mechanical — the
autostart *policy* (the ``cli_autostart`` setting, prompting, remembering
the choice) lives in ``scripthut.cli``.

Liveness is always determined by probing ``GET /ping`` on the configured
host/port; the pidfile is bookkeeping for ``daemon stop`` / ``daemon
status``, never the source of truth. A foreground server on the same port
is detected and used exactly like a daemon.

POSIX-only (``start_new_session`` + signals); Windows raises DaemonError.
"""

import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from scripthut.config_schema import ScriptHutConfig

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
PROBE_TIMEOUT = 2.0  # seconds, single /ping probe
READY_TIMEOUT = 20.0  # seconds, total readiness budget after spawn
READY_POLL_INTERVAL = 0.25
STOP_TERM_TIMEOUT = 10.0  # seconds to wait after SIGTERM before SIGKILL


class DaemonError(RuntimeError):
    """A daemon operation failed; the message is user-facing."""


class ForeignServerError(DaemonError):
    """Something answers on host:port but it isn't a scripthut server."""


@dataclass
class DaemonInfo:
    """What the pidfile records about a running daemon."""

    pid: int
    host: str
    port: int
    started_at: str  # isoformat UTC

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"


def resolve_host_port(config: "ScriptHutConfig | None") -> tuple[str, int]:
    """Host/port the daemon binds to — mirrors ``main.run()``'s fallback."""
    if config is not None:
        return config.settings.server_host, config.settings.server_port
    return DEFAULT_HOST, DEFAULT_PORT


def daemon_dir(config: "ScriptHutConfig | None") -> Path:
    if config is not None:
        base = config.settings.data_dir_resolved
    else:
        base = Path("~/.cache/scripthut").expanduser()
    return base / "daemon"


def pidfile_path(config: "ScriptHutConfig | None") -> Path:
    return daemon_dir(config) / "daemon.json"


def logfile_path(config: "ScriptHutConfig | None") -> Path:
    return daemon_dir(config) / "daemon.log"


def read_pidfile(path: Path) -> DaemonInfo | None:
    """Load the pidfile; missing or corrupt files are treated as absent."""
    try:
        data = json.loads(path.read_text())
        return DaemonInfo(
            pid=int(data["pid"]),
            host=str(data["host"]),
            port=int(data["port"]),
            started_at=str(data.get("started_at", "")),
        )
    except (OSError, ValueError, KeyError, TypeError):
        return None


def write_pidfile(path: Path, info: DaemonInfo) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(asdict(info)))
    tmp.rename(path)


def remove_pidfile(path: Path) -> None:
    path.unlink(missing_ok=True)


def pid_alive(pid: int) -> bool:
    """Whether a process with this pid exists (may belong to someone else)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def ping(host: str, port: int, *, timeout: float = PROBE_TIMEOUT) -> dict[str, Any] | None:
    """Probe ``GET /ping`` on host:port.

    Returns the pong payload (with ``pid``/``version`` on current servers),
    ``None`` when nothing is listening, and raises ForeignServerError when
    the port is serving something that isn't scripthut.
    """
    url = f"http://{host}:{port}/ping"
    try:
        resp = httpx.get(url, timeout=timeout)
    except httpx.RequestError:
        return None
    try:
        payload = resp.json()
    except ValueError:
        payload = None
    if resp.status_code == 200 and isinstance(payload, dict) and payload.get("status") == "pong":
        return payload
    raise ForeignServerError(
        f"port {port} on {host} is serving something that isn't scripthut; "
        f"change settings.server_port or stop that process"
    )


def _api_ready(host: str, port: int, *, timeout: float = PROBE_TIMEOUT) -> bool:
    """Whether the API layer is up with a loaded config (not just uvicorn)."""
    try:
        resp = httpx.get(f"http://{host}:{port}/api/v1/health", timeout=timeout)
        return resp.status_code == 200 and bool(resp.json().get("config_loaded"))
    except (httpx.RequestError, ValueError):
        return False


def _tail(path: Path | None, lines: int) -> str:
    if path is None:
        return ""
    try:
        content = path.read_text(errors="replace")
    except OSError:
        return ""
    return "\n".join(content.splitlines()[-lines:])


def spawn_daemon(
    host: str,
    port: int,
    *,
    config_path: Path | None,
    logfile: Path,
) -> subprocess.Popen:
    """Spawn a detached scripthut server; stdout/stderr append to logfile.

    The child runs from ``$HOME``: the daemon is a user-global service, so
    it must not pick up whichever project-local ``scripthut.yaml`` the
    spawning command's CWD happens to contain.
    """
    if sys.platform == "win32":
        raise DaemonError(
            "local daemon management is not supported on Windows; "
            "use --server local or run `scripthut` in another terminal"
        )
    cmd = [sys.executable, "-m", "scripthut", "--host", host, "--port", str(port)]
    if config_path is not None:
        cmd += ["--config", str(config_path.resolve())]
    logfile.parent.mkdir(parents=True, exist_ok=True)
    log_fd = open(logfile, "ab")
    try:
        stamp = datetime.now(UTC).isoformat(timespec="seconds")
        log_fd.write(f"--- daemon started {stamp}: {' '.join(cmd)} ---\n".encode())
        log_fd.flush()
        return subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=log_fd,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            cwd=Path.home(),
        )
    finally:
        log_fd.close()


def wait_ready(
    host: str,
    port: int,
    *,
    proc: subprocess.Popen | None = None,
    logfile: Path | None = None,
    timeout: float = READY_TIMEOUT,
) -> dict[str, Any]:
    """Poll until the server answers /ping and its API has a loaded config.

    Returns the pong payload. Raises DaemonError when the spawned child
    exits without ever answering, or when the deadline passes;
    ForeignServerError propagates immediately.
    """
    deadline = time.monotonic() + timeout
    while True:
        payload = ping(host, port)
        if payload is not None and _api_ready(host, port):
            return payload
        # A dead child with no pong means it crashed (bad config, port taken
        # by a process that appeared after our probe, ...). A dead child
        # *with* a pong is a lost spawn race — the winner answers, keep going.
        if proc is not None and proc.poll() is not None and payload is None:
            raise DaemonError(
                f"daemon exited with code {proc.returncode} before becoming "
                f"ready; last log lines:\n{_tail(logfile, 15)}\n"
                f"  full log: {logfile}"
            )
        if time.monotonic() >= deadline:
            raise DaemonError(
                f"daemon did not become ready within {timeout:.0f}s; see {logfile}"
            )
        time.sleep(READY_POLL_INTERVAL)


def start_daemon(
    config: "ScriptHutConfig | None",
    config_path: Path | None = None,
) -> DaemonInfo:
    """Ensure a daemon is running on the configured host/port and return it.

    Idempotent: when a server already answers, the pidfile is reconciled
    from its self-reported pid and no process is spawned.
    """
    host, port = resolve_host_port(config)
    logfile = logfile_path(config)
    now = datetime.now(UTC).isoformat(timespec="seconds")

    payload = ping(host, port)
    if payload is not None:
        info = DaemonInfo(pid=int(payload.get("pid", 0)), host=host, port=port, started_at=now)
        existing = read_pidfile(pidfile_path(config))
        if existing is not None and existing.pid == info.pid:
            return existing
        if info.pid:
            write_pidfile(pidfile_path(config), info)
        return info

    proc = spawn_daemon(host, port, config_path=config_path, logfile=logfile)
    payload = wait_ready(host, port, proc=proc, logfile=logfile)
    # Trust the server's self-reported pid over proc.pid: if our child lost
    # a port-bind race and exited, /ping answers from the winner.
    winner_pid = int(payload.get("pid", proc.pid))
    info = DaemonInfo(pid=winner_pid, host=host, port=port, started_at=now)
    write_pidfile(pidfile_path(config), info)
    return info


def _looks_like_scripthut(pid: int) -> bool:
    """Best-effort check that a pid's command line is a scripthut server."""
    try:
        out = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return False
    return "scripthut" in out


def stop_daemon(config: "ScriptHutConfig | None") -> str:
    """Stop the recorded daemon; returns a human-readable result."""
    pidfile = pidfile_path(config)
    info = read_pidfile(pidfile)
    if info is None:
        return "not running (no pidfile)"

    if not pid_alive(info.pid):
        remove_pidfile(pidfile)
        return f"not running (removed stale pidfile for pid {info.pid})"

    # Identity check before killing: never SIGTERM a recycled pid.
    confirmed = False
    try:
        payload = ping(info.host, info.port)
    except ForeignServerError:
        payload = None
    if payload is not None and payload.get("pid") == info.pid:
        confirmed = True
    elif _looks_like_scripthut(info.pid):
        confirmed = True
    if not confirmed:
        raise DaemonError(
            f"pid {info.pid} from {pidfile} is alive but does not look like "
            f"a scripthut server (pid may have been reused); refusing to "
            f"kill it. Verify and stop it manually, then delete the pidfile."
        )

    os.kill(info.pid, signal.SIGTERM)
    deadline = time.monotonic() + STOP_TERM_TIMEOUT
    while time.monotonic() < deadline:
        if not pid_alive(info.pid):
            remove_pidfile(pidfile)
            return f"stopped pid {info.pid}"
        time.sleep(0.2)
    os.kill(info.pid, signal.SIGKILL)
    remove_pidfile(pidfile)
    return f"stopped pid {info.pid} (SIGKILL after {STOP_TERM_TIMEOUT:.0f}s)"


def daemon_status(config: "ScriptHutConfig | None") -> dict[str, Any]:
    """Snapshot of the daemon state for ``scripthut daemon status``."""
    host, port = resolve_host_port(config)
    pidfile = pidfile_path(config)
    info = read_pidfile(pidfile)

    foreign = False
    try:
        payload = ping(host, port)
    except ForeignServerError:
        payload = None
        foreign = True

    running = payload is not None
    pid = payload.get("pid") if payload else None
    stale_pidfile = info is not None and (not running or (pid is not None and info.pid != pid))

    uptime_s: float | None = None
    started_at = info.started_at if (info and running and pid == info.pid) else None
    if started_at:
        try:
            uptime_s = (datetime.now(UTC) - datetime.fromisoformat(started_at)).total_seconds()
        except ValueError:
            uptime_s = None

    return {
        "running": running,
        "pid": pid,
        "url": f"http://{host}:{port}" if running else None,
        "version": payload.get("version") if payload else None,
        "started_at": started_at,
        "uptime_s": uptime_s,
        "foreign": foreign,
        "stale_pidfile": stale_pidfile,
        "pidfile": str(pidfile),
        "log_path": str(logfile_path(config)),
    }
