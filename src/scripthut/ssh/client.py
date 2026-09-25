"""Async SSH client with persistent connection management."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

import asyncssh

from scripthut.ssh.git_ssh import parse_openssh_version

if TYPE_CHECKING:
    from collections.abc import Callable

    from scripthut.ssh.command_log import CommandLogEntry

logger = logging.getLogger(__name__)

_UNPROBED = object()


class SSHClient:
    """Manages a persistent SSH connection with auto-reconnect."""

    def __init__(
        self,
        host: str,
        user: str,
        key_path: Path,
        port: int = 22,
        cert_path: Path | None = None,
        known_hosts: Path | None = None,
    ) -> None:
        self.host = host
        self.user = user
        self.key_path = key_path
        self.port = port
        self.cert_path = cert_path
        self.known_hosts = known_hosts
        self._connection: asyncssh.SSHClientConnection | None = None
        self._lock = asyncio.Lock()
        self.on_command: Callable[[CommandLogEntry], None] | None = None
        # Probed lazily by openssh_version(); _UNPROBED distinguishes "not
        # asked yet" from "asked, and the banner was unreadable".
        self._openssh_version: tuple[int, int] | None | object = _UNPROBED
        # Warn once per client, not on every reconnect.
        self._warned_unverified = False

    @property
    def is_connected(self) -> bool:
        """Check if the connection is active."""
        return self._connection is not None and not self._connection.is_closed()

    async def connect(self, timeout: int = 15) -> None:
        """Establish SSH connection.

        Args:
            timeout: Connection timeout in seconds (default 15).
        """
        async with self._lock:
            if self.is_connected:
                return

            logger.info(f"Connecting to {self.user}@{self.host}:{self.port}")

            # asyncssh wants the known_hosts path as a str. Handed a Path it
            # raises `'PosixPath' object is not subscriptable` out of
            # match_known_hosts, so turning verification on made a backend
            # fail to connect at all — which left disabling it as the only
            # way to get a working config.
            known_hosts_arg: str | None
            if self.known_hosts is not None:
                known_hosts_arg = str(self.known_hosts)
            else:
                # None disables host key checking entirely.
                known_hosts_arg = None
                if not self._warned_unverified:
                    logger.warning(
                        f"Host key verification is disabled for {self.host}; "
                        "set this backend's ssh.known_hosts to enable it"
                    )
                    self._warned_unverified = True

            try:
                # Build client_keys argument
                # If certificate is provided, pass as tuple (key, cert)
                if self.cert_path is not None:
                    client_keys = [(str(self.key_path), str(self.cert_path))]
                else:
                    client_keys = [str(self.key_path)]

                self._connection = await asyncio.wait_for(
                    asyncssh.connect(
                        host=self.host,
                        port=self.port,
                        username=self.user,
                        client_keys=client_keys,
                        known_hosts=known_hosts_arg,
                        keepalive_interval=30,
                        keepalive_count_max=3,
                        # Disable password/keyboard-interactive auth to prevent terminal prompts
                        password=None,
                        preferred_auth=["publickey"],
                    ),
                    timeout=timeout,
                )
                logger.info(f"Connected to {self.host}")
            except asyncio.TimeoutError:
                logger.error(f"SSH connection timed out after {timeout}s")
                raise RuntimeError(f"SSH connection timed out after {timeout}s")
            except asyncssh.Error as e:
                logger.error(f"SSH connection failed: {e}")
                raise

    async def disconnect(self) -> None:
        """Close the SSH connection."""
        async with self._lock:
            if self._connection is not None:
                self._connection.close()
                await self._connection.wait_closed()
                self._connection = None
                logger.info(f"Disconnected from {self.host}")

    async def create_interactive_session(
        self,
        command: str | None = None,
        term_type: str = "xterm-256color",
        term_size: tuple[int, int] = (80, 24),
    ) -> asyncssh.SSHClientProcess:
        """Create an interactive SSH process with a PTY.

        Args:
            command: Command to run (None for a login shell).
            term_type: Terminal type for the PTY.
            term_size: (cols, rows) terminal size.

        Returns:
            An SSHClientProcess with stdin/stdout streams.
        """
        if not self.is_connected:
            await self.connect()

        if self._connection is None:
            raise RuntimeError("Failed to establish SSH connection")

        process = await self._connection.create_process(
            command,
            term_type=term_type,
            term_size=term_size,
            encoding=None,
        )
        return process

    def _log_command(
        self, command: str, start: float,
        stdout: str = "", stderr: str = "", exit_code: int | None = None,
        error: str | None = None,
    ) -> None:
        """Record a command to the log callback if set."""
        if self.on_command is None:
            return
        from scripthut.ssh.command_log import CommandLogEntry
        from datetime import datetime, timezone

        self.on_command(CommandLogEntry(
            timestamp=datetime.now(timezone.utc),
            command=command,
            exit_code=exit_code,
            duration_ms=int((time.perf_counter() - start) * 1000),
            stdout=stdout,
            stderr=stderr,
            error=error,
        ))

    async def run_command(self, command: str, timeout: int = 30) -> tuple[str, str, int]:
        """
        Run a command on the remote host.

        Args:
            command: The command to run.
            timeout: Timeout in seconds (default 30).

        Returns:
            Tuple of (stdout, stderr, exit_code)
        """
        if not self.is_connected:
            await self.connect()

        if self._connection is None:
            raise RuntimeError("Failed to establish SSH connection")

        start = time.perf_counter()
        try:
            result = await asyncio.wait_for(
                self._connection.run(command, check=False),
                timeout=timeout,
            )
            stdout = result.stdout or ""
            stderr = result.stderr or ""
            exit_code = result.exit_status or 0
            self._log_command(command, start, stdout, stderr, exit_code)
            return (stdout, stderr, exit_code)
        except asyncio.TimeoutError:
            logger.error(f"Command timed out after {timeout}s: {command[:50]}...")
            self._log_command(command, start, error=f"Timeout after {timeout}s")
            raise RuntimeError(f"Command timed out after {timeout}s")
        except asyncssh.Error as e:
            logger.error(f"Command execution failed: {e}")
            self._log_command(command, start, error=str(e))
            # Try to reconnect on next attempt
            self._connection = None
            raise

    async def openssh_version(self) -> tuple[int, int] | None:
        """The remote ``ssh`` client's ``(major, minor)`` version, cached.

        Used to pick a ``StrictHostKeyChecking`` value the remote ssh
        actually understands — see :mod:`scripthut.ssh.git_ssh`. Cached
        for the lifetime of the client because it cannot change under
        us, and a clone should not pay for a round trip per call.

        Returns None if the banner is unreadable or the probe fails; the
        caller treats that as "assume old".
        """
        if self._openssh_version is not _UNPROBED:
            return self._openssh_version  # type: ignore[return-value]

        try:
            # `ssh -V` writes the banner to stderr, not stdout.
            stdout, stderr, _ = await self.run_command("ssh -V", timeout=15)
        except Exception as e:
            # A dropped connection says nothing about the remote ssh, so
            # don't cache it — answer "unknown" now and probe again later.
            logger.warning(f"Could not probe ssh version on {self.host}: {e}")
            return None

        version = parse_openssh_version(f"{stderr}\n{stdout}")
        if version is None:
            logger.warning(
                f"Unrecognised ssh version on {self.host}; "
                "assuming it predates StrictHostKeyChecking=accept-new"
            )
        self._openssh_version = version
        return version

    async def __aenter__(self) -> "SSHClient":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Async context manager exit."""
        await self.disconnect()
