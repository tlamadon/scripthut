"""Async SSH client with persistent connection management."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

import asyncssh

if TYPE_CHECKING:
    from collections.abc import Callable

    from scripthut.ssh.command_log import CommandLogEntry

logger = logging.getLogger(__name__)

# SFTP tuning for dataset transfers. asyncssh's 16 KiB default is
# latency-bound on a cluster link; 256 KiB blocks with deep pipelining keep
# the wire saturated on a long-haul upload.
_SFTP_BLOCK_SIZE = 256 * 1024
_SFTP_MAX_REQUESTS = 128


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

    @property
    def is_connected(self) -> bool:
        """Check if the connection is active."""
        return self._connection is not None and not self._connection.is_closed()

    def _connect_kwargs(self) -> dict:
        """Connection parameters shared by the pooled and one-off connections.

        One place so a dedicated transfer connection can never drift from the
        persistent one's auth or host-key policy.
        """
        # If a certificate is provided, pass as tuple (key, cert)
        if self.cert_path is not None:
            client_keys = [(str(self.key_path), str(self.cert_path))]
        else:
            client_keys = [str(self.key_path)]

        return dict(
            host=self.host,
            port=self.port,
            username=self.user,
            client_keys=client_keys,
            # None means don't validate (for development)
            known_hosts=self.known_hosts,
            keepalive_interval=30,
            keepalive_count_max=3,
            # Disable password/keyboard-interactive auth to prevent terminal prompts
            password=None,
            preferred_auth=["publickey"],
        )

    async def connect(self, timeout: int = 15) -> None:
        """Establish SSH connection.

        Args:
            timeout: Connection timeout in seconds (default 15).
        """
        async with self._lock:
            if self.is_connected:
                return

            logger.info(f"Connecting to {self.user}@{self.host}:{self.port}")

            try:
                self._connection = await asyncio.wait_for(
                    asyncssh.connect(**self._connect_kwargs()),
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

    async def put_tree(
        self,
        local_path: Path,
        remote_path: str,
        *,
        timeout: int = 86400,
        progress: Callable[[int, int], None] | None = None,
    ) -> int:
        """Upload a directory tree over a **dedicated** SFTP connection.

        Deliberately not on ``self._connection``: that one is shared with the
        status poller, which runs every ``poll_interval`` seconds. Saturating
        it for the hours a multi-gigabyte dataset takes would stall job-status
        updates and make the UI look wedged. This opens its own connection,
        transfers, and closes it.

        Preconditions, both the caller's job (``mkdir -p`` and ``mv`` stay on
        ``run_command``): ``remote_path`` must **not** exist, and its parent
        must. asyncssh then creates ``remote_path`` as a copy of
        ``local_path`` rather than nesting inside an existing directory.

        Symlinks are not followed. Datasets reject them upfront
        (``runs.datasets.build_manifest``), so anything encountered here would
        make the transferred tree disagree with the manifest that named it.

        Args:
            local_path: Directory on this host to upload.
            remote_path: Absolute destination path that must not yet exist.
            timeout: Wall-clock limit for the whole transfer.
            progress: Optional ``(bytes_copied, bytes_total)`` callback.

        Returns:
            Total bytes copied, as reported by the SFTP layer.

        Raises:
            RuntimeError: on timeout or any SFTP/connection failure. The
                partial tree is left in place for the caller to clean up; it
                is never moved onto a final path.
        """
        start = time.perf_counter()
        label = f"sftp put_tree {local_path} -> {remote_path}"
        copied = 0

        def _on_progress(
            _src: bytes, _dst: bytes, done: int, total: int
        ) -> None:
            nonlocal copied
            copied = done
            if progress is not None:
                progress(done, total)

        try:
            async with asyncssh.connect(**self._connect_kwargs()) as conn:
                async with conn.start_sftp_client() as sftp:
                    await asyncio.wait_for(
                        sftp.put(
                            str(local_path),
                            remote_path,
                            recurse=True,
                            follow_symlinks=False,
                            # asyncssh defaults to 16 KiB blocks, which is
                            # latency-bound on a cluster link; larger blocks
                            # with deep pipelining keep the wire busy.
                            block_size=_SFTP_BLOCK_SIZE,
                            max_requests=_SFTP_MAX_REQUESTS,
                            progress_handler=_on_progress,
                        ),
                        timeout=timeout,
                    )
        except asyncio.TimeoutError:
            logger.error(f"{label}: timed out after {timeout}s")
            self._log_command(label, start, error=f"Timeout after {timeout}s")
            raise RuntimeError(
                f"Transfer to {remote_path} timed out after {timeout}s"
            )
        except (OSError, asyncssh.Error) as e:
            logger.error(f"{label}: {e}")
            self._log_command(label, start, error=str(e))
            raise RuntimeError(f"Transfer to {remote_path} failed: {e}")

        self._log_command(label, start, stdout=f"{copied} bytes", exit_code=0)
        logger.info(f"{label}: {copied} bytes")
        return copied

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
