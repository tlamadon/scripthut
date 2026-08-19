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
# latency-bound on a cluster link; larger blocks with deep pipelining keep
# the wire saturated on a long-haul upload.
#
# OpenSSH sftp-server (Mercury included) hard-caps one SFTP message at
# SFTP_MAX_MSG_LENGTH = 256 KiB and calls cleanup_exit(11) if a WRITE is
# larger. That limit is the *whole packet* (headers + payload), so a 256 KiB
# payload overshoots and the channel drops as "Connection closed" on the
# first block of any file bigger than ~256 KiB. Tiny trees still succeed.
# SCP is a different protocol and is unaffected. 32 KiB stays safely under
# the cap; max_requests keeps the campus link busy.
_SFTP_BLOCK_SIZE = 32 * 1024
_SFTP_MAX_REQUESTS = 128
# OpenSSH SFTP_MAX_MSG_LENGTH. Payload must stay strictly below this.
_OPENSSH_SFTP_MAX_MSG = 256 * 1024


def _safe_relpath(rel: str) -> str:
    """Reject empty, absolute, or ``..`` paths. Return posix-style ``rel``."""
    cleaned = rel.strip().replace("\\", "/")
    if not cleaned or cleaned.startswith("/"):
        raise ValueError(f"Unsafe relative path: {rel!r}")
    parts = Path(cleaned).parts
    if ".." in parts or parts[:1] == ("/",):
        raise ValueError(f"Unsafe relative path: {rel!r}")
    return cleaned


def _join_remote(root: str, rel: str) -> str:
    return f"{root.rstrip('/')}/{rel}"


def _tree_nbytes(path: Path) -> int:
    """Regular-file byte count of ``path``. Symlinks are skipped."""
    total = 0
    for p in path.rglob("*"):
        if p.is_symlink() or not p.is_file():
            continue
        total += p.stat().st_size
    return total


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
            Total bytes in the local tree. asyncssh's progress callback is
            per-file, so the last ``done`` value is not a tree total.

        Raises:
            RuntimeError: on timeout or any SFTP/connection failure. The
                partial tree is left in place for the caller to clean up; it
                is never moved onto a final path.
        """
        start = time.perf_counter()
        label = f"sftp put_tree {local_path} -> {remote_path}"
        tree_total = _tree_nbytes(local_path)
        copied = 0
        current_src: bytes | str | None = None
        file_done = 0

        def _on_progress(
            src: bytes, _dst: bytes, done: int, file_total: int
        ) -> None:
            nonlocal copied, current_src, file_done
            if current_src is not None and src != current_src:
                copied += file_done
            current_src = src
            file_done = done
            if progress is not None:
                progress(
                    copied + done,
                    tree_total if tree_total > 0 else file_total,
                )

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

        copied += file_done
        if tree_total > 0:
            copied = tree_total
        self._log_command(label, start, stdout=f"{copied} bytes", exit_code=0)
        logger.info(f"{label}: {copied} bytes")
        return copied

    async def put_files(
        self,
        local_root: Path,
        remote_root: str,
        relpaths: list[str],
        *,
        timeout: int = 86400,
        progress: Callable[[int, int], None] | None = None,
    ) -> int:
        """Upload listed files; ``remote_root`` may already exist.

        Dedicated connection, same reason as :meth:`put_tree`. Parents of
        each file are created. Symlinks are not followed. Empty ``relpaths``
        is a no-op (no connection). A raised error means the dest may be
        partial — the caller must not treat it as complete.
        """
        paths = [_safe_relpath(p) for p in relpaths]
        if not paths:
            return 0

        start = time.perf_counter()
        label = f"sftp put_files {local_root} -> {remote_root}"
        copied = 0
        total = 0
        jobs: list[tuple[Path, str, int]] = []
        for rel in paths:
            src = local_root / rel
            if src.is_symlink() or not src.is_file():
                raise RuntimeError(
                    f"Transfer to {remote_root} failed: {rel} is not a regular file"
                )
            size = src.stat().st_size
            total += size
            jobs.append((src, _join_remote(remote_root, rel), size))

        def _on_progress(_src: bytes, _dst: bytes, done: int, _file_total: int) -> None:
            if progress is not None:
                progress(copied + done, total)

        try:
            async with asyncssh.connect(**self._connect_kwargs()) as conn:
                async with conn.start_sftp_client() as sftp:

                    async def _upload() -> int:
                        nonlocal copied
                        done = 0
                        for src, dest, size in jobs:
                            parent = dest.rsplit("/", 1)[0]
                            await sftp.makedirs(parent, exist_ok=True)
                            await sftp.put(
                                str(src),
                                dest,
                                follow_symlinks=False,
                                block_size=_SFTP_BLOCK_SIZE,
                                max_requests=_SFTP_MAX_REQUESTS,
                                progress_handler=_on_progress,
                            )
                            done += size
                            copied = done
                            if progress is not None:
                                progress(copied, total)
                        return done

                    result = await asyncio.wait_for(_upload(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.error(f"{label}: timed out after {timeout}s")
            self._log_command(label, start, error=f"Timeout after {timeout}s")
            raise RuntimeError(
                f"Transfer to {remote_root} timed out after {timeout}s"
            )
        except (OSError, asyncssh.Error) as e:
            logger.error(f"{label}: {e}")
            self._log_command(label, start, error=str(e))
            raise RuntimeError(f"Transfer to {remote_root} failed: {e}")

        self._log_command(label, start, stdout=f"{result} bytes", exit_code=0)
        logger.info(f"{label}: {result} bytes")
        return result

    async def get_files(
        self,
        remote_root: str,
        local_root: Path,
        relpaths: list[str],
        *,
        timeout: int = 86400,
        progress: Callable[[int, int], None] | None = None,
    ) -> int:
        """Download listed files into ``local_root``. Overwrite; do not delete.

        Dedicated connection. A missing remote file fails the call. Empty
        ``relpaths`` is a no-op.
        """
        paths = [_safe_relpath(p) for p in relpaths]
        if not paths:
            return 0

        start = time.perf_counter()
        label = f"sftp get_files {remote_root} -> {local_root}"

        try:
            async with asyncssh.connect(**self._connect_kwargs()) as conn:
                async with conn.start_sftp_client() as sftp:

                    async def _download() -> int:
                        done = 0
                        n = len(paths)
                        for i, rel in enumerate(paths, start=1):
                            dest = local_root / rel
                            dest.parent.mkdir(parents=True, exist_ok=True)
                            await sftp.get(
                                _join_remote(remote_root, rel),
                                str(dest),
                                follow_symlinks=False,
                                block_size=_SFTP_BLOCK_SIZE,
                                max_requests=_SFTP_MAX_REQUESTS,
                            )
                            if dest.is_file() and not dest.is_symlink():
                                done += dest.stat().st_size
                            if progress is not None:
                                progress(i, n)
                        return done

                    result = await asyncio.wait_for(_download(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.error(f"{label}: timed out after {timeout}s")
            self._log_command(label, start, error=f"Timeout after {timeout}s")
            raise RuntimeError(
                f"Transfer from {remote_root} timed out after {timeout}s"
            )
        except (OSError, asyncssh.Error) as e:
            logger.error(f"{label}: {e}")
            self._log_command(label, start, error=str(e))
            raise RuntimeError(f"Transfer from {remote_root} failed: {e}")

        self._log_command(label, start, stdout=f"{result} bytes", exit_code=0)
        logger.info(f"{label}: {result} bytes")
        return result

    async def list_files(
        self,
        remote_root: str,
        *,
        timeout: int = 60,
    ) -> list[str]:
        """Relative paths of regular files under ``remote_root``.

        Dedicated connection. A missing directory is an empty list, not an
        error — the sync return treats a missing ``output/`` as a no-op.
        Symlinks are skipped.
        """
        import stat as statmod

        start = time.perf_counter()
        label = f"sftp list_files {remote_root}"
        root = remote_root.rstrip("/") or remote_root

        async def _walk(sftp: object, abs_dir: str, prefix: str, out: list[str]) -> None:
            names = await sftp.listdir(abs_dir)  # type: ignore[attr-defined]
            for name in names:
                if name in (".", ".."):
                    continue
                abs_path = _join_remote(abs_dir, name)
                rel = f"{prefix}/{name}" if prefix else name
                attrs = await sftp.lstat(abs_path)  # type: ignore[attr-defined]
                mode = attrs.permissions or 0
                ftype = getattr(attrs, "type", None)
                is_link = ftype == 3 or statmod.S_ISLNK(mode)
                is_dir = ftype == 2 or statmod.S_ISDIR(mode)
                if is_link:
                    continue
                if is_dir:
                    await _walk(sftp, abs_path, rel, out)
                else:
                    out.append(rel.replace("\\", "/"))

        try:
            async with asyncssh.connect(**self._connect_kwargs()) as conn:
                async with conn.start_sftp_client() as sftp:
                    async def _list() -> list[str]:
                        try:
                            await sftp.stat(root)
                        except (asyncssh.SFTPNoSuchFile, asyncssh.SFTPNoSuchPath):
                            return []
                        found: list[str] = []
                        await _walk(sftp, root, "", found)
                        found.sort()
                        return found

                    result = await asyncio.wait_for(_list(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.error(f"{label}: timed out after {timeout}s")
            self._log_command(label, start, error=f"Timeout after {timeout}s")
            raise RuntimeError(
                f"Listing {remote_root} timed out after {timeout}s"
            )
        except (OSError, asyncssh.Error) as e:
            if isinstance(e, (asyncssh.SFTPNoSuchFile, asyncssh.SFTPNoSuchPath)):
                self._log_command(label, start, stdout="0 files", exit_code=0)
                return []
            logger.error(f"{label}: {e}")
            self._log_command(label, start, error=str(e))
            raise RuntimeError(f"Listing {remote_root} failed: {e}")

        self._log_command(
            label, start, stdout=f"{len(result)} files", exit_code=0,
        )
        return result

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
