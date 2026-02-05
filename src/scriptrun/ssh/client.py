"""Async SSH client with persistent connection management."""

import asyncio
import logging
from pathlib import Path

import asyncssh

logger = logging.getLogger(__name__)


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

    @property
    def is_connected(self) -> bool:
        """Check if the connection is active."""
        return self._connection is not None and not self._connection.is_closed()

    async def connect(self) -> None:
        """Establish SSH connection."""
        async with self._lock:
            if self.is_connected:
                return

            logger.info(f"Connecting to {self.user}@{self.host}:{self.port}")

            # Configure known_hosts handling
            known_hosts_arg: str | Path | None
            if self.known_hosts is not None:
                known_hosts_arg = self.known_hosts
            else:
                # None means don't validate (for development)
                known_hosts_arg = None

            try:
                # Build client_keys argument
                # If certificate is provided, pass as tuple (key, cert)
                if self.cert_path is not None:
                    client_keys = [(str(self.key_path), str(self.cert_path))]
                else:
                    client_keys = [str(self.key_path)]

                self._connection = await asyncssh.connect(
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
                )
                logger.info(f"Connected to {self.host}")
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

    async def run_command(self, command: str) -> tuple[str, str, int]:
        """
        Run a command on the remote host.

        Returns:
            Tuple of (stdout, stderr, exit_code)
        """
        if not self.is_connected:
            await self.connect()

        if self._connection is None:
            raise RuntimeError("Failed to establish SSH connection")

        try:
            result = await self._connection.run(command, check=False)
            return (
                result.stdout or "",
                result.stderr or "",
                result.exit_status or 0,
            )
        except asyncssh.Error as e:
            logger.error(f"Command execution failed: {e}")
            # Try to reconnect on next attempt
            self._connection = None
            raise

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
