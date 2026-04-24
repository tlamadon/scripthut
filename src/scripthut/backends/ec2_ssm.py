"""SSM-tunnelled SSH connections for the EC2 backend.

The EC2 backend does not require inbound port 22 on instances. Instead,
scripthut tunnels TCP/22 through an AWS SSM Session Manager port-forwarding
session, then authenticates with a one-shot SSH key pushed via EC2 Instance
Connect for each connection — no persistent key material, no EC2 KeyPair.

This module provides two pieces:

- :func:`find_free_port` / :class:`SSMTunnel` — spawn the AWS CLI
  ``ssm start-session`` subprocess and expose it as an async context
  manager that yields a local TCP port forwarded to the instance's port 22.
- :func:`push_ephemeral_key` — generate an RSA key pair locally and push
  the public half via ``ec2-instance-connect:SendSSHPublicKey`` (valid 60 s).
- :func:`open_ssh_via_ssm` — full end-to-end: tunnel + key push + asyncssh
  connection, returned as a :class:`scripthut.ssh.client.SSHClient` that
  the rest of the codebase already knows how to drive.

External requirements on the scripthut host:

- AWS CLI (``aws``)
- ``session-manager-plugin`` on PATH (see AWS docs)
- ``boto3`` (already a scripthut[batch] dependency; used here for EIC)
- Instance AMI must have SSM Agent and sshd installed, instance profile
  must include ``AmazonSSMManagedInstanceCore``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripthut.ssh.client import SSHClient

logger = logging.getLogger(__name__)


def find_free_port() -> int:
    """Return an unused TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class SSMTunnelError(RuntimeError):
    """Raised when an SSM port-forwarding session fails to come up."""


@dataclass
class SSMTunnel:
    """Manage an ``aws ssm start-session`` port-forwarding subprocess.

    Use as an async context manager. On ``__aenter__`` the subprocess is
    spawned and we wait for ``127.0.0.1:<local_port>`` to accept TCP
    connections. On ``__aexit__`` the subprocess is terminated (then killed
    after a short grace period if it doesn't exit).
    """

    instance_id: str
    region: str
    profile: str | None = None
    remote_port: int = 22
    local_port: int = 0  # 0 = auto-allocate in __aenter__
    open_timeout: float = 45.0
    _proc: asyncio.subprocess.Process | None = None

    async def __aenter__(self) -> "SSMTunnel":
        if self.local_port == 0:
            self.local_port = find_free_port()

        cmd = [
            "aws", "ssm", "start-session",
            "--target", self.instance_id,
            "--document-name", "AWS-StartPortForwardingSession",
            "--parameters",
            f"portNumber={self.remote_port},localPortNumber={self.local_port}",
            "--region", self.region,
        ]
        if self.profile:
            cmd.extend(["--profile", self.profile])

        logger.debug(
            f"Starting SSM tunnel to {self.instance_id} "
            f"(127.0.0.1:{self.local_port} -> :{self.remote_port})"
        )
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as e:
            raise SSMTunnelError(
                "`aws` CLI not found on PATH; the EC2 backend requires the "
                "AWS CLI + session-manager-plugin installed on the scripthut host."
            ) from e

        try:
            await self._wait_until_open()
        except Exception:
            await self.__aexit__(None, None, None)
            raise
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        proc = self._proc
        self._proc = None
        if proc is None:
            return
        if proc.returncode is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()

    async def _wait_until_open(self) -> None:
        """Poll localhost:local_port until it accepts a TCP connection.

        Also watches the subprocess — if it exits early (e.g. missing
        session-manager-plugin, IAM denial) we surface stderr in the error.
        """
        assert self._proc is not None
        deadline = asyncio.get_event_loop().time() + self.open_timeout
        while True:
            if self._proc.returncode is not None:
                stderr_bytes = b""
                if self._proc.stderr is not None:
                    try:
                        stderr_bytes = await self._proc.stderr.read()
                    except Exception:
                        pass
                stderr_text = stderr_bytes.decode("utf-8", errors="replace").strip()
                raise SSMTunnelError(
                    f"aws ssm start-session exited with code "
                    f"{self._proc.returncode}"
                    + (f": {stderr_text}" if stderr_text else "")
                )
            try:
                reader, writer = await asyncio.open_connection(
                    "127.0.0.1", self.local_port
                )
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
                logger.debug(
                    f"SSM tunnel to {self.instance_id} open on "
                    f"127.0.0.1:{self.local_port}"
                )
                return
            except (ConnectionRefusedError, OSError):
                if asyncio.get_event_loop().time() >= deadline:
                    raise SSMTunnelError(
                        f"SSM tunnel to {self.instance_id} did not accept "
                        f"connections on 127.0.0.1:{self.local_port} within "
                        f"{self.open_timeout:.0f}s"
                    )
                await asyncio.sleep(0.5)


async def push_ephemeral_public_key(
    ec2ic_client: Any,
    *,
    instance_id: str,
    public_key_openssh: str,
    os_user: str,
    availability_zone: str,
) -> None:
    """Push a one-shot SSH public key via EC2 Instance Connect (valid 60 s)."""
    await asyncio.to_thread(
        lambda: ec2ic_client.send_ssh_public_key(
            InstanceId=instance_id,
            InstanceOSUser=os_user,
            SSHPublicKey=public_key_openssh,
            AvailabilityZone=availability_zone,
        )
    )


def _generate_ssh_keypair() -> tuple[bytes, str]:
    """Generate an ephemeral RSA keypair for one-shot EC2 Instance Connect auth.

    Returns ``(private_key_openssh_bytes, public_key_openssh_str)``.
    """
    try:
        import asyncssh
    except ImportError as e:
        raise RuntimeError(
            "asyncssh is required for the EC2 backend (install scripthut)"
        ) from e
    key = asyncssh.generate_private_key("ssh-rsa", key_size=2048)
    priv = key.export_private_key()  # OpenSSH PEM
    pub = key.export_public_key().decode("ascii").strip()
    return (priv, pub)


def _write_ephemeral_key(private_key_bytes: bytes) -> Path:
    """Write a private key to a mode-600 temp file; returns its path.

    The caller is responsible for deleting the file.
    """
    fd, path = tempfile.mkstemp(prefix="scripthut-ec2-", suffix=".pem")
    try:
        os.write(fd, private_key_bytes)
    finally:
        os.close(fd)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    return Path(path)


class SSMSSHSession:
    """One SSM-tunnelled SSH connection to an EC2 instance.

    Use as an async context manager; ``.client`` gives you the underlying
    :class:`SSHClient`. Cleaning up closes SSH, kills the tunnel
    subprocess, and removes the ephemeral key file.
    """

    def __init__(
        self,
        *,
        instance_id: str,
        availability_zone: str,
        ssh_user: str,
        aws_region: str,
        aws_profile: str | None,
        ec2ic_client: Any,
    ) -> None:
        self.instance_id = instance_id
        self.availability_zone = availability_zone
        self.ssh_user = ssh_user
        self.aws_region = aws_region
        self.aws_profile = aws_profile
        self.ec2ic_client = ec2ic_client
        self._tunnel: SSMTunnel | None = None
        self._key_path: Path | None = None
        self._client: SSHClient | None = None

    @property
    def client(self) -> SSHClient:
        if self._client is None:
            raise RuntimeError("SSMSSHSession is not entered")
        return self._client

    async def __aenter__(self) -> "SSMSSHSession":
        # 1) Spawn SSM port-forwarding subprocess, wait for local port.
        tunnel = SSMTunnel(
            instance_id=self.instance_id,
            region=self.aws_region,
            profile=self.aws_profile,
        )
        await tunnel.__aenter__()
        self._tunnel = tunnel

        try:
            # 2) Generate an ephemeral keypair, write the private key to
            # a mode-600 temp file, push the public key via EIC.
            priv_bytes, pub_str = _generate_ssh_keypair()
            self._key_path = _write_ephemeral_key(priv_bytes)
            await push_ephemeral_public_key(
                self.ec2ic_client,
                instance_id=self.instance_id,
                public_key_openssh=pub_str,
                os_user=self.ssh_user,
                availability_zone=self.availability_zone,
            )

            # 3) Connect asyncssh through the tunnel. EIC keys expire after
            # 60 s, so we authenticate promptly — once the SSH session is up,
            # the expiry doesn't matter (asyncssh reuses the channel).
            client = SSHClient(
                host="127.0.0.1",
                port=tunnel.local_port,
                user=self.ssh_user,
                key_path=self._key_path,
            )
            await client.connect(timeout=30)
            self._client = client
        except Exception:
            await self._cleanup()
            raise
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self._cleanup()

    async def _cleanup(self) -> None:
        client, self._client = self._client, None
        tunnel, self._tunnel = self._tunnel, None
        key_path, self._key_path = self._key_path, None

        if client is not None:
            with contextlib.suppress(Exception):
                await client.disconnect()
        if tunnel is not None:
            with contextlib.suppress(Exception):
                await tunnel.__aexit__(None, None, None)
        if key_path is not None:
            with contextlib.suppress(FileNotFoundError):
                key_path.unlink()
