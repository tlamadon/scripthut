"""Shared backend connection lifecycle for the server and CLI."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import asyncssh

from scripthut.backends.base import JobBackend
from scripthut.backends.pbs import PBSBackend
from scripthut.backends.slurm import SlurmBackend
from scripthut.config_schema import (
    PBSBackendConfig,
    ScriptHutConfig,
    SlurmBackendConfig,
)
from scripthut.models import ConnectionStatus, HPCJob
from scripthut.ssh.client import SSHClient
from scripthut.ssh.command_log import CommandLog

logger = logging.getLogger(__name__)


@dataclass
class BackendState:
    """Connection, polling state and observed jobs for one backend."""

    name: str
    backend_type: str
    ssh_client: SSHClient | None = None
    backend: JobBackend | None = None
    jobs: list[HPCJob] = field(default_factory=list)
    status: ConnectionStatus = field(
        default_factory=lambda: ConnectionStatus(connected=False, host="")
    )
    enabled: bool = True
    command_log: CommandLog = field(default_factory=CommandLog)
    clone_dir: str = "~/scripthut-repos"
    _reconnect_after: float = 0.0
    _reconnect_delay: float = 0.0


@dataclass
class Runtime:
    """Shared backend connections."""

    config: ScriptHutConfig
    backends: dict[str, BackendState]








async def init_backend(backend_config: SlurmBackendConfig | PBSBackendConfig) -> BackendState:
    """Initialize an SSH-based backend connection (Slurm or PBS)."""
    ssh_client = SSHClient(
        host=backend_config.ssh.host,
        user=backend_config.ssh.user,
        key_path=backend_config.ssh.key_path_resolved,
        port=backend_config.ssh.port,
        cert_path=backend_config.ssh.cert_path_resolved,
        known_hosts=backend_config.ssh.known_hosts_resolved,
    )

    backend: JobBackend
    if isinstance(backend_config, PBSBackendConfig):
        backend = PBSBackend(ssh_client, default_queue=backend_config.queue)
        backend_type = "pbs"
    else:
        backend = SlurmBackend(
            ssh_client,
            account=backend_config.account,
            partition_map=backend_config.partition_map,
            default_partition=backend_config.default_partition,
        )
        backend_type = "slurm"

    backend_state = BackendState(
        name=backend_config.name,
        backend_type=backend_type,
        ssh_client=ssh_client,
        backend=backend,
        status=ConnectionStatus(connected=False, host=backend_config.ssh.host),
        clone_dir=backend_config.clone_dir,
    )
    ssh_client.on_command = backend_state.command_log.append

    try:
        await ssh_client.connect()
        backend_state.status = ConnectionStatus(
            connected=True,
            host=backend_config.ssh.host,
        )
        logger.info(
            f"Connected to {backend_type} backend '{backend_config.name}' "
            f"({backend_config.ssh.host})"
        )
    except asyncssh.PermissionDenied as e:
        # Auth failure — record long backoff so the polling task doesn't hammer the server
        backend_state._reconnect_delay = 60.0
        backend_state._reconnect_after = time.monotonic() + 60.0
        logger.error(f"Auth failed for backend '{backend_config.name}': {e}")
        backend_state.status = ConnectionStatus(
            connected=False,
            host=backend_config.ssh.host,
            error=str(e),
        )
    except Exception as e:
        logger.error(f"Failed to connect to backend '{backend_config.name}': {e}")
        backend_state.status = ConnectionStatus(
            connected=False,
            host=backend_config.ssh.host,
            error=str(e),
        )

    return backend_state





async def init_runtime(config: ScriptHutConfig, *, on_phase: Callable[[str], None] | None = None) -> Runtime:
    if on_phase:
        on_phase("connecting backends")
    connections = await asyncio.gather(*(init_backend(b) for b in config.backends))
    return Runtime(config=config, backends={b.name: b for b in connections})


async def shutdown_runtime(runtime: Runtime) -> None:
    for backend in runtime.backends.values():
        if backend.ssh_client:
            await backend.ssh_client.disconnect()
