"""Runtime initialization shared between the web server and the CLI.

The web server's ``lifespan`` and the CLI both need the same chain of
backend connections, run storage, and a configured ``RunManager``.  This
module isolates that chain so it can be invoked from either entrypoint
without dragging in FastAPI/uvicorn machinery.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import asyncssh

from scripthut.backends.base import JobBackend
from scripthut.backends.batch import BatchBackend
from scripthut.backends.ec2 import EC2Backend
from scripthut.backends.local import (
    LocalBackend,
    LocalExecClient,
    local_backend_supported,
)
from scripthut.backends.pbs import PBSBackend
from scripthut.backends.slurm import SlurmBackend
from scripthut.config_schema import (
    BatchBackendConfig,
    EC2BackendConfig,
    LocalBackendConfig,
    PBSBackendConfig,
    ScriptHutConfig,
    SlurmBackendConfig,
)
from scripthut.models import ConnectionStatus, HPCJob
from scripthut.runs.manager import RunManager
from scripthut.runs.storage import RunStorageManager
from scripthut.ssh.client import SSHClient
from scripthut.ssh.command_log import CommandLog

logger = logging.getLogger(__name__)


@dataclass
class BackendState:
    """State for a single backend.

    Holds the backend driver (Slurm/PBS/Batch/EC2), the optional SSH
    client used for SSH-based backends, polling state, and the most
    recently observed jobs.  Used by both the web server (for live
    polling) and the CLI (for one-shot submission).
    """

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
    """Bundle of objects produced by ``init_runtime``.

    Both the web app's ``state`` and the CLI consume these.  The web app
    additionally tracks SSE / polling state on top of this; the CLI uses
    ``run_manager`` directly and then calls ``shutdown_runtime``.
    """

    config: ScriptHutConfig
    backends: dict[str, BackendState]
    run_storage: RunStorageManager
    run_manager: RunManager


async def init_ec2_backend(
    backend_config: EC2BackendConfig, archive_root: Path,
) -> BackendState:
    """Initialize an AWS EC2-direct backend (no scripthut-side SSH connection)."""
    backend = EC2Backend(backend_config, archive_root=archive_root)
    backend_state = BackendState(
        name=backend_config.name,
        backend_type="ec2",
        ssh_client=None,
        backend=backend,
        status=ConnectionStatus(
            connected=False, host=f"aws:{backend_config.aws.region}"
        ),
        clone_dir="",
    )
    try:
        available = await backend.is_available()
    except Exception as e:
        logger.error(f"Failed to probe EC2 backend '{backend_config.name}': {e}")
        backend_state.status = ConnectionStatus(
            connected=False,
            host=backend_state.status.host,
            error=str(e),
        )
        return backend_state
    if available:
        backend_state.status = ConnectionStatus(
            connected=True, host=backend_state.status.host
        )
        logger.info(
            f"EC2 backend '{backend_config.name}' ready "
            f"(region={backend_config.aws.region}, ami={backend_config.ami})"
        )
    else:
        backend_state.status = ConnectionStatus(
            connected=False,
            host=backend_state.status.host,
            error=f"Subnet '{backend_config.subnet_id}' not reachable",
        )
    return backend_state


async def init_batch_backend(backend_config: BatchBackendConfig) -> BackendState:
    """Initialize an AWS Batch backend (no SSH connection)."""
    backend = BatchBackend(backend_config)
    backend_state = BackendState(
        name=backend_config.name,
        backend_type="batch",
        ssh_client=None,
        backend=backend,
        status=ConnectionStatus(connected=False, host=f"aws:{backend_config.aws.region}"),
        clone_dir="",
    )
    try:
        available = await backend.is_available()
    except Exception as e:
        logger.error(f"Failed to probe Batch backend '{backend_config.name}': {e}")
        backend_state.status = ConnectionStatus(
            connected=False,
            host=backend_state.status.host,
            error=str(e),
        )
        return backend_state
    if available:
        backend_state.status = ConnectionStatus(
            connected=True, host=backend_state.status.host
        )
        logger.info(
            f"Connected to Batch backend '{backend_config.name}' "
            f"(queue={backend_config.aws.job_queue}, region={backend_config.aws.region})"
        )
    else:
        backend_state.status = ConnectionStatus(
            connected=False,
            host=backend_state.status.host,
            error=f"Job queue '{backend_config.aws.job_queue}' not reachable",
        )
    return backend_state


def init_local_backend(
    backend_config: LocalBackendConfig, data_dir: Path,
) -> BackendState:
    """Initialize a local-machine backend (subprocess execution, no SSH).

    The ``ssh_client`` slot gets a :class:`LocalExecClient` so every
    shell-driven code path (cache hashing, generates_source, task-output
    collection) works against the local filesystem unchanged. Job state
    is spooled under ``<data_dir>/local-jobs/<name>`` so running jobs
    survive a scripthut restart.
    """
    exec_client = LocalExecClient()
    backend = LocalBackend(
        backend_config.name,
        spool_dir=data_dir / "local-jobs" / backend_config.name,
    )
    backend_state = BackendState(
        name=backend_config.name,
        backend_type="local",
        ssh_client=exec_client,  # type: ignore[arg-type] — duck-typed SSHClient
        backend=backend,
        status=ConnectionStatus(connected=True, host="localhost"),
        clone_dir=backend_config.clone_dir,
    )
    exec_client.on_command = backend_state.command_log.append
    logger.info(f"Local backend '{backend_config.name}' ready (localhost)")
    return backend_state


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


async def init_runtime(
    config: ScriptHutConfig, *, restore_runs: bool = True,
    on_phase: Callable[[str], None] | None = None,
) -> Runtime:
    """Initialize backends, storage, and run manager from a loaded config.

    Mirrors the web server's startup chain (minus polling and SSE state)
    so the CLI can produce a fully functional ``RunManager`` against the
    same on-disk store. ``on_phase`` (optional) receives coarse progress
    labels so a caller can surface startup state while this runs.
    """
    if on_phase:
        on_phase("connecting backends")
    backends: dict[str, BackendState] = {}

    # Escape hatch: a config with no backends at all still gets a working
    # executor — the built-in local backend, running tasks as subprocesses
    # on this machine. Explicitly-declared backends (including explicit
    # ``type: local`` ones) always take precedence; this only fires when
    # the backends list is empty, so existing configs are unaffected.
    # POSIX-only: on Windows the local backend's shell machinery can't
    # run, so it is skipped with a warning instead of registering an
    # executor whose every job would hang.
    if not config.backends:
        if local_backend_supported():
            logger.info(
                "No backends configured — registering the built-in local "
                "backend 'local' so runs can execute on this machine"
            )
            config.backends.append(LocalBackendConfig(name="local"))
        else:
            logger.warning(
                "No backends configured, and the local backend is not "
                "supported on this platform (needs a POSIX shell) — runs "
                "cannot execute until a backend is added"
            )

    for local_config in config.local_backends:
        if not local_backend_supported():
            logger.warning(
                f"Skipping local backend '{local_config.name}': the local "
                "backend needs a POSIX shell (Linux/macOS) and is not "
                "supported on this platform"
            )
            continue
        backends[local_config.name] = init_local_backend(
            local_config, config.settings.data_dir_resolved,
        )

    # SSH backends connect concurrently — one slow or unreachable cluster
    # shouldn't stack its handshake/timeout on top of the others'.
    ssh_configs = [*config.slurm_backends, *config.pbs_backends]
    ssh_states = await asyncio.gather(
        *(init_backend(c) for c in ssh_configs)
    )
    for backend_state in ssh_states:
        backends[backend_state.name] = backend_state

    for ecs_config in config.ecs_backends:
        logger.warning(
            f"ECS backend '{ecs_config.name}' configured but ECS backend not yet implemented"
        )

    for batch_config in config.batch_backends:
        backends[batch_config.name] = await init_batch_backend(batch_config)

    ec2_archive_root = config.settings.data_dir_resolved / "ec2-logs"
    for ec2_config in config.ec2_backends:
        backends[ec2_config.name] = await init_ec2_backend(ec2_config, ec2_archive_root)

    run_storage = RunStorageManager(config.settings.data_dir_resolved / "workflows")

    ssh_clients: dict[str, SSHClient] = {
        name: bs.ssh_client for name, bs in backends.items() if bs.ssh_client is not None
    }
    job_backends: dict[str, JobBackend] = {
        name: bs.backend for name, bs in backends.items() if bs.backend is not None
    }
    run_manager = RunManager(
        config, ssh_clients, storage=run_storage, job_backends=job_backends,
    )
    logger.info(
        f"Initialized run manager with {len(config.sources)} sources"
    )

    if restore_runs:
        if on_phase:
            on_phase("restoring runs")
        restored = await run_manager.restore_from_storage()
        if restored:
            logger.info(f"Restored {restored} runs from storage")

    return Runtime(
        config=config,
        backends=backends,
        run_storage=run_storage,
        run_manager=run_manager,
    )


async def shutdown_runtime(runtime: Runtime) -> None:
    """Persist any dirty runs and disconnect all SSH-based backends."""
    runtime.run_manager.save_dirty()
    for backend_state in runtime.backends.values():
        if backend_state.ssh_client:
            await backend_state.ssh_client.disconnect()
