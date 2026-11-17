"""Server observation through shared backend connections."""

from __future__ import annotations

import asyncio
import logging
import re
import shlex
import uuid
from datetime import UTC, datetime
from typing import Any, cast

from scripthut.backends.slurm import SLURM_TERMINAL_STATES
from scripthut.scriptflow import (
    TERMINAL_STATES,
    CachedRun,
    CommandTransport,
    ObservationError,
    RecordReader,
    ReferenceStore,
)
from scripthut.scriptflow_launch import LaunchStore

logger = logging.getLogger(__name__)


class CombinedRead:
    """Fetch record and controller evidence in one SSH command."""

    def __init__(self, transport: CommandTransport, job_id: str | None):
        self.transport = transport
        self.job_id = job_id
        self.controller: dict[str, Any] | None = None

    async def run_command(self, command: str, timeout: int = 30) -> tuple[str, str, int]:
        if not self.job_id:
            return await self.transport.run_command(command, timeout=timeout)
        if not re.fullmatch(r"[0-9]+", self.job_id):
            raise ObservationError("unknown", "Invalid controller ID")
        marker = "\nSF_" + uuid.uuid4().hex + "\n"
        query = shlex.join(["sacct", "-n", "-P", "-X", "--jobs", self.job_id, "--format=State"])
        queue = shlex.join(["squeue", "--noheader", "--jobs", self.job_id, "--format=%T"])
        script = command + "\nrc=$?\nprintf %s " + shlex.quote(marker) + '\nprintf "%s\\n" "$rc"\n'
        script += query + "\nqr=$?\nprintf %s " + shlex.quote(marker) + '\nprintf "%s\\n" "$qr"\n'
        script += queue + "\nqr=$?\nprintf %s " + shlex.quote(marker) + '\nprintf "%s\\n" "$qr"'
        stdout, stderr, code = await self.transport.run_command(script, timeout=timeout)
        if code:
            raise ObservationError("unreachable", "Combined record read failed")
        parts = stdout.split(marker)
        if len(parts) != 4:
            raise ObservationError("malformed", "Invalid combined read response")
        record_code, _, accounting = parts[1].partition("\n")
        accounting_code, _, queued = parts[2].partition("\n")
        queue_code = parts[3].strip()
        state = "unknown"
        if accounting_code == "0" and queue_code == "0":
            states = [
                line.strip().split()[0].rstrip("+|")
                for line in (queued.strip() or accounting).splitlines()
                if line.strip()
            ]
            if states and all(s in SLURM_TERMINAL_STATES for s in states):
                state = "ended"
            elif states and all(s in ("RUNNING", "COMPLETING", "SUSPENDED") for s in states):
                state = "running"
            elif states and all(s in ("PENDING", "CONFIGURING") for s in states):
                state = "queued"
        self.controller = {
            "job_id": self.job_id,
            "state": state,
            "observed_at": datetime.now(UTC).isoformat(),
        }
        return parts[0], stderr, int(record_code)


class ScriptFlowService:
    def __init__(self, state: Any):
        self.state = state
        self.locks: dict[str, asyncio.Lock] = {}

    @property
    def store(self) -> ReferenceStore:
        if self.state.config is None:
            raise ObservationError("unavailable", "Config not loaded")
        return ReferenceStore(self.state.config.settings.data_dir_resolved / "scriptflow")

    def lock(self, reference: str) -> asyncio.Lock:
        return self.locks.setdefault(reference, asyncio.Lock())

    def transport(self, cached: CachedRun) -> CommandTransport:
        backend = self.state.backends.get(cached.reference.backend)
        if backend is None or backend.ssh_client is None or not backend.enabled:
            raise ObservationError("unreachable", "Backend connection is unavailable")
        status = getattr(backend, "status", None)
        if status is not None and not status.connected:
            raise ObservationError(
                "unreachable", "Backend is disconnected; awaiting server reconnect"
            )
        return cast(CommandTransport, backend.ssh_client)

    async def refresh(self, reference: str) -> CachedRun:
        async with self.lock(reference):
            cached = await self.store.get(reference)
            await self.fetch(cached)
            return cached

    async def fetch(self, cached: CachedRun) -> None:
        try:
            job_id = None
            if cached.launch_key:
                launch = await LaunchStore(self.store.directory).get(cached.launch_key)
                job_id = launch.job_id if launch else None
            combined = CombinedRead(self.transport(cached), job_id)
            await RecordReader(combined).fetch(cached)
            if not cached.last_fetch_failed and combined.controller is not None:
                cached.controller = combined.controller
        except Exception as exc:
            cached.last_fetch_failed = True
            cached.last_fetch_at = datetime.now(UTC)
            cached.problem = "unreachable"
            cached.fetch_error = str(exc)
        await self.store.save(cached)

    async def poll(self) -> None:
        if self.state.config is None:
            return
        try:
            entries = await self.store.list()
        except Exception:
            logger.exception("Cannot load ScriptFlow references")
            return
        for cached in entries:
            terminal = cached.snapshot and cached.snapshot.get("state") in TERMINAL_STATES
            ended = not cached.launch_key or (cached.controller or {}).get("state") == "ended"
            if terminal and ended:
                continue
            try:
                await self.refresh(cached.reference.id)
            except Exception:
                logger.exception("ScriptFlow observation failed")
