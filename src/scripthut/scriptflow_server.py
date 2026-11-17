"""Server observation through shared backend connections."""

from __future__ import annotations

import asyncio
import logging
import re
import shlex
import uuid
from datetime import UTC, datetime
from typing import Any, cast

from scripthut.scheduler import SLURM_TERMINAL_STATES
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

    def __init__(self, transport: CommandTransport, job_id: str | None, *,
                 job_name: str | None = None, since: str | None = None,
                 image_command: str | None = None):
        self.image_command = image_command
        self.image_preparation: dict[str, str] | None = None
        self.transport = transport
        self.job_id = job_id
        self.job_name = job_name
        self.since = since
        self.controller: dict[str, Any] | None = None

    async def run_command(self, command: str, timeout: int = 30) -> tuple[str, str, int]:
        if not self.job_id:
            return await self.transport.run_command(command, timeout=timeout)
        if not re.fullmatch(r"[0-9]+", self.job_id):
            raise ObservationError("unknown", "Invalid controller ID")
        marker = "\nSF_" + uuid.uuid4().hex + "\n"
        accounting_args = ["sacct", "-n", "-P", "-X", "--jobs", self.job_id, "--format=State"]
        if self.since:
            accounting_args += ["--starttime", self.since[:10]]
        query = shlex.join(accounting_args)
        # Name queries succeed with an empty queue after a job has ended.
        selector = ["--name", self.job_name] if self.job_name else ["--jobs", self.job_id]
        queue = shlex.join(["squeue", "--noheader", *selector, "--format=%T"])
        script = command + "\nrc=$?\nprintf %s " + shlex.quote(marker) + '\nprintf "%s\\n" "$rc"\n'
        script += query + "\nqr=$?\nprintf %s " + shlex.quote(marker) + '\nprintf "%s\\n" "$qr"\n'
        script += queue + "\nqr=$?\nprintf %s " + shlex.quote(marker) + '\nprintf "%s\\n" "$qr"'
        image_marker = "\nSF_IMAGES_" + uuid.uuid4().hex + "\n"
        if self.image_command:
            script += "\nprintf %s " + shlex.quote(image_marker) + "\n" + self.image_command
        stdout, stderr, code = await self.transport.run_command(script, timeout=timeout)
        if self.image_command and not code:
            from scripthut.scriptflow_images import parse_status

            stdout, separator, content = stdout.partition(image_marker)
            if not separator:
                raise ObservationError("malformed", "Missing image observation frame")
            self.image_preparation = parse_status(content)
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
            launch = None
            if cached.launch_key:
                launch = await LaunchStore(self.store.directory).get(cached.launch_key)
                job_id = launch.job_id if launch else None
            from scripthut.scriptflow_images import observation_command

            combined = CombinedRead(
                self.transport(cached), job_id,
                job_name=launch.job_name if launch else None,
                since=launch.created_at if launch else None,
                image_command=observation_command(launch) if launch else None,
            )
            await RecordReader(combined).fetch(cached)
            if combined.image_command and cached.problem in (None, "missing"):
                cached.image_preparation = combined.image_preparation
                if combined.controller is not None:
                    cached.controller = combined.controller
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
