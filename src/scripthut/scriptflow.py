"""Read-only ScriptFlow records, remote file access, and a separate local cache."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import posixpath
import re
import shlex
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    ValidationError,
)

TERMINAL_STATES = frozenset({"completed", "failed", "cancelled", "interrupted"})
RunState = Literal["running", "completed", "failed", "cancelled", "interrupted"]


class ObservationError(RuntimeError):
    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


class Attempt(BaseModel):
    attempt_id: StrictStr
    state: StrictStr
    job_id: StrictStr | None = None
    log: StrictStr | None = None
    exit_code: StrictInt | StrictStr | None = None
    error: StrictStr | None = None
    started_at: AwareDatetime | None = None
    submitted_at: AwareDatetime | None = None
    finished_at: AwareDatetime | None = None


class Task(BaseModel):
    task_id: StrictStr
    name: StrictStr
    state: StrictStr
    error: StrictStr | None = None
    cpus: StrictInt | None = None
    memory: float | str | None = None
    walltime: float | str | None = None
    partition: StrictStr | None = None
    outputs: list[StrictStr] = Field(default_factory=list)
    attempts: list[Attempt]


class Snapshot(BaseModel):
    schema_version: Literal[1]
    run_id: StrictStr
    flow: StrictStr
    state: RunState
    error: StrictStr | None = None
    cleanup_pending: StrictBool
    created_at: AwareDatetime
    updated_at: AwareDatetime
    heartbeat: AwareDatetime
    finished_at: AwareDatetime | None = None
    host: StrictStr
    working_directory: StrictStr
    tasks: dict[str, Task]


def parse_record(content: str) -> tuple[dict[str, Any], Snapshot]:
    try:
        raw = json.loads(content)
    except (ValueError, RecursionError) as exc:
        raise ObservationError("malformed", "Malformed ScriptFlow JSON") from exc
    if not isinstance(raw, dict) or "schema_version" not in raw:
        raise ObservationError("malformed", "ScriptFlow record has no schema_version")
    if type(raw["schema_version"]) is not int or raw["schema_version"] != 1:
        raise ObservationError("unsupported_version", "Unsupported ScriptFlow schema_version")
    try:
        snapshot = Snapshot.model_validate(raw)
    except ValidationError as exc:
        fields = ", ".join(".".join(map(str, error["loc"])) for error in exc.errors())
        raise ObservationError("malformed", f"Invalid ScriptFlow fields: {fields}") from exc
    if any(key != task.task_id for key, task in snapshot.tasks.items()):
        raise ObservationError("malformed", "ScriptFlow task keys do not match task_id")
    return raw, snapshot


def absolute_path(path: str) -> str:
    if not path.startswith("/") or any(c in path for c in "\x00\n\r"):
        raise ObservationError("unsafe_path", "Expected an absolute single-line remote path")
    return "/" + posixpath.normpath(path).lstrip("/")


class Reference(BaseModel):
    backend: str
    run_directory: str

    @property
    def id(self) -> str:
        payload = json.dumps([self.backend, self.run_directory], separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()[:32]

    @classmethod
    def create(cls, backend: str, run_directory: str) -> Reference:
        if not backend.strip():
            raise ObservationError("invalid_reference", "Backend name cannot be empty")
        path = absolute_path(run_directory)
        if path == "/":
            raise ObservationError(
                "unsafe_path", "Register a run directory, not the filesystem root"
            )
        return cls(backend=backend, run_directory=path)


class CachedRun(BaseModel):
    reference: Reference
    snapshot: dict[str, Any] | None = None
    last_successful_fetch: AwareDatetime | None = None
    last_fetch_at: AwareDatetime | None = None
    last_fetch_failed: bool = False
    problem: str | None = None
    fetch_error: str | None = None
    launch_key: str | None = None
    images: list[dict[str, str]] = Field(default_factory=list)
    image_preparation: dict[str, Any] | None = None
    source: dict[str, Any] | None = None
    controller: dict[str, Any] | None = None
    cancel_present: bool | None = None
    cancel_requested_at: AwareDatetime | None = None

    def projection(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(UTC)
        snapshot = None
        age = None
        stale = False
        if self.snapshot is not None:
            _, parsed = parse_record(json.dumps(self.snapshot))
            snapshot = parsed.model_dump(mode="json")
            age = max(0.0, (now - parsed.heartbeat).total_seconds())
            stale = parsed.state not in TERMINAL_STATES and age > 30
        status = self.problem or ("stale" if stale else "fresh" if snapshot else "unfetched")
        preparation = dict(self.image_preparation) if self.image_preparation else None
        if preparation and preparation["status"] == "preparing":
            if preparation.get("started_at") is not None:
                preparation["elapsed_seconds"] = max(
                    0, int(now.timestamp()) - preparation["started_at"]
                )
            if self.controller and self.controller.get("state") == "ended":
                preparation["status"] = "failed"
                preparation["error"] = "Controller ended during image preparation."
        return {
            "reference_id": self.reference.id,
            "backend": self.reference.backend,
            "run_directory": self.reference.run_directory,
            "workflow": snapshot,
            "workflow_status": snapshot["state"]
            if snapshot
            else (
                "not started" if self.launch_key and self.problem == "missing" else "unavailable"
            ),
            "images": self.images,
            "image_preparation": preparation,
            "source": self.source,
            "controller": self.controller,
            "controller_managed": bool(self.launch_key),
            "controller_notice": (
                "Controller ended; workflow heartbeat is stale. Task jobs were left running. "
                "Next: scripthut scriptflow cleanup " + self.reference.id
                if self.controller
                and self.controller.get("state") == "ended"
                and snapshot
                and snapshot["state"] == "running"
                and stale
                else None
            ),
            "cancellation": {
                "requested": self.cancel_present,
                "requested_at": self.cancel_requested_at.isoformat()
                if self.cancel_requested_at
                else None,
            },
            "observation": {
                "status": status,
                "heartbeat_age_seconds": age,
                "stale": stale,
                "last_successful_fetch": (
                    self.last_successful_fetch.isoformat() if self.last_successful_fetch else None
                ),
                "last_fetch_at": self.last_fetch_at.isoformat() if self.last_fetch_at else None,
                "last_fetch_failed": self.last_fetch_failed,
                "error": self.fetch_error,
            },
        }


class ReferenceStore:
    """One atomically replaced file per reference, outside legacy run storage."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def _path(self, reference_id: str) -> Path:
        if not re.fullmatch(r"[a-f0-9]{32}", reference_id):
            raise ObservationError("invalid_reference", "Invalid ScriptFlow reference ID")
        return self.directory / f"{reference_id}.json"

    async def get(self, reference_id: str) -> CachedRun:
        path = self._path(reference_id)
        try:
            data = await asyncio.to_thread(path.read_text)
            cached = CachedRun.model_validate_json(data)
            if cached.reference.id != reference_id:
                raise ValueError("Reference ID mismatch")
            if cached.snapshot is not None:
                parse_record(json.dumps(cached.snapshot))
            return cached
        except FileNotFoundError as exc:
            raise ObservationError("not_found", "ScriptFlow reference is not registered") from exc
        except (OSError, ValueError) as exc:
            raise ObservationError("cache_error", "Cannot read ScriptFlow cache") from exc

    async def list(self) -> list[CachedRun]:
        paths = await asyncio.to_thread(lambda: sorted(self.directory.glob("*.json")))
        return [await self.get(path.stem) for path in paths]

    async def register(self, reference: Reference) -> CachedRun:
        try:
            return await self.get(reference.id)
        except ObservationError as exc:
            if exc.kind != "not_found":
                raise
        cached = CachedRun(reference=reference)
        await self.save(cached)
        return cached

    async def unregister(self, reference_id: str) -> None:
        try:
            await asyncio.to_thread(self._path(reference_id).unlink)
        except FileNotFoundError as exc:
            raise ObservationError("not_found", "ScriptFlow reference is not registered") from exc

    async def save(self, cached: CachedRun) -> None:
        def write() -> None:
            self.directory.mkdir(parents=True, exist_ok=True)
            fd, temporary = tempfile.mkstemp(dir=self.directory, prefix=".snapshot-")
            try:
                with os.fdopen(fd, "w") as stream:
                    stream.write(cached.model_dump_json(indent=2))
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, self._path(cached.reference.id))
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)

        await asyncio.to_thread(write)


class CommandTransport(Protocol):
    async def run_command(self, command: str, timeout: int = 30) -> tuple[str, str, int]: ...


class RecordReader:
    def __init__(self, transport: CommandTransport) -> None:
        self.transport = transport

    async def read(
        self,
        reference: Reference,
        path: str,
        tail: int | None = None,
        *,
        record: bool = False,
        max_bytes: int | None = None,
    ) -> str:
        root = absolute_path(reference.run_directory)
        path = absolute_path(path)
        if not path.startswith(root + "/"):
            raise ObservationError("unsafe_path", "File is outside the registered run directory")
        if tail is not None and tail < 0:
            raise ObservationError("invalid_tail", "Tail must be nonnegative")
        # Resolve symlinks remotely before reading file content.
        script = "\n".join(
            [
                'root=$(realpath -e -- "$1") || exit 44',
                'target=$(realpath -e -- "$2") || exit 44',
                'case "$target" in "$root"/*) ;; *) exit 43 ;; esac',
                '[ -f "$target" ] || exit 44',
                *(
                    [
                        'cancel=$(realpath -m -- "$root/cancel.json") || exit 44',
                        'case "$cancel" in "$root"/*) ;; *) exit 43 ;; esac',
                        'if [ -e "$root/cancel.json" ]; then printf "1\\n"; else printf "0\\n"; fi',
                    ]
                    if record
                    else []
                ),
                ('cat -- "$target"' if tail is None else f'tail -n {tail} -- "$target"')
                + (f" | head -c {max_bytes}" if max_bytes is not None else ""),
            ]
        )
        command = shlex.join(["sh", "-c", script, "scripthut-read", root, path])
        return await self.execute(command)

    async def execute(self, command: str) -> str:
        try:
            stdout, stderr, code = await self.transport.run_command(command)
        except Exception as exc:
            raise ObservationError("unreachable", f"Record transport failed: {exc}") from exc
        if code == 43:
            raise ObservationError("unsafe_path", "Resolved file is outside the run directory")
        if code == 44:
            raise ObservationError("missing", "Remote file or directory is missing or inaccessible")
        if code != 0:
            raise ObservationError(
                "unreachable", f"Remote read failed (exit {code}): {stderr.strip()}"
            )
        return stdout

    async def fetch(self, cached: CachedRun) -> CachedRun:
        try:
            content = await self.read(
                cached.reference,
                cached.reference.run_directory + "/run.json",
                record=True,
            )
            present, separator, content = content.partition("\n")
            if not separator or present not in ("0", "1"):
                raise ObservationError("malformed", "Invalid record transport response")
            raw, _ = parse_record(content)
        except ObservationError as exc:
            cached.last_fetch_failed = True
            cached.problem = exc.kind
            cached.fetch_error = str(exc)
        else:
            cached.snapshot = raw
            cached.cancel_present = present == "1"
            cached.last_successful_fetch = datetime.now(UTC)
            cached.last_fetch_failed = False
            cached.problem = None
            cached.fetch_error = None
        cached.last_fetch_at = datetime.now(UTC)
        return cached

    async def request_cancel(self, cached: CachedRun) -> bool:
        if cached.last_fetch_failed or cached.snapshot is None:
            raise ObservationError("unavailable", "Fetch a readable record before cancellation")
        _, snapshot = parse_record(json.dumps(cached.snapshot))
        if snapshot.state in TERMINAL_STATES:
            return False
        root = absolute_path(cached.reference.run_directory)
        stamp = datetime.now(UTC)
        payload = json.dumps({"requested_at": stamp.isoformat()})
        script = "\n".join(
            [
                'root=$(realpath -e -- "$1") || exit 44',
                'cd -P -- "$root" || exit 44',
                "target=$(realpath -m -- cancel.json) || exit 44",
                'case "$target" in "$root"/*) ;; *) exit 43 ;; esac',
                'if [ -e cancel.json ] || [ -L cancel.json ]; then printf "already"; exit 0; fi',
                "umask 077",
                "tmp=$(mktemp .cancel.XXXXXXXXXX) || exit 45",
                "trap 'rm -f -- \"$tmp\"' EXIT HUP INT TERM",
                'printf "%s\\n" "$2" > "$tmp" || exit 45',
                'mv -nT -- "$tmp" cancel.json || exit 45',
                'if [ -e "$tmp" ]; then printf "already"; else printf "requested"; fi',
            ]
        )
        response = await self.execute(
            shlex.join(["sh", "-c", script, "scripthut-cancel", root, payload])
        )
        if response not in ("requested", "already"):
            raise ObservationError(
                "unreachable", "Cancellation acknowledgement missing; retry to reconcile"
            )
        cached.cancel_present = True
        cached.cancel_requested_at = cached.cancel_requested_at or stamp
        return response == "requested"

    @staticmethod
    def select_task(snapshot: Snapshot, selector: str) -> str:
        if selector in snapshot.tasks:
            return selector
        matches = [key for key, task in snapshot.tasks.items() if task.name == selector]
        if len(matches) > 1:
            raise ObservationError("ambiguous_task", "Task name matches IDs: " + ", ".join(matches))
        if not matches:
            raise ObservationError("not_found", "Task is not present in this ScriptFlow record")
        return matches[0]

    async def logs(
        self,
        cached: CachedRun,
        task_id: str,
        attempt_number: int | None = None,
        tail: int | None = None,
        max_bytes: int | None = None,
    ) -> tuple[str, int]:
        if cached.last_fetch_failed or cached.snapshot is None:
            raise ObservationError("unavailable", "Fetch a readable current record before logs")
        _, snapshot = parse_record(json.dumps(cached.snapshot))
        task = snapshot.tasks[self.select_task(snapshot, task_id)]
        number = attempt_number if attempt_number is not None else len(task.attempts)
        if number < 1 or number > len(task.attempts):
            raise ObservationError("not_found", "Task has no such attempt")
        path = task.attempts[number - 1].log
        if not path:
            raise ObservationError("not_found", "Attempt has no log path")
        return await self.read(cached.reference, path, tail, max_bytes=max_bytes), number
