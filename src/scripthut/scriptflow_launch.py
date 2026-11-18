"""Standalone controller jobs and durable launch reconciliation."""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
import re
import shlex
import tempfile
import uuid
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from scripthut.config_schema import ScriptFlowConfig, ScriptHutConfig, SlurmBackendConfig
from scripthut.environment import LabeledRule, flatten, resolve
from scripthut.scheduler import SLURM_TERMINAL_STATES, SlurmControl
from scripthut.scriptflow import (
    TERMINAL_STATES,
    CachedRun,
    ObservationError,
    RecordReader,
    Reference,
    ReferenceStore,
    absolute_path,
)

q = shlex.quote


def accounting_start_date(launched_at: str) -> str:
    """Include the previous UTC day when querying a scheduler in local time."""
    return (datetime.fromisoformat(launched_at).astimezone(UTC) - timedelta(days=1)).date().isoformat()


def now() -> str:
    return datetime.now(UTC).isoformat()


class Launch(BaseModel):
    key: str
    request_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    backend: str
    workdir: str
    flow: str
    run_id: str
    runs_dir: str
    run_directory: str
    env_group: str
    resources: ScriptFlowConfig
    job_name: str
    session: str
    stage: str = "prepared"
    created_at: str = Field(default_factory=now)
    updated_at: str = Field(default_factory=now)
    force_pattern: str | None = None
    images: list[dict[str, str]] = Field(default_factory=list)
    source: str | None = None
    branch: str | None = None
    commit: str | None = None
    repository_workdir: str | None = None
    flow_dir: str = "."
    detached: bool = False
    snapshot: bool = False
    job_id: str | None = None
    cleanup_job_id: str | None = None
    cleanup_stage: str | None = None
    cleanup_token: str | None = None

    def source_metadata(self) -> dict[str, Any] | None:
        if not self.source:
            return None
        return {
            "name": self.source,
            "branch": self.branch,
            "commit": self.commit,
            "short_commit": self.commit[:12] if self.commit else None,
            "workdir": self.repository_workdir,
            "flow_dir": self.flow_dir,
            "detached": self.detached,
            "snapshot": self.snapshot,
        }

    @property
    def receipt(self) -> str:
        return f"{self.runs_dir}/.scripthut-{self.request_id}.receipt"

    @property
    def reference(self) -> Reference:
        return Reference.create(self.backend, self.run_directory)


class LaunchStore:
    def __init__(self, directory: Path):
        self.directory = directory / "launches"

    def path(self, key: str) -> Path:
        if not re.fullmatch(r"[a-f0-9]{32}", key):
            raise ValueError("Invalid launch key")
        return self.directory / f"{key}.json"

    @contextmanager
    def lock(self, key: str) -> Iterator[None]:
        self.directory.mkdir(parents=True, exist_ok=True)
        with self.path(key).with_suffix(".lock").open("a") as stream:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ObservationError("busy", "Another command is handling this launch") from exc
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)

    async def get(self, key: str) -> Launch | None:
        try:
            return Launch.model_validate_json(await asyncio.to_thread(self.path(key).read_text))
        except FileNotFoundError:
            return None

    async def save(self, launch: Launch) -> None:
        launch.updated_at = now()

        def write() -> None:
            self.directory.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(dir=self.directory)
            try:
                with os.fdopen(fd, "w") as stream:
                    stream.write(launch.model_dump_json(indent=2))
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(name, self.path(launch.key))
            finally:
                if os.path.exists(name):
                    os.unlink(name)

        await asyncio.to_thread(write)


class ControllerBackend(SlurmControl):
    async def _verify_job_accepted(self, job_id: str) -> bool:
        # Acknowledged sbatch IDs survive delayed queue/accounting visibility.
        return True

    async def query(self, name: str, since: str) -> dict[str, str]:
        result: dict[str, str] = {}
        commands = [
            shlex.join(
                [
                    "sacct",
                    "-n",
                    "-P",
                    "-X",
                    "--starttime",
                    accounting_start_date(since),
                    "--name",
                    name,
                    "--format=JobIDRaw,JobName%100,State",
                ]
            ),
            shlex.join(["squeue", "--noheader", "--name", name, "--format=%i|%j|%T"]),
        ]
        for command in commands:
            stdout, stderr, code = await self._ssh.run_command(command)
            if code:
                raise ObservationError("unknown", f"Controller query failed: {stderr.strip()}")
            for line in stdout.splitlines():
                if not line.strip():
                    continue
                parts = line.strip().split("|")
                if len(parts) != 3 or not re.fullmatch(r"[0-9]+", parts[0]):
                    raise ObservationError("unknown", "Malformed controller listing")
                job_id, job_name, state = parts
                if job_name == name:
                    result[job_id] = state.split()[0].rstrip("+")
        return result

    async def cancel_job(self, job_id: str) -> None:
        if not re.fullmatch(r"[0-9]+", job_id):
            raise ValueError("Invalid controller job ID")
        _, stderr, code = await self._ssh.run_command(shlex.join(["scancel", job_id]))
        if code:
            raise ObservationError("unknown", f"Controller release failed: {stderr.strip()}")


def backend_config(config: ScriptHutConfig, name: str) -> SlurmBackendConfig:
    backend = config.get_backend(name)
    if not isinstance(backend, SlurmBackendConfig):
        raise ValueError("Automated ScriptFlow controllers require a Slurm backend")
    return backend


def environment(config: ScriptHutConfig, launch: Launch) -> str:
    backend = backend_config(config, launch.backend)
    if launch.env_group not in backend.env_groups:
        raise ValueError(
            f"ScriptFlow env_group {launch.env_group!r} is not defined "
            f"on backend {launch.backend!r}"
        )
    rules = backend.env + backend.env_groups[launch.env_group]
    env, init = resolve(
        flatten(
            [LabeledRule(rule, "scriptflow") for rule in rules],
            backend.env_groups,
        ),
        {},
    )
    for key in env:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError("Invalid environment variable name")
    return "\n".join([*(f"export {key}={q(value)}" for key, value in env.items()), init])


def make_launch(config: ScriptHutConfig, args: Any) -> Launch:
    backend_config(config, args.backend)
    workdir = absolute_path(args.workdir)
    runs_dir = absolute_path(args.runs_dir or workdir + "/.sf/runs")
    run_id = args.run_id or uuid.uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", run_id):
        raise ValueError("Run ID must be a single alphanumeric path component")
    if not args.flow or args.flow.startswith("-") or any(c in args.flow for c in "\x00\n\r"):
        raise ValueError("Flow must be a single-line name")
    values = config.scriptflow.model_dump()
    values.update(
        {
            key: getattr(args, key)
            for key in ("cpus", "memory", "time_limit", "partition")
            if getattr(args, key, None) is not None
        }
    )
    resources = ScriptFlowConfig.model_validate(values)
    if not resources.env_group:
        raise ValueError("Configure scriptflow.env_group before launching")
    # Default-ID invocations reuse one request; an explicit new ID starts another.
    identity = (
        [args.backend, runs_dir, args.run_id]
        if args.run_id
        else [args.backend, workdir, args.flow, runs_dir, None]
    )
    key = hashlib.sha256(json.dumps(identity).encode()).hexdigest()[:32]
    request = uuid.uuid4().hex
    launch = Launch(
        key=key,
        request_id=request,
        backend=args.backend,
        workdir=workdir,
        flow=args.flow,
        run_id=run_id,
        runs_dir=runs_dir,
        run_directory=runs_dir + "/" + run_id,
        env_group=resources.env_group,
        resources=resources,
        job_name="sf-" + request,
        session="sf-" + request,
    )
    launch.force_pattern = getattr(args, "force_pattern", None)
    environment(config, launch)
    return launch


def job_script(config: ScriptHutConfig, launch: Launch, cleanup: bool = False) -> str:
    backend = backend_config(config, launch.backend)
    resources = launch.resources
    name = "sf-clean-" + str(launch.cleanup_token) if cleanup else launch.job_name
    receipt = (
        launch.receipt + ".cleanup-" + str(launch.cleanup_token) if cleanup else launch.receipt
    )
    partition = resources.partition
    if partition:
        partition = backend.partition_map.get(partition, backend.default_partition or partition)
    directives = {
        "job-name": name,
        "cpus-per-task": str(resources.cpus),
        "mem": resources.memory,
        "time": "00:05:00" if cleanup else resources.time_limit,
        "output": receipt + ".log",
        "account": backend.account,
        "partition": partition,
    }
    for value in directives.values():
        if value and any(c in value for c in "\n\r\x00"):
            raise ValueError("Invalid batch directive")
    lines = [
        "#!/bin/bash" + (" -l" if backend.login_shell else ""),
        *(f"#SBATCH --{key}={q(value)}" for key, value in directives.items() if value),
        "set -e",
        "set +u",
        environment(config, launch),
        "set -eu",
        "cd -- " + q(launch.workdir),
    ]
    if launch.images and not cleanup:
        from scripthut.scriptflow_images import token_start

        lines.insert(lines.index("set -e"), token_start(launch))
    command = (
        shlex.join(["scriptflow", "cleanup", launch.run_id, "--runs-dir", launch.runs_dir])
        if cleanup
        else shlex.join(
            [
                "scriptflow",
                "run",
                launch.flow,
                *(["-f", launch.force_pattern] if launch.force_pattern is not None else []),
                "--run-id",
                launch.run_id,
                "--runs-dir",
                launch.runs_dir,
            ]
        )
    )
    if not cleanup:
        claim = (
            launch.runs_dir
            + "/.scripthut-run-"
            + hashlib.sha256(launch.run_id.encode()).hexdigest()
            + ".claim"
        )
        lines += [
            f"test ! -e {q(launch.run_directory)} && "
            f"test ! -L {q(launch.run_directory)} || exit 73",
            "mkdir -- " + q(claim) + " || exit 73",
        ]
    if launch.images and not cleanup:
        from scripthut.scriptflow_images import preparation_script

        lines.append(preparation_script(config, launch))
    # Fixed-position receipt fields are parsed locally; no remote interpreter.
    writer = (
        f"receipt={q(receipt)}\n"
        'tmp=$(mktemp "${receipt}.XXXXXX")\n'
        'printf "%s\\n" "$SLURM_JOB_ID" "$(hostname)" '
        + q(launch.session)
        + ' "$SF_STARTED_AT" "$rc" > "$tmp"\n'
        'mv -f -- "$tmp" "$receipt"\n'
    )
    lines += ["export SF_STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)", "rc=running", writer]
    inner = "set +e\n" + command + "\nrc=$?\n" + writer + 'exit "$rc"'
    if cleanup:
        lines += [inner]
    else:
        # A dedicated tmux server inherits this job's environment.
        tmux = shlex.join(["tmux", "-L", launch.session])
        lines += [
            tmux + " new-session -d -s " + q(launch.session) + " " + q("bash -c " + q(inner)),
            f"while {tmux} has-session -t {q(launch.session)} 2>/dev/null; do sleep 2; done",
            "rc=$(tail -n 1 -- " + q(receipt) + ")",
            'case "$rc" in *[!0-9]*|"") exit 1 ;; *) exit "$rc" ;; esac',
        ]
    script = "\n".join(lines) + "\n"
    if "\nSCRIPTHUT_EOF\n" in script:
        raise ValueError("Environment contains reserved submission delimiter")
    return script


async def receipt_read(
    reader: RecordReader, launch: Launch, cleanup: bool = False
) -> dict[str, str] | None:
    path = launch.receipt + ".cleanup-" + str(launch.cleanup_token) if cleanup else launch.receipt
    try:
        text = await reader.read(Reference.create(launch.backend, launch.runs_dir), path)
    except ObservationError as exc:
        if exc.kind == "missing":
            return None
        raise
    fields = text.splitlines()
    if len(fields) != 5 or not re.fullmatch(r"[0-9]+", fields[0]):
        raise ObservationError("unknown", "Malformed controller receipt")
    return dict(
        zip(("job_id", "host", "session", "started_at", "exit_status"), fields, strict=True)
    )


async def register_launch(store: ReferenceStore, launch: Launch) -> CachedRun:
    cached = await store.register(launch.reference)
    cached.launch_key = launch.key
    cached.source = launch.source_metadata()
    cached.images = launch.images
    await store.save(cached)
    return cached


async def launch_run(
    config: ScriptHutConfig,
    proposed: Launch,
    store: ReferenceStore,
    backend: ControllerBackend,
    reader: RecordReader,
    resubmit: bool = False,
    prepare: Callable[[Launch], Awaitable[None]] | None = None,
) -> dict[str, Any]:
    launches = LaunchStore(store.directory)
    with launches.lock("0" * 32):
        launch = await launches.get(proposed.key)
        if launch is None:
            for path in await asyncio.to_thread(lambda: list(launches.directory.glob("*.json"))):
                candidate = await launches.get(path.stem)
                if candidate and candidate.reference == proposed.reference:
                    launch = candidate
                    break
        if launch is None:
            launch = proposed
            await launches.save(launch)
        if (
            launch.flow != proposed.flow
            or launch.workdir != proposed.workdir
            or launch.resources != proposed.resources
            or launch.images != proposed.images
            or launch.force_pattern != proposed.force_pattern
            or (launch.source, launch.branch, launch.commit, launch.flow_dir, launch.snapshot)
            != (
                proposed.source,
                proposed.branch,
                proposed.commit,
                proposed.flow_dir,
                proposed.snapshot,
            )
        ):
            raise ValueError("Existing launch uses different settings; use a new --run-id")
        if launch.job_id:
            await register_launch(store, launch)
            return launch_output(launch)
        if launch.stage in ("submitting", "ambiguous", "unknown"):
            try:
                jobs = await backend.query(launch.job_name, launch.created_at)
                receipt = await receipt_read(reader, launch)
                ids = set(jobs) | ({receipt["job_id"]} if receipt else set())
                if len(ids) > 1:
                    raise ObservationError(
                        "multiple_matches",
                        "Multiple controller jobs match; manual reconciliation required",
                    )
                if ids:
                    launch.job_id = ids.pop()
                    launch.stage = "acknowledged"
                    await launches.save(launch)
                    await register_launch(store, launch)
                    return launch_output(launch)
                launch.stage = "ambiguous"
            except Exception as exc:
                launch.stage = "unknown"
                await launches.save(launch)
                raise ObservationError("unknown", f"Launch reconciliation unknown: {exc}") from exc
            await launches.save(launch)
            if not resubmit:
                return launch_output(launch)
        script = job_script(config, launch)
        if prepare:
            assert launch.repository_workdir is not None
            for path in launches.directory.glob("*.json"):
                other = await launches.get(path.stem)
                if (
                    other is None
                    or other.key == launch.key
                    or other.backend != launch.backend
                    or other.repository_workdir != launch.repository_workdir
                    or other.stage not in ("submitting", "ambiguous", "unknown", "acknowledged")
                ):
                    continue
                observed = CachedRun(reference=other.reference)
                await reader.fetch(observed)
                if observed.last_fetch_failed:
                    controller = await controller_observation(backend, reader, other)
                    if observed.problem != "missing" or controller["state"] != "ended":
                        raise ValueError(
                            f"{launch.repository_workdir}: previous controller "
                            f"{other.job_name} has no readable workflow record; "
                            "observe or release it before preparing another launch"
                        )
                elif observed.snapshot and observed.snapshot["state"] == "running":
                    stale = (
                        " (stale heartbeat)"
                        if observed.projection()["observation"]["stale"]
                        else ""
                    )
                    raise ValueError(
                        f"{launch.repository_workdir}: running ScriptFlow run "
                        f"{other.run_id}{stale}; resolve it or run scriptflow cleanup "
                        f"{other.run_id} on a compute node"
                    )
            await reader.execute(
                shlex.join(
                    [
                        "sh",
                        "-c",
                        'if [ -e "$1" ] || [ -L "$1" ]; then '
                        'echo "$2: run directory $1 already exists" >&2; exit 73; fi',
                        "scripthut-source-guard",
                        launch.run_directory,
                        launch.repository_workdir,
                    ]
                )
            )
            await prepare(launch)
            await launches.save(launch)
        guard = 'if [ -e "$1" ] || [ -L "$1" ]; then exit 73; fi\n[ -d "$2" ] || exit 44'
        await reader.execute(
            shlex.join(["sh", "-c", guard, "scripthut-guard", launch.run_directory, launch.workdir])
        )
        await reader.execute(shlex.join(["mkdir", "-p", "--", launch.runs_dir]))
        if launch.images:
            from scripthut.scriptflow_images import upload_token

            await upload_token(config, reader, launch)
        launch.stage = "submitting"
        await launches.save(launch)
        try:
            launch.job_id = await backend.submit_job(script)
        except Exception:
            launch.stage = "ambiguous"
            await launches.save(launch)
            return launch_output(launch)
        launch.stage = "acknowledged"
        await launches.save(launch)
        await register_launch(store, launch)
        return launch_output(launch)


def launch_output(launch: Launch) -> dict[str, Any]:
    return {
        "images": launch.images,
        "source": launch.source_metadata(),
        "status": launch.stage,
        "request_id": launch.request_id,
        "reference_id": launch.reference.id,
        "run_id": launch.run_id,
        "job_id": launch.job_id,
        "attach_hint": shlex.join(["tmux", "-L", launch.session, "attach", "-t", launch.session])
        + " (on the compute node)",
        "message": (
            "Reuse the same launch options to reconcile; "
            "--resubmit-ambiguous explicitly permits another submission."
        )
        if not launch.job_id
        else "Controller submission acknowledged.",
    }


async def controller_observation(
    backend: ControllerBackend, reader: RecordReader, launch: Launch
) -> dict[str, Any]:
    data: dict[str, Any] = {"state": "unknown", "observed_at": now(), "job_id": launch.job_id}
    try:
        jobs = await backend.query(launch.job_name, launch.created_at)
        receipt = await receipt_read(reader, launch)
        data["receipt"] = receipt
        state = jobs.get(launch.job_id or "")
        if state in SLURM_TERMINAL_STATES:
            data["state"] = "ended"
        elif state in ("RUNNING", "COMPLETING", "SUSPENDED"):
            data["state"] = "running"
        elif state in ("PENDING", "CONFIGURING"):
            data["state"] = "queued"
        # An absent job is unknown, even if the application wrote an exit receipt.
    except Exception as exc:
        data["error"] = str(exc)
    return data


async def control(
    config: ScriptHutConfig,
    cached: CachedRun,
    store: ReferenceStore,
    backend: ControllerBackend,
    reader: RecordReader,
    action: str,
    force: bool = False,
) -> dict[str, Any]:
    if not cached.launch_key:
        raise ValueError("Controller ownership is unknown for this registered run")
    launches = LaunchStore(store.directory)
    with launches.lock(cached.launch_key):
        launch = await launches.get(cached.launch_key)
        if launch is None or not launch.job_id:
            raise ValueError("No acknowledged controller job")
        observation = await controller_observation(backend, reader, launch)
        if action == "release":
            cached.controller = observation
            await store.save(cached)
            if observation["state"] == "ended":
                return {
                    "status": "already_ended",
                    "job_id": launch.job_id,
                    "message": "Controller already ended; nothing to release.",
                }
            await reader.fetch(cached)
            await store.save(cached)
            workflow = cached.projection()["workflow"]
            if not force and (
                cached.last_fetch_failed
                or not workflow
                or workflow["state"] not in TERMINAL_STATES
                or workflow["cleanup_pending"]
            ):
                raise ValueError(
                    "Workflow is not confirmed terminal with cleanup complete; --force is required"
                )
            await backend.cancel_job(launch.job_id)
            return {"status": "release_requested", "job_id": launch.job_id}
        if observation["state"] != "ended":
            raise ValueError(
                "Cleanup requires a confirmed ended controller (queued/running/unknown refused)"
            )
        if launch.cleanup_stage == "failed":
            launch.cleanup_stage = None
            launch.cleanup_token = None
            launch.cleanup_job_id = None
        if not launch.cleanup_token:
            launch.cleanup_token = uuid.uuid4().hex
        if launch.cleanup_stage:
            receipt = await receipt_read(reader, launch, cleanup=True)
            if receipt and receipt["exit_status"] != "running":
                if receipt["exit_status"] != "0":
                    launch.cleanup_stage = "failed"
                    await launches.save(launch)
                return {
                    "status": "completed" if receipt["exit_status"] == "0" else "failed",
                    **receipt,
                }
            return {
                "status": "pending",
                "job_id": launch.cleanup_job_id,
                "message": (
                    "Cleanup already submitted or acknowledgement uncertain; "
                    "invoke again to observe its receipt."
                ),
            }
        script = job_script(config, launch, cleanup=True)
        launch.cleanup_stage = "submitting"
        await launches.save(launch)
        try:
            launch.cleanup_job_id = await backend.submit_job(script)
            launch.cleanup_stage = "acknowledged"
        finally:
            await launches.save(launch)
        return {
            "status": "pending",
            "job_id": launch.cleanup_job_id,
            "message": "Cleanup submitted; invoke cleanup again to read the compute-job result.",
        }


async def discover(
    reader: RecordReader,
    store: ReferenceStore,
    backend: str,
    workdir: str,
    runs_dir: str | None,
    register: bool,
) -> list[dict[str, Any]]:
    root = absolute_path(runs_dir or absolute_path(workdir) + "/.sf/runs")
    command = shlex.join(
        [
            "bash",
            "-o",
            "pipefail",
            "-c",
            '[ -d "$1" ] || exit 44\n'
            'find "$1" -mindepth 1 -maxdepth 1 -type d ! -name ".*" '
            '-printf "%T@ %f\\0" | sort -z -nr',
            "scripthut-discover",
            root,
        ]
    )
    listing = await reader.execute(command)
    results = []
    for entry in listing.split("\x00"):
        if not entry:
            continue
        _, run_id = entry.split(" ", 1)
        reference = Reference.create(backend, root + "/" + run_id)
        cached = CachedRun(reference=reference)
        await reader.fetch(cached)
        if register:
            try:
                await store.get(reference.id)
            except ObservationError as exc:
                if exc.kind != "not_found":
                    raise
                await store.save(cached)
        data = cached.projection()
        data["run_id"] = run_id
        results.append(data)
    return results
