"""Local-machine backend: run tasks as subprocesses on the scripthut host.

The escape hatch for when no cluster is available (or configured): the
same TaskDefinition JSON, the same dependency-order enforcement, the same
result-cache participation — only the executor differs. Two pieces make
that work:

* :class:`LocalExecClient` duck-types the subset of ``SSHClient`` the run
  machinery uses (``run_command`` & friends), so every shell-driven code
  path — cache hashing, ``generates_source`` reads, task-output
  collection, log-dir setup — flows through unchanged, just against the
  local filesystem.

* :class:`LocalBackend` implements :class:`JobBackend` over detached
  subprocesses. Execution is deliberately dumb, per the caching model's
  contract: there is no mtime or freshness logic here — a task runs
  unconditionally unless the (shared) result cache said hit before
  submission.

Job durability mirrors what a scheduler's accounting DB provides. Each
submission writes a *spool* entry (``<spool>/<job_id>.json`` with the
pid + metadata) and launches a tiny supervisor shell that records the
script's exit code to ``<job_id>.rc`` when it finishes. ``get_jobs`` /
``get_job_stats`` answer from the spool, not from in-process state, so a
scripthut restart doesn't orphan running jobs or lose their verdicts —
the same property sacct gives the Slurm backend.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shlex
import shutil
import signal
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from scripthut.backends.base import (
    ClusterInfo,
    DiskInfo,
    JobBackend,
    JobStats,
    PartitionInfo,
    SubmitResult,
)
from scripthut.backends.utils import generate_script_body
from scripthut.models import HPCJob, JobState

if TYPE_CHECKING:
    from scripthut.runs.models import TaskDefinition

logger = logging.getLogger(__name__)


def local_backend_supported() -> bool:
    """Whether this host can run the local backend at all.

    Everything here drives a POSIX shell — bash task scripts, the
    ``rc=$?``-style supervisor, process-group kills via ``os.killpg`` —
    and ``create_subprocess_shell`` on Windows goes through ``cmd.exe``,
    which silently mangles all of it (jobs launch but never record an
    exit code). Runtime wiring consults this and skips local backends on
    unsupported hosts rather than registering an executor whose every
    job would hang.
    """
    return os.name != "nt"


# How long a finished job stays visible in ``get_jobs`` after its exit
# code lands. Long enough for every poll consumer to observe the
# COMPLETED state and resolve the item via ``get_job_stats``; short
# enough that the listing stays bounded.
_FINISHED_LINGER_SECONDS = 900.0

LOCAL_FAILURE_STATES = {
    "FAILED": "Task exited with a non-zero code",
    "CANCELLED": "Cancelled",
}

LOCAL_TERMINAL_STATES = frozenset({"COMPLETED", "FAILED", "CANCELLED"})


class LocalExecClient:
    """``SSHClient``-compatible shell runner that executes on this machine.

    Implements the members the run machinery actually touches:
    ``run_command``, ``is_connected``, ``connect``/``disconnect``, and the
    ``on_command`` logging hook. Interactive sessions (browser terminal)
    are not supported locally.
    """

    def __init__(self) -> None:
        # CommandLog.append when wired by runtime (same hook as SSHClient).
        self.on_command: Callable[..., None] | None = None

    @property
    def is_connected(self) -> bool:
        return True

    async def connect(self, timeout: int = 15) -> None:
        return None

    async def disconnect(self) -> None:
        return None

    def _log(
        self, command: str, start: float,
        stdout: str = "", stderr: str = "", exit_code: int | None = None,
        error: str | None = None,
    ) -> None:
        if self.on_command is None:
            return
        from scripthut.ssh.command_log import CommandLogEntry

        self.on_command(CommandLogEntry(
            timestamp=datetime.now(timezone.utc),
            command=command,
            exit_code=exit_code,
            duration_ms=int((time.perf_counter() - start) * 1000),
            stdout=stdout,
            stderr=stderr,
            error=error,
        ))

    async def run_command(
        self, command: str, timeout: int = 30,
    ) -> tuple[str, str, int]:
        """Run ``command`` through the local shell; same contract as SSHClient."""
        start = time.perf_counter()
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(), timeout=timeout,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            self._log(command, start, error=f"Timeout after {timeout}s")
            raise RuntimeError(f"Command timed out after {timeout}s")
        stdout = stdout_b.decode("utf-8", errors="replace")
        stderr = stderr_b.decode("utf-8", errors="replace")
        exit_code = proc.returncode if proc.returncode is not None else -1
        self._log(command, start, stdout, stderr, exit_code)
        return (stdout, stderr, exit_code)


class LocalBackend(JobBackend):
    """Run tasks as detached local subprocesses with spool-backed accounting."""

    def __init__(self, backend_name: str, spool_dir: Path) -> None:
        self._name = backend_name
        self._spool = spool_dir
        self._spool.mkdir(parents=True, exist_ok=True)
        # Live process handles, kept only so children get reaped (the
        # authoritative job state lives in the spool files).
        self._procs: dict[str, asyncio.subprocess.Process] = {}
        self._reapers: set[asyncio.Task] = set()

    @property
    def name(self) -> str:
        return self._name

    # --- Spool helpers ------------------------------------------------------

    def _meta_path(self, job_id: str) -> Path:
        return self._spool / f"{job_id}.json"

    def _rc_path(self, job_id: str) -> Path:
        return self._spool / f"{job_id}.rc"

    def _script_path(self, job_id: str) -> Path:
        return self._spool / f"{job_id}.sh"

    def _read_meta(self, job_id: str) -> dict | None:
        try:
            return json.loads(self._meta_path(job_id).read_text())
        except (OSError, json.JSONDecodeError):
            return None

    def _read_rc(self, job_id: str) -> int | None:
        try:
            return int(self._rc_path(job_id).read_text().strip())
        except (OSError, ValueError):
            return None

    @staticmethod
    def _pid_alive(pid: int | None) -> bool:
        if not pid:
            return False
        try:
            os.kill(pid, 0)
        except (OSError, ProcessLookupError):
            return False
        return True

    # --- Submission ---------------------------------------------------------

    async def submit_task(
        self,
        task: "TaskDefinition",
        script: str,
        env_vars: dict[str, str] | None = None,
    ) -> SubmitResult:
        return await self._launch(
            script, task_name=task.name, cpus=task.cpus,
        )

    async def submit_job(self, script: str) -> str:
        result = await self._launch(script)
        return result.job_id

    async def _launch(
        self, script: str, *, task_name: str = "", cpus: int = 1,
    ) -> SubmitResult:
        job_id = uuid.uuid4().hex[:12]
        script_path = self._script_path(job_id)
        script_path.write_text(script)
        rc_path = self._rc_path(job_id)

        # Supervisor shell: run the script, then atomically record its
        # exit code. The tmp+mv dance means an ``.rc`` file, once visible,
        # is always complete. ``start_new_session`` detaches the whole
        # tree into its own process group so cancel can kill it wholesale
        # and a scripthut restart doesn't take running jobs down with it.
        supervisor = (
            f"bash {shlex.quote(str(script_path))}; rc=$?; "
            f"echo $rc > {shlex.quote(str(rc_path))}.tmp && "
            f"mv {shlex.quote(str(rc_path))}.tmp {shlex.quote(str(rc_path))}"
        )
        proc = await asyncio.create_subprocess_shell(
            supervisor,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )

        meta = {
            "job_id": job_id,
            "name": task_name,
            "cpus": cpus,
            "pid": proc.pid,
            "submitted_at": datetime.now(timezone.utc).isoformat(),
        }
        self._meta_path(job_id).write_text(json.dumps(meta))

        # Reap the child when it exits so it never zombies; job state is
        # read from the spool, not from this handle.
        self._procs[job_id] = proc
        reaper = asyncio.get_running_loop().create_task(
            self._reap(job_id, proc)
        )
        self._reapers.add(reaper)
        reaper.add_done_callback(self._reapers.discard)

        logger.info(
            f"local: launched job {job_id} (pid {proc.pid}"
            f"{', task ' + task_name if task_name else ''})"
        )
        return SubmitResult(
            job_id=job_id,
            submit_output=f"local job {job_id} (pid {proc.pid})",
        )

    async def _reap(self, job_id: str, proc: asyncio.subprocess.Process) -> None:
        try:
            await proc.wait()
        finally:
            self._procs.pop(job_id, None)

    # --- Observation --------------------------------------------------------

    async def get_jobs(self, user: str | None = None) -> list[HPCJob]:
        """List running jobs plus recently-finished ones (as COMPLETED).

        Finished jobs are reported with ``JobState.COMPLETED`` regardless
        of their exit code — mirroring a scheduler queue that only says
        "gone" — and the real verdict comes from :meth:`get_job_stats`,
        exactly like the Slurm SETTLING → sacct flow.
        """
        now = time.time()
        jobs: list[HPCJob] = []
        for meta_path in sorted(self._spool.glob("*.json")):
            job_id = meta_path.stem
            meta = self._read_meta(job_id)
            if meta is None:
                continue
            rc_path = self._rc_path(job_id)
            if rc_path.exists():
                if now - rc_path.stat().st_mtime > _FINISHED_LINGER_SECONDS:
                    continue
                state = JobState.COMPLETED
            elif self._pid_alive(meta.get("pid")):
                state = JobState.RUNNING
            else:
                # No exit record and the supervisor is gone (e.g. killed
                # -9). Don't list it — consumers resolve via stats/grace.
                continue

            submit_time = None
            raw = meta.get("submitted_at")
            if raw:
                try:
                    submit_time = datetime.fromisoformat(raw)
                except ValueError:
                    pass
            jobs.append(HPCJob(
                job_id=job_id,
                name=meta.get("name") or job_id,
                user=user or os.environ.get("USER", "local"),
                state=state,
                partition="local",
                time_used="",
                nodes="localhost",
                cpus=int(meta.get("cpus") or 1),
                memory="",
                submit_time=submit_time,
                start_time=submit_time,
            ))
        return jobs

    async def get_job_stats(
        self, job_ids: list[str], user: str | None = None
    ) -> dict[str, JobStats]:
        """Resolve verdicts from the spool's exit-code records."""
        stats: dict[str, JobStats] = {}
        for job_id in job_ids:
            rc = self._read_rc(job_id)
            if rc is None:
                continue  # still running or vanished — no verdict yet
            meta = self._read_meta(job_id) or {}
            start_time = None
            raw = meta.get("submitted_at")
            if raw:
                try:
                    start_time = datetime.fromisoformat(raw)
                except ValueError:
                    pass
            end_time = datetime.fromtimestamp(
                self._rc_path(job_id).stat().st_mtime, tz=timezone.utc,
            )
            stats[job_id] = JobStats(
                cpu_efficiency=0.0,
                max_rss="",
                total_cpu="",
                start_time=start_time,
                end_time=end_time,
                state="COMPLETED" if rc == 0 else "FAILED",
                exit_code=rc,
            )
        return stats

    async def is_available(self) -> bool:
        return True

    async def cancel_job(self, job_id: str) -> None:
        """SIGTERM the job's process group (supervisor + script + children)."""
        meta = self._read_meta(job_id)
        pid = meta.get("pid") if meta else None
        if not pid:
            return
        try:
            os.killpg(pid, signal.SIGTERM)
            logger.info(f"local: cancelled job {job_id} (pgid {pid})")
        except (OSError, ProcessLookupError):
            pass  # already gone

    async def get_cluster_info(self, user: str | None = None) -> ClusterInfo | None:
        total = os.cpu_count() or 1
        busy = 0
        for meta_path in self._spool.glob("*.json"):
            job_id = meta_path.stem
            if self._rc_path(job_id).exists():
                continue
            meta = self._read_meta(job_id)
            if meta and self._pid_alive(meta.get("pid")):
                busy += int(meta.get("cpus") or 1)
        idle = max(0, total - busy)
        return ClusterInfo(
            partitions=[PartitionInfo(
                name="local",
                state="up",
                cpus_allocated=min(busy, total),
                cpus_idle=idle,
                cpus_other=0,
                cpus_total=total,
                nodes_total=1,
                is_default=True,
                cpus_free_max_node=idle,
            )],
            pending_reasons={},
        )

    async def get_disk_info(self, path: str) -> DiskInfo | None:
        target = Path(path).expanduser()
        while not target.exists() and target != target.parent:
            target = target.parent
        try:
            usage = shutil.disk_usage(target)
        except OSError:
            return None
        return DiskInfo(
            total_bytes=usage.total, avail_bytes=usage.free, path=path,
        )

    async def fetch_log(
        self,
        job_id: str,
        log_path: str,
        log_type: str = "output",
        tail_lines: int | None = None,
    ) -> tuple[str | None, str | None]:
        path = Path(log_path).expanduser()
        try:
            content = path.read_text(errors="replace")
        except FileNotFoundError:
            return None, f"Log file not found: {log_path}"
        except OSError as e:
            return None, f"Could not read {log_path}: {e}"
        if tail_lines is not None and tail_lines > 0:
            content = "\n".join(content.splitlines()[-tail_lines:])
        return content, None

    def generate_script(
        self,
        task: "TaskDefinition",
        run_id: str,
        log_dir: str,
        account: str | None = None,
        login_shell: bool = False,
        env_vars: dict[str, str] | None = None,
        extra_init: str = "",
        interactive_wait: bool = False,
    ) -> str:
        """Plain bash script; log routing via ``exec`` instead of scheduler flags."""
        output_path = task.get_output_path(run_id, log_dir)
        error_path = task.get_error_path(run_id, log_dir)
        output_dir = task.get_output_dir(run_id, log_dir)
        run_summary_path = task.get_run_summary_path(run_id, log_dir)
        shebang = "#!/bin/bash -l" if login_shell else "#!/bin/bash"
        header = (
            f"{shebang}\n"
            f"# ScriptHut local task: {task.name} ({task.id})\n"
            f'mkdir -p "$(dirname "{output_path}")" "$(dirname "{error_path}")"\n'
            f'exec > "{output_path}" 2> "{error_path}"\n'
        )
        body = generate_script_body(
            task_name=task.name,
            task_id=task.id,
            command=task.command,
            working_dir=task.working_dir,
            env_vars=env_vars,
            extra_init=extra_init,
            # Interactive attach isn't supported for local jobs.
            interactive_wait=False,
            output_dir=output_dir,
            run_summary_path=run_summary_path,
        )
        return header + "\n" + body

    @property
    def failure_states(self) -> dict[str, str]:
        return LOCAL_FAILURE_STATES

    @property
    def terminal_states(self) -> frozenset[str]:
        return LOCAL_TERMINAL_STATES
