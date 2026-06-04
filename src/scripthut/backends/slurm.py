"""Slurm job backend implementation."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from scripthut.backends.base import (
    ClusterInfo,
    DiskInfo,
    JobBackend,
    JobStats,
    PartitionInfo,
    QuotaInfo,
    SubmitResult,
)
from scripthut.backends.utils import (
    fetch_disk_info,
    fetch_log_via_ssh,
    format_bytes,
    format_submit_output,
    generate_script_body,
    parse_duration_hms,
    parse_rss_to_bytes,
)
from scripthut.models import JobState, SlurmJob
from scripthut.ssh.client import SSHClient

if TYPE_CHECKING:
    from scripthut.runs.models import TaskDefinition

logger = logging.getLogger(__name__)


# Sacct states that indicate a Slurm-killed job
SLURM_FAILURE_STATES: dict[str, str] = {
    "FAILED": "Non-zero exit code",
    "TIMEOUT": "Exceeded walltime",
    "OUT_OF_MEMORY": "Out of memory (OOM killed)",
    "CANCELLED": "Cancelled",
    "NODE_FAIL": "Node failure",
    "PREEMPTED": "Preempted",
}

# All terminal sacct states (both success and failure)
SLURM_TERMINAL_STATES = frozenset({
    "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT",
    "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED",
    "DEADLINE", "BOOT_FAIL",
})


# Re-export for backward compatibility
parse_slurm_duration = parse_duration_hms


def _slurm_exitcode_indicates_failure(field: str) -> bool:
    """True when an ``ExitCode`` field reports a non-zero exit or signal.

    sacct's ``ExitCode`` column is formatted ``<exit>:<signal>`` (e.g.
    ``1:0`` for ``exit 1`` with no signal, ``0:15`` for SIGTERM with no
    explicit exit). Either non-zero half means the job didn't end
    cleanly. Empty / unparseable fields return ``False`` so we never
    flip COMPLETED → FAILED on a parse glitch — the existing State
    logic still owns the default verdict.
    """
    field = (field or "").strip()
    if ":" not in field:
        # Anything not in the documented ``exit:signal`` shape is treated as
        # unparseable. Better to leave the State-based verdict in place than
        # to invent a failure from a column scripthut doesn't recognize.
        return False
    parts = field.split(":")
    for p in parts:
        try:
            if int(p.strip()) != 0:
                return True
        except ValueError:
            return False
    return False


def _slurm_parse_exit_int(field: str) -> int | None:
    """Return the numeric exit code from a sacct ``ExitCode`` field.

    ``<exit>:<signal>`` is the documented format. We return the exit
    half as an int; signal-only failures (exit=0, signal!=0) surface as
    0 here, but the boolean ``_slurm_exitcode_indicates_failure`` will
    still flip the state to FAILED — consumers reading the numeric
    code should cross-check with status when interpreting a 0.
    Returns ``None`` for empty / non-``<n>:<n>`` shaped fields so the
    caller doesn't invent a value from a parse miss.
    """
    field = (field or "").strip()
    if ":" not in field:
        return None
    head = field.split(":", 1)[0].strip()
    try:
        return int(head)
    except ValueError:
        return None


def _safe_float(s: str) -> float | None:
    """Parse a Slurm numeric field; return None for empty / N/A / unparseable."""
    s = s.strip()
    if not s or s in ("N/A", "(null)"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _safe_int(s: str) -> int | None:
    s = s.strip()
    if not s or s in ("N/A", "(null)"):
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _min_int(a: int | None, b: int | None) -> int | None:
    """Lower bound of two optional ints; ``None`` means "no limit"."""
    if a is None:
        return b
    if b is None:
        return a
    return min(a, b)


def _tres_get(tres: str, key: str) -> int | None:
    """Look up a numeric value in a TRES string like ``cpu=128,gres/gpu=4``.

    Slurm uses ``cpu=…`` for CPUs and ``gres/gpu=…`` for GPUs. Returns
    ``None`` if the key is absent or the value isn't an integer (e.g.
    memory limits use ``M``/``G`` suffixes — we deliberately skip those).
    """
    if not tres or tres in ("N/A", "(null)"):
        return None
    for entry in tres.split(","):
        k, _, v = entry.strip().partition("=")
        if k.strip() == key:
            try:
                return int(v.strip())
            except ValueError:
                return None
    return None


def _parse_gres_gpu(gres_str: str) -> tuple[int, set[str]]:
    """Sum GPU counts from a Slurm Gres or GresUsed string.

    Accepts forms like::

        gpu:a100:8                  -> (8, {"a100"})
        gpu:8                       -> (8, set())
        gpu:a100:8(IDX:0-7),nvme:1  -> (8, {"a100"})
        gpu:a100:4,gpu:v100:2       -> (6, {"a100", "v100"})
        (null)                      -> (0, set())

    Non-GPU entries (``nvme``, ``mps``, …) are ignored. The trailing
    ``(IDX:…)`` allocation-detail suffix used by ``GresUsed`` is stripped.
    """
    if not gres_str or gres_str in ("(null)", "N/A"):
        return 0, set()
    total = 0
    types: set[str] = set()
    for raw in gres_str.split(","):
        entry = raw.split("(", 1)[0].strip()
        if not entry:
            continue
        parts = entry.split(":")
        if not parts or parts[0].lower() != "gpu":
            continue
        # Walk from the right: first integer-looking part is the count;
        # whatever precedes it (between "gpu" and the count) is the type.
        count: int | None = None
        for i in range(len(parts) - 1, 0, -1):
            try:
                count = int(parts[i])
                if i > 1:
                    types.add(parts[i - 1])
                break
            except ValueError:
                continue
        if count is not None:
            total += count
    return total, types


# squeue format string for extended job info
# Fields: JobID, Name, User, State, Partition, TimeUsed, NodeList, NumCPUs, MinMemory, SubmitTime, StartTime
SQUEUE_FORMAT = "%i|%j|%u|%T|%P|%M|%N|%C|%m|%V|%S"

# sbatch typically writes: "Submitted batch job 12345"
# Pull the numeric ID out of any line that matches this; tolerates extra
# warnings/info lines that some sbatch wrappers append before/after.
_SBATCH_JOB_ID_RE = re.compile(r"Submitted batch job\s+(\d+)")


def parse_slurm_datetime(dt_str: str) -> datetime | None:
    """Parse Slurm datetime string to Python datetime."""
    if not dt_str or dt_str in ("N/A", "Unknown", "None"):
        return None
    try:
        # Slurm uses ISO format: YYYY-MM-DDTHH:MM:SS
        dt = datetime.fromisoformat(dt_str.replace("T", " ").split(".")[0])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        logger.warning(f"Failed to parse datetime: {dt_str}")
        return None


def parse_squeue_line(line: str) -> SlurmJob | None:
    """Parse a single line of squeue output into a SlurmJob."""
    parts = line.strip().split("|")
    if len(parts) < 11:
        logger.warning(f"Invalid squeue line (expected 11 fields): {line}")
        return None

    try:
        return SlurmJob(
            job_id=parts[0],
            name=parts[1],
            user=parts[2],
            state=JobState.from_string(parts[3]),
            partition=parts[4],
            time_used=parts[5] if parts[5] else "0:00",
            nodes=parts[6] if parts[6] else "-",
            cpus=int(parts[7]) if parts[7] else 0,
            memory=parts[8] if parts[8] else "-",
            submit_time=parse_slurm_datetime(parts[9]),
            start_time=parse_slurm_datetime(parts[10]),
        )
    except (ValueError, IndexError) as e:
        logger.warning(f"Failed to parse squeue line: {line}, error: {e}")
        return None


class SlurmBackend(JobBackend):
    """Slurm job backend using SSH to run squeue."""

    def __init__(
        self,
        ssh_client: SSHClient,
        account: str | None = None,
        partition_map: dict[str, str] | None = None,
        default_partition: str | None = None,
    ) -> None:
        """Initialize with an SSH client connected to the Slurm head node.

        ``account`` is the Slurm account used at submission time
        (``sbatch --account=…``). When set, quota queries filter to this
        account so the dashboard reflects the user's actual charge group
        — not whichever ``DefaultAccount`` the site happens to set.

        ``partition_map`` rewrites a task's logical partition name to
        this cluster's actual partition (e.g. ``{"standard": "cpu"}``).
        ``default_partition`` is the fallback when the task's partition
        isn't in the map. With neither set, the task's value passes
        through unchanged.
        """
        self._ssh = ssh_client
        self._account = account
        self._partition_map = partition_map or {}
        self._default_partition = default_partition

    def _resolve_partition(self, task_partition: str) -> str:
        """Translate a task's logical partition to this cluster's name."""
        if task_partition in self._partition_map:
            return self._partition_map[task_partition]
        if self._default_partition:
            return self._default_partition
        return task_partition

    @property
    def name(self) -> str:
        return "slurm"

    async def get_jobs(self, user: str | None = None) -> list[SlurmJob]:
        """
        Fetch current jobs from Slurm using squeue.

        Args:
            user: Optional filter by username. None means all users.

        Returns:
            List of SlurmJob objects.
        """
        # Build squeue command
        cmd = f"squeue --noheader --format='{SQUEUE_FORMAT}'"
        if user:
            cmd += f" --user={user}"

        stdout, stderr, exit_code = await self._ssh.run_command(cmd)

        if exit_code != 0:
            logger.error(f"squeue failed (exit {exit_code}): {stderr}")
            return []

        jobs: list[SlurmJob] = []
        for line in stdout.strip().split("\n"):
            if not line.strip():
                continue
            job = parse_squeue_line(line)
            if job:
                jobs.append(job)

        logger.debug(f"Fetched {len(jobs)} jobs from Slurm")
        return jobs

    async def _get_known_sacct_ids(
        self, user: str | None = None,
    ) -> set[str] | None:
        """Return the set of job IDs that sacct knows about for *user*.

        Runs a lightweight ``sacct --format=JobIDRaw`` scoped to the user
        (last 60 days).  Returns ``None`` on failure so callers can fall back
        to optimistic behaviour.
        """
        cmd = "sacct --noheader --parsable2 --format=JobIDRaw --starttime=now-60days"
        if user:
            cmd += f" --user={user}"
        try:
            stdout, _, exit_code = await self._ssh.run_command(cmd, timeout=30)
        except Exception as e:
            logger.warning(f"sacct ID pre-check failed: {e}")
            return None

        if exit_code != 0 or not stdout.strip():
            return None

        known: set[str] = set()
        for line in stdout.strip().split("\n"):
            raw_id = line.strip().split("|")[0]
            base_id = raw_id.split(".")[0] if "." in raw_id else raw_id
            if base_id:
                known.add(base_id)
        return known

    async def get_job_stats(
        self,
        job_ids: list[str],
        user: str | None = None,
    ) -> dict[str, JobStats]:
        """Fetch resource utilization stats for completed jobs using sacct.

        Post-mortem analysis only using sacct accounting data.

        Args:
            job_ids: List of Slurm job IDs to query via sacct.
            user: Optional username to scope the pre-filter query.

        Returns:
            Dict mapping job_id to JobStats.  IDs not yet in sacct are
            omitted so callers retry on the next poll cycle.
        """
        if not job_ids:
            return {}

        # Pre-filter: ask sacct which IDs it still knows about so we never
        # pass invalid/expired IDs to the main query (which would make it
        # fail entirely).
        stats: dict[str, JobStats] = {}
        known_ids = await self._get_known_sacct_ids(user=user)
        if known_ids is not None:
            valid_ids = [jid for jid in job_ids if jid in known_ids]
            stale_ids = [jid for jid in job_ids if jid not in known_ids]
            if stale_ids:
                logger.info(
                    f"sacct pre-check: {len(stale_ids)} IDs not yet in accounting "
                    f"DB, will retry: {stale_ids[:10]}"
                )
        else:
            # Pre-check failed — fall back to querying all IDs.
            valid_ids = list(job_ids)

        if not valid_ids:
            return stats

        # --- sacct for CPU efficiency + post-completion memory + timing ---
        # ExitCode is included so we can override `State=COMPLETED` when
        # the script actually exited non-zero — happens with masking
        # bash patterns the framework can't always defend against
        # (e.g. trailing statement after the failing command).
        ids_str = ",".join(valid_ids)
        cmd = (
            f"sacct --noheader --parsable2"
            f" --format=JobIDRaw,TotalCPU,Elapsed,AllocCPUS,MaxRSS,"
            f"Start,End,State,ExitCode"
            f" --jobs={ids_str}"
        )

        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=30)
        except Exception as e:
            logger.warning(f"sacct command failed: {e}")
            return stats  # still return stale markers

        if exit_code != 0:
            logger.warning(f"sacct failed (exit {exit_code}): {stderr}")
            return stats

        logger.debug(f"sacct raw output ({len(stdout)} chars): {stdout[:500]}")

        # Parse sacct output. Each job produces multiple lines (main, .batch, .extern).
        # Collect Elapsed/AllocCPUS/TotalCPU from main entry, TotalCPU from .batch,
        # and MaxRSS from ALL steps (taking the maximum).
        main_data: dict[str, tuple[float, int, float]] = {}  # job_id -> (elapsed_s, alloc_cpus, total_cpu_s)
        batch_cpu: dict[str, float] = {}  # job_id -> total_cpu_s from .batch
        max_rss_bytes: dict[str, int] = {}  # job_id -> best MaxRSS in bytes across all steps
        sacct_start: dict[str, datetime | None] = {}  # job_id -> actual start time
        sacct_end: dict[str, datetime | None] = {}  # job_id -> actual end time
        sacct_state: dict[str, str] = {}  # job_id -> State from main entry
        # Numeric exit code, parsed from "exit:signal". .batch (where
        # the user's script ran) wins on conflict; main entry is the
        # fallback for jobs without a .batch step.
        sacct_exit_code: dict[str, int] = {}

        for line in stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = line.split("|")
            if len(parts) < 9:
                continue

            (raw_id, total_cpu, elapsed, alloc_cpus_str, max_rss,
             start_str, end_str, job_state, exit_code_str) = parts[:9]

            # Extract base job ID (strip .batch, .extern, .0, etc.)
            base_id = raw_id.split(".")[0] if "." in raw_id else raw_id

            # Track MaxRSS from every step, keep the maximum
            rss_b = parse_rss_to_bytes(max_rss)
            if rss_b > max_rss_bytes.get(base_id, 0):
                max_rss_bytes[base_id] = rss_b

            # ExitCode format is "<exit>:<signal>" — either non-zero means
            # the script didn't end cleanly. Used both to override
            # COMPLETED (boolean) and to populate `JobStats.exit_code`
            # (numeric, so consumers can read the actual code).
            exit_nonzero = _slurm_exitcode_indicates_failure(exit_code_str)
            exit_int = _slurm_parse_exit_int(exit_code_str)

            if ".batch" in raw_id:
                batch_cpu[base_id] = parse_slurm_duration(total_cpu)
                # .batch is where user code actually runs — if it failed
                # (e.g. OOM), override the main entry's state which may
                # misleadingly say COMPLETED.
                batch_state = job_state.split()[0] if job_state else ""
                if batch_state and batch_state in SLURM_FAILURE_STATES:
                    sacct_state[base_id] = batch_state
                elif exit_nonzero:
                    # State says COMPLETED but the script returned non-zero —
                    # most often a masked failure (`solve | tee log` without
                    # pipefail, trailing statement after the failing command).
                    # Treat as FAILED so the user sees the truth.
                    sacct_state[base_id] = "FAILED"
                # .batch exit code wins over the main entry's because
                # this is where user code ran.
                if exit_int is not None:
                    sacct_exit_code[base_id] = exit_int
            elif "." not in raw_id:
                # Main entry — has aggregate TotalCPU (used as fallback when .batch is 0)
                elapsed_s = parse_slurm_duration(elapsed)
                main_cpu_s = parse_slurm_duration(total_cpu)
                try:
                    alloc_cpus = int(alloc_cpus_str) if alloc_cpus_str else 1
                except ValueError:
                    alloc_cpus = 1
                main_data[raw_id] = (elapsed_s, alloc_cpus, main_cpu_s)
                # Strip trailing modifiers like "CANCELLED by 12345"
                main_state = job_state.split()[0] if job_state else ""
                # Only set if .batch hasn't already overridden with a failure
                if raw_id not in sacct_state:
                    sacct_state[raw_id] = main_state
                # Defense in depth: even if .batch was missing (e.g. interactive
                # job, srun-only step layout), an ExitCode != 0:0 on the main
                # entry still flips us to FAILED.
                if exit_nonzero and sacct_state.get(raw_id) == "COMPLETED":
                    sacct_state[raw_id] = "FAILED"
                # Main-entry exit code is a fallback for jobs without a
                # .batch row.
                if exit_int is not None and raw_id not in sacct_exit_code:
                    sacct_exit_code[raw_id] = exit_int

                # Parse actual start/end timestamps from sacct
                try:
                    if start_str and start_str not in ("Unknown", "None", "N/A", ""):
                        sacct_start[raw_id] = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                except ValueError:
                    pass
                try:
                    if end_str and end_str not in ("Unknown", "None", "N/A", ""):
                        sacct_end[raw_id] = datetime.strptime(end_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

        # --- Compute final stats ---
        for job_id in valid_ids:
            if job_id not in main_data:
                continue

            elapsed_s, alloc_cpus, main_cpu_s = main_data[job_id]
            # Prefer .batch TotalCPU, fall back to main entry's aggregate TotalCPU
            total_cpu_s = batch_cpu.get(job_id, 0.0) or main_cpu_s

            # CPU efficiency: total_cpu / (elapsed * alloc_cpus) * 100
            denominator = elapsed_s * max(alloc_cpus, 1)
            efficiency = (total_cpu_s / denominator * 100) if denominator > 0 else 0.0

            rss_formatted = format_bytes(max_rss_bytes.get(job_id, 0))

            stats[job_id] = JobStats(
                cpu_efficiency=round(efficiency, 1),
                max_rss=rss_formatted,
                total_cpu=f"{total_cpu_s:.0f}s",
                start_time=sacct_start.get(job_id),
                end_time=sacct_end.get(job_id),
                state=sacct_state.get(job_id),
                exit_code=sacct_exit_code.get(job_id),
            )

        logger.debug(f"Fetched stats for {len(stats)}/{len(job_ids)} jobs via sacct")
        return stats

    async def get_cluster_info(self, user: str | None = None) -> ClusterInfo | None:
        """Fetch per-partition availability, pending reasons, and optional quota.

        Runs ``sinfo`` (partition-level), ``sinfo --Node`` (Gres for GPU
        counts), ``squeue`` (pending reasons), and, when ``user`` is set,
        ``sshare`` + ``sacctmgr`` for fair-share and scheduling limits.
        Each query is best-effort: a failure of one does not block the
        others.
        """
        partitions = await self._fetch_partitions()
        if partitions is None:
            return None
        gpu_by_part = await self._fetch_gpu_info()
        for p in partitions:
            gpu = gpu_by_part.get(p.name)
            if gpu is not None:
                p.gpus_total, p.gpus_idle, p.gpu_types = gpu
        pending_reasons = await self._fetch_pending_reasons()
        user_quota = await self._fetch_user_quota(user) if user else None
        return ClusterInfo(
            partitions=partitions,
            pending_reasons=pending_reasons,
            user_quota=user_quota,
        )

    async def _fetch_partitions(self) -> list[PartitionInfo] | None:
        # Aggregate one row per partition with %P (default marked with *).
        # Fields: PartitionName | Avail | A/I/O/T cpus | total nodes | mem/node | timelimit | features
        fmt = "%P|%a|%C|%D|%m|%l|%f"
        cmd = f"sinfo --noheader --summarize --format='{fmt}'"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"sinfo failed: {e}")
            return None
        if exit_code != 0:
            logger.warning(f"sinfo failed (exit {exit_code}): {stderr}")
            return None

        partitions: list[PartitionInfo] = []
        for raw in stdout.strip().splitlines():
            line = raw.strip().strip("'")
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 7:
                logger.warning(f"Unexpected sinfo row: {line!r}")
                continue
            name_raw, avail, cpus_aiot, nodes, mem, timelimit, features = parts[:7]

            is_default = name_raw.endswith("*")
            name = name_raw.rstrip("*")

            cpu_parts = cpus_aiot.split("/")
            if len(cpu_parts) != 4:
                logger.warning(f"Unexpected sinfo CPUs field: {cpus_aiot!r}")
                continue
            try:
                alloc, idle, other, total = (int(x) for x in cpu_parts)
            except ValueError:
                logger.warning(f"Failed to parse CPUs: {cpus_aiot!r}")
                continue

            try:
                nodes_total = int(nodes)
            except ValueError:
                nodes_total = 0

            try:
                mem_mb: int | None = int(mem) if mem and mem != "N/A" else None
            except ValueError:
                mem_mb = None

            partitions.append(PartitionInfo(
                name=name,
                state=avail.lower(),
                cpus_allocated=alloc,
                cpus_idle=idle,
                cpus_other=other,
                cpus_total=total,
                nodes_total=nodes_total,
                timelimit=timelimit or None,
                mem_per_node_mb=mem_mb,
                features=features or None,
                is_default=is_default,
            ))

        return partitions

    async def _fetch_gpu_info(self) -> dict[str, tuple[int, int, str | None]]:
        """Return per-partition ``(gpus_total, gpus_idle, types)``.

        Walks ``sinfo --Node`` (one row per node) and aggregates GPU
        counts from each node's Gres / GresUsed strings. Nodes in
        down/drain/reserved states count toward total but not idle.

        Uses ``-O`` with explicit field widths so we can split by column
        position (sinfo doesn't accept a custom delimiter in long-Format
        mode). Wide enough to hold any realistic Gres string.
        """
        fmt = "Partition:32,StateLong:16,Gres:256,GresUsed:256"
        cmd = f"sinfo --noheader --Node --Format='{fmt}'"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"sinfo -N (gpu) failed: {e}")
            return {}
        if exit_code != 0:
            logger.warning(f"sinfo -N (gpu) failed (exit {exit_code}): {stderr}")
            return {}

        # Slice indices match the widths declared above.
        P_END, S_END, G_END, U_END = 32, 48, 304, 560

        # State strings considered "schedulable now" (GPUs on these nodes
        # contribute to the idle pool). Anything else is counted toward
        # total only.
        schedulable = {"idle", "mix", "mixed", "allocated", "alloc"}

        totals: dict[str, int] = {}
        idle_counts: dict[str, int] = {}
        types_by_part: dict[str, set[str]] = {}

        for raw in stdout.splitlines():
            line = raw.ljust(U_END)
            partition = line[0:P_END].strip()
            state = line[P_END:S_END].strip().lower()
            gres = line[S_END:G_END].strip()
            gres_used = line[G_END:U_END].strip()
            if not partition:
                continue

            total, types = _parse_gres_gpu(gres)
            used, _ = _parse_gres_gpu(gres_used)
            if total == 0:
                continue

            totals[partition] = totals.get(partition, 0) + total
            if state in schedulable:
                idle_counts[partition] = idle_counts.get(partition, 0) + max(0, total - used)
            else:
                idle_counts.setdefault(partition, 0)
            types_by_part.setdefault(partition, set()).update(types)

        result: dict[str, tuple[int, int, str | None]] = {}
        for name, total in totals.items():
            type_set = types_by_part.get(name, set())
            label = ",".join(sorted(t for t in type_set if t)) or None
            result[name] = (total, idle_counts.get(name, 0), label)
        return result

    async def _fetch_user_quota(self, user: str) -> QuotaInfo | None:
        """Combine ``sshare`` (fair-share), ``sacctmgr`` (limits), and
        ``squeue`` AllocTRES (current usage) for a user.

        Each query is independent — if one fails we still surface what
        the others returned. Returns ``None`` only if all three fail.
        """
        share = await self._fetch_sshare(user)
        limits = await self._fetch_sacctmgr_limits(user)
        usage = await self._fetch_running_tres(user)
        if share is None and limits is None and usage is None:
            return None
        q = share or QuotaInfo()
        if limits is not None:
            if limits.account and not q.account:
                q.account = limits.account
            if limits.jobs_max is not None:
                q.jobs_max = limits.jobs_max
            if limits.cpus_max is not None:
                q.cpus_max = limits.cpus_max
            if limits.gpus_max is not None:
                q.gpus_max = limits.gpus_max
        if usage is not None:
            q.jobs_used = usage.jobs_used
            q.cpus_used = usage.cpus_used
            q.gpus_used = usage.gpus_used
        return q

    async def _fetch_running_tres(self, user: str) -> QuotaInfo | None:
        """Tally CPU/GPU/job counts across the user's running jobs via squeue.

        Uses ``-O AllocTRES`` which returns lines like
        ``cpu=4,mem=4G,node=1,billing=4,gres/gpu=2`` per running job. When
        an account is configured, restrict to jobs charging that account
        so the totals line up with the configured account's limits.
        """
        cmd = f"squeue -h -t R -u {user} -O 'AllocTRES:256'"
        if self._account:
            cmd += f" --account={self._account}"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"squeue AllocTRES failed: {e}")
            return None
        if exit_code != 0:
            logger.warning(f"squeue AllocTRES failed (exit {exit_code}): {stderr}")
            return None

        jobs = 0
        cpus = 0
        gpus = 0
        for raw in stdout.splitlines():
            tres = raw.strip()
            if not tres:
                continue
            jobs += 1
            cpus += _tres_get(tres, "cpu") or 0
            gpus += _tres_get(tres, "gres/gpu") or 0
        return QuotaInfo(jobs_used=jobs, cpus_used=cpus, gpus_used=gpus)

    async def _fetch_sshare(self, user: str) -> QuotaInfo | None:
        """Run ``sshare`` for the given user; return fair-share + account.

        If the backend was configured with a specific ``account``, filter
        to that association (``-A account``) so a user with multiple
        accounts gets the row that matches what their jobs actually charge
        — not whichever ``DefaultAccount`` the site happens to set first.
        """
        cmd = (
            f"sshare -h --parsable2 -U -u {user} "
            f"--format=Account,User,FairShare,EffectvUsage"
        )
        if self._account:
            cmd += f" -A {self._account}"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"sshare failed: {e}")
            return None
        if exit_code != 0:
            logger.warning(f"sshare failed (exit {exit_code}): {stderr}")
            return None

        # Prefer the row whose Account matches self._account; fall back to
        # the first parseable row (handles older Slurm builds that ignore
        # -A or aliases like "pi-kilianhuber" vs "pi_kilianhuber").
        first: QuotaInfo | None = None
        for raw in stdout.splitlines():
            line = raw.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 4:
                continue
            account, _u, fair_share, eff_usage = parts[0], parts[1], parts[2], parts[3]
            entry = QuotaInfo(
                account=account or None,
                fair_share=_safe_float(fair_share),
                norm_usage=_safe_float(eff_usage),
            )
            if self._account and account == self._account:
                return entry
            if first is None:
                first = entry
        return first

    async def _fetch_sacctmgr_limits(self, user: str) -> QuotaInfo | None:
        """Run ``sacctmgr show user withassoc`` and parse limits.

        If a specific account is configured, restrict rows to it (both at
        the query level via ``account=…`` and as a client-side filter for
        older sacctmgr builds that ignore the query filter). Otherwise
        all rows are walked.
        """
        cmd = (
            f"sacctmgr -n -P show user {user} withassoc "
            f"format=User,Account,Partition,GrpJobs,GrpTRES,MaxJobs,MaxTRES"
        )
        if self._account:
            cmd += f" account={self._account}"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"sacctmgr failed: {e}")
            return None
        if exit_code != 0:
            logger.warning(f"sacctmgr failed (exit {exit_code}): {stderr}")
            return None

        # Walk rows; if an account is configured, only aggregate rows that
        # match it. Take the most-restrictive Grp/Max for each resource.
        best = QuotaInfo()
        for raw in stdout.splitlines():
            line = raw.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 7:
                continue
            _user, account, _part, grp_jobs, grp_tres, max_jobs, max_tres = parts[:7]
            if self._account and account != self._account:
                continue
            if best.account is None and account:
                best.account = account

            jobs_cap = _min_int(_safe_int(grp_jobs), _safe_int(max_jobs))
            cpus_cap = _min_int(_tres_get(grp_tres, "cpu"), _tres_get(max_tres, "cpu"))
            gpus_cap = _min_int(
                _tres_get(grp_tres, "gres/gpu"), _tres_get(max_tres, "gres/gpu")
            )

            best.jobs_max = _min_int(best.jobs_max, jobs_cap)
            best.cpus_max = _min_int(best.cpus_max, cpus_cap)
            best.gpus_max = _min_int(best.gpus_max, gpus_cap)

        if (
            best.account is None
            and best.jobs_max is None
            and best.cpus_max is None
            and best.gpus_max is None
        ):
            return None
        # Always carry the configured account name even if it wasn't on a row
        # (clearer for the dashboard than a blank field).
        if self._account and best.account is None:
            best.account = self._account
        return best

    async def _fetch_pending_reasons(self) -> dict[str, int]:
        """Tally ``squeue -t PD`` pending jobs by reason."""
        cmd = "squeue --noheader -t PD --format='%R'"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"squeue PD failed: {e}")
            return {}
        if exit_code != 0:
            logger.warning(f"squeue PD failed (exit {exit_code}): {stderr}")
            return {}

        reasons: dict[str, int] = {}
        for raw in stdout.splitlines():
            reason = raw.strip().strip("'").strip("()")
            if not reason:
                continue
            reasons[reason] = reasons.get(reason, 0) + 1
        return reasons

    async def get_disk_info(self, path: str) -> DiskInfo | None:
        """Fetch disk usage for ``path`` on the backend via ``df -Pk``."""
        return await fetch_disk_info(self._ssh, path)

    async def fetch_log(
        self,
        job_id: str,
        log_path: str,
        log_type: str = "output",
        tail_lines: int | None = None,
    ) -> tuple[str | None, str | None]:
        return await fetch_log_via_ssh(self._ssh, log_path, tail_lines)

    async def is_available(self) -> bool:
        """Check if Slurm is available by running squeue --version."""
        try:
            stdout, _, exit_code = await self._ssh.run_command("squeue --version")
            return exit_code == 0 and "slurm" in stdout.lower()
        except Exception as e:
            logger.warning(f"Slurm availability check failed: {e}")
            return False

    async def submit_job(self, script: str) -> str:
        """Submit a job script via sbatch and return the parsed job ID.

        Verifies the ID is recognized by Slurm (squeue or sacct) before
        returning so silent acceptance failures surface immediately rather
        than getting stuck on SUBMITTED until the next poll cycle.
        """
        result = await self._submit_and_verify(script)
        return result.job_id

    async def _submit_and_verify(self, script: str) -> SubmitResult:
        """Run sbatch, parse the job ID strictly, and verify it entered the queue.

        On any failure, raises ``RuntimeError`` with the captured sbatch
        stdout/stderr so the user can see exactly what came back.
        """
        escaped_script = script.replace("'", "'\\''")
        submit_cmd = f"sbatch <<'SCRIPTHUT_EOF'\n{escaped_script}\nSCRIPTHUT_EOF"
        stdout, stderr, exit_code = await self._ssh.run_command(submit_cmd)
        combined = format_submit_output(stdout, stderr)

        if exit_code != 0:
            raise RuntimeError(f"sbatch failed (exit {exit_code}): {combined}")

        match = _SBATCH_JOB_ID_RE.search(stdout) or _SBATCH_JOB_ID_RE.search(stderr)
        if not match:
            raise RuntimeError(
                f"Could not parse job ID from sbatch output: {combined}"
            )
        job_id = match.group(1)

        # Verify Slurm actually accepted the job. squeue covers
        # pending/running jobs; sacct catches very-fast jobs that already
        # finished or were rejected and recorded by the accounting DB.
        verified = await self._verify_job_accepted(job_id)
        if not verified:
            raise RuntimeError(
                f"sbatch returned job ID {job_id} but it does not appear in "
                f"squeue or sacct — the job likely never entered the queue. "
                f"sbatch output: {combined}"
            )

        return SubmitResult(job_id=job_id, submit_output=combined)

    async def _verify_job_accepted(self, job_id: str) -> bool:
        """Return True if Slurm recognizes ``job_id`` in squeue or sacct."""
        try:
            stdout, _, exit_code = await self._ssh.run_command(
                f"squeue --noheader --jobs={job_id} --format=%i", timeout=15
            )
        except Exception as e:
            logger.warning(f"squeue verify failed for {job_id}: {e}")
            stdout, exit_code = "", 1

        if exit_code == 0 and stdout.strip():
            return True

        try:
            stdout, _, exit_code = await self._ssh.run_command(
                f"sacct --noheader --parsable2 --format=JobIDRaw --jobs={job_id}",
                timeout=15,
            )
        except Exception as e:
            logger.warning(f"sacct verify failed for {job_id}: {e}")
            return False

        return exit_code == 0 and bool(stdout.strip())

    async def submit_task(
        self,
        task: "TaskDefinition",
        script: str,
        env_vars: dict[str, str] | None = None,
    ) -> SubmitResult:
        """Submit a Slurm task, returning the captured sbatch output.

        ``env_vars`` are already inlined into the script body via
        :func:`generate_script_body`, so they're not threaded through here.
        """
        del task, env_vars  # script carries everything Slurm needs
        return await self._submit_and_verify(script)

    async def cancel_job(self, job_id: str) -> None:
        """Cancel a Slurm job via scancel."""
        await self._ssh.run_command(f"scancel {job_id}")

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
        """Generate an sbatch submission script for a task."""
        output_path = task.get_output_path(run_id, log_dir)
        error_path = task.get_error_path(run_id, log_dir)
        # v0.11.0 task-outputs feature: wire the contract paths into
        # the script wrapper so user commands can write structured
        # outputs without further configuration.
        output_dir = task.get_output_dir(run_id, log_dir)
        run_summary_path = task.get_run_summary_path(run_id, log_dir)
        shebang = "#!/bin/bash -l" if login_shell else "#!/bin/bash"
        account_line = f"#SBATCH --account={account}\n" if account else ""
        gres_line = f"#SBATCH --gres={task.gres}\n" if task.gres else ""
        partition = self._resolve_partition(task.partition)

        header = f"""{shebang}
#SBATCH --job-name="{task.name}"
#SBATCH --partition={partition}
{account_line}#SBATCH --cpus-per-task={task.cpus}
#SBATCH --mem={task.memory}
#SBATCH --time={task.time_limit}
{gres_line}#SBATCH --output={output_path}
#SBATCH --error={error_path}
"""
        body = generate_script_body(
            task_name=task.name,
            task_id=task.id,
            command=task.command,
            working_dir=task.working_dir,
            env_vars=env_vars,
            extra_init=extra_init,
            interactive_wait=interactive_wait,
            output_dir=output_dir,
            run_summary_path=run_summary_path,
        )
        return header + "\n" + body

    @property
    def failure_states(self) -> dict[str, str]:
        """Slurm failure states from sacct."""
        return SLURM_FAILURE_STATES

    @property
    def terminal_states(self) -> frozenset[str]:
        """All terminal sacct states."""
        return SLURM_TERMINAL_STATES
