"""Slurm job backend implementation."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from scripthut.backends.base import DiskInfo, JobBackend, JobStats
from scripthut.backends.utils import (
    fetch_disk_info,
    fetch_log_via_ssh,
    format_bytes,
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


# squeue format string for extended job info
# Fields: JobID, Name, User, State, Partition, TimeUsed, NodeList, NumCPUs, MinMemory, SubmitTime, StartTime
SQUEUE_FORMAT = "%i|%j|%u|%T|%P|%M|%N|%C|%m|%V|%S"


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

    def __init__(self, ssh_client: SSHClient) -> None:
        """Initialize with an SSH client connected to the Slurm head node."""
        self._ssh = ssh_client

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
        ids_str = ",".join(valid_ids)
        cmd = (
            f"sacct --noheader --parsable2"
            f" --format=JobIDRaw,TotalCPU,Elapsed,AllocCPUS,MaxRSS,Start,End,State"
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

        for line in stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = line.split("|")
            if len(parts) < 8:
                continue

            raw_id, total_cpu, elapsed, alloc_cpus_str, max_rss, start_str, end_str, job_state = parts[:8]

            # Extract base job ID (strip .batch, .extern, .0, etc.)
            base_id = raw_id.split(".")[0] if "." in raw_id else raw_id

            # Track MaxRSS from every step, keep the maximum
            rss_b = parse_rss_to_bytes(max_rss)
            if rss_b > max_rss_bytes.get(base_id, 0):
                max_rss_bytes[base_id] = rss_b

            if ".batch" in raw_id:
                batch_cpu[base_id] = parse_slurm_duration(total_cpu)
                # .batch is where user code actually runs — if it failed
                # (e.g. OOM), override the main entry's state which may
                # misleadingly say COMPLETED.
                batch_state = job_state.split()[0] if job_state else ""
                if batch_state and batch_state in SLURM_FAILURE_STATES:
                    sacct_state[base_id] = batch_state
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
            )

        logger.debug(f"Fetched stats for {len(stats)}/{len(job_ids)} jobs via sacct")
        return stats

    async def get_cluster_info(self) -> tuple[int, int] | None:
        """Fetch total and idle CPU counts from sinfo.

        Returns:
            ``(total, idle)`` tuple, or ``None`` on failure.
        """
        cmd = "sinfo --noheader --format='%C'"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"sinfo failed: {e}")
            return None

        if exit_code != 0:
            logger.warning(f"sinfo failed (exit {exit_code}): {stderr}")
            return None

        # Format: "allocated/idle/other/total"
        line = stdout.strip().strip("'")
        parts = line.split("/")
        if len(parts) != 4:
            logger.warning(f"Unexpected sinfo output: {line}")
            return None

        try:
            total = int(parts[3])
            idle = int(parts[1])
            return total, idle
        except ValueError:
            logger.warning(f"Failed to parse sinfo numbers: {line}")
            return None

    # Backward-compat alias
    get_cluster_cpus = get_cluster_info

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
        """Submit a job script via sbatch. Returns the Slurm job ID."""
        escaped_script = script.replace("'", "'\\''")
        submit_cmd = f"sbatch <<'SCRIPTHUT_EOF'\n{escaped_script}\nSCRIPTHUT_EOF"
        stdout, stderr, exit_code = await self._ssh.run_command(submit_cmd)
        if exit_code != 0:
            raise RuntimeError(f"sbatch failed: {stderr}")
        try:
            return stdout.strip().split()[-1]  # "Submitted batch job 12345"
        except (IndexError, ValueError):
            raise RuntimeError(f"Could not parse job ID from sbatch output: {stdout}")

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
        shebang = "#!/bin/bash -l" if login_shell else "#!/bin/bash"
        account_line = f"#SBATCH --account={account}\n" if account else ""
        gres_line = f"#SBATCH --gres={task.gres}\n" if task.gres else ""

        header = f"""{shebang}
#SBATCH --job-name="{task.name}"
#SBATCH --partition={task.partition}
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
