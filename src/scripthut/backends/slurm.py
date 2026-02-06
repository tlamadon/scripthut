"""Slurm job backend implementation."""

import logging
from datetime import datetime

from scripthut.backends.base import JobBackend
from scripthut.models import JobState, SlurmJob
from scripthut.ssh.client import SSHClient

logger = logging.getLogger(__name__)

# squeue format string for extended job info
# Fields: JobID, Name, User, State, Partition, TimeUsed, NodeList, NumCPUs, MinMemory, SubmitTime, StartTime
SQUEUE_FORMAT = "%i|%j|%u|%T|%P|%M|%N|%C|%m|%V|%S"


def parse_slurm_datetime(dt_str: str) -> datetime | None:
    """Parse Slurm datetime string to Python datetime."""
    if not dt_str or dt_str in ("N/A", "Unknown", "None"):
        return None
    try:
        # Slurm uses ISO format: YYYY-MM-DDTHH:MM:SS
        return datetime.fromisoformat(dt_str.replace("T", " ").split(".")[0])
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

    async def is_available(self) -> bool:
        """Check if Slurm is available by running squeue --version."""
        try:
            stdout, _, exit_code = await self._ssh.run_command("squeue --version")
            return exit_code == 0 and "slurm" in stdout.lower()
        except Exception as e:
            logger.warning(f"Slurm availability check failed: {e}")
            return False
