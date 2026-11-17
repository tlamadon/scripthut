"""Slurm submission primitives independent of legacy workflow models."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from scripthut.ssh.client import SSHClient

logger = logging.getLogger(__name__)

SLURM_TERMINAL_STATES = frozenset({
    "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT",
    "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED",
    "DEADLINE", "BOOT_FAIL",
})

_SBATCH_JOB_ID_RE = re.compile(r"Submitted batch job\s+(\d+)")

@dataclass
class SubmitResult:
    """Outcome of a task submission to a backend.

    ``submit_output`` carries the raw stdout/stderr from the submission
    command (sbatch/qsub/AWS API response).  Stored on the run item so the
    user can inspect what the scheduler actually said even on apparent
    success — useful when the parsed job ID looks fine but the job never
    enters the queue.
    """

    job_id: str
    submit_output: str = ""


def format_submit_output(stdout: str, stderr: str) -> str:
    """Combine submission-command stdout/stderr into a single diagnostic blob.

    Stored on ``RunItem.submit_output`` so users can see exactly what
    sbatch/qsub/etc said even when submission appeared to succeed.
    """
    parts: list[str] = []
    out = (stdout or "").strip()
    err = (stderr or "").strip()
    if out:
        parts.append(f"stdout: {out}")
    if err:
        parts.append(f"stderr: {err}")
    return " | ".join(parts)


class SlurmControl:
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
        # Quoted heredoc delimiter (<<'SCRIPTHUT_EOF') already passes the body
        # through literally — no shell processing — so the script must NOT be
        # quote-escaped. Escaping single quotes here would inject literal
        # ``'\''`` sequences and corrupt any command containing a single quote
        # (e.g. an agent's ``tmux new-session -d -s "$S" '...'``).
        submit_cmd = f"sbatch <<'SCRIPTHUT_EOF'\n{script}\nSCRIPTHUT_EOF"
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

    async def cancel_job(self, job_id: str) -> None:
        """Cancel a Slurm job via scancel."""
        await self._ssh.run_command(f"scancel {job_id}")
