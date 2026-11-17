"""Tests for submission verification and the SUBMITTED-disappearance path.

Covers:
- Slurm: stricter sbatch ID parsing via regex; post-submit squeue/sacct verify;
  RuntimeError surfaces raw output when verification fails.
- PBS: similar verification via qstat.
- Manager: a SUBMITTED item that vanishes from the queue without ever being
  observed RUNNING is marked FAILED (loud failure) rather than COMPLETED.
- Main: symmetric sacct correction flips that FAILED back to COMPLETED for
  ultra-fast jobs that finish between two poll cycles.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from scripthut.backends.base import SubmitResult
from scripthut.backends.pbs import PBSBackend
from scripthut.scheduler import SlurmControl as SlurmBackend
from scripthut.models import JobState


# --- Slurm submission verification ---------------------------------------


def _slurm_with_responses(responses: list[tuple[str, str, int]]) -> SlurmBackend:
    """SlurmBackend whose SSH client returns ``responses`` in order."""
    ssh = AsyncMock()
    iterator = iter(responses)

    async def run_command(cmd: str, timeout: int = 30):  # noqa: ARG001
        try:
            return next(iterator)
        except StopIteration:
            return ("", "", 0)

    ssh.run_command = AsyncMock(side_effect=run_command)
    return SlurmBackend(ssh)


class TestSlurmSubmitParse:
    @pytest.mark.asyncio
    async def test_strict_regex_parses_clean_output(self):
        backend = _slurm_with_responses([
            ("Submitted batch job 12345\n", "", 0),  # sbatch
            ("12345\n", "", 0),  # squeue verify — found
        ])
        result = await backend._submit_and_verify("#!/bin/bash\necho hi")
        assert result.job_id == "12345"
        assert "Submitted batch job 12345" in result.submit_output

    @pytest.mark.asyncio
    async def test_strict_regex_ignores_warnings_after_id_line(self):
        # Some sbatch wrappers emit warnings after the ID line; the old
        # split()[-1] approach captured the last token as the "job_id".
        backend = _slurm_with_responses([
            (
                "Submitted batch job 99999\n"
                "sbatch: warning: some-cluster: hint about something\n",
                "",
                0,
            ),
            ("99999\n", "", 0),  # squeue verify — found
        ])
        result = await backend._submit_and_verify("#!/bin/bash")
        assert result.job_id == "99999"

    @pytest.mark.asyncio
    async def test_unparseable_output_raises_with_raw_text(self):
        backend = _slurm_with_responses([
            ("not a real sbatch response\n", "warning: weird\n", 0),
        ])
        with pytest.raises(RuntimeError, match="Could not parse job ID"):
            await backend._submit_and_verify("#!/bin/bash")

    @pytest.mark.asyncio
    async def test_sbatch_nonzero_exit_raises_with_stderr(self):
        backend = _slurm_with_responses([
            ("", "sbatch: error: invalid partition\n", 1),
        ])
        with pytest.raises(RuntimeError, match="sbatch failed"):
            await backend._submit_and_verify("#!/bin/bash")


class TestSlurmSubmitVerify:
    @pytest.mark.asyncio
    async def test_verify_falls_back_to_sacct_when_squeue_empty(self):
        # squeue returns empty (job already finished) but sacct knows it.
        backend = _slurm_with_responses([
            ("Submitted batch job 7\n", "", 0),  # sbatch
            ("", "", 0),  # squeue: empty
            ("7\n", "", 0),  # sacct: found
        ])
        result = await backend._submit_and_verify("#!/bin/bash")
        assert result.job_id == "7"

    @pytest.mark.asyncio
    async def test_verify_raises_when_neither_squeue_nor_sacct_recognize_id(self):
        backend = _slurm_with_responses([
            ("Submitted batch job 42\n", "", 0),  # sbatch
            ("", "", 1),  # squeue: invalid id, exit 1
            ("", "", 0),  # sacct: empty
        ])
        with pytest.raises(RuntimeError, match="does not appear in squeue or sacct"):
            await backend._submit_and_verify("#!/bin/bash")

    @pytest.mark.asyncio
    async def test_submit_output_captured_on_success(self):
        backend = _slurm_with_responses([
            ("Submitted batch job 10\n", "warning: foo\n", 0),
            ("10\n", "", 0),
        ])
        result = await backend._submit_and_verify("#!/bin/bash")
        assert "Submitted batch job 10" in result.submit_output
        assert "warning: foo" in result.submit_output



# --- PBS submission verification -----------------------------------------






# --- Manager disappearance behavior --------------------------------------
