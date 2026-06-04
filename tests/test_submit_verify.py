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
from scripthut.backends.slurm import SlurmBackend
from scripthut.models import JobState
from scripthut.runs.manager import (
    DISAPPEARED_BEFORE_RUNNING_MARKER,
    SUBMIT_TO_FAIL_GRACE_SECONDS,
    RunManager,
)
from scripthut.runs.models import (
    Run,
    RunItem,
    RunItemStatus,
    TaskDefinition,
)


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

    @pytest.mark.asyncio
    async def test_submit_task_returns_submit_result(self):
        backend = _slurm_with_responses([
            ("Submitted batch job 55\n", "", 0),
            ("55\n", "", 0),
        ])
        task = TaskDefinition(id="t1", name="t1", command="echo hi")
        result = await backend.submit_task(task, "#!/bin/bash")
        assert isinstance(result, SubmitResult)
        assert result.job_id == "55"


# --- PBS submission verification -----------------------------------------


def _pbs_with_responses(responses: list[tuple[str, str, int]]) -> PBSBackend:
    ssh = AsyncMock()
    iterator = iter(responses)

    async def run_command(cmd: str, timeout: int = 30):  # noqa: ARG001
        try:
            return next(iterator)
        except StopIteration:
            return ("", "", 0)

    ssh.run_command = AsyncMock(side_effect=run_command)
    return PBSBackend(ssh)


class TestPBSSubmitVerify:
    @pytest.mark.asyncio
    async def test_qsub_then_qstat_recognizes_job(self):
        backend = _pbs_with_responses([
            ("12345.pbs-server\n", "", 0),  # qsub
            ("Job Id: 12345.pbs-server\n", "", 0),  # qstat -f
        ])
        result = await backend._submit_and_verify("#!/bin/bash")
        assert result.job_id == "12345"
        assert "12345.pbs-server" in result.submit_output

    @pytest.mark.asyncio
    async def test_qsub_succeeds_but_qstat_does_not_recognize(self):
        backend = _pbs_with_responses([
            ("99.server\n", "", 0),  # qsub returns id
            ("", "", 1),  # qstat -f: not found
            ("", "", 1),  # qstat -xf: not found
        ])
        with pytest.raises(RuntimeError, match="PBS does not recognize"):
            await backend._submit_and_verify("#!/bin/bash")


# --- Manager disappearance behavior --------------------------------------


def _make_manager_with_run(item: RunItem) -> tuple[RunManager, Run]:
    """Build a minimal RunManager containing a single run with one item."""
    config = type(
        "_Cfg",
        (),
        {
            "get_backend": lambda self, name: None,
            "get_source": lambda self, name: None,
            "get_project": lambda self, name: None,
            "env": [],
        },
    )()
    manager = RunManager(config=config, backends={}, storage=None, job_backends={})
    run = Run(
        id="r1",
        workflow_name="wf",
        backend_name="b1",
        created_at=datetime.now(timezone.utc),
        items=[item],
        max_concurrent=None,
    )
    manager.runs[run.id] = run
    return manager, run


class TestManagerDisappearance:
    @pytest.mark.asyncio
    async def test_submitted_missing_from_queue_does_not_mark_failed(self):
        # New evidence-based contract: a SUBMITTED item missing from squeue
        # is NOT marked FAILED here — that requires positive evidence from
        # sacct. Stays SUBMITTED until sacct resolves it via the main.py
        # polling path.
        aged = datetime.now(timezone.utc) - timedelta(
            seconds=SUBMIT_TO_FAIL_GRACE_SECONDS + 5,
        )
        item = RunItem(
            task=TaskDefinition(id="t1", name="t1", command="echo hi"),
            status=RunItemStatus.SUBMITTED,
            job_id="12345",
            submitted_at=aged,
        )
        manager, run = _make_manager_with_run(item)

        await manager.update_run_status(run, slurm_jobs={})

        assert item.status == RunItemStatus.SUBMITTED
        assert item.error is None
        assert item.finished_at is None

    @pytest.mark.asyncio
    async def test_running_disappearance_marks_settling(self):
        # v0.10.0: a RUNNING item missing from squeue is in transition
        # between "scheduler done" and "accounting confirmed". Move to
        # SETTLING — the run stays non-terminal until sacct returns a
        # row, preventing `run watch --exit-status` from racing a
        # transient COMPLETED→FAILED flip.
        item = RunItem(
            task=TaskDefinition(id="t1", name="t1", command="echo hi"),
            status=RunItemStatus.RUNNING,
            job_id="12345",
            submitted_at=datetime.now(timezone.utc),
            started_at=datetime.now(timezone.utc),
        )
        manager, run = _make_manager_with_run(item)
        await manager.update_run_status(run, slurm_jobs={})
        assert item.status == RunItemStatus.SETTLING
        # finished_at is set to the queue-vanish moment so the long-
        # grace fallback in main.poll_backend can decide when to give
        # up on accounting.
        assert item.finished_at is not None

    @pytest.mark.asyncio
    async def test_queued_disappearance_marks_settling(self):
        # Same as RUNNING-vanished: SETTLING, not optimistic COMPLETED.
        item = RunItem(
            task=TaskDefinition(id="t1", name="t1", command="echo hi"),
            status=RunItemStatus.QUEUED,
            job_id="12345",
            submitted_at=datetime.now(timezone.utc),
        )
        manager, run = _make_manager_with_run(item)

        await manager.update_run_status(run, slurm_jobs={})

        assert item.status == RunItemStatus.SETTLING
        assert item.started_at is not None  # filled from submitted_at
        assert item.finished_at is not None

    @pytest.mark.asyncio
    async def test_pending_in_queue_transitions_submitted_to_queued(self):
        # The key evidence-based transition: SUBMITTED + observed in squeue
        # as PENDING → QUEUED. Now we *know* the scheduler has it.
        item = RunItem(
            task=TaskDefinition(id="t1", name="t1", command="echo hi"),
            status=RunItemStatus.SUBMITTED,
            job_id="12345",
            submitted_at=datetime.now(timezone.utc),
        )
        manager, run = _make_manager_with_run(item)

        await manager.update_run_status(run, slurm_jobs={"12345": JobState.PENDING})

        assert item.status == RunItemStatus.QUEUED
        assert item.error is None

    @pytest.mark.asyncio
    async def test_queued_stays_queued_when_still_pending_in_queue(self):
        item = RunItem(
            task=TaskDefinition(id="t1", name="t1", command="echo hi"),
            status=RunItemStatus.QUEUED,
            job_id="12345",
            submitted_at=datetime.now(timezone.utc),
        )
        manager, run = _make_manager_with_run(item)

        await manager.update_run_status(run, slurm_jobs={"12345": JobState.PENDING})

        assert item.status == RunItemStatus.QUEUED

    @pytest.mark.asyncio
    async def test_queued_transitions_to_running_when_observed_running(self):
        item = RunItem(
            task=TaskDefinition(id="t1", name="t1", command="echo hi"),
            status=RunItemStatus.QUEUED,
            job_id="12345",
            submitted_at=datetime.now(timezone.utc),
        )
        manager, run = _make_manager_with_run(item)

        await manager.update_run_status(run, slurm_jobs={"12345": JobState.RUNNING})

        assert item.status == RunItemStatus.RUNNING
        assert item.started_at is not None
