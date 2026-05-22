"""Tests for the evidence-based resolution of SUBMITTED items missing from squeue.

The new contract (replacing the old "speculative FAILED + correct later"
pattern): when a SUBMITTED item disappears from squeue past the grace
period, the polling loop in ``main.poll_backend`` asks sacct what
actually happened. Based on that evidence:

- sacct says ``COMPLETED`` → item flips directly to COMPLETED (no
  intermediate FAILED).
- sacct says any failure state → item flips to FAILED with the real
  reason from ``backend.failure_states``.
- sacct has no record AND the item is past
  ``SUBMITTED_NO_RECORD_TIMEOUT_SECONDS`` → item flips to FAILED with
  ``SCHEDULER_NO_RECORD_MARKER`` (the actual "scheduler dropped it" case).
- sacct has no record AND the item is still within the long timeout →
  stays SUBMITTED; we'll ask again on the next poll.

These tests drive ``poll_backend`` end-to-end with mocked backends so
we exercise the same code path the user sees in production.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from scripthut.backends.base import (
    ClusterInfo,
    DiskInfo,
    JobStats,
)
from scripthut.main import poll_backend
from scripthut.models import ConnectionStatus
from scripthut.runs.manager import (
    SCHEDULER_NO_RECORD_MARKER,
    SUBMIT_TO_FAIL_GRACE_SECONDS,
    SUBMITTED_NO_RECORD_TIMEOUT_SECONDS,
)
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition
from scripthut.runtime import BackendState


def _make_run(items: list[RunItem]) -> Run:
    return Run(
        id="r1",
        workflow_name="adhoc",
        backend_name="cluster",
        created_at=datetime.now(UTC),
        items=items,
        max_concurrent=None,
    )


def _backend_returning(
    *,
    squeue_jobs: list | None = None,
    sacct_results: dict[str, JobStats] | None = None,
    failure_states: dict[str, str] | None = None,
):
    """Build a mock job backend with prescribed squeue + sacct outputs."""
    job_backend = MagicMock()
    job_backend.get_jobs = AsyncMock(return_value=squeue_jobs or [])
    job_backend.get_cluster_info = AsyncMock(return_value=ClusterInfo(
        partitions=[], pending_reasons={},
    ))
    job_backend.get_disk_info = AsyncMock(return_value=DiskInfo(
        total_bytes=0, avail_bytes=0, path="/",
    ))
    job_backend.get_job_stats = AsyncMock(return_value=sacct_results or {})
    job_backend.terminal_states = frozenset({
        "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY",
        "NODE_FAIL", "PREEMPTED",
    })
    job_backend.failure_states = failure_states or {
        "FAILED": "Non-zero exit code",
        "TIMEOUT": "Exceeded walltime",
        "OUT_OF_MEMORY": "Out of memory (OOM killed)",
        "CANCELLED": "Cancelled",
    }
    return job_backend


def _state_for_run(run: Run, job_backend) -> tuple:
    backend_state = BackendState(
        name="cluster",
        backend_type="slurm",
        ssh_client=None,
        backend=job_backend,
        status=ConnectionStatus(connected=True, host="h"),
        clone_dir="/tmp",
    )

    run_manager = MagicMock()
    run_manager.runs = {run.id: run}
    run_manager.process_run = AsyncMock()
    run_manager._persist_run = MagicMock()
    run_manager.notify_run = MagicMock()
    run_manager._handle_generates_source = AsyncMock()
    run_manager.storage = MagicMock()
    run_manager.storage.add_external_job = MagicMock()

    state = MagicMock()
    state.run_manager = run_manager
    state.run_storage = run_manager.storage
    state.backends = {"cluster": backend_state}
    state.filter_user = None
    state.filter_enabled = False

    return state, backend_state, run_manager


def _aged_submitted(task_id: str, job_id: str, age_seconds: float) -> RunItem:
    submitted_at = datetime.now(UTC) - timedelta(seconds=age_seconds)
    return RunItem(
        task=TaskDefinition(id=task_id, name=task_id, command="true"),
        status=RunItemStatus.SUBMITTED,
        job_id=job_id,
        submitted_at=submitted_at,
    )


# ---------- positive resolution: sacct says COMPLETED -------------------


class TestSacctCompletedResolution:
    @pytest.mark.asyncio
    async def test_aged_submitted_completed_via_sacct_no_failed_detour(
        self, monkeypatch,
    ):
        # The ultra-fast-job case: SUBMITTED + past grace + missing from
        # squeue + sacct=COMPLETED → straight to COMPLETED.
        item = _aged_submitted(
            "fast", "100", age_seconds=SUBMIT_TO_FAIL_GRACE_SECONDS + 5,
        )
        run = _make_run([item])
        backend = _backend_returning(
            squeue_jobs=[],
            sacct_results={"100": JobStats(
                cpu_efficiency=95.0, max_rss="100M", total_cpu="10s",
                state="COMPLETED",
            )},
        )
        state, backend_state, _ = _state_for_run(run, backend)
        monkeypatch.setattr("scripthut.main.state", state)

        await poll_backend(backend_state)

        assert item.status == RunItemStatus.COMPLETED
        # Never went through FAILED — error stays None, no marker.
        assert item.error is None
        assert item.scheduler_state == "COMPLETED"
        assert item.started_at is not None
        assert item.finished_at is not None

    @pytest.mark.asyncio
    async def test_completed_resolution_triggers_process_run(self, monkeypatch):
        # Newly-COMPLETED items may unblock downstream tasks — process_run
        # must fire so they get submitted immediately.
        item = _aged_submitted(
            "parent", "100", age_seconds=SUBMIT_TO_FAIL_GRACE_SECONDS + 5,
        )
        run = _make_run([item])
        backend = _backend_returning(
            squeue_jobs=[],
            sacct_results={"100": JobStats(
                cpu_efficiency=0.0, max_rss="0", total_cpu="0s",
                state="COMPLETED",
            )},
        )
        state, backend_state, rm = _state_for_run(run, backend)
        monkeypatch.setattr("scripthut.main.state", state)

        await poll_backend(backend_state)

        rm.process_run.assert_awaited_once_with(run)


# ---------- positive resolution: sacct says failure ---------------------


class TestSacctFailureResolution:
    @pytest.mark.asyncio
    async def test_aged_submitted_oom_via_sacct_surfaces_real_reason(
        self, monkeypatch,
    ):
        item = _aged_submitted(
            "oom-victim", "200",
            age_seconds=SUBMIT_TO_FAIL_GRACE_SECONDS + 5,
        )
        run = _make_run([item])
        backend = _backend_returning(
            squeue_jobs=[],
            sacct_results={"200": JobStats(
                cpu_efficiency=0.0, max_rss="100G", total_cpu="0s",
                state="OUT_OF_MEMORY",
            )},
            failure_states={"OUT_OF_MEMORY": "Out of memory (OOM killed)"},
        )
        backend.terminal_states = frozenset({"OUT_OF_MEMORY"})
        state, backend_state, _ = _state_for_run(run, backend)
        monkeypatch.setattr("scripthut.main.state", state)

        await poll_backend(backend_state)

        assert item.status == RunItemStatus.FAILED
        # The error names the actual reason, not a speculative marker.
        assert "Out of memory" in (item.error or "")
        assert item.scheduler_state == "OUT_OF_MEMORY"


# ---------- no-record handling -----------------------------------------


class TestNoRecord:
    @pytest.mark.asyncio
    async def test_no_sacct_record_within_long_timeout_stays_submitted(
        self, monkeypatch,
    ):
        # Past grace but well within the long no-record timeout: keep
        # waiting for sacct (still no positive evidence).
        item = _aged_submitted(
            "still-waiting", "300",
            age_seconds=SUBMIT_TO_FAIL_GRACE_SECONDS + 5,
        )
        run = _make_run([item])
        backend = _backend_returning(squeue_jobs=[], sacct_results={})  # empty
        state, backend_state, _ = _state_for_run(run, backend)
        monkeypatch.setattr("scripthut.main.state", state)

        await poll_backend(backend_state)

        assert item.status == RunItemStatus.SUBMITTED
        assert item.error is None
        # The sacct lookup was attempted (we passed the grace threshold).
        backend.get_job_stats.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_sacct_record_past_long_timeout_marks_scheduler_dropped(
        self, monkeypatch,
    ):
        # The real "vanished" case: sbatch accepted but neither squeue
        # nor sacct knows about the job, past the long timeout.
        item = _aged_submitted(
            "ghost", "400",
            age_seconds=SUBMITTED_NO_RECORD_TIMEOUT_SECONDS + 30,
        )
        run = _make_run([item])
        backend = _backend_returning(squeue_jobs=[], sacct_results={})
        state, backend_state, _ = _state_for_run(run, backend)
        monkeypatch.setattr("scripthut.main.state", state)

        await poll_backend(backend_state)

        assert item.status == RunItemStatus.FAILED
        assert item.error == SCHEDULER_NO_RECORD_MARKER
        assert item.finished_at is not None


# ---------- not eligible (grace not yet expired) -----------------------


class TestWithinGrace:
    @pytest.mark.asyncio
    async def test_submitted_within_grace_not_queried_or_failed(
        self, monkeypatch,
    ):
        # Fresh submission missing from squeue: no sacct call yet,
        # stays SUBMITTED.
        item = _aged_submitted("fresh", "500", age_seconds=5.0)
        run = _make_run([item])
        backend = _backend_returning(squeue_jobs=[])
        state, backend_state, _ = _state_for_run(run, backend)
        monkeypatch.setattr("scripthut.main.state", state)

        await poll_backend(backend_state)

        assert item.status == RunItemStatus.SUBMITTED
        assert item.error is None
        # sacct_ids was empty (no Phase A items either) → no get_job_stats
        # call. Verify the lookup never fired.
        backend.get_job_stats.assert_not_awaited()
