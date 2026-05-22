"""Regression test: process_run must fire after a symmetric sacct
correction unwinds DEP_FAILED children.

Scenario (the user's bug report on slurm job 280081):

  setup.init   FAILED+marker (vanished mid-poll, ultra-fast job)
  build.x      DEP_FAILED (cascade-forward fired)
  final.merge  DEP_FAILED (cascade-forward fired)

Next poll, sacct reports COMPLETED for setup.init. The symmetric
correction at main.poll_backend flips it back to COMPLETED, calls
uncascade_dep_failures() which resets the children to PENDING.

The bug: process_run is *not* called, so PENDING items whose deps are
now satisfied never get submitted — the run stalls forever.

This test pins the fix: after a correction that unwinds children,
``RunManager.process_run`` is awaited on the affected run.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from scripthut.backends.base import (
    ClusterInfo,
    DiskInfo,
    JobStats,
)
from scripthut.main import poll_backend
from scripthut.models import ConnectionStatus
from scripthut.runs.manager import DISAPPEARED_BEFORE_RUNNING_MARKER
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition
from scripthut.runtime import BackendState


def _item(
    task_id: str,
    *,
    status: RunItemStatus = RunItemStatus.PENDING,
    job_id: str | None = None,
    error: str | None = None,
    deps: list[str] | None = None,
) -> RunItem:
    return RunItem(
        task=TaskDefinition(
            id=task_id, name=task_id, command="true",
            dependencies=deps or [],
        ),
        status=status,
        job_id=job_id,
        error=error,
    )


def _make_run(items: list[RunItem]) -> Run:
    return Run(
        id="r1",
        workflow_name="diamond",
        backend_name="cluster",
        created_at=datetime.now(UTC),
        items=items,
        max_concurrent=None,
    )


def _state_with_run(run: Run, sacct_state: str = "COMPLETED") -> tuple:
    """Build a (state, BackendState) pair where the backend returns
    ``sacct_state`` for every item's job_id via ``get_job_stats``.
    """
    job_backend = MagicMock()
    job_backend.get_jobs = AsyncMock(return_value=[])
    job_backend.get_cluster_info = AsyncMock(return_value=ClusterInfo(
        partitions=[], pending_reasons={},
    ))
    job_backend.get_disk_info = AsyncMock(return_value=DiskInfo(
        total_bytes=0, avail_bytes=0, path="/",
    ))
    job_backend.get_job_stats = AsyncMock(return_value={
        item.job_id: JobStats(
            cpu_efficiency=0.0, max_rss="0", total_cpu="0s",
            state=sacct_state,
        )
        for item in run.items if item.job_id
    })
    # Pin sacct as the only terminal state recognizer; failure_states
    # is empty because we're only exercising the COMPLETED path.
    job_backend.terminal_states = frozenset({"COMPLETED"})
    job_backend.failure_states = {}

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
    run_manager.storage = MagicMock()
    run_manager.storage.add_external_job = MagicMock()

    state = MagicMock()
    state.run_manager = run_manager
    state.run_storage = run_manager.storage
    state.backends = {"cluster": backend_state}
    state.filter_user = None
    state.filter_enabled = False

    return state, backend_state, run_manager


# A successful ultra-fast job: when poll cycle 1 saw it missing from squeue
# it got marked FAILED with the marker; sacct in poll cycle 2 will say
# COMPLETED so the symmetric correction fires.
def _vanished_failed_item(task_id: str, job_id: str) -> RunItem:
    return _item(
        task_id, status=RunItemStatus.FAILED, job_id=job_id,
        error=DISAPPEARED_BEFORE_RUNNING_MARKER,
    )


class TestUncascadeTriggersReprocess:
    @pytest.mark.asyncio
    async def test_corrected_parent_triggers_process_run_for_unwound_children(
        self, monkeypatch,
    ):
        # Parent was wrongly marked FAILED+marker; one child cascaded
        # forward to DEP_FAILED. After the symmetric correction, the
        # child must be eligible for submission and process_run must
        # actually be called.
        parent = _vanished_failed_item("setup.init", job_id="280069")
        child = _item(
            "build.x", status=RunItemStatus.DEP_FAILED,
            deps=["setup.init"],
            error="Dependency 'setup.init' failed",
        )
        run = _make_run([parent, child])
        state, backend_state, rm = _state_with_run(run)

        # poll_backend reads from main.state, so monkeypatch the module
        # global rather than passing it in.
        monkeypatch.setattr("scripthut.main.state", state)

        await poll_backend(backend_state)

        # Parent flipped back to COMPLETED, error cleared.
        assert parent.status == RunItemStatus.COMPLETED
        assert parent.error is None
        assert parent.scheduler_state == "COMPLETED"

        # Child unwound to PENDING, error cleared.
        assert child.status == RunItemStatus.PENDING
        assert child.error is None

        # And — the bug fix — process_run was actually called so the
        # newly-eligible child gets submitted on this cycle.
        rm.process_run.assert_awaited_once_with(run)

    @pytest.mark.asyncio
    async def test_no_reprocess_when_nothing_was_unwound(self, monkeypatch):
        # Same correction but no DEP_FAILED descendants — process_run
        # shouldn't fire (no newly-eligible items, no work to do).
        only = _vanished_failed_item("solo", job_id="42")
        run = _make_run([only])
        state, backend_state, rm = _state_with_run(run)

        monkeypatch.setattr("scripthut.main.state", state)
        await poll_backend(backend_state)

        assert only.status == RunItemStatus.COMPLETED
        assert only.error is None
        rm.process_run.assert_not_awaited()
