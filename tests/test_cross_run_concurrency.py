"""Regression test: a backend-level concurrency slot freed by one run must
be claimed by a *different* run that was blocked in PENDING.

The bug (user report): two workflows share a backend whose ``max_concurrent``
is full. Workflow A holds the slot; workflow B sits in PENDING. When A's job
finishes, B never resumes — it stays PENDING forever.

Cause: ``update_run_status`` only re-drives the run whose *own* items changed.
The backend cap is a cross-run constraint, but rescheduling was per-run, so a
run blocked purely by the backend cap was never reconsidered when an unrelated
run freed a slot.

Fix: ``update_all_runs`` does a second pass that re-drives every active run
with submittable work. ``process_run`` is a no-op while the cap is full, so it
only submits once a slot actually frees.

These tests keep ``process_run`` real (so the actual slot math runs) and only
stub ``submit_task`` to avoid the SSH/env-resolution machinery.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripthut.runs.manager import RunManager
from scripthut.runs.models import (
    Run,
    RunItem,
    RunItemStatus,
    RunStatus,
    TaskDefinition,
)


def _item(task_id: str, *, status: RunItemStatus, job_id: str | None = None) -> RunItem:
    return RunItem(
        task=TaskDefinition(id=task_id, name=task_id, command="true"),
        status=status,
        job_id=job_id,
    )


def _run(run_id: str, item: RunItem) -> Run:
    return Run(
        id=run_id,
        workflow_name="wf",
        backend_name="b1",
        created_at=datetime.now(timezone.utc),
        items=[item],
        max_concurrent=None,  # only the backend cap gates submission
    )


def _manager(backend_max: int) -> RunManager:
    config = SimpleNamespace(
        get_backend=lambda name: SimpleNamespace(max_concurrent=backend_max),
        get_source=lambda name: None,
        get_project=lambda name: None,
        env=[],
    )
    return RunManager(config=config, backends={}, storage=None, job_backends={})


def _stub_submit(manager: RunManager) -> AsyncMock:
    """Replace submit_task with a stub that just marks the item SUBMITTED,
    so process_run's real slot math runs without the SSH/env path."""
    async def fake_submit(run: Run, item: RunItem) -> bool:
        item.status = RunItemStatus.SUBMITTED
        item.job_id = f"job-{item.task.id}"
        item.submitted_at = datetime.now(timezone.utc)
        return True

    mock = AsyncMock(side_effect=fake_submit)
    manager.submit_task = mock  # type: ignore[method-assign]
    return mock


class TestCrossRunConcurrency:
    @pytest.mark.asyncio
    async def test_freed_backend_slot_is_claimed_by_blocked_run(self):
        # Backend cap of 1. Run A holds the slot (RUNNING); run B is PENDING.
        mgr = _manager(backend_max=1)
        a_item = _item("a", status=RunItemStatus.RUNNING, job_id="job-a")
        b_item = _item("b", status=RunItemStatus.PENDING)
        run_a = _run("A", a_item)
        run_b = _run("B", b_item)
        mgr.runs = {"A": run_a, "B": run_b}
        submit = _stub_submit(mgr)

        # --- Phase 1: cap is full, B must NOT be submitted. ---
        await mgr.update_all_runs(backend_jobs={})
        assert b_item.status == RunItemStatus.PENDING
        submit.assert_not_awaited()

        # --- Phase 2: A's job finishes, freeing the only slot. ---
        a_item.status = RunItemStatus.COMPLETED
        assert run_a.status == RunStatus.COMPLETED  # A is now terminal

        await mgr.update_all_runs(backend_jobs={})

        # The freed backend slot is claimed by B even though B itself had
        # no item-state change this cycle. This is the regression: before
        # the fix, process_run was never called for B and it stayed PENDING.
        assert b_item.status == RunItemStatus.SUBMITTED
        submit.assert_awaited_once_with(run_b, b_item)

    @pytest.mark.asyncio
    async def test_cap_still_respected_when_no_slot_frees(self):
        # Two runs both PENDING, backend cap of 1: exactly one gets in.
        mgr = _manager(backend_max=1)
        a_item = _item("a", status=RunItemStatus.PENDING)
        b_item = _item("b", status=RunItemStatus.PENDING)
        mgr.runs = {"A": _run("A", a_item), "B": _run("B", b_item)}
        _stub_submit(mgr)

        await mgr.update_all_runs(backend_jobs={})

        submitted = [
            i for i in (a_item, b_item) if i.status == RunItemStatus.SUBMITTED
        ]
        pending = [i for i in (a_item, b_item) if i.status == RunItemStatus.PENDING]
        assert len(submitted) == 1, "backend cap of 1 must not be exceeded"
        assert len(pending) == 1
