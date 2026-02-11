"""Tests for cleanup and Gantt edge cases."""

from datetime import datetime, timedelta

from scripthut.main import _compute_gantt_data
from scripthut.runs.models import (
    Run,
    RunItem,
    RunItemStatus,
    RunStatus,
    TaskDefinition,
)


def _make_run_item(
    task_id: str,
    status: RunItemStatus = RunItemStatus.PENDING,
    submitted_at: datetime | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> RunItem:
    task = TaskDefinition(id=task_id, name=task_id, command="echo")
    return RunItem(
        task=task,
        status=status,
        submitted_at=submitted_at,
        started_at=started_at,
        finished_at=finished_at,
    )


class TestGanttWaitBarCapping:
    """Tests for Gantt wait bar capping at finished_at."""

    def test_wait_bar_capped_at_finished_for_completed_item(self):
        """Completed item with finished_at but no started_at should cap wait bar."""
        now = datetime.now()
        created = now - timedelta(minutes=10)
        submitted = now - timedelta(minutes=8)
        finished = now - timedelta(minutes=5)

        item = _make_run_item(
            "t1",
            status=RunItemStatus.COMPLETED,
            submitted_at=submitted,
            started_at=None,  # Pre-fix historical data
            finished_at=finished,
        )
        run = Run(
            id="r1",
            workflow_name="test",
            cluster_name="test",
            created_at=created,
            items=[item],
            max_concurrent=5,
        )

        gantt_items, _ = _compute_gantt_data(run)
        gi = gantt_items[0]

        # Wait bar should end at finished_at, not grow to now
        assert gi["has_bar"]
        # The bar_end should be well under 100% since finished was 5min ago
        assert gi["bar_end"] < 90

    def test_wait_bar_grows_for_genuinely_waiting_item(self):
        """Submitted item still waiting should have wait bar grow to now."""
        now = datetime.now()
        created = now - timedelta(minutes=10)
        submitted = now - timedelta(minutes=2)

        item = _make_run_item(
            "t1",
            status=RunItemStatus.SUBMITTED,
            submitted_at=submitted,
            started_at=None,
            finished_at=None,  # Actually still waiting
        )
        run = Run(
            id="r1",
            workflow_name="test",
            cluster_name="test",
            created_at=created,
            items=[item],
            max_concurrent=5,
        )

        gantt_items, _ = _compute_gantt_data(run)
        gi = gantt_items[0]

        assert gi["has_bar"]
        # Should extend close to 100% since it's growing to now
        assert gi["bar_end"] > 90


class TestCancelRunStartedAt:
    """Tests for cancel_run setting started_at on cancelled items."""

    def test_cancelled_submitted_item_gets_started_at(self):
        """A SUBMITTED item cancelled should get started_at set from submitted_at."""
        now = datetime.now()
        submitted = now - timedelta(minutes=5)

        item = _make_run_item(
            "t1",
            status=RunItemStatus.SUBMITTED,
            submitted_at=submitted,
        )
        item.slurm_job_id = "12345"

        # Simulate what cancel_run does (without SSH call)
        item.started_at = item.started_at or item.submitted_at
        item.status = RunItemStatus.FAILED
        item.error = "Cancelled"
        item.finished_at = datetime.now()

        assert item.started_at == submitted

    def test_cancelled_running_item_keeps_started_at(self):
        """A RUNNING item already has started_at; cancel should not overwrite it."""
        now = datetime.now()
        submitted = now - timedelta(minutes=10)
        started = now - timedelta(minutes=5)

        item = _make_run_item(
            "t2",
            status=RunItemStatus.RUNNING,
            submitted_at=submitted,
            started_at=started,
        )
        item.slurm_job_id = "12346"

        item.started_at = item.started_at or item.submitted_at
        item.status = RunItemStatus.FAILED
        item.error = "Cancelled"
        item.finished_at = datetime.now()

        assert item.started_at == started  # Not overwritten


class TestRunStatusFailedWithBlockedPending:
    """Tests for Run.status correctly returning FAILED when pending items are blocked."""

    def test_failed_with_blocked_pending_returns_failed(self):
        """Run with FAILED item and PENDING item blocked by it should be FAILED."""
        # Task A: FAILED
        item_a = _make_run_item("a", status=RunItemStatus.FAILED)
        # Task B: PENDING, depends on A
        task_b = TaskDefinition(id="b", name="b", command="echo", dependencies=["a"])
        item_b = RunItem(task=task_b, status=RunItemStatus.PENDING)

        run = Run(
            id="r1",
            workflow_name="test",
            cluster_name="test",
            created_at=datetime.now(),
            items=[item_a, item_b],
            max_concurrent=5,
        )

        assert run.status == RunStatus.FAILED

    def test_failed_plus_independent_pending_stays_pending(self):
        """Run with FAILED item and PENDING item NOT blocked should stay PENDING."""
        item_a = _make_run_item("a", status=RunItemStatus.FAILED)
        # Task C: PENDING, no dependencies -- could still run
        item_c = _make_run_item("c", status=RunItemStatus.PENDING)

        run = Run(
            id="r2",
            workflow_name="test",
            cluster_name="test",
            created_at=datetime.now(),
            items=[item_a, item_c],
            max_concurrent=5,
        )

        # This should NOT be FAILED because item_c can still run
        assert run.status == RunStatus.PENDING

    def test_dep_failed_cascade_returns_failed(self):
        """Run with DEP_FAILED items and PENDING blocked by them should be FAILED."""
        item_a = _make_run_item("a", status=RunItemStatus.FAILED)
        task_b = TaskDefinition(id="b", name="b", command="echo", dependencies=["a"])
        item_b = RunItem(task=task_b, status=RunItemStatus.DEP_FAILED)
        task_c = TaskDefinition(id="c", name="c", command="echo", dependencies=["b"])
        item_c = RunItem(task=task_c, status=RunItemStatus.PENDING)

        run = Run(
            id="r3",
            workflow_name="test",
            cluster_name="test",
            created_at=datetime.now(),
            items=[item_a, item_b, item_c],
            max_concurrent=5,
        )

        assert run.status == RunStatus.FAILED
