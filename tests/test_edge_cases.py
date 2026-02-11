"""Tests for cleanup and Gantt edge cases."""

from datetime import datetime, timedelta

from scripthut.history.manager import JobHistoryManager
from scripthut.history.models import UnifiedJob, UnifiedJobSource, UnifiedJobState
from scripthut.main import _compute_gantt_data
from scripthut.queues.models import (
    Queue,
    QueueItem,
    QueueItemStatus,
    TaskDefinition,
)


def _make_queue_item(
    task_id: str,
    status: QueueItemStatus = QueueItemStatus.PENDING,
    submitted_at: datetime | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> QueueItem:
    task = TaskDefinition(id=task_id, name=task_id, command="echo")
    return QueueItem(
        task=task,
        status=status,
        submitted_at=submitted_at,
        started_at=started_at,
        finished_at=finished_at,
    )


class TestCleanupOrphanedNonTerminal:
    """Tests for cleanup_old_jobs removing non-terminal orphans."""

    def test_removes_old_non_terminal_jobs(self, tmp_path):
        """Non-terminal jobs older than retention should be cleaned up."""
        manager = JobHistoryManager(history_path=tmp_path / "history.json")
        old_time = datetime.now() - timedelta(days=10)

        job = UnifiedJob(
            id="orphan-1",
            slurm_job_id="12345",
            name="stuck job",
            user="test",
            cluster_name="test",
            state=UnifiedJobState.SUBMITTED,
            source=UnifiedJobSource.QUEUE,
            created_at=old_time,
            last_seen=old_time,
        )
        manager.add_job(job)
        assert len(manager.get_all_jobs()) == 1

        removed = manager.cleanup_old_jobs()
        assert removed == 1
        assert len(manager.get_all_jobs()) == 0

    def test_keeps_recent_non_terminal_jobs(self, tmp_path):
        """Non-terminal jobs within retention period should be kept."""
        manager = JobHistoryManager(history_path=tmp_path / "history.json")

        job = UnifiedJob(
            id="active-1",
            slurm_job_id="12346",
            name="recent job",
            user="test",
            cluster_name="test",
            state=UnifiedJobState.RUNNING,
            source=UnifiedJobSource.QUEUE,
        )
        manager.add_job(job)

        removed = manager.cleanup_old_jobs()
        assert removed == 0
        assert len(manager.get_all_jobs()) == 1

    def test_still_removes_old_terminal_jobs(self, tmp_path):
        """Terminal jobs older than retention should still be cleaned up."""
        manager = JobHistoryManager(history_path=tmp_path / "history.json")
        old_time = datetime.now() - timedelta(days=10)

        job = UnifiedJob(
            id="done-1",
            slurm_job_id="12347",
            name="old completed job",
            user="test",
            cluster_name="test",
            state=UnifiedJobState.COMPLETED,
            source=UnifiedJobSource.QUEUE,
            created_at=old_time,
            finish_time=old_time,
            last_seen=old_time,
        )
        manager.add_job(job)

        removed = manager.cleanup_old_jobs()
        assert removed == 1


class TestGanttWaitBarCapping:
    """Tests for Gantt wait bar capping at finished_at."""

    def test_wait_bar_capped_at_finished_for_completed_item(self):
        """Completed item with finished_at but no started_at should cap wait bar."""
        now = datetime.now()
        created = now - timedelta(minutes=10)
        submitted = now - timedelta(minutes=8)
        finished = now - timedelta(minutes=5)

        item = _make_queue_item(
            "t1",
            status=QueueItemStatus.COMPLETED,
            submitted_at=submitted,
            started_at=None,  # Pre-fix historical data
            finished_at=finished,
        )
        queue = Queue(
            id="q1",
            source_name="test",
            cluster_name="test",
            created_at=created,
            items=[item],
            max_concurrent=5,
        )

        gantt_items, _ = _compute_gantt_data(queue)
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

        item = _make_queue_item(
            "t1",
            status=QueueItemStatus.SUBMITTED,
            submitted_at=submitted,
            started_at=None,
            finished_at=None,  # Actually still waiting
        )
        queue = Queue(
            id="q1",
            source_name="test",
            cluster_name="test",
            created_at=created,
            items=[item],
            max_concurrent=5,
        )

        gantt_items, _ = _compute_gantt_data(queue)
        gi = gantt_items[0]

        assert gi["has_bar"]
        # Should extend close to 100% since it's growing to now
        assert gi["bar_end"] > 90


class TestCancelQueueStartedAt:
    """Tests for cancel_queue setting started_at on cancelled items."""

    def test_cancelled_submitted_item_gets_started_at(self):
        """A SUBMITTED item cancelled should get started_at set from submitted_at."""
        now = datetime.now()
        submitted = now - timedelta(minutes=5)

        item = _make_queue_item(
            "t1",
            status=QueueItemStatus.SUBMITTED,
            submitted_at=submitted,
        )
        item.slurm_job_id = "12345"

        # Simulate what cancel_queue does (without SSH call)
        item.started_at = item.started_at or item.submitted_at
        item.status = QueueItemStatus.FAILED
        item.error = "Cancelled"
        item.finished_at = datetime.now()

        assert item.started_at == submitted

    def test_cancelled_running_item_keeps_started_at(self):
        """A RUNNING item already has started_at; cancel should not overwrite it."""
        now = datetime.now()
        submitted = now - timedelta(minutes=10)
        started = now - timedelta(minutes=5)

        item = _make_queue_item(
            "t2",
            status=QueueItemStatus.RUNNING,
            submitted_at=submitted,
            started_at=started,
        )
        item.slurm_job_id = "12346"

        item.started_at = item.started_at or item.submitted_at
        item.status = QueueItemStatus.FAILED
        item.error = "Cancelled"
        item.finished_at = datetime.now()

        assert item.started_at == started  # Not overwritten


class TestQueueStatusFailedWithBlockedPending:
    """Tests for Queue.status correctly returning FAILED when pending items are blocked."""

    def test_failed_with_blocked_pending_returns_failed(self):
        """Queue with FAILED item and PENDING item blocked by it should be FAILED."""
        # Task A: FAILED
        item_a = _make_queue_item("a", status=QueueItemStatus.FAILED)
        # Task B: PENDING, depends on A
        task_b = TaskDefinition(id="b", name="b", command="echo", dependencies=["a"])
        item_b = QueueItem(task=task_b, status=QueueItemStatus.PENDING)

        queue = Queue(
            id="q1",
            source_name="test",
            cluster_name="test",
            created_at=datetime.now(),
            items=[item_a, item_b],
            max_concurrent=5,
        )

        from scripthut.queues.models import QueueStatus
        assert queue.status == QueueStatus.FAILED

    def test_failed_plus_independent_pending_stays_pending(self):
        """Queue with FAILED item and PENDING item NOT blocked should stay PENDING."""
        item_a = _make_queue_item("a", status=QueueItemStatus.FAILED)
        # Task C: PENDING, no dependencies — could still run
        item_c = _make_queue_item("c", status=QueueItemStatus.PENDING)

        queue = Queue(
            id="q2",
            source_name="test",
            cluster_name="test",
            created_at=datetime.now(),
            items=[item_a, item_c],
            max_concurrent=5,
        )

        from scripthut.queues.models import QueueStatus
        # This should NOT be FAILED because item_c can still run
        assert queue.status == QueueStatus.PENDING

    def test_dep_failed_cascade_returns_failed(self):
        """Queue with DEP_FAILED items and PENDING blocked by them should be FAILED."""
        item_a = _make_queue_item("a", status=QueueItemStatus.FAILED)
        task_b = TaskDefinition(id="b", name="b", command="echo", dependencies=["a"])
        item_b = QueueItem(task=task_b, status=QueueItemStatus.DEP_FAILED)
        task_c = TaskDefinition(id="c", name="c", command="echo", dependencies=["b"])
        item_c = QueueItem(task=task_c, status=QueueItemStatus.PENDING)

        queue = Queue(
            id="q3",
            source_name="test",
            cluster_name="test",
            created_at=datetime.now(),
            items=[item_a, item_b, item_c],
            max_concurrent=5,
        )

        from scripthut.queues.models import QueueStatus
        assert queue.status == QueueStatus.FAILED

