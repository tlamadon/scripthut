"""Tests for queue restoration from history."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripthut.history.manager import JobHistoryManager
from scripthut.queues.manager import QueueManager
from scripthut.queues.models import (
    Queue,
    QueueItem,
    QueueItemStatus,
    TaskDefinition,
)


def _make_task(id: str) -> TaskDefinition:
    return TaskDefinition(id=id, name=id, command="echo hi")


def _make_pending_queue() -> Queue:
    return Queue(
        id="pending-q",
        source_name="test-source",
        cluster_name="test-cluster",
        created_at=datetime.now(),
        items=[QueueItem(task=_make_task("t1"), status=QueueItemStatus.PENDING)],
        max_concurrent=5,
    )


def _make_completed_queue() -> Queue:
    return Queue(
        id="done-q",
        source_name="test-source",
        cluster_name="test-cluster",
        created_at=datetime.now(),
        items=[QueueItem(task=_make_task("t1"), status=QueueItemStatus.COMPLETED)],
        max_concurrent=5,
    )


class TestRestoreFromHistory:
    """Tests for restore_from_history resuming pending queues."""

    @pytest.mark.asyncio
    @patch.object(QueueManager, "process_queue", new_callable=AsyncMock)
    async def test_pending_queue_gets_processed(self, mock_process):
        """Restored queues with pending items should have process_queue called."""
        pending_queue = _make_pending_queue()

        history = MagicMock(spec=JobHistoryManager)
        history.reconstruct_all_queues.return_value = {"pending-q": pending_queue}

        config = MagicMock()
        config.settings.filter_user = "testuser"
        manager = QueueManager(config=config, clusters={}, history_manager=history)

        count = await manager.restore_from_history()

        assert count == 1
        assert "pending-q" in manager.queues
        mock_process.assert_called_once_with(pending_queue)

    @pytest.mark.asyncio
    @patch.object(QueueManager, "process_queue", new_callable=AsyncMock)
    async def test_completed_queue_not_processed(self, mock_process):
        """Restored queues that are already completed should NOT be reprocessed."""
        completed_queue = _make_completed_queue()

        history = MagicMock(spec=JobHistoryManager)
        history.reconstruct_all_queues.return_value = {"done-q": completed_queue}

        config = MagicMock()
        config.settings.filter_user = "testuser"
        manager = QueueManager(config=config, clusters={}, history_manager=history)

        count = await manager.restore_from_history()

        assert count == 1
        assert "done-q" in manager.queues
        mock_process.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_history_manager_returns_zero(self):
        """Without a history manager, restore should return 0."""
        config = MagicMock()
        config.settings.filter_user = "testuser"
        manager = QueueManager(config=config, clusters={})

        count = await manager.restore_from_history()
        assert count == 0
