"""Tests for run restoration from storage."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripthut.runs.manager import RunManager
from scripthut.runs.storage import RunStorageManager
from scripthut.runs.models import (
    Run,
    RunItem,
    RunItemStatus,
    TaskDefinition,
)


def _make_task(id: str) -> TaskDefinition:
    return TaskDefinition(id=id, name=id, command="echo hi")


def _make_pending_run() -> Run:
    return Run(
        id="pending-r",
        workflow_name="test-workflow",
        backend_name="test-cluster",
        created_at=datetime.now(),
        items=[RunItem(task=_make_task("t1"), status=RunItemStatus.PENDING)],
        max_concurrent=5,
    )


def _make_completed_run() -> Run:
    return Run(
        id="done-r",
        workflow_name="test-workflow",
        backend_name="test-cluster",
        created_at=datetime.now(),
        items=[RunItem(task=_make_task("t1"), status=RunItemStatus.COMPLETED)],
        max_concurrent=5,
    )


class TestRestoreFromStorage:
    """Tests for restore_from_storage resuming pending runs."""

    @pytest.mark.asyncio
    @patch.object(RunManager, "process_run", new_callable=AsyncMock)
    async def test_pending_run_gets_processed(self, mock_process):
        """Restored runs with pending items should have process_run called."""
        pending_run = _make_pending_run()

        storage = MagicMock(spec=RunStorageManager)
        storage.load_all_runs.return_value = {"pending-r": pending_run}

        config = MagicMock()
        config.settings.filter_user = "testuser"
        manager = RunManager(config=config, backends={}, storage=storage)

        count = await manager.restore_from_storage()

        assert count == 1
        assert "pending-r" in manager.runs
        mock_process.assert_called_once_with(pending_run)

    @pytest.mark.asyncio
    @patch.object(RunManager, "process_run", new_callable=AsyncMock)
    async def test_completed_run_not_processed(self, mock_process):
        """Restored runs that are already completed should NOT be reprocessed."""
        completed_run = _make_completed_run()

        storage = MagicMock(spec=RunStorageManager)
        storage.load_all_runs.return_value = {"done-r": completed_run}

        config = MagicMock()
        config.settings.filter_user = "testuser"
        manager = RunManager(config=config, backends={}, storage=storage)

        count = await manager.restore_from_storage()

        assert count == 1
        assert "done-r" in manager.runs
        mock_process.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_storage_returns_zero(self):
        """Without a storage manager, restore should return 0."""
        config = MagicMock()
        config.settings.filter_user = "testuser"
        manager = RunManager(config=config, backends={})

        count = await manager.restore_from_storage()
        assert count == 0

    @pytest.mark.asyncio
    @patch.object(RunManager, "process_run", new_callable=AsyncMock)
    async def test_default_workflow_runs_skipped(self, mock_process):
        """Runs from _default workflow should not be loaded into active management."""
        default_run = Run(
            id="2026-W07",
            workflow_name="_default",
            backend_name="test-cluster",
            created_at=datetime.now(),
            items=[],
            max_concurrent=0,
        )

        storage = MagicMock(spec=RunStorageManager)
        storage.load_all_runs.return_value = {"2026-W07": default_run}

        config = MagicMock()
        config.settings.filter_user = "testuser"
        manager = RunManager(config=config, backends={}, storage=storage)

        count = await manager.restore_from_storage()

        assert count == 0
        assert "2026-W07" not in manager.runs
        mock_process.assert_not_called()
