"""Tests for SSE notification infrastructure."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripthut.runs.manager import RunManager
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition


# -- Helpers ------------------------------------------------------------------


def _make_manager(ssh_mock: AsyncMock | None = None) -> RunManager:
    """Create a RunManager with a mocked SSH client."""
    config = MagicMock()
    config.clusters = {"test-cluster": MagicMock()}
    config.task_sources = []
    config.settings.poll_interval = 60
    clusters = {"test-cluster": ssh_mock or AsyncMock()}
    return RunManager(config=config, clusters=clusters)


# -- RunManager notify/wait tests ------------------------------------------


class TestNotifyRun:
    """Tests for the RunManager SSE notification mechanism."""

    def test_notify_creates_new_event(self) -> None:
        """notify_run rotates the event, leaving a fresh one for next waiters."""
        mgr = _make_manager()
        mgr.notify_run("q1")
        # After notify, there should be a fresh (unset) event
        assert "q1" in mgr._run_events
        assert not mgr._run_events["q1"].is_set()

    def test_notify_sets_old_event(self) -> None:
        """notify_run sets the old event to wake waiters."""
        mgr = _make_manager()
        # Pre-create an event (simulates a waiter holding it)
        old_event = asyncio.Event()
        mgr._run_events["q1"] = old_event
        mgr.notify_run("q1")
        # Old event should be set
        assert old_event.is_set()
        # New event should not be the same object
        assert mgr._run_events["q1"] is not old_event

    def test_notify_increments_version(self) -> None:
        """notify_run increments the version counter."""
        mgr = _make_manager()
        assert mgr._run_versions.get("q1", 0) == 0
        mgr.notify_run("q1")
        assert mgr._run_versions["q1"] == 1
        mgr.notify_run("q1")
        assert mgr._run_versions["q1"] == 2

    @pytest.mark.asyncio
    async def test_wait_returns_true_on_notify(self) -> None:
        """wait_for_update returns True when notified."""
        mgr = _make_manager()

        async def notify_soon():
            await asyncio.sleep(0.05)
            mgr.notify_run("q1")

        asyncio.create_task(notify_soon())
        result = await mgr.wait_for_update("q1", timeout=2.0)
        assert result is True

    @pytest.mark.asyncio
    async def test_wait_returns_false_on_timeout(self) -> None:
        """wait_for_update returns False on timeout."""
        mgr = _make_manager()
        result = await mgr.wait_for_update("q1", timeout=0.05)
        assert result is False

    @pytest.mark.asyncio
    async def test_multiple_waiters_all_wake(self) -> None:
        """Multiple concurrent waiters all get woken by a single notify."""
        mgr = _make_manager()

        async def notify_soon():
            await asyncio.sleep(0.05)
            mgr.notify_run("q1")

        asyncio.create_task(notify_soon())
        results = await asyncio.gather(
            mgr.wait_for_update("q1", timeout=2.0),
            mgr.wait_for_update("q1", timeout=2.0),
        )
        assert all(results)


# -- AppState poll event tests ------------------------------------------------


class TestAppStatePollEvent:
    """Tests for the global SSE poll event in AppState."""

    def test_notify_poll_wakes_waiters(self) -> None:
        """notify_poll sets the old event."""
        # Import here to avoid module-level side effects
        from scripthut.main import AppState

        s = AppState()
        old_event = s._poll_event
        s.notify_poll()
        assert old_event.is_set()
        assert s._poll_event is not old_event
        assert not s._poll_event.is_set()

    @pytest.mark.asyncio
    async def test_wait_for_poll_returns_true(self) -> None:
        """wait_for_poll returns True when notified."""
        from scripthut.main import AppState

        s = AppState()

        async def notify_soon():
            await asyncio.sleep(0.05)
            s.notify_poll()

        asyncio.create_task(notify_soon())
        result = await s.wait_for_poll(timeout=2.0)
        assert result is True

    @pytest.mark.asyncio
    async def test_wait_for_poll_returns_false_on_timeout(self) -> None:
        """wait_for_poll returns False on timeout."""
        from scripthut.main import AppState

        s = AppState()
        result = await s.wait_for_poll(timeout=0.05)
        assert result is False

    def test_seconds_until_next_poll_no_poll_yet(self) -> None:
        """Before any poll, seconds_until_next_poll returns poll_interval."""
        from scripthut.main import AppState

        s = AppState()
        s.config = MagicMock()
        s.config.settings.poll_interval = 60
        assert s.seconds_until_next_poll == 60

    def test_seconds_until_next_poll_after_poll(self) -> None:
        """After a poll, seconds_until_next_poll reflects remaining time."""
        from scripthut.main import AppState

        s = AppState()
        s.config = MagicMock()
        s.config.settings.poll_interval = 60
        s._last_poll_time = time.monotonic()
        # Should be close to 60 (within a second of recording)
        remaining = s.seconds_until_next_poll
        assert 58 <= remaining <= 60

    def test_seconds_until_next_poll_elapsed(self) -> None:
        """After full interval elapses, seconds_until_next_poll is 0."""
        from scripthut.main import AppState

        s = AppState()
        s.config = MagicMock()
        s.config.settings.poll_interval = 60
        # Simulate poll that happened 120s ago
        s._last_poll_time = time.monotonic() - 120
        assert s.seconds_until_next_poll == 0
