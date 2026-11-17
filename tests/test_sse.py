"""Tests for SSE notification infrastructure."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest



# -- Helpers ------------------------------------------------------------------




# -- RunManager notify/wait tests ------------------------------------------




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
