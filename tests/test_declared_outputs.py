"""Declared ``outputs`` are a postcondition, not just a cache key.

A task that exits 0 without writing what it declared is a failure. The
alternative — what this replaces — is a batch interpreter that swallows its
own error (Stata's ``-e`` masking a do-file's ``exit 601``, for one) reporting
COMPLETED while producing nothing, and every downstream task then failing for
want of an input that was never made.

The distinction that makes this safe lives in ``CacheManager.hash_paths``:
only "the walk ran and matched nothing" fails the task. "The walk could not be
made" leaves it alone, because inferring absence from a dropped connection is
the same error as pruning files because a listing failed.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from scripthut.runs.models import Run, RunItem, RunItemStatus
from tests.test_probe import _manager, _task


def _ssh(side):
    ssh = MagicMock()
    ssh.run_command = AsyncMock(side_effect=side)
    return ssh


def _fixture(side, **task_over):
    """A COMPLETED item whose backend answers every command with ``side``."""
    mgr = _manager(_ssh(side), cache_enabled=False)
    mgr._persist_run = MagicMock()
    mgr._maybe_store_cache = AsyncMock()
    item = RunItem(task=_task(**task_over), status=RunItemStatus.COMPLETED)
    item.exit_code = 0
    run = Run(
        id="r1", workflow_name="wf", backend_name="cluster",
        created_at=datetime.now(UTC), items=[item],
        max_concurrent=None, log_dir="/logs",
    )
    mgr.runs[run.id] = run
    return mgr, run, item


class TestDeclaredOutputsArePostcondition:
    @pytest.mark.asyncio
    async def test_missing_outputs_fail_the_task(self):
        """Walk succeeds, matches nothing -> the task did not do its job."""
        async def side(cmd, timeout=30):
            return ("", "", 0)

        mgr, run, item = _fixture(side)
        await mgr._after_item_completed(run, item)

        assert item.status == RunItemStatus.FAILED
        assert "declared outputs matched no files" in (item.error or "")
        # The message names the path so the failure is actionable.
        assert "model.pt" in (item.error or "")

    @pytest.mark.asyncio
    async def test_missing_outputs_are_not_cached(self):
        """Caching an empty output set would let the next run "succeed"."""
        async def side(cmd, timeout=30):
            return ("", "", 0)

        mgr, run, item = _fixture(side)
        await mgr._after_item_completed(run, item)

        assert item.status == RunItemStatus.FAILED
        mgr._maybe_store_cache.assert_not_called()

    @pytest.mark.asyncio
    async def test_ssh_failure_leaves_the_task_completed(self):
        """A dropped connection is not evidence of a missing file."""
        async def side(cmd, timeout=30):
            raise RuntimeError("connection dropped")

        mgr, run, item = _fixture(side)
        await mgr._after_item_completed(run, item)

        assert item.status == RunItemStatus.COMPLETED
        assert item.output_hashes is None
        # Still a normal completion, so the cache store is still attempted.
        mgr._maybe_store_cache.assert_called_once()

    @pytest.mark.asyncio
    async def test_nonzero_walk_leaves_the_task_completed(self):
        async def side(cmd, timeout=30):
            return ("", "find: cannot access", 1)

        mgr, run, item = _fixture(side)
        await mgr._after_item_completed(run, item)

        assert item.status == RunItemStatus.COMPLETED
        assert item.output_hashes is None

    @pytest.mark.asyncio
    async def test_present_outputs_hash_and_stay_completed(self):
        async def side(cmd, timeout=30):
            if "sha256sum" in cmd or "_scripthut_sha" in cmd:
                return ("deadbeef  model.pt\n", "", 0)
            return ("", "", 0)

        mgr, run, item = _fixture(side)
        await mgr._after_item_completed(run, item)

        assert item.status == RunItemStatus.COMPLETED
        assert item.output_hashes == {"model.pt": "deadbeef"}
        mgr._maybe_store_cache.assert_called_once()

    @pytest.mark.asyncio
    async def test_task_declaring_no_outputs_is_untouched(self):
        """No declaration, no postcondition — nothing to enforce."""
        async def side(cmd, timeout=30):
            return ("", "", 0)

        mgr, run, item = _fixture(side, outputs=[])
        await mgr._after_item_completed(run, item)

        assert item.status == RunItemStatus.COMPLETED
        mgr._maybe_store_cache.assert_called_once()
