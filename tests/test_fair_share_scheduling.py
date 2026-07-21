"""Fair-share dispatch across runs sharing a backend.

Before this, dispatch was pure FIFO by dict-insertion order: a long-lived run
with a big backlog re-claimed every freed backend slot before a later, smaller
run got any. ``process_run`` also took *all* free slots, so whichever run was
processed first won outright.

Now each contending run may claim at most an equal slice of the backend's free
slots (neediest first, ``created_at`` breaking ties), followed by a greedy
mop-up pass so a run whose share exceeded its ready set doesn't waste capacity.

As in test_cross_run_concurrency, ``process_run`` stays real so the actual slot
math runs; only ``submit_task`` is stubbed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
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

BASE_TIME = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)


def _items(prefix: str, n: int, *, status: RunItemStatus) -> list[RunItem]:
    return [
        RunItem(
            task=TaskDefinition(id=f"{prefix}{i}", name=f"{prefix}{i}", command="true"),
            status=status,
            job_id=f"job-{prefix}{i}" if status != RunItemStatus.PENDING else None,
        )
        for i in range(n)
    ]


def _run(run_id: str, items: list[RunItem], *, age_seconds: int = 0) -> Run:
    return Run(
        id=run_id,
        workflow_name="wf",
        backend_name="b1",
        created_at=BASE_TIME + timedelta(seconds=age_seconds),
        items=items,
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
    async def fake_submit(run: Run, item: RunItem) -> bool:
        item.status = RunItemStatus.SUBMITTED
        item.job_id = f"job-{item.task.id}"
        item.submitted_at = datetime.now(timezone.utc)
        return True

    mock = AsyncMock(side_effect=fake_submit)
    manager.submit_task = mock  # type: ignore[method-assign]
    return mock


def _count(run: Run, status: RunItemStatus) -> int:
    return sum(1 for i in run.items if i.status == status)


class TestFairShare:
    @pytest.mark.asyncio
    async def test_big_run_does_not_monopolise_free_slots(self):
        # Cap 10, both runs idle with plenty of ready work. Old behaviour:
        # whichever run was processed first took all 10.
        mgr = _manager(backend_max=10)
        big = _run("BIG", _items("big", 20, status=RunItemStatus.PENDING))
        small = _run("SMALL", _items("sml", 20, status=RunItemStatus.PENDING), age_seconds=60)
        mgr.runs = {"BIG": big, "SMALL": small}
        _stub_submit(mgr)

        await mgr.update_all_runs(backend_jobs={})

        assert _count(big, RunItemStatus.SUBMITTED) == 5
        assert _count(small, RunItemStatus.SUBMITTED) == 5

    @pytest.mark.asyncio
    async def test_freed_slots_go_to_the_starved_run(self):
        # BIG holds 8 of 10 slots, SMALL holds none and is waiting.
        mgr = _manager(backend_max=10)
        big = _run(
            "BIG",
            _items("big", 8, status=RunItemStatus.RUNNING)
            + _items("bigp", 12, status=RunItemStatus.PENDING),
        )
        small = _run("SMALL", _items("sml", 4, status=RunItemStatus.PENDING), age_seconds=60)
        mgr.runs = {"BIG": big, "SMALL": small}
        _stub_submit(mgr)

        await mgr.update_all_runs(backend_jobs={})

        # 2 free slots, 2 contenders -> 1 each on the fair pass. SMALL is
        # ordered first (running_count 0 vs 8), so it is not shut out by the
        # run that already holds the backend.
        assert _count(small, RunItemStatus.SUBMITTED) == 1
        assert _count(big, RunItemStatus.SUBMITTED) == 1

    @pytest.mark.asyncio
    async def test_mop_up_pass_fills_slots_the_fair_share_left_idle(self):
        # Cap 10, 2 contenders -> share of 5 each, but SMALL only has 1 ready
        # item. The greedy second pass must hand the leftover 4 to BIG rather
        # than leave the backend under-used.
        mgr = _manager(backend_max=10)
        big = _run("BIG", _items("big", 20, status=RunItemStatus.PENDING))
        small = _run("SMALL", _items("sml", 1, status=RunItemStatus.PENDING), age_seconds=60)
        mgr.runs = {"BIG": big, "SMALL": small}
        _stub_submit(mgr)

        await mgr.update_all_runs(backend_jobs={})

        assert _count(small, RunItemStatus.SUBMITTED) == 1
        assert _count(big, RunItemStatus.SUBMITTED) == 9
        total = _count(big, RunItemStatus.SUBMITTED) + _count(small, RunItemStatus.SUBMITTED)
        assert total == 10, "no capacity may be left idle"

    @pytest.mark.asyncio
    async def test_fewer_slots_than_contenders_does_not_oversubscribe(self):
        # The floor of 1 in _fair_share_slots must not let 3 runs each take a
        # slot when only 2 are free.
        mgr = _manager(backend_max=10)
        holder = _run("HOLD", _items("h", 8, status=RunItemStatus.RUNNING))
        runs = {"HOLD": holder}
        for n in range(3):
            runs[f"R{n}"] = _run(
                f"R{n}", _items(f"r{n}_", 5, status=RunItemStatus.PENDING), age_seconds=n
            )
        mgr.runs = runs
        _stub_submit(mgr)

        await mgr.update_all_runs(backend_jobs={})

        submitted = sum(_count(r, RunItemStatus.SUBMITTED) for r in runs.values())
        assert submitted == 2, "only the 2 genuinely free slots may be used"

    @pytest.mark.asyncio
    async def test_sole_contender_still_takes_everything(self):
        # Fair share must not throttle a run that is the only claimant.
        mgr = _manager(backend_max=10)
        only = _run("ONLY", _items("o", 20, status=RunItemStatus.PENDING))
        mgr.runs = {"ONLY": only}
        _stub_submit(mgr)

        await mgr.update_all_runs(backend_jobs={})

        assert _count(only, RunItemStatus.SUBMITTED) == 10

    @pytest.mark.asyncio
    async def test_equally_loaded_runs_break_ties_by_creation_time(self):
        # Both idle; only 1 slot free. The older run wins.
        mgr = _manager(backend_max=9)
        holder = _run("HOLD", _items("h", 8, status=RunItemStatus.RUNNING))
        older = _run("OLDER", _items("old", 5, status=RunItemStatus.PENDING), age_seconds=1)
        newer = _run("NEWER", _items("new", 5, status=RunItemStatus.PENDING), age_seconds=2)
        # Insertion order deliberately favours the newer run.
        mgr.runs = {"HOLD": holder, "NEWER": newer, "OLDER": older}
        _stub_submit(mgr)

        await mgr.update_all_runs(backend_jobs={})

        assert _count(older, RunItemStatus.SUBMITTED) == 1
        assert _count(newer, RunItemStatus.SUBMITTED) == 0

    @pytest.mark.asyncio
    async def test_backends_are_capped_independently(self):
        mgr = _manager(backend_max=4)
        a = _run("A", _items("a", 10, status=RunItemStatus.PENDING))
        b = _run("B", _items("b", 10, status=RunItemStatus.PENDING))
        b.backend_name = "b2"
        mgr.runs = {"A": a, "B": b}
        _stub_submit(mgr)

        await mgr.update_all_runs(backend_jobs={})

        # Each is the sole contender on its own backend.
        assert _count(a, RunItemStatus.SUBMITTED) == 4
        assert _count(b, RunItemStatus.SUBMITTED) == 4
