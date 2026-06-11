"""Tests for the run lifecycle NotificationHub."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import pytest

from scripthut.notifications import NotificationHub
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition


def _item(task_id: str, status: RunItemStatus, *, error: str | None = None,
          deps: list[str] | None = None) -> RunItem:
    return RunItem(
        task=TaskDefinition(id=task_id, name=task_id, command="true",
                            dependencies=deps or []),
        status=status,
        error=error,
    )


def _run(run_id: str, items: list[RunItem], workflow: str = "wf") -> Run:
    return Run(
        id=run_id,
        workflow_name=workflow,
        backend_name="b1",
        created_at=datetime.now(timezone.utc),
        items=items,
        max_concurrent=None,
    )


def _drain(queue: asyncio.Queue) -> list[tuple[str, dict]]:
    """Pull every queued message as (event, parsed_data) pairs."""
    out: list[tuple[str, dict]] = []
    while True:
        try:
            msg = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        out.append((msg["event"], json.loads(msg["data"])))
    return out


def _notifs(drained: list[tuple[str, dict]]) -> list[dict]:
    return [data for event, data in drained if event == "notify"]


class TestPriming:
    @pytest.mark.asyncio
    async def test_prime_does_not_announce_existing_runs_or_failures(self):
        hub = NotificationHub()
        done = _run("r1", [_item("a", RunItemStatus.COMPLETED)])
        failed = _run("r2", [_item("b", RunItemStatus.FAILED, error="boom")])
        q = hub.subscribe()

        hub.prime([done, failed])
        # A scan right after prime must stay silent — nothing changed.
        hub.scan([done, failed])

        assert _notifs(_drain(q)) == []


class TestScheduled:
    @pytest.mark.asyncio
    async def test_run_scheduled_emits_info(self):
        hub = NotificationHub()
        run = _run("r1", [_item("a", RunItemStatus.PENDING),
                          _item("b", RunItemStatus.PENDING)])
        q = hub.subscribe()

        hub.run_scheduled(run)

        notifs = _notifs(_drain(q))
        assert len(notifs) == 1
        assert notifs[0]["kind"] == "scheduled"
        assert notifs[0]["level"] == "info"
        assert "2 tasks" in notifs[0]["body"]
        assert notifs[0]["run_id"] == "r1"


class TestFailures:
    @pytest.mark.asyncio
    async def test_task_failure_announced_once(self):
        hub = NotificationHub()
        item = _item("a", RunItemStatus.RUNNING)
        run = _run("r1", [item])
        hub.prime([run])
        q = hub.subscribe()

        # Task fails.
        item.status = RunItemStatus.FAILED
        item.error = "exit 1"
        hub.scan([run])
        hub.scan([run])  # second scan must not re-announce

        notifs = _notifs(_drain(q))
        failures = [n for n in notifs if n["kind"] == "task_failed"]
        assert len(failures) == 1
        assert failures[0]["level"] == "error"
        assert "exit 1" in failures[0]["body"]

    @pytest.mark.asyncio
    async def test_dep_failed_not_announced_per_task(self):
        # dep_failed is downstream fallout — it must not generate its own
        # per-task notification (only the run-done summary counts it).
        hub = NotificationHub()
        a = _item("a", RunItemStatus.FAILED, error="boom")
        b = _item("b", RunItemStatus.DEP_FAILED, deps=["a"])
        run = _run("r1", [a, b])
        hub.prime([_run("r1", [_item("a", RunItemStatus.RUNNING),
                              _item("b", RunItemStatus.PENDING)])])
        q = hub.subscribe()

        hub.scan([run])
        kinds = [n["kind"] for n in _notifs(_drain(q))]
        assert kinds.count("task_failed") == 1  # only 'a', not 'b'


class TestRerun:
    @pytest.mark.asyncio
    async def test_rerun_reannounces_same_task_failure(self):
        # A rerun reuses the run id; a task that failed before and fails
        # again must be announced again, not suppressed.
        hub = NotificationHub()
        item = _item("a", RunItemStatus.FAILED, error="boom")
        run = _run("r1", [item])
        hub.prime([run])  # records the existing failure

        # Rerun: reset and reschedule.
        item.status = RunItemStatus.RUNNING
        item.error = None
        hub.run_scheduled(run)
        q = hub.subscribe()

        item.status = RunItemStatus.FAILED
        item.error = "boom again"
        hub.scan([run])

        failures = [n for n in _notifs(_drain(q)) if n["kind"] == "task_failed"]
        assert len(failures) == 1
        assert "boom again" in failures[0]["body"]


class TestRunDone:
    @pytest.mark.asyncio
    async def test_completed_transition_emits_success(self):
        hub = NotificationHub()
        item = _item("a", RunItemStatus.RUNNING)
        run = _run("r1", [item])
        hub.prime([run])  # seen RUNNING
        q = hub.subscribe()

        item.status = RunItemStatus.COMPLETED
        hub.scan([run])

        done = [n for n in _notifs(_drain(q)) if n["kind"] == "run_done"]
        assert len(done) == 1
        assert done[0]["level"] == "success"

    @pytest.mark.asyncio
    async def test_failed_run_emits_error_summary(self):
        hub = NotificationHub()
        a = _item("a", RunItemStatus.RUNNING)
        b = _item("b", RunItemStatus.COMPLETED)
        run = _run("r1", [a, b])
        hub.prime([run])
        q = hub.subscribe()

        a.status = RunItemStatus.FAILED  # run now terminal (FAILED)
        hub.scan([run])

        notifs = _notifs(_drain(q))
        done = [n for n in notifs if n["kind"] == "run_done"]
        assert len(done) == 1
        assert done[0]["level"] == "error"
        assert "1 done" in done[0]["body"] and "1 failed" in done[0]["body"]

    @pytest.mark.asyncio
    async def test_no_run_done_without_prior_active_state(self):
        # A run first observed already terminal (never seen active) must
        # not emit — guards against startup floods for historical runs.
        hub = NotificationHub()
        run = _run("r1", [_item("a", RunItemStatus.COMPLETED)])
        q = hub.subscribe()
        hub.scan([run])  # no prime, first sighting is terminal
        assert [n for n in _notifs(_drain(q)) if n["kind"] == "run_done"] == []


class TestStats:
    @pytest.mark.asyncio
    async def test_stats_count_only_active_runs(self):
        hub = NotificationHub()
        active = _run("r1", [
            _item("a", RunItemStatus.RUNNING),
            _item("b", RunItemStatus.PENDING),
            _item("c", RunItemStatus.COMPLETED),
            _item("d", RunItemStatus.FAILED),
        ])  # status RUNNING -> active
        finished = _run("r2", [_item("e", RunItemStatus.COMPLETED)])  # terminal
        q = hub.subscribe()

        hub.update_stats([active, finished])

        stats_events = [d for ev, d in _drain(q) if ev == "stats"]
        assert stats_events
        s = stats_events[-1]
        # Only the active run contributes.
        assert s == {"running": 1, "waiting": 1, "completed": 1,
                     "failed": 1, "active_runs": 1}
