"""Tests for the append-only usage ledger behind the activity graph.

The ledger exists so the graph can outlive run retention: runs are deleted
after 30 days, these entries are not.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition
from scripthut.runs.storage import RunStorageManager
from scripthut.runs.usage import (
    UsageLog,
    merged_records,
    records_from_runs,
    window_start,
)

NOW = datetime(2026, 8, 14, 12, 0, tzinfo=timezone.utc)


def _item(task_id: str, status: RunItemStatus, *, cpus: int = 4,
          started: datetime | None = None, finished: datetime | None = None) -> RunItem:
    return RunItem(
        task=TaskDefinition(id=task_id, name=task_id, command="x", cpus=cpus),
        status=status,
        started_at=started if started is not None else NOW - timedelta(hours=2),
        finished_at=finished,
    )


def _run(run_id: str = "r1", items: list[RunItem] | None = None) -> Run:
    return Run(
        id=run_id,
        workflow_name="demo/train",
        backend_name="hpc",
        created_at=NOW,
        items=items if items is not None else [
            _item("a", RunItemStatus.COMPLETED, finished=NOW - timedelta(hours=1))
        ],
        max_concurrent=None,
        source_name="demo",
    )


class TestRecording:
    def test_appends_terminal_tasks(self, tmp_path: Path):
        log = UsageLog(tmp_path / "usage.jsonl")

        assert log.record([_run()]) == 1

        (rec,) = log.records()
        assert rec.run_id == "r1"
        assert rec.task_id == "a"
        assert rec.cpus == 4
        assert rec.source_name == "demo"
        assert rec.status == "completed"

    def test_failed_work_is_recorded_too(self, tmp_path: Path):
        """Failed jobs burned CPU; leaving them out would understate usage."""
        log = UsageLog(tmp_path / "usage.jsonl")
        run = _run(items=[
            _item("ok", RunItemStatus.COMPLETED, finished=NOW),
            _item("bad", RunItemStatus.FAILED, finished=NOW),
            _item("skipped", RunItemStatus.DEP_FAILED, finished=NOW),
        ])

        assert log.record([run]) == 3
        assert {r.status for r in log.records()} == {"completed", "failed", "dep_failed"}

    def test_unfinished_work_is_not_recorded_yet(self, tmp_path: Path):
        """Running tasks come from live runs; the ledger holds settled facts."""
        log = UsageLog(tmp_path / "usage.jsonl")
        run = _run(items=[
            _item("live", RunItemStatus.RUNNING),
            _item("queued", RunItemStatus.QUEUED),
            _item("waiting", RunItemStatus.PENDING),
        ])

        assert log.record([run]) == 0
        assert log.records() == []

    def test_never_started_tasks_are_skipped(self, tmp_path: Path):
        """DEP_FAILED work that never ran consumed nothing."""
        log = UsageLog(tmp_path / "usage.jsonl")
        item = _item("skipped", RunItemStatus.DEP_FAILED, finished=NOW)
        item.started_at = None
        run = _run(items=[item])

        assert log.record([run]) == 0

    def test_recording_is_idempotent(self, tmp_path: Path):
        """Startup and the hourly sweep both flush; neither may double-count."""
        log = UsageLog(tmp_path / "usage.jsonl")
        run = _run()

        assert log.record([run]) == 1
        assert log.record([run]) == 0
        assert len(log.records()) == 1

    def test_dedup_survives_a_restart(self, tmp_path: Path):
        """A fresh UsageLog must read existing keys, not re-append them."""
        path = tmp_path / "usage.jsonl"
        UsageLog(path).record([_run()])

        reopened = UsageLog(path)
        assert reopened.record([_run()]) == 0
        assert len(reopened.records()) == 1

    def test_appends_rather_than_rewrites(self, tmp_path: Path):
        log = UsageLog(tmp_path / "usage.jsonl")
        log.record([_run("r1")])
        log.record([_run("r2")])

        assert {r.run_id for r in log.records()} == {"r1", "r2"}

    def test_creates_the_parent_directory(self, tmp_path: Path):
        log = UsageLog(tmp_path / "nested" / "deeper" / "usage.jsonl")

        assert log.record([_run()]) == 1
        assert log.path.exists()


class TestReading:
    def test_missing_file_reads_empty(self, tmp_path: Path):
        assert UsageLog(tmp_path / "absent.jsonl").records() == []

    def test_a_corrupt_line_does_not_blank_the_graph(self, tmp_path: Path):
        path = tmp_path / "usage.jsonl"
        UsageLog(path).record([_run("good")])
        with open(path, "a") as f:
            f.write("{not json at all\n")

        records = UsageLog(path).records()
        assert [r.run_id for r in records] == ["good"]

    def test_since_filters_by_start_day(self, tmp_path: Path):
        log = UsageLog(tmp_path / "usage.jsonl")
        old = NOW - timedelta(days=100)
        log.record([_run("old", items=[
            _item("a", RunItemStatus.COMPLETED, started=old, finished=old + timedelta(hours=1))
        ])])
        log.record([_run("new")])

        recent = log.records(since=NOW.astimezone().date() - timedelta(days=7))
        assert [r.run_id for r in recent] == ["new"]


class TestMerging:
    def test_live_runs_override_the_ledger(self, tmp_path: Path):
        """Between finishing and the next flush a task is in both places."""
        log = UsageLog(tmp_path / "usage.jsonl")
        log.record([_run()])

        merged = merged_records(log, [_run()])

        assert len(merged) == 1

    def test_running_work_comes_from_live_runs(self, tmp_path: Path):
        log = UsageLog(tmp_path / "usage.jsonl")
        live = _run("live", items=[_item("t", RunItemStatus.RUNNING)])

        merged = merged_records(log, [live])

        assert [r.task_id for r in merged] == ["t"]
        assert merged[0].finished_at is None

    def test_history_outlives_the_runs_it_came_from(self, tmp_path: Path):
        """The point of the ledger: deleted runs still count on the graph."""
        log = UsageLog(tmp_path / "usage.jsonl")
        log.record([_run("deleted")])

        merged = merged_records(log, [])  # run no longer in memory or on disk

        assert [r.run_id for r in merged] == ["deleted"]

    def test_no_log_still_works(self):
        """CLI paths construct a RunManager without a ledger."""
        merged = merged_records(None, [_run()])

        assert len(merged) == 1


class TestRecordsFromRuns:
    def test_skips_tasks_that_never_started(self):
        run = _run(items=[_item("t", RunItemStatus.PENDING)])
        run.items[0].started_at = None

        assert list(records_from_runs([run])) == []

    def test_carries_provenance(self):
        (rec,) = list(records_from_runs([_run()]))

        assert rec.workflow_name == "demo/train"
        assert rec.source_name == "demo"
        assert rec.backend_name == "hpc"


class TestWindowStart:
    def test_inclusive_of_today(self):
        assert window_start(1, today=date(2026, 8, 14)) == date(2026, 8, 14)
        assert window_start(30, today=date(2026, 8, 14)) == date(2026, 7, 16)


class TestRetentionInteraction:
    def test_ledger_survives_run_cleanup(self, tmp_path: Path):
        """Flush-then-cleanup is the order the poll loop uses; verify it holds."""
        storage = RunStorageManager(base_dir=tmp_path / "workflows")
        log = UsageLog(tmp_path / "usage.jsonl")

        expired = timedelta(days=RunStorageManager.RETENTION_DAYS + 5)
        old_start = datetime.now(timezone.utc) - expired
        run = Run(
            id="ancient",
            workflow_name="demo/train",
            backend_name="hpc",
            created_at=old_start,
            items=[_item("a", RunItemStatus.COMPLETED, started=old_start,
                         finished=old_start + timedelta(hours=2))],
            max_concurrent=None,
            source_name="demo",
        )
        storage.save_run(run)

        log.record([run])
        removed = storage.cleanup_old_runs()

        assert removed == 1
        assert storage.load_all_runs() == {}
        assert [r.run_id for r in log.records()] == ["ancient"]
