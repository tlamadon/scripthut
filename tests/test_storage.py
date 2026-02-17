"""Tests for RunStorageManager folder-based storage."""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from scripthut.runs.models import (
    Run,
    RunItem,
    RunItemStatus,
    TaskDefinition,
)
from scripthut.runs.storage import RunStorageManager


def _make_run(
    run_id: str = "abc123",
    workflow_name: str = "test-workflow",
    backend_name: str = "test-cluster",
    items: list[RunItem] | None = None,
    created_at: datetime | None = None,
) -> Run:
    if items is None:
        task = TaskDefinition(id="t1", name="Task 1", command="echo hi")
        items = [RunItem(task=task, status=RunItemStatus.PENDING)]
    return Run(
        id=run_id,
        workflow_name=workflow_name,
        backend_name=backend_name,
        created_at=created_at or datetime.now(),
        items=items,
        max_concurrent=5,
    )


class TestSaveAndLoad:
    """Tests for save_run and load_run round-trip."""

    def test_save_and_load_roundtrip(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        run = _make_run()

        storage.save_run(run)
        loaded = storage.load_all_runs()

        assert run.id in loaded
        restored = loaded[run.id]
        assert restored.workflow_name == run.workflow_name
        assert restored.backend_name == run.backend_name
        assert len(restored.items) == 1
        assert restored.items[0].task.id == "t1"

    def test_save_creates_directory_structure(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        run = _make_run(workflow_name="my-project/sim")

        storage.save_run(run)

        # Workflow dir should use sanitized name
        wf_dir = tmp_path / "my-project__sim"
        assert wf_dir.exists()
        # Should have one run directory
        run_dirs = list(wf_dir.iterdir())
        assert len(run_dirs) == 1
        assert (run_dirs[0] / "run.json").exists()

    def test_load_nonexistent_returns_none(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        result = storage.load_run(tmp_path / "nonexistent")
        assert result is None

    def test_load_all_empty_dir(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        runs = storage.load_all_runs()
        assert runs == {}

    def test_utilization_fields_persist(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        task = TaskDefinition(id="t1", name="T1", command="echo")
        item = RunItem(
            task=task,
            status=RunItemStatus.COMPLETED,
            cpu_efficiency=85.5,
            max_rss="2.1G",
        )
        run = _make_run(items=[item])

        storage.save_run(run)
        loaded = storage.load_all_runs()

        restored_item = loaded[run.id].items[0]
        assert restored_item.cpu_efficiency == 85.5
        assert restored_item.max_rss == "2.1G"


class TestDeleteRun:
    """Tests for delete_run."""

    def test_delete_removes_directory(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        run = _make_run()

        storage.save_run(run)
        assert storage.load_all_runs()  # Confirm it was saved

        result = storage.delete_run(run)
        assert result is True
        assert storage.load_all_runs() == {}

    def test_delete_nonexistent_returns_false(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        run = _make_run()
        result = storage.delete_run(run)
        assert result is False


class TestDirtyTracking:
    """Tests for dirty tracking and save_if_dirty."""

    def test_mark_dirty_and_save(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        run = _make_run()

        # Save initial state
        storage.save_run(run)

        # Modify the run
        run.items[0].status = RunItemStatus.COMPLETED

        # Mark dirty and save
        storage.mark_dirty(run.id)
        storage.save_if_dirty({run.id: run})

        # Reload and verify
        loaded = storage.load_all_runs()
        assert loaded[run.id].items[0].status == RunItemStatus.COMPLETED

    def test_save_if_dirty_clears_dirty_set(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        run = _make_run()
        storage.save_run(run)

        storage.mark_dirty(run.id)
        assert run.id in storage._dirty_runs

        storage.save_if_dirty({run.id: run})
        assert len(storage._dirty_runs) == 0


class TestWeeklyBins:
    """Tests for external job weekly binning."""

    def test_get_or_create_weekly_run(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        dt = datetime(2026, 2, 11, 10, 0, 0)  # A Wednesday

        run = storage.get_or_create_weekly_run("midway", dt)

        assert run.workflow_name == "_default"
        assert run.backend_name == "midway"
        assert "W07" in run.id  # ISO week 7 of 2026

    def test_weekly_run_cached(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        dt = datetime(2026, 2, 11, 10, 0, 0)

        run1 = storage.get_or_create_weekly_run("midway", dt)
        run2 = storage.get_or_create_weekly_run("midway", dt)

        assert run1 is run2  # Same object from cache

    def test_add_external_job(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        submit_time = datetime(2026, 2, 11, 10, 0, 0)

        storage.add_external_job(
            backend_name="midway",
            slurm_job_id="99999",
            name="external-job",
            user="testuser",
            state="running",
            submit_time=submit_time,
        )

        run = storage.get_or_create_weekly_run("midway", submit_time)
        assert len(run.items) == 1
        assert run.items[0].slurm_job_id == "99999"
        assert run.items[0].task.name == "external-job"

    def test_add_external_job_updates_existing(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        submit_time = datetime(2026, 2, 11, 10, 0, 0)

        storage.add_external_job(
            backend_name="midway",
            slurm_job_id="99999",
            name="my-job",
            user="testuser",
            state="running",
            submit_time=submit_time,
        )
        storage.add_external_job(
            backend_name="midway",
            slurm_job_id="99999",
            name="my-job",
            user="testuser",
            state="completed",
            submit_time=submit_time,
            cpu_efficiency=90.0,
            max_rss="1.5G",
        )

        run = storage.get_or_create_weekly_run("midway", submit_time)
        assert len(run.items) == 1  # Not duplicated
        assert run.items[0].status == RunItemStatus.COMPLETED
        assert run.items[0].cpu_efficiency == 90.0
        assert run.items[0].max_rss == "1.5G"


class TestReconcileExternalJobs:
    """Tests for reconcile_external_jobs."""

    def test_marks_stale_running_jobs_as_completed(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        submit_time = datetime(2026, 2, 11, 10, 0, 0)

        # Add two running external jobs
        storage.add_external_job(
            backend_name="midway", slurm_job_id="100",
            name="job-a", user="u", state="running", submit_time=submit_time,
        )
        storage.add_external_job(
            backend_name="midway", slurm_job_id="200",
            name="job-b", user="u", state="running", submit_time=submit_time,
        )

        # Only job 200 is still in squeue
        reconciled = storage.reconcile_external_jobs("midway", {"200"})

        assert reconciled == 1
        run = storage.get_or_create_weekly_run("midway", submit_time)
        statuses = {i.slurm_job_id: i.status for i in run.items}
        assert statuses["100"] == RunItemStatus.COMPLETED
        assert statuses["200"] == RunItemStatus.RUNNING

    def test_ignores_already_terminal_jobs(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        submit_time = datetime(2026, 2, 11, 10, 0, 0)

        storage.add_external_job(
            backend_name="midway", slurm_job_id="100",
            name="done-job", user="u", state="completed", submit_time=submit_time,
        )
        storage.add_external_job(
            backend_name="midway", slurm_job_id="200",
            name="fail-job", user="u", state="failed", submit_time=submit_time,
        )

        reconciled = storage.reconcile_external_jobs("midway", set())
        assert reconciled == 0

    def test_reconciles_across_weeks(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        week1 = datetime(2026, 2, 4, 10, 0, 0)   # W06
        week2 = datetime(2026, 2, 11, 10, 0, 0)  # W07

        storage.add_external_job(
            backend_name="midway", slurm_job_id="100",
            name="old-job", user="u", state="running", submit_time=week1,
        )
        storage.add_external_job(
            backend_name="midway", slurm_job_id="200",
            name="new-job", user="u", state="running", submit_time=week2,
        )

        # Save to disk so reconcile finds them
        storage.save_if_dirty({})

        # Neither job is in squeue anymore
        reconciled = storage.reconcile_external_jobs("midway", set())
        assert reconciled == 2

    def test_reconciles_from_disk_not_just_cache(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        submit_time = datetime(2026, 2, 11, 10, 0, 0)

        storage.add_external_job(
            backend_name="midway", slurm_job_id="100",
            name="cached-job", user="u", state="running", submit_time=submit_time,
        )
        storage.save_if_dirty({})

        # Fresh storage instance — empty cache, but data on disk
        storage2 = RunStorageManager(base_dir=tmp_path)
        reconciled = storage2.reconcile_external_jobs("midway", set())
        assert reconciled == 1


class TestCleanup:
    """Tests for cleanup_old_runs."""

    def test_cleanup_removes_old_terminal_runs(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)

        old_run = _make_run(
            run_id="old-run",
            items=[RunItem(
                task=TaskDefinition(id="t", name="T", command="echo"),
                status=RunItemStatus.COMPLETED,
            )],
            created_at=datetime.now() - timedelta(days=60),
        )
        storage.save_run(old_run)

        removed = storage.cleanup_old_runs()
        assert removed == 1
        assert storage.load_all_runs() == {}

    def test_cleanup_keeps_recent_runs(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)

        recent_run = _make_run(
            run_id="recent",
            items=[RunItem(
                task=TaskDefinition(id="t", name="T", command="echo"),
                status=RunItemStatus.COMPLETED,
            )],
            created_at=datetime.now() - timedelta(days=5),
        )
        storage.save_run(recent_run)

        removed = storage.cleanup_old_runs()
        assert removed == 0
        assert "recent" in storage.load_all_runs()

    def test_cleanup_keeps_active_runs(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)

        active_run = _make_run(
            run_id="active",
            items=[RunItem(
                task=TaskDefinition(id="t", name="T", command="echo"),
                status=RunItemStatus.RUNNING,
            )],
            created_at=datetime.now() - timedelta(days=60),
        )
        storage.save_run(active_run)

        removed = storage.cleanup_old_runs()
        assert removed == 0
        assert "active" in storage.load_all_runs()


class TestListWorkflows:
    """Tests for list_workflows."""

    def test_lists_workflow_dirs(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        run1 = _make_run(run_id="r1", workflow_name="project-a/sim")
        run2 = _make_run(run_id="r2", workflow_name="project-b/analysis")

        storage.save_run(run1)
        storage.save_run(run2)

        workflows = sorted(storage.list_workflows())
        assert workflows == ["project-a__sim", "project-b__analysis"]

    def test_empty_storage(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        assert storage.list_workflows() == []
