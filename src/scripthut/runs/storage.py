"""Folder-based storage for runs, organized by workflow."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition

logger = logging.getLogger(__name__)


class RunStorageManager:
    """Manages per-run folder-based storage.

    Storage layout:
        <base_dir>/
          <workflow-name>/
            <YYYYMMDD_HHMMSS>_<run-id>/
              run.json
          _default_<backend>/
            <YYYY-Wnn>/
              run.json
    """

    RETENTION_DAYS = 30

    def __init__(self, base_dir: Path | None = None) -> None:
        """Initialize with base directory for run JSON files."""
        if base_dir is None:
            base_dir = Path.home() / ".cache" / "scripthut" / "workflows"
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._dirty_runs: set[str] = set()
        # Cache for loaded weekly runs (backend_name -> {week_id -> Run})
        self._weekly_cache: dict[str, dict[str, Run]] = {}

    # --- Path helpers ---

    def _sanitize_name(self, name: str) -> str:
        """Sanitize a workflow name for use as a directory name."""
        # Replace path separators with double-underscore
        return name.replace("/", "__")

    def _workflow_dir(self, workflow_name: str) -> Path:
        return self.base_dir / self._sanitize_name(workflow_name)

    def _run_dir_name(self, run_id: str, created_at: datetime) -> str:
        ts = created_at.strftime("%Y%m%d_%H%M%S")
        return f"{ts}_{run_id}"

    def _run_dir(self, run: Run) -> Path:
        wf_dir = self._workflow_dir(run.workflow_name)
        return wf_dir / self._run_dir_name(run.id, run.created_at)

    def _default_workflow_dir(self, backend_name: str) -> Path:
        return self.base_dir / f"_default_{self._sanitize_name(backend_name)}"

    def _weekly_run_dir(self, backend_name: str, dt: datetime) -> Path:
        year, week, _ = dt.isocalendar()
        week_id = f"{year}-W{week:02d}"
        return self._default_workflow_dir(backend_name) / week_id

    # --- Core CRUD ---

    def save_run(self, run: Run) -> None:
        """Write run.json atomically."""
        if run.workflow_name == "_default":
            # Weekly bins use a different directory structure
            # Determine the week from created_at
            run_dir = self._weekly_run_dir(run.backend_name, run.created_at)
        else:
            run_dir = self._run_dir(run)

        run_dir.mkdir(parents=True, exist_ok=True)
        run_path = run_dir / "run.json"
        temp_path = run_dir / "run.json.tmp"

        data: dict[str, Any] = {
            "version": 2,
            "id": run.id,
            "workflow_name": run.workflow_name,
            "backend_name": run.backend_name,
            "created_at": run.created_at.isoformat(),
            "max_concurrent": run.max_concurrent,
            "log_dir": run.log_dir,
            "account": run.account,
            "login_shell": run.login_shell,
            "commit_hash": run.commit_hash,
            "items": [item.to_dict() for item in run.items],
        }

        try:
            with open(temp_path, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(temp_path, run_path)
        except Exception as e:
            logger.error(f"Failed to save run '{run.id}': {e}")
            if temp_path.exists():
                temp_path.unlink()

    def load_run(self, run_dir: Path) -> Run | None:
        """Load a Run from a directory's run.json."""
        run_path = run_dir / "run.json"
        if not run_path.exists():
            return None

        try:
            with open(run_path, "r") as f:
                data = json.load(f)

            items = [RunItem.from_dict(item_data) for item_data in data.get("items", [])]

            return Run(
                id=data["id"],
                workflow_name=data["workflow_name"],
                backend_name=data.get("backend_name", data.get("cluster_name")),
                created_at=datetime.fromisoformat(data["created_at"]),
                items=items,
                max_concurrent=data.get("max_concurrent"),
                log_dir=data.get("log_dir", "~/.cache/scripthut/logs"),
                account=data.get("account"),
                login_shell=data.get("login_shell", False),
                commit_hash=data.get("commit_hash"),
            )
        except Exception as e:
            logger.error(f"Failed to load run from {run_dir}: {e}")
            return None

    def load_all_runs(self) -> dict[str, Run]:
        """Scan all workflow dirs and load all runs."""
        runs: dict[str, Run] = {}
        if not self.base_dir.exists():
            return runs

        for wf_dir in self.base_dir.iterdir():
            if not wf_dir.is_dir():
                continue
            for run_dir in wf_dir.iterdir():
                if not run_dir.is_dir():
                    continue
                run = self.load_run(run_dir)
                if run is not None:
                    runs[run.id] = run

        logger.info(f"Loaded {len(runs)} runs from storage")
        return runs

    def load_runs_for_workflow(self, workflow_name: str) -> list[Run]:
        """Load all runs for a specific workflow, sorted by created_at desc."""
        wf_dir = self._workflow_dir(workflow_name)
        if not wf_dir.exists():
            return []

        runs: list[Run] = []
        for run_dir in wf_dir.iterdir():
            if not run_dir.is_dir():
                continue
            run = self.load_run(run_dir)
            if run is not None:
                runs.append(run)

        return sorted(runs, key=lambda r: r.created_at, reverse=True)

    def list_workflows(self) -> list[str]:
        """List all workflow directory names."""
        if not self.base_dir.exists():
            return []
        return [
            d.name for d in self.base_dir.iterdir()
            if d.is_dir()
        ]

    def delete_run(self, run: Run) -> bool:
        """Delete a run's directory."""
        if run.workflow_name == "_default":
            run_dir = self._weekly_run_dir(run.backend_name, run.created_at)
        else:
            run_dir = self._run_dir(run)

        if run_dir.exists():
            shutil.rmtree(run_dir)
            logger.info(f"Deleted run directory: {run_dir}")
            return True
        return False

    # --- Dirty tracking ---

    def mark_dirty(self, run_id: str) -> None:
        """Mark a run as having unsaved changes."""
        self._dirty_runs.add(run_id)

    def save_if_dirty(self, runs: dict[str, Run]) -> None:
        """Save all dirty runs to disk."""
        if not self._dirty_runs:
            return
        for run_id in list(self._dirty_runs):
            if run_id in runs:
                self.save_run(runs[run_id])
        # Also save dirty weekly runs from cache
        for backend_runs in self._weekly_cache.values():
            for week_id, run in backend_runs.items():
                if run.id in self._dirty_runs:
                    self.save_run(run)
        self._dirty_runs.clear()

    # --- External job weekly binning ---

    def get_or_create_weekly_run(
        self, backend_name: str, dt: datetime
    ) -> Run:
        """Get existing weekly run for the ISO week containing dt, or create a new one."""
        year, week, _ = dt.isocalendar()
        week_id = f"{year}-W{week:02d}"

        # Check cache first
        if backend_name not in self._weekly_cache:
            self._weekly_cache[backend_name] = {}

        if week_id in self._weekly_cache[backend_name]:
            return self._weekly_cache[backend_name][week_id]

        # Try loading from disk
        run_dir = self._weekly_run_dir(backend_name, dt)
        run = self.load_run(run_dir)
        if run is not None:
            self._weekly_cache[backend_name][week_id] = run
            return run

        # Create new weekly run
        # Monday of the ISO week
        from datetime import date
        monday = date.fromisocalendar(year, week, 1)
        created_at = datetime(monday.year, monday.month, monday.day)

        run = Run(
            id=week_id,
            workflow_name="_default",
            backend_name=backend_name,
            created_at=created_at,
            items=[],
            max_concurrent=0,
        )

        self._weekly_cache[backend_name][week_id] = run
        return run

    def add_external_job(
        self,
        backend_name: str,
        slurm_job_id: str,
        name: str,
        user: str,
        state: str,
        partition: str = "",
        cpus: int = 1,
        memory: str = "",
        time_limit: str = "",
        submit_time: datetime | None = None,
        start_time: datetime | None = None,
        finish_time: datetime | None = None,
        cpu_efficiency: float | None = None,
        max_rss: str | None = None,
    ) -> None:
        """Add or update an external job in the appropriate weekly bin."""
        dt = submit_time or datetime.now()
        run = self.get_or_create_weekly_run(backend_name, dt)

        # Check if job already exists
        existing = run.get_item_by_slurm_id(slurm_job_id)
        if existing:
            # Update existing
            if state:
                try:
                    existing.status = RunItemStatus(state)
                except ValueError:
                    pass
            if start_time:
                existing.started_at = start_time
            if finish_time:
                existing.finished_at = finish_time
            if cpu_efficiency is not None:
                existing.cpu_efficiency = cpu_efficiency
            if max_rss is not None:
                existing.max_rss = max_rss
        else:
            # Create new item
            task = TaskDefinition(
                id=f"ext-{slurm_job_id}",
                name=name,
                command="",
                partition=partition,
                cpus=cpus,
                memory=memory,
                time_limit=time_limit,
            )
            status = RunItemStatus.RUNNING
            try:
                status = RunItemStatus(state)
            except ValueError:
                pass
            item = RunItem(
                task=task,
                status=status,
                slurm_job_id=slurm_job_id,
                submitted_at=submit_time,
                started_at=start_time,
                finished_at=finish_time,
                cpu_efficiency=cpu_efficiency,
                max_rss=max_rss,
            )
            run.items.append(item)

        self._dirty_runs.add(run.id)

    def remove_external_job(self, slurm_job_id: str) -> bool:
        """Remove an external job from its weekly bin. Returns True if found and removed."""
        for backend_runs in self._weekly_cache.values():
            for run in backend_runs.values():
                item = run.get_item_by_slurm_id(slurm_job_id)
                if item is not None:
                    run.items.remove(item)
                    self._dirty_runs.add(run.id)
                    return True
        # Not in cache — scan storage
        for wf_name in self.list_workflows():
            if not wf_name.startswith("_default_"):
                continue
            for run in self.load_runs_for_workflow(wf_name):
                item = run.get_item_by_slurm_id(slurm_job_id)
                if item is not None:
                    run.items.remove(item)
                    self.save_run(run)
                    return True
        return False

    # --- Cleanup ---

    def cleanup_old_runs(self) -> int:
        """Remove run directories older than retention period."""
        cutoff = datetime.now() - timedelta(days=self.RETENTION_DAYS)
        removed = 0

        if not self.base_dir.exists():
            return 0

        for wf_dir in self.base_dir.iterdir():
            if not wf_dir.is_dir():
                continue
            for run_dir in list(wf_dir.iterdir()):
                if not run_dir.is_dir():
                    continue
                run = self.load_run(run_dir)
                if run is None:
                    continue
                # Only remove terminal runs
                if run.status.value not in ("completed", "failed", "cancelled"):
                    continue
                if run.created_at < cutoff:
                    shutil.rmtree(run_dir)
                    removed += 1

            # Remove empty workflow directories
            if wf_dir.exists() and not any(wf_dir.iterdir()):
                wf_dir.rmdir()

        if removed:
            logger.info(f"Cleaned up {removed} old runs")
        return removed
