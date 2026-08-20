"""Submit path for ``type: sync``: upload item, dest guards, local backend e2e."""

from __future__ import annotations

import asyncio
import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from scripthut.backends.local import LocalBackend, LocalExecClient, local_backend_supported
from scripthut.config_schema import (
    AWSBatchConfig,
    BatchBackendConfig,
    LocalBackendConfig,
    ScriptHutConfig,
    SyncSourceConfig,
)
from scripthut.runs.manager import RunManager
from scripthut.runs.models import RunItemStatus, RunStatus
from scripthut.runs.storage import RunStorageManager
from scripthut.runs.sync import SYNC_RETURN_ID, SYNC_UPLOAD_ID

pytestmark = pytest.mark.skipif(
    not local_backend_supported(),
    reason="local backend requires a POSIX shell (skipped on Windows)",
)


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.email=t@e.st", "-c", "user.name=t", *args],
        cwd=repo, check=True, capture_output=True,
    )


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    (repo / "keep.py").write_text("v1")
    (repo / ".gitignore").write_text("skip.dta\n")
    (repo / "skip.dta").write_text("data")
    (repo / "untracked.py").write_text("nope")
    out = repo / "output"
    out.mkdir()
    (out / "table.csv").write_text("stale")
    _git(repo, "add", "keep.py", ".gitignore", "output")
    (repo / "keep.py").write_text("v2-dirty")
    wf = repo / ".hut" / "workflows"
    wf.mkdir(parents=True)
    (wf / "run.json").write_text(json.dumps({
        "tasks": [{
            "id": "see",
            "name": "see",
            "command": (
                "cat keep.py > seen.txt && "
                "mkdir -p output && echo from-cluster > output/result.txt"
            ),
        }],
    }))
    return repo


def _mgr(tmp_path: Path, source: SyncSourceConfig, *, backends=None, job_backends=None):
    exec_client = LocalExecClient()
    backend = LocalBackend("local", spool_dir=tmp_path / "spool")
    cfg = ScriptHutConfig(
        backends=[LocalBackendConfig(
            name="local", max_concurrent=4, sync_dir=str(tmp_path / "scripthut-sync"),
        )],
        sources=[source],
    )
    mgr = RunManager(
        config=cfg,
        backends=backends if backends is not None else {"local": exec_client},
        storage=RunStorageManager(tmp_path / "workflows"),
        job_backends=job_backends if job_backends is not None else {"local": backend},
    )
    return mgr, backend, exec_client


async def _drive(mgr: RunManager, backend: LocalBackend, run, timeout=30.0):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        jobs = await backend.get_jobs()
        await mgr.update_run_status(run, {j.job_id: j.state for j in jobs})
        settling = [
            i for i in run.items
            if i.status == RunItemStatus.SETTLING and i.job_id
        ]
        if settling:
            stats = await backend.get_job_stats([i.job_id for i in settling])
            for item in settling:
                s = stats.get(item.job_id)
                if s is None:
                    continue
                item.exit_code = s.exit_code
                item.scheduler_state = s.state
                item.finished_at = s.end_time
                if s.state == "COMPLETED":
                    item.status = RunItemStatus.COMPLETED
                    await mgr._after_item_completed(run, item)
                elif s.state in backend.failure_states:
                    item.status = RunItemStatus.FAILED
                    item.error = f"Scheduler: {backend.failure_states[s.state]}"
            await mgr.process_run(run)
        if run.status in (RunStatus.COMPLETED, RunStatus.FAILED):
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(
        f"run stuck in {run.status}: "
        f"{[(i.task.id, i.status, i.error) for i in run.items]}"
    )


def _wf_json(repo: Path) -> str:
    return (repo / ".hut" / "workflows" / "run.json").read_text()


class TestCreateRunFromSyncSource:
    @pytest.mark.asyncio
    async def test_dirty_tree_is_what_the_task_sees(self, tmp_path: Path):
        repo = _repo(tmp_path)
        dest = tmp_path / "cluster-copy"
        src = SyncSourceConfig(
            name="wl", path=repo, backend="local", dest=str(dest),
        )
        mgr, backend, _ = _mgr(tmp_path, src)
        run = await mgr.create_run_from_source("wl", "run.json", _wf_json(repo), "local")
        await _drive(mgr, backend, run)

        assert run.status == RunStatus.COMPLETED
        assert run.commit_hash is None
        upload = run.get_item_by_task_id(SYNC_UPLOAD_ID)
        assert upload is not None
        assert upload.status == RunItemStatus.COMPLETED
        ret = run.get_item_by_task_id(SYNC_RETURN_ID)
        assert ret is not None
        assert ret.status == RunItemStatus.COMPLETED
        assert (dest / "keep.py").read_text() == "v2-dirty"
        assert (dest / "seen.txt").read_text().strip() == "v2-dirty"
        assert not (dest / "skip.dta").exists()
        assert not (dest / "untracked.py").exists()
        assert not (dest / ".git").exists()
        # Pull wrote the cluster result. It overwrites and adds; it never
        # deletes, so a local-only file in output/ survives.
        assert (repo / "output" / "result.txt").read_text().strip() == "from-cluster"
        assert (repo / "output" / "table.csv").read_text() == "stale"

    @pytest.mark.asyncio
    async def test_api_only_backend_refused(self, tmp_path: Path):
        repo = _repo(tmp_path)
        src = SyncSourceConfig(name="wl", path=repo, backend="batch")
        cfg = ScriptHutConfig(
            backends=[BatchBackendConfig(
                name="batch",
                aws=AWSBatchConfig(region="us-east-1", job_queue="q"),
                job_definition="jd",
            )],
            sources=[src],
        )
        mgr = RunManager(
            config=cfg,
            backends={},
            storage=RunStorageManager(tmp_path / "workflows"),
            job_backends={"batch": MagicMock()},
        )
        with pytest.raises(ValueError, match="no filesystem"):
            await mgr.create_run_from_source("wl", "run.json", _wf_json(repo), "batch")

    @pytest.mark.asyncio
    async def test_overlapping_dest_refused(self, tmp_path: Path):
        repo = _repo(tmp_path)
        dest = tmp_path / "cluster-copy"
        src = SyncSourceConfig(
            name="wl", path=repo, backend="local", dest=str(dest),
        )
        mgr, _backend, exec_client = _mgr(tmp_path, src)

        async def hang(*_a, **_k):
            await asyncio.Event().wait()

        exec_client.put_files = hang
        run1 = await mgr.create_run_from_source(
            "wl", "run.json", _wf_json(repo), "local",
        )
        try:
            with pytest.raises(ValueError, match="in use"):
                await mgr.create_run_from_source(
                    "wl", "run.json", _wf_json(repo), "local",
                )
        finally:
            await mgr.cancel_run(run1.id)

    @pytest.mark.asyncio
    async def test_local_dest_equal_to_source_path_refused(self, tmp_path: Path):
        repo = _repo(tmp_path)
        src = SyncSourceConfig(
            name="wl", path=repo, backend="local", dest=str(repo.resolve()),
        )
        mgr, _, _ = _mgr(tmp_path, src)
        with pytest.raises(ValueError, match="overlaps the source path"):
            await mgr.create_run_from_source("wl", "run.json", _wf_json(repo), "local")

    @pytest.mark.asyncio
    async def test_local_dest_inside_source_path_refused(self, tmp_path: Path):
        """dest that is a subdirectory of the repo also triggers the guard."""
        repo = _repo(tmp_path)
        src = SyncSourceConfig(
            name="wl", path=repo, backend="local",
            dest=str((repo / "subdir").resolve()),
        )
        mgr, _, _ = _mgr(tmp_path, src)
        with pytest.raises(ValueError, match="overlaps the source path"):
            await mgr.create_run_from_source("wl", "run.json", _wf_json(repo), "local")

    @pytest.mark.asyncio
    async def test_stale_remote_file_removed_on_second_run(self, tmp_path: Path):
        """A file deleted from the repo is gone from dest after the next upload."""
        repo = _repo(tmp_path)
        dest = tmp_path / "cluster-copy"
        src = SyncSourceConfig(
            name="wl", path=repo, backend="local", dest=str(dest),
        )
        # First run: keep.py is tracked, so it lands in dest.
        mgr, backend, _ = _mgr(tmp_path, src)
        tasks_json = json.dumps({"tasks": [{"id": "noop", "name": "noop", "command": "true"}]})
        run1 = await mgr.create_run_from_source("wl", "run.json", tasks_json, "local")
        await _drive(mgr, backend, run1)
        assert (dest / "keep.py").exists()

        # Remove the file from git between runs (-f because the working tree
        # copy is dirty — it was never re-committed after v2-dirty was written).
        _git(repo, "rm", "-f", "keep.py")

        # Second run: keep.py must not remain at dest.
        run2 = await mgr.create_run_from_source("wl", "run.json", tasks_json, "local")
        await _drive(mgr, backend, run2)
        assert run2.status == RunStatus.COMPLETED
        assert not (dest / "keep.py").exists()

    @pytest.mark.asyncio
    async def test_missing_remote_output_leaves_local_output_alone(
        self, tmp_path: Path
    ):
        """A run that writes no output/ must not touch the laptop's output/.

        The cluster's output/ is absent unless a task creates it (the upload
        excludes the return dir and the publish step replaces dest), so this
        is the ordinary shape of a run whose tasks produce nothing — not an
        error path. It must never be read as "delete the local copy".
        """
        repo = _repo(tmp_path)
        dest = tmp_path / "cluster-copy"
        src = SyncSourceConfig(
            name="wl", path=repo, backend="local", dest=str(dest),
        )
        mgr, backend, _ = _mgr(tmp_path, src)
        tasks_json = json.dumps(
            {"tasks": [{"id": "quiet", "name": "quiet", "command": "true"}]}
        )
        run = await mgr.create_run_from_source("wl", "run.json", tasks_json, "local")
        await _drive(mgr, backend, run)

        assert run.status == RunStatus.COMPLETED
        ret = run.get_item_by_task_id(SYNC_RETURN_ID)
        assert ret is not None
        assert ret.status == RunItemStatus.COMPLETED
        assert not (dest / "output").exists()
        # The pre-existing local file is still there.
        assert (repo / "output" / "table.csv").read_text() == "stale"

    @pytest.mark.asyncio
    async def test_pull_runs_after_failed_task(self, tmp_path: Path):
        repo = _repo(tmp_path)
        dest = tmp_path / "cluster-copy"
        src = SyncSourceConfig(
            name="wl", path=repo, backend="local", dest=str(dest),
        )
        mgr, backend, _ = _mgr(tmp_path, src)
        tasks_json = json.dumps({
            "tasks": [{
                "id": "boom",
                "name": "boom",
                "command": (
                    "mkdir -p output && echo fail-out > output/x.txt; exit 1"
                ),
            }],
        })
        run = await mgr.create_run_from_source("wl", "run.json", tasks_json, "local")
        await _drive(mgr, backend, run)

        assert run.status == RunStatus.FAILED
        ret = run.get_item_by_task_id(SYNC_RETURN_ID)
        assert ret is not None
        assert ret.status == RunItemStatus.COMPLETED
        assert (repo / "output" / "x.txt").read_text().strip() == "fail-out"
        assert (repo / "output" / "table.csv").read_text() == "stale"

    @pytest.mark.asyncio
    async def test_cancel_still_pulls_output(self, tmp_path: Path):
        import time

        repo = _repo(tmp_path)
        dest = tmp_path / "cluster-copy"
        src = SyncSourceConfig(
            name="wl", path=repo, backend="local", dest=str(dest),
        )
        mgr, backend, _ = _mgr(tmp_path, src)
        tasks_json = json.dumps({
            "tasks": [{
                "id": "slow",
                "name": "slow",
                "command": (
                    "mkdir -p output && echo cancelled-out > output/c.txt "
                    "&& sleep 60"
                ),
            }],
        })
        run = await mgr.create_run_from_source(
            "wl", "run.json", tasks_json, "local",
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if (dest / "output" / "c.txt").exists():
                break
            jobs = await backend.get_jobs()
            await mgr.update_run_status(
                run, {j.job_id: j.state for j in jobs},
            )
            await asyncio.sleep(0.05)
        else:
            raise TimeoutError("task never wrote output before cancel")
        await mgr.cancel_run(run.id)
        await _drive(mgr, backend, run)
        ret = run.get_item_by_task_id(SYNC_RETURN_ID)
        assert ret is not None
        assert ret.status == RunItemStatus.COMPLETED
        assert (repo / "output" / "c.txt").read_text().strip() == "cancelled-out"
