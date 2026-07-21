"""Tests for the versioned per-task manifest (scripthut.runs.manifest).

The manifest is the explicit deliverable: "these exact input hashes,
through this command, produced these exact output hashes" — verifiable
from the document alone. Pinned here:

- **Schema shape** (manifest_version 1) and null semantics.
- **Verifiability**: hashes in the manifest equal independently computed
  sha256 digests of the actual files.
- **Executor equivalence**: the same task run via the local backend and
  via a (mocked) Slurm backend yields equal manifests modulo the
  executor/timing/run-identity fields.
- **Cache symmetry**: a cache hit reports the same output hashes the
  original miss run did (carried through the stored cache manifest).
- **API exposure**: manifests ride on run results and the dedicated
  endpoint; non-terminal tasks are refused with 409.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripthut.api import make_api_router
from scripthut.runs.manifest import TASK_MANIFEST_VERSION, build_task_manifest
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition
from scripthut.backends.local import local_backend_supported
from tests.test_local_backend import (
    _FAKE_RCLONE,
    _drive,
    _make_runtimeish,
)

# Classes below that execute tasks for real ride the local backend,
# which is POSIX-only; the schema/API tests run everywhere.
_posix_only = pytest.mark.skipif(
    not local_backend_supported(),
    reason="real execution rides the POSIX-only local backend",
)


def _sha256(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def fake_store(tmp_path, monkeypatch):
    """PATH-shimmed rclone over a local directory (see test_local_backend).

    No sha256sum shim — content hashing must work with the host's own
    tools (sha256sum, or the shasum fallback on stock macOS)."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    rclone = bindir / "rclone"
    rclone.write_text(_FAKE_RCLONE)
    rclone.chmod(rclone.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{bindir}:{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_RCLONE_ROOT", str(tmp_path / "store"))
    return tmp_path / "store"


# ---------------------------------------------------------------------------
# Schema shape
# ---------------------------------------------------------------------------


class TestManifestShape:
    def _run_item(self):
        task = TaskDefinition(
            id="t", name="Train", command="python train.py",
            working_dir="/work", inputs=["in.txt"], outputs=["out.txt"],
            cache_scope="inputs",
        )
        item = RunItem(
            task=task, status=RunItemStatus.COMPLETED,
            job_id="42", exit_code=0,
            submitted_at=datetime(2026, 7, 21, 10, 0, 0, tzinfo=UTC),
            started_at=datetime(2026, 7, 21, 10, 0, 5, tzinfo=UTC),
            finished_at=datetime(2026, 7, 21, 10, 1, 5, tzinfo=UTC),
            cache_key="k" * 64, cache_hit=False,
            input_hashes={"in.txt": "a" * 64},
            output_hashes={"out.txt": "b" * 64},
        )
        run = Run(
            id="r1", workflow_name="wf", backend_name="cluster",
            created_at=datetime(2026, 7, 21, tzinfo=UTC), items=[item],
            max_concurrent=None, commit_hash="abc123", git_branch="main",
        )
        return run, item

    def test_full_shape(self):
        run, item = self._run_item()
        m = build_task_manifest(run, item, backend_type="slurm")
        assert m == {
            "manifest_version": TASK_MANIFEST_VERSION,
            "task": {
                "id": "t", "name": "Train", "command": "python train.py",
                "working_dir": "/work",
            },
            "inputs": {"in.txt": "a" * 64},
            "outputs": {"out.txt": "b" * 64},
            "cache": {"key": "k" * 64, "scope": "inputs", "hit": False},
            "executor": {"backend": "cluster", "type": "slurm", "job_id": "42"},
            "run": {"id": "r1", "workflow": "wf", "commit": "abc123",
                    "branch": "main"},
            "exit_code": 0,
            "timing": {
                "submitted_at": "2026-07-21T10:00:00+00:00",
                "started_at": "2026-07-21T10:00:05+00:00",
                "finished_at": "2026-07-21T10:01:05+00:00",
                "duration_seconds": 60.0,
            },
        }

    def test_nulls_when_not_hashed(self):
        run, item = self._run_item()
        item.input_hashes = None
        item.output_hashes = None
        item.started_at = None
        item.finished_at = None
        m = build_task_manifest(run, item)
        assert m["inputs"] is None       # "not hashed", never "empty and fine"
        assert m["outputs"] is None
        assert m["timing"]["duration_seconds"] is None
        assert m["executor"]["type"] is None

    def test_json_serializable(self):
        run, item = self._run_item()
        json.dumps(build_task_manifest(run, item))  # must not raise

    def test_persistence_roundtrip(self):
        _, item = self._run_item()
        rt = RunItem.from_dict(item.to_dict())
        assert rt.input_hashes == {"in.txt": "a" * 64}
        assert rt.output_hashes == {"out.txt": "b" * 64}

    def test_old_run_item_loads_without_hash_fields(self):
        item = RunItem.from_dict({
            "task": {"id": "t", "name": "t", "command": "true"},
            "status": "completed",
        })
        assert item.input_hashes is None
        assert item.output_hashes is None


# ---------------------------------------------------------------------------
# Verifiability: manifest hashes == real file digests (local backend, no cache)
# ---------------------------------------------------------------------------


@_posix_only
class TestManifestVerifiability:
    @pytest.mark.asyncio
    async def test_hashes_match_actual_files(self, tmp_path):
        from scripthut.runs.models import RunStatus

        workdir = tmp_path / "work"
        workdir.mkdir()
        (workdir / "in.txt").write_text("the input\n")

        mgr, backend, _ = _make_runtimeish(tmp_path)  # cache disabled
        task = TaskDefinition(
            id="t", name="t",
            command="printf 'the result\\n' > out.txt",
            working_dir=str(workdir),
            inputs=["in.txt"], outputs=["out.txt"],
        )
        run = await mgr._build_run(
            [task], "wf", "local",
            max_concurrent=None, ssh_client=mgr.get_ssh_client("local"),
        )
        await _drive(mgr, backend, run)
        assert run.status == RunStatus.COMPLETED

        m = mgr.get_task_manifest(run, run.items[0])
        # A downstream consumer verifying from the manifest alone:
        assert m["inputs"] == {"in.txt": _sha256(workdir / "in.txt")}
        assert m["outputs"] == {"out.txt": _sha256(workdir / "out.txt")}
        assert m["exit_code"] == 0
        assert m["executor"] == {
            "backend": "local", "type": "local",
            "job_id": run.items[0].job_id,
        }
        assert m["timing"]["duration_seconds"] is not None


# ---------------------------------------------------------------------------
# Executor equivalence: local vs (mocked) Slurm
# ---------------------------------------------------------------------------


def _normalize(manifest: dict) -> dict:
    """Strip the fields allowed to differ across executors: executor
    identity, timing, scheduler job id, and run identity."""
    m = json.loads(json.dumps(manifest))
    m.pop("executor")
    m.pop("timing")
    m["run"].pop("id")
    return m


@_posix_only
class TestExecutorEquivalence:
    @pytest.mark.asyncio
    async def test_local_and_remote_manifests_equivalent(self, tmp_path):
        from scripthut.runs.models import RunStatus
        from tests.test_probe import _FakeBackend, _manager

        workdir = tmp_path / "work"
        workdir.mkdir()
        (workdir / "in.txt").write_text("shared input\n")

        def task():
            return TaskDefinition(
                id="t", name="t",
                command="printf 'shared result\\n' > out.txt",
                working_dir=str(workdir),
                inputs=["in.txt"], outputs=["out.txt"],
            )

        # --- Local: real execution.
        mgr_l, backend_l, _ = _make_runtimeish(tmp_path)
        run_l = await mgr_l._build_run(
            [task()], "wf", "local",
            max_concurrent=None, ssh_client=mgr_l.get_ssh_client("local"),
        )
        await _drive(mgr_l, backend_l, run_l)
        assert run_l.status == RunStatus.COMPLETED
        manifest_local = mgr_l.get_task_manifest(run_l, run_l.items[0])

        # --- "Remote": mocked Slurm whose hash responses report the same
        # files (the hashes the cluster-side sha256sum would print).
        in_hash = _sha256(workdir / "in.txt")
        out_hash = _sha256(workdir / "out.txt")

        async def run_command(cmd, timeout=30):
            if "sha256sum" in cmd and "in.txt" in cmd:
                return (f"{in_hash}  in.txt\n", "", 0)
            if "sha256sum" in cmd and "out.txt" in cmd:
                return (f"{out_hash}  out.txt\n", "", 0)
            return ("", "", 0)

        ssh = MagicMock()
        ssh.run_command = AsyncMock(side_effect=run_command)
        mgr_r = _manager(ssh, cache_enabled=False)
        mgr_r._persist_run = MagicMock()
        item_r = RunItem(task=task(), status=RunItemStatus.PENDING)
        run_r = Run(
            id="rr", workflow_name="wf", backend_name="cluster",
            created_at=datetime.now(UTC), items=[item_r],
            max_concurrent=None, log_dir="/logs",
        )
        mgr_r.runs[run_r.id] = run_r
        await mgr_r.submit_task(run_r, item_r)
        # Simulate the poller's completion resolution.
        item_r.status = RunItemStatus.COMPLETED
        item_r.exit_code = 0
        item_r.started_at = datetime.now(UTC)
        item_r.finished_at = item_r.started_at + timedelta(seconds=1)
        await mgr_r._after_item_completed(run_r, item_r)
        manifest_remote = mgr_r.get_task_manifest(run_r, item_r)

        assert manifest_local["executor"]["type"] == "local"
        assert manifest_remote["executor"]["type"] == "slurm"
        assert _normalize(manifest_local) == _normalize(manifest_remote)


# ---------------------------------------------------------------------------
# Cache symmetry: hit reports the miss run's output hashes
# ---------------------------------------------------------------------------


@_posix_only
class TestCacheSymmetry:
    @pytest.mark.asyncio
    async def test_hit_manifest_matches_miss_manifest(
        self, tmp_path, fake_store,
    ):
        from scripthut.runs.models import RunStatus

        workdir = tmp_path / "work"
        workdir.mkdir()
        (workdir / "in.txt").write_text("input data\n")

        def task():
            return TaskDefinition(
                id="t", name="t",
                command="printf 'result\\n' > out.txt",
                working_dir=str(workdir),
                inputs=["in.txt"], outputs=["out.txt"],
            )

        mgr1, backend1, _ = _make_runtimeish(tmp_path, cache_store="store:cache")
        run1 = await mgr1._build_run(
            [task()], "wf", "local",
            max_concurrent=None, ssh_client=mgr1.get_ssh_client("local"),
        )
        await _drive(mgr1, backend1, run1)
        assert run1.status == RunStatus.COMPLETED
        miss = mgr1.get_task_manifest(run1, run1.items[0])
        assert miss["cache"]["hit"] is False
        assert miss["outputs"] == {"out.txt": _sha256(workdir / "out.txt")}

        # The stored cache manifest carries the per-file output hashes.
        ac_dir = fake_store / "cache" / "ac"
        [ac_file] = list(ac_dir.glob("*.json"))
        stored = json.loads(ac_file.read_text())
        assert stored["output_hashes"] == miss["outputs"]

        # Second run: cache hit; same inputs/outputs/key in the manifest.
        mgr2, _backend2, _ = _make_runtimeish(
            tmp_path / "second", cache_store="store:cache",
        )
        run2 = await mgr2._build_run(
            [task()], "wf", "local",
            max_concurrent=None, ssh_client=mgr2.get_ssh_client("local"),
        )
        assert run2.status == RunStatus.COMPLETED
        hit = mgr2.get_task_manifest(run2, run2.items[0])
        assert hit["cache"]["hit"] is True
        assert hit["cache"]["key"] == miss["cache"]["key"]
        assert hit["inputs"] == miss["inputs"]
        assert hit["outputs"] == miss["outputs"]

        # Probe reports the same hashes without running anything.
        [verdict] = await mgr2.probe_tasks([task()], "local")
        assert verdict["hit"] is True
        assert verdict["output_hashes"] == miss["outputs"]


# ---------------------------------------------------------------------------
# API exposure
# ---------------------------------------------------------------------------


def _api_client_with_run(run, mgr):
    state = MagicMock()
    state.run_manager = mgr
    state.config_error = None
    state.backends = {}
    state.config = None
    state.notify_poll = MagicMock()
    app = FastAPI()
    app.include_router(make_api_router(state))
    return TestClient(app)


def _mgr_with_run(tmp_path, run):
    mgr, _backend, _ = _make_runtimeish(tmp_path)
    mgr.runs[run.id] = run
    return mgr


def _terminal_run():
    task = TaskDefinition(
        id="t", name="t", command="true",
        inputs=["in.txt"], outputs=["out.txt"],
    )
    item = RunItem(
        task=task, status=RunItemStatus.COMPLETED, job_id="1",
        exit_code=0,
        input_hashes={"in.txt": "a" * 64},
        output_hashes={"out.txt": "b" * 64},
    )
    running = RunItem(
        task=TaskDefinition(id="t2", name="t2", command="true"),
        status=RunItemStatus.RUNNING, job_id="2",
    )
    return Run(
        id="r1", workflow_name="wf", backend_name="local",
        created_at=datetime(2026, 7, 21, tzinfo=UTC),
        items=[item, running], max_concurrent=None,
    )


class TestManifestAPI:
    def test_manifest_endpoint(self, tmp_path):
        run = _terminal_run()
        client = _api_client_with_run(run, _mgr_with_run(tmp_path, run))
        resp = client.get("/api/v1/runs/r1/tasks/t/manifest")
        assert resp.status_code == 200
        m = resp.json()
        assert m["manifest_version"] == TASK_MANIFEST_VERSION
        assert m["inputs"] == {"in.txt": "a" * 64}
        assert m["outputs"] == {"out.txt": "b" * 64}
        assert m["executor"]["type"] == "local"

    def test_non_terminal_task_409(self, tmp_path):
        run = _terminal_run()
        client = _api_client_with_run(run, _mgr_with_run(tmp_path, run))
        resp = client.get("/api/v1/runs/r1/tasks/t2/manifest")
        assert resp.status_code == 409

    def test_unknown_run_and_task_404(self, tmp_path):
        run = _terminal_run()
        client = _api_client_with_run(run, _mgr_with_run(tmp_path, run))
        assert client.get("/api/v1/runs/nope/tasks/t/manifest").status_code == 404
        assert client.get("/api/v1/runs/r1/tasks/nope/manifest").status_code == 404

    def test_run_detail_includes_manifest_for_terminal_items(self, tmp_path):
        run = _terminal_run()
        client = _api_client_with_run(run, _mgr_with_run(tmp_path, run))
        resp = client.get("/api/v1/runs/r1")
        assert resp.status_code == 200
        items = {i["task"]["id"]: i for i in resp.json()["items"]}
        assert items["t"]["manifest"]["outputs"] == {"out.txt": "b" * 64}
        assert "manifest" not in items["t2"]  # still running — not final
