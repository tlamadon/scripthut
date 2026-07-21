"""Tests for the dry-run cache probe (RunManager.probe_tasks + API).

The probe answers "would this task cache-hit?" for the same task JSON a
run submission takes, while executing nothing and writing nothing. Two
properties are load-bearing and pinned here:

- **Zero side effects**: no run records, no storage writes, no cache
  mutations, no restores — the only remote commands issued are the
  read-only input hashing and the manifest fetch.
- **Verdict parity**: the probe and the real submission path share
  ``probe_cache_for_task``, so a probe's hit/miss and cache key must
  match what submitting the same task would have done.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripthut.api import make_api_router
from scripthut.config_schema import (
    CacheConfig,
    ScriptHutConfig,
    SlurmBackendConfig,
    SSHConfig,
)
from scripthut.runs.manager import RunManager, probe_summary
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

HIT_MANIFEST = {
    "version": 1,
    "key": "k",
    "blob": "s3://b/p/cas/h.tar.gz",
    "content_hash": "cafebabe",
    "outputs": ["model.pt"],
    "exit_code": 0,
}


class _FakeBackend:
    def __init__(self):
        self.submitted = False

    def generate_script(self, *a, **k):
        return "#!/bin/bash\ntrue"

    async def submit_task(self, *a, **k):
        from scripthut.backends.base import SubmitResult
        self.submitted = True
        return SubmitResult(job_id="999")


def _route(routes):
    """SSH mock returning the first response whose substring matches; records commands."""
    cmds: list[str] = []

    async def run_command(cmd, timeout=30):
        cmds.append(cmd)
        for needle, resp in routes:
            if needle in cmd:
                return resp
        return ("", "", 0)

    ssh = MagicMock()
    ssh.run_command = AsyncMock(side_effect=run_command)
    return ssh, cmds


def _task(**over) -> TaskDefinition:
    kwargs = dict(
        id="t1", name="t1", command="python train.py",
        working_dir="/work", inputs=["data.csv"], outputs=["model.pt"],
    )
    kwargs.update(over)
    return TaskDefinition(**kwargs)


def _manager(ssh, *, cache_enabled=True, storage=None) -> RunManager:
    backend_cfg = SlurmBackendConfig(
        name="cluster", type="slurm", ssh=SSHConfig(host="h", user="u"),
    )
    cache = (
        CacheConfig(enabled=True, store="s3://b/p")
        if cache_enabled else CacheConfig()
    )
    cfg = ScriptHutConfig(backends=[backend_cfg], cache=cache)
    return RunManager(
        config=cfg, backends={"cluster": ssh},
        storage=storage, job_backends={"cluster": _FakeBackend()},
    )


# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------


class TestProbeVerdicts:
    @pytest.mark.asyncio
    async def test_hit_returns_content_hashes(self):
        ssh, _ = _route([
            ("/ac/", (json.dumps(HIT_MANIFEST), "", 0)),
            ("sha256sum", ("deadbeef  data.csv\n", "", 0)),
        ])
        mgr = _manager(ssh)
        [v] = await mgr.probe_tasks([_task()], "cluster")
        assert v["hit"] is True
        assert v["cacheable"] is True
        assert v["cache_key"]
        assert v["content_hash"] == "cafebabe"
        assert v["outputs"] == ["model.pt"]
        assert v["input_hashes"] == {"data.csv": "deadbeef"}

    @pytest.mark.asyncio
    async def test_miss(self):
        ssh, _ = _route([
            ("/ac/", ("", "", 1)),
            ("sha256sum", ("deadbeef  data.csv\n", "", 0)),
        ])
        mgr = _manager(ssh)
        [v] = await mgr.probe_tasks([_task()], "cluster")
        assert v["hit"] is False
        assert v["cacheable"] is True
        assert v["cache_key"]

    @pytest.mark.asyncio
    async def test_cached_failure_is_not_a_hit(self):
        bad = {**HIT_MANIFEST, "exit_code": 1}
        ssh, _ = _route([
            ("/ac/", (json.dumps(bad), "", 0)),
            ("sha256sum", ("deadbeef  data.csv\n", "", 0)),
        ])
        mgr = _manager(ssh)
        [v] = await mgr.probe_tasks([_task()], "cluster")
        assert v["hit"] is False
        assert "exit 1" in v["reason"]

    @pytest.mark.asyncio
    async def test_uncacheable_reasons(self):
        ssh, _ = _route([("sha256sum", ("deadbeef  data.csv\n", "", 0))])
        mgr = _manager(ssh, cache_enabled=False)
        [v] = await mgr.probe_tasks([_task()], "cluster")
        assert v == {
            "task_id": "t1", "cacheable": False,
            "reason": "cache disabled (no store configured)",
            "cache_key": None, "hit": False,
        }

        mgr = _manager(ssh)
        [v] = await mgr.probe_tasks([_task(outputs=[])], "cluster")
        assert v["cacheable"] is False and "outputs" in v["reason"]

        [v] = await mgr.probe_tasks([_task(cache=False)], "cluster")
        assert v["cacheable"] is False and "opted out" in v["reason"]

    @pytest.mark.asyncio
    async def test_input_hash_failure_is_uncacheable(self):
        ssh, _ = _route([("sha256sum", ("", "find: missing", 1))])
        mgr = _manager(ssh)
        [v] = await mgr.probe_tasks([_task()], "cluster")
        assert v["cacheable"] is False
        assert v["hit"] is False

    @pytest.mark.asyncio
    async def test_per_item_verdicts_for_task_list(self):
        ssh, _ = _route([
            ("/ac/", (json.dumps(HIT_MANIFEST), "", 0)),
            ("sha256sum", ("deadbeef  data.csv\n", "", 0)),
        ])
        mgr = _manager(ssh)
        tasks = [
            _task(id="a"),
            _task(id="b", dependencies=["a"], cache=False),
        ]
        verdicts = await mgr.probe_tasks(tasks, "cluster")
        assert [v["task_id"] for v in verdicts] == ["a", "b"]
        assert verdicts[0]["hit"] is True
        assert verdicts[1]["cacheable"] is False
        assert probe_summary(verdicts) == {
            "hit": 1, "miss": 0, "uncacheable": 1,
        }

    @pytest.mark.asyncio
    async def test_invalid_dependencies_rejected_like_submission(self):
        ssh, _ = _route([])
        mgr = _manager(ssh)
        with pytest.raises(ValueError, match="does not exist"):
            await mgr.probe_tasks(
                [_task(dependencies=["nope"])], "cluster",
            )

    @pytest.mark.asyncio
    async def test_unknown_backend_rejected(self):
        ssh, _ = _route([])
        mgr = _manager(ssh)
        with pytest.raises(ValueError, match="not found"):
            await mgr.probe_tasks([_task()], "elsewhere")

    @pytest.mark.asyncio
    async def test_commit_hash_feeds_commit_scope_keys(self):
        async def key_at(commit):
            ssh, _ = _route([
                ("/ac/", ("", "", 1)),
                ("sha256sum", ("deadbeef  data.csv\n", "", 0)),
            ])
            [v] = await _manager(ssh).probe_tasks(
                [_task()], "cluster", commit_hash=commit,
            )
            return v["cache_key"]

        assert await key_at("aaa") != await key_at("bbb")

        async def key_at_inputs_scope(commit):
            ssh, _ = _route([
                ("/ac/", ("", "", 1)),
                ("sha256sum", ("deadbeef  data.csv\n", "", 0)),
            ])
            [v] = await _manager(ssh).probe_tasks(
                [_task(cache_scope="inputs")], "cluster", commit_hash=commit,
            )
            return v["cache_key"]

        assert (
            await key_at_inputs_scope("aaa") == await key_at_inputs_scope("bbb")
        )


# ---------------------------------------------------------------------------
# Zero side effects
# ---------------------------------------------------------------------------


def _fs_snapshot(root: Path) -> dict:
    """Byte-level snapshot of every file under ``root``."""
    return {
        str(p.relative_to(root)): p.read_bytes()
        for p in root.rglob("*") if p.is_file()
    }


class TestProbeSideEffects:
    @pytest.mark.asyncio
    async def test_probe_leaves_zero_footprint(self, tmp_path):
        from scripthut.runs.storage import RunStorageManager

        storage = RunStorageManager(tmp_path / "workflows")
        ssh, cmds = _route([
            ("/ac/", (json.dumps(HIT_MANIFEST), "", 0)),
            ("sha256sum", ("deadbeef  data.csv\n", "", 0)),
        ])
        mgr = _manager(ssh, storage=storage)

        before = _fs_snapshot(tmp_path)
        verdicts = await mgr.probe_tasks(
            [_task(id="a"), _task(id="b")], "cluster",
        )
        mgr.save_dirty()  # would flush anything a probe wrongly dirtied
        after = _fs_snapshot(tmp_path)

        assert len(verdicts) == 2
        assert after == before  # no run records, nothing persisted
        assert mgr.runs == {}   # no in-memory run created either

        # Every remote command must be one of the two read-only shapes:
        # input hashing (find | sha256sum) or the manifest fetch (ac/).
        assert cmds, "probe should have issued the read-only commands"
        for cmd in cmds:
            assert ("sha256sum" in cmd) or ("/ac/" in cmd), cmd
            for mutation in (
                "tar xzf", "mkdir", "rcat", "copyto",
                "sbatch", "rm -", "> ", ">>",
            ):
                assert mutation not in cmd, f"probe issued a write: {cmd}"


# ---------------------------------------------------------------------------
# Parity with the real submission path
# ---------------------------------------------------------------------------


def _run_for(task: TaskDefinition, commit: str | None) -> tuple[Run, RunItem]:
    item = RunItem(task=task, status=RunItemStatus.PENDING)
    run = Run(
        id="r1", workflow_name="_probe", backend_name="cluster",
        created_at=datetime(2026, 7, 21, tzinfo=UTC),
        items=[item], max_concurrent=1,
        log_dir="/logs", commit_hash=commit,
    )
    return run, item


class TestProbeMatchesSubmission:
    @pytest.mark.asyncio
    async def test_hit_verdict_matches_submission(self):
        routes = [
            ("tar xzf", ("", "", 0)),
            ("/ac/", (json.dumps(HIT_MANIFEST), "", 0)),
            ("sha256sum", ("deadbeef  data.csv\n", "", 0)),
        ]
        ssh, _ = _route(routes)
        mgr = _manager(ssh)
        [verdict] = await mgr.probe_tasks(
            [_task()], "cluster", commit_hash="abc123",
        )

        ssh2, _ = _route(routes)
        mgr2 = _manager(ssh2)
        mgr2._persist_run = MagicMock()
        run, item = _run_for(_task(), "abc123")
        mgr2.runs[run.id] = run
        await mgr2.submit_task(run, item)

        assert verdict["hit"] is True
        assert item.cache_hit is True
        assert item.cache_key == verdict["cache_key"]

    @pytest.mark.asyncio
    async def test_miss_verdict_matches_submission(self):
        routes = [
            ("/ac/", ("", "", 1)),
            ("sha256sum", ("deadbeef  data.csv\n", "", 0)),
        ]
        ssh, _ = _route(routes)
        mgr = _manager(ssh)
        [verdict] = await mgr.probe_tasks(
            [_task()], "cluster", commit_hash="abc123",
        )

        ssh2, _ = _route(routes)
        mgr2 = _manager(ssh2)
        mgr2._persist_run = MagicMock()
        run, item = _run_for(_task(), "abc123")
        mgr2.runs[run.id] = run
        await mgr2.submit_task(run, item)

        assert verdict["hit"] is False
        assert item.cache_hit is False
        assert item.status == RunItemStatus.SUBMITTED  # really ran
        assert item.cache_key == verdict["cache_key"]


# ---------------------------------------------------------------------------
# API: POST /api/v1/tasks/probe
# ---------------------------------------------------------------------------


def _state(rm):
    state = MagicMock()
    state.run_manager = rm
    state.config_error = None
    state.backends = {}
    state.config = None
    state.notify_poll = MagicMock()
    return state


def _client(state) -> TestClient:
    app = FastAPI()
    app.include_router(make_api_router(state))
    return TestClient(app)


def _mock_rm(verdicts):
    rm = MagicMock()
    rm.probe_tasks = AsyncMock(return_value=verdicts)
    rm.cache_manager.enabled = True
    return rm


VERDICT = {
    "task_id": "t1", "cacheable": True, "reason": None,
    "cache_key": "k" * 64, "hit": True,
    "content_hash": "cafebabe", "outputs": ["model.pt"],
}


class TestProbeEndpoint:
    def test_single_task_form(self):
        rm = _mock_rm([VERDICT])
        client = _client(_state(rm))
        resp = client.post("/api/v1/tasks/probe", json={
            "task": {"id": "t1", "name": "t1", "command": "true"},
            "backend": "cluster",
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["summary"] == {"hit": 1, "miss": 0, "uncacheable": 0}
        assert body["results"][0]["content_hash"] == "cafebabe"
        tasks_arg = rm.probe_tasks.call_args.args[0]
        assert [t.id for t in tasks_arg] == ["t1"]

    def test_document_form_with_doc_env(self):
        rm = _mock_rm([VERDICT])
        client = _client(_state(rm))
        resp = client.post("/api/v1/tasks/probe", json={
            "tasks": {
                "tasks": [{"id": "t1", "name": "t1", "command": "true"}],
                "env": [{"set": {"A": "1"}}],
            },
            "backend": "cluster",
            "commit_hash": "abc123",
        })
        assert resp.status_code == 200
        kwargs = rm.probe_tasks.call_args.kwargs
        assert kwargs["commit_hash"] == "abc123"
        assert len(kwargs["doc_env"]) == 1

    def test_bare_list_form(self):
        rm = _mock_rm([VERDICT])
        client = _client(_state(rm))
        resp = client.post("/api/v1/tasks/probe", json={
            "tasks": [{"id": "t1", "name": "t1", "command": "true"}],
            "backend": "cluster",
        })
        assert resp.status_code == 200

    def test_missing_backend_422(self):
        client = _client(_state(_mock_rm([])))
        resp = client.post("/api/v1/tasks/probe", json={"task": {}})
        assert resp.status_code == 422

    def test_task_and_tasks_both_or_neither_422(self):
        client = _client(_state(_mock_rm([])))
        resp = client.post("/api/v1/tasks/probe", json={"backend": "c"})
        assert resp.status_code == 422
        resp = client.post("/api/v1/tasks/probe", json={
            "backend": "c",
            "task": {"id": "a", "name": "a", "command": "x"},
            "tasks": [],
        })
        assert resp.status_code == 422

    def test_unknown_backend_maps_to_422(self):
        rm = _mock_rm([])
        rm.probe_tasks = AsyncMock(side_effect=ValueError("Backend 'x' not found"))
        client = _client(_state(rm))
        resp = client.post("/api/v1/tasks/probe", json={
            "task": {"id": "t1", "name": "t1", "command": "true"},
            "backend": "x",
        })
        assert resp.status_code == 422
