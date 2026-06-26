"""Tests for the task-level result cache (scripthut.runs.cache).

Surfaces covered:

- ``CacheConfig`` validation (store required when enabled).
- ``CacheManager.compute_key`` determinism + sensitivity (command, env,
  commit, input hashes change the key; volatile SCRIPTHUT_* env does not).
- Tool-specific command building (aws vs rclone).
- ``hash_inputs`` / ``lookup`` / ``restore`` / ``store`` SSH parsing.
- ``RunManager`` integration: a cache hit restores artifacts and marks the
  item COMPLETED without submitting; a miss records the key and submits; the
  cache is skipped when disabled, when the task has no outputs, when opted
  out, and when the cached result was a failure.
- Persistence round-trips for the new TaskDefinition / RunItem fields.
- Project-local YAML rejects the ``cache`` section.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from scripthut.config_schema import CacheConfig
from scripthut.runs.cache import CacheManager
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition


# ---------------------------------------------------------------------------
# CacheConfig validation
# ---------------------------------------------------------------------------


class TestCacheConfig:
    def test_disabled_without_store_is_fine(self):
        cfg = CacheConfig()
        assert cfg.enabled is False
        assert CacheManager(cfg).enabled is False

    def test_enabled_requires_store(self):
        with pytest.raises(ValueError, match="cache.store"):
            CacheConfig(enabled=True)

    def test_enabled_with_store_ok(self):
        cfg = CacheConfig(enabled=True, store="s3://bucket/prefix")
        assert CacheManager(cfg).enabled is True

    def test_enabled_property_needs_both(self):
        # store set but disabled → not enabled
        assert CacheManager(CacheConfig(store="s3://b")).enabled is False


# ---------------------------------------------------------------------------
# compute_key
# ---------------------------------------------------------------------------


def _cm() -> CacheManager:
    return CacheManager(CacheConfig(enabled=True, store="s3://b/p"))


class TestComputeKey:
    def _key(self, **over):
        base = dict(
            command="python train.py",
            env={"FOO": "1"},
            commit_hash="abc123",
            input_hashes={"data.csv": "deadbeef"},
        )
        base.update(over)
        return _cm().compute_key(**base)

    def test_deterministic(self):
        assert self._key() == self._key()

    def test_command_change_busts_key(self):
        assert self._key() != self._key(command="python train.py --v2")

    def test_user_env_change_busts_key(self):
        assert self._key() != self._key(env={"FOO": "2"})

    def test_commit_change_busts_key(self):
        assert self._key() != self._key(commit_hash="zzz999")

    def test_input_hash_change_busts_key(self):
        assert self._key() != self._key(input_hashes={"data.csv": "feed00"})

    def test_volatile_scripthut_env_ignored(self):
        """SCRIPTHUT_RUN_ID / CREATED_AT differ every run; they must not
        enter the key or the cache would never hit."""
        a = self._key(env={"FOO": "1", "SCRIPTHUT_RUN_ID": "r1",
                            "SCRIPTHUT_CREATED_AT": "t1"})
        b = self._key(env={"FOO": "1", "SCRIPTHUT_RUN_ID": "r2",
                            "SCRIPTHUT_CREATED_AT": "t2"})
        assert a == b

    def test_empty_inputs_still_keys(self):
        # No declared inputs → keyed on command+env+commit only, still stable.
        assert self._key(input_hashes={}) == self._key(input_hashes={})

    def test_key_is_hex_sha256(self):
        k = self._key()
        assert len(k) == 64 and all(c in "0123456789abcdef" for c in k)


# ---------------------------------------------------------------------------
# Command building (tool abstraction)
# ---------------------------------------------------------------------------


class TestCommandBuilding:
    def test_uri_join(self):
        cm = CacheManager(CacheConfig(enabled=True, store="s3://bucket/prefix/"))
        assert cm._uri("ac", "k.json") == "s3://bucket/prefix/ac/k.json"

    def test_aws_commands(self):
        cm = _cm()
        import shlex
        q = shlex.quote("s3://b/p/ac/k.json")
        assert cm._cmd_cat(q) == f"aws s3 cp {q} -"
        assert cm._cmd_put_stdin(q) == f"aws s3 cp - {q}"
        assert cm._cmd_exists(q) == f"aws s3 ls {q}"
        assert cm._cmd_put_file('"$TMP"', q) == f'aws s3 cp "$TMP" {q}'

    def test_rclone_commands(self):
        cm = CacheManager(
            CacheConfig(enabled=True, store="remote:b/p", tool="rclone")
        )
        assert cm._cmd_cat("'x'") == "rclone cat 'x'"
        assert cm._cmd_put_stdin("'x'") == "rclone rcat 'x'"
        assert cm._cmd_exists("'x'") == "rclone lsf 'x'"
        assert cm._cmd_put_file('"$TMP"', "'x'") == 'rclone copyto "$TMP" \'x\''


# ---------------------------------------------------------------------------
# hash_inputs
# ---------------------------------------------------------------------------


def _ssh(side):
    ssh = MagicMock()
    ssh.run_command = AsyncMock(side_effect=side)
    return ssh


class TestHashInputs:
    @pytest.mark.asyncio
    async def test_empty_inputs_returns_empty_without_ssh(self):
        ssh = MagicMock()
        ssh.run_command = AsyncMock()
        out = await _cm().hash_inputs(ssh, "/wd", [])
        assert out == {}
        ssh.run_command.assert_not_called()

    @pytest.mark.asyncio
    async def test_parses_sha256sum_output(self):
        async def side(cmd, timeout=30):
            return (
                "deadbeef  data/x.parquet\n"
                "feed0000  ./base.yaml\n",
                "", 0,
            )
        out = await _cm().hash_inputs(_ssh(side), "/wd", ["data", "base.yaml"])
        # Leading "./" is stripped so paths are stable.
        assert out == {"data/x.parquet": "deadbeef", "base.yaml": "feed0000"}

    @pytest.mark.asyncio
    async def test_nonzero_exit_returns_none(self):
        async def side(cmd, timeout=30):
            return ("", "find: no such file", 1)
        out = await _cm().hash_inputs(_ssh(side), "/wd", ["missing"])
        assert out is None

    @pytest.mark.asyncio
    async def test_zero_files_returns_none(self):
        """Declared inputs that match nothing → don't cache on an empty set."""
        async def side(cmd, timeout=30):
            return ("", "", 0)
        out = await _cm().hash_inputs(_ssh(side), "/wd", ["data/*.parquet"])
        assert out is None

    @pytest.mark.asyncio
    async def test_ssh_exception_returns_none(self):
        async def side(cmd, timeout=30):
            raise RuntimeError("connection dropped")
        out = await _cm().hash_inputs(_ssh(side), "/wd", ["x"])
        assert out is None


# ---------------------------------------------------------------------------
# lookup
# ---------------------------------------------------------------------------


class TestLookup:
    @pytest.mark.asyncio
    async def test_hit_returns_manifest(self):
        manifest = {"key": "k", "blob": "s3://b/p/cas/h.tar.gz", "exit_code": 0}
        async def side(cmd, timeout=30):
            return (json.dumps(manifest), "", 0)
        out = await _cm().lookup(_ssh(side), "k")
        assert out == manifest

    @pytest.mark.asyncio
    async def test_miss_returns_none(self):
        async def side(cmd, timeout=30):
            return ("", "", 1)
        assert await _cm().lookup(_ssh(side), "k") is None

    @pytest.mark.asyncio
    async def test_invalid_json_returns_none(self):
        async def side(cmd, timeout=30):
            return ("not json{", "", 0)
        assert await _cm().lookup(_ssh(side), "k") is None

    @pytest.mark.asyncio
    async def test_manifest_without_blob_rejected(self):
        async def side(cmd, timeout=30):
            return (json.dumps({"key": "k"}), "", 0)
        assert await _cm().lookup(_ssh(side), "k") is None


# ---------------------------------------------------------------------------
# restore
# ---------------------------------------------------------------------------


class TestRestore:
    @pytest.mark.asyncio
    async def test_success(self):
        async def side(cmd, timeout=30):
            assert "tar xzf" in cmd
            return ("", "", 0)
        ok = await _cm().restore(
            _ssh(side), "/wd", {"blob": "s3://b/p/cas/h.tar.gz"}
        )
        assert ok is True

    @pytest.mark.asyncio
    async def test_failure_returns_false(self):
        async def side(cmd, timeout=30):
            return ("", "tar: corrupt", 2)
        ok = await _cm().restore(
            _ssh(side), "/wd", {"blob": "s3://b/p/cas/h.tar.gz"}
        )
        assert ok is False

    @pytest.mark.asyncio
    async def test_no_blob_returns_false(self):
        ssh = MagicMock()
        ssh.run_command = AsyncMock()
        ok = await _cm().restore(ssh, "/wd", {})
        assert ok is False
        ssh.run_command.assert_not_called()


# ---------------------------------------------------------------------------
# store
# ---------------------------------------------------------------------------


class TestStore:
    @pytest.mark.asyncio
    async def test_builds_and_uploads_manifest(self):
        calls = []

        async def side(cmd, timeout=30):
            calls.append(cmd)
            if "SCRIPTHUT_CACHE_HASH" in cmd:  # round-trip A (build/upload blob)
                return (
                    "SCRIPTHUT_CACHE_HASH=abcd1234\n"
                    "SCRIPTHUT_CACHE_BLOB=s3://b/p/cas/abcd1234.tar.gz\n"
                    "---MEMBERS---\n"
                    "model.pt\n"
                    "logs/\n"          # directory entry is dropped
                    "logs/run.txt\n",
                    "", 0,
                )
            return ("", "", 0)  # round-trip B (manifest put)

        manifest = await _cm().store(
            _ssh(side), "/wd",
            key="thekey",
            outputs=["model.pt", "logs"],
            meta={"command": "train", "exit_code": 0, "run_id": "r1"},
        )
        assert manifest is not None
        assert manifest["content_hash"] == "abcd1234"
        assert manifest["blob"] == "s3://b/p/cas/abcd1234.tar.gz"
        assert manifest["key"] == "thekey"
        assert manifest["outputs"] == ["model.pt", "logs/run.txt"]
        assert manifest["command"] == "train"
        assert manifest["exit_code"] == 0
        assert len(calls) == 2  # build + manifest upload

    @pytest.mark.asyncio
    async def test_no_outputs_returns_none(self):
        ssh = MagicMock()
        ssh.run_command = AsyncMock()
        out = await _cm().store(ssh, "/wd", key="k", outputs=[], meta={})
        assert out is None
        ssh.run_command.assert_not_called()

    @pytest.mark.asyncio
    async def test_build_failure_returns_none(self):
        async def side(cmd, timeout=30):
            return ("", "tar error", 2)
        out = await _cm().store(
            _ssh(side), "/wd", key="k", outputs=["x"], meta={}
        )
        assert out is None


# ---------------------------------------------------------------------------
# RunManager integration
# ---------------------------------------------------------------------------


class _FakeBackend:
    """Minimal JobBackend stand-in tracking whether submit_task ran."""

    def __init__(self):
        self.submitted = False

    def generate_script(self, *a, **k):
        return "#!/bin/bash\ntrue"

    async def submit_task(self, *a, **k):
        from scripthut.backends.base import SubmitResult
        self.submitted = True
        return SubmitResult(job_id="999", submit_output="Submitted batch job 999")


def _make_manager(ssh, *, cache_enabled=True, task_over=None):
    """Build (manager, run, item, backend) wired to ``ssh`` with cache on."""
    from scripthut.config_schema import (
        CacheConfig,
        ScriptHutConfig,
        SlurmBackendConfig,
        SSHConfig,
    )
    from scripthut.runs.manager import RunManager

    backend_cfg = SlurmBackendConfig(
        name="cluster", type="slurm", ssh=SSHConfig(host="h", user="u"),
    )
    cache = (
        CacheConfig(enabled=True, store="s3://b/p")
        if cache_enabled else CacheConfig()
    )
    cfg = ScriptHutConfig(backends=[backend_cfg], cache=cache)

    task_kwargs = dict(
        id="t1", name="t1", command="python train.py",
        working_dir="/work",
        outputs=["model.pt"], inputs=["data.csv"],
    )
    task_kwargs.update(task_over or {})
    task = TaskDefinition(**task_kwargs)
    item = RunItem(task=task, status=RunItemStatus.PENDING)
    run = Run(
        id="r1", workflow_name="wf", backend_name="cluster",
        created_at=datetime(2026, 6, 4, tzinfo=UTC),
        items=[item], max_concurrent=1,
        log_dir="/logs", commit_hash="abc123",
    )
    fake_backend = _FakeBackend()
    mgr = RunManager(
        config=cfg, backends={"cluster": ssh},
        storage=MagicMock(), job_backends={"cluster": fake_backend},
    )
    mgr._persist_run = MagicMock()
    mgr.runs[run.id] = run
    return mgr, run, item, fake_backend


def _route(routes):
    """SSH run_command that returns the first response whose substring matches."""
    async def run_command(cmd, timeout=30):
        for needle, resp in routes:
            if needle in cmd:
                return resp
        return ("", "", 0)
    ssh = MagicMock()
    ssh.run_command = AsyncMock(side_effect=run_command)
    return ssh


class TestSubmitTaskCacheIntegration:
    @pytest.mark.asyncio
    async def test_cache_hit_skips_submission(self):
        manifest = {
            "key": "k", "blob": "s3://b/p/cas/h.tar.gz", "exit_code": 0,
        }
        ssh = _route([
            ("tar xzf", ("", "", 0)),                       # restore
            ("/ac/", (json.dumps(manifest), "", 0)),        # lookup hit
            ("sha256sum", ("deadbeef  data.csv\n", "", 0)),  # hash_inputs
        ])
        mgr, run, item, backend = _make_manager(ssh)

        ok = await mgr.submit_task(run, item)

        assert ok is True
        assert item.status == RunItemStatus.COMPLETED
        assert item.cache_hit is True
        assert item.cache_key is not None
        assert item.exit_code == 0
        assert backend.submitted is False  # never touched the scheduler

    @pytest.mark.asyncio
    async def test_cache_miss_records_key_and_submits(self):
        ssh = _route([
            ("/ac/", ("", "", 1)),                           # lookup miss
            ("sha256sum", ("deadbeef  data.csv\n", "", 0)),  # hash_inputs
        ])
        mgr, run, item, backend = _make_manager(ssh)

        ok = await mgr.submit_task(run, item)

        assert ok is True
        assert item.status == RunItemStatus.SUBMITTED
        assert item.cache_hit is False
        assert item.cache_key is not None  # remembered for the store path
        assert backend.submitted is True

    @pytest.mark.asyncio
    async def test_cached_failure_not_reused(self):
        manifest = {"key": "k", "blob": "s3://b/p/cas/h.tar.gz", "exit_code": 1}
        ssh = _route([
            ("/ac/", (json.dumps(manifest), "", 0)),
            ("sha256sum", ("deadbeef  data.csv\n", "", 0)),
        ])
        mgr, run, item, backend = _make_manager(ssh)

        await mgr.submit_task(run, item)

        assert item.cache_hit is False
        assert item.status == RunItemStatus.SUBMITTED
        assert backend.submitted is True

    @pytest.mark.asyncio
    async def test_disabled_cache_is_noop(self):
        ssh = _route([])
        mgr, run, item, backend = _make_manager(ssh, cache_enabled=False)

        await mgr.submit_task(run, item)

        assert item.cache_key is None
        assert item.cache_hit is False
        assert backend.submitted is True

    @pytest.mark.asyncio
    async def test_no_outputs_not_cacheable(self):
        ssh = _route([
            ("sha256sum", ("deadbeef  data.csv\n", "", 0)),
        ])
        mgr, run, item, backend = _make_manager(ssh, task_over={"outputs": []})

        await mgr.submit_task(run, item)

        assert item.cache_key is None  # never computed
        assert backend.submitted is True

    @pytest.mark.asyncio
    async def test_opted_out_task_not_cacheable(self):
        ssh = _route([
            ("/ac/", (json.dumps({"blob": "x", "exit_code": 0}), "", 0)),
            ("sha256sum", ("deadbeef  data.csv\n", "", 0)),
        ])
        mgr, run, item, backend = _make_manager(ssh, task_over={"cache": False})

        await mgr.submit_task(run, item)

        assert item.cache_key is None
        assert item.cache_hit is False
        assert backend.submitted is True

    @pytest.mark.asyncio
    async def test_input_hash_failure_falls_back_to_submit(self):
        ssh = _route([
            ("sha256sum", ("", "find: missing", 1)),  # hashing fails
        ])
        mgr, run, item, backend = _make_manager(ssh)

        await mgr.submit_task(run, item)

        # Couldn't verify inputs → run rather than risk a stale hit. No key.
        assert item.cache_key is None
        assert backend.submitted is True


class TestProcessRunCacheReDrive:
    @pytest.mark.asyncio
    async def test_cache_hit_unblocks_dependent_task(self):
        """A cached task that completes synchronously inside process_run must
        re-drive so its dependent gets submitted now, not next poll."""
        from scripthut.config_schema import (
            CacheConfig,
            ScriptHutConfig,
            SlurmBackendConfig,
            SSHConfig,
        )
        from scripthut.runs.manager import RunManager
        from scripthut.runs.models import Run

        manifest = {"key": "k", "blob": "s3://b/p/cas/h.tar.gz", "exit_code": 0}
        ssh = _route([
            ("tar xzf", ("", "", 0)),
            ("/ac/", (json.dumps(manifest), "", 0)),
            ("sha256sum", ("deadbeef  data.csv\n", "", 0)),
        ])

        backend_cfg = SlurmBackendConfig(
            name="cluster", type="slurm", ssh=SSHConfig(host="h", user="u"),
        )
        cfg = ScriptHutConfig(
            backends=[backend_cfg],
            cache=CacheConfig(enabled=True, store="s3://b/p"),
        )
        # parent is cacheable (will hit); child depends on it and is NOT
        # cacheable (no outputs) so it must reach the scheduler.
        parent = TaskDefinition(
            id="parent", name="parent", command="make-data",
            working_dir="/work", inputs=["data.csv"], outputs=["model.pt"],
        )
        child = TaskDefinition(
            id="child", name="child", command="use-data",
            working_dir="/work", dependencies=["parent"],
        )
        p_item = RunItem(task=parent, status=RunItemStatus.PENDING)
        c_item = RunItem(task=child, status=RunItemStatus.PENDING)
        run = Run(
            id="r1", workflow_name="wf", backend_name="cluster",
            created_at=datetime(2026, 6, 4, tzinfo=UTC),
            items=[p_item, c_item], max_concurrent=None,
            log_dir="/logs", commit_hash="abc123",
        )
        backend = _FakeBackend()
        mgr = RunManager(
            config=cfg, backends={"cluster": ssh},
            storage=MagicMock(), job_backends={"cluster": backend},
        )
        mgr._persist_run = MagicMock()
        mgr.runs[run.id] = run

        await mgr.process_run(run)

        assert p_item.status == RunItemStatus.COMPLETED
        assert p_item.cache_hit is True
        # The dependent was submitted in the same pass thanks to the re-drive.
        assert c_item.status == RunItemStatus.SUBMITTED
        assert backend.submitted is True


class TestMaybeStoreCache:
    @pytest.mark.asyncio
    async def test_stores_on_completion(self):
        async def side(cmd, timeout=30):
            if "SCRIPTHUT_CACHE_HASH" in cmd:
                return (
                    "SCRIPTHUT_CACHE_HASH=h1\n"
                    "SCRIPTHUT_CACHE_BLOB=s3://b/p/cas/h1.tar.gz\n"
                    "---MEMBERS---\nmodel.pt\n",
                    "", 0,
                )
            return ("", "", 0)
        ssh = _ssh(side)
        mgr, run, item, _ = _make_manager(ssh)
        item.status = RunItemStatus.COMPLETED
        item.cache_key = "thekey"
        item.exit_code = 0

        # Spy on the manager's CacheManager.store to confirm it ran.
        store_spy = AsyncMock(wraps=mgr.cache_manager.store)
        mgr.cache_manager.store = store_spy
        await mgr._maybe_store_cache(run, item)
        store_spy.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_cache_hit(self):
        mgr, run, item, _ = _make_manager(_route([]))
        item.status = RunItemStatus.COMPLETED
        item.cache_key = "thekey"
        item.cache_hit = True
        mgr.cache_manager.store = AsyncMock()
        await mgr._maybe_store_cache(run, item)
        mgr.cache_manager.store.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_no_key(self):
        mgr, run, item, _ = _make_manager(_route([]))
        item.status = RunItemStatus.COMPLETED
        item.cache_key = None
        mgr.cache_manager.store = AsyncMock()
        await mgr._maybe_store_cache(run, item)
        mgr.cache_manager.store.assert_not_called()


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_task_cache_fields_round_trip(self):
        t = TaskDefinition(
            id="t", name="t", command="true",
            inputs=["a", "b"], outputs=["out/*.parquet"], cache=False,
        )
        rt = TaskDefinition.from_dict(t.to_dict())
        assert rt.inputs == ["a", "b"]
        assert rt.outputs == ["out/*.parquet"]
        assert rt.cache is False

    def test_run_item_cache_fields_round_trip(self):
        item = RunItem(
            task=TaskDefinition(id="t", name="t", command="true"),
            status=RunItemStatus.COMPLETED,
            cache_key="abc123", cache_hit=True,
        )
        rt = RunItem.from_dict(item.to_dict())
        assert rt.cache_key == "abc123"
        assert rt.cache_hit is True

    def test_old_task_without_cache_fields_loads(self):
        old = {"id": "t", "name": "t", "command": "true"}
        t = TaskDefinition.from_dict(old)
        assert t.inputs == []
        assert t.outputs == []
        assert t.cache is True  # default-on

    def test_old_run_item_without_cache_fields_loads(self):
        old = {
            "task": {"id": "t", "name": "t", "command": "true"},
            "status": "completed",
        }
        item = RunItem.from_dict(old)
        assert item.cache_key is None
        assert item.cache_hit is False


# ---------------------------------------------------------------------------
# Project-local YAML rejects cache
# ---------------------------------------------------------------------------


class TestProjectLocalRejectsCache:
    def test_cache_section_forbidden_in_project_yaml(self):
        from pathlib import Path

        from scripthut.config import ConfigError, _validate_project_local_yaml

        with pytest.raises(ConfigError, match="cache"):
            _validate_project_local_yaml(
                {"cache": {"enabled": True, "store": "s3://x"}},
                Path("scripthut.yaml"),
            )
