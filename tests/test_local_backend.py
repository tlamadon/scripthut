"""Tests for the local-machine backend (scripthut.backends.local).

The local backend is the escape hatch: same TaskDefinition JSON, same
dependency-order enforcement, same result-cache participation — executed
as subprocesses on the scripthut host. The contract pinned here:

- **Real execution end-to-end**: tasks actually run, dependency order is
  honored, failures cascade to dependents, exit codes come from the
  spool's rc records (the local stand-in for scheduler accounting).
- **Dumb execution**: no mtime/freshness logic — an identical task list
  re-runs unconditionally when no cache is configured.
- **Cache participation**: with a (faked-rclone) store configured, the
  second run of an identical task restores from the cache and never
  executes — the exact flow the SSH backends use, over LocalExecClient.
- **Restart durability**: a fresh LocalBackend on the same spool still
  answers get_jobs / get_job_stats for jobs started before the restart.
"""

from __future__ import annotations

import asyncio
import os
import stat
import time

import pytest

from scripthut.backends.local import LocalBackend, LocalExecClient
from scripthut.config_schema import (
    CacheConfig,
    LocalBackendConfig,
    ScriptHutConfig,
)
from scripthut.runs.manager import RunManager
from scripthut.runs.models import (
    Run,
    RunItemStatus,
    RunStatus,
    TaskDefinition,
)
from scripthut.runs.storage import RunStorageManager

# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


def _make_runtimeish(tmp_path, *, cache_store: str | None = None):
    """Build (manager, backend, exec_client) wired like init_runtime does."""
    exec_client = LocalExecClient()
    backend = LocalBackend("local", spool_dir=tmp_path / "spool")
    cache = (
        CacheConfig(enabled=True, store=cache_store, tool="rclone")
        if cache_store else CacheConfig()
    )
    cfg = ScriptHutConfig(
        backends=[LocalBackendConfig(name="local", max_concurrent=4)],
        cache=cache,
    )
    mgr = RunManager(
        config=cfg,
        backends={"local": exec_client},
        storage=RunStorageManager(tmp_path / "workflows"),
        job_backends={"local": backend},
    )
    return mgr, backend, exec_client


async def _drive(mgr: RunManager, backend: LocalBackend, run: Run, timeout=30.0):
    """Poll-and-resolve loop mirroring main.poll_backend's local essentials.

    get_jobs → update_run_status (RUNNING/SETTLING transitions), then
    get_job_stats resolves SETTLING items to COMPLETED/FAILED — the same
    evidence-based flow the server's poller applies.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        jobs = await backend.get_jobs()
        await mgr.update_run_status(
            run, {j.job_id: j.state for j in jobs},
        )
        settling = [
            i for i in run.items
            if i.status == RunItemStatus.SETTLING and i.job_id
        ]
        if settling:
            stats = await backend.get_job_stats(
                [i.job_id for i in settling]
            )
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
    raise TimeoutError(f"run stuck in {run.status}: "
                       f"{[(i.task.id, i.status) for i in run.items]}")


def _tasks_writing(marker, workdir, *, fail_first=False):
    """Two-task dep chain appending 'a' then 'b' to ``marker``."""
    first_cmd = (
        f"echo a >> {marker}" if not fail_first
        else f"echo a >> {marker}; exit 3"
    )
    return [
        TaskDefinition(
            id="a", name="a", command=first_cmd, working_dir=str(workdir),
        ),
        TaskDefinition(
            id="b", name="b", command=f"echo b >> {marker}",
            working_dir=str(workdir), dependencies=["a"],
        ),
    ]


# ---------------------------------------------------------------------------
# LocalExecClient
# ---------------------------------------------------------------------------


class TestLocalExecClient:
    @pytest.mark.asyncio
    async def test_run_command(self):
        client = LocalExecClient()
        stdout, stderr, code = await client.run_command("echo hi; echo err >&2")
        assert stdout.strip() == "hi"
        assert stderr.strip() == "err"
        assert code == 0
        assert client.is_connected is True

    @pytest.mark.asyncio
    async def test_nonzero_exit(self):
        client = LocalExecClient()
        _, _, code = await client.run_command("exit 7")
        assert code == 7

    @pytest.mark.asyncio
    async def test_timeout(self):
        client = LocalExecClient()
        with pytest.raises(RuntimeError, match="timed out"):
            await client.run_command("sleep 5", timeout=1)

    @pytest.mark.asyncio
    async def test_on_command_hook(self):
        entries = []
        client = LocalExecClient()
        client.on_command = entries.append
        await client.run_command("true")
        assert len(entries) == 1
        assert entries[0].command == "true"
        assert entries[0].exit_code == 0


# ---------------------------------------------------------------------------
# End-to-end execution
# ---------------------------------------------------------------------------


class TestLocalExecution:
    @pytest.mark.asyncio
    async def test_dep_chain_runs_in_order(self, tmp_path):
        mgr, backend, _ = _make_runtimeish(tmp_path)
        marker = tmp_path / "marker.txt"
        run = await mgr._build_run(
            _tasks_writing(marker, tmp_path), "wf", "local",
            max_concurrent=None, ssh_client=mgr.get_ssh_client("local"),
        )
        await _drive(mgr, backend, run)

        assert run.status == RunStatus.COMPLETED
        assert marker.read_text() == "a\nb\n"  # order enforced
        for item in run.items:
            assert item.exit_code == 0
            assert item.scheduler_state == "COMPLETED"

    @pytest.mark.asyncio
    async def test_failure_cascades_to_dependent(self, tmp_path):
        mgr, backend, _ = _make_runtimeish(tmp_path)
        marker = tmp_path / "marker.txt"
        run = await mgr._build_run(
            _tasks_writing(marker, tmp_path, fail_first=True), "wf", "local",
            max_concurrent=None, ssh_client=mgr.get_ssh_client("local"),
        )
        await _drive(mgr, backend, run)

        assert run.status == RunStatus.FAILED
        a = run.get_item_by_task_id("a")
        b = run.get_item_by_task_id("b")
        assert a.status == RunItemStatus.FAILED
        assert a.exit_code == 3
        assert b.status == RunItemStatus.DEP_FAILED
        assert marker.read_text() == "a\n"  # b never ran

    @pytest.mark.asyncio
    async def test_logs_written_and_fetchable(self, tmp_path):
        mgr, backend, _ = _make_runtimeish(tmp_path)
        task = TaskDefinition(
            id="t", name="t", command="echo hello-out; echo hello-err >&2",
            working_dir=str(tmp_path),
        )
        run = await mgr._build_run(
            [task], "wf", "local",
            max_concurrent=None, ssh_client=mgr.get_ssh_client("local"),
        )
        await _drive(mgr, backend, run)
        item = run.items[0]

        home = os.path.expanduser("~")
        log_dir = run.log_dir.replace("~", home, 1)
        out, err_msg = await backend.fetch_log(
            item.job_id, task.get_output_path(run.id, log_dir),
        )
        assert err_msg is None
        assert "hello-out" in out

    @pytest.mark.asyncio
    async def test_no_freshness_logic_reruns_unconditionally(self, tmp_path):
        """Dumb execution: identical work re-runs when no cache is set."""
        mgr, backend, _ = _make_runtimeish(tmp_path)
        marker = tmp_path / "marker.txt"
        task_kwargs = dict(
            id="t", name="t", command=f"echo ran >> {marker}",
            working_dir=str(tmp_path),
            inputs=["in.txt"], outputs=["out.txt"],
        )
        (tmp_path / "in.txt").write_text("same input\n")
        (tmp_path / "out.txt").write_text("preexisting output\n")

        for _ in range(2):
            run = await mgr._build_run(
                [TaskDefinition(**task_kwargs)], "wf", "local",
                max_concurrent=None, ssh_client=mgr.get_ssh_client("local"),
            )
            await _drive(mgr, backend, run)
            assert run.status == RunStatus.COMPLETED

        assert marker.read_text() == "ran\nran\n"  # executed both times

    @pytest.mark.asyncio
    async def test_cancel_kills_process(self, tmp_path):
        mgr, backend, _ = _make_runtimeish(tmp_path)
        sentinel = tmp_path / "never.txt"
        task = TaskDefinition(
            id="t", name="t",
            command=f"sleep 60; echo done >> {sentinel}",
            working_dir=str(tmp_path),
        )
        run = await mgr._build_run(
            [task], "wf", "local",
            max_concurrent=None, ssh_client=mgr.get_ssh_client("local"),
        )
        item = run.items[0]
        # Wait until the job shows as RUNNING.
        for _ in range(100):
            jobs = await backend.get_jobs()
            if any(j.job_id == item.job_id for j in jobs):
                break
            await asyncio.sleep(0.05)

        pid = backend._read_meta(item.job_id)["pid"]
        assert backend._pid_alive(pid)
        await mgr.cancel_run(run.id)
        # SIGTERM is asynchronous; give the group a moment to die.
        for _ in range(100):
            if not backend._pid_alive(pid):
                break
            await asyncio.sleep(0.05)
        assert not backend._pid_alive(pid)
        assert item.status == RunItemStatus.FAILED
        assert not sentinel.exists()


# ---------------------------------------------------------------------------
# Restart durability (spool-backed accounting)
# ---------------------------------------------------------------------------


class TestSpoolDurability:
    @pytest.mark.asyncio
    async def test_new_backend_instance_sees_finished_job(self, tmp_path):
        backend = LocalBackend("local", spool_dir=tmp_path / "spool")
        task = TaskDefinition(id="t", name="t", command="exit 5")
        script = backend.generate_script(task, "r1", str(tmp_path / "logs"))
        result = await backend.submit_task(task, script)

        for _ in range(100):
            if backend._rc_path(result.job_id).exists():
                break
            await asyncio.sleep(0.05)

        # Simulate a scripthut restart: fresh instance, same spool.
        reborn = LocalBackend("local", spool_dir=tmp_path / "spool")
        jobs = await reborn.get_jobs()
        assert any(j.job_id == result.job_id for j in jobs)
        stats = await reborn.get_job_stats([result.job_id])
        assert stats[result.job_id].state == "FAILED"
        assert stats[result.job_id].exit_code == 5

    @pytest.mark.asyncio
    async def test_running_job_not_reported_done(self, tmp_path):
        backend = LocalBackend("local", spool_dir=tmp_path / "spool")
        task = TaskDefinition(id="t", name="t", command="sleep 30")
        script = backend.generate_script(task, "r1", str(tmp_path / "logs"))
        result = await backend.submit_task(task, script)
        try:
            await asyncio.sleep(0.2)
            from scripthut.models import JobState
            jobs = await backend.get_jobs()
            [job] = [j for j in jobs if j.job_id == result.job_id]
            assert job.state == JobState.RUNNING
            stats = await backend.get_job_stats([result.job_id])
            assert result.job_id not in stats  # no verdict yet
        finally:
            await backend.cancel_job(result.job_id)


# ---------------------------------------------------------------------------
# Cache participation (full cycle through a faked rclone store)
# ---------------------------------------------------------------------------

_FAKE_RCLONE = """#!/bin/bash
# Minimal rclone stand-in mapping "store:<path>" to $FAKE_RCLONE_ROOT/<path>.
op="$1"; shift
p() { echo "$FAKE_RCLONE_ROOT/${1#*:}"; }
case "$op" in
  cat)    exec cat "$(p "$1")" ;;
  rcat)   mkdir -p "$(dirname "$(p "$1")")" && exec cat > "$(p "$1")" ;;
  copyto) mkdir -p "$(dirname "$(p "$2")")" && exec cp "$1" "$(p "$2")" ;;
  lsf)    [ -e "$(p "$1")" ] && basename "$(p "$1")" || exit 1 ;;
  *) echo "fake rclone: unknown op $op" >&2; exit 2 ;;
esac
"""

@pytest.fixture
def fake_store(tmp_path, monkeypatch):
    """PATH-shimmed rclone over a local dir.

    Deliberately no sha256sum shim: content hashing must work with
    whatever the host provides (sha256sum on Linux, the shasum fallback
    on stock macOS) — running this suite on a Mac proves the fallback.
    """
    bindir = tmp_path / "bin"
    bindir.mkdir()
    rclone = bindir / "rclone"
    rclone.write_text(_FAKE_RCLONE)
    rclone.chmod(rclone.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{bindir}:{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_RCLONE_ROOT", str(tmp_path / "store"))
    return tmp_path / "store"


class TestLocalCacheParticipation:
    @pytest.mark.asyncio
    async def test_second_run_hits_cache_and_skips_execution(
        self, tmp_path, fake_store,
    ):
        workdir = tmp_path / "work"
        workdir.mkdir()
        (workdir / "in.txt").write_text("input data\n")
        marker = tmp_path / "marker.txt"

        def task():
            return TaskDefinition(
                id="t", name="t",
                command=f"echo ran >> {marker} && echo result > out.txt",
                working_dir=str(workdir),
                inputs=["in.txt"], outputs=["out.txt"],
            )

        # Run 1: miss → executes → stores to the (fake) object store.
        mgr, backend, _ = _make_runtimeish(tmp_path, cache_store="store:cache")
        run1 = await mgr._build_run(
            [task()], "wf", "local",
            max_concurrent=None, ssh_client=mgr.get_ssh_client("local"),
        )
        await _drive(mgr, backend, run1)
        assert run1.status == RunStatus.COMPLETED
        assert run1.items[0].cache_hit is False
        assert marker.read_text() == "ran\n"
        assert (fake_store / "cache" / "ac").exists()  # manifest stored

        # Run 2 (fresh manager + backend, same store): hit → restored,
        # never executed.
        (workdir / "out.txt").unlink()
        mgr2, backend2, _ = _make_runtimeish(
            tmp_path / "second", cache_store="store:cache",
        )
        run2 = await mgr2._build_run(
            [task()], "wf", "local",
            max_concurrent=None, ssh_client=mgr2.get_ssh_client("local"),
        )
        # A cache hit completes synchronously inside _build_run.
        assert run2.status == RunStatus.COMPLETED
        item = run2.items[0]
        assert item.cache_hit is True
        assert item.scheduler_state == "CACHED"
        assert item.cache_key == run1.items[0].cache_key
        assert marker.read_text() == "ran\n"  # did NOT execute again
        assert (workdir / "out.txt").read_text() == "result\n"  # restored

        # And the probe agrees, without touching anything.
        [verdict] = await mgr2.probe_tasks([task()], "local")
        assert verdict["hit"] is True
        assert verdict["cache_key"] == item.cache_key


# ---------------------------------------------------------------------------
# Runtime wiring
# ---------------------------------------------------------------------------


class TestRuntimeWiring:
    @pytest.mark.asyncio
    async def test_empty_config_auto_registers_local_backend(self, tmp_path):
        from scripthut.config_schema import GlobalSettings
        from scripthut.runtime import init_runtime, shutdown_runtime

        cfg = ScriptHutConfig(
            settings=GlobalSettings(data_dir=str(tmp_path / "data")),
        )
        runtime = await init_runtime(cfg, restore_runs=False)
        try:
            assert "local" in runtime.backends
            bs = runtime.backends["local"]
            assert bs.backend_type == "local"
            assert bs.status.connected is True
            assert runtime.run_manager.get_ssh_client("local") is not None
            assert runtime.run_manager.get_job_backend("local") is not None
            assert cfg.get_backend("local") is not None
        finally:
            await shutdown_runtime(runtime)

    @pytest.mark.asyncio
    async def test_configured_backends_suppress_auto_local(self, tmp_path):
        from scripthut.config_schema import GlobalSettings
        from scripthut.runtime import init_runtime, shutdown_runtime

        cfg = ScriptHutConfig(
            backends=[LocalBackendConfig(name="mymac")],
            settings=GlobalSettings(data_dir=str(tmp_path / "data")),
        )
        runtime = await init_runtime(cfg, restore_runs=False)
        try:
            assert set(runtime.backends) == {"mymac"}
        finally:
            await shutdown_runtime(runtime)

    def test_yaml_round_trip(self):
        import yaml
        cfg = ScriptHutConfig.model_validate(yaml.safe_load(
            "backends:\n"
            "  - name: laptop\n"
            "    type: local\n"
            "    max_concurrent: 2\n"
        ))
        [b] = cfg.local_backends
        assert b.name == "laptop"
        assert b.max_concurrent == 2
