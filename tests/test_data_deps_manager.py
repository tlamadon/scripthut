"""Tests for dataset dependencies at the run-manager level.

The two things that must hold end to end:
- data present on the backend leaves the run *identical* to one with no
  ``data:`` at all (no extra item, no extra work)
- data that fails to arrive fails the run, loudly, before any task runs
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripthut.config_schema import DatasetConfig, EnvRule
from scripthut.runs.datasets import DatasetPlan, build_manifest
from scripthut.runs.manager import RunManager
from scripthut.runs.models import (
    DataDep,
    Run,
    RunItem,
    RunItemStatus,
    RunStatus,
    TaskDefinition,
)


def _local_dataset(tmp_path: Path, name: str = "acq") -> Path:
    path = tmp_path / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "a.csv").write_text("xy")
    return path


def _manager(
    *, datasets: list[DatasetConfig] | None = None, ssh: AsyncMock | None = None,
) -> RunManager:
    config = MagicMock()
    config.datasets = datasets or []
    config.get_dataset.side_effect = lambda n: next(
        (d for d in (datasets or []) if d.name == n), None
    )
    config.sources = []
    # dataset_dir/clone_dir must be real strings: a bare MagicMock would sail
    # through root validation as an object rather than a path.
    config.get_backend.return_value = MagicMock(
        env=[], max_concurrent=10,
        dataset_dir="/scratch/w", clone_dir="~/scripthut-repos",
    )
    backends = {"mercury": ssh} if ssh is not None else {}
    return RunManager(config=config, backends=backends)


def _ssh(present: bool = False) -> AsyncMock:
    ssh = AsyncMock()
    ssh.run_command = AsyncMock(return_value=("HOME\t/home/w\nSCRATCH\t/scratch/w\n", "", 0))
    return ssh


def _task(id: str, deps: list[str] | None = None) -> TaskDefinition:
    return TaskDefinition(id=id, name=id, command="echo hi", dependencies=deps or [])


def _plan(tmp_path: Path, *, reused: bool, name: str = "acq") -> DatasetPlan:
    local = _local_dataset(tmp_path, name)
    manifest = build_manifest(local)
    return DatasetPlan(
        name=name,
        local_path=local,
        manifest=manifest,
        dest=f"/scratch/w/{name}/{manifest.short}",
        reused=reused,
    )


# ---------- resolution at run creation ------------------------------------


class TestPlanDataDeps:
    @pytest.mark.asyncio
    async def test_unknown_dataset_names_the_configured_ones(self, tmp_path: Path):
        manager = _manager(
            datasets=[DatasetConfig(name="acq", path=_local_dataset(tmp_path))],
            ssh=_ssh(),
        )
        with pytest.raises(ValueError) as exc:
            await manager._plan_data_deps(
                ["typo"], backend_name="mercury",
                ssh_client=manager.get_ssh_client("mercury"), label="wf",
            )
        assert "unknown dataset 'typo'" in str(exc.value)
        assert "configured: acq" in str(exc.value)

    @pytest.mark.asyncio
    async def test_backend_without_a_filesystem_is_rejected(self, tmp_path: Path):
        manager = _manager(
            datasets=[DatasetConfig(name="acq", path=_local_dataset(tmp_path))]
        )
        with pytest.raises(ValueError, match="no filesystem"):
            await manager._plan_data_deps(
                ["acq"], backend_name="batch", ssh_client=None, label="wf",
            )

    @pytest.mark.asyncio
    async def test_no_datasets_touches_nothing(self):
        manager = _manager()
        assert await manager._plan_data_deps(
            [], backend_name="batch", ssh_client=None, label="wf",
        ) == ([], [])

    @pytest.mark.asyncio
    async def test_missing_local_directory_fails_before_any_ssh(self, tmp_path: Path):
        ssh = _ssh()
        manager = _manager(
            datasets=[DatasetConfig(name="acq", path=tmp_path / "nope")], ssh=ssh,
        )
        with pytest.raises(ValueError, match="does not exist"):
            await manager._plan_data_deps(
                ["acq"], backend_name="mercury",
                ssh_client=ssh, label="wf",
            )
        ssh.run_command.assert_not_called()

    @pytest.mark.asyncio
    async def test_present_dataset_is_marked_reused(self, tmp_path: Path):
        local = _local_dataset(tmp_path)
        hash12 = build_manifest(local).short
        ssh = _ssh()
        ssh.run_command = AsyncMock(side_effect=[
            ("HOME\t/home/w\nSCRATCH\t/scratch/w\n", "", 0),
            ("__SCRIPTHUT_PRESENT__\n__SCRIPTHUT_SIBLINGS__\n", "", 0),
        ])
        manager = _manager(datasets=[DatasetConfig(name="acq", path=local)], ssh=ssh)

        plans, warnings = await manager._plan_data_deps(
            ["acq"], backend_name="mercury", ssh_client=ssh, label="wf",
        )

        assert len(plans) == 1
        assert plans[0].reused is True
        assert plans[0].dest == f"/scratch/w/acq/{hash12}"
        assert warnings == []

    @pytest.mark.asyncio
    async def test_superseded_copies_produce_a_warning(self, tmp_path: Path):
        local = _local_dataset(tmp_path)
        ssh = _ssh()
        ssh.run_command = AsyncMock(side_effect=[
            ("HOME\t/home/w\nSCRATCH\t/scratch/w\n", "", 0),
            ("__SCRIPTHUT_SIBLINGS__\naaaaaaaaaaaa\nbbbbbbbbbbbb\n", "", 0),
        ])
        manager = _manager(datasets=[DatasetConfig(name="acq", path=local)], ssh=ssh)

        plans, warnings = await manager._plan_data_deps(
            ["acq"], backend_name="mercury", ssh_client=ssh, label="wf",
        )

        assert plans[0].must_stage is True
        assert len(warnings) == 1
        assert "2 superseded copies" in warnings[0]
        assert "aaaaaaaaaaaa" in warnings[0]


# ---------- env injection and item shaping --------------------------------


class TestApplyDataDeps:
    def test_no_datasets_changes_nothing(self):
        tasks = [_task("a")]
        env = [EnvRule(set={"X": "1"})]
        out_tasks, out_env = RunManager._apply_data_deps([], tasks, env)
        assert out_tasks == tasks
        assert out_env == env

    def test_single_dataset_sets_data_dir_and_the_namespaced_var(
        self, tmp_path: Path
    ):
        plan = _plan(tmp_path, reused=True)
        _, env = RunManager._apply_data_deps([plan], [_task("a")], [])
        assert env[0].set == {"DATA_DIR": plan.dest}
        assert env[-1].set == {"DATA_ACQ": plan.dest}

    def test_data_dir_is_prepended_so_the_workflow_can_override_it(
        self, tmp_path: Path
    ):
        plan = _plan(tmp_path, reused=True)
        own = EnvRule(set={"DATA_DIR": "/my/own"})
        _, env = RunManager._apply_data_deps([plan], [_task("a")], [own])
        assert env.index(own) > 0, "the workflow's own DATA_DIR must win"

    def test_two_datasets_do_not_get_a_data_dir(self, tmp_path: Path):
        plans = [
            _plan(tmp_path / "one", reused=True, name="acq"),
            _plan(tmp_path / "two", reused=True, name="ref"),
        ]
        _, env = RunManager._apply_data_deps(plans, [_task("a")], [])
        assert len(env) == 1
        assert set(env[0].set) == {"DATA_ACQ", "DATA_REF"}

    def test_present_data_adds_no_staging_item(self, tmp_path: Path):
        plan = _plan(tmp_path, reused=True)
        tasks, _ = RunManager._apply_data_deps([plan], [_task("a")], [])
        assert [t.id for t in tasks] == ["a"]
        assert tasks[0].dependencies == []

    def test_a_task_cannot_shadow_a_staging_item(self, tmp_path: Path):
        plan = _plan(tmp_path, reused=False)
        with pytest.raises(ValueError, match="reserved"):
            RunManager._apply_data_deps([plan], [_task("_data.acq")], [])

    def test_staging_item_does_not_decide_the_runs_log_dir(self, tmp_path: Path):
        # _build_run reads the first task's working_dir; a prepended staging
        # item must not be the one it reads.
        plan = _plan(tmp_path, reused=False)
        real = _task("a")
        real.working_dir = "/scratch/w/clones/abc"
        tasks, _ = RunManager._apply_data_deps([plan], [real], [])
        first_real = next(t for t in tasks if t.data_dep is None)
        assert first_real.working_dir == "/scratch/w/clones/abc"

    def test_env_rules_match_what_the_cache_probe_applies(self, tmp_path: Path):
        # The probe must key on the same env a submission would, or its
        # hit/miss verdict is about a task that never runs.
        plan = _plan(tmp_path, reused=False)
        _, submit_env = RunManager._apply_data_deps([plan], [_task("a")], [])
        probe_env = RunManager._data_env_rules([plan], [])
        assert [r.set for r in probe_env] == [r.set for r in submit_env]

    def test_injected_vars_survive_the_env_resolver(self, tmp_path: Path):
        # Regression: these were once named SCRIPTHUT_DATA_*, which the
        # resolver silently drops -- it refuses to let any rule set a key in
        # that reserved namespace, so the variable never reached the task and
        # only a log line said so.
        from scripthut.config_schema import ScriptHutConfig
        from scripthut.runs.env import resolve_for_task

        plan = _plan(tmp_path, reused=False)
        task = _task("a")
        env_rules = RunManager._data_env_rules([plan], [])
        merged, _ = resolve_for_task(
            ScriptHutConfig(),
            backend_name="mercury",
            workflow_name="w.json",
            run_id="r1",
            created_at=datetime.now(timezone.utc),
            task=task,
            doc_env=env_rules,
        )
        assert merged["DATA_ACQ"] == plan.dest
        assert merged["DATA_DIR"] == plan.dest

    def test_injected_vars_reach_the_cache_key(self, tmp_path: Path):
        # The claim that changing data invalidates downstream results only
        # holds if the cache actually hashes these names; it strips
        # SCRIPTHUT_* as volatile.
        from scripthut.runs.cache import CacheManager

        plan = _plan(tmp_path, reused=False)
        env = {"DATA_DIR": plan.dest, "DATA_ACQ": plan.dest, "SCRIPTHUT_RUN_ID": "r1"}
        keyed = CacheManager._env_for_key(env)
        assert keyed == {"DATA_DIR": plan.dest, "DATA_ACQ": plan.dest}

    def test_a_dataset_may_not_be_named_dir(self):
        # data_env_var("dir") is DATA_DIR, which would mean two things at once.
        with pytest.raises(ValueError, match="reserved"):
            DatasetConfig(name="dir", path=Path("/tmp/x"))

    def test_dry_run_preview_shows_the_same_vars_a_run_would_get(self, tmp_path: Path):
        # A preview that omits DATA_* describes a script that would never run.
        plan = _plan(tmp_path, reused=False)
        _, submit_env = RunManager._apply_data_deps([plan], [_task("a")], [])
        preview_env = RunManager._data_env_rules([plan], [])
        assert [r.set for r in preview_env] == [r.set for r in submit_env]

    def test_env_rules_do_not_mutate_task_dependencies(self, tmp_path: Path):
        plan = _plan(tmp_path, reused=False)
        tasks = [_task("a")]
        RunManager._data_env_rules([plan], [])
        assert tasks[0].dependencies == []

    def test_missing_data_gates_the_root_tasks(self, tmp_path: Path):
        plan = _plan(tmp_path, reused=False)
        tasks, _ = RunManager._apply_data_deps(
            [plan], [_task("a"), _task("b", deps=["a"])], [],
        )
        assert [t.id for t in tasks] == ["_data.acq", "a", "b"]
        assert tasks[0].data_dep is not None
        assert tasks[0].data_dep.dest == plan.dest
        # Only roots gain the dep; "b" inherits it through "a".
        assert tasks[1].dependencies == ["_data.acq"]
        assert tasks[2].dependencies == ["a"]


# ---------- the staging item lifecycle ------------------------------------


def _staging_run(tmp_path: Path, *, hash12: str | None = None) -> Run:
    local = _local_dataset(tmp_path)
    manifest = build_manifest(local)
    dep = DataDep(
        name="acq",
        local_path=str(local),
        dest=f"/scratch/w/acq/{manifest.short}",
        hash=hash12 or manifest.short,
    )
    staging = TaskDefinition(
        id="_data.acq", name="Stage acq", command=": stage acq", data_dep=dep,
    )
    return Run(
        id="r1",
        workflow_name="wf",
        backend_name="mercury",
        created_at=datetime.now(timezone.utc),
        items=[
            RunItem(task=staging),
            RunItem(task=_task("a", deps=["_data.acq"])),
        ],
        max_concurrent=None,
    )


def _job_backend() -> AsyncMock:
    backend = AsyncMock()
    backend.submit_task = AsyncMock(return_value=MagicMock(job_id="1", submit_output=""))
    backend.generate_script = MagicMock(return_value="#!/bin/bash")
    return backend


class TestStagingItem:
    @pytest.mark.asyncio
    async def test_successful_transfer_completes_and_releases_dependents(
        self, tmp_path: Path
    ):
        run = _staging_run(tmp_path)
        ssh = _ssh()
        manager = _manager(ssh=ssh)
        manager.job_backends = {"mercury": _job_backend()}
        manager.runs[run.id] = run

        with patch(
            "scripthut.runs.datasets.probe_presence",
            AsyncMock(return_value=(False, ())),
        ), patch(
            "scripthut.runs.datasets.stage_dataset", AsyncMock(return_value=4096)
        ):
            await manager.process_run(run)
            staging_item = run.items[0]
            assert staging_item.status == RunItemStatus.RUNNING
            # The dependent must not have been submitted yet.
            assert run.items[1].status == RunItemStatus.PENDING

            await manager._staging_tasks[f"{run.id}:_data.acq"]

        assert staging_item.status == RunItemStatus.COMPLETED
        assert "4096 bytes" in (staging_item.submit_output or "")
        assert run.items[1].status == RunItemStatus.SUBMITTED

    @pytest.mark.asyncio
    async def test_already_present_on_acquire_skips_the_transfer(
        self, tmp_path: Path
    ):
        run = _staging_run(tmp_path)
        manager = _manager(ssh=_ssh())
        manager.job_backends = {"mercury": _job_backend()}
        manager.runs[run.id] = run
        stage = AsyncMock(return_value=0)

        with patch(
            "scripthut.runs.datasets.probe_presence",
            AsyncMock(return_value=(True, ())),
        ), patch("scripthut.runs.datasets.stage_dataset", stage):
            await manager.process_run(run)
            await manager._staging_tasks[f"{run.id}:_data.acq"]

        stage.assert_not_called()
        assert run.items[0].status == RunItemStatus.COMPLETED
        assert "Reused existing copy" in (run.items[0].submit_output or "")

    @pytest.mark.asyncio
    async def test_failed_transfer_fails_the_run_rather_than_running_without_data(
        self, tmp_path: Path
    ):
        run = _staging_run(tmp_path)
        manager = _manager(ssh=_ssh())
        job_backend = _job_backend()
        manager.job_backends = {"mercury": job_backend}
        manager.runs[run.id] = run

        with patch(
            "scripthut.runs.datasets.probe_presence",
            AsyncMock(return_value=(False, ())),
        ), patch(
            "scripthut.runs.datasets.stage_dataset",
            AsyncMock(side_effect=RuntimeError("Disk quota exceeded")),
        ):
            await manager.process_run(run)
            await manager._staging_tasks[f"{run.id}:_data.acq"]

        assert run.items[0].status == RunItemStatus.FAILED
        assert "Disk quota exceeded" in (run.items[0].error or "")
        assert run.items[1].status == RunItemStatus.DEP_FAILED
        assert run.status == RunStatus.FAILED
        job_backend.submit_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_local_data_changed_since_creation_is_refused(self, tmp_path: Path):
        run = _staging_run(tmp_path, hash12="0" * 12)
        manager = _manager(ssh=_ssh())
        manager.job_backends = {"mercury": _job_backend()}
        manager.runs[run.id] = run
        stage = AsyncMock(return_value=0)

        with patch(
            "scripthut.runs.datasets.probe_presence",
            AsyncMock(return_value=(False, ())),
        ), patch("scripthut.runs.datasets.stage_dataset", stage):
            await manager.process_run(run)
            await manager._staging_tasks[f"{run.id}:_data.acq"]

        stage.assert_not_called()
        assert run.items[0].status == RunItemStatus.FAILED
        assert "changed since this run was created" in (run.items[0].error or "")

    @pytest.mark.asyncio
    async def test_backend_without_ssh_fails_the_item_immediately(
        self, tmp_path: Path
    ):
        run = _staging_run(tmp_path)
        manager = _manager()  # no SSH client registered
        manager.runs[run.id] = run

        await manager.process_run(run)

        assert run.items[0].status == RunItemStatus.FAILED
        assert "no filesystem" in (run.items[0].error or "")
        assert run.items[1].status == RunItemStatus.DEP_FAILED

    @pytest.mark.asyncio
    async def test_transfer_does_not_consume_a_scheduler_slot(self, tmp_path: Path):
        run = _staging_run(tmp_path)
        run.max_concurrent = 1
        manager = _manager(ssh=_ssh())
        manager.job_backends = {"mercury": _job_backend()}
        manager.runs[run.id] = run

        with patch(
            "scripthut.runs.datasets.probe_presence",
            AsyncMock(return_value=(False, ())),
        ), patch("scripthut.runs.datasets.stage_dataset", AsyncMock(return_value=1)):
            await manager.process_run(run)

            # RUNNING, yet it must not count against the backend or the run.
            assert run.items[0].status == RunItemStatus.RUNNING
            assert run.running_count == 1
            assert run.scheduler_running_count == 0
            assert manager._backend_running_count("mercury") == 0

            await manager._staging_tasks[f"{run.id}:_data.acq"]

        # With max_concurrent=1 the dependent still got its slot.
        assert run.items[1].status == RunItemStatus.SUBMITTED

    @pytest.mark.asyncio
    async def test_cancelling_a_run_stops_the_transfer(self, tmp_path: Path):
        run = _staging_run(tmp_path)
        manager = _manager(ssh=_ssh())
        manager.runs[run.id] = run
        started = asyncio.Event()

        async def _never_finishes(*args, **kwargs):
            started.set()
            await asyncio.sleep(3600)

        with patch(
            "scripthut.runs.datasets.probe_presence",
            AsyncMock(return_value=(False, ())),
        ), patch("scripthut.runs.datasets.stage_dataset", _never_finishes):
            await manager.process_run(run)
            transfer = manager._staging_tasks[f"{run.id}:_data.acq"]
            await started.wait()

            await manager.cancel_run(run.id)
            await asyncio.wait([transfer], timeout=5)

        assert transfer.done()
        assert run.items[0].status == RunItemStatus.FAILED
        assert run.items[0].error == "Cancelled"

    def test_restart_requeues_an_interrupted_transfer(self, tmp_path: Path):
        run = _staging_run(tmp_path)
        run.items[0].status = RunItemStatus.RUNNING
        run.items[0].started_at = datetime.now(timezone.utc)

        RunManager._requeue_orphaned_staging(run)

        assert run.items[0].status == RunItemStatus.PENDING
        assert run.items[0].started_at is None

    def test_restart_leaves_finished_transfers_alone(self, tmp_path: Path):
        run = _staging_run(tmp_path)
        run.items[0].status = RunItemStatus.COMPLETED

        RunManager._requeue_orphaned_staging(run)

        assert run.items[0].status == RunItemStatus.COMPLETED

    def test_poller_ignores_staging_items(self, tmp_path: Path):
        # No job_id means squeue can never speak to this item; the poller's
        # existing guard is what keeps it from being mis-marked.
        run = _staging_run(tmp_path)
        assert run.items[0].job_id is None
