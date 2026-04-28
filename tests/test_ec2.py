"""Tests for the AWS EC2-direct backend."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from scripthut.backends.ec2 import (
    EC2_FAILURE_STATES,
    EC2_TERMINAL_STATES,
    EC2Backend,
    _InstanceState,
    _hpc_job_from_instance,
)
from scripthut.backends.ec2_ssm import find_free_port
from scripthut.config_schema import AWSEC2Config, EC2BackendConfig
from scripthut.models import JobState
from scripthut.runs.models import TaskDefinition


def _make_config(**overrides) -> EC2BackendConfig:
    defaults: dict = dict(
        name="test-ec2",
        type="ec2",
        aws=AWSEC2Config(region="us-east-1"),
        ami="ami-0123456789abcdef0",
        subnet_id="subnet-01",
        security_group_ids=["sg-01"],
        instance_types={"default": "c5.xlarge", "gpu": "g4dn.xlarge"},
        default_image="ghcr.io/org/img:latest",
        max_instances=3,
    )
    defaults.update(overrides)
    return EC2BackendConfig(**defaults)


def _make_backend(tmp_path: Path, **overrides) -> EC2Backend:
    config = _make_config(**overrides)
    backend = EC2Backend(config, archive_root=tmp_path / "archive")
    backend._ec2_client = MagicMock()
    backend._ec2ic_client = MagicMock()
    return backend


def _make_task(**overrides) -> TaskDefinition:
    defaults = dict(
        id="t1",
        name="hello",
        command="echo hi",
        working_dir="/work",
        cpus=2,
        memory="4G",
        time_limit="1:00:00",
    )
    defaults.update(overrides)
    return TaskDefinition(**defaults)


def _patch_open_ssh_session(backend: EC2Backend, client_mock: MagicMock) -> None:
    """Replace EC2Backend._open_ssh_session with a mock yielding `client_mock`."""

    @asynccontextmanager
    async def _session_ctx():
        session_obj = MagicMock()
        session_obj.client = client_mock
        yield session_obj

    async def _opener(_state: _InstanceState):
        # Return the context manager itself (not the result of entering it);
        # callers do ``async with await self._open_ssh_session(s): ...``.
        return _session_ctx()

    backend._open_ssh_session = _opener  # type: ignore[assignment]


# -- simple utility tests --


class TestUtilities:
    def test_find_free_port_returns_int(self):
        port = find_free_port()
        assert isinstance(port, int)
        assert 1024 <= port < 65536

    def test_state_constants(self):
        assert "FAILED" in EC2_FAILURE_STATES
        assert "SUCCEEDED" in EC2_TERMINAL_STATES
        assert "FAILED" in EC2_TERMINAL_STATES


# -- user-data generation --


class TestUserData:
    def test_contains_expected_patterns(self, tmp_path):
        backend = _make_backend(tmp_path)
        task = _make_task()
        user_data = backend._build_user_data(
            task=task,
            script="#!/bin/bash\necho inside container\n",
            env_vars={"FOO": "bar", "BAZ": "q ux"},
            image="ghcr.io/org/img:latest",
        )
        # Safety timer
        assert "sleep " in user_data
        assert "shutdown -h now" in user_data
        # Task script gets base64-decoded
        assert "base64 -d" in user_data
        # Docker invocation
        assert "docker run --rm" in user_data
        assert "ghcr.io/org/img:latest" in user_data
        # Env flags from env_vars: keys appear; values are shell-quoted
        assert "FOO=bar" in user_data
        assert "BAZ=q ux" in user_data or "BAZ=q\\ ux" in user_data or "'BAZ=q ux'" in user_data
        # Sentinel written last
        assert "/var/run/scripthut/done" in user_data
        # Hard task timeout = time_limit
        assert "timeout 3600s" in user_data

    def test_safety_timer_exceeds_time_limit(self, tmp_path):
        backend = _make_backend(tmp_path, idle_terminate_seconds=900)
        task = _make_task(time_limit="0:30:00")  # 1800s
        ud = backend._build_user_data(
            task=task, script="#!/bin/bash\n",
            env_vars={}, image="img:t",
        )
        # Safety shutdown must be time_limit + idle_terminate_seconds = 1800 + 900 = 2700
        assert "sleep 2700" in ud


# -- instance_type resolution --


class TestInstanceTypeResolution:
    def test_partition_lookup(self, tmp_path):
        backend = _make_backend(tmp_path)
        assert backend._instance_type_for(_make_task(partition="gpu")) == "g4dn.xlarge"

    def test_default_fallback_on_unknown_partition(self, tmp_path):
        backend = _make_backend(tmp_path)
        assert backend._instance_type_for(_make_task(partition="missing")) == "c5.xlarge"

    def test_default_fallback_on_normal(self, tmp_path):
        """TaskDefinition's Slurm-default 'normal' should fall back to 'default'."""
        backend = _make_backend(tmp_path)
        assert backend._instance_type_for(_make_task(partition="normal")) == "c5.xlarge"


# -- submit_task --


class TestSubmit:
    @pytest.mark.asyncio
    async def test_submit_task_calls_run_instances_with_expected_fields(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend._ec2_client.run_instances.return_value = {
            "Instances": [{
                "InstanceId": "i-abc123",
                "Placement": {"AvailabilityZone": "us-east-1a"},
            }],
        }
        task = _make_task(partition="gpu")
        result = await backend.submit_task(
            task, "#!/bin/bash\necho hi\n",
            env_vars={"SCRIPTHUT_RUN_ID": "run-xyz"},
        )
        assert result.job_id == "i-abc123"

        kwargs = backend._ec2_client.run_instances.call_args.kwargs
        assert kwargs["ImageId"] == "ami-0123456789abcdef0"
        assert kwargs["InstanceType"] == "g4dn.xlarge"  # from partition
        assert kwargs["SubnetId"] == "subnet-01"
        assert kwargs["SecurityGroupIds"] == ["sg-01"]
        assert kwargs["InstanceInitiatedShutdownBehavior"] == "terminate"
        # Tags include scripthut:backend/task-id/run-id and Name
        tags = {t["Key"]: t["Value"] for t in kwargs["TagSpecifications"][0]["Tags"]}
        assert tags["scripthut:backend"] == "test-ec2"
        assert tags["scripthut:task-id"] == "t1"
        assert tags["scripthut:run-id"] == "run-xyz"
        assert tags["Name"].startswith("scripthut-")
        # Recorded in state
        st = backend._instances["i-abc123"]
        assert st.availability_zone == "us-east-1a"
        assert st.task_id == "t1"
        assert st.run_id == "run-xyz"
        assert st.instance_type == "g4dn.xlarge"

    @pytest.mark.asyncio
    async def test_submit_respects_max_instances(self, tmp_path):
        backend = _make_backend(tmp_path, max_instances=1)
        backend._ec2_client.run_instances.return_value = {
            "Instances": [{"InstanceId": "i-a", "Placement": {"AvailabilityZone": "us-east-1a"}}]
        }
        await backend.submit_task(_make_task(id="t1"), "s")
        with pytest.raises(RuntimeError, match="max_instances"):
            await backend.submit_task(_make_task(id="t2"), "s")
        # Only one RunInstances call reached boto3.
        assert backend._ec2_client.run_instances.call_count == 1

    @pytest.mark.asyncio
    async def test_submit_requires_image(self, tmp_path):
        backend = _make_backend(tmp_path, default_image=None)
        task = _make_task(image=None)
        with pytest.raises(RuntimeError, match="container image"):
            await backend.submit_task(task, "s")

    @pytest.mark.asyncio
    async def test_submit_uses_task_image_over_default(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend._ec2_client.run_instances.return_value = {
            "Instances": [{"InstanceId": "i-a", "Placement": {"AvailabilityZone": "us-east-1a"}}]
        }
        await backend.submit_task(
            _make_task(image="other:tag"),
            "#!/bin/bash\n",
        )
        kwargs = backend._ec2_client.run_instances.call_args.kwargs
        # The image is baked into UserData (not a top-level RunInstances field).
        assert "other:tag" in kwargs["UserData"]

    @pytest.mark.asyncio
    async def test_submit_job_direct_raises(self, tmp_path):
        backend = _make_backend(tmp_path)
        with pytest.raises(RuntimeError, match="submit_task"):
            await backend.submit_job("s")


# -- cancel_job --


class TestCancel:
    @pytest.mark.asyncio
    async def test_cancel_terminates_and_moves_to_completed(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend._instances["i-a"] = _InstanceState(
            instance_id="i-a", task_id="t1", run_id="r1",
            launched_at=datetime.now(timezone.utc),
        )
        await backend.cancel_job("i-a")
        backend._ec2_client.terminate_instances.assert_called_once()
        assert "i-a" not in backend._instances
        assert backend._completed["i-a"].terminal_state == "TERMINATED_EARLY"


# -- polling / finalization --


class TestPolling:
    @pytest.mark.asyncio
    async def test_running_without_sentinel_stays_active(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend._instances["i-a"] = _InstanceState(
            instance_id="i-a", task_id="t1", run_id="r1",
            launched_at=datetime.now(timezone.utc),
            availability_zone="us-east-1a",
            instance_type="c5.xlarge",
        )
        # EC2 says running, sentinel missing.
        backend._ec2_client.describe_instances.return_value = {
            "Reservations": [{
                "Instances": [{
                    "InstanceId": "i-a",
                    "State": {"Name": "running"},
                    "Placement": {"AvailabilityZone": "us-east-1a"},
                    "InstanceType": "c5.xlarge",
                    "LaunchTime": datetime.now(timezone.utc),
                    "Tags": [
                        {"Key": "scripthut:backend", "Value": "test-ec2"},
                        {"Key": "scripthut:task-id", "Value": "t1"},
                    ],
                }]
            }]
        }
        # SSH sentinel probe returns MISSING.
        ssh_client = MagicMock()
        ssh_client.run_command = AsyncMock(return_value=("MISSING\n", "", 0))
        _patch_open_ssh_session(backend, ssh_client)

        jobs = await backend.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].job_id == "i-a"
        assert jobs[0].state == JobState.RUNNING
        assert "i-a" in backend._instances
        assert "i-a" not in backend._completed

    @pytest.mark.asyncio
    async def test_sentinel_triggers_archive_and_terminate(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend._instances["i-a"] = _InstanceState(
            instance_id="i-a", task_id="t1", run_id="r1",
            launched_at=datetime.now(timezone.utc),
            availability_zone="us-east-1a",
            instance_type="c5.xlarge",
        )
        backend._ec2_client.describe_instances.return_value = {
            "Reservations": [{
                "Instances": [{
                    "InstanceId": "i-a",
                    "State": {"Name": "running"},
                    "Placement": {"AvailabilityZone": "us-east-1a"},
                    "InstanceType": "c5.xlarge",
                    "LaunchTime": datetime.now(timezone.utc),
                    "Tags": [
                        {"Key": "scripthut:backend", "Value": "test-ec2"},
                        {"Key": "scripthut:task-id", "Value": "t1"},
                    ],
                }]
            }]
        }
        # Scripted SSH: first call returns sentinel contents "0",
        # second call returns the archived log body.
        ssh_client = MagicMock()
        ssh_client.run_command = AsyncMock(side_effect=[
            ("0\n", "", 0),                    # sentinel
            ("task output goes here\n", "", 0),  # fetch_log_via_ssh -> cat
        ])
        _patch_open_ssh_session(backend, ssh_client)

        jobs = await backend.get_jobs()
        # Finished instance is NOT returned as active.
        assert jobs == []
        assert "i-a" not in backend._instances
        done = backend._completed["i-a"]
        assert done.exit_code == 0
        assert done.terminal_state == "SUCCEEDED"
        assert done.log_archive_path is not None
        assert done.log_archive_path.exists()
        assert "task output" in done.log_archive_path.read_text()
        # We called TerminateInstances on it.
        backend._ec2_client.terminate_instances.assert_called_once_with(
            InstanceIds=["i-a"]
        )

    @pytest.mark.asyncio
    async def test_nonzero_exit_code_maps_to_failed(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend._instances["i-a"] = _InstanceState(
            instance_id="i-a", task_id="t1", run_id="r1",
            launched_at=datetime.now(timezone.utc),
            availability_zone="us-east-1a",
        )
        backend._ec2_client.describe_instances.return_value = {
            "Reservations": [{
                "Instances": [{
                    "InstanceId": "i-a",
                    "State": {"Name": "running"},
                    "Placement": {"AvailabilityZone": "us-east-1a"},
                    "LaunchTime": datetime.now(timezone.utc),
                    "Tags": [{"Key": "scripthut:backend", "Value": "test-ec2"},
                             {"Key": "scripthut:task-id", "Value": "t1"}],
                }]
            }]
        }
        ssh_client = MagicMock()
        ssh_client.run_command = AsyncMock(side_effect=[
            ("42\n", "", 0),
            ("stderr-ish noise\n", "", 0),
        ])
        _patch_open_ssh_session(backend, ssh_client)

        await backend.get_jobs()
        assert backend._completed["i-a"].exit_code == 42
        assert backend._completed["i-a"].terminal_state == "FAILED"

    @pytest.mark.asyncio
    async def test_terminated_instance_without_sentinel_marks_terminated_early(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend._instances["i-a"] = _InstanceState(
            instance_id="i-a", task_id="t1", run_id="r1",
            launched_at=datetime.now(timezone.utc),
            availability_zone="us-east-1a",
        )
        # EC2 reports instance as already shutting-down.
        backend._ec2_client.describe_instances.return_value = {
            "Reservations": [{
                "Instances": [{
                    "InstanceId": "i-a",
                    "State": {"Name": "shutting-down"},
                    "Placement": {"AvailabilityZone": "us-east-1a"},
                    "LaunchTime": datetime.now(timezone.utc),
                    "Tags": [{"Key": "scripthut:backend", "Value": "test-ec2"},
                             {"Key": "scripthut:task-id", "Value": "t1"}],
                }]
            }]
        }
        jobs = await backend.get_jobs()
        assert jobs == []
        assert backend._completed["i-a"].terminal_state == "TERMINATED_EARLY"

    @pytest.mark.asyncio
    async def test_startup_reconcile_picks_up_tagged_instance(self, tmp_path):
        """An instance that scripthut didn't launch this session but is tagged
        with our backend should be adopted on the next poll."""
        backend = _make_backend(tmp_path)
        assert backend._instances == {}

        backend._ec2_client.describe_instances.return_value = {
            "Reservations": [{
                "Instances": [{
                    "InstanceId": "i-orphan",
                    "State": {"Name": "running"},
                    "Placement": {"AvailabilityZone": "us-east-1b"},
                    "InstanceType": "c5.xlarge",
                    "LaunchTime": datetime.now(timezone.utc),
                    "Tags": [
                        {"Key": "scripthut:backend", "Value": "test-ec2"},
                        {"Key": "scripthut:task-id", "Value": "prior-task"},
                        {"Key": "scripthut:run-id", "Value": "prior-run"},
                    ],
                }]
            }]
        }
        # Sentinel still missing → stays active.
        ssh_client = MagicMock()
        ssh_client.run_command = AsyncMock(return_value=("MISSING\n", "", 0))
        _patch_open_ssh_session(backend, ssh_client)

        jobs = await backend.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].job_id == "i-orphan"
        assert "i-orphan" in backend._instances
        assert backend._instances["i-orphan"].task_id == "prior-task"


# -- get_job_stats --


class TestGetJobStats:
    @pytest.mark.asyncio
    async def test_returns_stats_for_completed(self, tmp_path):
        backend = _make_backend(tmp_path)
        start = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)
        backend._completed["i-a"] = _InstanceState(
            instance_id="i-a", task_id="t1", run_id="r1",
            launched_at=start, started_at=start, completed_at=end,
            exit_code=0, terminal_state="SUCCEEDED",
        )
        stats = await backend.get_job_stats(["i-a", "i-missing"])
        assert set(stats.keys()) == {"i-a"}
        assert stats["i-a"].state == "SUCCEEDED"
        assert stats["i-a"].total_cpu == "300s"
        assert stats["i-a"].start_time == start
        assert stats["i-a"].end_time == end

    @pytest.mark.asyncio
    async def test_empty_ids_short_circuit(self, tmp_path):
        backend = _make_backend(tmp_path)
        assert await backend.get_job_stats([]) == {}


# -- fetch_log --


class TestFetchLog:
    @pytest.mark.asyncio
    async def test_error_log_returns_note(self, tmp_path):
        backend = _make_backend(tmp_path)
        content, err = await backend.fetch_log("i-a", "ignored", log_type="error")
        assert err is None
        assert "output tab" in (content or "")

    @pytest.mark.asyncio
    async def test_completed_reads_archive(self, tmp_path):
        backend = _make_backend(tmp_path)
        archive = tmp_path / "archive" / "r1" / "t1.log"
        archive.parent.mkdir(parents=True)
        archive.write_text("l1\nl2\nl3\n")
        backend._completed["i-a"] = _InstanceState(
            instance_id="i-a", task_id="t1", run_id="r1",
            launched_at=datetime.now(timezone.utc),
            log_archive_path=archive,
        )
        content, err = await backend.fetch_log("i-a", "ignored")
        assert err is None
        assert content == "l1\nl2\nl3\n"

    @pytest.mark.asyncio
    async def test_completed_tail_lines_truncate(self, tmp_path):
        backend = _make_backend(tmp_path)
        archive = tmp_path / "archive" / "r1" / "t1.log"
        archive.parent.mkdir(parents=True)
        archive.write_text("\n".join(f"line{i}" for i in range(10)))
        backend._completed["i-a"] = _InstanceState(
            instance_id="i-a", task_id="t1", run_id="r1",
            launched_at=datetime.now(timezone.utc),
            log_archive_path=archive,
        )
        content, _ = await backend.fetch_log("i-a", "ignored", tail_lines=3)
        assert content is not None
        assert content.splitlines() == ["line7", "line8", "line9"]

    @pytest.mark.asyncio
    async def test_active_live_tails_via_ssh(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend._instances["i-a"] = _InstanceState(
            instance_id="i-a", task_id="t1", run_id="r1",
            launched_at=datetime.now(timezone.utc),
            availability_zone="us-east-1a",
        )
        ssh_client = MagicMock()
        ssh_client.run_command = AsyncMock(return_value=("live content\n", "", 0))
        _patch_open_ssh_session(backend, ssh_client)

        content, err = await backend.fetch_log("i-a", "ignored", tail_lines=5)
        assert err is None
        assert content == "live content\n"
        # Verify fetch_log_via_ssh issued a tail command.
        issued = ssh_client.run_command.call_args[0][0]
        assert "tail" in issued and "/var/log/scripthut/task.log" in issued


# -- generate_script --


class TestGenerateScript:
    def test_no_sbatch_directives(self, tmp_path):
        backend = _make_backend(tmp_path)
        script = backend.generate_script(_make_task(), "run1", "ignored-log-dir")
        assert "#SBATCH" not in script
        assert "#PBS" not in script
        assert script.startswith("#!/bin/bash")
        assert "cd /work" in script
        assert "echo hi" in script

    def test_env_vars_not_exported(self, tmp_path):
        """EC2 injects env vars via docker run -e; the script doesn't."""
        backend = _make_backend(tmp_path)
        script = backend.generate_script(
            _make_task(), "run1", "ld",
            env_vars={"FOO": "bar"},
        )
        assert 'export FOO="bar"' not in script
        assert "docker run -e" in script or "FOO" in script  # name visible in comment

    def test_git_repo_injects_clone_block(self, tmp_path):
        backend = _make_backend(tmp_path)
        task = _make_task(working_dir="subdir")
        script = backend.generate_script(
            task, "run1", "ld",
            env_vars={
                "SCRIPTHUT_GIT_REPO": "https://github.com/org/repo.git",
                "SCRIPTHUT_GIT_SHA": "abc123",
            },
        )
        assert "git clone https://github.com/org/repo.git" in script
        assert "git checkout abc123" in script
        assert "$_SCRIPTHUT_CLONE_DIR/subdir" in script


# -- _hpc_job_from_instance --


class TestHPCJobFromInstance:
    def test_running(self):
        state = _InstanceState(
            instance_id="i-a", task_id="t1", run_id="r1",
            launched_at=datetime.now(timezone.utc),
            instance_type="c5.xlarge",
        )
        inst = {
            "InstanceId": "i-a",
            "State": {"Name": "running"},
            "Placement": {"AvailabilityZone": "us-east-1a"},
            "LaunchTime": datetime.now(timezone.utc),
        }
        hpc = _hpc_job_from_instance(state, inst)
        assert hpc.job_id == "i-a"
        assert hpc.name == "t1"
        assert hpc.state == JobState.RUNNING
        assert hpc.partition == "c5.xlarge"
        assert hpc.nodes == "us-east-1a"

    def test_pending(self):
        state = _InstanceState(
            instance_id="i-a", task_id="t1", run_id="r1",
            launched_at=datetime.now(timezone.utc),
        )
        inst = {"InstanceId": "i-a", "State": {"Name": "pending"}}
        hpc = _hpc_job_from_instance(state, inst)
        assert hpc.state == JobState.PENDING


# -- cluster_info --


class TestClusterInfo:
    @pytest.mark.asyncio
    async def test_returns_max_minus_used(self, tmp_path):
        backend = _make_backend(tmp_path, max_instances=10)
        backend._instances["i-a"] = _InstanceState(
            instance_id="i-a", task_id="t1", run_id="r1",
            launched_at=datetime.now(timezone.utc),
        )
        backend._instances["i-b"] = _InstanceState(
            instance_id="i-b", task_id="t2", run_id="r2",
            launched_at=datetime.now(timezone.utc),
        )
        total, idle = await backend.get_cluster_info() or (0, 0)
        assert total == 10
        assert idle == 8


# -- state constants exposed via JobBackend interface --


class TestStateContracts:
    def test_failure_and_terminal_states(self, tmp_path):
        backend = _make_backend(tmp_path)
        assert backend.failure_states == EC2_FAILURE_STATES
        assert backend.terminal_states == EC2_TERMINAL_STATES
