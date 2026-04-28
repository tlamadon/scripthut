"""Tests for the AWS Batch backend."""

from unittest.mock import MagicMock

import pytest

from scripthut.backends.base import JobStats
from scripthut.backends.batch import (
    BATCH_FAILURE_STATES,
    BATCH_TERMINAL_STATES,
    BatchBackend,
    _ms_to_datetime,
    gpu_count_from_gres,
    parse_memory_mib,
    parse_time_limit_seconds,
    sanitize_batch_name,
)
from scripthut.config_schema import AWSBatchConfig, BatchBackendConfig
from scripthut.models import JobState
from scripthut.runs.models import TaskDefinition


# -- Utility tests --


class TestParseMemoryMib:
    def test_gigabytes(self):
        assert parse_memory_mib("4G") == 4096

    def test_megabytes(self):
        assert parse_memory_mib("512M") == 512

    def test_kilobytes(self):
        assert parse_memory_mib("2048K") == 2

    def test_bare_number_treated_as_mib(self):
        assert parse_memory_mib("1024") == 1024

    def test_gb_suffix(self):
        assert parse_memory_mib("4GB") == 4096

    def test_empty_returns_default(self):
        assert parse_memory_mib("") == 2048

    def test_garbage_returns_default(self):
        assert parse_memory_mib("garbage") == 2048

    def test_fractional_gigabytes(self):
        assert parse_memory_mib("1.5G") == 1536


class TestParseTimeLimitSeconds:
    def test_hms(self):
        assert parse_time_limit_seconds("1:00:00") == 3600

    def test_mm_ss(self):
        assert parse_time_limit_seconds("10:00") == 600

    def test_day_format(self):
        assert parse_time_limit_seconds("1-00:00:00") == 86400

    def test_below_minimum_returns_none(self):
        assert parse_time_limit_seconds("0:30") is None  # 30s < 60s batch minimum

    def test_empty_returns_none(self):
        assert parse_time_limit_seconds("") is None

    def test_garbage_returns_none(self):
        assert parse_time_limit_seconds("garbage") is None


class TestGpuCountFromGres:
    def test_simple(self):
        assert gpu_count_from_gres("gpu:2") == 2

    def test_typed(self):
        assert gpu_count_from_gres("gpu:a100:4") == 4

    def test_bare_gpu(self):
        assert gpu_count_from_gres("gpu") == 1  # last segment is "gpu" → count 1

    def test_non_gpu_returns_zero(self):
        assert gpu_count_from_gres("mps:1") == 0

    def test_empty_returns_zero(self):
        assert gpu_count_from_gres("") == 0

    def test_none_returns_zero(self):
        assert gpu_count_from_gres(None) == 0


class TestSanitizeBatchName:
    def test_valid_unchanged(self):
        assert sanitize_batch_name("train-model-v1") == "train-model-v1"

    def test_spaces_become_dashes(self):
        assert sanitize_batch_name("my task 1") == "my-task-1"

    def test_slashes_removed(self):
        assert sanitize_batch_name("team/train/v1") == "team-train-v1"

    def test_truncated(self):
        assert len(sanitize_batch_name("a" * 200)) == 128

    def test_empty_fallback(self):
        assert sanitize_batch_name("") == "scripthut-task"


class TestMsToDatetime:
    def test_valid(self):
        dt = _ms_to_datetime(1_700_000_000_000)
        assert dt is not None
        assert dt.year == 2023

    def test_none(self):
        assert _ms_to_datetime(None) is None

    def test_garbage(self):
        assert _ms_to_datetime("not-a-number") is None


# -- Backend-level tests --


def _make_backend(
    *,
    default_image: str | None = "alpine:latest",
    job_queue: str = "q1",
    job_role: str | None = None,
    exec_role: str | None = None,
    job_definition: str | None = None,
    job_definition_mode: str = "locked",
    retry_attempts: int = 1,
) -> BatchBackend:
    config = BatchBackendConfig(
        name="test-batch",
        type="batch",
        aws=AWSBatchConfig(region="us-east-1", job_queue=job_queue),
        default_image=default_image,
        job_role_arn=job_role,
        execution_role_arn=exec_role,
        job_definition=job_definition,
        job_definition_mode=job_definition_mode,  # type: ignore[arg-type]
        retry_attempts=retry_attempts,
    )
    backend = BatchBackend(config)
    # Inject mocked boto3 clients so _get_clients doesn't try to import boto3.
    backend._batch_client = MagicMock()
    backend._logs_client = MagicMock()
    return backend


def _make_task(**overrides) -> TaskDefinition:
    defaults = {
        "id": "t1",
        "name": "my-task",
        "command": "python train.py",
        "working_dir": "/work",
        "partition": "q1",
        "cpus": 2,
        "memory": "4G",
        "time_limit": "1:00:00",
    }
    defaults.update(overrides)
    return TaskDefinition(**defaults)


class TestMetadata:
    def test_name_and_state_constants(self):
        backend = _make_backend()
        assert backend.name == "test-batch"
        assert backend.terminal_states == BATCH_TERMINAL_STATES
        assert backend.failure_states == BATCH_FAILURE_STATES
        assert "SUCCEEDED" in BATCH_TERMINAL_STATES
        assert "FAILED" in BATCH_FAILURE_STATES


class TestJobDictToHpc:
    def test_running_job(self):
        backend = _make_backend()
        j = {
            "jobId": "abc123",
            "jobName": "hello",
            "status": "RUNNING",
            "jobQueue": "arn:aws:batch:us-east-1:111:job-queue/my-queue",
            "createdAt": 1_700_000_000_000,
            "startedAt": 1_700_000_100_000,
            "container": {
                "resourceRequirements": [
                    {"type": "VCPU", "value": "4"},
                    {"type": "MEMORY", "value": "8192"},
                ]
            },
        }
        hpc = backend._job_dict_to_hpc(j)
        assert hpc.job_id == "abc123"
        assert hpc.name == "hello"
        assert hpc.state == JobState.RUNNING
        assert hpc.partition == "my-queue"
        assert hpc.cpus == 4
        assert hpc.memory == "8192M"

    def test_succeeded_maps_to_completed(self):
        backend = _make_backend()
        hpc = backend._job_dict_to_hpc({"jobId": "x", "status": "SUCCEEDED"})
        assert hpc.state == JobState.COMPLETED

    def test_runnable_maps_to_pending(self):
        backend = _make_backend()
        hpc = backend._job_dict_to_hpc({"jobId": "x", "status": "RUNNABLE"})
        assert hpc.state == JobState.PENDING


class TestGenerateScript:
    def test_basic_script_no_sbatch_directives(self):
        backend = _make_backend()
        task = _make_task()
        script = backend.generate_script(task, "run1", "ignored-log-dir")
        assert script.startswith("#!/bin/bash")
        # Batch scripts must NOT contain scheduler directives.
        assert "#SBATCH" not in script
        assert "#PBS" not in script
        assert "python train.py" in script
        assert "cd /work" in script

    def test_login_shell(self):
        backend = _make_backend()
        task = _make_task()
        script = backend.generate_script(task, "run1", "ld", login_shell=True)
        assert script.startswith("#!/bin/bash -l")

    def test_env_vars_not_exported_in_script(self):
        """Env vars are passed via containerOverrides.environment, not in the script."""
        backend = _make_backend()
        task = _make_task()
        script = backend.generate_script(
            task, "run1", "ld",
            env_vars={"FOO": "bar", "BAZ": "qux"},
        )
        # Must NOT inline exports — values go through the AWS API instead.
        assert 'export FOO="bar"' not in script
        assert 'export BAZ="qux"' not in script
        # But the script should document what's being set.
        assert "containerOverrides" in script
        assert "FOO" in script and "BAZ" in script

    def test_git_repo_triggers_clone_block(self):
        backend = _make_backend()
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
        assert "_SCRIPTHUT_CLONE_DIR" in script
        # working_dir "subdir" should be rewritten relative to the clone root.
        assert "$_SCRIPTHUT_CLONE_DIR/subdir" in script

    def test_absolute_working_dir_not_rewritten(self):
        backend = _make_backend()
        task = _make_task(working_dir="/opt/job")
        script = backend.generate_script(
            task, "run1", "ld",
            env_vars={
                "SCRIPTHUT_GIT_REPO": "https://github.com/org/repo.git",
            },
        )
        assert "cd /opt/job" in script
        # Should not rewrite to clone dir.
        assert "$_SCRIPTHUT_CLONE_DIR/opt" not in script

    def test_no_git_vars_skips_clone_block(self):
        backend = _make_backend()
        task = _make_task()
        script = backend.generate_script(task, "run1", "ld")
        assert "_SCRIPTHUT_CLONE_DIR" not in script


class TestSubmitTask:
    @pytest.mark.asyncio
    async def test_submit_task_maps_fields_to_batch(self):
        backend = _make_backend(default_image="alpine:latest")
        # Mock register_job_definition and submit_job returns.
        backend._batch_client.register_job_definition.return_value = {
            "jobDefinitionArn": "arn:aws:batch:us-east-1:111:job-definition/scripthut-abc:1",
        }
        backend._batch_client.submit_job.return_value = {"jobId": "job-xyz"}

        task = _make_task(cpus=4, memory="8G", gres="gpu:2", time_limit="2:00:00")
        result = await backend.submit_task(task, "#!/bin/bash\necho hi\n")

        assert result.job_id == "job-xyz"

        kwargs = backend._batch_client.submit_job.call_args.kwargs
        assert kwargs["jobName"] == "my-task"
        assert kwargs["jobQueue"] == "q1"
        overrides = kwargs["containerOverrides"]
        assert overrides["command"] == ["bash", "-c", "#!/bin/bash\necho hi\n"]
        rr = {r["type"]: r["value"] for r in overrides["resourceRequirements"]}
        assert rr["VCPU"] == "4"
        assert rr["MEMORY"] == "8192"
        assert rr["GPU"] == "2"
        assert kwargs["timeout"] == {"attemptDurationSeconds": 7200}

    @pytest.mark.asyncio
    async def test_submit_task_uses_task_image_over_default(self):
        backend = _make_backend(default_image="alpine:latest")
        backend._batch_client.register_job_definition.return_value = {
            "jobDefinitionArn": "arn:aws:batch:us-east-1:111:job-definition/scripthut-abc:1",
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j1"}

        task = _make_task(image="ghcr.io/org/custom:tag")
        await backend.submit_task(task, "#!/bin/bash\n")

        reg_kwargs = backend._batch_client.register_job_definition.call_args.kwargs
        assert reg_kwargs["containerProperties"]["image"] == "ghcr.io/org/custom:tag"

    @pytest.mark.asyncio
    async def test_submit_task_requires_image_when_auto_registering(self):
        """When no job_definition is configured, we need an image to register one."""
        backend = _make_backend(default_image=None)
        task = _make_task(image=None)
        with pytest.raises(RuntimeError, match="container image"):
            await backend.submit_task(task, "#!/bin/bash\n")

    @pytest.mark.asyncio
    async def test_job_definition_cache_reuses_registration(self):
        backend = _make_backend()
        backend._batch_client.register_job_definition.return_value = {
            "jobDefinitionArn": "arn:abc:1",
        }
        backend._batch_client.submit_job.return_value = {"jobId": "jid"}

        await backend.submit_task(_make_task(), "s1")
        await backend.submit_task(_make_task(), "s2")

        # Same image -> register_job_definition called exactly once.
        assert backend._batch_client.register_job_definition.call_count == 1
        assert backend._batch_client.submit_job.call_count == 2

    @pytest.mark.asyncio
    async def test_submit_job_direct_raises(self):
        backend = _make_backend()
        with pytest.raises(RuntimeError, match="submit_task"):
            await backend.submit_job("echo hi")

    @pytest.mark.asyncio
    async def test_env_vars_passed_via_container_overrides(self):
        backend = _make_backend()
        backend._batch_client.register_job_definition.return_value = {
            "jobDefinitionArn": "arn:x:1"
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j1"}

        task = _make_task()
        await backend.submit_task(
            task, "#!/bin/bash\n",
            env_vars={"S3_PATH": "s3://bucket/k", "CE_SCRIPT": "main.sh"},
        )

        overrides = backend._batch_client.submit_job.call_args.kwargs["containerOverrides"]
        env = {e["name"]: e["value"] for e in overrides["environment"]}
        assert env == {"S3_PATH": "s3://bucket/k", "CE_SCRIPT": "main.sh"}

    @pytest.mark.asyncio
    async def test_retry_attempts_attached(self):
        backend = _make_backend(retry_attempts=3)
        backend._batch_client.register_job_definition.return_value = {
            "jobDefinitionArn": "arn:x:1"
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j1"}

        await backend.submit_task(_make_task(), "#!/bin/bash\n")
        kwargs = backend._batch_client.submit_job.call_args.kwargs
        assert kwargs["retryStrategy"] == {"attempts": 3}

    @pytest.mark.asyncio
    async def test_default_partition_falls_back_to_aws_job_queue(self):
        """TaskDefinition.partition='normal' is a Slurm default — on Batch it
        must fall back to ``aws.job_queue``, not be sent as queue name 'normal'."""
        backend = _make_backend(job_queue="default-job-queue")
        backend._batch_client.register_job_definition.return_value = {
            "jobDefinitionArn": "arn:x:1"
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j1"}

        # _make_task uses partition="q1" by default — override to the
        # TaskDefinition actual default so we test the real code path.
        task = _make_task(partition="normal")
        await backend.submit_task(task, "#!/bin/bash\n")

        kwargs = backend._batch_client.submit_job.call_args.kwargs
        assert kwargs["jobQueue"] == "default-job-queue"

    @pytest.mark.asyncio
    async def test_explicit_partition_overrides_aws_job_queue(self):
        """When the task explicitly specifies a non-default partition, it wins."""
        backend = _make_backend(job_queue="default-job-queue")
        backend._batch_client.register_job_definition.return_value = {
            "jobDefinitionArn": "arn:x:1"
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j1"}

        task = _make_task(partition="my-explicit-queue")
        await backend.submit_task(task, "#!/bin/bash\n")

        kwargs = backend._batch_client.submit_job.call_args.kwargs
        assert kwargs["jobQueue"] == "my-explicit-queue"

    @pytest.mark.asyncio
    async def test_retry_attempts_default_omits_retry_strategy(self):
        backend = _make_backend()  # default retry_attempts=1
        backend._batch_client.register_job_definition.return_value = {
            "jobDefinitionArn": "arn:x:1"
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j1"}

        await backend.submit_task(_make_task(), "#!/bin/bash\n")
        kwargs = backend._batch_client.submit_job.call_args.kwargs
        assert "retryStrategy" not in kwargs


class TestPreRegisteredJobDefinition:
    """Tests for the job_definition backend config (skip RegisterJobDefinition)."""

    @pytest.mark.asyncio
    async def test_uses_configured_jd_and_skips_registration(self):
        backend = _make_backend(default_image=None, job_definition="simpleJobDef-b760b37")
        backend._batch_client.describe_job_definitions.return_value = {
            "jobDefinitions": [{
                "revision": 3,
                "status": "ACTIVE",
                "containerProperties": {"image": "123.dkr.ecr/foo:latest"},
            }]
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j1"}

        # Task with no image — should be fine because the JD provides one.
        await backend.submit_task(_make_task(image=None), "#!/bin/bash\n")

        # Registration must NOT be called.
        backend._batch_client.register_job_definition.assert_not_called()
        # submit_job used the configured JD verbatim.
        kwargs = backend._batch_client.submit_job.call_args.kwargs
        assert kwargs["jobDefinition"] == "simpleJobDef-b760b37"

    @pytest.mark.asyncio
    async def test_image_mismatch_warns_but_still_submits(self, caplog):
        backend = _make_backend(
            default_image="alpine:latest",
            job_definition="simpleJobDef-b760b37",
        )
        backend._batch_client.describe_job_definitions.return_value = {
            "jobDefinitions": [{
                "revision": 1,
                "status": "ACTIVE",
                "containerProperties": {"image": "some-other-image:tag"},
            }]
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j1"}

        import logging

        with caplog.at_level(logging.WARNING, logger="scripthut.backends.batch"):
            await backend.submit_task(_make_task(), "#!/bin/bash\n")

        assert any("does not support image overrides" in r.message for r in caplog.records)
        # Still submitted — warning, not error.
        backend._batch_client.submit_job.assert_called_once()

    @pytest.mark.asyncio
    async def test_image_match_no_warning(self, caplog):
        backend = _make_backend(
            default_image="same-image:tag",
            job_definition="simpleJobDef-b760b37",
        )
        backend._batch_client.describe_job_definitions.return_value = {
            "jobDefinitions": [{
                "revision": 1,
                "status": "ACTIVE",
                "containerProperties": {"image": "same-image:tag"},
            }]
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j1"}

        import logging

        with caplog.at_level(logging.WARNING, logger="scripthut.backends.batch"):
            await backend.submit_task(_make_task(), "#!/bin/bash\n")

        assert not any("image overrides" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_jd_image_cache_single_describe_call(self):
        """DescribeJobDefinitions is called once per configured JD, not per task."""
        backend = _make_backend(default_image=None, job_definition="my-jd")
        backend._batch_client.describe_job_definitions.return_value = {
            "jobDefinitions": [{
                "revision": 1,
                "containerProperties": {"image": "x:1"},
            }]
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j"}

        await backend.submit_task(_make_task(image="x:1"), "s1")
        await backend.submit_task(_make_task(image="x:1"), "s2")
        await backend.submit_task(_make_task(image="x:1"), "s3")

        # Only described once.
        assert backend._batch_client.describe_job_definitions.call_count == 1
        # But submitted three times.
        assert backend._batch_client.submit_job.call_count == 3

    @pytest.mark.asyncio
    async def test_configured_jd_with_arn_uses_jobDefinitions_arg(self):
        """ARN-shaped values are passed via jobDefinitions=[arn], not jobDefinitionName."""
        arn = "arn:aws:batch:us-east-1:111:job-definition/my-jd:3"
        backend = _make_backend(default_image=None, job_definition=arn)
        backend._batch_client.describe_job_definitions.return_value = {
            "jobDefinitions": [{"revision": 3, "containerProperties": {"image": "x:1"}}]
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j"}

        await backend.submit_task(_make_task(image="x:1"), "s1")

        describe_kwargs = backend._batch_client.describe_job_definitions.call_args.kwargs
        assert describe_kwargs.get("jobDefinitions") == [arn]
        assert "jobDefinitionName" not in describe_kwargs

    @pytest.mark.asyncio
    async def test_configured_jd_describe_failure_skips_image_check(self, caplog):
        """If we can't describe the JD, submit anyway — don't block on the check."""
        backend = _make_backend(default_image=None, job_definition="my-jd")
        backend._batch_client.describe_job_definitions.side_effect = Exception("AccessDenied")
        backend._batch_client.submit_job.return_value = {"jobId": "j"}

        await backend.submit_task(_make_task(image="x:1"), "s1")

        # Submission still happened.
        backend._batch_client.submit_job.assert_called_once()


def _make_template(
    *,
    name: str = "my-jd",
    revision: int = 3,
    image: str = "base-image:v1",
    extra: dict | None = None,
) -> dict:
    """Build a describe_job_definitions-style template dict."""
    base = {
        "jobDefinitionName": name,
        "jobDefinitionArn": f"arn:aws:batch:us-east-1:111:job-definition/{name}:{revision}",
        "revision": revision,
        "status": "ACTIVE",
        "type": "container",
        "containerProperties": {
            "image": image,
            "jobRoleArn": "arn:aws:iam::111:role/JobRole",
            "executionRoleArn": "arn:aws:iam::111:role/ExecRole",
            "resourceRequirements": [
                {"type": "VCPU", "value": "1"},
                {"type": "MEMORY", "value": "512"},
            ],
            "logConfiguration": {"logDriver": "awslogs"},
        },
        "propagateTags": True,
        "retryStrategy": {"attempts": 2},
        "timeout": {"attemptDurationSeconds": 3600},
        "tags": {"Team": "research"},
        "platformCapabilities": ["EC2"],
    }
    if extra:
        base.update(extra)
    return base


class TestJobDefinitionRevisionsMode:
    """Revisions mode: use the configured JD as a template, register per-image revisions."""

    @pytest.mark.asyncio
    async def test_matching_image_uses_template_no_registration(self):
        backend = _make_backend(
            default_image=None,
            job_definition="my-jd",
            job_definition_mode="revisions",
        )
        backend._batch_client.describe_job_definitions.return_value = {
            "jobDefinitions": [_make_template(image="base:v1")],
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j1"}

        # Task requests the same image the template already has.
        await backend.submit_task(_make_task(image="base:v1"), "#!/bin/bash\n")

        # No new revision registered.
        backend._batch_client.register_job_definition.assert_not_called()
        # Submission uses the configured value verbatim.
        kwargs = backend._batch_client.submit_job.call_args.kwargs
        assert kwargs["jobDefinition"] == "my-jd"

    @pytest.mark.asyncio
    async def test_new_image_registers_new_revision(self):
        backend = _make_backend(
            default_image=None,
            job_definition="my-jd",
            job_definition_mode="revisions",
        )
        backend._batch_client.describe_job_definitions.return_value = {
            "jobDefinitions": [_make_template(image="base:v1", revision=3)],
        }
        backend._batch_client.register_job_definition.return_value = {
            "jobDefinitionArn": "arn:aws:batch:us-east-1:111:job-definition/my-jd:4",
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j1"}

        await backend.submit_task(_make_task(image="new:v2"), "#!/bin/bash\n")

        # Template was fetched once; new revision registered; submit used the new ARN.
        assert backend._batch_client.describe_job_definitions.call_count == 1
        backend._batch_client.register_job_definition.assert_called_once()
        reg = backend._batch_client.register_job_definition.call_args.kwargs
        assert reg["jobDefinitionName"] == "my-jd"
        assert reg["containerProperties"]["image"] == "new:v2"
        submit_kwargs = backend._batch_client.submit_job.call_args.kwargs
        assert submit_kwargs["jobDefinition"] == (
            "arn:aws:batch:us-east-1:111:job-definition/my-jd:4"
        )

    @pytest.mark.asyncio
    async def test_revision_preserves_template_fields(self):
        """New revision carries over roles, log config, retry, timeout, tags, etc."""
        backend = _make_backend(
            default_image=None,
            job_definition="my-jd",
            job_definition_mode="revisions",
        )
        template = _make_template(image="base:v1")
        backend._batch_client.describe_job_definitions.return_value = {
            "jobDefinitions": [template]
        }
        backend._batch_client.register_job_definition.return_value = {
            "jobDefinitionArn": "arn:x:4"
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j1"}

        await backend.submit_task(_make_task(image="new:v2"), "#!/bin/bash\n")

        reg = backend._batch_client.register_job_definition.call_args.kwargs
        # Container properties: image swapped, everything else preserved.
        cp = reg["containerProperties"]
        assert cp["image"] == "new:v2"
        assert cp["jobRoleArn"] == "arn:aws:iam::111:role/JobRole"
        assert cp["executionRoleArn"] == "arn:aws:iam::111:role/ExecRole"
        assert cp["logConfiguration"] == {"logDriver": "awslogs"}
        # Top-level passthrough fields.
        assert reg["type"] == "container"
        assert reg["propagateTags"] is True
        assert reg["retryStrategy"] == {"attempts": 2}
        assert reg["timeout"] == {"attemptDurationSeconds": 3600}
        assert reg["tags"] == {"Team": "research"}
        assert reg["platformCapabilities"] == ["EC2"]

    @pytest.mark.asyncio
    async def test_revision_cache_per_image(self):
        """Same image across submissions → register once; new image → register again."""
        backend = _make_backend(
            default_image=None,
            job_definition="my-jd",
            job_definition_mode="revisions",
        )
        backend._batch_client.describe_job_definitions.return_value = {
            "jobDefinitions": [_make_template(image="base:v1", revision=3)]
        }
        backend._batch_client.register_job_definition.side_effect = [
            {"jobDefinitionArn": "arn:my-jd:4"},
            {"jobDefinitionArn": "arn:my-jd:5"},
        ]
        backend._batch_client.submit_job.return_value = {"jobId": "j"}

        # Three submissions: two with image A, one with image B.
        await backend.submit_task(_make_task(image="img:a"), "s1")
        await backend.submit_task(_make_task(image="img:a"), "s2")
        await backend.submit_task(_make_task(image="img:b"), "s3")

        # Only two registration calls (one per unique image).
        assert backend._batch_client.register_job_definition.call_count == 2
        # Template described only once.
        assert backend._batch_client.describe_job_definitions.call_count == 1
        # Third submission hit the cache — same ARN as first.
        submit_calls = backend._batch_client.submit_job.call_args_list
        assert submit_calls[0].kwargs["jobDefinition"] == "arn:my-jd:4"
        assert submit_calls[1].kwargs["jobDefinition"] == "arn:my-jd:4"
        assert submit_calls[2].kwargs["jobDefinition"] == "arn:my-jd:5"

    @pytest.mark.asyncio
    async def test_no_task_image_uses_template_image(self):
        """Task without image: use default_image or the template's image, no new revision."""
        backend = _make_backend(
            default_image=None,
            job_definition="my-jd",
            job_definition_mode="revisions",
        )
        backend._batch_client.describe_job_definitions.return_value = {
            "jobDefinitions": [_make_template(image="base:v1")]
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j"}

        # Task has no image; no default_image either.
        await backend.submit_task(_make_task(image=None), "s1")

        backend._batch_client.register_job_definition.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_template_raises(self):
        backend = _make_backend(
            default_image=None,
            job_definition="my-jd",
            job_definition_mode="revisions",
        )
        backend._batch_client.describe_job_definitions.return_value = {"jobDefinitions": []}

        with pytest.raises(RuntimeError, match="not found"):
            await backend.submit_task(_make_task(image="x:1"), "s1")

    @pytest.mark.asyncio
    async def test_describe_template_failure_propagates(self):
        """Unlike locked mode, revisions mode cannot proceed without the template."""
        backend = _make_backend(
            default_image=None,
            job_definition="my-jd",
            job_definition_mode="revisions",
        )
        backend._batch_client.describe_job_definitions.side_effect = Exception("AccessDenied")

        with pytest.raises(RuntimeError, match="DescribeJobDefinitions failed"):
            await backend.submit_task(_make_task(image="x:1"), "s1")

    @pytest.mark.asyncio
    async def test_picks_latest_revision_when_bare_name(self):
        """Multiple ACTIVE revisions returned → pick highest."""
        backend = _make_backend(
            default_image=None,
            job_definition="my-jd",
            job_definition_mode="revisions",
        )
        backend._batch_client.describe_job_definitions.return_value = {
            "jobDefinitions": [
                _make_template(image="old:v0", revision=1),
                _make_template(image="old:v0", revision=5),
                _make_template(image="old:v0", revision=3),
            ]
        }
        backend._batch_client.submit_job.return_value = {"jobId": "j"}

        await backend.submit_task(_make_task(image="old:v0"), "s1")
        # No registration needed (image matches template).
        backend._batch_client.register_job_definition.assert_not_called()
        # Template cache holds the revision 5 entry.
        assert backend._jd_template is not None
        assert backend._jd_template["revision"] == 5


class TestCancel:
    @pytest.mark.asyncio
    async def test_cancel_attempts_both_apis(self):
        backend = _make_backend()
        await backend.cancel_job("job-xyz")
        backend._batch_client.cancel_job.assert_called_once()
        backend._batch_client.terminate_job.assert_called_once()


class TestGetJobStats:
    @pytest.mark.asyncio
    async def test_only_terminal_states_returned(self):
        backend = _make_backend()
        backend._batch_client.describe_jobs.return_value = {
            "jobs": [
                {
                    "jobId": "done-1",
                    "status": "SUCCEEDED",
                    "startedAt": 1_700_000_000_000,
                    "stoppedAt": 1_700_000_060_000,
                },
                {
                    "jobId": "failed-1",
                    "status": "FAILED",
                    "startedAt": 1_700_000_000_000,
                    "stoppedAt": 1_700_000_030_000,
                },
                {
                    "jobId": "running-1",
                    "status": "RUNNING",
                    "startedAt": 1_700_000_000_000,
                },
            ]
        }
        stats = await backend.get_job_stats(["done-1", "failed-1", "running-1"])
        assert set(stats.keys()) == {"done-1", "failed-1"}
        assert stats["done-1"].state == "SUCCEEDED"
        assert isinstance(stats["done-1"], JobStats)
        assert stats["done-1"].total_cpu == "60s"

    @pytest.mark.asyncio
    async def test_empty_ids_short_circuit(self):
        backend = _make_backend()
        result = await backend.get_job_stats([])
        assert result == {}
        backend._batch_client.describe_jobs.assert_not_called()


class TestClusterInfo:
    @pytest.mark.asyncio
    async def test_sums_maxvcpus_across_compute_envs(self):
        backend = _make_backend()
        backend._batch_client.describe_job_queues.return_value = {
            "jobQueues": [{
                "computeEnvironmentOrder": [
                    {"computeEnvironment": "arn:ce:1"},
                    {"computeEnvironment": "arn:ce:2"},
                ]
            }]
        }
        backend._batch_client.describe_compute_environments.return_value = {
            "computeEnvironments": [
                {"computeResources": {"maxvCpus": 64}},
                {"computeResources": {"maxvCpus": 128}},
            ]
        }
        info = await backend.get_cluster_info()
        assert info == (192, 192)

    @pytest.mark.asyncio
    async def test_missing_queue_returns_none(self):
        backend = _make_backend()
        backend._batch_client.describe_job_queues.return_value = {"jobQueues": []}
        assert await backend.get_cluster_info() is None


class TestFetchLog:
    @pytest.mark.asyncio
    async def test_error_log_type_returns_note(self):
        backend = _make_backend()
        content, error = await backend.fetch_log("job-1", "ignored", log_type="error")
        assert error is None
        assert content is not None
        assert "CloudWatch" in content

    @pytest.mark.asyncio
    async def test_output_reads_cloudwatch_stream(self):
        backend = _make_backend()
        backend._batch_client.describe_jobs.return_value = {
            "jobs": [{
                "jobId": "job-1",
                "attempts": [{"container": {"logStreamName": "my-stream"}}],
            }]
        }
        backend._logs_client.get_log_events.return_value = {
            "events": [{"message": "line 1"}, {"message": "line 2"}],
        }
        content, error = await backend.fetch_log("job-1", "ignored", log_type="output")
        assert error is None
        assert content == "line 1\nline 2"

    @pytest.mark.asyncio
    async def test_missing_log_stream_returns_error(self):
        backend = _make_backend()
        backend._batch_client.describe_jobs.return_value = {
            "jobs": [{"jobId": "job-1", "attempts": []}],
        }
        content, error = await backend.fetch_log("job-1", "ignored")
        assert content is None
        assert error is not None
        assert "log stream" in error


class TestRunManagerIntegration:
    """End-to-end smoke tests for RunManager + BatchBackend (no SSH)."""

    @pytest.mark.asyncio
    async def test_create_run_from_git_source_without_ssh(self, tmp_path, monkeypatch):
        """Regression: POST /sources/<src>/workflows/<wf>/run must work on Batch."""
        import json as _json
        from scripthut.config_schema import (
            GitSourceConfig,
            GlobalSettings,
            ScriptHutConfig,
        )
        from scripthut.runs import manager as manager_mod
        from scripthut.runs.manager import RunManager
        from scripthut.runs.storage import RunStorageManager

        backend = _make_backend()
        # Stub submit_job so we don't actually call boto3.
        backend._batch_client.register_job_definition.return_value = {
            "jobDefinitionArn": "arn:aws:batch:us-east-1:111:job-definition/scripthut-x:1",
        }
        backend._batch_client.submit_job.return_value = {"jobId": "stub-job"}

        source = GitSourceConfig(
            name="hut-examples",
            type="git",
            url="git@github.com:example/repo.git",
            branch="main",
            backend="test-batch",
        )
        config = ScriptHutConfig(
            backends=[backend._config],
            sources=[source],
            settings=GlobalSettings(),
        )
        storage = RunStorageManager(tmp_path / "workflows")
        mgr = RunManager(
            config=config,
            backends={},  # no SSH clients
            storage=storage,
            job_backends={"test-batch": backend},
        )

        # Mock out local `git ls-remote` so no network call happens.
        async def _fake_shell(cmd, timeout=60.0):
            return ("deadbeefcafe0000 refs/heads/main\n", "", 0)

        monkeypatch.setattr(manager_mod, "_run_local_shell", _fake_shell)

        tasks_json = _json.dumps({
            "tasks": [{
                "id": "t1",
                "name": "hello",
                "command": "echo hi",
                "working_dir": "subdir",
                "cpus": 1,
                "memory": "1G",
                "time_limit": "5:00",
            }]
        })

        run = await mgr.create_run_from_source(
            "hut-examples", "diamond_tasks.json", tasks_json, "test-batch",
        )

        # Run was created and commit + git fields populated from the source.
        assert run.backend_name == "test-batch"
        assert run.git_repo == "git@github.com:example/repo.git"
        assert run.git_branch == "main"
        assert run.commit_hash == "deadbeefcafe0000"
        # working_dir should be left relative — BatchBackend rewrites it.
        assert run.items[0].task.working_dir == "subdir"
        # log_dir should be the synthetic placeholder (no filesystem path).
        assert run.log_dir.startswith("backend://")

        # The container script must include the clone block with the SHA.
        script = run.items[0].submit_script
        assert script is not None
        assert "git clone git@github.com:example/repo.git" in script
        assert "git checkout deadbeefcafe0000" in script
        # working_dir was rewritten against the runtime clone dir.
        assert "$_SCRIPTHUT_CLONE_DIR/subdir" in script

        # Submission actually reached the mocked boto3 client.
        backend._batch_client.submit_job.assert_called_once()
