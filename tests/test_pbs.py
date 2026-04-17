"""Tests for the PBS/Torque backend."""

import pytest
from unittest.mock import AsyncMock

from scripthut.backends.pbs import (
    PBS_FAILURE_STATES,
    PBS_TERMINAL_STATES,
    PBSBackend,
    _convert_memory_to_pbs,
    _gres_to_pbs_gpus,
    _strip_server_suffix,
    parse_pbs_datetime,
    parse_qstat_line,
)
from scripthut.backends.base import JobStats
from scripthut.models import HPCJob, JobState
from scripthut.runs.models import TaskDefinition


# -- Utility tests --


class TestMemoryConversion:
    def test_gigabytes(self):
        assert _convert_memory_to_pbs("4G") == "4gb"

    def test_megabytes(self):
        assert _convert_memory_to_pbs("512M") == "512mb"

    def test_kilobytes(self):
        assert _convert_memory_to_pbs("1024K") == "1024kb"

    def test_terabytes(self):
        assert _convert_memory_to_pbs("1T") == "1tb"

    def test_no_unit(self):
        assert _convert_memory_to_pbs("1024") == "1024b"

    def test_lowercase_passthrough(self):
        assert _convert_memory_to_pbs("4gb") == "4gb"


class TestStripServerSuffix:
    def test_with_server(self):
        assert _strip_server_suffix("12345.pbs-server") == "12345"

    def test_without_server(self):
        assert _strip_server_suffix("12345") == "12345"

    def test_multiple_dots(self):
        assert _strip_server_suffix("12345.pbs.local") == "12345"


class TestParsePBSDatetime:
    def test_standard_format(self):
        dt = parse_pbs_datetime("Wed Feb 12 10:00:00 2026")
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 2
        assert dt.day == 12

    def test_iso_format(self):
        dt = parse_pbs_datetime("2026-02-12T10:00:00")
        assert dt is not None
        assert dt.year == 2026

    def test_empty(self):
        assert parse_pbs_datetime("") is None
        assert parse_pbs_datetime("N/A") is None
        assert parse_pbs_datetime("--") is None


# -- qstat parsing --


class TestParseQstatLine:
    def test_running_job(self):
        line = "12345.pbs    user1  batch  myjob   1234   1    4   4gb  01:00  R  00:30"
        job = parse_qstat_line(line)
        assert job is not None
        assert job.job_id == "12345"
        assert job.user == "user1"
        assert job.name == "myjob"
        assert job.state == JobState.RUNNING
        assert job.partition == "batch"
        assert job.cpus == 4
        assert job.memory == "4gb"

    def test_queued_job(self):
        line = "12346.pbs    user2  normal  test-job  --   1    2   2gb  02:00  Q  --"
        job = parse_qstat_line(line)
        assert job is not None
        assert job.job_id == "12346"
        assert job.state == JobState.PENDING

    def test_held_job(self):
        line = "12347.pbs    user3  batch  held-job  --   1    1   1gb  00:30  H  --"
        job = parse_qstat_line(line)
        assert job is not None
        assert job.state == JobState.SUSPENDED

    def test_short_line(self):
        assert parse_qstat_line("too short") is None

    def test_no_server_suffix(self):
        line = "99999    user1  batch  myjob   1234   1    4   4gb  01:00  R  00:30"
        job = parse_qstat_line(line)
        assert job is not None
        assert job.job_id == "99999"


# -- Script generation --


class TestGenerateScript:
    def _make_backend(self) -> PBSBackend:
        ssh = AsyncMock()
        return PBSBackend(ssh)

    def test_basic_script(self):
        backend = self._make_backend()
        task = TaskDefinition(
            id="t1", name="test-task", command="echo hello",
            partition="batch", cpus=2, memory="4G", time_limit="1:00:00",
        )
        script = backend.generate_script(task, "run123", "/tmp/logs")

        assert "#!/bin/bash" in script
        assert "#PBS -N test-task" in script
        assert "#PBS -q batch" in script
        assert "nodes=1:ppn=2,mem=4gb,walltime=1:00:00" in script
        assert "#PBS -o /tmp/logs/scripthut_run123_t1.out" in script
        assert "#PBS -e /tmp/logs/scripthut_run123_t1.err" in script
        assert "echo hello" in script

    def test_script_with_account(self):
        backend = self._make_backend()
        task = TaskDefinition(id="t1", name="test", command="pwd")
        script = backend.generate_script(task, "r1", "/logs", account="myaccount")
        assert "#PBS -A myaccount" in script

    def test_script_no_account(self):
        backend = self._make_backend()
        task = TaskDefinition(id="t1", name="test", command="pwd")
        script = backend.generate_script(task, "r1", "/logs")
        assert "#PBS -A" not in script

    def test_login_shell(self):
        backend = self._make_backend()
        task = TaskDefinition(id="t1", name="test", command="pwd")
        script = backend.generate_script(task, "r1", "/logs", login_shell=True)
        assert "#!/bin/bash -l" in script

    def test_default_queue_override(self):
        ssh = AsyncMock()
        backend = PBSBackend(ssh, default_queue="priority")
        task = TaskDefinition(id="t1", name="test", command="pwd", partition="batch")
        script = backend.generate_script(task, "r1", "/logs")
        assert "#PBS -q priority" in script

    def test_env_vars(self):
        backend = self._make_backend()
        task = TaskDefinition(id="t1", name="test", command="pwd")
        script = backend.generate_script(
            task, "r1", "/logs", env_vars={"MY_VAR": "hello"}
        )
        assert 'export MY_VAR="hello"' in script

    def test_extra_init(self):
        backend = self._make_backend()
        task = TaskDefinition(id="t1", name="test", command="pwd")
        script = backend.generate_script(
            task, "r1", "/logs", extra_init="module load python"
        )
        assert "module load python" in script

    def test_gres_gpu_count(self):
        backend = self._make_backend()
        task = TaskDefinition(
            id="t1", name="test", command="pwd", cpus=2, gres="gpu:2",
        )
        script = backend.generate_script(task, "r1", "/logs")
        assert "nodes=1:ppn=2:gpus=2,mem=" in script

    def test_gres_gpu_typed(self):
        backend = self._make_backend()
        task = TaskDefinition(
            id="t1", name="test", command="pwd", cpus=4, gres="gpu:v100:3",
        )
        script = backend.generate_script(task, "r1", "/logs")
        assert "nodes=1:ppn=4:gpus=3,mem=" in script

    def test_gres_bare_gpu(self):
        backend = self._make_backend()
        task = TaskDefinition(
            id="t1", name="test", command="pwd", cpus=1, gres="gpu",
        )
        script = backend.generate_script(task, "r1", "/logs")
        assert "nodes=1:ppn=1:gpus=1,mem=" in script

    def test_gres_none(self):
        backend = self._make_backend()
        task = TaskDefinition(id="t1", name="test", command="pwd", cpus=2)
        script = backend.generate_script(task, "r1", "/logs")
        assert ":gpus=" not in script


class TestGresParsing:
    def test_gpu_count(self):
        assert _gres_to_pbs_gpus("gpu:4") == 4

    def test_gpu_typed(self):
        assert _gres_to_pbs_gpus("gpu:a100:2") == 2

    def test_bare_gpu(self):
        assert _gres_to_pbs_gpus("gpu") == 1

    def test_non_gpu_returns_none(self):
        assert _gres_to_pbs_gpus("mps:100") is None

    def test_empty_returns_none(self):
        assert _gres_to_pbs_gpus("") is None

    def test_unparseable_count_returns_none(self):
        assert _gres_to_pbs_gpus("gpu:v100:many") is None


# -- Submit / Cancel --


class TestSubmitCancel:
    @pytest.mark.asyncio
    async def test_submit_job(self):
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=("12345.pbs-server\n", "", 0))
        backend = PBSBackend(ssh)

        job_id = await backend.submit_job("#!/bin/bash\necho hi")
        assert job_id == "12345"

    @pytest.mark.asyncio
    async def test_submit_job_no_server_suffix(self):
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=("99999\n", "", 0))
        backend = PBSBackend(ssh)

        job_id = await backend.submit_job("#!/bin/bash\necho hi")
        assert job_id == "99999"

    @pytest.mark.asyncio
    async def test_submit_job_failure(self):
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=("", "qsub: error\n", 1))
        backend = PBSBackend(ssh)

        with pytest.raises(RuntimeError, match="qsub failed"):
            await backend.submit_job("bad script")

    @pytest.mark.asyncio
    async def test_cancel_job(self):
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=("", "", 0))
        backend = PBSBackend(ssh)

        await backend.cancel_job("12345")
        ssh.run_command.assert_called_once_with("qdel 12345")


# -- get_jobs --


class TestGetJobs:
    @pytest.mark.asyncio
    async def test_parse_qstat_output(self):
        qstat_output = (
            "                                                            Req'd  Req'd   Elap\n"
            "Job ID          Username Queue    Jobname    SessID NDS TSK Memory Time  S Time\n"
            "--------------- -------- -------- ---------- ------ --- --- ------ ----- - -----\n"
            "100.pbs         user1    batch    task-a     1234   1   4   4gb   01:00 R 00:30\n"
            "101.pbs         user1    normal   task-b     --     1   2   2gb   02:00 Q --\n"
        )
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=(qstat_output, "", 0))
        backend = PBSBackend(ssh)

        jobs = await backend.get_jobs()
        assert len(jobs) == 2
        assert jobs[0].job_id == "100"
        assert jobs[0].state == JobState.RUNNING
        assert jobs[1].job_id == "101"
        assert jobs[1].state == JobState.PENDING

    @pytest.mark.asyncio
    async def test_empty_qstat(self):
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=("", "", 0))
        backend = PBSBackend(ssh)

        jobs = await backend.get_jobs()
        assert jobs == []

    @pytest.mark.asyncio
    async def test_qstat_failure(self):
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=("", "error", 1))
        backend = PBSBackend(ssh)

        jobs = await backend.get_jobs()
        assert jobs == []


# -- get_job_stats --


class TestGetJobStats:
    @pytest.mark.asyncio
    async def test_parse_completed_job(self):
        qstat_xf_output = (
            "Job Id: 500.pbs-server\n"
            "    Job_Name = test-task\n"
            "    job_state = F\n"
            "    exit_status = 0\n"
            "    resources_used.cput = 00:05:00\n"
            "    resources_used.walltime = 00:10:00\n"
            "    resources_used.mem = 1024mb\n"
            "    Resource_List.ncpus = 2\n"
            "    Resource_List.walltime = 01:00:00\n"
        )
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=(qstat_xf_output, "", 0))
        backend = PBSBackend(ssh)

        stats = await backend.get_job_stats(["500"])
        assert "500" in stats
        assert stats["500"].state == "COMPLETED"
        assert stats["500"].cpu_efficiency > 0

    @pytest.mark.asyncio
    async def test_parse_failed_job(self):
        qstat_xf_output = (
            "Job Id: 501.pbs\n"
            "    Job_Name = bad-task\n"
            "    job_state = F\n"
            "    exit_status = 1\n"
            "    resources_used.cput = 00:00:05\n"
            "    resources_used.walltime = 00:00:10\n"
            "    resources_used.mem = 128mb\n"
            "    Resource_List.ncpus = 1\n"
        )
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=(qstat_xf_output, "", 0))
        backend = PBSBackend(ssh)

        stats = await backend.get_job_stats(["501"])
        assert stats["501"].state == "FAILED"

    @pytest.mark.asyncio
    async def test_walltime_exceeded(self):
        qstat_xf_output = (
            "Job Id: 502.pbs\n"
            "    job_state = F\n"
            "    exit_status = 271\n"
            "    resources_used.cput = 00:59:00\n"
            "    resources_used.walltime = 01:00:00\n"
            "    resources_used.mem = 256mb\n"
            "    Resource_List.ncpus = 1\n"
            "    Resource_List.walltime = 01:00:00\n"
        )
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=(qstat_xf_output, "", 0))
        backend = PBSBackend(ssh)

        stats = await backend.get_job_stats(["502"])
        assert stats["502"].state == "WALLTIME_EXCEEDED"

    @pytest.mark.asyncio
    async def test_cancelled_job(self):
        qstat_xf_output = (
            "Job Id: 503.pbs\n"
            "    job_state = F\n"
            "    exit_status = 271\n"
            "    resources_used.cput = 00:00:01\n"
            "    resources_used.walltime = 00:00:05\n"
            "    resources_used.mem = 64mb\n"
            "    Resource_List.ncpus = 1\n"
            "    Resource_List.walltime = 01:00:00\n"
        )
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=(qstat_xf_output, "", 0))
        backend = PBSBackend(ssh)

        stats = await backend.get_job_stats(["503"])
        # exit_status 271 with walltime not exceeded → cancelled
        assert stats["503"].state == "CANCELLED"

    @pytest.mark.asyncio
    async def test_multiple_jobs(self):
        qstat_xf_output = (
            "Job Id: 600.pbs\n"
            "    job_state = F\n"
            "    exit_status = 0\n"
            "    resources_used.cput = 00:05:00\n"
            "    resources_used.walltime = 00:10:00\n"
            "    resources_used.mem = 512mb\n"
            "    Resource_List.ncpus = 1\n"
            "\n"
            "Job Id: 601.pbs\n"
            "    job_state = F\n"
            "    exit_status = 1\n"
            "    resources_used.cput = 00:00:01\n"
            "    resources_used.walltime = 00:00:05\n"
            "    resources_used.mem = 128mb\n"
            "    Resource_List.ncpus = 1\n"
        )
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=(qstat_xf_output, "", 0))
        backend = PBSBackend(ssh)

        stats = await backend.get_job_stats(["600", "601"])
        assert stats["600"].state == "COMPLETED"
        assert stats["601"].state == "FAILED"

    @pytest.mark.asyncio
    async def test_empty_job_ids(self):
        ssh = AsyncMock()
        backend = PBSBackend(ssh)

        stats = await backend.get_job_stats([])
        assert stats == {}


# -- get_cluster_info --


class TestGetClusterInfo:
    @pytest.mark.asyncio
    async def test_parse_pbsnodes(self):
        pbsnodes_output = (
            "node1\n"
            "     state = free\n"
            "     np = 16\n"
            "\n"
            "node2\n"
            "     state = job-exclusive\n"
            "     np = 16\n"
            "\n"
            "node3\n"
            "     state = free\n"
            "     np = 8\n"
            "\n"
        )
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=(pbsnodes_output, "", 0))
        backend = PBSBackend(ssh)

        result = await backend.get_cluster_info()
        assert result is not None
        total, idle = result
        assert total == 40  # 16 + 16 + 8
        assert idle == 24   # 16 + 8 (free nodes)

    @pytest.mark.asyncio
    async def test_pbsnodes_failure(self):
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=("", "error", 1))
        backend = PBSBackend(ssh)

        result = await backend.get_cluster_info()
        assert result is None


# -- Properties --


class TestProperties:
    def test_failure_states(self):
        ssh = AsyncMock()
        backend = PBSBackend(ssh)
        assert "FAILED" in backend.failure_states
        assert "CANCELLED" in backend.failure_states
        assert "WALLTIME_EXCEEDED" in backend.failure_states
        assert "MEM_EXCEEDED" in backend.failure_states

    def test_terminal_states(self):
        ssh = AsyncMock()
        backend = PBSBackend(ssh)
        assert "COMPLETED" in backend.terminal_states
        assert "FAILED" in backend.terminal_states
        assert backend.terminal_states == PBS_TERMINAL_STATES

    def test_name(self):
        ssh = AsyncMock()
        backend = PBSBackend(ssh)
        assert backend.name == "pbs"
