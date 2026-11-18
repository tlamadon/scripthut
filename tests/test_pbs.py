"""Tests for the PBS/Torque backend."""

import pytest
from unittest.mock import AsyncMock

from scripthut.backends.pbs import (
    PBSBackend,
    _strip_server_suffix,
    parse_pbs_datetime,
    parse_qstat_line,
)
from scripthut.models import JobState


# -- Utility tests --


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
        assert len(result.partitions) == 1
        p = result.partitions[0]
        assert p.name == "default"
        assert p.cpus_total == 40  # 16 + 16 + 8
        assert p.cpus_idle == 24   # 16 + 8 (free nodes)
        assert p.cpus_allocated == 16  # job-exclusive
        assert p.nodes_total == 3
        # Aggregate accessors still work
        assert result.cpus_total == 40
        assert result.cpus_idle == 24
        assert result.pending_reasons == {}

    @pytest.mark.asyncio
    async def test_pbsnodes_failure(self):
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=("", "error", 1))
        backend = PBSBackend(ssh)

        result = await backend.get_cluster_info()
        assert result is None

    @pytest.mark.asyncio
    async def test_aggregates_gpus(self):
        pbsnodes_output = (
            "node1\n"
            "     state = free\n"
            "     np = 16\n"
            "     resources_available.ngpus = 4\n"
            "     resources_assigned.ngpus = 0\n"
            "\n"
            "node2\n"
            "     state = job-busy\n"
            "     np = 16\n"
            "     resources_available.ngpus = 4\n"
            "     resources_assigned.ngpus = 3\n"
            "\n"
            "node3\n"
            "     state = down\n"
            "     np = 16\n"
            "     resources_available.ngpus = 4\n"
            "     resources_assigned.ngpus = 0\n"
            "\n"
        )
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=(pbsnodes_output, "", 0))
        backend = PBSBackend(ssh)

        result = await backend.get_cluster_info()
        assert result is not None
        p = result.partitions[0]
        # 4 + 4 + 4 = 12 total
        assert p.gpus_total == 12
        # node1: 4 free; node2: 4-3=1 free; node3: down -> 0
        assert p.gpus_idle == 4 + 1

    @pytest.mark.asyncio
    async def test_dedicated_threads_accurate_idle_and_queue(self):
        # Torque marks a partially-used node "free"; the real allocation is in
        # dedicated_threads. Idle must be np - dedicated_threads, not all of np.
        pbsnodes_output = (
            "node1\n"
            "     state = free\n"
            "     np = 56\n"
            "     dedicated_threads = 28\n"
            "     status = opsys=linux,physmem=264053572kb,ncpus=56,gres=\n"
            "\n"
            "node2\n"
            "     state = job-exclusive\n"
            "     np = 56\n"
            "     dedicated_threads = 56\n"
            "\n"
            "node3\n"
            "     state = offline\n"
            "     np = 56\n"
            "\n"
        )
        qstat_qf = (
            "Queue: batch\n"
            "    queue_type = Execution\n"
            "    resources_max.walltime = 24:00:00\n"
            "    state_count = Transit:0 Queued:48 Held:248 Waiting:0 Running:34 Exiting:0 \n"
            "\tComplete:0\n"
            "    enabled = True\n"
        )

        async def fake_run(cmd, *a, **k):
            if cmd.startswith("qstat -Qf"):
                return (qstat_qf, "", 0)
            return (pbsnodes_output, "", 0)

        ssh = AsyncMock()
        ssh.run_command = AsyncMock(side_effect=fake_run)
        backend = PBSBackend(ssh, default_queue="batch")

        result = await backend.get_cluster_info()
        assert result is not None
        p = result.partitions[0]
        assert p.cpus_total == 168  # 3 * 56
        assert p.cpus_idle == 28  # node1: 56-28; node2: 0 (exclusive); node3: offline
        assert p.cpus_allocated == 84  # node1: 28 + node2: 56
        assert p.cpus_other == 56  # node3 offline
        assert p.name == "batch"
        assert p.timelimit == "24:00:00"
        assert p.mem_per_node_mb == 264053572 // 1024
        # Pending badge data: queued + held (waiting=0 is dropped).
        assert result.pending_reasons == {"Queued": 48, "Held": 248}


# -- parse_qstat_queues --


class TestParseQstatQueues:
    def test_unwraps_wrapped_state_count(self):
        from scripthut.backends.pbs import parse_qstat_queues

        out = (
            "Queue: batch\n"
            "    queue_type = Execution\n"
            "    state_count = Transit:0 Queued:48 Held:248 Waiting:0 Running:34 Exiting:0 \n"
            "\tComplete:0\n"
            "    resources_max.walltime = 100:00:00\n"
            "\n"
            "Queue: routeq\n"
            "    queue_type = Route\n"
        )
        q = parse_qstat_queues(out)
        assert set(q) == {"batch", "routeq"}
        assert q["batch"]["type"] == "execution"
        assert q["batch"]["walltime"] == "100:00:00"
        # The Complete:0 token wrapped onto a tab-continuation line is rejoined.
        assert q["batch"]["state_count"]["Queued"] == 48
        assert q["batch"]["state_count"]["Complete"] == 0
        assert q["routeq"]["type"] == "route"


# -- Properties --


class TestProperties:


    def test_name(self):
        ssh = AsyncMock()
        backend = PBSBackend(ssh)
        assert backend.name == "pbs"
