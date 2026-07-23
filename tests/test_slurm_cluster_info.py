"""Tests for SlurmBackend.get_cluster_info (per-partition + pending reasons)."""

from unittest.mock import AsyncMock

import pytest

from scripthut.backends.slurm import (
    SlurmBackend,
    _min_int,
    _parse_gres_gpu,
    _safe_float,
    _tres_get,
    parse_squeue_line,
)
from scripthut.models import JobState
from scripthut.runs.models import TaskDefinition


class TestParseSqueueReason:
    def test_pending_job_reason_stripped_of_parens(self):
        # %R for a pending job is the wait reason, wrapped in parens.
        line = "12345|job|alice|PENDING|cpu|0:00|-|4|4G|N/A|N/A|(Resources)"
        job = parse_squeue_line(line)
        assert job is not None
        assert job.state == JobState.PENDING
        assert job.reason == "Resources"

    def test_running_job_reason_is_none(self):
        # %R for a running job is the nodelist — not a wait reason — so
        # we drop it to avoid showing "nid001" as a reason.
        line = "12345|job|alice|RUNNING|cpu|1:00|nid001|4|4G|N/A|N/A|nid001"
        job = parse_squeue_line(line)
        assert job is not None
        assert job.state == JobState.RUNNING
        assert job.reason is None


@pytest.fixture
def ssh():
    return AsyncMock()


def _make_ssh_with_responses(
    sinfo_out: str,
    squeue_out: str = "",
    sinfo_node_out: str = "",
    sshare_out: str = "",
    sacctmgr_out: str = "",
    squeue_alloctres_out: str = "",
):
    """SSH mock that routes scheduler commands to test outputs."""
    ssh = AsyncMock()

    async def run_command(cmd, timeout=None):
        if cmd.startswith("sinfo") and "--Node" in cmd:
            return (sinfo_node_out, "", 0)
        if cmd.startswith("sinfo"):
            return (sinfo_out, "", 0)
        if cmd.startswith("sshare"):
            return (sshare_out, "", 0)
        if cmd.startswith("sacctmgr"):
            return (sacctmgr_out, "", 0)
        if cmd.startswith("squeue") and "AllocTRES" in cmd:
            return (squeue_alloctres_out, "", 0)
        if cmd.startswith("squeue"):
            return (squeue_out, "", 0)
        raise AssertionError(f"Unexpected command: {cmd}")

    ssh.run_command = AsyncMock(side_effect=run_command)
    return ssh


class TestParsePartitions:
    @pytest.mark.asyncio
    async def test_parses_multiple_partitions(self):
        sinfo_out = (
            "compute*|up|450/562/12/1024|32|196608|3-00:00:00|haswell\n"
            "gpu|up|28/4/0/32|2|1031168|1-00:00:00|a100,nvlink\n"
            "bigmem|up|0/256/0/256|4|1572864|7-00:00:00|(null)\n"
        )
        ssh = _make_ssh_with_responses(sinfo_out, "")
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info()
        assert info is not None
        assert len(info.partitions) == 3

        compute, gpu, bigmem = info.partitions
        assert compute.name == "compute"
        assert compute.is_default is True
        assert compute.state == "up"
        assert compute.cpus_allocated == 450
        assert compute.cpus_idle == 562
        assert compute.cpus_other == 12
        assert compute.cpus_total == 1024
        assert compute.nodes_total == 32
        assert compute.mem_per_node_mb == 196608
        assert compute.timelimit == "3-00:00:00"
        assert compute.features == "haswell"

        assert gpu.is_default is False
        assert gpu.features == "a100,nvlink"

        assert bigmem.cpus_idle == 256
        assert bigmem.features == "(null)"  # raw passthrough

        # Aggregate accessors
        assert info.cpus_total == 1024 + 32 + 256
        assert info.cpus_idle == 562 + 4 + 256

    @pytest.mark.asyncio
    async def test_returns_none_when_sinfo_fails(self):
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(return_value=("", "permission denied", 1))
        backend = SlurmBackend(ssh)

        assert await backend.get_cluster_info() is None

    @pytest.mark.asyncio
    async def test_skips_malformed_rows(self):
        # First row is malformed (missing fields), second is valid — parser
        # should warn on the bad row and still return the good one.
        sinfo_out = (
            "broken|up|garbage\n"
            "compute*|up|10/20/0/30|3|65536|1-00:00:00|\n"
        )
        ssh = _make_ssh_with_responses(sinfo_out, "")
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info()
        assert info is not None
        assert len(info.partitions) == 1
        assert info.partitions[0].name == "compute"
        assert info.partitions[0].features is None

    @pytest.mark.asyncio
    async def test_handles_na_memory(self):
        sinfo_out = "compute*|up|0/0/0/0|0|N/A|infinite|\n"
        ssh = _make_ssh_with_responses(sinfo_out, "")
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info()
        assert info is not None
        assert info.partitions[0].mem_per_node_mb is None
        assert info.partitions[0].timelimit == "infinite"


class TestPendingReasons:
    @pytest.mark.asyncio
    async def test_tallies_squeue_reasons(self):
        sinfo_out = "compute*|up|10/20/0/30|3|65536|1-00:00:00|\n"
        squeue_out = (
            "(Resources)\n"
            "(Resources)\n"
            "(Priority)\n"
            "(AssocMaxJobsLimit)\n"
            "(Resources)\n"
        )
        ssh = _make_ssh_with_responses(sinfo_out, squeue_out)
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info()
        assert info is not None
        assert info.pending_reasons == {
            "Resources": 3,
            "Priority": 1,
            "AssocMaxJobsLimit": 1,
        }

    @pytest.mark.asyncio
    async def test_empty_when_no_pending(self):
        ssh = _make_ssh_with_responses(
            "compute*|up|10/20/0/30|3|65536|1-00:00:00|\n", ""
        )
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info()
        assert info is not None
        assert info.pending_reasons == {}

    @pytest.mark.asyncio
    async def test_pending_reason_failure_does_not_block_partitions(self):
        # squeue fails, but sinfo succeeded — should still return partitions
        # with empty pending_reasons.
        ssh = AsyncMock()

        async def run_command(cmd, timeout=None):
            if cmd.startswith("sinfo"):
                return ("compute*|up|10/20/0/30|3|65536|1-00:00:00|\n", "", 0)
            return ("", "boom", 1)

        ssh.run_command = AsyncMock(side_effect=run_command)
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info()
        assert info is not None
        assert len(info.partitions) == 1
        assert info.pending_reasons == {}


class TestParseGresGpu:
    def test_typed_gpu(self):
        total, types = _parse_gres_gpu("gpu:a100:8")
        assert total == 8
        assert types == {"a100"}

    def test_untyped_gpu(self):
        total, types = _parse_gres_gpu("gpu:4")
        assert total == 4
        assert types == set()

    def test_strips_idx_suffix(self):
        total, types = _parse_gres_gpu("gpu:a100:6(IDX:0-5)")
        assert total == 6
        assert types == {"a100"}

    def test_ignores_non_gpu_entries(self):
        total, types = _parse_gres_gpu("gpu:a100:8,nvme:1T,mps:0")
        assert total == 8
        assert types == {"a100"}

    def test_mixed_types_sum(self):
        total, types = _parse_gres_gpu("gpu:a100:4,gpu:v100:2")
        assert total == 6
        assert types == {"a100", "v100"}

    def test_null_and_empty(self):
        assert _parse_gres_gpu("(null)") == (0, set())
        assert _parse_gres_gpu("") == (0, set())
        assert _parse_gres_gpu("N/A") == (0, set())

    def test_no_gpu_entry(self):
        total, types = _parse_gres_gpu("nvme:1T,mps:100")
        assert total == 0
        assert types == set()


def _pad(s: str, w: int) -> str:
    return s.ljust(w)


def _node_row(
    partition: str,
    state: str,
    gres: str = "(null)",
    gres_used: str = "(null)",
    cpus_state: str = "0/0/0/0",
    free_mem: str = "N/A",
) -> str:
    """Build a fixed-width sinfo -N row matching widths 32/16/20/14/256/256.

    Columns: Partition, StateLong, CPUsState, FreeMem, Gres, GresUsed.
    """
    return (
        _pad(partition, 32)
        + _pad(state, 16)
        + _pad(cpus_state, 20)
        + _pad(free_mem, 14)
        + _pad(gres, 256)
        + _pad(gres_used, 256)
    )


class TestGpuAggregation:
    @pytest.mark.asyncio
    async def test_aggregates_gpus_per_partition(self):
        sinfo_out = (
            "compute*|up|0/0/0/0|0|0|1-00:00:00|\n"
            "gpu|up|0/0/0/0|0|0|1-00:00:00|\n"
        )
        node_out = "\n".join([
            # gpu partition: 3 nodes, 8 a100 each. Two idle, one mixed with 4 in use.
            _node_row("gpu", "idle", "gpu:a100:8", "gpu:a100:0(IDX:N/A)"),
            _node_row("gpu", "idle", "gpu:a100:8", "gpu:a100:0(IDX:N/A)"),
            _node_row("gpu", "mixed", "gpu:a100:8", "gpu:a100:4(IDX:0-3)"),
            # compute partition: no GPUs
            _node_row("compute", "idle", "(null)", "(null)"),
        ]) + "\n"
        ssh = _make_ssh_with_responses(sinfo_out, "", node_out)
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info()
        assert info is not None
        by_name = {p.name: p for p in info.partitions}

        gpu = by_name["gpu"]
        assert gpu.gpus_total == 24
        assert gpu.gpus_idle == 8 + 8 + 4  # two idle nodes + 4 free on mixed
        assert gpu.gpu_types == "a100"

        compute = by_name["compute"]
        assert compute.gpus_total == 0
        assert compute.gpus_idle == 0

    @pytest.mark.asyncio
    async def test_down_node_counts_toward_total_only(self):
        sinfo_out = "gpu*|up|0/0/0/0|0|0|1-00:00:00|\n"
        node_out = (
            _node_row("gpu", "down", "gpu:v100:4", "(null)") + "\n"
            + _node_row("gpu", "idle", "gpu:v100:4", "(null)") + "\n"
        )
        ssh = _make_ssh_with_responses(sinfo_out, "", node_out)
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info()
        assert info is not None
        gpu = info.partitions[0]
        assert gpu.gpus_total == 8
        assert gpu.gpus_idle == 4  # only the idle node contributes
        assert gpu.gpu_types == "v100"

    @pytest.mark.asyncio
    async def test_gpu_fetch_failure_does_not_block_partitions(self):
        # sinfo -N call fails; partitions should still come back with 0 GPUs.
        ssh = AsyncMock()

        async def run_command(cmd, timeout=None):
            if "--Node" in cmd:
                return ("", "boom", 1)
            if cmd.startswith("sinfo"):
                return ("compute*|up|0/10/0/10|1|0|1-00:00:00|\n", "", 0)
            return ("", "", 0)

        ssh.run_command = AsyncMock(side_effect=run_command)
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info()
        assert info is not None
        assert info.partitions[0].gpus_total == 0
        assert info.partitions[0].gpus_idle == 0


class TestSchedulingHints:
    @pytest.mark.asyncio
    async def test_largest_free_cpu_slot_is_per_node(self):
        # 8 idle CPUs total but spread 4+4 across two nodes — the biggest
        # job that starts now is 4 CPUs, not 8.
        sinfo_out = "cpu*|up|0/0/0/0|0|0|1-00:00:00|\n"
        node_out = "\n".join([
            _node_row("cpu", "mix", cpus_state="12/4/0/16", free_mem="32000"),
            _node_row("cpu", "mix", cpus_state="12/4/0/16", free_mem="8000"),
        ]) + "\n"
        ssh = _make_ssh_with_responses(sinfo_out, "", node_out)
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info()
        assert info is not None
        p = info.partitions[0]
        assert p.cpus_free_max_node == 4
        assert p.mem_free_max_node_mb == 32000

    @pytest.mark.asyncio
    async def test_idle_gpu_on_full_cpu_node_is_not_schedulable(self):
        # The exact "idle GPUs but I still get queued" case: a node has a
        # free GPU but zero free CPUs, so the GPU can't actually be claimed.
        sinfo_out = "gpu*|up|0/0/0/0|0|0|1-00:00:00|\n"
        node_out = "\n".join([
            # free GPU but CPUs fully allocated
            _node_row("gpu", "mix", "gpu:a100:4", "gpu:a100:3", cpus_state="16/0/0/16"),
            # free GPU AND a free CPU -> genuinely schedulable
            _node_row("gpu", "mix", "gpu:a100:4", "gpu:a100:2", cpus_state="14/2/0/16"),
        ]) + "\n"
        ssh = _make_ssh_with_responses(sinfo_out, "", node_out)
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info()
        assert info is not None
        p = info.partitions[0]
        assert p.gpus_idle == 1 + 2  # all free GPUs
        assert p.gpus_schedulable == 2  # only those on the node with a free CPU

    @pytest.mark.asyncio
    async def test_down_node_contributes_no_free_slot(self):
        sinfo_out = "cpu*|up|0/0/0/0|0|0|1-00:00:00|\n"
        node_out = "\n".join([
            _node_row("cpu", "drain", cpus_state="0/16/0/16", free_mem="64000"),
            _node_row("cpu", "idle", cpus_state="0/8/0/8", free_mem="16000"),
        ]) + "\n"
        ssh = _make_ssh_with_responses(sinfo_out, "", node_out)
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info()
        assert info is not None
        p = info.partitions[0]
        # The drained node's 16 idle CPUs are unschedulable; only the idle
        # node's 8 count.
        assert p.cpus_free_max_node == 8
        assert p.mem_free_max_node_mb == 16000


class TestTresHelpers:
    def test_tres_get_finds_cpu(self):
        assert _tres_get("cpu=128,mem=512000,gres/gpu=4", "cpu") == 128

    def test_tres_get_finds_gpu(self):
        assert _tres_get("cpu=128,mem=512000,gres/gpu=4", "gres/gpu") == 4

    def test_tres_get_missing(self):
        assert _tres_get("cpu=128", "gres/gpu") is None

    def test_tres_get_skips_non_int(self):
        # Memory uses suffixes like "G" — explicitly out of scope.
        assert _tres_get("mem=512G", "mem") is None

    def test_tres_get_empty_and_null(self):
        assert _tres_get("", "cpu") is None
        assert _tres_get("(null)", "cpu") is None
        assert _tres_get("N/A", "cpu") is None

    def test_min_int_none_means_unlimited(self):
        assert _min_int(None, 5) == 5
        assert _min_int(5, None) == 5
        assert _min_int(None, None) is None
        assert _min_int(3, 7) == 3

    def test_safe_float(self):
        assert _safe_float("0.42") == 0.42
        assert _safe_float("") is None
        assert _safe_float("N/A") is None
        assert _safe_float("garbage") is None


class TestUserQuota:
    @pytest.mark.asyncio
    async def test_combines_sshare_sacctmgr_alloctres(self):
        sinfo_out = "compute*|up|10/20/0/30|3|65536|1-00:00:00|\n"
        sshare_out = "myaccount|alice|0.42|0.07\n"
        # User has GrpJobs=100 at root, MaxJobs=50 at partition-level → 50 wins.
        # GrpTRES cpu=512, gres/gpu=8 at user level.
        sacctmgr_out = (
            "alice|myaccount||100|cpu=512,gres/gpu=8|50|\n"
            "alice|myaccount|gpu|||10|cpu=64\n"
        )
        # Two running jobs: cpu=4 gpu=2, and cpu=16 gpu=4.
        alloctres_out = (
            "cpu=4,mem=4G,node=1,billing=4,gres/gpu=2\n"
            "cpu=16,mem=64G,node=2,billing=16,gres/gpu=4\n"
        )
        ssh = _make_ssh_with_responses(
            sinfo_out,
            sshare_out=sshare_out,
            sacctmgr_out=sacctmgr_out,
            squeue_alloctres_out=alloctres_out,
        )
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info(user="alice")
        assert info is not None
        q = info.user_quota
        assert q is not None
        assert q.account == "myaccount"
        assert q.fair_share == 0.42
        assert q.norm_usage == 0.07
        # Most-restrictive across rows
        assert q.jobs_max == 10  # partition-row MaxJobs=10 beats user MaxJobs=50
        assert q.cpus_max == 64  # gpu partition row tightens to 64
        assert q.gpus_max == 8
        # Aggregated from AllocTRES
        assert q.jobs_used == 2
        assert q.cpus_used == 20
        assert q.gpus_used == 6

    @pytest.mark.asyncio
    async def test_quota_uses_ssh_user_not_filter(self):
        # "Your usage" must reflect the login we actually submit as (the
        # SSH user), not the dashboard's job filter. Quota is fetched for
        # the SSH user even when no filter is passed, and a filter user is
        # ignored for quota — querying the wrong user reports all-zero
        # usage while our jobs are running (the reported bug).
        sinfo_out = "compute*|up|10/20/0/30|3|65536|1-00:00:00|\n"
        sshare_out = "myaccount|alice|0.42|0.07\n"
        alloctres_out = "cpu=4,gres/gpu=1\n"

        captured: list[str] = []
        ssh = AsyncMock()
        ssh.user = "alice"

        async def run_command(cmd, timeout=None):
            captured.append(cmd)
            if cmd.startswith("sinfo"):
                return (sinfo_out, "", 0)
            if cmd.startswith("sshare"):
                return (sshare_out, "", 0)
            if cmd.startswith("squeue") and "AllocTRES" in cmd:
                return (alloctres_out, "", 0)
            return ("", "", 0)

        ssh.run_command = AsyncMock(side_effect=run_command)
        backend = SlurmBackend(ssh)

        # No filter user passed -> quota is still fetched, for the SSH user.
        info = await backend.get_cluster_info()
        assert info is not None
        assert info.user_quota is not None
        assert info.user_quota.fair_share == 0.42
        assert info.user_quota.cpus_used == 4
        # The quota queries target the SSH user (alice).
        assert any(c.startswith("sshare") and "-u alice" in c for c in captured)
        assert any("AllocTRES" in c and "-u alice" in c for c in captured)

        # A filter user for the jobs list must NOT redirect quota to them.
        captured.clear()
        await backend.get_cluster_info(user="bob")
        assert all("bob" not in c for c in captured)
        assert any(c.startswith("sshare") and "-u alice" in c for c in captured)

    @pytest.mark.asyncio
    async def test_partial_failure_still_populates(self):
        # sshare fails; sacctmgr + AllocTRES still produce a quota.
        sinfo_out = "compute*|up|10/20/0/30|3|65536|1-00:00:00|\n"
        ssh = AsyncMock()

        async def run_command(cmd, timeout=None):
            if cmd.startswith("sinfo"):
                return (sinfo_out, "", 0)
            if cmd.startswith("sshare"):
                return ("", "permission denied", 1)
            if cmd.startswith("sacctmgr"):
                return ("alice|myacct||100|cpu=512|||\n", "", 0)
            if cmd.startswith("squeue") and "AllocTRES" in cmd:
                return ("cpu=4,gres/gpu=1\n", "", 0)
            return ("", "", 0)

        ssh.run_command = AsyncMock(side_effect=run_command)
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info(user="alice")
        assert info is not None
        q = info.user_quota
        assert q is not None
        assert q.fair_share is None  # sshare failed
        assert q.account == "myacct"
        assert q.jobs_max == 100
        assert q.cpus_max == 512
        assert q.cpus_used == 4
        assert q.gpus_used == 1
        assert q.jobs_used == 1

    @pytest.mark.asyncio
    async def test_configured_account_overrides_default_account_row(self):
        # User has two associations: 'basic' (DefaultAccount) and 'pi-kilianhuber'.
        # With account='pi-kilianhuber' configured, we should land on the
        # pi-kilianhuber row, not basic.
        sinfo_out = "compute*|up|10/20/0/30|3|65536|1-00:00:00|\n"
        sshare_out = (
            "basic|alice|0.50|0.10\n"
            "pi-kilianhuber|alice|0.20|0.40\n"
        )
        # sacctmgr query with account=pi-kilianhuber would return only matching
        # rows on a sane site; here we simulate that filter.
        sacctmgr_out = "alice|pi-kilianhuber||500|cpu=2048,gres/gpu=16||cpu=1024\n"
        alloctres_out = "cpu=8,gres/gpu=1\n"

        captured: list[str] = []
        ssh = AsyncMock()

        async def run_command(cmd, timeout=None):
            captured.append(cmd)
            if cmd.startswith("sinfo"):
                return (sinfo_out, "", 0)
            if cmd.startswith("sshare"):
                return (sshare_out, "", 0)
            if cmd.startswith("sacctmgr"):
                return (sacctmgr_out, "", 0)
            if cmd.startswith("squeue") and "AllocTRES" in cmd:
                return (alloctres_out, "", 0)
            return ("", "", 0)

        ssh.run_command = AsyncMock(side_effect=run_command)
        backend = SlurmBackend(ssh, account="pi-kilianhuber")

        info = await backend.get_cluster_info(user="alice")
        assert info is not None
        q = info.user_quota
        assert q is not None
        assert q.account == "pi-kilianhuber"  # not "basic"
        assert q.fair_share == 0.20  # from pi-kilianhuber row, not 0.50

        # Verify queries were scoped to the configured account
        sshare_cmds = [c for c in captured if c.startswith("sshare")]
        assert any("-A pi-kilianhuber" in c for c in sshare_cmds)
        sacctmgr_cmds = [c for c in captured if c.startswith("sacctmgr")]
        assert any("account=pi-kilianhuber" in c for c in sacctmgr_cmds)
        squeue_cmds = [c for c in captured if c.startswith("squeue")]
        assert any("--account=pi-kilianhuber" in c for c in squeue_cmds)

    @pytest.mark.asyncio
    async def test_falls_back_to_first_sshare_row_if_no_account_match(self):
        # Older sacctmgr/sshare versions may ignore the filter flags;
        # if no row's Account matches self._account, take the first row
        # rather than returning nothing.
        sinfo_out = "compute*|up|10/20/0/30|3|65536|1-00:00:00|\n"
        sshare_out = "otheracct|alice|0.50|0.10\n"
        ssh = _make_ssh_with_responses(
            sinfo_out,
            sshare_out=sshare_out,
            sacctmgr_out="",
            squeue_alloctres_out="",
        )
        backend = SlurmBackend(ssh, account="pi-kilianhuber")

        info = await backend.get_cluster_info(user="alice")
        assert info is not None
        q = info.user_quota
        assert q is not None
        assert q.fair_share == 0.50  # fell back to the only row

    @pytest.mark.asyncio
    async def test_all_quota_queries_fail_yields_none(self):
        sinfo_out = "compute*|up|10/20/0/30|3|65536|1-00:00:00|\n"
        ssh = AsyncMock()

        async def run_command(cmd, timeout=None):
            if cmd.startswith("sinfo"):
                return (sinfo_out, "", 0)
            return ("", "denied", 1)

        ssh.run_command = AsyncMock(side_effect=run_command)
        backend = SlurmBackend(ssh)

        info = await backend.get_cluster_info(user="alice")
        assert info is not None
        assert info.user_quota is None


def _make_task(partition: str = "standard") -> TaskDefinition:
    return TaskDefinition(
        id="t1",
        name="example",
        command="echo hello",
        partition=partition,
    )


class TestPartitionMapping:
    def test_passthrough_when_no_config(self):
        backend = SlurmBackend(AsyncMock())
        script = backend.generate_script(_make_task("standard"), "run1", "/tmp/log")
        assert "#SBATCH --partition=standard" in script

    def test_partition_map_rewrites(self):
        backend = SlurmBackend(
            AsyncMock(),
            partition_map={"standard": "cpu", "gpu": "gpu-a100"},
        )
        cpu_script = backend.generate_script(_make_task("standard"), "r", "/tmp/log")
        gpu_script = backend.generate_script(_make_task("gpu"), "r", "/tmp/log")
        assert "#SBATCH --partition=cpu" in cpu_script
        assert "#SBATCH --partition=gpu-a100" in gpu_script

    def test_default_partition_fallback_when_unmapped(self):
        backend = SlurmBackend(
            AsyncMock(),
            partition_map={"gpu": "gpu-a100"},
            default_partition="cpu",
        )
        script = backend.generate_script(_make_task("standard"), "r", "/tmp/log")
        assert "#SBATCH --partition=cpu" in script

    def test_map_takes_precedence_over_default(self):
        backend = SlurmBackend(
            AsyncMock(),
            partition_map={"standard": "cpu"},
            default_partition="fallback",
        )
        script = backend.generate_script(_make_task("standard"), "r", "/tmp/log")
        assert "#SBATCH --partition=cpu" in script
        assert "fallback" not in script

    def test_no_default_passthrough_when_unmapped(self):
        backend = SlurmBackend(
            AsyncMock(),
            partition_map={"gpu": "gpu-a100"},
        )
        script = backend.generate_script(_make_task("weird-name"), "r", "/tmp/log")
        assert "#SBATCH --partition=weird-name" in script
