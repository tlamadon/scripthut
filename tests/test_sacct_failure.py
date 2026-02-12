"""Tests for sacct-based failure detection."""

import pytest
from unittest.mock import AsyncMock

from scripthut.backends.slurm import (
    SLURM_FAILURE_STATES,
    JobStats,
    SlurmBackend,
)
from scripthut.runs.models import RunItem, RunItemStatus, TaskDefinition


def _make_item(
    status: RunItemStatus = RunItemStatus.COMPLETED,
    slurm_job_id: str = "12345",
) -> RunItem:
    task = TaskDefinition(id="t1", name="test-task", command="echo hi")
    return RunItem(task=task, status=status, slurm_job_id=slurm_job_id)


class TestJobStatsState:
    """Verify that JobStats.state is populated from sacct output."""

    def test_state_field_exists(self):
        stats = JobStats(
            cpu_efficiency=50.0,
            max_rss="1.2G",
            total_cpu="100s",
            state="OUT_OF_MEMORY",
        )
        assert stats.state == "OUT_OF_MEMORY"

    def test_state_defaults_to_none(self):
        stats = JobStats(cpu_efficiency=50.0, max_rss="1.2G", total_cpu="100s")
        assert stats.state is None


class TestFailureCorrection:
    """Verify the correction logic that flips false completions to FAILED."""

    @pytest.mark.parametrize("sacct_state,expected_reason", [
        ("OUT_OF_MEMORY", "Out of memory (OOM killed)"),
        ("TIMEOUT", "Exceeded walltime"),
        ("FAILED", "Non-zero exit code"),
        ("CANCELLED", "Cancelled"),
        ("NODE_FAIL", "Node failure"),
        ("PREEMPTED", "Preempted"),
    ])
    def test_failure_corrects_completed_item(self, sacct_state, expected_reason):
        """A COMPLETED item should flip to FAILED when sacct reports a failure state."""
        item = _make_item(status=RunItemStatus.COMPLETED)
        stats = JobStats(
            cpu_efficiency=0.0, max_rss="", total_cpu="0s", state=sacct_state
        )

        # Simulate the correction logic from poll_backend
        if (
            stats.state
            and stats.state in SLURM_FAILURE_STATES
            and item.status == RunItemStatus.COMPLETED
        ):
            reason = SLURM_FAILURE_STATES[stats.state]
            item.status = RunItemStatus.FAILED
            item.error = f"Slurm: {reason}"

        assert item.status == RunItemStatus.FAILED
        assert item.error == f"Slurm: {expected_reason}"

    def test_completed_stays_completed_when_sacct_says_completed(self):
        """A COMPLETED item should stay COMPLETED when sacct agrees."""
        item = _make_item(status=RunItemStatus.COMPLETED)
        stats = JobStats(
            cpu_efficiency=95.0, max_rss="512M", total_cpu="100s", state="COMPLETED"
        )

        if (
            stats.state
            and stats.state in SLURM_FAILURE_STATES
            and item.status == RunItemStatus.COMPLETED
        ):
            item.status = RunItemStatus.FAILED

        assert item.status == RunItemStatus.COMPLETED

    def test_already_failed_item_not_touched(self):
        """An already-FAILED item should not be re-corrected."""
        item = _make_item(status=RunItemStatus.FAILED)
        item.error = "sbatch failed: something"
        stats = JobStats(
            cpu_efficiency=0.0, max_rss="", total_cpu="0s", state="OUT_OF_MEMORY"
        )

        if (
            stats.state
            and stats.state in SLURM_FAILURE_STATES
            and item.status == RunItemStatus.COMPLETED
        ):
            item.status = RunItemStatus.FAILED
            item.error = f"Slurm: {SLURM_FAILURE_STATES[stats.state]}"

        # Original error should be preserved
        assert item.error == "sbatch failed: something"


# -- End-to-end sacct parsing tests ------------------------------------------


def _make_backend(sacct_id_stdout: str = "", sacct_stdout: str = "") -> SlurmBackend:
    """Create a SlurmBackend with mocked SSH returning given sacct output."""
    ssh = AsyncMock()

    async def run_command(cmd: str, timeout: int = 30):
        if "format=JobIDRaw --starttime" in cmd:
            # _get_known_sacct_ids pre-check
            return (sacct_id_stdout, "", 0)
        # Main sacct query
        return (sacct_stdout, "", 0)

    ssh.run_command = AsyncMock(side_effect=run_command)
    return SlurmBackend(ssh)


class TestSacctParsesState:
    """End-to-end: mock sacct output → get_job_stats → verify JobStats.state."""

    @pytest.mark.asyncio
    async def test_oom_state_parsed(self):
        """sacct reporting OUT_OF_MEMORY should populate state correctly."""
        # Realistic sacct output: main entry + .batch substep
        # Format: JobIDRaw|TotalCPU|Elapsed|AllocCPUS|MaxRSS|Start|End|State
        sacct_output = (
            "100|00:00:05|00:01:00|1||2026-02-11T10:00:00|2026-02-11T10:01:00|OUT_OF_MEMORY\n"
            "100.batch|00:00:05|00:01:00|1|512000K|2026-02-11T10:00:00|2026-02-11T10:01:00|OUT_OF_MEMORY\n"
        )
        backend = _make_backend(sacct_id_stdout="100\n", sacct_stdout=sacct_output)

        stats = await backend.get_job_stats(["100"])

        assert "100" in stats
        assert stats["100"].state == "OUT_OF_MEMORY"

    @pytest.mark.asyncio
    async def test_timeout_state_parsed(self):
        sacct_output = (
            "200|00:30:00|01:00:00|2||2026-02-11T09:00:00|2026-02-11T10:00:00|TIMEOUT\n"
            "200.batch|00:30:00|01:00:00|2|1024M|2026-02-11T09:00:00|2026-02-11T10:00:00|TIMEOUT\n"
        )
        backend = _make_backend(sacct_id_stdout="200\n", sacct_stdout=sacct_output)

        stats = await backend.get_job_stats(["200"])

        assert stats["200"].state == "TIMEOUT"

    @pytest.mark.asyncio
    async def test_completed_state_parsed(self):
        sacct_output = (
            "300|00:05:00|00:10:00|4||2026-02-11T08:00:00|2026-02-11T08:10:00|COMPLETED\n"
            "300.batch|00:05:00|00:10:00|4|2048M|2026-02-11T08:00:00|2026-02-11T08:10:00|COMPLETED\n"
        )
        backend = _make_backend(sacct_id_stdout="300\n", sacct_stdout=sacct_output)

        stats = await backend.get_job_stats(["300"])

        assert stats["300"].state == "COMPLETED"
        assert stats["300"].state not in SLURM_FAILURE_STATES

    @pytest.mark.asyncio
    async def test_cancelled_by_uid_strips_modifier(self):
        """sacct outputs 'CANCELLED by 12345'; we should strip to just 'CANCELLED'."""
        sacct_output = (
            "400|00:00:01|00:00:30|1||2026-02-11T07:00:00|2026-02-11T07:00:30|CANCELLED by 12345\n"
            "400.batch|00:00:01|00:00:30|1|100M|2026-02-11T07:00:00|2026-02-11T07:00:30|CANCELLED by 12345\n"
        )
        backend = _make_backend(sacct_id_stdout="400\n", sacct_stdout=sacct_output)

        stats = await backend.get_job_stats(["400"])

        assert stats["400"].state == "CANCELLED"

    @pytest.mark.asyncio
    async def test_multiple_jobs_parsed(self):
        """Multiple jobs in one sacct response each get their own state."""
        sacct_output = (
            "500|00:05:00|00:10:00|1||2026-02-11T06:00:00|2026-02-11T06:10:00|COMPLETED\n"
            "500.batch|00:05:00|00:10:00|1|256M|2026-02-11T06:00:00|2026-02-11T06:10:00|COMPLETED\n"
            "501|00:00:02|00:00:30|1||2026-02-11T06:00:00|2026-02-11T06:00:30|OUT_OF_MEMORY\n"
            "501.batch|00:00:02|00:00:30|1|4G|2026-02-11T06:00:00|2026-02-11T06:00:30|OUT_OF_MEMORY\n"
        )
        backend = _make_backend(
            sacct_id_stdout="500\n501\n", sacct_stdout=sacct_output
        )

        stats = await backend.get_job_stats(["500", "501"])

        assert stats["500"].state == "COMPLETED"
        assert stats["501"].state == "OUT_OF_MEMORY"

    @pytest.mark.asyncio
    async def test_stats_also_populated_alongside_state(self):
        """state doesn't break existing cpu_efficiency/max_rss parsing."""
        sacct_output = (
            "600|00:05:00|00:10:00|2||2026-02-11T05:00:00|2026-02-11T05:10:00|COMPLETED\n"
            "600.batch|00:05:00|00:10:00|2|1024M|2026-02-11T05:00:00|2026-02-11T05:10:00|COMPLETED\n"
        )
        backend = _make_backend(sacct_id_stdout="600\n", sacct_stdout=sacct_output)

        stats = await backend.get_job_stats(["600"])

        s = stats["600"]
        assert s.state == "COMPLETED"
        assert s.max_rss == "1.0G"  # 1024M -> 1.0G
        assert s.cpu_efficiency > 0

    @pytest.mark.asyncio
    async def test_batch_oom_overrides_main_completed(self):
        """Main entry says COMPLETED but .batch says OUT_OF_MEMORY — the
        .batch state should win because that's where user code actually runs."""
        sacct_output = (
            "700|00:00:00|00:00:05|1||2026-02-11T10:00:00|2026-02-11T10:00:05|COMPLETED\n"
            "700.batch|00:00:00|00:00:05|1|512K|2026-02-11T10:00:00|2026-02-11T10:00:05|OUT_OF_MEMORY\n"
            "700.extern|00:00:00|00:00:05|1||2026-02-11T10:00:00|2026-02-11T10:00:05|COMPLETED\n"
        )
        backend = _make_backend(sacct_id_stdout="700\n", sacct_stdout=sacct_output)

        stats = await backend.get_job_stats(["700"])

        assert stats["700"].state == "OUT_OF_MEMORY"

    @pytest.mark.asyncio
    async def test_batch_oom_overrides_regardless_of_line_order(self):
        """Same as above but .batch line appears before the main entry."""
        sacct_output = (
            "800.batch|00:00:00|00:00:05|1|512K|2026-02-11T10:00:00|2026-02-11T10:00:05|OUT_OF_MEMORY\n"
            "800|00:00:00|00:00:05|1||2026-02-11T10:00:00|2026-02-11T10:00:05|COMPLETED\n"
            "800.extern|00:00:00|00:00:05|1||2026-02-11T10:00:00|2026-02-11T10:00:05|COMPLETED\n"
        )
        backend = _make_backend(sacct_id_stdout="800\n", sacct_stdout=sacct_output)

        stats = await backend.get_job_stats(["800"])

        assert stats["800"].state == "OUT_OF_MEMORY"

