"""Tests for sacct-based failure detection."""

import pytest
from unittest.mock import AsyncMock

from scripthut.backends.base import JobStats
from scripthut.backends.slurm import (
    SLURM_FAILURE_STATES,
    SlurmBackend,
)
from scripthut.runs.models import RunItem, RunItemStatus, TaskDefinition


def _make_item(
    status: RunItemStatus = RunItemStatus.COMPLETED,
    job_id: str = "12345",
) -> RunItem:
    task = TaskDefinition(id="t1", name="test-task", command="echo hi")
    return RunItem(task=task, status=status, job_id=job_id)


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

    async def run_command(cmd: str, timeout: int = 30):  # noqa: ARG001
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
            "100|00:00:05|00:01:00|1||2026-02-11T10:00:00|2026-02-11T10:01:00|OUT_OF_MEMORY|0:0\n"
            "100.batch|00:00:05|00:01:00|1|512000K|2026-02-11T10:00:00|2026-02-11T10:01:00|OUT_OF_MEMORY|0:0\n"
        )
        backend = _make_backend(sacct_id_stdout="100\n", sacct_stdout=sacct_output)

        stats = await backend.get_job_stats(["100"])

        assert "100" in stats
        assert stats["100"].state == "OUT_OF_MEMORY"

    @pytest.mark.asyncio
    async def test_timeout_state_parsed(self):
        sacct_output = (
            "200|00:30:00|01:00:00|2||2026-02-11T09:00:00|2026-02-11T10:00:00|TIMEOUT|0:0\n"
            "200.batch|00:30:00|01:00:00|2|1024M|2026-02-11T09:00:00|2026-02-11T10:00:00|TIMEOUT|0:0\n"
        )
        backend = _make_backend(sacct_id_stdout="200\n", sacct_stdout=sacct_output)

        stats = await backend.get_job_stats(["200"])

        assert stats["200"].state == "TIMEOUT"

    @pytest.mark.asyncio
    async def test_completed_state_parsed(self):
        sacct_output = (
            "300|00:05:00|00:10:00|4||2026-02-11T08:00:00|2026-02-11T08:10:00|COMPLETED|0:0\n"
            "300.batch|00:05:00|00:10:00|4|2048M|2026-02-11T08:00:00|2026-02-11T08:10:00|COMPLETED|0:0\n"
        )
        backend = _make_backend(sacct_id_stdout="300\n", sacct_stdout=sacct_output)

        stats = await backend.get_job_stats(["300"])

        assert stats["300"].state == "COMPLETED"
        assert stats["300"].state not in SLURM_FAILURE_STATES

    @pytest.mark.asyncio
    async def test_cancelled_by_uid_strips_modifier(self):
        """sacct outputs 'CANCELLED by 12345'; we should strip to just 'CANCELLED'."""
        sacct_output = (
            "400|00:00:01|00:00:30|1||2026-02-11T07:00:00|2026-02-11T07:00:30|CANCELLED by 12345|0:0\n"
            "400.batch|00:00:01|00:00:30|1|100M|2026-02-11T07:00:00|2026-02-11T07:00:30|CANCELLED by 12345|0:0\n"
        )
        backend = _make_backend(sacct_id_stdout="400\n", sacct_stdout=sacct_output)

        stats = await backend.get_job_stats(["400"])

        assert stats["400"].state == "CANCELLED"

    @pytest.mark.asyncio
    async def test_multiple_jobs_parsed(self):
        """Multiple jobs in one sacct response each get their own state."""
        sacct_output = (
            "500|00:05:00|00:10:00|1||2026-02-11T06:00:00|2026-02-11T06:10:00|COMPLETED|0:0\n"
            "500.batch|00:05:00|00:10:00|1|256M|2026-02-11T06:00:00|2026-02-11T06:10:00|COMPLETED|0:0\n"
            "501|00:00:02|00:00:30|1||2026-02-11T06:00:00|2026-02-11T06:00:30|OUT_OF_MEMORY|0:0\n"
            "501.batch|00:00:02|00:00:30|1|4G|2026-02-11T06:00:00|2026-02-11T06:00:30|OUT_OF_MEMORY|0:0\n"
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
            "600|00:05:00|00:10:00|2||2026-02-11T05:00:00|2026-02-11T05:10:00|COMPLETED|0:0\n"
            "600.batch|00:05:00|00:10:00|2|1024M|2026-02-11T05:00:00|2026-02-11T05:10:00|COMPLETED|0:0\n"
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
            "700|00:00:00|00:00:05|1||2026-02-11T10:00:00|2026-02-11T10:00:05|COMPLETED|0:0\n"
            "700.batch|00:00:00|00:00:05|1|512K|2026-02-11T10:00:00|2026-02-11T10:00:05|OUT_OF_MEMORY|0:0\n"
            "700.extern|00:00:00|00:00:05|1||2026-02-11T10:00:00|2026-02-11T10:00:05|COMPLETED|0:0\n"
        )
        backend = _make_backend(sacct_id_stdout="700\n", sacct_stdout=sacct_output)

        stats = await backend.get_job_stats(["700"])

        assert stats["700"].state == "OUT_OF_MEMORY"

    @pytest.mark.asyncio
    async def test_batch_oom_overrides_regardless_of_line_order(self):
        """Same as above but .batch line appears before the main entry."""
        sacct_output = (
            "800.batch|00:00:00|00:00:05|1|512K|2026-02-11T10:00:00|2026-02-11T10:00:05|OUT_OF_MEMORY|0:0\n"
            "800|00:00:00|00:00:05|1||2026-02-11T10:00:00|2026-02-11T10:00:05|COMPLETED|0:0\n"
            "800.extern|00:00:00|00:00:05|1||2026-02-11T10:00:00|2026-02-11T10:00:05|COMPLETED|0:0\n"
        )
        backend = _make_backend(sacct_id_stdout="800\n", sacct_stdout=sacct_output)

        stats = await backend.get_job_stats(["800"])

        assert stats["800"].state == "OUT_OF_MEMORY"


class TestSacctExitCodeOverridesCompleted:
    """When Slurm `State=COMPLETED` but `ExitCode` is non-zero, the user's
    script actually failed (commonly: `solve | tee log` without pipefail,
    or a trailing statement after the failing command). Override to
    FAILED so the user sees the truth — both the framework's other
    safeguards (`.batch` failure-state override) miss this case because
    Slurm itself records the job as cleanly exited.
    """

    @pytest.mark.asyncio
    async def test_completed_with_nonzero_exit_flips_to_failed(self):
        """`.batch` reports COMPLETED but ExitCode 1:0 → FAILED."""
        sacct_output = (
            "900|00:01:00|00:01:00|1||2026-02-11T10:00:00|2026-02-11T10:01:00|COMPLETED|0:0\n"
            "900.batch|00:01:00|00:01:00|1|256M|2026-02-11T10:00:00|2026-02-11T10:01:00|COMPLETED|1:0\n"
        )
        backend = _make_backend(sacct_id_stdout="900\n", sacct_stdout=sacct_output)
        stats = await backend.get_job_stats(["900"])
        assert stats["900"].state == "FAILED"

    @pytest.mark.asyncio
    async def test_completed_with_signal_flips_to_failed(self):
        """ExitCode 0:9 (SIGKILL with clean exit) is still a failure."""
        sacct_output = (
            "910|00:01:00|00:01:00|1||2026-02-11T10:00:00|2026-02-11T10:01:00|COMPLETED|0:0\n"
            "910.batch|00:01:00|00:01:00|1|256M|2026-02-11T10:00:00|2026-02-11T10:01:00|COMPLETED|0:9\n"
        )
        backend = _make_backend(sacct_id_stdout="910\n", sacct_stdout=sacct_output)
        stats = await backend.get_job_stats(["910"])
        assert stats["910"].state == "FAILED"

    @pytest.mark.asyncio
    async def test_main_entry_only_with_nonzero_exit_flips_to_failed(self):
        """Defense in depth: even with no .batch row, ExitCode on main flips."""
        sacct_output = (
            "920|00:01:00|00:01:00|1||2026-02-11T10:00:00|2026-02-11T10:01:00|COMPLETED|2:0\n"
        )
        backend = _make_backend(sacct_id_stdout="920\n", sacct_stdout=sacct_output)
        stats = await backend.get_job_stats(["920"])
        assert stats["920"].state == "FAILED"

    @pytest.mark.asyncio
    async def test_zero_exit_keeps_completed(self):
        """The override must not turn clean COMPLETED runs into FAILED."""
        sacct_output = (
            "930|00:01:00|00:01:00|1||2026-02-11T10:00:00|2026-02-11T10:01:00|COMPLETED|0:0\n"
            "930.batch|00:01:00|00:01:00|1|256M|2026-02-11T10:00:00|2026-02-11T10:01:00|COMPLETED|0:0\n"
        )
        backend = _make_backend(sacct_id_stdout="930\n", sacct_stdout=sacct_output)
        stats = await backend.get_job_stats(["930"])
        assert stats["930"].state == "COMPLETED"

    @pytest.mark.asyncio
    async def test_unparseable_exit_code_does_not_panic(self):
        """A weird ExitCode field shouldn't change the verdict."""
        sacct_output = (
            "940|00:01:00|00:01:00|1||2026-02-11T10:00:00|2026-02-11T10:01:00|COMPLETED|garbage\n"
            "940.batch|00:01:00|00:01:00|1|256M|2026-02-11T10:00:00|2026-02-11T10:01:00|COMPLETED|garbage\n"
        )
        backend = _make_backend(sacct_id_stdout="940\n", sacct_stdout=sacct_output)
        stats = await backend.get_job_stats(["940"])
        # Tolerant: unparseable → assume State is authoritative.
        assert stats["940"].state == "COMPLETED"

    @pytest.mark.asyncio
    async def test_failure_state_still_wins_over_exit_check(self):
        """OOM with weird exit code stays OOM — the failure-state override
        runs first so the user gets the specific reason, not generic FAILED.
        """
        sacct_output = (
            "950|00:01:00|00:01:00|1||2026-02-11T10:00:00|2026-02-11T10:01:00|COMPLETED|0:0\n"
            "950.batch|00:01:00|00:01:00|1|256M|2026-02-11T10:00:00|2026-02-11T10:01:00|OUT_OF_MEMORY|0:9\n"
        )
        backend = _make_backend(sacct_id_stdout="950\n", sacct_stdout=sacct_output)
        stats = await backend.get_job_stats(["950"])
        assert stats["950"].state == "OUT_OF_MEMORY"


class TestExitcodeParser:
    """Unit tests for the small helper that's easy to break with edge cases."""

    def test_zero_exit_zero_signal_is_clean(self):
        from scripthut.backends.slurm import _slurm_exitcode_indicates_failure
        assert _slurm_exitcode_indicates_failure("0:0") is False

    def test_nonzero_exit_is_failure(self):
        from scripthut.backends.slurm import _slurm_exitcode_indicates_failure
        assert _slurm_exitcode_indicates_failure("1:0") is True
        assert _slurm_exitcode_indicates_failure("137:0") is True

    def test_nonzero_signal_is_failure(self):
        from scripthut.backends.slurm import _slurm_exitcode_indicates_failure
        assert _slurm_exitcode_indicates_failure("0:9") is True
        assert _slurm_exitcode_indicates_failure("0:15") is True

    def test_empty_or_unparseable_is_clean(self):
        """Tolerant parser — never invent failures from parse errors."""
        from scripthut.backends.slurm import _slurm_exitcode_indicates_failure
        assert _slurm_exitcode_indicates_failure("") is False
        assert _slurm_exitcode_indicates_failure("   ") is False
        assert _slurm_exitcode_indicates_failure("garbage") is False
        assert _slurm_exitcode_indicates_failure("1") is False  # missing colon


class TestGeneratedScriptSetsPipefail:
    """`set -o pipefail` must appear in every generated script body so
    `solve | tee log.txt` doesn't silently swallow solve's failure.
    Tested on the SlurmBackend wrapper — generate_script_body is the
    shared helper used by every backend that runs bash, so this also
    asserts the helper itself produces it.
    """

    def _backend(self) -> SlurmBackend:
        return SlurmBackend(AsyncMock())

    def test_pipefail_present_in_simple_command(self):
        task = TaskDefinition(id="t", name="t", command="echo hi")
        script = self._backend().generate_script(task, "r", "/logs")
        assert "set -o pipefail" in script

    def test_pipefail_present_with_extra_init(self):
        from scripthut.backends.utils import generate_script_body
        body = generate_script_body(
            "task", "id", "echo hi", "/work", extra_init="module load python",
        )
        assert "set -o pipefail" in body
        # pipefail must run before extra_init so init's own pipelines are
        # also protected.
        pf_idx = body.index("set -o pipefail")
        init_idx = body.index("module load python")
        assert pf_idx < init_idx, (
            "pipefail should appear before extra_init so module-load "
            "pipelines are also protected"
        )


class TestSlurmGenerateScriptGres:
    def _backend(self) -> SlurmBackend:
        return SlurmBackend(AsyncMock())

    def test_gres_emitted(self):
        task = TaskDefinition(
            id="t1", name="test", command="pwd", gres="gpu:2",
        )
        script = self._backend().generate_script(task, "r1", "/logs")
        assert "#SBATCH --gres=gpu:2" in script

    def test_gres_typed_emitted(self):
        task = TaskDefinition(
            id="t1", name="test", command="pwd", gres="gpu:v100:1",
        )
        script = self._backend().generate_script(task, "r1", "/logs")
        assert "#SBATCH --gres=gpu:v100:1" in script

    def test_gres_absent_when_unset(self):
        task = TaskDefinition(id="t1", name="test", command="pwd")
        script = self._backend().generate_script(task, "r1", "/logs")
        assert "--gres" not in script

