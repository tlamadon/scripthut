"""Tests for the SETTLING state machine (v0.10.0).

`run watch --exit-status` had a transient hazard: between a slurm job
vanishing from squeue and sacct returning a row, items were marked
optimistically COMPLETED, then potentially corrected to FAILED. An
automation polling the run during that window could see "completed"
and return success before the true verdict arrived.

v0.10.0 makes the state machine accounting-confirmed: queue-vanish
lands in SETTLING (non-terminal). The transition to COMPLETED or
FAILED requires sacct to return a row, with the numeric exit code
populated on `RunItem.exit_code` for downstream consumers. A
long-grace fallback (10 min default) lets sacct-unavailable cases
fall through to COMPLETED with a marker so runs don't hang forever.

Tests grouped by concern:

- `TestRunStatusTreatsSettlingAsNonTerminal` — run.status doesn't
  flip to a terminal state while any item is SETTLING.
- `TestSettlingPersistence` — RunItem.exit_code and the SETTLING
  status survive to_dict/from_dict.
- `TestSlurmExitCodeParser` — `_slurm_parse_exit_int` for sacct's
  ``<exit>:<signal>`` shape.
- `TestSlurmJobStatsExitCode` — get_job_stats populates the new
  exit_code field; .batch wins over main entry; absent column safe.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from scripthut.runs.models import (
    Run,
    RunItem,
    RunItemStatus,
    RunStatus,
    TaskDefinition,
)


def _item(status: RunItemStatus = RunItemStatus.SETTLING) -> RunItem:
    return RunItem(
        task=TaskDefinition(id="t", name="t", command="true"),
        status=status,
        job_id="9999",
        submitted_at=datetime(2026, 6, 4, 10, 0, tzinfo=UTC),
        started_at=datetime(2026, 6, 4, 10, 1, tzinfo=UTC),
    )


def _run(items: list[RunItem]) -> Run:
    return Run(
        id="r1", workflow_name="wf", backend_name="be",
        created_at=datetime(2026, 6, 4, 10, 0, tzinfo=UTC),
        items=items, max_concurrent=2,
    )


# ---------------------------------------------------------------------------
# Run.status with SETTLING items
# ---------------------------------------------------------------------------


class TestRunStatusTreatsSettlingAsNonTerminal:
    """The whole point of SETTLING is that the run keeps moving. If
    Run.status flipped to a terminal value while an item is SETTLING,
    `run watch --exit-status` would race a transient COMPLETED→FAILED
    flip — the bug this state was introduced to fix.
    """

    def test_run_with_only_settling_items_is_running(self):
        run = _run([_item(RunItemStatus.SETTLING)])
        assert run.status == RunStatus.RUNNING

    def test_mixed_settling_and_completed_is_running(self):
        run = _run([
            _item(RunItemStatus.SETTLING),
            _item(RunItemStatus.COMPLETED),
        ])
        assert run.status == RunStatus.RUNNING

    def test_settling_counts_against_running_count(self):
        """concurrency cap: SETTLING jobs still hold a slot since the
        cluster may not have fully released resources yet.
        """
        run = _run([
            _item(RunItemStatus.SETTLING),
            _item(RunItemStatus.RUNNING),
            _item(RunItemStatus.COMPLETED),
        ])
        assert run.running_count == 2

    def test_settling_is_NOT_in_progress_count(self):
        """``progress`` is "items in a terminal state" — SETTLING isn't
        terminal, so it shouldn't increment progress yet.
        """
        run = _run([
            _item(RunItemStatus.SETTLING),
            _item(RunItemStatus.COMPLETED),
        ])
        completed, total = run.progress
        assert completed == 1  # only the COMPLETED one
        assert total == 2

    def test_all_completed_when_no_settling_left_is_terminal(self):
        run = _run([
            _item(RunItemStatus.COMPLETED),
            _item(RunItemStatus.COMPLETED),
        ])
        assert run.status == RunStatus.COMPLETED


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


class TestSettlingPersistence:
    """SETTLING and the new exit_code field must survive disk."""

    def test_settling_status_roundtrip(self):
        item = _item(RunItemStatus.SETTLING)
        rt = RunItem.from_dict(item.to_dict())
        assert rt.status == RunItemStatus.SETTLING

    def test_exit_code_roundtrip(self):
        item = _item()
        item.exit_code = 1
        rt = RunItem.from_dict(item.to_dict())
        assert rt.exit_code == 1

    def test_zero_exit_code_distinguishable_from_unset(self):
        """``0`` and ``None`` must not be conflated — the v0.9.0 field
        report flagged the null-everywhere problem, and a successful
        run with exit 0 must serialize as 0, not null.
        """
        item = _item()
        item.exit_code = 0
        rt = RunItem.from_dict(item.to_dict())
        assert rt.exit_code == 0
        assert rt.exit_code is not None

    def test_no_exit_code_when_field_absent(self):
        """Old persisted runs (pre-0.10.0) don't have exit_code; load
        without crashing.
        """
        item = _item()
        raw = item.to_dict()
        raw.pop("exit_code", None)
        rt = RunItem.from_dict(raw)
        assert rt.exit_code is None


# ---------------------------------------------------------------------------
# Slurm exit-code parser
# ---------------------------------------------------------------------------


class TestSlurmExitCodeParser:
    def test_parses_zero_exit(self):
        from scripthut.backends.slurm import _slurm_parse_exit_int
        assert _slurm_parse_exit_int("0:0") == 0

    def test_parses_non_zero_exit(self):
        from scripthut.backends.slurm import _slurm_parse_exit_int
        assert _slurm_parse_exit_int("1:0") == 1
        assert _slurm_parse_exit_int("137:0") == 137

    def test_parses_signal_with_zero_exit(self):
        """Signal-only failure: returns the *exit* half (0). The boolean
        ``_slurm_exitcode_indicates_failure`` is what flips the state in
        that case — this helper just exposes the numeric value.
        """
        from scripthut.backends.slurm import _slurm_parse_exit_int
        assert _slurm_parse_exit_int("0:9") == 0

    def test_returns_none_for_unparseable(self):
        from scripthut.backends.slurm import _slurm_parse_exit_int
        assert _slurm_parse_exit_int("") is None
        assert _slurm_parse_exit_int("garbage") is None
        # Missing colon → unparseable (conservative; sacct always emits
        # the colon, so anything else is suspicious).
        assert _slurm_parse_exit_int("1") is None


# ---------------------------------------------------------------------------
# Slurm get_job_stats populates exit_code
# ---------------------------------------------------------------------------


class TestSlurmJobStatsExitCode:
    """Pin the exit_code field on JobStats so the polling layer
    (main.poll_backend) has something to assign to item.exit_code.
    """

    @pytest.mark.asyncio
    async def test_batch_exit_code_wins_over_main(self):
        """The .batch step is where the user's script actually ran, so
        its ExitCode is the authoritative one. The main entry can show
        a different number for jobs with srun wrappers.
        """
        from unittest.mock import AsyncMock

        from scripthut.backends.slurm import SlurmBackend
        ssh = AsyncMock()
        # Two sacct rows: main + .batch
        sacct_out = (
            "999|00:00:00|00:00:00|1||2026-06-04T10:00:00|"
            "2026-06-04T10:00:05|COMPLETED|0:0\n"
            "999.batch|00:00:00|00:00:00|1||2026-06-04T10:00:00|"
            "2026-06-04T10:00:05|COMPLETED|2:0\n"
        )
        # sacctmgr returns the main job id; sacct returns the parse rows
        ssh.run_command = AsyncMock(side_effect=[
            ("999\n", "", 0),     # id resolve
            (sacct_out, "", 0),    # sacct query
        ])
        backend = SlurmBackend(ssh)
        stats = await backend.get_job_stats(["999"])

        assert "999" in stats
        # .batch's exit=2 wins over main's exit=0.
        assert stats["999"].exit_code == 2

    @pytest.mark.asyncio
    async def test_main_exit_code_used_when_no_batch_row(self):
        from unittest.mock import AsyncMock

        from scripthut.backends.slurm import SlurmBackend
        ssh = AsyncMock()
        sacct_out = (
            "888|00:00:00|00:00:00|1||2026-06-04T10:00:00|"
            "2026-06-04T10:00:05|COMPLETED|0:0\n"
        )
        ssh.run_command = AsyncMock(side_effect=[
            ("888\n", "", 0),
            (sacct_out, "", 0),
        ])
        backend = SlurmBackend(ssh)
        stats = await backend.get_job_stats(["888"])

        assert stats["888"].exit_code == 0

    @pytest.mark.asyncio
    async def test_exit_code_none_when_field_unparseable(self):
        """Garbage in the ExitCode column → None, not 0."""
        from unittest.mock import AsyncMock

        from scripthut.backends.slurm import SlurmBackend
        ssh = AsyncMock()
        sacct_out = (
            "777|00:00:00|00:00:00|1||2026-06-04T10:00:00|"
            "2026-06-04T10:00:05|COMPLETED|garbage\n"
        )
        ssh.run_command = AsyncMock(side_effect=[
            ("777\n", "", 0),
            (sacct_out, "", 0),
        ])
        backend = SlurmBackend(ssh)
        stats = await backend.get_job_stats(["777"])

        assert stats["777"].exit_code is None
