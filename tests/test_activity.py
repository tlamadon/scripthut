"""Tests for the landing page's daily activity heatmap."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from scripthut.runs.activity import (
    ACTIVITY_WINDOW_DAYS,
    build_activity_grid,
)
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition
from scripthut.runs.usage import records_from_runs


def build_grid(runs, **kw):
    """Adapter: the grid consumes usage records, but these tests describe
    behaviour in terms of the runs that produce them."""
    return build_activity_grid(records_from_runs(runs), **kw)

# The grid buckets by *local* day, so fixtures are anchored in local time —
# otherwise these tests would pass or fail depending on the server's offset.
LOCAL = datetime.now(timezone.utc).astimezone().tzinfo
TODAY = date(2026, 8, 14)


def local(day: date | int, hour: int, *, month: int = 8, year: int = 2026) -> datetime:
    """A local-time timestamp, returned as the UTC the app would store."""
    d = day if isinstance(day, date) else date(year, month, day)
    return datetime(d.year, d.month, d.day, hour, tzinfo=LOCAL).astimezone(timezone.utc)


NOW = local(TODAY, 12)  # a fixed local noon; day arithmetic never straddles midnight


def _run(run_id: str, items: list[RunItem]) -> Run:
    return Run(
        id=run_id,
        workflow_name="demo/train",
        backend_name="hpc",
        created_at=NOW,
        items=items,
        max_concurrent=None,
        source_name="demo",
    )


def _item(started: datetime | None, finished: datetime | None, cpus: int = 1) -> RunItem:
    return RunItem(
        task=TaskDefinition(id="t", name="T", command="x", cpus=cpus),
        status=RunItemStatus.COMPLETED if finished else RunItemStatus.RUNNING,
        started_at=started,
        finished_at=finished,
    )


def _day(grid, when: date):
    return next(d for d in grid.days if d.day == when)


class TestWindow:
    def test_window_outlives_run_retention(self):
        """The ledger is the whole point: the graph is no longer capped by
        how long run records survive."""
        from scripthut.runs.storage import RunStorageManager

        assert ACTIVITY_WINDOW_DAYS == 365
        assert ACTIVITY_WINDOW_DAYS > RunStorageManager.RETENTION_DAYS

    def test_covers_exactly_the_trailing_window(self):
        grid = build_grid([], today=TODAY, now=NOW)

        assert len(grid.days) == ACTIVITY_WINDOW_DAYS
        assert grid.days[-1].day == TODAY
        assert grid.days[0].day == TODAY - timedelta(days=ACTIVITY_WINDOW_DAYS - 1)

    def test_work_outside_the_window_is_ignored(self):
        old = NOW - timedelta(days=ACTIVITY_WINDOW_DAYS + 5)
        grid = build_grid(
            [_run("r", [_item(old, old + timedelta(hours=4), cpus=8)])],
            today=TODAY, now=NOW,
        )

        assert grid.total_cpu_hours == 0
        assert grid.active_days == 0


class TestCpuHours:
    def test_cpu_hours_scale_with_the_core_count(self):
        start = local(12, 9)
        grid = build_grid(
            [_run("r", [_item(start, start + timedelta(hours=2), cpus=8)])],
            today=TODAY, now=NOW,
        )

        assert _day(grid, date(2026, 8, 12)).cpu_hours == 16.0
        assert grid.total_cpu_hours == 16.0

    def test_work_is_split_across_midnight(self):
        """A job spanning days must not dump its whole cost on day one."""
        start = local(11, 22)  # 22:00 local, running 4h into the next day
        grid = build_grid(
            [_run("r", [_item(start, start + timedelta(hours=4), cpus=2)])],
            today=TODAY, now=NOW,
        )

        assert _day(grid, date(2026, 8, 11)).cpu_hours == 4.0  # 2h × 2 cpus before midnight
        assert _day(grid, date(2026, 8, 12)).cpu_hours == 4.0  # 2h × 2 cpus after
        assert grid.total_cpu_hours == 8.0

    def test_running_task_counts_up_to_now(self):
        start = NOW - timedelta(hours=3)
        grid = build_grid(
            [_run("r", [_item(start, None, cpus=4)])], today=TODAY, now=NOW,
        )

        assert grid.total_cpu_hours == 12.0

    def test_unstarted_tasks_contribute_nothing(self):
        grid = build_grid(
            [_run("r", [_item(None, None, cpus=64)])], today=TODAY, now=NOW,
        )

        assert grid.total_cpu_hours == 0

    def test_runs_counted_once_per_day_regardless_of_task_count(self):
        start = local(12, 9)
        items = [_item(start, start + timedelta(hours=1)) for _ in range(3)]
        grid = build_grid([_run("r", items)], today=TODAY, now=NOW)

        cell = _day(grid, date(2026, 8, 12))
        assert cell.runs == 1
        assert cell.tasks == 3


class TestLevels:
    def _grid_with(self, cpus_by_offset: dict[int, int]):
        """One 1-hour task per day, sized by core count so none spans midnight."""
        items = []
        for offset, cpus in cpus_by_offset.items():
            start = local(TODAY - timedelta(days=offset), 9)
            items.append(_item(start, start + timedelta(hours=1), cpus=cpus))
        return build_grid([_run("r", items)], today=TODAY, now=NOW)

    def test_idle_days_are_level_zero(self):
        grid = self._grid_with({1: 5})

        assert _day(grid, TODAY).level == 0
        assert _day(grid, TODAY - timedelta(days=1)).level > 0

    def test_a_lone_active_day_is_the_darkest(self):
        """Quartiles say nothing with one value; it is still the busiest day."""
        grid = self._grid_with({1: 5})

        assert _day(grid, TODAY - timedelta(days=1)).level == 4

    def test_equally_busy_days_share_a_shade(self):
        grid = self._grid_with({1: 4, 2: 4, 3: 4})

        levels = {_day(grid, TODAY - timedelta(days=o)).level for o in (1, 2, 3)}
        assert len(levels) == 1

    def test_busier_days_get_higher_levels(self):
        grid = self._grid_with({1: 1, 2: 4, 3: 8, 4: 20})

        levels = [_day(grid, TODAY - timedelta(days=o)).level for o in (1, 2, 3, 4)]
        assert levels == sorted(levels)
        assert levels[0] < levels[-1]

    def test_one_huge_day_does_not_flatten_the_rest(self):
        """Quartiles over active days, not a ratio to the max."""
        grid = self._grid_with({1: 1, 2: 2, 3: 3, 4: 512})

        small = [_day(grid, TODAY - timedelta(days=o)).level for o in (1, 2, 3)]
        assert len(set(small)) > 1  # they still differ from each other
        assert _day(grid, TODAY - timedelta(days=4)).level == 4


class TestGridLayout:
    def test_columns_are_weeks_starting_monday(self):
        grid = build_grid([], today=TODAY, now=NOW)

        for week in grid.weeks:
            present = [d for d in week if d is not None]
            assert len(week) == 7
            # Row index is the weekday, so Monday is always row 0.
            for i, day in enumerate(week):
                if day is not None:
                    assert day.day.weekday() == i
            assert present  # no column is entirely empty

    def test_cells_before_the_window_are_blank(self):
        """The first column reaches back past the window, like GitHub's does."""
        grid = build_grid([], today=TODAY, now=NOW)
        start = TODAY - timedelta(days=ACTIVITY_WINDOW_DAYS - 1)

        if start.weekday() != 0:
            assert grid.weeks[0][0] is None

    def test_month_labels_mark_each_month_once(self):
        grid = build_grid([], today=TODAY, now=NOW)
        labels = grid.month_labels

        # A year spans 13 month boundaries (the start month recurs at the end).
        assert len(labels) == 13
        assert [name for _, name in labels][:3] == ["Aug", "Sep", "Oct"]
        columns = [col for col, _ in labels]
        assert columns == sorted(columns)
        assert len(set(columns)) == len(columns)  # never two labels on one column


class TestSummary:
    def test_busiest_day_and_active_count(self):
        items = []
        for offset, hours in ((1, 2.0), (3, 9.0), (5, 1.0)):
            start = local(TODAY - timedelta(days=offset), 9)
            items.append(_item(start, start + timedelta(hours=hours), cpus=1))
        grid = build_grid([_run("r", items)], today=TODAY, now=NOW)

        assert grid.active_days == 3
        assert grid.busiest.day == TODAY - timedelta(days=3)
        assert grid.busiest.cpu_hours == 9.0

    def test_empty_history_has_no_busiest_day(self):
        grid = build_grid([], today=TODAY, now=NOW)

        assert grid.busiest is None
        assert grid.active_days == 0
        assert grid.total_cpu_hours == 0
