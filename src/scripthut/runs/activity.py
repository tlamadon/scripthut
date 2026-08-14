"""Daily usage heatmap over recent runs — the landing page's activity grid.

Intensity is CPU-hours, not run count: on a scheduler, one 512-CPU job is
a bigger day than forty one-core jobs, and a grid that said otherwise
would be measuring how often you pressed the button rather than how much
of the cluster you used.

Work is attributed to the day it actually ran, split across midnight
rather than dumped on the day a task started — HPC jobs routinely span
days, and a two-day job crediting its whole cost to day one leaves a
false hole next to a false spike.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone

from scripthut.runs.models import Run
from scripthut.runs.storage import RunStorageManager

# Matched to storage retention rather than chosen: terminal runs older than
# this are deleted on an hourly sweep, so a longer window could only ever
# render as empty columns.
ACTIVITY_WINDOW_DAYS = RunStorageManager.RETENTION_DAYS


@dataclass
class ActivityDay:
    """One cell: everything that ran on one local calendar day."""

    day: date
    cpu_hours: float = 0.0
    runs: int = 0
    tasks: int = 0
    level: int = 0  # 0 (idle) to 4 (busiest quartile), drives the cell shade


@dataclass
class ActivityGrid:
    """A GitHub-style grid: one column per week, one row per weekday."""

    weeks: list[list[ActivityDay | None]] = field(default_factory=list)
    days: list[ActivityDay] = field(default_factory=list)
    total_cpu_hours: float = 0.0
    active_days: int = 0
    busiest: ActivityDay | None = None
    window_days: int = ACTIVITY_WINDOW_DAYS

    @property
    def month_labels(self) -> list[tuple[int, str]]:
        """``(column index, month name)`` for each month the grid spans."""
        labels: list[tuple[int, str]] = []
        seen: set[tuple[int, int]] = set()
        for col, week in enumerate(self.weeks):
            first = next((d for d in week if d is not None), None)
            if first is None:
                continue
            key = (first.day.year, first.day.month)
            if key not in seen:
                seen.add(key)
                labels.append((col, first.day.strftime("%b")))
        return labels


def _iter_day_slices(start: datetime, end: datetime) -> Iterator[tuple[date, float]]:
    """Yield ``(local day, seconds)`` for each local day the interval covers.

    Buckets in the server's local timezone: on a personal daemon that is
    the same clock the user reads the page by. Timestamps themselves stay
    UTC everywhere else in the app.
    """
    cursor = start.astimezone()
    end = end.astimezone()
    while cursor < end:
        midnight = datetime.combine(
            cursor.date() + timedelta(days=1), time.min, tzinfo=cursor.tzinfo
        )
        slice_end = min(midnight, end)
        if slice_end <= cursor:
            break  # DST arithmetic can stall the cursor; never loop forever
        yield cursor.date(), (slice_end - cursor).total_seconds()
        cursor = slice_end


def _assign_levels(days: list[ActivityDay]) -> None:
    """Bucket days into shades by quartile of the *active* days.

    Scaling against the maximum instead would let one huge day flatten
    every other day into the lightest shade, which is the failure mode
    that makes these grids unreadable on bursty workloads.
    """
    values = sorted(d.cpu_hours for d in days if d.cpu_hours > 0)
    if not values:
        return
    # Cuts are indexed off ``len - 1`` so the top cut can never *be* the
    # maximum, which would leave the darkest shade unreachable.
    cuts = [values[int((len(values) - 1) * f)] for f in (0.25, 0.5, 0.75)]
    top = values[-1]
    for d in days:
        if d.cpu_hours <= 0:
            d.level = 0
        elif d.cpu_hours >= top:
            # The busiest day is always the darkest, including the
            # degenerate cases (one active day, or all days equal) where
            # quartiles carry no information.
            d.level = 4
        elif d.cpu_hours <= cuts[0]:
            d.level = 1
        elif d.cpu_hours <= cuts[1]:
            d.level = 2
        elif d.cpu_hours <= cuts[2]:
            d.level = 3
        else:
            d.level = 4


def build_activity_grid(
    runs: Iterable[Run],
    *,
    today: date | None = None,
    now: datetime | None = None,
    window_days: int = ACTIVITY_WINDOW_DAYS,
) -> ActivityGrid:
    """Summarize CPU-hours per day over the trailing ``window_days``.

    ``today`` and ``now`` are injectable so the grid is testable without
    freezing the clock. Still-running tasks count up to ``now``, matching
    ``Run.total_cpu_hours``.
    """
    now = now or datetime.now(timezone.utc)
    today = today or now.astimezone().date()
    start = today - timedelta(days=window_days - 1)

    buckets: dict[date, ActivityDay] = {
        start + timedelta(days=i): ActivityDay(day=start + timedelta(days=i))
        for i in range(window_days)
    }
    run_ids: dict[date, set[str]] = {}

    for run in runs:
        for item in run.items:
            if item.started_at is None:
                continue
            end = item.finished_at or now
            for day, seconds in _iter_day_slices(item.started_at, end):
                bucket = buckets.get(day)
                if bucket is None:
                    continue  # outside the window
                bucket.cpu_hours += seconds * (item.task.cpus or 1) / 3600.0
                bucket.tasks += 1
                run_ids.setdefault(day, set()).add(run.id)

    for day, ids in run_ids.items():
        buckets[day].runs = len(ids)

    days = [buckets[start + timedelta(days=i)] for i in range(window_days)]
    _assign_levels(days)

    # Columns start on Monday, so the first one reaches back before the
    # window; those cells render empty exactly like GitHub's do.
    grid_start = start - timedelta(days=start.weekday())
    weeks: list[list[ActivityDay | None]] = []
    cursor = grid_start
    while cursor <= today:
        weeks.append([buckets.get(cursor + timedelta(days=i)) for i in range(7)])
        cursor += timedelta(days=7)

    active = [d for d in days if d.cpu_hours > 0]
    return ActivityGrid(
        weeks=weeks,
        days=days,
        total_cpu_hours=sum(d.cpu_hours for d in days),
        active_days=len(active),
        busiest=max(active, key=lambda d: d.cpu_hours) if active else None,
        window_days=window_days,
    )
