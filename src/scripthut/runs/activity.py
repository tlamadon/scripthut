"""Daily usage heatmap over recent runs — the landing page's activity grid.

Intensity is CPU-hours, not run count: on a scheduler, one 512-CPU job is
a bigger day than forty one-core jobs, and a grid that said otherwise
would be measuring how often you pressed the button rather than how much
of the cluster you used.

Work is attributed to the day it actually ran, split across midnight
rather than dumped on the day a task started — HPC jobs routinely span
days, and a two-day job crediting its whole cost to day one leaves a
false hole next to a false spike.

Input is :class:`UsageRecord`, not :class:`Run`, so the graph can outlive
the runs behind it: history comes from the usage ledger, which survives
run retention, merged with in-memory runs for work that is still going or
not yet flushed. See :mod:`scripthut.runs.usage`.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone

from scripthut.runs.usage import UsageRecord

# A year, GitHub-style. The ledger is what makes this possible — run
# records themselves are still deleted after 30 days. On a fresh upgrade
# only the surviving month is backfilled and the rest fills in over time.
ACTIVITY_WINDOW_DAYS = 365


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
    def window_label(self) -> str:
        """Human phrasing for the window — "365 days" reads worse than a year."""
        if self.window_days % 365 == 0:
            years = self.window_days // 365
            return "12 months" if years == 1 else f"{years} years"
        if self.window_days % 30 == 0:
            months = self.window_days // 30
            return "30 days" if months == 1 else f"{months} months"
        return f"{self.window_days} days"

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


def _iter_hour_slices(start: datetime, end: datetime) -> Iterator[tuple[datetime, float]]:
    """Yield ``(local hour start, seconds)`` for each hour the interval covers.

    The hourly twin of :func:`_iter_day_slices`, and for the same reason: a
    six-hour job belongs to the six hours it occupied, not to the one it
    was submitted in.
    """
    cursor = start.astimezone()
    end = end.astimezone()
    while cursor < end:
        hour_start = cursor.replace(minute=0, second=0, microsecond=0)
        slice_end = min(hour_start + timedelta(hours=1), end)
        if slice_end <= cursor:
            break  # same DST guard as the day slicer
        yield hour_start, (slice_end - cursor).total_seconds()
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
    records: Iterable[UsageRecord],
    *,
    today: date | None = None,
    now: datetime | None = None,
    window_days: int = ACTIVITY_WINDOW_DAYS,
) -> ActivityGrid:
    """Summarize CPU-hours per day over the trailing ``window_days``.

    ``today`` and ``now`` are injectable so the grid is testable without
    freezing the clock. Still-running tasks (no ``finished_at``) count up
    to ``now``, matching ``Run.total_cpu_hours``.
    """
    now = now or datetime.now(timezone.utc)
    today = today or now.astimezone().date()
    start = today - timedelta(days=window_days - 1)

    buckets: dict[date, ActivityDay] = {
        start + timedelta(days=i): ActivityDay(day=start + timedelta(days=i))
        for i in range(window_days)
    }
    run_ids: dict[date, set[str]] = {}

    for rec in records:
        end = rec.finished_at or now
        for day, seconds in _iter_day_slices(rec.started_at, end):
            bucket = buckets.get(day)
            if bucket is None:
                continue  # outside the window
            bucket.cpu_hours += seconds * (rec.cpus or 1) / 3600.0
            bucket.tasks += 1
            run_ids.setdefault(day, set()).add(rec.run_id)

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


# --- Hourly breakdown ---------------------------------------------------
#
# The heatmap answers "how much, over a year". This answers "what was the
# cluster doing today", split by source so a spike is attributable.

# Slots 1-5 of the validated categorical palette (blue, orange, aqua,
# yellow, magenta) plus a neutral for the "Other" rollup. Validated with
# the data-viz palette checker against a white card surface: lightness
# band, chroma floor, adjacent-pair CVD separation (worst ΔE 9.1) and the
# normal-vision floor (worst ΔE 19.6) all pass. Three of the five sit
# under 3:1 contrast on white, which obliges visible labels rather than
# colour alone — hence the legend carries a name and a number per series.
SOURCE_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
OTHER_COLOR = "#9ca3af"

# Past this many sources the chart stops being readable, so the tail folds
# into "Other" rather than inventing hues — a generated 9th colour is the
# thing the palette rules exist to prevent.
MAX_SOURCE_SERIES = len(SOURCE_COLORS)

HOURLY_WINDOW_HOURS = 24

_NO_SOURCE = "(no source)"


@dataclass
class HourSegment:
    """One source's slice of one hour's CPU-time."""

    source: str
    cpu_hours: float
    color: str
    height_pct: float = 0.0
    is_top: bool = False  # only the stack's free end gets a rounded cap


@dataclass
class HourBucket:
    start: datetime
    total_cpu_hours: float = 0.0
    segments: list[HourSegment] = field(default_factory=list)

    @property
    def label(self) -> str:
        return self.start.strftime("%H:%M")


@dataclass
class HourlyUsage:
    """CPU-hours per hour over a trailing window, stacked by source."""

    hours: list[HourBucket] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)  # busiest first
    colors: dict[str, str] = field(default_factory=dict)
    totals: dict[str, float] = field(default_factory=dict)
    max_total: float = 0.0
    total_cpu_hours: float = 0.0
    window_hours: int = HOURLY_WINDOW_HOURS

    @property
    def is_empty(self) -> bool:
        return self.total_cpu_hours <= 0


def build_hourly_usage(
    records: Iterable[UsageRecord],
    *,
    now: datetime | None = None,
    window_hours: int = HOURLY_WINDOW_HOURS,
) -> HourlyUsage:
    """CPU-hours per local hour over the trailing ``window_hours``, by source.

    The final bucket is the current, partial hour. Running tasks count up
    to ``now``, so the last bar grows through the hour rather than
    appearing all at once when a job ends.
    """
    now = (now or datetime.now(timezone.utc)).astimezone()
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    starts = [current_hour - timedelta(hours=window_hours - 1 - i) for i in range(window_hours)]
    index = {s: i for i, s in enumerate(starts)}

    per_hour: list[dict[str, float]] = [{} for _ in starts]
    totals: dict[str, float] = {}

    for rec in records:
        source = rec.source_name or _NO_SOURCE
        end = rec.finished_at or now
        for hour_start, seconds in _iter_hour_slices(rec.started_at, end):
            i = index.get(hour_start)
            if i is None:
                continue  # outside the window
            hours = seconds * (rec.cpus or 1) / 3600.0
            per_hour[i][source] = per_hour[i].get(source, 0.0) + hours
            totals[source] = totals.get(source, 0.0) + hours

    # Busiest source first, so colour follows the entity consistently and
    # the legend reads top-down in the order the stack is drawn.
    ranked = sorted(totals, key=lambda s: totals[s], reverse=True)
    shown, folded = ranked[:MAX_SOURCE_SERIES], ranked[MAX_SOURCE_SERIES:]

    colors = {name: SOURCE_COLORS[i] for i, name in enumerate(shown)}
    if folded:
        colors["Other"] = OTHER_COLOR
        totals["Other"] = sum(totals[s] for s in folded)
        for bucket in per_hour:
            rolled = sum(bucket.pop(s, 0.0) for s in folded)
            if rolled:
                bucket["Other"] = rolled
        shown = [*shown, "Other"]
    for s in folded:
        totals.pop(s, None)

    max_total = max((sum(b.values()) for b in per_hour), default=0.0)

    hours: list[HourBucket] = []
    for start, bucket in zip(starts, per_hour, strict=True):
        segments = [
            HourSegment(
                source=name,
                cpu_hours=bucket[name],
                color=colors[name],
                height_pct=(bucket[name] / max_total * 100) if max_total else 0.0,
            )
            for name in shown
            if bucket.get(name, 0.0) > 0
        ]
        if segments:
            segments[-1].is_top = True
        hours.append(
            HourBucket(
                start=start,
                total_cpu_hours=sum(bucket.values()),
                segments=segments,
            )
        )

    return HourlyUsage(
        hours=hours,
        sources=shown,
        colors=colors,
        totals={name: totals[name] for name in shown},
        max_total=max_total,
        total_cpu_hours=sum(totals[name] for name in shown),
        window_hours=window_hours,
    )
