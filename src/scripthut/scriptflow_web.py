"""Presentation data from cached ScriptFlow records; no backend reads."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from types import SimpleNamespace as View
from typing import Any

from scripthut.scriptflow import TERMINAL_STATES, CachedRun
from scripthut.scriptflow_activity import AttemptUsage, build_activity_grid, build_hourly_usage
from scripthut.scriptflow_launch import LaunchStore


def stamp(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def run_view(cached: CachedRun, now: datetime | None = None) -> View:
    now = now or datetime.now(UTC)
    data = cached.projection(now)
    record = data["workflow"] or {}
    intervals = []
    items = []
    counts = Counter()
    approximate = data["observation"]["status"] != "fresh"
    source = (data.get("source") or {}).get("name", "manual")
    for task_id, task in record.get("tasks", {}).items():
        counts[task["state"]] += 1
        cpus = task.get("cpus") or 1
        approximate |= task.get("cpus") is None
        attempts = []
        for number, raw in enumerate(task["attempts"], 1):
            start = stamp(raw.get("started_at") or raw.get("submitted_at"))
            finish = stamp(raw.get("finished_at"))
            estimated = task.get("cpus") is None or raw.get("started_at") is None
            if finish is None and raw["state"] != "running":
                finish = stamp(record.get("finished_at") or record.get("updated_at"))
                estimated = True
            end = min(finish or now, now)
            hours = 0.0
            if start and (
                raw.get("started_at") or raw["state"] not in ("pending", "submitted", "queued")
            ):
                hours = max(0, (end - start).total_seconds()) * cpus / 3600
                if hours:
                    intervals.append(AttemptUsage(cached.reference.id, source, start, end, cpus))
            approximate |= estimated
            attempts.append(
                View(
                    **raw,
                    number=number,
                    start=start,
                    finish=end,
                    cpu_hours=hours,
                    approximate=estimated,
                )
            )
        latest = task["attempts"][-1] if task["attempts"] else {}
        items.append(
            View(
                task=View(id=task_id, **{k: v for k, v in task.items() if k != "attempts"}),
                status=View(value=task["state"]),
                attempts=attempts,
                job_id=latest.get("job_id"),
                submitted_at=stamp(latest.get("submitted_at")),
                started_at=stamp(latest.get("started_at") or latest.get("submitted_at")),
                finished_at=stamp(latest.get("finished_at")),
                error=task.get("error"),
                pending_reason=None,
            )
        )
    created = (
        stamp(record.get("created_at"))
        or cached.last_successful_fetch
        or cached.last_fetch_at
        or now
    )
    finished = stamp(record.get("finished_at"))
    state = data["workflow_status"]
    if state in TERMINAL_STATES and finished is None:
        finished = stamp(record.get("updated_at")) or created
    elapsed = max(0, ((finished or now) - created).total_seconds())
    return View(
        **data,
        id=cached.reference.id,
        workflow_name=record.get("flow", "Not started"),
        display_name=record.get("flow", "Not started"),
        backend_name=cached.reference.backend,
        source_name=source,
        status=View(value=state),
        created_at=created,
        finished_at=finished,
        started_at=created,
        elapsed=elapsed,
        items=items,
        status_counts=dict(counts),
        total_cpu_hours=sum(
            i.cpus * (i.finished_at - i.started_at).total_seconds() / 3600 for i in intervals
        ),
        approximate=approximate,
        intervals=intervals,
        progress=(counts.get("completed", 0), len(items)),
        completed_count=counts.get("completed", 0),
        running_count=counts.get("running", 0),
        pending_count=sum(counts.get(k, 0) for k in ("pending", "queued", "submitted")),
        failed_count=counts.get("failed", 0),
        dep_failed_count=counts.get("dep_failed", 0),
    )


async def cached_view(entry: CachedRun, service: Any, now: datetime | None = None) -> View:
    now = now or datetime.now(UTC)
    launch = None
    if entry.launch_key:
        launch = await LaunchStore(service.store.directory).get(entry.launch_key)
        if launch:
            entry.source = launch.source_metadata()
    run = run_view(entry, now)
    if launch and not entry.snapshot:
        run.workflow_name = run.display_name = launch.flow
        run.created_at = stamp(launch.created_at)
        run.elapsed = max(0, (now - run.created_at).total_seconds())
    return run


TASK_PAGE_SIZE = 20


def task_page(run: View, page: int, task_state: str, search: str) -> dict[str, Any]:
    from urllib.parse import urlencode

    selected = [
        item
        for item in run.items
        if (not task_state or item.status.value == task_state)
        and (
            not search
            or search.casefold() in item.task.name.casefold()
            or search.casefold() in item.task.id.casefold()
        )
    ]
    count = len(selected)
    pages = max(1, (count + TASK_PAGE_SIZE - 1) // TASK_PAGE_SIZE)
    page = min(page, pages)
    offset = (page - 1) * TASK_PAGE_SIZE
    run.items = selected[offset : offset + TASK_PAGE_SIZE]
    query = dict(task_state=task_state, search=search)
    base = "/runs/" + run.id + "?"
    return dict(
        task_page=page,
        task_pages=pages,
        task_match_count=count,
        task_first=offset + 1 if count else 0,
        task_last=min(offset + TASK_PAGE_SIZE, count),
        task_state=task_state,
        task_search=search,
        task_query=urlencode(dict(query, page=page)),
        previous_page=base + urlencode(dict(query, page=page - 1)) if page > 1 else None,
        next_page=base + urlencode(dict(query, page=page + 1)) if page < pages else None,
    )


async def runs_context(service: Any, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    if service.state.config is None:
        cached = []
    else:
        cached = await service.store.list()
    runs = []
    for entry in cached:
        runs.append(await cached_view(entry, service, now))
    runs.sort(key=lambda run: run.created_at, reverse=True)
    intervals = [interval for run in runs for interval in run.intervals]
    return dict(
        runs=runs,
        active_runs=[r for r in runs if r.status.value not in TERMINAL_STATES],
        recent_runs=[r for r in runs if r.status.value in TERMINAL_STATES][:12],
        activity=build_activity_grid(intervals, now=now),
        hourly=build_hourly_usage(intervals, now=now),
        approximate=any(r.approximate for r in runs),
    )


def timeline(run: View) -> dict[str, Any]:
    rows = [(item, attempt) for item in run.items for attempt in item.attempts]
    starts = [attempt.start for _, attempt in rows if attempt.start]
    begin = min(starts, default=run.created_at)
    end = max([attempt.finish for _, attempt in rows] or [begin])
    duration = max(1, (end - begin).total_seconds())
    bars = []
    for item, attempt in rows:
        left = max(0, ((attempt.start or begin) - begin).total_seconds()) / duration * 100
        right = max(left, (attempt.finish - begin).total_seconds() / duration * 100)
        bars.append(
            dict(
                item=item,
                task_id=item.task.id,
                name=f"{item.task.name} · attempt {attempt.number}",
                status=attempt.state,
                has_bar=bool(attempt.start),
                wait_width=0,
                run_left=left,
                run_width=right - left,
                bar_start=left,
                bar_end=right,
                deps=[],
            )
        )
    return dict(
        gantt_items=bars,
        markers=[dict(pct=i * 25, label=f"{duration * i / 4 / 60:.1f}m") for i in range(5)],
    )


def backend_context(state: Any) -> dict[str, Any]:
    usage = {}
    jobs = []
    now = datetime.now(UTC)
    for name, backend in state.backends.items():
        running = [j for j in backend.jobs if j.state.value in ("RUNNING", "COMPLETING")]
        queued = [j for j in backend.jobs if j.state.value == "PENDING"]
        usage[name] = dict(
            jobs_running=len(running),
            jobs_pending=len(queued),
            cpus_running=sum(j.cpus for j in running),
            cpus_pending=sum(j.cpus for j in queued),
            cpus_used=sum(j.cpus for j in running),
            jobs_used=len(running),
            jobs=len(backend.jobs),
            cpus=sum(j.cpus for j in running),
            cpus_queued=sum(j.cpus for j in queued),
        )
        for job in backend.jobs:
            if state.live_view and job.state.value not in ("RUNNING", "PENDING", "COMPLETING"):
                continue
            hours = (
                max(0, (now - job.start_time).total_seconds()) * job.cpus / 3600
                if job.start_time
                else 0
            )
            jobs.append(
                View(
                    **{k: v for k, v in vars(job).items() if k not in ("state", "name")},
                    state=job.state.value.lower(),
                    name=job.name,
                    backend_name=name,
                    cpu_hours=hours,
                    source="external",
                    workflow_name=None,
                    run_id=None,
                    task_id=None,
                    is_terminal=job.state.value not in ("RUNNING", "PENDING", "COMPLETING"),
                    pending_reason=job.reason,
                )
            )
    return dict(
        backends=state.backends,
        backend_usage=usage,
        job_views=jobs,
        status=View(connected=state.any_connected),
        filter_enabled=state.filter_enabled,
        filter_user=state.filter_user,
        live_view=state.live_view,
        poll_interval=state.config.settings.poll_interval if state.config else 60,
        poll_remaining=state.seconds_until_next_poll,
    )


def configure_templates(templates: Any) -> None:
    if templates is None:
        return
    from importlib.metadata import metadata
    from urllib.parse import urlparse
    from uuid import uuid4

    urls = dict(value.split(", ", 1) for value in metadata("scripthut").get_all("Project-URL", []))
    project = urls.get("Homepage", "")
    parsed = urlparse(project)
    parts = parsed.path.strip("/").split("/")
    docs = (
        f"https://{parts[0]}.github.io/{parts[1]}/"
        if parsed.hostname == "github.com" and len(parts) == 2
        else project
    )
    templates.env.globals.update(
        project_url=project, docs_url=docs, new_request_id=lambda: "web-" + uuid4().hex
    )
    templates.env.filters["disk_bytes"] = disk_bytes


def disk_bytes(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return str(size)


async def dashboard_context(state: Any, service: Any) -> dict[str, Any]:
    data = {**backend_context(state), **await runs_context(service)}
    owners = {
        (run.backend_name, attempt.job_id): (run, item.task.id)
        for run in data["runs"]
        for item in run.items
        for attempt in item.attempts
        if attempt.job_id
    }
    for job in data["job_views"]:
        owner = owners.get((job.backend_name, job.job_id))
        if owner:
            run, task_id = owner
            job.run_id = run.id
            job.task_id = task_id
            job.workflow_name = run.workflow_name
            job.source = "run"
    return data
