"""Run lifecycle notifications + compact live progress stats.

A single :class:`NotificationHub` fans run events out to every connected
SSE client (the browser turns them into desktop notifications + toasts)
and tracks an aggregate progress figure for the page ``<title>``.

Detection is centralised here rather than threaded through every state
transition: the web layer feeds the hub the current runs once per poll
(:meth:`NotificationHub.scan`) and on creation
(:meth:`NotificationHub.run_scheduled`). The hub diffs against what it
last saw and emits only genuine transitions, so it never re-announces
runs that were already terminal when the server started.

All methods run on the asyncio event loop (poll loop + route handlers);
no locking is needed.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterable
from dataclasses import asdict, dataclass

from .runs.models import Run, RunItemStatus, RunStatus

logger = logging.getLogger(__name__)

# Run states the user cares about a transition *into*.
_TERMINAL = (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED)
_ACTIVE = (RunStatus.PENDING, RunStatus.RUNNING)


@dataclass
class Notification:
    """A single user-facing event."""

    kind: str  # "scheduled" | "run_done" | "task_failed"
    title: str
    body: str
    level: str  # "info" | "success" | "error" — drives toast/desktop styling
    run_id: str | None = None
    # Browser notification tag: a later notification with the same tag
    # replaces the earlier one (e.g. scheduled -> done for the same run).
    tag: str | None = None


def _empty_stats() -> dict[str, int]:
    return {"running": 0, "waiting": 0, "completed": 0, "failed": 0, "active_runs": 0}


def _compute_stats(runs: Iterable[Run]) -> dict[str, int]:
    """Aggregate item counts across currently-active runs.

    Only PENDING/RUNNING runs contribute — the title reflects work *in
    flight*, not the full history. Empty when nothing is active, so the
    title falls back to the bare page name.
    """
    s = _empty_stats()
    for run in runs:
        if run.status not in _ACTIVE:
            continue
        s["active_runs"] += 1
        for item in run.items:
            st = item.status
            if st in (RunItemStatus.RUNNING, RunItemStatus.SETTLING):
                s["running"] += 1
            elif st in (
                RunItemStatus.PENDING,
                RunItemStatus.SUBMITTED,
                RunItemStatus.QUEUED,
            ):
                s["waiting"] += 1
            elif st == RunItemStatus.COMPLETED:
                s["completed"] += 1
            elif st in (RunItemStatus.FAILED, RunItemStatus.DEP_FAILED):
                s["failed"] += 1
    return s


class NotificationHub:
    """Pub/sub of run notifications + the latest progress stats."""

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[dict[str, str]]] = set()
        # Last run status we announced, keyed by run id — drives terminal
        # transition detection.
        self._run_status: dict[str, RunStatus] = {}
        # Per-task failures already announced ("<run_id>:<task_id>") so a
        # failed task isn't re-announced on every subsequent poll.
        self._notified_failures: set[str] = set()
        self._stats: dict[str, int] = _empty_stats()

    # --- subscription -------------------------------------------------

    def subscribe(self) -> asyncio.Queue[dict[str, str]]:
        """Register a new SSE client; returns its message queue."""
        queue: asyncio.Queue[dict[str, str]] = asyncio.Queue(maxsize=100)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, str]]) -> None:
        self._subscribers.discard(queue)

    def _broadcast(self, event: str, data: dict) -> None:
        msg = {"event": event, "data": json.dumps(data)}
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(msg)
            except asyncio.QueueFull:
                # A slow/stuck client must never stall the poll loop —
                # drop the message for that client only.
                logger.debug("Dropping notification for a full subscriber queue")

    def _emit(self, n: Notification) -> None:
        self._broadcast("notify", asdict(n))

    # --- priming / hooks ----------------------------------------------

    def prime(self, runs: Iterable[Run]) -> None:
        """Record current state at startup *without* emitting anything.

        Prevents a flood of notifications for runs/failures that already
        existed before this process started.
        """
        for run in runs:
            self._run_status[run.id] = run.status
            for item in run.items:
                if item.status == RunItemStatus.FAILED:
                    self._notified_failures.add(f"{run.id}:{item.task.id}")
        self.update_stats(runs)

    def run_scheduled(self, run: Run) -> None:
        """Announce a freshly-created (or re-run) run."""
        self._run_status[run.id] = run.status
        # Forget failures from a prior incarnation (rerun reuses the id) so
        # the same task failing again is announced afresh.
        prefix = f"{run.id}:"
        self._notified_failures = {
            k for k in self._notified_failures if not k.startswith(prefix)
        }
        n = len(run.items)
        self._emit(
            Notification(
                kind="scheduled",
                title=f"Run scheduled: {run.workflow_name}",
                body=f"{n} task{'s' if n != 1 else ''} queued",
                level="info",
                run_id=run.id,
                tag=f"run-{run.id}",
            )
        )

    def scan(self, runs: Iterable[Run]) -> None:
        """Detect and announce new task failures + run terminal transitions."""
        for run in runs:
            for item in run.items:
                # Only real failures fire per-task; DEP_FAILED is downstream
                # fallout of a failure we already announced and would just
                # be noise. It still shows up in the run-done summary.
                if item.status == RunItemStatus.FAILED:
                    key = f"{run.id}:{item.task.id}"
                    if key not in self._notified_failures:
                        self._notified_failures.add(key)
                        self._emit(
                            Notification(
                                kind="task_failed",
                                title=f"Task failed: {item.task.name}",
                                body=(
                                    f"{run.workflow_name}"
                                    + (f" — {item.error}" if item.error else "")
                                ),
                                level="error",
                                run_id=run.id,
                                tag=f"task-{run.id}-{item.task.id}",
                            )
                        )

            prev = self._run_status.get(run.id)
            cur = run.status
            if cur != prev:
                self._run_status[run.id] = cur
                # Only announce a finish for a run we saw running/pending —
                # avoids firing for runs that were already terminal at startup.
                if cur in _TERMINAL and prev in _ACTIVE:
                    self._emit_run_done(run)

    def _emit_run_done(self, run: Run) -> None:
        completed = run.completed_count
        failed = run.failed_count + run.dep_failed_count
        if run.status == RunStatus.COMPLETED:
            title = f"Run completed: {run.workflow_name}"
            body = f"{completed} task{'s' if completed != 1 else ''} done"
            level = "success"
        elif run.status == RunStatus.CANCELLED:
            title = f"Run cancelled: {run.workflow_name}"
            body = f"{completed} done · {failed} not run"
            level = "info"
        else:  # FAILED
            title = f"Run finished with failures: {run.workflow_name}"
            body = f"{completed} done · {failed} failed"
            level = "error"
        self._emit(
            Notification(
                kind="run_done",
                title=title,
                body=body,
                level=level,
                run_id=run.id,
                tag=f"run-{run.id}",
            )
        )

    # --- progress stats -----------------------------------------------

    def update_stats(self, runs: Iterable[Run]) -> None:
        """Recompute progress stats and push them to subscribers."""
        self._stats = _compute_stats(runs)
        self._broadcast("stats", self._stats)

    @property
    def stats(self) -> dict[str, int]:
        """The most recently computed stats (sent to clients on connect)."""
        return self._stats
