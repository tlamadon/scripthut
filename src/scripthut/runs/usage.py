"""Append-only ledger of finished jobs, outliving the runs they came from.

Run records are deleted after ``RunStorageManager.RETENTION_DAYS``, which
capped the activity graph at a month. This file keeps one compact line per
finished task so the graph can span a year without keeping every run's
full JSON, logs, and manifests alive to do it.

It is *derived* data, rebuilt by reconciliation rather than maintained by
hooks: entries are flushed on startup and on the hourly sweep immediately
before old runs are deleted. Hooking each terminal transition instead
would mean catching every path an item can take to COMPLETED, FAILED or
DEP_FAILED — several sites across the manager and the poll loop — and
silently under-counting the day someone adds another.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from scripthut.runs.models import Run, RunItemStatus

logger = logging.getLogger(__name__)

_TERMINAL = (RunItemStatus.COMPLETED, RunItemStatus.FAILED, RunItemStatus.DEP_FAILED)


@dataclass(frozen=True)
class UsageRecord:
    """One task's resource consumption, from either the ledger or a live run."""

    run_id: str
    task_id: str
    cpus: int
    started_at: datetime
    finished_at: datetime | None  # None while still running
    workflow_name: str = ""
    source_name: str | None = None
    backend_name: str = ""
    status: str = ""

    @property
    def key(self) -> tuple[str, str]:
        """Identity for dedup between the ledger and in-memory runs."""
        return (self.run_id, self.task_id)


def records_from_runs(runs: Iterable[Run]) -> Iterator[UsageRecord]:
    """Every started task in ``runs``, terminal or not.

    Live runs are the authority for work the ledger hasn't seen yet — both
    still-running tasks and terminal ones not yet flushed.
    """
    for run in runs:
        for item in run.items:
            if item.started_at is None:
                continue
            yield UsageRecord(
                run_id=run.id,
                task_id=item.task.id,
                cpus=item.task.cpus or 1,
                started_at=item.started_at,
                finished_at=item.finished_at,
                workflow_name=run.workflow_name,
                source_name=run.source_name,
                backend_name=run.backend_name,
                status=item.status.value,
            )


class UsageLog:
    """JSONL ledger at ``<data_dir>/usage.jsonl``.

    One line per finished task, appended and never rewritten. At roughly
    200 bytes a line, a decade of a hundred jobs a day is under 80 MB; the
    file is not pruned, so the full history stays available for a longer
    graph later.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._keys: set[tuple[str, str]] | None = None

    # -- reading ---------------------------------------------------------

    def _parse(self, line: str) -> UsageRecord | None:
        try:
            d = json.loads(line)
            return UsageRecord(
                run_id=d["run"],
                task_id=d["task"],
                cpus=int(d.get("cpus", 1)),
                started_at=datetime.fromisoformat(d["started"]),
                finished_at=datetime.fromisoformat(d["finished"]) if d.get("finished") else None,
                workflow_name=d.get("workflow", ""),
                source_name=d.get("source"),
                backend_name=d.get("backend", ""),
                status=d.get("status", ""),
            )
        except (KeyError, ValueError, TypeError) as e:
            # One malformed line must not blank the whole graph.
            logger.warning(f"Skipping unreadable usage entry: {e}")
            return None

    def records(self, *, since: date | None = None) -> list[UsageRecord]:
        """Ledger entries, optionally limited to those starting on/after ``since``."""
        if not self.path.exists():
            return []
        out: list[UsageRecord] = []
        try:
            with open(self.path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rec = self._parse(line)
                    if rec is None:
                        continue
                    if since and rec.started_at.astimezone().date() < since:
                        continue
                    out.append(rec)
        except OSError as e:
            logger.error(f"Failed to read usage log {self.path}: {e}")
        return out

    def _known_keys(self) -> set[tuple[str, str]]:
        if self._keys is None:
            self._keys = {r.key for r in self.records()}
        return self._keys

    # -- writing ---------------------------------------------------------

    def record(self, runs: Iterable[Run]) -> int:
        """Append every terminal task not already in the ledger.

        Idempotent: re-running over the same runs appends nothing, so the
        startup flush and the hourly one can both run freely.
        """
        known = self._known_keys()
        pending = [
            rec for rec in records_from_runs(runs)
            if rec.status in {s.value for s in _TERMINAL} and rec.key not in known
        ]
        if not pending:
            return 0

        lines = []
        for rec in pending:
            lines.append(json.dumps({
                "run": rec.run_id,
                "task": rec.task_id,
                "cpus": rec.cpus,
                "started": rec.started_at.isoformat(),
                "finished": rec.finished_at.isoformat() if rec.finished_at else None,
                "workflow": rec.workflow_name,
                "source": rec.source_name,
                "backend": rec.backend_name,
                "status": rec.status,
            }, separators=(",", ":")))

        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a") as f:
                f.write("\n".join(lines) + "\n")
                f.flush()
                os.fsync(f.fileno())  # the runs themselves are about to be deleted
        except OSError as e:
            logger.error(f"Failed to append to usage log {self.path}: {e}")
            return 0

        known.update(rec.key for rec in pending)
        logger.info(f"Recorded {len(pending)} finished task(s) to the usage log")
        return len(pending)


def merged_records(
    log: UsageLog | None,
    runs: Iterable[Run],
    *,
    since: date | None = None,
) -> list[UsageRecord]:
    """Ledger history plus live runs, with live winning on overlap.

    A task appears in both while it sits between finishing and the next
    flush; the in-memory copy is the fresher of the two, and taking it
    also keeps still-running work on the graph.
    """
    live = {rec.key: rec for rec in records_from_runs(runs)}
    out = [rec for rec in (log.records(since=since) if log else []) if rec.key not in live]
    if since:
        out.extend(r for r in live.values() if r.started_at.astimezone().date() >= since)
    else:
        out.extend(live.values())
    return out


def window_start(days: int, today: date | None = None) -> date:
    """First day of a trailing ``days``-long window ending today."""
    today = today or datetime.now(timezone.utc).astimezone().date()
    return today - timedelta(days=days - 1)
