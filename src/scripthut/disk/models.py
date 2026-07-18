"""Data models for remote disk-usage scanning.

A scan inventories the directories ScriptHut creates on a backend's
filesystem (source clones, agent workspaces, stacks, logs) and
classifies each entry against the runs the server knows about. Phase 1
is report-only: classification marks what a future cleanup could act
on, but nothing here deletes anything.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

# Remote fallback log root used when a task's working dir is not inside
# a git checkout (runs/manager._build_run). Hardcoded there too — the
# path is a remote-side convention, not derived from local data_dir.
DEFAULT_LOG_ROOT = "~/.cache/scripthut/logs"


class DiskEntryKind(str, Enum):
    """What a scanned directory is, judged by location and naming."""

    CLONE = "clone"  # <clone_dir>/<12-hex commit hash>
    AGENT = "agent"  # <clone_dir>/agent-<8-hex uid>
    STACK = "stack"  # <cache_dir>/<name>/<hash>
    LOG = "log"      # <log_root>/<workflow>
    OTHER = "other"  # unrecognized item inside a scanned root


class DiskEntryClass(str, Enum):
    """How an entry relates to the runs this server knows about."""

    ACTIVE = "active"          # referenced by a pending/running run
    REFERENCED = "referenced"  # referenced by a terminal run still in local storage
    ORPHANED = "orphaned"      # scripthut-made but no known run references it
    UNKNOWN = "unknown"        # not scripthut naming — report size, never touch


@dataclass
class DiskEntry:
    """One directory (or stray file) found by the remote scan."""

    path: str  # absolute remote path
    kind: DiskEntryKind
    size_bytes: int | None = None  # None = du failed/timed out
    mtime: datetime | None = None
    classification: DiskEntryClass = DiskEntryClass.UNKNOWN
    run_ids: list[str] = field(default_factory=list)
    detail: str | None = None  # e.g. "julia/ab12cd34ef56", workflow name
    ready: bool | None = None  # stacks only: .ready sentinel present

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind.value,
            "size_bytes": self.size_bytes,
            "mtime": self.mtime.isoformat() if self.mtime else None,
            "classification": self.classification.value,
            "run_ids": self.run_ids,
            "detail": self.detail,
            "ready": self.ready,
        }


@dataclass
class DiskScanResult:
    """Outcome of one scan of one backend, cached server-side."""

    backend: str
    scanned_at: datetime
    duration_ms: int
    home_dir: str | None = None
    disk_total_bytes: int | None = None
    disk_avail_bytes: int | None = None
    entries: list[DiskEntry] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def total_bytes(self) -> int:
        """Sum of all sized entries (unsized ones count as 0)."""
        return sum(e.size_bytes or 0 for e in self.entries)

    def _totals(self, key: str) -> dict[str, tuple[int, int]]:
        totals: dict[str, tuple[int, int]] = {}
        for e in self.entries:
            k = getattr(e, key).value
            count, size = totals.get(k, (0, 0))
            totals[k] = (count + 1, size + (e.size_bytes or 0))
        return totals

    @property
    def totals_by_class(self) -> dict[str, tuple[int, int]]:
        """classification value -> (entry count, total bytes)."""
        return self._totals("classification")

    @property
    def totals_by_kind(self) -> dict[str, tuple[int, int]]:
        """kind value -> (entry count, total bytes)."""
        return self._totals("kind")

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "scanned_at": self.scanned_at.isoformat(),
            "duration_ms": self.duration_ms,
            "home_dir": self.home_dir,
            "disk_total_bytes": self.disk_total_bytes,
            "disk_avail_bytes": self.disk_avail_bytes,
            "total_bytes": self.total_bytes,
            "totals_by_class": {k: list(v) for k, v in self.totals_by_class.items()},
            "totals_by_kind": {k: list(v) for k, v in self.totals_by_kind.items()},
            "entries": [e.to_dict() for e in self.entries],
            "errors": self.errors,
        }


@dataclass
class ScanSpec:
    """What to scan on one backend."""

    backend: str
    clone_dirs: list[str] = field(default_factory=list)
    stack_dirs: list[str] = field(default_factory=list)
    log_roots: list[str] = field(default_factory=lambda: [DEFAULT_LOG_ROOT])
    du_entry_timeout: int = 60  # seconds; per-entry `timeout N du -sk`
