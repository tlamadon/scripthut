"""Cleanup planning and remote-deletion scripts (pure functions).

Phase 2 of the disk feature: turn scan classifications into deletions,
with the guardrails doing the real work:

- Only paths present in the backend's last scan can ever be deleted
  (the caller passes that scan in; unknown paths reject the request).
- Classification is recomputed here against *current* runs — an entry
  that was orphaned at scan time but is referenced or active now flips
  before selection, so a stale scan can only shrink what gets deleted.
- REFERENCED entries need per-path opt-in (``allow_referenced``);
  ACTIVE and unrecognized entries are never deletable.
- Agent workspaces are deleted only after a remote git check shows no
  uncommitted or unpushed work.
- Every path is re-validated for shape (under a scanned root, correct
  depth, naming pattern) before it reaches ``rm -rf``.

Nothing here talks to the network; the service layer wires SSH.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any

from scripthut.disk.classify import (
    RunReferences,
    classify_entries,
    normalize_remote_path,
)
from scripthut.disk.models import (
    DiskEntry,
    DiskEntryClass,
    DiskEntryKind,
    DiskScanResult,
    ScanSpec,
)
from scripthut.disk.scan import AGENT_DIR_RE, CLONE_HASH_RE

DELETABLE_KINDS = frozenset(
    {DiskEntryKind.CLONE, DiskEntryKind.AGENT, DiskEntryKind.STACK, DiskEntryKind.LOG}
)


@dataclass
class CleanupPlanEntry:
    """One entry's fate in a cleanup plan."""

    entry: DiskEntry  # fresh-classified copy, never the cached object
    action: str  # "delete" | "check_then_delete" | "skip"
    reason: str | None = None  # why skipped
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry": self.entry.to_dict(),
            "action": self.action,
            "reason": self.reason,
            "warnings": self.warnings,
        }


@dataclass
class CleanupPlan:
    backend: str
    planned_at: datetime
    entries: list[CleanupPlanEntry] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)  # request-level (unknown paths)

    @property
    def to_delete(self) -> list[CleanupPlanEntry]:
        return [e for e in self.entries if e.action != "skip"]

    @property
    def delete_bytes(self) -> int:
        return sum(e.entry.size_bytes or 0 for e in self.to_delete)

    @property
    def counts(self) -> dict[str, int]:
        c = {"delete": 0, "check": 0, "skip": 0}
        for e in self.entries:
            if e.action == "delete":
                c["delete"] += 1
            elif e.action == "check_then_delete":
                c["check"] += 1
            else:
                c["skip"] += 1
        return c

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "planned_at": self.planned_at.isoformat(),
            "entries": [e.to_dict() for e in self.entries],
            "errors": self.errors,
            "counts": self.counts,
            "delete_bytes": self.delete_bytes,
        }


@dataclass
class CleanupOutcome:
    path: str
    kind: DiskEntryKind
    size_bytes: int | None
    outcome: str  # "deleted" | "skipped" | "failed"
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind.value,
            "size_bytes": self.size_bytes,
            "outcome": self.outcome,
            "reason": self.reason,
        }


@dataclass
class CleanupReport:
    backend: str
    started_at: datetime
    finished_at: datetime | None = None
    outcomes: list[CleanupOutcome] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def freed_bytes(self) -> int:
        return sum(
            o.size_bytes or 0 for o in self.outcomes if o.outcome == "deleted"
        )

    @property
    def freed_is_lower_bound(self) -> bool:
        return any(
            o.size_bytes is None for o in self.outcomes if o.outcome == "deleted"
        )

    @property
    def counts(self) -> dict[str, int]:
        c = {"deleted": 0, "skipped": 0, "failed": 0}
        for o in self.outcomes:
            c[o.outcome] = c.get(o.outcome, 0) + 1
        return c

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "outcomes": [o.to_dict() for o in self.outcomes],
            "errors": self.errors,
            "counts": self.counts,
            "freed_bytes": self.freed_bytes,
            "freed_is_lower_bound": self.freed_is_lower_bound,
        }


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def _base_detail(e: DiskEntry) -> str | None:
    """Recompute detail from the path, dropping classifier annotations.

    ``classify_entries`` appends "(superseded)"/"(unconfigured)" to
    ``detail`` in place; re-classifying a cached entry directly would
    stack the annotations, so plans work on copies with detail rebuilt.
    """
    parts = e.path.split("/")
    if e.kind == DiskEntryKind.STACK:
        detail = "/".join(parts[-2:])
        if e.ready is False:
            detail += " (half-built)"
        return detail
    if e.kind == DiskEntryKind.LOG:
        return parts[-1]
    return None


def _fresh_copies(
    result: DiskScanResult,
    refs: RunReferences,
    current_stack_hashes: dict[str, set[str]] | None,
) -> list[DiskEntry]:
    copies = [
        replace(
            e,
            run_ids=[],
            classification=DiskEntryClass.UNKNOWN,
            detail=_base_detail(e),
        )
        for e in result.entries
    ]
    classify_entries(copies, refs, current_stack_hashes=current_stack_hashes)
    return copies


def _safety_reason(entry: DiskEntry, spec: ScanSpec, home: str | None) -> str | None:
    """Last line of defense before a path reaches ``rm -rf``."""
    p = entry.path
    parts = p.split("/")
    if not p.startswith("/") or any(seg in ("", ".", "..") for seg in parts[1:]):
        return "not a clean absolute path"
    if len([seg for seg in parts if seg]) < 3:
        return "path too shallow to delete safely"

    if entry.kind in (DiskEntryKind.CLONE, DiskEntryKind.AGENT):
        roots, depth = spec.clone_dirs, 1
    elif entry.kind == DiskEntryKind.STACK:
        roots, depth = spec.stack_dirs, 2
    else:
        roots, depth = spec.log_roots, 1

    matched_root: str | None = None
    for root in roots:
        norm = normalize_remote_path(root, home)
        if not norm.startswith("/"):
            continue  # ~-relative root with unknown home: can't verify against it
        if p.startswith(norm + "/"):
            matched_root = norm
            break
    if matched_root is None:
        return "not under any scanned root (or remote home unknown)"
    if home and p == home.rstrip("/"):
        return "path equals the remote home directory"

    below = p[len(matched_root) + 1 :].split("/")
    if len(below) != depth:
        return f"unexpected depth under {matched_root}"
    if entry.kind == DiskEntryKind.CLONE and not CLONE_HASH_RE.match(below[0]):
        return "clone name no longer matches the commit-hash pattern"
    if entry.kind == DiskEntryKind.AGENT and not AGENT_DIR_RE.match(below[0]):
        return "agent workspace name no longer matches the expected pattern"
    return None


def _log_warnings(entry: DiskEntry, refs: RunReferences) -> list[str]:
    if entry.kind != DiskEntryKind.CLONE:
        if entry.kind == DiskEntryKind.AGENT:
            return [
                "will be skipped at execution if the workspace has "
                "uncommitted or unpushed work"
            ]
        return []
    log_runs: set[str] = set()
    for ld, rids in refs.log_dirs.items():
        if ld == entry.path or ld.startswith(entry.path + "/"):
            log_runs |= rids
    if log_runs:
        return [
            f"contains logs of run(s) {', '.join(sorted(log_runs))} — "
            "logs are not archived"
        ]
    return ["may contain logs of forgotten runs (not archived)"]


def plan_cleanup(
    result: DiskScanResult,
    refs: RunReferences,
    *,
    spec: ScanSpec,
    current_stack_hashes: dict[str, set[str]] | None,
    planned_at: datetime,
    paths: list[str] | None = None,
    allow_referenced: frozenset[str] = frozenset(),
) -> CleanupPlan:
    """Decide, entry by entry, what a cleanup would delete and why not.

    ``paths=None`` is bulk mode: every entry that is ORPHANED *under
    current classification* and of a deletable kind. Explicit ``paths``
    must all exist in the scan (else ``plan.errors`` — reject the whole
    request); referenced entries among them additionally need their
    path in ``allow_referenced``.
    """
    plan = CleanupPlan(backend=result.backend, planned_at=planned_at)
    copies = _fresh_copies(result, refs, current_stack_hashes)
    by_path = {e.path: e for e in copies}
    allow = {normalize_remote_path(p, result.home_dir) for p in allow_referenced}

    if paths is None:
        candidates = [
            e
            for e in copies
            if e.classification == DiskEntryClass.ORPHANED
            and e.kind in DELETABLE_KINDS
        ]
    else:
        candidates = []
        for p in paths:
            norm = normalize_remote_path(p, result.home_dir)
            entry = by_path.get(norm)
            if entry is None:
                plan.errors.append(
                    f"path not in the last scan for this backend: {p}"
                )
            else:
                candidates.append(entry)
        if plan.errors:
            return plan

    for entry in candidates:
        reason: str | None = None
        if entry.kind not in DELETABLE_KINDS:
            reason = "unrecognized entries are never deletable"
        elif entry.classification == DiskEntryClass.ACTIVE:
            reason = (
                f"referenced by active run(s) {', '.join(entry.run_ids)}"
            )
        elif entry.classification == DiskEntryClass.UNKNOWN:
            reason = "classification unknown — not deletable"
        elif (
            entry.classification == DiskEntryClass.REFERENCED
            and entry.path not in allow
        ):
            reason = (
                f"referenced by run(s) {', '.join(entry.run_ids)}; "
                "delete individually to confirm"
            )
        else:
            reason = _safety_reason(entry, spec, result.home_dir)

        if reason is not None:
            plan.entries.append(
                CleanupPlanEntry(entry=entry, action="skip", reason=reason)
            )
            continue

        action = (
            "check_then_delete" if entry.kind == DiskEntryKind.AGENT else "delete"
        )
        plan.entries.append(
            CleanupPlanEntry(
                entry=entry, action=action, warnings=_log_warnings(entry, refs)
            )
        )
    return plan


# ---------------------------------------------------------------------------
# Remote scripts (same heredoc + tab-protocol style as scan.py)
# ---------------------------------------------------------------------------

_AGENT_HEREDOC = "__SCRIPTHUT_AGENTCHECK__"
_DELETE_HEREDOC = "__SCRIPTHUT_DELETE__"

# Paths here are absolute (enforced by _safety_reason), so shlex.quote's
# single quotes are safe — no tilde expansion needed, unlike scan roots.
_AGENT_CHECK_FN = """\
check_agent() {
  local p="$1" u
  if ! git -C "$p" rev-parse --git-dir >/dev/null 2>&1; then
    printf 'CHECK\\t%s\\tnogit\\t-\\n' "$p"; return
  fi
  if [ -n "$(git -C "$p" status --porcelain 2>/dev/null | head -1)" ]; then
    printf 'CHECK\\t%s\\tdirty\\t-\\n' "$p"; return
  fi
  u=$(git -C "$p" log --branches --not --remotes --oneline -1 2>/dev/null | tr '\\t' ' ')
  if [ -n "$u" ]; then
    printf 'CHECK\\t%s\\tunpushed\\t%s\\n' "$p" "$u"
  else
    printf 'CHECK\\t%s\\tclean\\t-\\n' "$p"
  fi
}
"""

_DELETE_FN = """\
del_one() {
  local p="$1" err rc
  err=$(rm -rf -- "$p" 2>&1); rc=$?
  if [ $rc -ne 0 ] || [ -e "$p" ]; then
    printf 'DEL\\t%s\\tFAIL\\t%s\\n' "$p" "$(printf '%s' "$err" | tr '\\t\\n' '  ' | head -c 300)"
  else
    printf 'DEL\\t%s\\tOK\\t-\\n' "$p"
  fi
}
"""


def build_agent_check_script(paths: list[str]) -> str:
    calls = "\n".join(f"check_agent {shlex.quote(p)}" for p in paths)
    return (
        f"bash -s <<'{_AGENT_HEREDOC}'\n{_AGENT_CHECK_FN}\n{calls}\n{_AGENT_HEREDOC}"
    )


def parse_agent_check_output(stdout: str) -> dict[str, tuple[str, str]]:
    """path -> (status, detail); status: clean|dirty|unpushed|nogit."""
    results: dict[str, tuple[str, str]] = {}
    for line in stdout.splitlines():
        fields = line.split("\t")
        if len(fields) >= 4 and fields[0] == "CHECK":
            results[fields[1]] = (fields[2], "\t".join(fields[3:]))
    return results


def build_delete_script(paths: list[str]) -> str:
    calls = "\n".join(f"del_one {shlex.quote(p)}" for p in paths)
    return f"bash -s <<'{_DELETE_HEREDOC}'\n{_DELETE_FN}\n{calls}\n{_DELETE_HEREDOC}"


def parse_delete_output(stdout: str) -> dict[str, tuple[bool, str]]:
    """path -> (ok, message)."""
    results: dict[str, tuple[bool, str]] = {}
    for line in stdout.splitlines():
        fields = line.split("\t")
        if len(fields) >= 4 and fields[0] == "DEL":
            results[fields[1]] = (fields[2] == "OK", "\t".join(fields[3:]))
    return results
