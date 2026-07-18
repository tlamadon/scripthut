"""Classify scanned disk entries against the runs this server knows.

Pure functions: the caller gathers runs (in-memory + storage) and the
remote ``$HOME``; everything here is deterministic and unit-testable.

"Orphaned" means *no run this server remembers references it* — local
run records are pruned after 30 days, so an orphaned entry is a strong
cleanup candidate but not proof of disuse (another server or an
expired run may have made it). Phase 1 only reports; a future cleanup
phase must keep that caveat in mind.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterable

from scripthut.disk.models import DiskEntry, DiskEntryClass, DiskEntryKind
from scripthut.disk.scan import AGENT_DIR_RE, CLONE_HASH_RE
from scripthut.runs.models import RunStatus

if TYPE_CHECKING:
    from scripthut.runs.models import Run


def normalize_remote_path(path: str, home: str | None) -> str:
    """Resolve a leading ``~`` against the remote home; strip trailing ``/``.

    Needed because config paths are usually ``~``-relative while paths
    recorded on runs (e.g. ``log_dir`` from ``git rev-parse``) are
    absolute — comparisons only work in one namespace.
    """
    p = path.strip()
    if home:
        h = home.rstrip("/")
        if p == "~":
            p = h
        elif p.startswith("~/"):
            p = h + p[1:]
    return p.rstrip("/") or "/"


@dataclass
class RunReferences:
    """Which remote directories the known runs point at."""

    clone_hashes: dict[str, set[str]] = field(default_factory=dict)  # 12-hex -> run ids
    agent_dirs: dict[str, set[str]] = field(default_factory=dict)    # abs path -> run ids
    log_dirs: dict[str, set[str]] = field(default_factory=dict)      # abs path -> run ids
    active_run_ids: set[str] = field(default_factory=set)


def _first_component_under(path: str, parent: str) -> str | None:
    """First path segment of ``path`` below ``parent``, or None if not below."""
    if path == parent or not path.startswith(parent + "/"):
        return None
    return path[len(parent) + 1 :].split("/", 1)[0]


def build_run_references(
    runs: Iterable[Run],
    backend_name: str,
    clone_dirs: list[str],
    home: str | None,
) -> RunReferences:
    """Derive reference sets for one backend from all known runs.

    References come from three places: the run's recorded commit hash
    (robust even when working-dir matching fails), each task's resolved
    working_dir (prefix-matched under a clone dir), and the run's
    log_dir — which, when it lives inside a clone, also counts as a
    reference to that clone (deleting the clone would take the logs).
    """
    refs = RunReferences()
    norm_clone_dirs = [normalize_remote_path(d, home) for d in clone_dirs]

    def note_clone_child(path: str, run_id: str) -> None:
        for cd in norm_clone_dirs:
            first = _first_component_under(path, cd)
            if first is None:
                continue
            if CLONE_HASH_RE.match(first):
                refs.clone_hashes.setdefault(first, set()).add(run_id)
            elif AGENT_DIR_RE.match(first):
                refs.agent_dirs.setdefault(f"{cd}/{first}", set()).add(run_id)
            return

    for run in runs:
        if run.backend_name != backend_name:
            continue
        if run.status in (RunStatus.PENDING, RunStatus.RUNNING):
            refs.active_run_ids.add(run.id)
        if run.commit_hash:
            refs.clone_hashes.setdefault(run.commit_hash[:12], set()).add(run.id)
        for item in run.items:
            note_clone_child(
                normalize_remote_path(item.task.working_dir, home), run.id
            )
        if run.log_dir and not run.log_dir.startswith("backend://"):
            ld = normalize_remote_path(run.log_dir, home)
            refs.log_dirs.setdefault(ld, set()).add(run.id)
            note_clone_child(ld, run.id)

    return refs


def classify_entries(
    entries: list[DiskEntry],
    refs: RunReferences,
    *,
    current_stack_hashes: dict[str, set[str]] | None = None,
) -> None:
    """Set classification/run_ids on each entry in place.

    ``current_stack_hashes`` maps a declared stack name -> the set of
    currently valid content hashes (a set because the server config and
    several sources' project files may each declare the name with
    different inputs); when provided, stack entries get
    "(superseded)"/"(unconfigured)" annotations.
    """
    for e in entries:
        if e.kind == DiskEntryKind.CLONE:
            _apply_refs(e, refs, refs.clone_hashes.get(e.path.rsplit("/", 1)[-1]))
        elif e.kind == DiskEntryKind.AGENT:
            _apply_refs(e, refs, refs.agent_dirs.get(e.path))
        elif e.kind == DiskEntryKind.LOG:
            ids: set[str] = set()
            for ld, rids in refs.log_dirs.items():
                if ld == e.path or ld.startswith(e.path + "/"):
                    ids |= rids
            _apply_refs(e, refs, ids or None)
        elif e.kind == DiskEntryKind.STACK:
            _classify_stack(e, current_stack_hashes)
        else:
            e.classification = DiskEntryClass.UNKNOWN


def _apply_refs(e: DiskEntry, refs: RunReferences, ids: set[str] | None) -> None:
    if ids:
        e.run_ids = sorted(ids)
        e.classification = (
            DiskEntryClass.ACTIVE
            if ids & refs.active_run_ids
            else DiskEntryClass.REFERENCED
        )
    else:
        e.classification = DiskEntryClass.ORPHANED


def _classify_stack(
    e: DiskEntry, current_stack_hashes: dict[str, set[str]] | None
) -> None:
    if e.ready is False:
        # Interrupted install: rebuild rm -rf's it anyway, safe to flag
        e.classification = DiskEntryClass.ORPHANED
        return
    parts = e.path.rsplit("/", 2)
    name, hash_ = (parts[-2], parts[-1]) if len(parts) >= 2 else ("", "")
    if current_stack_hashes is None:
        e.classification = DiskEntryClass.REFERENCED
    elif name not in current_stack_hashes:
        e.classification = DiskEntryClass.ORPHANED
        e.detail = (e.detail or "") + " (unconfigured)"
    elif hash_ not in current_stack_hashes[name]:
        e.classification = DiskEntryClass.REFERENCED
        e.detail = (e.detail or "") + " (superseded)"
    else:
        e.classification = DiskEntryClass.REFERENCED
