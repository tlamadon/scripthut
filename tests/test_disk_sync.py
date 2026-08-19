"""Tests for type: sync dests in the disk inventory.

A sync dest is a live working copy — inventoried so it shows up on the
disk page, classified against runs and the current config, and never
offered to ``disk clean``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

from scripthut.config_schema import (
    ScriptHutConfig,
    SlurmBackendConfig,
    SSHConfig,
    SyncSourceConfig,
)
from scripthut.disk.classify import RunReferences, build_run_references, classify_entries
from scripthut.disk.cleanup import DELETABLE_KINDS, plan_cleanup
from scripthut.disk.models import (
    DiskEntry,
    DiskEntryClass,
    DiskEntryKind,
    DiskScanResult,
    ScanSpec,
)
from scripthut.disk.scan import (
    RawEntry,
    build_scan_script,
    build_scan_spec,
    parse_scan_output,
    raw_to_entries,
)
from scripthut.disk.service import gather_sync_dirs
from scripthut.runs.models import Run, RunItem, RunItemStatus, SyncDep, TaskDefinition

HOME = "/home/alice"
SYNC_PARENT = f"{HOME}/scripthut-sync"
DEST = f"{SYNC_PARENT}/wl"
NOW = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)


def _ssh(home: str = HOME) -> AsyncMock:
    ssh = AsyncMock()
    ssh.run_command = AsyncMock(return_value=(f"HOME\t{home}\n", "", 0))
    return ssh


def _cfg(**kwargs) -> ScriptHutConfig:
    return ScriptHutConfig(
        backends=[
            SlurmBackendConfig(
                name="hpc",
                type="slurm",
                ssh=SSHConfig(host="login.example", user="alice"),
            )
        ],
        **kwargs,
    )


def _entry(path: str = DEST, **kw) -> DiskEntry:
    kw.setdefault("size_bytes", 1024)
    return DiskEntry(path=path, kind=DiskEntryKind.SYNC, **kw)


def _run(
    run_id: str = "r1",
    dest: str = DEST,
    statuses: list[RunItemStatus] | None = None,
    workflow_name: str = "wl/train",
) -> Run:
    statuses = statuses or [RunItemStatus.COMPLETED]
    items = [
        RunItem(
            task=TaskDefinition(
                id="_sync.upload",
                name="upload",
                command=": sync upload",
                working_dir=dest,
                sync_dep=SyncDep(
                    kind="upload",
                    local_path="/Users/me/wl",
                    dest=dest,
                ),
            ),
            status=statuses[0],
        )
    ]
    return Run(
        id=run_id,
        workflow_name=workflow_name,
        backend_name="hpc",
        created_at=NOW,
        items=items,
        max_concurrent=None,
        log_dir="",
    )


# ---------- scan ----------------------------------------------------------


class TestScan:
    def test_parents_are_walked_one_level(self):
        script = build_scan_script(
            ScanSpec(backend="hpc", sync_parents=["~/scripthut-sync"])
        )
        assert 'scan_dir sync "$HOME/scripthut-sync"' in script

    def test_explicit_dests_are_inventoried_as_whole_trees(self):
        script = build_scan_script(
            ScanSpec(backend="hpc", sync_dirs=["/scratch/wl"])
        )
        assert 'scan_self sync "/scratch/wl"' in script
        assert "scan_dir sync" not in script

    def test_sync_roots_are_covered_so_the_cache_sweep_skips_them(self):
        script = build_scan_script(
            ScanSpec(
                backend="hpc",
                clone_dirs=["/scratch/repos"],
                sync_parents=["/scratch/sync"],
                sync_dirs=["/scratch/wl"],
            )
        )
        prelude = script.split("scan_dir clones")[0]
        assert "/scratch/sync" in prelude
        assert "/scratch/wl" in prelude

    def test_build_scan_spec_normalizes_and_dedupes(self):
        spec = build_scan_spec(
            ScriptHutConfig(),
            "hpc",
            "~/clones",
            sync_parents=["/scratch/sync/", "/scratch/sync", ""],
            sync_dirs=["/scratch/wl/", "/scratch/wl"],
        )
        assert spec.sync_parents == ["/scratch/sync"]
        assert spec.sync_dirs == ["/scratch/wl"]

    def test_entries_parse_as_sync_kind_named_by_basename(self):
        stdout = f"ENTRY\tsync\t{DEST}\t1700000000\t2048\n"
        _, raw, _, errors = parse_scan_output(stdout)
        assert errors == []
        entries = raw_to_entries(raw)
        assert entries[0].kind == DiskEntryKind.SYNC
        assert entries[0].detail == "wl"
        assert entries[0].size_bytes == 2048 * 1024

    def test_raw_to_entries_sync_section(self):
        entries = raw_to_entries(
            [RawEntry(section="sync", path=DEST, mtime=None, size_bytes=1, ready=None)]
        )
        assert entries[0].kind == DiskEntryKind.SYNC
        assert entries[0].detail == "wl"


# ---------- gather --------------------------------------------------------


class TestGatherSyncDirs:
    async def test_no_ssh_returns_none_dest_map(self):
        parents, dests, mapping, errors = await gather_sync_dirs(
            _cfg(), "hpc", ssh=None,
        )
        assert (parents, dests, mapping, errors) == ([], [], None, [])

    async def test_walks_backend_sync_dir_even_without_sources(self):
        parents, dests, mapping, errors = await gather_sync_dirs(
            _cfg(), "hpc", ssh=_ssh(),
        )
        assert parents == [SYNC_PARENT]
        assert dests == []
        assert mapping == {}
        assert errors == []

    async def test_default_dest_is_found_by_walking_parent(self):
        cfg = _cfg(
            sources=[
                SyncSourceConfig(name="wl", path=Path("/tmp/wl"), backend="hpc"),
            ]
        )
        parents, dests, mapping, errors = await gather_sync_dirs(
            cfg, "hpc", ssh=_ssh(),
        )
        assert parents == [SYNC_PARENT]
        assert dests == []
        assert mapping == {DEST: "wl"}
        assert errors == []

    async def test_explicit_dest_outside_parent_is_self_scanned(self):
        cfg = _cfg(
            sources=[
                SyncSourceConfig(
                    name="wl",
                    path=Path("/tmp/wl"),
                    backend="hpc",
                    dest="/scratch/wl",
                ),
            ]
        )
        parents, dests, mapping, errors = await gather_sync_dirs(
            cfg, "hpc", ssh=_ssh(),
        )
        assert SYNC_PARENT in parents
        assert dests == ["/scratch/wl"]
        assert mapping == {"/scratch/wl": "wl"}
        assert errors == []

    async def test_dest_inside_clone_dir_is_error_not_scanned(self):
        cfg = _cfg(
            sources=[
                SyncSourceConfig(
                    name="wl",
                    path=Path("/tmp/wl"),
                    backend="hpc",
                    dest="~/scripthut-repos/wl",
                ),
            ]
        )
        parents, dests, mapping, errors = await gather_sync_dirs(
            cfg, "hpc", ssh=_ssh(),
        )
        assert mapping == {}
        assert dests == []
        assert errors
        assert "clone directory" in errors[0]
        # The default parent is still walked so leftovers remain visible.
        assert parents == [SYNC_PARENT]

    async def test_source_on_another_backend_is_ignored(self):
        cfg = _cfg(
            sources=[
                SyncSourceConfig(name="wl", path=Path("/tmp/wl"), backend="other"),
            ]
        )
        parents, dests, mapping, errors = await gather_sync_dirs(
            cfg, "hpc", ssh=_ssh(),
        )
        assert mapping == {}
        assert dests == []
        assert parents == [SYNC_PARENT]
        assert errors == []


# ---------- classification ------------------------------------------------


class TestClassifySync:
    def test_active_run_marks_dest_active(self):
        refs = build_run_references(
            [_run(statuses=[RunItemStatus.RUNNING])],
            "hpc",
            ["~/scripthut-repos"],
            HOME,
        )
        e = _entry()
        classify_entries(
            [e], refs, current_sync_dests={DEST: "wl"},
        )
        assert e.classification == DiskEntryClass.ACTIVE
        assert e.run_ids == ["r1"]
        assert e.source == "wl"

    def test_terminal_run_marks_dest_referenced(self):
        refs = build_run_references(
            [_run()], "hpc", ["~/scripthut-repos"], HOME,
        )
        e = _entry()
        classify_entries([e], refs, current_sync_dests={DEST: "wl"})
        assert e.classification == DiskEntryClass.REFERENCED
        assert e.source == "wl"

    def test_configured_dest_with_no_runs_is_live(self):
        e = _entry()
        classify_entries(
            [e], RunReferences(), current_sync_dests={DEST: "wl"},
        )
        assert e.classification == DiskEntryClass.REFERENCED
        assert e.source == "wl"

    def test_leftover_under_sync_dir_is_orphaned(self):
        leftover = f"{SYNC_PARENT}/old-source"
        e = _entry(leftover)
        classify_entries(
            [e], RunReferences(), current_sync_dests={DEST: "wl"},
        )
        assert e.classification == DiskEntryClass.ORPHANED
        assert e.source is None

    def test_without_dest_map_nothing_is_flagged_leftover(self):
        e = _entry(f"{SYNC_PARENT}/old-source")
        classify_entries([e], RunReferences(), current_sync_dests=None)
        assert e.classification == DiskEntryClass.REFERENCED


# ---------- cleanup -------------------------------------------------------


class TestCleanup:
    def test_sync_is_not_a_deletable_kind(self):
        assert DiskEntryKind.SYNC not in DELETABLE_KINDS

    def test_orphaned_leftover_is_not_in_bulk_plan(self):
        leftover = f"{SYNC_PARENT}/old-source"
        result = DiskScanResult(
            backend="hpc", scanned_at=NOW, duration_ms=1, home_dir=HOME,
            entries=[_entry(leftover)],
        )
        plan = plan_cleanup(
            result,
            RunReferences(),
            spec=ScanSpec(backend="hpc", sync_parents=[SYNC_PARENT]),
            current_stack_hashes={},
            current_sync_dests={DEST: "wl"},
            planned_at=NOW,
        )
        assert plan.to_delete == []
        assert plan.entries == []

    def test_explicit_path_is_skipped_not_deleted(self):
        result = DiskScanResult(
            backend="hpc", scanned_at=NOW, duration_ms=1, home_dir=HOME,
            entries=[_entry()],
        )
        plan = plan_cleanup(
            result,
            RunReferences(),
            spec=ScanSpec(backend="hpc", sync_parents=[SYNC_PARENT]),
            current_stack_hashes={},
            current_sync_dests={DEST: "wl"},
            planned_at=NOW,
            paths=[DEST],
        )
        assert plan.to_delete == []
        assert plan.entries[0].action == "skip"
        assert "never deleted" in (plan.entries[0].reason or "")
