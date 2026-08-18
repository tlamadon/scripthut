"""Tests for staged datasets in the disk inventory.

Datasets reuse the stack machinery: a two-level ``<root>/<name>/<hash>``
layout, "(superseded)" annotation when the local tree has moved on, and the
same depth/pattern guardrails before anything reaches ``rm -rf``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from scripthut.config_schema import DatasetConfig, ScriptHutConfig
from scripthut.disk.classify import RunReferences, classify_entries
from scripthut.disk.cleanup import DELETABLE_KINDS, _safety_reason, plan_cleanup
from scripthut.disk.models import (
    DiskEntry,
    DiskEntryClass,
    DiskEntryKind,
    DiskScanResult,
    ScanSpec,
)
from scripthut.disk.scan import (
    build_scan_script,
    build_scan_spec,
    parse_scan_output,
    raw_to_entries,
)
from scripthut.disk.service import compute_current_data_hashes


def _entry(path: str, kind: DiskEntryKind = DiskEntryKind.DATA) -> DiskEntry:
    return DiskEntry(path=path, kind=kind, size_bytes=1024)


def _spec(data_dirs: list[str] | None = None) -> ScanSpec:
    return ScanSpec(
        backend="mercury",
        data_dirs=["/scratch/w/acq"] if data_dirs is None else data_dirs,
    )


# ---------- scan ----------------------------------------------------------


class TestScan:
    def test_data_dirs_are_walked_one_level(self):
        script = build_scan_script(_spec())
        assert 'scan_dir data "/scratch/w/acq"' in script

    def test_data_dirs_are_covered_so_the_cache_sweep_skips_them(self):
        script = build_scan_script(_spec())
        prelude = script.split("scan_dir data")[0]
        assert "/scratch/w/acq" in prelude

    def test_build_scan_spec_normalizes_and_dedupes(self):
        config = ScriptHutConfig()
        spec = build_scan_spec(
            config, "mercury", "~/clones",
            data_dirs=["/scratch/w/acq/", "/scratch/w/acq", ""],
        )
        assert spec.data_dirs == ["/scratch/w/acq"]

    def test_entries_parse_with_a_name_slash_hash_detail(self):
        stdout = "ENTRY\tdata\t/scratch/w/acq/aaaaaaaaaaaa\t1700000000\t2048\n"
        _, raw, _, errors = parse_scan_output(stdout)
        assert errors == []
        entries = raw_to_entries(raw)
        assert entries[0].kind == DiskEntryKind.DATA
        assert entries[0].detail == "acq/aaaaaaaaaaaa"
        assert entries[0].size_bytes == 2048 * 1024


# ---------- classification ------------------------------------------------


class TestClassifyData:
    def test_current_hash_is_referenced_without_annotation(self):
        e = _entry("/scratch/w/acq/aaaaaaaaaaaa")
        classify_entries(
            [e], RunReferences(), current_data_hashes={"acq": {"aaaaaaaaaaaa"}},
        )
        assert e.classification == DiskEntryClass.REFERENCED
        assert not (e.detail or "").endswith("(superseded)")

    def test_other_hash_is_flagged_superseded(self):
        e = _entry("/scratch/w/acq/bbbbbbbbbbbb")
        classify_entries(
            [e], RunReferences(), current_data_hashes={"acq": {"aaaaaaaaaaaa"}},
        )
        assert e.classification == DiskEntryClass.REFERENCED
        assert "(superseded)" in (e.detail or "")

    def test_unconfigured_name_is_orphaned(self):
        e = _entry("/scratch/w/gone/aaaaaaaaaaaa")
        classify_entries(
            [e], RunReferences(), current_data_hashes={"acq": {"aaaaaaaaaaaa"}},
        )
        assert e.classification == DiskEntryClass.ORPHANED
        assert "(unconfigured)" in (e.detail or "")

    def test_without_hashes_nothing_is_marked_stale(self):
        e = _entry("/scratch/w/acq/bbbbbbbbbbbb")
        classify_entries([e], RunReferences(), current_data_hashes=None)
        assert e.classification == DiskEntryClass.REFERENCED
        assert e.detail is None


class TestComputeCurrentDataHashes:
    def test_hashes_each_configured_dataset(self, tmp_path: Path):
        local = tmp_path / "acq"
        local.mkdir()
        (local / "a.csv").write_text("xy")
        config = ScriptHutConfig(datasets=[DatasetConfig(name="acq", path=local)])

        hashes = compute_current_data_hashes(config)

        assert set(hashes) == {"acq"}
        assert len(next(iter(hashes.values()))) == 1

    def test_unreadable_dataset_yields_an_empty_set_not_a_crash(self, tmp_path: Path):
        config = ScriptHutConfig(
            datasets=[DatasetConfig(name="acq", path=tmp_path / "gone")]
        )
        assert compute_current_data_hashes(config) == {"acq": set()}


# ---------- cleanup guardrails --------------------------------------------


class TestSafety:
    def test_data_is_deletable_in_principle(self):
        assert DiskEntryKind.DATA in DELETABLE_KINDS

    def test_correct_shape_passes(self):
        entry = _entry("/scratch/w/acq/aaaaaaaaaaaa")
        assert _safety_reason(entry, _spec(), "/home/w") is None

    def test_non_hash_leaf_is_refused(self):
        entry = _entry("/scratch/w/acq/my-notes")
        assert "manifest-hash pattern" in (
            _safety_reason(entry, _spec(), "/home/w") or ""
        )

    def test_wrong_depth_is_refused(self):
        entry = _entry("/scratch/w/acq/aaaaaaaaaaaa/sub")
        assert "unexpected depth" in (
            _safety_reason(entry, _spec(), "/home/w") or ""
        )

    def test_path_outside_the_scanned_dirs_is_refused(self):
        entry = _entry("/scratch/w/other/aaaaaaaaaaaa")
        assert "not under any scanned root" in (
            _safety_reason(entry, _spec(), "/home/w") or ""
        )

    def test_unscanned_data_dirs_fail_closed(self):
        entry = _entry("/scratch/w/acq/aaaaaaaaaaaa")
        assert _safety_reason(entry, _spec(data_dirs=[]), "/home/w") is not None


class TestPlanCleanup:
    def _result(self, *entries: DiskEntry) -> DiskScanResult:
        return DiskScanResult(
            backend="mercury",
            scanned_at=datetime.now(timezone.utc),
            duration_ms=1,
            home_dir="/home/w",
            entries=list(entries),
        )

    def test_superseded_copies_are_not_deleted_in_bulk(self):
        # REFERENCED, so bulk mode leaves it alone — reclaiming an old
        # dataset copy stays an explicit, per-path decision.
        result = self._result(_entry("/scratch/w/acq/bbbbbbbbbbbb"))
        plan = plan_cleanup(
            result,
            RunReferences(),
            spec=_spec(),
            current_stack_hashes={},
            current_data_hashes={"acq": {"aaaaaaaaaaaa"}},
            planned_at=datetime.now(timezone.utc),
        )
        assert plan.to_delete == []

    def test_superseded_copy_can_be_deleted_when_named_and_confirmed(self):
        path = "/scratch/w/acq/bbbbbbbbbbbb"
        plan = plan_cleanup(
            self._result(_entry(path)),
            RunReferences(),
            spec=_spec(),
            current_stack_hashes={},
            current_data_hashes={"acq": {"aaaaaaaaaaaa"}},
            planned_at=datetime.now(timezone.utc),
            paths=[path],
            allow_referenced=frozenset({path}),
        )
        assert [e.entry.path for e in plan.to_delete] == [path]

    def test_the_current_copy_is_also_protected_from_bulk_deletion(self):
        result = self._result(_entry("/scratch/w/acq/aaaaaaaaaaaa"))
        plan = plan_cleanup(
            result,
            RunReferences(),
            spec=_spec(),
            current_stack_hashes={},
            current_data_hashes={"acq": {"aaaaaaaaaaaa"}},
            planned_at=datetime.now(timezone.utc),
        )
        assert plan.to_delete == []

    def test_replanning_does_not_stack_annotations(self):
        # plan_cleanup reclassifies copies; the detail must be rebuilt from
        # the path each time, not appended to.
        result = self._result(_entry("/scratch/w/acq/bbbbbbbbbbbb"))
        for _ in range(2):
            plan = plan_cleanup(
                result,
                RunReferences(),
                spec=_spec(),
                current_stack_hashes={},
                current_data_hashes={"acq": {"aaaaaaaaaaaa"}},
                planned_at=datetime.now(timezone.utc),
                paths=["/scratch/w/acq/bbbbbbbbbbbb"],
                allow_referenced=frozenset({"/scratch/w/acq/bbbbbbbbbbbb"}),
            )
        assert plan.to_delete[0].entry.detail == "acq/bbbbbbbbbbbb (superseded)"
