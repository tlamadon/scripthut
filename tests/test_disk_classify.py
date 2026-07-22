"""Tests for run-reference building and disk-entry classification."""

from __future__ import annotations

from datetime import UTC, datetime

from scripthut.disk.classify import (
    build_run_references,
    classify_entries,
    normalize_remote_path,
    run_source_label,
)
from scripthut.disk.models import DiskEntry, DiskEntryClass, DiskEntryKind
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition

HOME = "/home/alice"
CLONE_DIR = "~/scripthut-repos"
ABS_CLONE_DIR = f"{HOME}/scripthut-repos"
HASH = "a1b2c3d4e5f6"


def _run(
    run_id: str = "r1",
    backend: str = "hpc",
    working_dir: str = "~",
    statuses: list[RunItemStatus] | None = None,
    log_dir: str = "",
    commit_hash: str | None = None,
    workflow_name: str = "wf",
) -> Run:
    statuses = statuses or [RunItemStatus.COMPLETED]
    items = [
        RunItem(
            task=TaskDefinition(id=f"t{i}", name=f"t{i}", command="true", working_dir=working_dir),
            status=s,
        )
        for i, s in enumerate(statuses)
    ]
    return Run(
        id=run_id,
        workflow_name=workflow_name,
        backend_name=backend,
        created_at=datetime(2026, 7, 1, tzinfo=UTC),
        items=items,
        max_concurrent=None,
        log_dir=log_dir,
        commit_hash=commit_hash,
    )


class TestNormalizeRemotePath:
    def test_tilde_forms(self):
        assert normalize_remote_path("~", HOME) == HOME
        assert normalize_remote_path("~/x/y", HOME) == f"{HOME}/x/y"
        assert normalize_remote_path("~/x/", HOME) == f"{HOME}/x"

    def test_absolute_passthrough(self):
        assert normalize_remote_path("/scratch/r/", HOME) == "/scratch/r"

    def test_no_home_passthrough(self):
        assert normalize_remote_path("~/x", None) == "~/x"

    def test_root(self):
        assert normalize_remote_path("/", HOME) == "/"


class TestBuildRunReferences:
    def test_working_dir_clone_ref(self):
        runs = [_run(working_dir=f"{CLONE_DIR}/{HASH}/sub")]
        refs = build_run_references(runs, "hpc", [CLONE_DIR], HOME)
        assert refs.clone_hashes == {HASH: {"r1"}}
        assert refs.active_run_ids == set()

    def test_working_dir_agent_ref(self):
        runs = [_run(working_dir=f"{CLONE_DIR}/agent-1a2b3c4d")]
        refs = build_run_references(runs, "hpc", [CLONE_DIR], HOME)
        assert refs.agent_dirs == {f"{ABS_CLONE_DIR}/agent-1a2b3c4d": {"r1"}}

    def test_commit_hash_fallback(self):
        # working_dir outside any clone_dir, but commit_hash recorded
        runs = [_run(working_dir="/elsewhere", commit_hash="a1b2c3d4e5f6789abcde")]
        refs = build_run_references(runs, "hpc", [CLONE_DIR], HOME)
        assert refs.clone_hashes == {HASH: {"r1"}}

    def test_absolute_log_dir_inside_tilde_clone_dir(self):
        # the ~/absolute mismatch case: log_dir is absolute, clone_dir is ~
        runs = [_run(log_dir=f"{ABS_CLONE_DIR}/{HASH}/.scripthut/wf/logs")]
        refs = build_run_references(runs, "hpc", [CLONE_DIR], HOME)
        assert HASH in refs.clone_hashes
        assert refs.log_dirs == {f"{ABS_CLONE_DIR}/{HASH}/.scripthut/wf/logs": {"r1"}}

    def test_backend_scheme_log_dir_ignored(self):
        runs = [_run(log_dir="backend://hpc/wf")]
        refs = build_run_references(runs, "hpc", [CLONE_DIR], HOME)
        assert refs.log_dirs == {}

    def test_other_backend_ignored(self):
        runs = [_run(backend="other", working_dir=f"{CLONE_DIR}/{HASH}")]
        refs = build_run_references(runs, "hpc", [CLONE_DIR], HOME)
        assert refs.clone_hashes == {}

    def test_active_run_tracked(self):
        runs = [
            _run("r1", statuses=[RunItemStatus.RUNNING], working_dir=f"{CLONE_DIR}/{HASH}"),
            _run("r2", statuses=[RunItemStatus.COMPLETED], working_dir=f"{CLONE_DIR}/{HASH}"),
        ]
        refs = build_run_references(runs, "hpc", [CLONE_DIR], HOME)
        assert refs.active_run_ids == {"r1"}
        assert refs.clone_hashes == {HASH: {"r1", "r2"}}

    def test_working_dir_equal_to_clone_dir_is_not_a_ref(self):
        runs = [_run(working_dir=CLONE_DIR)]
        refs = build_run_references(runs, "hpc", [CLONE_DIR], HOME)
        assert refs.clone_hashes == {}
        assert refs.agent_dirs == {}


def _entry(kind: DiskEntryKind, path: str, **kw) -> DiskEntry:
    return DiskEntry(path=path, kind=kind, **kw)


class TestClassifyEntries:
    def _refs(self, runs):
        return build_run_references(runs, "hpc", [CLONE_DIR], HOME)

    def test_clone_active_referenced_orphaned(self):
        refs = self._refs(
            [
                _run("r1", statuses=[RunItemStatus.RUNNING], working_dir=f"{CLONE_DIR}/{HASH}"),
                _run("r2", working_dir=f"{CLONE_DIR}/ffffffffffff/x"),
            ]
        )
        entries = [
            _entry(DiskEntryKind.CLONE, f"{ABS_CLONE_DIR}/{HASH}"),
            _entry(DiskEntryKind.CLONE, f"{ABS_CLONE_DIR}/ffffffffffff"),
            _entry(DiskEntryKind.CLONE, f"{ABS_CLONE_DIR}/000000000000"),
        ]
        classify_entries(entries, refs)
        assert entries[0].classification == DiskEntryClass.ACTIVE
        assert entries[0].run_ids == ["r1"]
        assert entries[1].classification == DiskEntryClass.REFERENCED
        assert entries[2].classification == DiskEntryClass.ORPHANED

    def test_agent_classification(self):
        refs = self._refs([_run(working_dir=f"{CLONE_DIR}/agent-1a2b3c4d")])
        entries = [
            _entry(DiskEntryKind.AGENT, f"{ABS_CLONE_DIR}/agent-1a2b3c4d"),
            _entry(DiskEntryKind.AGENT, f"{ABS_CLONE_DIR}/agent-99999999"),
        ]
        classify_entries(entries, refs)
        assert entries[0].classification == DiskEntryClass.REFERENCED
        assert entries[1].classification == DiskEntryClass.ORPHANED

    def test_log_matched_by_equality_and_prefix(self):
        refs = self._refs([_run(log_dir="~/.cache/scripthut/logs/paper-sim")])
        entries = [
            _entry(DiskEntryKind.LOG, f"{HOME}/.cache/scripthut/logs/paper-sim"),
            _entry(DiskEntryKind.LOG, f"{HOME}/.cache/scripthut/logs/old-wf"),
        ]
        classify_entries(entries, refs)
        assert entries[0].classification == DiskEntryClass.REFERENCED
        assert entries[1].classification == DiskEntryClass.ORPHANED

    def test_other_always_unknown(self):
        refs = self._refs([])
        entries = [_entry(DiskEntryKind.OTHER, f"{ABS_CLONE_DIR}/my-checkout")]
        classify_entries(entries, refs)
        assert entries[0].classification == DiskEntryClass.UNKNOWN

    def test_stack_half_built_orphaned(self):
        entries = [
            _entry(DiskEntryKind.STACK, "/s/julia/ab12cd34ef56", ready=False, detail="julia/ab12cd34ef56 (half-built)"),
        ]
        classify_entries(entries, self._refs([]))
        assert entries[0].classification == DiskEntryClass.ORPHANED

    def test_stack_current_superseded_unconfigured(self):
        entries = [
            _entry(DiskEntryKind.STACK, "/s/julia/ab12cd34ef56", ready=True, detail="julia/ab12cd34ef56"),
            _entry(DiskEntryKind.STACK, "/s/julia/000000000000", ready=True, detail="julia/000000000000"),
            _entry(DiskEntryKind.STACK, "/s/gone/111111111111", ready=True, detail="gone/111111111111"),
        ]
        classify_entries(
            entries, self._refs([]), current_stack_hashes={"julia": {"ab12cd34ef56"}}
        )
        assert entries[0].classification == DiskEntryClass.REFERENCED
        assert "superseded" not in (entries[0].detail or "")
        assert entries[1].classification == DiskEntryClass.REFERENCED
        assert "(superseded)" in (entries[1].detail or "")
        assert entries[2].classification == DiskEntryClass.ORPHANED
        assert "(unconfigured)" in (entries[2].detail or "")


class TestRunSourceLabel:
    def test_source_workflow(self):
        assert run_source_label(_run(workflow_name="paper/train.json")) == "paper"

    def test_stack_install_with_source(self):
        assert run_source_label(_run(workflow_name="_stack/paper/julia")) == "paper"

    def test_synthetic_names_have_no_source(self):
        for wf in ("_adhoc/x", "_default", "_probe", "_stack/julia", "bare"):
            assert run_source_label(_run(workflow_name=wf)) is None


class TestCloneSource:
    def _refs(self, runs):
        return build_run_references(runs, "hpc", [CLONE_DIR], HOME)

    def test_clone_labelled_with_referencing_source(self):
        refs = self._refs([
            _run("r1", working_dir=f"{CLONE_DIR}/{HASH}", workflow_name="paper/train.json"),
        ])
        assert refs.clone_sources == {HASH: {"paper"}}
        entries = [_entry(DiskEntryKind.CLONE, f"{ABS_CLONE_DIR}/{HASH}")]
        classify_entries(entries, refs)
        assert entries[0].source == "paper"

    def test_clone_shared_by_two_sources_lists_both(self):
        refs = self._refs([
            _run("r1", commit_hash="a1b2c3d4e5f6789a", workflow_name="paper/a.json"),
            _run("r2", commit_hash="a1b2c3d4e5f6789a", workflow_name="thesis/b.json"),
        ])
        entries = [_entry(DiskEntryKind.CLONE, f"{ABS_CLONE_DIR}/{HASH}")]
        classify_entries(entries, refs)
        assert entries[0].source == "paper, thesis"

    def test_orphaned_clone_has_no_source(self):
        refs = self._refs([])
        entries = [_entry(DiskEntryKind.CLONE, f"{ABS_CLONE_DIR}/000000000000")]
        classify_entries(entries, refs)
        assert entries[0].source is None

    def test_agent_workspace_labelled_with_source(self):
        refs = self._refs([
            _run("r1", working_dir=f"{CLONE_DIR}/agent-1a2b3c4d", workflow_name="paper/run.json"),
        ])
        entries = [_entry(DiskEntryKind.AGENT, f"{ABS_CLONE_DIR}/agent-1a2b3c4d")]
        classify_entries(entries, refs)
        assert entries[0].source == "paper"

    def test_stack_multiple_valid_hashes_none_superseded(self):
        # server config and a source's project file both declare "julia"
        # with different inputs — both hashes are current, neither is
        # marked superseded by the other
        entries = [
            _entry(DiskEntryKind.STACK, "/s/julia/ab12cd34ef56", ready=True, detail="julia/ab12cd34ef56"),
            _entry(DiskEntryKind.STACK, "/s/julia/000000000000", ready=True, detail="julia/000000000000"),
        ]
        classify_entries(
            entries,
            self._refs([]),
            current_stack_hashes={"julia": {"ab12cd34ef56", "000000000000"}},
        )
        assert all(e.classification == DiskEntryClass.REFERENCED for e in entries)
        assert all("superseded" not in (e.detail or "") for e in entries)

    def test_stack_without_config_knowledge(self):
        entries = [_entry(DiskEntryKind.STACK, "/s/julia/ab12cd34ef56", ready=True)]
        classify_entries(entries, self._refs([]), current_stack_hashes=None)
        assert entries[0].classification == DiskEntryClass.REFERENCED
