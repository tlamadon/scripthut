"""Tests for cleanup planning, safety checks, and deletion scripts."""

from __future__ import annotations

from datetime import UTC, datetime

from scripthut.disk.classify import RunReferences, build_run_references
from scripthut.disk.cleanup import (
    build_agent_check_script,
    build_delete_script,
    parse_agent_check_output,
    parse_delete_output,
    plan_cleanup,
    CleanupOutcome,
    CleanupReport,
)
from scripthut.disk.models import (
    DiskEntry,
    DiskEntryClass,
    DiskEntryKind,
    DiskScanResult,
    ScanSpec,
)
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition

HOME = "/home/alice"
CLONE_DIR = f"{HOME}/scripthut-repos"
STACK_DIR = f"{HOME}/.cache/scripthut/stacks"
LOG_ROOT = f"{HOME}/.cache/scripthut/logs"
NOW = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)

SPEC = ScanSpec(
    backend="hpc",
    clone_dirs=["~/scripthut-repos"],
    stack_dirs=["~/.cache/scripthut/stacks"],
    log_roots=["~/.cache/scripthut/logs"],
)


def _entry(kind: DiskEntryKind, path: str, **kw) -> DiskEntry:
    kw.setdefault("size_bytes", 1024)
    return DiskEntry(path=path, kind=kind, **kw)


def _result(entries: list[DiskEntry]) -> DiskScanResult:
    return DiskScanResult(
        backend="hpc", scanned_at=NOW, duration_ms=1, home_dir=HOME, entries=entries,
    )


def _run(
    run_id: str = "r1",
    working_dir: str = "~",
    statuses: list[RunItemStatus] | None = None,
    log_dir: str = "",
) -> Run:
    statuses = statuses or [RunItemStatus.COMPLETED]
    return Run(
        id=run_id,
        workflow_name="wf",
        backend_name="hpc",
        created_at=NOW,
        items=[
            RunItem(
                task=TaskDefinition(id=f"t{i}", name=f"t{i}", command="true", working_dir=working_dir),
                status=s,
            )
            for i, s in enumerate(statuses)
        ],
        max_concurrent=None,
        log_dir=log_dir,
    )


def _refs(runs: list[Run]) -> RunReferences:
    return build_run_references(runs, "hpc", SPEC.clone_dirs, HOME)


def _plan(result, refs, **kw):
    return plan_cleanup(
        result, refs, spec=SPEC, current_stack_hashes=None, planned_at=NOW, **kw
    )


class TestBulkSelection:
    def test_selects_only_orphaned_deletable(self):
        result = _result(
            [
                _entry(DiskEntryKind.CLONE, f"{CLONE_DIR}/aaaaaaaaaaaa"),
                _entry(DiskEntryKind.AGENT, f"{CLONE_DIR}/agent-11111111"),
                _entry(DiskEntryKind.OTHER, f"{CLONE_DIR}/my-checkout"),
                _entry(DiskEntryKind.LOG, f"{LOG_ROOT}/old-wf"),
            ]
        )
        plan = _plan(result, _refs([]))
        actions = {e.entry.path: e.action for e in plan.entries}
        # OTHER never appears in a bulk plan (not even as a skip)
        assert f"{CLONE_DIR}/my-checkout" not in actions
        assert actions[f"{CLONE_DIR}/aaaaaaaaaaaa"] == "delete"
        assert actions[f"{CLONE_DIR}/agent-11111111"] == "check_then_delete"
        assert actions[f"{LOG_ROOT}/old-wf"] == "delete"
        assert plan.delete_bytes == 3 * 1024
        assert plan.counts == {"delete": 2, "check": 1, "skip": 0}

    def test_referenced_and_active_excluded_from_bulk(self):
        result = _result(
            [
                _entry(DiskEntryKind.CLONE, f"{CLONE_DIR}/aaaaaaaaaaaa"),
                _entry(DiskEntryKind.CLONE, f"{CLONE_DIR}/bbbbbbbbbbbb"),
            ]
        )
        refs = _refs(
            [
                _run("done", working_dir=f"{CLONE_DIR}/aaaaaaaaaaaa"),
                _run("live", working_dir=f"{CLONE_DIR}/bbbbbbbbbbbb",
                     statuses=[RunItemStatus.RUNNING]),
            ]
        )
        plan = _plan(result, refs)
        assert plan.entries == []

    def test_exec_time_flip_orphaned_to_active_skipped(self):
        # cached scan says orphaned; current refs say a running run uses it
        result = _result(
            [
                _entry(
                    DiskEntryKind.CLONE,
                    f"{CLONE_DIR}/aaaaaaaaaaaa",
                    classification=DiskEntryClass.ORPHANED,
                )
            ]
        )
        refs = _refs(
            [_run("live", working_dir=f"{CLONE_DIR}/aaaaaaaaaaaa",
                  statuses=[RunItemStatus.RUNNING])]
        )
        plan = _plan(result, refs, paths=[f"{CLONE_DIR}/aaaaaaaaaaaa"])
        assert plan.entries[0].action == "skip"
        assert "active run(s) live" in (plan.entries[0].reason or "")


class TestExplicitPaths:
    def test_unknown_path_rejects_request(self):
        result = _result([_entry(DiskEntryKind.CLONE, f"{CLONE_DIR}/aaaaaaaaaaaa")])
        plan = _plan(result, _refs([]), paths=["/nope/never/scanned"])
        assert plan.errors and "not in the last scan" in plan.errors[0]
        assert plan.entries == []

    def test_tilde_path_normalized_against_home(self):
        result = _result([_entry(DiskEntryKind.CLONE, f"{CLONE_DIR}/aaaaaaaaaaaa")])
        plan = _plan(result, _refs([]), paths=["~/scripthut-repos/aaaaaaaaaaaa/"])
        assert not plan.errors
        assert plan.entries[0].action == "delete"

    def test_referenced_needs_allow(self):
        result = _result([_entry(DiskEntryKind.CLONE, f"{CLONE_DIR}/aaaaaaaaaaaa")])
        refs = _refs([_run("old", working_dir=f"{CLONE_DIR}/aaaaaaaaaaaa")])
        path = f"{CLONE_DIR}/aaaaaaaaaaaa"

        plan = _plan(result, refs, paths=[path])
        assert plan.entries[0].action == "skip"
        assert "run(s) old" in (plan.entries[0].reason or "")

        plan = _plan(result, refs, paths=[path], allow_referenced=frozenset({path}))
        assert plan.entries[0].action == "delete"

    def test_other_kind_skipped_even_when_named(self):
        result = _result([_entry(DiskEntryKind.OTHER, f"{CLONE_DIR}/my-checkout")])
        plan = _plan(result, _refs([]), paths=[f"{CLONE_DIR}/my-checkout"])
        assert plan.entries[0].action == "skip"
        assert "never deletable" in (plan.entries[0].reason or "")


class TestStacks:
    def _stack_result(self):
        return _result(
            [
                _entry(DiskEntryKind.STACK, f"{STACK_DIR}/julia/aaaaaaaaaaaa",
                       ready=True, detail="julia/aaaaaaaaaaaa"),
                _entry(DiskEntryKind.STACK, f"{STACK_DIR}/julia/bbbbbbbbbbbb",
                       ready=True, detail="julia/bbbbbbbbbbbb (superseded)"),
                _entry(DiskEntryKind.STACK, f"{STACK_DIR}/julia/cccccccccccc",
                       ready=False, detail="julia/cccccccccccc (half-built)"),
                _entry(DiskEntryKind.STACK, f"{STACK_DIR}/gone/dddddddddddd",
                       ready=True, detail="gone/dddddddddddd (unconfigured)"),
            ]
        )

    def test_stack_selection_and_no_double_annotation(self):
        plan = plan_cleanup(
            self._stack_result(),
            _refs([]),
            spec=SPEC,
            current_stack_hashes={"julia": "aaaaaaaaaaaa"},
            planned_at=NOW,
        )
        by_path = {e.entry.path: e for e in plan.entries}
        # half-built + unconfigured are orphaned -> bulk-selected
        assert by_path[f"{STACK_DIR}/julia/cccccccccccc"].action == "delete"
        unconf = by_path[f"{STACK_DIR}/gone/dddddddddddd"]
        assert unconf.action == "delete"
        assert (unconf.entry.detail or "").count("(unconfigured)") == 1
        # current + superseded are referenced -> not in bulk plan
        assert f"{STACK_DIR}/julia/aaaaaaaaaaaa" not in by_path
        assert f"{STACK_DIR}/julia/bbbbbbbbbbbb" not in by_path

    def test_superseded_deletable_when_named(self):
        path = f"{STACK_DIR}/julia/bbbbbbbbbbbb"
        plan = plan_cleanup(
            self._stack_result(),
            _refs([]),
            spec=SPEC,
            current_stack_hashes={"julia": "aaaaaaaaaaaa"},
            planned_at=NOW,
            paths=[path],
            allow_referenced=frozenset({path}),
        )
        e = plan.entries[0]
        assert e.action == "delete"
        assert (e.entry.detail or "").count("(superseded)") == 1


class TestSafety:
    def _named_plan(self, entry, refs=None):
        result = _result([entry])
        return _plan(result, refs or _refs([]), paths=[entry.path])

    def test_outside_roots(self):
        e = _entry(DiskEntryKind.CLONE, "/etc/somewhere/aaaaaaaaaaaa")
        p = self._named_plan(e)
        assert p.entries[0].action == "skip"
        assert "not under any scanned root" in (p.entries[0].reason or "")

    def test_too_shallow(self):
        e = _entry(DiskEntryKind.LOG, "/tmp/x")
        p = self._named_plan(e)
        assert p.entries[0].action == "skip"
        assert "too shallow" in (p.entries[0].reason or "")

    def test_dotdot_component(self):
        e = _entry(DiskEntryKind.CLONE, f"{CLONE_DIR}/../aaaaaaaaaaaa")
        p = self._named_plan(e)
        assert p.entries[0].action == "skip"

    def test_wrong_depth_under_root(self):
        e = _entry(DiskEntryKind.CLONE, f"{CLONE_DIR}/aaaaaaaaaaaa/subdir")
        p = self._named_plan(e)
        assert p.entries[0].action == "skip"
        assert "unexpected depth" in (p.entries[0].reason or "")

    def test_stack_needs_two_levels(self):
        e = _entry(DiskEntryKind.STACK, f"{STACK_DIR}/loosefile")
        p = self._named_plan(e)
        assert p.entries[0].action == "skip"

    def test_home_unknown_tilde_roots_skip(self):
        result = DiskScanResult(
            backend="hpc", scanned_at=NOW, duration_ms=1, home_dir=None,
            entries=[_entry(DiskEntryKind.CLONE, f"{CLONE_DIR}/aaaaaaaaaaaa")],
        )
        plan = plan_cleanup(
            result, RunReferences(), spec=SPEC, current_stack_hashes=None,
            planned_at=NOW, paths=[f"{CLONE_DIR}/aaaaaaaaaaaa"],
        )
        assert plan.entries[0].action == "skip"
        assert "remote home unknown" in (plan.entries[0].reason or "")


class TestWarnings:
    def test_clone_with_known_run_logs(self):
        path = f"{CLONE_DIR}/aaaaaaaaaaaa"
        result = _result([_entry(DiskEntryKind.CLONE, path)])
        refs = _refs(
            [_run("old", log_dir=f"{path}/.scripthut/wf/logs")]
        )
        plan = _plan(result, refs, paths=[path], allow_referenced=frozenset({path}))
        assert any("logs of run(s) old" in w for w in plan.entries[0].warnings)

    def test_orphaned_clone_generic_warning(self):
        result = _result([_entry(DiskEntryKind.CLONE, f"{CLONE_DIR}/aaaaaaaaaaaa")])
        plan = _plan(result, _refs([]))
        assert any("forgotten runs" in w for w in plan.entries[0].warnings)

    def test_agent_warning(self):
        result = _result([_entry(DiskEntryKind.AGENT, f"{CLONE_DIR}/agent-11111111")])
        plan = _plan(result, _refs([]))
        assert any("uncommitted" in w for w in plan.entries[0].warnings)


class TestScripts:
    def test_agent_check_script_quotes_spaces(self):
        script = build_agent_check_script(["/home/al ice/repos/agent-11111111"])
        assert "'/home/al ice/repos/agent-11111111'" in script
        assert script.startswith("bash -s <<'__SCRIPTHUT_AGENTCHECK__'")
        assert "git -C" in script and "status --porcelain" in script

    def test_parse_agent_check_output(self):
        out = (
            "CHECK\t/r/agent-1\tclean\t-\n"
            "CHECK\t/r/agent-2\tdirty\t-\n"
            "CHECK\t/r/agent-3\tunpushed\tab12 wip commit\n"
            "CHECK\t/r/agent-4\tnogit\t-\n"
            "garbage line\n"
        )
        parsed = parse_agent_check_output(out)
        assert parsed["/r/agent-1"] == ("clean", "-")
        assert parsed["/r/agent-2"][0] == "dirty"
        assert parsed["/r/agent-3"] == ("unpushed", "ab12 wip commit")
        assert len(parsed) == 4

    def test_delete_script_and_parse(self):
        script = build_delete_script(["/r/aaaaaaaaaaaa", "/r/with space"])
        assert "rm -rf --" in script
        assert "'/r/with space'" in script
        parsed = parse_delete_output(
            "DEL\t/r/aaaaaaaaaaaa\tOK\t-\n"
            "DEL\t/r/with space\tFAIL\trm: cannot remove\n"
            "noise\n"
        )
        assert parsed["/r/aaaaaaaaaaaa"] == (True, "-")
        assert parsed["/r/with space"] == (False, "rm: cannot remove")


class TestReport:
    def test_freed_bytes_and_lower_bound(self):
        report = CleanupReport(
            backend="hpc",
            started_at=NOW,
            outcomes=[
                CleanupOutcome("/a", DiskEntryKind.CLONE, 1000, "deleted"),
                CleanupOutcome("/b", DiskEntryKind.CLONE, None, "deleted"),
                CleanupOutcome("/c", DiskEntryKind.AGENT, 500, "skipped", "dirty"),
            ],
        )
        assert report.freed_bytes == 1000
        assert report.freed_is_lower_bound is True
        assert report.counts == {"deleted": 2, "skipped": 1, "failed": 0}
        d = report.to_dict()
        assert d["freed_bytes"] == 1000 and d["counts"]["deleted"] == 2
