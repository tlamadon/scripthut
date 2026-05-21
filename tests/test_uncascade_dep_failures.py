"""Tests for Run.uncascade_dep_failures().

When the polling layer corrects a parent item that was wrongly marked FAILED
(e.g. an ultra-fast job that vanished from the queue but sacct later confirms
COMPLETED), any DEP_FAILED descendants must be reset to PENDING so the run
can resume — otherwise they stay terminal forever.
"""

from datetime import datetime, timezone

from scripthut.runs.models import (
    Run,
    RunItem,
    RunItemStatus,
    TaskDefinition,
)


def _item(
    task_id: str,
    deps: list[str] | None = None,
    status: RunItemStatus = RunItemStatus.PENDING,
    error: str | None = None,
) -> RunItem:
    return RunItem(
        task=TaskDefinition(
            id=task_id,
            name=task_id,
            command="echo " + task_id,
            dependencies=deps or [],
        ),
        status=status,
        error=error,
        finished_at=(
            datetime.now(timezone.utc)
            if status in (RunItemStatus.FAILED, RunItemStatus.DEP_FAILED)
            else None
        ),
    )


def _make_run(items: list[RunItem]) -> Run:
    return Run(
        id="r",
        workflow_name="wf",
        backend_name="b",
        created_at=datetime.now(timezone.utc),
        items=items,
        max_concurrent=None,
    )


class TestUncascadeDepFailures:
    def test_resets_direct_child_when_parent_completed(self):
        # parent went FAILED -> COMPLETED; child should come back to PENDING.
        parent = _item("a", status=RunItemStatus.COMPLETED)
        child = _item(
            "b", deps=["a"], status=RunItemStatus.DEP_FAILED, error="Dependency 'a' failed"
        )
        run = _make_run([parent, child])

        reset = run.uncascade_dep_failures()

        assert [r.task.id for r in reset] == ["b"]
        assert child.status == RunItemStatus.PENDING
        assert child.error is None
        assert child.finished_at is None
        # Parent untouched
        assert parent.status == RunItemStatus.COMPLETED

    def test_propagates_through_chain(self):
        # a -> b -> c. a was corrected; b and c are both DEP_FAILED. Both
        # should be reset (b first, then c since b is now satisfied).
        a = _item("a", status=RunItemStatus.COMPLETED)
        b = _item("b", deps=["a"], status=RunItemStatus.DEP_FAILED, error="x")
        c = _item("c", deps=["b"], status=RunItemStatus.DEP_FAILED, error="x")
        run = _make_run([a, b, c])

        reset = run.uncascade_dep_failures()

        assert {r.task.id for r in reset} == {"b", "c"}
        assert b.status == RunItemStatus.PENDING
        assert c.status == RunItemStatus.PENDING

    def test_diamond_resets_all_when_root_completed(self):
        # The exact shape from run 80daced1: setup -> build.x, build.y -> merge
        setup = _item("setup", status=RunItemStatus.COMPLETED)
        bx = _item("build.x", deps=["setup"], status=RunItemStatus.DEP_FAILED, error="x")
        by = _item("build.y", deps=["setup"], status=RunItemStatus.DEP_FAILED, error="x")
        merge = _item(
            "merge", deps=["build.x", "build.y"], status=RunItemStatus.DEP_FAILED, error="x",
        )
        run = _make_run([setup, bx, by, merge])

        reset = run.uncascade_dep_failures()

        assert {r.task.id for r in reset} == {"build.x", "build.y", "merge"}
        assert all(
            i.status == RunItemStatus.PENDING for i in (bx, by, merge)
        )

    def test_does_not_reset_when_another_dep_still_failed(self):
        # b depends on both a (corrected) and a2 (still FAILED). b should
        # remain DEP_FAILED — there's still a real upstream failure.
        a = _item("a", status=RunItemStatus.COMPLETED)
        a2 = _item("a2", status=RunItemStatus.FAILED, error="real failure")
        b = _item("b", deps=["a", "a2"], status=RunItemStatus.DEP_FAILED, error="x")
        run = _make_run([a, a2, b])

        reset = run.uncascade_dep_failures()

        assert reset == []
        assert b.status == RunItemStatus.DEP_FAILED
        assert b.error == "x"

    def test_idempotent_on_clean_run(self):
        # No DEP_FAILED items at all — nothing to do, no exceptions.
        a = _item("a", status=RunItemStatus.COMPLETED)
        b = _item("b", deps=["a"], status=RunItemStatus.PENDING)
        run = _make_run([a, b])

        reset = run.uncascade_dep_failures()

        assert reset == []
        assert b.status == RunItemStatus.PENDING

    def test_leaves_non_dep_failed_items_alone(self):
        # b is FAILED for its own reasons (not DEP_FAILED); uncascade
        # must not touch it.
        a = _item("a", status=RunItemStatus.COMPLETED)
        b = _item("b", deps=["a"], status=RunItemStatus.FAILED, error="real reason")
        run = _make_run([a, b])

        reset = run.uncascade_dep_failures()

        assert reset == []
        assert b.status == RunItemStatus.FAILED
        assert b.error == "real reason"
