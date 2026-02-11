"""Tests for wildcard dependency resolution."""

import pytest

from scripthut.runs.manager import RunManager
from scripthut.runs.models import TaskDefinition


def _make_task(id: str, deps: list[str] | None = None) -> TaskDefinition:
    return TaskDefinition(id=id, name=id, command="echo hi", dependencies=deps or [])


class TestResolveWildcardDeps:
    def test_no_wildcards_unchanged(self):
        tasks = [_make_task("a"), _make_task("b", ["a"])]
        RunManager._resolve_wildcard_deps(tasks)
        assert tasks[1].dependencies == ["a"]

    def test_star_wildcard_expands(self):
        tasks = [
            _make_task("build.x"),
            _make_task("build.y"),
            _make_task("deploy", ["build.*"]),
        ]
        RunManager._resolve_wildcard_deps(tasks)
        assert sorted(tasks[2].dependencies) == ["build.x", "build.y"]

    def test_wildcard_excludes_self(self):
        tasks = [
            _make_task("build.a"),
            _make_task("build.b", ["build.*"]),
        ]
        RunManager._resolve_wildcard_deps(tasks)
        assert tasks[1].dependencies == ["build.a"]

    def test_wildcard_no_match_raises(self):
        tasks = [
            _make_task("a"),
            _make_task("b", ["nope.*"]),
        ]
        with pytest.raises(ValueError, match="matches no tasks"):
            RunManager._resolve_wildcard_deps(tasks)

    def test_mixed_literal_and_wildcard(self):
        tasks = [
            _make_task("setup"),
            _make_task("build.x"),
            _make_task("build.y"),
            _make_task("deploy", ["setup", "build.*"]),
        ]
        RunManager._resolve_wildcard_deps(tasks)
        assert tasks[3].dependencies == ["setup", "build.x", "build.y"]

    def test_question_mark_wildcard(self):
        tasks = [
            _make_task("step.1"),
            _make_task("step.2"),
            _make_task("step.10"),
            _make_task("final", ["step.?"]),
        ]
        RunManager._resolve_wildcard_deps(tasks)
        assert sorted(tasks[3].dependencies) == ["step.1", "step.2"]

    def test_diamond_pattern_with_wildcards(self):
        tasks = [
            _make_task("setup.init"),
            _make_task("build.x", ["setup.*"]),
            _make_task("build.y", ["setup.*"]),
            _make_task("final.merge", ["build.*"]),
        ]
        RunManager._resolve_wildcard_deps(tasks)
        assert tasks[1].dependencies == ["setup.init"]
        assert tasks[2].dependencies == ["setup.init"]
        assert sorted(tasks[3].dependencies) == ["build.x", "build.y"]

        # Should also pass validation (no cycles)
        RunManager._validate_dependencies(tasks)

    def test_wildcard_then_validation_catches_cycle(self):
        tasks = [
            _make_task("a.1", ["b.*"]),
            _make_task("b.1", ["a.*"]),
        ]
        RunManager._resolve_wildcard_deps(tasks)
        with pytest.raises(ValueError, match="Circular dependency"):
            RunManager._validate_dependencies(tasks)
