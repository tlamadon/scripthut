"""Tests for running a source workflow from a non-default branch.

Covers the three layers of the branch-override path:

1. ``GitSourceManager.fetch_branch`` / ``discover_workflows_at`` — the
   server-side cache can serve any branch on demand from its
   ``--single-branch`` clone (integration tests against a real local
   git repo).
2. ``POST /api/v1/sources/{name}/run?branch=...`` — validation,
   branch-scoped discovery, and threading into the run manager.
3. ``RunManager.create_run_from_source(branch=...)`` — the source's
   configured branch is overridden for the backend clone and the run's
   recorded ``git_branch``.
"""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripthut.api import make_api_router
from scripthut.config_schema import (
    GitSourceConfig,
    GlobalSettings,
    PathSourceConfig,
    ScriptHutConfig,
)
from scripthut.runs.manager import RunManager
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition
from scripthut.runs.storage import RunStorageManager
from scripthut.sources.git import (
    GitSourceManager,
    SourceWorkflow,
    _match_workflows_glob,
    is_safe_branch_name,
)

# ---------------------------------------------------------------------------
# Branch-name validation
# ---------------------------------------------------------------------------


class TestIsSafeBranchName:
    def test_accepts_common_branch_names(self):
        for name in ("main", "feature/foo-bar", "v1.2", "user/x+y", "a"):
            assert is_safe_branch_name(name), name

    def test_rejects_shell_and_option_injection(self):
        for name in (
            "", "-rf", "a b", "a;b", "a&&b", "$(x)", "`x`", "a\nb",
            "a..b", "a'b", 'a"b', "~x", "a" * 201,
        ):
            assert not is_safe_branch_name(name), name


# ---------------------------------------------------------------------------
# Workflows-glob matching (tree paths vs Path.glob semantics)
# ---------------------------------------------------------------------------


class TestMatchWorkflowsGlob:
    def test_default_pattern_is_anchored(self):
        pat = ".hut/workflows/*.json"
        assert _match_workflows_glob(".hut/workflows/train.json", pat)
        assert not _match_workflows_glob("sub/.hut/workflows/train.json", pat)
        assert not _match_workflows_glob(".hut/workflows/sub/train.json", pat)
        assert not _match_workflows_glob(".hut/workflows/train.yaml", pat)

    def test_double_star_crosses_directories(self):
        pat = "**/*.hut.json"
        assert _match_workflows_glob("a/b/train.hut.json", pat)
        assert _match_workflows_glob("train.hut.json", pat)
        assert not _match_workflows_glob("a/b/train.json", pat)


# ---------------------------------------------------------------------------
# GitSourceManager against a real local repo
# ---------------------------------------------------------------------------


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(
        ["git", "-c", "user.email=t@e.st", "-c", "user.name=t", *args],
        cwd=cwd, check=True, capture_output=True,
    )


@pytest.fixture
def origin(tmp_path: Path) -> Path:
    """A local origin repo: ``main`` has train.json; ``feature`` edits it
    and adds extra.json."""
    repo = tmp_path / "origin"
    wf_dir = repo / ".hut" / "workflows"
    wf_dir.mkdir(parents=True)
    (wf_dir / "train.json").write_text(json.dumps(
        {"title": "main-train", "tasks": []},
    ))
    _git("init", "-b", "main", cwd=repo)
    _git("add", "-A", cwd=repo)
    _git("commit", "-m", "main", cwd=repo)
    _git("checkout", "-b", "feature", cwd=repo)
    (wf_dir / "train.json").write_text(json.dumps(
        {"title": "feature-train", "tasks": []},
    ))
    (wf_dir / "extra.json").write_text(json.dumps(
        {"title": "feature-extra", "tasks": []},
    ))
    _git("add", "-A", cwd=repo)
    _git("commit", "-m", "feature", cwd=repo)
    _git("checkout", "main", cwd=repo)
    return repo


@pytest.fixture
async def manager(origin: Path, tmp_path: Path) -> GitSourceManager:
    mgr = GitSourceManager(cache_dir=tmp_path / "cache")
    mgr.add_source(GitSourceConfig(name="src", url=str(origin), branch="main"))
    status = await mgr.clone_source("src")
    assert status.cloned, status.error
    return mgr


class TestFetchBranch:
    async def test_returns_branch_tip_commit(self, manager: GitSourceManager, origin: Path):
        commit = await manager.fetch_branch("src", "feature")
        expected = subprocess.run(
            ["git", "rev-parse", "feature"],
            cwd=origin, check=True, capture_output=True, text=True,
        ).stdout.strip()
        assert commit == expected

    async def test_unknown_branch_raises(self, manager: GitSourceManager):
        with pytest.raises(ValueError, match="nope"):
            await manager.fetch_branch("src", "nope")

    async def test_unsafe_branch_name_raises(self, manager: GitSourceManager):
        with pytest.raises(ValueError, match="Invalid branch name"):
            await manager.fetch_branch("src", "a;rm -rf /")

    async def test_clones_on_demand_when_cache_missing(
        self, origin: Path, tmp_path: Path,
    ):
        mgr = GitSourceManager(cache_dir=tmp_path / "cache2")
        mgr.add_source(
            GitSourceConfig(name="src", url=str(origin), branch="main"),
        )
        commit = await mgr.fetch_branch("src", "feature")
        assert len(commit) == 40


class TestDiscoverWorkflowsAt:
    async def test_reads_files_at_branch_tip(self, manager: GitSourceManager):
        commit = await manager.fetch_branch("src", "feature")
        wfs = await manager.discover_workflows_at("src", commit)
        by_file = {w.filename: w for w in wfs}
        assert set(by_file) == {"train.json", "extra.json"}
        assert by_file["train.json"].title == "feature-train"
        assert by_file["extra.json"].source_name == "src"

    async def test_default_branch_discovery_untouched(
        self, manager: GitSourceManager,
    ):
        commit = await manager.fetch_branch("src", "feature")
        await manager.discover_workflows_at("src", commit)
        # The working-tree discovery still reflects the configured branch.
        wfs = manager.discover_workflows("src")
        assert [w.filename for w in wfs] == ["train.json"]
        assert wfs[0].title == "main-train"

    async def test_bad_ref_raises(self, manager: GitSourceManager):
        with pytest.raises(ValueError, match="list tree"):
            await manager.discover_workflows_at("src", "0" * 40)


# ---------------------------------------------------------------------------
# API endpoint
# ---------------------------------------------------------------------------


def _make_run(workflow_name: str = "src/train") -> Run:
    return Run(
        id="run123",
        workflow_name=workflow_name,
        backend_name="cluster",
        created_at=datetime(2026, 7, 17, 12, 0, 0, tzinfo=UTC),
        items=[RunItem(
            task=TaskDefinition(id="t", name="t", command="true"),
            status=RunItemStatus.SUBMITTED,
        )],
        max_concurrent=1,
    )


def _make_state(sources: list, source_manager=None, source_workflows=None):
    state = MagicMock()
    rm = MagicMock()
    rm.create_run_from_source = AsyncMock(return_value=_make_run())
    state.run_manager = rm
    state.config_error = None
    state.backends = {}
    state.source_manager = source_manager
    state.source_workflows = source_workflows or {}
    state.config = MagicMock()
    state.config.sources = sources
    state.config.get_source = lambda name: next(
        (s for s in sources if s.name == name), None,
    )
    state.notify_poll = MagicMock()
    return state


def _client(state) -> TestClient:
    app = FastAPI()
    app.include_router(make_api_router(state))
    return TestClient(app)


def _branch_source_manager(workflows: list[SourceWorkflow]):
    sm = MagicMock()
    sm._sources = {"src": MagicMock()}
    sm.fetch_branch = AsyncMock(return_value="a" * 40)
    sm.discover_workflows_at = AsyncMock(return_value=workflows)
    return sm


class TestRunEndpointBranchOverride:
    def _git_source(self) -> GitSourceConfig:
        return GitSourceConfig(name="src", url="git@h:o/r.git", branch="main")

    def test_branch_threads_through_to_run_manager(self):
        wf = SourceWorkflow(
            name="src/train", source_name="src", filename="train.json",
            tasks_json='{"tasks": []}',
        )
        sm = _branch_source_manager([wf])
        state = _make_state([self._git_source()], source_manager=sm)

        resp = _client(state).post(
            "/api/v1/sources/src/run"
            "?workflow=train.json&backend=cluster&branch=feature"
        )

        assert resp.status_code == 200
        sm.fetch_branch.assert_awaited_once_with("src", "feature")
        sm.discover_workflows_at.assert_awaited_once_with("src", "a" * 40)
        state.run_manager.create_run_from_source.assert_awaited_once_with(
            "src", "train.json", '{"tasks": []}', backend="cluster",
            branch="feature",
        )

    def test_branch_equal_to_configured_uses_cached_path(self):
        wf = SourceWorkflow(
            name="src/train", source_name="src", filename="train.json",
            tasks_json="[]",
        )
        sm = _branch_source_manager([])
        sm.sync_source = AsyncMock()
        sm.discover_workflows = MagicMock(return_value=[wf])
        state = _make_state(
            [self._git_source()], source_manager=sm,
            source_workflows={"src": [wf]},
        )

        resp = _client(state).post(
            "/api/v1/sources/src/run"
            "?workflow=train.json&backend=cluster&branch=main"
        )

        assert resp.status_code == 200
        sm.fetch_branch.assert_not_awaited()
        state.run_manager.create_run_from_source.assert_awaited_once_with(
            "src", "train.json", "[]", backend="cluster", branch=None,
        )

    def test_branch_on_path_source_is_422(self):
        src = PathSourceConfig(name="src", backend="cluster", path="/r/s")
        state = _make_state([src])

        resp = _client(state).post(
            "/api/v1/sources/src/run?workflow=train.json&branch=feature"
        )

        assert resp.status_code == 422
        assert "git source" in resp.json()["detail"]

    def test_invalid_branch_name_is_422(self):
        state = _make_state(
            [self._git_source()], source_manager=_branch_source_manager([]),
        )

        resp = _client(state).post(
            "/api/v1/sources/src/run"
            "?workflow=train.json&backend=cluster&branch=a%3Bb"
        )

        assert resp.status_code == 422
        assert "Invalid branch name" in resp.json()["detail"]

    def test_unknown_branch_is_422_with_git_error(self):
        sm = _branch_source_manager([])
        sm.fetch_branch = AsyncMock(
            side_effect=ValueError("Failed to fetch branch 'nope'"),
        )
        state = _make_state([self._git_source()], source_manager=sm)

        resp = _client(state).post(
            "/api/v1/sources/src/run"
            "?workflow=train.json&backend=cluster&branch=nope"
        )

        assert resp.status_code == 422
        assert "Failed to fetch branch" in resp.json()["detail"]

    def test_workflow_missing_on_branch_is_404(self):
        sm = _branch_source_manager([])
        state = _make_state([self._git_source()], source_manager=sm)

        resp = _client(state).post(
            "/api/v1/sources/src/run"
            "?workflow=train.json&backend=cluster&branch=feature"
        )

        assert resp.status_code == 404
        assert "on branch 'feature'" in resp.json()["detail"]

    def test_no_source_manager_is_503(self):
        state = _make_state([self._git_source()], source_manager=None)

        resp = _client(state).post(
            "/api/v1/sources/src/run"
            "?workflow=train.json&backend=cluster&branch=feature"
        )

        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# RunManager.create_run_from_source(branch=...)
# ---------------------------------------------------------------------------


def _run_manager(source, tmp_path: Path) -> RunManager:
    config = ScriptHutConfig(
        sources=[source],
        settings=GlobalSettings(sources_cache_dir=tmp_path / "cache"),
    )
    return RunManager(
        config=config,
        backends={},
        storage=RunStorageManager(tmp_path / "runs"),
        job_backends={},
    )


class TestCreateRunFromSourceBranch:
    async def test_branch_override_flows_to_clone_and_run(
        self, tmp_path: Path,
    ):
        src = GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main")
        mgr = _run_manager(src, tmp_path)
        captured: dict = {}

        async def fake_build_run(
            tasks, workflow_name, backend_name, max_concurrent, ssh_client,
            *, git_repo=None, git_branch=None, commit_hash=None,
            doc_env=None, doc_env_groups=None, doc_stacks=None,
        ):
            captured["git_branch"] = git_branch
            return _make_run(workflow_name)

        ls_remote = AsyncMock(return_value="deadbeef0000")
        with patch.object(mgr, "_build_run", side_effect=fake_build_run), \
             patch.object(mgr, "_load_source_project_config",
                          AsyncMock(return_value=None)), \
             patch.object(mgr, "_ls_remote_commit", ls_remote), \
             patch.object(mgr, "get_job_backend", return_value=MagicMock()):
            await mgr.create_run_from_source(
                "demo", "wf.json", '{"tasks": []}', "test-batch",
                branch="feature",
            )

        ls_remote.assert_awaited_once_with("git@h:o/r.git", "feature")
        assert captured["git_branch"] == "feature"

    async def test_branch_on_path_source_raises(self, tmp_path: Path):
        src = PathSourceConfig(name="demo", backend="b", path="/x")
        mgr = _run_manager(src, tmp_path)
        with pytest.raises(ValueError, match="git source"):
            await mgr.create_run_from_source(
                "demo", "wf.json", '{"tasks": []}', "b", branch="feature",
            )

    async def test_unsafe_branch_raises(self, tmp_path: Path):
        src = GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main")
        mgr = _run_manager(src, tmp_path)
        with pytest.raises(ValueError, match="Invalid branch name"):
            await mgr.create_run_from_source(
                "demo", "wf.json", '{"tasks": []}', "b", branch="a;b",
            )

    async def test_branch_equal_to_configured_is_noop(self, tmp_path: Path):
        """Same-branch override must not copy the source or change flow."""
        src = GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main")
        mgr = _run_manager(src, tmp_path)
        captured: dict = {}

        async def fake_build_run(
            tasks, workflow_name, backend_name, max_concurrent, ssh_client,
            *, git_repo=None, git_branch=None, commit_hash=None,
            doc_env=None, doc_env_groups=None, doc_stacks=None,
        ):
            captured["git_branch"] = git_branch
            return _make_run(workflow_name)

        with patch.object(mgr, "_build_run", side_effect=fake_build_run), \
             patch.object(mgr, "_load_source_project_config",
                          AsyncMock(return_value=None)), \
             patch.object(mgr, "_ls_remote_commit",
                          AsyncMock(return_value="deadbeef0000")), \
             patch.object(mgr, "get_job_backend", return_value=MagicMock()):
            await mgr.create_run_from_source(
                "demo", "wf.json", '{"tasks": []}', "test-batch",
                branch="main",
            )

        assert captured["git_branch"] == "main"
