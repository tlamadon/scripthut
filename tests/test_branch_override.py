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
from scripthut.sources.git import (
    GitSourceManager,
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




# ---------------------------------------------------------------------------
# API endpoint
# ---------------------------------------------------------------------------












# ---------------------------------------------------------------------------
# RunManager.create_run_from_source(branch=...)
# ---------------------------------------------------------------------------
