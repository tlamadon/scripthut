"""Tests for the coding-agent launcher (``RunManager.create_agent_run``).

Covers the two interaction modes (``remote`` / ``tui``), the guards that
restrict agents to git sources on SSH backends, the new ``Run`` agent fields
round-tripping through storage, and the session-URL regex used by the
``/runs/{id}/agent-link`` endpoint.

The clone + submit boundaries are mocked so the tests run hermetically:
``_clone_agent_workspace`` and ``_get_git_root`` (SSH) and ``process_run``
(scheduler submission) are patched.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripthut.config_schema import (
    GitSourceConfig,
    GlobalSettings,
    PathSourceConfig,
    ScriptHutConfig,
)
from scripthut.runs.manager import RunManager
from scripthut.runs.models import Run, RunItem, TaskDefinition
from scripthut.runs.storage import RunStorageManager


def _config(source, *, cache_dir: Path) -> ScriptHutConfig:
    return ScriptHutConfig(
        sources=[source],
        settings=GlobalSettings(sources_cache_dir=cache_dir),
    )


def _manager(config: ScriptHutConfig, *, tmp_path: Path, with_ssh: bool = True) -> RunManager:
    backends = {"slurm": MagicMock()} if with_ssh else {}
    return RunManager(
        config=config,
        backends=backends,
        storage=RunStorageManager(tmp_path / "runs"),
        job_backends={},
    )


def _git_source() -> GitSourceConfig:
    return GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main")


def _patched(mgr: RunManager):
    """Patch the SSH/scheduler boundaries create_agent_run crosses."""
    workspace = "/repos/agent-abcd1234"
    return (
        patch.object(
            RunManager, "_clone_agent_workspace",
            new=AsyncMock(return_value=(workspace, "deadbeefcafe")),
        ),
        patch.object(RunManager, "_get_git_root", new=AsyncMock(return_value=workspace)),
        patch.object(RunManager, "process_run", new=AsyncMock()),
        workspace,
    )


class TestCreateAgentRun:
    @pytest.mark.asyncio
    async def test_remote_mode_builds_single_task(self, tmp_path: Path):
        mgr = _manager(_config(_git_source(), cache_dir=tmp_path / "cache"), tmp_path=tmp_path)
        p_clone, p_root, p_proc, workspace = _patched(mgr)
        with p_clone, p_root, p_proc:
            run = await mgr.create_agent_run(
                "demo", "slurm", mode="remote", session_name="mysession",
            )

        assert run.agent_session is True
        assert run.agent_mode == "remote"
        assert run.agent_session_name == "mysession"
        assert len(run.items) == 1
        task = run.items[0].task
        assert "claude remote-control" in task.command
        assert "mysession" in task.command
        # Fails fast with a clear message if claude isn't on the node.
        assert "command -v claude" in task.command
        assert "not found" in task.command
        assert task.working_dir == workspace
        assert run.git_repo == "git@h:o/r.git"
        assert run.git_branch == "main"
        assert run.commit_hash == "deadbeefcafe"

    @pytest.mark.asyncio
    async def test_tui_mode_wraps_in_tmux(self, tmp_path: Path):
        mgr = _manager(_config(_git_source(), cache_dir=tmp_path / "cache"), tmp_path=tmp_path)
        p_clone, p_root, p_proc, _ = _patched(mgr)
        with p_clone, p_root, p_proc:
            run = await mgr.create_agent_run("demo", "slurm", mode="tui", session_name="t1")

        assert run.agent_mode == "tui"
        cmd = run.items[0].task.command
        # tmux session named sh-<jobid> so the browser terminal's
        # session_type=job attach (tmux attach -t sh-<jobid>) works.
        assert "tmux new-session" in cmd
        assert "sh-${SLURM_JOB_ID" in cmd
        assert "tmux has-session" in cmd  # keep-alive loop
        # If the program exits (e.g. claude not installed) fall back to a shell
        # so the session/allocation survives and the attach isn't rejected.
        assert "exec bash -l" in cmd
        # Clear, actionable message when the binary is missing (exit 127).
        assert "not found" in cmd
        assert '"$rc" = 127' in cmd

    @pytest.mark.asyncio
    async def test_default_session_name_when_omitted(self, tmp_path: Path):
        mgr = _manager(_config(_git_source(), cache_dir=tmp_path / "cache"), tmp_path=tmp_path)
        p_clone, p_root, p_proc, _ = _patched(mgr)
        with p_clone, p_root, p_proc:
            run = await mgr.create_agent_run("demo", "slurm", mode="remote")
        assert run.agent_session_name and run.agent_session_name.startswith("demo-")

    @pytest.mark.asyncio
    async def test_env_group_applied(self, tmp_path: Path):
        cfg = _config(_git_source(), cache_dir=tmp_path / "cache")
        cfg.agent.env_group = "claude-auth"
        mgr = _manager(cfg, tmp_path=tmp_path)
        p_clone, p_root, p_proc, _ = _patched(mgr)
        with p_clone, p_root, p_proc:
            run = await mgr.create_agent_run("demo", "slurm", mode="remote")
        env = run.items[0].task.env
        assert len(env) == 1
        assert env[0].include == ["claude-auth"]

    @pytest.mark.asyncio
    async def test_rejects_unknown_mode(self, tmp_path: Path):
        mgr = _manager(_config(_git_source(), cache_dir=tmp_path / "cache"), tmp_path=tmp_path)
        with pytest.raises(ValueError, match="Unknown agent mode"):
            await mgr.create_agent_run("demo", "slurm", mode="bogus")

    @pytest.mark.asyncio
    async def test_rejects_path_source(self, tmp_path: Path):
        src = PathSourceConfig(name="demo", path="/data/repo", backend="slurm")
        mgr = _manager(_config(src, cache_dir=tmp_path / "cache"), tmp_path=tmp_path)
        with pytest.raises(ValueError, match="git source"):
            await mgr.create_agent_run("demo", "slurm", mode="remote")

    @pytest.mark.asyncio
    async def test_rejects_non_ssh_backend(self, tmp_path: Path):
        mgr = _manager(
            _config(_git_source(), cache_dir=tmp_path / "cache"),
            tmp_path=tmp_path, with_ssh=False,
        )
        with pytest.raises(ValueError, match="SSH backend"):
            await mgr.create_agent_run("demo", "slurm", mode="remote")

    @pytest.mark.asyncio
    async def test_rejects_unknown_source(self, tmp_path: Path):
        mgr = _manager(_config(_git_source(), cache_dir=tmp_path / "cache"), tmp_path=tmp_path)
        with pytest.raises(ValueError, match="not found"):
            await mgr.create_agent_run("nope", "slurm", mode="remote")


class TestAgentFieldsRoundTrip:
    def test_storage_round_trip(self, tmp_path: Path):
        storage = RunStorageManager(tmp_path / "runs")
        run = Run(
            id="run123",
            workflow_name="_agent/demo/sess",
            backend_name="slurm",
            created_at=datetime(2026, 6, 25, tzinfo=timezone.utc),
            items=[RunItem(task=TaskDefinition(id="agent", name="a", command="claude"))],
            max_concurrent=None,
            agent_session=True,
            agent_mode="tui",
            agent_session_name="sess",
            git_repo="git@h:o/r.git",
            commit_hash="abc123",
        )
        storage.save_run(run)
        loaded = storage.load_run(storage._run_dir(run))

        assert loaded is not None
        assert loaded.agent_session is True
        assert loaded.agent_mode == "tui"
        assert loaded.agent_session_name == "sess"

    def test_defaults_for_non_agent_run(self, tmp_path: Path):
        storage = RunStorageManager(tmp_path / "runs")
        run = Run(
            id="plain1",
            workflow_name="demo/wf",
            backend_name="slurm",
            created_at=datetime(2026, 6, 25, tzinfo=timezone.utc),
            items=[],
            max_concurrent=None,
        )
        storage.save_run(run)
        loaded = storage.load_run(storage._run_dir(run))
        assert loaded is not None
        assert loaded.agent_session is False
        assert loaded.agent_mode is None


class TestSessionUrlRegex:
    def test_extracts_claude_url(self):
        from scripthut.main import _CLAUDE_SESSION_URL_RE

        out = "Session ready\nOpen: https://claude.ai/code/sessions/abc-123 now\n"
        m = _CLAUDE_SESSION_URL_RE.search(out)
        assert m and m.group(0) == "https://claude.ai/code/sessions/abc-123"

    def test_no_url_returns_none(self):
        from scripthut.main import _CLAUDE_SESSION_URL_RE

        assert _CLAUDE_SESSION_URL_RE.search("nothing here yet") is None
