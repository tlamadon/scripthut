"""Tests for the source-repo ``scripthut.yaml`` overlay.

Covers both halves of v0.7's "source-side project config" feature:

1. ``RunManager._load_source_project_config`` — reads ``scripthut.yaml`` from
   the server's local cache (git sources, via ``git show``) or over SSH
   (path sources, via ``cat``), validates as project-local, returns the
   parsed config or ``None`` when the file is absent.
2. ``RunManager.create_run_from_source`` env merging — when the helper
   returns a config, its ``env`` is prepended to the workflow doc's env
   list and its ``env_groups`` are under-merged so the workflow doc wins
   on name collision.

The helper itself is mocked at the boundary (subprocess for git,
``SSHClient.run_command`` for path) so the tests run hermetically.
"""

from __future__ import annotations

import json as _json
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
from scripthut.runs.storage import RunStorageManager


def _config_with_source(source, *, cache_dir: Path) -> ScriptHutConfig:
    """Tiny ScriptHutConfig wired around a single source + cache dir."""
    return ScriptHutConfig(
        sources=[source],
        settings=GlobalSettings(sources_cache_dir=cache_dir),
    )


def _manager(config: ScriptHutConfig, *, tmp_path: Path) -> RunManager:
    return RunManager(
        config=config,
        backends={},
        storage=RunStorageManager(tmp_path / "runs"),
        job_backends={},
    )


# ---------------------------------------------------------------------------
# _load_source_project_config — git sources
# ---------------------------------------------------------------------------


class TestLoadGitSource:
    """Git sources: ``git show <sha>:scripthut.yaml`` on the server's cache."""

    @pytest.mark.asyncio
    async def test_returns_parsed_config_when_file_present(
        self, tmp_path: Path,
    ):
        cache = tmp_path / "cache"
        clone = cache / "demo"
        clone.mkdir(parents=True)
        src = GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main")
        mgr = _manager(_config_with_source(src, cache_dir=cache), tmp_path=tmp_path)

        yaml_text = (
            "env_groups:\n"
            "  julia-1.12:\n"
            "    - init: 'module load julia/1.12'\n"
            "    - set: {JULIA_DEPOT_PATH: '~/.julia'}\n"
        )
        proc = MagicMock(returncode=0, stdout=yaml_text, stderr="")
        with patch("subprocess.run", return_value=proc):
            cfg = await mgr._load_source_project_config(src)

        assert cfg is not None
        assert "julia-1.12" in cfg.env_groups
        assert cfg.env_groups["julia-1.12"][0].init == "module load julia/1.12"

    @pytest.mark.asyncio
    async def test_passes_commit_hash_to_git_show(self, tmp_path: Path):
        """Pinning to a specific SHA means the env matches the workflow rev."""
        cache = tmp_path / "cache"
        clone = cache / "demo"
        clone.mkdir(parents=True)
        src = GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main")
        mgr = _manager(_config_with_source(src, cache_dir=cache), tmp_path=tmp_path)

        proc = MagicMock(returncode=0, stdout="env: []\n", stderr="")
        with patch("subprocess.run", return_value=proc) as run_mock:
            await mgr._load_source_project_config(src, commit_hash="deadbeef1234")

        called_args = run_mock.call_args.args[0]
        assert called_args[0] == "git"
        assert called_args[-2] == "show"
        assert called_args[-1] == "deadbeef1234:scripthut.yaml"

    @pytest.mark.asyncio
    async def test_defaults_to_head_when_no_commit(self, tmp_path: Path):
        cache = tmp_path / "cache"
        clone = cache / "demo"
        clone.mkdir(parents=True)
        src = GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main")
        mgr = _manager(_config_with_source(src, cache_dir=cache), tmp_path=tmp_path)

        proc = MagicMock(returncode=0, stdout="env: []\n", stderr="")
        with patch("subprocess.run", return_value=proc) as run_mock:
            await mgr._load_source_project_config(src)

        assert run_mock.call_args.args[0][-1] == "HEAD:scripthut.yaml"

    @pytest.mark.asyncio
    async def test_missing_file_returns_none(self, tmp_path: Path):
        """`git show` exits 128 when the path doesn't exist at the commit.

        That's the "no per-repo overlay" case — must be silent so existing
        repos without ``scripthut.yaml`` keep working unchanged.
        """
        cache = tmp_path / "cache"
        clone = cache / "demo"
        clone.mkdir(parents=True)
        src = GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main")
        mgr = _manager(_config_with_source(src, cache_dir=cache), tmp_path=tmp_path)

        proc = MagicMock(returncode=128, stdout="", stderr="fatal: path 'scripthut.yaml' does not exist in 'HEAD'\n")
        with patch("subprocess.run", return_value=proc):
            assert await mgr._load_source_project_config(src) is None

    @pytest.mark.asyncio
    async def test_no_cache_clone_returns_none(self, tmp_path: Path):
        """Soft skip if the server hasn't synced this source yet."""
        cache = tmp_path / "cache"
        src = GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main")
        mgr = _manager(_config_with_source(src, cache_dir=cache), tmp_path=tmp_path)
        # cache/demo doesn't exist
        assert await mgr._load_source_project_config(src) is None


# ---------------------------------------------------------------------------
# _load_source_project_config — path sources
# ---------------------------------------------------------------------------


class TestLoadPathSource:
    """Path sources: ``cat <path>/scripthut.yaml`` over the backend's SSH."""

    @pytest.mark.asyncio
    async def test_returns_parsed_config_when_file_present(
        self, tmp_path: Path,
    ):
        src = PathSourceConfig(name="local", backend="cluster", path="/r/s")
        mgr = _manager(
            _config_with_source(src, cache_dir=tmp_path / "cache"),
            tmp_path=tmp_path,
        )
        ssh = MagicMock()
        ssh.run_command = AsyncMock(return_value=(
            "env_groups: {julia-1.12: [{init: 'module load julia/1.12'}]}\n",
            "", 0,
        ))
        cfg = await mgr._load_source_project_config(src, ssh_client=ssh)
        assert cfg is not None
        assert "julia-1.12" in cfg.env_groups
        # SSH command must reference the source's path.
        assert "/r/s/scripthut.yaml" in ssh.run_command.call_args.args[0]

    @pytest.mark.asyncio
    async def test_missing_file_returns_none(self, tmp_path: Path):
        src = PathSourceConfig(name="local", backend="cluster", path="/r/s")
        mgr = _manager(
            _config_with_source(src, cache_dir=tmp_path / "cache"),
            tmp_path=tmp_path,
        )
        ssh = MagicMock()
        # cat exits 1 when the file is missing.
        ssh.run_command = AsyncMock(return_value=("", "cat: ... No such file", 1))
        assert await mgr._load_source_project_config(src, ssh_client=ssh) is None

    @pytest.mark.asyncio
    async def test_requires_ssh_client(self, tmp_path: Path):
        src = PathSourceConfig(name="local", backend="cluster", path="/r/s")
        mgr = _manager(
            _config_with_source(src, cache_dir=tmp_path / "cache"),
            tmp_path=tmp_path,
        )
        with pytest.raises(ValueError, match="requires an SSH client"):
            await mgr._load_source_project_config(src, ssh_client=None)


# ---------------------------------------------------------------------------
# Validation: forbidden sections, malformed YAML
# ---------------------------------------------------------------------------


class TestProjectConfigValidation:
    """The repo's YAML must obey the same project-local rules as a local
    project ``scripthut.yaml`` — ``backends`` / ``sources`` / ``settings``
    / ``pricing`` are forbidden because a repo must never redefine
    infrastructure or server settings server-side.
    """

    @pytest.mark.asyncio
    async def test_forbidden_backends_section_raises(self, tmp_path: Path):
        cache = tmp_path / "cache"
        (cache / "demo").mkdir(parents=True)
        src = GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main")
        mgr = _manager(_config_with_source(src, cache_dir=cache), tmp_path=tmp_path)

        proc = MagicMock(
            returncode=0,
            stdout="backends:\n  - name: rogue\n    type: slurm\n",
            stderr="",
        )
        with patch("subprocess.run", return_value=proc):
            with pytest.raises(ValueError) as exc_info:
                await mgr._load_source_project_config(src)
        assert "backends" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_forbidden_settings_section_raises(self, tmp_path: Path):
        cache = tmp_path / "cache"
        (cache / "demo").mkdir(parents=True)
        src = GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main")
        mgr = _manager(_config_with_source(src, cache_dir=cache), tmp_path=tmp_path)

        proc = MagicMock(
            returncode=0,
            stdout="settings:\n  cli_server: https://evil.example\n",
            stderr="",
        )
        with patch("subprocess.run", return_value=proc):
            with pytest.raises(ValueError) as exc_info:
                await mgr._load_source_project_config(src)
        assert "settings" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_malformed_yaml_raises(self, tmp_path: Path):
        cache = tmp_path / "cache"
        (cache / "demo").mkdir(parents=True)
        src = GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main")
        mgr = _manager(_config_with_source(src, cache_dir=cache), tmp_path=tmp_path)

        proc = MagicMock(returncode=0, stdout="env: [: : :\n", stderr="")
        with patch("subprocess.run", return_value=proc):
            with pytest.raises(ValueError, match="invalid YAML"):
                await mgr._load_source_project_config(src)

    @pytest.mark.asyncio
    async def test_empty_yaml_returns_none(self, tmp_path: Path):
        """Empty / blank file is "no overlay", not a config with empty fields."""
        cache = tmp_path / "cache"
        (cache / "demo").mkdir(parents=True)
        src = GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main")
        mgr = _manager(_config_with_source(src, cache_dir=cache), tmp_path=tmp_path)
        proc = MagicMock(returncode=0, stdout="   \n", stderr="")
        with patch("subprocess.run", return_value=proc):
            assert await mgr._load_source_project_config(src) is None


# ---------------------------------------------------------------------------
# create_run_from_source: env precedence
# ---------------------------------------------------------------------------


class TestRunEnvLayering:
    """End-to-end: when a source has a project ``scripthut.yaml`` with
    ``env`` / ``env_groups``, those are folded into the run before the
    workflow file's own — workflow-inline keys win on collision, project
    env runs first.
    """

    def _git_config(self, tmp_path: Path) -> ScriptHutConfig:
        return _config_with_source(
            GitSourceConfig(name="demo", url="git@h:o/r.git", branch="main"),
            cache_dir=tmp_path / "cache",
        )

    async def _run_workflow(
        self, mgr: RunManager, *, project_cfg: ScriptHutConfig | None,
        workflow_doc: dict, backend: str = "test-batch",
    ):
        """Submit one workflow and capture the (doc_env, doc_env_groups)
        ``create_run_from_source`` actually passed to ``_build_run``.
        """
        captured: dict = {}

        async def fake_build_run(
            tasks, workflow_name, backend_name, max_concurrent, ssh_client,
            *, git_repo=None, git_branch=None, commit_hash=None,
            doc_env=None, doc_env_groups=None,
        ):
            captured["doc_env"] = doc_env
            captured["doc_env_groups"] = doc_env_groups
            # Return a minimal Run-shaped sentinel; the test never reads it.
            from scripthut.runs.models import (
                Run,
                RunItem,
                RunItemStatus,
                TaskDefinition,
            )
            from datetime import UTC, datetime
            return Run(
                id="r", workflow_name=workflow_name, backend_name=backend_name,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                items=[RunItem(
                    task=TaskDefinition(id="t", name="t", command="true"),
                    status=RunItemStatus.SUBMITTED,
                )],
                max_concurrent=1,
            )

        async def fake_commit():
            return "deadbeef0000"

        with patch.object(mgr, "_build_run", side_effect=fake_build_run), \
             patch.object(mgr, "_load_source_project_config",
                          AsyncMock(return_value=project_cfg)), \
             patch.object(mgr, "_ls_remote_commit",
                          AsyncMock(return_value="deadbeef0000")), \
             patch.object(mgr, "get_job_backend",
                          return_value=MagicMock()):
            await mgr.create_run_from_source(
                "demo", "wf.json", _json.dumps(workflow_doc), backend,
            )
        return captured

    @pytest.mark.asyncio
    async def test_project_env_groups_merged_when_no_collision(
        self, tmp_path: Path,
    ):
        mgr = _manager(self._git_config(tmp_path), tmp_path=tmp_path)
        project_cfg = ScriptHutConfig.model_validate({
            "env_groups": {
                "julia-1.12": [{"init": "module load julia/1.12"}],
            },
        })
        workflow_doc = {
            "tasks": [{"id": "t", "name": "t", "command": "echo hi"}],
            # No env_groups inline on this workflow.
        }
        captured = await self._run_workflow(
            mgr, project_cfg=project_cfg, workflow_doc=workflow_doc,
        )
        # The repo's group landed in the run's doc_env_groups.
        assert "julia-1.12" in captured["doc_env_groups"]

    @pytest.mark.asyncio
    async def test_workflow_inline_wins_env_groups_collision(
        self, tmp_path: Path,
    ):
        """Workflow-doc-inline group with the same name as a repo group
        must win — most-specific-source-of-truth-for-that-file convention.
        """
        mgr = _manager(self._git_config(tmp_path), tmp_path=tmp_path)
        project_cfg = ScriptHutConfig.model_validate({
            "env_groups": {
                "cuda": [{"set": {"CUDA_VISIBLE_DEVICES": "0"}}],
            },
        })
        workflow_doc = {
            "tasks": [{"id": "t", "name": "t", "command": "echo hi"}],
            "env_groups": {
                "cuda": [{"set": {"CUDA_VISIBLE_DEVICES": "1,2"}}],
            },
        }
        captured = await self._run_workflow(
            mgr, project_cfg=project_cfg, workflow_doc=workflow_doc,
        )
        groups = captured["doc_env_groups"]
        cuda_rules = groups["cuda"]
        # Workflow inline rule value wins, not the repo's.
        assert cuda_rules[0].set == {"CUDA_VISIBLE_DEVICES": "1,2"}

    @pytest.mark.asyncio
    async def test_project_env_list_prepended_before_workflow_env(
        self, tmp_path: Path,
    ):
        """Order matters in the resolver. Repo env runs before workflow
        env so workflow rules see (and can override) the repo's set vars.
        """
        mgr = _manager(self._git_config(tmp_path), tmp_path=tmp_path)
        project_cfg = ScriptHutConfig.model_validate({
            "env": [{"set": {"FROM_REPO": "yes"}}],
        })
        workflow_doc = {
            "tasks": [{"id": "t", "name": "t", "command": "echo hi"}],
            "env": [{"set": {"FROM_WORKFLOW": "yes"}}],
        }
        captured = await self._run_workflow(
            mgr, project_cfg=project_cfg, workflow_doc=workflow_doc,
        )
        env_list = captured["doc_env"]
        assert env_list[0].set == {"FROM_REPO": "yes"}
        assert env_list[1].set == {"FROM_WORKFLOW": "yes"}

    @pytest.mark.asyncio
    async def test_no_project_config_is_a_no_op(self, tmp_path: Path):
        """Existing repos without a scripthut.yaml must keep working
        identically — backwards compatibility.
        """
        mgr = _manager(self._git_config(tmp_path), tmp_path=tmp_path)
        workflow_doc = {
            "tasks": [{"id": "t", "name": "t", "command": "echo hi"}],
            "env": [{"set": {"FROM_WORKFLOW": "yes"}}],
        }
        captured = await self._run_workflow(
            mgr, project_cfg=None, workflow_doc=workflow_doc,
        )
        # Only the workflow's own env, nothing else.
        assert len(captured["doc_env"]) == 1
        assert captured["doc_env"][0].set == {"FROM_WORKFLOW": "yes"}
