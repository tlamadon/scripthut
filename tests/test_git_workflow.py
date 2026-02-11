"""Tests for git workflow support (clone-then-command)."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripthut.config_schema import WorkflowConfig, WorkflowGitConfig
from scripthut.runs.manager import RunManager
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition


# -- Helpers ------------------------------------------------------------------


def _make_workflow(
    name: str = "git-wf",
    backend: str = "test-cluster",
    command: str = "python get_tasks.py",
    git_repo: str = "git@github.com:org/repo.git",
    git_branch: str = "main",
    deploy_key: str | None = None,
) -> WorkflowConfig:
    git = WorkflowGitConfig(
        repo=git_repo,
        branch=git_branch,
        deploy_key=Path(deploy_key) if deploy_key else None,
    )
    return WorkflowConfig(
        name=name, backend=backend, command=command, git=git
    )


def _make_workflow_no_git(
    name: str = "plain-wf",
    backend: str = "test-cluster",
    command: str = "python get_tasks.py",
) -> WorkflowConfig:
    return WorkflowConfig(name=name, backend=backend, command=command)


def _make_manager(
    ssh_mock: AsyncMock | None = None,
    backend_name: str = "test-cluster",
    workflows: list[WorkflowConfig] | None = None,
) -> RunManager:
    config = MagicMock()
    config.settings.filter_user = "testuser"
    config.workflows = workflows or []

    def get_workflow(name: str) -> WorkflowConfig | None:
        for wf in config.workflows:
            if wf.name == name:
                return wf
        return None

    config.get_workflow = get_workflow
    config.get_backend.return_value = None

    backends = {}
    if ssh_mock:
        backends[backend_name] = ssh_mock
    return RunManager(config=config, backends=backends)


# -- _build_remote_git_ssh_command tests --------------------------------------


class TestBuildRemoteGitSshCommand:
    def test_with_deploy_key(self):
        result = RunManager._build_remote_git_ssh_command("/tmp/key123")
        assert "ssh -i /tmp/key123" in result
        assert "IdentitiesOnly=yes" in result
        assert "BatchMode=yes" in result
        assert result.startswith('GIT_SSH_COMMAND="')
        assert result.endswith('" ')

    def test_without_deploy_key(self):
        assert RunManager._build_remote_git_ssh_command(None) == ""

    def test_empty_string_deploy_key(self):
        assert RunManager._build_remote_git_ssh_command("") == ""


# -- _upload_deploy_key tests -------------------------------------------------


class TestUploadDeployKey:
    @pytest.mark.asyncio
    async def test_uploads_key_to_temp_file(self, tmp_path: Path):
        key_file = tmp_path / "deploy_key"
        key_file.write_text("-----BEGIN OPENSSH PRIVATE KEY-----\nfake\n-----END OPENSSH PRIVATE KEY-----\n")

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            return_value=("/tmp/scripthut_key_abc123", "", 0)
        )

        manager = _make_manager(ssh_mock)
        remote_path = await manager._upload_deploy_key(ssh_mock, key_file)

        assert remote_path == "/tmp/scripthut_key_abc123"
        ssh_mock.run_command.assert_called_once()
        cmd = ssh_mock.run_command.call_args[0][0]
        assert "mktemp" in cmd
        assert "base64" in cmd
        assert "chmod 600" in cmd

    @pytest.mark.asyncio
    async def test_raises_if_key_not_found(self):
        ssh_mock = AsyncMock()
        manager = _make_manager(ssh_mock)

        with pytest.raises(ValueError, match="Deploy key not found"):
            await manager._upload_deploy_key(ssh_mock, Path("/nonexistent/key"))

    @pytest.mark.asyncio
    async def test_raises_on_ssh_failure(self, tmp_path: Path):
        key_file = tmp_path / "deploy_key"
        key_file.write_text("fake-key")

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=("", "permission denied", 1))

        manager = _make_manager(ssh_mock)
        with pytest.raises(ValueError, match="Failed to upload deploy key"):
            await manager._upload_deploy_key(ssh_mock, key_file)


# -- _cleanup_deploy_key tests -----------------------------------------------


class TestCleanupDeployKey:
    @pytest.mark.asyncio
    async def test_removes_temp_file(self):
        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=("", "", 0))

        manager = _make_manager(ssh_mock)
        await manager._cleanup_deploy_key(ssh_mock, "/tmp/scripthut_key_abc123")

        ssh_mock.run_command.assert_called_once_with("rm -f /tmp/scripthut_key_abc123")


# -- _ensure_repo_cloned tests -----------------------------------------------


class TestEnsureRepoCloned:
    @pytest.mark.asyncio
    async def test_fresh_clone(self, tmp_path: Path):
        """Clone happens when directory doesn't exist."""
        key_file = tmp_path / "deploy_key"
        key_file.write_text("fake-key")

        workflow = _make_workflow(deploy_key=str(key_file))
        commit_hash = "abcdef123456" + "7890abcd"

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            side_effect=[
                # 1. Upload deploy key
                ("/tmp/scripthut_key_xyz", "", 0),
                # 2. git ls-remote
                (f"{commit_hash}\trefs/heads/main", "", 0),
                # 3. test -d (doesn't exist)
                ("", "", 1),
                # 4. git clone
                ("", "", 0),
                # 5. cleanup deploy key
                ("", "", 0),
            ]
        )

        manager = _make_manager(ssh_mock, workflows=[workflow])
        clone_dir, short_hash = await manager._ensure_repo_cloned(ssh_mock, workflow)

        assert short_hash == commit_hash[:12]
        assert clone_dir == f"~/scripthut-repos/{short_hash}"
        assert short_hash in clone_dir

        # Verify git clone was called with shallow clone
        clone_cmd = ssh_mock.run_command.call_args_list[3][0][0]
        assert "git clone" in clone_cmd
        assert "--single-branch" in clone_cmd
        assert "--depth 1" in clone_cmd

    @pytest.mark.asyncio
    async def test_idempotent_existing_clone(self, tmp_path: Path):
        """Skip clone when directory already exists."""
        key_file = tmp_path / "deploy_key"
        key_file.write_text("fake-key")

        workflow = _make_workflow(deploy_key=str(key_file))
        commit_hash = "abcdef123456" + "7890abcd"

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            side_effect=[
                # 1. Upload deploy key
                ("/tmp/scripthut_key_xyz", "", 0),
                # 2. git ls-remote
                (f"{commit_hash}\trefs/heads/main", "", 0),
                # 3. test -d (exists!)
                ("exists", "", 0),
                # 4. cleanup deploy key
                ("", "", 0),
            ]
        )

        manager = _make_manager(ssh_mock, workflows=[workflow])
        clone_dir, short_hash = await manager._ensure_repo_cloned(ssh_mock, workflow)

        assert short_hash == commit_hash[:12]
        # Only 4 calls (no git clone)
        assert ssh_mock.run_command.call_count == 4

    @pytest.mark.asyncio
    async def test_no_deploy_key(self):
        """Works without a deploy key."""
        workflow = _make_workflow(deploy_key=None)
        commit_hash = "abcdef123456" + "7890abcd"

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            side_effect=[
                # 1. git ls-remote (no key upload)
                (f"{commit_hash}\trefs/heads/main", "", 0),
                # 2. test -d (doesn't exist)
                ("", "", 1),
                # 3. git clone
                ("", "", 0),
                # No cleanup needed
            ]
        )

        manager = _make_manager(ssh_mock, workflows=[workflow])
        clone_dir, short_hash = await manager._ensure_repo_cloned(ssh_mock, workflow)

        assert short_hash == commit_hash[:12]
        # No GIT_SSH_COMMAND in the clone command
        clone_cmd = ssh_mock.run_command.call_args_list[2][0][0]
        assert "GIT_SSH_COMMAND" not in clone_cmd

    @pytest.mark.asyncio
    async def test_ls_remote_failure(self, tmp_path: Path):
        """Raises on git ls-remote failure."""
        key_file = tmp_path / "deploy_key"
        key_file.write_text("fake-key")

        workflow = _make_workflow(deploy_key=str(key_file))

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            side_effect=[
                # 1. Upload deploy key
                ("/tmp/scripthut_key_xyz", "", 0),
                # 2. git ls-remote fails
                ("", "fatal: repository not found", 128),
                # 3. cleanup deploy key (always runs)
                ("", "", 0),
            ]
        )

        manager = _make_manager(ssh_mock, workflows=[workflow])
        with pytest.raises(ValueError, match="Failed to resolve branch"):
            await manager._ensure_repo_cloned(ssh_mock, workflow)

        # Deploy key should still be cleaned up
        cleanup_cmd = ssh_mock.run_command.call_args_list[2][0][0]
        assert "rm -f" in cleanup_cmd

    @pytest.mark.asyncio
    async def test_clone_failure(self, tmp_path: Path):
        """Raises on git clone failure, still cleans up key."""
        key_file = tmp_path / "deploy_key"
        key_file.write_text("fake-key")

        workflow = _make_workflow(deploy_key=str(key_file))
        commit_hash = "abcdef123456" + "7890abcd"

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            side_effect=[
                # 1. Upload deploy key
                ("/tmp/scripthut_key_xyz", "", 0),
                # 2. git ls-remote
                (f"{commit_hash}\trefs/heads/main", "", 0),
                # 3. test -d (doesn't exist)
                ("", "", 1),
                # 4. git clone fails
                ("", "fatal: could not read from remote", 128),
                # 5. cleanup deploy key (always runs)
                ("", "", 0),
            ]
        )

        manager = _make_manager(ssh_mock, workflows=[workflow])
        with pytest.raises(ValueError, match="Git clone failed"):
            await manager._ensure_repo_cloned(ssh_mock, workflow)

        # Deploy key still cleaned up
        cleanup_cmd = ssh_mock.run_command.call_args_list[4][0][0]
        assert "rm -f" in cleanup_cmd

    @pytest.mark.asyncio
    async def test_custom_clone_dir(self):
        """Respects a custom clone_dir from the git config."""
        workflow = WorkflowConfig(
            name="custom-wf",
            backend="test-cluster",
            command="python tasks.py",
            git=WorkflowGitConfig(
                repo="git@github.com:org/repo.git",
                clone_dir="/scratch/user/repos",
            ),
        )
        commit_hash = "abcdef123456" + "7890abcd"

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            side_effect=[
                # 1. git ls-remote (no deploy key)
                (f"{commit_hash}\trefs/heads/main", "", 0),
                # 2. test -d (doesn't exist)
                ("", "", 1),
                # 3. git clone
                ("", "", 0),
            ]
        )

        manager = _make_manager(ssh_mock, workflows=[workflow])
        clone_dir, short_hash = await manager._ensure_repo_cloned(ssh_mock, workflow)

        assert clone_dir == f"/scratch/user/repos/{short_hash}"

    @pytest.mark.asyncio
    async def test_no_git_config_raises(self):
        """Raises if workflow has no git config."""
        workflow = _make_workflow_no_git()
        ssh_mock = AsyncMock()
        manager = _make_manager(ssh_mock, workflows=[workflow])

        with pytest.raises(ValueError, match="no git configuration"):
            await manager._ensure_repo_cloned(ssh_mock, workflow)


# -- create_run with git workflow tests ---------------------------------------


class TestCreateRunWithGit:
    @pytest.mark.asyncio
    @patch.object(RunManager, "_build_run", new_callable=AsyncMock)
    @patch.object(RunManager, "_ensure_repo_cloned", new_callable=AsyncMock)
    async def test_clones_and_builds_run(self, mock_clone, mock_build):
        """create_run clones repo, fetches tasks inside clone dir, builds run."""
        workflow = _make_workflow()
        tasks_json = json.dumps({
            "tasks": [
                {"id": "t1", "name": "Task 1", "command": "echo 1"},
                {"id": "t2", "name": "Task 2", "command": "echo 2"},
            ]
        })

        mock_clone.return_value = ("~/scripthut-repos/abcdef123456", "abcdef123456")
        mock_build.return_value = Run(
            id="test1234",
            workflow_name="git-wf",
            backend_name="test-cluster",
            created_at=__import__("datetime").datetime.now(),
            items=[],
            max_concurrent=5,
        )

        ssh_mock = AsyncMock()
        # fetch_tasks will call run_command with the cd-prefixed command
        ssh_mock.run_command = AsyncMock(return_value=(tasks_json, "", 0))

        manager = _make_manager(ssh_mock, workflows=[workflow])
        run = await manager.create_run("git-wf")

        # Verify clone was called
        mock_clone.assert_called_once_with(ssh_mock, workflow)

        # Verify _build_run was called with tasks
        mock_build.assert_called_once()
        tasks = mock_build.call_args[0][0]
        assert len(tasks) == 2

        # Verify tasks' working_dir was defaulted to clone dir
        assert tasks[0].working_dir == "~/scripthut-repos/abcdef123456"
        assert tasks[1].working_dir == "~/scripthut-repos/abcdef123456"

        # Verify commit_hash is set on run
        assert run.commit_hash == "abcdef123456"

    @pytest.mark.asyncio
    @patch.object(RunManager, "_build_run", new_callable=AsyncMock)
    @patch.object(RunManager, "_ensure_repo_cloned", new_callable=AsyncMock)
    async def test_preserves_explicit_working_dir(self, mock_clone, mock_build):
        """Tasks with explicit working_dir keep it, only default '~' is overridden."""
        workflow = _make_workflow()
        tasks_json = json.dumps({
            "tasks": [
                {"id": "t1", "name": "Task 1", "command": "echo 1", "working_dir": "/custom/path"},
                {"id": "t2", "name": "Task 2", "command": "echo 2"},
            ]
        })

        mock_clone.return_value = ("~/scripthut-repos/abc123", "abc123")
        mock_build.return_value = Run(
            id="test1234",
            workflow_name="git-wf",
            backend_name="test-cluster",
            created_at=__import__("datetime").datetime.now(),
            items=[],
            max_concurrent=5,
        )

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(tasks_json, "", 0))

        manager = _make_manager(ssh_mock, workflows=[workflow])
        await manager.create_run("git-wf")

        tasks = mock_build.call_args[0][0]
        assert tasks[0].working_dir == "/custom/path"  # preserved
        assert tasks[1].working_dir == "~/scripthut-repos/abc123"  # defaulted

    @pytest.mark.asyncio
    @patch.object(RunManager, "_build_run", new_callable=AsyncMock)
    async def test_no_git_skips_clone(self, mock_build):
        """create_run without git config works as before."""
        workflow = _make_workflow_no_git()
        tasks_json = json.dumps({
            "tasks": [
                {"id": "t1", "name": "Task 1", "command": "echo 1"},
            ]
        })

        mock_build.return_value = Run(
            id="test1234",
            workflow_name="plain-wf",
            backend_name="test-cluster",
            created_at=__import__("datetime").datetime.now(),
            items=[],
            max_concurrent=5,
        )

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(tasks_json, "", 0))

        manager = _make_manager(ssh_mock, workflows=[workflow])
        run = await manager.create_run("plain-wf")

        assert run.commit_hash is None


# -- fetch_tasks with clone_dir tests ----------------------------------------


class TestFetchTasksCloneDir:
    @pytest.mark.asyncio
    async def test_prefixes_command_with_cd(self):
        """When clone_dir is set, command is prefixed with cd."""
        workflow = _make_workflow()
        tasks_json = json.dumps({
            "tasks": [{"id": "t1", "name": "T1", "command": "echo hi"}]
        })

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(tasks_json, "", 0))

        manager = _make_manager(ssh_mock, workflows=[workflow])
        tasks = await manager.fetch_tasks(workflow, clone_dir="~/scripthut-repos/wf/abc123")

        cmd = ssh_mock.run_command.call_args[0][0]
        assert cmd.startswith("cd ~/scripthut-repos/wf/abc123 && ")
        assert "python get_tasks.py" in cmd

    @pytest.mark.asyncio
    async def test_no_clone_dir_uses_plain_command(self):
        """Without clone_dir, command is used as-is."""
        workflow = _make_workflow()
        tasks_json = json.dumps({
            "tasks": [{"id": "t1", "name": "T1", "command": "echo hi"}]
        })

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(tasks_json, "", 0))

        manager = _make_manager(ssh_mock, workflows=[workflow])
        await manager.fetch_tasks(workflow)

        cmd = ssh_mock.run_command.call_args[0][0]
        assert cmd == "python get_tasks.py"


# -- Config parsing tests -----------------------------------------------------


class TestWorkflowGitConfig:
    def test_parse_with_git(self):
        wf = WorkflowConfig(
            name="test",
            backend="cluster",
            command="python tasks.py",
            git=WorkflowGitConfig(
                repo="git@github.com:org/repo.git",
                branch="develop",
                deploy_key=Path("~/.ssh/my-key"),
            ),
        )
        assert wf.git is not None
        assert wf.git.repo == "git@github.com:org/repo.git"
        assert wf.git.branch == "develop"
        assert wf.git.deploy_key == Path("~/.ssh/my-key")
        assert wf.git.deploy_key_resolved == Path("~/.ssh/my-key").expanduser()

    def test_parse_without_git(self):
        wf = WorkflowConfig(
            name="test",
            backend="cluster",
            command="python tasks.py",
        )
        assert wf.git is None

    def test_git_defaults(self):
        git = WorkflowGitConfig(repo="git@github.com:org/repo.git")
        assert git.branch == "main"
        assert git.deploy_key is None
        assert git.deploy_key_resolved is None
        assert git.clone_dir == "~/scripthut-repos"

    def test_custom_clone_dir(self):
        git = WorkflowGitConfig(
            repo="git@github.com:org/repo.git",
            clone_dir="/scratch/user/repos",
        )
        assert git.clone_dir == "/scratch/user/repos"


# -- Storage roundtrip with commit_hash --------------------------------------


class TestCommitHashStorage:
    def test_save_and_load_with_commit_hash(self, tmp_path: Path):
        from scripthut.runs.storage import RunStorageManager
        from datetime import datetime

        storage = RunStorageManager(base_dir=tmp_path)
        task = TaskDefinition(id="t1", name="Task 1", command="echo hi")
        run = Run(
            id="abc12345",
            workflow_name="git-wf",
            backend_name="test-cluster",
            created_at=datetime.now(),
            items=[RunItem(task=task)],
            max_concurrent=5,
            commit_hash="abcdef123456",
        )

        storage.save_run(run)
        loaded = storage.load_all_runs()

        assert "abc12345" in loaded
        assert loaded["abc12345"].commit_hash == "abcdef123456"

    def test_save_and_load_without_commit_hash(self, tmp_path: Path):
        from scripthut.runs.storage import RunStorageManager
        from datetime import datetime

        storage = RunStorageManager(base_dir=tmp_path)
        task = TaskDefinition(id="t1", name="Task 1", command="echo hi")
        run = Run(
            id="xyz12345",
            workflow_name="plain-wf",
            backend_name="test-cluster",
            created_at=datetime.now(),
            items=[RunItem(task=task)],
            max_concurrent=5,
        )

        storage.save_run(run)
        loaded = storage.load_all_runs()

        assert "xyz12345" in loaded
        assert loaded["xyz12345"].commit_hash is None
