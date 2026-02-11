"""Tests for project-based workflow discovery and run creation."""

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripthut.config_schema import ProjectConfig
from scripthut.runs.manager import RunManager
from scripthut.runs.models import (
    Run,
    RunItem,
    RunItemStatus,
    TaskDefinition,
)


# -- Helpers ------------------------------------------------------------------


def _make_manager(
    ssh_mock: AsyncMock | None = None,
    cluster_name: str = "test-cluster",
    projects: list[ProjectConfig] | None = None,
) -> RunManager:
    """Create a RunManager with a mocked SSH client and optional projects."""
    config = MagicMock()
    config.settings.filter_user = "testuser"
    config.projects = projects or []

    # Wire up get_project to search the projects list
    def get_project(name: str) -> ProjectConfig | None:
        for p in config.projects:
            if p.name == name:
                return p
        return None
    config.get_project = get_project

    clusters = {}
    if ssh_mock:
        clusters[cluster_name] = ssh_mock
    return RunManager(config=config, clusters=clusters)


def _make_project(
    name: str = "test-project",
    cluster: str = "test-cluster",
    path: str = "~/my-project",
    max_concurrent: int = 5,
) -> ProjectConfig:
    return ProjectConfig(
        name=name, cluster=cluster, path=path, max_concurrent=max_concurrent
    )


# -- discover_workflows tests ------------------------------------------------


class TestDiscoverWorkflows:
    """Tests for the discover_workflows method."""

    @pytest.mark.asyncio
    async def test_discovers_sflow_files(self):
        """git ls-files output is parsed into a list of relative paths."""
        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            return_value=(
                "r_simulation/sflow.json\npython_analysis/sflow.json\n",
                "",
                0,
            )
        )

        project = _make_project()
        manager = _make_manager(ssh_mock, projects=[project])

        paths = await manager.discover_workflows("test-project")

        assert paths == ["r_simulation/sflow.json", "python_analysis/sflow.json"]
        ssh_mock.run_command.assert_called_once_with(
            "cd ~/my-project && git ls-files '*/sflow.json' 'sflow.json'"
        )

    @pytest.mark.asyncio
    async def test_discovers_root_sflow(self):
        """A sflow.json at the repo root is also discovered."""
        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            return_value=("sflow.json\n", "", 0)
        )

        project = _make_project()
        manager = _make_manager(ssh_mock, projects=[project])

        paths = await manager.discover_workflows("test-project")
        assert paths == ["sflow.json"]

    @pytest.mark.asyncio
    async def test_empty_repo_returns_empty_list(self):
        """No sflow.json files returns empty list."""
        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=("", "", 0))

        project = _make_project()
        manager = _make_manager(ssh_mock, projects=[project])

        paths = await manager.discover_workflows("test-project")
        assert paths == []

    @pytest.mark.asyncio
    async def test_unknown_project_raises(self):
        """Requesting unknown project raises ValueError."""
        manager = _make_manager()

        with pytest.raises(ValueError, match="not found"):
            await manager.discover_workflows("nonexistent")

    @pytest.mark.asyncio
    async def test_no_ssh_raises(self):
        """No SSH connection raises ValueError."""
        project = _make_project()
        manager = _make_manager(ssh_mock=None, projects=[project])

        with pytest.raises(ValueError, match="No SSH connection"):
            await manager.discover_workflows("test-project")

    @pytest.mark.asyncio
    async def test_git_failure_raises(self):
        """git ls-files failure raises ValueError."""
        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            return_value=("", "fatal: not a git repository", 128)
        )

        project = _make_project()
        manager = _make_manager(ssh_mock, projects=[project])

        with pytest.raises(ValueError, match="git ls-files failed"):
            await manager.discover_workflows("test-project")


# -- create_run_from_project tests -------------------------------------------


class TestCreateRunFromProject:
    """Tests for the create_run_from_project method."""

    @pytest.mark.asyncio
    @patch.object(RunManager, "_build_run", new_callable=AsyncMock)
    async def test_reads_sflow_and_builds_run(self, mock_build):
        """sflow.json is read via cat and tasks are passed to _build_run."""
        sflow_json = json.dumps({
            "tasks": [
                {"id": "t1", "name": "Task 1", "command": "echo 1"},
                {"id": "t2", "name": "Task 2", "command": "echo 2"},
            ]
        })

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(sflow_json, "", 0))

        mock_build.return_value = MagicMock(spec=Run)

        project = _make_project()
        manager = _make_manager(ssh_mock, projects=[project])

        await manager.create_run_from_project(
            "test-project", "r_simulation/sflow.json"
        )

        # Verify cat was called with the right path
        ssh_mock.run_command.assert_called_once_with(
            "cat ~/my-project/r_simulation/sflow.json"
        )

        # Verify _build_run was called
        mock_build.assert_called_once()
        args = mock_build.call_args
        tasks = args[0][0]  # first positional arg
        assert len(tasks) == 2
        assert tasks[0].id == "t1"

    @pytest.mark.asyncio
    @patch.object(RunManager, "_build_run", new_callable=AsyncMock)
    async def test_infers_working_dir(self, mock_build):
        """working_dir is set to the directory containing sflow.json."""
        sflow_json = json.dumps({
            "tasks": [
                {"id": "t1", "name": "Task 1", "command": "echo 1"},
            ]
        })

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(sflow_json, "", 0))
        mock_build.return_value = MagicMock(spec=Run)

        project = _make_project()
        manager = _make_manager(ssh_mock, projects=[project])

        await manager.create_run_from_project(
            "test-project", "r_simulation/sflow.json"
        )

        tasks = mock_build.call_args[0][0]
        assert tasks[0].working_dir == "~/my-project/r_simulation"

    @pytest.mark.asyncio
    @patch.object(RunManager, "_build_run", new_callable=AsyncMock)
    async def test_explicit_working_dir_not_overridden(self, mock_build):
        """Tasks with explicit working_dir are not overridden."""
        sflow_json = json.dumps({
            "tasks": [
                {
                    "id": "t1", "name": "Task 1", "command": "echo 1",
                    "working_dir": "/custom/path",
                },
            ]
        })

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(sflow_json, "", 0))
        mock_build.return_value = MagicMock(spec=Run)

        project = _make_project()
        manager = _make_manager(ssh_mock, projects=[project])

        await manager.create_run_from_project(
            "test-project", "r_simulation/sflow.json"
        )

        tasks = mock_build.call_args[0][0]
        assert tasks[0].working_dir == "/custom/path"

    @pytest.mark.asyncio
    @patch.object(RunManager, "_build_run", new_callable=AsyncMock)
    async def test_workflow_name_format(self, mock_build):
        """workflow_name = 'project_name/workflow_dir'."""
        sflow_json = json.dumps({
            "tasks": [{"id": "t", "name": "T", "command": "echo"}]
        })

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(sflow_json, "", 0))
        mock_build.return_value = MagicMock(spec=Run)

        project = _make_project()
        manager = _make_manager(ssh_mock, projects=[project])

        await manager.create_run_from_project(
            "test-project", "r_simulation/sflow.json"
        )

        workflow_name = mock_build.call_args[0][1]
        assert workflow_name == "test-project/r_simulation"

    @pytest.mark.asyncio
    @patch.object(RunManager, "_build_run", new_callable=AsyncMock)
    async def test_root_sflow_workflow_name(self, mock_build):
        """Root sflow.json workflow_name = just project name."""
        sflow_json = json.dumps({
            "tasks": [{"id": "t", "name": "T", "command": "echo"}]
        })

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(sflow_json, "", 0))
        mock_build.return_value = MagicMock(spec=Run)

        project = _make_project()
        manager = _make_manager(ssh_mock, projects=[project])

        await manager.create_run_from_project(
            "test-project", "sflow.json"
        )

        workflow_name = mock_build.call_args[0][1]
        assert workflow_name == "test-project"

    @pytest.mark.asyncio
    async def test_unknown_project_raises(self):
        """Requesting unknown project raises ValueError."""
        manager = _make_manager()

        with pytest.raises(ValueError, match="not found"):
            await manager.create_run_from_project("nonexistent", "sflow.json")

    @pytest.mark.asyncio
    async def test_cat_failure_raises(self):
        """Failed cat raises ValueError."""
        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            return_value=("", "No such file", 1)
        )

        project = _make_project()
        manager = _make_manager(ssh_mock, projects=[project])

        with pytest.raises(ValueError, match="Failed to read"):
            await manager.create_run_from_project(
                "test-project", "missing/sflow.json"
            )

    @pytest.mark.asyncio
    async def test_invalid_json_raises(self):
        """Invalid JSON raises ValueError."""
        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            return_value=("not json {{{", "", 0)
        )

        project = _make_project()
        manager = _make_manager(ssh_mock, projects=[project])

        with pytest.raises(ValueError, match="Invalid JSON"):
            await manager.create_run_from_project(
                "test-project", "bad/sflow.json"
            )
