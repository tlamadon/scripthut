"""Tests for the generates_source feature and git-aware project structure."""

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest

from scripthut.runs.manager import RunManager
from scripthut.runs.models import (
    Run,
    RunItem,
    RunItemStatus,
    TaskDefinition,
)


# -- Helpers ------------------------------------------------------------------


def _make_task(
    id: str,
    deps: list[str] | None = None,
    generates_source: str | None = None,
    working_dir: str = "~/project",
) -> TaskDefinition:
    return TaskDefinition(
        id=id,
        name=id,
        command="echo hi",
        dependencies=deps or [],
        generates_source=generates_source,
        working_dir=working_dir,
    )


def _make_run_item(
    id: str,
    status: RunItemStatus = RunItemStatus.PENDING,
    generates_source: str | None = None,
    working_dir: str = "~/project",
) -> RunItem:
    return RunItem(
        task=_make_task(id, generates_source=generates_source, working_dir=working_dir),
        status=status,
    )


def _make_run(
    items: list[RunItem],
    workflow_name: str = "test-source",
) -> Run:
    return Run(
        id="test-r",
        workflow_name=workflow_name,
        backend_name="test-cluster",
        created_at=datetime.now(),
        items=items,
        max_concurrent=5,
    )


def _make_manager(ssh_mock: AsyncMock | None = None) -> RunManager:
    """Create a RunManager with a mocked SSH client."""
    config = MagicMock()
    config.settings.filter_user = "testuser"
    backends = {}
    if ssh_mock:
        backends["test-cluster"] = ssh_mock
    return RunManager(config=config, backends=backends)


# -- TaskDefinition tests ----------------------------------------------------


class TestTaskDefinitionGeneratesSource:
    """Tests for the generates_source field on TaskDefinition."""

    def test_generates_source_default_none(self):
        task = TaskDefinition(id="t", name="t", command="echo")
        assert task.generates_source is None

    def test_generates_source_set(self):
        task = TaskDefinition(
            id="t", name="t", command="echo",
            generates_source="~/.scripthut/out.json",
        )
        assert task.generates_source == "~/.scripthut/out.json"

    def test_from_dict_with_generates_source(self):
        data = {
            "id": "gen",
            "name": "Generator",
            "command": "python gen.py --output out.json",
            "generates_source": ".scripthut/generated.json",
        }
        task = TaskDefinition.from_dict(data)
        assert task.generates_source == ".scripthut/generated.json"

    def test_from_dict_without_generates_source(self):
        data = {"id": "t", "name": "T", "command": "echo"}
        task = TaskDefinition.from_dict(data)
        assert task.generates_source is None


class TestHandleGeneratesSource:
    """Tests for the _handle_generates_source method."""

    @pytest.mark.asyncio
    @patch.object(RunManager, "process_run", new_callable=AsyncMock)
    async def test_appends_tasks_from_json(self, mock_process):
        """Tasks from generates_source are flat-appended to the run."""
        generated_json = json.dumps({
            "tasks": [
                {"id": "sim-0", "name": "Sim 0", "command": "echo 0"},
                {"id": "sim-1", "name": "Sim 1", "command": "echo 1"},
            ]
        })

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(generated_json, "", 0))

        manager = _make_manager(ssh_mock)
        item = _make_run_item(
            "gen", RunItemStatus.COMPLETED,
            generates_source="/path/to/tasks.json",
        )
        run = _make_run(items=[item])

        await manager._handle_generates_source(run, item)

        assert len(run.items) == 3  # original + 2 new
        assert run.items[1].task.id == "sim-0"
        assert run.items[2].task.id == "sim-1"
        assert run.items[1].status == RunItemStatus.PENDING

    @pytest.mark.asyncio
    @patch.object(RunManager, "process_run", new_callable=AsyncMock)
    async def test_accepts_bare_list_json(self, mock_process):
        """generates_source can return a bare list (no 'tasks' key)."""
        generated_json = json.dumps([
            {"id": "a", "name": "A", "command": "echo a"},
        ])

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(generated_json, "", 0))

        manager = _make_manager(ssh_mock)
        item = _make_run_item("gen", RunItemStatus.COMPLETED, generates_source="/p.json")
        run = _make_run(items=[item])

        await manager._handle_generates_source(run, item)

        assert len(run.items) == 2
        assert run.items[1].task.id == "a"

    @pytest.mark.asyncio
    @patch.object(RunManager, "process_run", new_callable=AsyncMock)
    async def test_resolves_relative_path(self, mock_process):
        """Relative generates_source paths are resolved against working_dir."""
        generated_json = json.dumps({"tasks": [
            {"id": "t", "name": "T", "command": "echo"},
        ]})

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(generated_json, "", 0))

        manager = _make_manager(ssh_mock)
        item = _make_run_item(
            "gen", RunItemStatus.COMPLETED,
            generates_source=".scripthut/tasks.json",
            working_dir="~/project/sub",
        )
        run = _make_run(items=[item])

        await manager._handle_generates_source(run, item)

        # Verify cat was called with the resolved path
        ssh_mock.run_command.assert_called_once_with(
            "cat ~/project/sub/.scripthut/tasks.json"
        )

    @pytest.mark.asyncio
    @patch.object(RunManager, "process_run", new_callable=AsyncMock)
    async def test_absolute_path_not_resolved(self, mock_process):
        """Absolute generates_source paths are used as-is."""
        generated_json = json.dumps({"tasks": [
            {"id": "t", "name": "T", "command": "echo"},
        ]})

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(generated_json, "", 0))

        manager = _make_manager(ssh_mock)
        item = _make_run_item(
            "gen", RunItemStatus.COMPLETED,
            generates_source="/absolute/path/tasks.json",
        )
        run = _make_run(items=[item])

        await manager._handle_generates_source(run, item)

        ssh_mock.run_command.assert_called_once_with("cat /absolute/path/tasks.json")

    @pytest.mark.asyncio
    @patch.object(RunManager, "process_run", new_callable=AsyncMock)
    async def test_tilde_path_not_resolved(self, mock_process):
        """Paths starting with ~ are used as-is (shell handles expansion)."""
        generated_json = json.dumps({"tasks": [
            {"id": "t", "name": "T", "command": "echo"},
        ]})

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(generated_json, "", 0))

        manager = _make_manager(ssh_mock)
        item = _make_run_item(
            "gen", RunItemStatus.COMPLETED,
            generates_source="~/.scripthut/tasks.json",
        )
        run = _make_run(items=[item])

        await manager._handle_generates_source(run, item)

        ssh_mock.run_command.assert_called_once_with("cat ~/.scripthut/tasks.json")

    @pytest.mark.asyncio
    async def test_cat_failure_does_not_crash(self):
        """If cat fails, log error but don't crash or append tasks."""
        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            return_value=("", "No such file or directory", 1)
        )

        manager = _make_manager(ssh_mock)
        item = _make_run_item("gen", RunItemStatus.COMPLETED, generates_source="/bad.json")
        run = _make_run(items=[item])

        await manager._handle_generates_source(run, item)

        assert len(run.items) == 1  # No tasks appended

    @pytest.mark.asyncio
    async def test_invalid_json_does_not_crash(self):
        """If JSON is invalid, log error but don't crash."""
        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=("not json {{{", "", 0))

        manager = _make_manager(ssh_mock)
        item = _make_run_item("gen", RunItemStatus.COMPLETED, generates_source="/p.json")
        run = _make_run(items=[item])

        await manager._handle_generates_source(run, item)

        assert len(run.items) == 1

    @pytest.mark.asyncio
    @patch.object(RunManager, "process_run", new_callable=AsyncMock)
    async def test_generated_tasks_can_have_deps_on_parent_tasks(self, mock_process):
        """Generated tasks can depend on tasks already in the run."""
        generated_json = json.dumps({"tasks": [
            {"id": "child", "name": "Child", "command": "echo", "deps": ["gen"]},
        ]})

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(generated_json, "", 0))

        manager = _make_manager(ssh_mock)
        item = _make_run_item("gen", RunItemStatus.COMPLETED, generates_source="/p.json")
        run = _make_run(items=[item])

        await manager._handle_generates_source(run, item)

        assert len(run.items) == 2
        assert run.items[1].task.dependencies == ["gen"]

    @pytest.mark.asyncio
    @patch.object(RunManager, "process_run", new_callable=AsyncMock)
    async def test_generated_tasks_wildcard_deps(self, mock_process):
        """Generated tasks can use wildcard deps matching parent run tasks."""
        generated_json = json.dumps({"tasks": [
            {"id": "agg", "name": "Aggregate", "command": "echo", "deps": ["sim-*"]},
        ]})

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(generated_json, "", 0))

        manager = _make_manager(ssh_mock)
        items = [
            _make_run_item("sim-0", RunItemStatus.COMPLETED),
            _make_run_item("sim-1", RunItemStatus.COMPLETED),
            _make_run_item("gen", RunItemStatus.COMPLETED, generates_source="/p.json"),
        ]
        run = _make_run(items=items)

        await manager._handle_generates_source(run, items[2])

        # agg should depend on both sim-0 and sim-1
        agg = run.items[3]
        assert sorted(agg.task.dependencies) == ["sim-0", "sim-1"]

    @pytest.mark.asyncio
    async def test_unknown_dep_in_generated_tasks_rejects(self):
        """Generated tasks referencing unknown deps are rejected."""
        generated_json = json.dumps({"tasks": [
            {"id": "t", "name": "T", "command": "echo", "deps": ["nonexistent"]},
        ]})

        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(return_value=(generated_json, "", 0))

        manager = _make_manager(ssh_mock)
        item = _make_run_item("gen", RunItemStatus.COMPLETED, generates_source="/p.json")
        run = _make_run(items=[item])

        await manager._handle_generates_source(run, item)

        assert len(run.items) == 1  # Rejected, nothing appended

    @pytest.mark.asyncio
    async def test_no_ssh_client_does_not_crash(self):
        """If backend has no SSH client, log error but don't crash."""
        manager = _make_manager()  # No SSH client
        item = _make_run_item("gen", RunItemStatus.COMPLETED, generates_source="/p.json")
        run = _make_run(items=[item])

        await manager._handle_generates_source(run, item)

        assert len(run.items) == 1

    @pytest.mark.asyncio
    async def test_none_generates_source_returns_early(self):
        """If generates_source is None, return immediately."""
        ssh_mock = AsyncMock()
        manager = _make_manager(ssh_mock)
        item = _make_run_item("gen", RunItemStatus.COMPLETED, generates_source=None)
        run = _make_run(items=[item])

        await manager._handle_generates_source(run, item)

        ssh_mock.run_command.assert_not_called()


# -- _get_git_root tests -----------------------------------------------------


class TestGetGitRoot:
    """Tests for the _get_git_root method."""

    @pytest.mark.asyncio
    async def test_returns_git_root(self):
        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            return_value=("/home/user/project\n", "", 0)
        )

        manager = _make_manager(ssh_mock)
        root = await manager._get_git_root(ssh_mock, "~/project/sub")

        assert root == "/home/user/project"
        ssh_mock.run_command.assert_called_once_with(
            "cd ~/project/sub && git rev-parse --show-toplevel"
        )

    @pytest.mark.asyncio
    async def test_raises_when_not_git_repo(self):
        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            return_value=("", "fatal: not a git repository", 128)
        )

        manager = _make_manager(ssh_mock)

        with pytest.raises(ValueError, match="not inside a git repository"):
            await manager._get_git_root(ssh_mock, "/tmp/not-a-repo")

    @pytest.mark.asyncio
    async def test_strips_whitespace(self):
        ssh_mock = AsyncMock()
        ssh_mock.run_command = AsyncMock(
            return_value=("  /home/user/project  \n", "", 0)
        )

        manager = _make_manager(ssh_mock)
        root = await manager._get_git_root(ssh_mock, "~/project")

        assert root == "/home/user/project"
