"""Tests for the ad-hoc task path (RunManager + API + CLI builder)."""

from __future__ import annotations

import argparse
import io
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripthut.api import make_api_router
from scripthut.cli import _build_adhoc_task_dict
from scripthut.runs.models import Run, RunItem, RunItemStatus, TaskDefinition


# ---------- API: POST /tasks/run -----------------------------------------


def _make_run(task_id: str = "adhoc-abc") -> Run:
    return Run(
        id="run42",
        workflow_name=f"_adhoc/{task_id}",
        backend_name="test-cluster",
        created_at=datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC),
        items=[
            RunItem(
                task=TaskDefinition(id=task_id, name="adhoc", command="echo hi"),
                status=RunItemStatus.SUBMITTED,
            ),
        ],
        max_concurrent=None,
    )


def _state(rm: MagicMock | None = None):
    state = MagicMock()
    state.run_manager = rm
    state.config_error = None
    state.backends = {}
    state.config = None
    state.notify_poll = MagicMock()
    return state


def _client(state) -> TestClient:
    app = FastAPI()
    app.include_router(make_api_router(state))
    return TestClient(app)


def test_adhoc_task_endpoint_creates_run():
    rm = MagicMock()
    rm.create_adhoc_run = AsyncMock(return_value=_make_run())
    state = _state(rm)

    body = {
        "backend": "test-cluster",
        "task": {"id": "adhoc-abc", "name": "adhoc", "command": "echo hi"},
    }
    resp = _client(state).post("/api/v1/tasks/run", json=body)
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == "run42"
    assert data["task_count"] == 1
    # The manager got the parsed TaskDefinition and the backend name.
    call_args = rm.create_adhoc_run.await_args
    parsed_task = call_args.args[0]
    assert isinstance(parsed_task, TaskDefinition)
    assert parsed_task.command == "echo hi"
    assert call_args.args[1] == "test-cluster"
    assert call_args.kwargs.get("run_name") is None
    state.notify_poll.assert_called_once()


def test_adhoc_task_endpoint_passes_run_name_override():
    rm = MagicMock()
    rm.create_adhoc_run = AsyncMock(return_value=_make_run())
    state = _state(rm)

    body = {
        "backend": "test-cluster",
        "task": {"id": "x", "name": "x", "command": "echo hi"},
        "run_name": "my-experiment",
    }
    _client(state).post("/api/v1/tasks/run", json=body)
    assert rm.create_adhoc_run.await_args.kwargs["run_name"] == "my-experiment"


def test_adhoc_task_missing_backend_returns_422():
    rm = MagicMock()
    state = _state(rm)

    body = {"task": {"id": "x", "name": "x", "command": "echo hi"}}
    resp = _client(state).post("/api/v1/tasks/run", json=body)
    assert resp.status_code == 422


def test_adhoc_task_invalid_task_dict_returns_422():
    rm = MagicMock()
    state = _state(rm)

    # Missing required 'id' field → TaskDefinition.from_dict raises KeyError
    body = {"backend": "test-cluster", "task": {"name": "x", "command": "echo hi"}}
    resp = _client(state).post("/api/v1/tasks/run", json=body)
    assert resp.status_code == 422


def test_adhoc_task_unknown_backend_returns_422():
    rm = MagicMock()
    rm.create_adhoc_run = AsyncMock(
        side_effect=ValueError("Backend 'nope' not found in config"),
    )
    state = _state(rm)

    body = {
        "backend": "nope",
        "task": {"id": "x", "name": "x", "command": "echo hi"},
    }
    resp = _client(state).post("/api/v1/tasks/run", json=body)
    assert resp.status_code == 422
    assert "not found" in resp.json()["detail"]


# ---------- CLI: _build_adhoc_task_dict ----------------------------------


def _ns(**kwargs) -> argparse.Namespace:
    defaults = {
        "command": None,
        "from_stdin": False,
        "from_file": None,
        "name": None,
        "id": None,
        "cpus": None,
        "memory": None,
        "time_limit": None,
        "partition": None,
        "working_dir": None,
        "gres": None,
        "image": None,
        "env": [],
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestBuildAdhocTaskDict:
    def test_positional_command_creates_minimal_task(self):
        td = _build_adhoc_task_dict(_ns(command="echo hi"))
        assert td["command"] == "echo hi"
        # id + name auto-filled
        assert td["id"].startswith("adhoc-")
        assert td["name"] == td["id"]

    def test_flags_override_resource_fields(self):
        td = _build_adhoc_task_dict(_ns(
            command="python train.py",
            id="my-train",
            name="My Train",
            cpus=8,
            memory="32G",
            time_limit="4:00:00",
            partition="gpu",
            gres="gpu:1",
            working_dir="/scratch/me/repo",
            image="ghcr.io/me/img:latest",
        ))
        assert td["id"] == "my-train"
        assert td["name"] == "My Train"
        assert td["cpus"] == 8
        assert td["memory"] == "32G"
        assert td["time_limit"] == "4:00:00"
        assert td["partition"] == "gpu"
        assert td["gres"] == "gpu:1"
        assert td["working_dir"] == "/scratch/me/repo"
        assert td["image"] == "ghcr.io/me/img:latest"

    def test_env_flag_appends_set_rule(self):
        td = _build_adhoc_task_dict(_ns(
            command="echo",
            env=["FOO=1", "BAR=baz"],
        ))
        assert td["env"] == [{"set": {"FOO": "1", "BAR": "baz"}}]

    def test_env_flag_rejects_value_without_equals(self):
        import pytest
        with pytest.raises(RuntimeError, match="KEY=VALUE"):
            _build_adhoc_task_dict(_ns(command="echo", env=["BOGUS"]))

    def test_from_stdin_reads_json_body(self, monkeypatch):
        body = {
            "id": "from-stdin",
            "name": "From Stdin",
            "command": "true",
            "cpus": 2,
        }
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(body)))

        td = _build_adhoc_task_dict(_ns(from_stdin=True))
        assert td["id"] == "from-stdin"
        assert td["cpus"] == 2

    def test_from_stdin_overridden_by_explicit_flags(self, monkeypatch):
        body = {"id": "from-stdin", "name": "x", "command": "true", "cpus": 2}
        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(body)))

        td = _build_adhoc_task_dict(_ns(from_stdin=True, cpus=16))
        # stdin body provided cpus=2, but --cpus flag wins
        assert td["cpus"] == 16

    def test_from_file_reads_json_body(self, tmp_path: Path):
        body = {"id": "f", "name": "f", "command": "echo from-file"}
        path = tmp_path / "task.json"
        path.write_text(json.dumps(body))

        td = _build_adhoc_task_dict(_ns(from_file=str(path)))
        assert td["command"] == "echo from-file"

    def test_no_source_at_all_raises(self):
        import pytest
        with pytest.raises(RuntimeError, match="command argument"):
            _build_adhoc_task_dict(_ns())

    def test_from_file_takes_precedence_over_stdin(self, tmp_path: Path, monkeypatch):
        # If both --from-file and --from-stdin are passed, --from-file wins.
        # (stdin body would still be readable but should be ignored.)
        path = tmp_path / "task.json"
        path.write_text(
            json.dumps({"id": "from-file", "name": "f", "command": "true"})
        )
        monkeypatch.setattr(
            "sys.stdin",
            io.StringIO(json.dumps({"id": "from-stdin", "name": "s", "command": "x"})),
        )

        td = _build_adhoc_task_dict(_ns(from_file=str(path), from_stdin=True))
        assert td["id"] == "from-file"

    def test_id_default_is_deterministic_per_command(self):
        # The auto-id seeds on (command + time_ns), so two consecutive
        # calls produce different ids even with the same command.
        a = _build_adhoc_task_dict(_ns(command="echo hi"))
        b = _build_adhoc_task_dict(_ns(command="echo hi"))
        assert a["id"] != b["id"]
        assert a["id"].startswith("adhoc-")
