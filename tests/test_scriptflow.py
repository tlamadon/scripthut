"""ScriptFlow observation without scheduler or legacy run side effects."""

from __future__ import annotations

import asyncio
import copy
import json
import shutil
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from scripthut import cli
from scripthut.config_schema import GlobalSettings, ScriptHutConfig, SlurmBackendConfig, SSHConfig
from scripthut.scriptflow import (
    CachedRun,
    ObservationError,
    RecordReader,
    Reference,
    ReferenceStore,
    parse_record,
)

NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)
RUN_DIRECTORY = "/data/project/.sf/runs/example-run"


def record(state: str = "running", age: int = 0) -> dict:
    stamp = (NOW - timedelta(seconds=age)).isoformat()
    return {
        "schema_version": 1,
        "run_id": "example-run",
        "flow": "analysis",
        "state": state,
        "error": None,
        "cleanup_pending": False,
        "created_at": stamp,
        "updated_at": stamp,
        "heartbeat": stamp,
        "finished_at": None,
        "host": "compute.example.edu",
        "working_directory": "/data/project",
        "tasks": {
            "first": {
                "task_id": "first",
                "name": "example",
                "state": "running",
                "outputs": [],
                "error": None,
                "attempts": [
                    {
                        "attempt_id": "attempt-one",
                        "state": "failed",
                        "job_id": "123",
                        "log": RUN_DIRECTORY + "/logs/one.out",
                        "exit_code": 7,
                        "error": "exit 7",
                    },
                    {
                        "attempt_id": "attempt-two",
                        "state": "running",
                        "job_id": "124",
                        "log": RUN_DIRECTORY + "/logs/two.out",
                        "error": None,
                    },
                ],
            },
        },
    }


class FakeTransport:
    def __init__(self, content: str = "", code: int = 0) -> None:
        self.content = content
        self.code = code
        self.commands: list[str] = []
        self.error: Exception | None = None
        self.disconnect = AsyncMock()

    async def run_command(self, command: str, timeout: int = 30) -> tuple[str, str, int]:
        self.commands.append(command)
        if self.error:
            raise self.error
        return self.content, "", self.code


@pytest.mark.parametrize("state", ["running", "completed", "failed", "cancelled", "interrupted"])
@pytest.mark.parametrize("attempts", [0, 1, 2])
def test_states_and_attempts(state, attempts):
    raw = record(state)
    raw["tasks"]["first"]["attempts"] = raw["tasks"]["first"]["attempts"][:attempts]
    _, projection = parse_record(json.dumps(raw))
    assert projection.state == state
    assert len(projection.tasks["first"].attempts) == attempts
    if attempts == 2:
        assert projection.tasks["first"].attempts[0].exit_code == 7
        assert projection.tasks["first"].attempts[1].exit_code is None


@pytest.mark.parametrize(
    "state,age,stale",
    [
        ("running", 0, False),
        ("running", 30, False),
        ("running", 31, True),
        ("running", -10, False),
        ("completed", 100, False),
        ("failed", 100, False),
        ("cancelled", 100, False),
        ("interrupted", 100, False),
    ],
)
def test_freshness_is_independent(state, age, stale):
    cached = CachedRun(
        reference=Reference.create("cluster", RUN_DIRECTORY), snapshot=record(state, age)
    )
    data = cached.projection(NOW)
    assert data["workflow"]["state"] == state
    assert data["observation"]["stale"] is stale
    assert data["observation"]["heartbeat_age_seconds"] == max(0, age)


@pytest.mark.parametrize(
    "content,kind",
    [
        ("not json", "malformed"),
        ("[]", "malformed"),
        ("{}", "malformed"),
        ('{"schema_version": 2}', "unsupported_version"),
        ('{"schema_version": "1"}', "unsupported_version"),
        ('{"schema_version": true}', "unsupported_version"),
        ('{"schema_version": 1}', "malformed"),
    ],
)
def test_bad_records(content, kind):
    with pytest.raises(ObservationError) as error:
        parse_record(content)
    assert error.value.kind == kind


@pytest.mark.parametrize(
    "field,value",
    [
        ("state", "unknown"),
        ("cleanup_pending", "false"),
        ("tasks", []),
        ("heartbeat", "yesterday"),
        ("heartbeat", "2026-01-01T12:00:00"),
    ],
)
def test_malformed_fields(field, value):
    raw = record()
    raw[field] = value
    with pytest.raises(ObservationError, match="Invalid ScriptFlow fields"):
        parse_record(json.dumps(raw))


@pytest.mark.parametrize("problem", ["unreachable", "malformed", "unsupported_version", "missing"])
async def test_failed_fetch_preserves_snapshot_and_timestamp(tmp_path, problem):
    transport = FakeTransport(json.dumps(record("completed")))
    reader = RecordReader(transport)
    store = ReferenceStore(tmp_path / "scriptflow")
    cached = await store.register(Reference.create("cluster", RUN_DIRECTORY))
    await reader.fetch(cached)
    await store.save(cached)
    good = copy.deepcopy(cached.snapshot)
    fetched = cached.last_successful_fetch
    if problem == "unreachable":
        transport.error = OSError("offline")
    elif problem == "malformed":
        transport.content = "broken"
    elif problem == "unsupported_version":
        transport.content = '{"schema_version": 3}'
    else:
        transport.code = 44
    await reader.fetch(cached)
    await store.save(cached)
    loaded = await store.get(cached.reference.id)
    data = loaded.projection(NOW)
    assert loaded.snapshot == good
    assert loaded.last_successful_fetch == fetched
    assert data["workflow"]["state"] == "completed"
    assert data["observation"]["last_fetch_failed"]
    assert data["observation"]["status"] == problem
    assert not data["observation"]["stale"]
    transport.error, transport.code, transport.content = None, 0, json.dumps(record("failed"))
    await reader.fetch(cached)
    assert not cached.last_fetch_failed
    assert cached.problem is None
    assert cached.snapshot["state"] == "failed"


async def test_registration_is_idempotent_and_backend_scoped(tmp_path):
    store = ReferenceStore(tmp_path / "scriptflow")
    first = Reference.create("cluster", RUN_DIRECTORY + "/")
    cached = await store.register(first)
    cached.snapshot = record()
    await store.save(cached)
    same = await store.register(Reference.create("cluster", RUN_DIRECTORY))
    other = await store.register(Reference.create("other-cluster", RUN_DIRECTORY))
    assert same.snapshot == cached.snapshot
    assert other.reference.id != same.reference.id
    assert len(await store.list()) == 2
    with pytest.raises(ObservationError):
        await store.get("../../escape")


@pytest.mark.parametrize("path", ["relative/path", "/", "/data/line\nbreak"])
def test_invalid_reference_paths(path):
    with pytest.raises(ObservationError):
        Reference.create("cluster", path)


@pytest.mark.parametrize(
    "path",
    [
        "/etc/passwd",
        RUN_DIRECTORY + "-other/log.out",
        RUN_DIRECTORY + "/../outside",
        "relative.log",
        RUN_DIRECTORY + "/logs/x\n.out",
    ],
)
async def test_outside_log_refused_before_transport(path):
    raw = record()
    raw["tasks"]["first"]["attempts"][-1]["log"] = path
    cached = CachedRun(reference=Reference.create("cluster", RUN_DIRECTORY), snapshot=raw)
    transport = FakeTransport()
    with pytest.raises(ObservationError):
        await RecordReader(transport).logs(cached, "first")
    assert transport.commands == []


async def test_logs_latest_explicit_attempt_and_tail():
    cached = CachedRun(reference=Reference.create("cluster", RUN_DIRECTORY), snapshot=record())
    transport = FakeTransport("line one\nline two\n")
    reader = RecordReader(transport)
    content, number = await reader.logs(cached, "first")
    assert number == 2
    assert content == transport.content
    assert "two.out" in transport.commands[-1]
    _, number = await reader.logs(cached, "first", 1, tail=5)
    assert number == 1
    assert "one.out" in transport.commands[-1]
    assert "tail -n 5" in transport.commands[-1]
    for task, attempt in [("missing", None), ("first", 0), ("first", 3)]:
        with pytest.raises(ObservationError):
            await reader.logs(cached, task, attempt)


async def test_remote_symlink_guard_result():
    reader = RecordReader(FakeTransport(code=43))
    with pytest.raises(ObservationError, match="Resolved file"):
        await reader.read(Reference.create("cluster", RUN_DIRECTORY), RUN_DIRECTORY + "/run.json")


@pytest.mark.skipif(
    not shutil.which("realpath") or not shutil.which("sh"), reason="POSIX utilities"
)
async def test_shell_guard_with_real_local_files(tmp_path):
    class LocalFakeTransport:
        async def run_command(self, command, timeout=30):
            proc = await asyncio.create_subprocess_exec(
                "sh",
                "-c",
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            return stdout.decode(), stderr.decode(), proc.returncode

    directory = tmp_path / "run 'quoted'; $(touch injected)"
    directory.mkdir()
    inside = directory / "log 'quoted'.out"
    inside.write_text("one\ntwo\nthree\n")
    outside = tmp_path / "private.out"
    outside.write_text("outside")
    (directory / "escaped.out").symlink_to(outside)
    reference = Reference.create("cluster", str(directory))
    reader = RecordReader(LocalFakeTransport())
    assert await reader.read(reference, str(inside), tail=2) == "two\nthree\n"
    assert await reader.read(reference, str(inside), tail=0) == ""
    with pytest.raises(ObservationError, match="Resolved file"):
        await reader.read(reference, str(directory / "escaped.out"))
    assert not (tmp_path / "injected").exists()


@pytest.fixture
def cli_context(tmp_path, monkeypatch):
    from scripthut import scriptflow_cli

    config = ScriptHutConfig(
        settings=GlobalSettings(data_dir=tmp_path),
        backends=[
            SlurmBackendConfig(
                name="cluster",
                ssh=SSHConfig(
                    host="hpc-cluster.example.edu",
                    user="alice",
                ),
            )
        ],
    )
    transport = FakeTransport(json.dumps(record()))
    monkeypatch.setattr(scriptflow_cli, "load_config", lambda _: config)
    monkeypatch.setattr(cli, "_ssh_client_for", lambda _: transport)

    def forbidden(*args, **kwargs):
        pytest.fail("Legacy runtime or run storage invoked")

    from scripthut.runs.manager import RunManager
    from scripthut.runs.storage import RunStorageManager

    monkeypatch.setattr(cli, "init_runtime", forbidden)
    monkeypatch.setattr(cli, "_make_client", forbidden)
    monkeypatch.setattr(cli, "_ensure_local_server", forbidden)
    monkeypatch.setattr(RunManager, "__init__", forbidden)
    monkeypatch.setattr(RunStorageManager, "__init__", forbidden)
    return config, transport


def test_cli_register_view_list_and_no_legacy_storage(cli_context, tmp_path, capsys):
    config, transport = cli_context
    assert (
        cli.main(
            ["scriptflow", "register", "--backend", "cluster", "--run-dir", RUN_DIRECTORY, "--json"]
        )
        == 0
    )
    registered = json.loads(capsys.readouterr().out)
    reference = registered["reference_id"]
    assert registered["workflow"]["state"] == "running"
    assert cli.main(["scriptflow", "view", reference, "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["reference_id"] == reference
    assert cli.main(["scriptflow", "list", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert {p.name for p in tmp_path.iterdir()} == {"scriptflow"}
    assert transport.disconnect.await_count == 3
    assert all(
        "python" not in command and "scriptflow inspect" not in command
        for command in transport.commands
    )
    assert cli.main(["scriptflow", "view", reference]) == 0
    output = capsys.readouterr().out
    assert "cleanup_pending: false" in output and "attempt 2: running" in output


def test_cli_unreachable_retains_state(cli_context, capsys):
    _, transport = cli_context
    assert (
        cli.main(
            ["scriptflow", "register", "--backend", "cluster", "--run-dir", RUN_DIRECTORY, "--json"]
        )
        == 0
    )
    reference = json.loads(capsys.readouterr().out)["reference_id"]
    transport.error = OSError("offline")
    assert cli.main(["scriptflow", "view", reference, "--json"]) == 1
    data = json.loads(capsys.readouterr().out)
    assert data["workflow"]["state"] == "running"
    assert data["observation"]["status"] == "unreachable"


def test_first_fetch_missing_still_registers(cli_context, capsys):
    _, transport = cli_context
    transport.code = 44
    assert (
        cli.main(
            ["scriptflow", "register", "--backend", "cluster", "--run-dir", RUN_DIRECTORY, "--json"]
        )
        == 1
    )
    data = json.loads(capsys.readouterr().out)
    assert data["workflow"] is None
    assert data["observation"]["status"] == "missing"
    assert cli.main(["scriptflow", "list", "--json"]) == 1
    assert len(json.loads(capsys.readouterr().out)) == 1


def test_cli_logs(cli_context, capsys, monkeypatch):
    _, transport = cli_context
    assert (
        cli.main(
            ["scriptflow", "register", "--backend", "cluster", "--run-dir", RUN_DIRECTORY, "--json"]
        )
        == 0
    )
    reference = json.loads(capsys.readouterr().out)["reference_id"]
    original = transport.run_command

    async def read(command, timeout=30):
        if "run.json" in command:
            return await original(command, timeout)
        return "example log\n", "", 0

    monkeypatch.setattr(transport, "run_command", read)
    assert cli.main(["scriptflow", "logs", reference, "first", "--tail", "10", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["content"] == "example log\n"
    assert data["attempt"] == 2
    assert cli.main(["scriptflow", "logs", reference, "first", "--attempt", "1"]) == 0
    assert capsys.readouterr().out == "example log\n"


def test_only_read_only_verbs_and_no_server_mode():
    parser = cli.build_parser()
    for verb in ("run", "cancel", "cleanup", "retry"):
        with pytest.raises(SystemExit):
            parser.parse_args(["scriptflow", verb])
    with pytest.raises(SystemExit):
        parser.parse_args(["scriptflow", "list", "--server", "https://example.edu"])
