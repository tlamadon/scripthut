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
        prefix = "0\n" if "scripthut-read" in command and "run.json" in command else ""
        return prefix + self.content, "", self.code


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
    monkeypatch.setattr("scripthut.ssh.config.ssh_client_for", lambda _: transport)

    def forbidden(*args, **kwargs):
        pytest.fail("Legacy runtime or run storage invoked")


    monkeypatch.setattr(cli, "init_runtime", forbidden)
    monkeypatch.setattr(cli, "_make_client", forbidden)
    monkeypatch.setattr(cli, "_ensure_local_server", forbidden)
    return config, transport


def test_cli_register_view_list(cli_context, tmp_path, capsys):
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
    assert cli.main(["scriptflow", "list", "--json"]) == 0
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


def test_no_execution_verbs_or_server_mode():
    parser = cli.build_parser()
    for verb in ("run", "cleanup", "retry"):
        with pytest.raises(SystemExit):
            parser.parse_args(["scriptflow", verb])
    with pytest.raises(SystemExit):
        parser.parse_args(["scriptflow", "list", "--server", "https://example.edu"])


class CancelTransport(FakeTransport):
    def __init__(self, records):
        super().__init__()
        self.records = records
        self.present = False
        self.writes = 0

    async def run_command(self, command, timeout=30):
        self.commands.append(command)
        if "scripthut-cancel" in command:
            if self.present:
                return "already", "", 0
            self.writes += 1
            self.present = True
            return "requested", "", 0
        raw = self.records.pop(0) if len(self.records) > 1 else self.records[0]
        if isinstance(raw, Exception):
            raise raw
        return f"{int(self.present)}\n" + json.dumps(raw), "", 0


def fresh_record(state="running", cleanup=False):
    raw = record(state)
    raw["heartbeat"] = datetime.now(UTC).isoformat()
    raw["cleanup_pending"] = cleanup
    return raw


async def cancel_context(tmp_path, transport, wait=None):
    from scripthut.scriptflow_cli import cancel

    store = ReferenceStore(tmp_path / "scriptflow")
    cached = await store.register(Reference.create("cluster", RUN_DIRECTORY))
    reader = RecordReader(transport)
    await reader.fetch(cached)
    await store.save(cached)
    return await cancel(reader, cached, store, wait), store, cached


async def test_cancel_requested_and_repeat(tmp_path):
    transport = CancelTransport([fresh_record()])
    (data, failed), store, cached = await cancel_context(tmp_path, transport)
    assert not failed
    assert data["cancel_result"]["status"] == "requested"
    assert data["cancellation"]["requested"]
    assert (await store.get(cached.reference.id)).cancel_requested_at is not None
    (data, failed), _, _ = await cancel_context(tmp_path, transport)
    assert not failed and "already requested" in data["cancel_result"]["message"]
    assert transport.writes == 1
    assert all(
        not any(word in command for word in ("scancel", "qdel", "scriptflow cancel", "python"))
        for command in transport.commands
    )


@pytest.mark.parametrize("state", ["completed", "failed", "cancelled", "interrupted"])
@pytest.mark.parametrize("cleanup", [False, True])
async def test_cancel_terminal_no_write(tmp_path, state, cleanup):
    transport = CancelTransport([fresh_record(state, cleanup)])
    (data, failed), _, _ = await cancel_context(tmp_path, transport)
    assert not failed and transport.writes == 0
    assert data["workflow"]["state"] == state
    assert data["workflow"]["cleanup_pending"] is cleanup
    assert all("scripthut-cancel" not in c for c in transport.commands)


async def test_cancel_unreadable_no_write(tmp_path):
    transport = CancelTransport([OSError("offline")])
    (data, failed), _, _ = await cancel_context(tmp_path, transport)
    assert failed and data["cancel_result"]["status"] == "unreachable"
    assert transport.writes == 0


@pytest.mark.parametrize("state", ["cancelled", "completed", "failed"])
async def test_wait_confirmed_preserves_outcome(tmp_path, monkeypatch, state):
    from scripthut import scriptflow_cli

    monkeypatch.setattr(scriptflow_cli.asyncio, "sleep", AsyncMock())
    transport = CancelTransport([fresh_record(), fresh_record(state)])
    (data, failed), _, _ = await cancel_context(tmp_path, transport, 180)
    assert not failed and data["cancel_result"]["status"] == "confirmed"
    assert data["workflow"]["state"] == state


@pytest.mark.parametrize(
    "raw,status",
    [
        (fresh_record(), "pending"),
        (fresh_record("cancelled", True), "pending"),
        (record(), "controller_not_responding"),
    ],
)
async def test_wait_pending_or_stale(tmp_path, raw, status):
    transport = CancelTransport([raw])
    (data, failed), _, _ = await cancel_context(tmp_path, transport, 0)
    assert failed and data["cancel_result"]["status"] == status
    if status == "controller_not_responding":
        assert "scriptflow cleanup example-run" in data["cancel_result"]["message"]
        assert data["workflow"]["state"] == "running"


async def test_wait_unreachable(tmp_path, monkeypatch):
    from scripthut import scriptflow_cli

    monkeypatch.setattr(scriptflow_cli.asyncio, "sleep", AsyncMock())
    transport = CancelTransport([fresh_record(), OSError("offline")])
    (data, failed), _, _ = await cancel_context(tmp_path, transport, 180)
    assert failed and data["cancel_result"]["status"] == "unreachable"
    assert data["workflow"]["state"] == "running"
    assert data["cancellation"]["requested"]


async def test_cancel_shell_atomic_containment_and_no_clobber(tmp_path):
    class LocalTransport:
        async def run_command(self, command, timeout=30):
            process = await asyncio.create_subprocess_exec(
                "sh", "-c", command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            return stdout.decode(), stderr.decode(), process.returncode

    directory = tmp_path / "run 'quoted'"
    directory.mkdir()
    (directory / "run.json").write_text(json.dumps(fresh_record()))
    cached = CachedRun(reference=Reference.create("cluster", str(directory)))
    reader = RecordReader(LocalTransport())
    await reader.fetch(cached)
    assert cached.cancel_present is False
    assert await reader.request_cancel(cached)
    path = directory / "cancel.json"
    original = path.read_text()
    assert datetime.fromisoformat(json.loads(original)["requested_at"]).utcoffset() == timedelta(0)
    assert not await reader.request_cancel(cached)
    assert path.read_text() == original
    assert not list(directory.glob(".cancel.*"))
    await reader.fetch(cached)
    assert cached.cancel_present is True
    path.unlink()
    outside = tmp_path / "outside.json"
    outside.write_text("untouched")
    path.symlink_to(outside)
    with pytest.raises(ObservationError, match="outside"):
        await reader.request_cancel(cached)
    assert outside.read_text() == "untouched"
    await reader.fetch(cached)
    assert cached.problem == "unsafe_path"


async def test_task_names_and_id_precedence():
    raw = record()
    reader = RecordReader(FakeTransport("log"))
    cached = CachedRun(reference=Reference.create("cluster", RUN_DIRECTORY), snapshot=raw)
    assert await reader.logs(cached, "example") == ("log", 2)
    raw["tasks"]["second"] = copy.deepcopy(raw["tasks"]["first"])
    raw["tasks"]["second"]["task_id"] = "second"
    with pytest.raises(ObservationError, match="first, second"):
        await reader.logs(cached, "example")
    raw["tasks"]["second"]["name"] = "first"
    assert await reader.logs(cached, "first") == ("log", 2)


def test_unregister_local_only(cli_context, capsys, tmp_path):
    _, transport = cli_context
    cli.main(
        ["scriptflow", "register", "--backend", "cluster", "--run-dir", RUN_DIRECTORY, "--json"]
    )
    reference = json.loads(capsys.readouterr().out)["reference_id"]
    transport.commands.clear()
    assert cli.main(["scriptflow", "unregister", reference, "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "unregistered"
    assert not list((tmp_path / "scriptflow").glob("*.json"))
    assert not transport.commands


def test_cli_cancel_default_wait(cli_context, capsys, monkeypatch):
    cli.main(
        ["scriptflow", "register", "--backend", "cluster", "--run-dir", RUN_DIRECTORY, "--json"]
    )
    reference = json.loads(capsys.readouterr().out)["reference_id"]
    transport = CancelTransport([fresh_record()])
    monkeypatch.setattr("scripthut.ssh.config.ssh_client_for", lambda _: transport)
    assert cli.main(["scriptflow", "cancel", reference, "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["cancel_result"]["status"] == "requested"
    args = cli.build_parser().parse_args(["scriptflow", "cancel", reference, "--wait"])
    assert args.wait == 180
    assert cli.main(["scriptflow", "cancel", reference, "--wait", "0", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["cancel_result"]["status"] == "pending"


def test_quiet_logging_restored(cli_context, caplog, capsys, monkeypatch):
    import logging

    _, transport = cli_context
    original = transport.run_command

    async def read(command, timeout=30):
        logging.getLogger("asyncssh").info("connection detail")
        logging.getLogger("scripthut.ssh.client").warning("connection warning")
        return await original(command, timeout)

    monkeypatch.setattr(transport, "run_command", read)
    with caplog.at_level(logging.INFO):
        cli.main(["scriptflow", "register", "--backend", "cluster", "--run-dir", RUN_DIRECTORY])
        assert "connection detail" not in caplog.text
        assert "connection warning" in caplog.text
        caplog.clear()
        cli.main(["scriptflow", "list", "--verbose"])
        assert "connection detail" in caplog.text


@pytest.mark.parametrize("content,code", [("broken", 0), ('{"schema_version": 9}', 0), ("", 44)])
def test_cli_cancel_bad_record_no_write(cli_context, capsys, content, code):
    _, transport = cli_context
    cli.main(
        ["scriptflow", "register", "--backend", "cluster", "--run-dir", RUN_DIRECTORY, "--json"]
    )
    reference = json.loads(capsys.readouterr().out)["reference_id"]
    transport.content, transport.code = content, code
    transport.commands.clear()
    assert cli.main(["scriptflow", "cancel", reference, "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["cancel_result"]["status"] == "unreachable"
    assert len(transport.commands) == 1
    assert "scripthut-cancel" not in transport.commands[0]


def test_list_unreachable_succeeds(cli_context, capsys):
    _, transport = cli_context
    cli.main(["scriptflow", "register", "--backend", "cluster", "--run-dir", RUN_DIRECTORY])
    capsys.readouterr()
    transport.error = OSError("offline")
    assert cli.main(["scriptflow", "list"]) == 0
    assert "unreachable" in capsys.readouterr().out


async def test_unregister_broken_cache(tmp_path):
    store = ReferenceStore(tmp_path)
    reference = Reference.create("cluster", RUN_DIRECTORY)
    path = tmp_path / f"{reference.id}.json"
    path.write_text("broken")
    await store.unregister(reference.id)
    assert not path.exists()
    with pytest.raises(ObservationError, match="not registered"):
        await store.unregister(reference.id)
