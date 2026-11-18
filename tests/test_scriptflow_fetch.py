"""Output copies preserve backend files and enforce both containment boundaries."""

import asyncio
import copy
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripthut import cli
from scripthut.config_schema import (
    GlobalSettings,
    ScriptFlowConfig,
    ScriptHutConfig,
    SlurmBackendConfig,
    SSHConfig,
)
from scripthut.scriptflow import Reference, ReferenceStore
from scripthut.scriptflow_cli import handle
from scripthut.scriptflow_fetch import fetch_outputs, handle_fetch
from scripthut.ssh.client import SSHClient
from tests.test_scriptflow import record


class LocalBackend:
    def __init__(self):
        self.commands = []
        self.downloads = []
        self.disconnect = AsyncMock()
        self.corrupt = False
        self.interrupt = False
        self.truncate = False
        self.before_read = None

    async def run_command(self, command, timeout=30):
        self.commands.append(command)
        process = await asyncio.create_subprocess_exec(
            "bash",
            "-c",
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await process.communicate()
        return out.decode(), err.decode(), process.returncode

    async def read_binary(self, command, stream, max_bytes):
        self.downloads.append(command)
        if self.before_read:
            self.before_read()
        process = await asyncio.create_subprocess_exec(
            "bash",
            "-c",
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await process.communicate()
        if process.returncode:
            raise RuntimeError("Remote read refused")
        if len(out) > max_bytes:
            raise RuntimeError("Size cap exceeded during transfer")
        stream.write(b"X" * len(out) if self.corrupt else out[:-1] if self.truncate else out)
        if self.interrupt:
            raise RuntimeError("Connection lost")


def tree(path):
    result = {}
    for entry in [path, *path.rglob("*")]:
        stat = entry.lstat()
        result[str(entry.relative_to(path))] = (
            stat.st_mode,
            stat.st_mtime_ns,
            os.readlink(entry)
            if entry.is_symlink()
            else entry.read_bytes()
            if entry.is_file()
            else None,
        )
    return result


@pytest.fixture
def example(tmp_path):
    root = tmp_path / "backend ' project"
    root.mkdir()
    (root / "stage").mkdir()
    (root / "stage/a.bin").write_bytes(bytes(range(256)) * 4)
    (root / "stage/b.txt").write_text("result\n")
    (root / "stage/empty").mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_text("outside sentinel")
    return SimpleNamespace(
        root=root, outside=outside, local=tmp_path / "local", client=LocalBackend()
    )


async def fetch(c, paths, **kwargs):
    before = tree(c.root), tree(c.outside)
    result = await fetch_outputs(c.client, str(c.root), paths, str(c.local), **kwargs)
    assert (tree(c.root), tree(c.outside)) == before
    return result


@pytest.mark.asyncio
async def test_directory_binary_strip_and_empty_directory(example):
    c = example
    (c.root / "stage/zero").write_bytes(b"")
    result = await fetch(c, ["stage"], strip="stage")
    assert result["ok"], result
    assert (c.local / "a.bin").read_bytes() == bytes(range(256)) * 4
    assert (c.local / "b.txt").read_text() == "result\n"
    assert (c.local / "empty").is_dir()
    assert (c.local / "zero").read_bytes() == b""
    assert not (c.local / "stage").exists()
    assert result["files"][1]["sha256"] or result["files"][2]["sha256"]


@pytest.mark.asyncio
async def test_overwrite_atomic_and_integrity_failures(example):
    c = example
    first = await fetch(c, ["stage/a.bin"])
    assert first["ok"]
    destination = c.local / "stage/a.bin"
    destination.write_bytes(b"old")
    assert not (await fetch(c, ["stage/a.bin"]))["ok"]
    c.client.before_read = lambda: (
        destination.read_bytes() == b"old" or pytest.fail("early overwrite")
    )
    for corrupt, interrupt in [(True, False), (False, True)]:
        c.client.corrupt, c.client.interrupt = corrupt, interrupt
        failed = await fetch(c, ["stage/a.bin"], overwrite=True)
        assert not failed["ok"]
        assert destination.read_bytes() == b"old"
        assert not list(c.local.rglob(".scripthut-fetch-*"))
    c.client.corrupt = c.client.interrupt = False
    c.client.truncate = True
    assert not (await fetch(c, ["stage/a.bin"], overwrite=True))["ok"]
    assert destination.read_bytes() == b"old"
    c.client.truncate = False
    assert (await fetch(c, ["stage/a.bin"], overwrite=True))["ok"]
    assert destination.read_bytes() == (c.root / "stage/a.bin").read_bytes()


@pytest.mark.parametrize(
    "path", ["../outside/secret", "/data/outside", "escape", "stage/../../outside/secret"]
)
@pytest.mark.asyncio
async def test_remote_escape_refused(example, path):
    c = example
    (c.root / "escape").symlink_to(c.outside / "secret")
    result = await fetch(c, [path])
    assert not result["ok"]
    assert c.client.downloads == []
    assert not c.local.exists()


@pytest.mark.asyncio
async def test_directory_with_escape_and_contained_file_link(example):
    c = example
    (c.root / "stage/escape").symlink_to(c.outside, target_is_directory=True)
    (c.root / "stage/link").symlink_to(c.root / "stage/b.txt")
    result = await fetch(c, ["stage"])
    assert not result["ok"]
    assert (c.local / "stage/link").read_text() == "result\n"
    assert not (c.local / "stage/escape").exists()


@pytest.mark.parametrize("location", ["root", "parent", "file"])
@pytest.mark.asyncio
async def test_local_symlink_escape_refused(example, location):
    c = example
    if location == "root":
        c.local.symlink_to(c.outside, target_is_directory=True)
        with pytest.raises(OSError):
            await fetch(c, ["stage/a.bin"], overwrite=True)
        return
    c.local.mkdir()
    if location == "parent":
        (c.local / "stage").symlink_to(c.outside, target_is_directory=True)
    else:
        (c.local / "stage").mkdir()
        (c.local / "stage/a.bin").symlink_to(c.outside / "secret")
    assert not (await fetch(c, ["stage/a.bin"], overwrite=True))["ok"]
    assert (c.outside / "secret").read_text() == "outside sentinel"


@pytest.mark.parametrize("strip", ["../escape", "/absolute"])
@pytest.mark.asyncio
async def test_invalid_strip(example, strip):
    with pytest.raises(ValueError):
        await fetch(example, ["stage/a.bin"], strip=strip)
    assert not example.local.exists()


@pytest.mark.asyncio
async def test_local_parent_traversal(example):
    with pytest.raises(ValueError):
        await fetch_outputs(
            example.client, str(example.root), ["stage/a.bin"], str(example.local / "../escape")
        )
    assert example.client.downloads == []


@pytest.mark.asyncio
async def test_caps_missing_and_dry_run(example):
    c = example
    result = await fetch(c, ["stage/a.bin", "absent", "stage/b.txt"], file_limit=10, dry_run=True)
    assert not result["ok"]
    assert "fetch_max_file_bytes" in result["files"][0]["reason"]
    assert result["files"][-1]["status"] == "would_fetch"
    assert not c.local.exists() and not c.client.downloads
    result = await fetch(c, ["stage/a.bin", "stage/b.txt"], total_limit=1024)
    assert not result["ok"]
    assert "fetch_max_total_bytes" in result["files"][-1]["reason"]
    assert not (c.local / "stage/b.txt").exists()
    result = await fetch(c, ["absent", "stage/b.txt"])
    assert not result["ok"]
    assert (c.local / "stage/b.txt").exists()


@pytest.mark.asyncio
async def test_strip_mismatch_and_dry_run_existing(example):
    c = example
    assert not (await fetch(c, ["stage/b.txt"], strip="other", dry_run=True))["ok"]
    assert not c.local.exists()
    assert (await fetch(c, ["stage/b.txt"]))["ok"]
    before = tree(c.local)
    result = await fetch(c, ["stage/b.txt", "stage/a.bin"], dry_run=True)
    assert not result["ok"] and "overwrite" in result["files"][0]["reason"]
    assert tree(c.local) == before


@pytest.mark.asyncio
async def test_selection_cli_and_source_root(example, tmp_path, monkeypatch, capsys):
    from scripthut.scriptflow_launch import LaunchStore, make_launch, register_launch

    c = example
    config = ScriptHutConfig(
        backends=[
            SlurmBackendConfig(
                name="cluster",
                ssh=SSHConfig(host="hpc-cluster.example.edu", user="alice"),
                env_groups={"workflow": []},
            )
        ],
        settings=GlobalSettings(data_dir=str(tmp_path / "state")),
        scriptflow=ScriptFlowConfig(env_group="workflow"),
    )
    store = ReferenceStore(config.settings.data_dir_resolved / "scriptflow")
    run_dir = c.root / ".sf/runs/run"
    run_dir.mkdir(parents=True)
    raw = record()
    raw["working_directory"] = str(c.root)
    raw["tasks"]["first"]["outputs"] = [str(c.root / "stage/a.bin")]
    raw["tasks"]["second"] = copy.deepcopy(raw["tasks"]["first"])
    raw["tasks"]["second"].update(
        task_id="second", name="other", outputs=[str(c.root / "stage/b.txt")]
    )
    (run_dir / "run.json").write_text(json.dumps(raw))
    cached = await store.register(Reference.create("cluster", str(run_dir)))
    monkeypatch.setattr("scripthut.ssh.config.ssh_client_for", lambda backend: c.client)
    monkeypatch.setattr("scripthut.scriptflow_cli.load_config", lambda _: config)
    parser = cli.build_parser()
    args = parser.parse_args(
        ["scriptflow", "fetch", cached.reference.id, "--to", str(c.local), "--json", "--dry-run"]
    )
    before = tree(c.root), tree(store.directory)
    assert await handle(args) == 0
    data = json.loads(capsys.readouterr().out)
    assert len(data["files"]) == 2 and not c.local.exists()
    assert (tree(c.root), tree(store.directory)) == before
    args.task = ["example"]
    data = await handle_fetch(config, store, args)
    assert len(data["files"]) == 1 and data["files"][0]["path"].endswith("a.bin")
    args.task = ["second", "first"]
    assert len((await handle_fetch(config, store, args))["files"]) == 2
    args.task = ["unknown"]
    with pytest.raises(RuntimeError):
        await handle_fetch(config, store, args)
    args.task = []
    args.path = ["absent", "stage/b.txt"]
    assert await handle(args) == 1
    capsys.readouterr()
    launch_args = SimpleNamespace(
        backend="cluster",
        workdir=str(c.root / "analysis"),
        flow="main",
        run_id="source",
        runs_dir=None,
    )
    launch = make_launch(config, launch_args)
    launch.source = "example"
    launch.repository_workdir = str(c.root)
    launch.flow_dir = "analysis"
    Path(launch.run_directory).mkdir(parents=True)
    raw["working_directory"] = launch.workdir
    Path(launch.run_directory, "run.json").write_text(json.dumps(raw))
    await LaunchStore(store.directory).save(launch)
    source = await register_launch(store, launch)
    args.reference = source.reference.id
    args.path = ["stage/b.txt"]
    assert (await handle_fetch(config, store, args))["ok"]
    launch.source = None
    await LaunchStore(store.directory).save(launch)
    assert not (await handle_fetch(config, store, args))["ok"]


def test_parser_selection_conflict():
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(
            [
                "scriptflow",
                "fetch",
                "reference",
                "--to",
                "/data/results",
                "--task",
                "first",
                "--path",
                "file",
            ]
        )


@pytest.mark.asyncio
async def test_binary_connection_stream_not_logged():
    payload = bytes(range(256))
    process = SimpleNamespace(
        stdout=SimpleNamespace(read=AsyncMock(side_effect=[payload, b""])),
        wait_closed=AsyncMock(),
        exit_status=0,
        close=lambda: None,
    )
    connection = SimpleNamespace(
        is_closed=lambda: False, create_process=AsyncMock(return_value=process)
    )
    client = SSHClient(host="hpc-cluster.example.edu", user="alice", key_path=Path("/data/key"))
    client._connection = connection
    client.on_command = lambda entry: pytest.fail("Binary payload logging")
    destination = io.BytesIO()
    await client.read_binary("read-contained-file", destination, len(payload))
    assert destination.getvalue() == payload
    assert connection.create_process.call_args.kwargs["encoding"] is None
    process.stdout.read = AsyncMock(return_value=payload)
    with pytest.raises(RuntimeError, match="size cap"):
        await client.read_binary("read-contained-file", io.BytesIO(), 1)


@pytest.mark.asyncio
async def test_contained_directory_symlink(example):
    c = example
    (c.root / "alias").symlink_to(c.root / "stage", target_is_directory=True)
    result = await fetch(c, ["alias"])
    assert result["ok"], result
    assert (c.local / "alias/a.bin").read_bytes() == (c.root / "stage/a.bin").read_bytes()


@pytest.mark.asyncio
async def test_concurrent_local_writer_is_preserved(example):
    c = example

    def concurrent_write():
        (c.local / "stage/b.txt").write_text("other writer")

    c.client.before_read = concurrent_write
    result = await fetch(c, ["stage/b.txt"])
    assert not result["ok"]
    assert (c.local / "stage/b.txt").read_text() == "other writer"
    assert not list(c.local.rglob(".scripthut-fetch-*"))


@pytest.mark.asyncio
async def test_local_symlink_changed_during_transfer(example):
    c = example

    def replace_target():
        (c.local / "stage/b.txt").symlink_to(c.outside / "secret")

    c.client.before_read = replace_target
    result = await fetch(c, ["stage/b.txt"], overwrite=True)
    assert not result["ok"]
    assert (c.outside / "secret").read_text() == "outside sentinel"
    assert not list(c.local.rglob(".scripthut-fetch-*"))


@pytest.mark.parametrize("field", ["fetch_max_file_bytes", "fetch_max_total_bytes"])
def test_fetch_caps_positive(field):
    with pytest.raises(ValueError):
        GlobalSettings(**{field: 0})
