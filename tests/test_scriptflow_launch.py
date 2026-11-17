"""Controller ownership and conservative launch reconciliation."""

import argparse
import asyncio
import json
import os
import shlex
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from scripthut import cli
from scripthut.config_schema import (
    EnvRule,
    GlobalSettings,
    ScriptFlowConfig,
    ScriptHutConfig,
    SlurmBackendConfig,
    SSHConfig,
)
from scripthut.scriptflow import (
    CachedRun,
    ObservationError,
    RecordReader,
    ReferenceStore,
)
from scripthut.scriptflow_launch import (
    ControllerBackend,
    LaunchStore,
    control,
    controller_observation,
    discover,
    job_script,
    launch_run,
    make_launch,
    register_launch,
)
from tests.test_scriptflow import fresh_record


@pytest.fixture
def setup(tmp_path):
    config = ScriptHutConfig(
        settings=GlobalSettings(data_dir=tmp_path),
        scriptflow=ScriptFlowConfig(env_group="workflow"),
        backends=[
            SlurmBackendConfig(
                name="cluster",
                ssh=SSHConfig(host="hpc-cluster.example.edu", user="alice"),
                env_groups={
                    "workflow": [
                        EnvRule(set={"EXAMPLE": "a 'quote'"}, init="source /data/venv/bin/activate")
                    ]
                },
            )
        ],
    )
    args = argparse.Namespace(
        backend="cluster",
        workdir="/data/project",
        flow="analysis",
        run_id="example",
        runs_dir=None,
        cpus=None,
        memory=None,
        time_limit=None,
        partition=None,
    )
    launch = make_launch(config, args)
    store = ReferenceStore(tmp_path / "scriptflow")
    return config, args, launch, store


class Transport:
    def __init__(self):
        self.commands = []
        self.receipt = None
        self.existing = False
        self.raw = fresh_record()
        self.before = None
        self.disconnect = AsyncMock()

    async def run_command(self, command, timeout=30):
        if self.before:
            await self.before(command)
        self.commands.append(command)
        if "scripthut-guard" in command:
            return "", "", 73 if self.existing else 0
        if "run.json" in command:
            return "0\n" + json.dumps(self.raw), "", 0
        if ".receipt" in command:
            return (self.receipt, "", 0) if self.receipt else ("", "", 44)
        return "", "", 0


class Backend:
    def __init__(self, store, launch):
        self.store, self.launch = store, launch
        self.scripts = []
        self.jobs = {}
        self.error = None
        self.cancelled = []

    async def submit_job(self, script):
        saved = await LaunchStore(self.store.directory).get(self.launch.key)
        assert saved.stage == "submitting" or saved.cleanup_stage == "submitting"
        self.scripts.append(script)
        if self.error:
            raise self.error
        return "1234"

    async def query(self, name, since):
        if isinstance(self.jobs, Exception):
            raise self.jobs
        return self.jobs

    async def cancel_job(self, job_id):
        self.cancelled.append(job_id)


async def test_launch_persisted_before_remote_and_submits_once(setup):
    config, args, launch, store = setup
    transport = Transport()

    async def check(command):
        assert await LaunchStore(store.directory).get(launch.key) is not None

    transport.before = check
    backend = Backend(store, launch)
    first = await launch_run(config, launch, store, backend, RecordReader(transport))
    assert first["job_id"] == "1234"
    before = len(transport.commands)
    second = await launch_run(
        config, make_launch(config, args), store, backend, RecordReader(transport)
    )
    assert second == first and len(backend.scripts) == 1
    assert len(transport.commands) == before
    cached = await store.get(launch.reference.id)
    assert cached.launch_key == launch.key


async def test_existing_directory_never_submits(setup):
    config, _, launch, store = setup
    transport = Transport()
    transport.existing = True
    backend = Backend(store, launch)
    with pytest.raises(ObservationError):
        await launch_run(config, launch, store, backend, RecordReader(transport))
    assert not backend.scripts


@pytest.mark.parametrize(
    "jobs,receipt,outcome",
    [
        ({"1234": "RUNNING"}, None, "acknowledged"),
        ({}, "1234\ncompute.example.edu\nsession\n2026-11-17T00:00:00Z\nrunning\n", "acknowledged"),
        ({}, None, "ambiguous"),
        ({"1": "RUNNING", "2": "PENDING"}, None, "unknown"),
        (OSError("query failed"), None, "unknown"),
    ],
)
async def test_lost_ack_reconciliation(setup, jobs, receipt, outcome):
    config, _, launch, store = setup
    transport = Transport()
    backend = Backend(store, launch)
    backend.error = OSError("lost acknowledgement")
    result = await launch_run(config, launch, store, backend, RecordReader(transport))
    assert result["status"] == "ambiguous"
    backend.error = None
    backend.jobs, transport.receipt = jobs, receipt
    if outcome == "unknown":
        with pytest.raises(ObservationError, match="unknown"):
            await launch_run(config, launch, store, backend, RecordReader(transport))
    else:
        result = await launch_run(config, launch, store, backend, RecordReader(transport))
        assert result["status"] == outcome
    assert len(backend.scripts) == 1


async def test_explicit_resubmission_only(setup):
    config, _, launch, store = setup
    backend, transport = Backend(store, launch), Transport()
    backend.error = OSError("lost ack")
    await launch_run(config, launch, store, backend, RecordReader(transport))
    backend.error = None
    await launch_run(config, launch, store, backend, RecordReader(transport), resubmit=True)
    assert len(backend.scripts) == 2


async def test_default_id_and_explicit_id_reuse(setup):
    config, args, _, store = setup
    args.run_id = None
    launch = make_launch(config, args)
    backend, reader = Backend(store, launch), RecordReader(Transport())
    first = await launch_run(config, launch, store, backend, reader)
    assert (await launch_run(config, make_launch(config, args), store, backend, reader)) == first
    args.run_id = first["run_id"]
    assert (await launch_run(config, make_launch(config, args), store, backend, reader)) == first
    assert len(backend.scripts) == 1


def test_script_contract_and_configuration(setup):
    config, args, launch, _ = setup
    args.flow = "flow 'quoted'"
    launch = make_launch(config, args)
    script = job_script(config, launch)
    assert "source /data/venv/bin/activate" in script
    assert "while tmux" in script and "has-session" in script
    assert "test ! -e /data/project/.sf/runs/example" in script
    assert launch.receipt.startswith(launch.runs_dir + "/.scripthut-")
    assert not launch.receipt.startswith(launch.run_directory + "/")
    assert "scriptflow run" in script and "mkdir --" in script and "rc=$?" in script
    config.scriptflow.env_group = "missing"
    with pytest.raises(ValueError, match="not defined"):
        make_launch(config, args)


@pytest.mark.parametrize(
    "values",
    [{"cpus": 0}, {"memory": "2G\n#SBATCH --x"}, {"time_limit": "bad"}, {"partition": "a\nb"}],
)
def test_resources_validated(values):
    with pytest.raises(ValueError):
        ScriptFlowConfig(**values)


@pytest.mark.parametrize(
    "state,expected",
    [("PENDING", "queued"), ("RUNNING", "running"), ("TIMEOUT", "ended"), (None, "unknown")],
)
async def test_controller_states_independent(setup, state, expected):
    _, _, launch, store = setup
    launch.job_id = "1234"
    backend = Backend(store, launch)
    backend.jobs = {"1234": state} if state else {}
    observed = await controller_observation(backend, RecordReader(Transport()), launch)
    assert observed["state"] == expected and observed["observed_at"]
    cached = CachedRun(
        reference=launch.reference,
        launch_key=launch.key,
        problem="missing",
        last_fetch_failed=True,
        controller=observed,
    )
    assert cached.projection()["workflow_status"] == "not started"
    assert cached.projection()["workflow"] is None


async def test_cleanup_release_guards_and_owned_job_only(setup):
    config, _, launch, store = setup
    launch.job_id = "1234"
    await LaunchStore(store.directory).save(launch)
    cached = await register_launch(store, launch)
    backend, transport = Backend(store, launch), Transport()
    backend.jobs = {"1234": "RUNNING"}
    with pytest.raises(ValueError, match="ended"):
        await control(config, cached, store, backend, RecordReader(transport), "cleanup")
    with pytest.raises(ValueError, match="terminal"):
        await control(config, cached, store, backend, RecordReader(transport), "release")
    assert not backend.scripts and not backend.cancelled
    await control(config, cached, store, backend, RecordReader(transport), "release", force=True)
    assert backend.cancelled == ["1234"]
    backend.jobs = {"1234": "TIMEOUT"}
    result = await control(config, cached, store, backend, RecordReader(transport), "cleanup")
    assert result["status"] == "pending"
    assert "scriptflow cleanup" in backend.scripts[0]
    assert "scancel" not in backend.scripts[0]
    transport.receipt = "5555\ncompute.example.edu\nsession\n2026-11-17T00:00:00Z\n0\n"
    result = await control(config, cached, store, backend, RecordReader(transport), "cleanup")
    assert result["status"] == "completed" and len(backend.scripts) == 1


async def test_backend_query_failure_absence_and_cancel_status():
    transport = Transport()
    transport.run_command = AsyncMock(return_value=("", "failed", 1))
    backend = ControllerBackend(transport)
    with pytest.raises(ObservationError):
        await backend.query("sf-example", "2026-11-17")
    with pytest.raises(ObservationError):
        await backend.cancel_job("1234")
    transport.run_command.return_value = ("", "", 0)
    assert await backend.query("sf-example", "2026-11-17") == {}
    transport.run_command.return_value = ("Submitted batch job 1234", "", 0)
    assert await backend.submit_job("#!/bin/bash\ntrue") == "1234"


class LocalTransport:
    async def run_command(self, command, timeout=30):
        process = await asyncio.create_subprocess_exec(
            "bash", "-c", command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        return stdout.decode(), stderr.decode(), process.returncode


async def test_discovery_local_files_and_register_once(tmp_path):
    root = tmp_path / "runs"
    root.mkdir()
    store = ReferenceStore(tmp_path / "cache")
    reader = RecordReader(LocalTransport())
    assert await discover(reader, store, "cluster", "/data", str(root), False) == []
    for index, name in enumerate(("old", "new", "unreadable", "unsupported")):
        directory = root / name
        directory.mkdir()
        if name != "unreadable":
            raw = fresh_record()
            raw["run_id"] = name
            if name == "unsupported":
                raw["schema_version"] = 99
            (directory / "run.json").write_text(json.dumps(raw))
        os.utime(directory, (100 + index, 100 + index))
    results = await discover(reader, store, "cluster", "/data", str(root), True)
    assert [r["run_id"] for r in results] == ["unsupported", "unreadable", "new", "old"]
    assert results[0]["observation"]["status"] == "unsupported_version"
    assert results[1]["observation"]["status"] == "missing"
    before = {p.name: p.read_text() for p in store.directory.glob("*.json")}
    await discover(reader, store, "cluster", "/data", str(root), True)
    assert before == {p.name: p.read_text() for p in store.directory.glob("*.json")}
    with pytest.raises(ObservationError, match="missing"):
        await discover(reader, store, "cluster", "/data", str(root / "missing"), False)


@pytest.mark.parametrize("exit_code", [0, 7])
async def test_generated_script_runs_locally_with_fake_tmux(setup, tmp_path, exit_code):
    config, args, _, _ = setup
    workdir = tmp_path / "work 'quoted'"
    workdir.mkdir()
    args.workdir = str(workdir)
    args.flow = "analysis 'quoted'"
    args.force_pattern = "task*'; echo BAD"
    launch = make_launch(config, args)
    Path(launch.runs_dir).mkdir(parents=True)
    bins = tmp_path / "bin"
    bins.mkdir()
    (bins / "tmux").write_text(
        '#!/bin/bash\nshift 2\nif [ "$1" = new-session ]; then bash -c "$5"; else exit 1; fi\n'
    )
    (bins / "scriptflow").write_text(
        '#!/bin/bash\nprintf "%s\\n" "$@" > "$CAPTURE"\n'
        'test "$EXAMPLE" = "a quote" || exit 7\nexit "$FAKE_EXIT"\n'
    )
    for path in bins.iterdir():
        path.chmod(0o755)
    config.backends[0].env_groups["workflow"] = [
        EnvRule(set={"EXAMPLE": "a quote"}, init=f"export PATH={shlex.quote(str(bins))}:$PATH")
    ]
    script = tmp_path / "controller.sh"
    script.write_text(job_script(config, launch))
    process = await asyncio.create_subprocess_exec(
        "bash",
        str(script),
        env={
            **os.environ,
            "SLURM_JOB_ID": "1234",
            "CAPTURE": str(tmp_path / "args"),
            "FAKE_EXIT": str(exit_code),
        },
    )
    assert await process.wait() == exit_code
    receipt = Path(launch.receipt).read_text().splitlines()
    assert receipt[0] == "1234" and receipt[-1] == str(exit_code)
    assert (tmp_path / "args").read_text().splitlines() == [
        "run",
        "analysis 'quoted'",
        "-f",
        "task*'; echo BAD",
        "--run-id",
        "example",
        "--runs-dir",
        launch.runs_dir,
    ]
    assert not Path(launch.run_directory).exists()
    process = await asyncio.create_subprocess_exec(
        "bash",
        str(script),
        env={**os.environ, "SLURM_JOB_ID": "1234"},
        stderr=asyncio.subprocess.DEVNULL,
    )
    assert await process.wait() == 73


def test_cli_launch_no_legacy(setup, monkeypatch, capsys):
    from scripthut import scriptflow_cli, scriptflow_launch
    from scripthut.runs.manager import RunManager
    from scripthut.runs.storage import RunStorageManager

    config, _, launch, store = setup
    transport = Transport()
    backend = Backend(store, launch)

    async def submit(script):
        backend.scripts.append(script)
        return "1234"

    backend.submit_job = submit
    monkeypatch.setattr(scriptflow_cli, "load_config", lambda _: config)
    monkeypatch.setattr(cli, "_ssh_client_for", lambda _: transport)
    monkeypatch.setattr(scriptflow_launch, "ControllerBackend", lambda _: backend)

    def forbidden(*args, **kwargs):
        pytest.fail("Legacy runtime invoked")

    for target, name in [
        (cli, "init_runtime"),
        (RunManager, "__init__"),
        (RunStorageManager, "__init__"),
    ]:
        monkeypatch.setattr(target, name, forbidden)
    assert (
        cli.main(
            [
                "scriptflow",
                "launch",
                "--backend",
                "cluster",
                "--workdir",
                "/data/project",
                "--flow",
                "analysis",
                "--run-id",
                "example",
                "--json",
            ]
        )
        == 0
    )
    data = json.loads(capsys.readouterr().out)
    assert data["job_id"] == "1234" and len(backend.scripts) == 1


def test_ended_stale_workflow_notice(setup):
    from tests.test_scriptflow import record

    _, _, launch, _ = setup
    cached = CachedRun(
        reference=launch.reference,
        launch_key=launch.key,
        snapshot=record(),
        controller={"state": "ended", "observed_at": "2026-11-17T00:00:00Z"},
    )
    data = cached.projection()
    assert data["workflow"]["state"] == "running"
    assert data["observation"]["stale"]
    assert "Task jobs were left running" in data["controller_notice"]
    assert "scriptflow cleanup" in data["controller_notice"]


async def test_cleanup_unknown_refused_and_release_terminal(setup):
    config, _, launch, store = setup
    launch.job_id = "1234"
    await LaunchStore(store.directory).save(launch)
    cached = await register_launch(store, launch)
    backend, transport = Backend(store, launch), Transport()
    with pytest.raises(ValueError, match="unknown"):
        await control(config, cached, store, backend, RecordReader(transport), "cleanup")
    transport.raw = fresh_record("completed")
    await control(config, cached, store, backend, RecordReader(transport), "release")
    assert backend.cancelled == ["1234"] and not backend.scripts


async def test_release_ended_no_cancel(setup):
    config, _, launch, store = setup
    launch.job_id = "1234"
    await LaunchStore(store.directory).save(launch)
    cached = await register_launch(store, launch)
    backend = Backend(store, launch)
    backend.jobs = {"1234": "COMPLETED"}
    result = await control(config, cached, store, backend, RecordReader(Transport()), "release")
    assert result["status"] == "already_ended" and not backend.cancelled


def test_force_pattern_stored_and_quoted(setup):
    config, args, _, _ = setup
    args.force_pattern = "task*'; echo BAD"
    launch = make_launch(config, args)
    assert launch.force_pattern == args.force_pattern
    script = job_script(config, launch)
    assert "-f" in script and "task*" in script
    assert "set +u\n" in script
    assert script.index("set +u") < script.index("source /data") < script.index("set -eu")


async def test_activation_unset_allowed_failure_stops(setup, tmp_path):
    config, _, launch, _ = setup
    for init, expected in [
        ('echo "$UNDEFINED_ACTIVATION_VARIABLE"\nexit 0', 0),
        ("false\necho should-not-run", 1),
    ]:
        config.backends[0].env_groups["workflow"] = [EnvRule(init=init)]
        path = tmp_path / "job.sh"
        path.write_text(job_script(config, launch))
        process = await asyncio.create_subprocess_exec(
            "bash", str(path), stdout=asyncio.subprocess.PIPE
        )
        stdout, _ = await process.communicate()
        assert process.returncode == expected and b"should-not-run" not in stdout
