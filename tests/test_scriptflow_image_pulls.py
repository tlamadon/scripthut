"""Compute-side scratch, bounded preparation and early credential consumption."""

import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

from scripthut import cli
from scripthut.config_schema import EnvRule, ImageBackendConfig
from scripthut.scriptflow_cli import format_view
from scripthut.scriptflow_images import observe_images, status_path, token_path, upload_token
from scripthut.scriptflow_launch import job_script, register_launch
from tests.test_scriptflow_images import TOKEN, image_job, run_job  # noqa: F401
from tests.test_scriptflow_launch import setup  # noqa: F401


def instrument(c):
    runtime = c.check / "bin/apptainer"
    runtime.write_text("""#!/bin/bash
[ ! -e "$TOKEN_PATH" ] || exit 70
printf '%s\\n' "$APPTAINER_TMPDIR" "$TMPDIR" "$APPTAINER_CONFIGDIR" \\
    "$APPTAINER_CACHEDIR" "$3" > "$CHECK/paths"
printf 'progress %s\\n' "${APPTAINER_DOCKER_PASSWORD:-public}"
if [ "${IGNORE_TERM:-0}" = 1 ]; then trap '' TERM; fi
printf '%s\\n' "$$" > "$CHECK/runtime-pid"
printf ready > "$CHECK/pulling"
sleep "${PULL_SLEEP:-0}"
if [ "${FAIL_PULL:-0}" = 1 ]; then exit 9; fi
printf 'intact image' > "$3"
""")


@pytest.mark.parametrize("selection", ["slurm", "tmp", "fallback", "override"])
@pytest.mark.asyncio
async def test_local_scratch_selection_and_shared_files(image_job, selection):  # noqa: F811
    c = image_job
    instrument(c)
    slurm = c.check / "slurm"
    slurm.mkdir()
    tmp = c.check / "tmp"
    tmp.mkdir()
    override = c.check / "override"
    override.mkdir()
    env = {"SLURM_TMPDIR": str(slurm), "TMPDIR": str(tmp), "TOKEN_PATH": token_path(c.launch)}
    expected = slurm
    if selection == "tmp":
        env["SLURM_TMPDIR"] = str(c.check / "absent")
        expected = tmp
    elif selection == "fallback":
        env.update(SLURM_TMPDIR="", TMPDIR=str(c.check / "absent"))
        expected = Path("/tmp")
    elif selection == "override":
        c.config.backends[0].image_scratch_dir = str(override)
        expected = override
    code, out, err = await run_job(c, **env)
    assert code == 0, err
    paths = (c.check / "paths").read_text().splitlines()
    scratch = Path(paths[0])
    assert scratch.parent == expected
    assert all(Path(p).is_relative_to(scratch) for p in paths)
    assert not scratch.exists()
    assert {p.name for p in c.root.iterdir()} == {
        Path(c.launch.images[0]["path"]).name,
        Path(c.launch.images[0]["path"]).name + ".lock",
    }


@pytest.mark.parametrize(
    "outcome", ["success", "failure", "timeout", "term", "term_ignore", "timeout_ignore"]
)
@pytest.mark.asyncio
async def test_token_lifetime_progress_and_cleanup(image_job, outcome):  # noqa: F811
    c = image_job
    instrument(c)
    settings = c.config.backends[0]
    settings.registry_user = "alice"
    settings.registry_token_file = c.token
    settings.image_pull_timeout_seconds = 2 if outcome.startswith("timeout") else 20
    scratch_base = c.check / "scratch"
    scratch_base.mkdir()
    settings.image_scratch_dir = str(scratch_base)
    await upload_token(c.config, c.reader, c.launch)
    c.root.mkdir(exist_ok=True)
    survivor = c.root / "other.sif"
    survivor.write_text("finished")
    unrelated = c.root / ".other-copy"
    unrelated.write_text("other job")
    proc = await asyncio.create_subprocess_exec(
        "bash",
        "-c",
        job_script(c.config, c.launch),
        env={
            **os.environ,
            "CHECK": str(c.check),
            "SLURM_JOB_ID": "1234",
            "TOKEN_PATH": token_path(c.launch),
            "PULL_SLEEP": "10"
            if outcome in ("timeout", "term", "term_ignore", "timeout_ignore")
            else "1",
            "FAIL_PULL": "1" if outcome == "failure" else "0",
            "IGNORE_TERM": "1" if outcome.endswith("ignore") else "0",
        },
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        for _ in range(100):
            if (c.check / "pulling").exists():
                break
            await asyncio.sleep(0.02)
        assert (c.check / "pulling").exists()
        assert not Path(token_path(c.launch)).exists()
        for _ in range(40):
            progress = await observe_images(c.reader, c.launch)
            if progress.get("log_tail"):
                break
            await asyncio.sleep(0.02)
        assert progress["status"] == "preparing"
        assert "progress [redacted]" in progress["log_tail"]
        assert TOKEN not in str(progress)
        cached = await register_launch(c.store, c.launch)
        cached.image_preparation = progress
        cached.problem = "missing"
        view = cached.projection(datetime.fromtimestamp(progress["started_at"] + 1, UTC))
        assert view["image_preparation"]["elapsed_seconds"] == 1
        assert "progress [redacted]" in format_view(view)
        assert "1 seconds" in format_view(view)
        template = Environment(loader=FileSystemLoader("src/scripthut/templates")).get_template(
            "scriptflow_states.html"
        )
        assert "progress [redacted]" in template.render(run=view)
        if outcome in ("term", "term_ignore"):
            proc.terminate()
        stdout, stderr = await asyncio.wait_for(proc.communicate(), 5)
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.communicate()
    assert not Path(token_path(c.launch)).exists()
    assert TOKEN.encode() not in stdout + stderr
    pid = (c.check / "runtime-pid").read_text().strip()
    process = Path("/proc") / pid / "stat"
    for _ in range(50):
        if not process.exists() or process.read_text().split()[2] == "Z":
            break
        await asyncio.sleep(0.01)
    assert not process.exists() or process.read_text().split()[2] == "Z"
    assert list(scratch_base.iterdir()) == []
    assert not list(c.root.glob(".image-copy-*"))
    assert survivor.read_text() == "finished" and unrelated.read_text() == "other job"
    status = await observe_images(c.reader, c.launch)
    assert status["status"] == ("ready" if outcome == "success" else "failed")
    assert (c.check / "started").exists() == (outcome == "success")
    if outcome.startswith("timeout"):
        assert "timed out after 2 seconds" in status["error"]
    if outcome in ("term", "term_ignore"):
        assert "terminated by scheduler signal" in status["error"]


@pytest.mark.asyncio
async def test_token_consumed_before_activation_failure(image_job):  # noqa: F811
    c = image_job
    settings = c.config.backends[0]
    settings.registry_user = "alice"
    settings.registry_token_file = c.token
    await upload_token(c.config, c.reader, c.launch)
    settings.env_groups["workflow"] = [EnvRule(init="exit 9")]
    code, _, _ = await run_job(c)
    assert code == 9
    assert not Path(token_path(c.launch)).exists()
    assert not (c.check / "pulls").exists()


@pytest.mark.asyncio
async def test_ended_preparation_projection(image_job):  # noqa: F811
    c = image_job
    cached = await register_launch(c.store, c.launch)
    cached.problem = "missing"
    cached.controller = {"state": "ended", "observed_at": "2026-01-01T00:00:00Z"}
    cached.image_preparation = {"status": "preparing", "error": "", "started_at": 1}
    data = cached.projection()
    assert data["workflow_status"] == "not started"
    assert data["image_preparation"]["status"] == "failed"
    assert "Controller ended during image preparation" in format_view(data)
    assert cached.image_preparation["status"] == "preparing"


def test_registry_typo_cli_has_no_traceback(tmp_path, capsys):
    config = tmp_path / "config.yaml"
    config.write_text("""backends:
  - name: cluster
    type: slurm
    ssh: {host: hpc-cluster.example.edu, user: alice}
    registry_user: alice
    registry_token_path: /data/token
""")
    assert cli.main(["scriptflow", "list", "--config", str(config)]) != 0
    output = capsys.readouterr()
    assert "registry_token_file" in output.err
    assert "registry_token_path" in output.err
    assert "Traceback" not in output.err
    assert "input_value" not in output.err


@pytest.mark.parametrize(
    "settings", [{"image_scratch_dir": "relative"}, {"image_pull_timeout_seconds": 0}]
)
def test_invalid_preparation_config(settings):
    with pytest.raises(ValueError):
        ImageBackendConfig(**settings)


@pytest.mark.parametrize("controller", ["RUNNING", "COMPLETED"])
@pytest.mark.asyncio
async def test_preparation_api_pages_and_combined_read(image_job, controller):  # noqa: F811
    import shlex
    import time
    from types import SimpleNamespace

    from fastapi import FastAPI
    from fastapi.templating import Jinja2Templates
    from fastapi.testclient import TestClient

    from scripthut.scriptflow_launch import LaunchStore
    from scripthut.scriptflow_routes import make_scriptflow_router
    from scripthut.scriptflow_server import ScriptFlowService

    c = image_job
    status = Path(status_path(c.launch))
    status.parent.mkdir(parents=True, exist_ok=True)
    status.write_text(f"preparing\nstarted_at={int(time.time()) - 10}\n")
    Path(str(status) + ".log").write_text("".join(f"progress {i} [redacted]\n" for i in range(30)))
    c.launch.job_id = "1234"
    await LaunchStore(c.store.directory).save(c.launch)
    cached = await register_launch(c.store, c.launch)
    bins = c.check / "bin"
    for name, text in (("sacct", controller), ("squeue", "")):
        path = bins / name
        path.write_text("#!/bin/sh\nprintf '%s\\n' " + shlex.quote(text) + "\n")
        path.chmod(0o755)
    original = c.transport.run_command

    async def with_path(command, timeout=30):
        return await original(
            "export PATH=" + shlex.quote(str(bins)) + ":$PATH\n" + command, timeout
        )

    c.transport.run_command = with_path
    service = ScriptFlowService(
        SimpleNamespace(
            config=c.config,
            backends={"cluster": SimpleNamespace(ssh_client=c.transport, enabled=True)},
        )
    )
    await service.store.save(cached)
    before = len(c.transport.commands)
    await service.fetch(cached)
    assert len(c.transport.commands) == before + 1
    app = FastAPI()
    app.include_router(
        make_scriptflow_router(service, Jinja2Templates(directory="src/scripthut/templates"))
    )
    with TestClient(app) as client:
        data = client.get("/api/v1/scriptflow/runs/" + cached.reference.id).json()
        listing = client.get("/api/v1/scriptflow/runs").json()[0]
        page = client.get("/scriptflow/" + cached.reference.id)
    assert page.status_code == 200
    assert data["workflow_status"] == listing["workflow_status"] == "not started"
    assert data["image_preparation"]["elapsed_seconds"] >= 10
    assert len(data["image_preparation"]["log_tail"].splitlines()) == 20
    assert "progress 29 [redacted]" in page.text
    if controller == "COMPLETED":
        assert (
            data["image_preparation"]["status"]
            == listing["image_preparation"]["status"]
            == "failed"
        )
        assert "Controller ended during image preparation" in page.text
    else:
        assert data["image_preparation"]["status"] == "preparing"
        assert "seconds elapsed" in page.text
