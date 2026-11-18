"""Image preparation stays in the controller and keeps tokens out of diagnostics."""

import asyncio
import json
import os
import shlex
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from jinja2 import Environment, FileSystemLoader

from scripthut import cli
from scripthut.config_schema import EnvRule, GitSourceConfig, SourceImage
from scripthut.scriptflow import CachedRun, ObservationError, RecordReader
from scripthut.scriptflow_cli import format_view, handle_controller
from scripthut.scriptflow_images import (
    image_paths,
    observe_images,
    presence,
    resolve_images,
    status_path,
    token_path,
    upload_token,
)
from scripthut.scriptflow_launch import (
    LaunchStore,
    job_script,
    launch_run,
    make_launch,
    register_launch,
)
from scripthut.scriptflow_server import ScriptFlowService
from scripthut.scriptflow_sources import resolve_revision
from scripthut.ssh.client import SSHClient
from tests.test_scriptflow_launch import Backend, setup  # noqa: F401
from tests.test_scriptflow_sources import Shell, checkout, git  # noqa: F401

REF = "registry.example/org/app@sha256:" + "a" * 64
TOKEN = "test-secret-no-public-output"


@pytest.mark.parametrize(
    "value,match",
    [
        ({"name": "app"}, "exactly one"),
        ({"name": "app", "ref": REF, "ref_file": "image.ref"}, "exactly one"),
        ({"name": "bad-name", "ref": REF}, "pattern"),
        ({"name": "app", "ref": "registry.example/org/app:latest"}, "pinned by digest"),
        ({"name": "app", "ref": "registry.example/org/app@sha256:bad"}, "valid sha256"),
        ({"name": "app", "ref_file": "../image.ref"}, "relative"),
        ({"name": "app", "ref_file": "/image.ref"}, "relative"),
    ],
)
def test_image_config(value, match):
    with pytest.raises(ValueError, match=match):
        SourceImage(**value)


def test_tag_opt_out_and_case_collision():
    assert SourceImage(name="app", ref="registry.example/org/app:v1", allow_tag=True)
    with pytest.raises(ValueError, match="unique"):
        GitSourceConfig(
            name="example",
            url="https://example.org/repo.git",
            images=[SourceImage(name="app", ref=REF), SourceImage(name="APP", ref=REF)],
        )


@pytest.mark.asyncio
async def test_ref_file_uses_commit_and_validates_before_remote(checkout):  # noqa: F811
    c = checkout
    c.source.local_path = c.repo
    c.source.images = [SourceImage(name="app", ref_file="image.ref")]
    (c.repo / "image.ref").write_text(REF + "\n")
    git(c.repo, "add", "image.ref")
    git(c.repo, "commit", "-m", "Pin image")
    revision = await resolve_revision(c.config, c.source, None, None)
    (c.repo / "image.ref").write_text("registry.example/org/app:changed")
    resolved = await resolve_images(c.config, revision)
    assert resolved[0]["ref"] == REF
    assert c.shell.commands == []
    c.source.local_path = None
    assert await resolve_images(c.config, revision) == resolved
    git(c.repo, "add", "image.ref")
    git(c.repo, "commit", "-m", "Unpinned example")
    revision = await resolve_revision(c.config, c.source, None, None)
    with pytest.raises(ValueError, match="pinned by digest"):
        await resolve_images(c.config, revision)
    assert c.shell.commands == []


class SecretShell(Shell):
    def __init__(self):
        super().__init__()
        self.uploads = 0

    async def write_private_file(self, command, content):
        self.commands.append(command)
        self.uploads += 1
        proc = await asyncio.create_subprocess_exec(
            "bash",
            "-c",
            command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await proc.communicate(content.encode())
        assert proc.returncode == 0, (out, err)


@pytest.fixture
def image_job(setup, tmp_path):  # noqa: F811
    config, args, _, store = setup
    args.workdir = str(tmp_path / "project ' quoted")
    Path(args.workdir).mkdir()
    launch = make_launch(config, args)
    Path(launch.runs_dir).mkdir(parents=True)
    root = tmp_path / "images ' quoted"
    launch.images = [
        {"name": "app", "ref": REF, "path": str(root / ("sha256-" + "a" * 64 + ".sif"))}
    ]
    bins = tmp_path / "bin"
    bins.mkdir()
    (bins / "apptainer").write_text("""#!/bin/bash
printf '%s\\n' "$@" >> "$CHECK/argv"
printf 'pull\\n' >> "$CHECK/pulls"
printf 'runtime says %s\\n' "${APPTAINER_DOCKER_PASSWORD:-public}"
if [ "${FAIL_PULL:-0}" = 1 ]; then echo 'registry denied access' >&2; exit 9; fi
sleep 0.1
if [ "${NO_IMAGE:-0}" != 1 ]; then printf 'intact image' > "$3"; fi
""")
    (bins / "tmux").write_text("""#!/bin/bash
shift 2
if [ "$1" = new-session ]; then bash -c "$5"; else exit 1; fi
""")
    (bins / "scriptflow").write_text("""#!/bin/bash
printf '%s\\n' "$SCRIPTHUT_IMAGE_APP" > "$CHECK/started"
[ -z "${APPTAINER_DOCKER_PASSWORD:-}" ] || exit 7
""")
    for path in bins.iterdir():
        path.chmod(0o755)
    config.backends[0].env_groups["workflow"] = [
        EnvRule(init="export PATH=" + shlex.quote(str(bins)) + ":$PATH")
    ]
    local_token = tmp_path / "local-token"
    local_token.write_text(TOKEN)
    transport = SecretShell()
    return SimpleNamespace(
        config=config,
        launch=launch,
        store=store,
        root=root,
        check=tmp_path,
        token=local_token,
        reader=RecordReader(transport),
        transport=transport,
    )


async def run_job(c, launch=None, **env):
    script = job_script(c.config, launch or c.launch)
    assert TOKEN not in script
    proc = await asyncio.create_subprocess_exec(
        "bash",
        "-c",
        script,
        env={**os.environ, "CHECK": str(c.check), "SLURM_JOB_ID": "1234", **env},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return proc.returncode, stdout.decode(), stderr.decode()


@pytest.mark.parametrize("failure", [None, "FAIL_PULL", "NO_IMAGE"])
@pytest.mark.asyncio
async def test_pull_exports_token_cleanup_and_redaction(image_job, failure):
    c = image_job
    settings = c.config.backends[0]
    settings.registry_user = "alice"
    settings.registry_token_file = c.token
    await upload_token(c.config, c.reader, c.launch)
    private = Path(token_path(c.launch))
    assert private.stat().st_mode & 0o777 == 0o600
    assert private.read_text().strip() == TOKEN
    backend = Backend(c.store, c.launch)
    await LaunchStore(c.store.directory).save(c.launch)
    assert TOKEN not in (await LaunchStore(c.store.directory).get(c.launch.key)).model_dump_json()
    code, stdout, stderr = await run_job(c, **({failure: "1"} if failure else {}))
    assert not private.exists()
    assert TOKEN not in stdout + stderr + (c.check / "argv").read_text()
    assert TOKEN not in "\n".join(c.transport.commands)
    status = await observe_images(c.reader, c.launch)
    assert status["status"] == ("failed" if failure else "ready")
    if failure:
        assert code != 0 and not (c.check / "started").exists()
        assert "[redacted]" in status["error"]
        assert not Path(c.launch.run_directory).exists()
    else:
        assert code == 0
        assert Path(c.launch.images[0]["path"]).read_text() == "intact image"
        assert (c.check / "started").read_text().strip() == c.launch.images[0]["path"]
        content = (Path(c.launch.workdir) / ".sf/images.sh").read_text()
        assert (
            content
            == "export SCRIPTHUT_IMAGE_APP=" + shlex.quote(c.launch.images[0]["path"]) + "\n"
        )
    for path in (Path(c.launch.workdir) / ".sf").rglob("*"):
        if path.is_file():
            assert TOKEN not in path.read_text()
    assert backend.scripts == []


@pytest.mark.asyncio
async def test_existing_image_no_upload_no_pull(image_job):
    c = image_job
    c.root.mkdir()
    target = Path(c.launch.images[0]["path"])
    target.write_text("existing image")
    c.config.backends[0].registry_user = "alice"
    c.config.backends[0].registry_token_file = Path("/not/a/token")
    await upload_token(c.config, c.reader, c.launch)
    assert c.transport.uploads == 0
    code, _, stderr = await run_job(c)
    assert code == 0, stderr
    assert target.read_text() == "existing image"
    assert not (c.check / "pulls").exists()


@pytest.mark.asyncio
async def test_concurrent_pull_once(image_job):
    c = image_job
    other = c.launch.model_copy(
        update={
            "run_id": "second",
            "request_id": "b" * 32,
            "run_directory": c.launch.runs_dir + "/second",
            "session": "sf-second",
        }
    )
    outcomes = await asyncio.gather(run_job(c), run_job(c, other))
    assert all(code == 0 for code, _, _ in outcomes), outcomes
    assert (c.check / "pulls").read_text().splitlines() == ["pull"]
    assert Path(c.launch.images[0]["path"]).read_text() == "intact image"


@pytest.mark.asyncio
async def test_listing_paths_and_symlink_refusal(image_job):
    c = image_job
    c.config.backends[0].clone_dir = str(c.check / "repos")
    declarations = [{"name": "app", "ref": REF, "filename": "sha256-example.sif"}]
    paths = await image_paths(c.config, c.reader, "cluster", declarations)
    assert paths[0]["path"] == str(c.check / "repos/images/sha256-example.sif")
    assert (await presence(c.reader, paths))[0]["present"] is False
    target = Path(paths[0]["path"])
    target.parent.mkdir(parents=True)
    target.write_text("image")
    assert (await presence(c.reader, paths))[0]["present"] is True
    link = target.parent / "link.sif"
    link.symlink_to(target)
    with pytest.raises(ObservationError):
        await presence(c.reader, [{**paths[0], "path": str(link)}])


@pytest.mark.asyncio
async def test_failed_pull_observed_in_one_server_read(image_job):
    c = image_job
    await run_job(c, FAIL_PULL="1")
    c.launch.job_id = "1234"
    await LaunchStore(c.store.directory).save(c.launch)
    cached = await register_launch(c.store, c.launch)
    bins = c.check / "bin"
    for name, text in (("sacct", "FAILED"), ("squeue", "")):
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
    # The test's store and the server share the configured data directory.
    await service.store.save(cached)
    before = len(c.transport.commands)
    await service.fetch(cached)
    assert len(c.transport.commands) == before + 1
    data = cached.projection()
    assert data["workflow_status"] == "not started"
    assert data["controller"]["state"] == "ended"
    assert data["workflow"] is None
    assert "registry denied access" in data["image_preparation"]["error"]
    assert "registry denied access" in format_view(data)
    env = Environment(loader=FileSystemLoader("src/scripthut/templates"))
    html = env.get_template("scriptflow.html").render(run=data, runs=[])
    assert "Launch problem" in html and "registry denied access" in html and REF in html


@pytest.mark.asyncio
async def test_ssh_secret_input_is_not_logged():
    connection = SimpleNamespace(
        is_closed=lambda: False,
        run=AsyncMock(return_value=SimpleNamespace(exit_status=0, stdout=TOKEN, stderr=TOKEN)),
    )
    client = SSHClient(host="hpc-cluster.example.edu", user="alice", key_path=Path("/data/key"))
    client._connection = connection
    log = []
    client.on_command = log.append
    await client.write_private_file("cat > /data/private-token", TOKEN)
    assert connection.run.call_args.kwargs["input"] == TOKEN
    assert TOKEN not in connection.run.call_args.args[0]
    assert log == []
    connection.run.side_effect = RuntimeError(TOKEN)
    with pytest.raises(RuntimeError) as error:
        await client.write_private_file("cat > /data/private-token", TOKEN)
    assert TOKEN not in str(error.value)


@pytest.mark.asyncio
async def test_multiple_images_and_upload_retry(image_job):
    c = image_job
    c.config.backends[0].registry_user = "alice"
    c.config.backends[0].registry_token_file = c.token
    c.launch.images.append(
        {
            "name": "tools",
            "ref": REF.replace("a" * 64, "b" * 64),
            "path": str(c.root / ("sha256-" + "b" * 64 + ".sif")),
        }
    )
    await upload_token(c.config, c.reader, c.launch)
    await upload_token(c.config, c.reader, c.launch)
    code, stdout, stderr = await run_job(c)
    assert code == 0, stderr
    assert not Path(token_path(c.launch)).exists()
    assert len((c.check / "pulls").read_text().splitlines()) == 2
    assert TOKEN not in stdout + stderr + Path(status_path(c.launch) + ".log").read_text()
    assert "SCRIPTHUT_IMAGE_TOOLS" in (Path(c.launch.workdir) / ".sf/images.sh").read_text()


@pytest.mark.asyncio
async def test_launch_records_images_and_uploads_once(image_job):
    c = image_job
    c.config.backends[0].registry_user = "alice"
    c.config.backends[0].registry_token_file = c.token
    backend = Backend(c.store, c.launch)
    result = await launch_run(c.config, c.launch, c.store, backend, c.reader)
    assert len(backend.scripts) == 1 and c.transport.uploads == 1
    assert result["images"] == c.launch.images
    await launch_run(c.config, c.launch, c.store, backend, c.reader)
    assert len(backend.scripts) == 1 and c.transport.uploads == 1
    assert TOKEN not in json.dumps(result) + "".join(backend.scripts)
    for path in c.store.directory.rglob("*.json"):
        assert TOKEN not in path.read_text()
    code, _, stderr = await run_job(c)
    assert code == 0, stderr


@pytest.mark.parametrize("backend_type", ["slurm", "pbs"])
@pytest.mark.asyncio
async def test_images_cli_read_only(checkout, monkeypatch, backend_type):  # noqa: F811
    c = checkout
    c.source.local_path = c.repo
    c.source.images = [SourceImage(name="app", ref=REF)]
    if backend_type == "pbs":
        from scripthut.config_schema import PBSBackendConfig

        c.config.backends = [PBSBackendConfig(name="cluster", ssh=c.config.backends[0].ssh)]
    c.config.backends[0].image_dir = str(c.repo.parent / "images")
    monkeypatch.setattr("scripthut.ssh.config.ssh_client_for", lambda settings: c.shell)
    args = cli.build_parser().parse_args(["scriptflow", "images", "--source", "example"])
    result = await handle_controller(c.config, c.store, args)
    assert result["images"][0]["present"] is False
    assert not Path(c.config.backends[0].image_dir).exists()
    assert not c.store.directory.exists()
    assert not any("apptainer" in command or "mkdir" in command for command in c.shell.commands)


def test_backend_credentials_validation():
    from scripthut.config_schema import SlurmBackendConfig, SSHConfig

    ssh = SSHConfig(host="hpc-cluster.example.edu", user="alice")
    with pytest.raises(ValueError, match="together"):
        SlurmBackendConfig(name="cluster", ssh=ssh, registry_user="alice")
    with pytest.raises(ValueError, match="absolute"):
        SlurmBackendConfig(name="cluster", ssh=ssh, image_dir="relative")


@pytest.mark.asyncio
async def test_no_source_job_has_no_image_steps(setup):  # noqa: F811
    config, args, launch, store = setup
    script = job_script(config, launch)
    assert "apptainer" not in script and "image_token" not in script
    assert launch.images == []


@pytest.mark.asyncio
async def test_source_launch_resolves_images_before_preparing(checkout, monkeypatch):  # noqa: F811
    c = checkout
    c.source.local_path = c.repo
    c.source.images = [SourceImage(name="app", ref=REF)]
    c.config.backends[0].image_dir = str(c.repo.parent / "image-files")
    monkeypatch.setattr("scripthut.ssh.config.ssh_client_for", lambda settings: c.shell)
    monkeypatch.setattr(
        "scripthut.scriptflow_sources.SourceCheckout._local_push_target",
        lambda self, source, backend: (source.clone_dir + "/.mirror.git", ""),
    )
    submitted = AsyncMock(return_value="1234")
    monkeypatch.setattr("scripthut.scriptflow_launch.ControllerBackend.submit_job", submitted)
    args = cli.build_parser().parse_args(
        ["scriptflow", "launch", "--source", "example", "--flow", "first", "--run-id", "image-run"]
    )
    result = await handle_controller(c.config, c.store, args)
    assert result["images"][0]["ref"] == REF
    assert "apptainer pull" in submitted.call_args.args[0]
    assert not any("apptainer pull" in command for command in c.shell.commands)
    cached = await c.store.get(result["reference_id"])
    assert cached.images == result["images"]
    await handle_controller(c.config, c.store, args)
    assert submitted.await_count == 1


@pytest.mark.asyncio
async def test_failure_metadata_in_api_and_listing(image_job):
    from fastapi import FastAPI
    from fastapi.templating import Jinja2Templates
    from fastapi.testclient import TestClient

    from scripthut.scriptflow_routes import make_scriptflow_router

    c = image_job
    await run_job(c, FAIL_PULL="1")
    cached = await register_launch(c.store, c.launch)
    cached.problem = "missing"
    cached.image_preparation = await observe_images(c.reader, c.launch)
    service = ScriptFlowService(SimpleNamespace(config=c.config, backends={}))
    await service.store.save(cached)
    app = FastAPI()
    app.include_router(
        make_scriptflow_router(service, Jinja2Templates(directory="src/scripthut/templates"))
    )
    with TestClient(app) as client:
        listing = client.get("/api/v1/scriptflow/runs").json()[0]
        view = client.get("/api/v1/scriptflow/runs/" + cached.reference.id).json()
    assert listing == view
    assert view["images"] == c.launch.images and view["workflow_status"] == "not started"
    assert "registry denied access" in view["image_preparation"]["error"]


def test_image_status_does_not_invent_workflow_outcome(image_job):
    c = image_job
    cached = CachedRun(
        reference=c.launch.reference,
        launch_key=c.launch.key,
        images=c.launch.images,
        image_preparation={"status": "failed", "error": "pull failed"},
    )
    for problem in ("unreachable", "malformed", "unsupported_version"):
        cached.problem = problem
        assert cached.projection()["workflow_status"] == "unavailable"
        assert cached.projection()["workflow"] is None


@pytest.mark.asyncio
async def test_concurrent_private_pulls_delete_both_tokens(image_job):
    c = image_job
    c.config.backends[0].registry_user = "alice"
    c.config.backends[0].registry_token_file = c.token
    other = c.launch.model_copy(
        update={
            "run_id": "second",
            "request_id": "c" * 32,
            "run_directory": c.launch.runs_dir + "/second",
            "session": "sf-second",
        }
    )
    await upload_token(c.config, c.reader, c.launch)
    await upload_token(c.config, c.reader, other)
    results = await asyncio.gather(run_job(c), run_job(c, other))
    assert all(code == 0 for code, _, _ in results), results
    assert len((c.check / "pulls").read_text().splitlines()) == 1
    assert not Path(token_path(c.launch)).exists() and not Path(token_path(other)).exists()
    for launch in (c.launch, other):
        assert TOKEN not in Path(status_path(launch) + ".log").read_text()
