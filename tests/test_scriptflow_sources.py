"""Source checkouts preserve user work and pin controller launches."""

import argparse
import asyncio
import json
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from jinja2 import Environment, FileSystemLoader

from scripthut import cli
from scripthut.config_schema import GitSourceConfig
from scripthut.scriptflow import RecordReader
from scripthut.scriptflow_cli import format_view, handle_controller
from scripthut.scriptflow_launch import LaunchStore, launch_run, make_launch
from scripthut.scriptflow_sources import (
    SourceCheckout,
    flow_workdir,
    list_flows,
    resolve_revision,
    source_rows,
    workdir_for,
)
from tests.test_scriptflow import fresh_record
from tests.test_scriptflow_launch import Backend, setup  # noqa: F401


def git(path, *args):
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "Example",
        "GIT_AUTHOR_EMAIL": "alice@example.org",
        "GIT_COMMITTER_NAME": "Example",
        "GIT_COMMITTER_EMAIL": "alice@example.org",
    }
    result = subprocess.run(
        ["git", "-C", str(path), *args], env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


class Shell:
    def __init__(self):
        self.commands = []
        self.disconnect = AsyncMock()

    async def run_command(self, command, timeout=30):
        self.commands.append(command)
        proc = await asyncio.create_subprocess_exec(
            "bash", "-c", command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        out, err = await proc.communicate()
        return out.decode(), err.decode(), proc.returncode


def tree(path):
    return {str(p.relative_to(path)): p.read_bytes() for p in path.rglob("*") if p.is_file()}


@pytest.fixture
def checkout(setup, tmp_path):  # noqa: F811
    config, args, _, store = setup
    repo = tmp_path / "origin"
    repo.mkdir()
    git(repo, "init", "-b", "main")
    (repo / "sflow.py").write_text("def flow_first():\n    pass\n\ndef flow_second():\n    pass\n")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "Initial example")
    source = GitSourceConfig(name="example", url=str(repo), clone_dir=str(tmp_path / "clones"))
    config.sources = [source]
    args.source = source.name
    args.branch = None
    args.commit = None
    args.snapshot = False
    args.resubmit_ambiguous = False
    args.scriptflow_cmd = "launch"
    shell = Shell()
    return SimpleNamespace(
        config=config,
        args=args,
        store=store,
        source=source,
        repo=repo,
        shell=shell,
        reader=RecordReader(shell),
        directory=Path(source.working_directory),
    )


async def proposed(c, run_id="example", commit=None, snapshot=False, branch=None):
    revision = await resolve_revision(c.config, c.source, branch, commit)
    args = argparse.Namespace(**vars(c.args))
    directory = await workdir_for(c.reader, c.source, revision.commit if snapshot else None)
    args.workdir = flow_workdir(directory, c.source)
    args.run_id = run_id
    launch = make_launch(c.config, args)
    launch.source = c.source.name
    launch.branch = revision.branch
    launch.commit = revision.commit
    launch.repository_workdir = directory
    launch.flow_dir = c.source.flow_dir
    launch.detached = revision.detached
    launch.snapshot = snapshot
    return launch, revision


async def launch(c, run_id="example", **kwargs):
    item, revision = await proposed(c, run_id, **kwargs)
    backend = Backend(c.store, item)

    async def prepare(saved):
        persisted = await LaunchStore(c.store.directory).get(saved.key)
        assert persisted.commit == revision.commit
        assert persisted.source == c.source.name
        await SourceCheckout(c.config).prepare(c.reader, saved, revision)

    result = await launch_run(c.config, item, c.store, backend, c.reader, prepare=prepare)
    return result, backend, item


def complete_previous(c):
    if c.directory.exists():
        record = c.directory / ".sf/runs/example"
        record.mkdir(parents=True, exist_ok=True)
        (record / "run.json").write_text(json.dumps(fresh_record("completed")))


def add_commit(c, content="def flow_new():\n    pass\n"):
    complete_previous(c)
    (c.repo / "sflow.py").write_text(content)
    git(c.repo, "add", ".")
    git(c.repo, "commit", "-m", "Next example")
    return git(c.repo, "rev-parse", "HEAD")


@pytest.mark.parametrize("flow_dir", ["../outside", "/outside", "a/../b", "a\n"])
def test_invalid_flow_directory(flow_dir):
    with pytest.raises(ValueError, match="flow_dir"):
        GitSourceConfig(name="example", url="https://example.org/repo.git", flow_dir=flow_dir)


def test_directory_defaults_and_validation():
    s = GitSourceConfig(name="example", url="https://example.org/repo.git")
    assert s.working_directory == "~/scripthut-repos/.worktrees/source-example"
    assert s.flow_dir == "."
    assert "/.worktrees/" in s.working_directory
    assert s.model_copy(update={"workdir": "/data/work"}).working_directory == "/data/work"
    with pytest.raises(ValueError, match="workdir"):
        GitSourceConfig(name="example", url=s.url, workdir="relative")


@pytest.mark.asyncio
async def test_first_launch_and_repeat(checkout):
    c = checkout
    result, backend, item = await launch(c)
    assert len(backend.scripts) == 1
    assert git(c.directory, "symbolic-ref", "--short", "HEAD") == "main"
    assert git(c.directory, "rev-parse", "HEAD") == item.commit
    assert git(c.directory, "config", "branch.main.merge") == "refs/heads/main"
    commands = list(c.shell.commands)
    files = tree(c.directory)
    result2, backend2, _ = await launch(c)
    assert result2 == result
    assert backend2.scripts == []
    assert c.shell.commands == commands
    assert tree(c.directory) == files
    cached = await c.store.get(result["reference_id"])
    assert cached.projection()["source"]["commit"] == item.commit
    assert item.commit[:12] in format_view(cached.projection())


@pytest.mark.asyncio
async def test_fast_forward_preserves_outputs(checkout):
    c = checkout
    await launch(c)
    (c.directory / "output.dat").write_bytes(b"keep output")
    record = c.directory / ".sf/runs/manual"
    record.mkdir(parents=True)
    raw = fresh_record()
    raw["state"] = "completed"
    (record / "run.json").write_text(json.dumps(raw))
    contents = (record / "run.json").read_bytes()
    commit = add_commit(c)
    await launch(c, "next")
    assert git(c.directory, "rev-parse", "HEAD") == commit
    assert git(c.directory, "symbolic-ref", "--short", "HEAD") == "main"
    assert (c.directory / "output.dat").read_bytes() == b"keep output"
    assert (record / "run.json").read_bytes() == contents
    git(c.directory, "pull", "--ff-only")


@pytest.mark.asyncio
async def test_explicit_commit_detaches(checkout):
    c = checkout
    old = git(c.repo, "rev-parse", "HEAD")
    add_commit(c)
    result, _, item = await launch(c, commit=old)
    assert result["source"]["detached"]
    assert git(c.directory, "rev-parse", "HEAD") == old
    assert (c.directory / ".git/HEAD").read_text().strip() == old
    assert item.detached


@pytest.mark.parametrize(
    "reason", ["dirty", "staged", "running", "stale", "diverged", "wrong", "untracked", "ignored"]
)
@pytest.mark.asyncio
async def test_refusal_preserves_checkout(checkout, reason):
    c = checkout
    await launch(c)
    new = "def flow_new():\n    pass\n"
    if reason in ("untracked", "ignored"):
        (c.repo / "output.dat").write_text("tracked later")
    add_commit(c, new)
    if reason == "dirty":
        (c.directory / "sflow.py").write_text("local edit")
    elif reason == "staged":
        (c.directory / "sflow.py").write_text("staged edit")
        git(c.directory, "add", ".")
    elif reason in ("running", "stale"):
        raw = fresh_record()
        raw["state"] = "running"
        if reason == "stale":
            raw["heartbeat"] = (datetime.now(UTC) - timedelta(minutes=2)).isoformat()
        record = c.directory / ".sf/runs/manual"
        record.mkdir(parents=True)
        (record / "run.json").write_text(json.dumps(raw))
    elif reason == "diverged":
        (c.directory / "local.txt").write_text("local commit")
        git(c.directory, "add", "local.txt")
        git(c.directory, "commit", "-m", "Local example")
    elif reason == "wrong":
        git(c.directory, "remote", "set-url", "origin", "/data/other.git")
    else:
        (c.directory / "output.dat").write_text("my output")
        if reason == "ignored":
            (c.directory / ".git/info/exclude").write_text("output.dat\n")
    before = tree(c.directory)
    item, revision = await proposed(c, "next")
    backend = Backend(c.store, item)
    with pytest.raises(ValueError) as error:
        await launch_run(
            c.config,
            item,
            c.store,
            backend,
            c.reader,
            prepare=lambda saved: SourceCheckout(c.config).prepare(c.reader, saved, revision),
        )
    assert str(c.directory) in str(error.value)
    if reason == "stale":
        assert "stale heartbeat" in str(error.value) and "scriptflow cleanup" in str(error.value)
    assert tree(c.directory) == before
    assert backend.scripts == []


@pytest.mark.asyncio
async def test_flow_subdirectory_snapshot_and_postclone(checkout):
    c = checkout
    (c.repo / "flows").mkdir()
    git(c.repo, "mv", "sflow.py", "flows/sflow.py")
    git(c.repo, "commit", "-m", "Flow directory")
    c.source.flow_dir = "flows"
    c.source.postclone = "printf prepared > postclone.txt"
    result, backend, item = await launch(c, snapshot=True)
    directory = Path(item.repository_workdir)
    assert directory.name == item.commit[:12]
    assert item.workdir == str(directory / "flows")
    assert item.runs_dir == str(directory / "flows/.sf/runs")
    assert (directory / "postclone.txt").read_text() == "prepared"
    assert "cd -- " + item.workdir in backend.scripts[0]
    assert result["source"]["flow_dir"] == "flows"


@pytest.mark.asyncio
async def test_flow_symlink_escape_is_refused(checkout):
    c = checkout
    (c.repo / "flows").symlink_to(c.repo, target_is_directory=True)
    git(c.repo, "add", "flows")
    git(c.repo, "commit", "-m", "External flow directory")
    c.source.flow_dir = "flows"
    with pytest.raises(ValueError, match="escapes"):
        await launch(c)
    assert not c.directory.exists()


@pytest.mark.asyncio
async def test_local_repo_push_reuses_delivery(checkout, monkeypatch):
    c = checkout
    c.source.local_path = c.repo
    monkeypatch.setattr(
        SourceCheckout,
        "_local_push_target",
        lambda self, source, backend: (source.clone_dir + "/.mirror.git", ""),
    )
    await launch(c)
    assert git(c.directory, "rev-parse", "HEAD") == git(c.repo, "rev-parse", "HEAD")
    add_commit(c)
    await launch(c, "next")
    git(c.directory, "pull", "--ff-only")
    assert git(c.directory, "symbolic-ref", "--short", "HEAD") == "main"


@pytest.mark.parametrize(
    "subdir,content,expected",
    [
        (".", "def flow_one():\n pass\ndef flow_two(x):\n pass\n", ["one", "two"]),
        ("analysis", "async def flow_three():\n pass\n", ["three"]),
        (".", "def helper():\n pass\n", []),
        (".", None, None),
    ],
)
@pytest.mark.asyncio
async def test_local_flow_names(checkout, subdir, content, expected):
    c = checkout
    c.source.local_path = c.repo
    c.source.flow_dir = subdir
    (c.repo / "sflow.py").unlink()
    target = c.repo / subdir / "sflow.py"
    target.parent.mkdir(exist_ok=True)
    if content is not None:
        target.write_text(content)
    git(c.repo, "add", "-A")
    git(c.repo, "commit", "-m", "Flow definitions")
    revision = await resolve_revision(c.config, c.source, None, None)
    if expected is None:
        with pytest.raises(ValueError, match="sflow.py"):
            await list_flows(c.config, revision)
    else:
        assert await list_flows(c.config, revision) == expected


@pytest.mark.asyncio
async def test_cli_source_launch_flows_discover_and_pages(checkout, monkeypatch):
    c = checkout
    from scripthut.scriptflow_launch import ControllerBackend

    monkeypatch.setattr("scripthut.ssh.config._ssh_client_for", lambda settings: c.shell)
    submit = AsyncMock(return_value="123")
    monkeypatch.setattr(ControllerBackend, "submit_job", submit)
    parser = cli.build_parser()
    args = parser.parse_args(
        ["scriptflow", "launch", "--source", "example", "--flow", "first", "--run-id", "cli"]
    )
    result = await handle_controller(c.config, c.store, args)
    assert result["job_id"] == "123"
    assert (await handle_controller(c.config, c.store, args))["job_id"] == "123"
    assert submit.await_count == 1
    args = parser.parse_args(["scriptflow", "flows", "--source", "example"])
    assert (await handle_controller(c.config, c.store, args))["flows"] == ["first", "second"]
    args = parser.parse_args(["scriptflow", "discover", "--source", "example"])
    assert await handle_controller(c.config, c.store, args) == []
    rows = await source_rows(
        c.config, {"cluster": SimpleNamespace(ssh_client=c.shell, enabled=True)}
    )
    env = Environment(loader=FileSystemLoader("src/scripthut/templates"), autoescape=True)
    from scripthut.scriptflow_web import configure_templates
    configure_templates(SimpleNamespace(env=env))
    html = env.get_template("sources.html").render(
        source_rows=rows, source_configs={c.source.name: c.source},
        source_workflows={c.source.name: rows[0]["flows"]}, source_statuses={},
        disabled_sources=set(), backend_names=["cluster"],
    )
    assert str(c.directory) in html and "first" in html and "second" in html
    run = (await c.store.get(result["reference_id"])).projection()
    html = env.get_template("scriptflow.html").render(run=run, runs=[])
    assert run["source"]["short_commit"] in html and "Branch: main" in html
    html = env.get_template("scriptflow.html").render(run=None, runs=[run])
    assert run["source"]["short_commit"] in html


def test_source_workdir_exclusive():
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(
            [
                "scriptflow",
                "launch",
                "--source",
                "example",
                "--workdir",
                "/data/project",
                "--flow",
                "first",
            ]
        )


@pytest.mark.asyncio
async def test_queued_controller_blocks_checkout_move(checkout):
    c = checkout
    await launch(c)
    (c.repo / "sflow.py").write_text("def flow_later():\n    pass\n")
    git(c.repo, "add", ".")
    git(c.repo, "commit", "-m", "Later example")
    before = tree(c.directory)
    with pytest.raises(ValueError, match="previous controller"):
        await launch(c, "next")
    assert tree(c.directory) == before


@pytest.mark.asyncio
async def test_custom_path_quotes_and_branch(checkout):
    c = checkout
    c.source.workdir = str(c.directory.parent / "custom ' checkout")
    c.directory = Path(c.source.workdir)
    git(c.repo, "checkout", "-b", "experiment")
    result, _, item = await launch(c, branch="experiment")
    assert Path(item.workdir) == c.directory
    assert result["source"]["branch"] == "experiment"
    assert git(c.directory, "symbolic-ref", "--short", "HEAD") == "experiment"


@pytest.mark.asyncio
async def test_unreadable_record_blocks_preparation(checkout):
    c = checkout
    await launch(c)
    complete_previous(c)
    record = c.directory / ".sf/runs/manual"
    record.mkdir()
    (record / "run.json").write_text("not json")
    before = tree(c.directory)
    with pytest.raises(ValueError, match="cannot verify run"):
        await launch(c, "next")
    assert tree(c.directory) == before


@pytest.mark.asyncio
async def test_api_carries_source_metadata(checkout):
    from fastapi import FastAPI
    from fastapi.templating import Jinja2Templates
    from fastapi.testclient import TestClient

    from scripthut.scriptflow_routes import make_scriptflow_router
    from scripthut.scriptflow_server import ScriptFlowService

    c = checkout
    result, _, _ = await launch(c)
    state = SimpleNamespace(config=c.config, backends={}, wait_for_poll=AsyncMock())
    service = ScriptFlowService(state)
    service_store = service.store
    cached = await c.store.get(result["reference_id"])
    await service_store.save(cached)
    app = FastAPI()
    app.include_router(
        make_scriptflow_router(service, Jinja2Templates(directory="src/scripthut/templates"))
    )
    with TestClient(app) as client:
        rows = client.get("/api/v1/scriptflow/runs").json()
        one = client.get("/api/v1/scriptflow/runs/" + cached.reference.id).json()
    assert rows[0]["source"] == one["source"] == result["source"]


@pytest.mark.asyncio
async def test_deploy_key_delivery_and_cleanup(checkout):
    c = checkout
    key = c.repo.parent / "example-key"
    key.write_text("example key material")
    c.source.deploy_key = key
    c.source.clone_dir = str(c.repo.parent / "clone directory")
    c.directory = Path(c.source.working_directory)
    c.shell.openssh_version = AsyncMock(return_value=(9, 2))
    await launch(c)
    assert git(c.directory, "rev-parse", "HEAD") == git(c.repo, "rev-parse", "HEAD")
    assert any("GIT_SSH_COMMAND=" in cmd and "git clone" in cmd for cmd in c.shell.commands)
    cleanup = [cmd for cmd in c.shell.commands if cmd.startswith("rm -f /tmp/scripthut_key_")]
    assert len(cleanup) == 1
    assert not Path(cleanup[0].removeprefix("rm -f ")).exists()


@pytest.mark.asyncio
async def test_lost_acknowledgement_does_not_prepare_twice(checkout):
    c = checkout
    item, revision = await proposed(c)
    backend = Backend(c.store, item)
    backend.error = OSError("lost reply")
    prepare = AsyncMock(side_effect=lambda saved: None)

    async def actual_prepare(saved):
        await SourceCheckout(c.config).prepare(c.reader, saved, revision)

    prepare.side_effect = actual_prepare
    first = await launch_run(c.config, item, c.store, backend, c.reader, prepare=prepare)
    assert first["status"] == "ambiguous"
    backend.jobs = {"123": "RUNNING"}
    second = await launch_run(c.config, item, c.store, backend, c.reader, prepare=prepare)
    assert second["job_id"] == "123"
    assert prepare.await_count == 1
    assert len(backend.scripts) == 1


@pytest.mark.asyncio
async def test_advancing_to_new_branch(checkout):
    c = checkout
    await launch(c)
    git(c.repo, "checkout", "-b", "experiment")
    commit = add_commit(c)
    await launch(c, "next", branch="experiment")
    assert git(c.directory, "symbolic-ref", "--short", "HEAD") == "experiment"
    assert git(c.directory, "rev-parse", "HEAD") == commit
    git(c.directory, "pull", "--ff-only")
