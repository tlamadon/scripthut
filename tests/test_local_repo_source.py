"""Local Git source validation, metadata and backend object delivery."""

from __future__ import annotations
from scripthut.sources.delivery import SourceDelivery

import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from scripthut.config_schema import (
    GitSourceConfig,
    GlobalSettings,
    ScriptHutConfig,
    SlurmBackendConfig,
    SSHConfig,
)
from scripthut.sources.git import GitSourceManager

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _git(*args: str, cwd: Path) -> str:
    """Run git with a fixed identity so no global config is needed."""
    proc = subprocess.run(
        ["git", "-c", "user.email=t@e.st", "-c", "user.name=t", *args],
        cwd=cwd, check=True, capture_output=True, text=True,
    )
    return proc.stdout.strip()


class _ScriptedSSH:
    """SSH mock returning pre-canned responses in order, recording commands."""

    def __init__(self, responses: list[tuple[str, str, int]] | None = None):
        self.responses = list(responses or [])
        self.commands: list[str] = []

    async def run_command(self, cmd: str, timeout: int = 30) -> tuple[str, str, int]:
        self.commands.append(cmd)
        if self.responses:
            return self.responses.pop(0)
        return ("", "", 0)

    def find(self, needle: str) -> str:
        """The one recorded command containing ``needle``."""
        hits = [c for c in self.commands if needle in c]
        assert len(hits) == 1, f"{needle!r} matched {len(hits)} of {self.commands}"
        return hits[0]


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A local git repo with one workflow file and a sentinel to guard.

    ``keep-me.txt`` is untracked-but-ignored on purpose: it is the canary
    for "did anything delete or clean this directory".
    """
    r = tmp_path / "myproject"
    wf = r / ".hut" / "workflows"
    wf.mkdir(parents=True)
    (wf / "train.json").write_text(json.dumps({"title": "train", "tasks": []}))
    (r / "code.py").write_text("print('hi')\n")
    (r / ".gitignore").write_text("keep-me.txt\n")
    _git("init", "-b", "main", ".", cwd=r)
    _git("add", "-A", cwd=r)
    _git("commit", "-m", "initial", cwd=r)
    (r / "keep-me.txt").write_text("untracked, ignored, precious\n")
    return r


def _manager(repo: Path, tmp_path: Path, **kw: Any) -> GitSourceManager:
    mgr = GitSourceManager(cache_dir=tmp_path / "cache")
    mgr.add_source(GitSourceConfig(name="demo", local_path=repo, **kw))
    return mgr


def _delivery(
    source: GitSourceConfig, tmp_path: Path, backends: list[Any],
) -> SourceDelivery:
    config = ScriptHutConfig(
        sources=[source],
        backends=backends,
        settings=GlobalSettings(sources_cache_dir=tmp_path / "cache"),
    )
    return SourceDelivery(config)


def _slurm(tmp_path: Path, port: int = 22) -> SlurmBackendConfig:
    return SlurmBackendConfig(
        name="cluster",
        ssh=SSHConfig(
            host="login.example", user="me", port=port,
            key_path=tmp_path / "id_ed25519",
        ),
    )


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class TestSchema:
    def test_local_path_alone_is_enough(self, tmp_path: Path):
        """The whole point: a source with no git remote at all."""
        src = GitSourceConfig(name="demo", local_path=tmp_path / "r")
        assert src.url == ""
        assert src.local_path_resolved == tmp_path / "r"

    def test_url_alone_still_works(self):
        src = GitSourceConfig(name="demo", url="git@h:o/r.git")
        assert src.local_path is None and src.local_path_resolved is None

    def test_neither_is_rejected(self):
        """Silently accepting a source with no code location would surface
        much later as a confusing clone failure."""
        with pytest.raises(ValueError, match="local_path"):
            GitSourceConfig(name="demo")

    def test_tilde_is_expanded(self):
        src = GitSourceConfig(name="demo", local_path="~/git/x")
        assert src.local_path_resolved == Path.home() / "git" / "x"


# ---------------------------------------------------------------------------
# GitSourceManager reads the repo in place and never writes to it
# ---------------------------------------------------------------------------


class TestReadInPlace:
    async def test_sync_does_not_touch_the_repo(self, repo: Path, tmp_path: Path):
        """``clone_source`` starts with ``shutil.rmtree(status.path)``. For a
        local source that path is the user's repository, so the guard that
        returns before it is the one thing that must never regress."""
        mgr = _manager(repo, tmp_path)
        head_before = _git("rev-parse", "HEAD", cwd=repo)

        status = await mgr.sync_source("demo")

        assert status.cloned, status.error
        assert status.path == repo
        assert (repo / "keep-me.txt").read_text().startswith("untracked")
        assert (repo / "code.py").exists()
        assert _git("rev-parse", "HEAD", cwd=repo) == head_before
        # No cache copy was made either.
        assert not (tmp_path / "cache" / "demo").exists()

    async def test_repeated_sync_is_still_harmless(self, repo: Path, tmp_path: Path):
        """The periodic background sync calls this on a loop; pull_source
        must not `git pull` in the user's tree either."""
        mgr = _manager(repo, tmp_path)
        await mgr.sync_source("demo")
        status = await mgr.sync_source("demo")  # now takes the pull_source path
        assert status.cloned
        assert (repo / "keep-me.txt").exists()

    async def test_reports_real_head_commit(self, repo: Path, tmp_path: Path):
        mgr = _manager(repo, tmp_path)
        status = await mgr.sync_source("demo")
        expected = _git("rev-parse", "--short", "HEAD", cwd=repo)
        assert status.last_commit == expected
        assert status.last_commit_date

    async def test_missing_path_is_an_error_not_a_crash(self, tmp_path: Path):
        mgr = GitSourceManager(cache_dir=tmp_path / "cache")
        mgr.add_source(GitSourceConfig(name="demo", local_path=tmp_path / "nope"))
        status = await mgr.sync_source("demo")
        assert not status.cloned
        assert "does not exist" in (status.error or "")

    async def test_non_repo_path_is_an_error(self, tmp_path: Path):
        plain = tmp_path / "plain"
        plain.mkdir()
        mgr = GitSourceManager(cache_dir=tmp_path / "cache")
        mgr.add_source(GitSourceConfig(name="demo", local_path=plain))
        status = await mgr.sync_source("demo")
        assert not status.cloned
        assert "not a git repository" in (status.error or "")


class TestDirtyTree:
    async def test_clean_tree_has_no_warning(self, repo: Path, tmp_path: Path):
        mgr = _manager(repo, tmp_path)
        status = await mgr.sync_source("demo")
        assert status.dirty is False

    async def test_tracked_edit_is_dirty(self, repo: Path, tmp_path: Path):
        (repo / "code.py").write_text("print('edited')\n")
        mgr = _manager(repo, tmp_path)
        status = await mgr.sync_source("demo")
        assert status.dirty is True

    async def test_gitignored_file_is_not_dirty(self, repo: Path, tmp_path: Path):
        """Build artefacts would otherwise warn on every single sync."""
        mgr = _manager(repo, tmp_path)
        status = await mgr.sync_source("demo")
        assert status.dirty is False  # keep-me.txt is ignored


# ---------------------------------------------------------------------------
# Pushing the repo to the backend
# ---------------------------------------------------------------------------


class TestPushLocalRepo:
    async def test_full_push_sequence(self, repo: Path, tmp_path: Path):
        src = GitSourceConfig(
            name="demo", local_path=repo, clone_dir="~/scripthut-repos",
            postclone="uv sync",
        )
        mgr = _delivery(src, tmp_path, [_slurm(tmp_path)])
        ssh = _ScriptedSSH()
        sha = _git("rev-parse", "HEAD", cwd=repo)
        short = sha[:12]

        pushed: list[str] = []

        async def fake_shell(cmd: str, timeout: float = 60.0):
            pushed.append(cmd)
            if "rev-parse" in cmd:
                return (sha + "\n", "", 0)
            return ("", "", 0)

        with patch.object(SourceDelivery, "_source_shell", staticmethod(fake_shell)):
            clone_path, got = await mgr._push_local_repo(ssh, src, "cluster")  # type: ignore[arg-type]

        assert (clone_path, got) == (f"~/scripthut-repos/{short}", short)

        # The mirror is created before anything is pushed into it.
        init = ssh.find("init --bare")
        assert "~/scripthut-repos/.mirror.git" in init
        assert "mkdir -p ~/scripthut-repos" in init

        push = next(c for c in pushed if "push --force" in c)
        assert f"{sha}:refs/heads/sh-{short}" in push
        assert "me@login.example:~/scripthut-repos/.mirror.git" in push
        assert "GIT_SSH_COMMAND=" in push

        clone = ssh.find("git clone")
        assert f"--branch sh-{short}" in clone
        assert "~/scripthut-repos/.mirror.git" in clone
        assert f"~/scripthut-repos/{short}" in clone

        assert "uv sync" in ssh.find("uv sync")

    async def test_existing_clone_is_reused(self, repo: Path, tmp_path: Path):
        """Content-addressed by commit, so re-running the same commit must not
        re-clone or re-run postclone."""
        src = GitSourceConfig(
            name="demo", local_path=repo, clone_dir="/repos", postclone="uv sync",
        )
        mgr = _delivery(src, tmp_path, [_slurm(tmp_path)])
        sha = _git("rev-parse", "HEAD", cwd=repo)
        ssh = _ScriptedSSH([
            ("", "", 0),            # mkdir + mirror init
            ("exists\n", "", 0),    # test -d <clone_path>
        ])

        async def fake_shell(cmd: str, timeout: float = 60.0):
            return ((sha + "\n", "", 0) if "rev-parse" in cmd else ("", "", 0))

        with patch.object(SourceDelivery, "_source_shell", staticmethod(fake_shell)):
            clone_path, short = await mgr._push_local_repo(ssh, src, "cluster")  # type: ignore[arg-type]

        assert clone_path == f"/repos/{sha[:12]}"
        assert not any("git clone" in c for c in ssh.commands)
        assert not any("uv sync" in c for c in ssh.commands)


    async def test_non_default_port_rides_in_the_ssh_command(
        self, repo: Path, tmp_path: Path,
    ):
        """scp-style URLs have nowhere to put a port."""
        src = GitSourceConfig(name="demo", local_path=repo, clone_dir="/repos")
        mgr = _delivery(src, tmp_path, [_slurm(tmp_path, port=2222)])
        ssh = _ScriptedSSH()
        sha = _git("rev-parse", "HEAD", cwd=repo)
        pushed: list[str] = []

        async def fake_shell(cmd: str, timeout: float = 60.0):
            pushed.append(cmd)
            return ((sha + "\n", "", 0) if "rev-parse" in cmd else ("", "", 0))

        with patch.object(SourceDelivery, "_source_shell", staticmethod(fake_shell)):
            await mgr._push_local_repo(ssh, src, "cluster")  # type: ignore[arg-type]

        push = next(c for c in pushed if "push --force" in c)
        assert "-p 2222" in push
        assert str(tmp_path / "id_ed25519") in push

    async def test_push_failure_names_the_target(self, repo: Path, tmp_path: Path):
        src = GitSourceConfig(name="demo", local_path=repo, clone_dir="/repos")
        mgr = _delivery(src, tmp_path, [_slurm(tmp_path)])
        ssh = _ScriptedSSH()
        sha = _git("rev-parse", "HEAD", cwd=repo)

        async def fake_shell(cmd: str, timeout: float = 60.0):
            if "rev-parse" in cmd:
                return (sha + "\n", "", 0)
            return ("", "Permission denied (publickey).", 128)

        with patch.object(SourceDelivery, "_source_shell", staticmethod(fake_shell)):
            with pytest.raises(ValueError, match="Permission denied"):
                await mgr._push_local_repo(ssh, src, "cluster")  # type: ignore[arg-type]

    async def test_unknown_branch_fails_before_any_remote_work(
        self, repo: Path, tmp_path: Path,
    ):
        src = GitSourceConfig(
            name="demo", local_path=repo, branch="nope", clone_dir="/repos",
        )
        mgr = _delivery(src, tmp_path, [_slurm(tmp_path)])
        ssh = _ScriptedSSH()
        with pytest.raises(ValueError, match="Could not resolve"):
            await mgr._push_local_repo(ssh, src, "cluster")  # type: ignore[arg-type]
        assert ssh.commands == []
