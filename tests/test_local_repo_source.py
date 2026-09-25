"""Git sources that live on this machine (``local_path``) instead of a remote.

A source with ``local_path`` set is read in place and materialised on the
backend by pushing to a bare mirror under its ``clone_dir``, rather than
having the backend clone from a git remote with a deploy key. Three things
need holding down:

1. **Nothing ever writes to the user's repo.** ``status.path`` for such a
   source *is* their working tree, and ``clone_source`` used to begin with
   ``shutil.rmtree(status.path)``. Everything here that touches
   GitSourceManager exists to keep that from coming back.
2. **The push sequence.** Mirror init, a content-addressed refspec, a clone
   of that ref, then postclone — and reuse of an already-materialised commit.
3. **What is read versus what runs.** Workflow JSON and ``scripthut.yaml``
   come from the working tree; the commit the backend runs is the branch
   tip. A dirty tree makes those differ, and has to say so.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripthut.config_schema import (
    AWSBatchConfig,
    BatchBackendConfig,
    GitSourceConfig,
    GlobalSettings,
    LocalBackendConfig,
    ScriptHutConfig,
    SlurmBackendConfig,
    SSHConfig,
)
from scripthut.runs.manager import RunManager
from scripthut.runs.storage import RunStorageManager
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


def _run_manager(
    source: GitSourceConfig, tmp_path: Path, backends: list[Any],
) -> RunManager:
    config = ScriptHutConfig(
        sources=[source],
        backends=backends,
        settings=GlobalSettings(sources_cache_dir=tmp_path / "cache"),
    )
    return RunManager(
        config=config,
        backends={},
        storage=RunStorageManager(tmp_path / "runs"),
        job_backends={},
    )


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
        assert status.dirty_warning is None

    async def test_tracked_edit_is_dirty(self, repo: Path, tmp_path: Path):
        (repo / "code.py").write_text("print('edited')\n")
        mgr = _manager(repo, tmp_path)
        status = await mgr.sync_source("demo")
        assert status.dirty is True
        assert "uncommitted changes" in (status.dirty_warning or "")
        assert status.last_commit in (status.dirty_warning or "")

    async def test_gitignored_file_is_not_dirty(self, repo: Path, tmp_path: Path):
        """Build artefacts would otherwise warn on every single sync."""
        mgr = _manager(repo, tmp_path)
        status = await mgr.sync_source("demo")
        assert status.dirty is False  # keep-me.txt is ignored

    async def test_warning_survives_workflow_discovery(
        self, repo: Path, tmp_path: Path,
    ):
        """discover_workflows rewrites `warnings` wholesale, and the server
        calls it right after every sync — so the notice has to be re-seeded
        there or it never reaches the UI."""
        (repo / "code.py").write_text("print('edited')\n")
        mgr = _manager(repo, tmp_path)
        await mgr.sync_source("demo")
        mgr.discover_workflows("demo")
        status = mgr.get_status("demo")
        assert status is not None
        assert any("uncommitted changes" in w for w in status.warnings)

    async def test_warning_survives_discovery_with_no_workflows(
        self, repo: Path, tmp_path: Path,
    ):
        (repo / "code.py").write_text("print('edited')\n")
        mgr = _manager(repo, tmp_path, workflows_glob="nothing/*.json")
        await mgr.sync_source("demo")
        mgr.discover_workflows("demo")
        status = mgr.get_status("demo")
        assert status is not None
        assert any("uncommitted changes" in w for w in status.warnings)


class TestWorkflowDiscovery:
    async def test_reads_the_working_tree(self, repo: Path, tmp_path: Path):
        mgr = _manager(repo, tmp_path)
        await mgr.sync_source("demo")
        wfs = mgr.discover_workflows("demo")
        assert [w.filename for w in wfs] == ["train.json"]

    async def test_uncommitted_workflow_file_is_visible(
        self, repo: Path, tmp_path: Path,
    ):
        """A local source reads the tree, so a new workflow works before it is
        committed — that is the iteration loop this feature is for."""
        (repo / ".hut" / "workflows" / "draft.json").write_text(
            json.dumps({"title": "draft", "tasks": []})
        )
        mgr = _manager(repo, tmp_path)
        await mgr.sync_source("demo")
        names = sorted(w.filename for w in mgr.discover_workflows("demo"))
        assert names == ["draft.json", "train.json"]


class TestFetchBranch:
    async def test_resolves_a_local_branch_without_fetching(
        self, repo: Path, tmp_path: Path,
    ):
        """There is no remote to fetch from, and fetching would churn the
        user's FETCH_HEAD for nothing."""
        _git("checkout", "-q", "-b", "feature", cwd=repo)
        (repo / "code.py").write_text("print('feature')\n")
        _git("commit", "-qam", "feature work", cwd=repo)
        _git("checkout", "-q", "main", cwd=repo)
        expected = _git("rev-parse", "feature", cwd=repo)

        mgr = _manager(repo, tmp_path)
        assert await mgr.fetch_branch("demo", "feature") == expected
        assert not (repo / ".git" / "FETCH_HEAD").exists()

    async def test_unknown_branch_raises(self, repo: Path, tmp_path: Path):
        mgr = _manager(repo, tmp_path)
        with pytest.raises(ValueError, match="not found in local repo"):
            await mgr.fetch_branch("demo", "no-such-branch")

    async def test_unsafe_branch_name_still_rejected(
        self, repo: Path, tmp_path: Path,
    ):
        mgr = _manager(repo, tmp_path)
        with pytest.raises(ValueError, match="Invalid branch name"):
            await mgr.fetch_branch("demo", "--upload-pack=evil")

    async def test_checkout_is_left_where_it_was(self, repo: Path, tmp_path: Path):
        _git("checkout", "-q", "-b", "feature", cwd=repo)
        _git("checkout", "-q", "main", cwd=repo)
        mgr = _manager(repo, tmp_path)
        await mgr.fetch_branch("demo", "feature")
        assert _git("rev-parse", "--abbrev-ref", "HEAD", cwd=repo) == "main"


# ---------------------------------------------------------------------------
# Pushing the repo to the backend
# ---------------------------------------------------------------------------


class TestPushLocalRepo:
    async def test_full_push_sequence(self, repo: Path, tmp_path: Path):
        src = GitSourceConfig(
            name="demo", local_path=repo, clone_dir="~/scripthut-repos",
            postclone="uv sync",
        )
        mgr = _run_manager(src, tmp_path, [_slurm(tmp_path)])
        ssh = _ScriptedSSH()
        sha = _git("rev-parse", "HEAD", cwd=repo)
        short = sha[:12]

        pushed: list[str] = []

        async def fake_shell(cmd: str, timeout: float = 60.0):
            pushed.append(cmd)
            if "rev-parse" in cmd:
                return (sha + "\n", "", 0)
            return ("", "", 0)

        with patch("scripthut.runs.manager._run_local_shell", fake_shell):
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
        mgr = _run_manager(src, tmp_path, [_slurm(tmp_path)])
        sha = _git("rev-parse", "HEAD", cwd=repo)
        ssh = _ScriptedSSH([
            ("", "", 0),            # mkdir + mirror init
            ("exists\n", "", 0),    # test -d <clone_path>
        ])

        async def fake_shell(cmd: str, timeout: float = 60.0):
            return ((sha + "\n", "", 0) if "rev-parse" in cmd else ("", "", 0))

        with patch("scripthut.runs.manager._run_local_shell", fake_shell):
            clone_path, short = await mgr._push_local_repo(ssh, src, "cluster")  # type: ignore[arg-type]

        assert clone_path == f"/repos/{sha[:12]}"
        assert not any("git clone" in c for c in ssh.commands)
        assert not any("uv sync" in c for c in ssh.commands)

    async def test_local_backend_pushes_to_a_filesystem_path(
        self, repo: Path, tmp_path: Path,
    ):
        """No ssh in the picture, and `~` has to be expanded here because git
        hands a filesystem path to no shell."""
        src = GitSourceConfig(
            name="demo", local_path=repo, clone_dir="~/scripthut-repos",
        )
        mgr = _run_manager(src, tmp_path, [LocalBackendConfig(name="local")])
        ssh = _ScriptedSSH()
        sha = _git("rev-parse", "HEAD", cwd=repo)
        pushed: list[str] = []

        async def fake_shell(cmd: str, timeout: float = 60.0):
            pushed.append(cmd)
            return ((sha + "\n", "", 0) if "rev-parse" in cmd else ("", "", 0))

        with patch("scripthut.runs.manager._run_local_shell", fake_shell):
            await mgr._push_local_repo(ssh, src, "local")  # type: ignore[arg-type]

        push = next(c for c in pushed if "push --force" in c)
        assert "GIT_SSH_COMMAND" not in push
        expected = str(Path.home() / "scripthut-repos" / ".mirror.git")
        target = push.split("push --force")[1].split()[0].strip("'\"")
        assert target == expected, target

    async def test_non_default_port_rides_in_the_ssh_command(
        self, repo: Path, tmp_path: Path,
    ):
        """scp-style URLs have nowhere to put a port."""
        src = GitSourceConfig(name="demo", local_path=repo, clone_dir="/repos")
        mgr = _run_manager(src, tmp_path, [_slurm(tmp_path, port=2222)])
        ssh = _ScriptedSSH()
        sha = _git("rev-parse", "HEAD", cwd=repo)
        pushed: list[str] = []

        async def fake_shell(cmd: str, timeout: float = 60.0):
            pushed.append(cmd)
            return ((sha + "\n", "", 0) if "rev-parse" in cmd else ("", "", 0))

        with patch("scripthut.runs.manager._run_local_shell", fake_shell):
            await mgr._push_local_repo(ssh, src, "cluster")  # type: ignore[arg-type]

        push = next(c for c in pushed if "push --force" in c)
        assert "-p 2222" in push
        assert str(tmp_path / "id_ed25519") in push

    async def test_push_failure_names_the_target(self, repo: Path, tmp_path: Path):
        src = GitSourceConfig(name="demo", local_path=repo, clone_dir="/repos")
        mgr = _run_manager(src, tmp_path, [_slurm(tmp_path)])
        ssh = _ScriptedSSH()
        sha = _git("rev-parse", "HEAD", cwd=repo)

        async def fake_shell(cmd: str, timeout: float = 60.0):
            if "rev-parse" in cmd:
                return (sha + "\n", "", 0)
            return ("", "Permission denied (publickey).", 128)

        with patch("scripthut.runs.manager._run_local_shell", fake_shell):
            with pytest.raises(ValueError, match="Permission denied"):
                await mgr._push_local_repo(ssh, src, "cluster")  # type: ignore[arg-type]

    async def test_unknown_branch_fails_before_any_remote_work(
        self, repo: Path, tmp_path: Path,
    ):
        src = GitSourceConfig(
            name="demo", local_path=repo, branch="nope", clone_dir="/repos",
        )
        mgr = _run_manager(src, tmp_path, [_slurm(tmp_path)])
        ssh = _ScriptedSSH()
        with pytest.raises(ValueError, match="Could not resolve"):
            await mgr._push_local_repo(ssh, src, "cluster")  # type: ignore[arg-type]
        assert ssh.commands == []

    async def test_api_only_backend_is_refused_with_a_way_forward(
        self, repo: Path, tmp_path: Path,
    ):
        """A Batch container clones a URL itself and cannot reach this disk."""
        src = GitSourceConfig(name="demo", local_path=repo, clone_dir="/repos")
        batch = BatchBackendConfig(
            name="batch",
            aws=AWSBatchConfig(region="us-east-1", job_queue="q"),
        )
        mgr = _run_manager(src, tmp_path, [batch])
        with pytest.raises(ValueError, match="no filesystem to push a repo to"):
            mgr._local_push_target(src, "batch")


class TestRunCreation:
    async def test_api_only_backend_run_is_refused(
        self, repo: Path, tmp_path: Path,
    ):
        """The guard has to sit on the submit path too, not just the
        transport — otherwise the run is built and fails later."""
        src = GitSourceConfig(name="demo", local_path=repo, clone_dir="/repos")
        batch = BatchBackendConfig(
            name="batch",
            aws=AWSBatchConfig(region="us-east-1", job_queue="q"),
        )
        mgr = _run_manager(src, tmp_path, [batch])
        with patch.object(mgr, "get_job_backend", return_value=MagicMock()):
            with pytest.raises(ValueError, match="only has local_path set"):
                await mgr.create_run_from_source(
                    "demo", "train.json", '{"tasks": []}', "batch",
                )

    async def test_source_with_both_keeps_the_url_path_on_batch(
        self, repo: Path, tmp_path: Path,
    ):
        """A source carrying both still works on an API-only backend."""
        src = GitSourceConfig(
            name="demo", local_path=repo, url="git@h:o/r.git", clone_dir="/repos",
        )
        batch = BatchBackendConfig(
            name="batch",
            aws=AWSBatchConfig(region="us-east-1", job_queue="q"),
        )
        mgr = _run_manager(src, tmp_path, [batch])
        ls_remote = AsyncMock(return_value="deadbeef0000")
        captured: dict[str, Any] = {}

        async def fake_build_run(
            tasks, workflow_name, backend_name, max_concurrent, ssh_client,
            *, git_repo=None, git_branch=None, commit_hash=None,
            doc_env=None, doc_env_groups=None, doc_stacks=None, source_name=None,
        ):
            captured["commit_hash"] = commit_hash
            captured["git_repo"] = git_repo
            return MagicMock()

        with patch.object(mgr, "_build_run", side_effect=fake_build_run), \
             patch.object(mgr, "_ls_remote_commit", ls_remote), \
             patch.object(mgr, "_load_source_project_config", AsyncMock(return_value=None)), \
             patch.object(mgr, "get_job_backend", return_value=MagicMock()):
            await mgr.create_run_from_source(
                "demo", "train.json", '{"tasks": []}', "batch",
            )

        ls_remote.assert_awaited_once_with("git@h:o/r.git", "main")
        assert captured["commit_hash"] == "deadbeef0000"
        assert captured["git_repo"] == "git@h:o/r.git"

    async def test_run_records_the_local_path_when_there_is_no_url(
        self, repo: Path, tmp_path: Path,
    ):
        """`git_repo` is run metadata; for a local-only source the path is the
        honest answer, and an empty string would be useless in the UI."""
        src = GitSourceConfig(name="demo", local_path=repo, clone_dir="/repos")
        mgr = _run_manager(src, tmp_path, [_slurm(tmp_path)])
        captured: dict[str, Any] = {}

        async def fake_build_run(
            tasks, workflow_name, backend_name, max_concurrent, ssh_client,
            *, git_repo=None, git_branch=None, commit_hash=None,
            doc_env=None, doc_env_groups=None, doc_stacks=None, source_name=None,
        ):
            captured["git_repo"] = git_repo
            captured["commit_hash"] = commit_hash
            return MagicMock()

        with patch.object(mgr, "_build_run", side_effect=fake_build_run), \
             patch.object(
                 mgr, "_clone_source_repo",
                 AsyncMock(return_value=("/repos/abc123def456", "abc123def456")),
             ), \
             patch.object(mgr, "_load_source_project_config", AsyncMock(return_value=None)), \
             patch.object(mgr, "get_ssh_client", return_value=MagicMock()), \
             patch.object(mgr, "get_job_backend", return_value=MagicMock()):
            await mgr.create_run_from_source(
                "demo", "train.json", '{"tasks": []}', "cluster",
            )

        assert captured["git_repo"] == str(repo)
        assert captured["commit_hash"] == "abc123def456"

    async def test_clone_source_repo_routes_local_sources_to_the_push(
        self, repo: Path, tmp_path: Path,
    ):
        src = GitSourceConfig(name="demo", local_path=repo, clone_dir="/repos")
        mgr = _run_manager(src, tmp_path, [_slurm(tmp_path)])
        push = AsyncMock(return_value=("/repos/abc", "abc"))
        clone = AsyncMock(return_value=("/repos/xyz", "xyz"))
        with patch.object(mgr, "_push_local_repo", push), \
             patch.object(mgr, "_clone_git_repo", clone):
            await mgr._clone_source_repo(MagicMock(), src, "cluster")
        push.assert_awaited_once()
        clone.assert_not_awaited()

    async def test_clone_source_repo_still_clones_a_url_source(
        self, tmp_path: Path,
    ):
        src = GitSourceConfig(name="demo", url="git@h:o/r.git", clone_dir="/repos")
        mgr = _run_manager(src, tmp_path, [_slurm(tmp_path)])
        push = AsyncMock(return_value=("/repos/abc", "abc"))
        clone = AsyncMock(return_value=("/repos/xyz", "xyz"))
        with patch.object(mgr, "_push_local_repo", push), \
             patch.object(mgr, "_clone_git_repo", clone):
            await mgr._clone_source_repo(MagicMock(), src, "cluster")
        clone.assert_awaited_once()
        push.assert_not_awaited()


class TestAgentRuns:
    async def test_local_only_source_is_refused(self, repo: Path, tmp_path: Path):
        """An agent commits and pushes a branch; a mirror on the cluster is
        not yet somewhere it can usefully push back to."""
        src = GitSourceConfig(name="demo", local_path=repo, clone_dir="/repos")
        mgr = _run_manager(src, tmp_path, [_slurm(tmp_path)])
        with pytest.raises(ValueError, match="only has local_path set"):
            await mgr.create_agent_run("demo", "cluster", mode="tui")


class TestProjectConfigOverlay:
    async def test_read_from_the_working_tree(self, repo: Path, tmp_path: Path):
        """Uncommitted, so this also proves it is not going through
        `git show <sha>:scripthut.yaml`."""
        (repo / "scripthut.yaml").write_text(
            "env_groups:\n  julia:\n    - init: 'module load julia'\n"
        )
        src = GitSourceConfig(name="demo", local_path=repo)
        mgr = _run_manager(src, tmp_path, [_slurm(tmp_path)])
        cfg = await mgr._load_source_project_config(src)
        assert cfg is not None
        assert "julia" in cfg.env_groups

    async def test_absent_overlay_is_not_an_error(self, repo: Path, tmp_path: Path):
        src = GitSourceConfig(name="demo", local_path=repo)
        mgr = _run_manager(src, tmp_path, [_slurm(tmp_path)])
        assert await mgr._load_source_project_config(src) is None

    async def test_forbidden_sections_still_raise(self, repo: Path, tmp_path: Path):
        """A repo-local file must not be able to declare infrastructure."""
        (repo / "scripthut.yaml").write_text(
            "backends:\n  - name: sneaky\n    type: local\n"
        )
        src = GitSourceConfig(name="demo", local_path=repo)
        mgr = _run_manager(src, tmp_path, [_slurm(tmp_path)])
        with pytest.raises(ValueError, match="backends"):
            await mgr._load_source_project_config(src)
