"""Git repository management with deploy key support."""

import asyncio
import fnmatch
import json
import logging
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from scripthut.config_schema import GitSourceConfig
from scripthut.ssh.git_ssh import build_git_ssh_command, local_openssh_version

logger = logging.getLogger(__name__)

# Conservative allowlist for branch names that reach shell command lines
# (remote `git ls-remote` / `git clone --branch` run over SSH). Tighter
# than git's own ref rules on purpose: no leading dash (option
# injection), no whitespace or shell metacharacters, no `..`.
_BRANCH_NAME_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._/+-]*[A-Za-z0-9])?$")


def is_safe_branch_name(branch: str) -> bool:
    """Whether ``branch`` is safe to interpolate into git command lines."""
    return (
        0 < len(branch) <= 200
        and _BRANCH_NAME_RE.match(branch) is not None
        and ".." not in branch
    )






@dataclass
class SourceStatus:
    """Status of a git source repository."""

    name: str
    path: Path
    cloned: bool
    branch: str
    last_commit: str | None = None
    last_commit_date: str | None = None  # ISO format datetime of HEAD commit
    error: str | None = None
    warnings: list[str] = field(default_factory=list)
    # local_path source whose working tree has drifted from HEAD. Kept as
    # a flag rather than only a warning string because `discover_workflows`
    # rewrites `warnings` on every pass.
    dirty: bool = False

    @property
    def dirty_warning(self) -> str | None:
        """What a dirty local working tree means for the next submission."""
        if not self.dirty:
            return None
        return (
            "working tree has uncommitted changes; the backend runs committed "
            f"HEAD ({self.last_commit or 'unknown'})"
        )


class GitSourceManager:
    """Manages git repository sources with deploy key support."""

    def __init__(self, cache_dir: Path) -> None:
        """Initialize the git source manager.

        Args:
            cache_dir: Directory to store cloned repositories.
        """
        self.cache_dir = cache_dir.expanduser()
        self._sources: dict[str, GitSourceConfig] = {}
        self._statuses: dict[str, SourceStatus] = {}

    def add_source(self, source: GitSourceConfig) -> None:
        """Register a source to be managed."""
        self._sources[source.name] = source
        self._statuses[source.name] = SourceStatus(
            name=source.name,
            path=self._get_source_path(source.name),
            cloned=False,
            branch=source.branch,
        )

    def _get_source_path(self, name: str) -> Path:
        """Where this source's repo lives on the scripthut host.

        A ``local_path`` source is read in place — the user's own working
        tree, never a copy under ``cache_dir``. Pointing the status here
        is what makes workflow discovery, HEAD metadata and tree reads at
        a ref work against it with no further changes.
        """
        source = self._sources.get(name)
        if source is not None:
            local = source.local_path_resolved
            if local is not None:
                return local
        return self.cache_dir / name

    def _build_ssh_command(self, deploy_key: Path | None) -> str:
        """Build the GIT_SSH_COMMAND for a deploy key.

        Host keys are kept in the cache dir rather than pointed at
        ``/dev/null``: forgetting every key on every run makes
        ``accept-new`` meaningless, since a host whose key silently
        changed always looks brand new.
        """
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        return build_git_ssh_command(
            key_path=str(deploy_key.expanduser()) if deploy_key else None,
            known_hosts=str(self.cache_dir / "known_hosts"),
            openssh_version=local_openssh_version(),
        )

    async def _run_git(
        self,
        args: list[str],
        cwd: Path | None = None,
        deploy_key: Path | None = None,
    ) -> tuple[str, str, int]:
        """Run a git command asynchronously.

        Args:
            args: Git command arguments (without 'git' prefix).
            cwd: Working directory for the command.
            deploy_key: Optional deploy key path.

        Returns:
            Tuple of (stdout, stderr, return_code).
        """
        env = {
            "GIT_SSH_COMMAND": self._build_ssh_command(deploy_key),
            "GIT_TERMINAL_PROMPT": "0",  # Disable git credential prompts
        }

        cmd = ["git"] + args
        logger.debug(f"Running: {' '.join(cmd)}")

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**dict(__import__("os").environ), **env},
        )

        stdout, stderr = await proc.communicate()
        return (
            stdout.decode().strip(),
            stderr.decode().strip(),
            proc.returncode or 0,
        )

    async def _refresh_local_source(self, name: str) -> SourceStatus:
        """Refresh metadata for a ``local_path`` source without touching it.

        The repo belongs to the user, so this only ever reads: no clone,
        no pull, no fetch, no checkout. "Synced" for such a source means
        no more than "the path is a readable git repo", and the dirty
        flag records whether its working tree has drifted from the commit
        a submission would actually send.
        """
        status = self._statuses[name]
        path = status.path

        if not path.exists():
            status.cloned = False
            status.dirty = False
            status.error = f"local_path does not exist: {path}"
            logger.error(f"Source {name}: {status.error}")
            return status

        _, stderr, code = await self._run_git(["rev-parse", "--git-dir"], cwd=path)
        if code != 0:
            status.cloned = False
            status.dirty = False
            status.error = (
                f"local_path is not a git repository: {path}"
                f"{f' ({stderr})' if stderr else ''}"
            )
            logger.error(f"Source {name}: {status.error}")
            return status

        status.cloned = True
        status.error = None
        status.last_commit = await self._get_head_commit(name)
        status.last_commit_date = await self._get_head_commit_date(name)

        porcelain, _, code = await self._run_git(
            ["status", "--porcelain"], cwd=path,
        )
        # Non-zero here means we could not tell; don't claim it is clean.
        status.dirty = bool(porcelain) or code != 0

        logger.info(
            f"Local source {name} at {path} "
            f"(commit {status.last_commit}{', dirty' if status.dirty else ''})"
        )
        return status

    async def clone_source(self, name: str) -> SourceStatus:
        """Clone a source repository.

        Args:
            name: Name of the source to clone.

        Returns:
            Updated SourceStatus.
        """
        if name not in self._sources:
            raise ValueError(f"Unknown source: {name}")

        source = self._sources[name]
        status = self._statuses[name]

        # A local_path source MUST NOT reach the rmtree below: status.path
        # is the user's own repository, not a cache copy we own.
        if source.local_path is not None:
            return await self._refresh_local_source(name)

        dest_path = status.path

        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Remove existing directory if it exists
        if dest_path.exists():
            shutil.rmtree(dest_path)

        logger.info(f"Cloning {source.url} to {dest_path}")

        stdout, stderr, code = await self._run_git(
            ["clone", "--branch", source.branch, "--single-branch", source.url, str(dest_path)],
            deploy_key=source.deploy_key_resolved,
        )

        if code != 0:
            status.cloned = False
            status.error = stderr or "Clone failed"
            logger.error(f"Failed to clone {name}: {status.error}")
        else:
            status.cloned = True
            status.error = None
            # Get the latest commit
            status.last_commit = await self._get_head_commit(name)
            status.last_commit_date = await self._get_head_commit_date(name)
            logger.info(f"Cloned {name} at commit {status.last_commit}")

        return status

    async def pull_source(self, name: str) -> SourceStatus:
        """Pull latest changes for a source repository.

        Args:
            name: Name of the source to pull.

        Returns:
            Updated SourceStatus.
        """
        if name not in self._sources:
            raise ValueError(f"Unknown source: {name}")

        source = self._sources[name]
        status = self._statuses[name]

        # Nothing to pull for a local source, and pulling would rewrite
        # the user's working tree. Just re-read it.
        if source.local_path is not None:
            return await self._refresh_local_source(name)

        if not status.cloned or not status.path.exists():
            return await self.clone_source(name)

        logger.info(f"Pulling latest changes for {name}")

        stdout, stderr, code = await self._run_git(
            ["pull", "--ff-only"],
            cwd=status.path,
            deploy_key=source.deploy_key_resolved,
        )

        if code != 0:
            status.error = stderr or "Pull failed"
            logger.error(f"Failed to pull {name}: {status.error}")
        else:
            status.error = None
            status.last_commit = await self._get_head_commit(name)
            status.last_commit_date = await self._get_head_commit_date(name)
            logger.info(f"Updated {name} to commit {status.last_commit}")

        return status

    async def _get_head_commit(self, name: str) -> str | None:
        """Get the HEAD commit hash for a source."""
        status = self._statuses[name]
        if not status.path.exists():
            return None

        stdout, _, code = await self._run_git(
            ["rev-parse", "--short", "HEAD"],
            cwd=status.path,
        )

        return stdout if code == 0 else None

    async def _get_head_commit_date(self, name: str) -> str | None:
        """Get the HEAD commit date in ISO format."""
        status = self._statuses[name]
        if not status.path.exists():
            return None

        stdout, _, code = await self._run_git(
            ["log", "-1", "--format=%aI"],
            cwd=status.path,
        )

        return stdout if code == 0 else None

    async def sync_source(self, name: str) -> SourceStatus:
        """Sync a source (clone if not exists, pull if exists).

        Args:
            name: Name of the source to sync.

        Returns:
            Updated SourceStatus.
        """
        status = self._statuses.get(name)
        if status is None:
            raise ValueError(f"Unknown source: {name}")

        if status.cloned and status.path.exists():
            return await self.pull_source(name)
        else:
            return await self.clone_source(name)

    async def sync_all(self) -> dict[str, SourceStatus]:
        """Sync all registered sources.

        Returns:
            Dictionary of source names to their statuses.
        """
        tasks = [self.sync_source(name) for name in self._sources]
        await asyncio.gather(*tasks, return_exceptions=True)
        return dict(self._statuses)

    async def fetch_branch(self, name: str, branch: str) -> str:
        """Fetch an arbitrary branch into the source's existing clone.

        The cache clone is ``--single-branch``, but that only limits the
        default fetch refspec — any branch can still be fetched on
        demand. The fetched objects land in the clone's object store
        (so later ``git show <sha>:...`` reads work) without touching
        the checked-out default branch.

        Args:
            name: Name of the source.
            branch: Branch to fetch.

        Returns:
            The full commit hash of the branch tip.

        Raises:
            ValueError: Unknown source, unsafe branch name, or the
                fetch/resolve failed (e.g. branch doesn't exist).
        """
        if name not in self._sources:
            raise ValueError(f"Unknown source: {name}")
        if not is_safe_branch_name(branch):
            raise ValueError(f"Invalid branch name: {branch!r}")

        source = self._sources[name]
        status = self._statuses[name]

        if source.local_path is not None:
            # Every branch is already here; fetching would only churn the
            # user's FETCH_HEAD.
            status = await self._refresh_local_source(name)
            if not status.cloned:
                raise ValueError(
                    f"Source '{name}' is not readable: {status.error}"
                )
            stdout, stderr, code = await self._run_git(
                ["rev-parse", "--verify", f"refs/heads/{branch}"],
                cwd=status.path,
            )
            if code != 0 or not stdout:
                raise ValueError(
                    f"Branch '{branch}' not found in local repo "
                    f"{status.path} for source '{name}': "
                    f"{stderr or 'rev-parse failed'}"
                )
            return stdout

        if not status.cloned or not status.path.exists():
            status = await self.clone_source(name)
            if not status.cloned:
                raise ValueError(
                    f"Source '{name}' could not be cloned: {status.error}"
                )

        _, stderr, code = await self._run_git(
            ["fetch", "origin", f"refs/heads/{branch}"],
            cwd=status.path,
            deploy_key=source.deploy_key_resolved,
        )
        if code != 0:
            raise ValueError(
                f"Failed to fetch branch '{branch}' for source '{name}': "
                f"{stderr or 'fetch failed'}"
            )

        stdout, stderr, code = await self._run_git(
            ["rev-parse", "FETCH_HEAD"], cwd=status.path,
        )
        if code != 0 or not stdout:
            raise ValueError(
                f"Failed to resolve branch '{branch}' for source '{name}': "
                f"{stderr or 'rev-parse failed'}"
            )
        return stdout

    def get_status(self, name: str) -> SourceStatus | None:
        """Get the status of a source."""
        return self._statuses.get(name)

    def get_all_statuses(self) -> dict[str, SourceStatus]:
        """Get statuses of all sources."""
        return dict(self._statuses)

    def get_source_path(self, name: str) -> Path | None:
        """Get the local path for a cloned source."""
        status = self._statuses.get(name)
        if status and status.cloned:
            return status.path
        return None
