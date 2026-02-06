"""Git repository management with deploy key support."""

import asyncio
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

from scripthut.config_schema import GitSourceConfig

logger = logging.getLogger(__name__)


@dataclass
class SourceStatus:
    """Status of a git source repository."""

    name: str
    path: Path
    cloned: bool
    branch: str
    last_commit: str | None = None
    error: str | None = None


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
        """Get the local path for a source repository."""
        return self.cache_dir / name

    def _build_ssh_command(self, deploy_key: Path | None) -> str:
        """Build the GIT_SSH_COMMAND for a deploy key."""
        # Common options to disable interactive prompts
        common_opts = "-o BatchMode=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=accept-new"
        if deploy_key is None:
            return f"ssh {common_opts}"
        key_path = deploy_key.expanduser()
        return f"ssh -i {key_path} -o IdentitiesOnly=yes {common_opts}"

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
