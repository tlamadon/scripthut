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


def _match_workflows_glob(path: str, pattern: str) -> bool:
    """Match a repo-relative posix path against a workflows glob.

    Mirrors ``Path.glob`` semantics (segment-anchored, ``*`` does not
    cross ``/``, ``**`` matches any number of segments) so tree-based
    discovery at a ref finds the same files a working-tree glob would.
    """
    def match(segs: list[str], pats: list[str]) -> bool:
        if not pats:
            return not segs
        head, rest = pats[0], pats[1:]
        if head == "**":
            return any(match(segs[i:], rest) for i in range(len(segs) + 1))
        if not segs:
            return False
        return fnmatch.fnmatchcase(segs[0], head) and match(segs[1:], rest)

    return match(path.split("/"), pattern.split("/"))


@dataclass
class SourceWorkflow:
    """A workflow discovered from a source's .hut/workflows/ directory."""

    name: str  # e.g. "ml-jobs/train-model"
    source_name: str
    filename: str  # e.g. "train-model.json"
    tasks_json: str  # raw JSON content
    title: str | None = None  # optional human-readable title from JSON


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
    workflows: list[SourceWorkflow] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


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
        common_opts = "-o BatchMode=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null"
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

    def discover_workflows(self, name: str) -> list[SourceWorkflow]:
        """Discover workflow JSON files in a git source's workflows directory.

        Reads .hut/workflows/*.json from the local clone.

        Args:
            name: Name of the git source.

        Returns:
            List of discovered SourceWorkflow objects.
        """
        if name not in self._sources:
            raise ValueError(f"Unknown source: {name}")

        source = self._sources[name]
        status = self._statuses[name]

        if not status.cloned or not status.path.exists():
            logger.warning(f"Source {name} not cloned yet, no workflows to discover")
            status.workflows = []
            return []

        workflows: list[SourceWorkflow] = []
        parse_warnings: list[str] = []
        matched_files = sorted(status.path.glob(source.workflows_glob))
        if not matched_files:
            logger.debug(f"No workflow files matching '{source.workflows_glob}' in {status.path}")
            status.workflows = []
            status.warnings = []
            return []

        for json_file in matched_files:
            try:
                tasks_json = json_file.read_text()
                # Validate it's parseable JSON and extract optional title
                parsed = json.loads(tasks_json)
                title = parsed.get("title") if isinstance(parsed, dict) else None
                stem = json_file.stem
                workflows.append(
                    SourceWorkflow(
                        name=f"{name}/{stem}",
                        source_name=name,
                        filename=json_file.name,
                        tasks_json=tasks_json,
                        title=title,
                    )
                )
            except (json.JSONDecodeError, OSError) as e:
                rel_path = json_file.relative_to(status.path)
                msg = f"{rel_path}: {e}"
                parse_warnings.append(msg)
                logger.warning(f"Skipping invalid workflow file {json_file}: {e}")

        status.warnings = parse_warnings

        status.workflows = workflows
        logger.info(f"Discovered {len(workflows)} workflows in source {name}")
        return workflows

    async def discover_workflows_at(
        self, name: str, ref: str,
    ) -> list[SourceWorkflow]:
        """Discover workflow JSON files at an arbitrary ref of a source.

        Tree-based counterpart to :meth:`discover_workflows`: reads the
        files with ``git ls-tree`` + ``git show`` against the local
        clone's object store, so it works for any fetched ref without a
        checkout. Does NOT mutate the cached status/workflows — those
        always reflect the configured branch.

        Args:
            name: Name of the git source.
            ref: Commit hash (or any local ref) to read, typically the
                return value of :meth:`fetch_branch`.

        Returns:
            List of discovered SourceWorkflow objects.

        Raises:
            ValueError: Unknown source, source not cloned, or the ref
                cannot be listed.
        """
        if name not in self._sources:
            raise ValueError(f"Unknown source: {name}")

        source = self._sources[name]
        status = self._statuses[name]
        if not status.cloned or not status.path.exists():
            raise ValueError(f"Source '{name}' is not cloned")

        stdout, stderr, code = await self._run_git(
            ["ls-tree", "-r", "--name-only", "-z", ref],
            cwd=status.path,
        )
        if code != 0:
            raise ValueError(
                f"Failed to list tree at '{ref}' for source '{name}': "
                f"{stderr or 'ls-tree failed'}"
            )
        matched = sorted(
            p for p in stdout.split("\0")
            if p and _match_workflows_glob(p, source.workflows_glob)
        )

        workflows: list[SourceWorkflow] = []
        for rel_path in matched:
            tasks_json, stderr, code = await self._run_git(
                ["show", f"{ref}:{rel_path}"],
                cwd=status.path,
            )
            if code != 0:
                logger.warning(
                    f"Skipping workflow file {rel_path} at {ref}: {stderr}"
                )
                continue
            try:
                parsed = json.loads(tasks_json)
            except json.JSONDecodeError as e:
                logger.warning(
                    f"Skipping invalid workflow file {rel_path} at {ref}: {e}"
                )
                continue
            title = parsed.get("title") if isinstance(parsed, dict) else None
            filename = rel_path.rsplit("/", 1)[-1]
            stem = filename.removesuffix(".json")
            workflows.append(
                SourceWorkflow(
                    name=f"{name}/{stem}",
                    source_name=name,
                    filename=filename,
                    tasks_json=tasks_json,
                    title=title,
                )
            )
        logger.info(
            f"Discovered {len(workflows)} workflows in source {name} at {ref}"
        )
        return workflows

    def get_workflows(self, name: str) -> list[SourceWorkflow]:
        """Get cached discovered workflows for a source."""
        status = self._statuses.get(name)
        if status is None:
            return []
        return status.workflows

    def get_all_workflows(self) -> list[SourceWorkflow]:
        """Get all discovered workflows across all git sources."""
        result: list[SourceWorkflow] = []
        for status in self._statuses.values():
            result.extend(status.workflows)
        return result
