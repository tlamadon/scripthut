"""Run manager for task submission and tracking."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import uuid
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import TYPE_CHECKING

from scripthut.backends.base import JobBackend
from scripthut.config_schema import (
    EnvRule,
    GitSourceConfig,
    PathSourceConfig,
    ProjectConfig,
    ScriptHutConfig,
)
from scripthut.models import JobState
from scripthut.runs.env import resolve_for_task
from scripthut.runs.models import (
    Run,
    RunItem,
    RunItemStatus,
    RunStatus,
    TaskDefinition,
)
from scripthut.ssh.client import SSHClient

if TYPE_CHECKING:
    from scripthut.runs.storage import RunStorageManager

logger = logging.getLogger(__name__)

# Marker stored on ``item.error`` when a SUBMITTED job vanishes from the
# scheduler queue without ever being observed RUNNING.  Used by the polling
# layer to flip the item back to COMPLETED if the accounting DB later
# confirms it actually ran successfully (covers ultra-fast jobs that finish
# between two poll cycles).
DISAPPEARED_BEFORE_RUNNING_MARKER = "Job vanished before being observed running"

# Grace period (seconds) before *consulting sacct* for a SUBMITTED item
# that's missing from squeue. We don't mark it FAILED on absence; we
# only start asking accounting whether the job ran or never made it.
# 60s covers typical poll intervals (5–60s) with headroom.
SUBMIT_TO_FAIL_GRACE_SECONDS = 60.0

# How long (seconds) we'll keep a SUBMITTED item alive when *neither*
# squeue nor sacct has any record of it. The rare real "vanished"
# case: sbatch returned an id but the scheduler dropped the job. After
# this we mark FAILED with an actionable message rather than wait
# indefinitely.
SUBMITTED_NO_RECORD_TIMEOUT_SECONDS = 300.0

# Message used for items that sbatch accepted but neither squeue nor
# sacct ever acknowledged. Different from the legacy "vanished before
# running" marker — that one was speculative; this one is evidence-based
# (we asked sacct and it had no record).
SCHEDULER_NO_RECORD_MARKER = (
    "sbatch accepted the job but the scheduler has no record of it"
)


async def _run_local_shell(command: str, timeout: float = 60.0) -> tuple[str, str, int]:
    """Run ``command`` locally (``sh -c``) and return ``(stdout, stderr, exit_code)``.

    Used for workflows whose backend has no SSH connection (e.g. AWS Batch) —
    the command runs on the scripthut host instead of a remote login node.
    """
    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise ValueError(f"Local command timed out after {timeout}s: {command}")
    return (
        stdout_b.decode("utf-8", errors="replace"),
        stderr_b.decode("utf-8", errors="replace"),
        proc.returncode if proc.returncode is not None else -1,
    )


class RunManager:
    """Manages runs - fetching, submitting, and tracking."""

    def __init__(
        self,
        config: ScriptHutConfig,
        backends: dict[str, SSHClient],
        storage: RunStorageManager | None = None,
        job_backends: dict[str, JobBackend] | None = None,
    ) -> None:
        """Initialize with config, SSH backends, and optional persistent storage."""
        self.config = config
        self.backends = backends
        self.runs: dict[str, Run] = {}
        self.storage = storage
        self.job_backends = job_backends or {}
        # SSE event bus: version counter + Event per run
        self._run_versions: dict[str, int] = {}
        self._run_events: dict[str, asyncio.Event] = {}

    def _resolve_environment(
        self, run: Run, task: TaskDefinition
    ) -> tuple[dict[str, str], str]:
        """Resolve env vars and extra_init for a task in the context of a run.

        Chains backend → server → workflow → task env rules and applies them
        against a seed of SCRIPTHUT_* runtime variables. Returns the fully
        merged env dict (ready to pass to ``generate_script``) and the
        concatenated extra_init bash text.
        """
        git_repo = run.git_repo
        git_branch = run.git_branch
        return resolve_for_task(
            self.config,
            backend_name=run.backend_name,
            workflow_name=run.workflow_name,
            run_id=run.id,
            created_at=run.created_at,
            task=task,
            git_repo=git_repo,
            git_branch=git_branch,
            git_sha=run.commit_hash,
            doc_env=run.doc_env,
            doc_env_groups=run.doc_env_groups,
        )

    @staticmethod
    def _resolve_wildcard_deps(tasks: list[TaskDefinition]) -> None:
        """Expand wildcard patterns in task dependencies to matching task IDs."""
        task_ids = [t.id for t in tasks]
        for task in tasks:
            expanded: list[str] = []
            for dep in task.dependencies:
                if any(c in dep for c in ("*", "?", "[")):
                    matches = [tid for tid in task_ids if fnmatch(tid, dep) and tid != task.id]
                    if not matches:
                        raise ValueError(
                            f"Task '{task.id}': wildcard dep '{dep}' matches no tasks"
                        )
                    expanded.extend(matches)
                else:
                    expanded.append(dep)
            task.dependencies = expanded

    @staticmethod
    def _validate_dependencies(tasks: list[TaskDefinition]) -> None:
        """Validate task dependencies (no missing refs, no cycles)."""
        task_ids = {t.id for t in tasks}

        for task in tasks:
            for dep_id in task.dependencies:
                if dep_id not in task_ids:
                    raise ValueError(
                        f"Task '{task.id}' depends on '{dep_id}', which does not exist"
                    )
                if dep_id == task.id:
                    raise ValueError(
                        f"Task '{task.id}' depends on itself"
                    )

        # Detect circular dependencies via DFS
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {t.id: WHITE for t in tasks}
        deps_map = {t.id: t.dependencies for t in tasks}

        def dfs(node: str, path: list[str]) -> None:
            """DFS cycle detection using three-color marking."""
            color[node] = GRAY
            path.append(node)
            for dep in deps_map[node]:
                if color[dep] == GRAY:
                    cycle_start = path.index(dep)
                    cycle = path[cycle_start:] + [dep]
                    raise ValueError(
                        f"Circular dependency detected: {' -> '.join(cycle)}"
                    )
                if color[dep] == WHITE:
                    dfs(dep, path)
            path.pop()
            color[node] = BLACK

        for task in tasks:
            if color[task.id] == WHITE:
                dfs(task.id, [])

    async def _handle_generates_source(
        self, run: Run, item: RunItem
    ) -> None:
        """Read a generated workflow JSON and append tasks to the run."""
        path = item.task.generates_source
        if not path:
            return

        logger.info(
            f"Task '{item.task.id}' completed with generates_source: {path}"
        )

        ssh_client = self.get_ssh_client(run.backend_name)
        if ssh_client is None:
            logger.error(
                f"No SSH client for backend '{run.backend_name}' — "
                f"cannot read generates_source '{path}'"
            )
            return

        # Resolve relative paths against the task's working_dir
        if not path.startswith("/") and not path.startswith("~"):
            path = f"{item.task.working_dir}/{path}"

        stdout, stderr, exit_code = await ssh_client.run_command(f"cat {path}")
        if exit_code != 0:
            logger.error(
                f"Failed to read generates_source '{path}': {stderr}"
            )
            return

        try:
            data = json.loads(stdout)
        except json.JSONDecodeError as e:
            logger.error(
                f"Invalid JSON in generates_source '{path}': {e}"
            )
            return

        try:
            new_tasks, child_doc_env, child_doc_groups = (
                TaskDefinition.parse_document(data)
            )
        except ValueError as e:
            logger.error(f"Invalid generates_source JSON '{path}': {e}")
            return

        # Generated docs can extend the run's doc-level env in addition to
        # adding new tasks. New rules append after the existing ones; new
        # groups merge in (later definitions shadow earlier).
        if child_doc_env:
            run.doc_env.extend(child_doc_env)
        if child_doc_groups:
            run.doc_env_groups.update(child_doc_groups)

        # Resolve working_dir relative to the parent task's working_dir
        # (same as _resolve_working_dirs does with clone_dir)
        self._resolve_working_dirs(new_tasks, item.task.working_dir)

        # Resolve wildcard deps against ALL tasks in the run
        all_tasks = [ri.task for ri in run.items] + new_tasks
        all_task_ids = [t.id for t in all_tasks]

        for task in new_tasks:
            expanded: list[str] = []
            for dep in task.dependencies:
                if any(c in dep for c in ("*", "?", "[")):
                    matches = [
                        tid for tid in all_task_ids
                        if fnmatch(tid, dep) and tid != task.id
                    ]
                    if not matches:
                        logger.error(
                            f"Task '{task.id}': wildcard dep '{dep}' "
                            f"matches no tasks in generates_source '{path}'"
                        )
                        return
                    expanded.extend(matches)
                else:
                    expanded.append(dep)
            task.dependencies = expanded

        for task in new_tasks:
            for dep_id in task.dependencies:
                if dep_id not in all_task_ids:
                    logger.error(
                        f"Task '{task.id}' has unknown dependency '{dep_id}' "
                        f"in generates_source '{path}'"
                    )
                    return

        new_items = [RunItem(task=t) for t in new_tasks]
        run.items.extend(new_items)
        self._persist_run(run)

        logger.info(
            f"Appended {len(new_tasks)} tasks from generates_source "
            f"'{path}' to run '{run.id}'"
        )

        await self.process_run(run)

    def get_ssh_client(self, backend_name: str) -> SSHClient | None:
        """Get SSH client for a backend."""
        return self.backends.get(backend_name)

    async def _get_git_root(self, ssh_client: SSHClient, working_dir: str) -> str:
        """Detect the git repository root for a working directory on the backend."""
        stdout, stderr, exit_code = await ssh_client.run_command(
            f"cd {working_dir} && git rev-parse --show-toplevel"
        )
        if exit_code != 0:
            raise ValueError(
                f"'{working_dir}' is not inside a git repository. "
                f"ScriptHut requires workflows to live in git repos."
            )
        return stdout.strip()

    # --- Git workflow support ---

    @staticmethod
    def _to_https_url(repo: str) -> str:
        """Convert git SSH URLs to HTTPS when possible.

        e.g. git@github.com:org/repo.git -> https://github.com/org/repo.git
        """
        if repo.startswith("git@"):
            # git@github.com:org/repo.git -> https://github.com/org/repo.git
            host_path = repo[4:]  # strip "git@"
            host, _, path = host_path.partition(":")
            if path:
                return f"https://{host}/{path}"
        return repo

    @staticmethod
    def _build_remote_git_ssh_command(remote_key_path: str | None) -> str:
        """Build GIT_SSH_COMMAND prefix for remote execution.

        Args:
            remote_key_path: Path to deploy key on the backend (or None).

        Returns:
            Shell prefix string to prepend to git commands, e.g.
            ``GIT_SSH_COMMAND="ssh -i /tmp/key ..." `` or empty string.
        """
        if not remote_key_path:
            return ""
        opts = "-o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new"
        return f'export GIT_SSH_COMMAND="ssh -i {remote_key_path} {opts}"; '

    async def _upload_deploy_key(
        self, ssh_client: SSHClient, local_key_path: Path
    ) -> str:
        """Upload a local deploy key to a temp file on the backend.

        The key content is base64-encoded before transmission over the
        encrypted SSH channel, then decoded into a temp file with 600
        permissions.

        Returns:
            The remote temp file path.
        """
        resolved = local_key_path.expanduser()
        if not resolved.exists():
            raise ValueError(f"Deploy key not found: {resolved}")
        key_content = resolved.read_text()
        key_b64 = base64.b64encode(key_content.encode()).decode()
        cmd = (
            "TMPKEY=$(mktemp /tmp/scripthut_key_XXXXXX) && "
            f"echo '{key_b64}' | base64 -d > $TMPKEY && "
            "chmod 600 $TMPKEY && echo $TMPKEY"
        )
        stdout, stderr, exit_code = await ssh_client.run_command(cmd)
        if exit_code != 0:
            raise ValueError(f"Failed to upload deploy key: {stderr}")
        return stdout.strip()

    async def _cleanup_deploy_key(
        self, ssh_client: SSHClient, remote_key_path: str
    ) -> None:
        """Remove a temporary deploy key from the backend."""
        await ssh_client.run_command(f"rm -f {remote_key_path}")

    async def _clone_git_repo(
        self,
        ssh_client: SSHClient,
        *,
        repo: str,
        branch: str,
        deploy_key: Path | None,
        clone_dir: str,
        postclone: str | None,
    ) -> tuple[str, str]:
        """Clone a git repo on the backend if not already present.

        Steps:
            1. Upload the local deploy key (if any) to a temp file on the backend.
            2. ``git ls-remote`` to resolve the branch HEAD commit hash.
            3. Clone into ``<clone_dir>/<short_hash>/`` if absent.
            4. Clean up the temp deploy key.

        Returns:
            ``(clone_path, short_hash)`` — the remote clone directory and the
            12-char commit hash prefix.
        """
        remote_key: str | None = None
        try:
            # 1. Upload deploy key if configured
            if deploy_key is not None:
                key_path = deploy_key.expanduser()
                remote_key = await self._upload_deploy_key(
                    ssh_client, key_path
                )

            git_ssh = self._build_remote_git_ssh_command(remote_key)
            # Use HTTPS for public repos (no deploy key) to avoid SSH key issues
            effective_repo = repo if remote_key else self._to_https_url(repo)

            # 2. Resolve HEAD commit hash
            cmd = f"{git_ssh}GIT_TERMINAL_PROMPT=0 git ls-remote {effective_repo} refs/heads/{branch}"
            stdout, stderr, exit_code = await ssh_client.run_command(cmd, timeout=30)
            if exit_code != 0 or not stdout.strip():
                raise ValueError(
                    f"Failed to resolve branch '{branch}' from "
                    f"'{repo}': {stderr}"
                )
            commit_hash = stdout.split()[0]
            short_hash = commit_hash[:12]

            # 3. Clone if not already present
            clone_path = f"{clone_dir}/{short_hash}"
            stdout, _, _ = await ssh_client.run_command(
                f"test -d {clone_path} && echo exists"
            )
            if "exists" not in stdout:
                logger.info(
                    f"Cloning {repo}@{branch} ({short_hash}) "
                    f"to {clone_path}"
                )
                cmd = (
                    f"{git_ssh}GIT_TERMINAL_PROMPT=0 git clone --branch {branch} "
                    f"--single-branch --depth 1 {effective_repo} {clone_path}"
                )
                _, stderr, exit_code = await ssh_client.run_command(
                    cmd, timeout=300
                )
                if exit_code != 0:
                    raise ValueError(f"Git clone failed: {stderr}")

                # Run postclone command if configured
                if postclone:
                    logger.info(f"Running postclone command in {clone_path}")
                    cmd = f"cd {clone_path} && {postclone}"
                    _, stderr, exit_code = await ssh_client.run_command(
                        cmd, timeout=300
                    )
                    if exit_code != 0:
                        raise ValueError(f"Postclone command failed: {stderr}")
            else:
                logger.info(
                    f"Reusing existing clone at {clone_path} ({short_hash})"
                )

            return clone_path, short_hash
        finally:
            # 4. Always clean up the temp deploy key
            if remote_key is not None:
                await self._cleanup_deploy_key(ssh_client, remote_key)

    def get_backend_account(self, backend_name: str) -> str | None:
        """Get the account for a backend (Slurm --account, PBS -A, etc.)."""
        backend = self.config.get_backend(backend_name)
        if backend and hasattr(backend, "account"):
            return backend.account  # type: ignore[union-attr]
        return None

    def get_backend_login_shell(self, backend_name: str) -> bool:
        """Get whether the backend uses login shell in submission scripts."""
        backend = self.config.get_backend(backend_name)
        if backend and hasattr(backend, "login_shell"):
            return backend.login_shell  # type: ignore[union-attr]
        return False

    async def _build_run(
        self,
        tasks: list[TaskDefinition],
        workflow_name: str,
        backend_name: str,
        max_concurrent: int | None,
        ssh_client: SSHClient | None,
        *,
        git_repo: str | None = None,
        git_branch: str | None = None,
        commit_hash: str | None = None,
        doc_env: list[EnvRule] | None = None,
        doc_env_groups: dict[str, list[EnvRule]] | None = None,
    ) -> Run:
        """Build a Run: resolve deps, validate, create, persist, and start processing.

        Git-related kwargs must be provided here (not set after) because
        ``_build_run`` triggers task submission via ``process_run`` before
        returning — the generated container scripts need the git info at
        submit time.
        """
        if not tasks:
            raise ValueError(f"No tasks for workflow '{workflow_name}'")

        self._resolve_wildcard_deps(tasks)
        self._validate_dependencies(tasks)

        account = self.get_backend_account(backend_name)
        login_shell = self.get_backend_login_shell(backend_name)

        first_working_dir = tasks[0].working_dir
        if ssh_client is not None:
            try:
                git_root = await self._get_git_root(ssh_client, first_working_dir)
                log_dir = f"{git_root}/.scripthut/{workflow_name}/logs"
                logger.info(f"Git root: {git_root} — logs at {log_dir}")
            except ValueError:
                log_dir = f"~/.cache/scripthut/logs/{workflow_name}"
                logger.info(f"No git root for '{first_working_dir}' — logs at {log_dir}")
        else:
            # API-based backends have no filesystem — use a synthetic placeholder.
            log_dir = f"backend://{backend_name}/{workflow_name}"
            logger.info(f"Backend '{backend_name}' has no filesystem — logs via backend API")

        run_id = str(uuid.uuid4())[:8]
        run = Run(
            id=run_id,
            workflow_name=workflow_name,
            backend_name=backend_name,
            created_at=datetime.now(timezone.utc),
            items=[RunItem(task=task) for task in tasks],
            max_concurrent=max_concurrent,
            account=account,
            login_shell=login_shell,
            log_dir=log_dir,
            git_repo=git_repo,
            git_branch=git_branch,
            commit_hash=commit_hash,
            doc_env=list(doc_env or []),
            doc_env_groups=dict(doc_env_groups or {}),
        )

        self.runs[run_id] = run
        logger.info(f"Created run '{run_id}' with {len(tasks)} tasks")

        # Persist to storage
        self._persist_run(run)

        # Start submitting tasks
        await self.process_run(run)

        return run

    async def create_adhoc_run(
        self,
        task: TaskDefinition,
        backend_name: str,
        *,
        run_name: str | None = None,
    ) -> Run:
        """Create a one-task run from an inline ``TaskDefinition``.

        Useful for ad-hoc CLI submissions and coding agents that
        synthesize a TaskDefinition directly.

        ``run_name`` becomes the run's ``workflow_name``; if omitted, a
        synthetic ``_adhoc/<task_id>`` label is used (the run still
        appears in `run list` and on the dashboard, just without a
        configured source behind it).

        Raises ``ValueError`` if the backend isn't in the config or
        doesn't have an available driver.
        """
        if self.config.get_backend(backend_name) is None:
            raise ValueError(f"Backend '{backend_name}' not found in config")

        ssh_client = self.get_ssh_client(backend_name)
        job_backend = self.get_job_backend(backend_name)
        if ssh_client is None and job_backend is None:
            raise ValueError(f"Backend '{backend_name}' is not available")

        workflow_name = run_name or f"_adhoc/{task.id}"
        return await self._build_run(
            [task], workflow_name, backend_name, max_concurrent=None,
            ssh_client=ssh_client,
        )

    async def _ls_remote_commit(
        self, repo: str, branch: str,
    ) -> str | None:
        """Resolve a remote git ref to a commit hash via local ``git ls-remote``.

        Used by API-only backends (Batch) so they can pass a concrete SHA to
        their container's runtime clone step.  Returns None on failure; the
        container will fall back to the branch name.
        """
        cmd = f"git ls-remote {repo} {branch}"
        try:
            stdout, stderr, code = await _run_local_shell(cmd, timeout=30.0)
        except Exception as e:
            logger.warning(f"Local git ls-remote failed: {e}")
            return None
        if code != 0:
            logger.warning(f"Local git ls-remote exit {code}: {stderr.strip()}")
            return None
        line = stdout.strip().split("\n", 1)[0]
        return line.split()[0] if line else None

    def _parse_tasks_json(
        self, tasks_json: str, label: str,
    ) -> tuple[list[TaskDefinition], list, dict]:
        """Parse a JSON workflow document string.

        Returns ``(tasks, doc_env, doc_env_groups)``. ``doc_env`` /
        ``doc_env_groups`` are populated from the top-level ``env:`` and
        ``env_groups:`` fields when the document uses the wrapped form.
        """
        try:
            data = json.loads(tasks_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {label}: {e}")

        try:
            return TaskDefinition.parse_document(data)
        except ValueError as e:
            raise ValueError(f"Invalid workflow JSON in {label}: {e}")

        return [TaskDefinition.from_dict(t) for t in tasks_data]

    async def _clone_source_repo(
        self, ssh_client: SSHClient, source: GitSourceConfig,
    ) -> tuple[str, str]:
        """Clone a git source's repo on the backend."""
        return await self._clone_git_repo(
            ssh_client,
            repo=source.url,
            branch=source.branch,
            deploy_key=source.deploy_key,
            clone_dir=source.clone_dir,
            postclone=source.postclone,
        )

    def _resolve_working_dirs(
        self, tasks: list[TaskDefinition], clone_dir: str,
    ) -> None:
        """Resolve task working_dir values relative to a clone directory."""
        for task in tasks:
            if task.working_dir == "~":
                task.working_dir = clone_dir
            elif not task.working_dir.startswith(("/", "~")):
                task.working_dir = f"{clone_dir}/{task.working_dir}"

    async def create_run_from_source(
        self, source_name: str, workflow_filename: str, tasks_json: str,
        backend: str,
    ) -> Run:
        """Create a run from a source workflow's JSON task list.

        For SSH-based backends and git sources, the repo is cloned on the
        backend and tasks run inside the cloned directory.  For API-based
        backends (AWS Batch), the commit hash is resolved locally and each
        container clones the source at runtime using env vars.  Path sources
        require SSH (they reference a filesystem path on the cluster).

        Args:
            source_name: Name of the source.
            workflow_filename: Filename of the workflow (e.g. "train.json").
            tasks_json: Raw JSON task list content.
            backend: Name of the backend to submit tasks to.
        """
        source = self.config.get_source(source_name)
        if source is None:
            raise ValueError(f"Source '{source_name}' not found")

        backend_name = backend
        ssh_client = self.get_ssh_client(backend_name)
        job_backend = self.get_job_backend(backend_name)
        if ssh_client is None and job_backend is None:
            raise ValueError(f"Backend '{backend_name}' is not available")

        stem = workflow_filename.removesuffix(".json")
        workflow_name = f"{source_name}/{stem}"
        label = f"source '{source_name}' workflow '{workflow_filename}'"

        tasks, doc_env, doc_env_groups = self._parse_tasks_json(tasks_json, label)

        clone_dir: str | None = None
        commit_hash: str | None = None

        if isinstance(source, GitSourceConfig):
            if ssh_client is not None:
                # SSH backend: clone on the backend filesystem now.
                clone_dir, commit_hash = await self._clone_source_repo(
                    ssh_client, source
                )
                self._resolve_working_dirs(tasks, clone_dir)
            else:
                # API-only backend: resolve the commit locally so the
                # container can check out the same ref at runtime.  Don't
                # touch tasks.working_dir — the backend's generate_script
                # rewrites relative paths against $_SCRIPTHUT_CLONE_DIR.
                commit_hash = await self._ls_remote_commit(source.url, source.branch)
        elif isinstance(source, PathSourceConfig):
            if ssh_client is None:
                raise ValueError(
                    f"Path source '{source_name}' requires an SSH backend; "
                    f"'{backend_name}' has no filesystem."
                )
            # Path sources already exist on the backend.
            self._resolve_working_dirs(tasks, source.path)

        git_repo = source.url if isinstance(source, GitSourceConfig) else None
        git_branch = source.branch if isinstance(source, GitSourceConfig) else None
        run = await self._build_run(
            tasks, workflow_name, backend_name, None, ssh_client,
            git_repo=git_repo, git_branch=git_branch, commit_hash=commit_hash,
            doc_env=doc_env, doc_env_groups=doc_env_groups,
        )
        return run

    async def dry_run_source(
        self, source_name: str, workflow_filename: str, tasks_json: str,
        backend: str,
    ) -> dict:
        """Dry run a source workflow — preview tasks without submitting."""
        source = self.config.get_source(source_name)
        if source is None:
            raise ValueError(f"Source '{source_name}' not found")
        backend_name = backend

        stem = workflow_filename.removesuffix(".json")
        workflow_name = f"{source_name}/{stem}"
        label = f"source '{source_name}' workflow '{workflow_filename}'"

        tasks, doc_env, doc_env_groups = self._parse_tasks_json(tasks_json, label)

        # Resolve working dirs for preview
        warnings: list[str] = []
        commit_hash: str | None = None
        if isinstance(source, GitSourceConfig):
            # For dry run, clone so we can show correct paths
            ssh_client = self.get_ssh_client(backend_name)
            if ssh_client:
                try:
                    clone_dir, commit_hash = await self._clone_source_repo(
                        ssh_client, source
                    )
                    self._resolve_working_dirs(tasks, clone_dir)
                except Exception as e:
                    logger.warning(f"Could not clone for dry run preview: {e}")
                    warnings.append(
                        f"Could not clone repository on backend '{backend_name}': {e}. "
                        f"The run will fail if this backend cannot access the git remote."
                    )
                    self._resolve_working_dirs(tasks, f"{source.clone_dir}/<commit>")
            else:
                warnings.append(f"Backend '{backend_name}' is not connected. Cannot preview clone paths.")
                self._resolve_working_dirs(tasks, f"{source.clone_dir}/<commit>")
        elif isinstance(source, PathSourceConfig):
            self._resolve_working_dirs(tasks, source.path)

        self._resolve_wildcard_deps(tasks)
        self._validate_dependencies(tasks)

        account = self.get_backend_account(backend_name)
        login_shell = self.get_backend_login_shell(backend_name)

        log_dir = "~/.cache/scripthut/logs"
        ssh_client = self.get_ssh_client(backend_name)
        if ssh_client and log_dir.startswith("~"):
            stdout, _, _ = await ssh_client.run_command("echo $HOME")
            home_dir = stdout.strip()
            log_dir = log_dir.replace("~", home_dir, 1)

        preview_run_id = "preview"
        preview_created_at = datetime.now(timezone.utc)
        job_backend = self.get_job_backend(backend_name)

        task_details = []
        for task in tasks:
            merged_env, extra_init = resolve_for_task(
                self.config,
                backend_name=backend_name,
                workflow_name=workflow_name,
                run_id=preview_run_id,
                created_at=preview_created_at,
                task=task,
                doc_env=doc_env,
                doc_env_groups=doc_env_groups,
            )
            if job_backend:
                script = job_backend.generate_script(
                    task, preview_run_id, log_dir,
                    account=account, login_shell=login_shell,
                    env_vars=merged_env, extra_init=extra_init,
                )
            else:
                script = task.to_sbatch_script(
                    preview_run_id, log_dir,
                    account=account, login_shell=login_shell,
                    env_vars=merged_env, extra_init=extra_init,
                )
            task_details.append({
                "task": task,
                "submit_script": script,
                "output_path": task.get_output_path(preview_run_id, log_dir),
                "error_path": task.get_error_path(preview_run_id, log_dir),
            })

        try:
            raw_output_formatted = json.dumps(json.loads(tasks_json), indent=2)
        except (json.JSONDecodeError, TypeError):
            raw_output_formatted = tasks_json

        return {
            "workflow": {
                "name": workflow_name,
                "description": "",
                "command": f"(from source {source_name}: {workflow_filename})",
            },
            "submit_url": f"/sources/{source_name}/workflows/{workflow_filename}/run?backend={backend_name}",
            "backend_name": backend_name,
            "task_count": len(tasks),
            "max_concurrent": None,
            "account": account,
            "commit_hash": commit_hash,
            "tasks": task_details,
            "raw_output": raw_output_formatted,
            "warnings": warnings,
        }

    async def rerun_in_place(self, run_id: str) -> Run:
        """Reset an existing run and reprocess it.

        All items are reset to PENDING with runtime state cleared.
        The run keeps its original ID, commit hash, and parameters.
        """
        run = self.get_run(run_id)
        if run is None:
            raise ValueError(f"Run '{run_id}' not found")

        if run.status in (RunStatus.RUNNING, RunStatus.PENDING):
            raise ValueError("Cannot rerun a run that is still active")

        # Reset all items to pending
        for item in run.items:
            item.status = RunItemStatus.PENDING
            item.job_id = None
            item.submitted_at = None
            item.started_at = None
            item.finished_at = None
            item.error = None
            item.submit_script = None
            item.submit_output = None
            item.cpu_efficiency = None
            item.max_rss = None
            item.scheduler_state = None

        run.created_at = datetime.now(timezone.utc)
        self._persist_run(run)
        self.notify_run(run_id)

        # Start submitting tasks
        await self.process_run(run)

        return run

    async def discover_workflows(self, project_name: str) -> list[str]:
        """Discover sflow.json files in a project repo via git ls-files."""
        project = self.config.get_project(project_name)
        if project is None:
            raise ValueError(f"Project '{project_name}' not found")

        ssh_client = self.get_ssh_client(project.backend)
        if ssh_client is None:
            raise ValueError(
                f"No SSH connection to backend '{project.backend}'"
            )

        stdout, stderr, exit_code = await ssh_client.run_command(
            f"cd {project.path} && git ls-files '*/sflow.json' 'sflow.json'"
        )
        if exit_code != 0:
            raise ValueError(
                f"git ls-files failed in {project.path}: {stderr}"
            )

        paths = [line.strip() for line in stdout.strip().splitlines() if line.strip()]
        logger.info(
            f"Discovered {len(paths)} workflows in project '{project_name}'"
        )
        return paths

    async def create_run_from_project(
        self, project_name: str, workflow_path: str, *, backend: str | None = None,
    ) -> Run:
        """Create a run from a sflow.json in a project repo.

        ``backend`` overrides the project's configured backend.
        """
        project = self.config.get_project(project_name)
        if project is None:
            raise ValueError(f"Project '{project_name}' not found")

        if backend and self.config.get_backend(backend) is None:
            raise ValueError(f"Backend '{backend}' not found in config")
        backend_name = backend or project.backend

        ssh_client = self.get_ssh_client(backend_name)
        if ssh_client is None:
            raise ValueError(
                f"No SSH connection to backend '{backend_name}'"
            )

        full_path = f"{project.path}/{workflow_path}"
        stdout, stderr, exit_code = await ssh_client.run_command(
            f"cat {full_path}"
        )
        if exit_code != 0:
            raise ValueError(
                f"Failed to read '{full_path}': {stderr}"
            )

        try:
            data = json.loads(stdout)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in '{full_path}': {e}")

        if isinstance(data, dict) and "tasks" in data:
            tasks_data = data["tasks"]
        elif isinstance(data, list):
            tasks_data = data
        else:
            raise ValueError(
                f"JSON in '{full_path}' must be a list or dict with 'tasks' key"
            )

        tasks = [TaskDefinition.from_dict(t) for t in tasks_data]

        sflow_dir = workflow_path.rsplit("/", 1)[0] if "/" in workflow_path else ""
        default_working_dir = (
            f"{project.path}/{sflow_dir}" if sflow_dir else project.path
        )
        for task in tasks:
            if task.working_dir == "~":
                task.working_dir = default_working_dir

        workflow_name = (
            f"{project.name}/{sflow_dir}" if sflow_dir else project.name
        )

        return await self._build_run(
            tasks, workflow_name, backend_name, project.max_concurrent,
            ssh_client
        )

    def get_job_backend(self, backend_name: str) -> JobBackend | None:
        """Get the JobBackend for a backend name."""
        return self.job_backends.get(backend_name)

    async def submit_task(self, run: Run, item: RunItem) -> bool:
        """Submit a single task to the scheduler."""
        job_backend = self.get_job_backend(run.backend_name)
        if job_backend is None:
            item.status = RunItemStatus.FAILED
            item.error = f"No job backend for '{run.backend_name}'"
            item.finished_at = datetime.now(timezone.utc)
            self._persist_run(run)
            return False

        ssh_client = self.get_ssh_client(run.backend_name)

        # SSH-based backends resolve ~ and create the log directory before
        # submission.  API-based backends (Batch) route logs elsewhere.
        log_dir = run.log_dir
        if ssh_client is not None:
            if log_dir.startswith("~"):
                stdout, _, _ = await ssh_client.run_command("echo $HOME")
                home_dir = stdout.strip()
                log_dir = log_dir.replace("~", home_dir, 1)
            await ssh_client.run_command(f"mkdir -p {log_dir}")

        merged_env, extra_init = self._resolve_environment(run, item.task)
        script = job_backend.generate_script(
            item.task, run.id, log_dir,
            account=run.account, login_shell=run.login_shell,
            env_vars=merged_env, extra_init=extra_init,
        )
        item.submit_script = script

        try:
            result = await job_backend.submit_task(
                item.task, script, env_vars=merged_env,
            )
            item.job_id = result.job_id
            item.submit_output = result.submit_output or None
            item.status = RunItemStatus.SUBMITTED
            item.submitted_at = datetime.now(timezone.utc)
            logger.info(f"Submitted task '{item.task.id}' as job {result.job_id}")
            self._persist_run(run)
            return True
        except RuntimeError as e:
            item.status = RunItemStatus.FAILED
            item.error = str(e)
            item.submit_output = item.submit_output or str(e)
            item.finished_at = datetime.now(timezone.utc)
            logger.error(f"Failed to submit task '{item.task.id}': {e}")
            self._persist_run(run)
            return False

    async def process_run(self, run: Run) -> None:
        """Process a run - submit tasks up to max_concurrent respecting dependencies."""
        # Cascade failures
        changed = True
        while changed:
            changed = False
            for item in run.items:
                if item.status != RunItemStatus.PENDING:
                    continue
                failed_deps = run.get_failed_deps(item)
                if failed_deps:
                    item.status = RunItemStatus.DEP_FAILED
                    item.error = f"Dependency '{failed_deps[0]}' failed"
                    item.finished_at = datetime.now(timezone.utc)
                    self._persist_run(run)
                    changed = True

        active_count = run.running_count

        ready_items = [
            item for item in run.items
            if item.status == RunItemStatus.PENDING
            and run.are_deps_satisfied(item)
        ]

        # Per-run cap (if set)
        if run.max_concurrent is not None:
            run_slots = run.max_concurrent - active_count
        else:
            run_slots = len(ready_items)

        # Backend-level cap
        backend_max = self._get_backend_max_concurrent(run.backend_name)
        backend_active = self._backend_running_count(run.backend_name)
        backend_slots = backend_max - backend_active

        slots_available = max(0, min(run_slots, backend_slots))
        to_submit = ready_items[:slots_available]

        if to_submit:
            task_ids = [item.task.id for item in to_submit]
            logger.info(
                f"Run '{run.id}': submitting {len(to_submit)} tasks: {task_ids} "
                f"(run_slots={run_slots}, backend_slots={backend_slots})"
            )

        for item in to_submit:
            success = await self.submit_task(run, item)
            if not success:
                # Submission failure (e.g. bad queue) — mark all remaining pending tasks as failed
                for pending_item in run.items:
                    if pending_item.status == RunItemStatus.PENDING:
                        pending_item.status = RunItemStatus.FAILED
                        pending_item.error = f"Submission halted: {item.error}"
                        pending_item.finished_at = datetime.now(timezone.utc)
                self._persist_run(run)
                break

        if to_submit:
            self.notify_run(run.id)

    def _backend_running_count(self, backend_name: str) -> int:
        """Count all running/submitted tasks across all runs on a backend."""
        count = 0
        for run in self.runs.values():
            if run.backend_name == backend_name:
                count += run.running_count
        return count

    def _get_backend_max_concurrent(self, backend_name: str) -> int:
        """Get the max_concurrent limit for a backend."""
        backend = self.config.get_backend(backend_name)
        if backend:
            return backend.max_concurrent
        return 100  # Default if backend not found

    def _persist_run(self, run: Run) -> None:
        """Mark run dirty for next save cycle."""
        if self.storage:
            self.storage.mark_dirty(run.id)

    def notify_run(self, run_id: str) -> None:
        """Wake all SSE listeners for a run."""
        self._run_versions[run_id] = self._run_versions.get(run_id, 0) + 1
        old_event = self._run_events.get(run_id)
        self._run_events[run_id] = asyncio.Event()
        if old_event:
            old_event.set()

    async def wait_for_update(self, run_id: str, timeout: float = 30.0) -> bool:
        """Wait for a run state change. Returns True if notified, False on timeout."""
        if run_id not in self._run_events:
            self._run_events[run_id] = asyncio.Event()
        event = self._run_events[run_id]
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def update_run_status(self, run: Run, slurm_jobs: dict[str, JobState]) -> None:
        """Update run item statuses based on Slurm job states."""
        changed = False
        changed_items: list[RunItem] = []

        # Snapshot items that existed before this update cycle.
        # _handle_generates_source (called below) may append new items and
        # submit them via process_run.  Those new jobs won't appear in the
        # current slurm_jobs dict, so checking them would falsely mark them
        # COMPLETED.  Only iterate over the pre-existing items.
        items_snapshot = list(run.items)

        # State transitions are driven by *evidence*: what squeue reported
        # for the job. We deliberately never mark SUBMITTED as FAILED on
        # absence-from-squeue here — that's an absence-of-evidence, not
        # evidence-of-failure, and it triggers exactly the false-failure
        # cascade we're trying to avoid. The evidence-based sacct path
        # (in main.poll_backend) resolves those cases.
        for item in items_snapshot:
            if item.job_id is None:
                continue

            if item.status in (
                RunItemStatus.COMPLETED,
                RunItemStatus.FAILED,
                RunItemStatus.DEP_FAILED,
            ):
                continue

            job_state = slurm_jobs.get(item.job_id)

            if job_state is None:
                # Missing from squeue. Action depends on whether we
                # *ever* saw the scheduler hold this job.
                if item.status in (RunItemStatus.QUEUED, RunItemStatus.RUNNING):
                    # We observed the scheduler had it; gone now means
                    # it finished. Optimistic COMPLETED; sacct will
                    # correct to FAILED if accounting disagrees.
                    item.started_at = item.started_at or item.submitted_at
                    item.status = RunItemStatus.COMPLETED
                    item.finished_at = datetime.now(timezone.utc)
                    changed = True
                    changed_items.append(item)
                    logger.info(
                        f"Task '{item.task.id}' (job {item.job_id}) "
                        f"left scheduler queue — marking COMPLETED (sacct may correct)"
                    )
                    if item.task.generates_source:
                        await self._handle_generates_source(run, item)
                # SUBMITTED + missing: do nothing here. The item stays
                # SUBMITTED until either it shows up in squeue or the
                # sacct-evidence path resolves it. No timer, no marker,
                # no false FAILED.
                continue

            # squeue gave us something — translate to our state.
            if job_state in (JobState.RUNNING, JobState.COMPLETING):
                if item.status != RunItemStatus.RUNNING:
                    item.status = RunItemStatus.RUNNING
                    item.started_at = item.started_at or datetime.now(timezone.utc)
                    changed = True
                    changed_items.append(item)
                    logger.info(
                        f"Task '{item.task.id}' (job {item.job_id}) started running"
                    )
            elif job_state == JobState.PENDING:
                # SUBMITTED -> QUEUED: scheduler now confirms the job
                # is in its queue. Stay QUEUED on re-observation; flip
                # back from RUNNING only logs, doesn't usually happen.
                if item.status not in (RunItemStatus.QUEUED, RunItemStatus.RUNNING):
                    item.status = RunItemStatus.QUEUED
                    changed = True
                    changed_items.append(item)
                    logger.info(
                        f"Task '{item.task.id}' (job {item.job_id}) "
                        f"acknowledged by scheduler — QUEUED"
                    )
            elif job_state == JobState.COMPLETED:
                item.started_at = item.started_at or item.submitted_at
                item.status = RunItemStatus.COMPLETED
                item.finished_at = datetime.now(timezone.utc)
                changed = True
                changed_items.append(item)
                logger.info(f"Task '{item.task.id}' (job {item.job_id}) completed")
                if item.task.generates_source:
                    await self._handle_generates_source(run, item)
            elif job_state in (
                JobState.FAILED,
                JobState.CANCELLED,
                JobState.TIMEOUT,
                JobState.NODE_FAIL,
                JobState.PREEMPTED,
                JobState.BOOT_FAIL,
                JobState.DEADLINE,
                JobState.OUT_OF_MEMORY,
            ):
                item.started_at = item.started_at or item.submitted_at
                item.status = RunItemStatus.FAILED
                item.error = f"Slurm job {job_state.value}"
                item.finished_at = datetime.now(timezone.utc)
                changed = True
                changed_items.append(item)
                logger.info(
                    f"Task '{item.task.id}' (job {item.job_id}) "
                    f"failed: {job_state.value}"
                )

        if changed:
            self._persist_run(run)
            await self.process_run(run)
            self.notify_run(run.id)

    async def update_all_runs(self, backend_jobs: dict[str, list[tuple[str, JobState]]]) -> None:
        """Update all active runs based on Slurm job states."""
        for run in self.runs.values():
            if run.status in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED):
                continue

            jobs = backend_jobs.get(run.backend_name, [])
            job_states = {job_id: state for job_id, state in jobs}

            await self.update_run_status(run, job_states)

    async def cancel_run(self, run_id: str) -> bool:
        """Cancel all pending and running items in a run."""
        run = self.runs.get(run_id)
        if run is None:
            return False

        ssh_client = self.get_ssh_client(run.backend_name)

        for item in run.items:
            if item.status == RunItemStatus.PENDING:
                item.status = RunItemStatus.FAILED
                item.error = "Cancelled"
                item.finished_at = datetime.now(timezone.utc)
            elif item.status in (RunItemStatus.SUBMITTED, RunItemStatus.RUNNING):
                if item.job_id:
                    job_backend = self.get_job_backend(run.backend_name)
                    if job_backend:
                        await job_backend.cancel_job(item.job_id)
                    elif ssh_client:
                        await ssh_client.run_command(f"scancel {item.job_id}")
                item.started_at = item.started_at or item.submitted_at
                item.status = RunItemStatus.FAILED
                item.error = "Cancelled"
                item.finished_at = datetime.now(timezone.utc)

        self._persist_run(run)
        self.notify_run(run.id)
        logger.info(f"Cancelled run '{run_id}'")
        return True

    def delete_run(self, run_id: str) -> bool:
        """Delete a terminal run."""
        run = self.runs.get(run_id)
        if run is None:
            return False

        if run.status in (RunStatus.PENDING, RunStatus.RUNNING):
            return False

        # Delete from storage
        if self.storage:
            self.storage.delete_run(run)

        del self.runs[run_id]

        self._run_versions.pop(run_id, None)
        self._run_events.pop(run_id, None)

        logger.info(f"Deleted run '{run_id}'")
        return True

    def get_run(self, run_id: str) -> Run | None:
        """Get a run by ID."""
        return self.runs.get(run_id)

    def get_all_runs(self) -> list[Run]:
        """Get all runs, sorted by creation time (newest first)."""
        return sorted(self.runs.values(), key=lambda r: r.created_at, reverse=True)

    def get_active_runs(self) -> list[Run]:
        """Get all runs that are still running."""
        return [
            r for r in self.runs.values()
            if r.status in (RunStatus.PENDING, RunStatus.RUNNING)
        ]

    async def restore_from_storage(self) -> int:
        """Restore runs from folder storage on startup."""
        if self.storage is None:
            return 0

        all_runs = self.storage.load_all_runs()
        for run_id, run in all_runs.items():
            if run.workflow_name == "_default":
                continue  # Don't load default runs into active management
            if run_id not in self.runs:
                self.runs[run_id] = run
                if run.status in (RunStatus.PENDING, RunStatus.RUNNING):
                    await self.process_run(run)

        logger.info(f"Restored {len(self.runs)} runs from storage")
        return len(self.runs)

    def save_dirty(self) -> None:
        """Save all dirty runs to disk."""
        if self.storage:
            self.storage.save_if_dirty(self.runs)

    async def fetch_log_file(
        self,
        run_id: str,
        task_id: str,
        log_type: str = "output",
        tail_lines: int | None = None,
    ) -> tuple[str | None, str | None]:
        """Fetch a log file from the backend.

        SSH-based backends read the file off the filesystem; API-based
        backends (e.g. Batch) route through CloudWatch.  In both cases the
        actual fetch is delegated to ``JobBackend.fetch_log``.
        """
        run = self.runs.get(run_id)
        if run is None:
            return None, f"Run '{run_id}' not found"

        item = run.get_item_by_task_id(task_id)
        if item is None:
            return None, f"Task '{task_id}' not found in run"

        if log_type not in ("output", "error"):
            return None, f"Invalid log_type: {log_type}"

        job_backend = self.get_job_backend(run.backend_name)
        if job_backend is None:
            return None, f"No job backend for '{run.backend_name}'"

        if item.job_id is None:
            return None, "Task has not been submitted yet"

        # Resolve the filesystem log path (SSH backends use it; API backends ignore).
        log_dir = run.log_dir
        ssh_client = self.get_ssh_client(run.backend_name)
        if ssh_client is not None and log_dir.startswith("~"):
            stdout, _, _ = await ssh_client.run_command("echo $HOME")
            home_dir = stdout.strip()
            log_dir = log_dir.replace("~", home_dir, 1)
        if log_type == "output":
            log_path = item.task.get_output_path(run.id, log_dir)
        else:
            log_path = item.task.get_error_path(run.id, log_dir)

        return await job_backend.fetch_log(
            job_id=item.job_id,
            log_path=log_path,
            log_type=log_type,
            tail_lines=tail_lines,
        )
