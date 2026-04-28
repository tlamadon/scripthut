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
    GitSourceConfig,
    PathSourceConfig,
    ProjectConfig,
    ScriptHutConfig,
    WorkflowConfig,
)
from scripthut.models import JobState
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
        self, task: TaskDefinition
    ) -> tuple[dict[str, str] | None, str]:
        """Resolve environment config for a task.

        Merges named environment variables (from config) with per-task
        env_vars specified by the generator.  Per-task values override
        named-environment values when keys collide.
        """
        env_vars: dict[str, str] = {}
        extra_init = ""

        # Named environment from config
        if task.environment:
            env_config = self.config.get_environment(task.environment)
            if env_config is None:
                logger.warning(
                    f"Task '{task.id}' references unknown environment '{task.environment}'"
                )
            else:
                env_vars.update(env_config.variables)
                extra_init = env_config.extra_init

        # Per-task env_vars from generator (override named env)
        if task.env_vars:
            env_vars.update(task.env_vars)

        return env_vars or None, extra_init

    @staticmethod
    def _scripthut_env_vars(
        workflow_name: str,
        run_id: str,
        created_at: datetime,
        git_repo: str | None = None,
        git_branch: str | None = None,
        git_sha: str | None = None,
    ) -> dict[str, str]:
        """Return standard SCRIPTHUT_* environment variables for a job."""
        env = {
            "SCRIPTHUT_WORKFLOW": workflow_name,
            "SCRIPTHUT_RUN_ID": run_id,
            "SCRIPTHUT_CREATED_AT": created_at.isoformat(),
        }
        if git_repo is not None:
            env["SCRIPTHUT_GIT_REPO"] = git_repo
        if git_branch is not None:
            env["SCRIPTHUT_GIT_BRANCH"] = git_branch
        if git_sha is not None:
            env["SCRIPTHUT_GIT_SHA"] = git_sha
        return env

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

        if isinstance(data, dict) and "tasks" in data:
            tasks_data = data["tasks"]
        elif isinstance(data, list):
            tasks_data = data
        else:
            logger.error(
                f"Unexpected JSON structure in generates_source '{path}': "
                f"expected dict with 'tasks' key or a list"
            )
            return

        new_tasks = [TaskDefinition.from_dict(t) for t in tasks_data]

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

    def get_workflow(self, name: str) -> WorkflowConfig | None:
        """Get a workflow by name."""
        return self.config.get_workflow(name)

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

    async def _ensure_repo_cloned(
        self, ssh_client: SSHClient, workflow: WorkflowConfig
    ) -> tuple[str, str]:
        """Clone a workflow's git repo on the backend if not already present."""
        git_cfg = workflow.git
        if git_cfg is None:
            raise ValueError("Workflow has no git configuration")

        return await self._clone_git_repo(
            ssh_client,
            repo=git_cfg.repo,
            branch=git_cfg.branch,
            deploy_key=git_cfg.deploy_key,
            clone_dir=git_cfg.clone_dir,
            postclone=git_cfg.postclone,
        )

    async def fetch_tasks(
        self,
        workflow: WorkflowConfig,
        *,
        return_raw: bool = False,
        clone_dir: str | None = None,
    ) -> list[TaskDefinition] | tuple[list[TaskDefinition], str]:
        """Fetch task list from a workflow.

        Runs the workflow command on the backend via SSH for SSH-based backends;
        for API-based backends (no SSH) the command is executed locally as a
        subprocess of the scripthut server.
        """
        logger.info(
            f"Fetching tasks from workflow '{workflow.name}' on backend '{workflow.backend}'"
        )

        command = workflow.command
        if clone_dir:
            command = f"cd {clone_dir} && {command}"

        ssh_client = self.get_ssh_client(workflow.backend)
        if ssh_client is not None:
            stdout, stderr, exit_code = await ssh_client.run_command(command)
        else:
            # API-based backend — run the generator locally so we still get a task list.
            stdout, stderr, exit_code = await _run_local_shell(command)

        if exit_code != 0:
            raise ValueError(f"Command failed (exit {exit_code}): {stderr}")

        try:
            data = json.loads(stdout)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON response: {e}")

        if isinstance(data, dict) and "tasks" in data:
            tasks_data = data["tasks"]
        elif isinstance(data, list):
            tasks_data = data
        else:
            raise ValueError("JSON must be a list or object with 'tasks' key")

        tasks = [TaskDefinition.from_dict(t) for t in tasks_data]
        logger.info(f"Fetched {len(tasks)} tasks from workflow '{workflow.name}'")

        if return_raw:
            return tasks, stdout
        return tasks

    async def dry_run(self, workflow_name: str) -> dict:
        """Perform a dry run - fetch tasks and show what would be submitted."""
        workflow = self.get_workflow(workflow_name)
        if workflow is None:
            raise ValueError(f"Workflow '{workflow_name}' not found")

        # Clone git repo on backend if configured
        clone_dir: str | None = None
        commit_hash: str | None = None
        if workflow.git is not None:
            ssh_client = self.get_ssh_client(workflow.backend)
            if ssh_client is not None:
                clone_dir, commit_hash = await self._ensure_repo_cloned(
                    ssh_client, workflow
                )

        result = await self.fetch_tasks(workflow, return_raw=True, clone_dir=clone_dir)
        tasks: list[TaskDefinition] = result[0]  # type: ignore[index]
        raw_output: str = result[1]  # type: ignore[index]

        # Resolve tasks' working_dir relative to clone directory
        if clone_dir:
            for task in tasks:
                if task.working_dir == "~":
                    task.working_dir = clone_dir
                elif not task.working_dir.startswith(("/", "~")):
                    task.working_dir = f"{clone_dir}/{task.working_dir}"

        self._resolve_wildcard_deps(tasks)
        self._validate_dependencies(tasks)

        preview_run_id = "preview"
        log_dir = "~/.cache/scripthut/logs"

        ssh_client = self.get_ssh_client(workflow.backend)
        if ssh_client and log_dir.startswith("~"):
            stdout, _, _ = await ssh_client.run_command("echo $HOME")
            home_dir = stdout.strip()
            log_dir = log_dir.replace("~", home_dir, 1)

        account = self.get_backend_account(workflow.backend)
        login_shell = self.get_backend_login_shell(workflow.backend)

        preview_created_at = datetime.now(timezone.utc)
        git_repo = workflow.git.repo if workflow.git else None
        git_branch = workflow.git.branch if workflow.git else None
        job_backend = self.get_job_backend(workflow.backend)

        task_details = []
        for task in tasks:
            env_vars, extra_init = self._resolve_environment(task)
            sh_vars = self._scripthut_env_vars(
                workflow_name, preview_run_id, preview_created_at,
                git_repo=git_repo, git_branch=git_branch, git_sha=commit_hash,
            )
            merged_env = {**sh_vars, **(env_vars or {})}
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

        logger.info(f"Dry run for workflow '{workflow_name}': {len(tasks)} tasks")

        try:
            raw_output_formatted = json.dumps(json.loads(raw_output), indent=2)
        except (json.JSONDecodeError, TypeError):
            raw_output_formatted = raw_output

        return {
            "workflow": workflow,
            "backend_name": workflow.backend,
            "task_count": len(tasks),
            "max_concurrent": workflow.max_concurrent,
            "account": account,
            "commit_hash": commit_hash,
            "tasks": task_details,
            "raw_output": raw_output_formatted,
        }

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
        )

        self.runs[run_id] = run
        logger.info(f"Created run '{run_id}' with {len(tasks)} tasks")

        # Persist to storage
        self._persist_run(run)

        # Start submitting tasks
        await self.process_run(run)

        return run

    async def create_run(self, workflow_name: str) -> Run:
        """Create a new run from a workflow."""
        workflow = self.get_workflow(workflow_name)
        if workflow is None:
            raise ValueError(f"Workflow '{workflow_name}' not found")

        ssh_client = self.get_ssh_client(workflow.backend)
        job_backend = self.get_job_backend(workflow.backend)
        if ssh_client is None and job_backend is None:
            raise ValueError(
                f"Backend '{workflow.backend}' is not available"
            )

        # Clone git repo on the backend if configured and SSH-capable.
        # For API-only backends (e.g. Batch), the commit hash is resolved locally
        # so the container can check out the same ref at runtime.
        clone_dir: str | None = None
        commit_hash: str | None = None
        if workflow.git is not None:
            if ssh_client is not None:
                clone_dir, commit_hash = await self._ensure_repo_cloned(
                    ssh_client, workflow
                )
            else:
                commit_hash = await self._resolve_commit_locally(workflow)

        tasks: list[TaskDefinition] = await self.fetch_tasks(  # type: ignore[assignment]
            workflow, clone_dir=clone_dir
        )

        # Resolve tasks' working_dir relative to the clone directory
        if clone_dir:
            self._resolve_working_dirs(tasks, clone_dir)

        git_repo = workflow.git.repo if workflow.git is not None else None
        git_branch = workflow.git.branch if workflow.git is not None else None
        run = await self._build_run(
            tasks, workflow_name, workflow.backend, workflow.max_concurrent, ssh_client,
            git_repo=git_repo, git_branch=git_branch, commit_hash=commit_hash,
        )
        return run

    async def _resolve_commit_locally(
        self, workflow: WorkflowConfig,
    ) -> str | None:
        """Best-effort resolution of a git workflow's ref to a commit hash."""
        git_cfg = workflow.git
        if git_cfg is None:
            return None
        return await self._ls_remote_commit(git_cfg.repo, git_cfg.branch)

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

    def _parse_tasks_json(self, tasks_json: str, label: str) -> list[TaskDefinition]:
        """Parse a JSON task list string into TaskDefinition objects."""
        try:
            data = json.loads(tasks_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {label}: {e}")

        if isinstance(data, dict) and "tasks" in data:
            tasks_data = data["tasks"]
        elif isinstance(data, list):
            tasks_data = data
        else:
            raise ValueError(f"JSON in {label} must be a list or dict with 'tasks' key")

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

        tasks = self._parse_tasks_json(tasks_json, label)

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
        )
        return run

    async def rerun_from(self, run_id: str) -> Run:
        """Re-run using the parameters of an existing run.

        If the original clone directory still exists on the backend, it is
        reused.  Otherwise the repo is cloned again and checked out at the
        same commit hash that was recorded on the original run.
        """
        original = self.get_run(run_id)
        if original is None:
            raise ValueError(f"Run '{run_id}' not found")

        workflow = self.get_workflow(original.workflow_name)
        if workflow is None:
            raise ValueError(
                f"Workflow '{original.workflow_name}' not found in current config"
            )

        ssh_client = self.get_ssh_client(original.backend_name)
        if ssh_client is None:
            raise ValueError(
                f"No SSH connection to backend '{original.backend_name}'"
            )

        clone_dir: str | None = None
        commit_hash = original.commit_hash

        if workflow.git is not None and commit_hash:
            git_cfg = workflow.git
            short_hash = commit_hash[:12]
            expected_dir = f"{git_cfg.clone_dir}/{short_hash}"

            # Check if the original clone directory still exists
            stdout, _, _ = await ssh_client.run_command(
                f"test -d {expected_dir} && echo exists"
            )
            if "exists" in stdout:
                logger.info(
                    f"Rerun: reusing existing clone at {expected_dir}"
                )
                clone_dir = expected_dir
            else:
                # Clone again at the exact commit
                logger.info(
                    f"Rerun: clone not found at {expected_dir}, "
                    f"cloning {git_cfg.repo} at {commit_hash}"
                )
                remote_key: str | None = None
                try:
                    if git_cfg.deploy_key is not None:
                        key_path = git_cfg.deploy_key.expanduser()
                        remote_key = await self._upload_deploy_key(
                            ssh_client, key_path
                        )
                    git_ssh = self._build_remote_git_ssh_command(remote_key)

                    # Full clone (need full history to checkout arbitrary SHA)
                    cmd = (
                        f"{git_ssh}git clone --single-branch "
                        f"--branch {git_cfg.branch} "
                        f"{git_cfg.repo} {expected_dir}"
                    )
                    _, stderr, exit_code = await ssh_client.run_command(
                        cmd, timeout=300
                    )
                    if exit_code != 0:
                        raise ValueError(f"Git clone failed: {stderr}")

                    # Checkout the exact commit
                    cmd = f"cd {expected_dir} && git checkout {commit_hash}"
                    _, stderr, exit_code = await ssh_client.run_command(
                        cmd, timeout=60
                    )
                    if exit_code != 0:
                        raise ValueError(
                            f"Git checkout {commit_hash} failed: {stderr}"
                        )

                    # Run postclone command if configured
                    if git_cfg.postclone:
                        logger.info(
                            f"Running postclone command in {expected_dir}"
                        )
                        cmd = f"cd {expected_dir} && {git_cfg.postclone}"
                        _, stderr, exit_code = await ssh_client.run_command(
                            cmd, timeout=300
                        )
                        if exit_code != 0:
                            raise ValueError(
                                f"Postclone command failed: {stderr}"
                            )

                    clone_dir = expected_dir
                finally:
                    if remote_key is not None:
                        await self._cleanup_deploy_key(
                            ssh_client, remote_key
                        )

        # Re-fetch tasks from the workflow command in the clone dir
        tasks: list[TaskDefinition] = await self.fetch_tasks(  # type: ignore[assignment]
            workflow, clone_dir=clone_dir
        )

        # Resolve working_dir relative to clone directory
        if clone_dir:
            self._resolve_working_dirs(tasks, clone_dir)

        git_repo = workflow.git.repo if workflow.git is not None else None
        git_branch = workflow.git.branch if workflow.git is not None else None
        run = await self._build_run(
            tasks,
            original.workflow_name,
            original.backend_name,
            original.max_concurrent,
            ssh_client,
            git_repo=git_repo,
            git_branch=git_branch,
            commit_hash=commit_hash,
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

        tasks = self._parse_tasks_json(tasks_json, label)

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
            env_vars, extra_init = self._resolve_environment(task)
            sh_vars = self._scripthut_env_vars(
                workflow_name, preview_run_id, preview_created_at,
            )
            merged_env = {**sh_vars, **(env_vars or {})}
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
        self, project_name: str, workflow_path: str
    ) -> Run:
        """Create a run from a sflow.json in a project repo."""
        project = self.config.get_project(project_name)
        if project is None:
            raise ValueError(f"Project '{project_name}' not found")

        ssh_client = self.get_ssh_client(project.backend)
        if ssh_client is None:
            raise ValueError(
                f"No SSH connection to backend '{project.backend}'"
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
            tasks, workflow_name, project.backend, project.max_concurrent,
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

        env_vars, extra_init = self._resolve_environment(item.task)
        # Prefer git info stored on the run (set for both git workflows and
        # git-source runs).  Fall back to the workflow config for older runs
        # that predate the run-level fields.
        git_repo = run.git_repo
        git_branch = run.git_branch
        if git_repo is None:
            workflow = self.config.get_workflow(run.workflow_name)
            if workflow and workflow.git:
                git_repo = workflow.git.repo
                git_branch = workflow.git.branch
        sh_vars = self._scripthut_env_vars(
            run.workflow_name, run.id, run.created_at,
            git_repo=git_repo, git_branch=git_branch, git_sha=run.commit_hash,
        )
        merged_env = {**sh_vars, **(env_vars or {})}
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

        for item in items_snapshot:
            if item.job_id is None:
                continue

            if item.status in (RunItemStatus.COMPLETED, RunItemStatus.FAILED):
                continue

            job_state = slurm_jobs.get(item.job_id)

            if job_state is None:
                if item.status == RunItemStatus.RUNNING:
                    # We saw it run; gone now means it finished. sacct may
                    # later flip this to FAILED if it was actually a failure.
                    item.status = RunItemStatus.COMPLETED
                    item.finished_at = datetime.now(timezone.utc)
                    changed = True
                    changed_items.append(item)
                    logger.info(f"Task '{item.task.id}' (job {item.job_id}) completed")
                    if item.task.generates_source:
                        await self._handle_generates_source(run, item)
                elif item.status == RunItemStatus.SUBMITTED:
                    # Never seen running and now missing from the queue —
                    # the job most likely never made it past sbatch validation
                    # or was rejected by the scheduler. Mark FAILED so the
                    # user gets a loud signal; the polling layer will revert
                    # this if sacct confirms the job actually ran fine
                    # (e.g. an ultra-fast job that finished between polls).
                    item.started_at = item.submitted_at
                    item.status = RunItemStatus.FAILED
                    item.error = DISAPPEARED_BEFORE_RUNNING_MARKER
                    item.finished_at = datetime.now(timezone.utc)
                    changed = True
                    changed_items.append(item)
                    logger.warning(
                        f"Task '{item.task.id}' (job {item.job_id}) vanished from "
                        f"queue before being observed running — marking FAILED"
                    )
            else:
                if job_state in (JobState.RUNNING, JobState.COMPLETING):
                    if item.status != RunItemStatus.RUNNING:
                        item.status = RunItemStatus.RUNNING
                        item.started_at = item.started_at or datetime.now(timezone.utc)
                        changed = True
                        changed_items.append(item)
                        logger.info(f"Task '{item.task.id}' (job {item.job_id}) started running")
                elif job_state == JobState.PENDING:
                    if item.status != RunItemStatus.SUBMITTED:
                        item.status = RunItemStatus.SUBMITTED
                        changed = True
                        changed_items.append(item)
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
                    logger.info(f"Task '{item.task.id}' (job {item.job_id}) failed: {job_state.value}")

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
