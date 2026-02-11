"""Run manager for task submission and tracking."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime
from fnmatch import fnmatch
from typing import TYPE_CHECKING

from scripthut.config_schema import (
    ProjectConfig,
    ScriptHutConfig,
    SlurmClusterConfig,
    TaskSourceConfig,
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


class RunManager:
    """Manages runs - fetching, submitting, and tracking."""

    def __init__(
        self,
        config: ScriptHutConfig,
        clusters: dict[str, SSHClient],
        storage: RunStorageManager | None = None,
    ) -> None:
        self.config = config
        self.clusters = clusters
        self.runs: dict[str, Run] = {}
        self.storage = storage
        # SSE event bus: version counter + Event per run
        self._run_versions: dict[str, int] = {}
        self._run_events: dict[str, asyncio.Event] = {}

    def _resolve_environment(
        self, task: TaskDefinition
    ) -> tuple[dict[str, str] | None, str]:
        """Resolve environment config for a task."""
        if not task.environment:
            return None, ""
        env_config = self.config.get_environment(task.environment)
        if env_config is None:
            logger.warning(
                f"Task '{task.id}' references unknown environment '{task.environment}'"
            )
            return None, ""
        return dict(env_config.variables) or None, env_config.extra_init

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
        """Read a generated task source JSON and append tasks to the run."""
        path = item.task.generates_source
        if not path:
            return

        logger.info(
            f"Task '{item.task.id}' completed with generates_source: {path}"
        )

        ssh_client = self.get_ssh_client(run.cluster_name)
        if ssh_client is None:
            logger.error(
                f"No SSH client for cluster '{run.cluster_name}' — "
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

    def get_task_source(self, name: str) -> TaskSourceConfig | None:
        """Get a task source by name."""
        return self.config.get_task_source(name)

    def get_ssh_client(self, cluster_name: str) -> SSHClient | None:
        """Get SSH client for a cluster."""
        return self.clusters.get(cluster_name)

    async def _get_git_root(self, ssh_client: SSHClient, working_dir: str) -> str:
        """Detect the git repository root for a working directory on the cluster."""
        stdout, stderr, exit_code = await ssh_client.run_command(
            f"cd {working_dir} && git rev-parse --show-toplevel"
        )
        if exit_code != 0:
            raise ValueError(
                f"'{working_dir}' is not inside a git repository. "
                f"ScriptHut requires workflows to live in git repos."
            )
        return stdout.strip()

    async def fetch_tasks(
        self, source: TaskSourceConfig, *, return_raw: bool = False
    ) -> list[TaskDefinition] | tuple[list[TaskDefinition], str]:
        """Fetch task list from a task source via SSH."""
        ssh_client = self.get_ssh_client(source.cluster)
        if ssh_client is None:
            raise ValueError(f"Cluster '{source.cluster}' not found or not connected")

        logger.info(f"Fetching tasks from source '{source.name}' on cluster '{source.cluster}'")

        stdout, stderr, exit_code = await ssh_client.run_command(source.command)

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
        logger.info(f"Fetched {len(tasks)} tasks from source '{source.name}'")

        if return_raw:
            return tasks, stdout
        return tasks

    async def dry_run(self, source_name: str) -> dict:
        """Perform a dry run - fetch tasks and show what would be submitted."""
        source = self.get_task_source(source_name)
        if source is None:
            raise ValueError(f"Task source '{source_name}' not found")

        tasks, raw_output = await self.fetch_tasks(source, return_raw=True)

        self._resolve_wildcard_deps(tasks)
        self._validate_dependencies(tasks)

        preview_run_id = "preview"
        log_dir = "~/.cache/scripthut/logs"

        ssh_client = self.get_ssh_client(source.cluster)
        if ssh_client and log_dir.startswith("~"):
            stdout, _, _ = await ssh_client.run_command("echo $HOME")
            home_dir = stdout.strip()
            log_dir = log_dir.replace("~", home_dir, 1)

        account = self.get_cluster_account(source.cluster)
        login_shell = self.get_cluster_login_shell(source.cluster)

        task_details = []
        for task in tasks:
            env_vars, extra_init = self._resolve_environment(task)
            script = task.to_sbatch_script(preview_run_id, log_dir, account=account, login_shell=login_shell, env_vars=env_vars, extra_init=extra_init)
            task_details.append({
                "task": task,
                "sbatch_script": script,
                "output_path": task.get_output_path(preview_run_id, log_dir),
                "error_path": task.get_error_path(preview_run_id, log_dir),
            })

        logger.info(f"Dry run for source '{source_name}': {len(tasks)} tasks")

        try:
            raw_output_formatted = json.dumps(json.loads(raw_output), indent=2)
        except (json.JSONDecodeError, TypeError):
            raw_output_formatted = raw_output

        return {
            "source": source,
            "cluster_name": source.cluster,
            "task_count": len(tasks),
            "max_concurrent": source.max_concurrent,
            "account": account,
            "tasks": task_details,
            "raw_output": raw_output_formatted,
        }

    def get_cluster_account(self, cluster_name: str) -> str | None:
        """Get the Slurm account for a cluster."""
        cluster = self.config.get_cluster(cluster_name)
        if cluster and isinstance(cluster, SlurmClusterConfig):
            return cluster.account
        return None

    def get_cluster_login_shell(self, cluster_name: str) -> bool:
        """Get whether the cluster uses login shell in sbatch scripts."""
        cluster = self.config.get_cluster(cluster_name)
        if cluster and isinstance(cluster, SlurmClusterConfig):
            return cluster.login_shell
        return False

    async def _build_run(
        self,
        tasks: list[TaskDefinition],
        workflow_name: str,
        cluster_name: str,
        max_concurrent: int,
        ssh_client: SSHClient,
    ) -> Run:
        """Build a Run: resolve deps, validate, create, persist, and start processing."""
        if not tasks:
            raise ValueError(f"No tasks for workflow '{workflow_name}'")

        self._resolve_wildcard_deps(tasks)
        self._validate_dependencies(tasks)

        account = self.get_cluster_account(cluster_name)
        login_shell = self.get_cluster_login_shell(cluster_name)

        first_working_dir = tasks[0].working_dir
        try:
            git_root = await self._get_git_root(ssh_client, first_working_dir)
            log_dir = f"{git_root}/.scripthut/{workflow_name}/logs"
            logger.info(f"Git root: {git_root} — logs at {log_dir}")
        except ValueError:
            log_dir = f"~/.cache/scripthut/logs/{workflow_name}"
            logger.info(f"No git root for '{first_working_dir}' — logs at {log_dir}")

        run_id = str(uuid.uuid4())[:8]
        run = Run(
            id=run_id,
            workflow_name=workflow_name,
            cluster_name=cluster_name,
            created_at=datetime.now(),
            items=[RunItem(task=task) for task in tasks],
            max_concurrent=max_concurrent,
            account=account,
            login_shell=login_shell,
            log_dir=log_dir,
        )

        self.runs[run_id] = run
        logger.info(f"Created run '{run_id}' with {len(tasks)} tasks")

        # Persist to storage
        self._persist_run(run)

        # Start submitting tasks
        await self.process_run(run)

        return run

    async def create_run(self, source_name: str) -> Run:
        """Create a new run from a legacy task source."""
        source = self.get_task_source(source_name)
        if source is None:
            raise ValueError(f"Task source '{source_name}' not found")

        tasks = await self.fetch_tasks(source)

        ssh_client = self.get_ssh_client(source.cluster)
        if ssh_client is None:
            raise ValueError(
                f"No SSH connection to cluster '{source.cluster}'"
            )

        return await self._build_run(
            tasks, source_name, source.cluster, source.max_concurrent, ssh_client
        )

    async def discover_workflows(self, project_name: str) -> list[str]:
        """Discover sflow.json files in a project repo via git ls-files."""
        project = self.config.get_project(project_name)
        if project is None:
            raise ValueError(f"Project '{project_name}' not found")

        ssh_client = self.get_ssh_client(project.cluster)
        if ssh_client is None:
            raise ValueError(
                f"No SSH connection to cluster '{project.cluster}'"
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

        ssh_client = self.get_ssh_client(project.cluster)
        if ssh_client is None:
            raise ValueError(
                f"No SSH connection to cluster '{project.cluster}'"
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
            tasks, workflow_name, project.cluster, project.max_concurrent,
            ssh_client
        )

    async def submit_task(self, run: Run, item: RunItem) -> bool:
        """Submit a single task to Slurm."""
        ssh_client = self.get_ssh_client(run.cluster_name)
        if ssh_client is None:
            item.status = RunItemStatus.FAILED
            item.error = f"Cluster '{run.cluster_name}' not connected"
            item.finished_at = datetime.now()
            self._persist_run(run)
            return False

        # Resolve ~ to actual home directory
        log_dir = run.log_dir
        if log_dir.startswith("~"):
            stdout, _, _ = await ssh_client.run_command("echo $HOME")
            home_dir = stdout.strip()
            log_dir = log_dir.replace("~", home_dir, 1)

        await ssh_client.run_command(f"mkdir -p {log_dir}")

        env_vars, extra_init = self._resolve_environment(item.task)
        script = item.task.to_sbatch_script(run.id, log_dir, account=run.account, login_shell=run.login_shell, env_vars=env_vars, extra_init=extra_init)
        item.sbatch_script = script

        escaped_script = script.replace("'", "'\\''")
        submit_cmd = f"sbatch <<'SCRIPTHUT_EOF'\n{escaped_script}\nSCRIPTHUT_EOF"

        stdout, stderr, exit_code = await ssh_client.run_command(submit_cmd)

        if exit_code != 0:
            item.status = RunItemStatus.FAILED
            item.error = f"sbatch failed: {stderr}"
            item.finished_at = datetime.now()
            logger.error(f"Failed to submit task '{item.task.id}': {stderr}")
            self._persist_run(run)
            return False

        try:
            job_id = stdout.strip().split()[-1]
            item.slurm_job_id = job_id
            item.status = RunItemStatus.SUBMITTED
            item.submitted_at = datetime.now()
            logger.info(f"Submitted task '{item.task.id}' as Slurm job {job_id}")
            self._persist_run(run)
            return True
        except (IndexError, ValueError):
            item.status = RunItemStatus.FAILED
            item.error = f"Could not parse job ID: {stdout}"
            item.finished_at = datetime.now()
            logger.error(f"Could not parse job ID from: {stdout}")
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
                    item.finished_at = datetime.now()
                    self._persist_run(run)
                    changed = True

        active_count = run.running_count

        ready_items = [
            item for item in run.items
            if item.status == RunItemStatus.PENDING
            and run.are_deps_satisfied(item)
        ]

        slots_available = run.max_concurrent - active_count
        to_submit = ready_items[:slots_available]

        for item in to_submit:
            await self.submit_task(run, item)

        if to_submit:
            self.notify_run(run.id)

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

        for item in run.items:
            if item.slurm_job_id is None:
                continue

            if item.status in (RunItemStatus.COMPLETED, RunItemStatus.FAILED):
                continue

            job_state = slurm_jobs.get(item.slurm_job_id)

            if job_state is None:
                if item.status in (RunItemStatus.SUBMITTED, RunItemStatus.RUNNING):
                    item.started_at = item.started_at or item.submitted_at
                    item.status = RunItemStatus.COMPLETED
                    item.finished_at = datetime.now()
                    changed = True
                    changed_items.append(item)
                    logger.info(f"Task '{item.task.id}' (job {item.slurm_job_id}) completed")
                    if item.task.generates_source:
                        await self._handle_generates_source(run, item)
            else:
                if job_state in (JobState.RUNNING, JobState.COMPLETING):
                    if item.status != RunItemStatus.RUNNING:
                        item.status = RunItemStatus.RUNNING
                        item.started_at = item.started_at or datetime.now()
                        changed = True
                        changed_items.append(item)
                        logger.info(f"Task '{item.task.id}' (job {item.slurm_job_id}) started running")
                elif job_state == JobState.PENDING:
                    if item.status != RunItemStatus.SUBMITTED:
                        item.status = RunItemStatus.SUBMITTED
                        changed = True
                        changed_items.append(item)
                elif job_state == JobState.COMPLETED:
                    item.started_at = item.started_at or item.submitted_at
                    item.status = RunItemStatus.COMPLETED
                    item.finished_at = datetime.now()
                    changed = True
                    changed_items.append(item)
                    logger.info(f"Task '{item.task.id}' (job {item.slurm_job_id}) completed")
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
                    item.finished_at = datetime.now()
                    changed = True
                    changed_items.append(item)
                    logger.info(f"Task '{item.task.id}' (job {item.slurm_job_id}) failed: {job_state.value}")

        if changed:
            self._persist_run(run)
            await self.process_run(run)
            self.notify_run(run.id)

    async def update_all_runs(self, cluster_jobs: dict[str, list[tuple[str, JobState]]]) -> None:
        """Update all active runs based on Slurm job states."""
        for run in self.runs.values():
            if run.status in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED):
                continue

            jobs = cluster_jobs.get(run.cluster_name, [])
            job_states = {job_id: state for job_id, state in jobs}

            await self.update_run_status(run, job_states)

    async def cancel_run(self, run_id: str) -> bool:
        """Cancel all pending and running items in a run."""
        run = self.runs.get(run_id)
        if run is None:
            return False

        ssh_client = self.get_ssh_client(run.cluster_name)

        for item in run.items:
            if item.status == RunItemStatus.PENDING:
                item.status = RunItemStatus.FAILED
                item.error = "Cancelled"
                item.finished_at = datetime.now()
            elif item.status in (RunItemStatus.SUBMITTED, RunItemStatus.RUNNING):
                if item.slurm_job_id and ssh_client:
                    await ssh_client.run_command(f"scancel {item.slurm_job_id}")
                item.started_at = item.started_at or item.submitted_at
                item.status = RunItemStatus.FAILED
                item.error = "Cancelled"
                item.finished_at = datetime.now()

        self._persist_run(run)
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
        """Fetch a log file from the remote cluster."""
        run = self.runs.get(run_id)
        if run is None:
            return None, f"Run '{run_id}' not found"

        item = run.get_item_by_task_id(task_id)
        if item is None:
            return None, f"Task '{task_id}' not found in run"

        ssh_client = self.get_ssh_client(run.cluster_name)
        if ssh_client is None:
            return None, f"Cluster '{run.cluster_name}' not connected"

        log_dir = run.log_dir
        if log_dir.startswith("~"):
            stdout, _, _ = await ssh_client.run_command("echo $HOME")
            home_dir = stdout.strip()
            log_dir = log_dir.replace("~", home_dir, 1)

        if log_type == "output":
            log_path = item.task.get_output_path(run.id, log_dir)
        elif log_type == "error":
            log_path = item.task.get_error_path(run.id, log_dir)
        else:
            return None, f"Invalid log_type: {log_type}"

        if tail_lines:
            cmd = f"tail -n {tail_lines} {log_path} 2>/dev/null || echo '[File not found or empty]'"
        else:
            cmd = f"cat {log_path} 2>/dev/null || echo '[File not found or empty]'"

        stdout, stderr, exit_code = await ssh_client.run_command(cmd)

        return stdout, None
