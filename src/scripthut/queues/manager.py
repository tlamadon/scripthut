"""Queue manager for task submission and tracking."""

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
from scripthut.queues.models import (
    Queue,
    QueueItem,
    QueueItemStatus,
    TaskDefinition,
)
from scripthut.ssh.client import SSHClient

if TYPE_CHECKING:
    from scripthut.history import JobHistoryManager

logger = logging.getLogger(__name__)


class QueueManager:
    """Manages task queues - fetching, submitting, and tracking."""

    def __init__(
        self,
        config: ScriptHutConfig,
        clusters: dict[str, SSHClient],
        history_manager: JobHistoryManager | None = None,
    ) -> None:
        """Initialize the queue manager.

        Args:
            config: Application configuration.
            clusters: Dictionary mapping cluster names to SSH clients.
            history_manager: Optional job history manager for persistence.
        """
        self.config = config
        self.clusters = clusters
        self.queues: dict[str, Queue] = {}
        self.history_manager = history_manager
        # SSE event bus: version counter + Event per queue
        self._queue_versions: dict[str, int] = {}
        self._queue_events: dict[str, asyncio.Event] = {}

    @staticmethod
    def _resolve_wildcard_deps(tasks: list[TaskDefinition]) -> None:
        """Expand wildcard patterns in task dependencies to matching task IDs.

        Modifies tasks in-place. A dependency like "build.*" will be replaced
        with all task IDs matching that glob pattern.

        Raises:
            ValueError: If a wildcard pattern matches no tasks.
        """
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
        """Validate task dependencies.

        Checks that all dependency IDs reference existing tasks and that
        there are no circular dependencies.

        Raises:
            ValueError: If validation fails.
        """
        task_ids = {t.id for t in tasks}

        # Check all deps reference existing tasks
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
                    # Found a cycle — extract the cycle from path
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
        self, queue: Queue, item: QueueItem
    ) -> None:
        """Read a generated task source JSON and append tasks to the queue.

        When a task with `generates_source` completes, this method reads
        the JSON file it produced (via `cat` on the head node), parses
        the tasks, and appends them flat into the same queue.

        Args:
            queue: The queue containing the completed item.
            item: The completed queue item with generates_source set.
        """
        path = item.task.generates_source
        if not path:
            return

        logger.info(
            f"Task '{item.task.id}' completed with generates_source: {path}"
        )

        # Get SSH client for this queue's cluster
        ssh_client = self.get_ssh_client(queue.cluster_name)
        if ssh_client is None:
            logger.error(
                f"No SSH client for cluster '{queue.cluster_name}' — "
                f"cannot read generates_source '{path}'"
            )
            return

        # Resolve relative paths against the task's working_dir
        if not path.startswith("/") and not path.startswith("~"):
            path = f"{item.task.working_dir}/{path}"

        # Read the generated JSON file via cat (safe head node command)
        stdout, stderr, exit_code = await ssh_client.run_command(f"cat {path}")
        if exit_code != 0:
            logger.error(
                f"Failed to read generates_source '{path}': {stderr}"
            )
            return

        # Parse JSON
        try:
            data = json.loads(stdout)
        except json.JSONDecodeError as e:
            logger.error(
                f"Invalid JSON in generates_source '{path}': {e}"
            )
            return

        # Extract tasks list
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

        # Resolve wildcard deps against ALL tasks in the queue (existing + new)
        # so that generated tasks can reference parent task IDs with wildcards
        all_tasks = [qi.task for qi in queue.items] + new_tasks
        all_task_ids = [t.id for t in all_tasks]

        # Only resolve wildcards in the new tasks' dependencies
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

        # Validate that all deps reference real task IDs
        for task in new_tasks:
            for dep_id in task.dependencies:
                if dep_id not in all_task_ids:
                    logger.error(
                        f"Task '{task.id}' has unknown dependency '{dep_id}' "
                        f"in generates_source '{path}'"
                    )
                    return

        # Append new items to the queue (flat append)
        new_items = [QueueItem(task=t) for t in new_tasks]
        queue.items.extend(new_items)

        # Register new items in history
        if self.history_manager:
            user = self.config.settings.filter_user or "unknown"
            for new_item in new_items:
                self.history_manager.register_queue_item(
                    new_item, queue.id, queue.cluster_name, user
                )

        logger.info(
            f"Appended {len(new_tasks)} tasks from generates_source "
            f"'{path}' to queue '{queue.id}'"
        )

        # Process queue to submit newly ready tasks
        await self.process_queue(queue)

    def get_task_source(self, name: str) -> TaskSourceConfig | None:
        """Get a task source by name."""
        return self.config.get_task_source(name)

    def get_ssh_client(self, cluster_name: str) -> SSHClient | None:
        """Get SSH client for a cluster."""
        return self.clusters.get(cluster_name)

    async def _get_git_root(self, ssh_client: SSHClient, working_dir: str) -> str:
        """Detect the git repository root for a working directory on the cluster.

        Args:
            ssh_client: SSH client connected to the cluster.
            working_dir: The working directory to check.

        Returns:
            The absolute path to the git repository root.

        Raises:
            ValueError: If the working directory is not inside a git repo.
        """
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
        """Fetch task list from a task source via SSH.

        Args:
            source: Task source configuration.
            return_raw: If True, also return the raw stdout from the command.

        Returns:
            List of TaskDefinition objects, or a tuple of (tasks, raw_output)
            if return_raw is True.

        Raises:
            ValueError: If cluster not found or command fails.
        """
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

        # Handle both {"tasks": [...]} and direct [...] format
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
        """Perform a dry run - fetch tasks and show what would be submitted.

        Args:
            source_name: Name of the task source to use.

        Returns:
            Dictionary with source info and list of tasks with generated scripts.

        Raises:
            ValueError: If source not found or fetch fails.
        """
        source = self.get_task_source(source_name)
        if source is None:
            raise ValueError(f"Task source '{source_name}' not found")

        # Fetch tasks (and raw output for display)
        tasks, raw_output = await self.fetch_tasks(source, return_raw=True)

        # Resolve wildcard deps, then validate
        self._resolve_wildcard_deps(tasks)
        self._validate_dependencies(tasks)

        # Generate a preview queue ID for script generation
        preview_queue_id = "preview"
        log_dir = "~/.cache/scripthut/logs"

        # Resolve ~ to actual home directory (Slurm doesn't expand ~ in directives)
        ssh_client = self.get_ssh_client(source.cluster)
        if ssh_client and log_dir.startswith("~"):
            stdout, _, _ = await ssh_client.run_command("echo $HOME")
            home_dir = stdout.strip()
            log_dir = log_dir.replace("~", home_dir, 1)

        # Get account and login_shell from cluster config
        account = self.get_cluster_account(source.cluster)
        login_shell = self.get_cluster_login_shell(source.cluster)

        # Build task details with generated scripts
        task_details = []
        for task in tasks:
            script = task.to_sbatch_script(preview_queue_id, log_dir, account=account, login_shell=login_shell)
            task_details.append({
                "task": task,
                "sbatch_script": script,
                "output_path": task.get_output_path(preview_queue_id, log_dir),
                "error_path": task.get_error_path(preview_queue_id, log_dir),
            })

        logger.info(f"Dry run for source '{source_name}': {len(tasks)} tasks")

        # Pretty-print the raw JSON for display
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

    async def _build_queue(
        self,
        tasks: list[TaskDefinition],
        source_name: str,
        cluster_name: str,
        max_concurrent: int,
        ssh_client: SSHClient,
    ) -> Queue:
        """Shared queue construction: resolve deps, validate, create Queue.

        Args:
            tasks: Parsed task definitions.
            source_name: Identifier for this queue (e.g. "r-simulation" or
                "scripthut-examples/r_simulation").
            cluster_name: Name of the cluster to submit to.
            max_concurrent: Max concurrent tasks.
            ssh_client: SSH client for the cluster.

        Returns:
            Created Queue object, registered and processing.

        Raises:
            ValueError: If tasks are empty or validation fails.
        """
        if not tasks:
            raise ValueError(f"No tasks for source '{source_name}'")

        # Resolve wildcard deps, then validate
        self._resolve_wildcard_deps(tasks)
        self._validate_dependencies(tasks)

        # Get account and login_shell from cluster config
        account = self.get_cluster_account(cluster_name)
        login_shell = self.get_cluster_login_shell(cluster_name)

        # Detect git root — mandatory, all workflows must live in a git repo
        first_working_dir = tasks[0].working_dir
        git_root = await self._get_git_root(ssh_client, first_working_dir)
        log_dir = f"{git_root}/.scripthut/{source_name}/logs"
        logger.info(f"Git root: {git_root} — logs at {log_dir}")

        # Create queue
        queue_id = str(uuid.uuid4())[:8]
        queue = Queue(
            id=queue_id,
            source_name=source_name,
            cluster_name=cluster_name,
            created_at=datetime.now(),
            items=[QueueItem(task=task) for task in tasks],
            max_concurrent=max_concurrent,
            account=account,
            login_shell=login_shell,
            log_dir=log_dir,
        )

        self.queues[queue_id] = queue
        logger.info(f"Created queue '{queue_id}' with {len(tasks)} tasks")

        # Register queue and items in history
        if self.history_manager:
            self.history_manager.register_queue(queue)
            user = self.config.settings.filter_user or "unknown"
            for item in queue.items:
                self.history_manager.register_queue_item(
                    item, queue.id, queue.cluster_name, user
                )

        # Start submitting tasks
        await self.process_queue(queue)

        return queue

    async def create_queue(self, source_name: str) -> Queue:
        """Create a new queue from a legacy task source.

        Args:
            source_name: Name of the task source to use.

        Returns:
            Created Queue object.

        Raises:
            ValueError: If source not found or fetch fails.
        """
        source = self.get_task_source(source_name)
        if source is None:
            raise ValueError(f"Task source '{source_name}' not found")

        tasks = await self.fetch_tasks(source)

        ssh_client = self.get_ssh_client(source.cluster)
        if ssh_client is None:
            raise ValueError(
                f"No SSH connection to cluster '{source.cluster}'"
            )

        return await self._build_queue(
            tasks, source_name, source.cluster, source.max_concurrent, ssh_client
        )

    async def discover_workflows(self, project_name: str) -> list[str]:
        """Discover sflow.json files in a project repo via git ls-files.

        Args:
            project_name: Name of the project config.

        Returns:
            List of relative paths, e.g. ["r_simulation/sflow.json"].

        Raises:
            ValueError: If project not found or git command fails.
        """
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

    async def create_queue_from_project(
        self, project_name: str, workflow_path: str
    ) -> Queue:
        """Create a queue from a sflow.json in a project repo.

        Args:
            project_name: Name of the project config.
            workflow_path: Relative path within the project,
                e.g. "r_simulation/sflow.json".

        Returns:
            Created Queue object.

        Raises:
            ValueError: If project not found or sflow.json can't be read.
        """
        project = self.config.get_project(project_name)
        if project is None:
            raise ValueError(f"Project '{project_name}' not found")

        ssh_client = self.get_ssh_client(project.cluster)
        if ssh_client is None:
            raise ValueError(
                f"No SSH connection to cluster '{project.cluster}'"
            )

        # Read sflow.json
        full_path = f"{project.path}/{workflow_path}"
        stdout, stderr, exit_code = await ssh_client.run_command(
            f"cat {full_path}"
        )
        if exit_code != 0:
            raise ValueError(
                f"Failed to read '{full_path}': {stderr}"
            )

        # Parse tasks
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

        # Set working_dir to the directory containing sflow.json
        # (for discovered workflows; generated ones specify their own)
        sflow_dir = workflow_path.rsplit("/", 1)[0] if "/" in workflow_path else ""
        default_working_dir = (
            f"{project.path}/{sflow_dir}" if sflow_dir else project.path
        )
        for task in tasks:
            if task.working_dir == "~":  # default, not explicitly set
                task.working_dir = default_working_dir

        # source_name = "project_name/workflow_dir"
        source_name = (
            f"{project.name}/{sflow_dir}" if sflow_dir else project.name
        )

        return await self._build_queue(
            tasks, source_name, project.cluster, project.max_concurrent,
            ssh_client
        )

    async def submit_task(self, queue: Queue, item: QueueItem) -> bool:
        """Submit a single task to Slurm.

        Args:
            queue: The queue containing the item.
            item: The queue item to submit.

        Returns:
            True if submission succeeded, False otherwise.
        """
        ssh_client = self.get_ssh_client(queue.cluster_name)
        if ssh_client is None:
            item.status = QueueItemStatus.FAILED
            item.error = f"Cluster '{queue.cluster_name}' not connected"
            item.finished_at = datetime.now()
            # Update job history with failure
            if self.history_manager:
                self.history_manager.update_from_queue_item(item, queue.id)
            return False

        # Resolve ~ to actual home directory (Slurm doesn't expand ~ in directives)
        log_dir = queue.log_dir
        if log_dir.startswith("~"):
            stdout, _, _ = await ssh_client.run_command("echo $HOME")
            home_dir = stdout.strip()
            log_dir = log_dir.replace("~", home_dir, 1)

        # Ensure log directory exists (home dir is shared across nodes)
        await ssh_client.run_command(f"mkdir -p {log_dir}")

        # Generate sbatch script and store it
        script = item.task.to_sbatch_script(queue.id, log_dir, account=queue.account, login_shell=queue.login_shell)
        item.sbatch_script = script

        # Submit via sbatch using heredoc
        # Escape any single quotes in the script
        escaped_script = script.replace("'", "'\\''")
        submit_cmd = f"sbatch <<'SCRIPTHUT_EOF'\n{escaped_script}\nSCRIPTHUT_EOF"

        stdout, stderr, exit_code = await ssh_client.run_command(submit_cmd)

        if exit_code != 0:
            item.status = QueueItemStatus.FAILED
            item.error = f"sbatch failed: {stderr}"
            item.finished_at = datetime.now()
            logger.error(f"Failed to submit task '{item.task.id}': {stderr}")
            return False

        # Parse job ID from sbatch output: "Submitted batch job 12345"
        try:
            job_id = stdout.strip().split()[-1]
            item.slurm_job_id = job_id
            item.status = QueueItemStatus.SUBMITTED
            item.submitted_at = datetime.now()
            logger.info(f"Submitted task '{item.task.id}' as Slurm job {job_id}")

            # Update job history with submission info
            if self.history_manager:
                self.history_manager.update_from_queue_item(item, queue.id)

            return True
        except (IndexError, ValueError) as e:
            item.status = QueueItemStatus.FAILED
            item.error = f"Could not parse job ID: {stdout}"
            item.finished_at = datetime.now()
            logger.error(f"Could not parse job ID from: {stdout}")

            # Update job history with failure
            if self.history_manager:
                self.history_manager.update_from_queue_item(item, queue.id)

            return False

    async def process_queue(self, queue: Queue) -> None:
        """Process a queue - submit tasks up to max_concurrent.

        Respects task dependencies: only submits tasks whose dependencies
        have all completed. Cascades failure to tasks whose dependencies
        have failed.

        Args:
            queue: The queue to process.
        """
        # Cascade failures: mark pending items with failed deps as DEP_FAILED.
        # Loop until no more items are marked, to handle transitive deps.
        changed = True
        while changed:
            changed = False
            for item in queue.items:
                if item.status != QueueItemStatus.PENDING:
                    continue
                failed_deps = queue.get_failed_deps(item)
                if failed_deps:
                    item.status = QueueItemStatus.DEP_FAILED
                    item.error = f"Dependency '{failed_deps[0]}' failed"
                    item.finished_at = datetime.now()
                    if self.history_manager:
                        self.history_manager.update_from_queue_item(item, queue.id)
                    changed = True

        # Count currently running/submitted
        active_count = queue.running_count

        # Find pending items whose dependencies are all satisfied
        ready_items = [
            item for item in queue.items
            if item.status == QueueItemStatus.PENDING
            and queue.are_deps_satisfied(item)
        ]

        # Submit up to max_concurrent
        slots_available = queue.max_concurrent - active_count
        to_submit = ready_items[:slots_available]

        for item in to_submit:
            await self.submit_task(queue, item)

        if to_submit:
            self.notify_queue(queue.id)

    def notify_queue(self, queue_id: str) -> None:
        """Wake all SSE listeners for a queue."""
        self._queue_versions[queue_id] = self._queue_versions.get(queue_id, 0) + 1
        old_event = self._queue_events.get(queue_id)
        self._queue_events[queue_id] = asyncio.Event()  # Fresh event for next cycle
        if old_event:
            old_event.set()  # Wake anyone waiting on the old event

    async def wait_for_update(self, queue_id: str, timeout: float = 30.0) -> bool:
        """Wait for a queue state change. Returns True if notified, False on timeout."""
        if queue_id not in self._queue_events:
            self._queue_events[queue_id] = asyncio.Event()
        event = self._queue_events[queue_id]
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def update_queue_status(self, queue: Queue, slurm_jobs: dict[str, JobState]) -> None:
        """Update queue item statuses based on Slurm job states.

        Args:
            queue: The queue to update.
            slurm_jobs: Dictionary mapping Slurm job IDs to their states.
        """
        changed = False
        changed_items: list[QueueItem] = []

        for item in queue.items:
            if item.slurm_job_id is None:
                continue

            if item.status in (QueueItemStatus.COMPLETED, QueueItemStatus.FAILED):
                continue  # Already final

            job_state = slurm_jobs.get(item.slurm_job_id)

            if job_state is None:
                # Job not in squeue - check if it was running before
                if item.status in (QueueItemStatus.SUBMITTED, QueueItemStatus.RUNNING):
                    # Job finished (completed or failed - we assume completed if no error)
                    item.status = QueueItemStatus.COMPLETED
                    item.finished_at = datetime.now()
                    changed = True
                    changed_items.append(item)
                    logger.info(f"Task '{item.task.id}' (job {item.slurm_job_id}) completed")
                    # Handle endogenous source generation
                    if item.task.generates_source:
                        await self._handle_generates_source(queue, item)
            else:
                # Job is in squeue
                if job_state in (JobState.RUNNING, JobState.COMPLETING):
                    if item.status != QueueItemStatus.RUNNING:
                        item.status = QueueItemStatus.RUNNING
                        item.started_at = item.started_at or datetime.now()
                        changed = True
                        changed_items.append(item)
                        logger.info(f"Task '{item.task.id}' (job {item.slurm_job_id}) started running")
                elif job_state == JobState.PENDING:
                    if item.status != QueueItemStatus.SUBMITTED:
                        item.status = QueueItemStatus.SUBMITTED
                        changed = True
                        changed_items.append(item)
                elif job_state == JobState.COMPLETED:
                    # Job completed and still visible in squeue
                    item.status = QueueItemStatus.COMPLETED
                    item.finished_at = datetime.now()
                    changed = True
                    changed_items.append(item)
                    logger.info(f"Task '{item.task.id}' (job {item.slurm_job_id}) completed")
                    # Handle endogenous source generation
                    if item.task.generates_source:
                        await self._handle_generates_source(queue, item)
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
                    item.status = QueueItemStatus.FAILED
                    item.error = f"Slurm job {job_state.value}"
                    item.finished_at = datetime.now()
                    changed = True
                    changed_items.append(item)
                    logger.info(f"Task '{item.task.id}' (job {item.slurm_job_id}) failed: {job_state.value}")

        # Update job history for changed items
        if self.history_manager and changed_items:
            for item in changed_items:
                self.history_manager.update_from_queue_item(item, queue.id)

        # If any items completed/failed, try to submit more
        if changed:
            await self.process_queue(queue)
            self.notify_queue(queue.id)

    async def update_all_queues(self, cluster_jobs: dict[str, list[tuple[str, JobState]]]) -> None:
        """Update all active queues based on Slurm job states.

        Args:
            cluster_jobs: Dictionary mapping cluster names to list of (job_id, state) tuples.
        """
        for queue in self.queues.values():
            if queue.status in (queue.status.COMPLETED, queue.status.FAILED, queue.status.CANCELLED):
                continue  # Skip finished queues

            # Get jobs for this queue's cluster
            jobs = cluster_jobs.get(queue.cluster_name, [])
            job_states = {job_id: state for job_id, state in jobs}

            await self.update_queue_status(queue, job_states)

    async def cancel_queue(self, queue_id: str) -> bool:
        """Cancel all pending and running items in a queue.

        Args:
            queue_id: ID of the queue to cancel.

        Returns:
            True if cancellation was initiated, False if queue not found.
        """
        queue = self.queues.get(queue_id)
        if queue is None:
            return False

        ssh_client = self.get_ssh_client(queue.cluster_name)
        cancelled_items: list[QueueItem] = []

        for item in queue.items:
            if item.status == QueueItemStatus.PENDING:
                item.status = QueueItemStatus.FAILED
                item.error = "Cancelled"
                item.finished_at = datetime.now()
                cancelled_items.append(item)
            elif item.status in (QueueItemStatus.SUBMITTED, QueueItemStatus.RUNNING):
                if item.slurm_job_id and ssh_client:
                    # Cancel the Slurm job
                    await ssh_client.run_command(f"scancel {item.slurm_job_id}")
                item.status = QueueItemStatus.FAILED
                item.error = "Cancelled"
                item.finished_at = datetime.now()
                cancelled_items.append(item)

        # Update job history for cancelled items
        if self.history_manager and cancelled_items:
            for item in cancelled_items:
                self.history_manager.update_from_queue_item(item, queue.id)

        logger.info(f"Cancelled queue '{queue_id}'")
        return True

    def get_queue(self, queue_id: str) -> Queue | None:
        """Get a queue by ID."""
        return self.queues.get(queue_id)

    def get_all_queues(self) -> list[Queue]:
        """Get all queues, sorted by creation time (newest first)."""
        return sorted(self.queues.values(), key=lambda q: q.created_at, reverse=True)

    def get_active_queues(self) -> list[Queue]:
        """Get all queues that are still running."""
        return [
            q for q in self.queues.values()
            if q.status in (q.status.PENDING, q.status.RUNNING)
        ]

    def restore_from_history(self) -> int:
        """Restore queues from history manager.

        Called on startup to restore previously created queues.

        Returns:
            Number of queues restored.
        """
        if self.history_manager is None:
            return 0

        restored_queues = self.history_manager.reconstruct_all_queues()
        for queue_id, queue in restored_queues.items():
            if queue_id not in self.queues:
                self.queues[queue_id] = queue

        logger.info(f"Restored {len(restored_queues)} queues from history")
        return len(restored_queues)

    async def fetch_log_file(
        self,
        queue_id: str,
        task_id: str,
        log_type: str = "output",
        tail_lines: int | None = None,
    ) -> tuple[str | None, str | None]:
        """Fetch a log file from the remote cluster.

        Args:
            queue_id: ID of the queue.
            task_id: ID of the task.
            log_type: "output" for stdout, "error" for stderr.
            tail_lines: If provided, only fetch the last N lines.

        Returns:
            Tuple of (content, error_message). Content is None if error occurred.
        """
        queue = self.queues.get(queue_id)
        if queue is None:
            return None, f"Queue '{queue_id}' not found"

        item = queue.get_item_by_task_id(task_id)
        if item is None:
            return None, f"Task '{task_id}' not found in queue"

        ssh_client = self.get_ssh_client(queue.cluster_name)
        if ssh_client is None:
            return None, f"Cluster '{queue.cluster_name}' not connected"

        # Resolve ~ to actual home directory
        log_dir = queue.log_dir
        if log_dir.startswith("~"):
            stdout, _, _ = await ssh_client.run_command("echo $HOME")
            home_dir = stdout.strip()
            log_dir = log_dir.replace("~", home_dir, 1)

        # Get log file path
        if log_type == "output":
            log_path = item.task.get_output_path(queue.id, log_dir)
        elif log_type == "error":
            log_path = item.task.get_error_path(queue.id, log_dir)
        else:
            return None, f"Invalid log_type: {log_type}"

        # Fetch file content
        if tail_lines:
            cmd = f"tail -n {tail_lines} {log_path} 2>/dev/null || echo '[File not found or empty]'"
        else:
            cmd = f"cat {log_path} 2>/dev/null || echo '[File not found or empty]'"

        stdout, stderr, exit_code = await ssh_client.run_command(cmd)

        return stdout, None
