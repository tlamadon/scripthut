"""Queue manager for task submission and tracking."""

import json
import logging
import uuid
from datetime import datetime

from scriptrun.config_schema import ScriptRunConfig, TaskSourceConfig
from scriptrun.models import JobState
from scriptrun.queues.models import (
    Queue,
    QueueItem,
    QueueItemStatus,
    TaskDefinition,
)
from scriptrun.ssh.client import SSHClient

logger = logging.getLogger(__name__)


class QueueManager:
    """Manages task queues - fetching, submitting, and tracking."""

    def __init__(
        self,
        config: ScriptRunConfig,
        clusters: dict[str, SSHClient],
    ) -> None:
        """Initialize the queue manager.

        Args:
            config: Application configuration.
            clusters: Dictionary mapping cluster names to SSH clients.
        """
        self.config = config
        self.clusters = clusters
        self.queues: dict[str, Queue] = {}

    def get_task_source(self, name: str) -> TaskSourceConfig | None:
        """Get a task source by name."""
        return self.config.get_task_source(name)

    def get_ssh_client(self, cluster_name: str) -> SSHClient | None:
        """Get SSH client for a cluster."""
        return self.clusters.get(cluster_name)

    async def fetch_tasks(self, source: TaskSourceConfig) -> list[TaskDefinition]:
        """Fetch task list from a task source via SSH.

        Args:
            source: Task source configuration.

        Returns:
            List of TaskDefinition objects.

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

        return tasks

    async def create_queue(self, source_name: str) -> Queue:
        """Create a new queue from a task source.

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

        # Fetch tasks
        tasks = await self.fetch_tasks(source)

        if not tasks:
            raise ValueError(f"No tasks returned from source '{source_name}'")

        # Create queue
        queue_id = str(uuid.uuid4())[:8]
        queue = Queue(
            id=queue_id,
            source_name=source_name,
            cluster_name=source.cluster,
            created_at=datetime.now(),
            items=[QueueItem(task=task) for task in tasks],
            max_concurrent=source.max_concurrent,
        )

        self.queues[queue_id] = queue
        logger.info(f"Created queue '{queue_id}' with {len(tasks)} tasks")

        # Start submitting tasks
        await self.process_queue(queue)

        return queue

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
            return False

        # Create log directory on remote
        log_dir = queue.log_dir
        await ssh_client.run_command(f"mkdir -p {log_dir}/{queue.id}")

        # Generate sbatch script and store it
        script = item.task.to_sbatch_script(queue.id, log_dir)
        item.sbatch_script = script

        # Submit via sbatch using heredoc
        # Escape any single quotes in the script
        escaped_script = script.replace("'", "'\\''")
        submit_cmd = f"sbatch <<'SCRIPTRUN_EOF'\n{escaped_script}\nSCRIPTRUN_EOF"

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
            return True
        except (IndexError, ValueError) as e:
            item.status = QueueItemStatus.FAILED
            item.error = f"Could not parse job ID: {stdout}"
            item.finished_at = datetime.now()
            logger.error(f"Could not parse job ID from: {stdout}")
            return False

    async def process_queue(self, queue: Queue) -> None:
        """Process a queue - submit tasks up to max_concurrent.

        Args:
            queue: The queue to process.
        """
        # Count currently running/submitted
        active_count = queue.running_count

        # Find pending items to submit
        pending_items = [
            item for item in queue.items
            if item.status == QueueItemStatus.PENDING
        ]

        # Submit up to max_concurrent
        slots_available = queue.max_concurrent - active_count
        to_submit = pending_items[:slots_available]

        for item in to_submit:
            await self.submit_task(queue, item)

    async def update_queue_status(self, queue: Queue, slurm_jobs: dict[str, JobState]) -> None:
        """Update queue item statuses based on Slurm job states.

        Args:
            queue: The queue to update.
            slurm_jobs: Dictionary mapping Slurm job IDs to their states.
        """
        changed = False

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
                    logger.info(f"Task '{item.task.id}' (job {item.slurm_job_id}) completed")
            else:
                # Job is in squeue
                if job_state == JobState.RUNNING:
                    if item.status != QueueItemStatus.RUNNING:
                        item.status = QueueItemStatus.RUNNING
                        item.started_at = datetime.now()
                        changed = True
                        logger.info(f"Task '{item.task.id}' (job {item.slurm_job_id}) started running")
                elif job_state == JobState.PENDING:
                    if item.status != QueueItemStatus.SUBMITTED:
                        item.status = QueueItemStatus.SUBMITTED
                        changed = True
                elif job_state in (JobState.FAILED, JobState.CANCELLED, JobState.TIMEOUT, JobState.NODE_FAIL):
                    item.status = QueueItemStatus.FAILED
                    item.error = f"Slurm job {job_state.value}"
                    item.finished_at = datetime.now()
                    changed = True
                    logger.info(f"Task '{item.task.id}' (job {item.slurm_job_id}) failed: {job_state.value}")

        # If any items completed/failed, try to submit more
        if changed:
            await self.process_queue(queue)

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

        for item in queue.items:
            if item.status == QueueItemStatus.PENDING:
                item.status = QueueItemStatus.FAILED
                item.error = "Cancelled"
                item.finished_at = datetime.now()
            elif item.status in (QueueItemStatus.SUBMITTED, QueueItemStatus.RUNNING):
                if item.slurm_job_id and ssh_client:
                    # Cancel the Slurm job
                    await ssh_client.run_command(f"scancel {item.slurm_job_id}")
                item.status = QueueItemStatus.FAILED
                item.error = "Cancelled"
                item.finished_at = datetime.now()

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

        # Get log file path
        if log_type == "output":
            log_path = item.task.get_output_path(queue.id, queue.log_dir)
        elif log_type == "error":
            log_path = item.task.get_error_path(queue.id, queue.log_dir)
        else:
            return None, f"Invalid log_type: {log_type}"

        # Fetch file content
        if tail_lines:
            cmd = f"tail -n {tail_lines} {log_path} 2>/dev/null || echo '[File not found or empty]'"
        else:
            cmd = f"cat {log_path} 2>/dev/null || echo '[File not found or empty]'"

        stdout, stderr, exit_code = await ssh_client.run_command(cmd)

        return stdout, None
