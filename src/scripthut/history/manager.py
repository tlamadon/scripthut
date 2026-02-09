"""Job history management with JSON persistence."""

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import IO

from scripthut.history.models import QueueMetadata, UnifiedJob, UnifiedJobSource, UnifiedJobState
from scripthut.models import JobState, SlurmJob
from scripthut.queues.models import Queue, QueueItem, QueueItemStatus, TaskDefinition

logger = logging.getLogger(__name__)


class JobHistoryManager:
    """Manages job history with JSON file persistence."""

    RETENTION_DAYS = 7

    def __init__(self, history_path: Path | None = None):
        """Initialize the history manager.

        Args:
            history_path: Path to history file. Defaults to ~/.cache/scripthut/job_history.json
        """
        if history_path is None:
            history_path = Path.home() / ".cache" / "scripthut" / "job_history.json"

        self.history_path = history_path
        self.history_path.parent.mkdir(parents=True, exist_ok=True)

        # In-memory cache
        self._jobs: dict[str, UnifiedJob] = {}
        self._queues: dict[str, QueueMetadata] = {}
        self._dirty: bool = False

        # Load existing history
        self._load()

    @staticmethod
    def _lock_file(f: IO, exclusive: bool = False) -> None:
        """Acquire a file lock (cross-platform)."""
        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK if exclusive else msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(f.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)

    @staticmethod
    def _unlock_file(f: IO) -> None:
        """Release a file lock (cross-platform)."""
        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def _load(self) -> None:
        """Load history from JSON file."""
        if not self.history_path.exists():
            logger.info(f"No history file found at {self.history_path}")
            return

        try:
            with open(self.history_path, "r") as f:
                self._lock_file(f, exclusive=False)
                try:
                    data = json.load(f)
                    for job_data in data.get("jobs", []):
                        job = UnifiedJob.from_dict(job_data)
                        self._jobs[job.id] = job
                    for queue_data in data.get("queues", []):
                        queue = QueueMetadata.from_dict(queue_data)
                        self._queues[queue.id] = queue
                    logger.info(
                        f"Loaded {len(self._jobs)} jobs and {len(self._queues)} queues from history"
                    )
                finally:
                    self._unlock_file(f)
        except Exception as e:
            logger.error(f"Failed to load history: {e}")

    def _save(self) -> None:
        """Save history to JSON file."""
        if not self._dirty:
            return

        try:
            # Write to temp file then rename for atomicity
            temp_path = self.history_path.with_suffix(".tmp")
            data = {
                "version": 1,
                "saved_at": datetime.now().isoformat(),
                "jobs": [job.to_dict() for job in self._jobs.values()],
                "queues": [queue.to_dict() for queue in self._queues.values()],
            }

            with open(temp_path, "w") as f:
                self._lock_file(f, exclusive=True)
                try:
                    json.dump(data, f, indent=2)
                finally:
                    self._unlock_file(f)

            temp_path.rename(self.history_path)
            self._dirty = False
            logger.debug(f"Saved {len(self._jobs)} jobs and {len(self._queues)} queues to history")
        except Exception as e:
            logger.error(f"Failed to save history: {e}")

    def save_if_dirty(self) -> None:
        """Save if there are unsaved changes. Call this periodically."""
        self._save()

    def get_job(self, job_id: str) -> UnifiedJob | None:
        """Get a job by ID."""
        return self._jobs.get(job_id)

    def get_job_by_slurm_id(self, slurm_job_id: str, cluster_name: str) -> UnifiedJob | None:
        """Get a job by its SLURM job ID and cluster."""
        for job in self._jobs.values():
            if job.slurm_job_id == slurm_job_id and job.cluster_name == cluster_name:
                return job
        return None

    def get_all_jobs(self, cluster_name: str | None = None) -> list[UnifiedJob]:
        """Get all jobs, optionally filtered by cluster."""
        jobs = list(self._jobs.values())
        if cluster_name:
            jobs = [j for j in jobs if j.cluster_name == cluster_name]
        # Sort by created_at descending (newest first)
        return sorted(jobs, key=lambda j: j.created_at, reverse=True)

    def get_active_jobs(self, cluster_name: str | None = None) -> list[UnifiedJob]:
        """Get jobs that are not in terminal state."""
        return [j for j in self.get_all_jobs(cluster_name) if not j.is_terminal]

    def add_job(self, job: UnifiedJob) -> None:
        """Add or update a job in history."""
        self._jobs[job.id] = job
        self._dirty = True

    def register_queue_item(
        self,
        item: QueueItem,
        queue_id: str,
        cluster_name: str,
        user: str,
    ) -> UnifiedJob:
        """Register a job from queue submission.

        Creates a UnifiedJob when a QueueItem is created, before SLURM submission.
        """
        job_id = f"q-{queue_id}-{item.task.id}"

        job = UnifiedJob(
            id=job_id,
            slurm_job_id=item.slurm_job_id,
            name=item.task.name,
            user=user,
            cluster_name=cluster_name,
            state=self._map_queue_status(item.status),
            source=UnifiedJobSource.QUEUE,
            partition=item.task.partition,
            cpus=item.task.cpus,
            memory=item.task.memory,
            time_limit=item.task.time_limit,
            created_at=datetime.now(),
            submit_time=item.submitted_at,
            start_time=item.started_at,
            finish_time=item.finished_at,
            queue_id=queue_id,
            task_id=item.task.id,
            error=item.error,
        )

        self.add_job(job)
        return job

    def update_from_queue_item(self, item: QueueItem, queue_id: str) -> None:
        """Update a job from queue item state changes."""
        job_id = f"q-{queue_id}-{item.task.id}"
        job = self._jobs.get(job_id)

        if job is None:
            logger.warning(f"Job {job_id} not found in history for update")
            return

        job.slurm_job_id = item.slurm_job_id
        job.state = self._map_queue_status(item.status)
        job.submit_time = item.submitted_at
        job.start_time = item.started_at
        job.finish_time = item.finished_at
        job.error = item.error
        job.last_seen = datetime.now()

        self._dirty = True

    def update_from_slurm_poll(
        self,
        slurm_jobs: list[SlurmJob],
        cluster_name: str,
    ) -> None:
        """Update history from SLURM polling results.

        - Updates existing jobs with new state
        - Adds new external jobs (not from queue)
        - Marks jobs not in poll as potentially completed
        """
        now = datetime.now()
        seen_slurm_ids: set[str] = set()

        for slurm_job in slurm_jobs:
            seen_slurm_ids.add(slurm_job.job_id)

            # Check if we already track this job
            existing = self.get_job_by_slurm_id(slurm_job.job_id, cluster_name)

            if existing:
                # Update existing job
                existing.state = self._map_slurm_state(slurm_job.state)
                existing.time_used = slurm_job.time_used
                existing.nodes = slurm_job.nodes
                existing.start_time = slurm_job.start_time or existing.start_time
                existing.last_seen = now
                self._dirty = True
            else:
                # New external job - add to history
                job = UnifiedJob(
                    id=f"ext-{cluster_name}-{slurm_job.job_id}",
                    slurm_job_id=slurm_job.job_id,
                    name=slurm_job.name,
                    user=slurm_job.user,
                    cluster_name=cluster_name,
                    state=self._map_slurm_state(slurm_job.state),
                    source=UnifiedJobSource.EXTERNAL,
                    partition=slurm_job.partition,
                    cpus=slurm_job.cpus,
                    memory=slurm_job.memory,
                    nodes=slurm_job.nodes,
                    time_used=slurm_job.time_used,
                    created_at=slurm_job.submit_time or now,
                    submit_time=slurm_job.submit_time,
                    start_time=slurm_job.start_time,
                    last_seen=now,
                )
                self.add_job(job)

        # Mark jobs not in poll as completed (if they were running/submitted)
        for job in self._jobs.values():
            if job.cluster_name != cluster_name:
                continue
            if job.slurm_job_id and job.slurm_job_id not in seen_slurm_ids:
                if not job.is_terminal:
                    # Job disappeared from squeue - assume completed
                    job.state = UnifiedJobState.COMPLETED
                    job.finish_time = now
                    job.last_seen = now
                    self._dirty = True
                    logger.info(f"Job {job.slurm_job_id} completed (disappeared from squeue)")

    def cleanup_old_jobs(self) -> int:
        """Remove jobs and queues older than retention period.

        Returns:
            Number of jobs removed.
        """
        cutoff = datetime.now() - timedelta(days=self.RETENTION_DAYS)
        jobs_to_remove = []

        for job_id, job in self._jobs.items():
            # Only remove terminal jobs
            if job.is_terminal:
                # Use finish_time if available, otherwise last_seen
                reference_time = job.finish_time or job.last_seen
                if reference_time < cutoff:
                    jobs_to_remove.append(job_id)

        for job_id in jobs_to_remove:
            del self._jobs[job_id]

        # Also clean up queues that have no remaining jobs and are old
        queues_to_remove = []
        for queue_id, queue_meta in self._queues.items():
            jobs_for_queue = self.get_jobs_for_queue(queue_id)
            if not jobs_for_queue and queue_meta.created_at < cutoff:
                queues_to_remove.append(queue_id)

        for queue_id in queues_to_remove:
            del self._queues[queue_id]

        if jobs_to_remove or queues_to_remove:
            self._dirty = True
            logger.info(f"Cleaned up {len(jobs_to_remove)} old jobs and {len(queues_to_remove)} old queues")

        return len(jobs_to_remove)

    def _map_queue_status(self, status: QueueItemStatus) -> UnifiedJobState:
        """Map QueueItemStatus to UnifiedJobState."""
        mapping = {
            QueueItemStatus.PENDING: UnifiedJobState.PENDING,
            QueueItemStatus.SUBMITTED: UnifiedJobState.SUBMITTED,
            QueueItemStatus.RUNNING: UnifiedJobState.RUNNING,
            QueueItemStatus.COMPLETED: UnifiedJobState.COMPLETED,
            QueueItemStatus.FAILED: UnifiedJobState.FAILED,
            QueueItemStatus.DEP_FAILED: UnifiedJobState.FAILED,
        }
        return mapping.get(status, UnifiedJobState.UNKNOWN)

    def _map_slurm_state(self, state: JobState) -> UnifiedJobState:
        """Map JobState (SLURM) to UnifiedJobState."""
        mapping = {
            JobState.PENDING: UnifiedJobState.SUBMITTED,  # SLURM pending = submitted to queue
            JobState.RUNNING: UnifiedJobState.RUNNING,
            JobState.COMPLETING: UnifiedJobState.RUNNING,
            JobState.COMPLETED: UnifiedJobState.COMPLETED,
            JobState.CANCELLED: UnifiedJobState.FAILED,
            JobState.FAILED: UnifiedJobState.FAILED,
            JobState.TIMEOUT: UnifiedJobState.FAILED,
            JobState.NODE_FAIL: UnifiedJobState.FAILED,
            JobState.PREEMPTED: UnifiedJobState.FAILED,
            JobState.BOOT_FAIL: UnifiedJobState.FAILED,
            JobState.DEADLINE: UnifiedJobState.FAILED,
            JobState.OUT_OF_MEMORY: UnifiedJobState.FAILED,
        }
        return mapping.get(state, UnifiedJobState.UNKNOWN)

    # Queue persistence methods

    def register_queue(self, queue: Queue) -> None:
        """Register a queue in history.

        Args:
            queue: The queue to register.
        """
        metadata = QueueMetadata(
            id=queue.id,
            source_name=queue.source_name,
            cluster_name=queue.cluster_name,
            created_at=queue.created_at,
            max_concurrent=queue.max_concurrent,
            log_dir=queue.log_dir,
            account=queue.account,
            login_shell=queue.login_shell,
        )
        self._queues[queue.id] = metadata
        self._dirty = True
        logger.info(f"Registered queue '{queue.id}' in history")

    def get_queue_metadata(self, queue_id: str) -> QueueMetadata | None:
        """Get queue metadata by ID."""
        return self._queues.get(queue_id)

    def get_all_queue_metadata(self) -> list[QueueMetadata]:
        """Get all queue metadata, sorted by creation time (newest first)."""
        return sorted(self._queues.values(), key=lambda q: q.created_at, reverse=True)

    def get_jobs_for_queue(self, queue_id: str) -> list[UnifiedJob]:
        """Get all jobs belonging to a specific queue."""
        return [
            job for job in self._jobs.values()
            if job.queue_id == queue_id
        ]

    def reconstruct_queue(self, queue_id: str) -> Queue | None:
        """Reconstruct a Queue object from history.

        Creates a Queue with QueueItems rebuilt from persisted jobs.
        Note: TaskDefinition will have minimal info (only what was stored in jobs).

        Args:
            queue_id: The queue ID to reconstruct.

        Returns:
            Reconstructed Queue or None if queue not found.
        """
        metadata = self._queues.get(queue_id)
        if metadata is None:
            return None

        # Get all jobs for this queue
        jobs = self.get_jobs_for_queue(queue_id)
        if not jobs:
            # Queue exists but has no jobs - return empty queue
            return Queue(
                id=metadata.id,
                source_name=metadata.source_name,
                cluster_name=metadata.cluster_name,
                created_at=metadata.created_at,
                items=[],
                max_concurrent=metadata.max_concurrent,
                log_dir=metadata.log_dir,
                account=metadata.account,
                login_shell=metadata.login_shell,
            )

        # Rebuild QueueItems from jobs
        items: list[QueueItem] = []
        for job in jobs:
            # Create minimal TaskDefinition from job info
            task = TaskDefinition(
                id=job.task_id or job.id,
                name=job.name,
                command="",  # Not stored in job history
                partition=job.partition,
                cpus=job.cpus,
                memory=job.memory,
                time_limit=job.time_limit,
            )

            # Map UnifiedJobState back to QueueItemStatus
            status_map = {
                UnifiedJobState.PENDING: QueueItemStatus.PENDING,
                UnifiedJobState.SUBMITTED: QueueItemStatus.SUBMITTED,
                UnifiedJobState.RUNNING: QueueItemStatus.RUNNING,
                UnifiedJobState.COMPLETED: QueueItemStatus.COMPLETED,
                UnifiedJobState.FAILED: QueueItemStatus.FAILED,
                UnifiedJobState.UNKNOWN: QueueItemStatus.PENDING,
            }

            item = QueueItem(
                task=task,
                status=status_map.get(job.state, QueueItemStatus.PENDING),
                slurm_job_id=job.slurm_job_id,
                submitted_at=job.submit_time,
                started_at=job.start_time,
                finished_at=job.finish_time,
                error=job.error,
            )
            items.append(item)

        return Queue(
            id=metadata.id,
            source_name=metadata.source_name,
            cluster_name=metadata.cluster_name,
            created_at=metadata.created_at,
            items=items,
            max_concurrent=metadata.max_concurrent,
            log_dir=metadata.log_dir,
            account=metadata.account,
            login_shell=metadata.login_shell,
        )

    def reconstruct_all_queues(self) -> dict[str, Queue]:
        """Reconstruct all queues from history.

        Returns:
            Dictionary mapping queue IDs to reconstructed Queue objects.
        """
        queues: dict[str, Queue] = {}
        for queue_id in self._queues:
            queue = self.reconstruct_queue(queue_id)
            if queue is not None:
                queues[queue_id] = queue
        logger.info(f"Reconstructed {len(queues)} queues from history")
        return queues
