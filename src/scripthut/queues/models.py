"""Data models for task queues."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class QueueItemStatus(str, Enum):
    """Status of a queue item."""

    PENDING = "pending"  # Waiting to be submitted
    SUBMITTED = "submitted"  # Submitted to Slurm, waiting to run
    RUNNING = "running"  # Currently running
    COMPLETED = "completed"  # Finished successfully
    FAILED = "failed"  # Failed or cancelled
    DEP_FAILED = "dep_failed"  # Skipped because a dependency failed


@dataclass
class TaskDefinition:
    """Definition of a task from JSON input."""

    id: str
    name: str
    command: str
    working_dir: str = "~"
    partition: str = "normal"
    cpus: int = 1
    memory: str = "4G"
    time_limit: str = "1:00:00"
    dependencies: list[str] = field(default_factory=list)
    generates_source: str | None = None  # Path to JSON file this task creates on the cluster
    output_file: str | None = None  # Custom stdout log path
    error_file: str | None = None  # Custom stderr log path
    environment: str | None = None  # Name of the environment to use (from config)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TaskDefinition":
        """Create TaskDefinition from dictionary."""
        return cls(
            id=data["id"],
            name=data["name"],
            command=data["command"],
            working_dir=data.get("working_dir", "~"),
            partition=data.get("partition", "normal"),
            cpus=data.get("cpus", 1),
            memory=data.get("memory", "4G"),
            time_limit=data.get("time_limit", "1:00:00"),
            dependencies=data.get("deps", data.get("dependencies", [])),
            generates_source=data.get("generates_source"),
            output_file=data.get("output_file"),
            error_file=data.get("error_file"),
            environment=data.get("environment"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON storage."""
        return {
            "id": self.id,
            "name": self.name,
            "command": self.command,
            "working_dir": self.working_dir,
            "partition": self.partition,
            "cpus": self.cpus,
            "memory": self.memory,
            "time_limit": self.time_limit,
            "dependencies": self.dependencies,
            "generates_source": self.generates_source,
            "output_file": self.output_file,
            "error_file": self.error_file,
            "environment": self.environment,
        }

    def get_output_path(self, queue_id: str, log_dir: str = "~/.cache/scripthut/logs") -> str:
        """Get the output log file path."""
        if self.output_file:
            return self.output_file
        # Flat structure with IDs in filename
        return f"{log_dir}/scripthut_{queue_id}_{self.id}.out"

    def get_error_path(self, queue_id: str, log_dir: str = "~/.cache/scripthut/logs") -> str:
        """Get the error log file path."""
        if self.error_file:
            return self.error_file
        # Flat structure with IDs in filename
        return f"{log_dir}/scripthut_{queue_id}_{self.id}.err"

    def to_sbatch_script(
        self,
        queue_id: str,
        log_dir: str = "~/.cache/scripthut/logs",
        account: str | None = None,
        login_shell: bool = False,
        env_vars: dict[str, str] | None = None,
        extra_init: str = "",
    ) -> str:
        """Generate sbatch script for this task."""
        output_path = self.get_output_path(queue_id, log_dir)
        error_path = self.get_error_path(queue_id, log_dir)

        # Build account line if specified
        account_line = f"#SBATCH --account={account}\n" if account else ""

        shebang = "#!/bin/bash -l" if login_shell else "#!/bin/bash"

        # Build environment variable exports
        env_lines = ""
        if env_vars:
            export_lines = [f"export {key}=\"{value}\"" for key, value in env_vars.items()]
            if export_lines:
                env_lines = "\n".join(export_lines) + "\n\n"

        # Build extra init lines (e.g. module load)
        extra_init_lines = ""
        if extra_init:
            extra_init_lines = extra_init + "\n\n"

        return f"""{shebang}
#SBATCH --job-name="{self.name}"
#SBATCH --partition={self.partition}
{account_line}#SBATCH --cpus-per-task={self.cpus}
#SBATCH --mem={self.memory}
#SBATCH --time={self.time_limit}
#SBATCH --output={output_path}
#SBATCH --error={error_path}

echo "=== ScriptHut Task: {self.name} ==="
echo "Task ID: {self.id}"
echo "Started: $(date)"
echo "Host: $(hostname)"
echo "Working dir: {self.working_dir}"
echo "=================================="
echo ""

{env_lines}{extra_init_lines}cd {self.working_dir}
{self.command}
EXIT_CODE=$?

echo ""
echo "=================================="
echo "Finished: $(date)"
echo "Exit code: $EXIT_CODE"
exit $EXIT_CODE
"""


@dataclass
class QueueItem:
    """A single item in a queue, wrapping a task with runtime state."""

    task: TaskDefinition
    status: QueueItemStatus = QueueItemStatus.PENDING
    slurm_job_id: str | None = None
    submitted_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    sbatch_script: str | None = None  # The generated sbatch script used for submission

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON storage."""
        return {
            "task": self.task.to_dict(),
            "status": self.status.value,
            "slurm_job_id": self.slurm_job_id,
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "error": self.error,
            "sbatch_script": self.sbatch_script,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueItem":
        """Deserialize from dictionary."""

        def parse_dt(val: str | None) -> datetime | None:
            return datetime.fromisoformat(val) if val else None

        return cls(
            task=TaskDefinition.from_dict(data["task"]),
            status=QueueItemStatus(data["status"]),
            slurm_job_id=data.get("slurm_job_id"),
            submitted_at=parse_dt(data.get("submitted_at")),
            started_at=parse_dt(data.get("started_at")),
            finished_at=parse_dt(data.get("finished_at")),
            error=data.get("error"),
            sbatch_script=data.get("sbatch_script"),
        )

    @property
    def status_class(self) -> str:
        """Return CSS class for status styling."""
        status_classes = {
            QueueItemStatus.PENDING: "text-gray-500",
            QueueItemStatus.SUBMITTED: "text-yellow-600",
            QueueItemStatus.RUNNING: "text-blue-600",
            QueueItemStatus.COMPLETED: "text-green-600",
            QueueItemStatus.FAILED: "text-red-600",
            QueueItemStatus.DEP_FAILED: "text-orange-600",
        }
        return status_classes.get(self.status, "text-gray-500")


class QueueStatus(str, Enum):
    """Overall status of a queue."""

    PENDING = "pending"  # Not started yet
    RUNNING = "running"  # Has running or submitted items
    COMPLETED = "completed"  # All items completed successfully
    FAILED = "failed"  # Some items failed
    CANCELLED = "cancelled"  # Queue was cancelled


@dataclass
class Queue:
    """A batch of tasks triggered from a task source."""

    id: str
    source_name: str
    cluster_name: str
    created_at: datetime
    items: list[QueueItem]
    max_concurrent: int
    log_dir: str = "~/.cache/scripthut/logs"  # Directory for log files on the remote cluster
    account: str | None = None  # Slurm account to charge jobs to
    login_shell: bool = False  # Use #!/bin/bash -l shebang

    @property
    def status(self) -> QueueStatus:
        """Calculate overall queue status."""
        if not self.items:
            return QueueStatus.COMPLETED

        statuses = [item.status for item in self.items]

        terminal_failures = (QueueItemStatus.FAILED, QueueItemStatus.DEP_FAILED)

        # Check for failures
        if any(s in terminal_failures for s in statuses):
            # Queue is FAILED if every item is either terminal-failed or completed,
            # OR if every non-terminal item is PENDING with all deps failed
            # (i.e. nothing more can ever progress).
            if all(s in (*terminal_failures, QueueItemStatus.COMPLETED) for s in statuses):
                return QueueStatus.FAILED
            # Also FAILED if remaining items are only PENDING (blocked by failed deps)
            if all(s in (*terminal_failures, QueueItemStatus.COMPLETED, QueueItemStatus.PENDING) for s in statuses):
                # Check that every pending item has at least one failed dep
                pending_items = [item for item in self.items if item.status == QueueItemStatus.PENDING]
                if pending_items and all(self.get_failed_deps(item) for item in pending_items):
                    return QueueStatus.FAILED

        # Check if all completed
        if all(s == QueueItemStatus.COMPLETED for s in statuses):
            return QueueStatus.COMPLETED

        # Check if any running or submitted
        if any(s in (QueueItemStatus.RUNNING, QueueItemStatus.SUBMITTED) for s in statuses):
            return QueueStatus.RUNNING

        # All pending
        return QueueStatus.PENDING

    @property
    def progress(self) -> tuple[int, int]:
        """Return (completed_count, total_count)."""
        completed = sum(
            1 for item in self.items
            if item.status in (QueueItemStatus.COMPLETED, QueueItemStatus.FAILED, QueueItemStatus.DEP_FAILED)
        )
        return (completed, len(self.items))

    @property
    def progress_percent(self) -> int:
        """Return progress as percentage."""
        completed, total = self.progress
        if total == 0:
            return 100
        return int((completed / total) * 100)

    @property
    def running_count(self) -> int:
        """Count of currently running items."""
        return sum(
            1 for item in self.items
            if item.status in (QueueItemStatus.RUNNING, QueueItemStatus.SUBMITTED)
        )

    @property
    def pending_count(self) -> int:
        """Count of pending items."""
        return sum(1 for item in self.items if item.status == QueueItemStatus.PENDING)

    @property
    def completed_count(self) -> int:
        """Count of completed items."""
        return sum(1 for item in self.items if item.status == QueueItemStatus.COMPLETED)

    @property
    def failed_count(self) -> int:
        """Count of failed items."""
        return sum(1 for item in self.items if item.status == QueueItemStatus.FAILED)

    @property
    def dep_failed_count(self) -> int:
        """Count of items that failed due to dependency failure."""
        return sum(1 for item in self.items if item.status == QueueItemStatus.DEP_FAILED)

    def are_deps_satisfied(self, item: QueueItem) -> bool:
        """Check if all dependencies of an item are completed."""
        if not item.task.dependencies:
            return True
        for dep_id in item.task.dependencies:
            dep_item = self.get_item_by_task_id(dep_id)
            if dep_item is None or dep_item.status != QueueItemStatus.COMPLETED:
                return False
        return True

    def get_failed_deps(self, item: QueueItem) -> list[str]:
        """Return list of dependency IDs that have failed or dep_failed."""
        failed = []
        for dep_id in item.task.dependencies:
            dep_item = self.get_item_by_task_id(dep_id)
            if dep_item is not None and dep_item.status in (
                QueueItemStatus.FAILED, QueueItemStatus.DEP_FAILED
            ):
                failed.append(dep_id)
        return failed

    def get_item_by_task_id(self, task_id: str) -> QueueItem | None:
        """Get a queue item by its task ID."""
        for item in self.items:
            if item.task.id == task_id:
                return item
        return None

    def get_item_by_slurm_id(self, slurm_job_id: str) -> QueueItem | None:
        """Get a queue item by its Slurm job ID."""
        for item in self.items:
            if item.slurm_job_id == slurm_job_id:
                return item
        return None

    @property
    def status_class(self) -> str:
        """Return CSS class for status styling."""
        status_classes = {
            QueueStatus.PENDING: "text-gray-500",
            QueueStatus.RUNNING: "text-blue-600",
            QueueStatus.COMPLETED: "text-green-600",
            QueueStatus.FAILED: "text-red-600",
            QueueStatus.CANCELLED: "text-gray-600",
        }
        return status_classes.get(self.status, "text-gray-500")
