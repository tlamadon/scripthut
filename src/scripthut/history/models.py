"""Data models for job history tracking."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


@dataclass
class QueueMetadata:
    """Metadata for a queue, used for persistence."""

    id: str
    source_name: str
    cluster_name: str
    created_at: datetime
    max_concurrent: int
    log_dir: str = "~/.cache/scripthut/logs"
    account: str | None = None
    login_shell: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON storage."""
        return {
            "id": self.id,
            "source_name": self.source_name,
            "cluster_name": self.cluster_name,
            "created_at": self.created_at.isoformat(),
            "max_concurrent": self.max_concurrent,
            "log_dir": self.log_dir,
            "account": self.account,
            "login_shell": self.login_shell,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueueMetadata":
        """Deserialize from dictionary."""
        return cls(
            id=data["id"],
            source_name=data["source_name"],
            cluster_name=data["cluster_name"],
            created_at=datetime.fromisoformat(data["created_at"]),
            max_concurrent=data["max_concurrent"],
            log_dir=data.get("log_dir", "~/.cache/scripthut/logs"),
            account=data.get("account"),
            login_shell=data.get("login_shell", False),
        )


class UnifiedJobSource(str, Enum):
    """Source of the job."""

    QUEUE = "queue"  # Submitted via our queue system
    EXTERNAL = "external"  # Detected via SLURM polling (not our submission)


class UnifiedJobState(str, Enum):
    """Unified state across all job sources."""

    PENDING = "pending"  # In queue, not yet submitted to SLURM
    SUBMITTED = "submitted"  # Submitted to SLURM, waiting in queue
    RUNNING = "running"  # Currently executing
    COMPLETED = "completed"  # Finished successfully
    FAILED = "failed"  # Failed or cancelled
    UNKNOWN = "unknown"  # State cannot be determined


@dataclass
class UnifiedJob:
    """Represents a job from any source with unified state tracking."""

    # Core identifiers
    id: str  # Unique ID (q-{queue_id}-{task_id} or ext-{cluster}-{slurm_id})
    slurm_job_id: str | None  # SLURM job ID (None if not yet submitted)

    # Display info
    name: str
    user: str
    cluster_name: str

    # State tracking
    state: UnifiedJobState
    source: UnifiedJobSource

    # Resources
    partition: str = ""
    cpus: int = 1
    memory: str = ""
    nodes: str = ""
    time_used: str = ""
    time_limit: str = ""

    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)
    submit_time: datetime | None = None
    start_time: datetime | None = None
    finish_time: datetime | None = None

    # Queue association (if from queue system)
    queue_id: str | None = None
    task_id: str | None = None

    # Error info
    error: str | None = None

    # Resource utilization (from sacct)
    cpu_efficiency: float | None = None  # 0-100%
    max_rss: str | None = None  # Peak memory, e.g. "1.2G"

    # For tracking updates
    last_seen: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON storage."""
        return {
            "id": self.id,
            "slurm_job_id": self.slurm_job_id,
            "name": self.name,
            "user": self.user,
            "cluster_name": self.cluster_name,
            "state": self.state.value,
            "source": self.source.value,
            "partition": self.partition,
            "cpus": self.cpus,
            "memory": self.memory,
            "nodes": self.nodes,
            "time_used": self.time_used,
            "time_limit": self.time_limit,
            "created_at": self.created_at.isoformat(),
            "submit_time": self.submit_time.isoformat() if self.submit_time else None,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "finish_time": self.finish_time.isoformat() if self.finish_time else None,
            "queue_id": self.queue_id,
            "task_id": self.task_id,
            "error": self.error,
            "cpu_efficiency": self.cpu_efficiency,
            "max_rss": self.max_rss,
            "last_seen": self.last_seen.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "UnifiedJob":
        """Deserialize from dictionary."""

        def parse_dt(val: str | None) -> datetime | None:
            return datetime.fromisoformat(val) if val else None

        return cls(
            id=data["id"],
            slurm_job_id=data.get("slurm_job_id"),
            name=data["name"],
            user=data["user"],
            cluster_name=data["cluster_name"],
            state=UnifiedJobState(data["state"]),
            source=UnifiedJobSource(data["source"]),
            partition=data.get("partition", ""),
            cpus=data.get("cpus", 1),
            memory=data.get("memory", ""),
            nodes=data.get("nodes", ""),
            time_used=data.get("time_used", ""),
            time_limit=data.get("time_limit", ""),
            created_at=parse_dt(data["created_at"]) or datetime.now(),
            submit_time=parse_dt(data.get("submit_time")),
            start_time=parse_dt(data.get("start_time")),
            finish_time=parse_dt(data.get("finish_time")),
            queue_id=data.get("queue_id"),
            task_id=data.get("task_id"),
            error=data.get("error"),
            cpu_efficiency=data.get("cpu_efficiency"),
            max_rss=data.get("max_rss"),
            last_seen=parse_dt(data.get("last_seen")) or datetime.now(),
        )

    @property
    def state_class(self) -> str:
        """Return CSS class for state styling."""
        state_classes = {
            UnifiedJobState.RUNNING: "text-green-600",
            UnifiedJobState.PENDING: "text-gray-500",
            UnifiedJobState.SUBMITTED: "text-yellow-600",
            UnifiedJobState.COMPLETED: "text-blue-600",
            UnifiedJobState.FAILED: "text-red-600",
        }
        return state_classes.get(self.state, "text-gray-500")

    @property
    def is_terminal(self) -> bool:
        """Check if job is in a terminal state."""
        return self.state in (UnifiedJobState.COMPLETED, UnifiedJobState.FAILED)
