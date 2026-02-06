"""Data models for job management."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class JobState(str, Enum):
    """Slurm job states."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"
    COMPLETING = "COMPLETING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    NODE_FAIL = "NODE_FAIL"
    PREEMPTED = "PREEMPTED"
    BOOT_FAIL = "BOOT_FAIL"
    DEADLINE = "DEADLINE"
    OUT_OF_MEMORY = "OUT_OF_ME+"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_string(cls, state: str) -> "JobState":
        """Parse a state string, handling abbreviated forms."""
        state = state.strip().upper()
        # Handle common abbreviations
        abbrev_map = {
            "PD": cls.PENDING,
            "R": cls.RUNNING,
            "S": cls.SUSPENDED,
            "CG": cls.COMPLETING,
            "CD": cls.COMPLETED,
            "CA": cls.CANCELLED,
            "F": cls.FAILED,
            "TO": cls.TIMEOUT,
            "NF": cls.NODE_FAIL,
            "PR": cls.PREEMPTED,
            "BF": cls.BOOT_FAIL,
            "DL": cls.DEADLINE,
            "OOM": cls.OUT_OF_MEMORY,
        }
        if state in abbrev_map:
            return abbrev_map[state]
        try:
            return cls(state)
        except ValueError:
            return cls.UNKNOWN


@dataclass
class SlurmJob:
    """Represents a Slurm job with extended information."""

    job_id: str
    name: str
    user: str
    state: JobState
    partition: str
    time_used: str  # Elapsed time in Slurm format (D-HH:MM:SS)
    nodes: str  # Node list or count
    cpus: int
    memory: str  # Memory with unit (e.g., "4G")
    submit_time: datetime | None
    start_time: datetime | None

    @property
    def state_class(self) -> str:
        """Return CSS class for job state styling."""
        state_classes = {
            JobState.RUNNING: "text-green-600",
            JobState.PENDING: "text-yellow-600",
            JobState.COMPLETED: "text-blue-600",
            JobState.FAILED: "text-red-600",
            JobState.CANCELLED: "text-gray-600",
            JobState.TIMEOUT: "text-orange-600",
        }
        return state_classes.get(self.state, "text-gray-500")


@dataclass
class ConnectionStatus:
    """Status of the SSH connection."""

    connected: bool
    host: str
    last_poll: datetime | None = None
    error: str | None = None
    last_poll_duration_ms: int | None = None  # How long the last poll took in milliseconds
    job_count: int = 0  # Number of jobs returned by last poll
