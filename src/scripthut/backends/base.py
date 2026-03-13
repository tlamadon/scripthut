"""Abstract base class for job backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from scripthut.runs.models import TaskDefinition


@dataclass
class JobStats:
    """Resource utilization stats from a job scheduler's accounting system."""

    cpu_efficiency: float  # 0-100%
    max_rss: str  # Human-readable, e.g. "1.2G"
    total_cpu: str  # Raw time string
    start_time: datetime | None = None
    end_time: datetime | None = None
    state: str | None = None  # Terminal state from accounting, e.g. "COMPLETED"


class JobBackend(ABC):
    """Abstract base class for job management backends (Slurm, PBS, ECS, etc.)."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the backend name."""
        ...

    @abstractmethod
    async def get_jobs(self, user: str | None = None) -> list[Any]:
        """Fetch current jobs from the backend.

        Args:
            user: Optional filter by username. None means all users.

        Returns:
            List of job objects (type depends on backend).
        """
        ...

    @abstractmethod
    async def is_available(self) -> bool:
        """Check if the backend is available and responding."""
        ...

    @abstractmethod
    async def submit_job(self, script: str) -> str:
        """Submit a job script to the scheduler.

        Args:
            script: The full submission script content.

        Returns:
            The scheduler-assigned job ID.

        Raises:
            RuntimeError: If submission fails.
        """
        ...

    @abstractmethod
    async def cancel_job(self, job_id: str) -> None:
        """Cancel a running or queued job."""
        ...

    @abstractmethod
    async def get_job_stats(
        self, job_ids: list[str], user: str | None = None
    ) -> dict[str, JobStats]:
        """Fetch resource utilization stats for completed jobs.

        Args:
            job_ids: List of job IDs to query.
            user: Optional username to scope the query.

        Returns:
            Dict mapping job_id to JobStats. IDs not found are omitted.
        """
        ...

    @abstractmethod
    async def get_cluster_info(self) -> tuple[int, int] | None:
        """Fetch cluster resource info.

        Returns:
            (total_cpus, idle_cpus) tuple, or None on failure.
        """
        ...

    @abstractmethod
    def generate_script(
        self,
        task: TaskDefinition,
        run_id: str,
        log_dir: str,
        account: str | None = None,
        login_shell: bool = False,
        env_vars: dict[str, str] | None = None,
        extra_init: str = "",
    ) -> str:
        """Generate a scheduler-specific submission script for a task."""
        ...

    @property
    @abstractmethod
    def failure_states(self) -> dict[str, str]:
        """Map of terminal failure state names to human-readable reasons."""
        ...

    @property
    @abstractmethod
    def terminal_states(self) -> frozenset[str]:
        """Set of all terminal accounting state names (success and failure)."""
        ...
