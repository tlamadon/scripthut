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


@dataclass
class SubmitResult:
    """Outcome of a task submission to a backend.

    ``submit_output`` carries the raw stdout/stderr from the submission
    command (sbatch/qsub/AWS API response).  Stored on the run item so the
    user can inspect what the scheduler actually said even on apparent
    success — useful when the parsed job ID looks fine but the job never
    enters the queue.
    """

    job_id: str
    submit_output: str = ""


@dataclass
class DiskInfo:
    """Disk usage for a path on the backend filesystem."""

    total_bytes: int
    avail_bytes: int
    path: str


@dataclass
class PartitionInfo:
    """Resource availability for a single scheduler partition/queue."""

    name: str
    state: str  # "up", "down", "drain", "inact", etc.
    cpus_allocated: int
    cpus_idle: int
    cpus_other: int  # drained/down/reserved
    cpus_total: int
    nodes_total: int
    timelimit: str | None = None  # e.g. "1-00:00:00" or "infinite"
    mem_per_node_mb: int | None = None
    features: str | None = None  # comma-separated, e.g. "gpu,a100"
    is_default: bool = False
    gpus_total: int = 0  # 0 means "no GPUs in this partition"
    gpus_idle: int = 0
    gpu_types: str | None = None  # comma-separated GPU type labels, e.g. "a100,v100"


@dataclass
class QuotaInfo:
    """Per-user fair-share, current usage, and scheduling limits.

    All fields are optional; the backend populates whatever it can
    determine. ``*_max == None`` means "no limit set" or "limit unknown".
    ``*_used`` is the user's current consumption across all running jobs
    (only counted when the backend has a cheap way to compute it).
    """

    account: str | None = None
    fair_share: float | None = None  # 0.0-1.0; higher = better priority weight
    norm_usage: float | None = None  # Recent usage as fraction of parent
    jobs_used: int | None = None
    jobs_max: int | None = None
    cpus_used: int | None = None
    cpus_max: int | None = None
    gpus_used: int | None = None
    gpus_max: int | None = None


@dataclass
class ClusterInfo:
    """Aggregate cluster resource snapshot.

    Backends with no partition concept (Batch, EC2) return a single
    pseudo-partition named ``"default"``.
    """

    partitions: list[PartitionInfo]
    pending_reasons: dict[str, int]  # squeue reason -> count of pending jobs
    user_quota: QuotaInfo | None = None  # Only populated when get_cluster_info(user=...) is given

    @property
    def cpus_total(self) -> int:
        return sum(p.cpus_total for p in self.partitions)

    @property
    def cpus_idle(self) -> int:
        return sum(p.cpus_idle for p in self.partitions)


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

    async def submit_task(
        self,
        task: "TaskDefinition",
        script: str,
        env_vars: dict[str, str] | None = None,
    ) -> SubmitResult:
        """Submit a task with its full context.

        Default implementation forwards to :meth:`submit_job`, which is
        sufficient for shell-script-based schedulers (env vars are already
        inlined as ``export`` lines in the script). API-based backends
        (e.g. AWS Batch) override this to use structured task metadata and
        pass ``env_vars`` through the API (e.g. ``containerOverrides.environment``)
        so the container's entrypoint sees them before the script runs.

        Returns a :class:`SubmitResult` so backends can additionally surface
        raw stdout/stderr from the submission command for diagnostics.
        """
        job_id = await self.submit_job(script)
        return SubmitResult(job_id=job_id)

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
    async def get_cluster_info(self, user: str | None = None) -> ClusterInfo | None:
        """Fetch cluster resource availability.

        Returns a snapshot with one entry per partition/queue plus a
        breakdown of why pending jobs are waiting. Backends without a
        partition concept (Batch, EC2) return a single ``"default"``
        pseudo-partition.

        Args:
            user: When given, populate ``user_quota`` with this user's
                fair-share / scheduling limits. Backends that don't
                support per-user quota leave the field as ``None``.

        Returns:
            ClusterInfo, or None on failure.
        """
        ...

    async def get_disk_info(self, path: str) -> DiskInfo | None:
        """Fetch disk usage for a path on the backend filesystem.

        Default implementation returns None; SSH-based backends override
        this to run ``df`` over the connection.
        """
        return None

    async def fetch_log(
        self,
        job_id: str,
        log_path: str,
        log_type: str = "output",
        tail_lines: int | None = None,
    ) -> tuple[str | None, str | None]:
        """Fetch stdout/stderr log contents for a submitted job.

        Args:
            job_id: Scheduler-assigned job ID (needed by API-based backends).
            log_path: Filesystem path to the log file (used by SSH backends).
            log_type: ``"output"`` or ``"error"``.
            tail_lines: If set, return only the last N lines.

        Returns:
            ``(content, error)`` — exactly one of the two will be set.
        """
        return None, "Log fetching not implemented for this backend"

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
        interactive_wait: bool = False,
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
