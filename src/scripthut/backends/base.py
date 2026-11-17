"""Abstract base class for job backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from scripthut.scheduler import SubmitResult
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any



@dataclass
class JobStats:
    """Resource utilization stats from a job scheduler's accounting system."""

    cpu_efficiency: float  # 0-100%
    max_rss: str  # Human-readable, e.g. "1.2G"
    total_cpu: str  # Raw time string
    start_time: datetime | None = None
    end_time: datetime | None = None
    state: str | None = None  # Terminal state from accounting, e.g. "COMPLETED"
    # Numeric exit code from accounting. For Slurm: the exit half of
    # sacct's ``ExitCode`` (``<exit>:<signal>``); signal != 0 with exit
    # 0 is treated as a non-zero exit by the consumer logic. ``None``
    # means accounting either didn't return a row or didn't surface a
    # parseable value — distinct from "exit was 0", which is an int.
    exit_code: int | None = None






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
    # --- "Can I schedule right now?" hints (node-level, single biggest slot) ---
    # Partition-level idle counts can lie: 100 idle CPUs spread one-per-node
    # can't run a 4-CPU job, and idle GPUs on a CPU-saturated node aren't
    # grabbable. These look at individual nodes so the UI can show the
    # largest job that would actually start immediately.
    cpus_free_max_node: int = 0  # most idle CPUs on any single schedulable node
    mem_free_max_node_mb: int | None = None  # most free memory on a single node
    # Idle GPUs that sit on a node which also has a free CPU — i.e. GPUs you
    # could actually claim now. ``gpus_idle`` minus this is "idle but stuck".
    gpus_schedulable: int = 0


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
    """Cluster partitions, pending reasons and optional user quota."""

    partitions: list[PartitionInfo]
    pending_reasons: dict[str, int]  # squeue reason -> count of pending jobs
    user_quota: QuotaInfo | None = None  # Usage/limits for the backend's own SSH login; None if unsupported

    @property
    def cpus_total(self) -> int:
        return sum(p.cpus_total for p in self.partitions)

    @property
    def cpus_idle(self) -> int:
        return sum(p.cpus_idle for p in self.partitions)


class JobBackend(ABC):
    """Read-only scheduler observation interface."""

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
            user: The dashboard's current job-list filter, if any. It does
                NOT choose whose quota to report: ``user_quota`` always
                reflects the identity the backend submits as (its own SSH
                login), since "Your usage" must track the account that
                actually owns our jobs. Backends that don't support
                per-user quota leave the field as ``None``.

        Returns:
            ClusterInfo, or None on failure.
        """
        ...


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
