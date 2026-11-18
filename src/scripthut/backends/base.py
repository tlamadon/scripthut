"""Abstract base class for job backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


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
    async def get_cluster_info(self, user: str | None = None) -> ClusterInfo | None:
        """Fetch cluster resource availability.

        Return partitions/queues and pending reasons. The job filter does
        not select quota identity: quota describes the SSH login.
        Return None when cluster observation fails.
        """
        ...
