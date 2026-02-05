"""Abstract base class for job backends."""

from abc import ABC, abstractmethod
from typing import Any


class JobBackend(ABC):
    """Abstract base class for job management backends (Slurm, ECS, Batch, etc.)."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the backend name."""
        ...

    @abstractmethod
    async def get_jobs(self, user: str | None = None) -> list[Any]:
        """
        Fetch current jobs from the backend.

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
