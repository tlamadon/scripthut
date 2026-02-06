"""Job backend implementations."""

from scripthut.backends.base import JobBackend
from scripthut.backends.slurm import SlurmBackend

__all__ = ["JobBackend", "SlurmBackend"]
