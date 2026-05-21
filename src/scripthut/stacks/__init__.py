"""Reusable software stacks: hash + lazy install + per-backend cache."""

from scripthut.stacks.manager import (
    StackManager,
    StackState,
    StackStatus,
    compute_stack_hash,
)

__all__ = [
    "StackManager",
    "StackState",
    "StackStatus",
    "compute_stack_hash",
]
