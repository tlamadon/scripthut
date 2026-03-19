"""In-memory ring buffer for SSH command logging."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone


MAX_OUTPUT_LEN = 2048  # Truncate stdout/stderr to this length


@dataclass
class CommandLogEntry:
    """A single logged SSH command."""

    timestamp: datetime
    command: str
    exit_code: int | None = None
    duration_ms: int = 0
    stdout: str = ""
    stderr: str = ""
    error: str | None = None  # Exception message if command failed


class CommandLog:
    """Ring buffer of SSH command log entries for a single backend."""

    def __init__(self, maxlen: int = 200) -> None:
        self._entries: deque[CommandLogEntry] = deque(maxlen=maxlen)

    def append(self, entry: CommandLogEntry) -> None:
        """Add an entry to the log."""
        # Truncate large outputs
        if len(entry.stdout) > MAX_OUTPUT_LEN:
            entry.stdout = entry.stdout[:MAX_OUTPUT_LEN] + f"\n... (truncated, {len(entry.stdout)} bytes total)"
        if len(entry.stderr) > MAX_OUTPUT_LEN:
            entry.stderr = entry.stderr[:MAX_OUTPUT_LEN] + f"\n... (truncated, {len(entry.stderr)} bytes total)"
        self._entries.append(entry)

    def get_entries(self) -> list[CommandLogEntry]:
        """Return entries newest first."""
        return list(reversed(self._entries))

    def clear(self) -> None:
        """Clear all entries."""
        self._entries.clear()

    def __len__(self) -> int:
        return len(self._entries)
