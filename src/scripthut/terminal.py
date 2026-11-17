"""Terminal session manager for web-based interactive terminals."""

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from fastapi import WebSocket


logger = logging.getLogger(__name__)


@dataclass
class TerminalSession:
    """Tracks a single active terminal session."""

    id: str
    backend_name: str
    session_type: str  # Existing compute-node tmux session
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    node: str | None = None
    job_id: str | None = None
    label: str | None = None
    websocket: WebSocket | None = field(default=None, repr=False)


class TerminalManager:
    """Manages active terminal sessions."""

    def __init__(self) -> None:
        self._sessions: dict[str, TerminalSession] = {}

    @property
    def sessions(self) -> dict[str, TerminalSession]:
        return self._sessions

    def create_session(
        self,
        backend_name: str,
        session_type: str,
        *,
        node: str | None = None,
        job_id: str | None = None,
        label: str | None = None,
    ) -> TerminalSession:
        session = TerminalSession(
            id=uuid.uuid4().hex[:12],
            backend_name=backend_name,
            session_type=session_type,
            node=node,
            job_id=job_id,
            label=label,
        )
        self._sessions[session.id] = session
        return session

    def remove_session(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)

    async def close_session(self, session_id: str) -> bool:
        """Close a session's WebSocket, triggering cleanup. Returns True if found."""
        session = self._sessions.get(session_id)
        if session is None:
            return False
        if session.websocket:
            try:
                await session.websocket.close()
            except Exception:
                pass
        return True
