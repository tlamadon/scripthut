"""Terminal session manager for web-based interactive terminals."""

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from fastapi import WebSocket, WebSocketDisconnect

from scripthut.ssh.client import SSHClient

logger = logging.getLogger(__name__)


@dataclass
class TerminalSession:
    """Tracks a single active terminal session."""

    id: str
    backend_name: str
    session_type: str  # "headnode", "attach", or "job"
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


async def handle_terminal_websocket(
    websocket: WebSocket,
    ssh_client: SSHClient,
    command: str | None = None,
) -> None:
    """Run the bidirectional WebSocket <-> SSH PTY relay.

    Protocol (JSON over text frames):
      Client -> Server:
        {"type": "input", "data": "..."}   -- terminal keystrokes
        {"type": "resize", "cols": N, "rows": N}
      Server -> Client:
        {"type": "output", "data": "..."}  -- terminal output
        {"type": "error", "message": "..."}
        {"type": "exit", "code": N}
    """
    # Read init message with terminal dimensions
    try:
        init_msg = await asyncio.wait_for(websocket.receive_json(), timeout=10)
    except Exception:
        await websocket.send_json({"type": "error", "message": "Expected init message"})
        await websocket.close()
        return

    cols = init_msg.get("cols", 80)
    rows = init_msg.get("rows", 24)

    # Create SSH process with PTY
    try:
        process = await ssh_client.create_interactive_session(
            command=command,
            term_size=(cols, rows),
        )
    except Exception as e:
        logger.error(f"Failed to create interactive session: {e}")
        await websocket.send_json(
            {"type": "error", "message": f"SSH session failed: {e}"}
        )
        await websocket.close()
        return

    async def ws_to_ssh() -> None:
        """Forward WebSocket input to SSH stdin."""
        try:
            while True:
                raw = await websocket.receive_text()
                msg = json.loads(raw)
                if msg["type"] == "input":
                    process.stdin.write(msg["data"].encode())
                elif msg["type"] == "resize":
                    process.change_terminal_size(
                        msg.get("cols", 80), msg.get("rows", 24)
                    )
        except (WebSocketDisconnect, Exception):
            process.close()

    async def ssh_to_ws() -> None:
        """Forward SSH stdout to WebSocket."""
        try:
            while not process.is_closing():
                data = await process.stdout.read(4096)
                if not data:
                    break
                text = data.decode("utf-8", errors="replace")
                await websocket.send_json({"type": "output", "data": text})
        except Exception:
            pass
        finally:
            exit_code = process.returncode if process.returncode is not None else -1
            try:
                await websocket.send_json({"type": "exit", "code": exit_code})
                await websocket.close()
            except Exception:
                pass

    # Run both directions concurrently; when either finishes, we're done
    done, pending = await asyncio.wait(
        [asyncio.create_task(ws_to_ssh()), asyncio.create_task(ssh_to_ws())],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
