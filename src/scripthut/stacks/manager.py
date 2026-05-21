"""Per-backend installer for reusable software stacks.

A stack is identified by a content hash over (name, prep script, sorted
`inputs` dict, contents of `input_files`). On a backend it lives at
``<cache_dir>/<name>/<hash>/`` with a ``.ready`` sentinel written only
when the prep script has succeeded end-to-end.

Operations are designed to be safe to interleave with the rest of
scripthut:

- ``check`` is a cheap remote stat; never modifies state.
- ``install`` is idempotent: if the ready sentinel exists for the
  current hash, it's a no-op unless ``rebuild=True``.
- ``delete`` removes a stack's entire directory (all hashes for that
  name); a subsequent ``install`` triggers a fresh build.

This module is deliberately SSH-only for v1. EC2/Batch backends don't
have a persistent filesystem; supporting them means rendering ``prep``
into a Dockerfile keyed on the same hash — a separate, larger change.
"""

from __future__ import annotations

import hashlib
import logging
import shlex
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from scripthut.config_schema import Stack
from scripthut.ssh.client import SSHClient

logger = logging.getLogger(__name__)


# Filename of the sentinel that marks a successful install.
READY_SENTINEL = ".ready"


class StackState(str, Enum):
    """Per-backend installation state for a stack at its current hash."""

    MISSING = "missing"          # No directory at all
    INSTALLING = "installing"    # Dir exists but no .ready (prior install crashed)
    READY = "ready"              # Dir + .ready sentinel both present


@dataclass
class StackStatus:
    """Snapshot of a stack on one backend."""

    name: str
    backend: str
    state: StackState
    hash: str
    path: str                              # Remote absolute path of the hash dir
    last_built: datetime | None = None     # mtime of the .ready sentinel
    size_bytes: int | None = None          # du -sb of the hash dir, when known
    error: str | None = None               # Set when state == INSTALLING due to a crash


def compute_stack_hash(stack: Stack) -> str:
    """Content-hash the stack definition.

    Includes: stack name, the prep script text, every (key, value) in
    ``inputs`` (sorted by key), and the contents of every file in
    ``input_files`` (in declaration order). Missing input files hash as
    the literal bytes ``<missing>`` so a typo is detectable from the
    hash flipping when the file appears.
    """
    h = hashlib.sha256()
    h.update(b"name=")
    h.update(stack.name.encode())
    h.update(b"\0prep=")
    h.update(stack.prep.encode())
    h.update(b"\0inputs=")
    for key in sorted(stack.inputs):
        h.update(f"{key}={stack.inputs[key]}".encode())
        h.update(b"\0")
    h.update(b"\0files=")
    for path in stack.input_files:
        h.update(str(path).encode())
        h.update(b"\0")
        try:
            h.update(path.expanduser().read_bytes())
        except FileNotFoundError:
            h.update(b"<missing>")
        except OSError as e:
            logger.warning(f"Reading stack input file {path}: {e}")
            h.update(f"<error:{e}>".encode())
        h.update(b"\0")
    return h.hexdigest()[:12]


class StackManager:
    """Inspects, installs, and removes stacks on SSH-based backends."""

    @staticmethod
    def hash_path(stack: Stack, hash_: str) -> str:
        """Return the canonical remote directory for a (stack, hash) pair.

        ``~`` is preserved in the returned string — the remote shell does
        the expansion, which is the same convention used elsewhere in
        scripthut (e.g. ``clone_dir``).
        """
        return f"{stack.cache_dir}/{stack.name}/{hash_}"

    async def check(
        self, stack: Stack, backend_name: str, ssh: SSHClient
    ) -> StackStatus:
        """Report the current state of ``stack`` on this backend."""
        h = compute_stack_hash(stack)
        path = self.hash_path(stack, h)

        # One round-trip: probe sentinel, dir, mtime, size.
        # ``stat -c %Y`` gives mtime as epoch seconds.
        ready = shlex.quote(f"{path}/{READY_SENTINEL}")
        dir_q = shlex.quote(path)
        cmd = (
            f"if [ -f {ready} ]; then "
            f"  echo READY; "
            f"  stat -c %Y {ready} 2>/dev/null || echo 0; "
            f"  du -sb {dir_q} 2>/dev/null | cut -f1 || echo 0; "
            f"elif [ -d {dir_q} ]; then "
            f"  echo INSTALLING; "
            f"else "
            f"  echo MISSING; "
            f"fi"
        )
        stdout, stderr, exit_code = await ssh.run_command(cmd, timeout=15)
        if exit_code != 0:
            logger.warning(f"stack check on {backend_name} failed: {stderr}")
            return StackStatus(
                name=stack.name,
                backend=backend_name,
                state=StackState.MISSING,
                hash=h,
                path=path,
                error=stderr.strip() or f"exit {exit_code}",
            )

        lines = stdout.strip().splitlines()
        if not lines:
            return StackStatus(
                name=stack.name, backend=backend_name,
                state=StackState.MISSING, hash=h, path=path,
            )

        first = lines[0].strip()
        if first == "MISSING":
            return StackStatus(
                name=stack.name, backend=backend_name,
                state=StackState.MISSING, hash=h, path=path,
            )
        if first == "INSTALLING":
            return StackStatus(
                name=stack.name, backend=backend_name,
                state=StackState.INSTALLING, hash=h, path=path,
                error="Cache directory exists but .ready sentinel is missing",
            )

        # READY
        last_built: datetime | None = None
        size_bytes: int | None = None
        if len(lines) >= 2:
            try:
                last_built = datetime.fromtimestamp(int(lines[1]), tz=timezone.utc)
            except (ValueError, OSError):
                pass
        if len(lines) >= 3:
            try:
                size_bytes = int(lines[2])
            except ValueError:
                pass
        return StackStatus(
            name=stack.name, backend=backend_name,
            state=StackState.READY, hash=h, path=path,
            last_built=last_built, size_bytes=size_bytes,
        )

    async def install(
        self,
        stack: Stack,
        backend_name: str,
        ssh: SSHClient,
        rebuild: bool = False,
    ) -> StackStatus:
        """Run ``prep`` if the stack isn't ready (or if ``rebuild=True``).

        Idempotent: a second call with ``rebuild=False`` after a successful
        install is a no-op. Reports the resulting state.
        """
        status = await self.check(stack, backend_name, ssh)
        if status.state == StackState.READY and not rebuild:
            logger.info(
                f"Stack '{stack.name}' on '{backend_name}' already ready "
                f"({status.hash})"
            )
            return status

        path = status.path
        dir_q = shlex.quote(path)
        ready_q = shlex.quote(f"{path}/{READY_SENTINEL}")

        # Wipe a half-built dir or a stale ready sentinel when rebuilding.
        if rebuild or status.state == StackState.INSTALLING:
            await ssh.run_command(f"rm -rf {dir_q}", timeout=60)

        await ssh.run_command(f"mkdir -p {dir_q}", timeout=15)

        prep_body = stack.prep.strip()
        if not prep_body:
            # Empty prep is a valid degenerate case (the stack is just
            # an init-only env layer that still wants a stable directory).
            await ssh.run_command(f"touch {ready_q}", timeout=15)
            logger.info(
                f"Stack '{stack.name}' on '{backend_name}': no prep, marked ready"
            )
            return await self.check(stack, backend_name, ssh)

        # Run prep with STACK_DIR pointing at the cache dir. We use
        # `bash -c` with the script body inlined via a here-doc-style
        # heredoc to avoid quoting hell. ``set -euo pipefail`` ensures
        # any failure aborts before we touch .ready.
        script = (
            f"set -euo pipefail\n"
            f"export STACK_DIR={dir_q}\n"
            f"cd {dir_q}\n"
            f"{prep_body}\n"
            f"touch {ready_q}\n"
        )
        # ``bash -s`` reads the script from stdin; we pipe it via run_command's
        # `input=` if supported, else fall back to printf | bash.
        try:
            stdout, stderr, exit_code = await ssh.run_command(
                f"bash -s <<'__SCRIPTHUT_PREP__'\n{script}\n__SCRIPTHUT_PREP__",
                timeout=3600,
            )
        except Exception as e:
            logger.error(f"Stack '{stack.name}' install on '{backend_name}': {e}")
            return await self.check(stack, backend_name, ssh)

        if exit_code != 0:
            logger.error(
                f"Stack '{stack.name}' prep failed on '{backend_name}' "
                f"(exit {exit_code}): {stderr.strip()[:400]}"
            )
            # Leave the dir in place (state=INSTALLING) so `check` reports
            # the half-built situation and `install --rebuild` can recover.
            status = await self.check(stack, backend_name, ssh)
            status.error = stderr.strip() or stdout.strip() or f"prep exit {exit_code}"
            return status

        logger.info(
            f"Stack '{stack.name}' installed on '{backend_name}' ({status.hash})"
        )
        return await self.check(stack, backend_name, ssh)

    async def delete(
        self, stack: Stack, backend_name: str, ssh: SSHClient
    ) -> None:
        """Remove every cached build of this stack on this backend.

        Removes ``<cache_dir>/<stack.name>/`` recursively — all hashes,
        not just the current one. A subsequent ``install`` will rebuild
        from scratch.
        """
        target = f"{stack.cache_dir}/{stack.name}"
        cmd = f"rm -rf {shlex.quote(target)}"
        _, stderr, exit_code = await ssh.run_command(cmd, timeout=60)
        if exit_code != 0:
            raise RuntimeError(
                f"Failed to delete stack '{stack.name}' on '{backend_name}': "
                f"{stderr.strip() or f'exit {exit_code}'}"
            )
        logger.info(f"Stack '{stack.name}' deleted on '{backend_name}'")
