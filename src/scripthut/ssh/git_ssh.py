"""Shared construction of ``GIT_SSH_COMMAND`` for git-over-SSH.

Two places shell out to the ``ssh`` binary: the local source cache
(:mod:`scripthut.sources.git`) and the clones scripthut drives on a
backend (:mod:`scripthut.runs.manager`). They need the same answer to
the same two questions — which host-key policy this particular ssh
understands, and where it should keep the keys it learns — so both go
through here.

The host-key policy is version-dependent. ``accept-new`` is the option
we want: it trusts a host nobody has seen before but refuses one whose
key has changed, which is the useful half of strict checking without a
prompt no batch job can answer. It landed in OpenSSH 7.6 (October 2017),
and older clients do not degrade gracefully — they abort before
connecting with::

    command-line line 0: unsupported option "accept-new".

leaving git to report only ``fatal: Could not read from remote
repository``. HPC login nodes are routinely old enough for this to bite
(RHEL/CentOS 7 ships OpenSSH 7.4), so the policy is chosen from the
version of the ssh that will actually run the clone, not from ours.
"""

from __future__ import annotations

import functools
import logging
import re
import shlex
import subprocess

logger = logging.getLogger(__name__)

# `accept-new` was added in OpenSSH 7.6; anything older rejects the value.
ACCEPT_NEW_MIN_VERSION = (7, 6)

_VERSION_RE = re.compile(r"OpenSSH[_-](\d+)\.(\d+)")


def parse_openssh_version(banner: str) -> tuple[int, int] | None:
    """Extract ``(major, minor)`` from an ``ssh -V`` banner.

    ``ssh -V`` writes to stderr and its exact shape varies by vendor
    (``OpenSSH_9.6p1, LibreSSL 3.3.6``, ``OpenSSH_7.4p1, OpenSSL
    1.0.2k-fips``). Returns ``None`` when nothing recognisable is there,
    which callers treat as "assume old".
    """
    match = _VERSION_RE.search(banner)
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2))


@functools.lru_cache(maxsize=1)
def local_openssh_version() -> tuple[int, int] | None:
    """Version of the ``ssh`` on this machine's PATH, probed once.

    Cached for the process: the binary will not change under a running
    server, and the local source sync calls this on every git command.
    """
    try:
        # `ssh -V` writes the banner to stderr, not stdout.
        proc = subprocess.run(
            ["ssh", "-V"], capture_output=True, text=True, timeout=10, check=False
        )
    except (OSError, subprocess.SubprocessError) as e:
        logger.warning(f"Could not probe local ssh version: {e}")
        return None
    version = parse_openssh_version(f"{proc.stderr}\n{proc.stdout}")
    if version is None:
        logger.warning(
            "Unrecognised local ssh version; assuming it predates "
            "StrictHostKeyChecking=accept-new"
        )
    return version


def host_key_checking_option(version: tuple[int, int] | None) -> str:
    """The ``StrictHostKeyChecking`` value this ssh will accept.

    Falls back to ``no`` for unknown versions: guessing ``accept-new``
    wrong breaks every clone, while guessing ``no`` wrong only means we
    keep trusting a host whose key changed.
    """
    if version is not None and version >= ACCEPT_NEW_MIN_VERSION:
        return "accept-new"
    return "no"


def build_git_ssh_command(
    *,
    key_path: str | None = None,
    known_hosts: str | None = None,
    openssh_version: tuple[int, int] | None = None,
    quote_paths: bool = True,
) -> str:
    """Build the ``ssh`` invocation for ``GIT_SSH_COMMAND``.

    Args:
        key_path: Identity file to use, or None for the agent/defaults.
        known_hosts: Where to persist learned host keys. None leaves
            ssh's own default (``~/.ssh/known_hosts``), which is the
            wrong choice on a cluster whose home may be read-only or
            over quota.
        openssh_version: Version of the ssh that will run this command,
            as returned by :func:`parse_openssh_version`.
        quote_paths: Shell-quote the paths. Callers that need a literal
            ``~`` to survive into the remote shell pass False — quoting
            it would create a directory named ``~``.
    """
    quote = shlex.quote if quote_paths else (lambda value: value)

    parts = ["ssh"]
    if key_path is not None:
        # IdentitiesOnly stops ssh from offering every agent key first and
        # tripping the server's MaxAuthTries before it reaches this one.
        parts += ["-i", quote(key_path), "-o", "IdentitiesOnly=yes"]
    parts += [
        "-o", "BatchMode=yes",
        "-o", "PasswordAuthentication=no",
        "-o", f"StrictHostKeyChecking={host_key_checking_option(openssh_version)}",
    ]
    if known_hosts is not None:
        parts += ["-o", f"UserKnownHostsFile={quote(known_hosts)}"]
    return " ".join(parts)
