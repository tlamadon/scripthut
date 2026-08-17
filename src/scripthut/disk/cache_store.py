"""Size and reachability of the shared object-store cache.

The task cache lives in an S3-compatible store split into two prefixes —
``ac/`` (one small JSON manifest per cached action) and ``cas/`` (the
content-addressed output tarballs). See :mod:`scripthut.runs.cache`.

Unlike the rest of the disk page this is *not* per-backend: the store is
shared global config, so every backend sees the same bucket. A backend is
only the vantage point, because the store is reachable cluster-side by
design — hashing and transfer run there over SSH, so the scripthut host
may have no credentials at all.

The listing is reduced on the *remote* side (``--summarize | tail``, or
``rclone size``) so a hundred-thousand-object cache sends back three
lines instead of ten megabytes of object names.
"""

from __future__ import annotations

import logging
import re
import shlex
from dataclasses import dataclass, field
from datetime import datetime, timezone

from scripthut.config_schema import CacheConfig
from scripthut.ssh.client import SSHClient

logger = logging.getLogger(__name__)

# Prefixes of the store, in the order the UI shows them.
AC_PREFIX = "ac"
CAS_PREFIX = "cas"

_SCAN_TIMEOUT = 120

# ``aws s3 ls --summarize`` tail:  "Total Objects: 42" / "   Total Size: 1234"
_AWS_OBJECTS = re.compile(r"Total Objects:\s*(\d+)")
_AWS_SIZE = re.compile(r"Total Size:\s*(\d+)")
# ``rclone size``:  "Total objects: 42" / "Total size: 1.2 KiB (1234 Byte)"
_RCLONE_OBJECTS = re.compile(r"Total objects:\s*([\d.]+)")
_RCLONE_SIZE = re.compile(r"\((\d+) Byte")


@dataclass
class CachePrefixStat:
    """One prefix of the store: how many objects and how much space."""

    name: str
    uri: str
    objects: int | None = None
    size_bytes: int | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.objects is not None


@dataclass
class CacheStoreStatus:
    """A point-in-time summary of the object-store cache."""

    enabled: bool = False
    store: str | None = None
    tool: str = "aws"
    backend: str | None = None  # vantage point the listing ran from
    scanned_at: datetime | None = None
    duration_ms: int = 0
    prefixes: list[CachePrefixStat] = field(default_factory=list)
    error: str | None = None

    @property
    def reachable(self) -> bool:
        return self.error is None and any(p.ok for p in self.prefixes)

    @property
    def total_bytes(self) -> int:
        return sum(p.size_bytes or 0 for p in self.prefixes)

    @property
    def total_objects(self) -> int:
        return sum(p.objects or 0 for p in self.prefixes)

    def prefix(self, name: str) -> CachePrefixStat | None:
        return next((p for p in self.prefixes if p.name == name), None)

    @property
    def actions(self) -> int | None:
        """Cached actions — one ``ac/`` manifest each."""
        p = self.prefix(AC_PREFIX)
        return p.objects if p else None

    @property
    def blobs(self) -> int | None:
        """Distinct output artifacts in the CAS."""
        p = self.prefix(CAS_PREFIX)
        return p.objects if p else None

    @property
    def blob_bytes(self) -> int:
        p = self.prefix(CAS_PREFIX)
        return (p.size_bytes or 0) if p else 0

    @property
    def mean_blob_bytes(self) -> int | None:
        """Average artifact size — a sense of what a cache hit is worth."""
        if not self.blobs:
            return None
        return self.blob_bytes // self.blobs


def _list_command(tool: str, uri: str) -> str:
    """Remote command summarizing ``uri``, reduced cluster-side.

    Both forms print only a handful of lines: ``aws`` lists every object to
    compute its summary, so the ``tail`` keeps that listing from crossing
    the SSH channel; ``rclone size`` summarizes natively.
    """
    q = shlex.quote(uri)
    if tool == "rclone":
        return f"rclone size {q}"
    return f"aws s3 ls --recursive --summarize {q} | tail -n 3"


def _parse(tool: str, stdout: str) -> tuple[int | None, int | None]:
    """Return ``(objects, bytes)`` parsed from the summary output."""
    if tool == "rclone":
        m_obj, m_size = _RCLONE_OBJECTS.search(stdout), _RCLONE_SIZE.search(stdout)
        objects = int(float(m_obj.group(1))) if m_obj else None
        # rclone prints a human size plus an exact byte count in parens;
        # take the parenthesised one so nothing is lost to rounding.
        return objects, int(m_size.group(1)) if m_size else None
    m_obj, m_size = _AWS_OBJECTS.search(stdout), _AWS_SIZE.search(stdout)
    return (
        int(m_obj.group(1)) if m_obj else None,
        int(m_size.group(1)) if m_size else None,
    )


async def scan_cache_store(
    cache: CacheConfig | None,
    *,
    backend_name: str,
    ssh: SSHClient,
) -> CacheStoreStatus:
    """Summarize the ``ac/`` and ``cas/`` prefixes of the configured store.

    Never raises: a store that is unconfigured, unreachable, or missing its
    CLI comes back as a status carrying the reason, because this feeds a
    page that must render either way.
    """
    started = datetime.now(timezone.utc)

    if cache is None or not cache.enabled or not cache.store:
        return CacheStoreStatus(
            enabled=bool(cache and cache.enabled),
            store=cache.store if cache else None,
            tool=cache.tool if cache else "aws",
            scanned_at=started,
        )

    base = cache.store.rstrip("/")
    status = CacheStoreStatus(
        enabled=True,
        store=base,
        tool=cache.tool,
        backend=backend_name,
        scanned_at=started,
    )

    for name in (AC_PREFIX, CAS_PREFIX):
        # A trailing slash keeps `aws s3 ls` from prefix-matching a
        # sibling whose name merely starts with "ac".
        uri = f"{base}/{name}/"
        stat = CachePrefixStat(name=name, uri=uri)
        try:
            stdout, stderr, code = await ssh.run_command(
                _list_command(cache.tool, uri), timeout=_SCAN_TIMEOUT,
            )
        except Exception as e:  # noqa: BLE001 — surfaced, not raised
            stat.error = str(e)
            status.prefixes.append(stat)
            continue

        if code != 0:
            # An empty prefix is not an error: neither tool has anything to
            # list before the first cache write, and aws exits 1 for it.
            detail = (stderr or stdout).strip().splitlines()
            stat.error = detail[-1] if detail else f"exit {code}"
            if not detail:
                stat.objects, stat.size_bytes, stat.error = 0, 0, None
        else:
            objects, size = _parse(cache.tool, stdout)
            if objects is None:
                stat.objects, stat.size_bytes = 0, 0
            else:
                stat.objects, stat.size_bytes = objects, size or 0
        status.prefixes.append(stat)

    status.duration_ms = int(
        (datetime.now(timezone.utc) - started).total_seconds() * 1000
    )
    if all(p.error for p in status.prefixes):
        status.error = status.prefixes[0].error
    logger.info(
        f"cache store scan via '{backend_name}': "
        f"{status.total_objects} objects, {status.total_bytes} bytes"
    )
    return status
