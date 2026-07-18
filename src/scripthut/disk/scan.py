"""Remote scan script builder and output parser (pure functions).

The scan runs as ONE SSH round trip per backend: a bash script that
walks each root of interest and prints tab-separated lines. Fields
never contain tabs (paths with embedded tabs are out of scope, same
stance as :func:`scripthut.backends.utils.shell_quote_path`), so the
parser splits on ``\\t`` and paths keep embedded spaces.

Line protocol emitted by the script:

    HOME\\t<abs home dir>
    DF\\t<total KiB>\\t<avail KiB>          (df of the primary clone_dir)
    SECTION\\t<section>\\t<abs root>        (root exists, entries follow)
    MISSING\\t<section>\\t<root>            (root doesn't exist — normal)
    ENTRY\\t<section>\\t<path>\\t<mtime>\\t<KiB|->        (clones/logs)
    ENTRY\\tstacks\\t<path>\\t<mtime>\\t<KiB|->\\t<0|1>   (1 = .ready present)

``mtime`` is epoch seconds (0 = unknown); size ``-`` means du failed or
timed out for that entry.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from scripthut.backends.utils import shell_quote_path
from scripthut.disk.models import DiskEntry, DiskEntryKind, ScanSpec

if TYPE_CHECKING:
    from scripthut.config_schema import ScriptHutConfig

CLONE_HASH_RE = re.compile(r"^[0-9a-f]{12}$")
AGENT_DIR_RE = re.compile(r"^agent-[0-9a-f]{8}$")

_HEREDOC_TAG = "__SCRIPTHUT_DISKSCAN__"

# Helper functions shared by the per-root scan calls. stat -c is GNU,
# stat -f the BSD fallback (lets the scan work against a macOS "remote"
# in local end-to-end testing); `timeout` is optional for the same
# reason. A missing timeout binary just means no per-entry du bound.
_SCRIPT_PRELUDE = """\
printf 'HOME\\t%s\\n' "$HOME"
{{ df -Pk {df_path} 2>/dev/null || df -Pk "$HOME" 2>/dev/null; }} \\
  | awk 'END {{ if (NF >= 5) printf "DF\\t%s\\t%s\\n", $(NF-4), $(NF-2) }}'

_sh_mtime() {{ stat -c %Y "$1" 2>/dev/null || stat -f %m "$1" 2>/dev/null || echo 0; }}
_sh_fsize() {{ stat -c %s "$1" 2>/dev/null || stat -f %z "$1" 2>/dev/null || echo 0; }}
if command -v timeout >/dev/null 2>&1; then
  _sh_du() {{ timeout {du_timeout} du -sk "$1" 2>/dev/null | cut -f1; }}
else
  _sh_du() {{ du -sk "$1" 2>/dev/null | cut -f1; }}
fi

scan_dir() {{
  local section="$1" d="$2"
  if [ ! -d "$d" ]; then printf 'MISSING\\t%s\\t%s\\n' "$section" "$d"; return; fi
  printf 'SECTION\\t%s\\t%s\\n' "$section" "$d"
  local e m s
  for e in "$d"/*; do
    [ -e "$e" ] || continue
    m=$(_sh_mtime "$e")
    if [ -d "$e" ]; then
      s=$(_sh_du "$e")
    else
      s=$(( ( $(_sh_fsize "$e") + 1023 ) / 1024 ))
    fi
    printf 'ENTRY\\t%s\\t%s\\t%s\\t%s\\n' "$section" "$e" "$m" "${{s:--}}"
  done
}}

scan_stacks() {{
  local d="$1"
  if [ ! -d "$d" ]; then printf 'MISSING\\tstacks\\t%s\\n' "$d"; return; fi
  printf 'SECTION\\tstacks\\t%s\\n' "$d"
  local n h m s r
  for n in "$d"/*/; do
    [ -d "$n" ] || continue
    for h in "$n"*/; do
      [ -d "$h" ] || continue
      h="${{h%/}}"
      m=$(_sh_mtime "$h")
      s=$(_sh_du "$h")
      if [ -f "$h/.ready" ]; then r=1; else r=0; fi
      printf 'ENTRY\\tstacks\\t%s\\t%s\\t%s\\t%s\\n' "$h" "$m" "${{s:--}}" "$r"
    done
  done
}}
"""


def build_scan_spec(
    config: ScriptHutConfig, backend_name: str, clone_dir: str
) -> ScanSpec:
    """Assemble the roots to scan for one backend.

    ``clone_dir`` is the backend's configured clone dir (from
    BackendState on the server, backend config in CLI local mode); git
    sources carry their own independent clone_dir, so the union of all
    of them is scanned. Stack cache dirs are filtered to stacks
    available on this backend (empty ``backends`` = every SSH backend).
    """
    from scripthut.config_schema import GitSourceConfig

    clone_dirs: list[str] = []
    for d in [clone_dir] + [
        s.clone_dir for s in config.sources if isinstance(s, GitSourceConfig)
    ]:
        d = d.rstrip("/")
        if d and d not in clone_dirs:
            clone_dirs.append(d)

    stack_dirs: list[str] = []
    for stack in config.stacks:
        if stack.backends and backend_name not in stack.backends:
            continue
        d = stack.cache_dir.rstrip("/")
        if d and d not in stack_dirs:
            stack_dirs.append(d)

    return ScanSpec(backend=backend_name, clone_dirs=clone_dirs, stack_dirs=stack_dirs)


def build_scan_script(spec: ScanSpec) -> str:
    """Produce the full remote command (heredoc-wrapped bash script)."""
    df_path = shell_quote_path(spec.clone_dirs[0]) if spec.clone_dirs else '"$HOME"'
    lines = [_SCRIPT_PRELUDE.format(df_path=df_path, du_timeout=spec.du_entry_timeout)]
    for d in spec.clone_dirs:
        lines.append(f"scan_dir clones {shell_quote_path(d)}")
    for d in spec.stack_dirs:
        lines.append(f"scan_stacks {shell_quote_path(d)}")
    for d in spec.log_roots:
        lines.append(f"scan_dir logs {shell_quote_path(d)}")
    body = "\n".join(lines)
    return f"bash -s <<'{_HEREDOC_TAG}'\n{body}\n{_HEREDOC_TAG}"


@dataclass
class RawEntry:
    """Parser output for one ENTRY line, pre-classification."""

    section: str  # "clones" | "stacks" | "logs"
    path: str
    mtime: datetime | None
    size_bytes: int | None
    ready: bool | None  # stacks only


def parse_scan_output(
    stdout: str,
) -> tuple[str | None, list[RawEntry], tuple[int, int] | None, list[str]]:
    """Parse scan stdout into ``(home, entries, (total, avail) bytes, errors)``.

    Tolerant by design: unknown tags and malformed lines are collected
    into ``errors`` rather than raising, so one bad line can't sink a
    whole scan.
    """
    home: str | None = None
    raw: list[RawEntry] = []
    df: tuple[int, int] | None = None
    errors: list[str] = []

    for line in stdout.splitlines():
        if not line.strip():
            continue
        fields = line.split("\t")
        tag = fields[0]
        if tag == "HOME" and len(fields) == 2:
            home = fields[1] or None
        elif tag == "DF" and len(fields) == 3:
            try:
                df = (int(fields[1]) * 1024, int(fields[2]) * 1024)
            except ValueError:
                errors.append(f"unparseable DF line: {line!r}")
        elif tag in ("SECTION", "MISSING") and len(fields) == 3:
            pass  # informational; a missing root is normal, not an error
        elif tag == "ENTRY":
            entry = _parse_entry(fields)
            if entry is None:
                errors.append(f"unparseable ENTRY line: {line!r}")
            else:
                raw.append(entry)
        else:
            errors.append(f"unparsed scan line: {line!r}")

    return home, raw, df, errors


def _parse_entry(fields: list[str]) -> RawEntry | None:
    section = fields[1] if len(fields) > 1 else ""
    if section == "stacks" and len(fields) == 6:
        path, mtime_s, size_s, ready_s = fields[2:6]
        ready: bool | None = ready_s == "1"
    elif section in ("clones", "logs") and len(fields) == 5:
        path, mtime_s, size_s = fields[2:5]
        ready = None
    else:
        return None

    mtime: datetime | None = None
    try:
        ts = int(mtime_s)
        if ts > 0:
            mtime = datetime.fromtimestamp(ts, tz=timezone.utc)
    except (ValueError, OSError):
        pass

    size: int | None = None
    if size_s != "-":
        try:
            size = int(size_s) * 1024  # du -sk reports KiB
        except ValueError:
            pass

    return RawEntry(
        section=section, path=path.rstrip("/") or "/", mtime=mtime,
        size_bytes=size, ready=ready,
    )


def raw_to_entries(raw: list[RawEntry]) -> list[DiskEntry]:
    """Assign a :class:`DiskEntryKind` to each raw entry by naming pattern."""
    entries: list[DiskEntry] = []
    for r in raw:
        parts = r.path.split("/")
        basename = parts[-1]
        detail: str | None = None
        if r.section == "stacks":
            kind = DiskEntryKind.STACK
            # <cache_dir>/<name>/<hash> — last two segments identify it
            detail = "/".join(parts[-2:])
            if r.ready is False:
                detail += " (half-built)"
        elif r.section == "logs":
            kind = DiskEntryKind.LOG
            detail = basename  # workflow name
        elif CLONE_HASH_RE.match(basename):
            kind = DiskEntryKind.CLONE
        elif AGENT_DIR_RE.match(basename):
            kind = DiskEntryKind.AGENT
        else:
            kind = DiskEntryKind.OTHER
        entries.append(
            DiskEntry(
                path=r.path, kind=kind, size_bytes=r.size_bytes,
                mtime=r.mtime, detail=detail, ready=r.ready,
            )
        )
    return entries
