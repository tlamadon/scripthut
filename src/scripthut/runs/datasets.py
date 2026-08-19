"""Content-addressed dataset staging.

A *dataset* is a directory on the **daemon host** that a workflow needs on a
backend's filesystem before its tasks run. ScriptHut identifies it by a
manifest hash over the local file list, so the remote copy lives at
``<root>/<name>/<hash12>`` — the same content-addressed shape
``StackManager.hash_path`` uses for stacks and ``_clone_git_repo`` uses for
commits. Reuse is then a plain ``test -d``: if the directory is there it is,
by construction, the tree that hashed to that name.

The hash covers relative paths and sizes only. No file contents are read, so
a multi-gigabyte tree hashes in milliseconds — cheap enough to run on every
submission. That is enough to catch the failure this design exists to prevent
(a truncated or partial copy silently reused as if complete). It deliberately
does not catch bit rot or an in-place edit that preserves byte count. mtime is
excluded because SFTP and rsync do not reliably preserve it, and a stray
``touch`` must not invalidate a good copy.

Layout and naming decisions live here and nowhere else: change the directory
shape, the hash width, the staging suffix, or the injected environment
variable names by editing this module.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scripthut.backends.utils import shell_quote_path

if TYPE_CHECKING:
    from scripthut.config_schema import DatasetConfig
    from scripthut.ssh.client import SSHClient

logger = logging.getLogger(__name__)

# Width of the hash that names a dataset directory. 48 bits, matching the
# 12-char convention _clone_git_repo already uses for commit hashes.
MANIFEST_HASH_LEN = 12

DATASET_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")

# Names a staged dataset directory; mirrors ``CLONE_HASH_RE`` for commits.
DATASET_HASH_RE = re.compile(r"^[0-9a-f]{12}$")

# A remote root is absolute, or ``~``-relative and expanded against the
# probed remote home before use. Either way it must be free of shell
# metacharacters: ``shell_quote_path`` wraps paths in *double* quotes, so an
# unvalidated ``$`` would expand on the cluster.
REMOTE_ROOT_RE = re.compile(r"^(?:/|~/)[A-Za-z0-9_./-]+$")

# Deliberately not ``SCRIPTHUT_``-prefixed: the env resolver drops any rule
# setting that namespace, and the cache strips it from the key. A dataset
# destination is set by a rule and must feed the key, so it needs both.
DATA_DIR_VAR = "DATA_DIR"
DATA_VAR_PREFIX = "DATA_"

# In-flight transfers land beside the final path and are moved onto it only
# after verification, so a crash never leaves something that looks complete.
STAGING_INFIX = ".staging-"


class DatasetError(ValueError):
    """A dataset is misconfigured, missing, or could not be staged.

    Subclasses ``ValueError`` so the existing ``create_run_from_source``
    error path (and its HTTP 422 mapping) treats it like any other bad
    workflow input without special-casing.
    """


def dataset_path(root: str, name: str, hash12: str) -> str:
    """The one place the ``<root>/<name>/<hash12>`` layout is spelled out."""
    return f"{root.rstrip('/')}/{name}/{hash12}"


def staging_path(dest: str, run_id: str) -> str:
    """Per-run staging directory beside ``dest``.

    Keyed by run id so two runs staging the same dataset cannot interleave
    their writes.
    """
    return f"{dest}{STAGING_INFIX}{run_id}"


def staging_glob(dest: str) -> str:
    """Shell glob matching every staging directory for ``dest``.

    Used to clear leftovers from a daemon that died mid-transfer.
    """
    return f"{dest}{STAGING_INFIX}*"


def data_env_var(name: str) -> str:
    """Environment variable exposing one dataset's destination.

    ``acquihired-raw`` -> ``DATA_ACQUIHIRED_RAW``. Injected last so it wins
    over a user rule of the same name; the generic ``DATA_DIR`` (set only
    when a workflow uses exactly one dataset) is injected first instead, as
    a default the workflow may override.
    """
    return DATA_VAR_PREFIX + re.sub(r"[^A-Za-z0-9]", "_", name).upper()


def validate_remote_root(root: str, *, origin: str) -> str:
    """Check a candidate root is absolute and shell-safe; return it normalized.

    ``origin`` names where the value came from so the error tells the user
    which knob to fix.
    """
    candidate = root.strip().rstrip("/")
    if not candidate:
        raise DatasetError(f"Dataset root from {origin} is empty")
    if not REMOTE_ROOT_RE.match(candidate):
        raise DatasetError(
            f"Dataset root from {origin} is not usable: {root!r}. "
            "It must be an absolute path (or start with '~/') containing only "
            "letters, digits, '_', '.', '-' and '/'. Shell variables like "
            "$USER are not expanded — write the path out."
        )
    return candidate


@dataclass(frozen=True)
class DatasetManifest:
    """The local file list that names a dataset.

    ``entries`` is ``(relative POSIX path, size in bytes)`` sorted by path.
    Sorting is plain code-point order, which for UTF-8 is byte order, so it
    matches ``LC_ALL=C sort`` on the cluster — that equality is what lets
    :meth:`render` be diffed against a remote ``find`` walk.
    """

    entries: tuple[tuple[str, int], ...]
    digest: str

    @property
    def short(self) -> str:
        """The 12-char form that names the remote directory."""
        return self.digest[:MANIFEST_HASH_LEN]

    @property
    def file_count(self) -> int:
        return len(self.entries)

    @property
    def total_bytes(self) -> int:
        return sum(size for _, size in self.entries)

    def render(self) -> str:
        """Canonical ``<path>\\t<size>`` text, one line per file.

        Byte-identical to what ``find <dir> -type f -printf '%P\\t%s\\n' |
        LC_ALL=C sort`` produces for a faithful copy, so verifying a staged
        tree is a string comparison.
        """
        return "".join(f"{path}\t{size}\n" for path, size in self.entries)


def parse_remote_listing(stdout: str) -> tuple[tuple[str, int], ...]:
    """Parse a remote ``find -printf '%P\\t%s\\n'`` walk into manifest entries.

    Tolerates the trailing newline and blank lines; anything that is not
    ``<path>\\t<int>`` is a protocol violation and raises, because silently
    dropping a line would weaken the very check this feeds.
    """
    entries: list[tuple[str, int]] = []
    for line in stdout.splitlines():
        if not line.strip():
            continue
        path, sep, raw_size = line.rpartition("\t")
        if not sep or not path:
            raise DatasetError(f"Unparseable remote listing line: {line!r}")
        try:
            entries.append((path, int(raw_size)))
        except ValueError:
            raise DatasetError(f"Unparseable size in remote listing: {line!r}")
    entries.sort(key=lambda e: e[0])
    return tuple(entries)


def diff_manifests(
    expected: DatasetManifest,
    actual: tuple[tuple[str, int], ...],
    *,
    limit: int = 10,
) -> list[str]:
    """Human-readable differences between a manifest and a staged tree.

    Empty list means the trees agree. Used to decide whether a staged
    directory may be moved onto its final path.
    """
    want = dict(expected.entries)
    have = dict(actual)
    problems: list[str] = []

    for path in sorted(want.keys() - have.keys()):
        problems.append(f"missing: {path}")
    for path in sorted(have.keys() - want.keys()):
        problems.append(f"unexpected: {path}")
    for path in sorted(want.keys() & have.keys()):
        if want[path] != have[path]:
            problems.append(
                f"size mismatch: {path} (expected {want[path]}, got {have[path]})"
            )

    if len(problems) > limit:
        hidden = len(problems) - limit
        problems = problems[:limit] + [f"... and {hidden} more"]
    return problems


def build_manifest(local_path: Path) -> DatasetManifest:
    """Hash the file list of ``local_path`` on the daemon host.

    Regular files only. Directory symlinks are not followed, and any symlink
    is rejected rather than guessed at: a link out of the tree cannot be
    copied faithfully, and a link inside it would make the manifest disagree
    with what SFTP actually transfers. Failing loudly beats a hash that does
    not describe the bytes that land on the cluster.
    """
    base = local_path.expanduser()
    if not base.exists():
        raise DatasetError(f"Dataset path does not exist: {base}")
    if not base.is_dir():
        raise DatasetError(f"Dataset path is not a directory: {base}")

    resolved_base = base.resolve()
    entries: list[tuple[str, int]] = []

    for dirpath, dirnames, filenames in os.walk(base, followlinks=False):
        here = Path(dirpath)
        for dirname in sorted(dirnames):
            if (here / dirname).is_symlink():
                raise DatasetError(
                    f"Dataset '{base}' contains a directory symlink "
                    f"({(here / dirname).relative_to(base)}). Symlinked "
                    "directories are not staged — replace it with a real "
                    "directory or move it out of the dataset."
                )
        for filename in sorted(filenames):
            entry = here / filename
            rel = entry.relative_to(base).as_posix()
            if entry.is_symlink():
                target = entry.resolve()
                if not target.exists():
                    raise DatasetError(
                        f"Dataset '{base}' contains a broken symlink: {rel}"
                    )
                if not target.is_relative_to(resolved_base):
                    raise DatasetError(
                        f"Dataset '{base}' contains a symlink pointing outside "
                        f"the dataset: {rel} -> {target}"
                    )
            if not entry.is_file():
                # Sockets, FIFOs and device nodes cannot be staged; they are
                # also never research data, so skipping is safe and quiet.
                continue
            if "\t" in rel or "\n" in rel:
                raise DatasetError(
                    f"Dataset '{base}' contains a file whose name has a tab or "
                    f"newline ({rel!r}); such names cannot be verified remotely."
                )
            entries.append((rel, entry.stat().st_size))

    if not entries:
        raise DatasetError(f"Dataset path is empty: {base}")

    entries.sort(key=lambda e: e[0])
    manifest = DatasetManifest(
        entries=tuple(entries),
        digest=hashlib.sha256(
            "".join(f"{path}\t{size}\n" for path, size in entries).encode()
        ).hexdigest(),
    )
    logger.debug(
        "Dataset manifest for %s: %d files, %d bytes, hash %s",
        base, manifest.file_count, manifest.total_bytes, manifest.short,
    )
    return manifest


# ---------------------------------------------------------------------------
# Destination resolution
# ---------------------------------------------------------------------------

# Used when a backend declares no ``dataset_dir``. Mirrors ``clone_dir``'s
# ``~/scripthut-repos``: scripthut has always defaulted backend-side storage
# to home, and a dataset is no different. Large data belongs on scratch, but
# that is a per-cluster fact the backend config states explicitly.
DEFAULT_DATASET_DIR = "~/scripthut-data"

# Only ``$HOME`` is needed now that the root comes from config rather than
# from the cluster's environment. A plain exec channel is enough — no login
# shell, because nothing here depends on profile scripts.
_PROBE_COMMAND = 'printf "HOME\\t%s\\n" "$HOME"'


@dataclass(frozen=True)
class BackendPaths:
    """Cluster facts probed once per run creation."""

    home: str | None = None


async def probe_backend_paths(ssh: SSHClient) -> BackendPaths:
    """Read ``$HOME`` from the backend, to expand a ``~``-relative root."""
    stdout, stderr, code = await ssh.run_command(_PROBE_COMMAND, timeout=30)
    if code != 0:
        logger.warning(f"Dataset path probe failed (exit {code}): {stderr.strip()}")
        return BackendPaths()

    home: str | None = None
    for line in stdout.splitlines():
        key, sep, value = line.partition("\t")
        if sep and key == "HOME" and value.strip():
            home = value.strip()
    return BackendPaths(home=home)


def backend_dataset_dir(backend_cfg: Any) -> str:
    """The backend's configured dataset root, or the default."""
    return (getattr(backend_cfg, "dataset_dir", None) or DEFAULT_DATASET_DIR).strip()


def reject_unsafe_root(
    root: str,
    *,
    origin: str,
    home: str | None,
    clone_dirs: Sequence[str] = (),
) -> None:
    """Refuse roots that would put a dataset somewhere it must never go.

    A *subdirectory* of home is fine and is in fact the default, matching
    ``clone_dir``. What stays forbidden is the home directory itself — which
    would scatter dataset directories across the user's top level — and
    anything inside a clone dir, because code and data must stay separate.
    """
    normalized_home = (home or "").rstrip("/")
    if normalized_home and root.rstrip("/") == normalized_home:
        raise DatasetError(
            f"Dataset root from {origin} is the remote home directory itself "
            f"({root}). Use a subdirectory such as '~/scripthut-data', or "
            "point the backend's 'dataset_dir' at scratch."
        )
    for clone_dir in clone_dirs:
        normalized = _normalize_remote(clone_dir, home)
        if normalized and _is_at_or_under(root, normalized):
            raise DatasetError(
                f"Dataset root from {origin} is inside the clone directory "
                f"({root}). Code and data must stay separate."
            )


def _normalize_remote(path: str, home: str | None) -> str:
    """Resolve a leading ``~`` against the remote home; strip trailing ``/``."""
    p = (path or "").strip()
    if home and (p == "~" or p.startswith("~/")):
        p = home.rstrip("/") + p[1:]
    return p.rstrip("/")


def _is_at_or_under(path: str, parent: str) -> bool:
    p = path.rstrip("/")
    parent = parent.rstrip("/")
    return bool(parent) and (p == parent or p.startswith(parent + "/"))


def resolve_root(
    dataset: DatasetConfig,
    *,
    backend_name: str,
    backend_cfg: Any,
    paths: BackendPaths,
    clone_dirs: Sequence[str] = (),
) -> str:
    """Find the parent directory to stage into. Always answers.

    Two layers only: the dataset's own ``root`` overrides, otherwise the
    backend's ``dataset_dir`` (itself defaulting to ``~/scripthut-data``). The
    root is config, never inferred from the cluster's environment, so the
    destination is predictable from the YAML alone.

    A ``~``-relative root is expanded here against the probed home, so the
    returned path is always absolute. That matters: it ends up in ``DATA_DIR``,
    and ``export DATA_DIR="~/x"`` would not expand the tilde.
    """
    if dataset.root:
        origin = f"dataset '{dataset.name}' root"
        raw = dataset.root
    else:
        origin = f"backend '{backend_name}' dataset_dir"
        raw = backend_dataset_dir(backend_cfg)

    root = validate_remote_root(raw, origin=origin)
    root = _expand_home(root, paths.home, origin=origin)
    reject_unsafe_root(
        root, origin=origin, home=paths.home, clone_dirs=clone_dirs,
    )
    return root


def _expand_home(root: str, home: str | None, *, origin: str) -> str:
    """Turn a ``~``-relative root into an absolute one."""
    if not root.startswith("~"):
        return root
    if not home:
        raise DatasetError(
            f"Dataset root from {origin} is '{root}', but scripthut could not "
            "read $HOME from the backend to expand it. Set an absolute path."
        )
    return home.rstrip("/") + root[1:]


# ---------------------------------------------------------------------------
# Presence
# ---------------------------------------------------------------------------

_PRESENT_MARKER = "__SCRIPTHUT_PRESENT__"
_SIBLINGS_MARKER = "__SCRIPTHUT_SIBLINGS__"


@dataclass(frozen=True)
class DatasetPlan:
    """What staging this dataset for one run would involve."""

    name: str
    local_path: Path
    manifest: DatasetManifest
    dest: str
    reused: bool
    # Other hash directories under ``<root>/<name>/`` — superseded copies.
    siblings: tuple[str, ...] = ()
    timeout: int = 86400

    @property
    def must_stage(self) -> bool:
        return not self.reused

    @property
    def hash(self) -> str:
        return self.manifest.short


async def probe_presence(
    ssh: SSHClient, dest: str, *, hash12: str
) -> tuple[bool, tuple[str, ...]]:
    """Is ``dest`` already there, and what other hashes sit beside it?

    One round trip. The sibling listing is advisory — it feeds a warning about
    scratch accumulating superseded copies and never affects the decision.
    """
    parent = dest.rsplit("/", 1)[0]
    cmd = (
        f"if [ -d {shell_quote_path(dest)} ]; then printf '%s\\n' "
        f"{_PRESENT_MARKER}; fi; "
        f"printf '%s\\n' {_SIBLINGS_MARKER}; "
        f"ls -1 {shell_quote_path(parent)} 2>/dev/null || true"
    )
    stdout, stderr, code = await ssh.run_command(cmd, timeout=30)
    if code != 0:
        raise DatasetError(
            f"Could not check for an existing copy at {dest}: {stderr.strip()}"
        )

    before, _, after = stdout.partition(_SIBLINGS_MARKER)
    present = _PRESENT_MARKER in before
    siblings = tuple(
        sorted(
            line.strip()
            for line in after.splitlines()
            if line.strip()
            and line.strip() != hash12
            and DATASET_HASH_RE.match(line.strip())
        )
    )
    return present, siblings


# ---------------------------------------------------------------------------
# Transfer
# ---------------------------------------------------------------------------

# GNU find prints the whole tree in one process; BSD find (used only when
# testing against a macOS "remote") has no -printf, so fall back to a single
# batched stat. Both are metadata-only walks — no file contents are read.
def _listing_command(directory: str) -> str:
    quoted = shell_quote_path(directory)
    return (
        f"cd {quoted} || exit 1; "
        "if find . -maxdepth 0 -printf '' 2>/dev/null; then "
        "  find . -type f -printf '%P\\t%s\\n'; "
        "else "
        "  find . -type f -exec stat -f '%N\t%z' {} +; "
        "fi | LC_ALL=C sort"
    )


async def _remote_listing(ssh: SSHClient, directory: str) -> tuple[tuple[str, int], ...]:
    stdout, stderr, code = await ssh.run_command(
        _listing_command(directory), timeout=600,
    )
    if code != 0:
        raise DatasetError(
            f"Could not list staged files at {directory}: {stderr.strip()}"
        )
    entries = parse_remote_listing(stdout)
    # BSD stat echoes the path as given ("./sub/a"); GNU -printf '%P' does not.
    return tuple(
        (path[2:] if path.startswith("./") else path, size) for path, size in entries
    )


async def stage_dataset(
    ssh: SSHClient,
    plan: DatasetPlan,
    *,
    run_id: str,
    progress: Any = None,
) -> int:
    """Copy the dataset to its content-addressed destination.

    Transfers into a per-run staging directory, verifies the staged tree
    against the manifest that named the destination, and only then moves it
    into place. An existing destination is never written into: if another run
    won the race while this transfer was in flight, the staged copy is
    discarded instead.

    Returns the number of bytes copied (0 when another run got there first).
    """
    staging = staging_path(plan.dest, run_id)
    parent = plan.dest.rsplit("/", 1)[0]

    # Clear leftovers from a daemon that died mid-transfer, and make sure the
    # parent exists so the SFTP put creates ``staging`` itself rather than
    # nesting inside something. The glob must stay outside the quotes.
    prepare = (
        f"rm -rf {shell_quote_path(plan.dest)}{STAGING_INFIX}* && "
        f"mkdir -p {shell_quote_path(parent)}"
    )
    _, stderr, code = await ssh.run_command(prepare, timeout=120)
    if code != 0:
        raise DatasetError(f"Could not prepare {parent} for staging: {stderr.strip()}")

    logger.info(
        "Staging dataset '%s' (%d files, %d bytes) to %s",
        plan.name, plan.manifest.file_count, plan.manifest.total_bytes, plan.dest,
    )
    copied = await ssh.put_tree(
        plan.local_path, staging, timeout=plan.timeout, progress=progress,
    )

    # SFTP can report success while a file is short (a quota hit, a truncated
    # write the client never surfaced). Moving that onto the hash path would
    # poison it permanently, since every later run would find it present and
    # reuse it. Metadata walk only, and only on the transfer path.
    problems = diff_manifests(plan.manifest, await _remote_listing(ssh, staging))
    if problems:
        raise DatasetError(
            f"Staged copy of '{plan.name}' does not match the local dataset; "
            f"leaving it at {staging} and not publishing it. "
            + "; ".join(problems)
        )

    # ``mv src dest`` moves *into* dest when dest exists, so re-check rather
    # than creating <dest>/<hash>.staging-<run>. A present dest means another
    # run staged the identical tree first; discard ours.
    finalize = (
        f"if [ -d {shell_quote_path(plan.dest)} ]; then "
        f"  rm -rf {shell_quote_path(staging)}; printf '%s\\n' RACED; "
        f"else "
        f"  mv {shell_quote_path(staging)} {shell_quote_path(plan.dest)}; "
        f"fi"
    )
    stdout, stderr, code = await ssh.run_command(finalize, timeout=300)
    if code != 0:
        raise DatasetError(
            f"Could not publish staged dataset '{plan.name}' to {plan.dest}: "
            f"{stderr.strip()}"
        )
    if "RACED" in stdout:
        logger.info(
            "Dataset '%s' was staged concurrently; discarded this copy",
            plan.name,
        )
        return 0

    logger.info("Dataset '%s' staged at %s (%d bytes)", plan.name, plan.dest, copied)
    return copied
