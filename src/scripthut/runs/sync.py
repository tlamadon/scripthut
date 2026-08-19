"""Laptop → cluster code copy for a ``type: sync`` source.

File-list, dest layout, and workflow discovery live here. Transfer is
``SSHClient.put_files`` / ``get_files`` (dedicated SFTP, dest may exist).
"""

from __future__ import annotations

import json
import logging
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scripthut.sources.git import SourceWorkflow

if TYPE_CHECKING:
    from scripthut.config_schema import SyncSourceConfig
    from scripthut.runs.models import TaskDefinition

logger = logging.getLogger(__name__)

DEFAULT_RETURN_DIR = "output"
DEFAULT_WORKFLOWS_GLOB = ".hut/workflows/*.json"


class SyncError(ValueError):
    """A sync source path is unusable or contains something we will not copy."""


def _normalize_return_dir(return_dir: str) -> str:
    cleaned = return_dir.strip().strip("/")
    if not cleaned or cleaned == "." or ".." in Path(cleaned).parts:
        raise SyncError(f"Invalid return dir: {return_dir!r}")
    return cleaned


def _is_under_return(rel: str, return_dir: str) -> bool:
    return rel == return_dir or rel.startswith(return_dir + "/")


def list_upload_paths(
    repo: Path,
    return_dir: str = DEFAULT_RETURN_DIR,
) -> list[str]:
    """Git-tracked paths to copy laptop → cluster, minus the return dir.

    Working-tree bytes, not HEAD blobs: a dirty tracked file is included.
    Untracked and gitignored paths are not. ``return_dir`` is dropped even
    when it is tracked, so stale local results cannot overwrite the cluster.
    """
    base = repo.expanduser()
    if not base.is_dir():
        raise SyncError(f"Sync path is not a directory: {base}")

    ret = _normalize_return_dir(return_dir)
    try:
        proc = subprocess.run(
            ["git", "-C", str(base), "ls-files", "-z"],
            capture_output=True,
            check=False,
        )
    except FileNotFoundError as e:
        raise SyncError("git is not installed on the scripthut host") from e

    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", errors="replace").strip()
        raise SyncError(
            f"Sync path is not a git repository ({base}): {err or 'git ls-files failed'}"
        )

    paths: list[str] = []
    resolved_base = base.resolve()
    raw = proc.stdout.split(b"\0")
    for chunk in raw:
        if not chunk:
            continue
        rel = chunk.decode("utf-8", errors="surrogateescape").replace("\\", "/")
        if _is_under_return(rel, ret):
            continue
        if "\t" in rel or "\n" in rel:
            raise SyncError(
                f"Sync path '{base}' contains a file whose name has a tab or "
                f"newline ({rel!r})"
            )
        entry = base / rel
        if entry.is_symlink():
            raise SyncError(
                f"Sync path '{base}' contains a tracked symlink ({rel}). "
                "Replace it with a regular file; symlinks are not copied."
            )
        if not entry.is_file():
            # Deleted from the working tree but still in the index, or a
            # submodule gitlink — neither is a file we can put.
            raise SyncError(
                f"Tracked path '{rel}' is missing from the working tree at {base}"
            )
        if not entry.resolve().is_relative_to(resolved_base):
            raise SyncError(
                f"Tracked path '{rel}' resolves outside the sync path {base}"
            )
        paths.append(rel)

    paths.sort()
    return paths


def discover_workflows(
    path: Path,
    workflows_glob: str = DEFAULT_WORKFLOWS_GLOB,
    *,
    source_name: str,
) -> list[SourceWorkflow]:
    """Glob workflow JSON on the laptop working tree. No clone, no SSH."""
    base = path.expanduser()
    if not base.is_dir():
        raise SyncError(f"Sync path is not a directory: {base}")

    workflows: list[SourceWorkflow] = []
    for json_file in sorted(base.glob(workflows_glob)):
        if not json_file.is_file():
            continue
        try:
            tasks_json = json_file.read_text()
            parsed = json.loads(tasks_json)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Skipping invalid workflow file %s: %s", json_file, e)
            continue
        title = parsed.get("title") if isinstance(parsed, dict) else None
        workflows.append(
            SourceWorkflow(
                name=f"{source_name}/{json_file.stem}",
                source_name=source_name,
                filename=json_file.name,
                tasks_json=tasks_json,
                title=title,
            )
        )
    return workflows


DEFAULT_SYNC_DIR = "~/scripthut-sync"
SYNC_UPLOAD_ID = "_sync.upload"
SYNC_RETURN_ID = "_sync.return"
SYNC_PREFIX = "_sync."

# In-flight uploads land in a per-run staging directory and are moved onto
# the final dest only after verification, so a crash never leaves a partial
# tree in place. The infix is distinct from datasets' ".staging-" so a
# ``rm -rf <dest>.sync-*`` glob cannot accidentally touch dataset copies
# sitting beside a sync dest.
SYNC_STAGING_INFIX = ".sync-"


def sync_staging_path(dest: str, run_id: str) -> str:
    """Per-run staging path beside ``dest``."""
    return f"{dest}{SYNC_STAGING_INFIX}{run_id}"


def resolve_dest(
    source: SyncSourceConfig,
    *,
    backend_name: str,
    backend_cfg: Any,
    home: str | None,
) -> str:
    """Absolute backend path for this source's working copy."""
    from scripthut.runs.datasets import (
        DatasetError,
        _expand_home,
        _is_at_or_under,
        _normalize_remote,
        validate_remote_root,
    )

    if source.dest:
        origin = f"source '{source.name}' dest"
        raw = source.dest
    else:
        origin = f"backend '{backend_name}' sync_dir"
        parent = getattr(backend_cfg, "sync_dir", None) or DEFAULT_SYNC_DIR
        raw = f"{parent.rstrip('/')}/{source.name}"

    try:
        dest = validate_remote_root(raw, origin=origin)
        dest = _expand_home(dest, home, origin=origin)
    except DatasetError as e:
        raise SyncError(str(e)) from e

    if home and dest.rstrip("/") == home.rstrip("/"):
        raise SyncError(
            f"Sync dest from {origin} is the remote home directory itself "
            f"({dest}). Use a subdirectory such as '~/scripthut-sync'."
        )
    clone = _normalize_remote(getattr(backend_cfg, "clone_dir", "") or "", home)
    data = _normalize_remote(getattr(backend_cfg, "dataset_dir", "") or "", home)
    if clone and _is_at_or_under(dest, clone):
        raise SyncError(
            f"Sync dest from {origin} is inside the clone directory ({dest}). "
            "Code copies must not sit under clone_dir."
        )
    if data and _is_at_or_under(dest, data):
        raise SyncError(
            f"Sync dest from {origin} is inside the dataset directory ({dest}). "
            "Code and data must stay separate."
        )
    return dest


def local_path_collides(local: Path, dest: str) -> bool:
    """True when local and dest overlap on the filesystem.

    Covers equality, dest-inside-local, and local-inside-dest — all three
    would cause an upload to write into or over the source repo on a local
    backend.
    """
    try:
        a = local.expanduser().resolve()
        b = Path(dest).expanduser().resolve()
        return a == b or b.is_relative_to(a) or a.is_relative_to(b)
    except OSError:
        return False


def apply_upload(
    tasks: list[TaskDefinition],
    *,
    local_path: Path,
    dest: str,
    return_dir: str,
    timeout: int = 86400,
) -> list[TaskDefinition]:
    """Prepend the ``_sync.upload`` item; root tasks wait on it."""
    from scripthut.runs.models import SyncDep, TaskDefinition

    clashes = sorted(t.id for t in tasks if t.id == SYNC_UPLOAD_ID or t.id.startswith(SYNC_PREFIX))
    if clashes:
        raise ValueError(
            f"Task id(s) {', '.join(clashes)} collide with scripthut's "
            "sync items; the '_sync.' prefix is reserved."
        )
    upload = TaskDefinition(
        id=SYNC_UPLOAD_ID,
        name="Sync code to backend",
        command=": sync upload",
        sync_dep=SyncDep(
            kind="upload",
            local_path=str(local_path),
            dest=dest,
            return_dir=return_dir,
            timeout=timeout,
        ),
    )
    for task in tasks:
        if not task.dependencies:
            task.dependencies = [SYNC_UPLOAD_ID]
    return [upload, *tasks]


def apply_return(
    tasks: list[TaskDefinition],
    *,
    local_path: Path,
    dest: str,
    return_dir: str,
    timeout: int = 86400,
) -> list[TaskDefinition]:
    """Append the ``_sync.return`` item with no dependencies.

    It is started only when every *other current* item is terminal — gated
    in the run manager, not via ``dependencies``, so a failed user task
    still pulls.
    """
    from scripthut.runs.models import SyncDep, TaskDefinition

    clashes = sorted(t.id for t in tasks if t.id == SYNC_RETURN_ID)
    if clashes:
        raise ValueError(
            f"Task id(s) {', '.join(clashes)} collide with scripthut's "
            "sync items; the '_sync.' prefix is reserved."
        )
    ret = TaskDefinition(
        id=SYNC_RETURN_ID,
        name="Sync output from backend",
        command=": sync return",
        sync_dep=SyncDep(
            kind="return",
            local_path=str(local_path),
            dest=dest,
            return_dir=return_dir,
            timeout=timeout,
        ),
    )
    return [*tasks, ret]
