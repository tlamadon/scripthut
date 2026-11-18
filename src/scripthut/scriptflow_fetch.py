"""Contained, verified copies of ScriptFlow outputs."""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import stat
import uuid
from pathlib import Path, PurePosixPath
from typing import Any

from scripthut.config_schema import PBSBackendConfig, ScriptHutConfig, SlurmBackendConfig
from scripthut.scriptflow import (
    ObservationError,
    RecordReader,
    ReferenceStore,
    absolute_path,
    parse_record,
)

# Resolve again for transfer; metadata alone does not authorize a later read.
GUARD = r"""set -eu
root=$(realpath -e -- "$1") || exit 44
target=$(realpath -e -- "$2") || exit 44
case "$target" in "$root"|"$root"/*) ;; *) exit 43 ;; esac
"""
INFO = (
    GUARD
    + r"""
limit=$3
export root limit
describe() {
    path=$1
    resolved=$(realpath -e -- "$path") || {
        printf '%s\0missing\00\0\0\0' "$path"; return 1;
    }
    case "$resolved" in "$root"|"$root"/*) ;; *)
        printf '%s\0unsafe_path\00\0\0\0' "$path"; return 1 ;; esac
    if [ -d "$resolved" ]; then
        printf '%s\0directory\00\0\0\0' "$path"
    elif [ -f "$resolved" ]; then
        (
            exec 3< "$resolved"
            opened=$(realpath -e /proc/self/fd/3)
            case "$opened" in "$root"/*) ;; *) exit 43 ;; esac
            size=$(stat -Lc %s /proc/self/fd/3)
            if [ "$size" -gt "$limit" ]; then
                printf '%s\0file_limit\0%s\0\0\0' "$path" "$size"
            else
                digest=$(sha256sum <&3)
                printf '%s\0file\0%s\0%s\0\0' "$path" "$size" "${digest%% *}"
            fi
        ) || printf '%s\0unreadable\00\0\0\0' "$path"
    else
        printf '%s\0unsupported_type\00\0\0\0' "$path"
    fi
}
export -f describe
find -L "$2" -exec bash -c 'describe "$1"' fetch-info {} \; -o -prune
"""
)
READ = (
    GUARD
    + r"""
[ -f "$target" ] || exit 44
exec 3< "$target"
opened=$(realpath -e /proc/self/fd/3)
case "$opened" in "$root"/*) ;; *) exit 43 ;; esac
head -c "$3" <&3
"""
)


def relative_path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or any(c in value for c in "\0\n\r"):
        raise ValueError("Expected a relative path without '..' or control characters")
    return path


def remote_path(root: str, value: str) -> str:
    if ".." in PurePosixPath(value).parts:
        raise ValueError("Parent traversal ('..') is not allowed")
    target = absolute_path(value if value.startswith("/") else root + "/" + value)
    if target != root and not target.startswith(root.rstrip("/") + "/"):
        raise ValueError("Requested path is outside the working directory")
    return target


def directory_fd(path: Path, create: bool) -> int | None:
    """Walk from the filesystem root without following local symlinks."""
    fd = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            try:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            except FileNotFoundError:
                if not create:
                    os.close(fd)
                    return None
                try:
                    os.mkdir(part, dir_fd=fd)
                except FileExistsError:
                    pass
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def check_destination(path: Path, directory: bool, overwrite: bool) -> None:
    fd = directory_fd(path if directory else path.parent, False)
    if fd is None:
        return
    try:
        if directory:
            return
        try:
            info = os.stat(path.name, dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("Local destination is not a regular file (symlinks are refused)")
        if not overwrite:
            raise ValueError("Local file exists; use --overwrite")
    finally:
        os.close(fd)


async def copy_file(
    client: Any, root: str, row: dict[str, Any], destination: Path, overwrite: bool
) -> None:
    fd = directory_fd(destination.parent, True)
    assert fd is not None
    temporary = ".scripthut-fetch-" + uuid.uuid4().hex
    created = False
    try:
        raw = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600, dir_fd=fd)
        created = True
        with os.fdopen(raw, "w+b") as stream:
            command = shlex.join(
                ["bash", "-c", READ, "fetch-file", root, row["path"], str(row["size"] + 1)]
            )
            await client.read_binary(command, stream, row["size"])
            stream.flush()
            if stream.tell() != row["size"]:
                raise ValueError("Size mismatch; remote file changed or transfer was incomplete")
            stream.seek(0)
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
            if digest != row["sha256"]:
                raise ValueError("Checksum mismatch; remote file changed or transfer was corrupted")
            os.fsync(stream.fileno())
        check_destination(destination, False, overwrite)
        if overwrite:
            os.replace(temporary, destination.name, src_dir_fd=fd, dst_dir_fd=fd)
        else:
            # Hard-link publication is atomic and cannot clobber a concurrent writer.
            os.link(temporary, destination.name, src_dir_fd=fd, dst_dir_fd=fd)
    finally:
        try:
            if created:
                os.unlink(temporary, dir_fd=fd)
        except FileNotFoundError:
            pass
        os.close(fd)


async def fetch_outputs(
    client: Any,
    root: str,
    paths: list[str],
    destination: str,
    *,
    strip: str = ".",
    overwrite: bool = False,
    dry_run: bool = False,
    file_limit: int = 1024**3,
    total_limit: int = 10 * 1024**3,
) -> dict[str, Any]:
    root = absolute_path(root)
    prefix = relative_path(strip)
    if ".." in Path(destination).parts:
        raise ValueError("Local destination must not contain '..'")
    local = Path(os.path.abspath(os.path.expanduser(destination)))
    probe = directory_fd(local, False)
    if probe is not None:
        os.close(probe)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for requested in paths:
        try:
            target = remote_path(root, requested)
            command = shlex.join(["bash", "-c", INFO, "fetch-info", root, target, str(file_limit)])
            try:
                output, _, code = await client.run_command(command, timeout=600)
            except Exception as exc:
                raise RuntimeError("Output metadata unavailable: " + type(exc).__name__) from None
            if code:
                raise ValueError(
                    {
                        43: "Path resolves outside working directory",
                        44: "Missing or unreadable output",
                    }.get(code, "Cannot enumerate output")
                )
            fields = output.split("\0")
            if fields[-1] != "" or (len(fields) - 1) % 5:
                raise ValueError("Malformed file metadata")
            for index in range(0, len(fields) - 1, 5):
                path, kind, size, digest, _ = fields[index : index + 5]
                if path in seen:
                    continue
                seen.add(path)
                row: dict[str, Any] = {
                    "path": path,
                    "size": int(size or 0),
                    "sha256": digest,
                    "kind": kind,
                    "status": "skipped",
                    "reason": "",
                }
                rows.append(row)
                if kind not in ("file", "directory"):
                    row["reason"] = {
                        "file_limit": (
                            "Per-file size cap exceeded; raise settings.fetch_max_file_bytes"
                        ),
                        "unsafe_path": "Path resolves outside working directory",
                        "missing": "Missing output",
                        "unreadable": "Output cannot be read",
                        "unsupported_type": "Only regular files and directories can be fetched",
                    }.get(kind, "Unsupported file metadata")
                    continue
                try:
                    relative = PurePosixPath(remote_path(root, path)).relative_to(root)
                    placement = relative.relative_to(prefix)
                    if placement == PurePosixPath(".") and kind != "directory":
                        raise ValueError("Strip prefix removes the entire filename")
                    relative_path(str(placement))
                    row["destination"] = str(local / str(placement))
                    check_destination(Path(row["destination"]), kind == "directory", overwrite)
                    row["status"] = "planned"
                except (OSError, ValueError) as exc:
                    row["reason"] = str(exc)
        except (OSError, ValueError, RuntimeError) as exc:
            rows.append({"path": requested, "size": None, "status": "skipped", "reason": str(exc)})
    used = 0
    for row in rows:
        if row["status"] != "planned":
            continue
        try:
            if used + row["size"] > total_limit:
                raise ValueError("Total size cap exceeded; raise settings.fetch_max_total_bytes")
            used += row["size"]
            if not dry_run:
                local_target = Path(row["destination"])
                if row["kind"] == "directory":
                    fd = directory_fd(local_target, True)
                    assert fd is not None
                    os.close(fd)
                else:
                    await copy_file(client, root, row, local_target, overwrite)
            row["status"] = "would_fetch" if dry_run else "fetched"
        except (OSError, ValueError, RuntimeError) as exc:
            row.update(status="skipped", reason=str(exc))
    return {
        "working_directory": root,
        "to": str(local),
        "dry_run": dry_run,
        "ok": all(row["status"] != "skipped" for row in rows),
        "files": rows,
    }


async def handle_fetch(config: ScriptHutConfig, store: ReferenceStore, args: Any) -> dict[str, Any]:
    from scripthut.scriptflow_launch import LaunchStore
    from scripthut.ssh.config import ssh_client_for

    cached = await store.get(args.reference)
    backend = config.get_backend(cached.reference.backend)
    if not isinstance(backend, (SlurmBackendConfig, PBSBackendConfig)):
        raise ValueError("Registered backend is not configured for SSH")
    client = ssh_client_for(backend)
    try:
        reader = RecordReader(client)
        await reader.fetch(cached)
        if cached.last_fetch_failed:
            raise ObservationError(
                cached.problem or "unreachable", cached.fetch_error or "Unreadable run"
            )
        _, record = parse_record(json.dumps(cached.snapshot))
        root = record.working_directory
        if cached.launch_key:
            launch = await LaunchStore(store.directory).get(cached.launch_key)
            if launch is None:
                raise ValueError("Launch metadata is unavailable; cannot determine fetch boundary")
            launch_root = launch.repository_workdir if launch.source else launch.workdir
            if not launch_root:
                raise ValueError("Source working directory is unavailable")
            root = launch_root
        selected = args.path
        if not selected:
            tasks = (
                [reader.select_task(record, task) for task in args.task]
                if args.task
                else record.tasks
            )
            selected = [path for task in tasks for path in record.tasks[task].outputs]
        return await fetch_outputs(
            client,
            root,
            selected,
            args.to,
            strip=args.strip,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
            file_limit=config.settings.fetch_max_file_bytes,
            total_limit=config.settings.fetch_max_total_bytes,
        )
    finally:
        await client.disconnect()
