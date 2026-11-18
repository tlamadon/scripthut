"""Read-only source and disk observations in the server's polling cycle."""

import shlex
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any

from scripthut.config_schema import GitSourceConfig
from scripthut.scriptflow import RecordReader
from scripthut.scriptflow_sources import directory_present, remote_path, source_rows, workdir_for


async def refresh_web_cache(state: Any, service: Any) -> None:
    if not state.config:
        return
    state.web_sources = await source_rows(state.config, state.backends)
    runs = await service.store.list()
    for name, connection in state.backends.items():
        previous = state.disk_usage.get(name, {})
        entry = dict(
            name=name,
            rows=previous.get("rows", []),
            observed_at=previous.get("observed_at"),
            error=None,
        )
        state.disk_usage[name] = entry
        if not connection.enabled or not connection.ssh_client or not connection.status.connected:
            entry["error"] = "Backend disconnected; showing last observation."
            continue
        try:
            reader = RecordReader(connection.ssh_client)
            locations = {}
            missing = set()
            for source in state.config.sources:
                if not isinstance(source, GitSourceConfig):
                    continue
                root = await workdir_for(reader, source)
                flow = str(PurePosixPath(root) / source.flow_dir)
                locations[root] = (source.name, "working directory")
                if not await directory_present(reader, root):
                    missing.add(root)
                    continue
                locations[str(PurePosixPath(flow) / ".sf/runs")] = (source.name, "run records")
                # Immediate directories include staging/results folders without guessing names.
                command = shlex.join(
                    ["find", flow, "-mindepth", "1", "-maxdepth", "1", "-type", "d", "-print0"]
                )
                stdout, stderr, code = await connection.ssh_client.run_command(command)
                if code == 0:
                    for child in stdout.split("\0"):
                        if child and PurePosixPath(child).name not in (".git", ".sf"):
                            is_result = any(
                                output == child or output.startswith(child + "/")
                                for run in runs
                                if run.reference.backend == name
                                for task in (run.snapshot or {}).get("tasks", {}).values()
                                for output in task.get("outputs", [])
                            )
                            locations.setdefault(
                                child, (source.name, "results folder" if is_result else "folder")
                            )
            for path, (source_name, kind) in list(locations.items()):
                if kind != "run records":
                    continue
                stdout, _, code = await connection.ssh_client.run_command(
                    shlex.join(
                        ["find", path, "-mindepth", "1", "-maxdepth", "1", "-type", "d", "-print0"]
                    )
                )
                if code == 0:
                    for child in stdout.split("\0"):
                        if child:
                            locations[child] = (source_name, "run record")
            for run in runs:
                if run.reference.backend != name:
                    continue
                source_name = (run.source or {}).get("name", "manual")
                locations[run.reference.run_directory] = (source_name, "run record")
            settings = state.config.get_backend(name)
            images = await remote_path(reader, settings.image_dir or settings.clone_dir + "/images")
            locations[images] = ("shared", "images")
            rows = []
            for path, (source_name, kind) in locations.items():
                if path in missing or not await directory_present(reader, path):
                    rows.append(
                        dict(
                            path=path,
                            source=source_name,
                            kind=kind,
                            size=None,
                            error="not present on this backend",
                        )
                    )
                    continue
                stdout, stderr, code = await connection.ssh_client.run_command(
                    shlex.join(["du", "-sb", "--", path])
                )
                try:
                    size = int(stdout.split("\t", 1)[0]) if code == 0 else None
                except ValueError:
                    size = None
                rows.append(
                    dict(
                        path=path,
                        source=source_name,
                        kind=kind,
                        size=size,
                        error=stderr.strip() or "Unavailable" if size is None else None,
                    )
                )
            entry.update(rows=rows, observed_at=datetime.now(UTC).isoformat())
        except (ValueError, OSError, RuntimeError) as exc:
            entry["error"] = str(exc)
