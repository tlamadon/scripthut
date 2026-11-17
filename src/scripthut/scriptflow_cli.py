"""On-demand ScriptFlow observation without the server or legacy runtime."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from scripthut.config import load_config
from scripthut.config_schema import PBSBackendConfig, ScriptHutConfig, SlurmBackendConfig
from scripthut.scriptflow import (
    CachedRun,
    ObservationError,
    RecordReader,
    Reference,
    ReferenceStore,
)

logger = logging.getLogger(__name__)


def add_parser(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
    add_common: Callable[[argparse.ArgumentParser], None],
) -> None:
    group = sub.add_parser("scriptflow", help="Observe existing ScriptFlow runs (read-only)")
    verbs = group.add_subparsers(dest="scriptflow_cmd", required=True)
    for verb in ("register", "list", "view", "logs"):
        parser = verbs.add_parser(verb)
        add_common(parser)
        if verb == "register":
            parser.add_argument("--backend", required=True)
            parser.add_argument("--run-dir", required=True, help="Absolute remote run directory")
        elif verb in ("view", "logs"):
            parser.add_argument("reference", help="Reference ID printed by register or list")
        if verb == "logs":
            parser.add_argument("task", help="Task ID from the ScriptFlow record")
            parser.add_argument(
                "--attempt", type=int, help="1-based attempt number; default latest"
            )
            parser.add_argument("--tail", type=int, help="Last N lines; default entire log")
        parser.set_defaults(handler=handle)


def format_view(data: dict[str, Any]) -> str:
    workflow = data["workflow"]
    observation = data["observation"]
    lines = [
        f"Reference {data['reference_id']} ({data['backend']})",
        f"  directory: {data['run_directory']}",
        f"  workflow: {workflow['state'] if workflow else 'unavailable'}",
        f"  observation: {observation['status']}",
        f"  last successful fetch: {observation['last_successful_fetch'] or 'never'}",
        f"  heartbeat age: {observation['heartbeat_age_seconds']} seconds",
    ]
    if observation["error"]:
        lines.append(f"  observation error: {observation['error']}")
    if workflow:
        lines.extend(
            [
                f"  run: {workflow['run_id']} / flow: {workflow['flow']}",
                f"  cleanup_pending: {str(workflow['cleanup_pending']).lower()}",
                f"  error: {workflow['error'] or '-'}",
            ]
        )
        for task_id, task in workflow["tasks"].items():
            lines.append(f"  task {task_id} ({task['name']}): {task['state']}")
            if task["error"]:
                lines.append(f"    error: {task['error']}")
            for number, attempt in enumerate(task["attempts"], 1):
                lines.extend(
                    [
                        f"    attempt {number}: {attempt['state']} job={attempt['job_id']} "
                        f"exit={attempt['exit_code']}",
                        f"      log: {attempt['log']}",
                        f"      error: {attempt['error'] or '-'}",
                    ]
                )
    return "\n".join(lines)


async def observe(
    config: ScriptHutConfig,
    cached: CachedRun,
    store: ReferenceStore,
    args: argparse.Namespace,
) -> tuple[dict[str, Any], bool]:
    from scripthut.cli import _ssh_client_for

    backend = config.get_backend(cached.reference.backend)
    client = None
    fetched = False
    try:
        if not isinstance(backend, (SlurmBackendConfig, PBSBackendConfig)):
            raise ObservationError("unreachable", "Registered backend is not configured for SSH")
        client = _ssh_client_for(backend)
        reader = RecordReader(client)
        await reader.fetch(cached)
        await store.save(cached)
        fetched = True
        data = cached.projection()
        if args.scriptflow_cmd == "logs" and not cached.last_fetch_failed:
            content, number = await reader.logs(cached, args.task, args.attempt, args.tail)
            data.update(task_id=args.task, attempt=number, content=content)
        return data, cached.last_fetch_failed
    except ObservationError as exc:
        if args.scriptflow_cmd == "logs" and fetched:
            data = cached.projection()
            data["log_error"] = {"kind": exc.kind, "message": str(exc)}
            return data, True
        cached.last_fetch_failed = True
        cached.last_fetch_at = datetime.now(UTC)
        cached.problem = exc.kind
        cached.fetch_error = str(exc)
        await store.save(cached)
        return cached.projection(), True
    finally:
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                logger.warning("ScriptFlow observation channel did not close cleanly")


async def handle(args: argparse.Namespace) -> int:
    try:
        if args.scriptflow_cmd == "logs":
            if args.attempt is not None and args.attempt < 1:
                raise ObservationError("invalid_attempt", "Attempt must be at least 1")
            if args.tail is not None and args.tail < 0:
                raise ObservationError("invalid_tail", "Tail must be nonnegative")
        config = load_config(args.config)
        store = ReferenceStore(config.settings.data_dir_resolved / "scriptflow")
        if args.scriptflow_cmd == "register":
            reference = Reference.create(args.backend, args.run_dir)
            backend = config.get_backend(reference.backend)
            if not isinstance(backend, (SlurmBackendConfig, PBSBackendConfig)):
                raise ObservationError(
                    "invalid_backend", "Select a configured Slurm or PBS backend"
                )
            entries = [await store.register(reference)]
        elif args.scriptflow_cmd == "list":
            entries = await store.list()
        else:
            entries = [await store.get(args.reference)]
        results = [await observe(config, entry, store, args) for entry in entries]
        failed = any(error for _, error in results)
        if args.json:
            payload = (
                [data for data, _ in results] if args.scriptflow_cmd == "list" else results[0][0]
            )
            print(json.dumps(payload, indent=2))
        elif args.scriptflow_cmd == "list":
            print("REFERENCE                         BACKEND          WORKFLOW       OBSERVATION")
            for data, _ in results:
                workflow = data["workflow"]
                state = workflow["state"] if workflow else "unavailable"
                print(
                    f"{data['reference_id']}  {data['backend']:<15}  "
                    f"{state:<13}  {data['observation']['status']}"
                )
        elif args.scriptflow_cmd == "logs" and not failed:
            sys.stdout.write(results[0][0]["content"])
        else:
            print(format_view(results[0][0]))
            if "log_error" in results[0][0]:
                print(results[0][0]["log_error"]["message"], file=sys.stderr)
        return 1 if failed else 0
    except (OSError, ValueError, RuntimeError) as exc:
        if args.json:
            print(json.dumps({"error": str(exc), "kind": getattr(exc, "kind", "error")}))
        else:
            print(f"Error: {exc}", file=sys.stderr)
        return 1
