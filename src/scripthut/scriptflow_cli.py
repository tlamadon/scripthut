"""On-demand ScriptFlow observation without the server or legacy runtime."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import shlex
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from scripthut.config import load_config
from scripthut.config_schema import PBSBackendConfig, ScriptHutConfig, SlurmBackendConfig
from scripthut.scriptflow import (
    TERMINAL_STATES,
    CachedRun,
    ObservationError,
    RecordReader,
    Reference,
    ReferenceStore,
    parse_record,
)

logger = logging.getLogger(__name__)


def add_parser(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
    add_common: Callable[[argparse.ArgumentParser], None],
) -> None:
    group = sub.add_parser("scriptflow", help="Observe ScriptFlow runs and request cancellation")
    verbs = group.add_subparsers(dest="scriptflow_cmd", required=True)
    for verb in (
        "register",
        "list",
        "view",
        "logs",
        "unregister",
        "cancel",
        "launch",
        "cleanup",
        "release",
        "discover",
        "flows",
        "images",
    ):
        parser = verbs.add_parser(verb)
        add_common(parser)
        parser.add_argument("--verbose", action="store_true", help="Show transport logging")
        if verb in ("launch", "discover"):
            parser.add_argument("--backend")
            location = parser.add_mutually_exclusive_group(required=True)
            location.add_argument("--workdir")
            location.add_argument("--source")
            parser.add_argument("--runs-dir")
        if verb in ("flows", "images"):
            parser.add_argument("--backend")
            parser.add_argument("--source", required=True)
            revision = parser.add_mutually_exclusive_group()
            revision.add_argument("--branch")
            revision.add_argument("--commit")
        if verb == "launch":
            parser.add_argument("--branch")
            parser.add_argument("--commit")
            parser.add_argument("--snapshot", action="store_true")
            parser.add_argument("--flow", required=True)
            parser.add_argument("--force-pattern", help="ScriptFlow task force pattern")
            parser.add_argument("--run-id")
            parser.add_argument("--cpus", type=int)
            for resource in ("memory", "time-limit", "partition"):
                parser.add_argument("--" + resource)
            parser.add_argument("--resubmit-ambiguous", action="store_true")
        if verb == "discover":
            parser.add_argument("--register", action="store_true")
        if verb == "release":
            parser.add_argument("--force", action="store_true")
        if verb == "register":
            parser.add_argument("--backend", required=True)
            parser.add_argument("--run-dir", required=True, help="Absolute remote run directory")
        elif verb in ("view", "logs", "unregister", "cancel", "cleanup", "release"):
            parser.add_argument("reference", help="Reference ID printed by register or list")
        if verb == "cleanup":
            parser.add_argument(
                "--wait", type=int, default=180, help="Result wait seconds (default 180)"
            )
        if verb == "cancel":
            parser.add_argument(
                "--wait", nargs="?", type=int, const=180, help="Wait up to SECONDS (default 180)"
            )
        if verb == "logs":
            parser.add_argument("task", help="Task ID or unique task name")
            parser.add_argument(
                "--attempt", type=int, help="1-based attempt number; default latest"
            )
            parser.add_argument("--tail", type=int, help="Last N lines; default entire log")
        parser.set_defaults(handler=handle)


def format_view(data: dict[str, Any]) -> str:
    workflow = data["workflow"]
    observation = data["observation"]
    state = workflow["state"] if workflow else data.get("workflow_status", "unavailable")
    lines = [
        f"Reference {data['reference_id']} ({data['backend']})",
        f"  directory: {data['run_directory']}",
        f"  workflow: {state}",
        f"  observation: {observation['status']}",
        f"  cancellation requested: {data['cancellation']['requested']}",
        f"  last successful fetch: {observation['last_successful_fetch'] or 'never'}",
        f"  heartbeat age: {observation['heartbeat_age_seconds']} seconds",
    ]
    for image in data.get("images", []):
        lines.append(f"  image {image['name']}: {image['ref']} -> {image['path']}")
    if data.get("image_preparation"):
        preparation = data["image_preparation"]
        lines.append(f"  image preparation: {preparation['status']} {preparation['error']}")
        if preparation.get("elapsed_seconds") is not None:
            lines.append(f"  image preparation elapsed: {preparation['elapsed_seconds']} seconds")
        if preparation.get("log_tail"):
            lines.append(preparation["log_tail"])
    if data.get("source"):
        source = data["source"]
        lines.append(
            f"  source: {source['name']} branch={source['branch']} "
            f"commit={source['short_commit']} detached={source['detached']}"
        )
    if data.get("controller"):
        lines.append(
            f"  controller: {data['controller']['state']} "
            f"(observed {data['controller']['observed_at']})"
        )
        if data.get("controller_notice"):
            lines.append("  " + data["controller_notice"])
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
    from scripthut.ssh.config import _ssh_client_for

    backend = config.get_backend(cached.reference.backend)
    client = None
    fetched = False
    try:
        if not isinstance(backend, (SlurmBackendConfig, PBSBackendConfig)):
            raise ObservationError("unreachable", "Registered backend is not configured for SSH")
        client = _ssh_client_for(backend)
        reader = RecordReader(client)
        await reader.fetch(cached)
        if cached.launch_key:
            from scripthut.scriptflow_launch import (
                ControllerBackend,
                LaunchStore,
                controller_observation,
            )

            launch = await LaunchStore(store.directory).get(cached.launch_key)
            if launch:
                from scripthut.scriptflow_images import observe_images

                cached.image_preparation = await observe_images(reader, launch)
                cached.controller = await controller_observation(
                    ControllerBackend(client), reader, launch
                )
        await store.save(cached)
        fetched = True
        data = cached.projection()
        if args.scriptflow_cmd == "cancel":
            return await cancel(reader, cached, store, args.wait)
        if args.scriptflow_cmd == "logs" and not cached.last_fetch_failed:
            content, number = await reader.logs(cached, args.task, args.attempt, args.tail)
            _, snapshot = parse_record(json.dumps(cached.snapshot))
            data.update(
                task_id=reader.select_task(snapshot, args.task), attempt=number, content=content
            )
        return data, cached.last_fetch_failed
    except ObservationError as exc:
        if args.scriptflow_cmd == "logs" and fetched:
            data = cached.projection()
            data["log_error"] = {"kind": exc.kind, "message": str(exc)}
            return data, True
        if cached.launch_key:
            cached.controller = {
                "state": "unknown",
                "observed_at": datetime.now(UTC).isoformat(),
                "error": str(exc),
            }
        cached.last_fetch_failed = True
        cached.last_fetch_at = datetime.now(UTC)
        cached.problem = exc.kind
        cached.fetch_error = str(exc)
        await store.save(cached)
        data = cached.projection()
        if args.scriptflow_cmd == "cancel":
            data["cancel_result"] = {
                "status": "unreachable",
                "message": "Record could not be read; no request written.",
            }
        return data, True
    finally:
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                logger.warning("ScriptFlow observation channel did not close cleanly")


def cancel_result(cached: CachedRun, status: str, message: str) -> tuple[dict[str, Any], bool]:
    data = cached.projection()
    data["cancel_result"] = {"status": status, "message": message}
    return data, status not in ("requested", "confirmed", "terminal")


async def cancel(
    reader: RecordReader, cached: CachedRun, store: ReferenceStore, wait: int | None
) -> tuple[dict[str, Any], bool]:
    if cached.last_fetch_failed:
        return cancel_result(cached, "unreachable", "Record could not be read; no request written.")
    workflow = cached.projection()["workflow"]
    terminal = workflow["state"] in TERMINAL_STATES
    if terminal and wait is None:
        return cancel_result(cached, "terminal", "Run is terminal; no request written.")
    if not terminal:
        try:
            created = await reader.request_cancel(cached)
        except ObservationError as exc:
            return cancel_result(
                cached, "unreachable", f"Request not acknowledged: {exc}. Retry to reconcile."
            )
        await store.save(cached)
        if wait is None:
            message = "Cancellation requested." if created else "Cancellation already requested."
            return cancel_result(
                cached, "requested", message + " The controller may take about a minute to notice."
            )
    deadline = time.monotonic() + (wait or 0)
    while True:
        data = cached.projection()
        workflow = data["workflow"]
        if cached.last_fetch_failed:
            return cancel_result(
                cached, "unreachable", "Record could not be read; cancellation is unconfirmed."
            )
        if workflow["state"] in TERMINAL_STATES and not workflow["cleanup_pending"]:
            return cancel_result(cached, "confirmed", "Run is terminal and cleanup is complete.")
        if workflow["state"] == "running" and data["observation"]["stale"]:
            command = "scriptflow cleanup " + shlex.quote(workflow["run_id"])
            return cancel_result(
                cached,
                "controller_not_responding",
                "Controller not responding (stale heartbeat). Run "
                + command
                + " on the cluster; no cleanup was performed.",
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return cancel_result(
                cached, "pending", "Wait timed out; cancellation or cleanup is still pending."
            )
        await asyncio.sleep(min(2, remaining))
        await reader.fetch(cached)
        await store.save(cached)


async def handle(args: argparse.Namespace) -> int:
    names = ("asyncssh", "scripthut.ssh.client")
    levels = {name: logging.getLogger(name).level for name in names}
    for name in names:
        logging.getLogger(name).setLevel(logging.DEBUG if args.verbose else logging.WARNING)
    try:
        if args.scriptflow_cmd in ("cancel", "cleanup") and args.wait is not None and args.wait < 0:
            raise ObservationError("invalid_wait", "Wait must be nonnegative")
        if args.scriptflow_cmd == "logs":
            if args.attempt is not None and args.attempt < 1:
                raise ObservationError("invalid_attempt", "Attempt must be at least 1")
            if args.tail is not None and args.tail < 0:
                raise ObservationError("invalid_tail", "Tail must be nonnegative")
        config = load_config(args.config)
        store = ReferenceStore(config.settings.data_dir_resolved / "scriptflow")
        if args.scriptflow_cmd in ("launch", "cleanup", "release", "discover", "flows", "images"):
            data = await handle_controller(config, store, args)
            if args.json:
                print(json.dumps(data, indent=2))
            elif isinstance(data, list):
                for item in data:
                    workflow = item["workflow"] or {}
                    print(
                        item["run_id"],
                        workflow.get("state", "unavailable"),
                        workflow.get("flow", "-"),
                        item["observation"]["status"],
                    )
            else:
                for key, value in data.items():
                    print(f"{key}: {value}")
            return int(
                isinstance(data, dict)
                and data.get("status") in ("ambiguous", "unknown", "failed", "pending")
            )
        if args.scriptflow_cmd == "unregister":
            await store.unregister(args.reference)
            if args.json:
                print(json.dumps({"reference_id": args.reference, "status": "unregistered"}))
            else:
                print(f"Unregistered {args.reference}; remote run unchanged.")
            return 0
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
        failed = args.scriptflow_cmd != "list" and any(error for _, error in results)
        if args.json:
            payload = (
                [data for data, _ in results] if args.scriptflow_cmd == "list" else results[0][0]
            )
            print(json.dumps(payload, indent=2))
        elif args.scriptflow_cmd == "list":
            print(
                "REFERENCE                         BACKEND          WORKFLOW       "
                "OBSERVATION       CANCEL REQUESTED  CONTROLLER"
            )
            for data, _ in results:
                workflow = data["workflow"]
                state = (
                    workflow["state"] if workflow else data.get("workflow_status", "unavailable")
                )
                print(
                    f"{data['reference_id']}  {data['backend']:<15}  "
                    f"{state:<13}  {data['observation']['status']:<16}  "
                    f"{data['cancellation']['requested']}  "
                    f"{(data.get('controller') or {}).get('state', '-')}"
                )
                for image in data.get("images", []):
                    print(f"  image {image['name']}: {image['ref']} -> {image['path']}")
                if data.get("image_preparation"):
                    preparation = data["image_preparation"]
                    print(f"  image preparation: {preparation['status']} {preparation['error']}")
                if data.get("source"):
                    source = data["source"]
                    print(
                        f"  source={source['name']} branch={source['branch']} "
                        f"commit={source['short_commit']} detached={source['detached']}"
                    )
                if data.get("controller_notice"):
                    print("  " + data["controller_notice"])
        elif args.scriptflow_cmd == "logs" and not failed:
            sys.stdout.write(results[0][0]["content"])
        else:
            print(format_view(results[0][0]))
            if "cancel_result" in results[0][0]:
                print(results[0][0]["cancel_result"]["message"])
            if "log_error" in results[0][0]:
                print(results[0][0]["log_error"]["message"], file=sys.stderr)
        return 1 if failed else 0
    except (OSError, ValueError, RuntimeError) as exc:
        if args.json:
            print(json.dumps({"error": str(exc), "kind": getattr(exc, "kind", "error")}))
        else:
            print(f"Error: {exc}", file=sys.stderr)
        return 1
    finally:
        for name, level in levels.items():
            logging.getLogger(name).setLevel(level)


async def handle_controller(
    config: ScriptHutConfig, store: ReferenceStore, args: argparse.Namespace
) -> Any:
    from scripthut.scriptflow_launch import (
        ControllerBackend,
        Launch,
        backend_config,
        control,
        discover,
        launch_run,
        make_launch,
    )
    from scripthut.scriptflow_images import image_paths, presence, resolve_images
    from scripthut.scriptflow_sources import (
        SourceCheckout,
        flow_workdir,
        list_flows,
        resolve_revision,
        select_backend,
        source_config,
        workdir_for,
    )
    from scripthut.ssh.config import _ssh_client_for

    source = source_config(config, args.source) if getattr(args, "source", None) else None
    revision = None
    if source and args.scriptflow_cmd in ("launch", "flows", "images"):
        revision = await resolve_revision(config, source, args.branch, args.commit)
    declared_images = []
    if revision is not None and args.scriptflow_cmd in ("launch", "images"):
        declared_images = await resolve_images(config, revision)
    if args.scriptflow_cmd == "flows" and source and source.local_path is not None:
        assert revision is not None
        return {
            "source": source.name,
            "commit": revision.commit,
            "flows": await list_flows(config, revision),
        }
    if source:
        args.backend = select_backend(config, args.backend)
    elif args.scriptflow_cmd == "launch" and (args.branch or args.commit or args.snapshot):
        raise ValueError("--branch, --commit and --snapshot require --source")
    cached = None
    proposed = None
    if args.scriptflow_cmd == "launch":
        if not source:
            proposed = make_launch(config, args)
        name = args.backend
    elif args.scriptflow_cmd in ("discover", "flows", "images"):
        name = args.backend
    else:
        cached = await store.get(args.reference)
        name = cached.reference.backend
    settings = config.get_backend(name) if args.scriptflow_cmd == "images" else backend_config(config, name)
    if settings is None:
        raise ValueError("Unknown image backend")
    client = _ssh_client_for(settings)
    backend = ControllerBackend(client)
    reader = RecordReader(client)
    try:
        prepare = None
        images = await image_paths(config, reader, name, declared_images)
        if args.scriptflow_cmd == "images":
            assert source is not None
            return {"source": source.name, "images": await presence(reader, images)}
        if source:
            directory = await workdir_for(
                reader,
                source,
                revision.commit if revision and getattr(args, "snapshot", False) else None,
            )
            args.workdir = flow_workdir(directory, source)
            if args.scriptflow_cmd == "flows":
                assert revision is not None
                return {
                    "source": source.name,
                    "commit": revision.commit,
                    "flows": await list_flows(config, revision, reader),
                }
            if args.scriptflow_cmd == "launch":
                assert revision is not None
                pinned_revision = revision
                proposed = make_launch(config, args)
                proposed.images = images
                proposed.source = source.name
                proposed.branch = revision.branch
                proposed.commit = revision.commit
                proposed.repository_workdir = directory
                proposed.flow_dir = source.flow_dir
                proposed.detached = revision.detached
                proposed.snapshot = args.snapshot
                if not args.run_id:
                    proposed.key = hashlib.sha256(
                        (proposed.key + source.name + revision.commit).encode()
                    ).hexdigest()[:32]

                async def prepare(launch: Launch) -> None:
                    await SourceCheckout(config).prepare(reader, launch, pinned_revision)

        if proposed:
            return await launch_run(
                config, proposed, store, backend, reader, args.resubmit_ambiguous, prepare
            )
        if args.scriptflow_cmd == "discover":
            return await discover(reader, store, name, args.workdir, args.runs_dir, args.register)
        assert cached is not None
        deadline = time.monotonic() + getattr(args, "wait", 0)
        result = await control(
            config,
            cached,
            store,
            backend,
            reader,
            args.scriptflow_cmd,
            getattr(args, "force", False),
        )
        while args.scriptflow_cmd == "cleanup" and result["status"] == "pending":
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            await asyncio.sleep(min(2, remaining))
            result = await control(config, cached, store, backend, reader, "cleanup")
        return result
    finally:
        try:
            await client.disconnect()
        except Exception:
            logger.warning("ScriptFlow controller channel did not close cleanly")
