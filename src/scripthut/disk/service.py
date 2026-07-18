"""Orchestration for on-demand disk scans: run, cache, guard.

Scans are user-triggered (never per-poll — ``du`` over a big NFS/Lustre
tree is metadata-heavy), run as background asyncio tasks so HTTP
requests return immediately, and the last result per backend is cached
here so the UI/CLI always has something dated to show. Failures are
cached too (as a result carrying only ``errors``) rather than
vanishing.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Coroutine, Sequence

from scripthut.disk.classify import build_run_references, classify_entries
from scripthut.disk.cleanup import (
    CleanupOutcome,
    CleanupPlan,
    CleanupReport,
    build_agent_check_script,
    build_delete_script,
    parse_agent_check_output,
    parse_delete_output,
    plan_cleanup,
)
from scripthut.disk.models import DiskEntry, DiskScanResult, ScanSpec
from scripthut.disk.scan import build_scan_script, parse_scan_output, raw_to_entries

if TYPE_CHECKING:
    from scripthut.config_schema import ScriptHutConfig, Stack
    from scripthut.runs.manager import RunManager
    from scripthut.runs.models import Run
    from scripthut.runs.storage import RunStorageManager
    from scripthut.ssh.client import SSHClient

logger = logging.getLogger(__name__)

SCAN_TIMEOUT = 600  # seconds for the whole remote script


class DiskScanService:
    """Per-backend scan/cleanup cache + single-flight guard.

    One background task per backend, whether scanning or cleaning — a
    cleanup ends in a rescan, so both kinds of task resolve to a
    :class:`DiskScanResult` and share the same storage guarantee.
    """

    def __init__(self) -> None:
        self._results: dict[str, DiskScanResult] = {}
        self._tasks: dict[str, asyncio.Task[DiskScanResult]] = {}
        self._kinds: dict[str, str] = {}  # backend -> "scan" | "clean"
        self._cleanups: dict[str, CleanupReport] = {}

    def get_cached(self, backend: str) -> DiskScanResult | None:
        return self._results.get(backend)

    def get_last_cleanup(self, backend: str) -> CleanupReport | None:
        return self._cleanups.get(backend)

    def is_busy(self, backend: str) -> bool:
        task = self._tasks.get(backend)
        return task is not None and not task.done()

    def is_scanning(self, backend: str) -> bool:
        return self.is_busy(backend) and self._kinds.get(backend) == "scan"

    def is_cleaning(self, backend: str) -> bool:
        return self.is_busy(backend) and self._kinds.get(backend) == "clean"

    def start_scan(
        self, backend: str, coro: Coroutine[None, None, DiskScanResult]
    ) -> bool:
        """Launch ``coro`` as this backend's scan; False if backend is busy."""
        return self._start(backend, "scan", coro)

    def start_clean(
        self, backend: str, coro: Coroutine[None, None, DiskScanResult]
    ) -> bool:
        """Launch ``coro`` as this backend's cleanup; False if busy."""
        return self._start(backend, "clean", coro)

    def _start(
        self, backend: str, kind: str, coro: Coroutine[None, None, DiskScanResult]
    ) -> bool:
        if self.is_busy(backend):
            coro.close()  # avoid "coroutine never awaited" warning
            return False
        task = asyncio.create_task(self._run_and_store(backend, coro))
        self._tasks[backend] = task
        self._kinds[backend] = kind

        def _done(t: asyncio.Task[DiskScanResult]) -> None:
            self._tasks.pop(backend, None)
            self._kinds.pop(backend, None)

        task.add_done_callback(_done)
        return True

    async def _run_and_store(
        self, backend: str, coro: Coroutine[None, None, DiskScanResult]
    ) -> DiskScanResult:
        """Await the scan and cache its result before the task turns done.

        Storing here (not in a done-callback) means ``task.done()`` never
        races ahead of the cache: a poller can't observe "not scanning"
        with yesterday's result still cached. Crashes are cached too so
        the UI shows a dated failure instead of "never scanned".
        """
        try:
            result = await coro
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # scan_backend catches its own errors; this is belt-and-braces
            logger.exception("disk scan task for '%s' died", backend)
            result = DiskScanResult(
                backend=backend,
                scanned_at=datetime.now(timezone.utc),
                duration_ms=0,
                errors=[f"scan crashed: {exc}"],
            )
        self._results[backend] = result
        return result

    async def scan_backend(
        self,
        *,
        spec: ScanSpec,
        ssh: SSHClient,
        runs: list[Run],
        current_stack_hashes: dict[str, set[str]] | None = None,
        extra_errors: list[str] | None = None,
        timeout: int = SCAN_TIMEOUT,
    ) -> DiskScanResult:
        """Run one scan end-to-end: SSH script, parse, classify.

        Always returns a result — SSH/timeout failures come back as a
        result with ``errors`` set so the caller can cache and show it.
        ``extra_errors`` (e.g. unreadable project scripthut.yaml files)
        are carried onto the result so they surface with the scan.
        """
        started = time.monotonic()
        scanned_at = datetime.now(timezone.utc)

        def _elapsed_ms() -> int:
            return int((time.monotonic() - started) * 1000)

        script = build_scan_script(spec)
        try:
            stdout, stderr, exit_code = await ssh.run_command(script, timeout=timeout)
        except Exception as e:
            logger.warning("disk scan on '%s' failed: %s", spec.backend, e)
            return DiskScanResult(
                backend=spec.backend,
                scanned_at=scanned_at,
                duration_ms=_elapsed_ms(),
                errors=list(extra_errors or []) + [f"scan failed: {e}"],
            )

        errors: list[str] = list(extra_errors or [])
        if exit_code != 0:
            errors.append(f"scan exited {exit_code}: {stderr.strip()[:500]}")

        home, raw, df, parse_errors = parse_scan_output(stdout)
        errors.extend(parse_errors)

        entries = raw_to_entries(raw)
        refs = build_run_references(runs, spec.backend, spec.clone_dirs, home)
        classify_entries(entries, refs, current_stack_hashes=current_stack_hashes)
        entries.sort(key=lambda e: e.size_bytes or 0, reverse=True)

        return DiskScanResult(
            backend=spec.backend,
            scanned_at=scanned_at,
            duration_ms=_elapsed_ms(),
            home_dir=home,
            disk_total_bytes=df[0] if df else None,
            disk_avail_bytes=df[1] if df else None,
            entries=entries,
            errors=errors,
        )

    async def clean_backend(
        self,
        *,
        plan: CleanupPlan,
        spec: ScanSpec,
        ssh: SSHClient,
        run_manager: RunManager | None,
        run_storage: RunStorageManager | None,
        config: ScriptHutConfig,
    ) -> DiskScanResult:
        """Execute a cleanup plan, cache its report, then rescan.

        The report is cached *before* the rescan starts, inside the
        task, so a poller can never observe "not cleaning" without the
        report being available. The returned (and cached) scan result
        is the post-cleanup truth.
        """
        try:
            report = await execute_cleanup(plan, ssh)
        except Exception as exc:  # belt-and-braces, like _run_and_store
            logger.exception("cleanup on '%s' crashed", spec.backend)
            report = CleanupReport(
                backend=spec.backend,
                started_at=datetime.now(timezone.utc),
                finished_at=datetime.now(timezone.utc),
                errors=[f"cleanup crashed: {exc}"],
            )
        self._cleanups[spec.backend] = report

        runs = await gather_all_runs(run_manager, run_storage)
        # Re-gather project stacks so the post-clean rescan classifies
        # project-declared env dirs the same way the original scan did.
        project_stacks, gather_errors = await gather_project_stacks(
            config, spec.backend, ssh=ssh,
        )
        hashes = compute_current_stack_hashes(config, project_stacks)
        return await self.scan_backend(
            spec=spec, ssh=ssh, runs=runs, current_stack_hashes=hashes,
            extra_errors=gather_errors,
        )


AGENT_CHECK_TIMEOUT = 120
DELETE_TIMEOUT = 1800  # rm -rf on shared filesystems can be very slow


async def execute_cleanup(plan: CleanupPlan, ssh: SSHClient) -> CleanupReport:
    """Run a cleanup plan over SSH: agent git checks, then deletion.

    Never marks an entry deleted without a positive ``OK`` line from
    the remote script; SSH failures degrade to skipped/failed outcomes
    rather than raising.
    """
    report = CleanupReport(
        backend=plan.backend, started_at=datetime.now(timezone.utc)
    )

    def _outcome(entry: DiskEntry, outcome: str, reason: str | None = None) -> None:
        report.outcomes.append(
            CleanupOutcome(
                path=entry.path,
                kind=entry.kind,
                size_bytes=entry.size_bytes,
                outcome=outcome,
                reason=reason,
            )
        )

    for pe in plan.entries:
        if pe.action == "skip":
            _outcome(pe.entry, "skipped", pe.reason)

    # Agent workspaces: verify no uncommitted/unpushed work first.
    delete_entries = [pe.entry for pe in plan.entries if pe.action == "delete"]
    agent_entries = [
        pe.entry for pe in plan.entries if pe.action == "check_then_delete"
    ]
    if agent_entries:
        try:
            stdout, _, _ = await ssh.run_command(
                build_agent_check_script([e.path for e in agent_entries]),
                timeout=AGENT_CHECK_TIMEOUT,
            )
            checks = parse_agent_check_output(stdout)
        except Exception as e:
            logger.warning("agent git check on '%s' failed: %s", plan.backend, e)
            checks = {}
            report.errors.append(f"agent git check failed: {e}")
        for entry in agent_entries:
            status, detail = checks.get(entry.path, ("missing", "-"))
            if status == "clean":
                delete_entries.append(entry)
            elif status == "dirty":
                _outcome(entry, "skipped", "workspace has uncommitted changes")
            elif status == "unpushed":
                _outcome(entry, "skipped", f"workspace has unpushed commits ({detail})")
            else:
                _outcome(entry, "skipped", "could not verify git state — not deleting")

    if delete_entries:
        by_path = {e.path: e for e in delete_entries}
        try:
            stdout, _, _ = await ssh.run_command(
                build_delete_script(list(by_path)), timeout=DELETE_TIMEOUT
            )
            results = parse_delete_output(stdout)
        except Exception as e:
            logger.warning("delete script on '%s' failed: %s", plan.backend, e)
            results = {}
            report.errors.append(
                f"delete script failed: {e}; some entries may be partially "
                "removed — the follow-up rescan shows what remains"
            )
        for path, entry in by_path.items():
            ok, message = results.get(path, (False, "no response from delete script"))
            if ok:
                _outcome(entry, "deleted")
            else:
                _outcome(entry, "failed", message)

    report.finished_at = datetime.now(timezone.utc)
    return report


async def gather_all_runs(
    run_manager: RunManager | None, run_storage: RunStorageManager | None
) -> list[Run]:
    """Union of on-disk runs and in-memory runs, in-memory winning.

    Storage adds terminal and ``_default`` (ad-hoc/external) runs that
    ``restore_from_storage`` skips; the in-memory copy has fresher item
    statuses for anything currently tracked. ``load_all_runs`` walks
    every run.json, so it runs in a thread.
    """
    all_runs: dict[str, Run] = {}
    if run_storage is not None:
        all_runs.update(await asyncio.to_thread(run_storage.load_all_runs))
    if run_manager is not None:
        all_runs.update(run_manager.runs)
    return list(all_runs.values())


def compute_current_stack_hashes(
    config: ScriptHutConfig, extra_stacks: Sequence[Stack] = (),
) -> dict[str, set[str]]:
    """Valid content hashes per stack name (for superseded detection).

    A *set* per name because the same stack name can be legitimately
    declared with different inputs by the server config and by several
    sources' project files — none of those declarations should mark the
    others superseded.
    """
    from scripthut.stacks.manager import compute_stack_hash

    hashes: dict[str, set[str]] = {}
    for s in list(config.stacks) + list(extra_stacks):
        hashes.setdefault(s.name, set()).add(compute_stack_hash(s))
    return hashes


async def gather_project_stacks(
    config: ScriptHutConfig, backend_name: str, *, ssh: SSHClient | None = None,
) -> tuple[list[Stack], list[str]]:
    """Stacks declared by each source's project ``scripthut.yaml``.

    Users keep project-specific env folders as stacks with a custom
    ``cache_dir`` in the repo's own scripthut.yaml; without this the
    scan only sees server-config stacks. Git sources read from the
    server's local sources cache (soft-skip when not synced); path
    sources need SSH and are only readable on their own backend. A
    broken project file becomes an error string, never an exception —
    one bad repo must not sink the whole scan.
    """
    from scripthut.config_schema import PathSourceConfig
    from scripthut.runs.manager import load_source_project_config

    stacks: list[Stack] = []
    errors: list[str] = []
    for source in config.sources:
        if isinstance(source, PathSourceConfig):
            if ssh is None or source.backend != backend_name:
                continue
            source_ssh: SSHClient | None = ssh
        else:
            source_ssh = None
        try:
            project_cfg = await load_source_project_config(
                config, source, ssh_client=source_ssh,
            )
        except ValueError as e:
            errors.append(f"source '{source.name}': {e}")
            continue
        except Exception as e:
            logger.warning(
                "reading project config for source '%s' failed: %s",
                source.name, e,
            )
            errors.append(f"source '{source.name}': {e}")
            continue
        if project_cfg is not None:
            stacks.extend(project_cfg.stacks)
    return stacks, errors


async def start_scan_for_backend(
    service: DiskScanService,
    *,
    config: ScriptHutConfig,
    backend_name: str,
    clone_dir: str,
    ssh: SSHClient,
    run_manager: RunManager | None,
    run_storage: RunStorageManager | None,
) -> bool:
    """Assemble scan inputs and launch the background scan.

    Shared by the JSON API and the HTML routes. Returns False when the
    backend is already scanning or cleaning.
    """
    from scripthut.disk.scan import build_scan_spec

    if service.is_busy(backend_name):
        return False
    project_stacks, gather_errors = await gather_project_stacks(
        config, backend_name, ssh=ssh,
    )
    spec = build_scan_spec(
        config, backend_name, clone_dir, extra_stacks=project_stacks,
    )
    runs = await gather_all_runs(run_manager, run_storage)
    hashes = compute_current_stack_hashes(config, project_stacks)
    return service.start_scan(
        backend_name,
        service.scan_backend(
            spec=spec, ssh=ssh, runs=runs, current_stack_hashes=hashes,
            extra_errors=gather_errors,
        ),
    )


async def plan_cleanup_for_backend(
    service: DiskScanService,
    *,
    config: ScriptHutConfig,
    backend_name: str,
    clone_dir: str,
    run_manager: RunManager | None,
    run_storage: RunStorageManager | None,
    paths: list[str] | None,
    allow_referenced: frozenset[str] = frozenset(),
    ssh: SSHClient | None = None,
) -> CleanupPlan | None:
    """Plan a cleanup against the cached scan and *current* runs.

    Returns None when no scan is cached for the backend (callers tell
    the user to scan first). ``ssh`` is only used to read path-sources'
    project scripthut.yaml files (git sources read from the local
    cache); without it those stacks are simply not part of the safety
    roots, which fails toward skipping — never toward deleting.
    """
    from scripthut.disk.scan import build_scan_spec

    cached = service.get_cached(backend_name)
    if cached is None:
        return None
    project_stacks, _ = await gather_project_stacks(
        config, backend_name, ssh=ssh,
    )
    spec = build_scan_spec(
        config, backend_name, clone_dir, extra_stacks=project_stacks,
    )
    runs = await gather_all_runs(run_manager, run_storage)
    refs = build_run_references(
        runs, backend_name, spec.clone_dirs, cached.home_dir
    )
    return plan_cleanup(
        cached,
        refs,
        spec=spec,
        current_stack_hashes=compute_current_stack_hashes(config, project_stacks),
        planned_at=datetime.now(timezone.utc),
        paths=paths,
        allow_referenced=allow_referenced,
    )


async def start_clean_for_backend(
    service: DiskScanService,
    *,
    config: ScriptHutConfig,
    backend_name: str,
    clone_dir: str,
    ssh: SSHClient,
    run_manager: RunManager | None,
    run_storage: RunStorageManager | None,
    paths: list[str] | None,
    allow_referenced: frozenset[str] = frozenset(),
) -> tuple[str, CleanupPlan | None]:
    """Plan and launch a background cleanup.

    Returns ``(status, plan)`` with status one of ``no_scan``,
    ``invalid`` (plan.errors set), ``nothing_to_clean``,
    ``already_running``, ``started``. The plan is computed here, at
    request time — which for the background task *is* execution time.
    """
    from scripthut.disk.scan import build_scan_spec

    if service.is_busy(backend_name):
        return "already_running", None
    plan = await plan_cleanup_for_backend(
        service,
        config=config,
        backend_name=backend_name,
        clone_dir=clone_dir,
        run_manager=run_manager,
        run_storage=run_storage,
        paths=paths,
        allow_referenced=allow_referenced,
        ssh=ssh,
    )
    if plan is None:
        return "no_scan", None
    if plan.errors:
        return "invalid", plan
    if not plan.to_delete:
        return "nothing_to_clean", plan
    project_stacks, _ = await gather_project_stacks(
        config, backend_name, ssh=ssh,
    )
    spec = build_scan_spec(
        config, backend_name, clone_dir, extra_stacks=project_stacks,
    )
    started = service.start_clean(
        backend_name,
        service.clean_backend(
            plan=plan,
            spec=spec,
            ssh=ssh,
            run_manager=run_manager,
            run_storage=run_storage,
            config=config,
        ),
    )
    return ("started" if started else "already_running"), plan
