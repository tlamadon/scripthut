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

from scripthut.disk.classify import (
    annotate_stack_envs,
    build_run_references,
    classify_entries,
)
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
        current_data_hashes: dict[str, set[str]] | None = None,
        current_sync_dests: dict[str, str] | None = None,
        stack_texts: dict[str, str] | None = None,
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
        classify_entries(
            entries,
            refs,
            current_stack_hashes=current_stack_hashes,
            current_data_hashes=current_data_hashes,
            current_sync_dests=current_sync_dests,
        )
        annotate_stack_envs(entries, stack_texts or {}, home)
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
        current_sync_dests: dict[str, str] | None = None,
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
            current_data_hashes=compute_current_data_hashes(config),
            current_sync_dests=current_sync_dests,
            stack_texts=collect_stack_texts(config, project_stacks),
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


def compute_current_data_hashes(config: ScriptHutConfig) -> dict[str, set[str]]:
    """Manifest hash per configured dataset (for superseded detection).

    A metadata walk of each local tree — milliseconds, no file contents. A
    dataset whose local directory has gone missing contributes an empty set,
    so every copy on the cluster reads as superseded rather than current;
    that is the honest answer, and it never deletes anything by itself.
    """
    from scripthut.runs.datasets import DatasetError, build_manifest

    hashes: dict[str, set[str]] = {}
    for dataset in getattr(config, "datasets", []):
        try:
            hashes.setdefault(dataset.name, set()).add(
                build_manifest(dataset.path).short
            )
        except (DatasetError, OSError) as e:
            logger.warning(f"Cannot hash dataset '{dataset.name}': {e}")
            hashes.setdefault(dataset.name, set())
    return hashes


async def gather_data_dirs(
    config: ScriptHutConfig, backend_name: str, *, ssh: SSHClient | None = None,
) -> tuple[list[str], list[str]]:
    """``<root>/<name>`` for each configured dataset on this backend.

    Async because a ``~/``-relative root has to be expanded against the
    backend's probed ``$HOME`` before it can be scanned — that SSH round trip
    is why this lives beside :func:`gather_project_stacks` rather than inside
    ``build_scan_spec``. A root that is unsafe (the home directory itself, or
    inside a clone dir) is reported as an error and simply not scanned.
    """
    from scripthut.runs.datasets import (
        DatasetError,
        probe_backend_paths,
        resolve_root,
    )

    datasets = list(getattr(config, "datasets", []))
    if not datasets or ssh is None:
        return [], []

    backend_cfg = config.get_backend(backend_name)
    clone_dirs = [
        s.clone_dir for s in config.sources if getattr(s, "clone_dir", None)
    ]
    backend_clone = getattr(backend_cfg, "clone_dir", None)
    if backend_clone:
        clone_dirs.append(backend_clone)

    paths = await probe_backend_paths(ssh)
    dirs: list[str] = []
    errors: list[str] = []
    for dataset in datasets:
        try:
            root = resolve_root(
                dataset,
                backend_name=backend_name,
                backend_cfg=backend_cfg,
                paths=paths,
                clone_dirs=clone_dirs,
            )
        except DatasetError as e:
            errors.append(f"dataset '{dataset.name}': {e}")
            continue
        dirs.append(f"{root}/{dataset.name}")
    return dirs, errors


async def gather_sync_dirs(
    config: ScriptHutConfig, backend_name: str, *, ssh: SSHClient | None = None,
) -> tuple[list[str], list[str], dict[str, str] | None, list[str]]:
    """Parents to walk, dests to inventory as trees, dest→source, errors.

    Async because a ``~/``-relative dest has to be expanded against the
    backend's probed ``$HOME``. A dest that fails the clone/dataset
    guards is reported and not scanned. Without SSH the dest map is
    ``None`` so classification does not treat configured dests as leftover.
    """
    from scripthut.config_schema import SyncSourceConfig
    from scripthut.runs.datasets import (
        DatasetError,
        _expand_home,
        _is_at_or_under,
        probe_backend_paths,
        validate_remote_root,
    )
    from scripthut.runs.sync import DEFAULT_SYNC_DIR, SyncError, resolve_dest

    if ssh is None:
        return [], [], None, []

    backend_cfg = config.get_backend(backend_name)
    paths = await probe_backend_paths(ssh)
    home = paths.home

    dest_to_source: dict[str, str] = {}
    errors: list[str] = []
    for source in config.sources:
        if not isinstance(source, SyncSourceConfig):
            continue
        if source.backend != backend_name:
            continue
        try:
            dest = resolve_dest(
                source,
                backend_name=backend_name,
                backend_cfg=backend_cfg,
                home=home,
            )
        except SyncError as e:
            errors.append(f"sync source '{source.name}': {e}")
            continue
        dest_to_source[dest.rstrip("/")] = source.name

    parents: list[str] = []
    if backend_cfg is not None and hasattr(backend_cfg, "sync_dir"):
        origin = f"backend '{backend_name}' sync_dir"
        raw = (getattr(backend_cfg, "sync_dir", None) or DEFAULT_SYNC_DIR)
        raw = raw.strip() or DEFAULT_SYNC_DIR
        try:
            parent = validate_remote_root(raw, origin=origin)
            parent = _expand_home(parent, home, origin=origin).rstrip("/")
        except DatasetError as e:
            errors.append(str(e))
        else:
            # A dest equal to the parent is inventoried as a whole tree.
            if parent not in dest_to_source:
                parents.append(parent)

    self_scan: list[str] = []
    for dest in dest_to_source:
        under_parent = any(
            _is_at_or_under(dest, p) and dest != p for p in parents
        )
        if not under_parent:
            self_scan.append(dest)

    return parents, self_scan, dest_to_source, errors


async def assemble_scan_spec(
    config: ScriptHutConfig,
    backend_name: str,
    clone_dir: str,
    *,
    ssh: SSHClient | None = None,
) -> tuple[ScanSpec, dict[str, str] | None, list[str], list[Stack]]:
    """Stacks, data dirs, and sync dests for one backend's scan or cleanup.

    Returns ``(spec, current_sync_dests, errors, project_stacks)``.
    ``current_sync_dests`` is ``None`` when SSH was not available.
    """
    from scripthut.disk.scan import build_scan_spec

    project_stacks, gather_errors = await gather_project_stacks(
        config, backend_name, ssh=ssh,
    )
    data_dirs, data_errors = await gather_data_dirs(
        config, backend_name, ssh=ssh,
    )
    sync_parents, sync_dirs, dest_map, sync_errors = await gather_sync_dirs(
        config, backend_name, ssh=ssh,
    )
    spec = build_scan_spec(
        config, backend_name, clone_dir,
        extra_stacks=project_stacks,
        data_dirs=data_dirs,
        sync_parents=sync_parents,
        sync_dirs=sync_dirs,
    )
    return spec, dest_map, gather_errors + data_errors + sync_errors, project_stacks


def collect_stack_texts(
    config: ScriptHutConfig, extra_stacks: Sequence[Stack] = (),
) -> dict[str, str]:
    """Stack name -> its ``prep`` + ``init`` text, for env attribution.

    Concatenated per name because the same name can be declared by the
    server config and by several sources; any of those scripts naming a
    path is enough to say the stack is behind it. See
    :func:`scripthut.disk.classify.annotate_stack_envs`.
    """
    texts: dict[str, str] = {}
    for s in list(config.stacks) + list(extra_stacks):
        texts[s.name] = f"{texts.get(s.name, '')}\n{s.prep}\n{s.init}"
    return texts


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
    if service.is_busy(backend_name):
        return False
    spec, dest_map, extra_errors, project_stacks = await assemble_scan_spec(
        config, backend_name, clone_dir, ssh=ssh,
    )
    runs = await gather_all_runs(run_manager, run_storage)
    hashes = compute_current_stack_hashes(config, project_stacks)
    return service.start_scan(
        backend_name,
        service.scan_backend(
            spec=spec, ssh=ssh, runs=runs, current_stack_hashes=hashes,
            current_data_hashes=compute_current_data_hashes(config),
            current_sync_dests=dest_map,
            stack_texts=collect_stack_texts(config, project_stacks),
            extra_errors=extra_errors,
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
    cached = service.get_cached(backend_name)
    if cached is None:
        return None
    spec, dest_map, _, project_stacks = await assemble_scan_spec(
        config, backend_name, clone_dir, ssh=ssh,
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
        current_data_hashes=compute_current_data_hashes(config),
        current_sync_dests=dest_map,
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
    spec, dest_map, _, _ = await assemble_scan_spec(
        config, backend_name, clone_dir, ssh=ssh,
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
            current_sync_dests=dest_map,
        ),
    )
    return ("started" if started else "already_running"), plan
