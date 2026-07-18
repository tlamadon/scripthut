"""Tests for the disk-scan service and the /api/v1/disk endpoints."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripthut.api import make_api_router
from scripthut.config_schema import ScriptHutConfig
from scripthut.disk.models import DiskEntryClass, DiskEntryKind, ScanSpec
from scripthut.disk.service import DiskScanService

SCAN_STDOUT = (
    "HOME\t/home/alice\n"
    "DF\t1000\t400\n"
    "SECTION\tclones\t/home/alice/scripthut-repos\n"
    "ENTRY\tclones\t/home/alice/scripthut-repos/a1b2c3d4e5f6\t1718600000\t2048\n"
    "MISSING\tlogs\t/home/alice/.cache/scripthut/logs\n"
)


def _ssh_returning(stdout: str, stderr: str = "", exit_code: int = 0) -> AsyncMock:
    ssh = AsyncMock()
    ssh.run_command = AsyncMock(return_value=(stdout, stderr, exit_code))
    return ssh


def _spec() -> ScanSpec:
    return ScanSpec(backend="hpc", clone_dirs=["~/scripthut-repos"])


# -- DiskScanService ---------------------------------------------------------


class TestDiskScanService:
    async def test_scan_backend_happy_path(self):
        svc = DiskScanService()
        result = await svc.scan_backend(
            spec=_spec(), ssh=_ssh_returning(SCAN_STDOUT), runs=[]
        )
        assert result.backend == "hpc"
        assert result.home_dir == "/home/alice"
        assert result.disk_total_bytes == 1000 * 1024
        assert result.disk_avail_bytes == 400 * 1024
        assert result.errors == []
        assert len(result.entries) == 1
        entry = result.entries[0]
        assert entry.kind == DiskEntryKind.CLONE
        assert entry.classification == DiskEntryClass.ORPHANED
        assert entry.size_bytes == 2048 * 1024

    async def test_scan_backend_ssh_error_returns_error_result(self):
        svc = DiskScanService()
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(side_effect=RuntimeError("Command timed out"))
        result = await svc.scan_backend(spec=_spec(), ssh=ssh, runs=[])
        assert result.entries == []
        assert any("Command timed out" in e for e in result.errors)

    async def test_scan_backend_nonzero_exit_recorded(self):
        svc = DiskScanService()
        result = await svc.scan_backend(
            spec=_spec(), ssh=_ssh_returning("", "boom", 1), runs=[]
        )
        assert any("exited 1" in e for e in result.errors)

    async def test_start_scan_guard_and_cache(self):
        svc = DiskScanService()
        release = asyncio.Event()

        async def slow_scan():
            await release.wait()
            return await svc.scan_backend(
                spec=_spec(), ssh=_ssh_returning(SCAN_STDOUT), runs=[]
            )

        assert svc.start_scan("hpc", slow_scan()) is True
        assert svc.is_scanning("hpc") is True
        # second start while running is refused
        assert svc.start_scan("hpc", slow_scan()) is False
        release.set()
        await asyncio.sleep(0)  # let the task run
        while svc.is_scanning("hpc"):
            await asyncio.sleep(0.01)
        cached = svc.get_cached("hpc")
        assert cached is not None
        assert cached.backend == "hpc"

    async def test_crashed_task_caches_error_result(self):
        svc = DiskScanService()

        async def boom():
            raise ValueError("kaput")

        svc.start_scan("hpc", boom())
        while svc.is_scanning("hpc"):
            await asyncio.sleep(0.01)
        result = svc.get_cached("hpc")
        assert result is not None
        assert any("kaput" in e for e in result.errors)


# -- /api/v1/disk routes -----------------------------------------------------


def _backend_state(backend_type: str = "slurm", clone_dir: str = "~/scripthut-repos"):
    bs = MagicMock()
    bs.backend_type = backend_type
    bs.clone_dir = clone_dir
    return bs


def _make_state(run_manager=None, config=None, backends=None):
    state = MagicMock()
    state.run_manager = run_manager
    state.config = config
    state.config_error = None
    state.backends = backends if backends is not None else {}
    state.run_storage = None
    state.disk_service = DiskScanService()
    state.notify_poll = MagicMock()
    return state


def _client(state) -> TestClient:
    app = FastAPI()
    app.include_router(make_api_router(state))
    return TestClient(app)


class TestDiskRoutes:
    def test_get_disk_empty_cache(self):
        state = _make_state(
            backends={"hpc": _backend_state(), "cloud": _backend_state("ec2")}
        )
        resp = _client(state).get("/api/v1/disk")
        assert resp.status_code == 200
        body = resp.json()
        # only SSH backends are listed
        assert set(body["backends"]) == {"hpc"}
        assert body["backends"]["hpc"] == {
            "scanning": False,
            "cleaning": False,
            "result": None,
            "last_cleanup": None,
        }

    def test_get_disk_unknown_backend_404(self):
        state = _make_state(backends={"hpc": _backend_state()})
        assert _client(state).get("/api/v1/disk?backend=nope").status_code == 404

    def test_scan_unknown_backend_404(self):
        state = _make_state(
            config=ScriptHutConfig(), backends={"hpc": _backend_state()}
        )
        assert _client(state).post("/api/v1/disk/scan?backend=nope").status_code == 404

    def test_scan_no_config_503(self):
        state = _make_state(backends={"hpc": _backend_state()})
        state.config = None
        assert _client(state).post("/api/v1/disk/scan?backend=hpc").status_code == 503

    def test_scan_no_ssh_503(self):
        rm = MagicMock()
        rm.get_ssh_client = MagicMock(return_value=None)
        state = _make_state(
            run_manager=rm,
            config=ScriptHutConfig(),
            backends={"hpc": _backend_state()},
        )
        assert _client(state).post("/api/v1/disk/scan?backend=hpc").status_code == 503

    def test_scan_started(self):
        rm = MagicMock()
        rm.runs = {}
        rm.get_ssh_client = MagicMock(return_value=_ssh_returning(SCAN_STDOUT))
        state = _make_state(
            run_manager=rm,
            config=ScriptHutConfig(),
            backends={"hpc": _backend_state()},
        )
        resp = _client(state).post("/api/v1/disk/scan?backend=hpc")
        assert resp.status_code == 200
        assert resp.json() == {"backend": "hpc", "status": "started"}


# -- cleanup service ---------------------------------------------------------

from datetime import UTC, datetime  # noqa: E402

from scripthut.disk.classify import RunReferences  # noqa: E402
from scripthut.disk.cleanup import plan_cleanup  # noqa: E402
from scripthut.disk.models import DiskEntry, DiskScanResult  # noqa: E402

HOME = "/home/alice"
CLONE_PATH = f"{HOME}/scripthut-repos/aaaaaaaaaaaa"
AGENT_PATH = f"{HOME}/scripthut-repos/agent-11111111"
NOW = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)

CLEAN_SPEC = ScanSpec(
    backend="hpc",
    clone_dirs=["~/scripthut-repos"],
    stack_dirs=[],
    log_roots=["~/.cache/scripthut/logs"],
)


def _scan_result(entries=None) -> DiskScanResult:
    if entries is None:
        entries = [
            DiskEntry(path=CLONE_PATH, kind=DiskEntryKind.CLONE, size_bytes=2048),
            DiskEntry(path=AGENT_PATH, kind=DiskEntryKind.AGENT, size_bytes=1024),
        ]
    return DiskScanResult(
        backend="hpc", scanned_at=NOW, duration_ms=1, home_dir=HOME, entries=entries,
    )


def _bulk_plan(result=None):
    result = result or _scan_result()
    return plan_cleanup(
        result, RunReferences(), spec=CLEAN_SPEC, current_stack_hashes=None,
        planned_at=NOW,
    )


AGENT_CLEAN_OUT = f"CHECK\t{AGENT_PATH}\tclean\t-\n"
AGENT_DIRTY_OUT = f"CHECK\t{AGENT_PATH}\tdirty\t-\n"
DELETE_OK_OUT = f"DEL\t{CLONE_PATH}\tOK\t-\nDEL\t{AGENT_PATH}\tOK\t-\n"


def _ssh_sequence(*stdouts: str) -> AsyncMock:
    ssh = AsyncMock()
    ssh.run_command = AsyncMock(side_effect=[(s, "", 0) for s in stdouts])
    return ssh


class TestCleanupService:
    async def test_clean_backend_end_to_end(self):
        svc = DiskScanService()
        plan = _bulk_plan()
        ssh = _ssh_sequence(AGENT_CLEAN_OUT, DELETE_OK_OUT, SCAN_STDOUT)
        from scripthut.config_schema import ScriptHutConfig as _Cfg

        assert svc.start_clean(
            "hpc",
            svc.clean_backend(
                plan=plan, spec=CLEAN_SPEC, ssh=ssh,
                run_manager=None, run_storage=None, config=_Cfg(),
            ),
        )
        assert svc.is_cleaning("hpc") and not svc.is_scanning("hpc")
        while svc.is_busy("hpc"):
            await asyncio.sleep(0.01)
        report = svc.get_last_cleanup("hpc")
        assert report is not None
        assert report.counts == {"deleted": 2, "skipped": 0, "failed": 0}
        assert report.freed_bytes == 3072
        # the auto-rescan replaced the cached result
        rescan = svc.get_cached("hpc")
        assert rescan is not None and rescan.home_dir == "/home/alice"

    async def test_dirty_agent_skipped(self):
        plan = _bulk_plan()
        ssh = _ssh_sequence(AGENT_DIRTY_OUT, f"DEL\t{CLONE_PATH}\tOK\t-\n")
        from scripthut.disk.service import execute_cleanup

        report = await execute_cleanup(plan, ssh)
        by_path = {o.path: o for o in report.outcomes}
        assert by_path[AGENT_PATH].outcome == "skipped"
        assert "uncommitted" in by_path[AGENT_PATH].reason
        assert by_path[CLONE_PATH].outcome == "deleted"

    async def test_delete_ssh_failure_marks_failed(self):
        plan = _bulk_plan(
            _scan_result([DiskEntry(path=CLONE_PATH, kind=DiskEntryKind.CLONE)])
        )
        ssh = AsyncMock()
        ssh.run_command = AsyncMock(side_effect=RuntimeError("Command timed out"))
        from scripthut.disk.service import execute_cleanup

        report = await execute_cleanup(plan, ssh)
        assert report.outcomes[0].outcome == "failed"
        assert report.errors and "delete script failed" in report.errors[0]

    async def test_missing_delete_response_is_failed_not_deleted(self):
        plan = _bulk_plan(
            _scan_result([DiskEntry(path=CLONE_PATH, kind=DiskEntryKind.CLONE)])
        )
        ssh = _ssh_sequence("")  # script returned nothing
        from scripthut.disk.service import execute_cleanup

        report = await execute_cleanup(plan, ssh)
        assert report.outcomes[0].outcome == "failed"
        assert "no response" in report.outcomes[0].reason

    async def test_scan_and_clean_mutually_exclusive(self):
        svc = DiskScanService()
        release = asyncio.Event()

        async def hold():
            await release.wait()
            return _scan_result()

        assert svc.start_scan("hpc", hold()) is True
        assert svc.start_clean("hpc", hold()) is False  # clean refused during scan
        release.set()
        while svc.is_busy("hpc"):
            await asyncio.sleep(0.01)

        release.clear()
        assert svc.start_clean("hpc", hold()) is True
        assert svc.start_scan("hpc", hold()) is False  # scan refused during clean
        assert svc.is_cleaning("hpc")
        release.set()
        while svc.is_busy("hpc"):
            await asyncio.sleep(0.01)

    async def test_report_survives_later_scans(self):
        svc = DiskScanService()
        plan = _bulk_plan(
            _scan_result([DiskEntry(path=CLONE_PATH, kind=DiskEntryKind.CLONE)])
        )
        ssh = _ssh_sequence(DELETE_OK_OUT, SCAN_STDOUT)
        from scripthut.config_schema import ScriptHutConfig as _Cfg

        svc.start_clean("hpc", svc.clean_backend(
            plan=plan, spec=CLEAN_SPEC, ssh=ssh,
            run_manager=None, run_storage=None, config=_Cfg(),
        ))
        while svc.is_busy("hpc"):
            await asyncio.sleep(0.01)
        report = svc.get_last_cleanup("hpc")
        # a later plain scan must not clobber the report
        await_result = await svc.scan_backend(
            spec=CLEAN_SPEC, ssh=_ssh_returning(SCAN_STDOUT), runs=[]
        )
        svc._results["hpc"] = await_result
        assert svc.get_last_cleanup("hpc") is report


# -- /api/v1/disk/clean route ------------------------------------------------


class TestCleanRoute:
    def _state_with_scan(self, ssh=None):
        rm = MagicMock()
        rm.runs = {}
        rm.get_ssh_client = MagicMock(return_value=ssh or _ssh_returning(""))
        state = _make_state(
            run_manager=rm,
            config=ScriptHutConfig(),
            backends={"hpc": _backend_state()},
        )
        state.disk_service._results["hpc"] = _scan_result()
        return state

    def test_dry_run_returns_plan_and_starts_nothing(self):
        state = self._state_with_scan()
        resp = _client(state).post(
            "/api/v1/disk/clean",
            json={"backend": "hpc", "dry_run": True},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["dry_run"] is True
        assert body["plan"]["counts"] == {"delete": 1, "check": 1, "skip": 0}
        assert state.disk_service.is_busy("hpc") is False

    def test_no_cached_scan_409(self):
        state = self._state_with_scan()
        state.disk_service._results.clear()
        resp = _client(state).post(
            "/api/v1/disk/clean", json={"backend": "hpc", "dry_run": True},
        )
        assert resp.status_code == 409

    def test_unknown_path_400(self):
        state = self._state_with_scan()
        resp = _client(state).post(
            "/api/v1/disk/clean",
            json={"backend": "hpc", "paths": ["/not/in/scan"]},
        )
        assert resp.status_code == 400
        assert "not in the last scan" in resp.json()["detail"]

    def test_unknown_backend_404(self):
        state = self._state_with_scan()
        resp = _client(state).post(
            "/api/v1/disk/clean", json={"backend": "nope"},
        )
        assert resp.status_code == 404

    def test_started(self):
        ssh = _ssh_sequence(AGENT_CLEAN_OUT, DELETE_OK_OUT, SCAN_STDOUT)
        state = self._state_with_scan(ssh=ssh)
        resp = _client(state).post("/api/v1/disk/clean", json={"backend": "hpc"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "started"
        assert body["planned"]["delete"] == 1 and body["planned"]["check"] == 1

    def test_nothing_to_clean(self):
        state = self._state_with_scan()
        state.disk_service._results["hpc"] = _scan_result(entries=[])
        resp = _client(state).post("/api/v1/disk/clean", json={"backend": "hpc"})
        assert resp.status_code == 200
        assert resp.json()["status"] == "nothing_to_clean"
