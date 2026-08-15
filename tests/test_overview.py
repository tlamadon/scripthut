"""Tests for the landing page and the run→source attribution behind it.

``/`` is now an overview of what is active; the backends dashboard it used
to render moved to ``/backends``. The overview groups and labels runs by
source, which is only possible because ``Run.source_name`` is recorded at
creation time — runs written before that field existed fall back to
deriving it from the workflow name.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import scripthut.main as main_module
from scripthut.runs.models import (
    Run,
    RunItem,
    RunItemStatus,
    TaskDefinition,
    derive_source_name,
)
from scripthut.runs.storage import RunStorageManager


def _run(
    run_id: str = "r1",
    workflow_name: str = "demo/train",
    source_name: str | None = "demo",
    statuses: list[RunItemStatus] | None = None,
    backend_name: str = "hpc",
    cpus: int = 4,
) -> Run:
    statuses = statuses or [RunItemStatus.RUNNING]
    items = [
        RunItem(
            task=TaskDefinition(id=f"t{i}", name=f"Task {i}", command="echo hi", cpus=cpus),
            status=s,
        )
        for i, s in enumerate(statuses)
    ]
    return Run(
        id=run_id,
        workflow_name=workflow_name,
        backend_name=backend_name,
        created_at=datetime.now(timezone.utc),
        items=items,
        max_concurrent=None,
        source_name=source_name,
    )


class TestDeriveSourceName:
    """Legacy fallback for runs written before ``Run.source_name`` existed."""

    @pytest.mark.parametrize(
        "workflow_name,expected",
        [
            ("demo/train", "demo"),
            ("demo/nested/train", "demo"),
            ("_agent/demo/session-a", "demo"),
            ("_stack/demo/julia", "demo"),
            # No source was recorded in these shapes — better to say
            # "unknown" than to invent one.
            ("_stack/julia", None),
            ("_adhoc/t1", None),
            ("_probe", None),
            ("_default", None),
            ("bare-workflow", None),
        ],
    )
    def test_derivation(self, workflow_name: str, expected: str | None):
        assert derive_source_name(workflow_name) == expected


class TestSourceNamePersistence:
    def test_roundtrips_through_storage(self, tmp_path: Path):
        storage = RunStorageManager(base_dir=tmp_path)
        storage.save_run(_run(source_name="demo"))

        restored = storage.load_all_runs()["r1"]
        assert restored.source_name == "demo"

    def test_explicit_none_survives_reload(self, tmp_path: Path):
        """An ad-hoc run must not acquire a source on the way back in."""
        storage = RunStorageManager(base_dir=tmp_path)
        storage.save_run(
            _run(workflow_name="_adhoc/t1", source_name=None)
        )

        assert storage.load_all_runs()["r1"].source_name is None

    def test_legacy_run_json_gets_derived_source(self, tmp_path: Path):
        """A run.json written before the field existed still groups correctly."""
        storage = RunStorageManager(base_dir=tmp_path)
        storage.save_run(_run(workflow_name="demo/train", source_name="demo"))

        # Strip the field to simulate a file written by an older version.
        run_json = next(tmp_path.glob("*/*/run.json"))
        data = json.loads(run_json.read_text())
        del data["source_name"]
        run_json.write_text(json.dumps(data))

        assert storage.load_all_runs()["r1"].source_name == "demo"


class TestDisplayName:
    def test_strips_redundant_source_prefix(self):
        assert _run(workflow_name="demo/train", source_name="demo").display_name == "train"

    def test_keeps_name_when_prefix_absent(self):
        # _agent/_stack names identify the run kind — stripping would lose that.
        run = _run(workflow_name="_agent/demo/sess", source_name="demo")
        assert run.display_name == "_agent/demo/sess"

    def test_keeps_name_when_no_source(self):
        assert _run(workflow_name="_adhoc/t1", source_name=None).display_name == "_adhoc/t1"


class TestBackendUsage:
    """Running and queued CPUs are tallied separately for the backend cards."""

    @pytest.fixture
    def with_runs(self):
        rm = MagicMock()
        rm.runs = {
            "r1": _run(
                "r1",
                statuses=[
                    RunItemStatus.RUNNING,
                    RunItemStatus.QUEUED,
                    RunItemStatus.SUBMITTED,
                    RunItemStatus.PENDING,
                    RunItemStatus.COMPLETED,
                ],
                cpus=4,
            ),
        }
        original = main_module.state.run_manager
        main_module.state.run_manager = rm
        try:
            yield
        finally:
            main_module.state.run_manager = original

    def test_splits_running_from_queued(self, with_runs):
        usage = main_module._backend_usage()["hpc"]

        assert usage["cpus"] == 4          # one RUNNING item
        assert usage["cpus_queued"] == 8   # QUEUED + SUBMITTED
        assert usage["running"] == 1
        assert usage["queued"] == 2
        # PENDING is scripthut-side (deps / concurrency cap), not the
        # scheduler's queue, so it contributes to neither CPU total.
        assert usage["pending"] == 1
        # COMPLETED is excluded entirely; the other four are active.
        assert usage["jobs"] == 4


class TestOverviewRoutes:
    @pytest.fixture
    def client(self):
        original = main_module.state.run_manager
        rm = MagicMock()
        runs = [
            _run("act1", "demo/train", "demo", [RunItemStatus.RUNNING]),
            _run("done1", "demo/eval", "demo", [RunItemStatus.COMPLETED]),
        ]
        rm.get_all_runs.return_value = runs
        rm.runs = {r.id: r for r in runs}
        main_module.state.run_manager = rm
        try:
            yield TestClient(main_module.app)
        finally:
            main_module.state.run_manager = original

    def test_root_renders_overview_not_the_job_table(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "/runs/act1" in resp.text
        assert 'sse-connect="/overview/stream"' in resp.text
        # The card is labelled with its source, and the heading shows the
        # workflow with the redundant "demo/" prefix stripped.
        assert "Source: demo" in resp.text
        assert ">\n                train\n" in resp.text

    def test_active_and_finished_runs_get_their_own_sections(self, client):
        """Finished work stays visible while something is running."""
        resp = client.get("/")
        assert "/runs/act1" in resp.text
        assert "/runs/done1" in resp.text
        assert "Current runs" in resp.text
        assert "Recent runs" in resp.text

    def test_section_order_is_activity_backends_current_recent(self, client):
        resp = client.get("/")
        positions = [
            resp.text.index("Activity ·"),
            resp.text.index(">Backends<"),
            resp.text.index("Current runs"),
            resp.text.index("Recent runs"),
        ]
        assert positions == sorted(positions)

    def test_idle_page_still_shows_the_current_runs_section(self):
        """The section stays put when nothing is running, rather than vanishing."""
        original = main_module.state.run_manager
        rm = MagicMock()
        runs = [_run("done1", "demo/train", "demo", [RunItemStatus.COMPLETED])]
        rm.get_all_runs.return_value = runs
        rm.runs = {r.id: r for r in runs}
        main_module.state.run_manager = rm
        try:
            resp = TestClient(main_module.app).get("/")
        finally:
            main_module.state.run_manager = original

        assert "Current runs" in resp.text
        assert "Nothing running right now" in resp.text
        assert "/runs/done1" in resp.text  # history still listed below

    def test_fresh_install_gets_the_onboarding_state_once(self):
        """No runs at all: one empty state, not two stacked headers."""
        original = main_module.state.run_manager
        rm = MagicMock()
        rm.get_all_runs.return_value = []
        rm.runs = {}
        main_module.state.run_manager = rm
        try:
            resp = TestClient(main_module.app).get("/")
        finally:
            main_module.state.run_manager = original

        assert "Nothing has run yet" in resp.text
        assert "Nothing running right now" not in resp.text
        assert "Recent runs" not in resp.text

    def test_backends_dashboard_moved_off_the_root(self, client):
        resp = client.get("/backends")
        assert resp.status_code == 200
        assert "ScriptHut - Backends" in resp.text

    def test_nav_highlights_overview_not_backends(self, client):
        """The old nav used a negative-match chain that got this wrong."""
        resp = client.get("/")
        assert 'href="/backends"' in resp.text
        # Exactly one nav entry carries the active-tab styling per menu
        # (desktop + mobile), and on "/" it must be Overview.
        active = resp.text.count("bg-blue-100 text-blue-700")
        assert active == 2
        overview_link, backends_link = (
            resp.text.split('href="/backends"')[0].rsplit('href="/"', 1)[-1],
            resp.text.split('href="/backends"')[1][:200],
        )
        assert "bg-blue-100 text-blue-700" in overview_link
        assert "bg-blue-100 text-blue-700" not in backends_link


class TestBulkDelete:
    """POST /runs/delete removes a selection and re-renders the list."""

    @pytest.fixture
    def rm(self):
        original = main_module.state.run_manager
        manager = MagicMock()
        main_module.state.run_manager = manager
        try:
            yield manager
        finally:
            main_module.state.run_manager = original

    def test_deletes_each_selected_run(self, rm):
        rm.delete_run.return_value = True
        rm.get_all_runs.return_value = []

        resp = TestClient(main_module.app).post(
            "/runs/delete", data={"run_ids": ["a1", "b2", "c3"]}
        )

        assert resp.status_code == 200
        assert [c.args[0] for c in rm.delete_run.call_args_list] == ["a1", "b2", "c3"]
        assert "Deleted 3 runs" in resp.text

    def test_active_runs_are_reported_as_skipped(self, rm):
        # delete_run refuses PENDING/RUNNING runs by returning False.
        rm.delete_run.side_effect = [True, False]
        rm.get_all_runs.return_value = []

        resp = TestClient(main_module.app).post(
            "/runs/delete", data={"run_ids": ["done", "running"]}
        )

        assert "Deleted 1 run." in resp.text
        assert "1 skipped" in resp.text

    def test_empty_selection_is_a_no_op(self, rm):
        rm.get_all_runs.return_value = []

        resp = TestClient(main_module.app).post("/runs/delete", data={})

        assert resp.status_code == 200
        rm.delete_run.assert_not_called()
        assert "Deleted" not in resp.text

    def test_returns_the_refreshed_list(self, rm):
        rm.delete_run.return_value = True
        rm.get_all_runs.return_value = [
            _run("survivor", "demo/train", "demo", [RunItemStatus.COMPLETED])
        ]

        resp = TestClient(main_module.app).post("/runs/delete", data={"run_ids": ["gone"]})

        assert "/runs/survivor" in resp.text


class TestRunsPageSelection:
    @pytest.fixture
    def client(self):
        original = main_module.state.run_manager
        rm = MagicMock()
        rm.get_all_runs.return_value = [
            _run("done1", "demo/train", "demo", [RunItemStatus.COMPLETED]),
            _run("live1", "demo/nightly", "demo", [RunItemStatus.RUNNING]),
        ]
        main_module.state.run_manager = rm
        try:
            yield TestClient(main_module.app)
        finally:
            main_module.state.run_manager = original

    def test_checkbox_only_on_deletable_runs(self, client):
        """An active run gets no box — delete_run would refuse it anyway."""
        resp = client.get("/runs")
        assert 'value="done1"' in resp.text
        assert 'value="live1"' not in resp.text

    def test_select_controls_present(self, client):
        resp = client.get("/runs")
        assert 'id="select-btn"' in resp.text
        assert 'hx-post="/runs/delete"' in resp.text
        assert 'hx-include="#runs-list"' in resp.text


class TestOverviewContextFallback:
    """With nothing active the page shows recent runs instead of a blank grid."""

    def test_recent_runs_fill_an_idle_page(self):
        original = main_module.state.run_manager
        rm = MagicMock()
        runs = [_run(f"d{i}", statuses=[RunItemStatus.COMPLETED]) for i in range(10)]
        rm.get_all_runs.return_value = runs
        rm.runs = {r.id: r for r in runs}
        main_module.state.run_manager = rm
        try:
            ctx = main_module._overview_context(MagicMock())
            assert ctx["active_runs"] == []
            assert len(ctx["recent_runs"]) == main_module.OVERVIEW_RECENT_LIMIT
        finally:
            main_module.state.run_manager = original

    def test_external_job_bins_are_excluded(self):
        """``_default`` runs hold cluster jobs scripthut did not submit."""
        original = main_module.state.run_manager
        rm = MagicMock()
        runs = [
            _run("ext", workflow_name="_default", source_name=None),
            _run("mine", workflow_name="demo/train", source_name="demo"),
        ]
        rm.get_all_runs.return_value = runs
        rm.runs = {r.id: r for r in runs}
        main_module.state.run_manager = rm
        try:
            ctx = main_module._overview_context(MagicMock())
            assert [r.id for r in ctx["active_runs"]] == ["mine"]
        finally:
            main_module.state.run_manager = original
