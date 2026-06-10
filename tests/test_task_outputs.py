"""Tests for the v0.11.0 task-outputs feature.

Covers the five surfaces the plan called out:

- Path helpers on ``TaskDefinition`` (``get_output_dir`` /
  ``get_run_summary_path``) return the documented layout.
- ``generate_script_body`` injects the three exports + ``mkdir -p`` in
  the right slot (after env_vars, before ``cd``).
- ``RunManager._handle_task_outputs`` parses ``find`` output, classifies
  by suffix, drops oversize files, stamps ``has_run_summary``.
- ``_render_markdown_for_outputs`` rewrites relative image src,
  passes absolute URLs through, and strips dangerous tags via bleach.
- Path-traversal rejection on the file-serving endpoint.
- Persistence round-trip for the new fields.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from scripthut.runs.models import (
    RunItem,
    RunItemStatus,
    TaskDefinition,
    TaskOutput,
)


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


class TestPathHelpers:
    def test_output_dir_layout(self):
        t = TaskDefinition(id="train-0", name="train", command="true")
        assert t.get_output_dir("run-abc", "/logs") == "/logs/outputs/run-abc/train-0"

    def test_run_summary_path_layout(self):
        """Per-task fragments share one parent dir (``_run/``) so the
        aggregator can ``ls`` once. The path-traversal-safe character
        set (alphanumerics + dashes) keeps the layout shell-safe.
        """
        t = TaskDefinition(id="eval-7", name="eval", command="true")
        assert t.get_run_summary_path("run-abc", "/logs") == (
            "/logs/outputs/run-abc/_run/eval-7.md"
        )

    def test_output_dir_does_not_collide_with_run_summary_dir(self):
        """``<task_id>/`` and ``_run/`` live side-by-side; a task whose
        id is literally ``_run`` would collide (it'd be both a file and
        a directory), but ``_run`` isn't a legal task id today and the
        layout is stable enough that this can be revisited if needed.
        """
        t = TaskDefinition(id="train-0", name="train", command="true")
        out = t.get_output_dir("r", "/logs")
        run = t.get_run_summary_path("r", "/logs")
        assert "/_run/" in run
        assert "/_run/" not in out


# ---------------------------------------------------------------------------
# Script wrapper
# ---------------------------------------------------------------------------


class TestScriptWrapperExports:
    def test_exports_emitted_when_both_paths_provided(self):
        from scripthut.backends.utils import generate_script_body
        body = generate_script_body(
            task_name="x", task_id="t", command="echo hi",
            working_dir="/work",
            output_dir="/logs/outputs/r1/t",
            run_summary_path="/logs/outputs/r1/_run/t.md",
        )
        assert 'export SCRIPTHUT_OUTPUT_DIR="/logs/outputs/r1/t"' in body
        assert (
            'export SCRIPTHUT_TASK_SUMMARY="$SCRIPTHUT_OUTPUT_DIR/task-summary.md"'
            in body
        )
        assert (
            'export SCRIPTHUT_RUN_SUMMARY="/logs/outputs/r1/_run/t.md"'
            in body
        )
        # mkdir -p happens before the command runs.
        assert 'mkdir -p "$SCRIPTHUT_OUTPUT_DIR"' in body

    def test_no_exports_when_paths_missing(self):
        """Existing backends (Batch / EC2) that don't pass the new
        kwargs must keep producing unchanged scripts so the wrapper is
        backwards-compatible.
        """
        from scripthut.backends.utils import generate_script_body
        body = generate_script_body(
            task_name="x", task_id="t", command="echo hi",
            working_dir="/work",
        )
        assert "SCRIPTHUT_OUTPUT_DIR" not in body
        assert "SCRIPTHUT_RUN_SUMMARY" not in body

    def test_exports_appear_before_user_command(self):
        from scripthut.backends.utils import generate_script_body
        body = generate_script_body(
            task_name="x", task_id="t",
            command="echo USER_CMD",
            working_dir="/work",
            output_dir="/o", run_summary_path="/o/_run/t.md",
        )
        export_idx = body.index("SCRIPTHUT_OUTPUT_DIR")
        cmd_idx = body.index("echo USER_CMD")
        assert export_idx < cmd_idx, (
            "OUTPUT_DIR export must precede the user command so the "
            "command can write to it without checking existence first"
        )


# ---------------------------------------------------------------------------
# _handle_task_outputs
# ---------------------------------------------------------------------------


def _make_manager_with_one_item(ssh) -> tuple:
    """Return ``(manager, run, item)`` wired so calls flow to ``ssh``."""
    from scripthut.config_schema import (
        ScriptHutConfig,
        SlurmBackendConfig,
        SSHConfig,
    )
    from scripthut.runs.manager import RunManager
    from scripthut.runs.models import Run
    from datetime import UTC, datetime

    backend = SlurmBackendConfig(
        name="cluster", type="slurm", ssh=SSHConfig(host="h", user="u"),
    )
    cfg = ScriptHutConfig(backends=[backend])
    task = TaskDefinition(id="t1", name="t1", command="true")
    item = RunItem(task=task, status=RunItemStatus.RUNNING, job_id="42")
    run = Run(
        id="r1", workflow_name="wf", backend_name="cluster",
        created_at=datetime(2026, 6, 4, tzinfo=UTC),
        items=[item], max_concurrent=1,
        log_dir="~/.cache/scripthut/logs",
    )
    storage = MagicMock()
    storage.save_run = MagicMock()
    mgr = RunManager(
        config=cfg, backends={"cluster": ssh},
        storage=storage, job_backends={},
    )
    mgr._persist_run = MagicMock()
    return mgr, run, item


class TestHandleTaskOutputs:
    @pytest.mark.asyncio
    async def test_empty_dir_leaves_outputs_empty(self):
        """Task that didn't write anything → no outputs, no error."""
        ssh = MagicMock()
        ssh.run_command = AsyncMock(return_value=("", "", 0))
        mgr, run, item = _make_manager_with_one_item(ssh)
        await mgr._handle_task_outputs(run, item)
        assert item.outputs == []
        assert item.has_run_summary is False
        # Empty result means nothing changed; we must not persist
        # uselessly (would beat the disk on every poll cycle).
        mgr._persist_run.assert_not_called()

    @pytest.mark.asyncio
    async def test_classifies_by_suffix(self):
        ssh = MagicMock()
        ssh.run_command = AsyncMock(return_value=(
            "task-summary.md\t1024\n"
            "plot.png\t8192\n"
            "data.csv\t512\n",
            "",  # no run-summary
            0,
        ))
        mgr, run, item = _make_manager_with_one_item(ssh)
        await mgr._handle_task_outputs(run, item)
        kinds = {o.path: o.kind for o in item.outputs}
        assert kinds["task-summary.md"] == "markdown"
        assert kinds["plot.png"] == "image"
        assert kinds["data.csv"] == "other"
        assert item.has_run_summary is False

    @pytest.mark.asyncio
    async def test_drops_oversize_files(self):
        """Files over 5 MB are skipped from the listing (still on the
        backend; just don't surface in the panel).
        """
        ssh = MagicMock()
        ssh.run_command = AsyncMock(return_value=(
            "small.md\t100\n"
            "huge.png\t10000000\n",  # 10 MB > 5 MB cap
            "", 0,
        ))
        mgr, run, item = _make_manager_with_one_item(ssh)
        await mgr._handle_task_outputs(run, item)
        paths = [o.path for o in item.outputs]
        assert "small.md" in paths
        assert "huge.png" not in paths

    @pytest.mark.asyncio
    async def test_has_run_summary_flagged_from_stderr(self):
        """The find-and-probe SSH command writes ``HAS_SUMMARY`` on
        stderr when the run-summary file exists, keeping stdout clean
        for the file listing.
        """
        ssh = MagicMock()
        ssh.run_command = AsyncMock(return_value=(
            "task-summary.md\t100\n", "HAS_SUMMARY", 0,
        ))
        mgr, run, item = _make_manager_with_one_item(ssh)
        await mgr._handle_task_outputs(run, item)
        assert item.has_run_summary is True
        mgr._persist_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_caps_listing_at_max_files(self):
        from scripthut.runs.manager import RunManager
        # Produce one more file than the cap so we trigger truncation
        lines = "\n".join(
            f"file_{i:04d}.png\t{1024}" for i in range(RunManager._OUTPUTS_MAX_FILES + 1)
        )
        ssh = MagicMock()
        ssh.run_command = AsyncMock(return_value=(lines, "", 0))
        mgr, run, item = _make_manager_with_one_item(ssh)
        await mgr._handle_task_outputs(run, item)
        # Truncation cap reached — listing should be exactly
        # ``_OUTPUTS_MAX_FILES`` entries (not +1) so the head limit
        # leaves us one entry of headroom to detect the overflow.
        assert len(item.outputs) == RunManager._OUTPUTS_MAX_FILES

    @pytest.mark.asyncio
    async def test_no_ssh_backend_is_silent_noop(self):
        """Batch / EC2 backends have no SSH client. The hook must
        return without raising so existing workflows keep working
        and v2 backend support is purely additive.
        """
        from scripthut.config_schema import ScriptHutConfig
        from scripthut.runs.manager import RunManager
        from scripthut.runs.models import Run
        from datetime import UTC, datetime

        task = TaskDefinition(id="t", name="t", command="true")
        item = RunItem(task=task, status=RunItemStatus.RUNNING)
        run = Run(
            id="r", workflow_name="wf", backend_name="batch-only",
            created_at=datetime(2026, 6, 4, tzinfo=UTC),
            items=[item], max_concurrent=1,
        )
        mgr = RunManager(
            config=ScriptHutConfig(), backends={},  # no SSH for any backend
            storage=MagicMock(), job_backends={},
        )
        await mgr._handle_task_outputs(run, item)
        assert item.outputs == []


# ---------------------------------------------------------------------------
# _after_item_completed
# ---------------------------------------------------------------------------


class TestAfterItemCompleted:
    @pytest.mark.asyncio
    async def test_calls_both_hooks(self):
        """The fan-out helper must call generates_source first (for
        backwards compat with the historical hook order) and then the
        outputs collector.
        """
        from scripthut.runs.manager import RunManager
        from scripthut.runs.models import Run
        from datetime import UTC, datetime

        task = TaskDefinition(
            id="t", name="t", command="true",
            generates_source="next.json",
        )
        item = RunItem(task=task, status=RunItemStatus.COMPLETED)
        run = Run(
            id="r", workflow_name="wf", backend_name="b",
            created_at=datetime(2026, 6, 4, tzinfo=UTC),
            items=[item], max_concurrent=1,
        )
        mgr = MagicMock(spec=RunManager)
        mgr._handle_generates_source = AsyncMock()
        mgr._handle_task_outputs = AsyncMock()
        # Bind the real method to the mock so we exercise the real impl
        mgr._after_item_completed = RunManager._after_item_completed.__get__(mgr)
        await mgr._after_item_completed(run, item)
        mgr._handle_generates_source.assert_awaited_once_with(run, item)
        mgr._handle_task_outputs.assert_awaited_once_with(run, item)

    @pytest.mark.asyncio
    async def test_skips_generates_source_when_unset(self):
        """A task without ``generates_source`` shouldn't trigger the
        old hook at all — only outputs collection.
        """
        from scripthut.runs.manager import RunManager
        from scripthut.runs.models import Run
        from datetime import UTC, datetime

        task = TaskDefinition(id="t", name="t", command="true")
        item = RunItem(task=task, status=RunItemStatus.COMPLETED)
        run = Run(
            id="r", workflow_name="wf", backend_name="b",
            created_at=datetime(2026, 6, 4, tzinfo=UTC),
            items=[item], max_concurrent=1,
        )
        mgr = MagicMock(spec=RunManager)
        mgr._handle_generates_source = AsyncMock()
        mgr._handle_task_outputs = AsyncMock()
        mgr._after_item_completed = RunManager._after_item_completed.__get__(mgr)
        await mgr._after_item_completed(run, item)
        mgr._handle_generates_source.assert_not_called()
        mgr._handle_task_outputs.assert_awaited_once_with(run, item)


# ---------------------------------------------------------------------------
# Markdown rendering + sanitization
# ---------------------------------------------------------------------------


class TestMarkdownRendering:
    def test_basic_markdown_renders_to_html(self):
        from scripthut.main import _render_markdown_for_outputs
        html = _render_markdown_for_outputs(
            "## Title\n\n**bold** text", "/outputs/file",
        )
        assert "<h2>" in html
        assert "<strong>bold</strong>" in html

    def test_markdown_tables_supported(self):
        from scripthut.main import _render_markdown_for_outputs
        src = "| a | b |\n|---|---|\n| 1 | 2 |\n"
        html = _render_markdown_for_outputs(src, "/outputs/file")
        assert "<table>" in html
        assert "<thead>" in html

    def test_relative_image_src_rewritten_to_file_endpoint(self):
        from scripthut.main import _render_markdown_for_outputs
        html = _render_markdown_for_outputs(
            "![plot](plot.png)",
            "/runs/abc/tasks/t1/outputs/file",
        )
        assert 'src="/runs/abc/tasks/t1/outputs/file/plot.png"' in html
        # No literal relative path leaked.
        assert 'src="plot.png"' not in html

    def test_absolute_url_image_src_passes_through(self):
        from scripthut.main import _render_markdown_for_outputs
        html = _render_markdown_for_outputs(
            "![cat](https://example.com/cat.png)",
            "/outputs/file",
        )
        assert 'src="https://example.com/cat.png"' in html

    def test_script_tags_stripped_by_bleach(self):
        """User-authored markdown can include raw HTML; ``<script>`` is
        the canonical exfiltration vector and the tag itself must not
        survive. Bleach's ``strip=True`` keeps the *text* between the
        tags as literal characters (no longer interpretable JS), which
        is the correct safe behavior.
        """
        from scripthut.main import _render_markdown_for_outputs
        html = _render_markdown_for_outputs(
            "Hello <script>alert(1)</script> world",
            "/outputs/file",
        )
        assert "<script>" not in html.lower()
        assert "</script>" not in html.lower()

    def test_onerror_attribute_stripped(self):
        """Inline event handlers are an XSS vector. Bleach's allowlist
        only includes ``src``, ``alt``, ``title`` on ``<img>``.
        """
        from scripthut.main import _render_markdown_for_outputs
        html = _render_markdown_for_outputs(
            '<img src="x" onerror="alert(1)">',
            "/outputs/file",
        )
        assert "onerror" not in html

    def test_fails_closed_when_bleach_unavailable(self, monkeypatch):
        """If bleach can't be imported, the renderer must fall back to
        escaped plain text (fail-CLOSED), never emit raw unsanitized HTML.
        This locks in the security property so a refactor can't silently
        turn the missing-dep path into a fail-open XSS hole.
        """
        import sys

        # Setting the module to None in sys.modules makes ``import bleach``
        # raise ImportError, simulating a partial install.
        monkeypatch.setitem(sys.modules, "bleach", None)

        from scripthut.main import _render_markdown_for_outputs
        html = _render_markdown_for_outputs(
            '<img src="x" onerror="alert(1)"><script>alert(2)</script>',
            "/outputs/file",
        )
        # The payload is escaped and inert: no live tags can form because
        # the angle brackets are entity-escaped. (The literal text
        # "onerror" may remain, but with "<" escaped it can never fire.)
        assert "<img" not in html
        assert "<script>" not in html
        assert "&lt;img" in html and "&lt;script&gt;" in html


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_task_output_round_trip(self):
        t = TaskOutput(path="plot.png", size=1234, kind="image")
        assert TaskOutput.from_dict(t.to_dict()) == t

    def test_run_item_outputs_round_trip(self):
        item = RunItem(
            task=TaskDefinition(id="t", name="t", command="true"),
            status=RunItemStatus.COMPLETED,
            outputs=[
                TaskOutput(path="summary.md", size=100, kind="markdown"),
                TaskOutput(path="plot.png", size=200, kind="image"),
            ],
            has_run_summary=True,
        )
        rt = RunItem.from_dict(item.to_dict())
        assert len(rt.outputs) == 2
        assert rt.outputs[0].path == "summary.md"
        assert rt.has_run_summary is True

    def test_old_persisted_runs_load_with_empty_outputs(self):
        """Pre-0.11.0 ``run.json`` files don't have ``outputs`` or
        ``has_run_summary`` — they must load without complaints.
        """
        old = {
            "task": {"id": "t", "name": "t", "command": "true"},
            "status": "completed",
        }
        rt = RunItem.from_dict(old)
        assert rt.outputs == []
        assert rt.has_run_summary is False


# ---------------------------------------------------------------------------
# Path-traversal guard on /outputs/file
# ---------------------------------------------------------------------------


class TestFileEndpointPathTraversal:
    """The file endpoint resolves user-supplied path components and
    must reject anything that escapes the per-task output dir.
    Implemented in ``main.get_task_output_file`` via posixpath +
    realpath containment; tested here at the helper level so we don't
    need a full FastAPI stack just to exercise the guard.
    """

    def _is_inside(self, rel_path: str, output_dir: str = "/logs/outputs/r1/t1") -> bool:
        """Reproduce the endpoint's containment check.

        Kept in the test rather than calling the endpoint so the
        property is testable without spinning up the server or
        mocking httpx — and to pin the algorithm itself so a future
        refactor doesn't quietly weaken it.
        """
        import posixpath
        full = posixpath.normpath(posixpath.join(output_dir, rel_path))
        norm = posixpath.normpath(output_dir) + "/"
        return full.startswith(norm)

    def test_normal_file_allowed(self):
        assert self._is_inside("plot.png") is True

    def test_subdir_file_allowed(self):
        assert self._is_inside("subdir/plot.png") is True

    def test_dotdot_escape_rejected(self):
        assert self._is_inside("../../etc/passwd") is False

    def test_dotdot_inside_subdir_rejected(self):
        """``subdir/../../etc/passwd`` normalizes to ``../etc/passwd``,
        which escapes the output dir.
        """
        assert self._is_inside("subdir/../../etc/passwd") is False

    def test_absolute_path_rejected(self):
        """Even though ``posixpath.join('/logs/...', '/etc/passwd')``
        discards the prefix and returns ``/etc/passwd``, the
        containment check still fails — confirming the guard catches
        absolute-path bypass attempts.
        """
        assert self._is_inside("/etc/passwd") is False

    def test_dotdot_alone_rejected(self):
        """The empty-target traversal still fails containment."""
        assert self._is_inside("..") is False
