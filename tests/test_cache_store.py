"""Tests for the object-store cache summary shown on the storage page."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from scripthut.config_schema import CacheConfig
from scripthut.disk.cache_store import (
    CachePrefixStat,
    CacheStoreStatus,
    _list_command,
    _parse,
    scan_cache_store,
)

AWS_SUMMARY = """
2026-08-01 10:00:00     123456 scripthut-cache/cas/aa11.tar.gz
Total Objects: 42
   Total Size: 1048576
"""

RCLONE_SUMMARY = """
Total objects: 42
Total size: 1 MiB (1048576 Byte)
"""


def _ssh(stdout: str = "", stderr: str = "", code: int = 0) -> MagicMock:
    ssh = MagicMock()
    ssh.run_command = AsyncMock(return_value=(stdout, stderr, code))
    return ssh


class TestCommands:
    def test_aws_listing_is_reduced_remotely(self):
        """The full listing must not cross the SSH channel."""
        cmd = _list_command("aws", "s3://b/cache/cas/")

        assert "--summarize" in cmd
        assert "tail" in cmd

    def test_rclone_uses_its_native_summary(self):
        # shlex leaves shell-safe strings unquoted, so match the shape.
        assert _list_command("rclone", "remote:b/cas/") == "rclone size remote:b/cas/"

    def test_uris_are_quoted(self):
        cmd = _list_command("aws", "s3://b/it's odd/")

        assert "'s3://b/it'\"'\"'s odd/'" in cmd


class TestParsing:
    def test_aws_summary(self):
        assert _parse("aws", AWS_SUMMARY) == (42, 1048576)

    def test_rclone_summary_uses_exact_bytes(self):
        """The human-readable size is rounded; the parenthesised one is not."""
        assert _parse("rclone", RCLONE_SUMMARY) == (42, 1048576)

    def test_unparseable_output_is_not_invented(self):
        assert _parse("aws", "something else entirely") == (None, None)


class TestScan:
    @pytest.mark.asyncio
    async def test_disabled_cache_reports_without_touching_ssh(self):
        ssh = _ssh()

        status = await scan_cache_store(
            CacheConfig(enabled=False), backend_name="hpc", ssh=ssh,
        )

        assert status.enabled is False
        assert status.prefixes == []
        ssh.run_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_config_at_all(self):
        status = await scan_cache_store(None, backend_name="hpc", ssh=_ssh())

        assert status.enabled is False

    @pytest.mark.asyncio
    async def test_summarizes_both_prefixes(self):
        ssh = _ssh(AWS_SUMMARY)

        status = await scan_cache_store(
            CacheConfig(enabled=True, store="s3://b/cache"),
            backend_name="hpc", ssh=ssh,
        )

        assert ssh.run_command.await_count == 2
        assert status.actions == 42
        assert status.blobs == 42
        assert status.blob_bytes == 1048576
        assert status.total_objects == 84
        assert status.reachable
        assert status.backend == "hpc"

    @pytest.mark.asyncio
    async def test_prefix_uris_end_in_a_slash(self):
        """Without it, `aws s3 ls .../ac` also matches a sibling like `ac2`."""
        ssh = _ssh(AWS_SUMMARY)

        await scan_cache_store(
            CacheConfig(enabled=True, store="s3://b/cache/"),
            backend_name="hpc", ssh=ssh,
        )

        commands = [c.args[0] for c in ssh.run_command.await_args_list]
        assert any("s3://b/cache/ac/" in c for c in commands)
        assert any("s3://b/cache/cas/" in c for c in commands)

    @pytest.mark.asyncio
    async def test_empty_prefix_is_zero_not_an_error(self):
        """Neither tool has anything to list before the first cache write."""
        ssh = _ssh(stdout="", stderr="", code=1)

        status = await scan_cache_store(
            CacheConfig(enabled=True, store="s3://b/cache"),
            backend_name="hpc", ssh=ssh,
        )

        assert status.total_objects == 0
        assert status.error is None
        assert all(p.error is None for p in status.prefixes)

    @pytest.mark.asyncio
    async def test_missing_cli_surfaces_the_reason(self):
        ssh = _ssh(stdout="", stderr="aws: command not found", code=127)

        status = await scan_cache_store(
            CacheConfig(enabled=True, store="s3://b/cache"),
            backend_name="hpc", ssh=ssh,
        )

        assert status.error is not None
        assert "command not found" in status.error
        assert not status.reachable

    @pytest.mark.asyncio
    async def test_ssh_failure_never_raises(self):
        """This feeds a page that has to render either way."""
        ssh = MagicMock()
        ssh.run_command = AsyncMock(side_effect=OSError("connection reset"))

        status = await scan_cache_store(
            CacheConfig(enabled=True, store="s3://b/cache"),
            backend_name="hpc", ssh=ssh,
        )

        assert "connection reset" in status.error


class TestDerivedFigures:
    def _status(self, actions: int, blobs: int, blob_bytes: int) -> CacheStoreStatus:
        return CacheStoreStatus(
            enabled=True, store="s3://b/c",
            prefixes=[
                CachePrefixStat("ac", "s3://b/c/ac/", actions, actions * 300),
                CachePrefixStat("cas", "s3://b/c/cas/", blobs, blob_bytes),
            ],
        )

    def test_mean_blob_size(self):
        assert self._status(10, 4, 4000).mean_blob_bytes == 1000

    def test_mean_is_none_with_no_blobs(self):
        assert self._status(0, 0, 0).mean_blob_bytes is None

    def test_totals_span_both_prefixes(self):
        status = self._status(10, 4, 4000)

        assert status.total_objects == 14
        assert status.total_bytes == 4000 + 3000


class TestPanelRendering:
    """The panel has several mutually exclusive states; render each.

    A live scan needs a real SSH backend, so the populated state is only
    reachable here — without this the interesting branch ships untested.
    """

    def _render(self, ctx: dict) -> str:
        from scripthut.main import templates

        base = {
            "cache": None, "configured": False, "vantage_backends": [],
            "scanning": False, "status": None,
        }
        return templates.env.get_template("disk_cache.html").render(
            {"request": MagicMock(), "c": {**base, **ctx}}
        )

    def test_unconfigured_points_at_settings(self):
        html = self._render({})

        assert "No result cache configured" in html
        assert 'href="/settings"' in html

    def test_configured_but_no_ssh_backend_explains_why(self):
        html = self._render({
            "configured": True,
            "cache": CacheConfig(enabled=True, store="s3://b/c"),
        })

        assert "No connected SSH backend" in html
        assert "cluster-side" in html

    def test_not_yet_scanned(self):
        html = self._render({
            "configured": True,
            "cache": CacheConfig(enabled=True, store="s3://b/c"),
            "vantage_backends": ["hpc"],
        })

        assert "Not scanned yet" in html
        assert "Scan store" in html

    def test_populated_shows_the_figures(self):
        from datetime import datetime, timezone

        status = CacheStoreStatus(
            enabled=True, store="s3://b/c", tool="aws", backend="hpc",
            scanned_at=datetime.now(timezone.utc), duration_ms=812,
            prefixes=[
                CachePrefixStat("ac", "s3://b/c/ac/", 120, 36000),
                CachePrefixStat("cas", "s3://b/c/cas/", 90, 900_000_000),
            ],
        )
        html = self._render({
            "configured": True,
            "cache": CacheConfig(enabled=True, store="s3://b/c"),
            "vantage_backends": ["hpc"],
            "status": status,
        })

        assert "120" in html  # cached actions
        assert "90" in html   # distinct blobs
        assert "via hpc" in html
        # Fewer blobs than actions is healthy dedup, and is explained.
        # (Collapse whitespace: the sentence wraps across template lines.)
        flat = " ".join(html.split())
        assert "30 actions resolved to artifacts that were already stored" in flat
        # The scan deliberately does not compute reclaimable space.
        assert "does not identify reclaimable space" in html

    def test_store_error_surfaces_the_tool_requirement(self):
        from datetime import datetime, timezone

        status = CacheStoreStatus(
            enabled=True, store="s3://b/c", tool="rclone",
            scanned_at=datetime.now(timezone.utc),
            error="rclone: command not found",
        )
        html = self._render({
            "configured": True,
            "cache": CacheConfig(enabled=True, store="s3://b/c", tool="rclone"),
            "vantage_backends": ["hpc"],
            "status": status,
        })

        assert "Could not read the store" in html
        assert "rclone: command not found" in html

    def test_scanning_state_polls_itself(self):
        html = self._render({
            "configured": True,
            "cache": CacheConfig(enabled=True, store="s3://b/c"),
            "vantage_backends": ["hpc"],
            "scanning": True,
        })

        assert 'hx-get="/disk/cache/partial"' in html
        assert "Scanning" in html


class TestCachedTaskBadge:
    """A cache hit completes without running; the views must say so.

    The data has been recorded on RunItem since caching shipped but no
    template read it, so a restored task was indistinguishable from one
    that actually ran — except for a suspicious zero runtime.
    """

    def _run(self, *, hits: int, total: int):
        from datetime import datetime, timezone

        from scripthut.runs.models import (
            Run,
            RunItem,
            RunItemStatus,
            TaskDefinition,
        )

        items = []
        for i in range(total):
            item = RunItem(
                task=TaskDefinition(id=f"t{i}", name=f"Task {i}", command="x"),
                status=RunItemStatus.COMPLETED,
            )
            if i < hits:
                item.cache_hit = True
                item.cache_key = "abcdef0123456789" * 4
            items.append(item)
        return Run(
            id="r1", workflow_name="demo/train", backend_name="hpc",
            created_at=datetime.now(timezone.utc), items=items,
            max_concurrent=None, source_name="demo",
        )

    def _render(self, name: str, run) -> str:
        from scripthut.main import templates

        return templates.env.get_template(name).render(
            {"request": MagicMock(), "run": run}
        )

    def test_hit_items_are_badged_with_a_truncated_key(self):
        html = self._render("run_items.html", self._run(hits=1, total=2))

        assert "cached" in html
        assert "Restored from the result cache" in html
        assert "key abcdef012345" in html  # first 12 chars, not the whole hash

    def test_uncached_run_has_no_badge(self):
        html = self._render("run_items.html", self._run(hits=0, total=2))

        assert "Restored from the result cache" not in html

    def test_run_header_counts_the_hits(self):
        html = self._render("run_info.html", self._run(hits=3, total=5))
        flat = " ".join(html.split())

        assert "From cache" in flat
        assert "3 / 5" in flat

    def test_run_header_omits_the_stat_when_nothing_was_cached(self):
        """A permanent "0 cached" would be noise for the majority."""
        html = self._render("run_info.html", self._run(hits=0, total=5))

        assert "From cache" not in html

    def test_cache_hit_count_property(self):
        assert self._run(hits=2, total=5).cache_hit_count == 2
