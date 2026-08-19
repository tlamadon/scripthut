"""Every template compiles, and the disk card renders.

These exist because a Jinja `{# #}` comment placed *inside* a `{% for %}`
tag's expression shipped to production: nothing imports a template at
import time, no unit test rendered one, so a TemplateSyntaxError only
surfaced as a 500 on the live page. Compiling is cheap — there is no
reason for a broken template to reach a release again.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from scripthut.disk.models import (
    DiskEntry,
    DiskEntryClass,
    DiskEntryKind,
    DiskScanResult,
)


def _env():
    """The app's configured environment — custom filters and all."""
    from scripthut.main import templates

    return templates.env


def _template_names() -> list[str]:
    return sorted(_env().list_templates(extensions=["html"]))


def test_templates_are_discovered():
    # Guards the parametrization below against silently testing nothing.
    names = _template_names()
    assert len(names) > 10
    assert "disk_backend.html" in names


@pytest.mark.parametrize("name", _template_names())
def test_template_compiles(name: str):
    """Catches syntax errors: unclosed tags, comments inside expressions."""
    _env().get_template(name)


def _scan_result() -> DiskScanResult:
    now = datetime.now(timezone.utc)
    return DiskScanResult(
        backend="hpc",
        scanned_at=now - timedelta(minutes=5),
        duration_ms=42000,
        home_dir="/home/alice",
        disk_total_bytes=1_000_000_000_000,
        disk_avail_bytes=120_000_000_000,
        entries=[
            DiskEntry(
                path="/home/alice/.cache/scripthut/mlye",
                kind=DiskEntryKind.OTHER,
                size_bytes=8_400_000_000,
                mtime=now - timedelta(days=3),
                classification=DiskEntryClass.UNKNOWN,
                detail="mlye (env of stack mlye)",
            ),
            DiskEntry(
                path="/home/alice/scripthut-sync/wl-hcpu",
                kind=DiskEntryKind.SYNC,
                size_bytes=12_000_000,
                mtime=now - timedelta(hours=2),
                classification=DiskEntryClass.REFERENCED,
                detail="wl-hcpu",
                source="wl-hcpu",
            ),
            DiskEntry(
                path="/home/alice/scripthut-repos/a1b2c3d4e5f6",
                kind=DiskEntryKind.CLONE,
                size_bytes=570_000_000,
                mtime=now - timedelta(days=1),
                classification=DiskEntryClass.ORPHANED,
                source="variational",
            ),
            DiskEntry(
                path="/home/alice/.cache/scripthut/stacks/mlye/50b925faacea",
                kind=DiskEntryKind.STACK,
                size_bytes=57344,
                mtime=now,
                classification=DiskEntryClass.REFERENCED,
                detail="mlye/50b925faacea (superseded)",
                ready=True,
                run_ids=["r1", "r2"],
            ),
            DiskEntry(
                path="/home/alice/.cache/scripthut/logs/_adhoc",
                kind=DiskEntryKind.LOG,
                size_bytes=None,  # du timed out — renders as "?"
                mtime=None,
                classification=DiskEntryClass.ACTIVE,
                detail="_adhoc",
                run_ids=["r3"],
            ),
        ],
    )


def _render_card(**overrides) -> str:
    ctx = {
        "name": "hpc",
        "type": "slurm",
        "ssh": "alice@login",
        "scanning": False,
        "cleaning": False,
        "result": _scan_result(),
    }
    ctx.update(overrides)
    return _env().get_template("disk_backend.html").render(b=ctx)


class TestDiskBackendCard:
    def test_renders_every_kind_section(self):
        html = _render_card()
        assert "Repository clones" in html
        assert "Sync working copies" in html
        assert "Software stacks" in html
        assert "Logs &amp; outputs" in html or "Logs & outputs" in html
        # The cache-sweep section: without it a stack-built venv is invisible
        assert "Other files &amp; environments" in html
        assert "mlye (env of stack mlye)" in html

    def test_unsized_entry_renders_as_question_mark(self):
        assert ">?<" in _render_card()

    def test_scanning_state_renders(self):
        html = _render_card(scanning=True, result=None)
        assert "Scanning…" in html

    def test_never_scanned_state_renders(self):
        html = _render_card(result=None)
        assert "Scan" in html
