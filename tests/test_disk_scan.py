"""Tests for the remote disk-scan script builder and parser."""

from __future__ import annotations

from datetime import datetime, timezone

from scripthut.config_schema import GitSourceConfig, ScriptHutConfig, Stack
from scripthut.disk.models import DiskEntryKind, ScanSpec
from scripthut.disk.scan import (
    RawEntry,
    build_scan_script,
    build_scan_spec,
    parse_scan_output,
    raw_to_entries,
)


def _config(**kwargs) -> ScriptHutConfig:
    return ScriptHutConfig(**kwargs)


class TestBuildScanSpec:
    def test_backend_clone_dir_only(self):
        spec = build_scan_spec(_config(), "hpc", "~/scripthut-repos")
        assert spec.clone_dirs == ["~/scripthut-repos"]
        assert spec.stack_dirs == []
        assert spec.log_roots == ["~/.cache/scripthut/logs"]

    def test_git_source_clone_dirs_unioned_and_deduped(self):
        cfg = _config(
            sources=[
                # default clone_dir == backend default -> dedupes
                GitSourceConfig(name="a", url="git@x:a.git"),
                GitSourceConfig(name="b", url="git@x:b.git", clone_dir="/scratch/repos/"),
            ]
        )
        spec = build_scan_spec(cfg, "hpc", "~/scripthut-repos")
        assert spec.clone_dirs == ["~/scripthut-repos", "/scratch/repos"]

    def test_path_sources_ignored(self):
        cfg = _config(
            sources=[
                {"name": "p", "type": "path", "path": "/data/proj", "backend": "hpc"},
            ]
        )
        spec = build_scan_spec(cfg, "hpc", "~/scripthut-repos")
        assert spec.clone_dirs == ["~/scripthut-repos"]

    def test_stacks_filtered_by_backend(self):
        cfg = _config(
            stacks=[
                Stack(name="everywhere"),  # empty backends = all
                Stack(name="here", backends=["hpc"]),
                Stack(name="elsewhere", backends=["other"], cache_dir="~/other-stacks"),
            ]
        )
        spec = build_scan_spec(cfg, "hpc", "~/scripthut-repos")
        # both applicable stacks share the default cache_dir -> one entry
        assert spec.stack_dirs == ["~/.cache/scripthut/stacks"]

    def test_empty_backend_clone_dir_skipped(self):
        spec = build_scan_spec(_config(), "ec2", "")
        assert spec.clone_dirs == []


class TestBuildScanScript:
    def test_script_structure(self):
        spec = ScanSpec(
            backend="hpc",
            clone_dirs=["~/scripthut-repos", "/scratch/repos"],
            stack_dirs=["~/.cache/scripthut/stacks"],
        )
        script = build_scan_script(spec)
        # heredoc wrapper
        assert script.startswith("bash -s <<'__SCRIPTHUT_DISKSCAN__'")
        assert script.endswith("__SCRIPTHUT_DISKSCAN__")
        # ~ rewritten to $HOME inside double quotes
        assert 'scan_dir clones "$HOME/scripthut-repos"' in script
        assert 'scan_dir clones "/scratch/repos"' in script
        assert 'scan_stacks "$HOME/.cache/scripthut/stacks"' in script
        assert 'scan_dir logs "$HOME/.cache/scripthut/logs"' in script
        # per-entry du bound
        assert "timeout 60 du -sk" in script
        # df of the primary clone_dir
        assert 'df -Pk "$HOME/scripthut-repos"' in script

    def test_no_clone_dirs_falls_back_to_home_df(self):
        script = build_scan_script(ScanSpec(backend="x", clone_dirs=[]))
        assert 'df -Pk "$HOME"' in script
        assert "scan_dir clones" not in script

    def test_custom_du_timeout(self):
        script = build_scan_script(
            ScanSpec(backend="x", clone_dirs=["~/r"], du_entry_timeout=120)
        )
        assert "timeout 120 du -sk" in script


class TestParseScanOutput:
    def test_happy_path(self):
        stdout = (
            "HOME\t/home/alice\n"
            "DF\t1000\t400\n"
            "SECTION\tclones\t/home/alice/scripthut-repos\n"
            "ENTRY\tclones\t/home/alice/scripthut-repos/a1b2c3d4e5f6\t1718600000\t2048\n"
            "ENTRY\tclones\t/home/alice/scripthut-repos/agent-1a2b3c4d\t1718600001\t512\n"
            "SECTION\tstacks\t/home/alice/.cache/scripthut/stacks\n"
            "ENTRY\tstacks\t/home/alice/.cache/scripthut/stacks/julia/ab12cd34ef56\t1718600002\t4096\t1\n"
            "MISSING\tlogs\t/home/alice/.cache/scripthut/logs\n"
        )
        home, raw, df, errors = parse_scan_output(stdout)
        assert home == "/home/alice"
        assert df == (1000 * 1024, 400 * 1024)
        assert errors == []
        assert len(raw) == 3
        assert raw[0].path == "/home/alice/scripthut-repos/a1b2c3d4e5f6"
        assert raw[0].size_bytes == 2048 * 1024
        assert raw[0].mtime == datetime.fromtimestamp(1718600000, tz=timezone.utc)
        assert raw[2].section == "stacks"
        assert raw[2].ready is True

    def test_size_dash_and_mtime_zero(self):
        stdout = "ENTRY\tclones\t/r/a1b2c3d4e5f6\t0\t-\n"
        _, raw, _, errors = parse_scan_output(stdout)
        assert errors == []
        assert raw[0].size_bytes is None
        assert raw[0].mtime is None

    def test_path_with_spaces(self):
        stdout = "ENTRY\tlogs\t/home/al ice/.cache/scripthut/logs/my workflow\t1718600000\t8\n"
        _, raw, _, errors = parse_scan_output(stdout)
        assert errors == []
        assert raw[0].path == "/home/al ice/.cache/scripthut/logs/my workflow"

    def test_stacks_not_ready(self):
        stdout = "ENTRY\tstacks\t/s/julia/ab12cd34ef56\t1718600000\t100\t0\n"
        _, raw, _, _ = parse_scan_output(stdout)
        assert raw[0].ready is False

    def test_garbage_lines_collected_not_raised(self):
        stdout = (
            "bash: warning: something\n"
            "ENTRY\tclones\ttoo\tfew\n"
            "ENTRY\tclones\t/r/a1b2c3d4e5f6\t1718600000\t10\n"
        )
        _, raw, _, errors = parse_scan_output(stdout)
        assert len(raw) == 1
        assert len(errors) == 2

    def test_empty_stdout(self):
        assert parse_scan_output("") == (None, [], None, [])

    def test_unparseable_df(self):
        _, _, df, errors = parse_scan_output("DF\tx\ty\n")
        assert df is None
        assert len(errors) == 1


class TestRawToEntries:
    def _raw(self, section: str, path: str, ready: bool | None = None) -> RawEntry:
        return RawEntry(section=section, path=path, mtime=None, size_bytes=None, ready=ready)

    def test_kinds(self):
        entries = raw_to_entries(
            [
                self._raw("clones", "/r/a1b2c3d4e5f6"),
                self._raw("clones", "/r/agent-1a2b3c4d"),
                self._raw("clones", "/r/my-checkout"),
                self._raw("clones", "/r/A1B2C3D4E5F6"),  # uppercase: not a hash
                self._raw("stacks", "/s/julia/ab12cd34ef56", ready=True),
                self._raw("logs", "/l/paper-sim"),
            ]
        )
        kinds = [e.kind for e in entries]
        assert kinds == [
            DiskEntryKind.CLONE,
            DiskEntryKind.AGENT,
            DiskEntryKind.OTHER,
            DiskEntryKind.OTHER,
            DiskEntryKind.STACK,
            DiskEntryKind.LOG,
        ]

    def test_details(self):
        entries = raw_to_entries(
            [
                self._raw("stacks", "/s/julia/ab12cd34ef56", ready=True),
                self._raw("stacks", "/s/julia/00000000ffff", ready=False),
                self._raw("logs", "/l/paper-sim"),
            ]
        )
        assert entries[0].detail == "julia/ab12cd34ef56"
        assert entries[1].detail == "julia/00000000ffff (half-built)"
        assert entries[2].detail == "paper-sim"
