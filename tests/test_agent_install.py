"""Tests for `scripthut agent install` (Claude Code skill + slash command).

The rendered files are pinned by *structure* (frontmatter fields, marker,
key invocation patterns) rather than exact prose. The install logic is
pinned by its safety contract: managed files are freely overwritten,
foreign files are never touched without --force.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from scripthut import agent_skill
from scripthut.agent_skill import (
    DEBUG_COMMAND_RELPATH,
    MANAGED_MARKER,
    SKILL_RELPATH,
    install_assets,
    render_debug_command,
    render_skill,
)
from scripthut.cli import _cmd_agent_install

# ---------- rendering -----------------------------------------------------


class TestRenderSkill:
    def test_frontmatter_name_matches_directory(self):
        skill = render_skill()
        assert skill.startswith("---\n")
        assert "name: scripthut" in skill
        # Skill name must equal its directory name for Claude Code to load it.
        assert SKILL_RELPATH.parent.name == "scripthut"

    def test_description_carries_trigger_keywords(self):
        skill = render_skill()
        head = skill.split("---")[1]  # frontmatter block
        for kw in ("Slurm", "PBS", "AWS Batch", "logs", "stacks"):
            assert kw in head, f"trigger keyword missing from description: {kw}"

    def test_body_defers_to_live_briefing(self):
        skill = render_skill()
        assert "scripthut agent prompt" in skill
        assert MANAGED_MARKER in skill

    def test_body_covers_core_loop_commands(self):
        skill = render_skill()
        for cmd in (
            "scripthut status",
            "scripthut backend list --json",
            "scripthut run view",
            "scripthut run logs",
            "--dry-run",
        ):
            assert cmd in skill, f"core-loop command missing: {cmd}"

    def test_no_baked_inventory(self):
        # The skill must stay config-agnostic — no live backend/stack names.
        assert "## What's available here" not in render_skill()


class TestRenderDebugCommand:
    def test_frontmatter_and_arguments_placeholder(self):
        cmd = render_debug_command()
        assert cmd.startswith("---\n")
        assert "description:" in cmd
        assert "argument-hint:" in cmd
        assert "$ARGUMENTS" in cmd
        assert MANAGED_MARKER in cmd

    def test_walks_diagnosis_flow(self):
        cmd = render_debug_command()
        for step in (
            "scripthut run view",
            "--error --tail 200",
            "scripthut stack check",
        ):
            assert step in cmd, f"diagnosis step missing: {step}"


# ---------- install logic -------------------------------------------------


class TestInstallAssets:
    def test_fresh_install_creates_both_files(self, tmp_path: Path):
        results = install_assets(tmp_path / ".claude")
        assert [r.action for r in results] == ["created", "created"]
        assert (tmp_path / ".claude" / SKILL_RELPATH).read_text() == render_skill()
        assert (
            tmp_path / ".claude" / DEBUG_COMMAND_RELPATH
        ).read_text() == render_debug_command()

    def test_reinstall_is_idempotent(self, tmp_path: Path):
        claude = tmp_path / ".claude"
        install_assets(claude)
        results = install_assets(claude)
        assert [r.action for r in results] == ["unchanged", "unchanged"]

    def test_stale_managed_file_is_updated(self, tmp_path: Path):
        claude = tmp_path / ".claude"
        install_assets(claude)
        skill_path = claude / SKILL_RELPATH
        # Simulate output from an older scripthut version — marker intact.
        skill_path.write_text(f"old content\n{MANAGED_MARKER}\n")
        results = install_assets(claude)
        assert results[0].action == "updated"
        assert skill_path.read_text() == render_skill()

    def test_foreign_file_is_skipped_without_force(self, tmp_path: Path):
        claude = tmp_path / ".claude"
        skill_path = claude / SKILL_RELPATH
        skill_path.parent.mkdir(parents=True)
        skill_path.write_text("hand-written skill, no marker\n")
        results = install_assets(claude)
        assert results[0].action == "skipped"
        assert skill_path.read_text() == "hand-written skill, no marker\n"
        # The other file is still installed.
        assert results[1].action == "created"

    def test_force_overwrites_foreign_file(self, tmp_path: Path):
        claude = tmp_path / ".claude"
        skill_path = claude / SKILL_RELPATH
        skill_path.parent.mkdir(parents=True)
        skill_path.write_text("hand-written skill, no marker\n")
        results = install_assets(claude, force=True)
        assert results[0].action == "updated"
        assert skill_path.read_text() == render_skill()


# ---------- CLI handler ---------------------------------------------------


def _args(**kw) -> argparse.Namespace:
    defaults: dict = {"user": False, "force": False}
    defaults.update(kw)
    return argparse.Namespace(**defaults)


class TestCmdAgentInstall:
    @pytest.mark.asyncio
    async def test_installs_into_cwd_claude_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        rc = await _cmd_agent_install(_args())
        assert rc == 0
        assert (tmp_path / ".claude" / SKILL_RELPATH).exists()
        assert (tmp_path / ".claude" / DEBUG_COMMAND_RELPATH).exists()

    @pytest.mark.asyncio
    async def test_user_flag_targets_home(self, tmp_path, monkeypatch):
        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path / "home"))
        monkeypatch.chdir(tmp_path)
        rc = await _cmd_agent_install(_args(user=True))
        assert rc == 0
        assert (tmp_path / "home" / ".claude" / SKILL_RELPATH).exists()
        assert not (tmp_path / ".claude").exists()

    @pytest.mark.asyncio
    async def test_skip_yields_nonzero_exit(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        skill_path = tmp_path / ".claude" / SKILL_RELPATH
        skill_path.parent.mkdir(parents=True)
        skill_path.write_text("foreign\n")
        rc = await _cmd_agent_install(_args())
        assert rc == 1
        assert skill_path.read_text() == "foreign\n"


def test_parser_agent_install_dispatches_to_handler():
    from scripthut import cli

    parser = cli.build_parser()
    args = parser.parse_args(["agent", "install", "--user", "--force"])
    assert args.handler is cli._cmd_agent_install
    assert args.user is True
    assert args.force is True


def test_version_appears_in_rendered_marker():
    from scripthut import __version__

    assert f"v{__version__}" in render_skill()
    assert f"v{__version__}" in agent_skill.render_debug_command()
