"""Tests for `RunManager.create_run_from_stack` and the synthesized command.

v0.9.0 turned stack install from a blocking SSH call into a normal
workflow run submission. These tests pin:

- The synthesized bash does what `StackManager.install` does
  (sentinel check, mkdir, prep, touch ready) but as a single inline
  command suitable for the task's ``command`` field.
- The task carries the stack's resource fields so heavy preps actually
  get the allocation they need.
- ``source_name`` lands in the workflow_name label for provenance.
- The empty-prep degenerate case still produces a valid install (just
  the .ready sentinel).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripthut.config_schema import (
    ScriptHutConfig,
    SlurmBackendConfig,
    SSHConfig,
    Stack,
)
from scripthut.runs.manager import RunManager
from scripthut.runs.storage import RunStorageManager


def _mgr(stack: Stack, tmp_path: Path) -> RunManager:
    """Build a minimal RunManager wired to one Slurm backend."""
    backend = SlurmBackendConfig(
        name="cluster", type="slurm", ssh=SSHConfig(host="h", user="u"),
    )
    config = ScriptHutConfig(backends=[backend], stacks=[stack])
    mgr = RunManager(
        config=config,
        backends={"cluster": MagicMock()},
        storage=RunStorageManager(tmp_path / "runs"),
        job_backends={},
    )
    # Provide an SSH client so create_run_from_stack passes the
    # availability check without actually running anything.
    mgr.get_ssh_client = MagicMock(return_value=MagicMock())
    return mgr


# ---------------------------------------------------------------------------
# _synthesize_stack_install_command
# ---------------------------------------------------------------------------


def test_synth_command_has_sentinel_check_and_prep_and_touch():
    """The synthesized bash must reproduce StackManager.install's flow."""
    stack = Stack(
        name="julia-1.12", prep="curl -sSL https://install.julialang.org | sh",
    )
    cmd = RunManager._synthesize_stack_install_command(stack, "abcdef123456", False)

    # Sentinel exists check + early exit on already-ready
    assert "READY=" in cmd
    assert "[ -f \"$READY\" ]" in cmd
    assert "exit 0" in cmd

    # Pre-prep cleanup for rebuild OR half-built leftover
    assert 'rm -rf "$HASH_DIR"' in cmd

    # The actual prep body must appear unchanged
    assert "curl -sSL https://install.julialang.org" in cmd

    # set -euo pipefail before the prep so any failure aborts BEFORE touching ready
    pipefail_idx = cmd.index("set -euo pipefail")
    prep_idx = cmd.index("curl -sSL")
    ready_idx = cmd.index("touch \"$READY\"")
    assert pipefail_idx < prep_idx < ready_idx, (
        "ordering broken: pipefail must precede prep, ready must follow prep"
    )

    # STACK_DIR exported so the user's prep can write to it
    assert 'export STACK_DIR="$HASH_DIR"' in cmd


def test_synth_command_expands_tilde_to_dollar_home():
    """``~`` in cache_dir must become ``$HOME`` at synthesis time so the
    emitted command works regardless of which bash invocation runs it
    (single-quoting would otherwise break tilde expansion).
    """
    stack = Stack(name="foo", cache_dir="~/.cache/sh-stacks", prep="echo")
    cmd = RunManager._synthesize_stack_install_command(stack, "h0", False)
    assert "$HOME/.cache/sh-stacks/foo/h0" in cmd
    # No raw ~ left in the path — confirms the substitution happened.
    assert "~/.cache/sh-stacks/foo" not in cmd


def test_synth_command_absolute_cache_dir_passes_through():
    """A non-tilde cache_dir (e.g. /scratch/...) must be used as-is."""
    stack = Stack(name="foo", cache_dir="/scratch/me/stacks", prep="echo")
    cmd = RunManager._synthesize_stack_install_command(stack, "h0", False)
    assert "/scratch/me/stacks/foo/h0" in cmd
    # No tilde-substitution-gone-wrong
    assert "$HOME/scratch" not in cmd


def test_synth_command_rebuild_flag_changes_behavior():
    """rebuild=True must short-circuit the "already-ready" exit so the
    install actually rebuilds, AND wipe the dir before prep."""
    stack = Stack(name="foo", prep="echo prep")
    cmd_no_rebuild = RunManager._synthesize_stack_install_command(stack, "h", False)
    cmd_rebuild = RunManager._synthesize_stack_install_command(stack, "h", True)
    assert "REBUILD=0" in cmd_no_rebuild
    assert "REBUILD=1" in cmd_rebuild


def test_synth_command_empty_prep_just_marks_ready():
    """Degenerate stack with no prep — produce a tiny bash that just
    mkdirs and touches ready. Init-only stacks still need a stable dir.
    """
    stack = Stack(name="init-only", prep="")
    cmd = RunManager._synthesize_stack_install_command(stack, "h0", False)
    # Nothing to run, just create the directory and mark ready.
    assert 'mkdir -p "$HASH_DIR"' in cmd
    assert "touch" in cmd
    # No sentinel check needed — touching an existing .ready is a no-op.
    assert "exit 0" not in cmd


# ---------------------------------------------------------------------------
# create_run_from_stack
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_run_from_stack_resources_come_from_stack(tmp_path: Path):
    """Heavy installs need real allocations — task resources match stack."""
    stack = Stack(
        name="cuda-12", prep="bash install.sh",
        cpus=8, memory="32G", time_limit="2:00:00", partition="gpu",
    )
    mgr = _mgr(stack, tmp_path)
    captured: dict = {}

    async def fake_build_run(tasks, workflow_name, backend_name, max_concurrent,
                             ssh_client, **kw):
        captured["task"] = tasks[0]
        captured["workflow_name"] = workflow_name
        captured["backend_name"] = backend_name
        from datetime import UTC, datetime
        from scripthut.runs.models import Run, RunItem, RunItemStatus
        return Run(
            id="r", workflow_name=workflow_name, backend_name=backend_name,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            items=[RunItem(task=tasks[0], status=RunItemStatus.SUBMITTED)],
            max_concurrent=1,
        )

    with patch.object(mgr, "_build_run", side_effect=fake_build_run):
        await mgr.create_run_from_stack(stack, "cluster")

    task = captured["task"]
    assert task.cpus == 8
    assert task.memory == "32G"
    assert task.time_limit == "2:00:00"
    assert task.partition == "gpu"
    # Task name has the stack prefix so it's recognizable in the runs page.
    assert task.name == "stack/cuda-12"


@pytest.mark.asyncio
async def test_create_run_from_stack_workflow_name_includes_source(
    tmp_path: Path,
):
    """`_stack/<source>/<stack>` lets the runs page show *which* repo's
    stack got installed (vs. server-global).
    """
    stack = Stack(name="julia", prep="echo")
    mgr = _mgr(stack, tmp_path)
    captured: dict = {}

    async def fake_build_run(tasks, workflow_name, backend_name, *a, **kw):
        captured["wf"] = workflow_name
        from datetime import UTC, datetime
        from scripthut.runs.models import Run, RunItem, RunItemStatus
        return Run(
            id="r", workflow_name=workflow_name, backend_name=backend_name,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            items=[RunItem(task=tasks[0], status=RunItemStatus.SUBMITTED)],
            max_concurrent=1,
        )

    with patch.object(mgr, "_build_run", side_effect=fake_build_run):
        await mgr.create_run_from_stack(
            stack, "cluster", source_name="balke-jmp",
        )
    assert captured["wf"] == "_stack/balke-jmp/julia"

    with patch.object(mgr, "_build_run", side_effect=fake_build_run):
        await mgr.create_run_from_stack(stack, "cluster")  # no source
    assert captured["wf"] == "_stack/julia"


@pytest.mark.asyncio
async def test_create_run_from_stack_rejects_unknown_backend(tmp_path: Path):
    stack = Stack(name="foo", prep="echo")
    mgr = _mgr(stack, tmp_path)
    with pytest.raises(ValueError, match="not found in config"):
        await mgr.create_run_from_stack(stack, "missing-cluster")


@pytest.mark.asyncio
async def test_create_run_from_stack_rejects_unavailable_backend(tmp_path: Path):
    """Backend in config but neither SSH nor an API driver — same error
    shape as ``create_adhoc_run`` so callers can pattern-match.
    """
    stack = Stack(name="foo", prep="echo")
    mgr = _mgr(stack, tmp_path)
    mgr.get_ssh_client = MagicMock(return_value=None)
    mgr.get_job_backend = MagicMock(return_value=None)
    with pytest.raises(ValueError, match="not available"):
        await mgr.create_run_from_stack(stack, "cluster")


@pytest.mark.asyncio
async def test_create_run_from_stack_command_embeds_synthesized_bash(
    tmp_path: Path,
):
    """The task's `command` is the synthesized install bash. Sanity-check
    the wiring end-to-end so future refactors that decouple them break here.
    """
    stack = Stack(name="foo", prep="curl ... | sh")
    mgr = _mgr(stack, tmp_path)
    captured: dict = {}

    async def fake_build_run(tasks, *a, **kw):
        captured["command"] = tasks[0].command
        from datetime import UTC, datetime
        from scripthut.runs.models import Run, RunItem, RunItemStatus
        return Run(
            id="r", workflow_name="_stack/foo", backend_name="cluster",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            items=[RunItem(task=tasks[0], status=RunItemStatus.SUBMITTED)],
            max_concurrent=1,
        )

    with patch.object(mgr, "_build_run", side_effect=fake_build_run):
        await mgr.create_run_from_stack(stack, "cluster", rebuild=True)

    cmd = captured["command"]
    assert "curl ... | sh" in cmd            # prep body present
    assert "REBUILD=1" in cmd                  # rebuild forwarded
    assert "touch \"$READY\"" in cmd          # marks ready at the end
