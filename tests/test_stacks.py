"""Tests for the Stack config model and StackManager.

Mock SSH transport so the install/check/delete flow is exercised without
hitting a real backend. We assert on the commands the manager issues and
on its decoded view of remote state.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from scripthut.config_schema import Stack
from scripthut.stacks import (
    StackManager,
    StackState,
    compute_stack_hash,
)
from scripthut.stacks.manager import READY_SENTINEL


# ---------- hash determinism --------------------------------------------


class TestComputeStackHash:
    def test_same_inputs_same_hash(self):
        a = Stack(name="x", prep="pip install foo", inputs={"py": "3.12"})
        b = Stack(name="x", prep="pip install foo", inputs={"py": "3.12"})
        assert compute_stack_hash(a) == compute_stack_hash(b)

    def test_prep_change_changes_hash(self):
        a = Stack(name="x", prep="pip install foo", inputs={"py": "3.12"})
        b = Stack(name="x", prep="pip install bar", inputs={"py": "3.12"})
        assert compute_stack_hash(a) != compute_stack_hash(b)

    def test_input_value_change_changes_hash(self):
        a = Stack(name="x", prep="p", inputs={"py": "3.12"})
        b = Stack(name="x", prep="p", inputs={"py": "3.13"})
        assert compute_stack_hash(a) != compute_stack_hash(b)

    def test_input_key_order_does_not_matter(self):
        # dict order shouldn't change hash — we sort by key when feeding.
        a = Stack(name="x", prep="p", inputs={"a": "1", "b": "2"})
        b = Stack(name="x", prep="p", inputs={"b": "2", "a": "1"})
        assert compute_stack_hash(a) == compute_stack_hash(b)

    def test_input_file_content_change_changes_hash(self, tmp_path: Path):
        f = tmp_path / "requirements.txt"
        f.write_text("foo==1.0\n")
        a = Stack(name="x", prep="p", input_files=[f])
        h1 = compute_stack_hash(a)
        f.write_text("foo==2.0\n")
        h2 = compute_stack_hash(a)
        assert h1 != h2

    def test_input_file_missing_hashes_deterministically(self, tmp_path: Path):
        # Missing file hashes to a stable sentinel — same hash repeated.
        nonexistent = tmp_path / "no-such-file.txt"
        s = Stack(name="x", prep="p", input_files=[nonexistent])
        assert compute_stack_hash(s) == compute_stack_hash(s)

    def test_hash_length(self):
        s = Stack(name="x", prep="p")
        h = compute_stack_hash(s)
        assert len(h) == 12
        assert all(c in "0123456789abcdef" for c in h)


# ---------- check ---------------------------------------------------------


def _ssh_returning(stdout: str, stderr: str = "", exit_code: int = 0):
    ssh = AsyncMock()
    ssh.run_command = AsyncMock(return_value=(stdout, stderr, exit_code))
    return ssh


class TestCheck:
    @pytest.mark.asyncio
    async def test_missing_when_no_dir(self):
        ssh = _ssh_returning("MISSING\n")
        status = await StackManager().check(
            Stack(name="x", prep="echo hi"), "be", ssh,
        )
        assert status.state == StackState.MISSING
        assert status.last_built is None
        assert status.size_bytes is None

    @pytest.mark.asyncio
    async def test_installing_when_dir_but_no_sentinel(self):
        ssh = _ssh_returning("INSTALLING\n")
        status = await StackManager().check(
            Stack(name="x", prep="echo hi"), "be", ssh,
        )
        assert status.state == StackState.INSTALLING
        assert status.error is not None  # Half-built note

    @pytest.mark.asyncio
    async def test_ready_parses_mtime_and_size(self):
        # READY / mtime epoch / size bytes
        epoch = 1700000000
        ssh = _ssh_returning(f"READY\n{epoch}\n12345\n")
        status = await StackManager().check(
            Stack(name="x", prep="echo hi"), "be", ssh,
        )
        assert status.state == StackState.READY
        assert status.last_built == datetime.fromtimestamp(epoch, tz=timezone.utc)
        assert status.size_bytes == 12345

    @pytest.mark.asyncio
    async def test_ready_with_unparseable_size_still_ready(self):
        ssh = _ssh_returning("READY\n0\nNaN\n")
        status = await StackManager().check(
            Stack(name="x", prep="echo hi"), "be", ssh,
        )
        assert status.state == StackState.READY
        assert status.size_bytes is None

    @pytest.mark.asyncio
    async def test_command_failure_returns_missing_with_error(self):
        ssh = _ssh_returning("", "permission denied", 1)
        status = await StackManager().check(
            Stack(name="x", prep="echo hi"), "be", ssh,
        )
        assert status.state == StackState.MISSING
        assert "permission denied" in (status.error or "")

    @pytest.mark.asyncio
    async def test_hash_path_uses_cache_dir_name_hash(self):
        stack = Stack(
            name="julia-1.11",
            prep="echo hi",
            cache_dir="/scratch/me/stacks",
        )
        h = compute_stack_hash(stack)
        assert StackManager.hash_path(stack, h) == f"/scratch/me/stacks/julia-1.11/{h}"


# ---------- install -------------------------------------------------------


class _ScriptedSSH:
    """SSH mock that returns pre-canned responses and records every command."""

    def __init__(self, responses: list[tuple[str, str, int]]):
        self.responses = list(responses)
        self.commands: list[str] = []
        self.run_command = self._run_command  # so it looks like AsyncMock externally

    async def _run_command(self, cmd: str, timeout: int = 30):
        self.commands.append(cmd)
        if self.responses:
            return self.responses.pop(0)
        return ("", "", 0)


class TestInstall:
    @pytest.mark.asyncio
    async def test_noop_when_already_ready(self):
        ssh = _ScriptedSSH([("READY\n0\n100\n", "", 0)])
        status = await StackManager().install(
            Stack(name="x", prep="echo hi"), "be", ssh,
        )
        assert status.state == StackState.READY
        # Only the initial check ran; no mkdir / bash / touch.
        assert len(ssh.commands) == 1

    @pytest.mark.asyncio
    async def test_runs_prep_when_missing(self):
        # check (missing) -> mkdir -> bash prep -> re-check (ready)
        ssh = _ScriptedSSH([
            ("MISSING\n", "", 0),       # initial check
            ("", "", 0),                # mkdir -p
            ("install ok\n", "", 0),    # bash <heredoc>
            ("READY\n0\n42\n", "", 0),  # final check
        ])
        status = await StackManager().install(
            Stack(name="x", prep="pip install foo"), "be", ssh,
        )
        assert status.state == StackState.READY
        # The bash invocation got a heredoc with the prep body and a final
        # ``touch .ready`` so success persists the sentinel.
        bash_cmd = next(c for c in ssh.commands if c.startswith("bash"))
        assert "pip install foo" in bash_cmd
        assert "touch" in bash_cmd
        assert READY_SENTINEL in bash_cmd
        # STACK_DIR was exported so the prep can use it.
        assert "STACK_DIR=" in bash_cmd

    @pytest.mark.asyncio
    async def test_prep_failure_leaves_dir_in_installing_state(self):
        ssh = _ScriptedSSH([
            ("MISSING\n", "", 0),
            ("", "", 0),                       # mkdir
            ("", "boom: missing dep", 1),      # prep failed
            ("INSTALLING\n", "", 0),           # re-check sees half-built dir
        ])
        status = await StackManager().install(
            Stack(name="x", prep="false"), "be", ssh,
        )
        assert status.state == StackState.INSTALLING
        assert "boom" in (status.error or "")

    @pytest.mark.asyncio
    async def test_rebuild_wipes_existing_ready_then_reinstalls(self):
        ssh = _ScriptedSSH([
            ("READY\n0\n100\n", "", 0),  # already ready
            ("", "", 0),                 # rm -rf because rebuild=True
            ("", "", 0),                 # mkdir
            ("", "", 0),                 # bash prep
            ("READY\n0\n200\n", "", 0),  # re-check
        ])
        status = await StackManager().install(
            Stack(name="x", prep="echo hi"), "be", ssh, rebuild=True,
        )
        assert status.state == StackState.READY
        assert any(c.startswith("rm -rf") for c in ssh.commands)

    @pytest.mark.asyncio
    async def test_slurm_wraps_prep_with_srun(self):
        # On Slurm, prep must land on a worker — not the login node.
        # Verify the bash invocation is wrapped with srun and that the
        # stack's resource fields are passed through.
        ssh = _ScriptedSSH([
            ("MISSING\n", "", 0),
            ("", "", 0),                # mkdir
            ("ok\n", "", 0),            # srun bash -s heredoc
            ("READY\n0\n42\n", "", 0),  # final check
        ])
        stack = Stack(
            name="x", prep="pip install foo",
            cpus=8, memory="32G", time_limit="2:00:00", partition="bigmem",
        )
        status = await StackManager().install(
            stack, "slurm-be", ssh, scheduler="slurm",
        )
        assert status.state == StackState.READY
        runner_cmd = next(c for c in ssh.commands if "bash -s" in c)
        assert runner_cmd.startswith("srun")
        assert "--cpus-per-task=8" in runner_cmd
        assert "--mem=32G" in runner_cmd
        assert "--time=2:00:00" in runner_cmd
        assert "--partition=bigmem" in runner_cmd
        # Prep still inlined via the heredoc.
        assert "pip install foo" in runner_cmd

    @pytest.mark.asyncio
    async def test_slurm_omits_partition_flag_when_unset(self):
        # Default partition is None — let Slurm pick its default rather
        # than passing an empty --partition= flag.
        ssh = _ScriptedSSH([
            ("MISSING\n", "", 0), ("", "", 0), ("", "", 0),
            ("READY\n0\n1\n", "", 0),
        ])
        await StackManager().install(
            Stack(name="x", prep="echo hi"), "slurm-be", ssh, scheduler="slurm",
        )
        runner_cmd = next(c for c in ssh.commands if "bash -s" in c)
        assert runner_cmd.startswith("srun")
        assert "--partition" not in runner_cmd

    @pytest.mark.asyncio
    async def test_no_scheduler_runs_inline_as_before(self):
        # When scheduler is None (or PBS, which isn't wired yet), prep
        # runs inline — preserves the existing v1 behavior for any caller
        # that doesn't pass a scheduler kind.
        ssh = _ScriptedSSH([
            ("MISSING\n", "", 0), ("", "", 0), ("", "", 0),
            ("READY\n0\n1\n", "", 0),
        ])
        await StackManager().install(
            Stack(name="x", prep="echo hi", cpus=8, memory="32G"),
            "be", ssh,
        )
        runner_cmd = next(c for c in ssh.commands if "bash -s" in c)
        assert not runner_cmd.startswith("srun")

    @pytest.mark.asyncio
    async def test_empty_prep_just_marks_ready(self):
        # An init-only stack with no prep should still end up READY so
        # downstream "is it ready?" gating works uniformly.
        ssh = _ScriptedSSH([
            ("MISSING\n", "", 0),
            ("", "", 0),                # mkdir
            ("", "", 0),                # touch .ready (no bash heredoc)
            ("READY\n0\n0\n", "", 0),
        ])
        status = await StackManager().install(
            Stack(name="x", prep=""), "be", ssh,
        )
        assert status.state == StackState.READY
        # No bash heredoc when prep is empty.
        assert not any(c.startswith("bash") for c in ssh.commands)
        assert any("touch" in c and READY_SENTINEL in c for c in ssh.commands)


# ---------- delete --------------------------------------------------------


class TestDelete:
    @pytest.mark.asyncio
    async def test_removes_entire_stack_dir(self):
        # Delete targets the stack-name dir (all hashes), not just current hash.
        ssh = _ScriptedSSH([("", "", 0)])
        stack = Stack(
            name="julia-1.11", prep="echo hi", cache_dir="/scratch/me/stacks",
        )
        await StackManager().delete(stack, "be", ssh)
        assert len(ssh.commands) == 1
        cmd = ssh.commands[0]
        assert cmd.startswith("rm -rf")
        assert "/scratch/me/stacks/julia-1.11" in cmd
        # No hash suffix — we want every cached build cleared.
        assert compute_stack_hash(stack) not in cmd

    @pytest.mark.asyncio
    async def test_failure_raises(self):
        ssh = _ScriptedSSH([("", "Permission denied", 1)])
        stack = Stack(name="x", prep="echo hi")
        with pytest.raises(RuntimeError, match="Permission denied"):
            await StackManager().delete(stack, "be", ssh)


# ---------- tilde expansion in shell commands -----------------------------


class TestTildeExpansion:
    """The check/install/delete commands must substitute leading ``~`` with
    ``$HOME`` rather than single-quoting it. ``shlex.quote('~/foo')`` returns
    ``'~/foo'`` which bash treats as a literal tilde character — a check
    after a successful install would silently report MISSING because the
    install creates the file at ``$HOME/foo`` but the check probes a
    literal ``~/foo`` path. v0.9.0 fixed install on the server side; the
    pre-existing local-mode shell helpers needed the same treatment.
    """

    @pytest.mark.asyncio
    async def test_check_uses_dollar_home_not_literal_tilde(self):
        """The check command must reference ``$HOME`` so the shell expands it."""
        ssh = _ScriptedSSH([("MISSING\n", "", 0)])
        stack = Stack(
            name="julia", prep="echo hi", cache_dir="~/.cache/sh-stacks",
        )
        await StackManager().check(stack, "be", ssh)

        assert len(ssh.commands) == 1
        cmd = ssh.commands[0]
        # The path should appear as $HOME/.cache/... in double quotes so
        # bash expands it. A literal '~/.cache/...' (single-quoted) would
        # be the bug.
        assert '"$HOME/.cache/sh-stacks/julia/' in cmd
        assert "'~/.cache/sh-stacks" not in cmd

    @pytest.mark.asyncio
    async def test_install_uses_dollar_home_not_literal_tilde(self):
        # Sequence: check (MISSING) → mkdir → bash -s heredoc (prep) → check
        ssh = _ScriptedSSH([
            ("MISSING\n", "", 0),  # initial check
            ("", "", 0),            # mkdir
            ("", "", 0),            # prep heredoc
            ("READY\n0\n100\n", "", 0),  # final check
        ])
        stack = Stack(
            name="cuda", prep="echo build", cache_dir="~/.cache/sh-stacks",
        )
        await StackManager().install(stack, "be", ssh)

        # mkdir, the prep heredoc, and the final check should all use
        # the expanded path. No single-quoted literal tilde anywhere.
        all_cmds = "\n".join(ssh.commands)
        assert "$HOME/.cache/sh-stacks/cuda/" in all_cmds
        assert "'~/.cache/sh-stacks" not in all_cmds

    @pytest.mark.asyncio
    async def test_delete_uses_dollar_home_not_literal_tilde(self):
        ssh = _ScriptedSSH([("", "", 0)])
        stack = Stack(
            name="julia", prep="echo hi", cache_dir="~/.cache/sh-stacks",
        )
        await StackManager().delete(stack, "be", ssh)

        cmd = ssh.commands[0]
        assert 'rm -rf "$HOME/.cache/sh-stacks/julia"' == cmd
        assert "'~" not in cmd

    @pytest.mark.asyncio
    async def test_absolute_cache_dir_passes_through_unchanged(self):
        """A non-tilde cache_dir (e.g. /scratch/me/stacks) shouldn't grow a
        spurious $HOME prefix.
        """
        ssh = _ScriptedSSH([("MISSING\n", "", 0)])
        stack = Stack(
            name="julia", prep="echo", cache_dir="/scratch/me/stacks",
        )
        await StackManager().check(stack, "be", ssh)
        cmd = ssh.commands[0]
        assert '"/scratch/me/stacks/julia/' in cmd
        assert "$HOME/scratch" not in cmd
