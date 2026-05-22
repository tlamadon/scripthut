"""Tests for `scripthut agent prompt`.

The renderer mixes a static reference with a live inventory of the user's
configured backends/stacks/workflows. The tests pin the *structure*
(headings, key invocation patterns) and the *dynamic substitutions*
(specific names from a fake config) — not the exact prose, which can
evolve without rewriting tests.
"""

from __future__ import annotations

from scripthut.cli import _render_agent_prompt
from scripthut.config_schema import (
    PBSBackendConfig,
    SSHConfig,
    ScriptHutConfig,
    SlurmBackendConfig,
    Stack,
    WorkflowConfig,
)


def _ssh(host: str = "h", user: str = "u") -> SSHConfig:
    return SSHConfig(host=host, user=user)


# ---------- structure ----------------------------------------------------


class TestPromptStructure:
    def test_all_section_headings_present_for_populated_config(self):
        cfg = ScriptHutConfig(
            backends=[
                SlurmBackendConfig(
                    name="mercury-nb", type="slurm", ssh=_ssh("m", "me"),
                ),
            ],
            stacks=[Stack(name="julia", prep="echo")],
            workflows=[
                WorkflowConfig(name="grid", backend="mercury-nb", command="echo"),
            ],
        )
        prompt = _render_agent_prompt(cfg)

        # Headings we care about:
        for heading in (
            "# ScriptHut Agent Brief",
            "## What's available here",
            "### Backends",
            "### Stacks (reusable software environments)",
            "### Workflows",
            "## Submitting work",
            "### One-off command",
            "### Structured submission (JSON on stdin)",
            "### TaskDefinition shape",
            "## Inspecting state",
            "## Exit codes",
            "## Gotchas",
            "## Typical agent loop",
        ):
            assert heading in prompt, f"missing heading: {heading!r}"

    def test_static_invocation_patterns_appear(self):
        # The prompt must teach these exact entry points or an agent
        # following it will get stuck. Pin them.
        cfg = ScriptHutConfig()
        prompt = _render_agent_prompt(cfg)
        for snippet in (
            "scripthut task run",
            "--from-stdin",
            "--backend",
            "--json",
            "scripthut run view",
            "scripthut run logs",
            "scripthut stack check",
            "scripthut backend list",
        ):
            assert snippet in prompt, f"missing pattern: {snippet!r}"


# ---------- live inventory substitutions ---------------------------------


class TestLiveInventory:
    def test_backends_listed_with_type_and_ssh_user_host(self):
        cfg = ScriptHutConfig(
            backends=[
                SlurmBackendConfig(
                    name="mercury-nb", type="slurm",
                    ssh=_ssh("mercury.cluster.edu", "alice"),
                    account="pi-faculty",
                ),
                PBSBackendConfig(
                    name="acropolis-tl", type="pbs",
                    ssh=_ssh("acropolis.cluster.edu", "alice"),
                    queue="batch",
                ),
            ],
        )
        prompt = _render_agent_prompt(cfg)

        assert "`mercury-nb` (slurm)" in prompt
        assert "alice@mercury.cluster.edu" in prompt
        assert "pi-faculty" in prompt
        assert "`acropolis-tl` (pbs)" in prompt
        assert "alice@acropolis.cluster.edu" in prompt

    def test_partition_map_surfaced_in_backend_line(self):
        cfg = ScriptHutConfig(
            backends=[
                SlurmBackendConfig(
                    name="mercury-nb", type="slurm",
                    ssh=_ssh("m", "me"),
                    partition_map={"standard": "cpu", "gpu": "gpu-a100"},
                ),
            ],
        )
        prompt = _render_agent_prompt(cfg)
        assert "standard→cpu" in prompt
        assert "gpu→gpu-a100" in prompt

    def test_no_backends_shows_explicit_empty_note(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        assert "No backends configured" in prompt

    def test_stacks_listed_with_backends_and_inputs(self):
        cfg = ScriptHutConfig(
            stacks=[
                Stack(
                    name="julia",
                    backends=["mercury-nb"],
                    inputs={"julia_version": "1.11.3"},
                    prep="echo",
                ),
                Stack(name="python-ml", prep="pip install ..."),
            ],
        )
        prompt = _render_agent_prompt(cfg)
        assert "`julia`" in prompt
        assert "mercury-nb" in prompt
        assert "julia_version=1.11.3" in prompt
        # No `backends:` set means "all SSH" surface
        assert "`python-ml`" in prompt
        assert "all SSH backends" in prompt
        # The "check before running" reminder is present when stacks exist
        assert "stack check" in prompt

    def test_workflows_listed_with_description(self):
        cfg = ScriptHutConfig(
            workflows=[
                WorkflowConfig(
                    name="grid", backend="mercury-nb",
                    command="echo []",
                    description="parameter sweep",
                ),
            ],
        )
        prompt = _render_agent_prompt(cfg)
        assert "`grid`" in prompt
        assert "`mercury-nb`" in prompt
        assert "parameter sweep" in prompt


# ---------- no-config fallback -------------------------------------------


class TestNoConfigFallback:
    def test_no_config_says_so_but_still_teaches_cli(self):
        prompt = _render_agent_prompt(None)
        # Friendly note acknowledging no config
        assert "No `scripthut.yaml` was discovered" in prompt
        # Static reference still present
        assert "scripthut task run" in prompt
        assert "## Submitting work" in prompt
        assert "## Exit codes" in prompt
