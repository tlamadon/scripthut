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
    GitSourceConfig,
    PathSourceConfig,
    PBSBackendConfig,
    SSHConfig,
    ScriptHutConfig,
    SlurmBackendConfig,
    Stack,
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
        )
        prompt = _render_agent_prompt(cfg)

        # Headings we care about — pinned so renames are deliberate.
        for heading in (
            "# ScriptHut Agent Brief",
            "## What's available here",
            "### Backends",
            "### Stacks (reusable software environments)",
            "## Submitting work — pick the smallest tool that fits",
            "### A) `--inline-script <local-file>`",
            "### B) Positional command (one-liner)",
            "### C) `--from-stdin`",
            "### TaskDefinition shape",
            "## Resource sizing — default small, escalate deliberately",
            "## Inspecting state",
            "## Exit codes",
            "## Gotchas",
            "## Typical agent loop — verify, then submit",
        ):
            assert heading in prompt, f"missing heading: {heading!r}"

    def test_static_invocation_patterns_appear(self):
        # The prompt must teach these exact entry points or an agent
        # following it will get stuck. Pin them.
        cfg = ScriptHutConfig()
        prompt = _render_agent_prompt(cfg)
        for snippet in (
            "scripthut task run",
            "--inline-script",
            "--from-stdin",
            "--dry-run",
            "--backend",
            "--json",
            "scripthut run view",
            "scripthut run logs",
            "scripthut stack check",
            "scripthut backend list",
            "scripthut workflow run",
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

    def test_git_source_surfaces_branch_url_and_default_backend(self):
        """The agent needs to see the branch — `workflow run` syncs to HEAD
        on that branch, so the user picking the wrong branch is the most
        likely silent failure mode for "wrong code ran".
        """
        cfg = ScriptHutConfig(
            sources=[
                GitSourceConfig(
                    name="balke-jmp",
                    url="git@github.com:computecon/balke-jmp.git",
                    branch="main",
                    backend="mercury-nb",
                ),
            ],
        )
        prompt = _render_agent_prompt(cfg)
        assert "### Sources" in prompt
        assert "`balke-jmp` (git)" in prompt
        assert "git@github.com:computecon/balke-jmp.git" in prompt
        assert "branch `main`" in prompt
        assert "default backend `mercury-nb`" in prompt
        # The latest-HEAD-on-branch behavior must be explained — otherwise
        # the agent will be surprised when reruns pick up new commits.
        assert "latest HEAD" in prompt
        # Refresh instructions for newly-pushed workflow files.
        assert "source sync" in prompt

    def test_path_source_surfaces_backend_and_path(self):
        cfg = ScriptHutConfig(
            sources=[
                PathSourceConfig(
                    name="sandbox",
                    backend="mercury-nb",
                    path="/scratch/me/sandbox",
                ),
            ],
        )
        prompt = _render_agent_prompt(cfg)
        assert "`sandbox` (path)" in prompt
        assert "path `/scratch/me/sandbox` on `mercury-nb`" in prompt

    def test_no_sources_still_shows_section_with_guidance(self):
        cfg = ScriptHutConfig(backends=[
            SlurmBackendConfig(name="b", type="slurm", ssh=_ssh()),
        ])
        prompt = _render_agent_prompt(cfg)
        # Section heading should always be present when config is populated.
        assert "### Sources" in prompt
        assert "No sources configured" in prompt


# ---------- regression: no project-era stragglers ------------------------


class TestNoProjectStragglers:
    """The `projects` config concept was removed in 0.6.0. The agent prompt
    must not still teach `--project` or `scripthut project view`, or agents
    following the briefing will issue commands that no longer exist.
    """

    def test_no_dash_dash_project_flag_appears(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        assert "--project" not in prompt

    def test_no_project_view_subcommand_appears(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        assert "scripthut project" not in prompt
        assert "project view" not in prompt
        assert "project list" not in prompt


# ---------- status + sync + log surfaces ---------------------------------


class TestYamlEditingGuidance:
    """Agents are routinely asked to add env vars / stacks / workflows to
    the user's YAML. Without targeted guidance they're likely to (a) put
    the change in the wrong file, (b) overwrite the whole YAML, or
    (c) miss the env-rule syntax. Pin the basics.
    """

    def test_yaml_editing_section_present(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        assert "## Editing scripthut.yaml" in prompt

    def test_two_layer_model_is_explained(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        # Both file paths are explicitly named so the agent picks the right one.
        assert "~/.config/scripthut/scripthut.yaml" in prompt
        assert "./scripthut.yaml" in prompt
        # The four project-local-allowed sections are named.
        for section in ("stacks", "workflows", "env", "env_groups"):
            assert section in prompt
        # The four global-only sections are named, with the loader-rejects
        # consequence explicit.
        for section in ("backends", "sources", "settings", "pricing"):
            assert section in prompt
        assert "ConfigError" in prompt or "rejects" in prompt or "rejected" in prompt

    def test_env_rule_shape_taught_with_all_common_keys(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        # The schema's five fields on EnvRule — agents see at least the
        # action ones plus the guard.
        for key in ("set:", "if:", "include:", "append:", "init:"):
            assert key in prompt
        # The non-obvious semantic — AND across keys in `if:`.
        assert "AND" in prompt
        # Resolver order so the agent knows which layer wins.
        assert "server → backend → repo-project → workflow-doc → task" in prompt

    def test_merge_semantics_documented(self):
        """The agent needs to know: env CONCATS, env_groups MERGE,
        stacks/workflows OVERRIDE-BY-NAME. Different from each other.
        """
        prompt = _render_agent_prompt(ScriptHutConfig())
        assert "concatenated" in prompt          # env
        assert "dict-merged" in prompt           # env_groups
        assert "by-name override" in prompt      # stacks / workflows

    def test_edit_discipline_taught(self):
        """The most expensive failure mode for YAML edits: overwriting
        the file. Make sure the briefing tells the agent not to.
        """
        prompt = _render_agent_prompt(ScriptHutConfig())
        # Read-first is non-negotiable.
        assert "Read the file first" in prompt
        # Minimal-diff is the operating principle.
        assert "minimal diff" in prompt
        # Hot-reload vs restart — agent shouldn't blindly tell the user
        # to restart.
        assert "hot-reload" in prompt.lower()


class TestStackGuidance:
    """The most common follow-up question after env_groups is "how do I
    get a fresh Julia/CUDA/conda env onto every task?" The briefing must
    teach the full define → install → reference workflow plus the two
    things that bite hardest (unknown name → ValueError, no auto-install)
    so agents don't suggest `stacks: [name]` without first checking
    `stack check`.
    """

    def test_stacks_section_present(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        # The dedicated section header.
        assert "## Stacks — define once, install once, reference everywhere" in prompt

    def test_define_step_is_documented_with_required_fields(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        # The four schema fields the author actually needs to know.
        assert "name:" in prompt
        assert "prep:" in prompt
        assert "init:" in prompt
        assert "inputs:" in prompt
        # Server vs. per-repo placement explicitly contrasted.
        assert "Server-global" in prompt
        assert "Per-repo" in prompt

    def test_install_step_is_documented(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        assert "scripthut stack check" in prompt
        assert "scripthut stack install" in prompt
        # The --source variant — the v0.7.0 feature for installing
        # repo-defined stacks without first cloning.
        assert "--source" in prompt
        # The rebuild option is documented (common follow-up question).
        assert "--rebuild" in prompt

    def test_reference_step_shows_stacks_env_rule(self):
        """The v0.7.1 new thing — using `stacks: [name]` on env rules."""
        prompt = _render_agent_prompt(ScriptHutConfig())
        # The JSON usage example with the new field.
        assert '"stacks":' in prompt or '"stacks"' in prompt
        # The YAML env_groups variant — common pattern.
        assert "stacks: [julia-1.12]" in prompt

    def test_critical_gotchas_are_taught(self):
        """The two failure modes that bite hardest must be loud."""
        prompt = _render_agent_prompt(ScriptHutConfig())
        # Unknown name → loud failure, not silent skip.
        assert "Unknown stack name" in prompt
        assert "ValueError" in prompt
        # No auto-install — operator's responsibility.
        assert "No auto-install" in prompt
        # The check-before-running pattern is the recovery path.
        assert "stack check" in prompt


class TestTaskOutputsGuidance:
    """v0.11.0's task-outputs feature only shows up in the UI if the
    task author / agent knows to write to ``$SCRIPTHUT_OUTPUT_DIR`` and
    friends. The agent prompt must teach the three env vars + the
    behaviors that bite (SSH-only, post-completion-not-streaming,
    size caps).
    """

    def test_section_present(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        assert "## Emitting structured outputs" in prompt

    def test_all_three_env_vars_documented(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        # All three convention paths must be named — losing any one
        # would silently hide a UI surface from agents.
        assert "$SCRIPTHUT_OUTPUT_DIR" in prompt
        assert "$SCRIPTHUT_TASK_SUMMARY" in prompt
        assert "$SCRIPTHUT_RUN_SUMMARY" in prompt

    def test_per_task_vs_per_run_distinction_taught(self):
        """The user asked for these two surfaces specifically — the
        agent must understand the difference and pick the right one.
        """
        prompt = _render_agent_prompt(ScriptHutConfig())
        # Both UI panels named.
        assert "Outputs" in prompt
        assert "Run Summary" in prompt
        # The when-to-use guidance with a clear distinction.
        assert "Per-task" in prompt or "per-task" in prompt
        assert "Run-wide" in prompt or "run-wide" in prompt

    def test_critical_caveats_taught(self):
        """The two failure modes that bite hardest: backends-without-
        SSH silently skip collection, and the panel doesn't stream
        (it appears after the task finishes).
        """
        prompt = _render_agent_prompt(ScriptHutConfig())
        # SSH-only scope — agents shouldn't promise UI panels on Batch/EC2.
        assert "Slurm" in prompt
        assert "Batch" in prompt
        # Post-completion, not streaming.
        assert "post-completion" in prompt.lower() or "after" in prompt.lower()
        # Size caps so agents don't write 10k plots in a training loop.
        assert "5 MB" in prompt or "5MB" in prompt
        assert "200 files" in prompt or "200 files per" in prompt

    def test_worked_example_present(self):
        """A real bash snippet the agent can paste/adapt. Pin the
        markdown table syntax so future drift doesn't quietly drop it.
        """
        prompt = _render_agent_prompt(ScriptHutConfig())
        assert 'savefig' in prompt or '.png' in prompt
        # The convention path appears in a redirect — confirms the
        # snippet shows the write pattern, not just the path.
        assert '> "$SCRIPTHUT_TASK_SUMMARY"' in prompt
        assert '> "$SCRIPTHUT_RUN_SUMMARY"' in prompt


class TestObservabilitySurfaces:
    """The user explicitly asked: the agent must know how to check
    status, output, and logs, and how to refresh sources.
    """

    def test_status_command_is_taught(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        # Both in the cheat sheet AND in the verify checklist.
        assert "scripthut status" in prompt
        # And tied to the "before anything else" intent so the agent
        # actually runs it first.
        assert "reachable" in prompt or "auth is working" in prompt

    def test_source_sync_command_is_taught(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        assert "scripthut source sync" in prompt

    def test_logs_stdout_and_stderr_both_taught(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        assert "scripthut run logs" in prompt
        assert "--error" in prompt   # stderr access
        assert "--tail" in prompt    # historical access
        assert "-f" in prompt        # live tailing


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


# ---------- cache / probe / manifest / local backend ----------------------


class TestCacheProbeManifestContent:
    def test_cache_probe_manifest_patterns_present(self):
        """The prompt must teach the cache-adjacent entry points added in
        v0.13: the dry-run probe, the manifest command, and the caching
        TaskDefinition fields."""
        prompt = _render_agent_prompt(ScriptHutConfig())
        for snippet in (
            "## Result cache, probe, and manifests",
            "scripthut task probe",
            "--from-file",
            "scripthut run manifest",
            "POST /api/v1/tasks/probe",
            "`cache_scope`",
            "`inputs`",
            "`outputs`",
        ):
            assert snippet in prompt, f"missing pattern: {snippet!r}"

    def test_cache_inventory_reflects_enabled_store(self):
        from scripthut.config_schema import CacheConfig

        cfg = ScriptHutConfig(
            cache=CacheConfig(enabled=True, store="s3://bucket/prefix"),
        )
        prompt = _render_agent_prompt(cfg)
        assert "### Result cache" in prompt
        assert "**Enabled**" in prompt
        assert "s3://bucket/prefix" in prompt

    def test_cache_inventory_reflects_disabled(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        assert "### Result cache" in prompt
        assert "Disabled" in prompt

    def test_local_backend_listed_with_annotation(self):
        from scripthut.config_schema import LocalBackendConfig

        cfg = ScriptHutConfig(
            backends=[LocalBackendConfig(name="laptop", max_concurrent=4)],
        )
        prompt = _render_agent_prompt(cfg)
        assert "`laptop` (local)" in prompt
        assert "subprocesses on the scripthut host" in prompt

    def test_empty_backends_mentions_auto_local(self):
        prompt = _render_agent_prompt(ScriptHutConfig())
        assert "No backends configured" in prompt
        assert "auto-registers a" in prompt
        assert "--backend local" in prompt
