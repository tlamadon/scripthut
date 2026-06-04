"""Unit tests for the env-rule resolver."""

from __future__ import annotations

import logging

import pytest

from scripthut.config_schema import EnvRule
from scripthut.runs.env import (
    LabeledRule,
    _expand,
    _matches,
    flatten,
    resolve,
    resolve_detailed,
)


def rule(**kwargs):
    """Helper: build an EnvRule using YAML-style 'if' alias."""
    return EnvRule(**kwargs)


# ---- ${name} expansion -----------------------------------------------------


def test_expand_no_vars():
    assert _expand("hello", {}) == "hello"


def test_expand_single_var():
    assert _expand("/scratch/${USER}", {"USER": "alice"}) == "/scratch/alice"


def test_expand_multiple_vars():
    env = {"BASE": "/data", "PROJ": "training"}
    assert _expand("${BASE}/${PROJ}/runs", env) == "/data/training/runs"


def test_expand_missing_var_logs_warning_and_uses_empty(caplog):
    with caplog.at_level(logging.WARNING):
        assert _expand("${MISSING}/x", {}) == "/x"
    assert any("MISSING" in r.message for r in caplog.records)


def test_expand_ignores_non_braced_dollar():
    # ${name} only — $name is left alone
    assert _expand("$PATH:/foo", {"PATH": "/bin"}) == "$PATH:/foo"


# ---- if: matcher -----------------------------------------------------------


def test_matches_single_string_equality():
    assert _matches({"K": "v"}, {"K": "v"}) is True
    assert _matches({"K": "v"}, {"K": "other"}) is False
    assert _matches({"K": "v"}, {}) is False


def test_matches_list_value_is_or():
    assert _matches({"K": ["a", "b", "c"]}, {"K": "b"}) is True
    assert _matches({"K": ["a", "b"]}, {"K": "z"}) is False


def test_matches_multi_key_is_and():
    if_ = {"A": "1", "B": "2"}
    assert _matches(if_, {"A": "1", "B": "2"}) is True
    assert _matches(if_, {"A": "1", "B": "x"}) is False
    assert _matches(if_, {"A": "1"}) is False


# ---- resolve(): always-applied set ----------------------------------------


def test_resolve_empty():
    env, init = resolve([], {})
    assert env == {}
    assert init == ""


def test_resolve_seed_carries_through():
    env, init = resolve([], {"SCRIPTHUT_RUN_ID": "r1"})
    assert env == {"SCRIPTHUT_RUN_ID": "r1"}
    assert init == ""


def test_resolve_single_set():
    env, init = resolve([rule(set={"FOO": "bar"})], {})
    assert env == {"FOO": "bar"}


def test_resolve_set_overrides_earlier_set():
    env, _ = resolve(
        [rule(set={"FOO": "1"}), rule(set={"FOO": "2"})],
        {},
    )
    assert env["FOO"] == "2"


# ---- resolve(): conditional ------------------------------------------------


def test_resolve_conditional_matches():
    env, _ = resolve(
        [rule(**{"if": {"BACKEND": "mercury"}, "set": {"SCRATCH": "/scratch"}})],
        {"BACKEND": "mercury"},
    )
    assert env["SCRATCH"] == "/scratch"


def test_resolve_conditional_skips_when_no_match():
    env, _ = resolve(
        [rule(**{"if": {"BACKEND": "mercury"}, "set": {"SCRATCH": "/scratch"}})],
        {"BACKEND": "anvil"},
    )
    assert "SCRATCH" not in env


def test_resolve_conditional_list_or():
    rules = [
        rule(**{"if": {"BACKEND": ["mercury", "delta"]}, "set": {"X": "ok"}}),
    ]
    assert resolve(rules, {"BACKEND": "mercury"})[0]["X"] == "ok"
    assert resolve(rules, {"BACKEND": "delta"})[0]["X"] == "ok"
    assert "X" not in resolve(rules, {"BACKEND": "anvil"})[0]


def test_resolve_conditional_sees_earlier_rules():
    """A rule's if: can match a key set by an earlier rule."""
    rules = [
        rule(set={"MODE": "gpu"}),
        rule(**{"if": {"MODE": "gpu"}, "set": {"CUDA": "11"}}),
    ]
    env, _ = resolve(rules, {})
    assert env == {"MODE": "gpu", "CUDA": "11"}


# ---- resolve(): append -----------------------------------------------------


def test_resolve_append_to_empty_starts_value():
    env, _ = resolve([rule(append={"PATH": "/foo"})], {})
    assert env["PATH"] == "/foo"


def test_resolve_append_joins_with_colon():
    env, _ = resolve(
        [rule(set={"PATH": "/a"}), rule(append={"PATH": "/b"})],
        {},
    )
    assert env["PATH"] == "/a:/b"


def test_resolve_append_accumulates_across_rules():
    rules = [
        rule(append={"PATH": "/a"}),
        rule(append={"PATH": "/b"}),
        rule(append={"PATH": "/c"}),
    ]
    env, _ = resolve(rules, {})
    assert env["PATH"] == "/a:/b:/c"


# ---- resolve(): init -------------------------------------------------------


def test_resolve_init_concatenates():
    rules = [
        rule(init="source /etc/profile"),
        rule(init="module load gcc"),
    ]
    _, init = resolve(rules, {})
    assert init == "source /etc/profile\nmodule load gcc"


def test_resolve_init_skipped_when_guard_fails():
    rules = [
        rule(**{"if": {"BACKEND": "mercury"}, "init": "module load cuda"}),
    ]
    _, init = resolve(rules, {"BACKEND": "anvil"})
    assert init == ""


def test_resolve_init_expands_variables():
    rules = [rule(set={"USER": "alice"}, init="cd /home/${USER}")]
    _, init = resolve(rules, {})
    assert init == "cd /home/alice"


# ---- resolve(): expansion + cascading -------------------------------------


def test_resolve_set_expands_against_earlier_set():
    rules = [
        rule(set={"BASE": "/data"}),
        rule(set={"OUT": "${BASE}/run"}),
    ]
    env, _ = resolve(rules, {})
    assert env["OUT"] == "/data/run"


def test_resolve_set_expands_against_seed():
    env, _ = resolve(
        [rule(set={"OUT": "/scratch/${SCRIPTHUT_RUN_ID}"})],
        {"SCRIPTHUT_RUN_ID": "r-7"},
    )
    assert env["OUT"] == "/scratch/r-7"


def test_resolve_append_expands_value():
    rules = [
        rule(set={"BASE": "/data"}),
        rule(set={"PATH": "/bin"}),
        rule(append={"PATH": "${BASE}/bin"}),
    ]
    env, _ = resolve(rules, {})
    assert env["PATH"] == "/bin:/data/bin"


# ---- resolve(): SCRIPTHUT_* protection -------------------------------------


def test_resolve_set_protected_key_is_ignored(caplog):
    rules = [rule(set={"SCRIPTHUT_RUN_ID": "hacked"})]
    with caplog.at_level(logging.WARNING):
        env, _ = resolve(rules, {"SCRIPTHUT_RUN_ID": "real"})
    assert env["SCRIPTHUT_RUN_ID"] == "real"
    assert any("protected" in r.message for r in caplog.records)


def test_resolve_append_protected_key_is_ignored(caplog):
    rules = [rule(append={"SCRIPTHUT_BACKEND": "extra"})]
    with caplog.at_level(logging.WARNING):
        env, _ = resolve(rules, {"SCRIPTHUT_BACKEND": "mercury"})
    assert env["SCRIPTHUT_BACKEND"] == "mercury"


# ---- resolve_detailed(): provenance ---------------------------------------


def test_resolve_detailed_records_seed_provenance():
    _, _, prov = resolve_detailed([], {"SCRIPTHUT_RUN_ID": "r1"})
    assert prov["SCRIPTHUT_RUN_ID"].ops == [("seed", "seed", "r1")]


def test_resolve_detailed_records_labeled_source():
    rules = [
        LabeledRule(rule(set={"FOO": "1"}), source="backend:mercury"),
        LabeledRule(rule(set={"FOO": "2"}), source="task"),
    ]
    _, _, prov = resolve_detailed(rules, {})
    assert prov["FOO"].value == "2"
    assert [op[:2] for op in prov["FOO"].ops] == [
        ("backend:mercury", "set"),
        ("task", "set"),
    ]


def test_resolve_detailed_records_append_chain():
    rules = [
        LabeledRule(rule(set={"PATH": "/a"}), source="backend"),
        LabeledRule(rule(append={"PATH": "/b"}), source="workflow"),
        LabeledRule(rule(append={"PATH": "/c"}), source="task"),
    ]
    _, _, prov = resolve_detailed(rules, {})
    assert prov["PATH"].value == "/a:/b:/c"
    assert [(o[0], o[1]) for o in prov["PATH"].ops] == [
        ("backend", "set"),
        ("workflow", "append"),
        ("task", "append"),
    ]


# ---- end-to-end "module load" example from the design ---------------------


def test_design_scenario_per_backend_module_load():
    """Mirrors the canonical example from the design discussion."""
    rules = [
        rule(set={"PROJECT": "training"}),
        rule(
            **{
                "if": {"SCRIPTHUT_BACKEND": "mercury"},
                "set": {"SCRATCH": "/scratch/${USER}"},
                "init": "module load gcc/12 cuda/11",
            }
        ),
        rule(
            **{
                "if": {"SCRIPTHUT_BACKEND": ["anvil", "delta"]},
                "set": {"SCRATCH": "/tmp/work/${USER}"},
                "init": "module load gcc cuda-toolkit",
            }
        ),
        rule(
            **{
                "if": {"SCRIPTHUT_BACKEND": "mercury", "GPU": "1"},
                "append": {"PATH": "/opt/cuda/bin"},
            }
        ),
    ]

    # Run on mercury without GPU
    env, init = resolve(rules, {"SCRIPTHUT_BACKEND": "mercury", "USER": "alice"})
    assert env["PROJECT"] == "training"
    assert env["SCRATCH"] == "/scratch/alice"
    assert init == "module load gcc/12 cuda/11"
    assert "PATH" not in env

    # Run on mercury with GPU
    env, init = resolve(
        rules,
        {"SCRIPTHUT_BACKEND": "mercury", "USER": "alice", "GPU": "1"},
    )
    assert env["PATH"] == "/opt/cuda/bin"

    # Run on anvil
    env, init = resolve(rules, {"SCRIPTHUT_BACKEND": "anvil", "USER": "bob"})
    assert env["SCRATCH"] == "/tmp/work/bob"
    assert init == "module load gcc cuda-toolkit"

    # Run on an unknown cluster — no SCRATCH set, no module load
    env, init = resolve(rules, {"SCRIPTHUT_BACKEND": "laptop", "USER": "carol"})
    assert "SCRATCH" not in env
    assert init == ""


# ---- flatten() / env_groups (include:) -------------------------------------


def _lr(r, source="task", guards=None):
    return LabeledRule(r, source=source, extra_guards=guards or [])


def test_flatten_no_includes_passthrough():
    rules = [_lr(rule(set={"FOO": "1"})), _lr(rule(append={"PATH": "/x"}))]
    assert flatten(rules, {}) == rules


def test_flatten_expands_include_in_order():
    groups = {
        "gpu": [rule(init="module load cuda"), rule(append={"PATH": "/opt/cuda/bin"})],
    }
    rules = [
        _lr(rule(set={"BEFORE": "1"}), source="workflow:train"),
        _lr(rule(include=["gpu"]), source="workflow:train"),
        _lr(rule(set={"AFTER": "1"}), source="workflow:train"),
    ]
    out = flatten(rules, groups)
    assert [r.source for r in out] == [
        "workflow:train",
        "workflow:train via group:gpu",
        "workflow:train via group:gpu",
        "workflow:train",
    ]
    assert out[1].rule.init == "module load cuda"
    assert out[2].rule.append == {"PATH": "/opt/cuda/bin"}


def test_flatten_include_rule_with_extras_keeps_extras():
    """A rule with both include: and set: keeps the set: after the included rules."""
    groups = {"base": [rule(set={"FROM_GROUP": "g"})]}
    rules = [_lr(rule(include=["base"], set={"FROM_PARENT": "p"}), source="task")]
    out = flatten(rules, groups)
    assert [r.rule.set for r in out] == [{"FROM_GROUP": "g"}, {"FROM_PARENT": "p"}]


def test_flatten_pure_include_rule_does_not_remain():
    """A rule whose only field is include: vanishes after flatten."""
    groups = {"g": [rule(set={"X": "1"})]}
    rules = [_lr(rule(include=["g"]))]
    out = flatten(rules, groups)
    assert len(out) == 1
    assert out[0].rule.set == {"X": "1"}


def test_flatten_multiple_includes_in_one_rule_in_listed_order():
    groups = {
        "a": [rule(set={"FROM": "a"})],
        "b": [rule(set={"FROM": "b"})],
    }
    rules = [_lr(rule(include=["a", "b"]))]
    env, _ = resolve(flatten(rules, groups), {})
    assert env["FROM"] == "b"  # b included after a, so it wins


def test_flatten_unknown_group_warns_and_skips(caplog):
    rules = [_lr(rule(include=["missing"]))]
    with caplog.at_level(logging.WARNING):
        out = flatten(rules, {})
    assert out == []
    assert any("missing" in r.message for r in caplog.records)


def test_flatten_nested_includes():
    groups = {
        "outer": [rule(include=["inner"]), rule(set={"FROM_OUTER": "1"})],
        "inner": [rule(set={"FROM_INNER": "1"})],
    }
    rules = [_lr(rule(include=["outer"]))]
    out = flatten(rules, groups)
    sets = [r.rule.set for r in out]
    assert {"FROM_INNER": "1"} in sets
    assert {"FROM_OUTER": "1"} in sets
    # Nested source label
    nested = next(r for r in out if r.rule.set == {"FROM_INNER": "1"})
    assert "group:outer via group:inner" in nested.source


def test_flatten_detects_direct_cycle():
    groups = {"a": [rule(include=["a"])]}
    rules = [_lr(rule(include=["a"]))]
    with pytest.raises(ValueError, match="cycle"):
        flatten(rules, groups)


def test_flatten_detects_indirect_cycle():
    groups = {
        "a": [rule(include=["b"])],
        "b": [rule(include=["a"])],
    }
    rules = [_lr(rule(include=["a"]))]
    with pytest.raises(ValueError, match="cycle"):
        flatten(rules, groups)


def test_flatten_inherits_parent_guard():
    """Inner rules inherit the parent include rule's if-guard."""
    groups = {"g": [rule(set={"X": "1"})]}
    rules = [_lr(rule(if_={"BACKEND": "mercury"}, include=["g"]))]
    out = flatten(rules, groups)
    assert out[0].extra_guards == [{"BACKEND": "mercury"}]

    # Resolve confirms the guard applies
    env, _ = resolve(out, {"BACKEND": "mercury"})
    assert env["X"] == "1"
    env, _ = resolve(out, {"BACKEND": "anvil"})
    assert "X" not in env


def test_flatten_ands_multiple_guards():
    """Parent guard + child guard both must match — including conflicting keys."""
    groups = {"g": [rule(if_={"GPU": "1"}, set={"X": "1"})]}
    rules = [_lr(rule(if_={"BACKEND": "mercury"}, include=["g"]))]
    out = flatten(rules, groups)
    # The rule has both its own if_ (GPU=1) and an inherited guard (BACKEND=mercury)
    assert out[0].extra_guards == [{"BACKEND": "mercury"}]
    assert out[0].rule.if_ == {"GPU": "1"}

    # Both must match
    env, _ = resolve(out, {"BACKEND": "mercury", "GPU": "1"})
    assert env["X"] == "1"
    env, _ = resolve(out, {"BACKEND": "mercury", "GPU": "0"})
    assert "X" not in env
    env, _ = resolve(out, {"BACKEND": "anvil", "GPU": "1"})
    assert "X" not in env


def test_flatten_guards_with_conflicting_keys_never_match():
    """If two guards constrain the same key to different values, rule can never apply."""
    # Parent says BACKEND=mercury, child group says BACKEND=anvil — impossible
    groups = {"g": [rule(if_={"BACKEND": "anvil"}, set={"X": "1"})]}
    rules = [_lr(rule(if_={"BACKEND": "mercury"}, include=["g"]))]
    out = flatten(rules, groups)
    env, _ = resolve(out, {"BACKEND": "mercury"})
    assert "X" not in env
    env, _ = resolve(out, {"BACKEND": "anvil"})
    assert "X" not in env


# ---- stacks: references ----------------------------------------------------


from scripthut.config_schema import Stack as _Stack


def _stack(name: str, init: str = "") -> _Stack:
    return _Stack(name=name, init=init, prep="echo prep")


def test_flatten_stacks_emits_init_rule():
    """A `stacks: [name]` reference expands to a synthetic init rule."""
    stacks = {"julia-1.12": _stack("julia-1.12", init="module load julia/1.12")}
    rules = [_lr(rule(stacks=["julia-1.12"]))]
    out = flatten(rules, {}, stacks)
    assert len(out) == 1
    assert out[0].rule.init == "module load julia/1.12"
    # Source is annotated so provenance still tracks the original reference.
    assert "stack:julia-1.12" in out[0].source


def test_flatten_stacks_then_set_apply_in_order():
    """Same rule with stacks: AND set: — stack's init expands first, then
    the rule's own set runs after. Lets workflows do
    ``stacks: [foo], set: {OVERRIDE: '1'}`` to layer on top.
    """
    stacks = {"foo": _stack("foo", init="echo stack-init")}
    r = rule(stacks=["foo"], set={"X": "1"})
    out = flatten([_lr(r)], {}, stacks)
    # First the stack's init, then the original rule (carrying set).
    assert len(out) == 2
    assert out[0].rule.init == "echo stack-init"
    assert out[1].rule.set == {"X": "1"}


def test_flatten_unknown_stack_raises():
    """Stack typos must fail loudly. Otherwise the task runs without the
    env the author expected — silent dropping is the worst failure mode.
    """
    rules = [_lr(rule(stacks=["typo-1.12"]))]
    with pytest.raises(ValueError, match="stack 'typo-1.12'"):
        flatten(rules, {}, {})


def test_flatten_empty_init_stack_is_noop():
    """A stack with no init: produces no env contribution, no error."""
    stacks = {"silent": _stack("silent", init="")}
    rules = [_lr(rule(stacks=["silent"]))]
    out = flatten(rules, {}, stacks)
    assert out == []


def test_flatten_multiple_stacks_in_one_rule():
    stacks = {
        "a": _stack("a", init="load A"),
        "b": _stack("b", init="load B"),
    }
    rules = [_lr(rule(stacks=["a", "b"]))]
    out = flatten(rules, {}, stacks)
    assert [r.rule.init for r in out] == ["load A", "load B"]


def test_flatten_stack_with_if_guard_inherits():
    """A stacks: rule under an if-guard means the expanded init is also
    guarded — useful for backend-specific stack selection.
    """
    stacks = {"cuda": _stack("cuda", init="export CUDA_HOME=/x")}
    rules = [_lr(rule(if_={"GPU": "1"}, stacks=["cuda"]))]
    out = flatten(rules, {}, stacks)
    assert out[0].extra_guards == [{"GPU": "1"}]
    env, init = resolve(out, {"GPU": "1"})
    assert init == "export CUDA_HOME=/x"
    env, init = resolve(out, {"GPU": "0"})
    assert init == ""


def test_flatten_include_and_stacks_compose():
    """A rule can both include an env_group AND reference stacks. The
    expansion order is groups first (per existing convention), then
    stacks, then the rule's own set/init.
    """
    groups = {"g": [rule(set={"FROM_GROUP": "1"})]}
    stacks = {"s": _stack("s", init="from-stack")}
    rules = [_lr(rule(include=["g"], stacks=["s"], set={"FROM_RULE": "1"}))]
    out = flatten(rules, groups, stacks)
    # Group expanded (1) + stack expanded (1) + original rule (carrying set).
    assert len(out) == 3
    env, init = resolve(out, {})
    assert env["FROM_GROUP"] == "1"
    assert env["FROM_RULE"] == "1"
    assert init == "from-stack"


# ---- collect_stacks --------------------------------------------------------


def test_collect_stacks_server_only():
    from scripthut.config_schema import ScriptHutConfig
    from scripthut.runs.env import collect_stacks

    cfg = ScriptHutConfig(stacks=[_stack("base")])
    out = collect_stacks(cfg)
    assert set(out) == {"base"}


def test_collect_stacks_doc_overrides_server():
    """Repo-project stacks (passed as doc_stacks) win on name collision."""
    from scripthut.config_schema import ScriptHutConfig
    from scripthut.runs.env import collect_stacks

    cfg = ScriptHutConfig(stacks=[_stack("julia", init="from-server")])
    doc = {"julia": _stack("julia", init="from-repo")}
    out = collect_stacks(cfg, doc_stacks=doc)
    assert out["julia"].init == "from-repo"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
