"""Env-rule resolution for tasks.

The resolver collects ``EnvRule`` lists from every layer that contributes
environment configuration — backend, server, workflow, task — concatenates
them, and evaluates them in order against a seed of ``SCRIPTHUT_*`` runtime
variables.

Design notes:
- Conditionals see the env as resolved so far, so later rules can react to
  earlier rules (including the SCRIPTHUT_* seed).
- ``if:`` is AND across keys; list values inside one if-clause mean OR.
- ``${name}`` substitution is applied to every string in ``set:``, ``append:``,
  and ``init:`` against the env-so-far. Missing keys expand to "" with a warning.
- Keys starting with ``SCRIPTHUT_`` are protected — rules cannot overwrite or
  append to them. The resolver warns and skips silently.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

from scripthut.config_schema import EnvRule, ScriptHutConfig, Stack
from scripthut.runs.models import TaskDefinition

logger = logging.getLogger(__name__)

_PROTECTED_PREFIX = "SCRIPTHUT_"
_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass
class LabeledRule:
    """A rule paired with the layer it came from (for provenance).

    ``extra_guards`` carries if-guards inherited from outer ``include:`` rules.
    Every guard plus the rule's own ``if_`` must match for the rule to apply.
    """

    rule: EnvRule
    source: str  # e.g. "backend:mercury", "server", "workflow:train", "task"
    extra_guards: list[dict[str, str | list[str]]] = field(default_factory=list)


@dataclass
class Provenance:
    """Per-key audit trail of every write to that key during resolution."""

    value: str
    ops: list[tuple[str, str, str]] = field(default_factory=list)
    # Each tuple: (source_label, op_name in {"seed","set","append"}, contribution_string)


def _expand(value: str, env: Mapping[str, str]) -> str:
    """Substitute ``${name}`` occurrences in *value* against *env*."""

    def _replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in env:
            logger.warning("env expansion: ${%s} undefined, using empty string", key)
            return ""
        return env[key]

    return _VAR_PATTERN.sub(_replace, value)


def _matches(if_block: Mapping[str, str | list[str]], env: Mapping[str, str]) -> bool:
    """AND across keys; OR across list values inside one key."""
    for k, expected in if_block.items():
        actual = env.get(k)
        if isinstance(expected, list):
            if actual not in expected:
                return False
        elif actual != expected:
            return False
    return True


def _is_protected(key: str) -> bool:
    return key.startswith(_PROTECTED_PREFIX)


def resolve(
    rules: list[EnvRule] | list[LabeledRule],
    seed: Mapping[str, str],
) -> tuple[dict[str, str], str]:
    """Apply *rules* in order against *seed*. Returns ``(env, extra_init)``."""
    env, init, _ = resolve_detailed(rules, seed)
    return env, init


def resolve_detailed(
    rules: list[EnvRule] | list[LabeledRule],
    seed: Mapping[str, str],
) -> tuple[dict[str, str], str, dict[str, Provenance]]:
    """Resolve with per-key provenance tracking.

    Accepts either bare ``EnvRule``s (provenance source becomes ``rule[i]``)
    or ``LabeledRule``s (provenance source is the rule's ``source`` label).
    """
    env: dict[str, str] = {}
    prov: dict[str, Provenance] = {}
    for k, v in seed.items():
        env[k] = v
        prov[k] = Provenance(value=v, ops=[("seed", "seed", v)])

    init_parts: list[str] = []
    for index, item in enumerate(rules):
        if isinstance(item, LabeledRule):
            rule, source, extra_guards = item.rule, item.source, item.extra_guards
        else:
            rule, source, extra_guards = item, f"rule[{index}]", []

        if any(not _matches(g, env) for g in extra_guards):
            continue
        if rule.if_ is not None and not _matches(rule.if_, env):
            continue

        for key, raw in rule.set.items():
            if _is_protected(key):
                logger.warning(
                    "env rule %s attempted to set protected key '%s'; ignored",
                    source, key,
                )
                continue
            value = _expand(raw, env)
            env[key] = value
            p = prov.setdefault(key, Provenance(value=value))
            p.value = value
            p.ops.append((source, "set", value))

        for key, raw in rule.append.items():
            if _is_protected(key):
                logger.warning(
                    "env rule %s attempted to append to protected key '%s'; ignored",
                    source, key,
                )
                continue
            contribution = _expand(raw, env)
            existing = env.get(key, "")
            value = f"{existing}:{contribution}" if existing else contribution
            env[key] = value
            p = prov.setdefault(key, Provenance(value=value))
            p.value = value
            p.ops.append((source, "append", contribution))

        if rule.init:
            init_parts.append(_expand(rule.init, env))

    return env, "\n".join(init_parts), prov


def build_seed(
    *,
    backend_name: str,
    workflow_name: str,
    run_id: str,
    created_at: datetime,
    git_repo: str | None = None,
    git_branch: str | None = None,
    git_sha: str | None = None,
) -> dict[str, str]:
    """Build the SCRIPTHUT_* runtime seed env."""
    seed: dict[str, str] = {
        "SCRIPTHUT_BACKEND": backend_name,
        "SCRIPTHUT_WORKFLOW": workflow_name,
        "SCRIPTHUT_RUN_ID": run_id,
        "SCRIPTHUT_CREATED_AT": created_at.isoformat(),
    }
    if git_repo is not None:
        seed["SCRIPTHUT_GIT_REPO"] = git_repo
    if git_branch is not None:
        seed["SCRIPTHUT_GIT_BRANCH"] = git_branch
    if git_sha is not None:
        seed["SCRIPTHUT_GIT_SHA"] = git_sha
    return seed


def collect_rules(
    config: ScriptHutConfig,
    *,
    backend_name: str,
    workflow_name: str,
    task: TaskDefinition,
    doc_env: list[EnvRule] | None = None,
) -> list[LabeledRule]:
    """Concatenate rules from every layer in fixed order, with source labels.

    Layer order: backend → server → workflow-doc (the generator's JSON
    top-level ``env:``) → task.
    """
    out: list[LabeledRule] = []
    backend = config.get_backend(backend_name)
    if backend is not None and getattr(backend, "env", None):
        out.extend(LabeledRule(r, f"backend:{backend_name}") for r in backend.env)
    out.extend(LabeledRule(r, "server") for r in config.env)
    if doc_env:
        out.extend(LabeledRule(r, "workflow-doc") for r in doc_env)
    out.extend(LabeledRule(r, "task") for r in task.env)
    return out


def collect_groups(
    config: ScriptHutConfig,
    *,
    backend_name: str,
    workflow_name: str,
    doc_env_groups: dict[str, list[EnvRule]] | None = None,
) -> dict[str, list[EnvRule]]:
    """Merge env_groups from every layer; later layers shadow earlier by name.

    Order: backend → server → workflow-doc (the JSON document's top-level
    ``env_groups:``). Later definitions win.
    """
    groups: dict[str, list[EnvRule]] = {}
    backend = config.get_backend(backend_name)
    if backend is not None:
        groups.update(getattr(backend, "env_groups", {}) or {})
    groups.update(getattr(config, "env_groups", {}) or {})
    if doc_env_groups:
        groups.update(doc_env_groups)
    return groups


def collect_stacks(
    config: ScriptHutConfig,
    *,
    doc_stacks: dict[str, Stack] | None = None,
) -> dict[str, Stack]:
    """Build the merged ``name → Stack`` map the resolver references.

    Server config's ``stacks:`` is the base; ``doc_stacks`` overlays on
    top (already pre-merged in ``RunManager.create_run_from_source`` to
    include the source repo's project-local stacks when present). The
    by-name override convention matches ``_merge_configs`` — a more
    specific layer wins.
    """
    stacks: dict[str, Stack] = {s.name: s for s in config.stacks}
    if doc_stacks:
        stacks.update(doc_stacks)
    return stacks


def flatten(
    rules: list[LabeledRule],
    groups: Mapping[str, list[EnvRule]],
    stacks: Mapping[str, Stack] | None = None,
    _seen: frozenset[str] = frozenset(),
) -> list[LabeledRule]:
    """Expand ``include:`` rules into env_group rules and ``stacks:`` into
    synthetic ``init:`` rules carrying the stack's prep-completion text.

    Inner rules inherit the parent rule's source (annotated with ``via
    group:NAME`` or ``via stack:NAME``) and its ``if:`` guard (added to
    ``extra_guards`` so multiple guards AND together).

    Stack references differ from group references in three places:

    - Unknown stack names raise ``ValueError`` (vs. the lenient
      "skip-with-warning" for unknown groups). Stacks are explicit
      task ↔ environment contracts; a typo there silently producing a
      task without the expected env is a worse failure mode than
      failing the whole run.
    - A stack expands to exactly one synthetic ``EnvRule`` carrying its
      ``init:`` text (no recursive expansion — stacks don't reference
      other stacks).
    - The expansion happens at resolve time only. The runtime does NOT
      verify the stack is installed on the backend; that's the
      operator's responsibility (``scripthut stack install``).
    """
    stacks = stacks or {}
    out: list[LabeledRule] = []
    for lr in rules:
        if lr.rule.include:
            for name in lr.rule.include:
                if name in _seen:
                    chain = " → ".join([*_seen, name])
                    raise ValueError(f"env_group cycle detected: {chain}")
                if name not in groups:
                    logger.warning(
                        "env_group %r referenced from %s but not defined; skipping",
                        name, lr.source,
                    )
                    continue
                inherited_guards = list(lr.extra_guards)
                if lr.rule.if_ is not None:
                    inherited_guards.append(lr.rule.if_)
                wrapped = [
                    LabeledRule(
                        rule=r,
                        source=f"{lr.source} via group:{name}",
                        extra_guards=list(inherited_guards),
                    )
                    for r in groups[name]
                ]
                expanded = flatten(wrapped, groups, stacks, _seen | {name})
                out.extend(expanded)
        if lr.rule.stacks:
            for name in lr.rule.stacks:
                if name not in stacks:
                    raise ValueError(
                        f"stack '{name}' referenced from {lr.source} but "
                        f"not defined in the server config or the source's "
                        f"project scripthut.yaml. Either define it or fix "
                        f"the typo — silently skipping a stack reference "
                        f"would let the task run without the env the "
                        f"author expected."
                    )
                stack_init = stacks[name].init
                if not stack_init:
                    # Defining a stack with no `init:` is legal (the prep
                    # may just produce files the task reads). Emit nothing
                    # for the env layer — there's nothing to inject.
                    continue
                inherited_guards = list(lr.extra_guards)
                if lr.rule.if_ is not None:
                    inherited_guards.append(lr.rule.if_)
                out.append(LabeledRule(
                    rule=EnvRule(init=stack_init),
                    source=f"{lr.source} via stack:{name}",
                    extra_guards=list(inherited_guards),
                ))
        # If the rule contributes anything besides include: / stacks:,
        # keep it so its set/append/init still applies.
        if lr.rule.set or lr.rule.append or lr.rule.init:
            out.append(lr)
    return out


def resolve_for_task(
    config: ScriptHutConfig,
    *,
    backend_name: str,
    workflow_name: str,
    run_id: str,
    created_at: datetime,
    task: TaskDefinition,
    git_repo: str | None = None,
    git_branch: str | None = None,
    git_sha: str | None = None,
    doc_env: list[EnvRule] | None = None,
    doc_env_groups: dict[str, list[EnvRule]] | None = None,
    doc_stacks: dict[str, Stack] | None = None,
) -> tuple[dict[str, str], str]:
    """Resolve env for a task by chaining all layers.

    ``doc_env`` / ``doc_env_groups`` come from the workflow JSON document
    itself (top-level ``env:`` and ``env_groups:`` on the generator's output)
    and slot between the workflow config layer and the task layer.
    ``doc_stacks`` carries any stacks the source repo's project YAML
    contributed (merged on top of server-config stacks with the repo
    winning on collision); referenced via ``stacks:`` on env rules.
    """
    seed = build_seed(
        backend_name=backend_name,
        workflow_name=workflow_name,
        run_id=run_id,
        created_at=created_at,
        git_repo=git_repo,
        git_branch=git_branch,
        git_sha=git_sha,
    )
    rules = collect_rules(
        config, backend_name=backend_name, workflow_name=workflow_name,
        task=task, doc_env=doc_env,
    )
    groups = collect_groups(
        config, backend_name=backend_name, workflow_name=workflow_name,
        doc_env_groups=doc_env_groups,
    )
    stacks = collect_stacks(config, doc_stacks=doc_stacks)
    rules = flatten(rules, groups, stacks)
    return resolve(rules, seed)


def resolve_for_task_detailed(
    config: ScriptHutConfig,
    *,
    backend_name: str,
    workflow_name: str,
    run_id: str,
    created_at: datetime,
    task: TaskDefinition,
    git_repo: str | None = None,
    git_branch: str | None = None,
    git_sha: str | None = None,
    doc_env: list[EnvRule] | None = None,
    doc_env_groups: dict[str, list[EnvRule]] | None = None,
    doc_stacks: dict[str, Stack] | None = None,
) -> tuple[dict[str, str], str, dict[str, Provenance]]:
    """Same as ``resolve_for_task`` but also returns per-key provenance."""
    seed = build_seed(
        backend_name=backend_name,
        workflow_name=workflow_name,
        run_id=run_id,
        created_at=created_at,
        git_repo=git_repo,
        git_branch=git_branch,
        git_sha=git_sha,
    )
    rules = collect_rules(
        config, backend_name=backend_name, workflow_name=workflow_name,
        task=task, doc_env=doc_env,
    )
    groups = collect_groups(
        config, backend_name=backend_name, workflow_name=workflow_name,
        doc_env_groups=doc_env_groups,
    )
    stacks = collect_stacks(config, doc_stacks=doc_stacks)
    rules = flatten(rules, groups, stacks)
    return resolve_detailed(rules, seed)
