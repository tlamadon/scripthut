"""Resolve ordered backend environment rules and named groups.

Conditions see variables set by earlier rules. Keys in a condition are ANDed;
list values are ORed. Substitution uses the resolved environment.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, field

from scripthut.config_schema import EnvRule


logger = logging.getLogger(__name__)

_PROTECTED_PREFIX = "SCRIPTHUT_"
_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass
class LabeledRule:
    """A rule paired with its source label and inherited conditions.

    ``extra_guards`` carries if-guards inherited from outer ``include:`` rules.
    Every guard plus the rule's own ``if_`` must match for the rule to apply.
    """

    rule: EnvRule
    source: str  # e.g. "backend:cluster", "scriptflow"
    extra_guards: list[dict[str, str | list[str]]] = field(default_factory=list)



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
    """Apply rules against the seed and return environment and shell initialization."""
    env: dict[str, str] = {}
    for k, v in seed.items():
        env[k] = v

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

        if rule.init:
            init_parts.append(_expand(rule.init, env))

    return env, "\n".join(init_parts)


def flatten(
    rules: list[LabeledRule],
    groups: Mapping[str, list[EnvRule]],
    _seen: frozenset[str] = frozenset(),
) -> list[LabeledRule]:
    """Expand named environment groups, preserving inherited conditions."""
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
                expanded = flatten(wrapped, groups, _seen | {name})
                out.extend(expanded)
        if lr.rule.set or lr.rule.append or lr.rule.init:
            out.append(lr)
    return out
