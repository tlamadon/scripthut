"""Only ScriptFlow controller operations may mutate scheduler state."""

import ast
import re
from pathlib import Path

from scripthut import cli, main


def test_scheduler_calls_are_confined():
    root = Path(main.__file__).parent
    allowed = {"scheduler.py", "scriptflow_launch.py"}
    violations = []
    for path in root.rglob("*.py"):
        if path.name in allowed:
            continue
        tree = ast.parse(path.read_text())
        docstrings = {
            id(node.body[0].value)
            for node in ast.walk(tree)
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            and node.body and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in {"submit_job", "submit_task", "cancel_job", "terminate_job", "process_run"}:
                    violations.append((str(path.relative_to(root)), node.lineno))
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings:
                if re.search(r"(?:^|[\s;|])(?:sbatch|qsub|scancel|qdel|srun|salloc)(?:\s|$)", node.value):
                    violations.append((str(path.relative_to(root)), node.lineno))
    assert violations == []


def test_removed_modules_and_commands_are_absent():
    root = Path(main.__file__).parent
    for name in ("runs", "disk", "stacks", "setup"):
        assert not list((root / name).rglob("*.py"))
    for name in ("legacy", "notifications", "pricing", "backends/batch", "backends/ec2", "backends/ec2_ssm", "backends/local"):
        assert not (root / (name + ".py")).exists()
    import argparse
    commands = next(a.choices for a in cli.build_parser()._actions if isinstance(a, argparse._SubParsersAction))
    assert set(commands) == {"scriptflow", "source", "backend", "agent", "status", "daemon"}
    paths = [getattr(route, "path", "") for route in main.app.routes]
    assert not any(path.startswith(("/api/v1/runs", "/api/v1/tasks", "/api/v1/stacks", "/terminal/interactive")) for path in paths)


def test_public_help_lists_retained_commands():
    import subprocess
    import sys
    result = subprocess.run([sys.executable, "-m", "scripthut", "--help"], capture_output=True, text=True)
    assert result.returncode == 0
    assert "Commands: agent, backend, daemon, scriptflow, source, status" in result.stdout
    assert "setup-aws" not in result.stdout and "workflow" not in result.stdout
