"""ScriptFlow imports remain independent of legacy execution modules."""

import subprocess
import sys

import pytest


@pytest.mark.parametrize("module", ["scriptflow", "scriptflow_cli", "scriptflow_launch", "scriptflow_server", "scriptflow_routes"])
def test_import_boundary(module):
    script = f"""
import importlib, sys
importlib.import_module('scripthut.{module}')
forbidden = {{'scripthut.runs.manager', 'scripthut.runs.models', 'scripthut.runs.storage',
             'scripthut.runs.cache', 'scripthut.api', 'scripthut.cli'}}
assert not forbidden.intersection(sys.modules), sorted(forbidden.intersection(sys.modules))
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
