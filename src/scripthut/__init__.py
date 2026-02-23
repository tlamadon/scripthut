"""ScriptHut - Remote job management system."""

import tomllib
from pathlib import Path


def _read_version() -> str:
    """Read version from pyproject.toml (single source of truth)."""
    pyproject = Path(__file__).resolve().parent.parent.parent / "pyproject.toml"
    try:
        data = tomllib.loads(pyproject.read_text())
        return data["project"]["version"]
    except Exception:
        return "dev"


__version__ = _read_version()
