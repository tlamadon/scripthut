"""ScriptHut - Remote job management system."""

import tomllib
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path


def _read_version() -> str:
    """Resolve the package version.

    Prefer ``importlib.metadata`` so installed wheels (pip, pipx, uv) get
    the real version from their dist-info. Fall back to reading
    ``pyproject.toml`` for working out of a source checkout where the
    package may not be formally installed. ``"dev"`` is the last resort.
    """
    try:
        return version("scripthut")
    except PackageNotFoundError:
        pass
    pyproject = Path(__file__).resolve().parent.parent.parent / "pyproject.toml"
    try:
        data = tomllib.loads(pyproject.read_text())
        return data["project"]["version"]
    except Exception:
        return "dev"


__version__ = _read_version()
