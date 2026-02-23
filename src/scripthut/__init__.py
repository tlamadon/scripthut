"""ScriptHut - Remote job management system."""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("scripthut")
except PackageNotFoundError:
    __version__ = "dev"
