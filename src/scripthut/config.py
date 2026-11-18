"""Configuration management - supports YAML config and .env fallback."""

import logging
import re
import warnings
from pathlib import Path

import yaml
from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from scripthut.config_schema import (
    GlobalSettings,
    ScriptHutConfig,
    SlurmBackendConfig,
    SSHConfig,
)

logger = logging.getLogger(__name__)

# Default config file locations (in priority order)
DEFAULT_CONFIG_PATHS = [
    Path("./scripthut.yaml"),
    Path("./scripthut.yml"),
]

# User-global config locations searched in order; first hit wins.
GLOBAL_CONFIG_PATHS = [
    Path.home() / ".config" / "scripthut" / "scripthut.yaml",
    Path.home() / ".scripthut.yaml",
]


class LegacySettings(BaseSettings):
    """Legacy settings loaded from environment variables or .env file.

    Used for backwards compatibility when no YAML config is found.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # SSH Configuration
    ssh_host: str = Field(description="Hostname of the remote Slurm machine")
    ssh_port: int = Field(default=22, description="SSH port")
    ssh_user: str = Field(description="SSH username")
    ssh_key_path: Path = Field(
        default=Path.home() / ".ssh" / "id_rsa",
        description="Path to SSH private key",
    )
    ssh_known_hosts: Path | None = Field(
        default=None,
        description="Path to known_hosts file (None to disable host key checking)",
    )

    # Polling Configuration
    poll_interval: int = Field(
        default=60,
        ge=5,
        description="Interval in seconds between job status polls",
    )

    # Server Configuration
    server_host: str = Field(default="127.0.0.1", description="Host to bind the server to")
    server_port: int = Field(default=8000, description="Port to bind the server to")


def find_config_file(config_path: Path | None = None) -> Path | None:
    """Find the configuration file.

    Args:
        config_path: Explicit path to config file. If provided, must exist.

    Returns:
        Path to config file, or None if not found.

    Raises:
        FileNotFoundError: If explicit config_path is provided but doesn't exist.
    """
    if config_path is not None:
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        return config_path

    # Search default locations
    for path in DEFAULT_CONFIG_PATHS:
        if path.exists():
            return path

    return None


def discover_global_config() -> Path | None:
    """Find the user-global ``scripthut.yaml`` if it exists."""
    for p in GLOBAL_CONFIG_PATHS:
        if p.exists():
            return p
    return None


def _friendly_validation_error(raw_config: dict, exc: ValidationError) -> str:
    """Turn a Pydantic ValidationError into a human-readable message."""
    # Map top-level list fields to their allowed type values
    DISCRIMINATED_FIELDS: dict[str, dict[str, str | list[str]]] = {
        "backends": {"discriminator": "type", "allowed": ["slurm", "pbs"]},
        "sources": {"discriminator": "type", "allowed": ["git", "path"]},
    }

    parts: list[str] = []
    for error in exc.errors():
        loc = error.get("loc", ())
        err_type = error.get("type", "")

        # Discriminator errors: e.g. loc = ('sources', 0) or ('backends', 1)
        if (
            err_type == "union_tag_not_found"
            and len(loc) >= 2
            and isinstance(loc[0], str)
            and isinstance(loc[1], int)
        ):
            section = loc[0]
            idx = loc[1]
            info = DISCRIMINATED_FIELDS.get(section, {})
            disc = info.get("discriminator", "type")
            allowed = info.get("allowed", [])

            # Try to get the item name from raw config
            items = raw_config.get(section, [])
            item_name = None
            if isinstance(items, list) and idx < len(items) and isinstance(items[idx], dict):
                item_name = items[idx].get("name", None)

            label = f"'{item_name}'" if item_name else f"entry {idx + 1}"
            allowed_str = ", ".join(f"'{v}'" for v in allowed)
            parts.append(
                f"{section}[{idx}] ({label}): missing required field '{disc}'.\n"
                f"  Each item in '{section}' must have a '{disc}' field set to one of: {allowed_str}"
            )
            continue

        # Generic fallback: use Pydantic's message with a cleaner location
        loc_str = " → ".join(str(l) for l in loc) if loc else "(root)"
        msg = error.get("msg", "Unknown error")
        parts.append(f"{loc_str}: {msg}")

    return "\n\n".join(parts)


def load_yaml_config(config_path: Path) -> ScriptHutConfig:
    """Load and validate YAML configuration.

    Args:
        config_path: Path to the YAML config file.

    Returns:
        Validated ScriptHutConfig object.

    Raises:
        ConfigError: If the YAML doesn't match the schema (with friendly message).
        yaml.YAMLError: If the YAML is malformed.
    """
    logger.info(f"Loading configuration from {config_path}")

    with open(config_path) as f:
        raw_config = yaml.safe_load(f)

    if raw_config is None:
        raw_config = {}

    try:
        cfg = ScriptHutConfig.model_validate(raw_config)
    except ValidationError as exc:
        friendly = _friendly_validation_error(raw_config, exc)
        raise ConfigError(friendly) from exc

    return cfg


class ConfigError(Exception):
    """User-friendly configuration error."""


def load_legacy_config() -> ScriptHutConfig:
    """Load configuration from .env file (legacy mode).

    Emits a deprecation warning and converts to ScriptHutConfig format.

    Returns:
        ScriptHutConfig object created from .env settings.
    """
    warnings.warn(
        "Using .env configuration is deprecated. "
        "Please migrate to scripthut.yaml format.",
        DeprecationWarning,
        stacklevel=2,
    )

    legacy = LegacySettings()

    # Map environment settings into the YAML schema.
    ssh_config = SSHConfig(
        host=legacy.ssh_host,
        port=legacy.ssh_port,
        user=legacy.ssh_user,
        key_path=legacy.ssh_key_path,
        known_hosts=legacy.ssh_known_hosts,
    )

    slurm_backend = SlurmBackendConfig(
        name="default",
        type="slurm",
        ssh=ssh_config,
    )

    settings = GlobalSettings(
        poll_interval=legacy.poll_interval,
        server_host=legacy.server_host,
        server_port=legacy.server_port,
    )

    return ScriptHutConfig(
        backends=[slurm_backend],
        sources=[],
        settings=settings,
    )


def load_config(config_path: Path | None = None) -> ScriptHutConfig:
    """Load application configuration.

    An explicit path selects one file. Otherwise the user-global YAML takes
    priority over a YAML file in the current directory. With no YAML file,
    load SSH and server settings from the environment or .env.
    """
    if config_path is not None:
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        return load_yaml_config(config_path)

    path = discover_global_config() or find_config_file()
    if path is not None:
        return load_yaml_config(path)

    # No YAML found anywhere — fall back to the legacy .env loader.
    try:
        return load_legacy_config()
    except ValidationError as e:
        raise FileNotFoundError(
            "No configuration found. Create scripthut.yaml in the current "
            "project, or ~/.config/scripthut/scripthut.yaml. "
            "See scripthut.example.yaml for the recommended format."
        ) from e


def set_global_setting(key: str, value: str) -> Path:
    """Set ``settings.<key>: <value>`` in the user-global config file.

    Edits the YAML with line-level text surgery rather than a parse/dump
    round-trip so the user's comments and formatting survive. Creates
    ``~/.config/scripthut/scripthut.yaml`` when no global config exists.
    The edited text is validated as a :class:`ScriptHutConfig` before being
    written; if the edit would produce an invalid config nothing is written
    and a ``RuntimeError`` asks the user to edit the file manually.
    """
    path = discover_global_config()
    if path is None:
        path = GLOBAL_CONFIG_PATHS[0]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"settings:\n  {key}: {value}\n")
        return path

    original = path.read_text()
    key_re = re.compile(rf"^(\s+){re.escape(key)}:.*$", re.MULTILINE)
    settings_re = re.compile(r"^settings:\s*(#.*)?$", re.MULTILINE)

    if key_re.search(original):
        updated = key_re.sub(lambda m: f"{m.group(1)}{key}: {value}", original, count=1)
    elif (section := settings_re.search(original)) is not None:
        insert_at = section.end()
        updated = original[:insert_at] + f"\n  {key}: {value}" + original[insert_at:]
    else:
        sep = "" if (not original or original.endswith("\n")) else "\n"
        updated = original + f"{sep}settings:\n  {key}: {value}\n"

    try:
        raw = yaml.safe_load(updated) or {}
        ScriptHutConfig.model_validate(raw)
    except Exception as e:
        raise RuntimeError(
            f"Could not update {path} automatically; add '{key}: {value}' "
            f"under the 'settings:' section manually."
        ) from e

    path.write_text(updated)
    return path
