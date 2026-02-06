"""Configuration management - supports YAML config and .env fallback."""

import logging
import warnings
from pathlib import Path

import yaml
from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from scripthut.config_schema import (
    GlobalSettings,
    ScriptHutConfig,
    SlurmClusterConfig,
    SSHConfig,
)

logger = logging.getLogger(__name__)

# Default config file locations (in priority order)
DEFAULT_CONFIG_PATHS = [
    Path("./scripthut.yaml"),
    Path("./scripthut.yml"),
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


def load_yaml_config(config_path: Path) -> ScriptHutConfig:
    """Load and validate YAML configuration.

    Args:
        config_path: Path to the YAML config file.

    Returns:
        Validated ScriptHutConfig object.

    Raises:
        ValidationError: If the YAML doesn't match the schema.
        yaml.YAMLError: If the YAML is malformed.
    """
    logger.info(f"Loading configuration from {config_path}")

    with open(config_path) as f:
        raw_config = yaml.safe_load(f)

    if raw_config is None:
        raw_config = {}

    return ScriptHutConfig.model_validate(raw_config)


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

    # Convert to new config format
    ssh_config = SSHConfig(
        host=legacy.ssh_host,
        port=legacy.ssh_port,
        user=legacy.ssh_user,
        key_path=legacy.ssh_key_path,
        known_hosts=legacy.ssh_known_hosts,
    )

    slurm_cluster = SlurmClusterConfig(
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
        clusters=[slurm_cluster],
        sources=[],
        settings=settings,
    )


def load_config(config_path: Path | None = None) -> ScriptHutConfig:
    """Load application configuration.

    Priority:
    1. Explicit config_path argument
    2. ./scripthut.yaml or ./scripthut.yml
    3. .env file (legacy, deprecated)

    Args:
        config_path: Optional explicit path to config file.

    Returns:
        ScriptHutConfig object.

    Raises:
        FileNotFoundError: If explicit config_path doesn't exist.
        ValidationError: If config is invalid.
    """
    yaml_path = find_config_file(config_path)

    if yaml_path is not None:
        return load_yaml_config(yaml_path)

    # Try legacy .env config
    try:
        return load_legacy_config()
    except ValidationError as e:
        if config_path is not None:
            # User explicitly requested a config file
            raise
        # No config found at all
        raise FileNotFoundError(
            "No configuration found. Create scripthut.yaml or .env file. "
            "See scripthut.example.yaml for the recommended format."
        ) from e


# Global config instance (set by main.py)
_config: ScriptHutConfig | None = None


def get_config() -> ScriptHutConfig:
    """Get the loaded configuration.

    Raises:
        RuntimeError: If config hasn't been loaded yet.
    """
    if _config is None:
        raise RuntimeError("Configuration not loaded. Call load_config() first.")
    return _config


def set_config(config: ScriptHutConfig) -> None:
    """Set the global configuration instance."""
    global _config
    _config = config
