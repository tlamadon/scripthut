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

# Top-level fields that may only appear in the user-global config because
# they describe the user's infrastructure (backends, secrets, settings),
# not the project. A project-local config that uses any of these is
# rejected with a clear error.
PROJECT_FORBIDDEN_FIELDS = {"backends", "sources", "settings", "pricing"}


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


def discover_project_config(start_dir: Path | None = None) -> Path | None:
    """Walk up from ``start_dir`` looking for a project-local ``scripthut.yaml``.

    Returns the first ``scripthut.yaml`` / ``scripthut.yml`` found at the
    starting directory or any of its ancestors. Returns ``None`` if the
    only match is the user-global config (so we don't double-count it as
    project-local when CWD happens to be under ``~/.config``).
    """
    cwd = (start_dir or Path.cwd()).resolve()
    global_resolved = {p.resolve() for p in GLOBAL_CONFIG_PATHS if p.exists()}
    for candidate_dir in [cwd, *cwd.parents]:
        for name in ("scripthut.yaml", "scripthut.yml"):
            f = candidate_dir / name
            if f.exists():
                if f.resolve() in global_resolved:
                    return None
                return f
    return None


def _friendly_validation_error(raw_config: dict, exc: ValidationError) -> str:
    """Turn a Pydantic ValidationError into a human-readable message."""
    # Map top-level list fields to their allowed type values
    DISCRIMINATED_FIELDS: dict[str, dict[str, str | list[str]]] = {
        "backends": {"discriminator": "type", "allowed": ["slurm", "pbs", "ecs", "batch", "ec2"]},
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

    _resolve_stack_input_files(cfg, config_path.resolve().parent)
    return cfg


def _resolve_stack_input_files(cfg: ScriptHutConfig, base: Path) -> None:
    """Rewrite each stack's ``input_files`` to be absolute paths.

    Relative paths in a config file are resolved against the *directory
    of that config file*, not the process CWD. This is the only thing
    that makes ``input_files: [requirements.txt]`` in a project-local
    config behave intuitively when the CLI is invoked from anywhere.
    Absolute paths and ``~``-prefixed paths are kept as-is.
    """
    for stack in cfg.stacks:
        resolved: list[Path] = []
        for p in stack.input_files:
            expanded = Path(str(p)).expanduser()
            if expanded.is_absolute():
                resolved.append(expanded)
            else:
                resolved.append((base / expanded).resolve())
        stack.input_files = resolved


def _validate_project_local_yaml(raw: dict, path: Path) -> None:
    """Reject project-local configs that try to define user-infra fields."""
    bad = sorted(set(raw.keys()) & PROJECT_FORBIDDEN_FIELDS)
    if not bad:
        return
    raise ConfigError(
        f"Project-local config '{path}' contains fields that belong in the "
        f"user-global config (~/.config/scripthut/scripthut.yaml): "
        f"{', '.join(bad)}.\n\n"
        f"Move those sections to the global file and keep only "
        f"stacks / workflows / projects / env / env_groups in the "
        f"project file."
    )


def _merge_configs(
    global_cfg: ScriptHutConfig, project_cfg: ScriptHutConfig
) -> ScriptHutConfig:
    """Overlay project-local entries on top of global.

    By-name overrides for: ``stacks``, ``workflows``, ``projects``. The
    ``env_groups`` mapping is dict-merged (project keys win). The ``env``
    list is concatenated (global first, then project) so project rules
    run *after* global rules and can react to the env they set up.
    Infrastructure fields (``backends``, ``sources``, ``settings``,
    ``pricing``) come strictly from the global config.
    """
    def by_name(items):
        return {i.name: i for i in items}

    merged_stacks = list(
        {**by_name(global_cfg.stacks), **by_name(project_cfg.stacks)}.values()
    )
    merged_projects = list(
        {**by_name(global_cfg.projects), **by_name(project_cfg.projects)}.values()
    )
    merged_env_groups = {**global_cfg.env_groups, **project_cfg.env_groups}
    merged_env = list(global_cfg.env) + list(project_cfg.env)

    return global_cfg.model_copy(update={
        "stacks": merged_stacks,
        "projects": merged_projects,
        "env_groups": merged_env_groups,
        "env": merged_env,
    })


def load_layered_config(cwd: Path | None = None) -> ScriptHutConfig | None:
    """Load and merge user-global + project-local configs.

    Returns ``None`` when neither a global nor a project-local YAML
    is discovered, letting the caller decide on a legacy fallback.

    Discovery rules:

    - The user-global config is at ``~/.config/scripthut/scripthut.yaml``
      (or the legacy ``~/.scripthut.yaml``).
    - The project-local config is the first ``scripthut.yaml`` found by
      walking up from ``cwd``. Skipped if it resolves to the same file
      as the global one.
    - If both exist they're merged (project overrides by name). The
      project-local file is rejected if it tries to define backends,
      sources, settings, or pricing.
    - If only one exists, that file is used directly (matches the
      legacy single-file setup).
    """
    global_path = discover_global_config()
    project_path = discover_project_config(cwd)

    if global_path is None and project_path is None:
        return None

    if global_path is None:
        # Only a project-local file — treat it as the entire config.
        assert project_path is not None
        return load_yaml_config(project_path)

    global_cfg = load_yaml_config(global_path)
    if project_path is None:
        return global_cfg

    # Validate raw YAML before parsing the layered form so the error
    # message names the offending field directly from the user's source.
    with open(project_path) as f:
        raw = yaml.safe_load(f) or {}
    _validate_project_local_yaml(raw, project_path)

    project_cfg = load_yaml_config(project_path)
    merged = _merge_configs(global_cfg, project_cfg)
    logger.info(
        f"Loaded layered config: global={global_path}, project={project_path}"
    )
    return merged


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

    # Convert to new config format
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

    With an explicit ``config_path`` only that file is loaded (used by
    ``--config`` and tests). Otherwise scripthut layers a user-global
    config and a project-local one:

    1. User-global at ``~/.config/scripthut/scripthut.yaml`` carries
       backends, settings, pricing, and remote sources.
    2. Project-local ``scripthut.yaml`` found by walking up from CWD
       carries the project's own stacks, workflows, and env rules.
       It may **not** define backends/sources/settings/pricing.
    3. When both exist, they're merged (project overrides by name).
    4. When neither exists, falls back to the legacy ``.env`` loader.
    """
    if config_path is not None:
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        return load_yaml_config(config_path)

    layered = load_layered_config()
    if layered is not None:
        return layered

    # No YAML found anywhere — fall back to the legacy .env loader.
    try:
        return load_legacy_config()
    except ValidationError as e:
        raise FileNotFoundError(
            "No configuration found. Create scripthut.yaml in the current "
            "project, or ~/.config/scripthut/scripthut.yaml. "
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
