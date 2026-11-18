"""Pydantic models for YAML configuration schema."""

import re
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, Field, model_validator


class EnvRule(BaseModel):
    """A single environment-resolution rule.

    Backend rules and the selected controller group are evaluated in order.
    Conditions see variables resolved by earlier rules.
    """

    model_config = ConfigDict(populate_by_name=True)

    if_: dict[str, str | list[str]] | None = Field(
        default=None,
        alias="if",
        description=(
            "Optional guard. AND across keys; list value means OR. The rule "
            "is skipped when the guard does not match the env-so-far."
        ),
    )
    include: list[str] = Field(
        default_factory=list,
        description=(
            "Names of env_groups to inline at this position. The included rules "
            "are evaluated in order, and inherit this rule's if-guard if any."
        ),
    )
    set: dict[str, str] = Field(
        default_factory=dict,
        description="Variables to set (overwrites prior values). ${name} is expanded against env-so-far.",
    )
    append: dict[str, str] = Field(
        default_factory=dict,
        description='Variables to append to (joined with ":"). ${name} is expanded against env-so-far.',
    )
    init: str = Field(
        default="",
        description="Bash text concatenated into extra_init. ${name} is expanded against env-so-far.",
    )


class SSHConfig(BaseModel):
    """SSH connection configuration."""

    host: str = Field(description="Hostname of the remote machine")
    port: int = Field(default=22, description="SSH port")
    user: str = Field(description="SSH username")
    key_path: Path = Field(
        default=Path("~/.ssh/id_rsa"),
        description="Path to SSH private key",
    )
    cert_path: Path | None = Field(
        default=None,
        description="Path to SSH certificate (for certificate-based auth)",
    )
    known_hosts: Path | None = Field(
        default=None,
        description="Path to known_hosts file (None to disable host key checking)",
    )

    @property
    def key_path_resolved(self) -> Path:
        """Return the resolved SSH key path with ~ expansion."""
        return self.key_path.expanduser()

    @property
    def cert_path_resolved(self) -> Path | None:
        """Return the resolved certificate path with ~ expansion."""
        return self.cert_path.expanduser() if self.cert_path else None

    @property
    def known_hosts_resolved(self) -> Path | None:
        """Return the resolved known_hosts path with ~ expansion."""
        return self.known_hosts.expanduser() if self.known_hosts else None


def image_reference(value: str, allow_tag: bool = False) -> str:
    value = value.strip()
    if "://" in value.removeprefix("docker://"):
        raise ValueError("Image reference must use a registry reference or docker:// URI")
    if not re.fullmatch(r"(?:docker://)?[A-Za-z0-9][A-Za-z0-9._:/@+-]*", value):
        raise ValueError("Image reference must be a registry reference without credentials or whitespace")
    if value.count("@") > 1:
        raise ValueError("Image reference must not contain registry credentials")
    if "@" in value and not re.search(r"@sha256:[a-fA-F0-9]{64}$", value):
        raise ValueError("Image reference requires a valid sha256 digest")
    if not allow_tag and not re.search(r"@sha256:[a-fA-F0-9]{64}$", value):
        raise ValueError("Image reference must be pinned by digest; set allow_tag: true to opt out")
    return value


class SourceImage(BaseModel):
    name: str = Field(pattern=r"^[A-Za-z0-9_]+$")
    ref: str | None = None
    ref_file: str | None = None
    allow_tag: bool = False

    @model_validator(mode="after")
    def _validate_image(self) -> "SourceImage":
        if (self.ref is None) == (self.ref_file is None):
            raise ValueError("Image requires exactly one of ref and ref_file")
        if self.ref is not None:
            self.ref = image_reference(self.ref, self.allow_tag)
        if self.ref_file is not None and (
            not self.ref_file or PurePosixPath(self.ref_file).is_absolute()
            or ".." in PurePosixPath(self.ref_file).parts
            or any(c in self.ref_file for c in "\0\r\n")
        ):
            raise ValueError("Image ref_file must be a relative repository path without '..'")
        if self.ref_file is not None:
            self.ref_file = str(PurePosixPath(self.ref_file))
        return self


class ImageBackendConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    image_dir: str | None = None
    image_scratch_dir: str | None = None
    image_pull_timeout_seconds: int = Field(default=600, ge=1)
    registry_user: str | None = None
    registry_token_file: Path | None = None

    @model_validator(mode="before")
    @classmethod
    def _registry_keys(cls, value: Any) -> Any:
        if isinstance(value, dict):
            unknown = sorted(
                k for k in value if isinstance(k, str) and k.startswith("registry_")
                and k not in {"registry_user", "registry_token_file"}
            )
            if unknown:
                raise ValueError(
                    "Unknown registry key(s): " + ", ".join(unknown)
                    + "; expected registry_user and registry_token_file"
                )
        return value

    @model_validator(mode="after")
    def _validate_images(self) -> "ImageBackendConfig":
        if self.image_dir is not None and (
            not self.image_dir.startswith("/") or ".." in PurePosixPath(self.image_dir).parts
            or any(c in self.image_dir for c in "\0\r\n")
        ):
            raise ValueError("image_dir must be an absolute backend path")
        if self.image_scratch_dir is not None and (
            not self.image_scratch_dir.startswith("/")
            or ".." in PurePosixPath(self.image_scratch_dir).parts
            or any(c in self.image_scratch_dir for c in "\0\r\n")
        ):
            raise ValueError("image_scratch_dir must be an absolute node-local path")
        if bool(self.registry_user) != (self.registry_token_file is not None):
            raise ValueError("registry_user and registry_token_file must be configured together")
        if self.registry_user and any(c in self.registry_user for c in "\0\r\n"):
            raise ValueError("registry_user must be a single line")
        return self


class SlurmBackendConfig(ImageBackendConfig):
    """Slurm backend configuration."""

    name: str = Field(description="Unique identifier for this backend")
    type: Literal["slurm"] = "slurm"
    ssh: SSHConfig = Field(description="SSH connection settings")
    account: str | None = Field(
        default=None,
        description="Slurm account to charge jobs to (e.g., phd, pi-faculty)",
    )
    partition_map: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Map controller partition names to this cluster's actual partition "
            "names (e.g. {standard: cpu, gpu: gpu-a100}). Controllers use a "
            "logical name; each backend translates at submit time."
        ),
    )
    default_partition: str | None = Field(
        default=None,
        description=(
            "Partition to use when the controller's partition isn't in partition_map. "
            "If unset, the controller's literal partition value is used."
        ),
    )
    login_shell: bool = Field(
        default=False,
        description="Use login shell (#!/bin/bash -l) in controller scripts to source profile",
    )
    clone_dir: str = Field(
        default="~/scripthut-repos",
        description="Parent directory for backend source checkouts",
    )
    env: list[EnvRule] = Field(
        default_factory=list,
        description="Backend-level env rules — cluster facts like SCRATCH and module init",
    )
    env_groups: dict[str, list[EnvRule]] = Field(
        default_factory=dict,
        description="Named, reusable rule lists. Used by this backend's controller environment.",
    )


class PBSBackendConfig(BaseModel):
    """PBS/Torque backend configuration."""

    model_config = ConfigDict(extra="forbid")

    image_dir: str | None = None

    name: str = Field(description="Unique identifier for this backend")
    type: Literal["pbs"] = "pbs"
    ssh: SSHConfig = Field(description="SSH connection settings")
    queue: str | None = Field(
        default=None,
        description="Default queue for the cluster availability display",
    )
    clone_dir: str = Field(
        default="~/scripthut-repos",
        description="Parent directory for backend source checkouts",
    )

BackendConfig = Annotated[
    SlurmBackendConfig | PBSBackendConfig,
    Field(discriminator="type"),
]


class GitSourceConfig(BaseModel):
    """Git repository source configuration.

    Two ways to say where the code lives, and they compose:

    - ``url`` — a git remote. The server keeps a cache clone under
      ``settings.sources_cache_dir`` for repository inspection, and the
      backend clones the remote itself (needing ``deploy_key`` for a
      private repo).
    - ``local_path`` — a git repo on the machine running scripthut. The
      server reads that working tree directly (no cache clone), and the
      commit is pushed to the backend over the SSH connection scripthut
      already holds. No deploy key, no network, no git remote at all.

    With ``local_path`` set it wins for both reading and materialising;
    ``url`` then stays as metadata for repository identification.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(description="Unique identifier for this source")
    type: Literal["git"] = "git"
    url: str = Field(
        default="",
        description=(
            "Git repository URL (SSH format recommended). Optional when "
            "'local_path' is set"
        ),
    )
    local_path: Path | None = Field(
        default=None,
        description=(
            "Path to a git repo on the scripthut host. When set, files "
            "are read from this working tree and the resolved commit is "
            "pushed to the backend instead of cloned from 'url'"
        ),
    )
    branch: str = Field(default="main", description="Branch to track")
    deploy_key: Path | None = Field(
        default=None,
        description="Path to deploy key for this repository",
    )
    clone_dir: str = Field(
        default="~/scripthut-repos",
        description="Parent directory for fixed source checkouts and commit snapshots",
    )
    images: list[SourceImage] = Field(default_factory=list)
    workdir: str | None = None
    flow_dir: str = "."

    postclone: str | None = Field(
        default=None,
        description="Shell command to run in the clone directory after cloning",
    )
    description: str = Field(
        default="",
        description="Human-readable description of this source",
    )

    @property
    def working_directory(self) -> str:
        return self.workdir or f"{self.clone_dir}/.worktrees/source-{quote(self.name, safe='')}"

    @model_validator(mode="after")
    def _check_directories(self) -> "GitSourceConfig":
        names = [image.name.upper() for image in self.images]
        if len(names) != len(set(names)):
            raise ValueError("Image names must be unique after upper-casing")
        if self.workdir is not None and (
            not self.workdir.startswith("/")
            or ".." in PurePosixPath(self.workdir).parts
            or any(c in self.workdir for c in "\0\n\r")
        ):
            raise ValueError("workdir must be an absolute backend path without '..'")
        if (
            PurePosixPath(self.flow_dir).is_absolute()
            or ".." in PurePosixPath(self.flow_dir).parts
            or any(c in self.flow_dir for c in "\0\n\r")
        ):
            raise ValueError("flow_dir must be relative and stay inside the repository (no '..')")
        self.flow_dir = str(PurePosixPath(self.flow_dir))
        return self

    @property
    def deploy_key_resolved(self) -> Path | None:
        """Return the resolved deploy key path with ~ expansion."""
        return self.deploy_key.expanduser() if self.deploy_key else None

    @property
    def local_path_resolved(self) -> Path | None:
        """Return the resolved local repo path with ~ expansion."""
        return self.local_path.expanduser() if self.local_path else None

    @model_validator(mode="after")
    def _check_url_or_local_path(self) -> "GitSourceConfig":
        if not self.url and self.local_path is None:
            raise ValueError(
                f"git source '{self.name}' needs either 'url' (a git remote) "
                "or 'local_path' (a git repo on this machine); it has neither."
            )
        return self


class PathSourceConfig(BaseModel):
    """Display-only metadata for a directory on a backend filesystem."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(description="Unique identifier for this source")
    type: Literal["path"] = "path"
    path: str = Field(description="Path to directory on the backend filesystem")
    backend: str = Field(
        description="Name of the backend where this path exists",
    )
    description: str = Field(
        default="",
        description="Human-readable description of this source",
    )


SourceConfig = Annotated[
    GitSourceConfig | PathSourceConfig,
    Field(discriminator="type"),
]


class ScriptFlowConfig(BaseModel):
    """Compute-job defaults for standalone ScriptFlow controllers."""

    env_group: str | None = Field(default=None, min_length=1)
    cpus: int = Field(default=1, ge=1)
    memory: str = Field(default="2G", pattern=r"^[1-9][0-9]*[KMGTP]?$")
    time_limit: str = Field(default="01:00:00", pattern=r"^[0-9]+(?:-[0-9]+)?(?::[0-9]+){0,2}$")
    partition: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_.-]+$")


class CliAuthConfig(BaseModel):
    """Auth credentials the CLI sends when talking to ``cli_server``.

    Currently scoped to Cloudflare Access — populate either the service-token
    pair (``cf_client_id`` + ``cf_client_secret``, best for unattended use)
    or ``cf_access_token`` (a JWT, e.g. from ``cloudflared access token``).
    ``cloudflared_app`` lets the CLI fetch a fresh JWT on demand instead.
    """

    cf_client_id: str | None = Field(
        default=None,
        description="Cloudflare Access service-token client ID (sent as CF-Access-Client-Id).",
    )
    cf_client_secret: str | None = Field(
        default=None,
        description="Cloudflare Access service-token client secret (sent as CF-Access-Client-Secret).",
    )
    cf_access_token: str | None = Field(
        default=None,
        description="Cloudflare Access user JWT (sent as cf-access-token header).",
    )
    cloudflared_app: str | None = Field(
        default=None,
        description=(
            "If set, run `cloudflared access token --app=<value>` to obtain a "
            "JWT at CLI invocation time. Used only when no token is supplied "
            "explicitly via flag / env / cf_access_token."
        ),
    )


class GlobalSettings(BaseModel):
    """Global application settings."""

    fetch_max_file_bytes: int = Field(default=1024**3, ge=1)
    fetch_max_total_bytes: int = Field(default=10 * 1024**3, ge=1)

    poll_interval: int = Field(
        default=60,
        ge=5,
        description="Interval in seconds between job status polls",
    )
    server_host: str = Field(
        default="127.0.0.1",
        description="Host to bind the server to",
    )
    server_port: int = Field(
        default=8000,
        description="Port to bind the server to",
    )
    data_dir: Path = Field(
        default=Path("~/.cache/scripthut"),
        description="Base directory for ScriptFlow records, observations and source clones",
    )
    sources_cache_dir: Path | None = Field(
        default=None,
        description="Directory to cache cloned repositories (default: <data_dir>/sources)",
    )
    filter_user: str | None = Field(
        default=None,
        description="Default username to filter jobs by (None for all users)",
    )
    cli_server: str | None = Field(
        default=None,
        description=(
            "Default URL of a running scripthut server for the CLI. "
            "When set, CLI commands hit this server instead of running locally. "
            "Overridden by --server and the SCRIPTHUT_SERVER env var."
        ),
    )
    cli_auth: CliAuthConfig | None = Field(
        default=None,
        description=(
            "Default Cloudflare Access credentials the CLI sends to cli_server. "
            "Overridden by per-command flags and SCRIPTHUT_CF_* env vars."
        ),
    )
    cli_autostart: Literal["ask", "always", "never"] = Field(
        default="ask",
        description=(
            "What the CLI does when no server is configured and no local "
            "daemon is running: 'ask' prompts on a TTY (fails in "
            "non-interactive sessions), 'always' starts a daemon without "
            "asking, 'never' neither prompts nor starts one (use "
            "`--server local` or `scripthut daemon start` instead)."
        ),
    )

    @property
    def data_dir_resolved(self) -> Path:
        """Return the resolved data directory with ~ expansion."""
        return self.data_dir.expanduser()

    @property
    def sources_cache_dir_resolved(self) -> Path:
        """Return the resolved cache directory with ~ expansion."""
        if self.sources_cache_dir is not None:
            return self.sources_cache_dir.expanduser()
        return self.data_dir_resolved / "sources"


class ScriptHutConfig(BaseModel):
    """Root configuration model for scripthut.yaml."""


    @model_validator(mode="before")
    @classmethod
    def _reject_obsolete(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        obsolete = []
        for key in ("cache", "stacks", "agent", "pricing", "projects", "workflows", "notifications", "disk", "env", "env_groups"):
            if key in data:
                obsolete.append(key)
        settings = data.get("settings", {})
        if isinstance(settings, dict) and "legacy_runs_enabled" in settings:
            obsolete.append("settings.legacy_runs_enabled")
        for index, backend in enumerate(data.get("backends", []) or []):
            if not isinstance(backend, dict):
                continue
            prefix = f"backends[{index}]"
            if backend.get("type") not in ("slurm", "pbs"):
                obsolete.append(prefix + ".type=" + str(backend.get("type")) + " (unsupported backend type)")
            if "max_concurrent" in backend:
                obsolete.append(prefix + ".max_concurrent")
        for index, source in enumerate(data.get("sources", []) or []):
            if isinstance(source, dict):
                for key in ("workflows_glob", "max_concurrent", "workflows"):
                    if key in source:
                        obsolete.append(f"sources[{index}].{key}")
                if source.get("type") == "git" and "backend" in source:
                    obsolete.append(f"sources[{index}].backend")
        def env_keys(container: Any, prefix: str) -> None:
            if not isinstance(container, dict):
                return
            def rules(value: Any, path: str) -> None:
                if isinstance(value, list):
                    for index, rule in enumerate(value):
                        if isinstance(rule, dict) and "stacks" in rule:
                            obsolete.append(f"{path}[{index}].stacks")
            rules(container.get("env"), prefix + "env")
            groups = container.get("env_groups", {})
            if isinstance(groups, dict):
                for name, value in groups.items():
                    rules(value, prefix + f"env_groups.{name}")
        env_keys(data, "")
        for index, backend in enumerate(data.get("backends", []) or []):
            env_keys(backend, f"backends[{index}].")
        if obsolete:
            raise ValueError("Obsolete configuration keys: " + ", ".join(sorted(set(obsolete))) + ". Use backend controller environments and ScriptFlow task definitions.")
        return data

    backends: list[BackendConfig] = Field(
        default_factory=list,
        description="List of remote backends (Slurm, PBS)",
    )
    sources: list[SourceConfig] = Field(
        default_factory=list,
        description=(
            "List of sources (git remotes, local git repos, or backend "
            "paths)"
        ),
    )
    scriptflow: ScriptFlowConfig = Field(default_factory=ScriptFlowConfig)
    settings: GlobalSettings = Field(
        default_factory=GlobalSettings,
        description="Global application settings",
    )

    def get_backend(self, name: str) -> BackendConfig | None:
        """Get a backend by name."""
        for backend in self.backends:
            if backend.name == name:
                return backend
        return None

    def get_source(self, name: str) -> GitSourceConfig | PathSourceConfig | None:
        """Get a source by name."""
        for source in self.sources:
            if source.name == name:
                return source
        return None
