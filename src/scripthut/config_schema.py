"""Pydantic models for YAML configuration schema."""

from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field


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


class AWSConfig(BaseModel):
    """AWS configuration for ECS backends."""

    profile: str | None = Field(
        default=None,
        description="AWS CLI profile name (uses default credential chain if not set)",
    )
    region: str = Field(description="AWS region")
    cluster_name: str = Field(description="ECS cluster name")


class SlurmBackendConfig(BaseModel):
    """Slurm backend configuration."""

    name: str = Field(description="Unique identifier for this backend")
    type: Literal["slurm"] = "slurm"
    ssh: SSHConfig = Field(description="SSH connection settings")
    account: str | None = Field(
        default=None,
        description="Slurm account to charge jobs to (e.g., phd, pi-faculty)",
    )
    login_shell: bool = Field(
        default=False,
        description="Use login shell (#!/bin/bash -l) in sbatch scripts to source profile",
    )


class ECSBackendConfig(BaseModel):
    """ECS backend configuration."""

    name: str = Field(description="Unique identifier for this backend")
    type: Literal["ecs"] = "ecs"
    aws: AWSConfig = Field(description="AWS configuration")


BackendConfig = Annotated[
    SlurmBackendConfig | ECSBackendConfig,
    Field(discriminator="type"),
]


class GitSourceConfig(BaseModel):
    """Git repository source configuration."""

    name: str = Field(description="Unique identifier for this source")
    url: str = Field(description="Git repository URL (SSH format recommended)")
    branch: str = Field(default="main", description="Branch to track")
    deploy_key: Path | None = Field(
        default=None,
        description="Path to deploy key for this repository",
    )

    @property
    def deploy_key_resolved(self) -> Path | None:
        """Return the resolved deploy key path with ~ expansion."""
        return self.deploy_key.expanduser() if self.deploy_key else None


class WorkflowGitConfig(BaseModel):
    """Git repository to clone on the backend before running the workflow command."""

    repo: str = Field(description="Git repository URL (SSH format recommended)")
    branch: str = Field(default="main", description="Branch to clone")
    deploy_key: Path | None = Field(
        default=None,
        description="Path to deploy key on the local machine (will be uploaded to backend temporarily)",
    )
    clone_dir: str = Field(
        default="~/scripthut-repos",
        description="Parent directory on the backend where repos are cloned into (clone goes into <clone_dir>/<commit_hash>/)",
    )

    @property
    def deploy_key_resolved(self) -> Path | None:
        """Return the resolved deploy key path with ~ expansion."""
        return self.deploy_key.expanduser() if self.deploy_key else None


class WorkflowConfig(BaseModel):
    """Workflow configuration - SSH command that returns JSON task list."""

    name: str = Field(description="Unique identifier for this workflow")
    backend: str = Field(description="Name of the backend to submit tasks to")
    command: str = Field(description="SSH command that returns JSON task list")
    max_concurrent: int = Field(
        default=5,
        ge=1,
        description="Maximum number of tasks to run concurrently",
    )
    description: str = Field(
        default="",
        description="Human-readable description of this workflow",
    )
    git: WorkflowGitConfig | None = Field(
        default=None,
        description="Optional: clone a git repo on the backend before running the workflow command",
    )


class ProjectConfig(BaseModel):
    """A git repository on a backend containing sflow.json workflow files."""

    name: str = Field(description="Unique identifier for this project")
    backend: str = Field(description="Name of the backend this project lives on")
    path: str = Field(description="Path to the git repo on the backend")
    max_concurrent: int = Field(
        default=5,
        ge=1,
        description="Default max concurrent tasks for workflows in this project",
    )
    description: str = Field(
        default="",
        description="Human-readable description of this project",
    )


class EnvironmentConfig(BaseModel):
    """Named environment with key-value variables and optional init script."""

    name: str = Field(description="Unique identifier for this environment")
    variables: dict[str, str] = Field(
        default_factory=dict,
        description="Key-value pairs exported as environment variables",
    )
    extra_init: str = Field(
        default="",
        description="Raw bash lines to run before the task command (e.g. module load)",
    )


class GlobalSettings(BaseModel):
    """Global application settings."""

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
    sources_cache_dir: Path = Field(
        default=Path("~/.cache/scripthut/sources"),
        description="Directory to cache cloned repositories",
    )
    filter_user: str | None = Field(
        default=None,
        description="Default username to filter jobs by (None for all users)",
    )

    @property
    def sources_cache_dir_resolved(self) -> Path:
        """Return the resolved cache directory with ~ expansion."""
        return self.sources_cache_dir.expanduser()


class ScriptHutConfig(BaseModel):
    """Root configuration model for scripthut.yaml."""

    backends: list[BackendConfig] = Field(
        default_factory=list,
        description="List of remote backends (Slurm, ECS)",
    )
    sources: list[GitSourceConfig] = Field(
        default_factory=list,
        description="List of git repository sources for job definitions",
    )
    workflows: list[WorkflowConfig] = Field(
        default_factory=list,
        description="List of workflows (SSH commands returning JSON task lists)",
    )
    projects: list[ProjectConfig] = Field(
        default_factory=list,
        description="List of git projects containing sflow.json workflow files",
    )
    environments: list[EnvironmentConfig] = Field(
        default_factory=list,
        description="Named environments with key-value variables for tasks",
    )
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

    def get_source(self, name: str) -> GitSourceConfig | None:
        """Get a git source by name."""
        for source in self.sources:
            if source.name == name:
                return source
        return None

    def get_workflow(self, name: str) -> WorkflowConfig | None:
        """Get a workflow by name."""
        for workflow in self.workflows:
            if workflow.name == name:
                return workflow
        return None

    def get_project(self, name: str) -> ProjectConfig | None:
        """Get a project by name."""
        for project in self.projects:
            if project.name == name:
                return project
        return None

    def get_environment(self, name: str) -> EnvironmentConfig | None:
        """Get an environment by name."""
        for env in self.environments:
            if env.name == name:
                return env
        return None

    @property
    def slurm_backends(self) -> list[SlurmBackendConfig]:
        """Get all Slurm backends."""
        return [c for c in self.backends if isinstance(c, SlurmBackendConfig)]

    @property
    def ecs_backends(self) -> list[ECSBackendConfig]:
        """Get all ECS backends."""
        return [c for c in self.backends if isinstance(c, ECSBackendConfig)]
