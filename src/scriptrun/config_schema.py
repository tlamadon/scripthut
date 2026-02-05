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
    """AWS configuration for ECS clusters."""

    profile: str | None = Field(
        default=None,
        description="AWS CLI profile name (uses default credential chain if not set)",
    )
    region: str = Field(description="AWS region")
    cluster_name: str = Field(description="ECS cluster name")


class SlurmClusterConfig(BaseModel):
    """Slurm cluster configuration."""

    name: str = Field(description="Unique identifier for this cluster")
    type: Literal["slurm"] = "slurm"
    ssh: SSHConfig = Field(description="SSH connection settings")


class ECSClusterConfig(BaseModel):
    """ECS cluster configuration."""

    name: str = Field(description="Unique identifier for this cluster")
    type: Literal["ecs"] = "ecs"
    aws: AWSConfig = Field(description="AWS configuration")


ClusterConfig = Annotated[
    SlurmClusterConfig | ECSClusterConfig,
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


class TaskSourceConfig(BaseModel):
    """Task source configuration - SSH command that returns JSON task list."""

    name: str = Field(description="Unique identifier for this task source")
    cluster: str = Field(description="Name of the cluster to submit tasks to")
    command: str = Field(description="SSH command that returns JSON task list")
    max_concurrent: int = Field(
        default=5,
        ge=1,
        description="Maximum number of tasks to run concurrently",
    )
    description: str = Field(
        default="",
        description="Human-readable description of this task source",
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
        default=Path("~/.cache/scriptrun/sources"),
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


class ScriptRunConfig(BaseModel):
    """Root configuration model for scriptrun.yaml."""

    clusters: list[ClusterConfig] = Field(
        default_factory=list,
        description="List of remote clusters (Slurm, ECS)",
    )
    sources: list[GitSourceConfig] = Field(
        default_factory=list,
        description="List of git repository sources for job definitions",
    )
    task_sources: list[TaskSourceConfig] = Field(
        default_factory=list,
        description="List of task sources (SSH commands returning JSON task lists)",
    )
    settings: GlobalSettings = Field(
        default_factory=GlobalSettings,
        description="Global application settings",
    )

    def get_cluster(self, name: str) -> ClusterConfig | None:
        """Get a cluster by name."""
        for cluster in self.clusters:
            if cluster.name == name:
                return cluster
        return None

    def get_source(self, name: str) -> GitSourceConfig | None:
        """Get a git source by name."""
        for source in self.sources:
            if source.name == name:
                return source
        return None

    def get_task_source(self, name: str) -> TaskSourceConfig | None:
        """Get a task source by name."""
        for source in self.task_sources:
            if source.name == name:
                return source
        return None

    @property
    def slurm_clusters(self) -> list[SlurmClusterConfig]:
        """Get all Slurm clusters."""
        return [c for c in self.clusters if isinstance(c, SlurmClusterConfig)]

    @property
    def ecs_clusters(self) -> list[ECSClusterConfig]:
        """Get all ECS clusters."""
        return [c for c in self.clusters if isinstance(c, ECSClusterConfig)]
