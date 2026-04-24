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


class AWSBatchConfig(BaseModel):
    """AWS configuration for Batch backends."""

    profile: str | None = Field(
        default=None,
        description="AWS CLI profile name (uses default credential chain if not set)",
    )
    region: str = Field(description="AWS region")
    job_queue: str = Field(
        description="Default AWS Batch job queue name (overridable per-task via partition)"
    )


class AWSEC2Config(BaseModel):
    """AWS configuration for EC2 backends."""

    profile: str | None = Field(
        default=None,
        description="AWS CLI profile name (uses default credential chain if not set)",
    )
    region: str = Field(description="AWS region")


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
    max_concurrent: int = Field(
        default=100,
        ge=1,
        description="Maximum total concurrent jobs across all runs on this backend",
    )
    clone_dir: str = Field(
        default="~/scripthut-repos",
        description="Path on the backend whose disk usage is reported in the backend status panel (typically the parent directory where source repos are cloned)",
    )


class PBSBackendConfig(BaseModel):
    """PBS/Torque backend configuration."""

    name: str = Field(description="Unique identifier for this backend")
    type: Literal["pbs"] = "pbs"
    ssh: SSHConfig = Field(description="SSH connection settings")
    account: str | None = Field(
        default=None,
        description="PBS account to charge jobs to (-A flag)",
    )
    login_shell: bool = Field(
        default=False,
        description="Use login shell (#!/bin/bash -l) in PBS scripts to source profile",
    )
    max_concurrent: int = Field(
        default=100,
        ge=1,
        description="Maximum total concurrent jobs across all runs on this backend",
    )
    queue: str | None = Field(
        default=None,
        description="Default PBS queue to submit jobs to (overrides task partition)",
    )
    clone_dir: str = Field(
        default="~/scripthut-repos",
        description="Path on the backend whose disk usage is reported in the backend status panel (typically the parent directory where source repos are cloned)",
    )


class ECSBackendConfig(BaseModel):
    """ECS backend configuration."""

    name: str = Field(description="Unique identifier for this backend")
    type: Literal["ecs"] = "ecs"
    aws: AWSConfig = Field(description="AWS configuration")
    max_concurrent: int = Field(
        default=100,
        ge=1,
        description="Maximum total concurrent jobs across all runs on this backend",
    )


class BatchBackendConfig(BaseModel):
    """AWS Batch backend configuration."""

    name: str = Field(description="Unique identifier for this backend")
    type: Literal["batch"] = "batch"
    aws: AWSBatchConfig = Field(description="AWS configuration")
    job_definition: str | None = Field(
        default=None,
        description=(
            "Pre-registered AWS Batch job definition to submit against. "
            "Accepts a bare name (e.g. 'simpleJobDef'), a name:revision, or a "
            "full ARN. Behavior depends on ``job_definition_mode``: 'locked' "
            "(default) uses the definition as-is and ignores per-task images; "
            "'revisions' uses it as a template and registers a new revision "
            "when a task requests a different image."
        ),
    )
    job_definition_mode: Literal["locked", "revisions"] = Field(
        default="locked",
        description=(
            "How to handle ``job_definition`` when tasks request different "
            "container images. 'locked' (default): always submit against the "
            "configured definition; task-level ``image`` is ignored with a "
            "warning on mismatch. 'revisions': use the configured definition "
            "as a template — on first submit with a new image, clone its "
            "container properties (image swapped, roles and other settings "
            "preserved) and register a new revision via RegisterJobDefinition. "
            "'revisions' mode requires batch:RegisterJobDefinition and is "
            "subject to iam:PassRole if the template's role ARNs are "
            "cross-account."
        ),
    )
    default_image: str | None = Field(
        default=None,
        description=(
            "Container image URI used when scripthut auto-registers a job "
            "definition (i.e. when ``job_definition`` is unset). Required in "
            "that case; ignored when ``job_definition`` is set."
        ),
    )
    job_role_arn: str | None = Field(
        default=None,
        description=(
            "IAM role ARN the container assumes at runtime (jobRoleArn). "
            "Only used when scripthut auto-registers a job definition."
        ),
    )
    execution_role_arn: str | None = Field(
        default=None,
        description=(
            "IAM role used by ECS to pull images and write logs (executionRoleArn). "
            "Only used when scripthut auto-registers a job definition."
        ),
    )
    retry_attempts: int = Field(
        default=1,
        ge=1,
        le=10,
        description="Batch retryStrategy.attempts (1 = no retry; max 10).",
    )
    log_group: str = Field(
        default="/aws/batch/job",
        description="CloudWatch Logs group where Batch writes container logs",
    )
    max_concurrent: int = Field(
        default=100,
        ge=1,
        description="Maximum total concurrent jobs across all runs on this backend",
    )
    clone_dir: str = Field(
        default="",
        description="Unused for Batch (no shared filesystem). Kept for UI compatibility.",
    )


class EC2BackendConfig(BaseModel):
    """AWS EC2-direct backend configuration.

    Launches one EC2 instance per task, runs the task's container inside via
    user-data, and connects over SSH tunnelled through SSM Session Manager
    (no inbound port 22, no key-pair management — authentication uses a
    one-shot SSH key pushed via EC2 Instance Connect for each connection).
    """

    name: str = Field(description="Unique identifier for this backend")
    type: Literal["ec2"] = "ec2"
    aws: AWSEC2Config = Field(description="AWS configuration")

    # Compute
    ami: str = Field(description="AMI ID used for every task instance (must have sshd + SSM Agent)")
    subnet_id: str = Field(description="VPC subnet ID where task instances are launched")
    security_group_ids: list[str] = Field(
        default_factory=list,
        description="Optional security group IDs to attach (inbound port 22 NOT required — scripthut tunnels via SSM)",
    )
    instance_types: dict[str, str] = Field(
        default_factory=lambda: {"default": "c5.xlarge"},
        description=(
            "Maps ``task.partition`` (or 'default') to an EC2 instance type. "
            "Tasks whose partition isn't in this map fall back to the 'default' entry."
        ),
    )
    instance_profile_arn: str | None = Field(
        default=None,
        description=(
            "Optional IAM instance profile ARN attached to each task instance. "
            "Must include AmazonSSMManagedInstanceCore at minimum; add ECR/S3 "
            "permissions if your container pulls from ECR or reads/writes S3."
        ),
    )

    # Execution
    default_image: str | None = Field(
        default=None,
        description=(
            "Default container image URI. Tasks can override via ``image``. "
            "Required unless every task specifies its own image."
        ),
    )
    ssh_user: str = Field(
        default="ec2-user",
        description=(
            "OS user for SSH connections via SSM tunnel. 'ec2-user' for Amazon "
            "Linux, 'ubuntu' for Ubuntu AMIs."
        ),
    )

    # Safety
    max_instances: int = Field(
        default=20,
        ge=1,
        description="Hard cap on concurrently-running task instances (refuses further submits)",
    )
    max_concurrent: int = Field(
        default=100,
        ge=1,
        description="Maximum total concurrent tasks across all runs on this backend (per-run caps apply too)",
    )
    idle_terminate_seconds: int = Field(
        default=1800,
        ge=60,
        description=(
            "Instance self-terminates (via user-data safety timer) if it sits "
            "idle without a task-completion sentinel for this many seconds. "
            "Belt-and-braces against scripthut crashes."
        ),
    )
    startup_timeout_seconds: int = Field(
        default=600,
        ge=60,
        description="Scripthut marks a task FAILED if its instance hasn't reached 'running' within this many seconds",
    )

    # Tagging — scripthut uses these to find its own instances on reconcile
    tag_prefix: str = Field(
        default="scripthut",
        description="Tag key prefix scripthut uses to identify its own instances",
    )
    extra_tags: dict[str, str] = Field(
        default_factory=dict,
        description="Extra tags applied to every launched instance (useful for cost allocation)",
    )

    # For UI compatibility only (disk_usage panel expects this field)
    clone_dir: str = Field(
        default="",
        description="Unused for EC2 (no shared filesystem). Kept for UI compatibility.",
    )


BackendConfig = Annotated[
    SlurmBackendConfig | PBSBackendConfig | ECSBackendConfig | BatchBackendConfig | EC2BackendConfig,
    Field(discriminator="type"),
]


class GitSourceConfig(BaseModel):
    """Git repository source configuration."""

    name: str = Field(description="Unique identifier for this source")
    type: Literal["git"] = "git"
    url: str = Field(description="Git repository URL (SSH format recommended)")
    branch: str = Field(default="main", description="Branch to track")
    deploy_key: Path | None = Field(
        default=None,
        description="Path to deploy key for this repository",
    )
    backend: str | None = Field(
        default=None,
        description="Deprecated: backend is now selected at run time",
        exclude=True,
    )
    workflows_glob: str = Field(
        default=".hut/workflows/*.json",
        description="Glob pattern to find workflow JSON files within the repo (e.g. '**/*.hut.json')",
    )
    clone_dir: str = Field(
        default="~/scripthut-repos",
        description="Parent directory on the backend where repos are cloned into (clone goes into <clone_dir>/<commit_hash>/)",
    )
    postclone: str | None = Field(
        default=None,
        description="Shell command to run in the clone directory after cloning",
    )

    @property
    def deploy_key_resolved(self) -> Path | None:
        """Return the resolved deploy key path with ~ expansion."""
        return self.deploy_key.expanduser() if self.deploy_key else None


class PathSourceConfig(BaseModel):
    """Path-based source on a backend filesystem."""

    name: str = Field(description="Unique identifier for this source")
    type: Literal["path"] = "path"
    path: str = Field(description="Path to directory on the backend filesystem")
    backend: str = Field(
        description="Name of the backend where this path exists and where tasks are submitted",
    )
    workflows_glob: str = Field(
        default=".hut/workflows/*.json",
        description="Glob pattern to find workflow JSON files within the path (e.g. '**/*.hut.json')",
    )


SourceConfig = Annotated[
    GitSourceConfig | PathSourceConfig,
    Field(discriminator="type"),
]


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
    postclone: str | None = Field(
        default=None,
        description="Shell command to run in the clone directory after cloning (e.g. to remove large files)",
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
    max_concurrent: int | None = Field(
        default=None,
        ge=1,
        description="Max concurrent tasks per run (None = backend limit only)",
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
    max_concurrent: int | None = Field(
        default=None,
        ge=1,
        description="Default max concurrent tasks per run (None = backend limit only)",
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


class PricingConfig(BaseModel):
    """EC2-equivalent cost estimation configuration."""

    region: str = Field(
        default="us-east-1",
        description="AWS region for pricing lookup",
    )
    price_type: str = Field(
        default="ondemand",
        description="Pricing type: ondemand, spot_avg, spot_min, spot_max",
    )
    partitions: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of Slurm partition names to EC2 instance types",
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
    data_dir: Path = Field(
        default=Path("~/.cache/scripthut"),
        description="Base directory for all stored data (workflows, logs, sources)",
    )
    sources_cache_dir: Path | None = Field(
        default=None,
        description="Directory to cache cloned repositories (default: <data_dir>/sources)",
    )
    filter_user: str | None = Field(
        default=None,
        description="Default username to filter jobs by (None for all users)",
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

    backends: list[BackendConfig] = Field(
        default_factory=list,
        description="List of remote backends (Slurm, ECS)",
    )
    sources: list[SourceConfig] = Field(
        default_factory=list,
        description="List of sources (git repos or backend paths) with workflow definitions",
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
    pricing: PricingConfig | None = Field(
        default=None,
        description="Optional EC2-equivalent cost estimation using instances.vantage.sh",
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

    def get_source(self, name: str) -> GitSourceConfig | PathSourceConfig | None:
        """Get a source by name."""
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
    def pbs_backends(self) -> list[PBSBackendConfig]:
        """Get all PBS backends."""
        return [c for c in self.backends if isinstance(c, PBSBackendConfig)]

    @property
    def ecs_backends(self) -> list[ECSBackendConfig]:
        """Get all ECS backends."""
        return [c for c in self.backends if isinstance(c, ECSBackendConfig)]

    @property
    def batch_backends(self) -> list[BatchBackendConfig]:
        """Get all AWS Batch backends."""
        return [c for c in self.backends if isinstance(c, BatchBackendConfig)]

    @property
    def ec2_backends(self) -> list[EC2BackendConfig]:
        """Get all AWS EC2-direct backends."""
        return [c for c in self.backends if isinstance(c, EC2BackendConfig)]
