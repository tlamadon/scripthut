"""Pydantic models for YAML configuration schema."""

from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class EnvRule(BaseModel):
    """A single environment-resolution rule.

    Rules are accumulated from every layer (backend, server, workflow, task)
    into a single ordered list, then evaluated top-to-bottom against a seed
    of ``SCRIPTHUT_*`` runtime vars. Conditionals see the env as resolved so
    far, so later rules can react to earlier rules.
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
    stacks: list[str] = Field(
        default_factory=list,
        description=(
            "Names of stacks whose ``init:`` text is inlined at this position, "
            "exactly as if the user had written an env rule with that init body. "
            "The runtime does NOT auto-install or auto-check the stack — "
            "operator manages that via ``scripthut stack install``. An unknown "
            "stack name raises at resolve time so a typo fails loudly rather "
            "than silently dropping the dependency."
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
    partition_map: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Map logical task partition names to this cluster's actual partition "
            "names (e.g. {standard: cpu, gpu: gpu-a100}). Tasks keep a portable "
            "logical name; each backend translates at submit time."
        ),
    )
    default_partition: str | None = Field(
        default=None,
        description=(
            "Partition to use when the task's partition isn't in partition_map. "
            "If unset, the task's literal partition value is used."
        ),
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
    env: list[EnvRule] = Field(
        default_factory=list,
        description="Backend-level env rules — cluster facts like SCRATCH and module init",
    )
    env_groups: dict[str, list[EnvRule]] = Field(
        default_factory=dict,
        description="Named, reusable rule lists. Visible to this backend's env: and to all later layers (server, workflow, task).",
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
    env: list[EnvRule] = Field(
        default_factory=list,
        description="Backend-level env rules — cluster facts like SCRATCH and module init",
    )
    env_groups: dict[str, list[EnvRule]] = Field(
        default_factory=dict,
        description="Named, reusable rule lists. Visible to this backend's env: and to all later layers (server, workflow, task).",
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
    env: list[EnvRule] = Field(
        default_factory=list,
        description="Backend-level env rules — cluster facts like SCRATCH and module init",
    )
    env_groups: dict[str, list[EnvRule]] = Field(
        default_factory=dict,
        description="Named, reusable rule lists. Visible to this backend's env: and to all later layers (server, workflow, task).",
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
    env: list[EnvRule] = Field(
        default_factory=list,
        description="Backend-level env rules — cluster facts like SCRATCH and module init",
    )
    env_groups: dict[str, list[EnvRule]] = Field(
        default_factory=dict,
        description="Named, reusable rule lists. Visible to this backend's env: and to all later layers (server, workflow, task).",
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
    env: list[EnvRule] = Field(
        default_factory=list,
        description="Backend-level env rules — cluster facts like SCRATCH and module init",
    )
    env_groups: dict[str, list[EnvRule]] = Field(
        default_factory=dict,
        description="Named, reusable rule lists. Visible to this backend's env: and to all later layers (server, workflow, task).",
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
    max_concurrent: int | None = Field(
        default=None,
        ge=1,
        description="Default max concurrent tasks per run (None = backend limit only)",
    )
    description: str = Field(
        default="",
        description="Human-readable description of this source",
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
    max_concurrent: int | None = Field(
        default=None,
        ge=1,
        description="Default max concurrent tasks per run (None = backend limit only)",
    )
    description: str = Field(
        default="",
        description="Human-readable description of this source",
    )


SourceConfig = Annotated[
    GitSourceConfig | PathSourceConfig,
    Field(discriminator="type"),
]


class AgentConfig(BaseModel):
    """Defaults for launching a Claude Code coding-agent job on a git source.

    Two interaction modes share these defaults (see ``RunManager.create_agent_run``):

    - ``remote`` — ``remote_command`` starts a ``claude remote-control`` server
      session on the compute node, driven from the claude.ai web interface.
    - ``tui`` — ``tui_command`` runs the interactive ``claude`` TUI inside a tmux
      session the existing browser terminal attaches to.

    Credentials (``GH_TOKEN``, ``ANTHROPIC_API_KEY``, optional ``CLAUDE_CONFIG_DIR``)
    are supplied as ordinary env rules referenced via ``env_group`` — there's no
    dedicated secret mechanism here.
    """

    remote_command: str = Field(
        default="claude remote-control --name {name} --capacity 1",
        description="Mode 'remote': command launching a Remote Control server session. "
        "'{name}' is substituted with the (shell-quoted) session name.",
    )
    tui_command: str = Field(
        default="claude",
        description="Mode 'tui': interactive program run inside the tmux session the "
        "browser terminal attaches to.",
    )
    cpus: int = Field(default=2, ge=1, description="CPUs requested for the agent job")
    memory: str = Field(default="8G", description="Memory requested for the agent job")
    time_limit: str = Field(
        default="12:00:00",
        description="Wall-clock limit; the session lives as long as the job does",
    )
    partition: str | None = Field(
        default=None,
        description="Partition/queue for the agent job (None = TaskDefinition default)",
    )
    env_group: str | None = Field(
        default=None,
        description="Name of an env_group to apply to the agent task (tokens/creds)",
    )
    clone_full_history: bool = Field(
        default=True,
        description="Clone the repo with full history so the agent can branch/commit/push",
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


class CacheConfig(BaseModel):
    """Task-level result cache backed by an object store (S3-compatible).

    When enabled, a task that declares ``outputs`` is content-addressed by a
    key derived from its command, resolved environment, git commit, and the
    hashes of its declared ``inputs``. If a previous run produced the same
    key, scripthut restores that run's output artifacts onto the backend
    instead of resubmitting the job — see :mod:`scripthut.runs.cache`.

    The store is shared infrastructure (an S3 bucket/prefix or an rclone
    remote), so this lives in the user-global config — never project-local.
    Hashing and artifact transfer run cluster-side over SSH, so the backend
    must have the chosen ``tool`` (``aws`` or ``rclone``) on its PATH. Only
    SSH-based backends (Slurm, PBS) support caching; API-only backends
    (Batch/EC2) silently skip it, exactly like the task-outputs feature.
    """

    enabled: bool = Field(
        default=False,
        description="Master switch for the task result cache.",
    )
    store: str | None = Field(
        default=None,
        description=(
            "Base URI of the object store for cache entries. For tool=aws an "
            "S3 URI like 's3://my-bucket/scripthut-cache'; for tool=rclone an "
            "rclone remote like 'myremote:my-bucket/scripthut-cache'. Required "
            "when enabled."
        ),
    )
    tool: Literal["aws", "rclone"] = Field(
        default="aws",
        description=(
            "CLI used on the backend to talk to the store. 'aws' uses the AWS "
            "CLI ('aws s3 cp/ls'); 'rclone' uses rclone (cat/rcat/copyto/lsf)."
        ),
    )

    @model_validator(mode="after")
    def _check_store_when_enabled(self) -> "CacheConfig":
        if self.enabled and not self.store:
            raise ValueError(
                "cache.enabled is true but cache.store is unset — set it to "
                "the object-store base URI (e.g. s3://bucket/prefix)."
            )
        return self


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


class Stack(BaseModel):
    """A reusable software environment installed once on a backend.

    A stack is the user's own bash script (``prep``) plus a content-hashed
    cache: scripthut runs ``prep`` once per (backend × hash), stores the
    result under ``<cache_dir>/<name>/<hash>/``, and exports ``init`` on
    every task that uses the stack. Inputs (``inputs`` literals + the
    contents of ``input_files``) feed the hash so any meaningful change
    forces a rebuild.
    """

    name: str = Field(description="Stack identifier referenced by tasks")
    backends: list[str] = Field(
        default_factory=list,
        description=(
            "Backend names this stack is available on. Empty means every "
            "SSH-based backend in the config."
        ),
    )
    cache_dir: str = Field(
        default="~/.cache/scripthut/stacks",
        description="Parent directory on the backend (~ expanded remotely).",
    )
    inputs: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Named literal inputs hashed into the stack identity, e.g. "
            "{python_version: '3.12'}. Any change forces a rebuild."
        ),
    )
    input_files: list[Path] = Field(
        default_factory=list,
        description=(
            "Local files whose contents are hashed into the identity "
            "(e.g. requirements.txt, Manifest.toml)."
        ),
    )
    prep: str = Field(
        default="",
        description=(
            "Bash script run once per hash. Sees ``STACK_DIR`` (the cache "
            "directory) and any env from the task that triggered install."
        ),
    )
    init: str = Field(
        default="",
        description=(
            "Bash text exported on every task that uses this stack. "
            "``${STACK_DIR}`` is the resolved cache directory."
        ),
    )
    # Resources used to run ``prep`` itself. Building a stack can require
    # real CPU/memory (compiling, conda solves, large pip resolutions);
    # running it on the login node is a portability and politeness issue.
    # On Slurm these are passed through to ``srun`` so the build lands on
    # a worker node. PBS doesn't honor these yet — see manager.install.
    cpus: int = Field(
        default=1,
        description="CPUs requested for the prep job (Slurm: --cpus-per-task)",
    )
    memory: str = Field(
        default="4G",
        description="Memory requested for the prep job (Slurm: --mem)",
    )
    time_limit: str = Field(
        default="1:00:00",
        description="Wall-clock limit for the prep job (Slurm: --time)",
    )
    partition: str | None = Field(
        default=None,
        description=(
            "Partition/queue for the prep job. None lets the scheduler "
            "pick its default (Slurm: --partition omitted)."
        ),
    )


class ScriptHutConfig(BaseModel):
    """Root configuration model for scripthut.yaml."""

    @model_validator(mode="before")
    @classmethod
    def _reject_legacy_projects(cls, data: Any) -> Any:
        """Refuse YAML carrying the legacy ``projects:`` key (removed in 0.6.0).

        Loud failure beats silent ignore: pydantic would otherwise drop the
        section quietly and leave the user wondering why their workflows
        vanished. The error message includes a concrete one-to-one mapping
        to the equivalent ``sources:`` entry so they can edit and retry.
        """
        if isinstance(data, dict) and "projects" in data:
            example = (
                "    sources:\n"
                "      - name: <project_name>\n"
                "        type: path                  # or 'git' for a remote repo\n"
                "        backend: <backend_name>\n"
                "        path: <path_on_backend>\n"
                "        # optional: workflows_glob, max_concurrent, description"
            )
            raise ValueError(
                "`projects:` was removed in scripthut 0.6.0. Convert each "
                "project entry to a `sources:` entry, e.g.\n"
                f"{example}\n"
                "See scripthut.example.yaml or `scripthut source list` "
                "(against a server) for working examples."
            )
        return data

    backends: list[BackendConfig] = Field(
        default_factory=list,
        description="List of remote backends (Slurm, ECS)",
    )
    sources: list[SourceConfig] = Field(
        default_factory=list,
        description="List of sources (git repos or backend paths) with workflow definitions",
    )
    env: list[EnvRule] = Field(
        default_factory=list,
        description="Server-level env rules applied to every task on every backend",
    )
    env_groups: dict[str, list[EnvRule]] = Field(
        default_factory=dict,
        description="Named, reusable rule lists. Visible to server env: and to all workflows / tasks.",
    )
    stacks: list[Stack] = Field(
        default_factory=list,
        description="Reusable software stacks (Python venv, Julia depot, etc.) installed once per backend",
    )
    agent: AgentConfig = Field(
        default_factory=AgentConfig,
        description="Defaults for launching Claude coding-agent jobs on git sources",
    )
    pricing: PricingConfig | None = Field(
        default=None,
        description="Optional EC2-equivalent cost estimation using instances.vantage.sh",
    )
    settings: GlobalSettings = Field(
        default_factory=GlobalSettings,
        description="Global application settings",
    )
    cache: CacheConfig = Field(
        default_factory=CacheConfig,
        description="Task-level result cache backed by an object store (S3/rclone)",
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

    def get_stack(self, name: str) -> Stack | None:
        """Get a stack by name."""
        for stack in self.stacks:
            if stack.name == name:
                return stack
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
