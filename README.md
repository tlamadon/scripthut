# ScriptHut

[![PyPI version](https://img.shields.io/pypi/v/scripthut)](https://pypi.org/project/scripthut/)
[![Tests](https://github.com/tlamadon/scripthut/actions/workflows/tests.yml/badge.svg)](https://github.com/tlamadon/scripthut/actions/workflows/tests.yml)
[![Docker](https://ghcr-badge.egpl.dev/tlamadon/scripthut/latest_tag?trim=major&label=docker)](https://github.com/tlamadon/scripthut/pkgs/container/scripthut)
[![Docs](https://img.shields.io/badge/docs-tlamadon.github.io%2Fscripthut-blue)](https://tlamadon.github.io/scripthut/)

A Python web interface to start and track jobs on remote HPC systems (Slurm, PBS/Torque) over SSH, on **AWS Batch** via the AWS API, and on **AWS EC2** directly (one instance per task, SSH tunnelled via SSM).

## Features

- **Multi-backend support** - Monitor Slurm, PBS/Torque, AWS Batch, and AWS EC2 queues from a single dashboard
- **Real-time job monitoring** - View running and pending jobs with auto-refresh via SSE
- **Task runs** - Submit batches of jobs with configurable concurrency limits and dependencies
- **Unified job view** - See run-submitted and external jobs in one dashboard
- **Git workflow integration** - Clone repos on the backend before running task generators
- **Persistent SSH connections** - Maintains connections with keepalive and auto-reconnect
- **HTMX frontend** - Dynamic updates without full page reloads
- **Cost estimation** - Estimate run costs using EC2 spot/on-demand pricing from [instances.vantage.sh](https://instances.vantage.sh/)
- **Extensible** - Abstract backend system ready for additional schedulers

## Examples

See [scripthut-examples](https://github.com/thomaswiemann/scripthut-examples) for complete, self-contained workflow examples in R, Python, Julia, and Apptainer.

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/scripthut.git
cd scripthut

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install the package
pip install -e .

# For AWS Batch support (installs boto3)
pip install -e ".[batch]"

# For development (includes mypy, ruff, pytest)
pip install -e ".[dev]"
```

### Docker

```bash
docker run -d -p 8000:8000 \
  -v ./scripthut.yaml:/app/scripthut.yaml \
  -v ~/.ssh:/root/.ssh:ro \
  ghcr.io/tlamadon/scripthut:main
```

Then open http://localhost:8000.

## Configuration

ScriptHut uses a YAML configuration file. Copy the example and customize:

```bash
cp scripthut.example.yaml scripthut.yaml
```

### YAML Configuration

```yaml
# scripthut.yaml

backends:
  # Slurm cluster
  - name: hpc-cluster
    type: slurm
    ssh:
      host: slurm-login.cluster.edu
      port: 22
      user: researcher
      key_path: ~/.ssh/id_rsa
    account: my-allocation       # optional: --account flag
    login_shell: false           # optional: use #!/bin/bash -l
    max_concurrent: 100          # optional: max jobs across all runs

  # PBS/Torque cluster
  - name: pbs-cluster
    type: pbs
    ssh:
      host: pbs-login.cluster.edu
      user: researcher
      key_path: ~/.ssh/id_rsa
    account: my-allocation       # optional: -A flag
    queue: batch                 # optional: default queue (overrides task partition)
    login_shell: false
    max_concurrent: 100

# Sources: git repos or backend paths with workflow JSON files (matched via workflows_glob)
sources:
  - name: ml-jobs
    type: git
    url: git@github.com:org/ml-pipelines.git
    branch: main
    deploy_key: ~/.ssh/ml-jobs-deploy-key
    backend: hpc-cluster

settings:
  data_dir: ~/.cache/scripthut          # base for all stored data
  poll_interval: 60
  server_host: 127.0.0.1
  server_port: 8000
```

### Configuration Options

#### Backends

**Common fields (Slurm and PBS):**

| Field | Description |
|-------|-------------|
| `name` | Unique identifier for the backend |
| `type` | Backend type: `slurm`, `pbs`, or `ecs` |
| `ssh.host` | SSH hostname |
| `ssh.port` | SSH port (default: 22) |
| `ssh.user` | SSH username |
| `ssh.key_path` | Path to SSH private key |
| `ssh.cert_path` | Path to SSH certificate (optional) |
| `ssh.known_hosts` | Path to known_hosts file (optional) |
| `account` | Account to charge jobs to (Slurm `--account`, PBS `-A`) |
| `login_shell` | Use `#!/bin/bash -l` in submission scripts (default: false) |
| `max_concurrent` | Max concurrent jobs across all runs (default: 100) |
| `clone_dir` | Path on the backend whose disk usage is shown in the UI (default: `~/scripthut-repos`) |

**PBS-specific:**

| Field | Description |
|-------|-------------|
| `queue` | Default PBS queue (overrides task `partition` field) |

**AWS Batch backend** (`type: batch`):

AWS Batch is an API-based backend — there is no SSH host. Install the `[batch]` extra for boto3 support. AWS credentials come from the standard AWS credential chain (see [AWS Credentials](#aws-credentials) below).

```yaml
backends:
  - name: aws-batch
    type: batch
    aws:
      profile: my-profile            # optional, omit to use the default chain
      region: us-east-1
      job_queue: my-batch-queue      # default queue; task.partition overrides
    # --- Option 1: use an existing job definition (recommended) ---
    job_definition: simpleJobDef-b760b37
    # --- Option 2: let scripthut auto-register job definitions per image ---
    # default_image: ghcr.io/org/workflow:latest
    # job_role_arn: arn:aws:iam::123456789012:role/BatchJobRole
    # execution_role_arn: arn:aws:iam::123456789012:role/BatchExec
    retry_attempts: 1                # Batch retryStrategy.attempts (1–10)
    log_group: /aws/batch/job        # CloudWatch log group (default)
    max_concurrent: 50
```

| Field | Required | Description |
|-------|----------|-------------|
| `aws.profile` | No | AWS CLI profile name (from `~/.aws/credentials`). Omit to use the default credential chain. |
| `aws.region` | Yes | AWS region (e.g. `us-east-1`). |
| `aws.job_queue` | Yes | Default AWS Batch job queue. Tasks whose `partition` is unset use this queue. |
| `job_definition` | No* | Pre-registered Batch job definition to submit against. Accepts a bare name (`simpleJobDef`), `name:revision`, or full ARN. Works in two modes (see `job_definition_mode`). *Either `job_definition` or `default_image` must be set. |
| `job_definition_mode` | No | `locked` (default) — always submits against the configured definition, ignoring per-task `image` with a warning on mismatch. `revisions` — treats the configured definition as a **template**: on first submit with a new image, scripthut copies its container properties (image swapped, roles / log config / retry / tags preserved), registers a new revision of the same `jobDefinitionName`, and caches it per image. Requires `batch:RegisterJobDefinition`. |
| `default_image` | No* | Container image URI used when scripthut auto-registers a job definition. Ignored when `job_definition` is set. |
| `job_role_arn` | No | IAM role ARN the container assumes at runtime (`jobRoleArn`). Only used for auto-registration. |
| `execution_role_arn` | No | IAM role used by ECS to pull the image and push logs (`executionRoleArn`). Only used for auto-registration. |
| `retry_attempts` | No | `retryStrategy.attempts` in Batch terms. Default `1` (no retry). Allowed range 1–10. |
| `log_group` | No | CloudWatch Logs group where Batch writes container logs. Default: `/aws/batch/job`. |

**How ScriptHut maps task fields to AWS Batch:**

| scripthut field | AWS Batch concept |
|-----------------|-------------------|
| `task.partition` | Job queue (falls back to `aws.job_queue`) |
| `task.cpus` / `task.memory` | `containerOverrides.resourceRequirements` (VCPU / MEMORY in MiB) |
| `task.time_limit` | `timeout.attemptDurationSeconds` (min 60s; omitted if below) |
| `task.gres="gpu:N"` | GPU resource requirement |
| `task.image` (or `default_image`) | Container image URI |
| Generated script | `containerOverrides.command = ["bash", "-c", <script>]` |

**Three modes for job definitions:**

1. **Pre-registered, locked** (`job_definition: <name>` — default `job_definition_mode: locked`): scripthut submits against the existing definition verbatim and skips `RegisterJobDefinition`. Sidesteps the cross-account `iam:PassRole` restriction that `RegisterJobDefinition` enforces. **The image is locked** to whatever the definition was registered with — task-level `image` values are ignored with a warning on mismatch.

2. **Pre-registered, revisions** (`job_definition: <name>` + `job_definition_mode: revisions`): scripthut treats the configured definition as a template. On first submit with a new image, it describes the template, clones its container properties (image swapped, roles / log config / retry / tags preserved), and calls `RegisterJobDefinition` with the same `jobDefinitionName` — AWS auto-increments the revision. Subsequent submits with the same image hit an in-process cache. Good for workflows where different tasks use different images but share roles/networking. Requires `batch:RegisterJobDefinition`; PassRole is still subject to the template's role ARN accounts.

3. **Auto-register** (`default_image: <image>` and optional role ARNs, no `job_definition`): on first submission for a given image signature, scripthut registers a job definition named `scripthut-<hash>` and reuses it. Per-task `command`, `env`, `vcpus`, `memory`, and `timeout` are all applied via `containerOverrides` at submit time.

**Environment variables** are passed via `containerOverrides.environment` so the container's entrypoint sees them at process start, not only after the bash script exports them. Variable names are listed as a comment in the generated script for visibility, but values travel through the AWS API.

**AWS EC2 backend** (`type: ec2`):

Launches one dedicated EC2 instance per task, `docker run`s the container inside via user-data, and reconciles completion over SSH tunnelled through AWS SSM Session Manager (no inbound port 22 on your instances, no EC2 key pair management).

```yaml
backends:
  - name: aws-ec2
    type: ec2
    aws:
      profile: scripthut                  # optional
      region: us-east-2
    ami: ami-0abc123...                   # must have sshd + SSM Agent + docker
    subnet_id: subnet-0abc...
    security_group_ids: [sg-0abc...]      # inbound port 22 NOT required
    instance_types:
      default: c5.xlarge                  # maps task.partition → instance type
      gpu: g4dn.xlarge
    default_image: ghcr.io/org/image:latest
    instance_profile_arn: arn:aws:iam::123:instance-profile/ScriptHutTask
    ssh_user: ec2-user                    # "ubuntu" for Ubuntu AMIs
    max_instances: 20                     # hard cap — refuses submits above this
    idle_terminate_seconds: 1800          # safety-timer slack beyond task.time_limit
    startup_timeout_seconds: 600
    tag_prefix: scripthut                 # used to reconcile instances on restart
    extra_tags:
      Environment: research
```

| Field | Required | Description |
|-------|----------|-------------|
| `aws.region` | Yes | AWS region. |
| `aws.profile` | No | AWS CLI profile (omit to use default chain). |
| `ami` | Yes | AMI used for every task instance. Must include sshd, SSM Agent, and docker. |
| `subnet_id` | Yes | VPC subnet where task instances launch. |
| `security_group_ids` | No | Optional SG IDs. **No inbound rules needed** — SSH traffic is tunnelled via SSM. |
| `instance_types` | No | Mapping of `task.partition` → EC2 instance type. Keys default to `{"default": "c5.xlarge"}`. |
| `instance_profile_arn` | No | IAM instance profile for each task. At minimum include `AmazonSSMManagedInstanceCore`; add ECR/S3 permissions if the container needs them. |
| `default_image` | No | Container image used when a task has no `image:` set. Required unless every task specifies its own. |
| `ssh_user` | No | OS user scripthut logs in as (default `ec2-user`). |
| `max_instances` | No | Hard cap on concurrent task instances (default 20). |
| `idle_terminate_seconds` | No | Safety-shutdown slack past the task's `time_limit` (default 1800). |
| `startup_timeout_seconds` | No | Scripthut fails a task if its instance never reaches "running" (default 600). |
| `tag_prefix` | No | Tag namespace scripthut uses to recognize its own instances on startup (default `scripthut`). |
| `extra_tags` | No | Additional tags applied to every launched instance. |

**How it works:**

1. `submit_task` → `RunInstances` with `InstanceInitiatedShutdownBehavior=terminate` and a user-data script that pulls your image, `docker run`s it with the task's bash script mounted, pipes output to `/var/log/scripthut/task.log`, and writes `/var/run/scripthut/done` containing the exit code.
2. Each poll cycle, scripthut opens a one-shot SSM Session Manager port-forward to the instance, pushes an ephemeral SSH key via EC2 Instance Connect (valid 60 s), connects over the tunnel, and `cat`s the sentinel.
3. When the sentinel appears, scripthut SSH-copies the log to `<data_dir>/ec2-logs/<run_id>/<task_id>.log`, then calls `TerminateInstances`.
4. On scripthut restart, a startup reconciler picks up any existing `tag:scripthut:backend=<name>` instances and resumes polling them.

**Safety layers (in order):**

1. `InstanceInitiatedShutdownBehavior=terminate` + a `shutdown -h now` safety timer in user-data — the instance self-destructs after `task.time_limit + idle_terminate_seconds` regardless of what scripthut does.
2. `timeout <task.time_limit>s` wraps the `docker run` itself — guaranteed hard kill of runaway containers.
3. Startup reconciler adopts orphans after a scripthut restart.

**Requirements on the scripthut host:**

- AWS CLI (`aws`) on PATH
- [`session-manager-plugin`](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html)
- `boto3` (from `pip install 'scripthut[batch]'`)

**Minimum IAM for the principal scripthut runs as:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:RunInstances", "ec2:DescribeInstances",
        "ec2:TerminateInstances", "ec2:CreateTags",
        "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "<instance_profile_role_arn>"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ssm:StartSession", "ssm:TerminateSession", "ssm:DescribeSessions",
        "ec2-instance-connect:SendSSHPublicKey"
      ],
      "Resource": "*"
    }
  ]
}
```

No CloudWatch Logs permissions required — logs stream over the SSH tunnel.

For the IAM bootstrap, use the included CloudFormation template:

```bash
aws cloudformation deploy \
  --template-file cloudformation/scripthut-ec2-iam.yaml \
  --stack-name scripthut-ec2-iam \
  --capabilities CAPABILITY_NAMED_IAM
```

It creates the task instance role + instance profile and a managed policy you attach to whichever IAM principal scripthut runs as. Outputs map straight into `scripthut.yaml` — see [docs/configuration.md](docs/configuration.md) for the full quick-start.

**Logs:** AWS Batch writes stdout+stderr to a single CloudWatch Log stream per job. ScriptHut reads them via `logs:GetLogEvents`. The "error" log tab shows a note pointing you to the "output" tab.

**Git workflows on AWS Batch:** There is no shared filesystem, so ScriptHut resolves the commit SHA locally (via `git ls-remote`) and passes it to each container via the `SCRIPTHUT_GIT_REPO`, `SCRIPTHUT_GIT_BRANCH`, and `SCRIPTHUT_GIT_SHA` environment variables. The generated bash script includes a runtime `git clone` + `git checkout` block so each container fetches the same ref before running the task command.

### AWS Credentials

ScriptHut **never** stores AWS credentials in `scripthut.yaml`. It uses boto3's standard credential resolution chain, which picks up credentials from (in order):

1. **Environment variables** — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (and optionally `AWS_DEFAULT_REGION`).
2. **Shared credentials file** — `~/.aws/credentials` with `aws.profile` selecting which profile to use. Run `aws configure` or `aws configure sso --profile <name>` to populate it.
3. **IAM role for the instance** (recommended for production) — when scripthut runs on EC2 / ECS / EKS / Fargate, the role attached to the instance is used automatically. No credentials are stored on the host.

The simplest setups:

```bash
# Option A — named CLI profile
aws configure --profile scripthut
# Then set aws.profile: scripthut in scripthut.yaml

# Option B — environment variables (useful for CI or one-off runs)
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1

# Option C — AWS SSO
aws sso login --profile my-sso-profile
# Then set aws.profile: my-sso-profile in scripthut.yaml
```

For Docker deployments, mount your credentials read-only (or prefer an IAM task role if running on AWS):

```bash
docker run -d -p 8000:8000 \
  -v ./scripthut.yaml:/app/scripthut.yaml \
  -v ~/.aws:/root/.aws:ro \
  -e AWS_PROFILE=scripthut \
  ghcr.io/tlamadon/scripthut:latest
```

**Minimum IAM permissions** required for the principal scripthut runs as:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "batch:SubmitJob",
        "batch:DescribeJobs",
        "batch:ListJobs",
        "batch:CancelJob",
        "batch:TerminateJob",
        "batch:RegisterJobDefinition",
        "batch:DescribeJobQueues",
        "batch:DescribeComputeEnvironments"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["logs:GetLogEvents"],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/batch/job:*"
    }
  ]
}
```

If you configure `job_role_arn` / `execution_role_arn`, the principal also needs `iam:PassRole` on those roles.

#### Sources

Sources are git repositories or backend filesystem paths containing workflow definitions. ScriptHut discovers workflow JSON files using a configurable glob pattern (`workflows_glob`, default: `.hut/workflows/*.json`). Use patterns like `**/*.hut.json` to match files recursively across any subdirectory. Each matched JSON file appears as a triggerable workflow on the Sources page.

**Common fields:**

| Field | Description |
|-------|-------------|
| `name` | Unique identifier for the source |
| `type` | Source type: `git` or `path` |
| `backend` | Backend to submit discovered workflow tasks to |
| `workflows_glob` | Glob pattern to find workflow JSON files (default: `.hut/workflows/*.json`, supports `**` for recursive) |

**Git source fields (`type: git`):**

| Field | Description |
|-------|-------------|
| `url` | Git repository URL (SSH format recommended) |
| `branch` | Branch to track (default: `main`) |
| `deploy_key` | Path to deploy key for authentication |
| `clone_dir` | Parent directory on backend for clones (default: `~/scripthut-repos`) |
| `postclone` | Shell command to run after cloning |

**Path source fields (`type: path`):**

| Field | Description |
|-------|-------------|
| `path` | Directory on the backend filesystem |

#### Settings

| Field | Description | Default |
|-------|-------------|---------|
| `data_dir` | Base directory for all stored data | `~/.cache/scripthut` |
| `poll_interval` | Seconds between job polls | `60` |
| `server_host` | Web server bind host | `127.0.0.1` |
| `server_port` | Web server bind port | `8000` |
| `sources_cache_dir` | Directory for cloned repos (overrides `<data_dir>/sources`) | `None` |

## Usage

```bash
# Use default config (./scripthut.yaml)
scripthut

# Specify config file
scripthut --config /path/to/config.yaml

# Override host/port
scripthut --host 0.0.0.0 --port 9000
```

Open http://127.0.0.1:8000 in your browser.

### API Endpoints

#### Jobs

| Endpoint | Description |
|----------|-------------|
| `GET /` | Main page with unified job list |
| `GET /jobs` | HTMX partial for job table |
| `GET /jobs/stream` | SSE endpoint for live updates |
| `POST /filter/toggle` | Toggle user filter on/off |

#### Runs

| Endpoint | Description |
|----------|-------------|
| `GET /runs` | Run management page |
| `GET /runs/{id}` | Run detail page |
| `GET /runs/{id}/items` | HTMX partial for run items |
| `POST /runs/{id}/cancel` | Cancel all pending/running items |
| `GET /runs/{id}/tasks/{task_id}/script` | View submission script |
| `GET /runs/{id}/tasks/{task_id}/logs/{type}` | View task logs (output/error) |

#### Workflows

| Endpoint | Description |
|----------|-------------|
| `GET /workflows` | List configured workflows (JSON) |
| `POST /workflows/{name}/run` | Create a new run from workflow |
| `GET /workflows/{name}/dry-run` | Preview tasks without submitting |

#### System

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check (JSON) |
| `GET /api/sources` | List source statuses (JSON) |
| `GET /sources` | Sources page (HTML) |
| `POST /sources/{name}/sync` | Trigger source sync |
| `GET /sources/{name}/workflows` | List discovered workflows |
| `GET /sources/{name}/workflows/{file}/dry-run` | Preview a source workflow |
| `POST /sources/{name}/workflows/{file}/run` | Trigger a source workflow |

## Resources and Lifecycles

ScriptHut tracks several interconnected resources. Understanding their lifecycles helps you effectively monitor and manage your jobs.

### Jobs

Jobs are the primary resource displayed on the dashboard. ScriptHut tracks jobs from two sources:

- **Run jobs**: Submitted through ScriptHut's run system
- **External jobs**: Detected via scheduler polling (jobs submitted outside ScriptHut)

#### Job States

```
┌─────────┐     ┌───────────┐     ┌─────────┐     ┌───────────┐
│ PENDING │────>│ SUBMITTED │────>│ RUNNING │────>│ COMPLETED │
└─────────┘     └───────────┘     └─────────┘     └───────────┘
     │               │                 │
     │               │                 │          ┌────────┐
     └───────────────┴─────────────────┴─────────>│ FAILED │
                                                   └────────┘
```

| State | Description |
|-------|-------------|
| `pending` | Job is in a run, waiting to be submitted to the scheduler |
| `submitted` | Job has been submitted (sbatch/qsub), waiting in scheduler queue |
| `running` | Job is actively executing on compute nodes |
| `completed` | Job finished successfully |
| `failed` | Job failed, was cancelled, timed out, or encountered an error |
| `dep_failed` | Job was skipped because a dependency failed |

### Runs

Runs are batches of tasks created from a Workflow. Each run manages multiple jobs with configurable concurrency.

#### Run Lifecycle

```
┌─────────────────┐
│    Workflow     │  (SSH command returns JSON task list)
└────────┬────────┘
         │ POST /workflows/{name}/run
         v
┌─────────────────┐
│  Run Created    │  (All tasks registered as PENDING)
└────────┬────────┘
         │ Submit up to max_concurrent tasks
         v
┌─────────────────┐
│  Run Running    │  (Mix of PENDING, SUBMITTED, RUNNING tasks)
└────────┬────────┘
         │ As tasks complete, new ones are submitted
         v
┌─────────────────┐
│  Run Completed  │  (All tasks COMPLETED or FAILED)
└─────────────────┘
```

#### Run States

| State | Description |
|-------|-------------|
| `pending` | Run created but no tasks submitted yet |
| `running` | Has tasks that are submitted or running |
| `completed` | All tasks completed successfully |
| `failed` | Some tasks failed (others may have completed) |
| `cancelled` | Run was manually cancelled |

### Workflows

Workflows define how to fetch a list of tasks to run. They execute an SSH command that returns JSON.

```yaml
workflows:
  - name: my-batch-jobs
    backend: hpc-cluster
    command: "python ~/scripts/generate_tasks.py"
    max_concurrent: 10
    description: "Run my batch processing jobs"
```

#### Git Workflows

Workflows can optionally clone a git repository on the backend before running the command. This is useful when your task-generating script lives in a repo rather than being pre-installed on the cluster.

```yaml
workflows:
  - name: ml-training-git
    backend: hpc-cluster
    git:
      repo: git@github.com:your-org/ml-pipelines.git
      branch: main
      deploy_key: ~/.ssh/ml-deploy-key    # local path, uploaded temporarily
      clone_dir: ~/scripthut-repos        # parent dir on backend (default)
    command: "python get_tasks.py"
    max_concurrent: 5
    description: "ML training from git repo"
```

When a workflow has a `git` section, ScriptHut will:

1. Upload the deploy key (if any) to the backend temporarily
2. Resolve the branch HEAD commit hash via `git ls-remote`
3. Clone into `<clone_dir>/<commit_hash>/` (skipped if already present)
4. Run the `command` inside the cloned directory
5. Clean up the temporary deploy key

| Field | Required | Description |
|-------|----------|-------------|
| `git.repo` | Yes | Git repository URL (SSH format recommended) |
| `git.branch` | No | Branch to clone (default: `main`) |
| `git.deploy_key` | No | Path to deploy key on local machine |
| `git.clone_dir` | No | Parent directory on backend (default: `~/scripthut-repos`) |

**Working directory resolution:** When a git workflow is active, each task's `working_dir` is resolved relative to the clone directory:

- **Default** (`~` or omitted) -- set to the clone directory
- **Relative path** (e.g., `simulations`, `src/analysis`) -- joined as `<clone_dir>/<working_dir>`
- **Absolute path** (e.g., `/scratch/data`) or home-relative (e.g., `~/other`) -- used as-is

#### Task JSON Format

The command must return JSON in one of these formats:

```json
// Array format
[
  {"id": "task1", "name": "Process A", "command": "python process.py --id=1"},
  {"id": "task2", "name": "Process B", "command": "python process.py --id=2"}
]

// Object format
{
  "tasks": [
    {"id": "task1", "name": "Process A", "command": "python process.py --id=1"}
  ]
}
```

#### Task Definition Fields

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique identifier for the task |
| `name` | Yes | Display name for the task |
| `command` | Yes | Shell command to execute |
| `deps` | No | List of task IDs this task depends on (supports wildcards) |
| `working_dir` | No | Working directory (default: `~`); relative paths are resolved against the git clone directory for git workflows |
| `partition` | No | Scheduler partition/queue (default: `normal`). On PBS backends, the config-level `queue` field takes precedence |
| `cpus` | No | CPUs per task (default: `1`) |
| `memory` | No | Memory allocation (default: `4G`). Automatically converted to PBS format (e.g., `4G` becomes `4gb`) |
| `time_limit` | No | Time limit (default: `1:00:00`) |
| `output_file` | No | Custom stdout log path |
| `error_file` | No | Custom stderr log path |
| `environment` | No | Name of an environment defined in `scripthut.yaml` |
| `env_vars` | No | Per-task environment variables as a `{"KEY": "VALUE"}` object |
| `generates_source` | No | Path to a JSON file this task creates on the backend; new tasks are appended to the run on completion |

#### Task Dependencies

Tasks can declare dependencies on other tasks via the `deps` field. A task will only be submitted once all its dependencies have completed successfully. If a dependency fails, the task is marked as `dep_failed` and skipped.

```json
{
  "tasks": [
    {"id": "setup", "name": "Setup", "command": "bash setup.sh"},
    {"id": "build", "name": "Build", "command": "make", "deps": ["setup"]},
    {"id": "test",  "name": "Test",  "command": "make test", "deps": ["build"]}
  ]
}
```

#### Wildcard Dependencies

Dependencies support glob-style wildcard patterns (`*`, `?`, `[...]`), which makes it easy to express "depend on all tasks in a group" without listing them individually.

Use **dot-notation** in task IDs to create logical groups, then use wildcards to depend on entire groups:

```json
{
  "tasks": [
    {"id": "setup.init",   "name": "Setup",     "command": "bash setup.sh"},
    {"id": "build.x",      "name": "Build X",   "command": "make x",    "deps": ["setup.*"]},
    {"id": "build.y",      "name": "Build Y",   "command": "make y",    "deps": ["setup.*"]},
    {"id": "final.merge",  "name": "Finalize",  "command": "make dist", "deps": ["build.*"]}
  ]
}
```

In this example, `final.merge` depends on `"build.*"` which automatically expands to `["build.x", "build.y"]`. This is equivalent to listing them explicitly but stays correct as you add or remove tasks in the `build` group.

Supported patterns:

| Pattern | Matches |
|---------|---------|
| `build.*` | All tasks starting with `build.` |
| `step.?` | `step.1`, `step.2`, but not `step.10` |
| `data.[ab]` | `data.a` and `data.b` |

Tasks with dot-notation IDs are also displayed hierarchically in the run detail UI, grouped by their prefix.

#### Dynamic Task Generation (`generates_source`)

A task can dynamically produce new tasks that get appended to the run when it completes. This is useful for two-phase workflows where the first task determines what work needs to be done (e.g., scanning a directory, querying a database) and the second phase executes that work.

To use this feature, set the `generates_source` field on a task to the path of a JSON file that the task will create on the backend:

```json
{
  "tasks": [
    {
      "id": "plan",
      "name": "Plan simulations",
      "command": "python plan.py --output tasks.json",
      "working_dir": "~/project",
      "generates_source": "tasks.json"
    }
  ]
}
```

When the `plan` task completes, ScriptHut reads `tasks.json` from the backend via SSH and appends the tasks it contains to the current run. The generated JSON file uses the same format as the workflow task JSON (either `{"tasks": [...]}` or a bare `[...]` array).

| Field | Description |
|-------|-------------|
| `generates_source` | Path to a JSON file the task creates on the backend. Relative paths are resolved against the task's `working_dir`. Absolute paths and `~`-prefixed paths are used as-is. |

**Generated tasks can use dependencies** to control execution order. They can depend on tasks already in the run (including the generator task itself) and on other generated tasks. Wildcard dependencies are also supported:

```json
{
  "tasks": [
    {
      "id": "plan",
      "name": "Plan",
      "command": "python plan.py --output tasks.json",
      "generates_source": "tasks.json"
    },
    {
      "id": "setup",
      "name": "Setup data",
      "command": "bash setup.sh"
    }
  ]
}
```

The generated `tasks.json` might contain:

```json
{
  "tasks": [
    {"id": "sim-1", "name": "Sim 1", "command": "python sim.py 1", "deps": ["setup"]},
    {"id": "sim-2", "name": "Sim 2", "command": "python sim.py 2", "deps": ["setup"]},
    {"id": "aggregate", "name": "Aggregate", "command": "python agg.py", "deps": ["sim-*"]}
  ]
}
```

If a generated task references a dependency that doesn't exist in the run, the entire batch of generated tasks is rejected and an error is logged.

### Environments

Environments let you define reusable sets of environment variables and initialization commands in `scripthut.yaml`. Tasks reference an environment by name, and the corresponding variables and init lines are injected into the generated submission script.

#### Defining Environments

Add an `environments` section to your `scripthut.yaml`:

```yaml
environments:
  - name: julia-1.10
    variables:
      JULIA_DEPOT_PATH: "/scratch/user/julia_depot"
      JULIA_NUM_THREADS: "8"
    extra_init: "module load julia/1.10"

  - name: python-ml
    variables:
      CUDA_VISIBLE_DEVICES: "0,1"
      OMP_NUM_THREADS: "4"
    extra_init: |
      module load cuda/12.0
      source ~/envs/ml/bin/activate
```

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Unique identifier referenced by tasks |
| `variables` | No | Key-value pairs exported as `export KEY="VALUE"` |
| `extra_init` | No | Raw bash lines inserted before the task command (e.g. `module load`, `source activate`) |

#### Using Environments in Tasks

Tasks declare which environment to use via the `environment` field in their JSON definition:

```json
[
  {"id": "solve", "name": "Solve Model", "command": "julia solve.jl", "environment": "julia-1.10"}
]
```

This produces a submission script like:

```bash
#!/bin/bash -l
#SBATCH --job-name="Solve Model"    # Slurm
#PBS -N Solve Model                  # or PBS
...

export JULIA_DEPOT_PATH="/scratch/user/julia_depot"
export JULIA_NUM_THREADS="8"

module load julia/1.10

cd ~/projects/jmp
julia solve.jl
```

If a task references an environment name that doesn't exist in the config, a warning is logged and the script is generated without any environment setup.

#### Per-Task Environment Variables

In addition to named environments, generators can specify inline environment variables per task via the `env_vars` field:

```json
[
  {
    "id": "sim-1", "name": "Sim 1",
    "command": "python sim.py",
    "env_vars": {"MY_PARAM": "42", "DATA_DIR": "/scratch/data"}
  }
]
```

When both a named `environment` and per-task `env_vars` are set, they are merged. Per-task values override named-environment values when keys collide.

#### Automatic ScriptHut Variables

ScriptHut automatically injects environment variables into every submission script. These are always present:

| Variable | Description | Example |
|----------|-------------|---------|
| `SCRIPTHUT_WORKFLOW` | Name of the workflow | `jmp-grid-pure` |
| `SCRIPTHUT_RUN_ID` | Unique run identifier | `a1b2c3d4` |
| `SCRIPTHUT_CREATED_AT` | ISO timestamp of run creation | `2026-02-19T14:30:00` |

For **git workflows**, these additional variables are also set:

| Variable | Description | Example |
|----------|-------------|---------|
| `SCRIPTHUT_GIT_REPO` | Repository URL | `git@github.com:org/repo.git` |
| `SCRIPTHUT_GIT_BRANCH` | Branch name | `main` |
| `SCRIPTHUT_GIT_SHA` | Full commit hash used for the run | `abc123def456...` |

#### Variable Priority

Environment variables are merged in the following order (later entries win):

1. **ScriptHut automatic variables** (`SCRIPTHUT_*`)
2. **Named environment** (from `environments` config)
3. **Per-task `env_vars`** (from generator JSON)

This means a generator can override any variable, including the automatic ones, if needed.

### Cost Estimation

ScriptHut can estimate the cost of a run by mapping Slurm partitions to EC2 instance types and looking up pricing from [instances.vantage.sh](https://instances.vantage.sh/). Pricing data is fetched once and cached locally for 24 hours.

#### Configuration

Add a `pricing` section to your `scripthut.yaml`:

```yaml
pricing:
  region: us-east-1           # AWS region for price lookup
  price_type: spot_avg        # ondemand, spot_avg, spot_min, spot_max
  partitions:                 # map Slurm partitions to EC2 instance types
    standard: c5.xlarge
    gpu: p3.2xlarge
```

| Field | Required | Description | Default |
|-------|----------|-------------|---------|
| `region` | No | AWS region for pricing lookup | `us-east-1` |
| `price_type` | No | Pricing type: `ondemand`, `spot_avg`, `spot_min`, `spot_max` | `ondemand` |
| `partitions` | Yes | Mapping of Slurm partition names to EC2 instance types | -- |

#### How It Works

For each completed task with timing data from the scheduler:

```
cost = elapsed_hours × (task_cpus / instance_vcpus) × price_per_hour
```

The total estimated cost is displayed in the run detail header. Tasks on unmapped partitions or without timing data are counted separately (e.g., "8/10 tasks costed").

If the pricing section is omitted or the pricing data cannot be fetched, the cost display is simply hidden -- no errors or disruption to the rest of the UI.

### Data Flow

```
                                    ┌─────────────────────┐
                                    │   Run Storage       │
                                    │   (JSON files)      │
                                    └──────────┬──────────┘
                                               │
                                               v
┌──────────────┐    polling    ┌─────────────────────────────┐    display
│ HPC Cluster  │<─────────────>│     ScriptHut Server        │────────────> Web UI
│ (Slurm/PBS)  │  squeue/qstat │                             │
└──────────────┘               │  ┌─────────────────────┐    │
       ^                       │  │ RunManager          │    │
       │ sbatch/qsub           │  │ - create runs       │    │
       │                       │  │ - update states     │    │
┌──────┴───────┐               │  │ - persist to JSON   │    │
│ Job Backend  │<──────────────│  └─────────────────────┘    │
│ (abstract)   │  submit tasks │                             │
└──────────────┘               └─────────────────────────────┘
       ^
       │ create run
       │
┌──────┴───────┐
│  Workflow    │  (SSH command -> JSON tasks)
└──────────────┘
```

## Architecture

```
src/scripthut/
├── main.py           # FastAPI app, routes, background polling
├── config.py         # Configuration loading (YAML + .env)
├── config_schema.py  # Pydantic models for YAML schema
├── models.py         # Data models (HPCJob, JobState, ConnectionStatus)
├── pricing.py        # EC2-equivalent cost estimation (instances.vantage.sh)
├── ssh/
│   └── client.py     # Async SSH client with connection management
├── backends/
│   ├── base.py       # Abstract JobBackend interface + JobStats
│   ├── utils.py      # Shared utilities (duration parsing, script body generation)
│   ├── slurm.py      # Slurm implementation (squeue/sacct/sbatch/scancel/sinfo)
│   ├── pbs.py        # PBS/Torque implementation (qstat/qsub/qdel/pbsnodes)
│   ├── batch.py      # AWS Batch implementation (boto3 + CloudWatch Logs)
│   ├── ec2.py        # AWS EC2-direct implementation (one instance per task)
│   └── ec2_ssm.py    # SSM-tunnelled SSH + ephemeral EC2 Instance Connect keys
├── sources/
│   └── git.py        # Git repository management with deploy keys
└── runs/
    ├── models.py     # Run, RunItem, TaskDefinition models
    ├── manager.py    # Run lifecycle and task submission
    └── storage.py    # Folder-based JSON persistence
```

### Supported Backends

| Backend | Scheduler | Transport | Status |
|---------|-----------|-----------|--------|
| Slurm | sbatch, squeue, sacct, scancel, sinfo | SSH | Stable |
| PBS/Torque | qsub, qstat, qdel, pbsnodes | SSH | Stable |
| AWS Batch | AWS Batch + CloudWatch Logs API | boto3 (AWS credential chain) | Stable |
| AWS EC2 | RunInstances + DescribeInstances (one instance per task) | boto3 + SSH via SSM Session Manager | New |
| ECS | AWS ECS API | boto3 | Planned |

### Adding New Backends

To add support for a new job system:

1. Create a new file in `src/scripthut/backends/` (e.g., `batch.py`)
2. Implement the `JobBackend` abstract class from `backends/base.py`
3. Add a config model in `config_schema.py` and add it to the `BackendConfig` union
4. Wire it into `init_backend()` in `main.py`

```python
from scripthut.backends.base import JobBackend, JobStats

class MyBackend(JobBackend):
    @property
    def name(self) -> str:
        return "my-backend"

    async def get_jobs(self, user=None) -> list[HPCJob]: ...
    async def submit_job(self, script: str) -> str: ...
    async def cancel_job(self, job_id: str) -> None: ...
    async def get_job_stats(self, job_ids, user=None) -> dict[str, JobStats]: ...
    async def get_cluster_info(self) -> tuple[int, int] | None: ...
    def generate_script(self, task, run_id, log_dir, **kwargs) -> str: ...
    async def is_available(self) -> bool: ...

    @property
    def failure_states(self) -> dict[str, str]: ...
    @property
    def terminal_states(self) -> frozenset[str]: ...
```

Shared utilities in `backends/utils.py` (duration parsing, memory formatting, script body generation) can be reused across backends.

## Development

```bash
# Run type checking
mypy src/

# Run linter
ruff check src/

# Run tests
pytest
```

## Roadmap

- [x] **Phase 1**: Multi-backend Slurm monitoring
- [x] **Phase 1**: Git source integration with deploy keys
- [x] **Phase 2**: Submit jobs to Slurm from UI (task runs)
- [x] **Phase 2**: Job persistence and history
- [x] **Phase 2**: Job logs viewer
- [x] **Phase 3**: PBS/Torque backend support
- [x] **Phase 3**: AWS Batch backend support (boto3 + CloudWatch Logs)
- [x] **Phase 3**: AWS EC2-direct backend support (SSM-tunnelled SSH, one instance per task)
- [ ] **Phase 3**: ECS backend support
- [ ] **Phase 4**: Job notifications and alerts

## Requirements

- Python 3.11+
- For Slurm/PBS: SSH access to the cluster's login node with key-based authentication
- For AWS Batch: `pip install 'scripthut[batch]'` and AWS credentials reachable via the standard [credential chain](#aws-credentials) (IAM role, env vars, or `~/.aws/credentials` profile)
- For AWS EC2: same boto3 install; additionally [`session-manager-plugin`](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html) on the scripthut host, and an AMI with sshd + SSM Agent + docker

## License

MIT
