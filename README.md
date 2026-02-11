# ScriptHut

[![PyPI version](https://img.shields.io/pypi/v/scripthut)](https://pypi.org/project/scripthut/)
[![Tests](https://github.com/tlamadon/scripthut/actions/workflows/tests.yml/badge.svg)](https://github.com/tlamadon/scripthut/actions/workflows/tests.yml)

A Python web interface to start and track jobs on remote systems like Slurm, ECS, and AWS Batch over SSH.

## Features

- **Multi-backend support** - Monitor multiple Slurm/ECS backends from a single dashboard
- **Real-time job monitoring** - View running and pending jobs with auto-refresh
- **Job persistence** - Jobs survive server restarts with automatic 7-day history retention
- **Task queues** - Submit batches of jobs with configurable concurrency limits and dependencies
- **Unified job view** - See queue-submitted and external jobs in one dashboard
- **Git source integration** - Clone job repositories with deploy key support
- **Persistent SSH connections** - Maintains connections with keepalive and auto-reconnect
- **HTMX frontend** - Dynamic updates without full page reloads
- **Type-safe** - Full type annotations with mypy strict mode support
- **Extensible** - Abstract backend system ready for ECS/AWS Batch support

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

# For development (includes mypy, ruff, pytest)
pip install -e ".[dev]"
```

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

  # ECS backend (coming soon)
  - name: production-ecs
    type: ecs
    aws:
      profile: my-aws-profile
      region: us-east-1
      cluster_name: my-ecs-cluster

# Git repositories with job definitions
sources:
  - name: ml-jobs
    url: git@github.com:org/ml-pipelines.git
    branch: main
    deploy_key: ~/.ssh/ml-jobs-deploy-key

settings:
  poll_interval: 60
  server_host: 127.0.0.1
  server_port: 8000
  sources_cache_dir: ~/.cache/scripthut/sources
```

### Configuration Options

#### Backends

| Field | Description |
|-------|-------------|
| `name` | Unique identifier for the backend |
| `type` | Backend type: `slurm` or `ecs` |
| `ssh.host` | SSH hostname (Slurm only) |
| `ssh.port` | SSH port (default: 22) |
| `ssh.user` | SSH username |
| `ssh.key_path` | Path to SSH private key |
| `aws.profile` | AWS CLI profile name (ECS only) |
| `aws.region` | AWS region (ECS only) |
| `aws.cluster_name` | ECS cluster name |

#### Sources

| Field | Description |
|-------|-------------|
| `name` | Unique identifier for the source |
| `url` | Git repository URL (SSH format recommended) |
| `branch` | Branch to track (default: main) |
| `deploy_key` | Path to deploy key for authentication |

#### Settings

| Field | Description | Default |
|-------|-------------|---------|
| `poll_interval` | Seconds between job polls | `60` |
| `server_host` | Web server bind host | `127.0.0.1` |
| `server_port` | Web server bind port | `8000` |
| `sources_cache_dir` | Directory for cloned repos | `~/.cache/scripthut/sources` |

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

#### Queues

| Endpoint | Description |
|----------|-------------|
| `GET /queues` | Queue management page |
| `GET /queues/{id}` | Queue detail page |
| `GET /queues/{id}/items` | HTMX partial for queue items |
| `POST /queues/{id}/cancel` | Cancel all pending/running items |
| `GET /queues/{id}/tasks/{task_id}/script` | View sbatch script |
| `GET /queues/{id}/tasks/{task_id}/logs/{type}` | View task logs (output/error) |

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
| `GET /sources` | List git sources |
| `POST /sources/{name}/sync` | Trigger git source sync |

## Resources and Lifecycles

ScriptHut tracks several interconnected resources. Understanding their lifecycles helps you effectively monitor and manage your jobs.

### Jobs

Jobs are the primary resource displayed on the dashboard. ScriptHut tracks jobs from two sources:

- **Queue jobs**: Submitted through ScriptHut's task queue system
- **External jobs**: Detected via SLURM polling (jobs submitted outside ScriptHut)

#### Job States

```
┌─────────┐     ┌───────────┐     ┌─────────┐     ┌───────────┐
│ PENDING │────▶│ SUBMITTED │────▶│ RUNNING │────▶│ COMPLETED │
└─────────┘     └───────────┘     └─────────┘     └───────────┘
     │               │                 │
     │               │                 │          ┌────────┐
     └───────────────┴─────────────────┴─────────▶│ FAILED │
                                                   └────────┘
```

| State | Description |
|-------|-------------|
| `pending` | Job is in a queue, waiting to be submitted to SLURM |
| `submitted` | Job has been submitted to SLURM (`sbatch`), waiting in SLURM queue |
| `running` | Job is actively executing on compute nodes |
| `completed` | Job finished successfully (disappeared from `squeue`) |
| `failed` | Job failed, was cancelled, timed out, or encountered an error |
| `dep_failed` | Job was skipped because a dependency failed |

#### Job Persistence

Jobs are persisted to `~/.cache/scripthut/job_history.json`:

- History survives server restarts
- Jobs are saved after each polling cycle
- Completed/failed jobs older than 7 days are automatically cleaned up
- External jobs are added to history when first detected via polling

### Queues

Queues are batches of tasks created from a Workflow. Each queue manages multiple jobs with configurable concurrency.

#### Queue Lifecycle

```
┌─────────────────┐
│    Workflow     │  (SSH command returns JSON task list)
└────────┬────────┘
         │ POST /workflows/{name}/run
         ▼
┌─────────────────┐
│  Queue Created  │  (All tasks registered as PENDING jobs)
└────────┬────────┘
         │ Submit up to max_concurrent tasks
         ▼
┌─────────────────┐
│ Queue Running   │  (Mix of PENDING, SUBMITTED, RUNNING tasks)
└────────┬────────┘
         │ As tasks complete, new ones are submitted
         ▼
┌─────────────────┐
│ Queue Completed │  (All tasks COMPLETED or FAILED)
└─────────────────┘
```

#### Queue States

| State | Description |
|-------|-------------|
| `pending` | Queue created but no tasks submitted yet |
| `running` | Has tasks that are submitted or running |
| `completed` | All tasks completed successfully |
| `failed` | Some tasks failed (others may have completed) |
| `cancelled` | Queue was manually cancelled |

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
| `working_dir` | No | Working directory (default: `~`) |
| `partition` | No | SLURM partition (default: `normal`) |
| `cpus` | No | CPUs per task (default: `1`) |
| `memory` | No | Memory allocation (default: `4G`) |
| `time_limit` | No | Time limit (default: `1:00:00`) |
| `output_file` | No | Custom stdout log path |
| `error_file` | No | Custom stderr log path |
| `environment` | No | Name of an environment defined in `scripthut.yaml` |

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

Tasks with dot-notation IDs are also displayed hierarchically in the queue detail UI, grouped by their prefix.

### Environments

Environments let you define reusable sets of environment variables and initialization commands in `scripthut.yaml`. Tasks reference an environment by name, and the corresponding variables and init lines are injected into the generated sbatch script.

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

This produces an sbatch script like:

```bash
#!/bin/bash -l
#SBATCH --job-name="Solve Model"
#SBATCH ...

echo "=== ScriptHut Task: Solve Model ==="
...

export JULIA_DEPOT_PATH="/scratch/user/julia_depot"
export JULIA_NUM_THREADS="8"

module load julia/1.10

cd ~/projects/jmp
julia solve.jl
```

If a task references an environment name that doesn't exist in the config, a warning is logged and the script is generated without any environment setup.

### Data Flow

```
                                    ┌─────────────────────┐
                                    │   Job History       │
                                    │   (JSON file)       │
                                    └──────────┬──────────┘
                                               │
                                               ▼
┌──────────────┐    polling    ┌─────────────────────────────┐    display
│ SLURM        │◀─────────────▶│     ScriptHut Server        │─────────────▶ Web UI
│ Cluster      │    squeue     │                             │
└──────────────┘               │  ┌─────────────────────┐    │
       ▲                       │  │ JobHistoryManager   │    │
       │ sbatch                │  │ - register jobs     │    │
       │                       │  │ - update states     │    │
┌──────┴───────┐               │  │ - persist to JSON   │    │
│ Queue        │◀──────────────│  └─────────────────────┘    │
│ Manager      │  submit tasks │                             │
└──────────────┘               └─────────────────────────────┘
       ▲
       │ create queue
       │
┌──────┴───────┐
│  Workflow    │  (SSH command → JSON tasks)
└──────────────┘
```

## Architecture

```
src/scripthut/
├── main.py           # FastAPI app, routes, background polling
├── config.py         # Configuration loading (YAML + .env)
├── config_schema.py  # Pydantic models for YAML schema
├── models.py         # Data models (SlurmJob, JobState, ConnectionStatus)
├── ssh/
│   └── client.py     # Async SSH client with connection management
├── backends/
│   ├── base.py       # Abstract JobBackend interface
│   └── slurm.py      # Slurm implementation (squeue parsing)
├── sources/
│   └── git.py        # Git repository management with deploy keys
├── queues/
│   ├── models.py     # Queue, QueueItem, TaskDefinition models
│   └── manager.py    # Queue lifecycle and task submission
└── history/
    ├── models.py     # UnifiedJob model for persistence
    └── manager.py    # JobHistoryManager for JSON persistence
```

### Adding New Backends

To add support for a new job system (e.g., AWS Batch):

1. Create a new file in `src/scripthut/backends/` (e.g., `batch.py`)
2. Implement the `JobBackend` abstract class
3. Define appropriate job models in `models.py`

```python
from scripthut.backends.base import JobBackend

class BatchBackend(JobBackend):
    @property
    def name(self) -> str:
        return "aws-batch"

    async def get_jobs(self, user: str | None = None) -> list[BatchJob]:
        # Implementation here
        ...

    async def is_available(self) -> bool:
        # Implementation here
        ...
```

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
- [x] **Phase 2**: Submit jobs to Slurm from UI (task queues)
- [x] **Phase 2**: Job persistence and history
- [x] **Phase 2**: Job logs viewer
- [ ] **Phase 3**: ECS/AWS Batch support
- [ ] **Phase 4**: Job notifications and alerts

## Requirements

- Python 3.11+
- SSH access to remote Slurm clusters with key-based authentication

## License

MIT
