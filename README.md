# ScriptHut

[![PyPI version](https://img.shields.io/pypi/v/scripthut)](https://pypi.org/project/scripthut/)
[![Tests](https://github.com/tlamadon/scripthut/actions/workflows/tests.yml/badge.svg)](https://github.com/tlamadon/scripthut/actions/workflows/tests.yml)
[![Docker](https://ghcr-badge.egpl.dev/tlamadon/scripthut/latest_tag?trim=major&label=docker)](https://github.com/tlamadon/scripthut/pkgs/container/scripthut)
[![Docs](https://img.shields.io/badge/docs-tlamadon.github.io%2Fscripthut-blue)](https://tlamadon.github.io/scripthut/)

A Python web interface to start and track jobs on remote HPC systems (Slurm, PBS/Torque) over SSH.

## Features

- **Multi-backend support** - Monitor multiple Slurm and PBS/Torque clusters from a single dashboard
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

**PBS-specific:**

| Field | Description |
|-------|-------------|
| `queue` | Default PBS queue (overrides task `partition` field) |

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
│   └── pbs.py        # PBS/Torque implementation (qstat/qsub/qdel/pbsnodes)
├── sources/
│   └── git.py        # Git repository management with deploy keys
└── runs/
    ├── models.py     # Run, RunItem, TaskDefinition models
    ├── manager.py    # Run lifecycle and task submission
    └── storage.py    # Folder-based JSON persistence
```

### Supported Backends

| Backend | Scheduler | Commands | Status |
|---------|-----------|----------|--------|
| Slurm | sbatch, squeue, sacct, scancel, sinfo | Submit, monitor, cancel, resource stats | Stable |
| PBS/Torque | qsub, qstat, qdel, pbsnodes | Submit, monitor, cancel, resource stats | New |
| ECS | AWS ECS API | -- | Planned |

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
- [ ] **Phase 3**: ECS/AWS Batch support
- [ ] **Phase 4**: Job notifications and alerts

## Requirements

- Python 3.11+
- SSH access to remote HPC clusters (Slurm or PBS/Torque) with key-based authentication

## License

MIT
