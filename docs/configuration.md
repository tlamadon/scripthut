# YAML Configuration

ScriptHut is configured via a `scripthut.yaml` file. By default, the application looks for this file in the current working directory. You can also specify a custom path when starting the server.

The configuration file has the following top-level sections:

```yaml
backends: [...]       # Remote compute backends (Slurm, PBS, ECS)
sources: [...]        # Git repository sources
workflows: [...]      # Task generators (SSH commands returning JSON)
projects: [...]       # Git projects with sflow.json files
environments: [...]   # Named environment variable sets
pricing: {...}        # EC2-equivalent cost estimation
settings: {...}       # Global application settings
```

All sections are optional and default to empty lists or sensible defaults.

---

## Backends

Backends define the remote compute systems where jobs are submitted. ScriptHut supports **Slurm**, **PBS/Torque**, and **ECS** (planned) backend types. Each backend is identified by a unique `name` and discriminated by its `type` field.

### Slurm Backend

```yaml
backends:
  - name: hpc-cluster
    type: slurm
    ssh:
      host: slurm-login.cluster.edu
      port: 22
      user: your_username
      key_path: ~/.ssh/id_rsa
      cert_path: ~/.ssh/id_rsa-cert.pub   # optional
      known_hosts: ~/.ssh/known_hosts      # optional
    account: pi-faculty       # optional
    login_shell: false        # optional, default: false
    max_concurrent: 100       # optional, default: 100
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this backend. Referenced by workflows. |
| `type` | string | **required** | Must be `"slurm"`. |
| `ssh` | object | **required** | SSH connection settings (see [SSH Config](#ssh-config) below). |
| `account` | string | `null` | Slurm account to charge jobs to. Passed as `--account` to `sbatch`. |
| `login_shell` | boolean | `false` | If `true`, job scripts use `#!/bin/bash -l` to source your login profile (`.bash_profile`, etc.). |
| `max_concurrent` | integer | `100` | Maximum total concurrent jobs across all runs on this backend. Must be >= 1. |

### PBS/Torque Backend

```yaml
backends:
  - name: pbs-cluster
    type: pbs
    ssh:
      host: pbs-login.cluster.edu
      user: your_username
      key_path: ~/.ssh/id_rsa
    account: my-project       # optional
    login_shell: false        # optional
    max_concurrent: 100       # optional
    queue: batch              # optional
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this backend. |
| `type` | string | **required** | Must be `"pbs"`. |
| `ssh` | object | **required** | SSH connection settings. |
| `account` | string | `null` | PBS account (`-A` flag). |
| `login_shell` | boolean | `false` | Use login shell in job scripts. |
| `max_concurrent` | integer | `100` | Maximum concurrent jobs. |
| `queue` | string | `null` | Default PBS queue. Overrides the `partition` field in task definitions. |

### ECS Backend

!!! warning "Not yet implemented"
    ECS backend support is planned but not yet available.

```yaml
backends:
  - name: production-ecs
    type: ecs
    aws:
      profile: my-aws-profile    # optional
      region: us-east-1
      cluster_name: my-cluster
    max_concurrent: 100
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this backend. |
| `type` | string | **required** | Must be `"ecs"`. |
| `aws` | object | **required** | AWS configuration (see below). |
| `max_concurrent` | integer | `100` | Maximum concurrent jobs. |

**AWS Config:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `profile` | string | `null` | AWS CLI profile name. Uses default credential chain if not set. |
| `region` | string | **required** | AWS region (e.g., `us-east-1`). |
| `cluster_name` | string | **required** | ECS cluster name. |

### SSH Config

SSH settings are shared by both Slurm and PBS backends.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | string | **required** | Hostname of the remote machine. |
| `port` | integer | `22` | SSH port. |
| `user` | string | **required** | SSH username. |
| `key_path` | path | `~/.ssh/id_rsa` | Path to SSH private key. Supports `~` expansion. |
| `cert_path` | path | `null` | Path to SSH certificate for certificate-based authentication. |
| `known_hosts` | path | `null` | Path to `known_hosts` file. If `null`, host key checking is disabled. |

---

## Sources

Sources are git repositories or backend filesystem paths containing workflow definitions. ScriptHut discovers JSON task lists in the `.hut/workflows/` directory of each source. Each JSON file appears as a triggerable workflow on the Sources page.

For **git sources**, the repository is cloned locally for workflow discovery, and also cloned on the backend when a workflow is triggered (tasks run inside the cloned directory, just like git-based workflows).

For **path sources**, workflows are discovered via SSH on the backend, and tasks run with `working_dir` resolved relative to the source path.

### Git Source

```yaml
sources:
  - name: ml-jobs
    type: git
    url: git@github.com:your-org/ml-pipelines.git
    branch: main
    deploy_key: ~/.ssh/ml-jobs-deploy-key
    backend: hpc-cluster
    # workflows_dir: .hut/workflows    # default
    # clone_dir: ~/scripthut-repos     # default
    # postclone: "rm -rf large_files"  # optional
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this source. |
| `type` | string | **required** | Must be `"git"`. |
| `url` | string | **required** | Git repository URL. SSH format recommended. |
| `branch` | string | `"main"` | Branch to track. |
| `deploy_key` | path | `null` | Path to deploy key for this repository. |
| `backend` | string | **required** | Backend to submit discovered workflow tasks to. |
| `workflows_dir` | string | `".hut/workflows"` | Directory within the repo containing workflow JSON files. |
| `clone_dir` | string | `"~/scripthut-repos"` | Parent directory on the backend. The repo is cloned into `<clone_dir>/<commit_hash>/`. |
| `postclone` | string | `null` | Shell command to run in the clone directory after cloning. |

### Path Source

```yaml
sources:
  - name: shared-workflows
    type: path
    path: /shared/project-workflows
    backend: hpc-cluster
    # workflows_dir: .hut/workflows  # default
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this source. |
| `type` | string | **required** | Must be `"path"`. |
| `path` | string | **required** | Directory on the backend filesystem. |
| `backend` | string | **required** | Backend where this path exists and where tasks are submitted. |
| `workflows_dir` | string | `".hut/workflows"` | Directory within the path containing workflow JSON files. |

---

## Workflows

Workflows are the primary mechanism for submitting batch jobs. A workflow defines an SSH command that runs on a backend and returns a JSON list of tasks (see [Task JSON Format](task-json.md) for details on the expected output).

### Basic Workflow

```yaml
workflows:
  - name: ml-training
    backend: hpc-cluster
    command: "python /shared/scripts/get_training_tasks.py"
    max_concurrent: 5
    description: "ML model training pipeline"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this workflow. Shown in the UI. |
| `backend` | string | **required** | Name of a backend defined in the `backends` section. |
| `command` | string | **required** | Shell command executed via SSH that must print JSON to stdout. |
| `max_concurrent` | integer | `null` | Max concurrent tasks per run. If `null`, only the backend-level limit applies. |
| `description` | string | `""` | Human-readable description shown in the UI. |
| `git` | object | `null` | Optional git repository to clone on the backend before running the command. |

### Git Workflows

Git workflows clone a repository on the remote backend before executing the command. The command runs inside the cloned directory. This is useful when your task generator script lives in a repository.

```yaml
workflows:
  - name: ml-training-git
    backend: hpc-cluster
    git:
      repo: git@github.com:your-org/ml-pipelines.git
      branch: main
      deploy_key: ~/.ssh/ml-deploy-key
      clone_dir: ~/scripthut-repos
      postclone: "rm -rf large_files"
    command: "python get_tasks.py"
    max_concurrent: 5
    description: "ML training from git repo"
```

**Git Config Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `repo` | string | **required** | Git repository URL. SSH format recommended. |
| `branch` | string | `"main"` | Branch to clone. |
| `deploy_key` | path | `null` | Path to deploy key on the **local** machine. It is uploaded to the backend temporarily during the clone operation. |
| `clone_dir` | string | `"~/scripthut-repos"` | Parent directory on the backend. The repo is cloned into `<clone_dir>/<commit_hash>/`. |
| `postclone` | string | `null` | Shell command to run in the clone directory after cloning (e.g., to remove large files or install dependencies). |

When using a git workflow:

- The `command` runs with the clone directory as its working directory.
- Task `working_dir` values using `~` or relative paths are resolved relative to the clone directory.
- Git metadata is injected as environment variables into every task (see [Automatic Environment Variables](task-json.md#automatic-environment-variables)).

---

## Projects

Projects reference git repositories that already exist on the backend and contain `sflow.json` workflow definition files.

```yaml
projects:
  - name: my-project
    backend: hpc-cluster
    path: /home/user/my-project
    max_concurrent: 10
    description: "My research project"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this project. |
| `backend` | string | **required** | Name of the backend where the project lives. |
| `path` | string | **required** | Path to the git repository on the backend. |
| `max_concurrent` | integer | `null` | Default max concurrent tasks per run. |
| `description` | string | `""` | Human-readable description. |

---

## Environments

Named environments define sets of environment variables and initialization commands that can be referenced by tasks. This is useful for loading modules, setting up language-specific paths, or configuring runtime parameters.

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
      PYTHONPATH: "/home/user/libs"
    extra_init: |
      module load cuda/12.0
      source /home/user/venvs/ml/bin/activate
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier. Referenced by the `environment` field in task definitions. |
| `variables` | object | `{}` | Key-value pairs exported as environment variables before the task command. |
| `extra_init` | string | `""` | Raw bash lines to run before the task command (e.g., `module load` commands). Runs after environment variable exports. |

Tasks reference environments by name in their JSON definition:

```json
{
  "id": "train-model",
  "name": "Train Model",
  "command": "julia train.jl",
  "environment": "julia-1.10"
}
```

See [Environment Variable Priority](task-json.md#environment-variable-priority) for how environment variables from different sources are merged.

---

## Pricing

Optional EC2-equivalent cost estimation. Maps scheduler partitions to EC2 instance types and fetches pricing data from `instances.vantage.sh`.

```yaml
pricing:
  region: us-east-1
  price_type: spot_avg
  partitions:
    standard: c5.xlarge
    gpu: p3.2xlarge
    highmem: r5.4xlarge
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `region` | string | `"us-east-1"` | AWS region for pricing lookup. |
| `price_type` | string | `"ondemand"` | Pricing type. One of: `ondemand`, `spot_avg`, `spot_min`, `spot_max`. |
| `partitions` | object | `{}` | Mapping of scheduler partition names to EC2 instance types. |

Cost estimates appear in the UI when tasks specify a `partition` that has a mapping defined here.

---

## Settings

Global application settings that control server behavior and data storage.

```yaml
settings:
  poll_interval: 60
  server_host: 127.0.0.1
  server_port: 8000
  data_dir: ~/.cache/scripthut
  sources_cache_dir: ~/.cache/scripthut/sources
  filter_user: your_username
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `poll_interval` | integer | `60` | Interval in seconds between job status polls. Minimum: 5. |
| `server_host` | string | `"127.0.0.1"` | Host to bind the web server to. Use `0.0.0.0` to listen on all interfaces. |
| `server_port` | integer | `8000` | Port to bind the web server to. |
| `data_dir` | path | `~/.cache/scripthut` | Base directory for all stored data (run history, logs). |
| `sources_cache_dir` | path | `<data_dir>/sources` | Directory to cache cloned repositories. |
| `filter_user` | string | `null` | Default username for the "My Jobs" filter in the UI. If `null`, all users' jobs are shown. |

---

## Configuration File Lookup

ScriptHut searches for configuration in the following order:

1. Explicit path passed via command-line argument
2. `./scripthut.yaml` in the current directory
3. `./scripthut.yml` in the current directory

All path fields (e.g., `key_path`, `data_dir`, `deploy_key`) support `~` expansion to the user's home directory.

---

## Complete Example

```yaml
backends:
  - name: hpc-cluster
    type: slurm
    ssh:
      host: slurm-login.cluster.edu
      port: 22
      user: researcher
      key_path: ~/.ssh/id_rsa
    account: pi-faculty
    login_shell: true
    max_concurrent: 50

  - name: pbs-cluster
    type: pbs
    ssh:
      host: pbs-login.cluster.edu
      user: researcher
      key_path: ~/.ssh/id_rsa
    queue: batch

sources:
  - name: ml-jobs
    type: git
    url: git@github.com:my-org/ml-pipelines.git
    branch: main
    deploy_key: ~/.ssh/ml-deploy-key
    backend: hpc-cluster

workflows:
  - name: simple-tasks
    backend: hpc-cluster
    command: "python /shared/scripts/generate_tasks.py --count 10"
    max_concurrent: 5
    description: "Simple test tasks"

  - name: ml-training
    backend: hpc-cluster
    git:
      repo: git@github.com:my-org/ml-pipelines.git
      branch: main
      deploy_key: ~/.ssh/ml-deploy-key
      clone_dir: ~/scripthut-repos
    command: "python generate_tasks.py"
    max_concurrent: 3
    description: "ML training pipeline from git"

environments:
  - name: julia-env
    variables:
      JULIA_DEPOT_PATH: "/scratch/researcher/julia_depot"
      JULIA_NUM_THREADS: "8"
    extra_init: "module load julia/1.10"

pricing:
  region: us-east-1
  price_type: spot_avg
  partitions:
    normal: c5.xlarge
    gpu: p3.2xlarge

settings:
  poll_interval: 30
  server_host: 127.0.0.1
  server_port: 8000
  data_dir: ~/.cache/scripthut
  filter_user: researcher
```
