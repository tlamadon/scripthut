# YAML Configuration

ScriptHut is configured via a `scripthut.yaml` file. By default, the application looks for this file in the current working directory. You can also specify a custom path when starting the server.

The configuration file has the following top-level sections:

```yaml
backends: [...]       # Remote compute backends (Slurm, PBS, AWS Batch, AWS EC2)
sources: [...]        # Git repository sources
workflows: [...]      # Task generators (SSH commands returning JSON)
projects: [...]       # Git projects with sflow.json files
env: [...]            # Server-level env rules (see Environments)
env_groups: {...}     # Named, reusable rule lists
pricing: {...}        # EC2-equivalent cost estimation
settings: {...}       # Global application settings
```

Both `env:` and `env_groups:` can also appear inside individual backend and workflow entries — see [Environments](environments.md).

All sections are optional and default to empty lists or sensible defaults.

This page covers the small top-level concerns — file lookup, pricing, settings, and a complete example. The substantial sections each have their own page:

- [Backends](backends.md) — Slurm, PBS, AWS Batch, AWS EC2, SSH config
- [Workflows](workflows.md) — workflows (incl. git), sources, projects
- [Environments](environments.md) — the env-rule resolver and `env_groups`

---

## Configuration File Lookup

ScriptHut searches for configuration in the following order:

1. Explicit path passed via command-line argument
2. `./scripthut.yaml` in the current directory
3. `./scripthut.yml` in the current directory

All path fields (e.g., `key_path`, `data_dir`, `deploy_key`) support `~` expansion to the user's home directory.

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
| `cli_server` | string | `null` | Default URL of a running scripthut server for the CLI. Overridden by `--server` and `SCRIPTHUT_SERVER`. See [CLI](../cli.md). |

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

env_groups:
  julia-env:
    - set:
        JULIA_DEPOT_PATH: "/scratch/researcher/julia_depot"
        JULIA_NUM_THREADS: "8"
    - init: "module load julia/1.10"

env:
  - set:
      LOG_LEVEL: info

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
