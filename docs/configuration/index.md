# YAML Configuration

ScriptHut is configured via one or two `scripthut.yaml` files. The simplest setup is a single file in the current directory; the recommended setup for day-to-day use is **two layers**: a user-global file for your infrastructure and a project-local file for each repo's workflows and stacks. See [From a project](../from-a-project.md) for the layered model end-to-end.

The configuration file has the following top-level sections:

```yaml
backends: [...]       # Remote compute backends (Slurm, PBS, AWS Batch, AWS EC2)
sources: [...]        # Git repository sources
workflows: [...]      # Task generators (SSH commands returning JSON)
projects: [...]       # Git projects with sflow.json files
stacks: [...]         # Reusable software environments installed once per backend
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
- [Stacks](stacks.md) — reusable software environments (Python/Julia/Conda…)

---

## Configuration File Lookup

ScriptHut supports a layered model: a user-global file for infrastructure and an optional project-local file at the repo root. Both are optional; if only one exists, behavior matches the single-file setup.

Resolution order:

1. **Explicit path** via `--config <path>` — loads exactly that file with no merging.
2. **User-global config** at `~/.config/scripthut/scripthut.yaml` (or the legacy `~/.scripthut.yaml`).
3. **Project-local config** found by walking up from the current working directory until a `scripthut.yaml` (or `.yml`) is hit.
4. **Merge** when both 2 and 3 exist: project-local overrides global by name in `stacks` / `workflows` / `projects`, env lists concatenate (global first), and `env_groups` dict-merge. Infrastructure fields (`backends`, `sources`, `settings`, `pricing`) come strictly from the global file and are **rejected** in a project-local file.
5. **Legacy `.env`** fallback if no YAML is found anywhere (deprecated).

All path fields (e.g., `key_path`, `data_dir`, `deploy_key`, `input_files`) support `~` expansion. Relative paths inside a YAML are resolved against **that file's directory**, not the process CWD — so a project-local `input_files: [requirements.txt]` always refers to the project's `requirements.txt`, regardless of where the CLI was invoked from.

See [From a project](../from-a-project.md) for a full walkthrough of how the two layers compose in practice.

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
