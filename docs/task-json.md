# Task JSON Format

Workflows in ScriptHut work by executing a command that prints a JSON document to **stdout** describing the tasks to submit. For Slurm and PBS backends, the command runs on the cluster's login node via SSH. For AWS Batch backends (which have no login node), the command runs locally on the scripthut host instead — plan the command path accordingly (e.g. `python /app/generate_tasks.py` inside a Docker deployment). This page covers the expected JSON structure, all available task fields, dependencies, dynamic task generation, and environment variables.

---

## JSON Structure

ScriptHut accepts two top-level formats:

=== "Object format (recommended)"

    ```json
    {
      "tasks": [
        {
          "id": "task-001",
          "name": "First Task",
          "command": "python train.py"
        },
        {
          "id": "task-002",
          "name": "Second Task",
          "command": "python evaluate.py"
        }
      ]
    }
    ```

=== "Array format"

    ```json
    [
      {
        "id": "task-001",
        "name": "First Task",
        "command": "python train.py"
      },
      {
        "id": "task-002",
        "name": "Second Task",
        "command": "python evaluate.py"
      }
    ]
    ```

Both formats are equivalent. The object format with the `"tasks"` key is recommended as it leaves room for future top-level metadata.

---

## Task Fields

Each task object supports the following fields:

| Field | Required | Type | Default | Description |
|-------|----------|------|---------|-------------|
| `id` | **yes** | string | — | Unique task identifier. Supports dot-notation for hierarchical grouping (e.g., `build.x`). |
| `name` | **yes** | string | — | Human-readable display name shown in the UI. |
| `command` | **yes** | string | — | Shell command to execute. Can be multi-line. |
| `working_dir` | no | string | `"~"` | Working directory for the command. Supports `~` expansion. For git workflows, relative paths resolve against the clone directory. |
| `partition` | no | string | `"normal"` | Scheduler partition/queue to submit to. For PBS backends, this may be overridden by the backend's `queue` setting. |
| `cpus` | no | integer | `1` | Number of CPUs per task. |
| `memory` | no | string | `"4G"` | Memory allocation. Use Slurm format (e.g., `"4G"`, `"500M"`, `"16G"`). Automatically converted to PBS format when needed. |
| `time_limit` | no | string | `"1:00:00"` | Wall-time limit in `HH:MM:SS` format. |
| `gres` | no | string | `null` | Slurm-style generic resource request (e.g., `"gpu:2"`, `"gpu:v100:1"`). Passed to `--gres` on Slurm; on PBS the GPU count is translated to `:gpus=N` on the node spec. On AWS Batch the GPU count is added as a `GPU` resource requirement. Non-GPU `gres` is ignored outside Slurm. |
| `image` | no | string | `null` | *(AWS Batch only)* Container image URI for this task. Overrides the backend's `default_image` when scripthut auto-registers a job definition. Ignored when the backend has `job_definition` set (Batch locks the image to the registered definition; scripthut logs a warning on mismatch). Ignored by Slurm and PBS. |
| `deps` | no | array | `[]` | List of task IDs this task depends on. Supports wildcard patterns. See [Dependencies](#dependencies). |
| `output_file` | no | string | auto | Custom path for stdout log (Slurm/PBS only). If not set, defaults to `<log_dir>/scripthut_<run_id>_<task_id>.out`. Ignored on AWS Batch — logs are routed to CloudWatch. |
| `error_file` | no | string | auto | Custom path for stderr log (Slurm/PBS only). If not set, defaults to `<log_dir>/scripthut_<run_id>_<task_id>.err`. Ignored on AWS Batch — stderr is merged into the CloudWatch stream. |
| `env` | no | array | `[]` | Task-level env rules. Each entry is an `EnvRule` with optional `if`, `set`, `append`, `init`, and `include` fields. Resolved together with backend/server/workflow rules — see [Environment Variables](#environment-variables) and [the env model](configuration.md#environments). |
| `generates_source` | no | string | `null` | Path to a JSON file this task creates on the backend containing additional tasks. See [Dynamic Task Generation](#dynamic-task-generation). |

### Minimal Example

The only required fields are `id`, `name`, and `command`:

```json
{
  "tasks": [
    {
      "id": "hello",
      "name": "Hello World",
      "command": "echo 'Hello from ScriptHut!'"
    }
  ]
}
```

This task will run with default resources: 1 CPU, 4G memory, 1 hour time limit, in the `normal` partition.

### Full Example

```json
{
  "tasks": [
    {
      "id": "train-model-v1",
      "name": "Train Model v1",
      "command": "python train.py --config config.yaml --seed 42",
      "working_dir": "/home/user/project",
      "partition": "gpu",
      "cpus": 4,
      "memory": "16G",
      "time_limit": "4:00:00",
      "output_file": "/scratch/user/logs/train-v1.out",
      "error_file": "/scratch/user/logs/train-v1.err",
      "env": [
        {"set": {"MODEL_NAME": "resnet50", "DATA_DIR": "/scratch/data/imagenet"}},
        {"if": {"SCRIPTHUT_BACKEND": "hpc-cluster"}, "init": "module load cuda/12"}
      ]
    }
  ]
}
```

---

## Dependencies

Tasks can declare dependencies on other tasks using the `deps` field. A task will not be submitted to the scheduler until all of its dependencies have completed successfully.

### Basic Dependencies

Reference other tasks by their `id`:

```json
{
  "tasks": [
    {
      "id": "download",
      "name": "Download Data",
      "command": "wget https://example.com/data.tar.gz"
    },
    {
      "id": "extract",
      "name": "Extract Data",
      "command": "tar xzf data.tar.gz",
      "deps": ["download"]
    },
    {
      "id": "process",
      "name": "Process Data",
      "command": "python process.py",
      "deps": ["extract"]
    }
  ]
}
```

In this example, `extract` waits for `download` to complete, and `process` waits for `extract`.

### Wildcard Dependencies

Dependencies support glob-style wildcard patterns using `*`, `?`, and `[...]`:

| Pattern | Matches |
|---------|---------|
| `build.*` | `build.x`, `build.y`, `build.z`, etc. |
| `step.?` | `step.a`, `step.1`, etc. (single character) |
| `data.[ab]` | `data.a`, `data.b` |

Wildcard patterns are expanded at run creation time against all task IDs in the same run. A wildcard that matches no tasks will cause an error.

### Diamond Pattern Example

This is a common pattern where multiple tasks fan out from a single setup step and then converge:

```json
{
  "tasks": [
    {
      "id": "setup.init",
      "name": "Setup",
      "command": "bash setup.sh"
    },
    {
      "id": "build.x",
      "name": "Build X",
      "command": "make build-x",
      "deps": ["setup.init"]
    },
    {
      "id": "build.y",
      "name": "Build Y",
      "command": "make build-y",
      "deps": ["setup.init"]
    },
    {
      "id": "final.merge",
      "name": "Merge Results",
      "command": "python merge.py",
      "deps": ["build.*"]
    }
  ]
}
```

The dependency graph looks like:

```
      setup.init
       /      \
  build.x    build.y
       \      /
     final.merge
```

- `setup.init` runs first (no dependencies)
- `build.x` and `build.y` run in parallel after `setup.init` completes
- `final.merge` waits for all tasks matching `build.*` — both `build.x` and `build.y`

### Dot-Notation Task IDs

Using dots in task IDs (e.g., `setup.init`, `build.x`) enables:

1. **Hierarchical display** in the ScriptHut UI — tasks are grouped by their prefix
2. **Wildcard matching** — `build.*` naturally matches all tasks in the `build` group

This convention is optional but recommended for workflows with many tasks.

### Dependency Failure Propagation

When a task fails:

- All tasks that depend on it (directly or transitively) are marked as `dep_failed`
- `dep_failed` tasks are never submitted to the scheduler
- Other independent tasks in the run continue executing normally

### Validation

ScriptHut validates dependencies at run creation time:

- **Missing references**: A dependency on a non-existent task ID raises an error
- **Self-dependencies**: A task cannot depend on itself
- **Circular dependencies**: Detected via DFS cycle detection. For example, `A → B → C → A` raises an error with the cycle path

---

## Dynamic Task Generation

The `generates_source` field enables a task to dynamically create additional tasks at runtime. When a task with `generates_source` completes successfully, ScriptHut reads the specified JSON file from the backend and appends the new tasks to the current run.

### How It Works

1. A task declares `generates_source` pointing to a file path on the backend
2. The task runs and writes a JSON file at that path (same format as the top-level task JSON)
3. When ScriptHut detects the task has completed, it reads the file via SSH
4. The new tasks are validated, their dependencies are resolved, and they are appended to the run
5. The new tasks can depend on existing tasks or on each other

### Example: Two-Phase Workflow

**Phase 1** — A planning task that determines what simulations to run:

```json
{
  "tasks": [
    {
      "id": "plan",
      "name": "Plan Simulations",
      "command": "python plan.py --output /scratch/user/tasks.json",
      "generates_source": "/scratch/user/tasks.json"
    }
  ]
}
```

**Phase 2** — The `plan.py` script writes `/scratch/user/tasks.json`:

```json
{
  "tasks": [
    {
      "id": "sim.1",
      "name": "Simulation 1",
      "command": "python simulate.py --param 0.1",
      "deps": ["plan"]
    },
    {
      "id": "sim.2",
      "name": "Simulation 2",
      "command": "python simulate.py --param 0.5",
      "deps": ["plan"]
    },
    {
      "id": "aggregate",
      "name": "Aggregate Results",
      "command": "python aggregate.py",
      "deps": ["sim.*"]
    }
  ]
}
```

The dynamically generated tasks can:

- Depend on the generating task itself (e.g., `"deps": ["plan"]`)
- Depend on each other (e.g., `aggregate` depends on `sim.*`)
- Depend on any other task in the run
- Themselves use `generates_source` for multi-level dynamic generation

---

## Environment Variables

ScriptHut resolves a task's environment by walking an ordered chain of **env rules** from four layers — **backend → server → workflow → task** — against a seed of `SCRIPTHUT_*` runtime variables. For the full model (rule shape, conditionals, `${name}` expansion, reusable groups, `SCRIPTHUT_*` protection), see [Environments](configuration.md#environments) in the configuration reference. This page covers only the **task-level** `env:` field that a generator can emit.

### Automatic seed variables

Every task starts with these variables already set (any task-level rule's `if:` can branch on them):

| Variable | Description |
|----------|-------------|
| `SCRIPTHUT_BACKEND` | Name of the backend this task runs on. |
| `SCRIPTHUT_WORKFLOW` | Name of the workflow that created this run. |
| `SCRIPTHUT_RUN_ID` | Unique identifier for this run. |
| `SCRIPTHUT_CREATED_AT` | ISO 8601 timestamp of when the run was created. |
| `SCRIPTHUT_GIT_REPO` | *(git workflows only)* Repository URL. |
| `SCRIPTHUT_GIT_BRANCH` | *(git workflows only)* Branch name. |
| `SCRIPTHUT_GIT_SHA` | *(git workflows only)* Resolved commit hash. |

These keys are protected: any rule attempting to `set:` or `append:` to a `SCRIPTHUT_` key is ignored with a warning.

### Task-level `env:` rules

A task's `env:` is a list of `EnvRule` entries. Each entry can `set:` variables, `append:` to PATH-like variables (joined with `:`), `init:` bash lines that run before the task command, and `include:` named env-groups defined at any earlier layer. An optional `if:` guards the rule against the env-so-far.

```json
{
  "id": "train",
  "name": "Train Model",
  "command": "python train.py",
  "env": [
    {"set": {"LEARNING_RATE": "0.001", "BATCH_SIZE": "64"}},
    {"if": {"SCRIPTHUT_BACKEND": "mercury"}, "init": "module load cuda/11"},
    {"include": ["monitoring"]}
  ]
}
```

`${name}` substitution inside values references the env as resolved so far (seed + earlier layers + earlier rules in this list):

```json
{
  "env": [
    {"set": {"RUN_DIR": "/scratch/${USER}/${SCRIPTHUT_RUN_ID}"}}
  ]
}
```

### Full workflow example

This is what a generator's complete stdout looks like — the same top-level `{"tasks": [...]}` document, with several tasks demonstrating different env-rule patterns:

```json
{
  "tasks": [
    {
      "id": "prep",
      "name": "Prepare data",
      "command": "python prep.py --out ${DATA_DIR}/run.parquet",
      "cpus": 2,
      "memory": "8G",
      "env": [
        {"set": {"DATA_DIR": "/scratch/${USER}/${SCRIPTHUT_RUN_ID}"}}
      ]
    },
    {
      "id": "train.gpu",
      "name": "Train (GPU)",
      "command": "python train.py --data ${DATA_DIR}/run.parquet --seed ${SEED}",
      "deps": ["prep"],
      "cpus": 4,
      "memory": "32G",
      "gres": "gpu:1",
      "env": [
        {"set": {
          "DATA_DIR": "/scratch/${USER}/${SCRIPTHUT_RUN_ID}",
          "SEED": "42"
        }},
        {"if": {"SCRIPTHUT_BACKEND": "mercury"},
         "init": "module load cuda/11"},
        {"if": {"SCRIPTHUT_BACKEND": "anvil"},
         "init": "module load cuda-toolkit"},
        {"include": ["wandb"]},
        {"append": {"PATH": "/opt/cuda/bin"}}
      ]
    },
    {
      "id": "report",
      "name": "Render report",
      "command": "python report.py --data ${DATA_DIR}/run.parquet",
      "deps": ["train.gpu"],
      "env": [
        {"set": {"DATA_DIR": "/scratch/${USER}/${SCRIPTHUT_RUN_ID}"}}
      ]
    }
  ]
}
```

Points worth noting:

- **`set:` cascades within a single task's `env:`** — `DATA_DIR` is written in the first rule, then referenced via `${DATA_DIR}` in the task's `command`. The `command` itself is *not* expanded by the resolver; rather, the generated submission script exports `DATA_DIR=...` before running the command so the shell does the substitution.
- **`include:` resolves against env_groups defined upstream** — `wandb` here would be defined at the workflow level (in `scripthut.yaml`) or at the server level. The task doesn't define groups itself; it only references them.
- **`if:` rules guard their entire block** — when neither `mercury` nor `anvil` matches `SCRIPTHUT_BACKEND` (e.g. running on a laptop), neither `module load` line is added. The `append: { PATH: /opt/cuda/bin }` after them is unconditional; if you only want it on GPU clusters, wrap it in an `if:` as well or move it inside an `env_group` whose include is guarded.
- **Repeating values across tasks** — if many tasks share `DATA_DIR: /scratch/${USER}/${SCRIPTHUT_RUN_ID}`, lift it to the workflow's `env:` in `scripthut.yaml` instead of repeating it in every task. Anything not specific to a single task belongs upstream.

### Resolution order — later rules win

Rules from each layer are concatenated and evaluated top to bottom: backend rules, then server, then workflow, then task. `set:` overwrites; `append:` extends. So if the workflow sets `DATA_DIR=/shared` and the task sets `DATA_DIR=/scratch/local`, the task wins. The Env tab on the task detail page in the UI shows the resolved env with per-key provenance (which layer / which group wrote each value) — use it to debug surprising values. The same data is exposed at `GET /runs/{run_id}/tasks/{task_id}/env`.

### Legacy fields (removed)

The earlier `environment:` (named-bundle reference) and `env_vars:` (per-task variable dict) fields are no longer accepted. Tasks emitting either field will fail at parse time with a clear migration message. Replace them with `env:` rule lists — a single `{"set": {...}}` rule reproduces the old `env_vars` behavior; a workflow-level `env_groups:` block plus `{"include": ["..."]}` reproduces the old named bundles.

---

## Writing a Task Generator

A task generator is any executable (script, binary, etc.) that prints valid JSON to stdout. Here are examples in different languages.

### Python

```python
#!/usr/bin/env python3
"""Generate tasks for ScriptHut."""

import argparse
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--partition", default="normal")
    args = parser.parse_args()

    tasks = []
    for i in range(1, args.count + 1):
        tasks.append({
            "id": f"task-{i:03d}",
            "name": f"Task {i}",
            "command": f"python process.py --index {i}",
            "partition": args.partition,
            "cpus": 2,
            "memory": "8G",
            "time_limit": "2:00:00",
        })

    print(json.dumps({"tasks": tasks}, indent=2))

if __name__ == "__main__":
    main()
```

Configure in `scripthut.yaml`:

```yaml
workflows:
  - name: batch-processing
    backend: hpc-cluster
    command: "python /path/to/generate_tasks.py --count 20 --partition gpu"
    max_concurrent: 5
    description: "Batch processing pipeline"
```

### Bash

```bash
#!/bin/bash
# Generate tasks as JSON using heredoc

cat <<'EOF'
{
  "tasks": [
    {
      "id": "step-1",
      "name": "Download",
      "command": "wget https://example.com/data.csv",
      "time_limit": "00:30:00"
    },
    {
      "id": "step-2",
      "name": "Process",
      "command": "python process.py data.csv",
      "deps": ["step-1"],
      "cpus": 4,
      "memory": "16G"
    }
  ]
}
EOF
```

### Static JSON File

The simplest approach — just `cat` a pre-existing JSON file:

```yaml
workflows:
  - name: static-tasks
    backend: hpc-cluster
    command: "cat /shared/tasks/my_pipeline.json"
    description: "Run predefined pipeline"
```

### Julia

```julia
#!/usr/bin/env julia
using JSON

tasks = [
    Dict(
        "id" => "sim-$i",
        "name" => "Simulation $i",
        "command" => "julia run_sim.jl --seed $i",
        "partition" => "normal",
        "cpus" => 4,
        "memory" => "8G",
        "time_limit" => "6:00:00",
        "env" => [Dict("include" => ["julia-1.10"])]
    )
    for i in 1:10
]

println(JSON.json(Dict("tasks" => tasks), 2))
```

### R

```r
#!/usr/bin/env Rscript
library(jsonlite)

tasks <- lapply(1:10, function(i) {
  list(
    id = sprintf("analysis-%03d", i),
    name = sprintf("Analysis %d", i),
    command = sprintf("Rscript run_analysis.R --chunk %d", i),
    partition = "normal",
    cpus = 1,
    memory = "4G",
    time_limit = "1:00:00"
  )
})

cat(toJSON(list(tasks = tasks), auto_unbox = TRUE, pretty = TRUE))
```

---

## Task Lifecycle

Once tasks are submitted via a workflow, they go through the following states:

```
PENDING ──→ SUBMITTED ──→ RUNNING ──→ COMPLETED
                │              │
                │              └──→ FAILED
                └──→ FAILED

          DEP_FAILED (dependency failed, never submitted)
```

| Status | Description |
|--------|-------------|
| `pending` | Task is waiting to be submitted. Either waiting for dependencies or for a concurrency slot. |
| `submitted` | Task has been submitted to the scheduler (Slurm/PBS) and is queued. |
| `running` | Task is actively executing on the backend. |
| `completed` | Task finished successfully (exit code 0). |
| `failed` | Task failed (non-zero exit, scheduler error, or cancelled). |
| `dep_failed` | Task was never submitted because a dependency failed. |

ScriptHut respects the `max_concurrent` limits at both the workflow level and the backend level. Tasks remain in `pending` state until a slot is available and all dependencies are satisfied.
