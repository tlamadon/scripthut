# Task JSON Format

Workflows in ScriptHut work by executing a command on the remote backend via SSH. That command must print a JSON document to **stdout** describing the tasks to submit. This page covers the expected JSON structure, all available task fields, dependencies, dynamic task generation, and environment variables.

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
| `deps` | no | array | `[]` | List of task IDs this task depends on. Supports wildcard patterns. See [Dependencies](#dependencies). |
| `output_file` | no | string | auto | Custom path for stdout log. If not set, defaults to `<log_dir>/scripthut_<run_id>_<task_id>.out`. |
| `error_file` | no | string | auto | Custom path for stderr log. If not set, defaults to `<log_dir>/scripthut_<run_id>_<task_id>.err`. |
| `environment` | no | string | `null` | Name of a named environment from the [backend configuration](configuration.md#environments). |
| `env_vars` | no | object | `{}` | Per-task environment variables as key-value pairs. |
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
      "environment": "python-ml",
      "env_vars": {
        "MODEL_NAME": "resnet50",
        "DATA_DIR": "/scratch/data/imagenet"
      }
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

Tasks can receive environment variables from three sources, which are merged in a defined priority order.

### Automatic Environment Variables

ScriptHut automatically injects the following variables into every task:

| Variable | Description |
|----------|-------------|
| `SCRIPTHUT_WORKFLOW` | Name of the workflow that created this run. |
| `SCRIPTHUT_RUN_ID` | Unique identifier for this run. |
| `SCRIPTHUT_CREATED_AT` | ISO 8601 timestamp of when the run was created. |
| `SCRIPTHUT_GIT_REPO` | *(git workflows only)* Repository URL. |
| `SCRIPTHUT_GIT_BRANCH` | *(git workflows only)* Branch name. |
| `SCRIPTHUT_GIT_SHA` | *(git workflows only)* Commit hash. |

These variables are always available and cannot be overridden.

### Named Environments

Tasks can reference a named environment from the [backend configuration](configuration.md#environments) using the `environment` field:

```json
{
  "id": "train",
  "name": "Train Model",
  "command": "julia train.jl",
  "environment": "julia"
}
```

If the backend's environment `julia` is defined in `scripthut.yaml` as:

```yaml
backends:
  - name: mercury
    type: slurm
    ssh: { ... }
    environments:
      - name: julia
        variables:
          JULIA_DEPOT_PATH: "/scratch/user/julia_depot"
          JULIA_NUM_THREADS: "8"
        extra_init: "module load julia/1.10"
```

Then the task will have `JULIA_DEPOT_PATH` and `JULIA_NUM_THREADS` exported, and `module load julia/1.10` will be run before the task command. The same task JSON works across different backends — each backend resolves the environment name to its own cluster-specific modules.

### Per-Task Environment Variables

Individual tasks can set environment variables using `env_vars`:

```json
{
  "id": "train",
  "name": "Train Model",
  "command": "python train.py",
  "environment": "python-ml",
  "env_vars": {
    "LEARNING_RATE": "0.001",
    "BATCH_SIZE": "64"
  }
}
```

### Environment Variable Priority

When the same variable is defined at multiple levels, later sources override earlier ones:

1. **ScriptHut automatic variables** (lowest priority)
2. **Named environment variables** (from the backend's `environments` config)
3. **Per-task `env_vars`** (highest priority)

For example, if a named environment sets `DATA_DIR=/shared/data` and the task sets `"env_vars": {"DATA_DIR": "/scratch/local"}`, the task will see `DATA_DIR=/scratch/local`.

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
        "environment" => "julia"
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
