# Environment Variables

ScriptHut resolves a task's environment by walking an ordered chain of **env rules**: **backend → server → workflow (config) → workflow JSON document → task** — against a seed of `SCRIPTHUT_*` runtime variables. For the full model (rule shape, conditionals, `${name}` expansion, reusable groups, `SCRIPTHUT_*` protection), see [Environments](../configuration/environments.md) in the configuration reference. This page covers the two places env rules can appear inside the **workflow JSON document itself**: at the top level (`"env"` / `"env_groups"`), and per task.

## Automatic seed variables

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

A workflow that declares [data dependencies](../configuration/data.md) also gets `DATA_<NAME>` per dataset, plus `DATA_DIR` when it uses exactly one. Those are *not* protected — `DATA_DIR` is injected first so your own rule can override it, while `DATA_<NAME>` is injected last and wins — but treat the `DATA_` prefix as reserved and pick another name for unrelated variables, or a later dataset will silently shadow yours.

## Document-level `env:` and `env_groups:`

The workflow JSON itself can carry an `env:` rule list and an `env_groups:` dictionary at the top level — alongside `tasks:`. Use this for **project** knobs (thread counts, `WANDB_PROJECT`). Cluster dialect (`module load`) belongs in the **backend's** `env_groups`, referenced here only by name.

```json
{
  "title": "Grid over 2 params",
  "env": [
    {"include": ["julia-1.12"]}
  ],
  "tasks": [
    {"id": "prepare",  "name": "Prepare",  "command": "julia --project -e 'using Pkg; Pkg.instantiate()'"},
    {"id": "generate", "name": "Generate", "command": "python3 generate_tasks.py > tasks.json",
     "deps": ["prepare"], "generates_source": "tasks.json"}
  ]
}
```

`julia-1.12` is either a **stack** (`"stacks": ["julia-1.12"]`) ScriptHut built, or an env group mapped on each backend. Do not put `module load julia/…` in this file.


In the resolver chain, document-level rules sit between the workflow-config layer (from `scripthut.yaml`) and the task layer:

```
backend → server → repo / workflow-doc → task
```

A `generates_source` child JSON can also carry its own top-level `env:` and `env_groups:`. New env rules append to the run's existing list; new groups merge in (later definitions shadow earlier). So a generator can dynamically add env config alongside the dynamic tasks it produces.

## Task-level `env:` rules

A task's `env:` is a list of `EnvRule` entries. Each entry can `set:` variables, `append:` to PATH-like variables (joined with `:`), `init:` bash lines that run before the task command, and `include:` named env-groups defined at any earlier layer. An optional `if:` guards the rule against the env-so-far.

```json
{
  "id": "train",
  "name": "Train Model",
  "command": "python train.py",
  "env": [
    {"set": {"LEARNING_RATE": "0.001", "BATCH_SIZE": "64"}},
    {"include": ["cuda"]},
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

## Full workflow example

This is what a generator's complete stdout looks like — the same top-level `{"tasks": [...]}` document, with several tasks demonstrating different env-rule patterns:

```json
{
  "tasks": [
    {
      "id": "prep",
      "name": "Prepare data",
      "command": "python prep.py --out ${WORK_DIR}/run.parquet",
      "cpus": 2,
      "memory": "8G",
      "env": [
        {"set": {"WORK_DIR": "/scratch/${USER}/${SCRIPTHUT_RUN_ID}"}}
      ]
    },
    {
      "id": "train.gpu",
      "name": "Train (GPU)",
      "command": "python train.py --data ${WORK_DIR}/run.parquet --seed ${SEED}",
      "deps": ["prep"],
      "cpus": 4,
      "memory": "32G",
      "gres": "gpu:1",
      "env": [
        {"set": {
          "WORK_DIR": "/scratch/${USER}/${SCRIPTHUT_RUN_ID}",
          "SEED": "42"
        }},
        {"include": ["cuda"]},
        {"include": ["wandb"]},
        {"append": {"PATH": "/opt/cuda/bin"}}
      ]
    },
    {
      "id": "report",
      "name": "Render report",
      "command": "python report.py --data ${WORK_DIR}/run.parquet",
      "deps": ["train.gpu"],
      "env": [
        {"set": {"WORK_DIR": "/scratch/${USER}/${SCRIPTHUT_RUN_ID}"}}
      ]
    }
  ]
}
```

Points worth noting:

- **`set:` cascades within a single task's `env:`** — `WORK_DIR` is written in the first rule, then referenced via `${WORK_DIR}` in the task's `command`. The `command` itself is *not* expanded by the resolver; rather, the generated submission script exports `WORK_DIR=...` before running the command so the shell does the substitution.
- **`include:` resolves against env_groups defined upstream** — `cuda` is mapped per backend (`module load cuda/11` on Mercury, empty list where the toolkit is already on PATH). `wandb` can be a server- or project-level group. Unknown names fail the submit unless this rule's `if:` does not match.
- **`if:` guards the whole rule** — use it for optional extras, not to hide a missing cluster map. Prefer a per-backend `env_groups` entry (including `[]` for no-op).
- **Repeating values across tasks** — if many tasks share `WORK_DIR: /scratch/${USER}/${SCRIPTHUT_RUN_ID}`, lift it to the document-level `"env"` instead of repeating it in every task. Anything not specific to a single task belongs upstream.

## Resolution order — later rules win

Rules from each layer are concatenated and evaluated top to bottom: backend, server, repo/document, then task. `set:` overwrites; `append:` extends. So if the document sets `WORK_DIR=/shared` and the task sets `WORK_DIR=/scratch/local`, the task wins. The Env tab on the task detail page in the UI shows the resolved env with per-key provenance (which layer / which group wrote each value) — use it to debug surprising values. The same data is exposed at `GET /runs/{run_id}/tasks/{task_id}/env`.

## Legacy fields (removed)

The earlier `environment:` (named-bundle reference) and `env_vars:` (per-task variable dict) fields are no longer accepted. Tasks emitting either field will fail at parse time with a clear migration message. Replace them with `env:` rule lists — a single `{"set": {...}}` rule reproduces the old `env_vars` behavior; a workflow-level `env_groups:` block plus `{"include": ["..."]}` reproduces the old named bundles.
