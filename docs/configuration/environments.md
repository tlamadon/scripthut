# Environments

ScriptHut resolves the environment for each task by walking an ordered chain of **env rules** from five layers — **backend → server → workflow (config) → workflow (document) → task** — against a seed of `SCRIPTHUT_*` runtime variables. Every layer contributes rules to the same list; later rules see earlier rules' effects, so conditionals can branch on any value set upstream (including the seed).

## The `EnvRule` shape

Every entry in any `env:` list is an `EnvRule`:

```yaml
env:
  - set:                              # always-applied
      PROJECT: training
      LOG_LEVEL: info

  - if:                               # optional guard
      SCRIPTHUT_BACKEND: mercury      # single value = equality
    set:
      SCRATCH: /scratch/${USER}       # ${name} expanded against env-so-far
    init: "module load gcc/12 cuda/11"

  - if:
      SCRIPTHUT_BACKEND: [anvil, delta]   # list value = OR
    set:
      SCRATCH: /tmp/work/${USER}
    init: "module load gcc cuda-toolkit"

  - if:
      SCRIPTHUT_BACKEND: mercury
      GPU: "1"                        # multiple keys = AND
    append:
      PATH: /opt/cuda/bin             # joined with ":" to existing value
```

| Field | Type | Description |
|-------|------|-------------|
| `if` | object | Optional guard. Each key must match the env-so-far. A list value matches if the actual value is in the list (OR). When all keys match, the rule applies. |
| `set` | object | Variables to write. Overwrites any prior value. `${name}` is expanded against env-so-far. |
| `append` | object | Variables to extend. Joined to the existing value with `:` (creates the value if absent). `${name}` is expanded. |
| `init` | string | Bash text appended (newline-joined) into the `extra_init` block. Runs **before** `set:`/`append:` exports in the generated script, so user-set vars override anything `module load` / `source` placed into the env. Followed by `cd <working_dir>` and the task command. `${name}` is expanded. |
| `include` | list of strings | Names of `env_groups` to inline at this position. See [Reusable groups](#reusable-groups). |

## Where rules live

`env:` is a list of rules accepted at five locations, evaluated in this order:

| Layer | Defined in | Typical use |
|-------|------------|-------------|
| **Backend** | `env:` on each backend entry in `scripthut.yaml` | Cluster-specific facts — scratch paths, module-system bootstrap |
| **Server** | top-level `env:` in `scripthut.yaml` | Org-wide defaults across all clusters |
| **Workflow (config)** | `env:` on each workflow entry in `scripthut.yaml` | Workflow-specific overrides for ops-controlled config |
| **Workflow (document)** | top-level `env:` inside the JSON the generator emits | Project-controlled config — lives in your repo, ships with your code. See [Task JSON → Environment Variables](../task-json/environments.md) |
| **Task** | `env:` on each task in the JSON generator output | Per-task adjustments |

Rules from each layer are concatenated in that order, then evaluated top-to-bottom. There is no separate "merge" step — `set` overwrites and `append` extends, so layer X overrides layer X-1 simply by being written later.

## `SCRIPTHUT_*` runtime seed

Before any user rule runs, ScriptHut seeds the env with run-context variables. Any rule's `if:` can branch on these:

| Variable | Always present | Description |
|----------|----------------|-------------|
| `SCRIPTHUT_BACKEND` | yes | Name of the backend this task runs on. |
| `SCRIPTHUT_WORKFLOW` | yes | Name of the workflow. |
| `SCRIPTHUT_RUN_ID` | yes | Unique 8-char run identifier. |
| `SCRIPTHUT_CREATED_AT` | yes | ISO 8601 timestamp of run creation. |
| `SCRIPTHUT_GIT_REPO` | git workflows only | Repository URL. |
| `SCRIPTHUT_GIT_BRANCH` | git workflows only | Branch name. |
| `SCRIPTHUT_GIT_SHA` | git workflows only | Resolved commit hash. |

These keys are **protected**: any rule attempting to `set:` or `append:` to a key starting with `SCRIPTHUT_` is rejected with a warning and ignored.

## `${name}` expansion

String values in `set:`, `append:`, and `init:` may reference other variables with `${name}`. Expansion happens against the env as resolved *so far*, so it sees the seed plus everything earlier rules have written. Unknown names expand to an empty string and log a warning. Only the `${name}` form is interpolated — bare `$name` is left alone, so shell expressions written into `init:` are passed through unchanged.

## Reusable groups

Define a rule list once with `env_groups:` and inline it from any `env:` rule using `include:`:

```yaml
env_groups:
  monitoring:
    - set:
        OTEL_EXPORTER: otlp
        OTEL_ENDPOINT: "http://collector:4318"

backends:
  - name: mercury
    env_groups:
      gpu-stack:                      # mercury's flavor of "gpu-stack"
        - init: "module load gcc/12 cuda/11"
        - append: { PATH: /opt/cuda/bin }
    env:
      - include: [gpu-stack, monitoring]
```

`env_groups:` is accepted on each backend, on the server (top-level), on each workflow in `scripthut.yaml`, and on the workflow JSON document itself (top-level alongside `tasks:` — see [Task JSON → Environment Variables](../task-json/environments.md)). **Scoping**: a group defined at layer X is visible to that layer's own `env:` and to all later layers. Names defined at a later layer shadow earlier ones, so each backend can declare its own `gpu-stack` group and any workflow's `include: [gpu-stack]` resolves to the right one based on `SCRIPTHUT_BACKEND`.

Includes can be guarded — an `if:` clause on the include-rule is inherited by every inlined rule (ANDed with any guard the inlined rule already carries):

```yaml
env:
  - if:
      SCRIPTHUT_BACKEND: mercury
    include: [gpu-stack]              # only fires on mercury
```

Cycles between groups are detected and raise a clear error. Unknown group names log a warning and are skipped.

## Inspecting the resolved env

The task detail page in the UI has an **Env** tab that shows the resolved env for the selected task, with **per-key provenance** — every `set` / `append` operation lists its source layer (and which group, if any) and contribution. The same information is available as JSON at:

```
GET /runs/{run_id}/tasks/{task_id}/env
```

Use this when a value isn't what you expect: the provenance pinpoints which layer wrote it.

## Worked example: cluster-specific module loads

The canonical case — "Mercury needs `module load gcc/12 cuda/11`, Anvil needs `module load gcc cuda-toolkit`, the laptop needs nothing":

```yaml
backends:
  - name: mercury
    type: slurm
    ssh: { host: mercury.example.edu, user: alice }
    env:
      - set: { SCRATCH: /scratch/${USER} }
      - init: "source /etc/profile.d/modules.sh"

  - name: anvil
    type: pbs
    ssh: { host: anvil.example.edu, user: alice }
    env:
      - set: { SCRATCH: /tmp/work/${USER} }

workflows:
  - name: train
    backend: mercury                  # this workflow targets mercury
    command: "python generate_tasks.py"
    env:
      - if: { SCRIPTHUT_BACKEND: mercury }
        init: "module load gcc/12 cuda/11"
      - if: { SCRIPTHUT_BACKEND: anvil }
        init: "module load gcc cuda-toolkit"
```

The same workflow definition, ported to a different backend (e.g. by adding a second `train-anvil` workflow that points to `anvil`), gets the right module loads automatically — no per-backend duplication of the workflow.
