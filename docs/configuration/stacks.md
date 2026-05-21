# Stacks

A **stack** is a reusable software environment (Python venv, Julia depot, Conda env, …) installed once on a backend and reused across many task runs. ScriptHut handles the bookkeeping: detecting when the stack needs to rebuild, where to put it on each backend, and how to expose it to tasks.

You write the install script. ScriptHut hashes its inputs, caches the result, and runs the script again only when something changes.

---

## When to use one

You probably want a stack when:

- A task's first step is "set up the runtime" (`pip install -r requirements.txt`, `julia --project=. -e 'Pkg.instantiate()'`, `conda env create -f env.yml`).
- The same runtime is needed by many tasks (a parameter sweep, a multi-step pipeline).
- You want collaborators to get the same environment without coordinating manual setup.

You don't need one if the task's runtime is already pre-built (a container image with everything baked in, or modules pre-installed on the cluster).

---

## Defining a stack

Stacks live under the top-level `stacks:` section of `scripthut.yaml`. They can be defined globally or in a project-local config (see [From a project](../from-a-project.md)).

```yaml
stacks:
  - name: julia-1.11
    backends: [mercury-nb, pythia-nb]   # empty = every SSH backend
    cache_dir: /scratch/me/stacks       # parent dir on the backend
    inputs:
      julia_version: "1.11.3"           # literal values; any change forces rebuild
    input_files:
      - Manifest.toml                   # contents hashed; resolved relative to this yaml
      - Project.toml
    prep: |
      module load julia/1.11.3
      mkdir -p ${STACK_DIR}/depot
      JULIA_DEPOT_PATH="${STACK_DIR}/depot:" \
        julia --project=. -e 'using Pkg; Pkg.instantiate()'
    init: |
      module load julia/1.11.3
      export JULIA_DEPOT_PATH="${STACK_DIR}/depot:"
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | required | Identifier referenced by CLI commands. |
| `backends` | list of strings | `[]` (all SSH backends) | Which backends this stack is available on. Non-SSH backends (Batch, EC2) are skipped silently for now. |
| `cache_dir` | string | `~/.cache/scripthut/stacks` | Parent directory on the backend. `~` is expanded **remotely**, so it refers to the backend user's `$HOME`. |
| `inputs` | dict | `{}` | Named literal values hashed into the stack identity. Use this for versions, flags, anything the install depends on that isn't a file. |
| `input_files` | list of paths | `[]` | Local files whose **contents** are hashed in. Relative paths are resolved against the directory of the YAML that declared the stack. |
| `prep` | string (bash) | `""` | One-time install script. Runs with `STACK_DIR` set to the cache directory; `set -euo pipefail` is applied so any failure aborts. |
| `init` | string (bash) | `""` | Per-task env layer exported on every task that uses this stack. Same `${STACK_DIR}` is available. |

---

## Lifecycle

A stack lives at `<cache_dir>/<name>/<hash>/` on the backend, where `<hash>` is a 12-character SHA-256 of `(name, prep, sorted inputs, input_files contents)`. A successful install leaves a `.ready` sentinel file inside; that's the marker ScriptHut uses to know it can skip the install.

```
~/.cache/scripthut/stacks/julia-1.11/
├── 9b2f1c7d4ae3/      ← old build (different Manifest.toml)
└── e6a08f3b15c2/      ← current build
    ├── .ready          ← sentinel; only written on success
    └── depot/          ← whatever `prep` populated
```

When you change any input (literal value, file contents, prep script, or stack name), the hash changes, the old directory becomes "stale", and the next install builds into a new directory next to it. Old hash directories don't auto-evict — you remove them with `scripthut stack delete <name>` (which clears every hash for that stack at once).

### States

| State | Meaning |
|-------|---------|
| `missing` | No directory at the current hash. Next install runs `prep` from scratch. |
| `installing` | The hash directory exists but no `.ready` sentinel — a previous `prep` started and didn't finish (most often because it failed). `scripthut stack install --rebuild` recovers. |
| `ready` | Directory + sentinel both present. Safe to reference from tasks. |

---

## CLI

All four commands compose with `--backend X` to scope to one backend; without it, they run against every backend the stack declares (or every SSH-based backend if the list is empty).

```bash
scripthut stack list                              # configured stacks (no SSH)
scripthut stack check [<name>] [--backend X]      # per-backend state table
scripthut stack install <name> [--backend X] [--rebuild]
scripthut stack delete <name> [--backend X]
```

### `list`

Prints the configured stacks and their declared backends. Doesn't touch any backend — pure config view.

```
NAME                 BACKENDS                       INPUTS
julia-1.11           mercury-nb,pythia-nb           julia_version=1.11.3
python-ml            (all SSH)                      python_version=3.12
```

### `check`

For each (stack × backend) pair, opens an SSH connection and probes for the cache directory and sentinel. Reports:

```
STACK                BACKEND              STATE        HASH           BUILT        SIZE     NOTE
julia-1.11           mercury-nb           ready        e6a08f3b15c2   2d ago       1.4G
julia-1.11           pythia-nb            missing      e6a08f3b15c2   -            -
python-ml            mercury-nb           installing   1ad3f7c92e58   -            -        prep exit 1: ...
```

`check` exits non-zero if any stack is missing or installing — handy as a CI gate before submitting work.

### `install`

Idempotent: a no-op if the stack is already `ready` at the current hash. Otherwise:

1. Wipes the hash directory if it's in `installing` state (or if `--rebuild` is passed).
2. Creates the hash directory.
3. Runs `prep` via `bash -s` with `STACK_DIR` exported and `set -euo pipefail` in effect.
4. Writes `.ready` only if `prep` exits 0.

Failed installs leave the directory in `installing` state so `check` reports it; recover with `install --rebuild`.

### `delete`

Removes the **whole `<cache_dir>/<name>/` directory**, not just the current hash. Use this to free disk on a backend or to force a clean reinstall.

```bash
scripthut stack delete julia-1.11 --backend mercury-nb
```

---

## Hashing in detail

The hash is computed from:

```
name=<stack name>
prep=<full prep script text>
inputs=<sorted key=value pairs>
files=<for each input_file: path + contents>
```

A few corollaries that matter in practice:

- **Whitespace in `prep` matters.** Reformatting the script changes the hash. Use comments to record intent rather than restructuring the script.
- **Sorting of `inputs` is stable.** YAML dict order doesn't affect the hash; you can reorder freely.
- **Missing input files hash to `<missing>`.** A typo in `input_files: [requirments.txt]` (sic) produces a stable but wrong hash; once you fix the typo, the hash changes and the stack reinstalls.
- **The hash is 12 hex chars (~48 bits of entropy)** — plenty for cache distinctness on a single backend, but not a cryptographic guarantee.

You can inspect the current hash for a stack without touching any backend by reading `scripthut stack check <name> --json`.

---

## Tying stacks to tasks (future)

Today the CLI verbs are the integration point: you `install` the stack manually before submitting work, and tasks reference its installed binaries via `${STACK_DIR}` or by re-`module load`ing the same modules in their own commands. A follow-up will let tasks declare `stack: julia-1.11` and have ScriptHut:

1. Lazily install the stack on the target backend if it's not ready.
2. Splice the stack's `init:` block into the task's resolved env (after backend env, before workflow env).
3. Expose `STACK_DIR` to the task automatically.

Until that lands, treat stacks as a managed cache you populate explicitly with `scripthut stack install`.

---

## What's not supported yet

- **EC2 / AWS Batch backends.** Stacks are filesystem-based for v1; a Docker-image variant keyed on the same hash is planned for these backends but not implemented.
- **Lazy auto-install at submission.** You currently install explicitly; the lazy path will come with task-stack binding.
- **Locks across concurrent installs.** If two `scripthut stack install` invocations race on the same hash, they may both write into the same directory. Practically rare — for now, run one install at a time per backend.
- **Garbage-collecting old hashes.** Only the current hash is kept fresh; old ones accumulate until you `delete`. A future GC command will prune everything except the current hash for each named stack.
