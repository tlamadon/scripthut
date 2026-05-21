# Using ScriptHut from a project directory

ScriptHut is most useful when its CLI feels like a natural part of your project's tooling — you `cd` into a repo and `scripthut workflow run …` or `scripthut stack install …` Just Works, the same way `cargo build` or `npm test` do, without needing to drag the global infrastructure config along.

To get that behavior, ScriptHut supports **two layers of configuration** that compose:

1. A **user-global** `scripthut.yaml` — describes *your* infrastructure (backends, SSH keys, settings).
2. A **project-local** `scripthut.yaml` at your repo root — describes what *this project* runs (stacks, workflows, env rules).

Both layers are optional. If only one is present, ScriptHut behaves exactly like a single-file setup.

---

## TL;DR

```
~/.config/scripthut/scripthut.yaml      ← global: backends, settings, secrets
~/git/my-project/scripthut.yaml         ← project-local: stacks, workflows, env
~/git/my-project/$ scripthut stack install julia
                            ↑
        loads both, merges, knows mercury-nb backend, knows julia stack,
        resolves Manifest.toml relative to the project root, installs.
```

If you've used `pyproject.toml` + `pip config`, or `.envrc` + `~/.config/direnv/`, the model is the same: per-project intent layered on top of per-user infrastructure.

---

## Discovery rules

When the CLI starts (or the server boots), ScriptHut runs three steps:

1. **Find the user-global config.** Looks at `~/.config/scripthut/scripthut.yaml`, then `~/.scripthut.yaml`. The first hit wins, or `None` if neither exists.
2. **Find the project-local config.** Walks up from the current working directory looking for `scripthut.yaml` (then `scripthut.yml`). Stops at the first hit. If that file happens to be the same as the global one, it's ignored — we don't double-count it.
3. **Combine.** If both files exist, ScriptHut validates and merges them (rules below). If only one exists, that file is used directly. If neither exists, the CLI falls back to the legacy `.env` loader.

You can short-circuit discovery for one command with `--config <path>` — that loads just that file and skips the layering.

### What goes where

| Field | Global config | Project-local config |
|-------|:-------------:|:--------------------:|
| `backends` | yes | **no** |
| `sources` | yes | **no** |
| `settings` | yes | **no** |
| `pricing` | yes | **no** |
| `stacks` | yes | yes |
| `workflows` | yes | yes |
| `projects` | yes | yes |
| `env`, `env_groups` | yes | yes |

The four "no" rows are deliberate. They describe infrastructure that's specific to the **user's machine and identity** (SSH keys, server bindings, AWS profiles, the user's `filter_user` for the UI) — not portable facts about a project. If a project-local file tries to declare any of them, ScriptHut refuses to load it with an error pointing at the offending fields:

```
ConfigError: Project-local config '/home/me/repo/scripthut.yaml' contains
fields that belong in the user-global config
(~/.config/scripthut/scripthut.yaml): backends.

Move those sections to the global file and keep only stacks /
workflows / projects / env / env_groups in the project file.
```

### Merge semantics

When both files are present:

- **`stacks`, `workflows`, `projects`** — by-name override. A project-local entry with the same `name:` as a global one replaces it; new names extend the list.
- **`env_groups`** — dict-merge. Same name in both files? The project-local one wins.
- **`env`** — concatenated, global first, project second. Project rules can therefore react to the env that global rules have already set up.
- **`backends`, `sources`, `settings`, `pricing`** — taken entirely from the global file (the project file can't define them at all).

The merge happens once at load time and produces a single `ScriptHutConfig` that the rest of ScriptHut consumes uniformly. There's no runtime "where did this come from?" lookup later.

---

## Concrete example

### Global config

`~/.config/scripthut/scripthut.yaml` — created once when you set up your machine, then mostly forgotten:

```yaml
backends:
  - name: mercury-nb
    type: slurm
    account: pi-faculty
    login_shell: true
    partition_map:
      standard: cpu
      gpu: gpu-a100
    ssh:
      host: mercury.cluster.edu
      user: me
      key_path: ~/.ssh/id_ed25519

  - name: acropolis-tl
    type: pbs
    account: faculty
    queue: batch
    ssh:
      host: acropolis.cluster.edu
      user: me
      key_path: ~/.ssh/id_ed25519

settings:
  filter_user: me
  poll_interval: 30
  cli_server: "http://127.0.0.1:8082"  # if you run a local server

env:
  # Rules everything inherits — set once, used everywhere.
  - set:
      LANG: C.UTF-8
```

### Project-local config

`~/git/my-project/scripthut.yaml` — committed into the repo, shared with collaborators:

```yaml
stacks:
  - name: julia
    backends: [mercury-nb]          # only installable on Slurm-side
    cache_dir: /scratch/me/stacks   # cluster-scratch is faster than $HOME
    inputs:
      julia_version: "1.11.3"
    input_files:
      - Manifest.toml               # resolved relative to THIS file's dir
      - Project.toml
    prep: |
      module load julia/1.11.3
      mkdir -p ${STACK_DIR}/depot
      JULIA_DEPOT_PATH="${STACK_DIR}/depot:" \
        julia --project=. -e 'using Pkg; Pkg.instantiate()'
    init: |
      module load julia/1.11.3
      export JULIA_DEPOT_PATH="${STACK_DIR}/depot:"

workflows:
  - name: grid-search
    backend: mercury-nb
    command: "julia --project=. scripts/generate_tasks.jl"
    max_concurrent: 30

env_groups:
  julia-runtime:
    - include: []
    - init: "echo Running with $(julia --version)"
```

The project-local file is now self-describing: anyone who clones the repo and has a global `scripthut.yaml` with `mercury-nb` configured can run the workflows. The repo doesn't need to leak SSH keys or backend hostnames to do that.

---

## Typical session

```bash
cd ~/git/my-project

# See what stacks this project declares (uses merged config).
scripthut stack list

# Install the Julia stack on every backend it targets.
# scripthut walks up to find the project YAML, merges with global,
# uses Manifest.toml from the project root as a hash input.
scripthut stack install julia

# Verify it's ready on each backend before submitting work.
scripthut stack check julia

# Submit the workflow. The project's working tree at HEAD becomes
# the source the backend clones (you've configured a deploy key
# in the project YAML, or you push the branch first).
scripthut workflow run grid-search

# Outside the project? CLI falls back to global config only.
cd /tmp
scripthut stack list        # the julia stack isn't visible here
scripthut backend list      # but your backends still are
```

If you forget to `cd` into the project before running a CLI command, ScriptHut quietly uses just the global config — no error, no surprise. You'll see "stack 'julia' not found" and the missing context becomes obvious.

---

## Where files are resolved from

`Stack.input_files` (and any other path field in a YAML) is resolved **relative to the directory of the file that declared it**, not the process CWD. That means `input_files: [requirements.txt]` in a project-local config always refers to the project's `requirements.txt`, no matter where you invoke the CLI from. Absolute paths and `~`-prefixed paths pass through unchanged.

This is what makes the model work in practice: hash inputs travel with the project that owns them, and the CLI's CWD only matters for *finding* the project, not for interpreting its contents.

---

## Migrating an existing single-file setup

If you have one big `scripthut.yaml` in your project today (the common starting point), you don't need to change anything — single-file setups keep working.

When you're ready to split, the rule of thumb is:

- Move **`backends`, `settings`, `pricing`, `sources`** to `~/.config/scripthut/scripthut.yaml`. These describe *your* environment.
- Leave **`stacks`, `workflows`, `projects`, `env`, `env_groups`** in the project's `scripthut.yaml`. These describe *what the project does*.

After splitting, run `scripthut backend list` from inside the project — if it still sees your backends, the merge is working. If it lists nothing, the global file isn't being discovered (check `ls ~/.config/scripthut/scripthut.yaml`).

---

## Troubleshooting

- **"Project-local config contains fields that belong in the user-global config"** — see the rule table above. Move those fields to `~/.config/scripthut/scripthut.yaml` (or remove them).
- **`scripthut stack install julia` says "stack not found"** — you may be running from outside the project. Check `pwd`, then re-run from inside the project directory. The CLI doesn't currently log which files it's loading; an explicit `--config` flag confirms the file it's pointed at.
- **Stack input hash keeps changing** — `input_files` are resolved relative to the config file's directory. If you've moved the file or the inputs, the hash changes. `scripthut stack check <name>` prints the current hash; comparing it across runs tells you whether your inputs are actually stable.
- **Project-local file conflicts with global on a name** — that's by design: the project wins. If you want a project to *opt out* of a globally-defined stack, give it a different name locally instead of redefining the same one.
