# Workflows and Sources

These two sections of `scripthut.yaml` configure *task generators* — mechanisms for telling ScriptHut "here's a way to get a list of tasks to run".

- **Workflows** — a fixed SSH command (optionally inside a cloned git repo) that prints task JSON
- **Sources** — a git repo or backend filesystem path containing one or more workflow JSON files, discovered via glob

> The legacy `projects:` section was removed in scripthut 0.6.0. Convert any project entry to an equivalent `sources:` entry (type `path` for a directory on a backend, type `git` for a remote repo).

See [Task JSON Format](../task-json/index.md) for the JSON shape every generator must emit.

---

## Workflows

Workflows are the primary mechanism for submitting batch jobs. A workflow defines an SSH command that runs on a backend and returns a JSON list of tasks.

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
| `env` | list | `[]` | Workflow-level env rules applied to every task in the workflow. See [Environments](environments.md). |
| `env_groups` | object | `{}` | Named, reusable env-rule lists local to this workflow (also visible to its tasks). |

The workflow *document* the command prints may also carry a top-level `data` list naming [datasets](data.md) to stage before any task runs.

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
- Git metadata is injected as environment variables into every task (see [Environments → `SCRIPTHUT_*` runtime seed](environments.md#scripthut_-runtime-seed)).

---

## Sources

Sources are git repositories, backend filesystem paths, or a laptop working tree (`type: sync`) containing workflow definitions. ScriptHut discovers workflow JSON files using the `workflows_glob` pattern (default: `.hut/workflows/*.json`). You can use glob wildcards like `**/*.hut.json` to match files recursively across any subdirectory. Each matched JSON file appears as a triggerable workflow on the Sources page.

For **git sources**, the repository is cloned locally for workflow discovery, and also cloned on the backend when a workflow is triggered (tasks run inside the cloned directory, just like git-based workflows).

For **path sources**, workflows are discovered via SSH on the backend, and tasks run with `working_dir` resolved relative to the source path.

For **sync sources**, workflows are discovered on the laptop working tree. On submit, git-tracked files are copied to the backend; when the run finishes, `output/` is pulled back. See [Sync Source](#sync-source).

### Git Source

```yaml
sources:
  - name: ml-jobs
    type: git
    url: git@github.com:your-org/ml-pipelines.git
    branch: main
    deploy_key: ~/.ssh/ml-jobs-deploy-key
    backend: hpc-cluster
    # workflows_glob: "**/*.hut.json"  # default: .hut/workflows/*.json
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
| `workflows_glob` | string | `".hut/workflows/*.json"` | Glob pattern to find workflow JSON files (supports `**` for recursive matching). |
| `clone_dir` | string | `"~/scripthut-repos"` | Parent directory on the backend. The repo is cloned into `<clone_dir>/<commit_hash>/`. |
| `postclone` | string | `null` | Shell command to run in the clone directory after cloning. |

### Path Source

```yaml
sources:
  - name: shared-workflows
    type: path
    path: /shared/project-workflows
    backend: hpc-cluster
    # workflows_glob: "**/*.hut.json"  # default: .hut/workflows/*.json
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this source. |
| `type` | string | **required** | Must be `"path"`. |
| `path` | string | **required** | Directory on the backend filesystem. |
| `backend` | string | **required** | Backend where this path exists and where tasks are submitted. |
| `workflows_glob` | string | `".hut/workflows/*.json"` | Glob pattern to find workflow JSON files (supports `**` for recursive matching). |

### Sync Source

A laptop git working tree copied to the backend on submit. Use this when you edit on a laptop and run on HPC: tracked dirty files go up, jobs run at `dest`, `output/` comes back when the run finishes (success, failure, or cancel). Not a git remote and not Unison — two one-way copies of disjoint paths. Datasets (`data:` / `$DATA_*`) are unchanged.

```yaml
sources:
  - name: wl-hcpu
    type: sync
    path: ~/Documents/GitHub/wl_hcpu   # laptop
    backend: hpc-cluster
    # dest: /scratch/you/wl_hcpu       # default: <backend.sync_dir>/<name>/
    # return: output                   # cluster → laptop; excluded from upload
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this source. |
| `type` | string | **required** | Must be `"sync"`. |
| `path` | path | **required** | Git repository on the scripthut host. Relative paths resolve against the config file's directory. |
| `backend` | string | **required** | Backend whose filesystem receives the copy and runs the jobs. SSH backends only. |
| `dest` | string | `<sync_dir>/<name>/` | Directory on the backend to copy into. Must not sit under `clone_dir` or `dataset_dir`, or equal the laptop `path`. |
| `return` | string | `"output"` | Relative directory pulled cluster → laptop after the run. Excluded from the upload even if git-tracked. Must stay inside `path`: leading/trailing `/` are stripped and `..` is refused. |
| `workflows_glob` | string | `".hut/workflows/*.json"` | Glob pattern to find workflow JSON files on the laptop working tree. |
| `timeout` | int | `86400` | Wall-clock seconds for the upload and for the pull, each. |

On submit ScriptHut copies `git ls-files` working-tree bytes (dirty tracked files yes; untracked and gitignored no; `.git` not copied). Tracked symlinks are refused. Root tasks wait on `_sync.upload`; `_sync.return` starts when every other current item is terminal, so a failed task still pulls. `run watch` waits for the pull. A second submit to the same dest is refused while another run is still active.

The two directions are deliberately asymmetric:

- **Upload replaces `dest`.** The tree is staged beside it, verified against the local file sizes, then moved into place — so a file you delete locally is gone from the backend on the next run, and a half-finished transfer is never left looking complete.
- **The pull only overwrites and adds. It never deletes.** Local leftovers in `output/` that the cluster did not write stay, and are yours to remove. This is not tidiness lost: a failed listing, a partial remote walk, and a run that simply produced no output all look identical to the puller, so pruning on that signal would delete your own files.

!!! warning "The default dest is home, which has a quota"
    `~/scripthut-sync/<name>/` mirrors `clone_dir` and `dataset_dir`. Point the backend's `sync_dir` (or the source's `dest`) at a larger filesystem before copying anything big — often `/scratch/<user>`, though not every cluster provides one.

`scripthut disk scan` lists each dest under **Sync working copies**. A dest still named in config is live even with no remembered run; a leftover directory under `sync_dir` from a source you deleted shows as orphaned. `disk clean` will not delete either — these are working copies, not hashed clones.

Cache identity is **not** the upload tree. Leave `commit_hash` unset (like a path source). Skip vs rerun is per-task: `cache: false`, or `cache_scope: "inputs"` listing the script.

