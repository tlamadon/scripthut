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

Sources are git repositories or backend filesystem paths containing workflow definitions. ScriptHut discovers workflow JSON files using the `workflows_glob` pattern (default: `.hut/workflows/*.json`). You can use glob wildcards like `**/*.hut.json` to match files recursively across any subdirectory. Each matched JSON file appears as a triggerable workflow on the Sources page.

For **git sources**, the repository is cloned locally for workflow discovery, and also cloned on the backend when a workflow is triggered (tasks run inside the cloned directory, just like git-based workflows).

For **git sources with `local_path`** — a repo already on the machine running ScriptHut — neither clone happens: workflows are read from that working tree, and the commit is pushed to the backend over the SSH connection ScriptHut already holds. No deploy key, no git remote, no network. See [Local git source](#local-git-source).

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
    # workflows_glob: "**/*.hut.json"  # default: .hut/workflows/*.json
    # clone_dir: ~/scripthut-repos     # default
    # postclone: "rm -rf large_files"  # optional
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this source. |
| `type` | string | **required** | Must be `"git"`. |
| `url` | string | **required** unless `local_path` is set | Git repository URL. SSH format recommended. |
| `branch` | string | `"main"` | Branch to track. |
| `deploy_key` | path | `null` | Path to deploy key for this repository. |
| `backend` | string | **required** | Backend to submit discovered workflow tasks to. |
| `workflows_glob` | string | `".hut/workflows/*.json"` | Glob pattern to find workflow JSON files (supports `**` for recursive matching). |
| `clone_dir` | string | `"~/scripthut-repos"` | Parent directory on the backend. The repo is cloned into `<clone_dir>/<commit_hash>/`. |
| `postclone` | string | `null` | Shell command to run in the clone directory after cloning. |

### Local Git Source

When the repo is already on the machine running ScriptHut, set `local_path` instead of (or alongside) `url`. ScriptHut then reads that working tree directly and pushes the commit to the backend itself:

```yaml
sources:
  - name: my-project
    type: git
    local_path: ~/git/my-project
    branch: main
    # url: git@github.com:me/my-project.git  # optional; only coding agents need it
    # clone_dir: ~/scripthut-repos
    # postclone: "uv sync"
```

`local_path` replaces `deploy_key` rather than complementing it — there is no remote to authenticate to. A source must declare `url` or `local_path` (or both); declaring neither is a config error.

**How the code reaches the backend.** On submit, ScriptHut resolves the branch tip locally, ensures a shared bare mirror at `<clone_dir>/.mirror.git` on the backend, pushes that commit to it as `refs/heads/sh-<commit>`, then clones that ref into `<clone_dir>/<commit>/` — the same content-addressed layout a `url` source uses, so re-running a commit reuses the existing directory and skips `postclone`. Only the first push carries history; later ones send just the new objects. The clone out of the mirror is local to the backend, so git hardlinks the objects and it costs almost no time or disk.

**What is read versus what runs.** Workflow JSON and the repo's own `scripthut.yaml` are read from the **working tree**, so edits take effect immediately without a commit. The code the backend runs is the **branch tip**. Those differ whenever the tree is dirty, so `scripthut source view <name>` and the Sources page warn when it is:

```
$ scripthut source view my-project
Source 'my-project' (type: git)
  local_path: /home/me/git/my-project  (pushed to the backend)
  url:    <none>
  branch: main
  warning: working tree has uncommitted changes; the backend runs committed HEAD (a1b2c3d)
```

Commit before submitting if you meant those changes to run. Files ignored by `.gitignore` never count as dirty.

**Supported backends.** SSH backends (`slurm`, `pbs`) and the `local` backend. AWS Batch and EC2 run containers that clone a URL themselves and have no route back to your machine's disk, so a `local_path`-only source is refused there with an error saying so — give the source a `url` to use those.

**Coding agents** still require a `url`: an agent commits and pushes a branch, and needs a remote to push it to.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `local_path` | path | `null` | Path to a git repo on the ScriptHut host. When set, workflows are read from this working tree and the resolved commit is pushed to the backend instead of cloned from `url`. |
| `url` | string | `""` | Optional once `local_path` is set. Still used for coding-agent runs and shown as run metadata. |

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

