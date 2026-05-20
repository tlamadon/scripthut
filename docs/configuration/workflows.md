# Workflows, Sources, and Projects

These three sections of `scripthut.yaml` all configure *task generators* — different mechanisms for telling ScriptHut "here's a way to get a list of tasks to run".

- **Workflows** — a fixed SSH command (optionally inside a cloned git repo) that prints task JSON
- **Sources** — a git repo or backend filesystem path containing one or more workflow JSON files, discovered via glob
- **Projects** — a git repo on the backend with `sflow.json` files in it

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

---

## Projects

Projects reference git repositories that already exist on the backend and contain `sflow.json` workflow definition files.

```yaml
projects:
  - name: my-project
    backend: hpc-cluster
    path: /home/user/my-project
    max_concurrent: 10
    description: "My research project"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Unique identifier for this project. |
| `backend` | string | **required** | Name of the backend where the project lives. |
| `path` | string | **required** | Path to the git repository on the backend. |
| `max_concurrent` | integer | `null` | Default max concurrent tasks per run. |
| `description` | string | `""` | Human-readable description. |
