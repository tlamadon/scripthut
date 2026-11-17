# Working from a project

Install ScriptFlow in the compute environment and configure a git source:

```yaml
sources:
  - name: example
    type: git
    local_path: /data/local-example
    branch: main
    clone_dir: /data/repos
    workdir: /data/example
    flow_dir: analysis
scriptflow:
  env_group: scriptflow
```

The backend's `scriptflow` environment group should activate the environment.
Use `url` and optional `deploy_key` instead of `local_path` for a remote repository.
The repository contains `analysis/sflow.py` with a function named `flow_analysis`.

| From the local machine | By hand on the cluster |
|---|---|
| Commit the desired code in `/data/local-example`. | Work in `/data/example`; push or reconcile local commits before requesting an automated update. |
| `scripthut scriptflow launch --source example --backend cluster --flow analysis --run-id example-1` | Obtain a compute allocation using your site's commands, start tmux, and activate the ScriptFlow environment. |
| ScriptHut delivers the exact commit and starts a detached controller batch job. | `cd /data/example/analysis`, then `scriptflow run analysis`. |
| `scripthut scriptflow discover --source example --backend cluster --register` finds runs from either route. | Outputs and `.sf/runs` stay in this same directory. |

At a branch tip the fixed checkout stays on that branch and advances by
fast-forward, with upstream tracking so a manual `git pull --ff-only` works.
For local sources the upstream is the backend mirror of delivered commits; pulling
there does not fetch new changes from the local machine until ScriptHut delivers
them. Private URL sources may require the user's own Git credentials for manual
pulls: the temporary deployment key is removed after delivery.

Preparation refuses modified tracked files, running ScriptFlow records (including
stale controllers), unreadable records, another known controller awaiting its
first record, a different repository, divergent branch history, or a collision
with untracked/ignored files. The refusal names the directory. No reset, stash,
clean or forced checkout is performed; untracked outputs and records survive.
Resolve stale runs with `scriptflow cleanup RUN_ID` on a compute node (or the
explicit ScriptHut cleanup command for an owned ended controller), then retry.
Do not edit a checkout concurrently with preparation or an active run. Custom
record locations used by hand must be supplied with `--runs-dir`; default records
and locally known launches are also checked.

Use `--branch experiment` to select another branch. Use `--commit FULL_SHA` for a
specific commit; a non-tip commit is detached and reported in the launch and run
views. A repeated acknowledged request never moves the checkout again. Use a new
`--run-id` for another run or different settings.

`--snapshot` selects `<clone_dir>/<commit[:12]>` instead of the shared working
directory. This isolates it from the fixed checkout; repeated uses of the same
commit still refer to the same per-commit directory. `flow_dir` applies in both
modes. No result cache is involved: ScriptFlow alone decides whether existing
outputs allow a task to be skipped.

The explicit existing-directory form also remains available:

```sh
scripthut scriptflow launch --backend cluster --workdir /data/project --flow analysis --run-id manual-project --json
scripthut scriptflow discover --backend cluster --workdir /data/project --register
```
