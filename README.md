# ScriptHut

ScriptHut launches ScriptFlow controllers on Slurm and observes their run records,
tasks, attempts and logs. ScriptFlow owns task scheduling, retries and cancellation.
Runs started by hand work without ScriptHut, and disconnecting ScriptHut does not
stop a controller.

ScriptHut also provides Slurm/PBS connection status and read-only job listing,
attachment to existing tmux sessions, git source delivery into shared working directories,
and a local server daemon. It does not execute its own workflows or tasks.

## Install and configure

```sh
uv tool install -e .
cp scripthut.example.yaml scripthut.yaml
```

Edit the placeholder backend, environment and paths. Install ScriptFlow in the
compute environment separately. Launch currently requires Slurm; PBS supports
observation and backend job listing.

```sh
scripthut --config scripthut.yaml
```

The default address remains `http://127.0.0.1:8000`; configuration or existing
`--host`/`--port` overrides control binding. `/` opens the ScriptFlow runs page.
Other pages are `/backends`, `/sources`, `/terminals` and `/settings`.

## Observe or launch

```sh
scripthut scriptflow register --config scripthut.yaml --backend cluster --run-dir /data/project/.sf/runs/example --json
scripthut scriptflow list --config scripthut.yaml --json
scripthut scriptflow view REFERENCE --config scripthut.yaml --json
scripthut scriptflow logs REFERENCE TASK --config scripthut.yaml --tail 200
scripthut scriptflow fetch REFERENCE --config scripthut.yaml --to ./results --dry-run
scripthut scriptflow launch --config scripthut.yaml --backend cluster --workdir /data/project --flow analysis --run-id example --json
```

Launch submits one controller batch job, starts detached tmux on the compute node,
and waits for it. Repeated identical launches reconcile the saved request rather
than creating another controller. ScriptFlow commands never run on the login node.

Configured git sources can prepare and launch a fixed checkout:

```sh
scripthut scriptflow launch --config scripthut.yaml --source example --flow analysis --run-id source-example --json
scripthut scriptflow flows --config scripthut.yaml --source example --json
scripthut scriptflow discover --config scripthut.yaml --source example --register
```

The default checkout is `<clone_dir>/.worktrees/source-<name>`; `workdir` overrides
it, and `flow_dir` selects a subdirectory containing `sflow.py`. Tracked changes,
running runs (including stale ones), and non-fast-forward updates are refused.
Untracked outputs stay in place. See [working from a project](docs/from-a-project.md)
for the manual equivalent, revision selection and snapshot mode.

Sources can also declare digest-pinned container images. The controller pulls
missing images on the compute node, exports `SCRIPTHUT_IMAGE_<NAME>`, and writes
`.sf/images.sh` for manual runs. Pulls use node-local scratch with a separate
timeout; `scriptflow view` shows redacted progress and elapsed time. `scripthut scriptflow images --source example
--json` checks presence without pulling. See [image configuration](docs/configuration/index.md#source-container-images).

Workflow outcome, controller state and observation freshness are separate.
Unreachable or stale observations never mean success or failure. The server and
CLI must use the same data directory to share references.

```sh
scripthut scriptflow cancel REFERENCE --config scripthut.yaml --wait --json
scripthut scriptflow cleanup REFERENCE --config scripthut.yaml
scripthut scriptflow release REFERENCE --config scripthut.yaml
```

Cancel writes ScriptFlow's request file. Cleanup explicitly submits a compute job
after the controller is confirmed ended. Release targets only the controller;
normally it exits by itself. Controller expiry leaves task jobs running.

## Reference

- [CLI and exit status](docs/cli.md)
- [Configuration and obsolete-key migration](docs/configuration/index.md)
- [ScriptFlow API and web views](docs/scriptflow.md)
- [Environment groups](docs/configuration/environments.md)
- [Coding-agent guidance](docs/coding-agents.md)

Removed features include legacy scheduling, disk tools, result caching, software
stacks, coding-agent jobs, cloud/local execution, pricing and run notifications.
Their existing data files are left untouched and are no longer read.

## Development

```sh
uv sync --extra dev
uv run pytest tests/ -q
```

Tests use fake transports and local temporary repositories; no cluster is needed.
