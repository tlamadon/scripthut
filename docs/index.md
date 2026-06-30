# ScriptHut

Run your compute workflows the way GitHub Actions runs CI — declarative runs defined in your git repo — but on **your own** infrastructure: HPC clusters (Slurm, PBS/Torque), AWS Batch, and AWS EC2. Drive everything from a local CLI built for humans and coding agents alike, backed by a small-footprint control plane that watches your flows and surfaces logs, errors, and status.

## Why ScriptHut

- **Local-first CLI** — submit workflows, watch runs, tail logs, inspect errors, cancel, and check cluster status from your terminal. The CLI runs standalone or drives a running control plane over its API.
- **Small-footprint control plane** — one lightweight server (a single `pip install`) gives a live view of every run — status, dependency DAGs, streamed logs, errors — over SSE. No database; state is plain JSON files.
- **Agent-native** — every operation is exposed through the CLI and its API, so a coding agent can drive ScriptHut end to end (`scripthut agent prompt`), or you can launch [Claude coding agents](coding-agents.md) onto a git source.
- **GitHub Action-style runs** — define workflows as files in your git repo; ScriptHut clones the repo on your backend (or pins the commit for Batch/EC2) and runs the task DAG with dependencies and concurrency caps.
- **Optional result caching** — tasks that declare `inputs`/`outputs` are content-addressed and their artifacts cached to an S3-compatible store, so a matching later run restores results instead of recomputing. Off by default, opt-in per task.

## Quick install

```bash
pip install scripthut
```

## Next steps

- [Installation](installation.md) — install via pip or Docker and start the server
- [Configuration](configuration/index.md) — YAML configuration reference (backends, workflows, environments, stacks, settings)
- [From a project](from-a-project.md) — using the CLI from inside your project directory with a layered (global + project-local) config
- [Task JSON Format](task-json/index.md) — how to write task generators that produce JSON
- [CLI](cli.md) — `scripthut workflow / run / backend / project / stack` subcommands, local vs remote transports, exit codes
