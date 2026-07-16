# CLI

ScriptHut ships a `gh`-style CLI for triggering workflows, managing stacks, firing one-off tasks, inspecting runs, and tailing logs without opening the web UI. The single binary is the same `scripthut` entry point that runs the server — when called with a subcommand (`workflow`, `run`, `backend`, `project`, `stack`, `task`, `agent`, `daemon`) it dispatches to the CLI instead.

```bash
scripthut workflow list          # CLI
scripthut --port 8000            # server (no subcommand)
```

The CLI is designed to feel native from inside a project directory: it walks up from the current working directory looking for a `scripthut.yaml`, merges it with your user-global config (`~/.config/scripthut/scripthut.yaml`), and gives every subcommand the right context automatically. See [From a project](from-a-project.md) for the full layered-config model and a worked example.

## Transports — local vs remote

The CLI talks to your workflows through a client picked automatically by the *server-resolution chain*:

1. **`--server <url>`** argument on the command (overrides everything; pass `local` to force in-process mode)
2. **`SCRIPTHUT_SERVER`** environment variable
3. **`settings.cli_server`** in `scripthut.yaml`
4. None of the above → **local daemon** (auto-started if allowed — see [Local daemon](#local-daemon))

| Transport | Picked when | Behavior |
|-----------|-------------|----------|
| `RemoteClient` (HTTP) | a server URL is resolved | Calls the running server's `/api/v1` endpoints via httpx. The server submits and tracks the work; the CLI just queries it. |
| Local daemon (HTTP) | no server URL anywhere | Same `RemoteClient`, pointed at a background `scripthut` server on `settings.server_host:server_port`. Started on demand (with your consent) and shared by every subsequent command — plus you get the web admin for free. |
| `LocalClient` (in-process) | `--server local` | Boots a `Runtime` in-process — same backend SSH connections, storage, and `RunManager` the server uses. Paid on every invocation; escape hatch for debugging and one-off scripting. |

So `scripthut workflow run train` on a laptop offers to start a local daemon on the first command, then every later command is fast. The same command pointed at a running server (`--server https://scripthut.team.example`) hits its API instead.

Set the default server once in `scripthut.yaml` so day-to-day CLI use is point-free:

```yaml
settings:
  cli_server: "https://scripthut.team.example"
```

## Local daemon

When nothing resolves a server, the CLI probes `settings.server_host:server_port` (default `127.0.0.1:8000`). A running server there — daemon or a foreground `scripthut` — is used as-is. If nothing answers, `settings.cli_autostart` decides what happens:

| `cli_autostart` | On a TTY | In a script / CI (no TTY) |
|-----------------|----------|---------------------------|
| `ask` (default) | Prompts `Start a local daemon? [Y/n]`, with an offer to remember your answer in the global config | Fails with guidance — never prompts, never spawns |
| `always` | Starts the daemon silently (one notice on stderr) | Same — `always` is an explicit opt-in, so it works unattended |
| `never` | Fails with guidance (`scripthut daemon start`, `--server local`, or configure a server) | Same |

The daemon is a detached `scripthut` server: it keeps running until you stop it (or reboot), serves the web admin at its URL, and is spawned from `$HOME` so it always uses your user-global config regardless of which project directory triggered it. Its pidfile and log live under `<data_dir>/daemon/` (default `~/.cache/scripthut/daemon/`).

Manage it explicitly with the `daemon` noun:

```bash
scripthut daemon start     # start detached (idempotent — no-op if running)
scripthut daemon status    # pid, url, uptime; exit 0 running / 3 not
scripthut daemon logs      # print the log path and its last 100 lines
scripthut daemon stop      # SIGTERM, then SIGKILL after 10s
```

Prompts and notices go to **stderr**, so `scripthut run list --json | jq` stays clean. If the configured port is occupied by something that isn't scripthut, the CLI says so and refuses to wait — change `settings.server_port` or stop that process.

## Global flags

Every subcommand accepts these:

| Flag | Description |
|------|-------------|
| `--server <url>` | Server URL to target. Pass `local` to force local mode. |
| `--config <path>`, `-c <path>` | Path to `scripthut.yaml`. Loads **exactly that file** and skips the layered discovery. Useful in tests and one-off scripts. |
| `--json` | Print machine-readable JSON instead of a formatted table (where supported). |

### Config discovery (without `--config`)

When you don't pass `--config`, the CLI loads up to two files and merges them:

1. **User-global**: `~/.config/scripthut/scripthut.yaml` (or `~/.scripthut.yaml`).
2. **Project-local**: the first `scripthut.yaml` found by walking up from `$PWD`.

Project-local files may **only** define `stacks`, `workflows`, `projects`, `env`, `env_groups`. Infrastructure fields (`backends`, `sources`, `settings`, `pricing`) come from the global file and are rejected in a project-local file with a clear error. Full details and examples in [From a project](from-a-project.md).

## `workflow` — manage workflows

```bash
scripthut workflow list                       # show all workflows and projects
scripthut workflow view <name>                # dry-run preview the tasks
scripthut workflow view <name> --backend <b>  # preview against a different backend
scripthut workflow run <name>                 # submit a run
scripthut workflow run <name> --backend <b>   # submit, overriding the backend
scripthut workflow run <sflow.json> --project <name>   # submit from a git project
```

`workflow run` prints the new run's ID and a link/path you can pass straight to `scripthut run watch <id>`.

## `run` — inspect and control runs

```bash
scripthut run list                            # 20 most recent runs
scripthut run list --limit 100                # last 100
scripthut run view <id>                       # task table + status counts
scripthut run watch <id>                      # poll until the run terminates
scripthut run watch <id> --exit-status        # exit non-zero on FAILED/CANCELLED
scripthut run watch <id> --interval 2         # tighter polling
scripthut run cancel <id>                     # cancel a running run
scripthut run rerun <id>                      # re-execute as a NEW run
scripthut run rerun <id> --in-place           # reset and resubmit the same run
scripthut run logs <id> <task>                # stdout for one task
scripthut run logs <id> <task> --error        # stderr
scripthut run logs <id> <task> --tail 100     # only the last 100 lines
scripthut run logs <id> <task> --follow       # tail until the task ends
```

`watch --exit-status` is the CI-friendly form: it returns 0 only when every task in the run completes successfully.

## `backend` — inspect configured backends

```bash
scripthut backend list                        # connection status, max_concurrent, type
```

Useful when a workflow hangs at submission to confirm the right backend is actually reachable.

## `agent` — coding-agent helpers

```bash
scripthut agent prompt                        # markdown briefing for an agent
scripthut agent install                       # Claude Code skill + /scripthut:debug into ./.claude
scripthut agent install --user                # same, into ~/.claude (all projects)
```

`agent prompt` emits a markdown document that teaches a coding agent how to use this scripthut from the current project. It's a mix of a **static reference** (CLI patterns, TaskDefinition shape, exit codes, common gotchas) and a **live inventory** (the backends, stacks, and workflows the layered config currently exposes) so the agent's suggestions reference real names rather than placeholders.

Typical use is to pipe it into the agent's context:

```bash
# Capture once and feed to your agent of choice
scripthut agent prompt > .agent-brief.md

# Or pipe through xclip / pbcopy / etc.
scripthut agent prompt | pbcopy
```

Re-run whenever the user adds a backend, stack, or workflow — the inventory is read fresh each invocation.

`agent install` writes a Claude Code **skill** (`.claude/skills/scripthut/SKILL.md`) and a **`/scripthut:debug [run-id]` slash command** (`.claude/commands/scripthut/debug.md`). The skill loads on demand when a session involves compute work and defers to `agent prompt` for live inventory, so it never goes stale; the command walks the failure-diagnosis flow for a failed run. Managed files (identified by a `generated by` marker) are updated in place on re-install; hand-written files at those paths are skipped unless `--force` is passed. See [coding agents](coding-agents.md) for the full story.

## `task` — submit ad-hoc tasks

Sometimes you don't want to commit a task definition to a git repo or wire a workflow generator — you just want to fire a single command at a configured backend. `task run` is the shortest path to that, and is the entry point that's friendliest for coding agents.

```bash
# Simplest form: a one-line command on a configured backend.
scripthut task run "python -c 'print(2+2)'" --backend mercury-nb

# Run a local script on the backend WITHOUT staging files first
# (script is base64-embedded into the task command).
scripthut task run --inline-script ./probe.py \
  --backend mercury-nb --cpus 1 --memory 1G --time 0:05:00

# With explicit resource shape.
scripthut task run "python train.py" \
  --backend mercury-nb \
  --cpus 8 --memory 32G --time 4:00:00 \
  --partition gpu --gres gpu:1 \
  --working-dir /scratch/me/repo \
  --env CUDA_VISIBLE_DEVICES=0 --env WANDB_PROJECT=demo

# Feed a full TaskDefinition JSON via stdin — handy for agents.
echo '{
  "id": "exp-42",
  "name": "exp 42",
  "command": "python train.py --lr 1e-3",
  "cpus": 8,
  "partition": "gpu",
  "gres": "gpu:1"
}' | scripthut task run --from-stdin --backend mercury-nb --json

# Or from a JSON file (CLI flags still override individual fields).
scripthut task run --from-file experiment.json --backend mercury-nb --cpus 16

# Verify the assembled task body without submitting.
scripthut task run "echo hi" --backend mercury-nb --dry-run
```

### What it does

`task run` builds a single `TaskDefinition` (the same shape used by workflow generators) and submits it as a one-item run. The run shows up in the dashboard and in `scripthut run list` like any other; behind the scenes its `workflow_name` is `_adhoc/<task-id>` (override with `--run-name <label>` if you want something more memorable).

### Input modes (mutually exclusive)

| Source | When it's used |
|--------|----------------|
| `command` (positional) | Genuine one-liners. If you find yourself quoting a multi-line script, switch to `--inline-script`. |
| `--inline-script <local-path>` | A local script file you want to run on the backend without copying or git-committing it first. ScriptHut base64-embeds the file into the task command and the backend decodes + executes it. Files without a `#!` line get `#!/bin/bash` prepended. Best for files up to a few hundred KB. |
| `--from-stdin` | Pipe a full TaskDefinition JSON. Most reliable for agents that construct the payload programmatically. |
| `--from-file <path>` | Same JSON shape, from a file. CLI flags layered on top still override individual fields. |

Passing more than one of these is an error — silent precedence would mean you thought you were submitting one thing and you weren't.

`--dry-run` prints the assembled `{"task": ..., "backend": ...}` and exits without touching any backend — let an agent verify the payload before committing.

### Default id and name

If you don't pass `--id`, the task gets `adhoc-<8-hex-chars>` derived from the command and a nanosecond timestamp. Two consecutive runs with the same command get different ids, so they don't collide on disk. `--name` defaults to whatever `--id` resolves to.

### Output

Without `--json`, prints a single human-readable line and the command to inspect the run:

```
Run a1b2c3d4 submitted to mercury-nb (task 'adhoc-1f2e3d4a').
  scripthut run view a1b2c3d4
```

With `--json`, prints the full run summary:

```json
{
  "id": "a1b2c3d4",
  "workflow_name": "_adhoc/adhoc-1f2e3d4a",
  "backend_name": "mercury-nb",
  "task_count": 1,
  "submitted_count": 1,
  "status_counts": {"submitted": 1},
  ...
}
```

The shape matches `workflow run --json`, so an agent can pipe straight into `scripthut run watch "$ID"` or other automation built around that contract.

### Use with stacks

Tasks that need a particular runtime should be paired with a [stack](configuration/stacks.md):

```bash
scripthut stack install julia-1.11 --backend mercury-nb
scripthut task run "julia --project=. scripts/run.jl" \
  --backend mercury-nb \
  --working-dir /home/me/balke-jmp \
  --cpus 16 --memory 64G
```

Stacks are installed once; ad-hoc tasks reference them via their resolved `STACK_DIR` in the command or working directory.

### Notes for coding agents

- The CLI is the supported entry point — there's no separate "agent API." The `--json` flag plus stable exit codes (`0` submitted, `1` error) are the contract.
- **Default pattern**: write your script to a local file → submit with `--inline-script <path>` → capture `id` from `--json` → poll `scripthut run view <id> --json`. No file staging, no scp, no git commit needed for small scripts.
- For large or multi-file work, fall back to a workflow with a git repo — `--inline-script` is for "run this file" not "run my whole repo."
- `--dry-run` is a good safety check before submission; pair it with `scripthut backend list` to verify the target backend is reachable.
- The HTTP form is `POST /api/v1/tasks/run` with a body of `{"task": {...}, "backend": "...", "run_name": "..."}` — use it directly if you're talking to a running scripthut server (set `SCRIPTHUT_SERVER` and the CLI picks remote mode automatically).
- Tasks submitted this way still respect the layered config — `working_dir` resolution, env rules, partition mapping, and account selection from `scripthut.yaml` all apply.
- For a self-contained briefing you can paste into your context window, run `scripthut agent prompt` (see the `agent` section above).

## `stack` — manage reusable software stacks

A stack is a software environment (Python venv, Julia depot, Conda env, …) ScriptHut installs once per backend and reuses across runs. The CLI is the lifecycle interface — see [Stacks](configuration/stacks.md) for the model and YAML schema.

```bash
scripthut stack list                                  # configured stacks (no SSH)
scripthut stack check [<name>] [--backend X]          # per-backend state table
scripthut stack install <name> [--backend X] [--rebuild]
scripthut stack delete <name> [--backend X]
```

Each command opens an SSH connection per (stack × selected backend). Without `--backend`, the command iterates every backend the stack declares (or every SSH-capable backend if the stack's `backends:` list is empty). Non-SSH backends (Batch, EC2) are silently skipped for now.

- `check` exits non-zero if any stack on any selected backend is not `ready` — handy as a CI gate before submitting work.
- `install` is idempotent: a no-op when the stack is already ready at the current hash. `--rebuild` forces a fresh build even when the hash matches.
- `delete` removes the entire `<cache_dir>/<name>/` directory on the backend (every hash, not just the current one). A subsequent `install` rebuilds from scratch.

Example session from inside a project:

```bash
cd ~/git/my-project
scripthut stack check julia-1.11        # is it built on every backend?
scripthut stack install julia-1.11      # build any that aren't
scripthut workflow run grid-search      # submit work that relies on the stack
```

## `project` — inspect git projects

```bash
scripthut project list                        # all configured projects
scripthut project view <name>                 # show sflow.json files + their workflows
```

A "project" is a git repo on a backend that contains one or more `sflow.json` workflow files; `project view` lists them so you can pick one to feed into `workflow run --project`.

## Common patterns

### Submit a workflow and follow it to completion

```bash
RUN_ID=$(scripthut workflow run train --json | jq -r .id)
scripthut run watch "$RUN_ID" --exit-status
```

The two commands compose because `--json` on `workflow run` yields a stable shape (`{"id": "...", "items": N, ...}`).

### Tail one task's stderr until it finishes

```bash
scripthut run logs $RUN_ID train.shard-3 --error --follow
```

`--follow` reads the file once it appears on the backend, then polls until the task moves to a terminal state.

### Inspect the resolved environment for a task

The Env tab in the web UI shows resolved env with per-key provenance. The same data is available via the `/runs/{id}/tasks/{task_id}/env` endpoint, which `RemoteClient` can hit directly:

```bash
curl -s "$SCRIPTHUT_SERVER/runs/$RUN_ID/tasks/train.shard-3/env" | jq
```

See [Environments](configuration/environments.md) for the full env-rule resolution model that produces these values.

### CI / automation

In CI you usually want:

```bash
export SCRIPTHUT_SERVER="https://scripthut.team.example"
RUN_ID=$(scripthut workflow run nightly-eval --json | jq -r .id)
scripthut run watch "$RUN_ID" --exit-status
```

Setting `SCRIPTHUT_SERVER` once at the top means the rest of the script reads naturally and `scripthut` never accidentally drops into local mode.

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Command succeeded (and for `run watch --exit-status`, the run completed successfully) |
| `1` | Command-level error: bad arguments, missing workflow/run, server unreachable, autostart declined/disabled, etc. |
| `2` | (`run watch --exit-status` only) The run terminated in a non-success state (`FAILED` / `CANCELLED`) |
| `3` | (`daemon status` only) No local daemon is running |

Without `--exit-status`, `run watch` always returns 0 once the run reaches a terminal state — the watch itself succeeded, even if the work didn't.
