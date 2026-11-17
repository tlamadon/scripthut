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
| `--json` | Print machine-readable JSON instead of a formatted table. Accepted by **every** subcommand; stdout then holds exactly one JSON document (`run watch --json` suppresses the live redraw and prints the final run detail; `run logs --json` wraps the log in `{"content": ...}`; `run manifest` is JSON either way). |

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

### `run manifest` — a completed task's provenance record

`scripthut run manifest <run_id> <task_id>` prints the task's [versioned manifest](task-json/manifests.md): input hashes as used, output paths + content hashes, executor, timestamps, duration, exit code — the self-contained record a downstream consumer can verify work from. Always JSON (the document *is* the artifact).

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

### `task probe` — dry-run cache probe

`task probe` answers, per task, whether a submission would hit the [result cache](task-json/caching.md) — **without executing anything or writing anything** (no run records, no cache mutations, no restores). It takes the same JSON a submission would: a single task object, or a full workflow document (`{"tasks": [...]}` or a bare list).

```bash
# Probe a whole task list
scripthut task probe --from-file tasks.json --backend mercury-nb

# Probe from stdin, machine-readable
cat tasks.json | scripthut task probe --from-stdin --backend mercury-nb --json

# Pin the commit used for cache_scope="commit" tasks' keys
scripthut task probe --from-file tasks.json --backend mercury-nb --commit 3f2a91c
```

Human output is one line per task — `HIT` (with the cached outputs' content hash), `MISS` (cacheable, would run), or `RUN` (not cacheable, with the reason) — plus a summary line. `--json` returns the full verdicts, including each task's `cache_key`, `input_hashes`, and on a hit the cached `outputs` and `content_hash`.

The HTTP form is `POST /api/v1/tasks/probe` with a body of `{"task": {...}}` **or** `{"tasks": [...]}` plus `"backend"` and optional `"commit_hash"` / `"workflow_name"`. Verdicts are computed by the same code path a real submission uses, so a probe's hit/miss matches what submitting would have done at that moment.

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

## `scriptflow` — observe existing runs

ScriptFlow commands observe independently started runs, launch Slurm controllers, and
request cancellation through ScriptFlow’s file protocol. They use
local configuration and command transport directly, without initializing the legacy
run manager or starting a daemon. They do not use the API, web UI, or background polling.
The configured backend must be Slurm or PBS with SSH file access.

```bash
scripthut scriptflow register --backend hpc --run-dir /data/project/.sf/runs/example-run --json
scripthut scriptflow list --json
scripthut scriptflow view <reference-id> --json
scripthut scriptflow logs <reference-id> <task-id>
scripthut scriptflow logs <reference-id> <task-id> --attempt 1 --tail 50 --json
```

All commands accept `--config <file>`, `--json` and `--verbose`. SSH connection detail
is hidden by default; warnings and errors remain visible. `--verbose` enables transport logs. They intentionally have no
`--server` or HTTP-auth options; `SCRIPTHUT_SERVER` and `settings.cli_server` do not
select their transport. The machine running the CLI needs the selected backend's
SSH credentials. No Python or ScriptFlow command is executed on the login node.

Registration stores the backend name and absolute run directory and fetches its
`run.json`. The deterministic `reference_id` identifies that pair; it is distinct from
the remote ScriptFlow `run_id`. Registering the same pair again preserves the existing
reference and refreshes it. If the initial fetch fails, the reference remains registered
for a later retry. `list` fetches every registered run; `view` fetches one. There is no
watching loop. Registration does not start or adopt execution of the workflow.

References and last successful raw snapshots live in
`<settings.data_dir>/scriptflow/<reference-id>.json`, separate from legacy run storage.
A failed fetch preserves the last successful snapshot and its fetch timestamp. It records
an observation problem instead of changing the workflow state. Local filesystem errors
are reported; the remote record remains authoritative.

JSON `view`/`register` results have `reference_id`, `backend`, `run_directory`, `workflow`,
`observation`, and `cancellation`. `list --json` returns an array of the same shape.

- `workflow`: schema-1 projection with run ID, flow, state, error, cleanup flag, timestamps,
  host, working directory, and task/attempt records. It is null until the first successful
  fetch. Attempt exit codes and errors can be null if absent from the remote record.
- `observation`: `status`, `heartbeat_age_seconds`, `stale`, `last_successful_fetch`,
  `last_fetch_at`, `last_fetch_failed`, and `error`. Status is `fresh` or `stale` after a
  successful fetch, or an observation problem such as `unreachable`, `missing`, `malformed`,
  `unsupported_version`, or `unsafe_path`. Heartbeat age is clamped to zero for future
  timestamps. Stale means a nonterminal heartbeat older than 30 seconds; terminal runs
  are never heartbeat-stale, even if cleanup is pending. Fetch failures remain separate.

Logs accept a task ID or a unique task name. An ID always wins; an ambiguous name
returns an error listing matching IDs. JSON reports the selected task ID.
Logs default to the last attempt in the task's `attempts` array. `--attempt` selects its
1-based position; `--tail N` returns the final N lines (zero returns none). Without `--tail`,
the entire log is read. `logs --json` adds `task_id`, `attempt`, and `content` to the view
shape. A log error is separate (`log_error`) and does not change a successful record fetch.
Log retrieval refreshes the record first and refuses to use cached paths after a failed
refresh. A task without attempts or a log path returns an error.

Only absolute paths inside the registered directory are readable. Both record and log
reads check remote canonical paths with `realpath -e` before using `cat` or `tail`, so a
symlink to a file outside the directory is refused. The remote host must provide these
basic utilities. Use the canonical run directory if symlink aliases would make absolute
log paths appear outside it. Output paths are displayed in JSON but never fetched.

For register/view/logs, exit codes: 0 means the observation/read succeeded (including a stale heartbeat or a
workflow that reported failure); 1 means an observation, reference, configuration, cache,
or log error; 2 is argument-parser usage failure. Thus observation exit status is not a
workflow completion verdict. On errors, JSON output includes cached workflow evidence
when available; command-level errors instead return `error` and `kind`.


### Local removal and listing

```bash
scripthut scriptflow unregister <reference-id> --json
```

This deletes only the local reference and cached snapshot; it makes no remote call.
JSON returns `{"reference_id": "...", "status": "unregistered"}`. Missing references
return exit 1. A successfully produced `list` exits 0 even when individual references
are unreachable or missing; their problems remain in the OBSERVATION column and JSON.
Failure to load configuration or local storage still exits 1.

### Cancellation requests

```bash
scripthut scriptflow cancel <reference-id> --json
scripthut scriptflow cancel <reference-id> --wait --json
scripthut scriptflow cancel <reference-id> --wait 300 --json
```

Cancel first fetches the record; an unreadable record prevents writing. For a nonterminal
run, it writes UTC `requested_at` JSON to a temporary file inside the run directory and
renames it to `cancel.json` without overwriting an existing request. Canonical-path
containment applies to writes too. This requires `mktemp`, GNU-compatible `mv -nT`,
`realpath -m`, and basic shell utilities. ScriptFlow alone cancels its task jobs.
A repeat request leaves the file intact and reports “already requested”. An uncertain
write acknowledgement returns an error; retrying reconciles with the existing file.

Without `--wait`, a terminal record causes no write and exits 0, reporting its state
and `cleanup_pending`. Otherwise successful delivery exits 0 with status `requested`.
Delivery does not mean cancellation is finished. The controller may take about a minute
to notice the file on network storage.

`--wait` defaults to 180 seconds; an explicit nonnegative number sets the polling budget.
It polls every two seconds until terminal state and `cleanup_pending: false`. An in-flight
transport read may extend the budget by its timeout. It does not relabel outcomes: a run
that finishes first can report `completed` or `failed`. With `--wait`, even a terminal
record with cleanup pending must be confirmed before exit 0.

JSON includes `cancel_result.status` and `cancel_result.message` alongside the normal view:

| Status | Meaning | Exit |
| --- | --- | --- |
| `requested` | Request delivered or already present; no wait requested | 0 |
| `terminal` | Already terminal; no wait requested, no write | 0 |
| `confirmed` | Terminal with cleanup complete | 0 |
| `pending` | Wait timed out while running or cleanup pending | 1 |
| `controller_not_responding` | Running with stale heartbeat; workflow state unchanged | 1 |
| `unreachable` | Record unreadable or request acknowledgement unavailable | 1 |

For a stale controller, the message asks the user to run `scriptflow cleanup <run-id>`
on the cluster in the appropriate environment. The cancel command performs no cleanup or
controller release; explicit controller commands are described below. Observation errors are never converted into workflow outcomes.

View and list report `cancellation.requested` (the last observed existence of `cancel.json`,
or null before a successful read). Existence is fetched in the same command as `run.json`.
`cancellation.requested_at` is the local time this client first acknowledged a cancellation
request, not the original timestamp of a request created elsewhere; it is null if this
client has only observed it. Both fields are cached evidence on fetch failure; consult
`observation` for freshness. Unregistering discards this local history.


### Automated Slurm launch

Configure a backend-local environment group and controller defaults (see
[`scriptflow` configuration](configuration/environments.md#scriptflow-controllers)):

```bash
scripthut scriptflow launch --backend hpc --workdir /data/project --flow analysis --run-id example-run --json
```

Options: `--runs-dir /absolute/path` (default `<workdir>/.sf/runs`), `--cpus`, `--memory`,
`--time-limit`, `--partition`. `--run-id` is optional. Backend-local `env_group` comes
from `scriptflow:` configuration; environment rules execute only in the compute job.
Automated launch/cleanup/release currently require Slurm. Existing read/cancel support
for PBS is unchanged. The project and environment must already exist on shared storage.

Before any remote operation, ScriptHut saves a durable launch record under
`<settings.data_dir>/scriptflow/launches/`. The batch script applies the environment,
changes directory, refuses an existing run directory, writes a receipt outside that
directory, starts detached tmux on a private socket, and waits until the session exits.
The receipt records controller job ID, host, session, start time and ScriptFlow exit code.
The batch job exits when ScriptFlow exits; it does not leave an interactive fallback shell.

Launch output contains `status`, `request_id`, `reference_id`, `run_id`, `job_id` and
`attach_hint`. An acknowledged launch exits 0. The attach hint applies on the compute
node. ScriptHut disconnecting does not terminate the batch job or tmux session.

Repeating the same default-ID invocation reuses its saved request. To start another
workflow, pass a new explicit `--run-id`. Repeating an explicit ID also reuses the saved
request; conflicting flow, working directory or resources are refused. Keep the local
store: it contains the submission/reconciliation history. A local lock prevents concurrent
launch commands from submitting the same request twice.

A lost submission acknowledgement is `ambiguous` (exit 1). Repeat the original launch
command to reconcile its unique controller name against checked queue/accounting listings
and its receipt. Exactly one matching ID is adopted. Multiple IDs or failed queries are
errors; an empty successful listing remains ambiguous, never proof of nonexistence.
`--resubmit-ambiguous` explicitly permits another submission after empty reconciliation.
It never overrides a failed query or multiple matches. A persistent per-run claim outside
the run directory prevents two controller scripts from executing the same run, including
a late original submission. Claim removal/recovery is not automated.

View/list add controller state (`queued`, `running`, `ended`, `unknown`) and observation
time independently of workflow state and record freshness. Missing run.json before first
observation is displayed as `not started`, with the file observation problem still visible.
An absent scheduler entry is unknown. If a controller ends while the workflow remains
running with a stale heartbeat, `controller_notice` names explicit cleanup as the next step.
Task jobs are left running. No failure/success is invented from stale or unreachable data.

### Explicit cleanup and controller release

```bash
scripthut scriptflow cleanup <reference-id> --json
scripthut scriptflow cleanup <reference-id> --wait 0 --json
scripthut scriptflow release <reference-id> --json
scripthut scriptflow release <reference-id> --force --json
```

Cleanup requires a locally owned controller confirmed ended, and refuses queued, running
or unknown controller state. It submits a five-minute batch job with the saved environment
group/resources that runs `scriptflow cleanup <run-id> --runs-dir <runs-dir>` on a compute
node. It waits up to 180 seconds by default for a receipt; `--wait N` changes that budget.
Result status is `completed` (exit 0), `failed` (exit 1), or `pending` (exit 1). Repeat the
command to observe an existing cleanup job; uncertain submissions are never automatically
retried. After a failed cleanup result has been reported, a subsequent explicit cleanup
invocation submits a fresh attempt. A failed cleanup may leave unconfirmed task jobs; no scheduler operations are
performed on their IDs by ScriptHut. Cleanup of manually registered runs is refused because
controller ownership is unknown. No cleanup is automatic.

Release cancels only the saved controller job ID. It requires a freshly read terminal
workflow with `cleanup_pending: false`, unless `--force` explicitly overrides that gate.
The scheduler cancellation exit status is checked. `release_requested` confirms that
command succeeded, not that task cleanup happened. Normally release is unnecessary because
the controller job ends by itself. Neither release nor expiry cancels ScriptFlow task jobs.

### Discover existing runs

```bash
scripthut scriptflow discover --backend hpc --workdir /data/project --json
scripthut scriptflow discover --backend hpc --workdir /data/project --runs-dir /data/runs --register --json
```

Remote discovery is read-only: plain shell lists non-hidden run directories by directory
modification time, newest first, and the record reader parses their run.json locally.
Unreadable or unsupported records remain listed with their problem. An empty directory
returns an empty list (exit 0); a missing runs directory returns an error (exit 1).
`--register` saves only previously unregistered references and snapshots, preserving existing
cache/ownership information. Discovery does not adopt controller ownership from receipts.
GNU-compatible `find -printf`, `sort -z`, `realpath`, and Bash are required on the login host;
Bash, tmux and the configured ScriptFlow environment are required on compute nodes.


### Server views and launch options

The same registered references are available in the server at `/scriptflow` and through
`/api/v1/scriptflow/runs`; see [API and web views](scriptflow.md). The CLI continues to
use its local config/store and direct transport. Select the server's data directory to
share references.

Pass `--force-pattern 'task_*'` to launch to supply ScriptFlow's `run -f PATTERN` on the
compute node. The pattern is persisted and quoted. A saved request cannot change its force
pattern; use a new run ID. Environment activation tolerates unset variables but failures
still abort the job; strict unset checking resumes afterward.

Release of a controller already confirmed ended returns `already_ended` (exit 0), without
issuing a cancellation command. Other release guards are unchanged.
