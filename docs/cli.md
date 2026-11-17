# CLI

The command groups are `scriptflow`, `backend`, `source`, `agent`, `status` and
`daemon`. Use `scripthut GROUP --help` for subcommands and flags. `scripthut --help`
describes server startup. Every CLI leaf supports `--json`.

## ScriptFlow

| Command | Behavior |
|---|---|
| `register --backend B --run-dir /absolute/run` | Register an existing run directory |
| `list` | Fetch references on demand; include per-reference observation problems |
| `view REFERENCE` | Show workflow, controller, freshness, tasks and attempts |
| `logs REFERENCE TASK --tail 200` | Read an attempt log; task ID or unique name; `--attempt` is 1-based |
| `unregister REFERENCE` | Remove local reference/cache only |
| `launch --backend B --workdir /absolute/project --flow FLOW` | Submit one detached controller; supports `--run-id`, `--runs-dir`, resource overrides and `--force-pattern` |
| `discover --backend B --workdir /absolute/project --register` | List remote run directories; optionally register locally |
| `cancel REFERENCE --wait` | Write cancellation request; optionally wait for terminal state and complete cleanup |
| `cleanup REFERENCE` | Explicit compute cleanup job after controller ended |
| `release REFERENCE` | Cancel owned controller after terminal workflow and cleanup, unless `--force` |

Pass `--config scripthut.yaml` after the subcommand. ScriptFlow commands use local
configuration and command transport directly; they do not require a running web
server. `--verbose` enables connection detail. No Python or ScriptFlow command
runs on the login node.

`list` exits 0 if the listing operation succeeds, including entries with observation
problems. Unreadable `view`/`logs` exit 1. Cancellation with `--wait [SECONDS]`
(default 180) exits 0 only when confirmed; without wait, a delivered request is
success. A terminal record is a no-op. Pending, unreachable or stale-controller
outcomes are reported explicitly. Release of an ended controller reports
`already_ended` and stores that observation. Other operational errors are nonzero.

Launch records persist before submission. Identical requests reuse acknowledged
jobs; uncertain submission is reconciled by unique job name and receipt. An
ambiguous request is not resubmitted without `--resubmit-ambiguous`.

## Other commands

- `backend list`: connection status; `/backends` and the backend jobs API provide
  read-only job listing.
- `source list`, `source view NAME`, `source sync [NAME]`: source metadata/sync.
- `status`: configuration, API connectivity and authentication diagnostics.
- `daemon start`, `stop`, `status`, `logs`: local server lifecycle.
- `agent prompt`, `agent install`: current guidance and local skill installer.

Shared API commands resolve `--server`, then `SCRIPTHUT_SERVER`, then
`settings.cli_server`; otherwise they use the local daemon according to
`cli_autostart`. `--server local` uses short-lived in-process connections for
metadata inspection; source synchronization needs the server. Cloudflare Access
credentials can come from CLI flags, environment or `settings.cli_auth`.

The removed `run`, `workflow`, `task`, `stack`, `disk` and AWS setup commands are
not available. ScriptFlow owns task execution; there is no replacement task API.
