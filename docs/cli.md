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
| `fetch REFERENCE --to LOCAL_DIR` | Copy declared outputs; repeatable `--task` or `--path`, `--strip`, `--dry-run`, `--overwrite` |
| `unregister REFERENCE` | Remove local reference/cache only |
| `launch --backend B --workdir /absolute/project --flow FLOW` | Submit one detached controller; supports `--run-id`, `--runs-dir`, resource overrides and `--force-pattern` |
| `launch --source NAME --flow FLOW` | Prepare a fixed git checkout, then launch; `--branch`, `--commit`, `--snapshot`, optional `--backend` |
| `images --source NAME [--branch B \| --commit SHA] [--backend B]` | List declared image references, target paths and remote file presence; no pull |
| `flows --source NAME [--branch B \| --commit SHA]` | Read `flow_*` names from committed `sflow.py`, without importing it |
| `discover --source NAME --register` | Discover under the fixed checkout's `flow_dir`; optionally register locally |
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

`--source` and `--workdir` are mutually exclusive. `--branch`, `--commit` and
`--snapshot` require a source. Branch defaults to its configured branch; an
explicit commit at that branch's tip remains on the branch, while an older commit
is detached. Remote URL sources require a full 40-character commit SHA; local
repositories also resolve abbreviated SHAs. A fixed checkout already at the
commit stays as it is, including its current attachment/detachment.

With no run ID, the same source/revision/flow reuses one request. A new revision
creates a new default request. Reusing an explicit run ID with changed settings
is refused; use a new ID to launch different code. Acknowledged repeats never
prepare the checkout again. Source name, branch, commit and detached status are
included in launch output and run list/view JSON and text.

`flows` reads committed text from `local_path`, otherwise the existing fixed
backend checkout. The latter must already contain the requested commit; a missing
checkout, commit or `sflow.py` is an explicit error, not an empty flow list.
It performs no clone, fetch, import or launch. The polling loop observes local code or each connected backend's existing copy.
The Sources page renders those cached flow names and errors; its Launch action
uses the same controller launch handler as this CLI.

For source launches with `images`, references resolve before backend preparation
and are saved in the launch record without credentials. Pulling happens inside the
controller before ScriptFlow starts. A failure is reported by `view`, `list` and
the API as `image_preparation.status: failed`, with error text, while the workflow
remains `not started`. Runtime diagnostics are redacted and retained under the flow
folder's `.sf`; view exposes up to 64 KiB of error text. While preparing, `view`
and the run page show elapsed seconds and the last 20 redacted log lines (capped at
16 KiB). An ended controller with unfinished preparation reports
"Controller ended during image preparation", with workflow `not started`. Retry with a **new run ID**
after resolving the image problem; an acknowledged launch is never resubmitted.
`launch --workdir` has no image preparation behavior.

## Fetch outputs

```sh
scripthut scriptflow fetch REFERENCE --to ./results --dry-run --json
scripthut scriptflow fetch REFERENCE --to ./results --task analysis --task summary
scripthut scriptflow fetch REFERENCE --to ./results --path staging --strip staging
scripthut scriptflow fetch REFERENCE --to ./results --path tables/result.csv --overwrite
```

Without a selection, `fetch` copies all tasks' declared outputs from a freshly read
run record. Repeat `--task` to select IDs or unique names (an ID wins); repeat
`--path` to select files or directory trees instead. These selectors are mutually
exclusive. Directory selections include empty directories and contained symlink
targets; local copies are regular files/directories. Duplicate paths transfer once.

Paths must remain inside the source repository working directory for a source
launch, the launch workdir otherwise, or the record's `working_directory` for a
manually registered run. A source's `flow_dir` does not narrow the repository
boundary. `--path` accepts absolute paths or paths relative to that boundary.
Parent traversal (`..`) and escaping symlinks are refused. Symlink cycles and
unreadable directory enumeration are reported as errors.

Local paths preserve their path relative to that working directory. `--strip`
removes a relative leading prefix: `staging/tables/result.csv` becomes
`tables/result.csv` with `--strip staging`. Files outside that prefix are skipped
with an error. Local symlinks, including destination parents, are refused. Existing
files require `--overwrite`; directories can already exist.

Transfers stream binary bytes over the existing SSH connection. Size and SHA-256
must match the plan before publication. A temporary local file is atomically
published without clobbering concurrent writers (hard link, then removal of the
temporary name); `--overwrite` uses atomic replacement. A failed transfer never
publishes partial contents or replaces an existing file. Local filesystem support
for POSIX directory descriptors, no-follow opens and hard links is required.
Remote files are only read; no archive or other backend file is created. Bash and
GNU `find`, `realpath`, `stat`, `sha256sum` and `head` are used on the backend.

Defaults are **1 GiB per file** and **10 GiB total per invocation**, in bytes:

```yaml
settings:
  fetch_max_file_bytes: 1073741824
  fetch_max_total_bytes: 10737418240
```

Raise these positive integer settings for larger outputs. Files beyond a cap are
reported and skipped; other eligible files continue. Bytes reserved for a failed
transfer still count against the total cap. Metadata reads and individual binary
transfers each have a 600-second timeout.

`--dry-run` reports paths, sizes, destinations and skip reasons without creating
local directories, transferring contents, or saving the run cache. It still reads
remote metadata and checksums. JSON contains `ok`, `dry_run`, `working_directory`,
`to` and `files`; entries contain `path`, `size`, `status`, `reason` and, when
available, `kind`, `sha256`, `destination`. Status is `fetched`, `would_fetch` or
`skipped`. Exit 0 means every selected entry was fetched (or would be fetched);
exit 1 means something requested could not be fetched, including missing outputs,
existing local files or failed verification. Successful files are retained.

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
