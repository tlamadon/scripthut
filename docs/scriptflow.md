# ScriptFlow server views

The server displays the same ScriptFlow references and cached projections as the CLI,
using `<settings.data_dir>/scriptflow/`. Use the same config/data directory when registering
or launching runs with the CLI. ScriptFlow remains the execution owner.

Start the server with its existing configuration:

```bash
scripthut --config scripthut.yaml
```

No bind settings are changed. Use the configured `settings.server_host` and
`settings.server_port` (defaults `127.0.0.1:8000`). Open `/` for the overview,
`/runs` for runs and `/runs/<reference-id>` for a run. `/scriptflow` and
`/scriptflow/<reference-id>` remain aliases.

The list and detail page separate workflow state, observation status and controller state.
They show the last successful read, errors, cleanup pending and cancellation requested.
Attempts appear in record order without a dependency graph. Each attempt offers a log
viewer; task IDs, paths and log content are escaped as text. Terminal runs have a disabled
cancel button. Refresh only reads remote evidence and updates the local cache.

The existing single polling loop refreshes references with nonterminal/unknown workflows,
and launched runs whose controller is not confirmed ended. Terminal manual runs, and
terminal launched runs with ended controllers, refresh only on demand. Each selected
reference uses at most one remote command per cycle through the server's shared connection.
For launched runs that command includes checked controller queue/accounting observations.
Failed record reads retain the last workflow, controller and cancellation evidence, updating
only observation error/fetch fields. Stale/unreachable never becomes a workflow outcome.

Pages receive passive SSE fragments from the existing poll event: `/overview/stream`,
`/runs/stream`, `/runs/<reference-id>/events`, `/jobs/stream` and `/disk/stream`.
The compatibility `/scriptflow/stream` notification remains available. Page loads
and SSE do not read remote files or trigger jobs.
There is no independent polling timer or automatic cleanup/release.

## JSON API

These routes share the existing API's server/reverse-proxy authentication boundary; ScriptHut adds no separate authentication scheme or bind address. Errors use FastAPI's existing
`{"detail": ...}` shape. Missing references return 404; unavailable transports/logs return
503; invalid parameters and unsafe log paths return 422.

| Method | Route | Result |
| --- | --- | --- |
| GET | `/api/v1/scriptflow/runs` | Cached summaries with task counts, without tasks; `?summary=false` opts into full projections |
| GET | `/api/v1/scriptflow/runs/{reference}` | Full cached CLI-compatible projection |
| POST | `/api/v1/scriptflow/runs/{reference}/refresh` | Fetch evidence and return projection |
| GET | `/api/v1/scriptflow/runs/{reference}/logs?task=ID&attempt=1&tail=200` | Attempt log content, truncation flag and cap |
| POST | `/api/v1/scriptflow/runs/{reference}/cancel` | Non-waiting cancellation result plus projection |

Projection fields include reference/backend, workflow, workflow_status, observation,
cancellation, controller and controller_notice. List responses omit `workflow.tasks` by default and add `task_count` and `task_counts`
by state. Use `?summary=false` only when full task records are needed. Single-run
responses still contain the full projection. Refresh observation failures return
HTTP 200 with an explicit observation problem and retained last-known workflow evidence.

Logs refresh the record before reading the selected attempt, enforce canonical containment,
and cap remote output and response content at 256 KiB (one extra byte detects truncation).
`attempt` is 1-based; `tail` defaults to 200 and accepts 0–10000 lines. Response fields:
`content`, `attempt`, `truncated`, `max_bytes`. A clipped UTF-8 boundary is omitted.

Cancellation uses ScriptFlow's file protocol, fetching first and refusing writes on unreadable
records. `cancel_result.status` is `requested`, `terminal`, or `unreachable`, with a message.
These outcomes return HTTP 200; delivery does not imply cleanup is finished. Clients read
or refresh the run to observe eventual terminal state and `cleanup_pending: false`.
There are no cleanup or release endpoints. The Sources form posts to
`/sources/launch` with `source`, `flow`, `backend` and `run_id`. It calls the CLI's
existing controller launch handler and returns the same result; refusals return
HTTP 409 with their original message in `detail`. A form keeps its run ID for
retries, so a repeated acknowledged launch submits no second job. Reloading the
Sources page gives each form a new run ID for an intentional new run.

## Launch fixes

`scriptflow launch --force-pattern 'task_*'` passes `-f 'task_*'` to ScriptFlow on the compute
node. The exact pattern is saved in the launch record and shell-quoted. Reusing a request
with a different pattern is refused; use a new run ID.

Environment activation runs with unset-variable checking disabled, while failure remains
fatal. Strict `set -eu` resumes after activation. `scriptflow release` on a controller
confirmed ended reports `already_ended` and “nothing to release” without issuing scancel.

For older controllers, server accounting queries start one UTC day before the launch date and
queue queries use the unique controller name. Empty scheduler results or query
failures remain unknown. Release persists a confirmed ended observation locally.

Source launches carry a `source` object in list/view JSON: `name`, `branch`,
`commit`, `short_commit`, repository `workdir`, `flow_dir`, `detached`, and
`snapshot`. Both run pages display this metadata alongside the three independent
states. `/sources` displays each source's working directory, flow directory and
flow names from committed text. An absent checkout is shown as “Not delivered to
<backend> yet”; flows from a configured local repository or an existing local source
clone remain available. Connection and Git failures are still shown as errors.
It offers the controller Launch action described above.

Image declarations appear in `images` on launch and run projections, with `name`,
`ref` and `path`. `image_preparation` is null or has `status` (`preparing`, `ready`,
`failed`) and `error`. New preparation records include `started_at` (Unix seconds).
While preparing, the projection includes `elapsed_seconds` and `log_tail` (last
20 redacted lines, at most 16 KiB). An observed ended controller turns an unfinished
preparation into a launch problem: "Controller ended during image preparation".
The workflow remains `not started` when its record is absent. A pull error is a launch problem, never a workflow failure.
The run page and list show the images and error separately from workflow,
controller and observation states. Server polling reads image status in the same
remote command as the workflow record and controller queries.

## Dashboard pages

The layout, navigation, status panels and styles follow the original ScriptHut
interface. The run data is now ScriptFlow's cached record, with no legacy run model.

| Page | Contents |
| --- | --- |
| `/` | Activity calendar, hourly CPU-hours by source, active/recent runs and backend cards |
| `/backends` | Connections, account/fair-share, partition CPU/node/memory/time-limit/features, pending reasons and jobs |
| `/runs` | Run table and mobile cards; filters for state, backend, source and flow |
| `/runs/<reference-id>` | Three independent states, source/images, tasks and attempts, attempt timeline, logs and cancellation |
| `/sources` | Cached flow names, working directory, flow directory, images and controller Launch forms |
| `/terminals` | Active browser sessions and attachment to an existing node/tmux session; no allocation action |
| `/disk` | Last observed sizes of source working directories, their immediate result/other folders, run-record folders and shared images |
| `/settings` | Existing configuration editor |

CPU-hours are requested task CPUs multiplied by each attempt's elapsed time.
Running attempts end at now. Missing `cpus` defaults to 1; missing `started_at`
falls back to `submitted_at`. These estimates, and stale observations, are marked
approximate. Calendar and hourly buckets use the server's local timezone. Sources
come from launch records; manually registered runs appear under `manual`. No pricing
or cost data is displayed.

Source inspection and read-only disk sizing run in the same polling cycle using
existing backend connections. Disk uses plain `find` and `du`; it has no deletion,
classification or cleanup operation. Absent directories show “not present on this backend”; failed disk reads are shown explicitly, with
the last observation retained on transport failure. Folder sizes overlap their
parent totals and are not added together. Hidden `.git` and `.sf` directories are
excluded from the immediate folder rows; `.sf/runs` has separate rows. Only folders
containing paths declared as task outputs in cached records are labelled “results
folder”; other directories, including logs, are simply labelled “folder”.

The log page opens the existing bounded log API on demand. It does not load logs
while rendering the page. Cancellation writes the existing request file only;
ScriptFlow still owns every task job. Legacy workflow/task APIs, retry/delete-run
routes, disk cleanup routes and interactive task submission remain absent.

## Large runs and missing fields

Run details show 20 tasks per page, shared by the table, timeline and sidebar.
The state selector includes counts for the whole run; name/ID search narrows the
page. Previous/Next links fetch another page on demand. Query parameters are
`page`, `task_state` and `search`, and passive SSE updates preserve that selection.
The attempt log page renders only the selected task in its sidebar, with a link
back to browse all tasks. Pages do not request the full JSON list.

The 3,000-task regression fixture's first page and each SSE update cycle must stay
below 256 KiB; its log page must stay below 32 KiB and JSON list summary below
8 KiB. These bounds describe the fixture, not a cap on arbitrary task/error text.

Missing resources display a dash; CPU estimates still use the documented fallback.
“CPU time” shows minutes below one hour and hours otherwise. Runs registered by
hand show “not managed by ScriptHut” for the controller; “unknown” is reserved for
a managed controller without a readable state.
