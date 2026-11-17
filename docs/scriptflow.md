# ScriptFlow server views

The server displays the same ScriptFlow references and cached projections as the CLI,
using `<settings.data_dir>/scriptflow/`. Use the same config/data directory when registering
or launching runs with the CLI. ScriptFlow remains the execution owner.

Start the server with its existing configuration:

```bash
scripthut --config scripthut.yaml
```

No bind settings are changed. Use the configured `settings.server_host` and
`settings.server_port` (defaults `127.0.0.1:8000`). Open `/scriptflow` via the navigation
link, then choose a run at `/scriptflow/<reference-id>`.

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

Pages receive passive SSE notifications at `/scriptflow/stream` from the existing poll
event, then reload cached data. Page loads and SSE do not read remote files or trigger jobs.
There is no independent polling timer or automatic cleanup/release.

## JSON API

These routes share the existing API's server/reverse-proxy authentication boundary; ScriptHut adds no separate authentication scheme or bind address. Errors use FastAPI's existing
`{"detail": ...}` shape. Missing references return 404; unavailable transports/logs return
503; invalid parameters and unsafe log paths return 422.

| Method | Route | Result |
| --- | --- | --- |
| GET | `/api/v1/scriptflow/runs` | Cached projections for registered references |
| GET | `/api/v1/scriptflow/runs/{reference}` | Full cached CLI-compatible projection |
| POST | `/api/v1/scriptflow/runs/{reference}/refresh` | Fetch evidence and return projection |
| GET | `/api/v1/scriptflow/runs/{reference}/logs?task=ID&attempt=1&tail=200` | Attempt log content, truncation flag and cap |
| POST | `/api/v1/scriptflow/runs/{reference}/cancel` | Non-waiting cancellation result plus projection |

Projection fields include reference/backend, workflow, workflow_status, observation,
cancellation, controller and controller_notice. List responses include the full projections,
so clients can select summaries without another fetch. Refresh observation failures return
HTTP 200 with an explicit observation problem and retained last-known workflow evidence.

Logs refresh the record before reading the selected attempt, enforce canonical containment,
and cap remote output and response content at 256 KiB (one extra byte detects truncation).
`attempt` is 1-based; `tail` defaults to 200 and accepts 0–10000 lines. Response fields:
`content`, `attempt`, `truncated`, `max_bytes`. A clipped UTF-8 boundary is omitted.

Cancellation uses ScriptFlow's file protocol, fetching first and refusing writes on unreadable
records. `cancel_result.status` is `requested`, `terminal`, or `unreachable`, with a message.
These outcomes return HTTP 200; delivery does not imply cleanup is finished. Clients read
or refresh the run to observe eventual terminal state and `cleanup_pending: false`.
No launch, cleanup or release endpoints are exposed.

## Launch fixes

`scriptflow launch --force-pattern 'task_*'` passes `-f 'task_*'` to ScriptFlow on the compute
node. The exact pattern is saved in the launch record and shell-quoted. Reusing a request
with a different pattern is refused; use a new run ID.

Environment activation runs with unset-variable checking disabled, while failure remains
fatal. Strict `set -eu` resumes after activation. `scriptflow release` on a controller
confirmed ended reports `already_ended` and “nothing to release” without issuing scancel.

For older controllers, server accounting queries start at the launch date and
queue queries use the unique controller name. Empty scheduler results or query
failures remain unknown. Release persists a confirmed ended observation locally.
