# Task Manifests

Every completed task can be summarized as a single, self-contained, versioned JSON document — the **task manifest**: *these exact input hashes, through this command, produced these exact output hashes*, plus who executed it and when. A downstream consumer (a build notary, a provenance ledger, an audit script) can verify a task's work from the manifest alone, without querying anything else.

```json
{
  "manifest_version": 1,
  "task": {
    "id": "train",
    "name": "Train model",
    "command": "python train.py",
    "working_dir": "/home/me/scripthut-repos/3f2a91c0aa11"
  },
  "inputs":  { "data/train.csv": "9f2c…", "train.py": "1ab4…" },
  "outputs": { "model.pt": "77d0…", "metrics.json": "e51f…" },
  "cache":   { "key": "c3a9…", "scope": "inputs", "hit": false },
  "executor": { "backend": "hpc-cluster", "type": "slurm", "job_id": "48211" },
  "run":     { "id": "b41c9a2f", "workflow": "ml/train", "commit": "3f2a91c0aa11", "branch": "main" },
  "exit_code": 0,
  "timing": {
    "submitted_at": "2026-07-21T14:03:10+00:00",
    "started_at":   "2026-07-21T14:05:22+00:00",
    "finished_at":  "2026-07-21T14:41:07+00:00",
    "duration_seconds": 2145.0
  }
}
```

## Schema (`manifest_version: 1`)

| Field | Meaning |
|-------|---------|
| `task` | The task's identity as defined: `id`, `name`, `command`, `working_dir`. |
| `inputs` | `{path: sha256}` of the declared [`inputs`](caching.md#task-fields), hashed **at submit time** — the input state the task actually saw. `null` means *not hashed* (nothing declared, or hashing wasn't possible) — never treat `null` as "empty and fine". |
| `outputs` | `{path: sha256}` of the declared `outputs`, hashed **after completion**. On a cache hit the hashes are carried from the stored cache manifest, so hit and miss report identical hashes for identical work. `null` means not hashed. |
| `cache` | The [result-cache](caching.md) view: the computed `key`, the task's `scope` (`commit`/`inputs`), and whether this completion was a `hit`. |
| `executor` | Which backend ran it: configured `backend` name, its `type` (`slurm`, `pbs`, `local`, …), and the scheduler `job_id`. |
| `run` | Provenance: run `id`, `workflow` name, git `commit` and `branch` (when known). |
| `exit_code` | Numeric exit code as confirmed by the backend's accounting; `null` if never confirmed. |
| `timing` | Submit/start/finish timestamps (ISO 8601) and `duration_seconds` (start → finish). |

Versioning contract: within a major `manifest_version`, fields are only ever **added** — existing fields never change meaning. A consumer should reject manifests whose `manifest_version` it doesn't know.

## Where hashes come from

- Tasks that declare `inputs` or `outputs` get their inputs hashed at submit time and their outputs hashed at completion, **whether or not the result cache is enabled** — manifests stand on their own. Tasks that declare neither carry `inputs: null` / `outputs: null` and cost nothing extra.
- Hashing runs on the executing backend (over SSH, or locally for the local backend), so the hashes describe the files the task actually touched.

## Getting a manifest

- **Run results**: `GET /api/v1/runs/{run_id}` includes a `manifest` object on every terminal item.
- **Standalone**: `GET /api/v1/runs/{run_id}/tasks/{task_id}/manifest` returns just the document. Non-terminal tasks answer `409` — a manifest is only final once the task is.
- **CLI**: `scripthut run manifest <run_id> <task_id>` prints the JSON.
- **From the probe, on cache hits**: [`POST /api/v1/tasks/probe`](caching.md#probing-the-cache-dry-run) verdicts include `input_hashes`, the `cache_key`, and — on a hit — the cached `output_hashes`, so a consumer can learn "these inputs would map to these outputs" without executing anything.
