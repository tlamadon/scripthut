# Result Caching

ScriptHut can skip a task entirely when an earlier run already did the same work, restoring the earlier run's output files instead of resubmitting the job. The cache is **off by default** and **opt-in per task**: only tasks that declare `outputs` participate, and only when the server has a cache store configured.

Caching is Bazel-style content addressing, split across two stores inside one S3-compatible bucket/prefix:

- **Action cache (`ac/`)** — maps a *cache key* (what work was requested) to a small JSON manifest describing what that work produced.
- **CAS (`cas/`)** — content-addressed tarballs of output files. Identical artifacts are stored once and shared across runs, branches, and clusters.

All hashing and artifact transfer run **cluster-side over SSH** — the scripthut host only orchestrates. Only SSH-based backends (Slurm, PBS, local) participate; API-only backends (AWS Batch, EC2) silently skip caching.

---

## Server Configuration

The cache store is shared infrastructure, so it is configured in the server's global `scripthut.yaml` — never in a project-local file:

```yaml
cache:
  enabled: true
  store: s3://my-bucket/scripthut-cache   # or an rclone remote
  tool: aws                               # "aws" (default) or "rclone"
```

The chosen `tool` must be on the `PATH` of the backend (the cluster login node), since that's where transfers run.

---

## Task Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `inputs` | array | `[]` | Paths/globs (relative to `working_dir`) whose **content** feeds the cache key. The task re-runs when any of these files change. |
| `outputs` | array | `[]` | Paths/globs (relative to `working_dir`) that are the task's real artifacts. Stored to the cache on completion, restored into `working_dir` on a hit. **A task with no `outputs` is never cached.** |
| `cache` | bool | `true` | Per-task opt-out. Set `false` to always run this task even when the global cache is enabled. |
| `cache_scope` | string | `"commit"` | What identifies "the same work". `"commit"` folds the git commit into the key; `"inputs"` drops it. See [Cache scope](#cache-scope-commit-vs-inputs). |

Example:

```json
{
  "id": "train",
  "name": "Train model",
  "command": "python train.py",
  "inputs": ["data/train.csv", "train.py"],
  "outputs": ["model.pt", "metrics.json"]
}
```

---

## The Cache Key

Before a cacheable task is submitted, scripthut computes a key from:

- the task `command`
- the task's resolved environment (minus volatile `SCRIPTHUT_*` runtime variables)
- the run's git commit hash (unless `cache_scope: "inputs"`)
- the sha256 of every file matched by `inputs`

Resource settings (`cpus`, `memory`, `partition`, `time_limit`) are deliberately **excluded** — resizing a job must not bust the cache.

If the key matches a previous successful run, that run's `outputs` are restored into the working directory and the task is marked completed without touching the scheduler (it shows as `CACHED` in the run view). On a miss the task runs normally and its outputs are stored under the key when it completes.

Safety properties:

- **Failures are never reused.** A cached entry with a non-zero exit code is ignored and the task re-runs.
- **Unverifiable inputs mean no caching.** If input hashing fails or a declared `inputs` pattern matches no files, the task runs unconditionally rather than risk a stale hit.
- **A failed restore falls back to running the task.**

---

## Cache Scope: `commit` vs `inputs`

By default (`cache_scope: "commit"`), the git commit is part of the key, so **any** new commit invalidates every task's cache — even commits that touched nothing the task reads. That is the safe default, because a task's command may read files from the repo that it never declared as `inputs`.

`cache_scope: "inputs"` drops the commit from the key. Two submissions with identical command, environment, and input hashes then share one cache entry **regardless of which commit they were submitted from**. This gives per-task invalidation granularity: only changes to the declared inputs (or the command/env) re-run the task.

```json
{
  "id": "preprocess",
  "name": "Preprocess data",
  "command": "python preprocess.py",
  "inputs": ["data/raw.csv", "preprocess.py"],
  "outputs": ["data/clean.parquet"],
  "cache_scope": "inputs"
}
```

!!! warning "Only sound with complete inputs"
    `cache_scope: "inputs"` is only correct when `inputs` covers **everything the command reads** — including the code files themselves (`preprocess.py` above), any modules they import, and config files. Anything the command reads but doesn't declare can change without busting the key, and you'd get a stale hit. When in doubt, keep the default.
