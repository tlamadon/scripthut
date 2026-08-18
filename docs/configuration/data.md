# Data Dependencies

A workflow can depend on a directory that lives on the **scripthut host** rather than in the git repo. ScriptHut copies it to the backend the first time a workflow asks for it and reuses that copy afterwards — clone-if-absent, applied to data instead of code. Nothing is ever `scp`'d by hand.

Datasets are declared in the **user-global** `scripthut.yaml`; a local path is a fact about the machine, not the project. `datasets:` in a project-local file raises `ConfigError`.

```yaml
datasets:
  - name: sales-raw          # -> DATA_SALES_RAW
    path: ~/data/sales       # directory on the scripthut host
    # root: /scratch/you     # optional: override this backend's dataset_dir
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Identifier used by `data:` and in `DATA_<NAME>`. Starts with a letter, then letters, digits, `_` or `-`. Must be unique; `dir` is reserved, since its variable would collide with `DATA_DIR`. |
| `path` | path | **required** | Directory on the scripthut host. Relative paths resolve against the config file's directory, never the CWD. Must be a non-empty directory. |
| `root` | string | backend `dataset_dir` | Parent directory on the backend. Absolute, or `~/`-relative and expanded against the backend's `$HOME`. |
| `timeout` | integer | `86400` | Wall-clock limit in seconds for one transfer. |

---

## Where It Lands

Per-cluster, set on the backend beside `clone_dir`:

```yaml
backends:
  - name: mercury
    type: slurm
    clone_dir: ~/scripthut-repos
    dataset_dir: /scratch/your_username   # default: ~/scripthut-data
```

Resolution is config only, two layers: the dataset's `root` if set, otherwise the backend's `dataset_dir`. Nothing is read from the cluster environment, so the destination follows from the YAML alone — the same whether submitting, previewing with `--dry-run`, or scanning disk.

Paths must be literal: `$USER` is never expanded, and only `A-Z a-z 0-9 _ . - /` are accepted, so spaces are rejected. A leading `~/` is the exception, expanded against the backend's `$HOME` so the destination is absolute (`export DATA_DIR="~/x"` would not expand a tilde).

The remote home directory *itself* and any path inside a clone directory are refused. Subdirectories of home are fine, and are the default.

!!! warning "The default is home, which has a quota"
    `~/scripthut-data` mirrors `clone_dir` and suits small data. HPC home quotas are usually far below real dataset sizes, and filling one breaks everything else you are running. Set `dataset_dir` to scratch before staging anything large.

---

## Using a Dataset

The workflow document names it at the top level, beside `tasks`:

```json
{
  "data": ["sales-raw"],
  "tasks": [ { "id": "fit", "command": "python fit.py --data $DATA_DIR" } ]
}
```

Each dataset is injected as `DATA_<NAME>` — uppercased, non-alphanumerics to `_` — plus `DATA_DIR` when the workflow uses exactly one. These avoid the `SCRIPTHUT_` prefix deliberately: that namespace cannot be set by env rules and is stripped from cache keys, so a destination there would vanish silently and stop invalidating stale results.

Never write into `$DATA_DIR`. The copy is shared by every run whose data hashes the same; write results to the working directory.

---

## Staging

ScriptHut hashes the local file list — relative paths and sizes, **not** contents — giving the destination `<root_or_dataset_dir>/<name>/<hash12>`.

If that directory exists the run reuses it and transfers nothing. Otherwise the run gains an item `_data.<name>` that every root task depends on. Staging runs on the daemon, so `workflow run` returns immediately and the copy is tracked like any other item (`scripthut run watch`). The transfer writes to a per-run staging directory, verifies it against the manifest, then moves it into place, so a partial copy is never mistaken for a complete one. Concurrent runs wanting the same missing dataset are serialized.

---

## When the Data Changes

The destination carries the hash, so editing the local directory yields a new one: the next run stages a fresh copy beside the old rather than mutating data another run may be reading. Downstream cache keys change with it.

Old copies persist until removed. `scripthut disk scan` lists them under **Staged datasets**:

| Class | Meaning |
|-------|---------|
| `current` | Matches a configured dataset's present hash. |
| `superseded` | Still configured, but the local tree now hashes differently. Safe to delete. |
| `unconfigured` | No configured dataset claims it. Possibly the only copy on that backend. |

Remove them with `scripthut disk clean` or the disk page. Unconfigured copies are excluded from the bulk sweep: an orphaned clone is re-clonable from git, an orphaned dataset copy may not be.

---

## Containers

A locally-built image stages the same way. Point `path` at the **directory** holding the `.sif` (a file path is rejected):

```bash
apptainer exec $DATA_MY_CONTAINER/analysis.sif python run.py
```

---

## Limits and Failures

- Staging needs a filesystem, so `data:` fails on API-only backends (AWS Batch, EC2).
- A document produced by `generates_source` may not declare `data:`; dependencies must be known before the run starts.
- The `_data.` task-id prefix is reserved.
- The tree may not contain directory symlinks, broken file symlinks, or file symlinks pointing outside it.

A failed staging item's `error` names the cause; everything downstream is `dep_failed`. Fix it and submit a new run rather than chasing dependents. The partial copy is left at `<dest>.staging-<run-id>` for inspection and reclaimed by the next attempt or `disk clean`.

```bash
scripthut run view "$RUN_ID" --json | jq -r '.items[] | "\(.task.id)\t\(.status)\t\(.error // "-")"'
```
