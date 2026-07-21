"""The per-task manifest: a stable, versioned record of what a task did.

Every completed task can be summarized as a single self-contained JSON
document: *these exact input hashes, through this command, produced these
exact output hashes* — plus who executed it and when. Scripthut already
computes every ingredient (input hashing at submit, output hashing at
completion, cache key, timestamps); this module is the one place that
assembles them into a documented shape downstream consumers can rely on.

Schema — ``manifest_version: 1``::

    {
      "manifest_version": 1,
      "task":   {"id", "name", "command", "working_dir"},
      "inputs":  {"<path>": "<sha256>", ...} | null,
      "outputs": {"<path>": "<sha256>", ...} | null,
      "cache":  {"key", "scope", "hit"},
      "executor": {"backend", "type", "job_id"},
      "run":    {"id", "workflow", "commit", "branch"},
      "exit_code": int | null,
      "timing": {"submitted_at", "started_at", "finished_at",
                 "duration_seconds"},
    }

Guarantees:

- ``inputs`` / ``outputs`` map declared paths to sha256 content hashes.
  ``null`` (as opposed to ``{}``) means "not hashed" — the task declared
  nothing to hash, or hashing wasn't possible; a verifier must treat
  ``null`` as unverifiable, never as "empty and fine".
- ``outputs`` on a cache hit is carried from the stored cache manifest,
  so hit and miss report the same content hashes for the same work.
- Fields are only ever *added* within a major ``manifest_version``;
  existing fields never change meaning.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from scripthut.runs.models import Run, RunItem

TASK_MANIFEST_VERSION = 1


def build_task_manifest(
    run: "Run", item: "RunItem", *, backend_type: str | None = None,
) -> dict[str, Any]:
    """Assemble the versioned manifest for one run item.

    Callable at any point in the item's life — fields the run hasn't
    produced yet are ``null`` — but the intended consumption point is a
    terminal item, where the hash and timing fields are final.
    ``backend_type`` is the backend's config ``type`` ("slurm", "local",
    …); the caller resolves it since the run only stores the name.
    """
    task = item.task
    duration: float | None = None
    if item.started_at and item.finished_at:
        duration = (item.finished_at - item.started_at).total_seconds()

    return {
        "manifest_version": TASK_MANIFEST_VERSION,
        "task": {
            "id": task.id,
            "name": task.name,
            "command": task.command,
            "working_dir": task.working_dir,
        },
        "inputs": item.input_hashes,
        "outputs": item.output_hashes,
        "cache": {
            "key": item.cache_key,
            "scope": task.cache_scope,
            "hit": item.cache_hit,
        },
        "executor": {
            "backend": run.backend_name,
            "type": backend_type,
            "job_id": item.job_id,
        },
        "run": {
            "id": run.id,
            "workflow": run.workflow_name,
            "commit": run.commit_hash,
            "branch": run.git_branch,
        },
        "exit_code": item.exit_code,
        "timing": {
            "submitted_at": item.submitted_at.isoformat() if item.submitted_at else None,
            "started_at": item.started_at.isoformat() if item.started_at else None,
            "finished_at": item.finished_at.isoformat() if item.finished_at else None,
            "duration_seconds": duration,
        },
    }
