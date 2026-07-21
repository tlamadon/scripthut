"""Data models for task runs."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal

from scripthut.config_schema import EnvRule, Stack


@dataclass
class TaskOutput:
    """A single file the task wrote under ``$SCRIPTHUT_OUTPUT_DIR``.

    Populated by ``RunManager._handle_task_outputs`` after the item
    transitions to COMPLETED. The content itself is not stored — the
    UI fetches files on demand via the file endpoint, mirroring how
    logs are read.
    """

    path: str  # Relative to the task's output dir (no leading slash, no ``..``)
    size: int  # Bytes, as reported by ``find -printf '%s'``
    # Classification by suffix. ``markdown`` files are rendered inline
    # in the Outputs subtab; ``image`` files become inline ``<img>``
    # tags pointing at the file endpoint; ``other`` files surface as a
    # download link so binary blobs (CSV, JSON, PDFs) are still
    # accessible without scripthut trying to interpret them.
    kind: Literal["markdown", "image", "other"]

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "size": self.size, "kind": self.kind}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TaskOutput":
        return cls(
            path=data["path"],
            size=int(data.get("size", 0)),
            kind=data.get("kind", "other"),
        )


class RunItemStatus(str, Enum):
    """Status of a run item.

    The submitted/queued split exists because they communicate very
    different operational signals: a SUBMITTED job is one the scheduler
    has not yet acknowledged (sbatch returned an id but we haven't seen
    it in squeue), while a QUEUED job is positively known to be in the
    scheduler's queue. Treating "missing from squeue" as failure only
    makes sense once we've ever observed the job — otherwise we have no
    evidence either way, and the right move is to ask sacct rather than
    guess.

    The SETTLING state exists for the same reason but for the
    opposite end: a job that *was* in the queue but vanished isn't
    necessarily done — sacct hasn't returned a row yet. Marking it
    COMPLETED on queue-vanish and then correcting to FAILED when
    accounting lands creates a transient window where an automation
    using ``run watch --exit-status`` can return success before the
    real verdict is in. SETTLING keeps the run non-terminal until
    accounting confirms what actually happened.
    """

    PENDING = "pending"           # Waiting to be submitted
    SUBMITTED = "submitted"       # sbatch returned; not yet observed in squeue
    QUEUED = "queued"             # Observed in squeue (PENDING); awaiting resources
    RUNNING = "running"           # Currently running
    SETTLING = "settling"         # Left the scheduler queue; awaiting accounting (sacct)
    COMPLETED = "completed"       # Finished successfully (accounting confirmed)
    FAILED = "failed"             # Failed or cancelled (accounting confirmed)
    DEP_FAILED = "dep_failed"     # Skipped because a dependency failed


@dataclass
class TaskDefinition:
    """Definition of a task from JSON input."""

    id: str
    name: str
    command: str
    working_dir: str = "~"
    partition: str = "normal"
    cpus: int = 1
    memory: str = "4G"
    time_limit: str = "1:00:00"
    dependencies: list[str] = field(default_factory=list)
    generates_source: str | None = None  # Path to JSON file this task creates on the backend
    output_file: str | None = None  # Custom stdout log path
    error_file: str | None = None  # Custom stderr log path
    env: list[EnvRule] = field(default_factory=list)  # Task-level env rules
    gres: str | None = None  # Slurm-style generic resource spec, e.g. "gpu:2" or "gpu:v100:1"
    image: str | None = None  # Container image URI (AWS Batch/ECS); overrides backend default
    # --- Result caching (see scripthut.runs.cache) ---
    # Paths/globs (relative to working_dir) whose *content* feeds the cache
    # key, so the task re-runs when its data changes. Empty means the key
    # depends only on command + env + git commit.
    inputs: list[str] = field(default_factory=list)
    # Paths/globs (relative to working_dir) that constitute the task's real
    # artifacts. These are tar'd and stored on a cache hit's *miss*, and
    # restored to working_dir on a hit. A task with no outputs is never
    # cached (there is nothing to restore).
    outputs: list[str] = field(default_factory=list)
    # Per-task opt-out: set false to always run even when the global cache
    # is enabled and outputs are declared.
    cache: bool = True
    # What identifies "the same work" for the cache key. The default
    # ("commit") folds the git commit in, so any commit busts the key — safe
    # when the command may read undeclared files from the repo. "inputs"
    # drops the commit, keying on command + env + declared input hashes
    # only, so unrelated commits reuse cached results. Only sound when the
    # task's ``inputs`` cover everything it reads (declare the code files
    # too, not just the data).
    cache_scope: Literal["commit", "inputs"] = "commit"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TaskDefinition":
        """Create TaskDefinition from dictionary."""
        if "environment" in data or "env_vars" in data:
            raise ValueError(
                f"Task '{data.get('id', '?')}': legacy 'environment' / 'env_vars' "
                "fields are no longer supported. Use the 'env' rule list instead "
                "(see scripthut docs on env resolution)."
            )
        env_raw = data.get("env", [])
        env_rules = [EnvRule.model_validate(r) for r in env_raw]
        cache_scope = data.get("cache_scope", "commit")
        if cache_scope not in ("commit", "inputs"):
            raise ValueError(
                f"Task '{data.get('id', '?')}': cache_scope must be 'commit' "
                f"or 'inputs', got {cache_scope!r}"
            )
        return cls(
            id=data["id"],
            name=data["name"],
            command=data["command"],
            working_dir=data.get("working_dir", "~"),
            partition=data.get("partition", "normal"),
            cpus=data.get("cpus", 1),
            memory=data.get("memory", "4G"),
            time_limit=data.get("time_limit", "1:00:00"),
            dependencies=data.get("deps", data.get("dependencies", [])),
            generates_source=data.get("generates_source"),
            output_file=data.get("output_file"),
            error_file=data.get("error_file"),
            env=env_rules,
            gres=data.get("gres"),
            image=data.get("image"),
            inputs=list(data.get("inputs", [])),
            outputs=list(data.get("outputs", [])),
            cache=bool(data.get("cache", True)),
            cache_scope=cache_scope,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON storage."""
        return {
            "id": self.id,
            "name": self.name,
            "command": self.command,
            "working_dir": self.working_dir,
            "partition": self.partition,
            "cpus": self.cpus,
            "memory": self.memory,
            "time_limit": self.time_limit,
            "dependencies": self.dependencies,
            "generates_source": self.generates_source,
            "output_file": self.output_file,
            "error_file": self.error_file,
            "env": [r.model_dump(by_alias=True, exclude_defaults=True) for r in self.env],
            "gres": self.gres,
            "image": self.image,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "cache": self.cache,
            "cache_scope": self.cache_scope,
        }

    @property
    def gpu_label(self) -> str | None:
        """Human-readable GPU summary from the gres spec, or None.

        Maps Slurm-style gres values to a short label for the UI:
        ``"gpu:2"`` → ``"2 GPU"``, ``"gpu:v100:1"`` → ``"1 GPU (v100)"``.
        Non-GPU gres specs (or a bare ``"gpu"``) fall back to the raw value.
        """
        if not self.gres:
            return None
        parts = self.gres.split(":")
        if parts[0] != "gpu":
            return self.gres
        # "gpu:<count>" or "gpu:<type>:<count>"
        if len(parts) == 2 and parts[1].isdigit():
            return f"{parts[1]} GPU"
        if len(parts) == 3 and parts[2].isdigit():
            return f"{parts[2]} GPU ({parts[1]})"
        return self.gres

    @staticmethod
    def parse_document(
        data: dict[str, Any] | list[Any],
    ) -> tuple[list["TaskDefinition"], list[EnvRule], dict[str, list[EnvRule]]]:
        """Parse a workflow-JSON document into tasks + doc-level env + env_groups.

        Accepts either the wrapped form ``{"tasks": [...], "env": [...], "env_groups": {...}}``
        or the bare list form ``[...]`` (env and env_groups are empty in that case).
        Top-level keys other than tasks/env/env_groups are ignored.
        """
        if isinstance(data, list):
            tasks_data = data
            env_raw: list[Any] = []
            groups_raw: dict[str, Any] = {}
        elif isinstance(data, dict) and "tasks" in data:
            tasks_data = data["tasks"]
            env_raw = data.get("env", []) or []
            groups_raw = data.get("env_groups", {}) or {}
        else:
            raise ValueError("JSON must be a list or dict with 'tasks' key")

        tasks = [TaskDefinition.from_dict(t) for t in tasks_data]
        doc_env = [EnvRule.model_validate(r) for r in env_raw]
        doc_groups: dict[str, list[EnvRule]] = {
            name: [EnvRule.model_validate(r) for r in rules]
            for name, rules in groups_raw.items()
        }
        return tasks, doc_env, doc_groups

    def get_output_path(self, run_id: str, log_dir: str) -> str:
        """Get the output log file path."""
        if self.output_file:
            return self.output_file
        return f"{log_dir}/scripthut_{run_id}_{self.id}.out"

    def get_error_path(self, run_id: str, log_dir: str) -> str:
        """Get the error log file path."""
        if self.error_file:
            return self.error_file
        return f"{log_dir}/scripthut_{run_id}_{self.id}.err"

    def get_output_dir(self, run_id: str, log_dir: str) -> str:
        """Directory the task writes structured outputs to (v0.11.0).

        Exposed to the user's command as ``$SCRIPTHUT_OUTPUT_DIR``. The
        script wrapper ``mkdir -p``s this before the command runs;
        anything written here is collected by ``_handle_task_outputs``
        when the item transitions to COMPLETED and shown in the
        per-task Outputs panel.
        """
        return f"{log_dir}/outputs/{run_id}/{self.id}"

    def get_run_summary_path(self, run_id: str, log_dir: str) -> str:
        """Per-task markdown fragment that feeds the run-wide Summary panel.

        Exposed as ``$SCRIPTHUT_RUN_SUMMARY``. All tasks' contributions
        live under ``outputs/<run_id>/_run/`` so the run-summary
        aggregator can find them with one ``ls``.
        """
        return f"{log_dir}/outputs/{run_id}/_run/{self.id}.md"

    def to_sbatch_script(
        self,
        run_id: str,
        log_dir: str,
        account: str | None = None,
        login_shell: bool = False,
        env_vars: dict[str, str] | None = None,
        extra_init: str = "",
    ) -> str:
        """Generate sbatch script for this task."""
        output_path = self.get_output_path(run_id, log_dir)
        error_path = self.get_error_path(run_id, log_dir)

        # Build account line if specified
        account_line = f"#SBATCH --account={account}\n" if account else ""

        shebang = "#!/bin/bash -l" if login_shell else "#!/bin/bash"

        # Build environment variable exports
        env_lines = ""
        if env_vars:
            export_lines = [f"export {key}=\"{value}\"" for key, value in env_vars.items()]
            if export_lines:
                env_lines = "\n".join(export_lines) + "\n\n"

        # Build extra init lines (e.g. module load)
        extra_init_lines = ""
        if extra_init:
            extra_init_lines = extra_init + "\n\n"

        gres_line = f"#SBATCH --gres={self.gres}\n" if self.gres else ""

        return f"""{shebang}
#SBATCH --job-name="{self.name}"
#SBATCH --partition={self.partition}
{account_line}#SBATCH --cpus-per-task={self.cpus}
#SBATCH --mem={self.memory}
#SBATCH --time={self.time_limit}
{gres_line}#SBATCH --output={output_path}
#SBATCH --error={error_path}

echo "=== ScriptHut Task: {self.name} ==="
echo "Task ID: {self.id}"
echo "Started: $(date)"
echo "Host: $(hostname)"
echo "Working dir: {self.working_dir}"
echo "=================================="
echo ""

{extra_init_lines}{env_lines}cd {self.working_dir}
{self.command}
EXIT_CODE=$?

echo ""
echo "=================================="
echo "Finished: $(date)"
echo "Exit code: $EXIT_CODE"
exit $EXIT_CODE
"""


@dataclass
class RunItem:
    """A single item in a run, wrapping a task with runtime state."""

    task: TaskDefinition
    status: RunItemStatus = RunItemStatus.PENDING
    job_id: str | None = None  # Scheduler-assigned job ID
    # Owning username as reported by the scheduler (squeue %u / qstat).
    # Set for external jobs (which may belong to any cluster user); None
    # for scripthut-submitted items, which belong to the configured user.
    user: str | None = None
    submitted_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    submit_script: str | None = None  # The generated submission script
    submit_output: str | None = None  # Raw stdout/stderr from sbatch/qsub/etc
    # Resource utilization (from scheduler accounting)
    cpu_efficiency: float | None = None  # 0-100%
    max_rss: str | None = None  # Peak memory, e.g. "1.2G"
    scheduler_state: str | None = None  # Confirmed final state from accounting
    # Numeric exit code from the scheduler's accounting (sacct's
    # ``ExitCode`` field). Populated once the item leaves SETTLING.
    # ``None`` means "not yet observed" — for SUBMITTED/QUEUED/RUNNING
    # items it's expected; for COMPLETED items it means accounting
    # didn't return a row within the grace window.
    exit_code: int | None = None
    # Structured output files the task wrote under
    # ``$SCRIPTHUT_OUTPUT_DIR``. Populated by ``_handle_task_outputs``
    # on the SETTLING → COMPLETED transition; empty list when the
    # task didn't emit anything (or output collection isn't supported
    # on this backend yet — Batch/EC2 in v0.11.0).
    outputs: list[TaskOutput] = field(default_factory=list)
    # ``True`` when the task wrote to ``$SCRIPTHUT_RUN_SUMMARY``. Lets
    # ``/runs/{id}/summary`` know which items contribute to the
    # aggregated panel without doing N stat probes at request time.
    has_run_summary: bool = False
    # Scheduler's reason a QUEUED job is still waiting (Slurm squeue %R,
    # e.g. "Resources", "Priority", "QOSMaxCpuPerUserLimit"). Surfaced as
    # a tooltip on the queued status pill. Set while the item is QUEUED,
    # cleared once it starts running or reaches a terminal state.
    pending_reason: str | None = None
    # --- Result caching (see scripthut.runs.cache) ---
    # The content-address (input/action hash) computed at submit time for a
    # cacheable task. ``None`` means the task wasn't cacheable (cache off,
    # no declared outputs, opted out, or input hashing failed). Persisted so
    # the completion path can store artifacts under the same key.
    cache_key: str | None = None
    # True when this item was satisfied by restoring a prior run's artifacts
    # from the cache instead of submitting a job. Such items never re-store.
    cache_hit: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON storage."""
        return {
            "task": self.task.to_dict(),
            "status": self.status.value,
            "job_id": self.job_id,
            "user": self.user,
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "error": self.error,
            "submit_script": self.submit_script,
            "submit_output": self.submit_output,
            "cpu_efficiency": self.cpu_efficiency,
            "max_rss": self.max_rss,
            "scheduler_state": self.scheduler_state,
            "exit_code": self.exit_code,
            "outputs": [o.to_dict() for o in self.outputs],
            "has_run_summary": self.has_run_summary,
            "pending_reason": self.pending_reason,
            "cache_key": self.cache_key,
            "cache_hit": self.cache_hit,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RunItem":
        """Deserialize from dictionary."""

        def parse_dt(val: str | None) -> datetime | None:
            if not val:
                return None
            dt = datetime.fromisoformat(val)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt

        return cls(
            task=TaskDefinition.from_dict(data["task"]),
            status=RunItemStatus(data["status"]),
            job_id=data.get("job_id") or data.get("slurm_job_id"),
            user=data.get("user"),
            submitted_at=parse_dt(data.get("submitted_at")),
            started_at=parse_dt(data.get("started_at")),
            finished_at=parse_dt(data.get("finished_at")),
            error=data.get("error"),
            submit_script=data.get("submit_script") or data.get("sbatch_script"),
            submit_output=data.get("submit_output"),
            cpu_efficiency=data.get("cpu_efficiency"),
            max_rss=data.get("max_rss"),
            scheduler_state=data.get("scheduler_state") or data.get("sacct_state"),
            exit_code=data.get("exit_code"),
            outputs=[TaskOutput.from_dict(o) for o in data.get("outputs", [])],
            has_run_summary=bool(data.get("has_run_summary", False)),
            pending_reason=data.get("pending_reason"),
            cache_key=data.get("cache_key"),
            cache_hit=bool(data.get("cache_hit", False)),
        )

    @property
    def status_class(self) -> str:
        """Return CSS class for status styling."""
        status_classes = {
            RunItemStatus.PENDING: "text-gray-500",
            # SUBMITTED is intentionally dimmer than QUEUED — it
            # signals "we don't have positive evidence the scheduler
            # has this yet"; QUEUED says "scheduler has it, waiting
            # for resources." Different operational signals.
            RunItemStatus.SUBMITTED: "text-yellow-500",
            RunItemStatus.QUEUED: "text-yellow-700",
            RunItemStatus.RUNNING: "text-blue-600",
            # SETTLING reuses the running blue but a shade lighter to
            # signal "scheduler-side done, waiting on accounting" — not
            # a terminal state.
            RunItemStatus.SETTLING: "text-blue-400",
            RunItemStatus.COMPLETED: "text-green-600",
            RunItemStatus.FAILED: "text-red-600",
            RunItemStatus.DEP_FAILED: "text-orange-600",
        }
        return status_classes.get(self.status, "text-gray-500")


class RunStatus(str, Enum):
    """Overall status of a run."""

    PENDING = "pending"  # Not started yet
    RUNNING = "running"  # Has running or submitted items
    COMPLETED = "completed"  # All items completed successfully
    FAILED = "failed"  # Some items failed
    CANCELLED = "cancelled"  # Run was cancelled


@dataclass
class Run:
    """A single execution of tasks (formerly Queue)."""

    id: str
    workflow_name: str
    backend_name: str
    created_at: datetime
    items: list[RunItem]
    max_concurrent: int | None
    log_dir: str = ""  # Directory for log files on the remote backend
    account: str | None = None  # Slurm account to charge jobs to
    login_shell: bool = False  # Use #!/bin/bash -l shebang
    commit_hash: str | None = None  # Git commit hash if run from a git workflow
    git_repo: str | None = None  # Git repo URL if run from a git workflow or git source
    git_branch: str | None = None  # Git branch if run from a git workflow or git source
    # Document-level env rules / groups parsed from the workflow JSON itself
    # (the generator's stdout, or a generates_source JSON). Slot between the
    # workflow-config env and the per-task env in the resolver chain.
    doc_env: list[EnvRule] = field(default_factory=list)
    doc_env_groups: dict[str, list[EnvRule]] = field(default_factory=dict)
    # Stacks available to ``stacks:`` env-rule references for this run.
    # Pre-merged at submit time (server config | source repo's project
    # YAML, source wins on collision) so the resolver doesn't have to
    # know about the merge layers.
    doc_stacks: dict[str, Stack] = field(default_factory=dict)
    # Coding-agent runs (one task launching a Claude Code session in a fresh
    # clone). ``agent_mode`` is "remote" (claude.ai-driven server session) or
    # "tui" (interactive TUI attached via the browser terminal). Drives the
    # mode-aware banner / controls on the run detail page.
    agent_session: bool = False
    agent_mode: str | None = None
    agent_session_name: str | None = None

    @property
    def status(self) -> RunStatus:
        """Calculate overall run status."""
        if not self.items:
            return RunStatus.COMPLETED

        statuses = [item.status for item in self.items]

        terminal_failures = (RunItemStatus.FAILED, RunItemStatus.DEP_FAILED)

        # Check for failures
        if any(s in terminal_failures for s in statuses):
            if all(s in (*terminal_failures, RunItemStatus.COMPLETED) for s in statuses):
                return RunStatus.FAILED
            if all(s in (*terminal_failures, RunItemStatus.COMPLETED, RunItemStatus.PENDING) for s in statuses):
                pending_items = [item for item in self.items if item.status == RunItemStatus.PENDING]
                if pending_items and all(self.get_failed_deps(item) for item in pending_items):
                    return RunStatus.FAILED

        # Check if all completed
        if all(s == RunItemStatus.COMPLETED for s in statuses):
            return RunStatus.COMPLETED

        # Check if any running, queued, submitted, or settling. SETTLING
        # is non-terminal by design: the scheduler is done but accounting
        # hasn't confirmed COMPLETED vs FAILED yet, so the run must not
        # report a terminal status (otherwise `run watch --exit-status`
        # could exit 0 before the real verdict lands).
        if any(
            s in (
                RunItemStatus.RUNNING, RunItemStatus.QUEUED,
                RunItemStatus.SUBMITTED, RunItemStatus.SETTLING,
            )
            for s in statuses
        ):
            return RunStatus.RUNNING

        # All pending
        return RunStatus.PENDING

    @property
    def progress(self) -> tuple[int, int]:
        """Return (completed_count, total_count)."""
        completed = sum(
            1 for item in self.items
            if item.status in (RunItemStatus.COMPLETED, RunItemStatus.FAILED, RunItemStatus.DEP_FAILED)
        )
        return (completed, len(self.items))

    @property
    def progress_percent(self) -> int:
        """Return progress as percentage."""
        completed, total = self.progress
        if total == 0:
            return 100
        return int((completed / total) * 100)

    @property
    def status_counts(self) -> dict[str, int]:
        """Count of items per status value, e.g. {"completed": 3, "running": 2}."""
        counts: dict[str, int] = {}
        for item in self.items:
            counts[item.status.value] = counts.get(item.status.value, 0) + 1
        return counts

    @property
    def running_count(self) -> int:
        """Count of items that consume a concurrency slot.

        Includes SUBMITTED (waiting for scheduler ack), QUEUED (in
        scheduler queue), RUNNING, and SETTLING (job vanished from
        queue, awaiting sacct confirmation — still counts since the
        scheduler resources may still be settling on the cluster side).
        The concurrency cap exists to avoid drowning the scheduler in
        submissions, so anything we've already submitted counts — even
        if the scheduler hasn't picked it up yet.
        """
        return sum(
            1 for item in self.items
            if item.status in (
                RunItemStatus.RUNNING,
                RunItemStatus.QUEUED,
                RunItemStatus.SUBMITTED,
                RunItemStatus.SETTLING,
            )
        )

    @property
    def pending_count(self) -> int:
        """Count of pending items."""
        return sum(1 for item in self.items if item.status == RunItemStatus.PENDING)

    @property
    def completed_count(self) -> int:
        """Count of completed items."""
        return sum(1 for item in self.items if item.status == RunItemStatus.COMPLETED)

    @property
    def failed_count(self) -> int:
        """Count of failed items."""
        return sum(1 for item in self.items if item.status == RunItemStatus.FAILED)

    @property
    def dep_failed_count(self) -> int:
        """Count of items that failed due to dependency failure."""
        return sum(1 for item in self.items if item.status == RunItemStatus.DEP_FAILED)

    def are_deps_satisfied(self, item: RunItem) -> bool:
        """Check if all dependencies of an item are completed."""
        if not item.task.dependencies:
            return True
        for dep_id in item.task.dependencies:
            dep_item = self.get_item_by_task_id(dep_id)
            if dep_item is None or dep_item.status != RunItemStatus.COMPLETED:
                return False
        return True

    def get_failed_deps(self, item: RunItem) -> list[str]:
        """Return list of dependency IDs that have failed or dep_failed."""
        failed = []
        for dep_id in item.task.dependencies:
            dep_item = self.get_item_by_task_id(dep_id)
            if dep_item is not None and dep_item.status in (
                RunItemStatus.FAILED, RunItemStatus.DEP_FAILED
            ):
                failed.append(dep_id)
        return failed

    def get_item_by_task_id(self, task_id: str) -> RunItem | None:
        """Get a run item by its task ID."""
        for item in self.items:
            if item.task.id == task_id:
                return item
        return None

    def uncascade_dep_failures(self) -> list[RunItem]:
        """Reset DEP_FAILED items whose dependencies are no longer failed.

        Runs to a fixed point so a chain of corrections propagates (e.g.
        a sacct correction on the root un-fails a child, which then
        un-fails its own child). An item is eligible iff every dep is
        present in the run and not in {FAILED, DEP_FAILED}.

        Returns the items that were reset to PENDING so the caller can
        persist / log them.
        """
        reset_items: list[RunItem] = []
        changed = True
        while changed:
            changed = False
            for item in self.items:
                if item.status != RunItemStatus.DEP_FAILED:
                    continue
                if self.get_failed_deps(item):
                    continue
                item.status = RunItemStatus.PENDING
                item.error = None
                item.finished_at = None
                reset_items.append(item)
                changed = True
        return reset_items

    def get_item_by_job_id(self, job_id: str) -> RunItem | None:
        """Get a run item by its scheduler job ID."""
        for item in self.items:
            if item.job_id == job_id:
                return item
        return None

    @property
    def total_cpu_hours(self) -> float:
        """Total CPU hours accumulated across all items so far."""
        now = datetime.now(timezone.utc)
        total_seconds = 0.0
        for item in self.items:
            if item.started_at is None:
                continue
            end = item.finished_at if item.finished_at else now
            elapsed = (end - item.started_at).total_seconds()
            if elapsed > 0:
                total_seconds += elapsed * item.task.cpus
        return total_seconds / 3600.0

    @property
    def status_class(self) -> str:
        """Return CSS class for status styling."""
        status_classes = {
            RunStatus.PENDING: "text-gray-500",
            RunStatus.RUNNING: "text-blue-600",
            RunStatus.COMPLETED: "text-green-600",
            RunStatus.FAILED: "text-red-600",
            RunStatus.CANCELLED: "text-gray-600",
        }
        return status_classes.get(self.status, "text-gray-500")
