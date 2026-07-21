"""Run manager for task submission and tracking."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import shlex
import uuid
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import TYPE_CHECKING

from scripthut.backends.base import JobBackend
from scripthut.config_schema import (
    AgentConfig,
    EnvRule,
    GitSourceConfig,
    PathSourceConfig,
    ScriptHutConfig,
    Stack,
)
from scripthut.models import JobState
from scripthut.runs.cache import CacheManager
from scripthut.runs.env import resolve_for_task
from scripthut.runs.models import (
    Run,
    RunItem,
    RunItemStatus,
    RunStatus,
    TaskDefinition,
    TaskOutput,
)
from scripthut.sources.git import is_safe_branch_name
from scripthut.ssh.client import SSHClient

if TYPE_CHECKING:
    from scripthut.runs.storage import RunStorageManager

logger = logging.getLogger(__name__)

# Marker stored on ``item.error`` when a SUBMITTED job vanishes from the
# scheduler queue without ever being observed RUNNING.  Used by the polling
# layer to flip the item back to COMPLETED if the accounting DB later
# confirms it actually ran successfully (covers ultra-fast jobs that finish
# between two poll cycles).
DISAPPEARED_BEFORE_RUNNING_MARKER = "Job vanished before being observed running"

# Grace period (seconds) before *consulting sacct* for a SUBMITTED item
# that's missing from squeue. We don't mark it FAILED on absence; we
# only start asking accounting whether the job ran or never made it.
# 60s covers typical poll intervals (5–60s) with headroom.
SUBMIT_TO_FAIL_GRACE_SECONDS = 60.0

# How long (seconds) we'll keep a SUBMITTED item alive when *neither*
# squeue nor sacct has any record of it. The rare real "vanished"
# case: sbatch returned an id but the scheduler dropped the job. After
# this we mark FAILED with an actionable message rather than wait
# indefinitely.
SUBMITTED_NO_RECORD_TIMEOUT_SECONDS = 300.0

# Message used for items that sbatch accepted but neither squeue nor
# sacct ever acknowledged. Different from the legacy "vanished before
# running" marker — that one was speculative; this one is evidence-based
# (we asked sacct and it had no record).
SCHEDULER_NO_RECORD_MARKER = (
    "sbatch accepted the job but the scheduler has no record of it"
)

# How long (seconds) we let a SETTLING item wait for sacct before
# giving up and marking it COMPLETED-without-exit-code. The job
# scheduler said the job is done; we're only waiting on accounting.
# Long-grace because slurm DBs can lag significantly under load, and
# the alternative (mark FAILED on a hung accounting DB) would be
# worse — we'd be inventing failures from a DB-availability problem.
SETTLING_NO_RECORD_TIMEOUT_SECONDS = 600.0

# Marker stored on ``item.error`` when SETTLING fell back to
# COMPLETED because sacct never returned a row. Lets downstream
# consumers (especially agents reading exit_code) know the verdict
# isn't accounting-confirmed.
SETTLING_UNCONFIRMED_MARKER = (
    "marked COMPLETED after the scheduler queue clear but accounting "
    "never returned a row — exit code unknown"
)


def probe_summary(results: list[dict]) -> dict:
    """Aggregate per-task probe verdicts into hit/miss/uncacheable counts.

    Shared by the API endpoint and the CLI's local mode so the two
    responses can't drift.
    """
    return {
        "hit": sum(1 for r in results if r["hit"]),
        "miss": sum(1 for r in results if r["cacheable"] and not r["hit"]),
        "uncacheable": sum(1 for r in results if not r["cacheable"]),
    }


async def _run_local_shell(command: str, timeout: float = 60.0) -> tuple[str, str, int]:
    """Run ``command`` locally (``sh -c``) and return ``(stdout, stderr, exit_code)``.

    Used for workflows whose backend has no SSH connection (e.g. AWS Batch) —
    the command runs on the scripthut host instead of a remote login node.
    """
    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise ValueError(f"Local command timed out after {timeout}s: {command}")
    return (
        stdout_b.decode("utf-8", errors="replace"),
        stderr_b.decode("utf-8", errors="replace"),
        proc.returncode if proc.returncode is not None else -1,
    )


class RunManager:
    """Manages runs - fetching, submitting, and tracking."""

    def __init__(
        self,
        config: ScriptHutConfig,
        backends: dict[str, SSHClient],
        storage: RunStorageManager | None = None,
        job_backends: dict[str, JobBackend] | None = None,
    ) -> None:
        """Initialize with config, SSH backends, and optional persistent storage."""
        self.config = config
        self.backends = backends
        self.runs: dict[str, Run] = {}
        self.storage = storage
        self.job_backends = job_backends or {}
        # Task result cache (no-op unless config.cache.enabled + store set).
        # ``getattr`` keeps lightweight test config stubs (which omit the
        # field) working — they fall back to a disabled default.
        cache_cfg = getattr(self.config, "cache", None)
        if cache_cfg is None:
            from scripthut.config_schema import CacheConfig
            cache_cfg = CacheConfig()
        self.cache_manager = CacheManager(cache_cfg)
        # SSE event bus: version counter + Event per run
        self._run_versions: dict[str, int] = {}
        self._run_events: dict[str, asyncio.Event] = {}

    def _resolve_environment(
        self, run: Run, task: TaskDefinition
    ) -> tuple[dict[str, str], str]:
        """Resolve env vars and extra_init for a task in the context of a run.

        Chains backend → server → workflow → task env rules and applies them
        against a seed of SCRIPTHUT_* runtime variables. Returns the fully
        merged env dict (ready to pass to ``generate_script``) and the
        concatenated extra_init bash text.
        """
        git_repo = run.git_repo
        git_branch = run.git_branch
        return resolve_for_task(
            self.config,
            backend_name=run.backend_name,
            workflow_name=run.workflow_name,
            run_id=run.id,
            created_at=run.created_at,
            task=task,
            git_repo=git_repo,
            git_branch=git_branch,
            git_sha=run.commit_hash,
            doc_env=run.doc_env,
            doc_env_groups=run.doc_env_groups,
            doc_stacks=run.doc_stacks,
        )

    @staticmethod
    def _resolve_wildcard_deps(tasks: list[TaskDefinition]) -> None:
        """Expand wildcard patterns in task dependencies to matching task IDs."""
        task_ids = [t.id for t in tasks]
        for task in tasks:
            expanded: list[str] = []
            for dep in task.dependencies:
                if any(c in dep for c in ("*", "?", "[")):
                    matches = [tid for tid in task_ids if fnmatch(tid, dep) and tid != task.id]
                    if not matches:
                        raise ValueError(
                            f"Task '{task.id}': wildcard dep '{dep}' matches no tasks"
                        )
                    expanded.extend(matches)
                else:
                    expanded.append(dep)
            task.dependencies = expanded

    @staticmethod
    def _validate_dependencies(tasks: list[TaskDefinition]) -> None:
        """Validate task dependencies (no missing refs, no cycles)."""
        task_ids = {t.id for t in tasks}

        for task in tasks:
            for dep_id in task.dependencies:
                if dep_id not in task_ids:
                    raise ValueError(
                        f"Task '{task.id}' depends on '{dep_id}', which does not exist"
                    )
                if dep_id == task.id:
                    raise ValueError(
                        f"Task '{task.id}' depends on itself"
                    )

        # Detect circular dependencies via DFS
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {t.id: WHITE for t in tasks}
        deps_map = {t.id: t.dependencies for t in tasks}

        def dfs(node: str, path: list[str]) -> None:
            """DFS cycle detection using three-color marking."""
            color[node] = GRAY
            path.append(node)
            for dep in deps_map[node]:
                if color[dep] == GRAY:
                    cycle_start = path.index(dep)
                    cycle = path[cycle_start:] + [dep]
                    raise ValueError(
                        f"Circular dependency detected: {' -> '.join(cycle)}"
                    )
                if color[dep] == WHITE:
                    dfs(dep, path)
            path.pop()
            color[node] = BLACK

        for task in tasks:
            if color[task.id] == WHITE:
                dfs(task.id, [])

    async def _handle_generates_source(
        self, run: Run, item: RunItem
    ) -> None:
        """Read a generated workflow JSON and append tasks to the run."""
        path = item.task.generates_source
        if not path:
            return

        logger.info(
            f"Task '{item.task.id}' completed with generates_source: {path}"
        )

        ssh_client = self.get_ssh_client(run.backend_name)
        if ssh_client is None:
            logger.error(
                f"No SSH client for backend '{run.backend_name}' — "
                f"cannot read generates_source '{path}'"
            )
            return

        # Resolve relative paths against the task's working_dir
        if not path.startswith("/") and not path.startswith("~"):
            path = f"{item.task.working_dir}/{path}"

        stdout, stderr, exit_code = await ssh_client.run_command(f"cat {path}")
        if exit_code != 0:
            logger.error(
                f"Failed to read generates_source '{path}': {stderr}"
            )
            return

        try:
            data = json.loads(stdout)
        except json.JSONDecodeError as e:
            logger.error(
                f"Invalid JSON in generates_source '{path}': {e}"
            )
            return

        try:
            new_tasks, child_doc_env, child_doc_groups = (
                TaskDefinition.parse_document(data)
            )
        except ValueError as e:
            logger.error(f"Invalid generates_source JSON '{path}': {e}")
            return

        # Generated docs can extend the run's doc-level env in addition to
        # adding new tasks. New rules append after the existing ones; new
        # groups merge in (later definitions shadow earlier).
        if child_doc_env:
            run.doc_env.extend(child_doc_env)
        if child_doc_groups:
            run.doc_env_groups.update(child_doc_groups)

        # Resolve working_dir relative to the parent task's working_dir
        # (same as _resolve_working_dirs does with clone_dir)
        self._resolve_working_dirs(new_tasks, item.task.working_dir)

        # Resolve wildcard deps against ALL tasks in the run
        all_tasks = [ri.task for ri in run.items] + new_tasks
        all_task_ids = [t.id for t in all_tasks]

        for task in new_tasks:
            expanded: list[str] = []
            for dep in task.dependencies:
                if any(c in dep for c in ("*", "?", "[")):
                    matches = [
                        tid for tid in all_task_ids
                        if fnmatch(tid, dep) and tid != task.id
                    ]
                    if not matches:
                        logger.error(
                            f"Task '{task.id}': wildcard dep '{dep}' "
                            f"matches no tasks in generates_source '{path}'"
                        )
                        return
                    expanded.extend(matches)
                else:
                    expanded.append(dep)
            task.dependencies = expanded

        for task in new_tasks:
            for dep_id in task.dependencies:
                if dep_id not in all_task_ids:
                    logger.error(
                        f"Task '{task.id}' has unknown dependency '{dep_id}' "
                        f"in generates_source '{path}'"
                    )
                    return

        new_items = [RunItem(task=t) for t in new_tasks]
        run.items.extend(new_items)
        self._persist_run(run)

        logger.info(
            f"Appended {len(new_tasks)} tasks from generates_source "
            f"'{path}' to run '{run.id}'"
        )

        await self.process_run(run)

    # Maximum file size we'll surface in the per-task outputs panel.
    # Larger files are dropped from the listing — the user can still
    # access them through their own paths, scripthut just doesn't try
    # to embed or render them. Matches the size cap documented in the
    # v0.11.0 plan; revisit if real workloads need bigger payloads.
    _OUTPUTS_MAX_FILE_BYTES = 5 * 1024 * 1024

    # Cap the number of files the listing surfaces so a runaway
    # ``$SCRIPTHUT_OUTPUT_DIR`` (e.g. someone redirects a million-row
    # iteration into a per-iteration plot) doesn't bloat ``run.json``.
    # 200 is more than any reasonable summary; the ``+1`` head limit
    # lets us detect overflow.
    _OUTPUTS_MAX_FILES = 200

    @staticmethod
    def _classify_output(path: str) -> str:
        """Suffix-based ``TaskOutput.kind`` mapping. Permissive on case."""
        lower = path.lower()
        if lower.endswith((".md", ".markdown")):
            return "markdown"
        if lower.endswith((".png", ".jpg", ".jpeg", ".svg", ".gif", ".webp")):
            return "image"
        return "other"

    async def _handle_task_outputs(self, run: Run, item: RunItem) -> None:
        """Collect ``$SCRIPTHUT_OUTPUT_DIR`` contents + ``$SCRIPTHUT_RUN_SUMMARY``.

        Mirrors ``_handle_generates_source`` in shape: one SSH round-trip,
        parse, mutate the item, persist. v0.11.0 supports SSH backends
        (Slurm + PBS) only — backends without an SSH client (Batch /
        EC2 in their API-only modes) silently no-op so existing
        workflows keep working.

        The ``find`` command lists the output dir at depth ≤3 with a
        ``%P\\t%s\\n`` format (path-relative, then byte size), capped at
        ``_OUTPUTS_MAX_FILES + 1`` lines so we can detect overflow. The
        same SSH command also probes for ``$SCRIPTHUT_RUN_SUMMARY``
        existence so we don't pay a second round-trip — the result
        rides on stderr where the script writes either ``has-summary``
        or nothing, keeping the listing on stdout clean.

        Errors are non-fatal: a missing dir, an unreachable SSH
        connection, or unparseable lines result in an empty outputs
        list. The run continues regardless — outputs are decorative,
        not correctness-critical.
        """
        ssh_client = self.get_ssh_client(run.backend_name)
        if ssh_client is None:
            # Backend has no SSH (Batch / EC2-API) — skip silently.
            # Batch/EC2 output collection is the v2 work tracked
            # separately; not having it doesn't fail the run.
            return

        output_dir = item.task.get_output_dir(run.id, run.log_dir)
        run_summary_path = item.task.get_run_summary_path(run.id, run.log_dir)
        max_files_plus_one = self._OUTPUTS_MAX_FILES + 1
        # Single round-trip: list files + probe run-summary existence.
        # ``2>/dev/null`` swallows the "no such file" error case so a
        # task that didn't emit anything just produces empty stdout.
        cmd = (
            f"find {output_dir} -maxdepth 3 -type f "
            f"-printf '%P\\t%s\\n' 2>/dev/null | head -{max_files_plus_one}; "
            f"[ -f {run_summary_path} ] && echo HAS_SUMMARY >&2"
        )
        try:
            stdout, stderr, exit_code = await ssh_client.run_command(cmd)
        except Exception as e:
            logger.warning(
                f"Failed to list outputs for task '{item.task.id}' on "
                f"'{run.backend_name}': {e}"
            )
            return

        outputs: list[TaskOutput] = []
        truncated = False
        for line in stdout.splitlines():
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) != 2:
                continue
            path_rel, size_str = parts
            try:
                size = int(size_str)
            except ValueError:
                continue
            if size > self._OUTPUTS_MAX_FILE_BYTES:
                # Skip oversize files but log so users notice the cap.
                logger.info(
                    f"Task '{item.task.id}': output '{path_rel}' is "
                    f"{size} bytes; skipping from listing "
                    f"(max {self._OUTPUTS_MAX_FILE_BYTES})"
                )
                continue
            if len(outputs) >= self._OUTPUTS_MAX_FILES:
                truncated = True
                break
            outputs.append(TaskOutput(
                path=path_rel, size=size,
                kind=self._classify_output(path_rel),
            ))

        item.outputs = outputs
        item.has_run_summary = "HAS_SUMMARY" in (stderr or "")
        if outputs or item.has_run_summary:
            logger.info(
                f"Task '{item.task.id}': collected {len(outputs)} output "
                f"file(s){' (truncated)' if truncated else ''}, "
                f"run_summary={'yes' if item.has_run_summary else 'no'}"
            )
            self._persist_run(run)

    async def _after_item_completed(self, run: Run, item: RunItem) -> None:
        """Fan-out point for every "this item just reached COMPLETED" hook.

        Concentrating the calls here means callers (the three branches
        in ``main.poll_backend`` that transition items to COMPLETED:
        SETTLING→COMPLETED, SUBMITTED-past-grace, and the SETTLING
        long-grace fallback) don't have to remember to invoke each
        hook separately. New post-completion behavior should land
        here, not at the call sites.

        Order matters: ``generates_source`` may extend the run with
        more tasks, but outputs collection only touches the current
        item — running both is safe in either order. We keep
        generates_source first because it has been the historically
        established hook.
        """
        if item.task.generates_source:
            await self._handle_generates_source(run, item)
        await self._handle_task_outputs(run, item)
        # Persist the task's declared output artifacts to the result cache so
        # a future run with the same inputs can skip it. No-op for cache hits
        # and non-cacheable tasks.
        await self._maybe_store_cache(run, item)

    def get_ssh_client(self, backend_name: str) -> SSHClient | None:
        """Get SSH client for a backend."""
        return self.backends.get(backend_name)

    async def _get_git_root(self, ssh_client: SSHClient, working_dir: str) -> str:
        """Detect the git repository root for a working directory on the backend."""
        stdout, stderr, exit_code = await ssh_client.run_command(
            f"cd {working_dir} && git rev-parse --show-toplevel"
        )
        if exit_code != 0:
            raise ValueError(
                f"'{working_dir}' is not inside a git repository. "
                f"ScriptHut requires workflows to live in git repos."
            )
        return stdout.strip()

    # --- Git workflow support ---

    @staticmethod
    def _to_https_url(repo: str) -> str:
        """Convert git SSH URLs to HTTPS when possible.

        e.g. git@github.com:org/repo.git -> https://github.com/org/repo.git
        """
        if repo.startswith("git@"):
            # git@github.com:org/repo.git -> https://github.com/org/repo.git
            host_path = repo[4:]  # strip "git@"
            host, _, path = host_path.partition(":")
            if path:
                return f"https://{host}/{path}"
        return repo

    @staticmethod
    def _build_remote_git_ssh_command(remote_key_path: str | None) -> str:
        """Build GIT_SSH_COMMAND prefix for remote execution.

        Args:
            remote_key_path: Path to deploy key on the backend (or None).

        Returns:
            Shell prefix string to prepend to git commands, e.g.
            ``GIT_SSH_COMMAND="ssh -i /tmp/key ..." `` or empty string.
        """
        if not remote_key_path:
            return ""
        opts = "-o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new"
        return f'export GIT_SSH_COMMAND="ssh -i {remote_key_path} {opts}"; '

    async def _upload_deploy_key(
        self, ssh_client: SSHClient, local_key_path: Path
    ) -> str:
        """Upload a local deploy key to a temp file on the backend.

        The key content is base64-encoded before transmission over the
        encrypted SSH channel, then decoded into a temp file with 600
        permissions.

        Returns:
            The remote temp file path.
        """
        resolved = local_key_path.expanduser()
        if not resolved.exists():
            raise ValueError(f"Deploy key not found: {resolved}")
        key_content = resolved.read_text()
        key_b64 = base64.b64encode(key_content.encode()).decode()
        cmd = (
            "TMPKEY=$(mktemp /tmp/scripthut_key_XXXXXX) && "
            f"echo '{key_b64}' | base64 -d > $TMPKEY && "
            "chmod 600 $TMPKEY && echo $TMPKEY"
        )
        stdout, stderr, exit_code = await ssh_client.run_command(cmd)
        if exit_code != 0:
            raise ValueError(f"Failed to upload deploy key: {stderr}")
        return stdout.strip()

    async def _cleanup_deploy_key(
        self, ssh_client: SSHClient, remote_key_path: str
    ) -> None:
        """Remove a temporary deploy key from the backend."""
        await ssh_client.run_command(f"rm -f {remote_key_path}")

    async def _clone_git_repo(
        self,
        ssh_client: SSHClient,
        *,
        repo: str,
        branch: str,
        deploy_key: Path | None,
        clone_dir: str,
        postclone: str | None,
    ) -> tuple[str, str]:
        """Clone a git repo on the backend if not already present.

        Steps:
            1. Upload the local deploy key (if any) to a temp file on the backend.
            2. ``git ls-remote`` to resolve the branch HEAD commit hash.
            3. Clone into ``<clone_dir>/<short_hash>/`` if absent.
            4. Clean up the temp deploy key.

        Returns:
            ``(clone_path, short_hash)`` — the remote clone directory and the
            12-char commit hash prefix.
        """
        remote_key: str | None = None
        try:
            # 1. Upload deploy key if configured
            if deploy_key is not None:
                key_path = deploy_key.expanduser()
                remote_key = await self._upload_deploy_key(
                    ssh_client, key_path
                )

            git_ssh = self._build_remote_git_ssh_command(remote_key)
            # Use HTTPS for public repos (no deploy key) to avoid SSH key issues
            effective_repo = repo if remote_key else self._to_https_url(repo)

            # 2. Resolve HEAD commit hash
            cmd = f"{git_ssh}GIT_TERMINAL_PROMPT=0 git ls-remote {effective_repo} refs/heads/{branch}"
            stdout, stderr, exit_code = await ssh_client.run_command(cmd, timeout=30)
            if exit_code != 0 or not stdout.strip():
                raise ValueError(
                    f"Failed to resolve branch '{branch}' from "
                    f"'{repo}': {stderr}"
                )
            commit_hash = stdout.split()[0]
            short_hash = commit_hash[:12]

            # 3. Clone if not already present
            clone_path = f"{clone_dir}/{short_hash}"
            stdout, _, _ = await ssh_client.run_command(
                f"test -d {clone_path} && echo exists"
            )
            if "exists" not in stdout:
                logger.info(
                    f"Cloning {repo}@{branch} ({short_hash}) "
                    f"to {clone_path}"
                )
                cmd = (
                    f"{git_ssh}GIT_TERMINAL_PROMPT=0 git clone --branch {branch} "
                    f"--single-branch --depth 1 {effective_repo} {clone_path}"
                )
                _, stderr, exit_code = await ssh_client.run_command(
                    cmd, timeout=300
                )
                if exit_code != 0:
                    raise ValueError(f"Git clone failed: {stderr}")

                # Run postclone command if configured
                if postclone:
                    logger.info(f"Running postclone command in {clone_path}")
                    cmd = f"cd {clone_path} && {postclone}"
                    _, stderr, exit_code = await ssh_client.run_command(
                        cmd, timeout=300
                    )
                    if exit_code != 0:
                        raise ValueError(f"Postclone command failed: {stderr}")
            else:
                logger.info(
                    f"Reusing existing clone at {clone_path} ({short_hash})"
                )

            return clone_path, short_hash
        finally:
            # 4. Always clean up the temp deploy key
            if remote_key is not None:
                await self._cleanup_deploy_key(ssh_client, remote_key)

    def get_backend_account(self, backend_name: str) -> str | None:
        """Get the account for a backend (Slurm --account, PBS -A, etc.)."""
        backend = self.config.get_backend(backend_name)
        if backend and hasattr(backend, "account"):
            return backend.account  # type: ignore[union-attr]
        return None

    def get_backend_login_shell(self, backend_name: str) -> bool:
        """Get whether the backend uses login shell in submission scripts."""
        backend = self.config.get_backend(backend_name)
        if backend and hasattr(backend, "login_shell"):
            return backend.login_shell  # type: ignore[union-attr]
        return False

    async def _build_run(
        self,
        tasks: list[TaskDefinition],
        workflow_name: str,
        backend_name: str,
        max_concurrent: int | None,
        ssh_client: SSHClient | None,
        *,
        git_repo: str | None = None,
        git_branch: str | None = None,
        commit_hash: str | None = None,
        doc_env: list[EnvRule] | None = None,
        doc_env_groups: dict[str, list[EnvRule]] | None = None,
        doc_stacks: dict[str, Stack] | None = None,
    ) -> Run:
        """Build a Run: resolve deps, validate, create, persist, and start processing.

        Git-related kwargs must be provided here (not set after) because
        ``_build_run`` triggers task submission via ``process_run`` before
        returning — the generated container scripts need the git info at
        submit time.
        """
        if not tasks:
            raise ValueError(f"No tasks for workflow '{workflow_name}'")

        self._resolve_wildcard_deps(tasks)
        self._validate_dependencies(tasks)

        account = self.get_backend_account(backend_name)
        login_shell = self.get_backend_login_shell(backend_name)

        first_working_dir = tasks[0].working_dir
        if ssh_client is not None:
            try:
                git_root = await self._get_git_root(ssh_client, first_working_dir)
                log_dir = f"{git_root}/.scripthut/{workflow_name}/logs"
                logger.info(f"Git root: {git_root} — logs at {log_dir}")
            except ValueError:
                log_dir = f"~/.cache/scripthut/logs/{workflow_name}"
                logger.info(f"No git root for '{first_working_dir}' — logs at {log_dir}")
        else:
            # API-based backends have no filesystem — use a synthetic placeholder.
            log_dir = f"backend://{backend_name}/{workflow_name}"
            logger.info(f"Backend '{backend_name}' has no filesystem — logs via backend API")

        run_id = str(uuid.uuid4())[:8]
        run = Run(
            id=run_id,
            workflow_name=workflow_name,
            backend_name=backend_name,
            created_at=datetime.now(timezone.utc),
            items=[RunItem(task=task) for task in tasks],
            max_concurrent=max_concurrent,
            account=account,
            login_shell=login_shell,
            log_dir=log_dir,
            git_repo=git_repo,
            git_branch=git_branch,
            commit_hash=commit_hash,
            doc_env=list(doc_env or []),
            doc_env_groups=dict(doc_env_groups or {}),
            doc_stacks=dict(doc_stacks or {}),
        )

        self.runs[run_id] = run
        logger.info(f"Created run '{run_id}' with {len(tasks)} tasks")

        # Persist to storage
        self._persist_run(run)

        # Start submitting tasks
        await self.process_run(run)

        return run

    @staticmethod
    def _synthesize_stack_install_command(
        stack: Stack, hash_: str, rebuild: bool,
    ) -> str:
        """Render the bash that does the install dance for a task command.

        Reproduces ``StackManager.install``'s logic — sentinel check,
        cleanup, mkdir, ``STACK_DIR`` export, prep, touch ``.ready`` —
        but as a single inline script so the task can be submitted
        through the normal scheduler path (sbatch / qsub / Batch).

        Two design choices worth flagging:

        - **``~`` → ``$HOME`` substitution at synthesis time.** The
          existing ``StackManager`` quotes paths with ``shlex.quote``,
          which single-quotes tilde paths and would break ``~``
          expansion on the backend. We expand once at the Python layer
          so the emitted command works regardless of which bash
          invocation lands it.
        - **Idempotency lives in the command, not in a Python
          pre-check.** When a stack is already ready and ``rebuild`` is
          False the task exits 0 immediately — no Python round-trip
          required, and the task still shows up in ``run list`` as a
          successful run (so the user sees "I tried to install julia,
          it was already there"). Pre-flighting on the Python side
          would skip the run entirely, which is harder to observe.
        """
        from scripthut.stacks.manager import READY_SENTINEL

        cache_dir = stack.cache_dir.replace("~", "$HOME", 1)
        hash_dir = f"{cache_dir}/{stack.name}/{hash_}"
        rebuild_flag = "1" if rebuild else "0"
        prep = stack.prep.strip()

        if not prep:
            # Empty prep is legitimate (init-only stack that just wants a
            # stable directory). Make ready and call it a day.
            return (
                f'HASH_DIR="{hash_dir}"\n'
                f'mkdir -p "$HASH_DIR"\n'
                f'touch "$HASH_DIR/{READY_SENTINEL}"\n'
                f'echo "Stack {stack.name!r} marked ready at $HASH_DIR"\n'
            )

        return (
            f'HASH_DIR="{hash_dir}"\n'
            f'READY="$HASH_DIR/{READY_SENTINEL}"\n'
            f'REBUILD={rebuild_flag}\n'
            f'\n'
            f'# Fast path: already ready, not rebuilding.\n'
            f'if [ -f "$READY" ] && [ "$REBUILD" != "1" ]; then\n'
            f'  echo "Stack {stack.name!r} already ready at $HASH_DIR (hash {hash_})"\n'
            f'  exit 0\n'
            f'fi\n'
            f'\n'
            f'# Cleanup: rebuild OR half-built leftover from a prior crash.\n'
            f'if [ "$REBUILD" = "1" ] || [ -d "$HASH_DIR" ]; then\n'
            f'  rm -rf "$HASH_DIR"\n'
            f'fi\n'
            f'\n'
            f'set -euo pipefail\n'
            f'mkdir -p "$HASH_DIR"\n'
            f'export STACK_DIR="$HASH_DIR"\n'
            f'cd "$HASH_DIR"\n'
            f'\n'
            f'# === User prep ===\n'
            f'{prep}\n'
            f'# === /User prep ===\n'
            f'\n'
            f'touch "$READY"\n'
            f'echo "Stack {stack.name!r} installed at $HASH_DIR (hash {hash_})"\n'
        )

    async def create_run_from_stack(
        self,
        stack: Stack,
        backend_name: str,
        *,
        rebuild: bool = False,
        source_name: str | None = None,
    ) -> Run:
        """Submit a stack install as a one-task workflow run.

        The install becomes a normal run — appears in ``run list``,
        readable via ``run view``, tailable via ``run logs -f``,
        cancelable via ``run cancel``, watchable via ``run watch
        --exit-status``. No new observability surface.

        Task resources (``cpus`` / ``memory`` / ``time_limit`` /
        ``partition``) come from the stack so heavy preps (compiling
        Julia, downloading large conda envs) actually get the
        allocation they need. ``source_name`` is folded into
        ``workflow_name`` (``_stack/<source>/<stack>``) for
        provenance — operators reading the runs page can see *which
        repo's* stack got installed.
        """
        from scripthut.stacks.manager import compute_stack_hash

        if self.config.get_backend(backend_name) is None:
            raise ValueError(f"Backend '{backend_name}' not found in config")

        ssh_client = self.get_ssh_client(backend_name)
        job_backend = self.get_job_backend(backend_name)
        if ssh_client is None and job_backend is None:
            raise ValueError(f"Backend '{backend_name}' is not available")

        hash_ = compute_stack_hash(stack)
        command = self._synthesize_stack_install_command(
            stack, hash_, rebuild,
        )

        task = TaskDefinition(
            id=f"install-{hash_[:8]}",
            name=f"stack/{stack.name}",
            command=command,
            cpus=stack.cpus,
            memory=stack.memory,
            time_limit=stack.time_limit,
            partition=stack.partition or "normal",
        )

        workflow_name = (
            f"_stack/{source_name}/{stack.name}"
            if source_name else f"_stack/{stack.name}"
        )

        return await self._build_run(
            [task], workflow_name, backend_name, max_concurrent=None,
            ssh_client=ssh_client,
        )

    async def create_adhoc_run(
        self,
        task: TaskDefinition,
        backend_name: str,
        *,
        run_name: str | None = None,
    ) -> Run:
        """Create a one-task run from an inline ``TaskDefinition``.

        Useful for ad-hoc CLI submissions and coding agents that
        synthesize a TaskDefinition directly.

        ``run_name`` becomes the run's ``workflow_name``; if omitted, a
        synthetic ``_adhoc/<task_id>`` label is used (the run still
        appears in `run list` and on the dashboard, just without a
        configured source behind it).

        Raises ``ValueError`` if the backend isn't in the config or
        doesn't have an available driver.
        """
        if self.config.get_backend(backend_name) is None:
            raise ValueError(f"Backend '{backend_name}' not found in config")

        ssh_client = self.get_ssh_client(backend_name)
        job_backend = self.get_job_backend(backend_name)
        if ssh_client is None and job_backend is None:
            raise ValueError(f"Backend '{backend_name}' is not available")

        workflow_name = run_name or f"_adhoc/{task.id}"
        return await self._build_run(
            [task], workflow_name, backend_name, max_concurrent=None,
            ssh_client=ssh_client,
        )

    async def _ls_remote_commit(
        self, repo: str, branch: str,
    ) -> str | None:
        """Resolve a remote git ref to a commit hash via local ``git ls-remote``.

        Used by API-only backends (Batch) so they can pass a concrete SHA to
        their container's runtime clone step.  Returns None on failure; the
        container will fall back to the branch name.
        """
        cmd = f"git ls-remote {repo} {branch}"
        try:
            stdout, stderr, code = await _run_local_shell(cmd, timeout=30.0)
        except Exception as e:
            logger.warning(f"Local git ls-remote failed: {e}")
            return None
        if code != 0:
            logger.warning(f"Local git ls-remote exit {code}: {stderr.strip()}")
            return None
        line = stdout.strip().split("\n", 1)[0]
        return line.split()[0] if line else None

    def _parse_tasks_json(
        self, tasks_json: str, label: str,
    ) -> tuple[list[TaskDefinition], list, dict]:
        """Parse a JSON workflow document string.

        Returns ``(tasks, doc_env, doc_env_groups)``. ``doc_env`` /
        ``doc_env_groups`` are populated from the top-level ``env:`` and
        ``env_groups:`` fields when the document uses the wrapped form.
        """
        try:
            data = json.loads(tasks_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {label}: {e}")

        try:
            return TaskDefinition.parse_document(data)
        except ValueError as e:
            raise ValueError(f"Invalid workflow JSON in {label}: {e}")

        return [TaskDefinition.from_dict(t) for t in tasks_data]

    async def _clone_source_repo(
        self, ssh_client: SSHClient, source: GitSourceConfig,
    ) -> tuple[str, str]:
        """Clone a git source's repo on the backend."""
        return await self._clone_git_repo(
            ssh_client,
            repo=source.url,
            branch=source.branch,
            deploy_key=source.deploy_key,
            clone_dir=source.clone_dir,
            postclone=source.postclone,
        )

    async def _clone_agent_workspace(
        self, ssh_client: SSHClient, source: GitSourceConfig, *, full_history: bool,
    ) -> tuple[str, str]:
        """Clone a *fresh, writable, unique* workspace for a coding-agent run.

        Unlike ``_clone_source_repo`` (which is content-addressed by commit and
        shared/reused across runs), each agent gets its own throwaway clone at
        ``<clone_dir>/agent-<uid>`` so it can create branches, commit, and push
        without colliding with other runs or dirtying the shared cache. When
        ``full_history`` is True the clone keeps full history (needed for branch
        + PR work); otherwise it's a shallow single-branch clone.

        Returns ``(workspace_path, short_hash)``.
        """
        wsid = uuid.uuid4().hex[:8]
        remote_key: str | None = None
        try:
            if source.deploy_key is not None:
                remote_key = await self._upload_deploy_key(
                    ssh_client, source.deploy_key.expanduser()
                )

            git_ssh = self._build_remote_git_ssh_command(remote_key)
            effective_repo = source.url if remote_key else self._to_https_url(source.url)

            # Resolve HEAD commit for run metadata.
            cmd = (
                f"{git_ssh}GIT_TERMINAL_PROMPT=0 git ls-remote "
                f"{effective_repo} refs/heads/{source.branch}"
            )
            stdout, stderr, exit_code = await ssh_client.run_command(cmd, timeout=30)
            if exit_code != 0 or not stdout.strip():
                raise ValueError(
                    f"Failed to resolve branch '{source.branch}' from "
                    f"'{source.url}': {stderr}"
                )
            short_hash = stdout.split()[0][:12]

            workspace = f"{source.clone_dir}/agent-{wsid}"
            depth = "" if full_history else "--single-branch --depth 1 "
            logger.info(
                f"Cloning agent workspace {source.url}@{source.branch} "
                f"({short_hash}) to {workspace}"
            )
            cmd = (
                f"{git_ssh}GIT_TERMINAL_PROMPT=0 git clone --branch {source.branch} "
                f"{depth}{effective_repo} {workspace}"
            )
            _, stderr, exit_code = await ssh_client.run_command(cmd, timeout=600)
            if exit_code != 0:
                raise ValueError(f"Git clone failed: {stderr}")

            if source.postclone:
                logger.info(f"Running postclone command in {workspace}")
                cmd = f"cd {workspace} && {source.postclone}"
                _, stderr, exit_code = await ssh_client.run_command(cmd, timeout=600)
                if exit_code != 0:
                    raise ValueError(f"Postclone command failed: {stderr}")

            return workspace, short_hash
        finally:
            if remote_key is not None:
                await self._cleanup_deploy_key(ssh_client, remote_key)

    def _resolve_working_dirs(
        self, tasks: list[TaskDefinition], clone_dir: str,
    ) -> None:
        """Resolve task working_dir values relative to a clone directory."""
        for task in tasks:
            if task.working_dir == "~":
                task.working_dir = clone_dir
            elif not task.working_dir.startswith(("/", "~")):
                task.working_dir = f"{clone_dir}/{task.working_dir}"

    async def _load_source_project_config(
        self,
        source: GitSourceConfig | PathSourceConfig,
        *,
        commit_hash: str | None = None,
        ssh_client: SSHClient | None = None,
    ) -> ScriptHutConfig | None:
        """Delegate to :func:`load_source_project_config` with this config."""
        return await load_source_project_config(
            self.config, source, commit_hash=commit_hash, ssh_client=ssh_client,
        )

    async def create_run_from_source(
        self, source_name: str, workflow_filename: str, tasks_json: str,
        backend: str, branch: str | None = None,
    ) -> Run:
        """Create a run from a source workflow's JSON task list.

        For SSH-based backends and git sources, the repo is cloned on the
        backend and tasks run inside the cloned directory.  For API-based
        backends (AWS Batch), the commit hash is resolved locally and each
        container clones the source at runtime using env vars.  Path sources
        require SSH (they reference a filesystem path on the cluster).

        Args:
            source_name: Name of the source.
            workflow_filename: Filename of the workflow (e.g. "train.json").
            tasks_json: Raw JSON task list content.
            backend: Name of the backend to submit tasks to.
            branch: Git branch to run from, overriding the source's
                configured branch. Only valid for git sources. The
                caller is responsible for ``tasks_json`` matching this
                branch (the API fetches + discovers at the branch first).
        """
        source = self.config.get_source(source_name)
        if source is None:
            raise ValueError(f"Source '{source_name}' not found")

        if branch is not None and branch != getattr(source, "branch", None):
            if not isinstance(source, GitSourceConfig):
                raise ValueError(
                    f"Source '{source_name}' is not a git source; "
                    "branch override is only supported for git sources."
                )
            if not is_safe_branch_name(branch):
                raise ValueError(f"Invalid branch name: {branch!r}")
            # Everything downstream (backend clone, ls-remote, the
            # scripthut.yaml overlay, run.git_branch) reads source.branch,
            # so a shallow copy with the override is all it takes.
            source = source.model_copy(update={"branch": branch})

        backend_name = backend
        ssh_client = self.get_ssh_client(backend_name)
        job_backend = self.get_job_backend(backend_name)
        if ssh_client is None and job_backend is None:
            raise ValueError(f"Backend '{backend_name}' is not available")

        stem = workflow_filename.removesuffix(".json")
        workflow_name = f"{source_name}/{stem}"
        label = f"source '{source_name}' workflow '{workflow_filename}'"

        tasks, doc_env, doc_env_groups = self._parse_tasks_json(tasks_json, label)

        clone_dir: str | None = None
        commit_hash: str | None = None

        if isinstance(source, GitSourceConfig):
            if ssh_client is not None:
                # SSH backend: clone on the backend filesystem now.
                clone_dir, commit_hash = await self._clone_source_repo(
                    ssh_client, source
                )
                self._resolve_working_dirs(tasks, clone_dir)
            else:
                # API-only backend: resolve the commit locally so the
                # container can check out the same ref at runtime.  Don't
                # touch tasks.working_dir — the backend's generate_script
                # rewrites relative paths against $_SCRIPTHUT_CLONE_DIR.
                commit_hash = await self._ls_remote_commit(source.url, source.branch)
        elif isinstance(source, PathSourceConfig):
            if ssh_client is None:
                raise ValueError(
                    f"Path source '{source_name}' requires an SSH backend; "
                    f"'{backend_name}' has no filesystem."
                )
            # Path sources already exist on the backend.
            self._resolve_working_dirs(tasks, source.path)

        # Overlay the source repo's project-local scripthut.yaml on top of
        # what the workflow doc itself carries. Precedence in the final
        # resolver: server config → backend → repo-project (here) →
        # workflow-doc inline → task. We prepend the repo's env list and
        # *under-merge* its env_groups so the workflow doc wins on any
        # name collision (most-specific source of truth for that file).
        # Generated tasks inherit automatically because run.doc_env /
        # doc_env_groups are extended in _handle_generates_source.
        project_cfg = await self._load_source_project_config(
            source, commit_hash=commit_hash, ssh_client=ssh_client,
        )
        doc_stacks: dict[str, Stack] = {}
        if project_cfg is not None:
            doc_env = list(project_cfg.env) + list(doc_env)
            # Workflow-inline keys win on collision.
            doc_env_groups = {
                **project_cfg.env_groups,
                **doc_env_groups,
            }
            # Repo stacks are visible to ``stacks:`` env-rule references.
            # The resolver merges these on top of server-config stacks
            # (collect_stacks: server | doc_stacks), so a repo can override
            # a server-defined stack — same convention as env_groups.
            doc_stacks = {s.name: s for s in project_cfg.stacks}

        git_repo = source.url if isinstance(source, GitSourceConfig) else None
        git_branch = source.branch if isinstance(source, GitSourceConfig) else None
        run = await self._build_run(
            tasks, workflow_name, backend_name, None, ssh_client,
            git_repo=git_repo, git_branch=git_branch, commit_hash=commit_hash,
            doc_env=doc_env, doc_env_groups=doc_env_groups,
            doc_stacks=doc_stacks,
        )
        return run

    @staticmethod
    def _build_agent_command(agent_cfg: AgentConfig, mode: str, name: str) -> str:
        """Render the job command for a coding-agent run.

        ``remote`` substitutes the (shell-quoted) session name into
        ``remote_command``. ``tui`` runs the program inside a tmux session named
        ``sh-<jobid>`` — the same convention the interactive-debug flow uses
        (``backends/utils.py``) so the browser terminal's ``session_type=job``
        attach (``tmux attach -t sh-<jobid>``) works unchanged. The job stays
        alive while that tmux session (and thus claude) lives.
        """
        quoted_name = shlex.quote(name)
        hint = (
            "install Claude Code on the compute nodes, or set agent.env_group "
            "to an env_group whose init activates an environment that provides "
            "it (e.g. via conda; see scripthut.example.yaml)"
        )
        if mode == "remote":
            bin_ = RunManager._agent_binary(agent_cfg.remote_command)
            cmd = agent_cfg.remote_command.format(name=quoted_name)
            # Fail fast with a clear message if the binary is missing, so the
            # run shows FAILED with an actionable reason rather than an opaque
            # "command not found".
            return (
                f"if ! command -v {bin_} >/dev/null 2>&1; then\n"
                f'  echo "ERROR: {bin_} not found on $(hostname). {hint}." >&2\n'
                "  exit 127\n"
                "fi\n"
                f"{cmd}\n"
            )
        # TUI: run the program inside tmux, but if it exits (e.g. claude not
        # installed -> exit 127, or a crash) fall back to a login shell so the
        # tmux session — and therefore the Slurm/PBS allocation — survives.
        # Otherwise the job ends within seconds, the node allocation is
        # released, and the browser-terminal attach is rejected by pam_slurm
        # ("you have no job on this node"). Keeping a shell open also lets the
        # user see the failure and install/fix things in place; Ctrl-D or
        # cancelling the run ends it. (No single quotes in `inner` — it gets
        # shlex.quoted into the tmux argument.)
        bin_ = RunManager._agent_binary(agent_cfg.tui_command)
        inner = (
            f'{agent_cfg.tui_command} || {{ rc=$?; echo; '
            f'if [ "$rc" = 127 ]; then '
            f'echo "[ {bin_} not found on $(hostname) -- {hint}; opening a shell ]"; '
            f'else echo "[ agent command exited $rc -- opening a shell to inspect ]"; fi; '
            f'echo "[ Ctrl-D or cancel the run to end ]"; exec bash -l; }}'
        )
        return (
            'S="sh-${SLURM_JOB_ID:-${PBS_JOBID:-$$}}"\n'
            f'tmux new-session -d -s "$S" {shlex.quote(inner)}\n'
            'echo "Agent TUI session $S ready on $(hostname)"\n'
            'while tmux has-session -t "$S" 2>/dev/null; do sleep 10; done\n'
        )

    @staticmethod
    def _agent_binary(command: str) -> str:
        """First token (executable) of an agent command, for a presence check."""
        try:
            parts = shlex.split(command)
        except ValueError:
            parts = command.split()
        return parts[0] if parts else command

    async def create_agent_run(
        self,
        source_name: str,
        backend: str,
        *,
        mode: str = "remote",
        session_name: str | None = None,
    ) -> Run:
        """Create a one-task run that launches a Claude coding agent in a fresh clone.

        Modes:
          - ``remote``: ``claude remote-control`` server session, driven from the
            claude.ai web interface.
          - ``tui``: interactive ``claude`` TUI inside a tmux session the browser
            terminal attaches to.

        Restricted to git sources on SSH backends (needs a fresh writable clone on
        a real filesystem). Raises ``ValueError`` otherwise.
        """
        if mode not in ("remote", "tui"):
            raise ValueError(
                f"Unknown agent mode '{mode}' (expected 'remote' or 'tui')"
            )

        source = self.config.get_source(source_name)
        if source is None:
            raise ValueError(f"Source '{source_name}' not found")
        if not isinstance(source, GitSourceConfig):
            raise ValueError(
                f"Coding agents require a git source; '{source_name}' is not one"
            )

        backend_name = backend
        ssh_client = self.get_ssh_client(backend_name)
        if ssh_client is None:
            raise ValueError(
                f"Coding agents require an SSH backend; '{backend_name}' is not "
                "available or has no filesystem"
            )

        agent_cfg = self.config.agent
        name = session_name or f"{source_name}-{uuid.uuid4().hex[:6]}"

        workspace, commit_hash = await self._clone_agent_workspace(
            ssh_client, source, full_history=agent_cfg.clone_full_history,
        )

        env_rules: list[EnvRule] = []
        if agent_cfg.env_group:
            env_rules.append(EnvRule(include=[agent_cfg.env_group]))

        task = TaskDefinition(
            id="agent",
            name=f"Claude agent: {name}",
            command=self._build_agent_command(agent_cfg, mode, name),
            working_dir=workspace,
            cpus=agent_cfg.cpus,
            memory=agent_cfg.memory,
            time_limit=agent_cfg.time_limit,
            partition=agent_cfg.partition or "normal",
            env=env_rules,
        )

        workflow_name = f"_agent/{source_name}/{name}"
        run = await self._build_run(
            [task], workflow_name, backend_name, max_concurrent=None,
            ssh_client=ssh_client,
            git_repo=source.url, git_branch=source.branch, commit_hash=commit_hash,
        )
        run.agent_session = True
        run.agent_mode = mode
        run.agent_session_name = name
        self._persist_run(run)
        return run

    async def dry_run_source(
        self, source_name: str, workflow_filename: str, tasks_json: str,
        backend: str,
    ) -> dict:
        """Dry run a source workflow — preview tasks without submitting."""
        source = self.config.get_source(source_name)
        if source is None:
            raise ValueError(f"Source '{source_name}' not found")
        backend_name = backend

        stem = workflow_filename.removesuffix(".json")
        workflow_name = f"{source_name}/{stem}"
        label = f"source '{source_name}' workflow '{workflow_filename}'"

        tasks, doc_env, doc_env_groups = self._parse_tasks_json(tasks_json, label)

        # Resolve working dirs for preview
        warnings: list[str] = []
        commit_hash: str | None = None
        if isinstance(source, GitSourceConfig):
            # For dry run, clone so we can show correct paths
            ssh_client = self.get_ssh_client(backend_name)
            if ssh_client:
                try:
                    clone_dir, commit_hash = await self._clone_source_repo(
                        ssh_client, source
                    )
                    self._resolve_working_dirs(tasks, clone_dir)
                except Exception as e:
                    logger.warning(f"Could not clone for dry run preview: {e}")
                    warnings.append(
                        f"Could not clone repository on backend '{backend_name}': {e}. "
                        f"The run will fail if this backend cannot access the git remote."
                    )
                    self._resolve_working_dirs(tasks, f"{source.clone_dir}/<commit>")
            else:
                warnings.append(f"Backend '{backend_name}' is not connected. Cannot preview clone paths.")
                self._resolve_working_dirs(tasks, f"{source.clone_dir}/<commit>")
        elif isinstance(source, PathSourceConfig):
            self._resolve_working_dirs(tasks, source.path)

        # Same overlay as the real submission path so the preview reflects
        # what would actually happen: repo env / env_groups / stacks fold
        # in, workflow-doc inline keys win on collision.
        ssh_for_cfg = self.get_ssh_client(backend_name)
        try:
            project_cfg = await self._load_source_project_config(
                source, commit_hash=commit_hash, ssh_client=ssh_for_cfg,
            )
        except ValueError as e:
            # In dry-run, surface the project-YAML error as a warning so
            # the user sees the diagnostic without aborting the preview.
            warnings.append(f"Source project config error: {e}")
            project_cfg = None
        doc_stacks: dict[str, Stack] = {}
        if project_cfg is not None:
            doc_env = list(project_cfg.env) + list(doc_env)
            doc_env_groups = {
                **project_cfg.env_groups,
                **doc_env_groups,
            }
            doc_stacks = {s.name: s for s in project_cfg.stacks}

        self._resolve_wildcard_deps(tasks)
        self._validate_dependencies(tasks)

        account = self.get_backend_account(backend_name)
        login_shell = self.get_backend_login_shell(backend_name)

        log_dir = "~/.cache/scripthut/logs"
        ssh_client = self.get_ssh_client(backend_name)
        if ssh_client and log_dir.startswith("~"):
            stdout, _, _ = await ssh_client.run_command("echo $HOME")
            home_dir = stdout.strip()
            log_dir = log_dir.replace("~", home_dir, 1)

        preview_run_id = "preview"
        preview_created_at = datetime.now(timezone.utc)
        job_backend = self.get_job_backend(backend_name)

        task_details = []
        for task in tasks:
            merged_env, extra_init = resolve_for_task(
                self.config,
                backend_name=backend_name,
                workflow_name=workflow_name,
                run_id=preview_run_id,
                created_at=preview_created_at,
                task=task,
                doc_env=doc_env,
                doc_env_groups=doc_env_groups,
                doc_stacks=doc_stacks,
            )
            if job_backend:
                script = job_backend.generate_script(
                    task, preview_run_id, log_dir,
                    account=account, login_shell=login_shell,
                    env_vars=merged_env, extra_init=extra_init,
                )
            else:
                script = task.to_sbatch_script(
                    preview_run_id, log_dir,
                    account=account, login_shell=login_shell,
                    env_vars=merged_env, extra_init=extra_init,
                )
            task_details.append({
                "task": task,
                "submit_script": script,
                "output_path": task.get_output_path(preview_run_id, log_dir),
                "error_path": task.get_error_path(preview_run_id, log_dir),
            })

        try:
            raw_output_formatted = json.dumps(json.loads(tasks_json), indent=2)
        except (json.JSONDecodeError, TypeError):
            raw_output_formatted = tasks_json

        return {
            "workflow": {
                "name": workflow_name,
                "description": "",
                "command": f"(from source {source_name}: {workflow_filename})",
            },
            "submit_url": f"/sources/{source_name}/workflows/{workflow_filename}/run?backend={backend_name}",
            "backend_name": backend_name,
            "task_count": len(tasks),
            "max_concurrent": None,
            "account": account,
            "commit_hash": commit_hash,
            "tasks": task_details,
            "raw_output": raw_output_formatted,
            "warnings": warnings,
        }

    async def rerun_in_place(self, run_id: str) -> Run:
        """Reset an existing run and reprocess it.

        All items are reset to PENDING with runtime state cleared.
        The run keeps its original ID, commit hash, and parameters.
        """
        run = self.get_run(run_id)
        if run is None:
            raise ValueError(f"Run '{run_id}' not found")

        if run.status in (RunStatus.RUNNING, RunStatus.PENDING):
            raise ValueError("Cannot rerun a run that is still active")

        # Reset all items to pending
        for item in run.items:
            item.status = RunItemStatus.PENDING
            item.job_id = None
            item.submitted_at = None
            item.started_at = None
            item.finished_at = None
            item.error = None
            item.submit_script = None
            item.submit_output = None
            item.cpu_efficiency = None
            item.max_rss = None
            item.scheduler_state = None

        run.created_at = datetime.now(timezone.utc)
        self._persist_run(run)
        self.notify_run(run_id)

        # Start submitting tasks
        await self.process_run(run)

        return run


    def get_job_backend(self, backend_name: str) -> JobBackend | None:
        """Get the JobBackend for a backend name."""
        return self.job_backends.get(backend_name)

    async def probe_cache_for_task(
        self,
        task: TaskDefinition,
        merged_env: dict[str, str],
        commit_hash: str | None,
        ssh_client: SSHClient | None,
    ) -> tuple[dict, dict | None]:
        """Read-only cache probe for one task: compute the key, look it up.

        This is the single source of truth for "would this task cache-hit?"
        — the real submission path (:meth:`_try_restore_from_cache`) runs
        through it too, so a probe verdict cannot drift from what a real
        submission would do. The only remote commands issued are the
        read-only input hashing (``find | sha256sum``) and the manifest
        fetch; nothing is executed, restored, stored, or recorded.

        Returns ``(verdict, manifest)``. ``verdict`` is a JSON-able dict:

        - ``task_id``: the task's id.
        - ``cacheable``: whether the task participates in the cache at all
          (with ``reason`` explaining why not).
        - ``cache_key``: the computed key, when input hashing succeeded.
        - ``hit``: whether a usable (successful) cached result exists.
        - On a hit: ``content_hash`` (the output tarball's sha256),
          ``outputs`` (cached file list), and any per-file ``output_hashes``
          the manifest carries.

        ``manifest`` is the raw action-cache manifest on a usable hit so the
        submission path can restore from it without a second lookup; ``None``
        otherwise.
        """
        cm = self.cache_manager

        def _no(reason: str, key: str | None = None) -> tuple[dict, None]:
            return ({
                "task_id": task.id,
                "cacheable": False,
                "reason": reason,
                "cache_key": key,
                "hit": False,
            }, None)

        if not cm.enabled:
            return _no("cache disabled (no store configured)")
        if ssh_client is None:
            return _no("backend has no shell access")
        # Only tasks that declare real outputs are cacheable — otherwise
        # there is nothing to restore. ``cache: false`` opts a task out.
        if not task.cache:
            return _no("task opted out (cache: false)")
        if not task.outputs:
            return _no("task declares no outputs")

        input_hashes = await cm.hash_inputs(
            ssh_client, task.working_dir, task.inputs
        )
        if input_hashes is None:
            # Couldn't verify the inputs (missing files / SSH error). Don't
            # risk a stale hit — the task must run.
            return _no("input hashing failed or inputs matched no files")

        key = cm.compute_key(
            command=task.command,
            env=merged_env,
            # cache_scope="inputs" drops the commit from the key so runs
            # from different commits reuse each other's results as long as
            # command + env + declared input hashes match.
            commit_hash=commit_hash if task.cache_scope == "commit" else None,
            input_hashes=input_hashes,
        )
        verdict: dict = {
            "task_id": task.id,
            "cacheable": True,
            "reason": None,
            "cache_key": key,
            "input_hashes": input_hashes,
            "hit": False,
        }

        manifest = await cm.lookup(ssh_client, key)
        if manifest is None:
            return verdict, None  # miss

        # Never reuse a cached failure.
        cached_exit = manifest.get("exit_code")
        if cached_exit not in (0, None):
            logger.info(
                f"cache: key {key[:12]} hit for '{task.id}' but cached "
                f"exit_code={cached_exit} — not reusable"
            )
            verdict["reason"] = f"cached result failed (exit {cached_exit})"
            return verdict, None

        verdict["hit"] = True
        verdict["content_hash"] = manifest.get("content_hash")
        verdict["outputs"] = manifest.get("outputs", [])
        if "output_hashes" in manifest:
            verdict["output_hashes"] = manifest["output_hashes"]
        return verdict, manifest

    async def probe_tasks(
        self,
        tasks: list[TaskDefinition],
        backend_name: str,
        *,
        workflow_name: str = "_probe",
        commit_hash: str | None = None,
        git_repo: str | None = None,
        git_branch: str | None = None,
        doc_env: list[EnvRule] | None = None,
        doc_env_groups: dict[str, list[EnvRule]] | None = None,
        doc_stacks: dict[str, Stack] | None = None,
    ) -> list[dict]:
        """Answer hit/miss for a task list without executing or writing anything.

        Validates the document the same way a submission would (wildcard
        deps expanded, missing refs / cycles rejected), resolves each task's
        environment through the same resolver chain, and probes the cache
        per task via :meth:`probe_cache_for_task`. No run records are
        created, nothing is persisted, no artifacts move — the verdicts are
        exactly what a real submission would have decided at submit time.

        The env seed uses a synthetic run id; that cannot skew verdicts
        because volatile ``SCRIPTHUT_*`` values are excluded from the cache
        key. ``commit_hash`` feeds ``cache_scope="commit"`` tasks' keys and
        defaults to ``None`` — matching ad-hoc submissions, which carry no
        commit.
        """
        if self.config.get_backend(backend_name) is None:
            raise ValueError(f"Backend '{backend_name}' not found in config")
        self._resolve_wildcard_deps(tasks)
        self._validate_dependencies(tasks)

        ssh_client = self.get_ssh_client(backend_name)
        created_at = datetime.now(timezone.utc)
        verdicts: list[dict] = []
        for task in tasks:
            try:
                merged_env, _extra_init = resolve_for_task(
                    self.config,
                    backend_name=backend_name,
                    workflow_name=workflow_name,
                    run_id="probe",
                    created_at=created_at,
                    task=task,
                    git_repo=git_repo,
                    git_branch=git_branch,
                    git_sha=commit_hash,
                    doc_env=list(doc_env or []),
                    doc_env_groups=dict(doc_env_groups or {}),
                    doc_stacks=dict(doc_stacks or {}),
                )
            except ValueError as e:
                # A real submission would FAIL this item at submit time;
                # surface the same diagnostic instead of aborting the probe.
                verdicts.append({
                    "task_id": task.id,
                    "cacheable": False,
                    "reason": f"env resolution failed: {e}",
                    "cache_key": None,
                    "hit": False,
                })
                continue
            verdict, _manifest = await self.probe_cache_for_task(
                task, merged_env, commit_hash, ssh_client,
            )
            verdicts.append(verdict)
        return verdicts

    async def _try_restore_from_cache(
        self,
        run: Run,
        item: RunItem,
        merged_env: dict[str, str],
        ssh_client: SSHClient | None,
    ) -> bool:
        """Restore a prior run's artifacts for this task if the cache matches.

        Returns ``True`` when the item was satisfied from cache (the caller
        then skips submission). Every uncertainty — caching off, no SSH
        backend, the task opted out or declares no outputs, input hashing
        failed, a miss, or a failed restore — returns ``False`` so the task
        runs normally. A cache must never *replace* computation it can't
        positively prove is reusable.

        As a side effect on a cacheable task, ``item.cache_key`` is set so the
        completion path can store this task's outputs under the same key on a
        miss.
        """
        cm = self.cache_manager
        task = item.task
        verdict, manifest = await self.probe_cache_for_task(
            task, merged_env, run.commit_hash, ssh_client,
        )
        if verdict.get("cache_key"):
            item.cache_key = verdict["cache_key"]  # for the miss → store path
        if not verdict["hit"] or manifest is None:
            return False

        if not await cm.restore(ssh_client, task.working_dir, manifest):
            return False

        now = datetime.now(timezone.utc)
        item.status = RunItemStatus.COMPLETED
        item.cache_hit = True
        item.started_at = item.started_at or now
        item.finished_at = now
        item.exit_code = 0
        item.scheduler_state = "CACHED"
        self._persist_run(run)
        logger.info(
            f"cache: restored '{task.id}' from key "
            f"{(item.cache_key or '')[:12]} — skipped scheduler submission"
        )
        return True

    async def _maybe_store_cache(self, run: Run, item: RunItem) -> None:
        """Store a freshly-completed task's declared outputs to the cache.

        Best-effort: any failure is logged and swallowed — a cache write must
        never fail the run. Skips cache hits (already in the store), tasks
        that weren't cacheable at submit time (no ``cache_key``), and tasks
        with no declared outputs.
        """
        cm = self.cache_manager
        task = item.task
        if not cm.enabled or item.cache_hit:
            return
        if item.cache_key is None or not task.outputs:
            return
        if item.status != RunItemStatus.COMPLETED:
            return
        ssh_client = self.get_ssh_client(run.backend_name)
        if ssh_client is None:
            return

        meta = {
            "command": task.command,
            "commit": run.commit_hash or "",
            "run_id": run.id,
            "workflow": run.workflow_name,
            "exit_code": item.exit_code if item.exit_code is not None else 0,
            "created_at": run.created_at.isoformat(),
        }
        try:
            await cm.store(
                ssh_client, task.working_dir,
                key=item.cache_key, outputs=task.outputs, meta=meta,
            )
        except Exception as e:  # noqa: BLE001 — storing must not fail the run
            logger.warning(
                f"cache: storing outputs for '{task.id}' failed: {e}"
            )

    async def submit_task(self, run: Run, item: RunItem) -> bool:
        """Submit a single task to the scheduler."""
        job_backend = self.get_job_backend(run.backend_name)
        if job_backend is None:
            item.status = RunItemStatus.FAILED
            item.error = f"No job backend for '{run.backend_name}'"
            item.finished_at = datetime.now(timezone.utc)
            self._persist_run(run)
            return False

        ssh_client = self.get_ssh_client(run.backend_name)

        # SSH-based backends resolve ~ and create the log directory before
        # submission.  API-based backends (Batch) route logs elsewhere.
        log_dir = run.log_dir
        if ssh_client is not None:
            if log_dir.startswith("~"):
                stdout, _, _ = await ssh_client.run_command("echo $HOME")
                home_dir = stdout.strip()
                log_dir = log_dir.replace("~", home_dir, 1)
            await ssh_client.run_command(f"mkdir -p {log_dir}")

        # Env resolution can raise ValueError for bad config (e.g. an
        # undefined stack reference or legacy fields).  Treat that as a task
        # failure rather than letting it bubble up — otherwise a single bad
        # run aborts startup in restore_from_storage.
        try:
            merged_env, extra_init = self._resolve_environment(run, item.task)
            script = job_backend.generate_script(
                item.task, run.id, log_dir,
                account=run.account, login_shell=run.login_shell,
                env_vars=merged_env, extra_init=extra_init,
            )
        except ValueError as e:
            item.status = RunItemStatus.FAILED
            item.error = str(e)
            item.submit_output = item.submit_output or str(e)
            item.finished_at = datetime.now(timezone.utc)
            logger.error(f"Failed to prepare task '{item.task.id}': {e}")
            self._persist_run(run)
            return False
        item.submit_script = script

        # Before touching the scheduler, see if a prior run already produced
        # this exact task (same command + env + commit + input hashes). On a
        # hit we restore its artifacts and mark the item COMPLETED here.
        if await self._try_restore_from_cache(run, item, merged_env, ssh_client):
            return True

        try:
            result = await job_backend.submit_task(
                item.task, script, env_vars=merged_env,
            )
            item.job_id = result.job_id
            item.submit_output = result.submit_output or None
            item.status = RunItemStatus.SUBMITTED
            item.submitted_at = datetime.now(timezone.utc)
            logger.info(f"Submitted task '{item.task.id}' as job {result.job_id}")
            self._persist_run(run)
            return True
        except RuntimeError as e:
            item.status = RunItemStatus.FAILED
            item.error = str(e)
            item.submit_output = item.submit_output or str(e)
            item.finished_at = datetime.now(timezone.utc)
            logger.error(f"Failed to submit task '{item.task.id}': {e}")
            self._persist_run(run)
            return False

    async def process_run(self, run: Run) -> None:
        """Process a run - submit tasks up to max_concurrent respecting dependencies."""
        # Cascade failures
        changed = True
        while changed:
            changed = False
            for item in run.items:
                if item.status != RunItemStatus.PENDING:
                    continue
                failed_deps = run.get_failed_deps(item)
                if failed_deps:
                    item.status = RunItemStatus.DEP_FAILED
                    item.error = f"Dependency '{failed_deps[0]}' failed"
                    item.finished_at = datetime.now(timezone.utc)
                    self._persist_run(run)
                    changed = True

        active_count = run.running_count

        ready_items = [
            item for item in run.items
            if item.status == RunItemStatus.PENDING
            and run.are_deps_satisfied(item)
        ]

        # Per-run cap (if set)
        if run.max_concurrent is not None:
            run_slots = run.max_concurrent - active_count
        else:
            run_slots = len(ready_items)

        # Backend-level cap
        backend_max = self._get_backend_max_concurrent(run.backend_name)
        backend_active = self._backend_running_count(run.backend_name)
        backend_slots = backend_max - backend_active

        slots_available = max(0, min(run_slots, backend_slots))
        to_submit = ready_items[:slots_available]

        if to_submit:
            task_ids = [item.task.id for item in to_submit]
            logger.info(
                f"Run '{run.id}': submitting {len(to_submit)} tasks: {task_ids} "
                f"(run_slots={run_slots}, backend_slots={backend_slots})"
            )

        cache_completed: list[RunItem] = []
        for item in to_submit:
            success = await self.submit_task(run, item)
            if not success:
                # Submission failure (e.g. bad queue) — mark all remaining pending tasks as failed
                for pending_item in run.items:
                    if pending_item.status == RunItemStatus.PENDING:
                        pending_item.status = RunItemStatus.FAILED
                        pending_item.error = f"Submission halted: {item.error}"
                        pending_item.finished_at = datetime.now(timezone.utc)
                self._persist_run(run)
                break
            # A cache hit completes the item synchronously instead of
            # submitting a job; collect it so post-completion hooks run.
            if item.cache_hit and item.status == RunItemStatus.COMPLETED:
                cache_completed.append(item)

        # Run completion hooks for cache hits (outputs panel, generates_source,
        # and the cache-store no-op). Done after the submit loop so we're not
        # mutating run.items mid-iteration.
        for item in cache_completed:
            await self._after_item_completed(run, item)

        if to_submit:
            self.notify_run(run.id)

        # Cache hits finish items immediately, which can unblock dependents.
        # Re-drive so they're submitted now rather than waiting for the next
        # poll. Terminates: each hit moves an item out of PENDING, so the
        # ready set strictly shrinks until no cache-completions remain.
        if cache_completed:
            await self.process_run(run)

    def _backend_running_count(self, backend_name: str) -> int:
        """Count all running/submitted tasks across all runs on a backend."""
        count = 0
        for run in self.runs.values():
            if run.backend_name == backend_name:
                count += run.running_count
        return count

    def _get_backend_max_concurrent(self, backend_name: str) -> int:
        """Get the max_concurrent limit for a backend."""
        backend = self.config.get_backend(backend_name)
        if backend:
            return backend.max_concurrent
        return 100  # Default if backend not found

    def _persist_run(self, run: Run) -> None:
        """Mark run dirty for next save cycle."""
        if self.storage:
            self.storage.mark_dirty(run.id)

    def notify_run(self, run_id: str) -> None:
        """Wake all SSE listeners for a run."""
        self._run_versions[run_id] = self._run_versions.get(run_id, 0) + 1
        old_event = self._run_events.get(run_id)
        self._run_events[run_id] = asyncio.Event()
        if old_event:
            old_event.set()

    async def wait_for_update(self, run_id: str, timeout: float = 30.0) -> bool:
        """Wait for a run state change. Returns True if notified, False on timeout."""
        if run_id not in self._run_events:
            self._run_events[run_id] = asyncio.Event()
        event = self._run_events[run_id]
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def update_run_status(
        self,
        run: Run,
        slurm_jobs: dict[str, JobState],
        pending_reasons: dict[str, str] | None = None,
    ) -> None:
        """Update run item statuses based on Slurm job states.

        ``pending_reasons`` maps job_id -> the scheduler's reason a still-
        pending job is waiting (squeue %R). Surfaced on QUEUED items and
        cleared once they leave the queue.
        """
        pending_reasons = pending_reasons or {}
        changed = False
        changed_items: list[RunItem] = []

        # Snapshot items that existed before this update cycle.
        # _handle_generates_source (called below) may append new items and
        # submit them via process_run.  Those new jobs won't appear in the
        # current slurm_jobs dict, so checking them would falsely mark them
        # COMPLETED.  Only iterate over the pre-existing items.
        items_snapshot = list(run.items)

        # State transitions are driven by *evidence*: what squeue reported
        # for the job. We deliberately never mark SUBMITTED as FAILED on
        # absence-from-squeue here — that's an absence-of-evidence, not
        # evidence-of-failure, and it triggers exactly the false-failure
        # cascade we're trying to avoid. The evidence-based sacct path
        # (in main.poll_backend) resolves those cases.
        for item in items_snapshot:
            if item.job_id is None:
                continue

            if item.status in (
                RunItemStatus.COMPLETED,
                RunItemStatus.FAILED,
                RunItemStatus.DEP_FAILED,
            ):
                continue

            job_state = slurm_jobs.get(item.job_id)

            if job_state is None:
                # Missing from squeue. Action depends on whether we
                # *ever* saw the scheduler hold this job.
                if item.status in (RunItemStatus.QUEUED, RunItemStatus.RUNNING):
                    # We observed the scheduler had it; gone now means
                    # it's between "scheduler done" and "accounting
                    # confirmed". Move to SETTLING — the run stays
                    # non-terminal until sacct returns a row (handled
                    # in main.poll_backend). This eliminates the
                    # transient COMPLETED→FAILED flip that broke
                    # `run watch --exit-status` automation.
                    item.started_at = item.started_at or item.submitted_at
                    item.status = RunItemStatus.SETTLING
                    item.pending_reason = None  # no longer waiting
                    # ``finished_at`` is set to "now" — the timestamp
                    # when scripthut noticed the job left the queue.
                    # This is approximate (the job may have finished
                    # seconds earlier on the cluster); sacct updates
                    # it to the precise end time during SETTLING
                    # resolution. We need *some* timestamp here so the
                    # long-grace fallback can decide when to give up
                    # waiting for accounting.
                    item.finished_at = datetime.now(timezone.utc)
                    changed = True
                    changed_items.append(item)
                    logger.info(
                        f"Task '{item.task.id}' (job {item.job_id}) "
                        f"left scheduler queue — SETTLING (awaiting sacct)"
                    )
                    # generates_source handling waits too — we don't
                    # want to spawn dependent tasks based on an
                    # unconfirmed completion. Triggers on the sacct
                    # COMPLETED transition in main.poll_backend.
                # SUBMITTED + missing: do nothing here. The item stays
                # SUBMITTED until either it shows up in squeue or the
                # sacct-evidence path resolves it. No timer, no marker,
                # no false FAILED.
                continue

            # squeue gave us something — translate to our state.
            if job_state in (JobState.RUNNING, JobState.COMPLETING):
                # No longer waiting — drop any stale pending reason.
                if item.pending_reason is not None:
                    item.pending_reason = None
                    changed = True
                if item.status != RunItemStatus.RUNNING:
                    item.status = RunItemStatus.RUNNING
                    item.started_at = item.started_at or datetime.now(timezone.utc)
                    changed = True
                    changed_items.append(item)
                    logger.info(
                        f"Task '{item.task.id}' (job {item.job_id}) started running"
                    )
            elif job_state == JobState.PENDING:
                # SUBMITTED -> QUEUED: scheduler now confirms the job
                # is in its queue. Stay QUEUED on re-observation; flip
                # back from RUNNING only logs, doesn't usually happen.
                if item.status not in (RunItemStatus.QUEUED, RunItemStatus.RUNNING):
                    item.status = RunItemStatus.QUEUED
                    changed = True
                    changed_items.append(item)
                    logger.info(
                        f"Task '{item.task.id}' (job {item.job_id}) "
                        f"acknowledged by scheduler — QUEUED"
                    )
                # Refresh the pending reason on every observation — the
                # scheduler can change it (Priority -> Resources -> ...)
                # while the job waits.
                new_reason = pending_reasons.get(item.job_id)
                if item.pending_reason != new_reason:
                    item.pending_reason = new_reason
                    changed = True
            elif job_state == JobState.COMPLETED:
                # squeue says COMPLETED, but the script's exit code might
                # still disagree (the v0.6.5 fix specifically handles this
                # for sacct's ExitCode). Route through SETTLING so
                # `run watch --exit-status` waits for accounting to
                # confirm. The same path resolves SETTLING → COMPLETED
                # (with generates_source handling) or → FAILED if the
                # ExitCode was non-zero.
                item.started_at = item.started_at or item.submitted_at
                item.status = RunItemStatus.SETTLING
                item.pending_reason = None  # no longer waiting
                changed = True
                changed_items.append(item)
                logger.info(
                    f"Task '{item.task.id}' (job {item.job_id}) "
                    f"reported COMPLETED by scheduler — SETTLING (awaiting sacct)"
                )
            elif job_state in (
                JobState.FAILED,
                JobState.CANCELLED,
                JobState.TIMEOUT,
                JobState.NODE_FAIL,
                JobState.PREEMPTED,
                JobState.BOOT_FAIL,
                JobState.DEADLINE,
                JobState.OUT_OF_MEMORY,
            ):
                item.started_at = item.started_at or item.submitted_at
                item.status = RunItemStatus.FAILED
                item.pending_reason = None  # no longer waiting
                item.error = f"Slurm job {job_state.value}"
                item.finished_at = datetime.now(timezone.utc)
                changed = True
                changed_items.append(item)
                logger.info(
                    f"Task '{item.task.id}' (job {item.job_id}) "
                    f"failed: {job_state.value}"
                )

        if changed:
            self._persist_run(run)
            await self.process_run(run)
            self.notify_run(run.id)

    async def update_all_runs(
        self, backend_jobs: dict[str, list[tuple]]
    ) -> None:
        """Update all active runs based on Slurm job states.

        Each entry in a backend's list is ``(job_id, state)`` or
        ``(job_id, state, reason)`` — the optional third element is the
        scheduler's pending reason (squeue %R). Two-element tuples stay
        supported so existing callers/tests don't break.
        """
        for run in self.runs.values():
            if run.status in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED):
                continue

            jobs = backend_jobs.get(run.backend_name, [])
            job_states: dict[str, JobState] = {}
            pending_reasons: dict[str, str] = {}
            for entry in jobs:
                job_id, job_state = entry[0], entry[1]
                job_states[job_id] = job_state
                if len(entry) > 2 and entry[2]:
                    pending_reasons[job_id] = entry[2]

            await self.update_run_status(run, job_states, pending_reasons)

        # Cross-run backpressure: a job finishing in one run frees a
        # backend-level concurrency slot that a *different* run may be
        # blocked on. update_run_status only re-drives the run whose own
        # items changed, so a run sitting in PENDING purely because the
        # backend cap was full would otherwise never be reconsidered when
        # an unrelated run frees a slot — it would stay stuck forever.
        # Re-drive every active run that still has submittable work.
        # process_run recomputes the backend slot count and submits nothing
        # when the cap is still full, so this is cheap and safe to run each
        # poll; it also self-heals runs that are already stuck.
        for run in self.runs.values():
            if run.status in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED):
                continue
            if self._has_submittable_items(run):
                await self.process_run(run)

    def _has_submittable_items(self, run: Run) -> bool:
        """True if the run has PENDING items whose dependencies are satisfied."""
        return any(
            item.status == RunItemStatus.PENDING and run.are_deps_satisfied(item)
            for item in run.items
        )

    async def cancel_run(self, run_id: str) -> bool:
        """Cancel all pending and running items in a run."""
        run = self.runs.get(run_id)
        if run is None:
            return False

        ssh_client = self.get_ssh_client(run.backend_name)

        for item in run.items:
            if item.status == RunItemStatus.PENDING:
                item.status = RunItemStatus.FAILED
                item.error = "Cancelled"
                item.finished_at = datetime.now(timezone.utc)
            elif item.status in (
                RunItemStatus.SUBMITTED,
                RunItemStatus.QUEUED,
                RunItemStatus.RUNNING,
            ):
                # QUEUED jobs are confirmed in the scheduler's queue awaiting
                # resources — they have a job_id and must be scancel/qdel'd,
                # same as RUNNING. (SETTLING is intentionally excluded: it has
                # already left the queue and may have completed successfully;
                # let accounting resolve it rather than mislabel it cancelled.)
                if item.job_id:
                    job_backend = self.get_job_backend(run.backend_name)
                    if job_backend:
                        await job_backend.cancel_job(item.job_id)
                    elif ssh_client:
                        await ssh_client.run_command(f"scancel {item.job_id}")
                item.started_at = item.started_at or item.submitted_at
                item.status = RunItemStatus.FAILED
                item.error = "Cancelled"
                item.finished_at = datetime.now(timezone.utc)

        self._persist_run(run)
        self.notify_run(run.id)
        logger.info(f"Cancelled run '{run_id}'")
        return True

    def delete_run(self, run_id: str) -> bool:
        """Delete a terminal run."""
        run = self.runs.get(run_id)
        if run is None:
            return False

        if run.status in (RunStatus.PENDING, RunStatus.RUNNING):
            return False

        # Delete from storage
        if self.storage:
            self.storage.delete_run(run)

        del self.runs[run_id]

        self._run_versions.pop(run_id, None)
        self._run_events.pop(run_id, None)

        logger.info(f"Deleted run '{run_id}'")
        return True

    def get_run(self, run_id: str) -> Run | None:
        """Get a run by ID."""
        return self.runs.get(run_id)

    def get_all_runs(self) -> list[Run]:
        """Get all runs, sorted by creation time (newest first)."""
        return sorted(self.runs.values(), key=lambda r: r.created_at, reverse=True)

    def get_active_runs(self) -> list[Run]:
        """Get all runs that are still running."""
        return [
            r for r in self.runs.values()
            if r.status in (RunStatus.PENDING, RunStatus.RUNNING)
        ]

    async def restore_from_storage(self) -> int:
        """Restore runs from folder storage on startup."""
        if self.storage is None:
            return 0

        all_runs = self.storage.load_all_runs()
        for run_id, run in all_runs.items():
            if run.workflow_name == "_default":
                continue  # Don't load default runs into active management
            if run_id not in self.runs:
                self.runs[run_id] = run
                if run.status in (RunStatus.PENDING, RunStatus.RUNNING):
                    try:
                        await self.process_run(run)
                    except Exception as e:
                        # Never let a single broken run abort server startup.
                        logger.error(
                            f"Failed to process run '{run_id}' during restore: {e}"
                        )

        logger.info(f"Restored {len(self.runs)} runs from storage")
        return len(self.runs)

    def save_dirty(self) -> None:
        """Save all dirty runs to disk."""
        if self.storage:
            self.storage.save_if_dirty(self.runs)

    async def fetch_log_file(
        self,
        run_id: str,
        task_id: str,
        log_type: str = "output",
        tail_lines: int | None = None,
    ) -> tuple[str | None, str | None]:
        """Fetch a log file from the backend.

        SSH-based backends read the file off the filesystem; API-based
        backends (e.g. Batch) route through CloudWatch.  In both cases the
        actual fetch is delegated to ``JobBackend.fetch_log``.
        """
        run = self.runs.get(run_id)
        if run is None:
            return None, f"Run '{run_id}' not found"

        item = run.get_item_by_task_id(task_id)
        if item is None:
            return None, f"Task '{task_id}' not found in run"

        if log_type not in ("output", "error"):
            return None, f"Invalid log_type: {log_type}"

        job_backend = self.get_job_backend(run.backend_name)
        if job_backend is None:
            return None, f"No job backend for '{run.backend_name}'"

        if item.job_id is None:
            return None, "Task has not been submitted yet"

        # Resolve the filesystem log path (SSH backends use it; API backends ignore).
        log_dir = run.log_dir
        ssh_client = self.get_ssh_client(run.backend_name)
        if ssh_client is not None and log_dir.startswith("~"):
            stdout, _, _ = await ssh_client.run_command("echo $HOME")
            home_dir = stdout.strip()
            log_dir = log_dir.replace("~", home_dir, 1)
        if log_type == "output":
            log_path = item.task.get_output_path(run.id, log_dir)
        else:
            log_path = item.task.get_error_path(run.id, log_dir)

        return await job_backend.fetch_log(
            job_id=item.job_id,
            log_path=log_path,
            log_type=log_type,
            tail_lines=tail_lines,
        )


async def load_source_project_config(
    config: ScriptHutConfig,
    source: GitSourceConfig | PathSourceConfig,
    *,
    commit_hash: str | None = None,
    ssh_client: SSHClient | None = None,
) -> ScriptHutConfig | None:
    """Read ``<repo>/scripthut.yaml`` from a source as project-local config.

    Module-level (not a RunManager method) so callers without a manager —
    the disk scanner, CLI local mode — can read project overlays too.

    Returns the parsed ``ScriptHutConfig`` when the file is present.
    Returns ``None`` when there's no ``scripthut.yaml`` at the source
    root — that's the legitimate "no per-repo overlay" case and the
    merge path must treat it as a no-op.

    Raises ``ValueError`` if the YAML carries sections that aren't
    allowed in a project-local file (``backends`` / ``sources`` /
    ``settings`` / ``pricing``) so a misconfigured repo fails the
    run loudly rather than silently dropping the entries. The error
    message reuses ``_validate_project_local_yaml``'s text so users
    get the same diagnostic they'd see from the CLI.

    Read location depends on source type:

    - **Git**: ``git show <sha>:scripthut.yaml`` against the server's
      local clone at ``<sources_cache_dir>/<source.name>``. ``commit_hash=None``
      falls back to ``HEAD``, which after a fresh sync points at the
      configured branch's tip — what the user expects when submitting
      "latest HEAD" runs. Reading from the server's clone (not the
      backend's) keeps git+SSH and git+API-only backends uniform and
      avoids an extra SSH round-trip.
    - **Path**: ``cat <source.path>/scripthut.yaml`` over the
      ``ssh_client`` passed in (the source's backend). The caller
      already has this connection in ``create_run_from_source``.
    """
    import subprocess

    import yaml

    from scripthut.config import (
        ConfigError,
        _validate_project_local_yaml,
    )

    raw_text: str | None = None
    identity = f"source '{source.name}'/scripthut.yaml"

    if isinstance(source, GitSourceConfig):
        clone_path = (
            config.settings.sources_cache_dir_resolved / source.name
        )
        if not clone_path.exists():
            # Cache hasn't been populated yet (server hasn't synced
            # this source). Treat as missing rather than erroring —
            # the run can still proceed without the overlay.
            return None
        ref = commit_hash or "HEAD"
        try:
            proc = subprocess.run(
                ["git", "-C", str(clone_path), "show", f"{ref}:scripthut.yaml"],
                capture_output=True, text=True, timeout=10,
            )
        except (subprocess.SubprocessError, FileNotFoundError) as e:
            # `git` not installed or cache directory unreadable — not
            # the user's per-run problem. Log and skip the overlay.
            logger.warning(f"git show failed for {identity}: {e}")
            return None
        if proc.returncode != 0:
            # `git show` exits 128 when the path doesn't exist at the
            # given commit; treat as "no overlay configured". Any
            # other nonzero is also fail-soft so a transient git
            # issue doesn't block submissions.
            return None
        raw_text = proc.stdout
    elif isinstance(source, PathSourceConfig):
        if ssh_client is None:
            # Path sources always require SSH (verified at the call
            # site); a missing client here is a programming error.
            raise ValueError(
                f"Path source '{source.name}' requires an SSH client "
                "to read its project config"
            )
        stdout, _stderr, exit_code = await ssh_client.run_command(
            f"cat {source.path}/scripthut.yaml",
            timeout=10,
        )
        if exit_code != 0:
            # cat exits 1 when the file is missing; same soft skip.
            return None
        raw_text = stdout
    else:
        return None

    if not raw_text or not raw_text.strip():
        return None

    try:
        raw = yaml.safe_load(raw_text)
    except yaml.YAMLError as e:
        raise ValueError(
            f"{identity}: invalid YAML — {e}"
        ) from e
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError(
            f"{identity}: top level must be a mapping, got {type(raw).__name__}"
        )

    try:
        _validate_project_local_yaml(raw, Path(f"<{identity}>"))
    except ConfigError as e:
        raise ValueError(str(e)) from e

    try:
        return ScriptHutConfig.model_validate(raw)
    except Exception as e:
        raise ValueError(f"{identity}: schema validation failed — {e}") from e
