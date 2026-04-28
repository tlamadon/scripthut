"""AWS EC2-direct job backend.

Each task runs on its own EC2 instance. Scripthut:

- Launches an instance via ``RunInstances`` with a user-data script that
  ``docker run``s the task's container, tees output to
  ``/var/log/scripthut/task.log``, writes ``/var/run/scripthut/done`` with
  the exit code, and arms a self-shutdown safety timer.
- Polls the instance over an SSM-tunnelled SSH connection
  (see :mod:`scripthut.backends.ec2_ssm`): checks for the sentinel file,
  tails the log for the live UI, fetches the final log to local disk, and
  calls ``TerminateInstances`` when the task is done.
- Reconciles on startup: instances tagged with this backend's prefix are
  picked up from ``DescribeInstances`` so a scripthut restart doesn't
  orphan running work or leak cost.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import shlex
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scripthut.backends.base import JobBackend, JobStats, SubmitResult
from scripthut.backends.ec2_ssm import SSMSSHSession
from scripthut.backends.utils import (
    fetch_log_via_ssh,
    generate_script_body,
    parse_duration_hms,
)
from scripthut.models import HPCJob, JobState

if TYPE_CHECKING:
    from scripthut.config_schema import EC2BackendConfig
    from scripthut.runs.models import TaskDefinition

logger = logging.getLogger(__name__)


# Terminal / failure state names surfaced to scripthut's run manager.
EC2_FAILURE_STATES: dict[str, str] = {
    "FAILED": "Task container exited non-zero",
    "LAUNCH_FAILED": "EC2 instance failed to reach 'running'",
    "TERMINATED_EARLY": "Instance terminated before sentinel was written",
}

EC2_TERMINAL_STATES = frozenset({
    "SUCCEEDED", "FAILED", "LAUNCH_FAILED", "TERMINATED_EARLY",
})


# Fixed paths inside each task instance (written by the user-data script).
SENTINEL_PATH = "/var/run/scripthut/done"
TASK_LOG_PATH = "/var/log/scripthut/task.log"


@dataclass
class _InstanceState:
    """Scripthut's bookkeeping for one EC2 instance running a task."""

    instance_id: str
    task_id: str
    run_id: str
    launched_at: datetime
    availability_zone: str | None = None
    instance_type: str = ""
    # Populated after the task completes + we've pulled its log:
    exit_code: int | None = None
    completed_at: datetime | None = None
    started_at: datetime | None = None
    log_archive_path: Path | None = None
    # Terminal state name surfaced via get_job_stats (drives run_manager).
    terminal_state: str | None = None


class EC2Backend(JobBackend):
    """Job backend that runs each task on its own on-demand EC2 instance."""

    def __init__(self, config: EC2BackendConfig, archive_root: Path) -> None:
        self._config = config
        # Logs captured on completion land at
        # ``<archive_root>/<run_id>/<task_id>.log``.
        self._archive_root = archive_root
        self._ec2_client: Any = None
        self._ec2ic_client: Any = None
        # Tracked instances (pending + running + sentinel-not-yet-seen).
        self._instances: dict[str, _InstanceState] = {}
        # Completed instances — kept around so get_job_stats can return
        # their stats after the instance has been terminated.
        self._completed: dict[str, _InstanceState] = {}
        # Bound concurrency so a big wave of poll-time probes doesn't
        # spawn N parallel SSM sessions or asyncssh connections.
        self._probe_sem = asyncio.Semaphore(5)

    @property
    def name(self) -> str:
        return self._config.name

    # -- boto3 plumbing --

    def _get_clients(self) -> tuple[Any, Any]:
        if self._ec2_client is None:
            try:
                import boto3
            except ImportError as e:
                raise RuntimeError(
                    "boto3 is required for the EC2 backend. "
                    "Install with: pip install 'scripthut[batch]'"
                ) from e
            session_kwargs: dict[str, Any] = {"region_name": self._config.aws.region}
            if self._config.aws.profile:
                session_kwargs["profile_name"] = self._config.aws.profile
            session = boto3.Session(**session_kwargs)
            self._ec2_client = session.client("ec2")
            self._ec2ic_client = session.client("ec2-instance-connect")
        return self._ec2_client, self._ec2ic_client

    async def _run(self, fn: Any, **kwargs: Any) -> Any:
        """Run a sync boto3 call on a worker thread."""
        return await asyncio.to_thread(lambda: fn(**kwargs))

    async def is_available(self) -> bool:
        try:
            ec2, _ = self._get_clients()
            await self._run(
                ec2.describe_subnets, SubnetIds=[self._config.subnet_id]
            )
            return True
        except Exception as e:
            logger.warning(f"EC2 availability check failed for '{self.name}': {e}")
            return False

    # -- task submission --

    async def submit_job(self, script: str) -> str:
        raise RuntimeError(
            "EC2 backend requires submit_task(task, script) — call that instead."
        )

    async def submit_task(
        self,
        task: TaskDefinition,
        script: str,
        env_vars: dict[str, str] | None = None,
    ) -> SubmitResult:
        if len(self._instances) >= self._config.max_instances:
            raise RuntimeError(
                f"EC2 backend '{self.name}' is at max_instances "
                f"({self._config.max_instances}); submission refused."
            )
        image = task.image or self._config.default_image
        if not image:
            raise RuntimeError(
                f"Task '{task.id}' has no container image and backend "
                f"'{self.name}' has no default_image configured"
            )
        instance_type = self._instance_type_for(task)
        run_id = (env_vars or {}).get("SCRIPTHUT_RUN_ID", "")
        user_data = self._build_user_data(
            task=task, script=script, env_vars=env_vars or {}, image=image,
        )

        run_kwargs: dict[str, Any] = {
            "ImageId": self._config.ami,
            "InstanceType": instance_type,
            "MinCount": 1,
            "MaxCount": 1,
            "SubnetId": self._config.subnet_id,
            "UserData": user_data,
            "InstanceInitiatedShutdownBehavior": "terminate",
            "TagSpecifications": [{
                "ResourceType": "instance",
                "Tags": self._build_tags(task=task, run_id=run_id),
            }],
        }
        if self._config.security_group_ids:
            run_kwargs["SecurityGroupIds"] = list(self._config.security_group_ids)
        if self._config.instance_profile_arn:
            run_kwargs["IamInstanceProfile"] = {
                "Arn": self._config.instance_profile_arn
            }

        ec2, _ = self._get_clients()
        try:
            resp = await self._run(ec2.run_instances, **run_kwargs)
        except Exception as e:
            raise RuntimeError(f"EC2 RunInstances failed: {e}") from e

        inst = resp["Instances"][0]
        instance_id = inst["InstanceId"]
        az = (inst.get("Placement") or {}).get("AvailabilityZone")

        self._instances[instance_id] = _InstanceState(
            instance_id=instance_id,
            task_id=task.id,
            run_id=run_id,
            launched_at=datetime.now(timezone.utc),
            availability_zone=az,
            instance_type=instance_type,
        )
        logger.info(
            f"Launched EC2 {instance_id} ({instance_type}) for task "
            f"'{task.id}' (backend '{self.name}')"
        )
        submit_output = (
            f"EC2 instance={instance_id} type={instance_type} az={az or '-'}"
        )
        return SubmitResult(job_id=instance_id, submit_output=submit_output)

    def _instance_type_for(self, task: TaskDefinition) -> str:
        partition = (task.partition or "").strip()
        types = self._config.instance_types
        # Ignore the Slurm-flavored TaskDefinition default.
        if partition and partition != "normal" and partition in types:
            return types[partition]
        if "default" in types:
            return types["default"]
        # Fall back to any entry the user configured, alphabetical order
        # for determinism.
        if types:
            return types[sorted(types.keys())[0]]
        raise RuntimeError(
            f"Backend '{self.name}' has no instance_types configured"
        )

    def _build_tags(
        self, *, task: TaskDefinition, run_id: str,
    ) -> list[dict[str, str]]:
        prefix = self._config.tag_prefix
        tags: list[dict[str, str]] = [
            {"Key": f"{prefix}:backend", "Value": self.name},
            {"Key": f"{prefix}:task-id", "Value": task.id},
            {"Key": f"{prefix}:run-id", "Value": run_id or ""},
            {"Key": "Name", "Value": f"scripthut-{task.id}"[:255]},
        ]
        for k, v in (self._config.extra_tags or {}).items():
            tags.append({"Key": k, "Value": v})
        return tags

    def _build_user_data(
        self,
        *,
        task: TaskDefinition,
        script: str,
        env_vars: dict[str, str],
        image: str,
    ) -> str:
        """Build the bash script the EC2 instance runs on boot."""
        script_b64 = base64.b64encode(script.encode("utf-8")).decode("ascii")
        env_flags = " ".join(
            f"-e {shlex.quote(f'{k}={v}')}" for k, v in env_vars.items()
        )
        # Hard timeout = task.time_limit. Safety shutdown = that + slack.
        time_limit_s = int(parse_duration_hms(task.time_limit or "1:00:00")) or 3600
        safety_shutdown_s = time_limit_s + self._config.idle_terminate_seconds
        return f"""#!/bin/bash
set -euxo pipefail
mkdir -p /var/run/scripthut /var/log/scripthut

# Safety net: if scripthut never collects our log, self-terminate so we
# don't accrue cost. We wait the task's own time_limit plus slack.
(
  sleep {safety_shutdown_s}
  shutdown -h now
) &
SAFETY_PID=$!
disown $SAFETY_PID

TASK_SCRIPT=$(mktemp)
echo {shlex.quote(script_b64)} | base64 -d > "$TASK_SCRIPT"
chmod +x "$TASK_SCRIPT"

# Pull first so docker run doesn't mix pull output with task output.
docker pull {shlex.quote(image)} || true

set +e
timeout {time_limit_s}s docker run --rm \\
  -v "$TASK_SCRIPT":/scripthut-task.sh:ro \\
  {env_flags} \\
  {shlex.quote(image)} \\
  bash /scripthut-task.sh 2>&1 | tee /var/log/scripthut/task.log
EXIT=${{PIPESTATUS[0]}}
set -e

# Write sentinel *last* so scripthut only reads logs once everything's done.
echo "$EXIT" > /var/run/scripthut/done
"""

    # -- cancellation --

    async def cancel_job(self, job_id: str) -> None:
        ec2, _ = self._get_clients()
        try:
            await self._run(ec2.terminate_instances, InstanceIds=[job_id])
            logger.info(f"Terminated EC2 {job_id} (cancel_job)")
        except Exception as e:
            logger.warning(f"EC2 TerminateInstances failed for {job_id}: {e}")
        # Record as TERMINATED_EARLY if we hadn't already captured a stat.
        state = self._instances.pop(job_id, None)
        if state is not None:
            state.completed_at = datetime.now(timezone.utc)
            state.terminal_state = "TERMINATED_EARLY"
            state.exit_code = state.exit_code if state.exit_code is not None else -1
            self._completed[job_id] = state

    # -- polling / completion detection --

    async def get_jobs(self, user: str | None = None) -> list[HPCJob]:
        """Describe our tagged instances, finalize ones whose sentinel is up,
        and return the rest as active HPCJobs."""
        if user:
            logger.debug(
                f"EC2 backend '{self.name}' ignores user filter '{user}'"
            )

        ec2, _ = self._get_clients()
        try:
            resp = await self._run(
                ec2.describe_instances,
                Filters=[{
                    "Name": f"tag:{self._config.tag_prefix}:backend",
                    "Values": [self.name],
                }],
            )
        except Exception as e:
            logger.warning(f"EC2 DescribeInstances failed: {e}")
            return []

        live_instances: list[dict[str, Any]] = []
        for reservation in resp.get("Reservations", []):
            for inst in reservation.get("Instances", []):
                live_instances.append(inst)

        # Startup-reconcile: pick up any instance with our tag that we
        # don't yet know about.
        for inst in live_instances:
            iid = inst["InstanceId"]
            state_name = (inst.get("State") or {}).get("Name", "")
            if state_name in ("terminated", "shutting-down", "stopped"):
                continue
            if iid in self._instances or iid in self._completed:
                continue
            tags = {t["Key"]: t["Value"] for t in (inst.get("Tags") or [])}
            task_id = tags.get(f"{self._config.tag_prefix}:task-id", "")
            run_id = tags.get(f"{self._config.tag_prefix}:run-id", "")
            if not task_id:
                continue  # not scripthut's or missing tags
            launch_time_raw = inst.get("LaunchTime")
            launched = _aws_datetime(launch_time_raw) or datetime.now(timezone.utc)
            self._instances[iid] = _InstanceState(
                instance_id=iid,
                task_id=task_id,
                run_id=run_id,
                launched_at=launched,
                availability_zone=(inst.get("Placement") or {}).get("AvailabilityZone"),
                instance_type=inst.get("InstanceType", ""),
            )
            logger.info(
                f"Reconciled existing EC2 {iid} (task '{task_id}') on startup"
            )

        # Build a quick lookup for instance dicts by ID.
        inst_by_id = {i["InstanceId"]: i for i in live_instances}

        # Probe each tracked instance in parallel; finalize those that are
        # done, retain those still running.
        tracked = list(self._instances.values())
        results = await asyncio.gather(
            *[self._probe_one(st, inst_by_id.get(st.instance_id)) for st in tracked],
            return_exceptions=True,
        )

        active: list[HPCJob] = []
        for state, outcome in zip(tracked, results, strict=False):
            if isinstance(outcome, Exception):
                logger.warning(
                    f"Probe for EC2 {state.instance_id} failed: {outcome}"
                )
                # Leave it tracked; try again next poll.
                inst = inst_by_id.get(state.instance_id)
                if inst is not None:
                    active.append(_hpc_job_from_instance(state, inst))
                continue
            if outcome is None:
                # Still running; get the current instance dict for state.
                inst = inst_by_id.get(state.instance_id)
                if inst is not None:
                    active.append(_hpc_job_from_instance(state, inst))
            # If outcome is an InstanceState, the probe finalized it —
            # already moved to self._completed; skip.

        return active

    async def _probe_one(
        self,
        state: _InstanceState,
        inst: dict[str, Any] | None,
    ) -> _InstanceState | None:
        """Probe a single instance. Returns the state on finalization, None otherwise."""
        if inst is None:
            # Instance is gone from EC2 entirely (rare — we expected to
            # catch it earlier). Treat as terminated_early.
            state.completed_at = datetime.now(timezone.utc)
            state.terminal_state = "TERMINATED_EARLY"
            state.exit_code = state.exit_code if state.exit_code is not None else -1
            self._completed[state.instance_id] = state
            self._instances.pop(state.instance_id, None)
            return state

        state_name = (inst.get("State") or {}).get("Name", "")
        if state_name in ("pending",):
            return None  # still booting
        if state_name in ("shutting-down", "terminated", "stopped", "stopping"):
            # Instance terminated — if we hadn't already archived, we've lost
            # the log. Mark as TERMINATED_EARLY unless we have an exit code.
            if state.terminal_state is None:
                state.completed_at = _aws_datetime(inst.get("StateTransitionReason")) or \
                    datetime.now(timezone.utc)
                state.terminal_state = "TERMINATED_EARLY"
                if state.exit_code is None:
                    state.exit_code = -1
                self._completed[state.instance_id] = state
                self._instances.pop(state.instance_id, None)
            return state

        # state_name is "running" — probe for the sentinel over SSH via SSM.
        async with self._probe_sem:
            exit_code = await self._check_sentinel(state)
        if exit_code is None:
            return None  # task still running

        # Task finished — archive log, then terminate the instance.
        await self._archive_log(state)
        await self._terminate_instance(state.instance_id)

        state.exit_code = exit_code
        state.completed_at = datetime.now(timezone.utc)
        state.started_at = state.started_at or state.launched_at
        state.terminal_state = "SUCCEEDED" if exit_code == 0 else "FAILED"
        self._completed[state.instance_id] = state
        self._instances.pop(state.instance_id, None)
        logger.info(
            f"EC2 task '{state.task_id}' on {state.instance_id} finished "
            f"with exit_code={exit_code}"
        )
        return state

    async def _open_ssh_session(self, state: _InstanceState) -> SSMSSHSession:
        _, ec2ic = self._get_clients()
        if not state.availability_zone:
            raise RuntimeError(
                f"No availability zone for EC2 {state.instance_id}; "
                f"EC2 Instance Connect requires one."
            )
        return SSMSSHSession(
            instance_id=state.instance_id,
            availability_zone=state.availability_zone,
            ssh_user=self._config.ssh_user,
            aws_region=self._config.aws.region,
            aws_profile=self._config.aws.profile,
            ec2ic_client=ec2ic,
        )

    async def _check_sentinel(self, state: _InstanceState) -> int | None:
        """If the sentinel is present, return its int exit code; else None."""
        try:
            async with await self._open_ssh_session(state) as session:
                stdout, _, exit_code = await session.client.run_command(
                    f"test -f {SENTINEL_PATH} && cat {SENTINEL_PATH} || echo MISSING",
                    timeout=15,
                )
        except Exception as e:
            logger.debug(
                f"sentinel check on {state.instance_id} failed "
                f"(will retry next poll): {e}"
            )
            return None
        if exit_code != 0:
            return None
        line = stdout.strip()
        if line == "MISSING" or not line:
            return None
        try:
            return int(line)
        except ValueError:
            logger.warning(
                f"Sentinel on {state.instance_id} had unparseable contents: {line!r}"
            )
            return None

    async def _archive_log(self, state: _InstanceState) -> None:
        """Fetch the task log from the instance and store it locally."""
        archive_path = self._archive_path_for(state)
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with await self._open_ssh_session(state) as session:
                content, err = await fetch_log_via_ssh(
                    session.client, TASK_LOG_PATH, tail_lines=None,
                )
        except Exception as e:
            logger.warning(
                f"Failed to archive log for EC2 {state.instance_id}: {e}"
            )
            return
        if err is not None or content is None:
            logger.warning(
                f"Could not read task log on {state.instance_id}: {err}"
            )
            return
        archive_path.write_text(content)
        state.log_archive_path = archive_path
        logger.info(
            f"Archived task log for '{state.task_id}' to {archive_path}"
        )

    def _archive_path_for(self, state: _InstanceState) -> Path:
        run_id = state.run_id or "unknown-run"
        return self._archive_root / run_id / f"{state.task_id}.log"

    async def _terminate_instance(self, instance_id: str) -> None:
        ec2, _ = self._get_clients()
        try:
            await self._run(ec2.terminate_instances, InstanceIds=[instance_id])
        except Exception as e:
            logger.warning(f"EC2 TerminateInstances failed for {instance_id}: {e}")

    # -- stats / cluster info / disk --

    async def get_job_stats(
        self, job_ids: list[str], user: str | None = None,
    ) -> dict[str, JobStats]:
        if not job_ids:
            return {}
        stats: dict[str, JobStats] = {}
        for jid in job_ids:
            state = self._completed.get(jid)
            if state is None:
                continue
            total = 0.0
            if state.started_at and state.completed_at:
                total = (state.completed_at - state.started_at).total_seconds()
            stats[jid] = JobStats(
                cpu_efficiency=0.0,  # not tracked
                max_rss="",
                total_cpu=f"{total:.0f}s",
                start_time=state.started_at,
                end_time=state.completed_at,
                state=state.terminal_state,
            )
        return stats

    async def get_cluster_info(self) -> tuple[int, int] | None:
        """Approximate cluster info: (max_slots, idle_slots) in task units."""
        used = len(self._instances)
        total = self._config.max_instances
        idle = max(0, total - used)
        return total, idle

    # -- log fetching (UI) --

    async def fetch_log(
        self,
        job_id: str,
        log_path: str,
        log_type: str = "output",
        tail_lines: int | None = None,
    ) -> tuple[str | None, str | None]:
        if log_type == "error":
            return (
                "[EC2 backend captures stdout+stderr in a single file — "
                "see the output tab.]",
                None,
            )
        # Finished task — read the local archive.
        done = self._completed.get(job_id)
        if done is not None and done.log_archive_path is not None:
            try:
                content = done.log_archive_path.read_text()
            except Exception as e:
                return None, f"Could not read archived log: {e}"
            if tail_lines:
                content = "\n".join(content.splitlines()[-tail_lines:])
            return content, None

        # Active task — live tail via SSM-tunnelled SSH.
        active = self._instances.get(job_id)
        if active is None:
            return None, f"EC2 instance {job_id} is not tracked by this backend"
        try:
            async with await self._open_ssh_session(active) as session:
                return await fetch_log_via_ssh(
                    session.client, TASK_LOG_PATH, tail_lines=tail_lines,
                )
        except Exception as e:
            return None, f"Could not live-tail log on {job_id}: {e}"

    # -- script generation --

    def generate_script(
        self,
        task: TaskDefinition,
        run_id: str,
        log_dir: str,
        account: str | None = None,
        login_shell: bool = False,
        env_vars: dict[str, str] | None = None,
        extra_init: str = "",
        interactive_wait: bool = False,
    ) -> str:
        """Generate the bash script that runs *inside* the container.

        The EC2 user-data wrapper (built at submit time) pulls the container,
        writes this script to a tmpfile, mounts it read-only inside the
        container, and runs ``bash /scripthut-task.sh``.
        """
        env_vars_map = dict(env_vars or {})
        git_repo = env_vars_map.get("SCRIPTHUT_GIT_REPO")
        git_sha = env_vars_map.get("SCRIPTHUT_GIT_SHA")
        git_branch = env_vars_map.get("SCRIPTHUT_GIT_BRANCH")

        clone_block = ""
        working_dir = task.working_dir
        if git_repo:
            ref = git_sha or git_branch or "HEAD"
            clone_block = (
                "# --- ScriptHut runtime git clone ---\n"
                '_SCRIPTHUT_CLONE_DIR="${SCRIPTHUT_CLONE_DIR:-/tmp/scripthut-src}"\n'
                'if [ ! -d "$_SCRIPTHUT_CLONE_DIR/.git" ]; then\n'
                f'    git clone {git_repo} "$_SCRIPTHUT_CLONE_DIR"\n'
                "fi\n"
                f'(cd "$_SCRIPTHUT_CLONE_DIR" && git fetch --all --tags && git checkout {ref})\n'
                "# --- End git clone ---\n"
            )
            if working_dir in ("", "~"):
                working_dir = "$_SCRIPTHUT_CLONE_DIR"
            elif not working_dir.startswith(("/", "$")):
                working_dir = f"$_SCRIPTHUT_CLONE_DIR/{working_dir}"

        env_comment = ""
        if env_vars_map:
            names = ", ".join(sorted(env_vars_map.keys()))
            env_comment = (
                "# Environment variables (injected via docker run -e):\n"
                f"#   {names}\n"
            )

        extra_init_parts: list[str] = []
        if extra_init:
            extra_init_parts.append(extra_init)
        if env_comment:
            extra_init_parts.append(env_comment)
        if clone_block:
            extra_init_parts.append(clone_block)
        extra_init_combined = "\n".join(extra_init_parts)

        shebang = "#!/bin/bash -l" if login_shell else "#!/bin/bash"
        header = f"{shebang}\nset -o pipefail\n"
        body = generate_script_body(
            task_name=task.name,
            task_id=task.id,
            command=task.command,
            working_dir=working_dir,
            env_vars=None,  # injected by user-data via docker run -e
            extra_init=extra_init_combined,
            interactive_wait=False,
        )
        return header + "\n" + body

    @property
    def failure_states(self) -> dict[str, str]:
        return EC2_FAILURE_STATES

    @property
    def terminal_states(self) -> frozenset[str]:
        return EC2_TERMINAL_STATES


# -- helpers --


def _aws_datetime(value: Any) -> datetime | None:
    """Convert boto3's datetime (tz-aware) or an ISO string to a UTC datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        with contextlib.suppress(ValueError):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
    return None


def _hpc_job_from_instance(
    state: _InstanceState, inst: dict[str, Any],
) -> HPCJob:
    ec2_state = (inst.get("State") or {}).get("Name", "")
    mapped = {
        "pending": JobState.PENDING,
        "running": JobState.RUNNING,
        "stopping": JobState.COMPLETING,
        "shutting-down": JobState.COMPLETING,
        "stopped": JobState.COMPLETED,
        "terminated": JobState.COMPLETED,
    }.get(ec2_state, JobState.UNKNOWN)
    launch = _aws_datetime(inst.get("LaunchTime")) or state.launched_at
    start = launch if ec2_state == "running" else None
    elapsed = "0:00"
    if start is not None:
        secs = max(0, int((datetime.now(timezone.utc) - start).total_seconds()))
        h, rem = divmod(secs, 3600)
        m, s = divmod(rem, 60)
        elapsed = f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"
    # EC2 instance type is our "cpus" proxy — we don't parse it precisely;
    # show it as-is in "memory" to give the UI something useful.
    return HPCJob(
        job_id=state.instance_id,
        name=state.task_id,
        user="ec2",
        state=mapped,
        partition=state.instance_type or "-",
        time_used=elapsed,
        nodes=(inst.get("Placement") or {}).get("AvailabilityZone", "-"),
        cpus=0,
        memory="-",
        submit_time=state.launched_at,
        start_time=start,
    )
