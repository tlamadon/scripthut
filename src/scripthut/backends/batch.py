"""AWS Batch job backend implementation."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from scripthut.backends.base import JobBackend, JobStats
from scripthut.backends.utils import generate_script_body
from scripthut.models import HPCJob, JobState

if TYPE_CHECKING:
    from scripthut.config_schema import BatchBackendConfig
    from scripthut.runs.models import TaskDefinition

logger = logging.getLogger(__name__)


# TaskDefinition.partition defaults to this value when not explicitly set
# by the task generator (a Slurm-flavored convention). For the Batch backend
# we treat it as "unset" and fall back to the backend's ``aws.job_queue``.
_UNSET_PARTITION_DEFAULT = "normal"


# AWS Batch status -> scripthut JobState
BATCH_STATE_MAP: dict[str, JobState] = {
    "SUBMITTED": JobState.PENDING,
    "PENDING": JobState.PENDING,
    "RUNNABLE": JobState.PENDING,
    "STARTING": JobState.RUNNING,
    "RUNNING": JobState.RUNNING,
    "SUCCEEDED": JobState.COMPLETED,
    "FAILED": JobState.FAILED,
}

BATCH_FAILURE_STATES: dict[str, str] = {
    "FAILED": "Batch job failed",
}

BATCH_TERMINAL_STATES = frozenset({"SUCCEEDED", "FAILED"})


def _ms_to_datetime(ms: int | None) -> datetime | None:
    """Convert AWS epoch-milliseconds to a UTC datetime."""
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    except (ValueError, OverflowError, TypeError):
        return None


def parse_memory_mib(mem_str: str) -> int:
    """Parse a Slurm-style memory string (e.g. '4G', '512M', '4096') to MiB.

    Batch's MEMORY resourceRequirement is expressed in MiB as a string.
    Returns a sensible default (2048 MiB) on parse failure.
    """
    if not mem_str:
        return 2048
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*([KMGTP]?)B?\s*$", mem_str, re.IGNORECASE)
    if not m:
        return 2048
    value = float(m.group(1))
    unit = m.group(2).upper() if m.group(2) else "M"  # bare number → MiB
    multipliers = {
        "K": 1 / 1024,
        "M": 1.0,
        "G": 1024.0,
        "T": 1024.0 * 1024.0,
        "P": 1024.0 * 1024.0 * 1024.0,
    }
    mib = value * multipliers.get(unit, 1.0)
    return max(1, int(mib))


def parse_time_limit_seconds(time_str: str) -> int | None:
    """Parse HH:MM:SS or D-HH:MM:SS to seconds (for Batch attemptDurationSeconds).

    Returns None if unparseable or below Batch's 60-second minimum.
    """
    if not time_str:
        return None
    days = 0
    s = time_str.strip()
    if "-" in s:
        day_part, s = s.split("-", 1)
        try:
            days = int(day_part)
        except ValueError:
            return None
    parts = s.split(":")
    try:
        if len(parts) == 3:
            h, m, sec = int(parts[0]), int(parts[1]), int(float(parts[2]))
        elif len(parts) == 2:
            h, m, sec = 0, int(parts[0]), int(float(parts[1]))
        else:
            return None
    except ValueError:
        return None
    total = days * 86400 + h * 3600 + m * 60 + sec
    if total < 60:
        return None
    return total


def gpu_count_from_gres(gres: str | None) -> int:
    """Extract GPU count from a Slurm-style gres spec ('gpu:2', 'gpu:a100:4')."""
    if not gres:
        return 0
    parts = gres.strip().split(":")
    if not parts or parts[0].lower() != "gpu":
        return 0
    last = parts[-1]
    try:
        return int(last)
    except ValueError:
        return 1


def sanitize_batch_name(name: str, max_len: int = 128) -> str:
    """Batch job/definition names must match [a-zA-Z0-9_-]{1,128}."""
    cleaned = re.sub(r"[^a-zA-Z0-9_-]", "-", name)
    cleaned = cleaned.strip("-") or "scripthut-task"
    return cleaned[:max_len]


class BatchBackend(JobBackend):
    """AWS Batch backend (boto3 wrapped with ``asyncio.to_thread``).

    Maps scripthut concepts onto AWS Batch:

    - ``task.partition`` → Batch job queue name (falls back to ``aws.job_queue``)
    - ``task.cpus`` / ``task.memory`` → container ``resourceRequirements``
    - ``task.time_limit`` → ``timeout.attemptDurationSeconds``
    - ``task.gres='gpu:N'`` → GPU resource requirement
    - ``task.image`` (or backend ``default_image``) → container image
    - Generated bash script → container command via ``containerOverrides``
    """

    def __init__(self, config: BatchBackendConfig) -> None:
        self._config = config
        self._batch_client: Any = None
        self._logs_client: Any = None
        # Memoize auto-registered job definitions (keyed by image signature).
        self._job_def_cache: dict[str, str] = {}
        # Cache the container image of the user-supplied ``job_definition``
        # so we only hit DescribeJobDefinitions once.
        self._configured_jd_image: str | None = None
        # Track whether we've already warned about an image mismatch, so we
        # don't spam the log for every task in a run.
        self._warned_image_mismatch: set[str] = set()
        # Revisions mode: full template (describe_job_definitions result)
        # cached for the first lookup, plus a per-image cache of registered
        # revision ARNs.
        self._jd_template: dict[str, Any] | None = None
        self._revision_cache: dict[str, str] = {}

    @property
    def name(self) -> str:
        return self._config.name

    # -- boto3 plumbing --

    def _get_clients(self) -> tuple[Any, Any]:
        if self._batch_client is None:
            try:
                import boto3
            except ImportError as e:
                raise RuntimeError(
                    "boto3 is required for the AWS Batch backend. "
                    "Install with: pip install boto3"
                ) from e
            session_kwargs: dict[str, Any] = {"region_name": self._config.aws.region}
            if self._config.aws.profile:
                session_kwargs["profile_name"] = self._config.aws.profile
            session = boto3.Session(**session_kwargs)
            self._batch_client = session.client("batch")
            self._logs_client = session.client("logs")
        return self._batch_client, self._logs_client

    async def _run(self, fn: Any, **kwargs: Any) -> Any:
        """Run a sync boto3 call on a worker thread."""
        return await asyncio.to_thread(lambda: fn(**kwargs))

    async def is_available(self) -> bool:
        try:
            batch, _ = self._get_clients()
            await self._run(
                batch.describe_job_queues, jobQueues=[self._config.aws.job_queue]
            )
            return True
        except Exception as e:
            logger.warning(f"Batch availability check failed for '{self.name}': {e}")
            return False

    # -- active job listing --

    async def get_jobs(self, user: str | None = None) -> list[HPCJob]:
        if user:
            logger.debug(
                f"Batch backend '{self.name}' ignores user filter '{user}' (no user scoping)"
            )
        batch, _ = self._get_clients()
        statuses = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]
        all_ids: list[str] = []
        for status in statuses:
            next_token: str | None = None
            while True:
                kwargs: dict[str, Any] = {
                    "jobQueue": self._config.aws.job_queue,
                    "jobStatus": status,
                    "maxResults": 100,
                }
                if next_token:
                    kwargs["nextToken"] = next_token
                try:
                    resp = await self._run(batch.list_jobs, **kwargs)
                except Exception as e:
                    logger.warning(f"Batch list_jobs failed (status={status}): {e}")
                    break
                for j in resp.get("jobSummaryList", []):
                    all_ids.append(j["jobId"])
                next_token = resp.get("nextToken")
                if not next_token:
                    break
        if not all_ids:
            return []
        return await self._describe_jobs_as_hpc(all_ids)

    async def _describe_jobs_as_hpc(self, job_ids: list[str]) -> list[HPCJob]:
        batch, _ = self._get_clients()
        out: list[HPCJob] = []
        for i in range(0, len(job_ids), 100):
            chunk = job_ids[i : i + 100]
            try:
                resp = await self._run(batch.describe_jobs, jobs=chunk)
            except Exception as e:
                logger.warning(f"Batch describe_jobs failed: {e}")
                continue
            for j in resp.get("jobs", []):
                out.append(self._job_dict_to_hpc(j))
        return out

    @staticmethod
    def _extract_resources(job: dict[str, Any]) -> tuple[int, int]:
        """Return ``(vcpus, memory_mib)`` from a describe_jobs result."""
        container = job.get("container") or {}
        rr = container.get("resourceRequirements") or []
        vcpus = 0
        memory_mib = 0
        for r in rr:
            t = r.get("type", "")
            try:
                v = float(r.get("value", 0))
            except (ValueError, TypeError):
                continue
            if t == "VCPU":
                vcpus = int(v)
            elif t == "MEMORY":
                memory_mib = int(v)
        return vcpus, memory_mib

    def _job_dict_to_hpc(self, j: dict[str, Any]) -> HPCJob:
        state = BATCH_STATE_MAP.get(j.get("status", ""), JobState.UNKNOWN)
        vcpus, memory_mib = self._extract_resources(j)
        submit_time = _ms_to_datetime(j.get("createdAt"))
        start_time = _ms_to_datetime(j.get("startedAt"))
        elapsed = "0:00"
        if start_time:
            end = _ms_to_datetime(j.get("stoppedAt")) or datetime.now(timezone.utc)
            secs = max(0, int((end - start_time).total_seconds()))
            h, rem = divmod(secs, 3600)
            m, s = divmod(rem, 60)
            elapsed = f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"
        queue = j.get("jobQueue", "")
        queue_name = queue.split("/")[-1] if "/" in queue else queue
        return HPCJob(
            job_id=j["jobId"],
            name=j.get("jobName", ""),
            user="aws-batch",
            state=state,
            partition=queue_name,
            time_used=elapsed,
            nodes="-",
            cpus=vcpus,
            memory=f"{memory_mib}M" if memory_mib else "-",
            submit_time=submit_time,
            start_time=start_time,
        )

    # -- submission --

    async def submit_job(self, script: str) -> str:
        """Not used for Batch — requires the full TaskDefinition; see submit_task."""
        raise RuntimeError(
            "Batch backend requires submit_task(task, script) instead of submit_job(script). "
            "The run manager should prefer submit_task on backends that define it."
        )

    async def submit_task(
        self,
        task: TaskDefinition,
        script: str,
        env_vars: dict[str, str] | None = None,
    ) -> str:
        """Submit one Batch job for a task.

        If ``job_definition`` is configured on the backend, that definition
        is reused for every submission (and the container image is locked to
        whatever the definition was registered with). Otherwise scripthut
        auto-registers a generic definition per container image.
        """
        vcpus = max(1, task.cpus)
        memory_mib = parse_memory_mib(task.memory)
        gpus = gpu_count_from_gres(task.gres)
        # Only honor task.partition when it's been explicitly set to something
        # other than TaskDefinition's Slurm-flavored default ("normal"); that
        # default doesn't map to a real AWS Batch queue.
        if task.partition and task.partition != _UNSET_PARTITION_DEFAULT:
            queue = task.partition
        else:
            queue = self._config.aws.job_queue
        attempt_seconds = parse_time_limit_seconds(task.time_limit)

        job_def = await self._resolve_job_definition(task)

        resource_requirements: list[dict[str, str]] = [
            {"type": "VCPU", "value": str(vcpus)},
            {"type": "MEMORY", "value": str(memory_mib)},
        ]
        if gpus > 0:
            resource_requirements.append({"type": "GPU", "value": str(gpus)})

        container_overrides: dict[str, Any] = {
            "command": ["bash", "-c", script],
            "resourceRequirements": resource_requirements,
        }
        if env_vars:
            container_overrides["environment"] = [
                {"name": k, "value": v} for k, v in env_vars.items()
            ]

        submit_kwargs: dict[str, Any] = {
            "jobName": sanitize_batch_name(task.name),
            "jobQueue": queue,
            "jobDefinition": job_def,
            "containerOverrides": container_overrides,
        }
        if attempt_seconds:
            submit_kwargs["timeout"] = {"attemptDurationSeconds": attempt_seconds}
        if self._config.retry_attempts > 1:
            submit_kwargs["retryStrategy"] = {"attempts": self._config.retry_attempts}

        batch, _ = self._get_clients()
        try:
            resp = await self._run(batch.submit_job, **submit_kwargs)
        except Exception as e:
            raise RuntimeError(f"Batch submit_job failed: {e}") from e

        job_id = resp["jobId"]
        logger.info(
            f"Submitted task '{task.id}' to Batch queue '{queue}' as job {job_id}"
        )
        return job_id

    async def _resolve_job_definition(self, task: TaskDefinition) -> str:
        """Return the Batch jobDefinition string to submit against.

        Three modes:

        * No ``job_definition`` configured — auto-register one per image,
          keyed by ``(image, roles, log_group)``.
        * ``job_definition`` set with ``job_definition_mode="locked"`` — use
          that definition as-is; warn when the task's image doesn't match.
        * ``job_definition`` set with ``job_definition_mode="revisions"`` —
          use the configured definition as a template; register a new
          revision (via the same ``jobDefinitionName``) the first time a new
          image shows up, cache subsequent submissions per image.
        """
        configured = self._config.job_definition
        if configured:
            if self._config.job_definition_mode == "revisions":
                return await self._jd_revision_for_image(configured, task)
            # "locked" mode: image override is a no-op, warn once per mismatch.
            requested_image = task.image or self._config.default_image
            if requested_image:
                jd_image = await self._describe_configured_jd_image(configured)
                mismatch_key = f"{configured}::{requested_image}"
                if (
                    jd_image
                    and jd_image != requested_image
                    and mismatch_key not in self._warned_image_mismatch
                ):
                    self._warned_image_mismatch.add(mismatch_key)
                    logger.warning(
                        f"Task '{task.id}' requests image '{requested_image}' but "
                        f"job definition '{configured}' is registered with "
                        f"'{jd_image}'. AWS Batch does not support image overrides "
                        f"via containerOverrides — the job will run with "
                        f"'{jd_image}'. To run a different image per task, set "
                        f"`job_definition_mode: revisions` so scripthut registers "
                        f"new revisions on the fly."
                    )
            return configured

        # Auto-register path — require an image.
        image = task.image or self._config.default_image
        if not image:
            raise RuntimeError(
                f"Task '{task.id}' has no container image and backend "
                f"'{self.name}' has neither `default_image` nor `job_definition` set"
            )
        return await self._ensure_job_definition(image)

    # Fields copied verbatim from the template when registering a new
    # revision. ``containerProperties`` gets special treatment (image swap);
    # everything else is passed through if present in the template.
    _JD_PASSTHROUGH_FIELDS = (
        "type",
        "parameters",
        "schedulingPriority",
        "retryStrategy",
        "propagateTags",
        "timeout",
        "tags",
        "platformCapabilities",
        "nodeProperties",
        "eksProperties",
        "ecsProperties",
    )

    async def _jd_revision_for_image(
        self, configured_jd: str, task: TaskDefinition,
    ) -> str:
        """Resolve the Batch jobDefinition for revisions mode.

        Returns ``name:revision`` of a registered revision carrying the task's
        requested image. If the task's image matches the template's image, the
        template itself is used (no new revision). Cached per image.
        """
        template = await self._get_jd_template(configured_jd)
        base_props = template.get("containerProperties") or {}
        base_image = base_props.get("image")
        requested = task.image or self._config.default_image or base_image
        jd_name = template.get("jobDefinitionName")
        if not jd_name:
            raise RuntimeError(
                f"Template job definition '{configured_jd}' has no "
                f"jobDefinitionName — cannot register revisions"
            )

        if not requested or requested == base_image:
            # No image change needed — just submit against the configured value.
            return configured_jd

        if requested in self._revision_cache:
            return self._revision_cache[requested]

        new_props = dict(base_props)
        new_props["image"] = requested

        register_kwargs: dict[str, Any] = {
            "jobDefinitionName": jd_name,
            "containerProperties": new_props,
        }
        for field in self._JD_PASSTHROUGH_FIELDS:
            if field in template and template[field] is not None:
                register_kwargs[field] = template[field]
        register_kwargs.setdefault("type", "container")

        batch, _ = self._get_clients()
        try:
            resp = await self._run(
                batch.register_job_definition, **register_kwargs
            )
        except Exception as e:
            raise RuntimeError(
                f"RegisterJobDefinition failed while creating a new revision "
                f"of '{jd_name}' for image '{requested}': {e}"
            ) from e

        arn = resp["jobDefinitionArn"]
        self._revision_cache[requested] = arn
        logger.info(
            f"Registered new revision of '{jd_name}' for image "
            f"'{requested}' → {arn}"
        )
        return arn

    async def _get_jd_template(self, configured_jd: str) -> dict[str, Any]:
        """Fetch and cache the template job definition for revisions mode.

        Picks the latest ACTIVE revision when given a bare name; passes a
        name:revision or ARN through directly.
        """
        if self._jd_template is not None:
            return self._jd_template

        batch, _ = self._get_clients()
        try:
            if configured_jd.startswith("arn:") or ":" in configured_jd:
                resp = await self._run(
                    batch.describe_job_definitions, jobDefinitions=[configured_jd]
                )
            else:
                resp = await self._run(
                    batch.describe_job_definitions,
                    jobDefinitionName=configured_jd,
                    status="ACTIVE",
                    maxResults=100,
                )
        except Exception as e:
            raise RuntimeError(
                f"DescribeJobDefinitions failed for template '{configured_jd}' "
                f"(revisions mode requires this lookup): {e}"
            ) from e

        defs = resp.get("jobDefinitions") or []
        if not defs:
            raise RuntimeError(
                f"Template job definition '{configured_jd}' not found "
                f"(revisions mode cannot proceed without it)"
            )
        latest = max(defs, key=lambda d: int(d.get("revision", 0)))
        self._jd_template = latest
        base_image = (latest.get("containerProperties") or {}).get("image")
        logger.info(
            f"Loaded Batch template '{latest.get('jobDefinitionName')}' "
            f"(revision {latest.get('revision')}, base image '{base_image}')"
        )
        return latest

    async def _describe_configured_jd_image(self, jd: str) -> str | None:
        """Fetch (and cache) the container image of a configured job definition.

        Accepts a bare name, ``name:revision``, or full ARN. Returns None if
        the image can't be determined (bad permissions, missing definition, etc.)
        — in that case we skip the mismatch check rather than blocking submission.
        """
        if self._configured_jd_image is not None:
            return self._configured_jd_image or None  # empty string → None

        batch, _ = self._get_clients()
        try:
            if jd.startswith("arn:") or ":" in jd:
                resp = await self._run(batch.describe_job_definitions, jobDefinitions=[jd])
            else:
                resp = await self._run(
                    batch.describe_job_definitions,
                    jobDefinitionName=jd,
                    status="ACTIVE",
                    maxResults=100,
                )
        except Exception as e:
            logger.warning(f"describe_job_definitions failed for '{jd}': {e}")
            self._configured_jd_image = ""  # cache the failure
            return None

        defs = resp.get("jobDefinitions") or []
        if not defs:
            logger.warning(f"Job definition '{jd}' not found (empty response)")
            self._configured_jd_image = ""
            return None

        # For bare-name lookups we get all ACTIVE revisions; pick the highest.
        latest = max(defs, key=lambda d: int(d.get("revision", 0)))
        image = (latest.get("containerProperties") or {}).get("image")
        self._configured_jd_image = image or ""
        if image:
            logger.info(
                f"Using configured Batch job definition '{jd}' "
                f"(revision {latest.get('revision')}, image '{image}')"
            )
        return image

    async def _ensure_job_definition(self, image: str) -> str:
        """Register a generic job definition for ``image`` (if not cached)."""
        sig_source = "|".join(
            (
                image,
                self._config.job_role_arn or "",
                self._config.execution_role_arn or "",
                self._config.log_group,
            )
        )
        sig = hashlib.sha1(sig_source.encode()).hexdigest()[:12]
        if sig in self._job_def_cache:
            return self._job_def_cache[sig]

        batch, _ = self._get_clients()
        job_def_name = sanitize_batch_name(f"scripthut-{sig}")
        container_properties: dict[str, Any] = {
            "image": image,
            "command": ["sh", "-c", "echo 'missing command override'"],
            # Placeholder resource reqs; always overridden at submitJob time.
            "resourceRequirements": [
                {"type": "VCPU", "value": "1"},
                {"type": "MEMORY", "value": "512"},
            ],
            "logConfiguration": {"logDriver": "awslogs"},
        }
        if self._config.job_role_arn:
            container_properties["jobRoleArn"] = self._config.job_role_arn
        if self._config.execution_role_arn:
            container_properties["executionRoleArn"] = self._config.execution_role_arn

        try:
            resp = await self._run(
                batch.register_job_definition,
                jobDefinitionName=job_def_name,
                type="container",
                containerProperties=container_properties,
                propagateTags=True,
            )
        except Exception as e:
            raise RuntimeError(f"Batch register_job_definition failed: {e}") from e

        arn = resp["jobDefinitionArn"]
        self._job_def_cache[sig] = arn
        logger.info(f"Registered Batch job definition '{job_def_name}' -> {arn}")
        return arn

    # -- cancellation --

    async def cancel_job(self, job_id: str) -> None:
        batch, _ = self._get_clients()
        reason = "Cancelled via ScriptHut"
        # cancel_job only works before STARTING; terminate_job handles running jobs.
        try:
            await self._run(batch.cancel_job, jobId=job_id, reason=reason)
        except Exception as e:
            logger.debug(f"Batch cancel_job failed for {job_id}: {e}")
        try:
            await self._run(batch.terminate_job, jobId=job_id, reason=reason)
        except Exception as e:
            logger.warning(f"Batch terminate_job failed for {job_id}: {e}")

    # -- post-mortem stats --

    async def get_job_stats(
        self, job_ids: list[str], user: str | None = None
    ) -> dict[str, JobStats]:
        if not job_ids:
            return {}
        batch, _ = self._get_clients()
        stats: dict[str, JobStats] = {}
        for i in range(0, len(job_ids), 100):
            chunk = job_ids[i : i + 100]
            try:
                resp = await self._run(batch.describe_jobs, jobs=chunk)
            except Exception as e:
                logger.warning(f"Batch describe_jobs failed: {e}")
                continue
            for j in resp.get("jobs", []):
                status = j.get("status", "")
                if status not in BATCH_TERMINAL_STATES:
                    continue
                start = _ms_to_datetime(j.get("startedAt"))
                end = _ms_to_datetime(j.get("stoppedAt"))
                total_cpu_s = (
                    (end - start).total_seconds() if (start and end) else 0.0
                )
                stats[j["jobId"]] = JobStats(
                    cpu_efficiency=0.0,  # not exposed by Batch
                    max_rss="",  # not exposed by Batch
                    total_cpu=f"{total_cpu_s:.0f}s",
                    start_time=start,
                    end_time=end,
                    state=status,
                )
        return stats

    # -- cluster info --

    async def get_cluster_info(self) -> tuple[int, int] | None:
        """Return ``(total_vcpus, idle_vcpus)`` across the queue's compute envs.

        Idle is approximated as ``total`` for now — Batch doesn't expose in-use
        vCPU counts directly, and computing it would require summing
        resourceRequirements across all RUNNING jobs.
        """
        batch, _ = self._get_clients()
        try:
            q_resp = await self._run(
                batch.describe_job_queues, jobQueues=[self._config.aws.job_queue]
            )
            queues = q_resp.get("jobQueues", [])
            if not queues:
                return None
            ce_arns = [
                ce["computeEnvironment"]
                for ce in queues[0].get("computeEnvironmentOrder", [])
            ]
            if not ce_arns:
                return None
            total = 0
            for i in range(0, len(ce_arns), 100):
                chunk = ce_arns[i : i + 100]
                ce_resp = await self._run(
                    batch.describe_compute_environments, computeEnvironments=chunk
                )
                for ce in ce_resp.get("computeEnvironments", []):
                    cr = ce.get("computeResources") or {}
                    try:
                        total += int(cr.get("maxvCpus", 0) or 0)
                    except (ValueError, TypeError):
                        pass
            return total, total
        except Exception as e:
            logger.warning(f"Batch get_cluster_info failed: {e}")
            return None

    # -- log fetching --

    async def fetch_log(
        self,
        job_id: str,
        log_path: str,
        log_type: str = "output",
        tail_lines: int | None = None,
    ) -> tuple[str | None, str | None]:
        """Fetch CloudWatch log stream for the job's most recent attempt."""
        if log_type == "error":
            return (
                "[AWS Batch merges stdout and stderr into a single CloudWatch stream — "
                "see the output tab.]",
                None,
            )
        batch, logs = self._get_clients()
        try:
            resp = await self._run(batch.describe_jobs, jobs=[job_id])
        except Exception as e:
            return None, f"describe_jobs failed: {e}"
        jobs = resp.get("jobs") or []
        if not jobs:
            return None, f"Job {job_id} not found"
        job = jobs[0]
        log_stream: str | None = None
        attempts = job.get("attempts") or []
        if attempts:
            last = attempts[-1]
            log_stream = (last.get("container") or {}).get("logStreamName")
        if not log_stream:
            log_stream = (job.get("container") or {}).get("logStreamName")
        if not log_stream:
            return None, "No log stream available yet (job may not have started)"
        try:
            kwargs: dict[str, Any] = {
                "logGroupName": self._config.log_group,
                "logStreamName": log_stream,
                "startFromHead": tail_lines is None,
            }
            if tail_lines:
                kwargs["limit"] = tail_lines
            resp = await self._run(logs.get_log_events, **kwargs)
        except Exception as e:
            return None, f"CloudWatch get_log_events failed: {e}"
        events = resp.get("events") or []
        content = "\n".join(e.get("message", "") for e in events)
        return content or "[No log events yet]", None

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
        """Generate the bash script the container will run.

        AWS Batch has no scheduler directives. Environment variables are
        passed via ``containerOverrides.environment`` at submit time (so the
        container's entrypoint sees them), not as ``export`` lines in this
        script — we add a small comment block listing their names for
        visibility when reviewing the script in the UI.

        If the run's ScriptHut env vars indicate a git workflow
        (SCRIPTHUT_GIT_REPO/_SHA/_BRANCH), a runtime clone+checkout block is
        prepended so the container has the source tree.
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
            # Resolve a relative/home working_dir against the clone root.
            if working_dir in ("", "~"):
                working_dir = "$_SCRIPTHUT_CLONE_DIR"
            elif not working_dir.startswith(("/", "$")):
                working_dir = f"$_SCRIPTHUT_CLONE_DIR/{working_dir}"

        env_comment = ""
        if env_vars_map:
            names = ", ".join(sorted(env_vars_map.keys()))
            env_comment = (
                "# Environment variables (set by AWS Batch containerOverrides):\n"
                f"#   {names}\n"
            )

        extra_init_combined_parts: list[str] = []
        if extra_init:
            extra_init_combined_parts.append(extra_init)
        if env_comment:
            extra_init_combined_parts.append(env_comment)
        if clone_block:
            extra_init_combined_parts.append(clone_block)
        extra_init_combined = "\n".join(extra_init_combined_parts)

        shebang = "#!/bin/bash -l" if login_shell else "#!/bin/bash"
        header = f"{shebang}\nset -o pipefail\n"
        body = generate_script_body(
            task_name=task.name,
            task_id=task.id,
            command=task.command,
            working_dir=working_dir,
            env_vars=None,  # env goes through containerOverrides.environment
            extra_init=extra_init_combined,
            interactive_wait=False,  # tmux interactive wait not supported on Batch
        )
        return header + "\n" + body

    @property
    def failure_states(self) -> dict[str, str]:
        return BATCH_FAILURE_STATES

    @property
    def terminal_states(self) -> frozenset[str]:
        return BATCH_TERMINAL_STATES
