"""AWS Batch job backend implementation."""

import asyncio
import logging
import re
from enum import Enum
from typing import Any

import boto3
from botocore.exceptions import ClientError

from scripthut.backends.base import JobBackend
from scripthut.config_schema import AWSBatchBackendConfig

logger = logging.getLogger(__name__)


class BatchJobState(str, Enum):
    """AWS Batch job states."""

    SUBMITTED = "SUBMITTED"
    PENDING = "PENDING"
    RUNNABLE = "RUNNABLE"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


# States that mean the job is still active (not terminal)
BATCH_ACTIVE_STATES = {
    BatchJobState.SUBMITTED,
    BatchJobState.PENDING,
    BatchJobState.RUNNABLE,
    BatchJobState.STARTING,
    BatchJobState.RUNNING,
}


def _parse_memory_to_mib(memory_str: str) -> int:
    """Parse a memory string like '4G', '512M', '4096' to MiB.

    AWS Batch requires memory in MiB.
    """
    memory_str = memory_str.strip()
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([GMKT]?)(?:i?B)?$", memory_str, re.IGNORECASE)
    if not match:
        # Try plain integer (assume MiB)
        try:
            return int(memory_str)
        except ValueError:
            return 4096  # Default 4 GiB

    value = float(match.group(1))
    unit = match.group(2).upper() if match.group(2) else ""

    if unit == "G":
        return int(value * 1024)
    elif unit == "T":
        return int(value * 1024 * 1024)
    elif unit == "K":
        return max(1, int(value / 1024))
    elif unit == "M" or unit == "":
        return int(value)
    return int(value)


class AWSBatchBackend(JobBackend):
    """AWS Batch job backend using boto3."""

    def __init__(self, config: AWSBatchBackendConfig) -> None:
        self._config = config
        self._batch_client: Any | None = None
        self._logs_client: Any | None = None
        self._job_def_cache: dict[str, str] = {}  # cache_key → job definition ARN

    @property
    def name(self) -> str:
        return self._config.name

    def _get_batch_client(self) -> Any:
        """Lazy-init boto3 Batch client."""
        if self._batch_client is None:
            session = boto3.Session(
                profile_name=self._config.profile,
                region_name=self._config.region,
            )
            self._batch_client = session.client("batch")
        return self._batch_client

    def _get_logs_client(self) -> Any:
        """Lazy-init boto3 CloudWatch Logs client."""
        if self._logs_client is None:
            session = boto3.Session(
                profile_name=self._config.profile,
                region_name=self._config.region,
            )
            self._logs_client = session.client("logs")
        return self._logs_client

    async def register_job_definition(
        self,
        container_image: str,
        env_name: str,
        vcpus: int = 1,
        memory_mib: int = 4096,
    ) -> str:
        """Register or reuse a job definition for a container image.

        Creates a job definition named ``{prefix}-{env_name}`` with the given
        container image. If a definition with the same name already exists and
        has the same container image, it reuses it. Otherwise it registers a
        new revision.

        Returns the job definition ARN.
        """
        cache_key = f"{env_name}:{container_image}"
        if cache_key in self._job_def_cache:
            return self._job_def_cache[cache_key]

        job_def_name = f"{self._config.job_definition_prefix}-{env_name}"
        client = self._get_batch_client()

        # Check if a matching definition already exists
        try:
            resp = await asyncio.to_thread(
                client.describe_job_definitions,
                jobDefinitionName=job_def_name,
                status="ACTIVE",
                maxResults=1,
            )
            existing = resp.get("jobDefinitions", [])
            if existing:
                existing_def = existing[0]
                existing_image = (
                    existing_def.get("containerProperties", {}).get("image", "")
                )
                if existing_image == container_image:
                    arn = existing_def["jobDefinitionArn"]
                    self._job_def_cache[cache_key] = arn
                    logger.info(
                        f"Reusing job definition '{job_def_name}' "
                        f"(rev {existing_def['revision']})"
                    )
                    return arn
        except ClientError as e:
            logger.warning(f"Failed to describe job definitions: {e}")

        # Register a new revision
        logger.info(
            f"Registering job definition '{job_def_name}' "
            f"with image '{container_image}'"
        )
        try:
            resp = await asyncio.to_thread(
                client.register_job_definition,
                jobDefinitionName=job_def_name,
                type="container",
                containerProperties={
                    "image": container_image,
                    "resourceRequirements": [
                        {"type": "VCPU", "value": str(vcpus)},
                        {"type": "MEMORY", "value": str(memory_mib)},
                    ],
                    "command": ["/bin/bash", "-c", "echo 'default'"],
                },
            )
            arn = resp["jobDefinitionArn"]
            self._job_def_cache[cache_key] = arn
            logger.info(f"Registered job definition: {arn}")
            return arn
        except ClientError as e:
            raise ValueError(f"Failed to register job definition: {e}") from e

    async def submit_job(
        self,
        job_def_arn: str,
        job_name: str,
        command: str,
        env_vars: dict[str, str],
        vcpus: int = 1,
        memory_mib: int = 4096,
    ) -> str:
        """Submit a job to AWS Batch.

        Returns the job ID.
        """
        client = self._get_batch_client()

        # Sanitize job name (AWS Batch requires [a-zA-Z0-9_-], max 128 chars)
        sanitized_name = re.sub(r"[^a-zA-Z0-9_-]", "_", job_name)[:128]

        container_overrides: dict[str, Any] = {
            "command": ["/bin/bash", "-c", command],
            "environment": [
                {"name": k, "value": str(v)} for k, v in env_vars.items()
            ],
            "resourceRequirements": [
                {"type": "VCPU", "value": str(vcpus)},
                {"type": "MEMORY", "value": str(memory_mib)},
            ],
        }

        try:
            resp = await asyncio.to_thread(
                client.submit_job,
                jobName=sanitized_name,
                jobQueue=self._config.job_queue,
                jobDefinition=job_def_arn,
                containerOverrides=container_overrides,
            )
            job_id = resp["jobId"]
            logger.info(f"Submitted Batch job '{sanitized_name}' → {job_id}")
            return job_id
        except ClientError as e:
            raise ValueError(f"Failed to submit Batch job: {e}") from e

    async def get_jobs(self, user: str | None = None) -> list[Any]:
        """List active jobs in the queue.

        Returns a list of dicts with keys: jobId, jobName, status, createdAt.
        The ``user`` parameter is ignored for Batch (no user concept).
        """
        client = self._get_batch_client()
        all_jobs: list[dict[str, Any]] = []

        for status in ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"):
            try:
                paginator = client.get_paginator("list_jobs")
                page_iter = paginator.paginate(
                    jobQueue=self._config.job_queue,
                    jobStatus=status,
                )
                # Paginate synchronously inside to_thread
                def _list_pages(pi: Any = page_iter) -> list[dict[str, Any]]:
                    jobs: list[dict[str, Any]] = []
                    for page in pi:
                        jobs.extend(page.get("jobSummaryList", []))
                    return jobs

                jobs = await asyncio.to_thread(_list_pages)
                all_jobs.extend(jobs)
            except ClientError as e:
                logger.warning(f"Failed to list Batch jobs (status={status}): {e}")

        logger.debug(f"Listed {len(all_jobs)} active Batch jobs")
        return all_jobs

    async def describe_jobs(self, job_ids: list[str]) -> dict[str, BatchJobState]:
        """Get status of specific jobs.

        AWS Batch describe_jobs supports up to 100 IDs per call.
        Returns a mapping of job_id → BatchJobState.
        """
        if not job_ids:
            return {}

        client = self._get_batch_client()
        result: dict[str, BatchJobState] = {}

        # Batch in chunks of 100
        for i in range(0, len(job_ids), 100):
            chunk = job_ids[i : i + 100]
            try:
                resp = await asyncio.to_thread(
                    client.describe_jobs, jobs=chunk
                )
                for job in resp.get("jobs", []):
                    try:
                        state = BatchJobState(job["status"])
                    except ValueError:
                        state = BatchJobState.FAILED
                    result[job["jobId"]] = state
            except ClientError as e:
                logger.warning(f"Failed to describe Batch jobs: {e}")

        return result

    async def describe_jobs_full(self, job_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Get full job details for specific jobs.

        Returns a mapping of job_id → full job description dict.
        """
        if not job_ids:
            return {}

        client = self._get_batch_client()
        result: dict[str, dict[str, Any]] = {}

        for i in range(0, len(job_ids), 100):
            chunk = job_ids[i : i + 100]
            try:
                resp = await asyncio.to_thread(
                    client.describe_jobs, jobs=chunk
                )
                for job in resp.get("jobs", []):
                    result[job["jobId"]] = job
            except ClientError as e:
                logger.warning(f"Failed to describe Batch jobs: {e}")

        return result

    async def cancel_job(
        self, job_id: str, reason: str = "Cancelled by ScriptHut"
    ) -> None:
        """Cancel or terminate a Batch job."""
        client = self._get_batch_client()

        try:
            await asyncio.to_thread(
                client.cancel_job, jobId=job_id, reason=reason
            )
            logger.info(f"Cancelled Batch job {job_id}")
        except ClientError:
            # If cancel fails (job already started), try terminate
            try:
                await asyncio.to_thread(
                    client.terminate_job, jobId=job_id, reason=reason
                )
                logger.info(f"Terminated Batch job {job_id}")
            except ClientError as e:
                logger.warning(f"Failed to cancel/terminate Batch job {job_id}: {e}")

    async def get_log_events(
        self,
        log_stream_name: str,
        tail: int | None = None,
    ) -> str | None:
        """Fetch log output from CloudWatch Logs.

        AWS Batch jobs log to CloudWatch under the log group
        ``/aws/batch/job`` (or the configured log_group) with a log stream
        named ``{job_def_name}/default/{job_id}``.

        Args:
            log_stream_name: The CloudWatch log stream name.
            tail: If set, only return the last N events.

        Returns:
            The log content as a string, or None on failure.
        """
        logs_client = self._get_logs_client()
        log_group = self._config.log_group or "/aws/batch/job"

        try:
            kwargs: dict[str, Any] = {
                "logGroupName": log_group,
                "logStreamName": log_stream_name,
                "startFromHead": True,
            }
            if tail:
                kwargs["startFromHead"] = False
                kwargs["limit"] = tail

            resp = await asyncio.to_thread(
                logs_client.get_log_events, **kwargs
            )
            events = resp.get("events", [])
            return "\n".join(e.get("message", "") for e in events)
        except ClientError as e:
            logger.warning(
                f"Failed to fetch CloudWatch logs "
                f"(group={log_group}, stream={log_stream_name}): {e}"
            )
            return None

    async def get_log_stream_for_job(self, job_id: str) -> str | None:
        """Determine the CloudWatch log stream name for a Batch job.

        Looks up the job description to find the log stream name.
        """
        details = await self.describe_jobs_full([job_id])
        job = details.get(job_id)
        if not job:
            return None

        container = job.get("container", {})
        return container.get("logStreamName")

    async def is_available(self) -> bool:
        """Check if the job queue exists and is valid."""
        client = self._get_batch_client()
        try:
            resp = await asyncio.to_thread(
                client.describe_job_queues,
                jobQueues=[self._config.job_queue],
            )
            queues = resp.get("jobQueues", [])
            if queues:
                state = queues[0].get("state", "")
                status = queues[0].get("status", "")
                available = state == "ENABLED" and status == "VALID"
                if not available:
                    logger.warning(
                        f"Batch queue '{self._config.job_queue}' "
                        f"state={state} status={status}"
                    )
                return available
            logger.warning(f"Batch queue '{self._config.job_queue}' not found")
            return False
        except Exception as e:
            logger.warning(f"Batch availability check failed: {e}")
            return False
