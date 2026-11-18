"""PBS/Torque job backend implementation."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from scripthut.backends.base import (
    ClusterInfo,
    JobBackend,
    PartitionInfo,
)
from scripthut.models import HPCJob, JobState
from scripthut.ssh.client import SSHClient


logger = logging.getLogger(__name__)


# Map PBS single-letter states to our JobState
_PBS_STATE_MAP: dict[str, JobState] = {
    "Q": JobState.PENDING,       # Queued
    "H": JobState.SUSPENDED,     # Held
    "R": JobState.RUNNING,       # Running
    "E": JobState.COMPLETING,    # Exiting
    "C": JobState.COMPLETED,     # Completed
    "T": JobState.RUNNING,       # Being moved
    "W": JobState.PENDING,       # Waiting
    "S": JobState.SUSPENDED,     # Suspended
    "B": JobState.RUNNING,       # Array job running
    "F": JobState.COMPLETED,     # Finished (PBS Pro)
    "M": JobState.RUNNING,       # Moved (PBS Pro)
    "X": JobState.COMPLETED,     # Subjob completed (PBS Pro)
    "U": JobState.SUSPENDED,     # Suspended due to workstation becoming busy
}


def _strip_server_suffix(job_id: str) -> str:
    """Strip PBS server suffix from job ID.

    PBS Pro returns IDs like '12345.pbs-server', we want just '12345'.
    """
    return job_id.split(".")[0] if "." in job_id else job_id


def parse_pbs_datetime(dt_str: str) -> datetime | None:
    """Parse PBS datetime string to Python datetime."""
    if not dt_str or dt_str.strip() in ("N/A", "Unknown", "None", "--", ""):
        return None
    try:
        # PBS uses formats like: Wed Feb 12 10:00:00 2026
        dt = datetime.strptime(dt_str.strip(), "%a %b %d %H:%M:%S %Y")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    try:
        # PBS Pro may use ISO-ish format
        dt = datetime.strptime(dt_str.strip(), "%Y-%m-%dT%H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    try:
        # Epoch seconds (some PBS versions)
        ts = int(dt_str.strip())
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except (ValueError, OverflowError):
        pass
    logger.warning(f"Failed to parse PBS datetime: {dt_str}")
    return None


def parse_qstat_line(line: str) -> HPCJob | None:
    """Parse a single line of qstat -a output into an HPCJob.

    Expected format (qstat -a):
    JobID  User  Queue  Jobname  SessID  NDS  TSK  Memory  Time  S  Elap
    """
    parts = line.split()
    if len(parts) < 11:
        return None

    try:
        job_id_raw = parts[0]
        job_id = _strip_server_suffix(job_id_raw)
        user = parts[1]
        queue = parts[2]
        name = parts[3]
        # parts[4] = SessID, parts[5] = NDS (nodes)
        nodes = parts[5] if parts[5] != "--" else "-"
        # parts[6] = TSK (tasks/cpus)
        try:
            cpus = int(parts[6]) if parts[6] != "--" else 1
        except ValueError:
            cpus = 1
        memory = parts[7] if parts[7] != "--" else "-"
        state_char = parts[9]
        elapsed = parts[10] if len(parts) > 10 and parts[10] != "--" else "0:00"

        job_state = _PBS_STATE_MAP.get(state_char, JobState.UNKNOWN)

        return HPCJob(
            job_id=job_id,
            name=name,
            user=user,
            state=job_state,
            partition=queue,
            time_used=elapsed,
            nodes=nodes,
            cpus=cpus,
            memory=memory,
            submit_time=None,
            start_time=None,
        )
    except (ValueError, IndexError) as e:
        logger.warning(f"Failed to parse qstat line: {line}, error: {e}")
        return None


def parse_qstat_queues(stdout: str) -> dict[str, dict]:
    """Parse ``qstat -Qf`` into ``{queue: {type, walltime, state_count}}``.

    ``walltime`` is the queue's ``resources_max.walltime`` (or None), and
    ``state_count`` is the per-state job counter PBS reports, e.g.
    ``{"Queued": 48, "Held": 248, "Running": 34}``. Long values that PBS
    wraps onto a tab-indented continuation line are rejoined first.
    """
    queues: dict[str, dict] = {}
    cur: dict | None = None
    # qstat wraps long attribute values onto a leading-tab continuation line.
    unwrapped = stdout.replace("\n\t", "")
    for line in unwrapped.split("\n"):
        stripped = line.strip()
        m = re.match(r"^Queue:\s+(\S+)", stripped)
        if m:
            cur = {"type": "", "walltime": None, "state_count": {}}
            queues[m.group(1)] = cur
            continue
        if cur is None or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        value = value.strip()
        if key == "queue_type":
            cur["type"] = value.lower()
        elif key == "resources_max.walltime":
            cur["walltime"] = value
        elif key == "state_count":
            sc: dict[str, int] = {}
            for tok in value.split():
                state_name, _, count = tok.partition(":")
                try:
                    sc[state_name] = int(count)
                except ValueError:
                    pass
            cur["state_count"] = sc
    return queues


class PBSBackend(JobBackend):
    """Read-only PBS/Torque scheduler observation over SSH."""

    def __init__(self, ssh_client: SSHClient, default_queue: str | None = None) -> None:
        """Initialize with an SSH client connected to the PBS head node."""
        self._ssh = ssh_client
        self._default_queue = default_queue

    @property
    def name(self) -> str:
        return "pbs"

    async def get_jobs(self, user: str | None = None) -> list[HPCJob]:
        """Fetch current jobs from PBS using qstat -a."""
        cmd = "qstat -a"
        if user:
            cmd += f" -u {user}"

        stdout, stderr, exit_code = await self._ssh.run_command(cmd)

        if exit_code != 0:
            logger.error(f"qstat failed (exit {exit_code}): {stderr}")
            return []

        jobs: list[HPCJob] = []
        in_data = False
        for line in stdout.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            # Skip header lines (look for the dashed separator)
            if line.startswith("---") or line.startswith("==="):
                in_data = True
                continue
            if not in_data:
                # Still in header
                if "Job ID" in line or "Job id" in line:
                    continue
                # Try parsing anyway in case there's no separator
                continue

            job = parse_qstat_line(line)
            if job:
                jobs.append(job)

        logger.debug(f"Fetched {len(jobs)} jobs from PBS")
        return jobs


    async def get_cluster_info(self, user: str | None = None) -> ClusterInfo | None:
        """Fetch cluster availability from pbsnodes + qstat.

        PBS/Torque has no Slurm-style partitions, so all nodes roll up into
        a single pseudo-partition named after the configured queue. Idle CPUs
        are computed from each node's allocated-thread counter
        (``dedicated_threads`` on Torque, ``resources_assigned.ncpus`` on PBS
        Pro) rather than the node ``state`` flag — Torque reports a partially
        used node as ``free``, so a state-only count badly overstates idle.
        Per-user quota is not implemented — ``user`` is accepted for
        interface compatibility.
        """
        _ = user  # unused
        cmd = "pbsnodes -a 2>/dev/null"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"pbsnodes failed: {e}")
            return None

        if exit_code != 0:
            logger.warning(f"pbsnodes failed (exit {exit_code}): {stderr}")
            return None

        total_cpus = 0
        idle_cpus = 0
        alloc_cpus = 0
        other_cpus = 0
        total_gpus = 0
        idle_gpus = 0
        node_count = 0
        mem_per_node_mb: int | None = None
        current_ncpus = 0
        current_state = ""
        current_assigned = 0
        current_physmem_kb = 0
        current_ngpus_avail = 0
        current_ngpus_assigned = 0

        def _reset():
            nonlocal current_ncpus, current_state, current_assigned
            nonlocal current_physmem_kb, current_ngpus_avail, current_ngpus_assigned
            current_ncpus = 0
            current_state = ""
            current_assigned = 0
            current_physmem_kb = 0
            current_ngpus_avail = 0
            current_ngpus_assigned = 0

        def _flush():
            nonlocal total_cpus, idle_cpus, alloc_cpus, other_cpus, node_count
            nonlocal total_gpus, idle_gpus, mem_per_node_mb
            if current_ncpus == 0:
                return
            total_cpus += current_ncpus
            node_count += 1
            if current_physmem_kb and (
                mem_per_node_mb is None or current_physmem_kb // 1024 > mem_per_node_mb
            ):
                mem_per_node_mb = current_physmem_kb // 1024
            is_other = any(
                s in current_state for s in ("down", "offline", "drain", "unknown")
            )
            if is_other:
                other_cpus += current_ncpus
            else:
                assigned = current_assigned
                # Exclusive/busy nodes are fully allocated even when the
                # per-node thread counter is absent.
                if assigned <= 0 and ("exclusive" in current_state or "busy" in current_state):
                    assigned = current_ncpus
                assigned = max(0, min(assigned, current_ncpus))
                alloc_cpus += assigned
                idle_cpus += current_ncpus - assigned
            if current_ngpus_avail > 0:
                total_gpus += current_ngpus_avail
                if not is_other:
                    idle_gpus += max(
                        0, current_ngpus_avail - current_ngpus_assigned
                    )

        for line in stdout.split("\n"):
            line = line.strip()
            if not line:
                _flush()
                _reset()
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()
                if key == "np" or key == "pcpus" or key == "resources_available.ncpus":
                    try:
                        current_ncpus = int(value)
                    except ValueError:
                        pass
                elif key == "state":
                    current_state = value.lower()
                elif key == "dedicated_threads" or key == "resources_assigned.ncpus":
                    try:
                        current_assigned = int(value)
                    except ValueError:
                        pass
                elif key == "status":
                    # Torque packs node metrics into one comma-list value.
                    pm = re.search(r"physmem=(\d+)kb", value)
                    if pm:
                        current_physmem_kb = int(pm.group(1))
                elif key == "resources_available.ngpus":
                    try:
                        current_ngpus_avail = int(value)
                    except ValueError:
                        pass
                elif key == "resources_assigned.ngpus":
                    try:
                        current_ngpus_assigned = int(value)
                    except ValueError:
                        pass

        # Flush last node
        _flush()

        if total_cpus == 0:
            return None

        # Queue metadata (best-effort): names, walltime limit, pending counts.
        queue_meta: dict[str, dict] = {}
        try:
            q_out, _, q_rc = await self._ssh.run_command(
                "qstat -Qf 2>/dev/null", timeout=15
            )
            if q_rc == 0:
                queue_meta = parse_qstat_queues(q_out)
        except Exception as e:
            logger.debug(f"qstat -Qf failed: {e}")

        exec_queues = {
            name: m for name, m in queue_meta.items() if "execution" in m["type"]
        }
        # Aggregate not-yet-running jobs across execution queues so the UI can
        # show a pending badge (and render the partition-details table at all).
        pending_reasons: dict[str, int] = {}
        for m in exec_queues.values():
            for st in ("Queued", "Held", "Waiting"):
                count = m["state_count"].get(st, 0)
                if count:
                    pending_reasons[st] = pending_reasons.get(st, 0) + count

        # Name the rolled-up partition after the configured queue (falling back
        # to the first execution queue), and lift its walltime limit.
        pname = self._default_queue or "default"
        timelimit = None
        if exec_queues:
            primary = (
                self._default_queue
                if self._default_queue in exec_queues
                else next(iter(exec_queues))
            )
            timelimit = exec_queues[primary].get("walltime")
            if not self._default_queue:
                pname = primary

        partition = PartitionInfo(
            name=pname,
            state="up",
            cpus_allocated=alloc_cpus,
            cpus_idle=idle_cpus,
            cpus_other=other_cpus,
            cpus_total=total_cpus,
            nodes_total=node_count,
            is_default=True,
            timelimit=timelimit,
            mem_per_node_mb=mem_per_node_mb,
            gpus_total=total_gpus,
            gpus_idle=idle_gpus,
        )
        return ClusterInfo(partitions=[partition], pending_reasons=pending_reasons)
