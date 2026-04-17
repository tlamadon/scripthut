"""PBS/Torque job backend implementation."""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from scripthut.backends.base import DiskInfo, JobBackend, JobStats
from scripthut.backends.utils import (
    fetch_disk_info,
    format_bytes,
    generate_script_body,
    parse_duration_hms,
    parse_rss_to_bytes,
)
from scripthut.models import HPCJob, JobState
from scripthut.ssh.client import SSHClient

if TYPE_CHECKING:
    from scripthut.runs.models import TaskDefinition

logger = logging.getLogger(__name__)


# PBS failure states from accounting
PBS_FAILURE_STATES: dict[str, str] = {
    "FAILED": "Non-zero exit code",
    "CANCELLED": "Cancelled (qdel)",
    "WALLTIME_EXCEEDED": "Exceeded walltime",
    "MEM_EXCEEDED": "Exceeded memory limit",
    "NODE_FAIL": "Node failure",
}

# All terminal PBS states
PBS_TERMINAL_STATES = frozenset({
    "COMPLETED", "FAILED", "CANCELLED", "WALLTIME_EXCEEDED",
    "MEM_EXCEEDED", "NODE_FAIL",
})

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


def _gres_to_pbs_gpus(gres: str) -> int | None:
    """Translate a Slurm-style gres spec to a PBS GPU count.

    Accepts forms like ``gpu:2``, ``gpu:v100:1``, or bare ``gpu`` (count 1).
    Returns the integer count if the resource is ``gpu``, else None (and logs
    a warning for non-GPU gres, which has no portable PBS equivalent).
    """
    spec = gres.strip()
    if not spec:
        return None
    parts = spec.split(":")
    if parts[0].lower() != "gpu":
        logger.warning(
            f"PBS backend only maps GPU gres; ignoring non-GPU gres '{gres}'"
        )
        return None
    last = parts[-1]
    if last.lower() == "gpu":
        return 1
    try:
        return int(last)
    except ValueError:
        logger.warning(f"Could not parse GPU count from gres '{gres}'")
        return None


def _convert_memory_to_pbs(mem_str: str) -> str:
    """Convert memory string from Slurm format to PBS format.

    Slurm: 4G, 512M, 1024K
    PBS:   4gb, 512mb, 1024kb
    """
    match = re.match(r"^(\d+)([KMGTP]?)$", mem_str.strip(), re.IGNORECASE)
    if not match:
        return mem_str.lower()
    value = match.group(1)
    unit = match.group(2).upper() if match.group(2) else ""
    unit_map = {"K": "kb", "M": "mb", "G": "gb", "T": "tb", "P": "pb", "": "b"}
    return f"{value}{unit_map.get(unit, 'b')}"


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
        time_limit = parts[8] if parts[8] != "--" else "0:00"
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


class PBSBackend(JobBackend):
    """PBS/Torque job backend using SSH to run qstat/qsub/qdel."""

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

    async def get_job_stats(
        self, job_ids: list[str], user: str | None = None
    ) -> dict[str, JobStats]:
        """Fetch resource utilization stats for completed jobs using qstat -xf.

        PBS Pro: qstat -xf <job_id> returns detailed XML/text output for
        completed jobs. For Torque, tracejob or qstat -f may be needed.
        """
        if not job_ids:
            return {}

        stats: dict[str, JobStats] = {}

        for job_id in job_ids:
            # Try qstat -xf first (PBS Pro), fall back to tracejob (Torque)
            s = await self._get_job_stats_qstat(job_id)
            if s is None:
                s = await self._get_job_stats_tracejob(job_id)
            if s is not None:
                stats[job_id] = s

        logger.info(
            f"PBS stats: fetched {len(stats)}/{len(job_ids)} jobs"
        )
        return stats

    async def _get_job_stats_qstat(self, job_id: str) -> JobStats | None:
        """Try fetching stats via qstat -xf (PBS Pro / some Torque)."""
        try:
            stdout, _, exit_code = await self._ssh.run_command(
                f"qstat -xf {job_id} 2>/dev/null", timeout=15
            )
        except Exception:
            return None
        if exit_code != 0 or not stdout.strip():
            return None
        parsed = self._parse_qstat_xf(stdout)
        return parsed.get(job_id)

    async def _get_job_stats_tracejob(self, job_id: str) -> JobStats | None:
        """Fetch stats via tracejob (Torque fallback).

        Parses the "obit" / exit line from tracejob output, e.g.:
            Exit_status=0 resources_used.cput=1 resources_used.walltime=00:00:11
            resources_used.mem=0kb resources_used.vmem=0kb
        """
        try:
            stdout, _, exit_code = await self._ssh.run_command(
                f"tracejob -n 1 {job_id} 2>/dev/null", timeout=15
            )
        except Exception as e:
            logger.debug(f"tracejob failed for {job_id}: {e}")
            return None
        if exit_code != 0 or not stdout.strip():
            return None
        return self._parse_tracejob(job_id, stdout)

    def _parse_tracejob(self, job_id: str, output: str) -> JobStats | None:
        """Parse tracejob output into JobStats."""
        attrs: dict[str, str] = {}
        job_run_time: datetime | None = None
        job_exit_time: datetime | None = None

        for line in output.split("\n"):
            line = line.strip()

            # Parse "Job Run at request of" line for start time
            # Format: MM/DD/YYYY HH:MM:SS.mmm S    Job Run at request of ...
            if "Job Run at request" in line:
                job_run_time = self._parse_tracejob_timestamp(line)

            # Parse the exit/resources line
            # Format: MM/DD/YYYY HH:MM:SS.mmm S    Exit_status=0 resources_used.cput=...
            if "Exit_status=" in line:
                job_exit_time = self._parse_tracejob_timestamp(line)
                # Extract key=value pairs from the line
                # Find the part after the S marker
                parts = line.split("    ", 1)
                if len(parts) >= 2:
                    kv_part = parts[-1]
                else:
                    kv_part = line
                for token in kv_part.split():
                    if "=" in token:
                        key, _, value = token.partition("=")
                        attrs[key.strip()] = value.strip()

        if not attrs:
            return None

        # Map tracejob fields to the same format as qstat -xf
        walltime_str = attrs.get("resources_used.walltime", "")
        cput_str = attrs.get("resources_used.cput", "")
        mem_str = attrs.get("resources_used.mem", "")

        walltime_s = parse_duration_hms(walltime_str)
        # tracejob may report cput as plain seconds (e.g. "1")
        if cput_str and ":" not in cput_str:
            try:
                cput_s = float(cput_str)
            except ValueError:
                cput_s = 0.0
        else:
            cput_s = parse_duration_hms(cput_str)

        mem_bytes = 0
        if mem_str:
            mem_clean = re.sub(r"([kmgtp])b$", lambda m: m.group(1).upper(), mem_str, flags=re.IGNORECASE)
            mem_bytes = parse_rss_to_bytes(mem_clean)
        rss_formatted = format_bytes(mem_bytes)

        denominator = walltime_s * 1  # tracejob doesn't report ncpus easily
        efficiency = (cput_s / denominator * 100) if denominator > 0 else 0.0

        # Determine state
        exit_status_str = attrs.get("Exit_status", attrs.get("exit_status", ""))
        try:
            exit_status = int(exit_status_str)
            pbs_state = "COMPLETED" if exit_status == 0 else "FAILED"
        except (ValueError, TypeError):
            pbs_state = None

        return JobStats(
            cpu_efficiency=round(efficiency, 1),
            max_rss=rss_formatted,
            total_cpu=f"{cput_s:.0f}s",
            start_time=job_run_time,
            end_time=job_exit_time,
            state=pbs_state,
        )

    @staticmethod
    def _parse_tracejob_timestamp(line: str) -> datetime | None:
        """Parse timestamp from a tracejob line like '03/19/2026 15:38:18.133 S ...'"""
        parts = line.split()
        if len(parts) >= 2:
            dt_str = f"{parts[0]} {parts[1].split('.')[0]}"
            try:
                dt = datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S")
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        return None

    def _parse_qstat_xf(self, output: str) -> dict[str, JobStats]:
        """Parse qstat -xf output into JobStats.

        Supports both XML (Torque) and text key-value (PBS Pro) formats.
        """
        stripped = output.strip()
        if stripped.startswith("<?xml") or stripped.startswith("<Data"):
            return self._parse_qstat_xf_xml(stripped)
        return self._parse_qstat_xf_text(stripped)

    def _parse_qstat_xf_xml(self, output: str) -> dict[str, JobStats]:
        """Parse Torque XML qstat -xf output."""
        stats: dict[str, JobStats] = {}
        try:
            root = ET.fromstring(output)
        except ET.ParseError as e:
            logger.warning(f"Failed to parse qstat -xf XML: {e}")
            return stats

        for job_el in root.findall("Job"):
            job_id_el = job_el.find("Job_Id")
            if job_id_el is None or not job_id_el.text:
                continue
            job_id = _strip_server_suffix(job_id_el.text.strip())

            # Flatten XML into dot-notation attrs dict
            attrs: dict[str, str] = {}
            for child in job_el:
                if len(child) > 0:
                    # Nested element (e.g. <resources_used><cput>...</cput></resources_used>)
                    prefix = child.tag
                    for sub in child:
                        if sub.text:
                            attrs[f"{prefix}.{sub.tag}"] = sub.text.strip()
                elif child.text:
                    attrs[child.tag] = child.text.strip()

            s = self._build_stats_from_attrs(job_id, attrs)
            if s is not None:
                stats[job_id] = s

        return stats

    def _parse_qstat_xf_text(self, output: str) -> dict[str, JobStats]:
        """Parse PBS Pro text key-value qstat -xf output."""
        stats: dict[str, JobStats] = {}
        current_job_id: str | None = None
        attrs: dict[str, str] = {}

        def _flush() -> None:
            nonlocal current_job_id, attrs
            if current_job_id is not None:
                s = self._build_stats_from_attrs(current_job_id, attrs)
                if s is not None:
                    stats[current_job_id] = s
            current_job_id = None
            attrs = {}

        for line in output.split("\n"):
            if line.startswith("Job Id:"):
                _flush()
                raw_id = line.split(":", 1)[1].strip()
                current_job_id = _strip_server_suffix(raw_id)
                continue

            line = line.strip()
            if "=" in line and current_job_id is not None:
                key, _, value = line.partition("=")
                attrs[key.strip()] = value.strip()

        _flush()
        return stats

    def _build_stats_from_attrs(
        self, job_id: str, attrs: dict[str, str]
    ) -> JobStats | None:
        """Build a JobStats from parsed qstat -xf attributes."""
        walltime_str = attrs.get("resources_used.walltime", "")
        cput_str = attrs.get("resources_used.cput", "")
        mem_str = attrs.get("resources_used.mem", "")

        walltime_s = parse_duration_hms(walltime_str)
        cput_s = parse_duration_hms(cput_str)

        # Parse memory used (PBS uses lowercase units: 1024mb, 512kb)
        mem_bytes = 0
        if mem_str:
            mem_clean = re.sub(r"([kmgtp])b$", lambda m: m.group(1).upper(), mem_str, flags=re.IGNORECASE)
            mem_bytes = parse_rss_to_bytes(mem_clean)
        rss_formatted = format_bytes(mem_bytes)

        # CPU efficiency — PBS Pro uses Resource_List.ncpus,
        # Torque uses Resource_List.nodes (e.g. "1:ppn=4")
        ncpus = 1
        ncpus_str = attrs.get("Resource_List.ncpus", "")
        if ncpus_str:
            try:
                ncpus = int(ncpus_str)
            except ValueError:
                pass
        else:
            nodes_str = attrs.get("Resource_List.nodes", "")
            ppn_match = re.search(r"ppn=(\d+)", nodes_str)
            if ppn_match:
                ncpus = int(ppn_match.group(1))
        denominator = walltime_s * max(ncpus, 1)
        efficiency = (cput_s / denominator * 100) if denominator > 0 else 0.0

        # Determine terminal state
        pbs_state = self._determine_terminal_state(attrs)

        # Parse timestamps (PBS Pro uses "stime", Torque XML uses "start_time")
        start_time = parse_pbs_datetime(attrs.get("stime", "")) or parse_pbs_datetime(
            attrs.get("start_time", "")
        )
        end_time = parse_pbs_datetime(attrs.get("comp_time", "")) or parse_pbs_datetime(
            attrs.get("mtime", "")
        )

        return JobStats(
            cpu_efficiency=round(efficiency, 1),
            max_rss=rss_formatted,
            total_cpu=f"{cput_s:.0f}s",
            start_time=start_time,
            end_time=end_time,
            state=pbs_state,
        )

    @staticmethod
    def _determine_terminal_state(attrs: dict[str, str]) -> str | None:
        """Determine the terminal state from qstat -xf attributes."""
        exit_status_str = attrs.get("exit_status", "")
        job_state = attrs.get("job_state", "")

        # Check for walltime/memory exceeded first (from resources)
        resources_used_walltime = attrs.get("resources_used.walltime", "")
        resource_list_walltime = attrs.get("Resource_List.walltime", "")
        if resources_used_walltime and resource_list_walltime:
            used_s = parse_duration_hms(resources_used_walltime)
            limit_s = parse_duration_hms(resource_list_walltime)
            if limit_s > 0 and used_s >= limit_s:
                return "WALLTIME_EXCEEDED"

        # Check exit status
        try:
            exit_status = int(exit_status_str)
        except (ValueError, TypeError):
            exit_status = None

        if exit_status is not None:
            if exit_status == 0:
                return "COMPLETED"
            # PBS signals: 271 = qdel (SIGTERM+256), -11 = SIGSEGV
            if exit_status == 271 or exit_status == -29:
                return "CANCELLED"
            return "FAILED"

        # Fall back to job_state letter
        if job_state in ("C", "F", "X"):
            return "COMPLETED"

        return None

    async def get_cluster_info(self) -> tuple[int, int] | None:
        """Fetch total and free CPU counts from pbsnodes."""
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
        free_cpus = 0
        current_ncpus = 0
        current_state = ""

        for line in stdout.split("\n"):
            line = line.strip()
            if not line:
                # End of a node record — tally up
                if current_ncpus > 0:
                    total_cpus += current_ncpus
                    if "free" in current_state:
                        free_cpus += current_ncpus
                current_ncpus = 0
                current_state = ""
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

        # Flush last node
        if current_ncpus > 0:
            total_cpus += current_ncpus
            if "free" in current_state:
                free_cpus += current_ncpus

        if total_cpus == 0:
            return None
        return total_cpus, free_cpus

    async def get_disk_info(self, path: str) -> DiskInfo | None:
        """Fetch disk usage for ``path`` on the backend via ``df -Pk``."""
        return await fetch_disk_info(self._ssh, path)

    async def submit_job(self, script: str) -> str:
        """Submit a job script via qsub. Returns the PBS job ID."""
        escaped_script = script.replace("'", "'\\''")
        submit_cmd = f"qsub <<'SCRIPTHUT_EOF'\n{escaped_script}\nSCRIPTHUT_EOF"
        stdout, stderr, exit_code = await self._ssh.run_command(submit_cmd)
        if exit_code != 0:
            raise RuntimeError(f"qsub failed: {stderr}")
        # qsub output: "12345.pbs-server" or just "12345"
        raw_id = stdout.strip().split("\n")[-1].strip()
        return _strip_server_suffix(raw_id)

    async def cancel_job(self, job_id: str) -> None:
        """Cancel a PBS job via qdel."""
        await self._ssh.run_command(f"qdel {job_id}")

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
        """Generate a PBS submission script for a task."""
        output_path = task.get_output_path(run_id, log_dir)
        error_path = task.get_error_path(run_id, log_dir)
        shebang = "#!/bin/bash -l" if login_shell else "#!/bin/bash"

        queue = self._default_queue or task.partition
        pbs_mem = _convert_memory_to_pbs(task.memory)

        account_line = f"#PBS -A {account}\n" if account else ""

        gpu_suffix = ""
        if task.gres:
            gpus = _gres_to_pbs_gpus(task.gres)
            if gpus is not None:
                gpu_suffix = f":gpus={gpus}"

        header = f"""{shebang}
#PBS -N {task.name}
#PBS -q {queue}
{account_line}#PBS -l nodes=1:ppn={task.cpus}{gpu_suffix},mem={pbs_mem},walltime={task.time_limit}
#PBS -o {output_path}
#PBS -e {error_path}
#PBS -V
"""
        body = generate_script_body(
            task_name=task.name,
            task_id=task.id,
            command=task.command,
            working_dir=task.working_dir,
            env_vars=env_vars,
            extra_init=extra_init,
            interactive_wait=interactive_wait,
        )
        return header + "\n" + body

    async def is_available(self) -> bool:
        """Check if PBS is available by running qstat --version."""
        try:
            stdout, _, exit_code = await self._ssh.run_command("qstat --version 2>&1")
            if exit_code == 0:
                return True
            # Some PBS versions return non-zero for --version but still work
            stdout2, _, exit_code2 = await self._ssh.run_command("qstat 2>&1")
            return exit_code2 == 0 or "pbs" in stdout2.lower()
        except Exception as e:
            logger.warning(f"PBS availability check failed: {e}")
            return False

    @property
    def failure_states(self) -> dict[str, str]:
        """PBS failure states."""
        return PBS_FAILURE_STATES

    @property
    def terminal_states(self) -> frozenset[str]:
        """All terminal PBS states."""
        return PBS_TERMINAL_STATES
