"""Slurm job backend implementation."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from scripthut.backends.base import (
    ClusterInfo,
    JobBackend,
    PartitionInfo,
    QuotaInfo,
)
from scripthut.models import JobState, HPCJob
from scripthut.ssh.client import SSHClient


logger = logging.getLogger(__name__)


def _safe_float(s: str) -> float | None:
    """Parse a Slurm numeric field; return None for empty / N/A / unparseable."""
    s = s.strip()
    if not s or s in ("N/A", "(null)"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _safe_int(s: str) -> int | None:
    s = s.strip()
    if not s or s in ("N/A", "(null)"):
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _min_int(a: int | None, b: int | None) -> int | None:
    """Lower bound of two optional ints; ``None`` means "no limit"."""
    if a is None:
        return b
    if b is None:
        return a
    return min(a, b)


def _tres_get(tres: str, key: str) -> int | None:
    """Look up a numeric value in a TRES string like ``cpu=128,gres/gpu=4``.

    Slurm uses ``cpu=…`` for CPUs and ``gres/gpu=…`` for GPUs. Returns
    ``None`` if the key is absent or the value isn't an integer (e.g.
    memory limits use ``M``/``G`` suffixes — we deliberately skip those).
    """
    if not tres or tres in ("N/A", "(null)"):
        return None
    for entry in tres.split(","):
        k, _, v = entry.strip().partition("=")
        if k.strip() == key:
            try:
                return int(v.strip())
            except ValueError:
                return None
    return None


def _parse_cpus_state(field: str) -> int:
    """Return the idle CPU count from a sinfo ``CPUsState`` field.

    The field is formatted ``allocated/idle/other/total`` (e.g.
    ``4/12/0/16``). Returns the idle half; 0 for empty / malformed input.
    """
    parts = (field or "").strip().split("/")
    if len(parts) != 4:
        return 0
    try:
        return int(parts[1])
    except ValueError:
        return 0


def _parse_free_mem(field: str) -> int | None:
    """Return free memory in MB from a sinfo ``FreeMem`` field.

    ``FreeMem`` is an integer count of megabytes, or ``N/A`` when the
    node doesn't report it. Returns ``None`` for missing / unparseable
    values so the caller doesn't invent a number.
    """
    field = (field or "").strip()
    if not field or field in ("N/A", "(null)", "-"):
        return None
    try:
        return int(field)
    except ValueError:
        return None


@dataclass
class _PartitionNodeStats:
    """Per-partition aggregates derived from the node-level sinfo walk."""

    gpus_total: int = 0
    gpus_idle: int = 0
    gpu_types: str | None = None
    cpus_free_max_node: int = 0
    mem_free_max_node_mb: int | None = None
    gpus_schedulable: int = 0


def _parse_gres_gpu(gres_str: str) -> tuple[int, set[str]]:
    """Sum GPU counts from a Slurm Gres or GresUsed string.

    Accepts forms like::

        gpu:a100:8                  -> (8, {"a100"})
        gpu:8                       -> (8, set())
        gpu:a100:8(IDX:0-7),nvme:1  -> (8, {"a100"})
        gpu:a100:4,gpu:v100:2       -> (6, {"a100", "v100"})
        (null)                      -> (0, set())

    Non-GPU entries (``nvme``, ``mps``, …) are ignored. The trailing
    ``(IDX:…)`` allocation-detail suffix used by ``GresUsed`` is stripped.
    """
    if not gres_str or gres_str in ("(null)", "N/A"):
        return 0, set()
    total = 0
    types: set[str] = set()
    for raw in gres_str.split(","):
        entry = raw.split("(", 1)[0].strip()
        if not entry:
            continue
        parts = entry.split(":")
        if not parts or parts[0].lower() != "gpu":
            continue
        # Walk from the right: first integer-looking part is the count;
        # whatever precedes it (between "gpu" and the count) is the type.
        count: int | None = None
        for i in range(len(parts) - 1, 0, -1):
            try:
                count = int(parts[i])
                if i > 1:
                    types.add(parts[i - 1])
                break
            except ValueError:
                continue
        if count is not None:
            total += count
    return total, types


# squeue format string for extended job info
# Fields: JobID, Name, User, State, Partition, TimeUsed, NodeList, NumCPUs,
#         MinMemory, SubmitTime, StartTime, Reason
# %R is the pending reason (in parentheses) for waiting jobs; for running
# jobs it's the nodelist, so we only keep it for PENDING jobs below.
SQUEUE_FORMAT = "%i|%j|%u|%T|%P|%M|%N|%C|%m|%V|%S|%R"



def parse_slurm_datetime(dt_str: str) -> datetime | None:
    """Parse Slurm datetime string to Python datetime."""
    if not dt_str or dt_str in ("N/A", "Unknown", "None"):
        return None
    try:
        # Slurm uses ISO format: YYYY-MM-DDTHH:MM:SS
        dt = datetime.fromisoformat(dt_str.replace("T", " ").split(".")[0])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        logger.warning(f"Failed to parse datetime: {dt_str}")
        return None


def parse_squeue_line(line: str) -> HPCJob | None:
    """Parse a single line of squeue output into a HPCJob."""
    parts = line.strip().split("|")
    if len(parts) < 12:
        logger.warning(f"Invalid squeue line (expected 12 fields): {line}")
        return None

    try:
        state = JobState.from_string(parts[3])
        # %R is only a "why am I waiting" reason for PENDING jobs; for
        # running/terminal jobs it's the nodelist (already in %N), so we
        # drop it to avoid showing "nid00123" as a pending reason. Slurm
        # wraps the reason in parentheses — strip them for display.
        reason: str | None = None
        if state == JobState.PENDING:
            raw_reason = parts[11].strip().strip("()")
            reason = raw_reason or None
        return HPCJob(
            job_id=parts[0],
            name=parts[1],
            user=parts[2],
            state=state,
            partition=parts[4],
            time_used=parts[5] if parts[5] else "0:00",
            nodes=parts[6] if parts[6] else "-",
            cpus=int(parts[7]) if parts[7] else 0,
            memory=parts[8] if parts[8] else "-",
            submit_time=parse_slurm_datetime(parts[9]),
            start_time=parse_slurm_datetime(parts[10]),
            reason=reason,
        )
    except (ValueError, IndexError) as e:
        logger.warning(f"Failed to parse squeue line: {line}, error: {e}")
        return None


class SlurmBackend(JobBackend):
    """Slurm job backend using SSH to run squeue."""


    def __init__(self, ssh_client: SSHClient, account: str | None = None):
        self._ssh = ssh_client
        self._account = account


    @property
    def name(self) -> str:
        return "slurm"

    async def get_jobs(self, user: str | None = None) -> list[HPCJob]:
        """
        Fetch current jobs from Slurm using squeue.

        Args:
            user: Optional filter by username. None means all users.

        Returns:
            List of HPCJob objects.
        """
        # Build squeue command. TZ=UTC forces Slurm to format SubmitTime/StartTime
        # in UTC so the naive timestamps we parse line up with the tzinfo=utc we
        # stamp on them (otherwise they'd be cluster-local read as UTC).
        cmd = f"TZ=UTC squeue --noheader --format='{SQUEUE_FORMAT}'"
        if user:
            cmd += f" --user={user}"

        stdout, stderr, exit_code = await self._ssh.run_command(cmd)

        if exit_code != 0:
            logger.error(f"squeue failed (exit {exit_code}): {stderr}")
            return []

        jobs: list[HPCJob] = []
        for line in stdout.strip().split("\n"):
            if not line.strip():
                continue
            job = parse_squeue_line(line)
            if job:
                jobs.append(job)

        logger.debug(f"Fetched {len(jobs)} jobs from Slurm")
        return jobs


    async def get_cluster_info(self, user: str | None = None) -> ClusterInfo | None:
        """Fetch per-partition availability, pending reasons, and quota.

        Runs ``sinfo`` (partition-level), ``sinfo --Node`` (per-node CPU
        state, free memory, and Gres for GPU counts), ``squeue`` (pending
        reasons), plus ``sshare`` + ``sacctmgr`` for fair-share and
        scheduling limits. Each query is best-effort: a failure of one
        does not block the others.

        Quota queries use the SSH login and configured account, independent
        of the dashboard's job filter.
        """
        partitions = await self._fetch_partitions()
        if partitions is None:
            return None
        node_by_part = await self._fetch_node_info()
        for p in partitions:
            ns = node_by_part.get(p.name)
            if ns is not None:
                p.gpus_total = ns.gpus_total
                p.gpus_idle = ns.gpus_idle
                p.gpu_types = ns.gpu_types
                p.cpus_free_max_node = ns.cpus_free_max_node
                p.mem_free_max_node_mb = ns.mem_free_max_node_mb
                p.gpus_schedulable = ns.gpus_schedulable
        pending_reasons = await self._fetch_pending_reasons()
        quota_user = self._ssh.user
        user_quota = await self._fetch_user_quota(quota_user) if quota_user else None
        return ClusterInfo(
            partitions=partitions,
            pending_reasons=pending_reasons,
            user_quota=user_quota,
        )

    async def _fetch_partitions(self) -> list[PartitionInfo] | None:
        # Aggregate one row per partition with %P (default marked with *).
        # Fields: PartitionName | Avail | A/I/O/T cpus | total nodes | mem/node | timelimit | features
        fmt = "%P|%a|%C|%D|%m|%l|%f"
        cmd = f"sinfo --noheader --summarize --format='{fmt}'"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"sinfo failed: {e}")
            return None
        if exit_code != 0:
            logger.warning(f"sinfo failed (exit {exit_code}): {stderr}")
            return None

        partitions: list[PartitionInfo] = []
        for raw in stdout.strip().splitlines():
            line = raw.strip().strip("'")
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 7:
                logger.warning(f"Unexpected sinfo row: {line!r}")
                continue
            name_raw, avail, cpus_aiot, nodes, mem, timelimit, features = parts[:7]

            is_default = name_raw.endswith("*")
            name = name_raw.rstrip("*")

            cpu_parts = cpus_aiot.split("/")
            if len(cpu_parts) != 4:
                logger.warning(f"Unexpected sinfo CPUs field: {cpus_aiot!r}")
                continue
            try:
                alloc, idle, other, total = (int(x) for x in cpu_parts)
            except ValueError:
                logger.warning(f"Failed to parse CPUs: {cpus_aiot!r}")
                continue

            try:
                nodes_total = int(nodes)
            except ValueError:
                nodes_total = 0

            try:
                mem_mb: int | None = int(mem) if mem and mem != "N/A" else None
            except ValueError:
                mem_mb = None

            partitions.append(PartitionInfo(
                name=name,
                state=avail.lower(),
                cpus_allocated=alloc,
                cpus_idle=idle,
                cpus_other=other,
                cpus_total=total,
                nodes_total=nodes_total,
                timelimit=timelimit or None,
                mem_per_node_mb=mem_mb,
                features=features or None,
                is_default=is_default,
            ))

        return partitions

    async def _fetch_node_info(self) -> dict[str, _PartitionNodeStats]:
        """Return per-partition node-level availability for scheduling hints.

        Walks ``sinfo --Node`` (one row per node, per partition) and, for
        each partition, aggregates:

        * ``gpus_total`` / ``gpus_idle`` / ``gpu_types`` — from each node's
          Gres / GresUsed (nodes in down/drain/reserved states count toward
          total only).
        * ``cpus_free_max_node`` / ``mem_free_max_node_mb`` — the single
          biggest free slot on any one schedulable node, so the UI can show
          the largest job that would start immediately.
        * ``gpus_schedulable`` — idle GPUs sitting on a node that *also* has
          a free CPU. This is the number you can actually claim now;
          ``gpus_idle`` minus it is idle-but-stuck (the usual reason a
          GPU job queues despite "idle GPUs").

        Uses explicit field widths so we can split by column position
        (sinfo doesn't accept a custom delimiter in long-Format mode).
        """
        fmt = "Partition:32,StateLong:16,CPUsState:20,FreeMem:14,Gres:256,GresUsed:256"
        cmd = f"sinfo --noheader --Node --Format='{fmt}'"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"sinfo -N (nodes) failed: {e}")
            return {}
        if exit_code != 0:
            logger.warning(f"sinfo -N (nodes) failed (exit {exit_code}): {stderr}")
            return {}

        # Slice indices match the widths declared above.
        P_END, S_END, C_END, F_END, G_END, U_END = 32, 48, 68, 82, 338, 594

        # State strings considered "schedulable now". Anything else (down,
        # drain, reserved, …) counts toward totals only — its free CPUs/GPUs
        # can't actually be claimed.
        schedulable = {"idle", "mix", "mixed", "allocated", "alloc"}

        stats: dict[str, _PartitionNodeStats] = {}
        types_by_part: dict[str, set[str]] = {}

        for raw in stdout.splitlines():
            line = raw.ljust(U_END)
            partition = line[0:P_END].strip()
            state = line[P_END:S_END].strip().lower()
            cpus_state = line[S_END:C_END].strip()
            free_mem = line[C_END:F_END].strip()
            gres = line[F_END:G_END].strip()
            gres_used = line[G_END:U_END].strip()
            if not partition:
                continue

            is_schedulable = state in schedulable
            gpu_total, types = _parse_gres_gpu(gres)
            gpu_used, _ = _parse_gres_gpu(gres_used)
            free_cpus = _parse_cpus_state(cpus_state) if is_schedulable else 0
            free_gpus = max(0, gpu_total - gpu_used) if is_schedulable else 0
            free_mem_mb = _parse_free_mem(free_mem) if is_schedulable else None

            st = stats.setdefault(partition, _PartitionNodeStats())
            st.gpus_total += gpu_total
            st.gpus_idle += free_gpus
            # Largest single-node slot bounds the immediately schedulable job size.
            if free_cpus > st.cpus_free_max_node:
                st.cpus_free_max_node = free_cpus
            if free_mem_mb is not None and (
                st.mem_free_max_node_mb is None
                or free_mem_mb > st.mem_free_max_node_mb
            ):
                st.mem_free_max_node_mb = free_mem_mb
            # GPUs are only grabbable if the node has a free CPU to pair them
            # with — this is what makes "idle GPUs" actually schedulable.
            if free_cpus > 0:
                st.gpus_schedulable += free_gpus
            if types:
                types_by_part.setdefault(partition, set()).update(types)

        for name, st in stats.items():
            type_set = types_by_part.get(name, set())
            st.gpu_types = ",".join(sorted(t for t in type_set if t)) or None
        return stats

    async def _fetch_user_quota(self, user: str) -> QuotaInfo | None:
        """Combine ``sshare`` (fair-share), ``sacctmgr`` (limits), and
        ``squeue`` AllocTRES (current usage) for a user.

        Each query is independent — if one fails we still surface what
        the others returned. Returns ``None`` only if all three fail.
        """
        share = await self._fetch_sshare(user)
        limits = await self._fetch_sacctmgr_limits(user)
        usage = await self._fetch_running_tres(user)
        if share is None and limits is None and usage is None:
            return None
        q = share or QuotaInfo()
        if limits is not None:
            if limits.account and not q.account:
                q.account = limits.account
            if limits.jobs_max is not None:
                q.jobs_max = limits.jobs_max
            if limits.cpus_max is not None:
                q.cpus_max = limits.cpus_max
            if limits.gpus_max is not None:
                q.gpus_max = limits.gpus_max
        if usage is not None:
            q.jobs_used = usage.jobs_used
            q.cpus_used = usage.cpus_used
            q.gpus_used = usage.gpus_used
        return q

    async def _fetch_running_tres(self, user: str) -> QuotaInfo | None:
        """Tally CPU/GPU/job counts across the user's running jobs via squeue.

        Uses ``-O AllocTRES`` which returns lines like
        ``cpu=4,mem=4G,node=1,billing=4,gres/gpu=2`` per running job. When
        an account is configured, restrict to jobs charging that account
        so the totals line up with the configured account's limits.
        """
        cmd = f"squeue -h -t R -u {user} -O 'AllocTRES:256'"
        if self._account:
            cmd += f" --account={self._account}"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"squeue AllocTRES failed: {e}")
            return None
        if exit_code != 0:
            logger.warning(f"squeue AllocTRES failed (exit {exit_code}): {stderr}")
            return None

        jobs = 0
        cpus = 0
        gpus = 0
        for raw in stdout.splitlines():
            tres = raw.strip()
            if not tres:
                continue
            jobs += 1
            cpus += _tres_get(tres, "cpu") or 0
            gpus += _tres_get(tres, "gres/gpu") or 0
        return QuotaInfo(jobs_used=jobs, cpus_used=cpus, gpus_used=gpus)

    async def _fetch_sshare(self, user: str) -> QuotaInfo | None:
        """Run ``sshare`` for the given user; return fair-share + account.

        If the backend was configured with a specific ``account``, filter
        to that association (``-A account``) so a user with multiple
        accounts gets the row that matches what their jobs actually charge
        — not whichever ``DefaultAccount`` the site happens to set first.
        """
        cmd = (
            f"sshare -h --parsable2 -U -u {user} "
            f"--format=Account,User,FairShare,EffectvUsage"
        )
        if self._account:
            cmd += f" -A {self._account}"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"sshare failed: {e}")
            return None
        if exit_code != 0:
            logger.warning(f"sshare failed (exit {exit_code}): {stderr}")
            return None

        # Prefer the row whose Account matches self._account; fall back to
        # the first parseable row (handles older Slurm builds that ignore
        # -A or aliases like "pi-example" vs "pi_example").
        first: QuotaInfo | None = None
        for raw in stdout.splitlines():
            line = raw.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 4:
                continue
            account, _u, fair_share, eff_usage = parts[0], parts[1], parts[2], parts[3]
            entry = QuotaInfo(
                account=account or None,
                fair_share=_safe_float(fair_share),
                norm_usage=_safe_float(eff_usage),
            )
            if self._account and account == self._account:
                return entry
            if first is None:
                first = entry
        return first

    async def _fetch_sacctmgr_limits(self, user: str) -> QuotaInfo | None:
        """Run ``sacctmgr show user withassoc`` and parse limits.

        If a specific account is configured, restrict rows to it (both at
        the query level via ``account=…`` and as a client-side filter for
        older sacctmgr builds that ignore the query filter). Otherwise
        all rows are walked.
        """
        cmd = (
            f"sacctmgr -n -P show user {user} withassoc "
            f"format=User,Account,Partition,GrpJobs,GrpTRES,MaxJobs,MaxTRES"
        )
        if self._account:
            cmd += f" account={self._account}"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"sacctmgr failed: {e}")
            return None
        if exit_code != 0:
            logger.warning(f"sacctmgr failed (exit {exit_code}): {stderr}")
            return None

        # Walk rows; if an account is configured, only aggregate rows that
        # match it. Take the most-restrictive Grp/Max for each resource.
        best = QuotaInfo()
        for raw in stdout.splitlines():
            line = raw.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 7:
                continue
            _user, account, _part, grp_jobs, grp_tres, max_jobs, max_tres = parts[:7]
            if self._account and account != self._account:
                continue
            if best.account is None and account:
                best.account = account

            jobs_cap = _min_int(_safe_int(grp_jobs), _safe_int(max_jobs))
            cpus_cap = _min_int(_tres_get(grp_tres, "cpu"), _tres_get(max_tres, "cpu"))
            gpus_cap = _min_int(
                _tres_get(grp_tres, "gres/gpu"), _tres_get(max_tres, "gres/gpu")
            )

            best.jobs_max = _min_int(best.jobs_max, jobs_cap)
            best.cpus_max = _min_int(best.cpus_max, cpus_cap)
            best.gpus_max = _min_int(best.gpus_max, gpus_cap)

        if (
            best.account is None
            and best.jobs_max is None
            and best.cpus_max is None
            and best.gpus_max is None
        ):
            return None
        # Use the configured account when accounting omits it.
        if self._account and best.account is None:
            best.account = self._account
        return best

    async def _fetch_pending_reasons(self) -> dict[str, int]:
        """Tally ``squeue -t PD`` pending jobs by reason."""
        cmd = "squeue --noheader -t PD --format='%R'"
        try:
            stdout, stderr, exit_code = await self._ssh.run_command(cmd, timeout=15)
        except Exception as e:
            logger.warning(f"squeue PD failed: {e}")
            return {}
        if exit_code != 0:
            logger.warning(f"squeue PD failed (exit {exit_code}): {stderr}")
            return {}

        reasons: dict[str, int] = {}
        for raw in stdout.splitlines():
            reason = raw.strip().strip("'").strip("()")
            if not reason:
                continue
            reasons[reason] = reasons.get(reason, 0) + 1
        return reasons
