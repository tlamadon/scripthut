"""Slurm accounting states for controller observation."""

import shlex


SLURM_TERMINAL_STATES = frozenset({
    "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT",
    "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED",
    "DEADLINE", "BOOT_FAIL",
})


def controller_commands(
    *, name: str | None, job_id: str | None = None, start_date: str | None = None,
) -> tuple[str, str]:
    """Build accounting and queue queries for a launch name or acknowledged ID."""
    if not name and not job_id:
        raise ValueError("A controller name or job ID is required")
    accounting = ["sacct", "-n", "-P", "-X"]
    if job_id:
        accounting += ["--jobs", job_id, "--format=State"]
    if start_date:
        accounting += ["--starttime", start_date]
    if not job_id:
        assert name is not None
        accounting += ["--name", name, "--format=JobIDRaw,JobName%100,State"]
    # A name query succeeds with an empty queue after a job ends.
    selector = ["--name", name] if name else ["--jobs", str(job_id)]
    queue = ["squeue", "--noheader", *selector,
             "--format=%T" if job_id else "--format=%i|%j|%T"]
    return shlex.join(accounting), shlex.join(queue)


def controller_state(states: list[str]) -> str:
    """Classify consistent scheduler evidence; missing or mixed evidence is unknown."""
    if states and all(state in SLURM_TERMINAL_STATES for state in states):
        return "ended"
    if states and all(state in ("RUNNING", "COMPLETING", "SUSPENDED") for state in states):
        return "running"
    if states and all(state in ("PENDING", "CONFIGURING") for state in states):
        return "queued"
    return "unknown"
