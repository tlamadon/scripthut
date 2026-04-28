"""Shared utilities for HPC scheduler backends."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scripthut.backends.base import DiskInfo
    from scripthut.ssh.client import SSHClient

logger = logging.getLogger(__name__)


def format_submit_output(stdout: str, stderr: str) -> str:
    """Combine submission-command stdout/stderr into a single diagnostic blob.

    Stored on ``RunItem.submit_output`` so users can see exactly what
    sbatch/qsub/etc said even when submission appeared to succeed.
    """
    parts: list[str] = []
    out = (stdout or "").strip()
    err = (stderr or "").strip()
    if out:
        parts.append(f"stdout: {out}")
    if err:
        parts.append(f"stderr: {err}")
    return " | ".join(parts)


def parse_duration_hms(time_str: str) -> float:
    """Parse duration string to total seconds.

    Handles formats: MM:SS, HH:MM:SS, D-HH:MM:SS
    (Common to both Slurm and PBS/Torque.)
    """
    if not time_str or time_str in ("", "N/A", "Unknown", "None", "INVALID"):
        return 0.0

    days = 0
    if "-" in time_str:
        day_part, time_str = time_str.split("-", 1)
        days = int(day_part)

    parts = time_str.split(":")
    if len(parts) == 3:
        hours, minutes, seconds = int(parts[0]), int(parts[1]), float(parts[2])
    elif len(parts) == 2:
        hours, minutes, seconds = 0, int(parts[0]), float(parts[1])
    else:
        return 0.0

    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def parse_rss_to_bytes(rss_str: str) -> int:
    """Parse RSS string (e.g. '4556K', '1024M') to bytes. Returns 0 on failure."""
    if not rss_str or rss_str.strip() in ("", "0", "N/A"):
        return 0
    match = re.match(r"^([\d.]+)([KMGTP]?)$", rss_str.strip(), re.IGNORECASE)
    if not match:
        return 0
    value = float(match.group(1))
    unit = match.group(2).upper() if match.group(2) else ""
    multipliers = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    return int(value * multipliers.get(unit, 1))


def format_bytes(byte_val: int) -> str:
    """Format byte count to human-readable form."""
    if byte_val <= 0:
        return ""
    if byte_val >= 1024**3:
        return f"{byte_val / 1024**3:.1f}G"
    elif byte_val >= 1024**2:
        return f"{byte_val / 1024**2:.0f}M"
    elif byte_val >= 1024:
        return f"{byte_val / 1024:.0f}K"
    return f"{byte_val:.0f}B"


def format_rss(rss_str: str) -> str:
    """Convert RSS string (e.g. '4556K', '1024M') to human-readable form."""
    return format_bytes(parse_rss_to_bytes(rss_str))


def parse_df_output(stdout: str) -> tuple[int, int] | None:
    """Parse ``df -Pk <path>`` output. Returns ``(total_bytes, avail_bytes)`` or None.

    POSIX ``df -P`` format is:
        Filesystem    1024-blocks      Used  Available  Capacity  Mounted on
        /dev/sda1       123456789  12345678  111111111       11%  /home

    The data line may wrap if the Filesystem field is long, so we read the
    last non-empty line and pick the 1024-block and Available columns.
    """
    lines = [line for line in stdout.splitlines() if line.strip()]
    if len(lines) < 2:
        return None
    parts = lines[-1].split()
    if len(parts) < 4:
        return None
    try:
        total_kb = int(parts[-5])
        avail_kb = int(parts[-3])
    except (ValueError, IndexError):
        return None
    return total_kb * 1024, avail_kb * 1024


async def fetch_log_via_ssh(
    ssh: SSHClient,
    log_path: str,
    tail_lines: int | None = None,
) -> tuple[str | None, str | None]:
    """Read a log file over SSH with ``tail`` or ``cat``. Returns ``(content, error)``."""
    if tail_lines:
        cmd = f"tail -n {tail_lines} {log_path} 2>/dev/null || echo '[File not found or empty]'"
    else:
        cmd = f"cat {log_path} 2>/dev/null || echo '[File not found or empty]'"
    try:
        stdout, _, _ = await ssh.run_command(cmd)
    except Exception as e:
        return None, f"SSH error fetching log: {e}"
    return stdout, None


async def fetch_disk_info(ssh: SSHClient, path: str) -> DiskInfo | None:
    """Run ``df -Pk <path>`` over SSH and return a :class:`DiskInfo`.

    Returns None on SSH error, non-zero exit, or unparseable output.
    """
    from scripthut.backends.base import DiskInfo

    cmd = f"df -Pk {path}"
    try:
        stdout, stderr, exit_code = await ssh.run_command(cmd, timeout=15)
    except Exception as e:
        logger.warning(f"df failed on '{path}': {e}")
        return None
    if exit_code != 0:
        logger.warning(f"df failed on '{path}' (exit {exit_code}): {stderr.strip()}")
        return None
    parsed = parse_df_output(stdout)
    if parsed is None:
        logger.warning(f"Could not parse df output for '{path}': {stdout!r}")
        return None
    total, avail = parsed
    return DiskInfo(total_bytes=total, avail_bytes=avail, path=path)


def generate_script_body(
    task_name: str,
    task_id: str,
    command: str,
    working_dir: str,
    env_vars: dict[str, str] | None = None,
    extra_init: str = "",
    interactive_wait: bool = False,
) -> str:
    """Generate the common body of an HPC submission script.

    This produces the shared portion after scheduler-specific directives:
    echo header, environment variable exports, extra init, cd, command, exit code.

    If interactive_wait is True, a tmux session is started after environment
    setup and the script blocks until a continue signal is sent.  This lets
    users attach to the job interactively before the heavy command runs.
    """
    env_lines = ""
    if env_vars:
        export_lines = [f'export {key}="{value}"' for key, value in env_vars.items()]
        if export_lines:
            env_lines = "\n".join(export_lines) + "\n\n"

    extra_init_lines = ""
    if extra_init:
        extra_init_lines = extra_init + "\n\n"

    tmux_block = ""
    if interactive_wait:
        tmux_block = """
# --- ScriptHut interactive wait ---
SCRIPTHUT_TMUX_SESSION="sh-${SLURM_JOB_ID:-${PBS_JOBID:-$$}}"
if command -v tmux >/dev/null 2>&1; then
    # Write a setup script that restores the full job environment inside tmux
    _scripthut_setup=$(mktemp /tmp/scripthut-setup.XXXXXX.sh)
    export -p > "$_scripthut_setup"
    echo "cd $(pwd)" >> "$_scripthut_setup"
    echo "rm -f \\"$_scripthut_setup\\"" >> "$_scripthut_setup"
    echo "exec bash" >> "$_scripthut_setup"
    tmux new-session -d -s "$SCRIPTHUT_TMUX_SESSION" "bash $_scripthut_setup"
    echo "Interactive tmux session '$SCRIPTHUT_TMUX_SESSION' ready on $(hostname)"
    echo "Waiting for continue signal..."
    tmux wait-for "continue-${SCRIPTHUT_TMUX_SESSION}"
    echo "Continue signal received, proceeding with main command..."
    tmux kill-session -t "$SCRIPTHUT_TMUX_SESSION" 2>/dev/null || true
else
    echo "WARNING: tmux not found, skipping interactive wait"
fi
# --- End interactive wait ---

"""

    return f"""echo "=== ScriptHut Task: {task_name} ==="
echo "Task ID: {task_id}"
echo "Started: $(date)"
echo "Host: $(hostname)"
echo "Working dir: {working_dir}"
echo "=================================="
echo ""

{env_lines}{extra_init_lines}cd {working_dir}
{tmux_block}{command}
EXIT_CODE=$?

echo ""
echo "=================================="
echo "Finished: $(date)"
echo "Exit code: $EXIT_CODE"
exit $EXIT_CODE
"""
