"""Shared utilities for HPC scheduler backends."""

import re


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


def generate_script_body(
    task_name: str,
    task_id: str,
    command: str,
    working_dir: str,
    env_vars: dict[str, str] | None = None,
    extra_init: str = "",
) -> str:
    """Generate the common body of an HPC submission script.

    This produces the shared portion after scheduler-specific directives:
    echo header, environment variable exports, extra init, cd, command, exit code.
    """
    env_lines = ""
    if env_vars:
        export_lines = [f'export {key}="{value}"' for key, value in env_vars.items()]
        if export_lines:
            env_lines = "\n".join(export_lines) + "\n\n"

    extra_init_lines = ""
    if extra_init:
        extra_init_lines = extra_init + "\n\n"

    return f"""echo "=== ScriptHut Task: {task_name} ==="
echo "Task ID: {task_id}"
echo "Started: $(date)"
echo "Host: $(hostname)"
echo "Working dir: {working_dir}"
echo "=================================="
echo ""

{env_lines}{extra_init_lines}cd {working_dir}
{command}
EXIT_CODE=$?

echo ""
echo "=================================="
echo "Finished: $(date)"
echo "Exit code: $EXIT_CODE"
exit $EXIT_CODE
"""
