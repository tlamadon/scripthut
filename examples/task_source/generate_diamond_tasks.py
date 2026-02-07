#!/usr/bin/env python3
"""
Example task source with a diamond dependency pattern for ScriptHut.

Generates 4 tasks with the following dependency graph:

        A
       / \\
      B   C
       \\ /
        D

- Task A runs first (no deps)
- Tasks B and C both depend on A (can run in parallel once A completes)
- Task D depends on both B and C (runs only after both finish)

Usage:
    python generate_diamond_tasks.py [--working-dir DIR] [--partition PARTITION]

Example scripthut.yaml config:
    task_sources:
      - name: diamond-tasks
        cluster: hpc-cluster
        command: "python /path/to/generate_diamond_tasks.py"
        max_concurrent: 4
        description: "Diamond dependency pattern (A -> B,C -> D)"
"""

import argparse
import json
import os


def generate_diamond_tasks(working_dir: str, partition: str) -> dict:
    """Generate 4 tasks with a diamond dependency pattern."""
    return {
        "tasks": [
            {
                "id": "task-a",
                "name": "Setup",
                "command": "bash simple_task.sh 1",
                "working_dir": working_dir,
                "partition": partition,
                "cpus": 1,
                "memory": "1G",
                "time_limit": "00:10:00",
            },
            {
                "id": "task-b",
                "name": "Build-X",
                "command": "bash simple_task.sh 2",
                "working_dir": working_dir,
                "partition": partition,
                "cpus": 2,
                "memory": "2G",
                "time_limit": "00:10:00",
                "deps": ["task-a"],
            },
            {
                "id": "task-c",
                "name": "Build-Y",
                "command": "bash simple_task.sh 3",
                "working_dir": working_dir,
                "partition": partition,
                "cpus": 2,
                "memory": "2G",
                "time_limit": "00:10:00",
                "deps": ["task-a"],
            },
            {
                "id": "task-d",
                "name": "Finalize",
                "command": "bash simple_task.sh 4",
                "working_dir": working_dir,
                "partition": partition,
                "cpus": 1,
                "memory": "1G",
                "time_limit": "00:10:00",
                "deps": ["task-b", "task-c"],
            },
        ]
    }


def main():
    parser = argparse.ArgumentParser(
        description="Generate diamond dependency tasks for ScriptHut"
    )
    parser.add_argument(
        "--working-dir", "-d",
        type=str,
        default=os.getcwd(),
        help="Working directory for tasks (default: current directory)",
    )
    parser.add_argument(
        "--partition", "-p",
        type=str,
        default="normal",
        help="Slurm partition to use (default: normal)",
    )

    args = parser.parse_args()

    tasks = generate_diamond_tasks(args.working_dir, args.partition)
    print(json.dumps(tasks, indent=2))


if __name__ == "__main__":
    main()
