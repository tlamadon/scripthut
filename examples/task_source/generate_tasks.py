#!/usr/bin/env python3
"""
Example task source script for ScriptHut.

This script generates a list of simple test tasks that can be submitted to Slurm.
Each task runs a small script that sleeps and writes output.

Usage:
    python generate_tasks.py [--count N] [--working-dir DIR]

Example scripthut.yaml config:
    workflows:
      - name: test-tasks
        backend: hpc-cluster
        command: "python /path/to/generate_tasks.py --count 5"
        max_concurrent: 2
        description: "Test tasks for ScriptHut"
"""

import argparse
import json
import os
from pathlib import Path


def generate_tasks(count: int, working_dir: str, partition: str, with_deps: bool = False) -> dict:
    """Generate a list of test tasks."""
    tasks = []

    for i in range(1, count + 1):
        task = {
            "id": f"test-{i:03d}",
            "name": f"test-task-{i}",
            # Run the simple_task.sh script with a task number
            "command": f"bash simple_task.sh {i}",
            "working_dir": working_dir,
            "partition": partition,
            "cpus": 1,
            "memory": "1G",
            "time_limit": "00:05:00",  # 5 minutes max
        }
        # With --with-deps, each task depends on the previous one (chain)
        if with_deps and i > 1:
            task["deps"] = [f"test-{i-1:03d}"]
        tasks.append(task)

    return {"tasks": tasks}


def main():
    parser = argparse.ArgumentParser(description="Generate test tasks for ScriptHut")
    parser.add_argument(
        "--count", "-n",
        type=int,
        default=3,
        help="Number of tasks to generate (default: 3)"
    )
    parser.add_argument(
        "--working-dir", "-d",
        type=str,
        default=os.getcwd(),
        help="Working directory for tasks (default: current directory)"
    )
    parser.add_argument(
        "--partition", "-p",
        type=str,
        default="normal",
        help="Slurm partition to use (default: normal)"
    )
    parser.add_argument(
        "--with-deps",
        action="store_true",
        default=False,
        help="Generate tasks with dependency chains (each task depends on the previous)"
    )

    args = parser.parse_args()

    tasks = generate_tasks(args.count, args.working_dir, args.partition, args.with_deps)

    # Output JSON to stdout
    print(json.dumps(tasks, indent=2))


if __name__ == "__main__":
    main()
