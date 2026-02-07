#!/usr/bin/env python3
"""
Example task source with a diamond dependency pattern for ScriptHut.

Uses dot-notation task IDs and wildcard dependencies.

Generates 4 tasks with the following dependency graph:

        setup.init
         /     \\
  build.x     build.y
         \\     /
       final.merge

- setup.init runs first (no deps)
- build.x and build.y both depend on setup.init (run in parallel)
- final.merge depends on "build.*" (wildcard matching both build tasks)

Usage:
    python generate_diamond_tasks.py [--working-dir DIR] [--partition PARTITION]

Example scripthut.yaml config:
    task_sources:
      - name: diamond-tasks
        cluster: hpc-cluster
        command: "python /path/to/generate_diamond_tasks.py"
        max_concurrent: 4
        description: "Diamond dependency pattern with wildcard deps"
"""

import argparse
import json
import os


def generate_diamond_tasks(working_dir: str, partition: str) -> dict:
    """Generate 4 tasks with a diamond dependency pattern using wildcards."""
    return {
        "tasks": [
            {
                "id": "setup.init",
                "name": "Setup",
                "command": "bash simple_task.sh 1",
                "working_dir": working_dir,
                "partition": partition,
                "cpus": 1,
                "memory": "1G",
                "time_limit": "00:10:00",
            },
            {
                "id": "build.x",
                "name": "Build-X",
                "command": "bash simple_task.sh 2",
                "working_dir": working_dir,
                "partition": partition,
                "cpus": 2,
                "memory": "2G",
                "time_limit": "00:10:00",
                "deps": ["setup.init"],
            },
            {
                "id": "build.y",
                "name": "Build-Y",
                "command": "bash simple_task.sh 3",
                "working_dir": working_dir,
                "partition": partition,
                "cpus": 2,
                "memory": "2G",
                "time_limit": "00:10:00",
                "deps": ["setup.init"],
            },
            {
                "id": "final.merge",
                "name": "Finalize",
                "command": "bash simple_task.sh 4",
                "working_dir": working_dir,
                "partition": partition,
                "cpus": 1,
                "memory": "1G",
                "time_limit": "00:10:00",
                "deps": ["build.*"],
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
