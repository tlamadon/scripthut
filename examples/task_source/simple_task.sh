#!/bin/bash
#
# Simple test task script for ScriptRun.
# This script simulates a job by sleeping and writing output.
#
# Usage: bash simple_task.sh <task_number>
#

TASK_NUM=${1:-1}
SLEEP_TIME=$((10 + RANDOM % 20))  # Random sleep between 10-30 seconds

echo "=========================================="
echo "ScriptRun Test Task #${TASK_NUM}"
echo "=========================================="
echo ""
echo "Started at: $(date)"
echo "Hostname: $(hostname)"
echo "Working directory: $(pwd)"
echo "User: $(whoami)"
echo ""
echo "Slurm Job ID: ${SLURM_JOB_ID:-N/A}"
echo "Slurm Job Name: ${SLURM_JOB_NAME:-N/A}"
echo "Slurm Partition: ${SLURM_JOB_PARTITION:-N/A}"
echo ""
echo "Simulating work for ${SLEEP_TIME} seconds..."
echo ""

# Simulate work with progress updates
for i in $(seq 1 5); do
    sleep $((SLEEP_TIME / 5))
    echo "[$(date +%H:%M:%S)] Progress: $((i * 20))%"
done

echo ""
echo "Task #${TASK_NUM} completed successfully!"
echo "Finished at: $(date)"
