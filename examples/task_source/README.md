# ScriptHut Task Source Example

This folder contains a simple example to test ScriptHut's task submission feature.

## Files

- `generate_tasks.py` - Task source script that generates JSON task list
- `simple_task.sh` - Simple task script that simulates work

## Setup

1. Copy this folder to your Slurm cluster:

```bash
scp -r examples/task_source user@cluster:/home/user/scripthut-test
```

2. Make the scripts executable:

```bash
ssh user@cluster "chmod +x /home/user/scripthut-test/*.sh"
```

3. Add a task source to your `scripthut.yaml`:

```yaml
task_sources:
  - name: test-tasks
    cluster: hpc-cluster  # Your cluster name from clusters config
    command: "python /home/user/scripthut-test/generate_tasks.py --count 3 --working-dir /home/user/scripthut-test --partition normal"
    max_concurrent: 2
    description: "Test tasks for ScriptHut"
```

Adjust the paths and partition name for your cluster.

## Usage

1. Start ScriptHut:

```bash
scripthut
```

2. Open http://localhost:8000/queues in your browser

3. Click "Run" next to "test-tasks"

4. Watch the tasks get submitted and track their progress

5. Click on individual tasks to view:
   - The generated sbatch script
   - stdout output log
   - stderr error log

## Customizing

### Generate more tasks

```bash
python generate_tasks.py --count 10 --partition gpu
```

### Test locally (without Slurm)

```bash
# Generate task list
python generate_tasks.py

# Run task script directly
bash simple_task.sh 1
```

## Expected Output

Each task will produce output like:

```
==========================================
ScriptHut Test Task #1
==========================================

Started at: Wed Feb  4 10:30:00 UTC 2026
Hostname: node001
Working directory: /home/user/scripthut-test
User: user

Slurm Job ID: 12345
Slurm Job Name: test-task-1
Slurm Partition: normal

Simulating work for 25 seconds...

[10:30:05] Progress: 20%
[10:30:10] Progress: 40%
[10:30:15] Progress: 60%
[10:30:20] Progress: 80%
[10:30:25] Progress: 100%

Task #1 completed successfully!
Finished at: Wed Feb  4 10:30:25 UTC 2026
```
