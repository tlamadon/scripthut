# Writing a Task Generator

A task generator is any executable (script, binary, etc.) that prints valid JSON to stdout. The JSON must match the [Task JSON Format](index.md) — typically the wrapped object form `{"tasks": [...]}`, optionally with top-level [`env:` / `env_groups:`](environments.md).

This page shows generator skeletons in several languages. Each example produces a simple parameter sweep that you can adapt.

## Python

```python
#!/usr/bin/env python3
"""Generate tasks for ScriptHut."""

import argparse
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--partition", default="normal")
    args = parser.parse_args()

    tasks = []
    for i in range(1, args.count + 1):
        tasks.append({
            "id": f"task-{i:03d}",
            "name": f"Task {i}",
            "command": f"python process.py --index {i}",
            "partition": args.partition,
            "cpus": 2,
            "memory": "8G",
            "time_limit": "2:00:00",
        })

    print(json.dumps({"tasks": tasks}, indent=2))

if __name__ == "__main__":
    main()
```

Configure in `scripthut.yaml`:

```yaml
workflows:
  - name: batch-processing
    backend: hpc-cluster
    command: "python /path/to/generate_tasks.py --count 20 --partition gpu"
    max_concurrent: 5
    description: "Batch processing pipeline"
```

## Bash

```bash
#!/bin/bash
# Generate tasks as JSON using heredoc

cat <<'EOF'
{
  "tasks": [
    {
      "id": "step-1",
      "name": "Download",
      "command": "wget https://example.com/data.csv",
      "time_limit": "00:30:00"
    },
    {
      "id": "step-2",
      "name": "Process",
      "command": "python process.py data.csv",
      "deps": ["step-1"],
      "cpus": 4,
      "memory": "16G"
    }
  ]
}
EOF
```

## Static JSON File

The simplest approach — just `cat` a pre-existing JSON file:

```yaml
workflows:
  - name: static-tasks
    backend: hpc-cluster
    command: "cat /shared/tasks/my_pipeline.json"
    description: "Run predefined pipeline"
```

## Julia

```julia
#!/usr/bin/env julia
using JSON

tasks = [
    Dict(
        "id" => "sim-$i",
        "name" => "Simulation $i",
        "command" => "julia run_sim.jl --seed $i",
        "partition" => "normal",
        "cpus" => 4,
        "memory" => "8G",
        "time_limit" => "6:00:00",
        "env" => [Dict("include" => ["julia-1.10"])]
    )
    for i in 1:10
]

println(JSON.json(Dict("tasks" => tasks), 2))
```

## R

```r
#!/usr/bin/env Rscript
library(jsonlite)

tasks <- lapply(1:10, function(i) {
  list(
    id = sprintf("analysis-%03d", i),
    name = sprintf("Analysis %d", i),
    command = sprintf("Rscript run_analysis.R --chunk %d", i),
    partition = "normal",
    cpus = 1,
    memory = "4G",
    time_limit = "1:00:00"
  )
})

cat(toJSON(list(tasks = tasks), auto_unbox = TRUE, pretty = TRUE))
```
