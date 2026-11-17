# Environment rules

Environment groups contain ordered rules with `set`, `append`, `init`, `include`
and optional `if`. `set` assigns strings; `append` joins values with `:`; `init`
provides shell initialization. `${NAME}` expands against variables already resolved.
Conditions AND their keys; a list of values for one key is OR. Included groups
inherit conditions, and cyclic includes are errors. `SCRIPTHUT_*` seed variables
are protected from overrides.

```yaml
backends:
  - name: cluster
    type: slurm
    ssh: {host: hpc-cluster.example.edu, user: alice}
    env_groups:
      base:
        - set: {PROJECT_DATA: /data/project}
      scriptflow:
        - include: [base]
        - init: source /data/venv/bin/activate
scriptflow:
  env_group: scriptflow
```

Controller activation occurs on the compute node with unset-variable checking
disabled, while activation failures still stop the job. Strict mode is restored
before starting ScriptFlow. There are no `stacks` references or automatic installs.
