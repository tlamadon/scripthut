# Backends

Slurm and PBS use the SSH command transport. Configure `name`, `type`
and `ssh` (`host`, `user`, optional `port`, `key_path`, `cert_path`, `known_hosts`).
Set `known_hosts` to enable host-key verification. Secrets and private hostnames
belong in your untracked configuration.

```yaml
backends:
  - name: cluster
    type: slurm
    ssh:
      host: hpc-cluster.example.edu
      user: alice
      key_path: ~/.ssh/id_ed25519
      known_hosts: ~/.ssh/known_hosts
    env_groups:
      scriptflow:
        - init: source /data/venv/bin/activate
scriptflow:
  env_group: scriptflow
  cpus: 1
  memory: 2G
  time_limit: '01:00:00'
```

`/backends` shows connection status and read-only scheduler job listing.
`/api/v1/backends` reports connections; `/api/v1/backends/NAME/jobs` returns cached
job observations. No endpoint cancels or submits these listed jobs.

Automated ScriptFlow launch/cleanup/release uses Slurm. PBS supports run
observation and job listing. Its configuration fields are `name`, `type`, `ssh`,
`queue` (display fallback), `clone_dir`, and `image_dir` for read-only image
inspection.
