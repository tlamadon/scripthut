# Configuration

Use `scripthut --config scripthut.yaml` or pass `--config` after a CLI subcommand.
Without an explicit path, ScriptHut discovers the user-global file
`~/.config/scripthut/scripthut.yaml` (or `~/.scripthut.yaml`) and a project
`scripthut.yaml` by walking up from the working directory. When both exist,
project `env` rules append to global rules and project `env_groups` override
names. Infrastructure belongs in the global file. The deprecated `.env` fallback
still supports a single SSH backend when no YAML exists.

## Sections

| Section | Purpose |
|---|---|
| `backends` | Named Slurm or PBS SSH connections; see [backends](backends.md) |
| `sources` | Git remote/local repository metadata, deploy keys, clone directory and postclone; path sources describe an existing backend directory |
| `env`, `env_groups` | Environment rules and reusable groups; see [environments](environments.md) |
| `scriptflow` | Controller `env_group`, `cpus`, `memory`, `time_limit`, optional `partition` |
| `settings` | Server, polling, local state and CLI connection defaults |

`scriptflow.env_group` must name an environment group on the selected backend.
Launch options can override resources. Configuration does not install ScriptFlow.

## Settings

| Key | Default | Purpose |
|---|---|---|
| `poll_interval` | 60 | Single polling loop interval in seconds, minimum 5 |
| `server_host` | `127.0.0.1` | Bind address |
| `server_port` | 8000 | Bind port |
| `data_dir` | `~/.cache/scripthut` | ScriptFlow references, cached observations and launch records |
| `sources_cache_dir` | `<data_dir>/sources` | Local git source clones |
| `filter_user` | unset | Scheduler job listing filter |
| `cli_server` | unset | Default API server; command flag and environment take precedence |
| `cli_auth` | unset | Cloudflare Access service-token/JWT credentials |
| `cli_autostart` | `ask` | Local daemon startup policy: ask, always or never |

See the repository's `scripthut.example.yaml` for a complete placeholder config.
Backend changes require server restart. Settings/environment/source metadata can
be reloaded through the settings page; no reload submits tasks.

## Obsolete keys: errors, never ignored

Every detected obsolete key causes a validation error. One message lists all
detected keys, with indexed paths for nested settings. Remove them and retry;
there is no compatibility mode or `legacy_runs_enabled` switch.

- Top-level: `cache`, `stacks`, `agent`, `pricing`, `projects`, `workflows`,
  `notifications`, `disk`.
- Settings: `legacy_runs_enabled`.
- Backend entries: `max_concurrent`; backend types other than `slurm` and `pbs`
  are errors, including removed `batch`, `ecs`, `ec2`, `local` and unknown types.
- Source entries: `workflows_glob`, `max_concurrent`, `workflows`, and the obsolete git-source
  `backend` selector.
- Environment rules/groups: `stacks` references. Replace needed activation with
  an environment group's `init`/`include` rules; ScriptHut does not install stacks.

Old legacy run records and usage ledgers remain on disk untouched. ScriptHut no
longer loads or cleans them. The disk inspection/deletion tools themselves are removed.
