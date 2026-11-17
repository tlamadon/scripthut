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

## Git source working directories

```yaml
sources:
  - name: example
    type: git
    url: https://example.edu/example.git
    branch: main
    clone_dir: /data/repos
    workdir: /data/example       # optional; absolute backend path
    flow_dir: analysis           # optional; default repository root (.)
    # local_path: /data/local-example  # use local committed code instead of URL
    # deploy_key: ~/.ssh/example_deploy
    # postclone: mkdir -p output
```

Without `workdir`, the fixed checkout is
`<clone_dir>/.worktrees/source-<URL-encoded source name>`. This cannot collide
with the existing `<clone_dir>/<12-character commit>` layout selected by
`--snapshot`. `flow_dir` is relative, contains no `..`, and must resolve inside
the repository. ScriptFlow runs from `<workdir>/<flow_dir>`; its default records
live there under `.sf/runs`. `--runs-dir` can override the records directory.

Git sources are backend-independent: specify `--backend` if more than one is
configured. A single backend is selected automatically. `local_path` takes
precedence over `url`; committed objects travel through the existing Git push
mechanism into a source-specific backend mirror. URL sources use the configured
deploy key, or HTTPS conversion for public Git SSH URLs. Postclone runs only for
a newly created checkout. No result cache is used.

## Source container images

```yaml
backends:
  - name: cluster
    type: slurm
    ssh:
      host: hpc-cluster.example.edu
      user: alice
    clone_dir: /data/repos
    image_dir: /data/images        # optional; default <backend clone_dir>/images
    # For private pulls, configure both:
    # registry_user: alice
    # registry_token_file: /data/local-secrets/registry-token
    env_groups:
      scriptflow:
        - init: module load apptainer; source /data/venv/bin/activate
sources:
  - name: example
    type: git
    local_path: /data/local-example
    images:
      - name: app
        ref_file: image.ref
      - name: tools
        ref: registry.example/org/tools@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
```

Each image requires exactly one of `ref` and `ref_file`. Names contain letters,
digits or underscores, and must be unique after upper-casing. `ref_file` is relative
to the repository root and read from the selected commit, never the dirty working
tree. References are Docker/OCI registry references with optional `docker://`
prefix and must end in `@sha256:` plus 64 hexadecimal digits. A tag-only reference
requires explicit `allow_tag: true`. That opt-out uses a hash of the reference as
its filename and reuses the first pulled file; tags are not refreshed automatically.

Digest files are shared by sources/runs on the backend as
`<image_dir>/sha256-<digest>.sif`. Only the controller compute job pulls, using
Apptainer from the configured environment group. It locks each target, pulls to a
private temporary directory inside the image directory and publishes by rename.
Existing images are never overwritten or deleted. Partial images, temporary runtime
directories and lock files are retained; there is no image cleanup command.
The shared filesystem must support `flock`; compute nodes also need Bash and GNU
file utilities. Runtime configuration and temporary files stay in the image directory.

Registry tokens are read from a **local** file only when an image is missing, sent
through SSH stdin into a mode-0600 backend file, and read via Apptainer's credential
environment variables inside the pull subprocess. Tokens are not arguments, job
script literals, launch-record data or log text. Runtime stdout/stderr is redacted
before being saved. The controller removes its token file on success or failure;
SIGKILL, node loss or a controller that never starts can prevent this cleanup.
Apptainer's [credential and runtime environment variables](https://apptainer.org/docs/user/latest/appendix.html)
are used without a registry-login command. Public pulls work without credential settings.
The backend's single credential pair
applies to its declared missing images.

The controller exports `SCRIPTHUT_IMAGE_<UPPERCASE_NAME>` and atomically writes
`<flow directory>/.sf/images.sh`. See [working from a project](../from-a-project.md)
for flow code and manual use. No bind-mount settings are added: binds belong to the
flow, and module loads/runtime setup belong to `env_groups`.

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
