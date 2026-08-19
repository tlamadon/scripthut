# ScriptHut — agent instructions

ScriptHut runs compute workflows on the user's own infrastructure — HPC clusters (Slurm, PBS/Torque) over SSH, AWS Batch via the API, and AWS EC2 (one instance per task, SSM-tunnelled). It is driven from a local CLI, also usable by coding agents, backed by a small control plane that monitors flows and surfaces logs, errors, and status. FastAPI, asyncssh, Pydantic, HTMX.

## Architecture principles

These are load-bearing. Violating one produces code that works in a test and fails on a real cluster.

1. **Nothing runs on the head node except scheduling.** All user code runs as scheduler jobs on compute nodes. The only commands ScriptHut runs directly over SSH are `sbatch`, `squeue`, `scancel`, `cat`, `mkdir` and similar basics. Never assume `python`, `R`, or any toolchain exists on the head node.
2. **Endogenous workflows.** A task source can be produced by a task. The user points ScriptHut at a single generator task, which runs on a compute node and writes JSON; ScriptHut reads that and schedules the real work. From the control plane's view it is all just tasks and sources.
3. **Language agnostic.** Whatever emits the task JSON is the user's business. Only the JSON contract matters.
4. **Single polling loop.** One `poll_jobs()` loop drives every backend interaction — `squeue`, `sacct`, run status, external job tracking, SSE notifications. There must never be a competing loop or an independent timer making SSH calls. `POST /poll` wakes the same loop early rather than creating a parallel path.
5. **Passive SSE.** Every SSE endpoint waits on an `asyncio.Event` set by the poll loop or run manager. They never poll and never make SSH calls; the frontend is pushed to, not pulling.

## Conventions

- Type annotations on all functions; async/await for all I/O.
- Pydantic models for configuration and validation.
- Abstract base classes for extensible backends (`backends/base.py`).
- Tests are pytest + pytest-asyncio. Dev deps live in an extra, so the loop is `uv sync --extra dev && uv run pytest`.
- Planning notes and design discussions go in `.discussion/` at the repo root.

## Change map

Where to edit when changing a behavior, and what else has to move with it.

| Changing | Edit | Also update |
|---|---|---|
| Dataset destination layout, manifest hash width, `DATA_*` variable names | `src/scripthut/runs/datasets.py` (the module docstring marks it as the one place) | Every doc layer below |
| Sync dest layout, `output/` return dir, `DATA_*`-unrelated `sync_dir` | `src/scripthut/runs/sync.py` (the module docstring marks it as the one place) | Every doc layer below |
| A config field | `src/scripthut/config_schema.py` — the `description=` is user-visible via JSON schema and the Settings page, so it must be true | `scripthut.example.yaml`, README table, `docs/configuration/` |
| Backend behavior | `src/scripthut/backends/` | `docs/configuration/backends.md` |
| Anything an agent needs to know | `_render_agent_prompt()` in `src/scripthut/cli.py` | `tests/test_agent_prompt.py` pins its headings and key strings |

### Documentation layers

A user-facing change is not done until every layer that mentions the feature agrees. Grep for the feature's tokens across all of these before calling it finished:

1. `_render_agent_prompt()` in `src/scripthut/cli.py` — what `scripthut agent prompt` emits. This is the contract coding agents read first; `tests/test_agent_prompt.py` guards it.
2. `render_skill()` in `src/scripthut/agent_skill.py` — the skill `scripthut agent install` writes.
3. Pydantic `description=` strings in `src/scripthut/config_schema.py`.
4. `scripthut.example.yaml` — the canonical annotated config.
5. `README.md`.
6. `docs/` plus the `mkdocs.yml` nav. Verify with `uvx --with mkdocs-material --with mkdocs-redirects --with mike mkdocs build --strict`.
7. This file.

The house skill at `dotfiles/skills/scripthut/` is maintained separately and is *not* part of this repo; never edit the deployed copy under `~/.claude` or `~/.cursor`.

## Gotchas

- A backend's `dataset_dir` is the remote parent for staged datasets; the unrelated `settings.data_dir` is the daemon's local cache base. The names were split deliberately so the two cannot be confused — do not reintroduce `data_dir` on a backend.
- A backend's `sync_dir` is the remote parent for `type: sync` working copies. It must not sit under `clone_dir` or `dataset_dir`. Disk scan inventories dests as live; disk clean never deletes them.
- `SCRIPTHUT_`-prefixed variables cannot be set by env rules and are stripped from cache keys. Anything that must survive both — like a dataset destination — must not use that prefix.
