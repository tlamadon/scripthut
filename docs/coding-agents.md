# Using ScriptHut from a coding agent

If you want a coding agent (Claude Code, Codex, Cursor, Aider, etc.) to drive ScriptHut on your behalf, you don't need a custom plugin. Just give it the output of one command:

```bash
scripthut agent prompt
```

That prints a Markdown briefing — a static reference for the CLI surface, exit codes, and submission patterns, **plus a live inventory** of the backends, sources, and stacks your local `scripthut.yaml` defines. Drop the output where your agent reads context and you're done.

## Three ways to wire it in

### 1. Project file the agent auto-loads

Most coding agents read a file at the project root on startup. Pipe the briefing there once per project:

```bash
cd /path/to/your/project
scripthut agent prompt > CLAUDE.md      # Claude Code reads CLAUDE.md
scripthut agent prompt > AGENTS.md      # Codex reads AGENTS.md
scripthut agent prompt > .cursorrules   # Cursor reads .cursorrules
```

Re-run after you change `scripthut.yaml` (new backend, new source, new stack) so the inventory section stays accurate.

### 2. Inject into a Claude Code session

Claude Code's `!` prefix runs a shell command and pipes its output directly into the conversation. Type this as a message in the chat:

```
!scripthut agent prompt
```

Useful when you want to brief an existing session without persisting to a file.

### 3. Copy-paste

Same idea, manual:

```bash
scripthut agent prompt | pbcopy        # macOS
scripthut agent prompt | xclip -selection clipboard  # Linux
```

…then ⌘V into the agent's prompt area.

## What the briefing covers

A re-read shows you the current contents, but for orientation:

- **Inventory** — the backends, stacks, and sources you've configured (when a `scripthut.yaml` is discoverable).
- **Submitting work** — the three input modes for `scripthut task run` (`--inline-script`, positional command, `--from-stdin`) and when to prefer a source workflow (`scripthut workflow run`).
- **TaskDefinition shape** — the JSON schema agents need when constructing tasks programmatically.
- **Resource sizing defaults** — start small (1 CPU / 1 GB / 5 min), escalate only when an earlier run hit OOM or timed out.
- **Editing `scripthut.yaml`** — the two-layer model (user-global vs. project-local), what each file is allowed to carry, merge semantics, env-rule schema (`set` / `if` / `include` / `stacks` / `append` / `init`), and edit discipline (read first, minimal diff, hot-reload not restart).
- **Stacks** — full define → install → reference flow, including the v0.7.1 `stacks: [name]` env-rule reference syntax and the two failure modes that matter (unknown name → loud `ValueError`, no auto-install at submit time).
- **Verify-then-submit loop** — the read-only steps the agent should run before any submission (`status`, `backend list`, `source view`, `stack check`, `--dry-run`) and how to track a run's status / output / logs after.
- **Gotchas and exit codes** — `working_dir` is a path on the *backend*, partition names are remapped per backend, exit code `2` is reserved for `run watch --exit-status` failure, etc.

## Targeting a remote server

`scripthut agent prompt` itself reads your *local* `scripthut.yaml` to populate the inventory section — it doesn't pull from a running server. When the agent is working against a remote server (`SCRIPTHUT_SERVER=...`), tell it to also run these against the server for the live picture:

```bash
scripthut status                       # server reachable + auth OK, plus configured sources/backends
scripthut backend list --json          # live connectivity per backend
scripthut source list --json           # sources known to the server
scripthut source view <name> --json    # workflows discovered in a specific source
scripthut stack check --source <name>  # per-backend stack state for repo-defined stacks
```

The briefing's "Inspecting state" cheat-sheet already lists these, so an agent that's read it will reach for them.

## Keeping the briefing trustworthy

The static reference inside the briefing is **auto-tested against the actual CLI** — there's a regression suite (`tests/test_agent_prompt.py`) that asserts the commands, flags, and behaviors the briefing teaches still exist. So when ScriptHut adds or renames a subcommand, the briefing fails CI until it's updated. If you spot something stale, re-run `scripthut agent prompt` after `pip install -U scripthut` — it'll be current with the installed version.

## Tips for productive sessions

- **Run `scripthut status` first** in any new session — if the CLI can't reach the server or auth is broken, nothing the agent does will work, and `status` produces a clean diagnostic the agent can show you.
- **Use `--json` everywhere the agent parses output.** Every read-only command supports it; the JSON shapes are stable across releases.
- **Capture run IDs explicitly:** `RUN_ID=$(scripthut task run … --json | jq -r .id)` — easier than scraping a human-readable submission summary.
- **For non-trivial tasks, use `--dry-run` first** so the agent can show you the assembled `TaskDefinition` (resources, env resolution, generated script) before anything submits.
