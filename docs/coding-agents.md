# Coding agents

`scripthut agent prompt` prints current supported commands and configured backend
and source names. `scripthut agent install` installs the local skill and diagnostic
command; `--user` targets the user's agent directory, otherwise the current project.
unmanaged files are preserved unless `--force` is explicit.

Agents use ScriptFlow launch, view,
logs and cancellation, respecting the separation between workflow outcome and
observation freshness. Never use direct remote login as a fallback.

The diagnostic command reads state and attempt logs, reports a cause and proposes
a fix. Launch, cancellation, cleanup and release require the user's task scope.
