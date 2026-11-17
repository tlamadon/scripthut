# Installation

Python 3.11 or newer is required. Install from a checkout:

```sh
uv tool install -e .
```

Alternatively use `pip install -e .`. The local machine needs Git and an SSH
client for configured git sources. The backend needs plain shell tools and the
scheduler client; tmux and ScriptFlow belong in the compute-node environment.
No Python or ScriptFlow installation is required on the login node.

Copy `scripthut.example.yaml` to `scripthut.yaml` and replace the placeholders.
Start with `scripthut --config scripthut.yaml`. Defaults remain `127.0.0.1:8000`.
The daemon commands manage the same local server process.

For development, use `uv sync --extra dev` and `uv run pytest tests/ -q`.
