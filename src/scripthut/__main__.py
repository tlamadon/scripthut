"""Allow ``python -m scripthut`` to behave like the ``scripthut`` console script.

The CLI daemon autostart re-invokes the interpreter this way so it works in
any environment (venv, uv, pipx) without depending on the console script
being on PATH.
"""

from scripthut.main import run

if __name__ == "__main__":
    run()
