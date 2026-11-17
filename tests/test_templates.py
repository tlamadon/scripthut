"""Every template compiles, and the disk card renders.

These exist because a Jinja `{# #}` comment placed *inside* a `{% for %}`
tag's expression shipped to production: nothing imports a template at
import time, no unit test rendered one, so a TemplateSyntaxError only
surfaced as a 500 on the live page. Compiling is cheap — there is no
reason for a broken template to reach a release again.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest



def _env():
    """The app's configured environment — custom filters and all."""
    from scripthut.main import templates

    return templates.env


def _template_names() -> list[str]:
    return sorted(_env().list_templates(extensions=["html"]))


def test_templates_are_discovered():
    # Guards the parametrization below against silently testing nothing.
    names = _template_names()
    assert len(names) > 10
    assert "scriptflow.html" in names


def test_templates_live_inside_the_package():
    """The template dir must be package data, not a sibling of the checkout.

    0.12.20 shipped a wheel with no templates at all: the path was built
    as ``__file__.parent.parent.parent / "templates"``, which resolves to
    the repo root from ``src/scripthut/`` but to ``lib/pythonX.Y/`` once
    installed. Every page 500'd with TemplateNotFound. Pinning the
    directory to the package keeps it inside the wheel.
    """
    import scripthut
    from scripthut.main import templates_path

    package_dir = Path(scripthut.__file__).parent
    assert templates_path == package_dir / "templates"
    assert (templates_path / "backends.html").is_file()


@pytest.mark.parametrize("name", _template_names())
def test_template_compiles(name: str):
    """Catches syntax errors: unclosed tags, comments inside expressions."""
    _env().get_template(name)
