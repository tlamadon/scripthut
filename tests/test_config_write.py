"""Tests for the cli_autostart setting and the global-config writer."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from scripthut import config as config_mod
from scripthut.config import set_global_setting
from scripthut.config_schema import GlobalSettings


# -- cli_autostart schema ------------------------------------------------------


def test_cli_autostart_defaults_to_ask():
    assert GlobalSettings().cli_autostart == "ask"


@pytest.mark.parametrize("value", ["ask", "always", "never"])
def test_cli_autostart_accepts_valid_values(value):
    assert GlobalSettings(cli_autostart=value).cli_autostart == value


def test_cli_autostart_rejects_unknown_value():
    with pytest.raises(ValidationError):
        GlobalSettings(cli_autostart="sometimes")


# -- set_global_setting --------------------------------------------------------


@pytest.fixture
def global_cfg(tmp_path, monkeypatch):
    """Point config discovery at a tmp file; returns its path (not created)."""
    path = tmp_path / ".config" / "scripthut" / "scripthut.yaml"
    monkeypatch.setattr(config_mod, "GLOBAL_CONFIG_PATHS", [path])
    monkeypatch.setattr(
        config_mod, "discover_global_config",
        lambda: path if path.exists() else None,
    )
    return path


def test_creates_global_config_when_missing(global_cfg):
    out = set_global_setting("cli_autostart", "always")
    assert out == global_cfg
    assert global_cfg.read_text() == "settings:\n  cli_autostart: always\n"


def test_replaces_existing_key_preserving_comments(global_cfg):
    global_cfg.parent.mkdir(parents=True)
    global_cfg.write_text(
        "# my hand-written config\n"
        "settings:\n"
        "  server_port: 9000  # custom port\n"
        "  cli_autostart: always\n"
    )
    set_global_setting("cli_autostart", "never")
    text = global_cfg.read_text()
    assert "cli_autostart: never" in text
    assert "always" not in text
    assert "# my hand-written config" in text
    assert "server_port: 9000  # custom port" in text


def test_inserts_under_existing_settings_section(global_cfg):
    global_cfg.parent.mkdir(parents=True)
    global_cfg.write_text("settings:\n  server_port: 9000\n")
    set_global_setting("cli_autostart", "never")
    text = global_cfg.read_text()
    assert "settings:\n  cli_autostart: never\n  server_port: 9000" in text
    import yaml
    assert yaml.safe_load(text)["settings"]["cli_autostart"] == "never"


def test_appends_settings_block_when_absent(global_cfg):
    global_cfg.parent.mkdir(parents=True)
    global_cfg.write_text("backends: []\n")
    set_global_setting("cli_autostart", "always")
    import yaml
    data = yaml.safe_load(global_cfg.read_text())
    assert data["settings"]["cli_autostart"] == "always"
    assert data["backends"] == []


def test_invalid_result_raises_and_leaves_file_untouched(global_cfg):
    global_cfg.parent.mkdir(parents=True)
    original = "settings:\n  server_port: 9000\n"
    global_cfg.write_text(original)
    with pytest.raises(RuntimeError, match="manually"):
        set_global_setting("cli_autostart", "bogus-value")
    assert global_cfg.read_text() == original
