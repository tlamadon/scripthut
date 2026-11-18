"""Configuration loading rejects ineffective keys and selects one file."""

import pytest
import yaml

from scripthut import config


def write_config(path, data):
    path.write_text(yaml.safe_dump(data))
    return path


@pytest.mark.parametrize("key,value", [("env", []), ("env_groups", {})])
def test_top_level_environment_is_obsolete(tmp_path, key, value):
    path = write_config(tmp_path / "config.yaml", {key: value})
    with pytest.raises(config.ConfigError, match=f"Obsolete configuration keys: {key}"):
        config.load_config(path)


@pytest.mark.parametrize("kind", ["slurm", "pbs", "git", "path"])
def test_unknown_backend_and_source_keys_are_errors(tmp_path, kind):
    entry = {"name": "example", "type": kind, "stale_option": True}
    if kind in ("slurm", "pbs"):
        entry["ssh"] = {"host": "hpc-cluster.example.edu", "user": "alice"}
        section = "backends"
    else:
        section = "sources"
        entry.update({"url": "https://example.edu/repo.git"} if kind == "git"
                     else {"path": "/data/source", "backend": "cluster"})
    path = write_config(tmp_path / "config.yaml", {section: [entry]})
    with pytest.raises(config.ConfigError, match="stale_option"):
        config.load_config(path)


def test_global_local_and_explicit_config_selection(tmp_path, monkeypatch):
    global_path = write_config(tmp_path / "global.yaml", {"settings": {"server_port": 8100}})
    local_path = write_config(tmp_path / "scripthut.yaml", {"settings": {"server_port": 8200}})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config, "GLOBAL_CONFIG_PATHS", [global_path])
    assert config.load_config().settings.server_port == 8100
    assert config.load_config(local_path).settings.server_port == 8200
    global_path.unlink()
    assert config.load_config().settings.server_port == 8200
    child = tmp_path / "child"
    child.mkdir()
    monkeypatch.chdir(child)
    assert config.find_config_file() is None
