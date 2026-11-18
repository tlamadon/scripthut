"""Obsolete configuration is rejected explicitly before startup."""

import pytest
import yaml

from scripthut.config import ConfigError, load_yaml_config


def test_all_obsolete_keys_reported(tmp_path):
    data = {
        "cache": {}, "stacks": [], "agent": {}, "pricing": {}, "disk": {},
        "settings": {"legacy_runs_enabled": False},
        "sources": [{"type": "git", "name": "example", "url": "https://example.edu/repo.git", "workflows_glob": "*.json", "max_concurrent": 2}],
        "env_groups": {"old": [{"stacks": ["python"]}]},
        "backends": [{"name": "cloud", "type": "ec2"}],
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data))
    with pytest.raises(ConfigError) as error:
        load_yaml_config(path)
    for key in ("cache", "stacks", "agent", "pricing", "disk", "settings.legacy_runs_enabled", "sources[0].workflows_glob", "sources[0].max_concurrent", "env_groups.old[0].stacks", "backends[0].type=ec2"):
        assert key in str(error.value)


@pytest.mark.parametrize("kind", ["batch", "ecs", "ec2", "local", "unrecognized"])
def test_unsupported_backend_is_error(tmp_path, kind):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump({"backends": [{"name": "example", "type": kind}]}))
    with pytest.raises(ConfigError, match="unsupported backend type"):
        load_yaml_config(path)


def test_environment_names_are_not_obsolete_keys(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump({"backends": [{"type": "slurm", "name": "cluster", "ssh": {"host": "hpc-cluster.example.edu", "user": "alice"}, "env_groups": {"stacks": [{"if": {"stacks": "ready"}, "set": {"stacks": "value"}}]}}]}))
    config = load_yaml_config(path)
    assert config.backends[0].env_groups["stacks"][0].set == {"stacks": "value"}


def test_backend_rule_obsolete_reference(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump({"backends": [{"type": "slurm", "name": "cluster", "ssh": {"host": "hpc-cluster.example.edu", "user": "alice"}, "env_groups": {"base": [{"stacks": ["old"]}]}}]}))
    with pytest.raises(ConfigError, match=r"backends\[0\].env_groups.base\[0\].stacks"):
        load_yaml_config(path)
