"""Agent instructions describe only the supported command surface."""
from scripthut.cli import _render_agent_prompt
from scripthut.config_schema import ScriptHutConfig, SlurmBackendConfig, SSHConfig, GitSourceConfig


def test_prompt_inventory():
    config = ScriptHutConfig(backends=[SlurmBackendConfig(name="cluster", ssh=SSHConfig(host="hpc-cluster.example.edu", user="alice"))], sources=[GitSourceConfig(name="example", url="https://example.edu/repo.git")])
    text = _render_agent_prompt(config)
    assert '"cluster"' in text and '"example"' in text


def test_prompt_surface():
    text = _render_agent_prompt(ScriptHutConfig())
    for command in ("scriptflow launch", "scriptflow view", "scriptflow cancel", "scriptflow logs", "daemon start", "backend list"):
        assert command in text
    for command in ("scripthut run ", "scripthut workflow ", "scripthut task ", "scripthut stack ", "scripthut disk "):
        assert command not in text
    assert "observation problems" in text
