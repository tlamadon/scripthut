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


def test_prompt_without_config(monkeypatch, capsys):
    from scripthut import cli

    def missing_config(_):
        raise FileNotFoundError("No configuration")

    monkeypatch.setattr(cli, "load_config", missing_config)
    assert cli.main(["agent", "prompt"]) == 0
    output = capsys.readouterr().out
    assert "scriptflow launch" in output
    assert "Configured inventory" not in output
