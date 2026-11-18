"""Source discovery distinguishes absent flow files from Git failures."""

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from scripthut import main
from scripthut.config_schema import GitSourceConfig, ScriptHutConfig
from scripthut.scriptflow import RecordReader
from scripthut.scriptflow_sources import Revision, list_flows


@pytest.mark.parametrize("local", [True, False])
@pytest.mark.parametrize("flow_dir", [".", "analysis"])
@pytest.mark.parametrize("missing", [True, False])
async def test_source_flow_error(tmp_path, monkeypatch, local, flow_dir, missing):
    source = GitSourceConfig(
        name="example", url="https://example.edu/repo.git", flow_dir=flow_dir,
        local_path=tmp_path if local else None, workdir="/data/source",
    )
    config = ScriptHutConfig(sources=[source])
    path = "sflow.py" if flow_dir == "." else "analysis/sflow.py"
    error = f"fatal: path '{path}' does not exist in 'abc123'" if missing else "fatal: permission denied"
    monkeypatch.setattr("scripthut.sources.git.GitSourceManager._run_git",
                        AsyncMock(return_value=("", error, 128)))
    monkeypatch.setattr("scripthut.scriptflow_sources.workdir_for",
                        AsyncMock(return_value="/data/source"))
    transport = AsyncMock()
    transport.run_command.return_value = ("", error, 128)
    expected = "no sflow.py in this source" if missing else "permission denied"
    with pytest.raises(ValueError, match=expected) as caught:
        await list_flows(config, Revision(source, "main", "abc123", "abc123"), RecordReader(transport))
    if missing:
        assert str(caught.value) == expected
        monkeypatch.setattr(main.state, "config", config)
        monkeypatch.setattr(main.state, "web_sources", [
            dict(config=source, flows=[], error=str(caught.value),
                 workdir=source.working_directory, flow_dir=flow_dir),
        ])
        response = TestClient(main.app).get("/sources")
        assert response.status_code == 200
        assert expected in response.text
        assert "fatal: path" not in response.text
