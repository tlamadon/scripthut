"""Tests for layered (global + project-local) config loading.

Covers:
- Discovery: walking up from CWD; ignoring the global file when it
  happens to be discovered as ``project-local``.
- Merge semantics: project entries override global by name; env lists
  concatenate (global before project); env_groups dict-merge.
- Rejection of infra fields (backends/sources/settings/pricing) inside a
  project-local file.
- ``Stack.input_files`` resolved relative to the config file's directory,
  not the process CWD.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from scripthut.config import (
    ConfigError,
    _merge_configs,
    _validate_project_local_yaml,
    discover_global_config,
    discover_project_config,
    load_layered_config,
    load_yaml_config,
)


def _write_yaml(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return path


# ---------- discovery ----------------------------------------------------


class TestDiscovery:
    def test_walks_up_to_find_project_yaml(self, tmp_path: Path):
        # repo/scripthut.yaml exists; CWD is repo/subdir/deeper
        project_yaml = _write_yaml(
            tmp_path / "repo" / "scripthut.yaml", "stacks: []\n"
        )
        deep = tmp_path / "repo" / "subdir" / "deeper"
        deep.mkdir(parents=True)

        found = discover_project_config(deep)
        assert found is not None
        assert found.resolve() == project_yaml.resolve()

    def test_returns_none_when_no_yaml_anywhere(self, tmp_path: Path):
        # tmp_path tree contains no scripthut.yaml. Force the global
        # search to also miss so we don't accidentally pick one up.
        with patch("scripthut.config.GLOBAL_CONFIG_PATHS", []):
            assert discover_project_config(tmp_path) is None

    def test_skips_yaml_that_is_actually_the_global_file(self, tmp_path: Path):
        # If CWD happens to be the directory containing the global
        # config, we must not return it as project-local.
        global_yaml = _write_yaml(tmp_path / "global.yaml", "backends: []\n")
        # Pretend the global config path lives at tmp_path/global.yaml,
        # and CWD is some subdirectory where walking up finds... nothing.
        # We then verify: if the global path lives directly under tmp_path
        # and we put a file with the same name in tmp_path, discovery
        # would return that file *unless* we hide it via the same name
        # check. So construct that situation explicitly:
        sub = tmp_path / "sub"
        sub.mkdir()
        # Put a scripthut.yaml at tmp_path that happens to be the
        # configured global file.
        local_clone = _write_yaml(tmp_path / "scripthut.yaml", "stacks: []\n")

        with patch("scripthut.config.GLOBAL_CONFIG_PATHS", [local_clone]):
            # walking up from sub finds tmp_path/scripthut.yaml, but it
            # resolves to the global path → discovery returns None.
            assert discover_project_config(sub) is None
        # control: when the global list is empty, the same file IS
        # treated as project-local.
        _ = global_yaml  # silence unused-var
        with patch("scripthut.config.GLOBAL_CONFIG_PATHS", []):
            assert discover_project_config(sub) == local_clone

    def test_discover_global_picks_first_existing(self, tmp_path: Path):
        first = tmp_path / "first.yaml"
        second = _write_yaml(tmp_path / "second.yaml", "stacks: []\n")
        with patch("scripthut.config.GLOBAL_CONFIG_PATHS", [first, second]):
            # First doesn't exist; should fall through to second.
            assert discover_global_config() == second

    def test_discover_global_returns_none_when_no_file(self, tmp_path: Path):
        with patch(
            "scripthut.config.GLOBAL_CONFIG_PATHS",
            [tmp_path / "missing-a.yaml", tmp_path / "missing-b.yaml"],
        ):
            assert discover_global_config() is None


# ---------- project-local validation -------------------------------------


class TestProjectLocalValidation:
    def test_rejects_backends(self, tmp_path: Path):
        raw = {"backends": [{"name": "x", "type": "slurm"}]}
        with pytest.raises(ConfigError, match="backends"):
            _validate_project_local_yaml(raw, tmp_path / "scripthut.yaml")

    def test_rejects_sources_settings_pricing(self, tmp_path: Path):
        raw = {
            "sources": [], "settings": {}, "pricing": None,
        }
        with pytest.raises(ConfigError) as exc:
            _validate_project_local_yaml(raw, tmp_path / "scripthut.yaml")
        msg = str(exc.value)
        for field in ("sources", "settings", "pricing"):
            assert field in msg

    def test_allows_stacks_workflows_env(self, tmp_path: Path):
        # No exception expected
        _validate_project_local_yaml(
            {"stacks": [], "workflows": [], "env": [], "env_groups": {}},
            tmp_path / "scripthut.yaml",
        )


# ---------- merge --------------------------------------------------------


def _yaml_for_global() -> str:
    return (
        "backends: []\n"
        "stacks:\n"
        "  - name: shared\n"
        "    prep: |\n"
        "      echo global\n"
        "  - name: tool\n"
        "    prep: |\n"
        "      echo from-global\n"
        "env:\n"
        '  - set: { GLOBAL_VAR: "1" }\n'
        "env_groups:\n"
        "  base: []\n"
    )


def _yaml_for_project() -> str:
    return (
        "stacks:\n"
        "  - name: tool\n"
        "    prep: |\n"
        "      echo from-project\n"
        "  - name: project-only\n"
        "    prep: |\n"
        "      echo new\n"
        "env:\n"
        '  - set: { PROJECT_VAR: "1" }\n'
        "env_groups:\n"
        "  extra: []\n"
    )


class TestMerge:
    def test_project_overrides_by_name_in_stacks(self, tmp_path: Path):
        g = load_yaml_config(_write_yaml(tmp_path / "g.yaml", _yaml_for_global()))
        p = load_yaml_config(_write_yaml(tmp_path / "p.yaml", _yaml_for_project()))
        merged = _merge_configs(g, p)

        names = {s.name for s in merged.stacks}
        assert names == {"shared", "tool", "project-only"}

        tool = next(s for s in merged.stacks if s.name == "tool")
        assert "from-project" in tool.prep  # project wins

        shared = next(s for s in merged.stacks if s.name == "shared")
        assert "global" in shared.prep  # unchanged

    def test_env_concatenates_global_first(self, tmp_path: Path):
        g = load_yaml_config(_write_yaml(tmp_path / "g.yaml", _yaml_for_global()))
        p = load_yaml_config(_write_yaml(tmp_path / "p.yaml", _yaml_for_project()))
        merged = _merge_configs(g, p)

        # Both env rules are present, in order: global then project.
        flat = [(r.set or {}) for r in merged.env]
        assert flat == [{"GLOBAL_VAR": "1"}, {"PROJECT_VAR": "1"}]

    def test_env_groups_merge_project_wins(self, tmp_path: Path):
        g = load_yaml_config(_write_yaml(tmp_path / "g.yaml", _yaml_for_global()))
        p = load_yaml_config(_write_yaml(tmp_path / "p.yaml", _yaml_for_project()))
        merged = _merge_configs(g, p)
        assert set(merged.env_groups) == {"base", "extra"}


# ---------- input_files resolution ---------------------------------------


class TestInputFilesResolution:
    def test_relative_paths_resolved_to_config_dir(self, tmp_path: Path):
        project_dir = tmp_path / "repo"
        project_dir.mkdir()
        (project_dir / "requirements.txt").write_text("foo==1.0\n")
        cfg_path = _write_yaml(
            project_dir / "scripthut.yaml",
            (
                "stacks:\n"
                "  - name: py\n"
                "    prep: pip install -r requirements.txt\n"
                "    input_files:\n"
                "      - requirements.txt\n"
            ),
        )

        cfg = load_yaml_config(cfg_path)
        assert len(cfg.stacks) == 1
        resolved = cfg.stacks[0].input_files[0]
        assert resolved.is_absolute()
        assert resolved == (project_dir / "requirements.txt").resolve()

    def test_absolute_path_left_unchanged(self, tmp_path: Path):
        (tmp_path / "abs.txt").write_text("hi\n")
        cfg_path = _write_yaml(
            tmp_path / "scripthut.yaml",
            f"stacks:\n"
            f"  - name: x\n"
            f"    prep: echo hi\n"
            f"    input_files:\n"
            f"      - {tmp_path / 'abs.txt'}\n",
        )
        cfg = load_yaml_config(cfg_path)
        assert cfg.stacks[0].input_files[0] == tmp_path / "abs.txt"


# ---------- layered load end-to-end --------------------------------------


class TestLoadLayered:
    def test_only_global(self, tmp_path: Path):
        g = _write_yaml(tmp_path / "global.yaml", "backends: []\n")
        with patch("scripthut.config.GLOBAL_CONFIG_PATHS", [g]):
            # CWD points somewhere with no scripthut.yaml.
            other = tmp_path / "elsewhere"
            other.mkdir()
            cfg = load_layered_config(cwd=other)
        assert cfg is not None
        assert cfg.backends == []

    def test_only_project(self, tmp_path: Path):
        project_dir = tmp_path / "repo"
        project_dir.mkdir()
        _write_yaml(
            project_dir / "scripthut.yaml",
            "stacks:\n  - name: solo\n    prep: echo hi\n",
        )
        with patch("scripthut.config.GLOBAL_CONFIG_PATHS", []):
            cfg = load_layered_config(cwd=project_dir)
        assert cfg is not None
        assert [s.name for s in cfg.stacks] == ["solo"]

    def test_both_merge(self, tmp_path: Path):
        g = _write_yaml(
            tmp_path / "global.yaml",
            "backends: []\nstacks:\n  - name: shared\n    prep: echo g\n",
        )
        project_dir = tmp_path / "repo"
        project_dir.mkdir()
        _write_yaml(
            project_dir / "scripthut.yaml",
            "stacks:\n  - name: shared\n    prep: echo p\n",
        )
        with patch("scripthut.config.GLOBAL_CONFIG_PATHS", [g]):
            cfg = load_layered_config(cwd=project_dir)
        assert cfg is not None
        # project override wins
        assert "p" in cfg.stacks[0].prep

    def test_project_with_forbidden_field_raises(self, tmp_path: Path):
        g = _write_yaml(tmp_path / "global.yaml", "backends: []\n")
        project_dir = tmp_path / "repo"
        project_dir.mkdir()
        _write_yaml(
            project_dir / "scripthut.yaml",
            (
                "backends:\n"
                "  - name: oops\n"
                "    type: slurm\n"
                "    ssh: { host: x, user: y }\n"
            ),
        )
        with patch("scripthut.config.GLOBAL_CONFIG_PATHS", [g]):
            with pytest.raises(ConfigError, match="backends"):
                load_layered_config(cwd=project_dir)

    def test_returns_none_when_no_config_found(self, tmp_path: Path):
        empty = tmp_path / "empty"
        empty.mkdir()
        with patch("scripthut.config.GLOBAL_CONFIG_PATHS", []):
            assert load_layered_config(cwd=empty) is None
