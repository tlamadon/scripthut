"""Tests for content-addressed dataset staging (phase 1: pure logic).

Covers:
- ``build_manifest``: determinism, sensitivity to the things that should
  change the hash, insensitivity to the things that should not, and loud
  failure on symlinks it cannot stage faithfully.
- Layout/env helpers, which are the single named place those conventions live.
- ``parse_data_deps`` on workflow documents.
- ``DatasetConfig`` schema rules and the project-local ``datasets:`` ban.
- ``datasets[].path`` resolving against the config file, never the cwd.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from scripthut.config import ConfigError, _validate_project_local_yaml, load_yaml_config
from scripthut.config_schema import ScriptHutConfig
from scripthut.runs.datasets import (
    DATA_DIR_VAR,
    MANIFEST_HASH_LEN,
    DatasetError,
    build_manifest,
    data_env_var,
    dataset_path,
    diff_manifests,
    parse_remote_listing,
    staging_glob,
    staging_path,
    validate_remote_root,
)
from scripthut.runs.models import DataDep, TaskDefinition, parse_data_deps


def _tree(base: Path, files: dict[str, str]) -> Path:
    base.mkdir(parents=True, exist_ok=True)
    for rel, body in files.items():
        p = base / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(body)
    return base


# ---------- manifest hash ------------------------------------------------


class TestBuildManifest:
    def test_hashes_paths_and_sizes(self, tmp_path: Path):
        m = build_manifest(_tree(tmp_path, {"a.csv": "xy", "sub/b.csv": "zzz"}))
        assert m.entries == (("a.csv", 2), ("sub/b.csv", 3))
        assert m.file_count == 2
        assert m.total_bytes == 5
        assert len(m.short) == MANIFEST_HASH_LEN

    def test_identical_trees_in_different_parents_agree(self, tmp_path: Path):
        files = {"a.csv": "xy", "sub/b.csv": "zzz"}
        one = build_manifest(_tree(tmp_path / "one", dict(files)))
        two = build_manifest(_tree(tmp_path / "two", dict(files)))
        assert one.digest == two.digest

    def test_creation_order_does_not_matter(self, tmp_path: Path):
        a = _tree(tmp_path / "a", {})
        for rel in ("z.csv", "m.csv", "a.csv"):
            (a / rel).write_text("x")
        b = _tree(tmp_path / "b", {})
        for rel in ("a.csv", "m.csv", "z.csv"):
            (b / rel).write_text("x")
        assert build_manifest(a).digest == build_manifest(b).digest

    def test_size_change_changes_hash(self, tmp_path: Path):
        base = _tree(tmp_path, {"a.csv": "xy"})
        before = build_manifest(base).digest
        (base / "a.csv").write_text("x")  # truncated
        assert build_manifest(base).digest != before

    def test_added_and_removed_files_change_hash(self, tmp_path: Path):
        base = _tree(tmp_path, {"a.csv": "xy"})
        before = build_manifest(base).digest
        (base / "b.csv").write_text("q")
        after_add = build_manifest(base).digest
        assert after_add != before
        (base / "b.csv").unlink()
        assert build_manifest(base).digest == before

    def test_rename_changes_hash(self, tmp_path: Path):
        base = _tree(tmp_path, {"a.csv": "xy"})
        before = build_manifest(base).digest
        (base / "a.csv").rename(base / "renamed.csv")
        assert build_manifest(base).digest != before

    def test_mtime_does_not_change_hash(self, tmp_path: Path):
        base = _tree(tmp_path, {"a.csv": "xy"})
        before = build_manifest(base).digest
        os.utime(base / "a.csv", (0, 0))
        assert build_manifest(base).digest == before

    def test_empty_directories_do_not_change_hash(self, tmp_path: Path):
        base = _tree(tmp_path, {"a.csv": "xy"})
        before = build_manifest(base).digest
        (base / "empty").mkdir()
        assert build_manifest(base).digest == before

    def test_missing_path_raises(self, tmp_path: Path):
        with pytest.raises(DatasetError, match="does not exist"):
            build_manifest(tmp_path / "nope")

    def test_file_instead_of_directory_raises(self, tmp_path: Path):
        target = tmp_path / "a.csv"
        target.write_text("x")
        with pytest.raises(DatasetError, match="not a directory"):
            build_manifest(target)

    def test_empty_dataset_raises(self, tmp_path: Path):
        with pytest.raises(DatasetError, match="empty"):
            build_manifest(_tree(tmp_path, {}))

    def test_symlink_escaping_the_tree_raises(self, tmp_path: Path):
        outside = tmp_path / "outside.csv"
        outside.write_text("secret")
        base = _tree(tmp_path / "data", {"a.csv": "xy"})
        (base / "link.csv").symlink_to(outside)
        with pytest.raises(DatasetError, match="outside"):
            build_manifest(base)

    def test_broken_symlink_raises(self, tmp_path: Path):
        base = _tree(tmp_path / "data", {"a.csv": "xy"})
        (base / "link.csv").symlink_to(tmp_path / "gone.csv")
        with pytest.raises(DatasetError, match="broken symlink"):
            build_manifest(base)

    def test_directory_symlink_raises(self, tmp_path: Path):
        base = _tree(tmp_path / "data", {"real/a.csv": "xy"})
        (base / "alias").symlink_to(base / "real")
        with pytest.raises(DatasetError, match="directory symlink"):
            build_manifest(base)

    def test_render_matches_remote_listing_round_trip(self, tmp_path: Path):
        m = build_manifest(_tree(tmp_path, {"a.csv": "xy", "sub/b.csv": "zzz"}))
        # What `find -printf '%P\t%s\n' | LC_ALL=C sort` would emit.
        assert parse_remote_listing(m.render()) == m.entries


# ---------- staged-tree verification -------------------------------------


class TestDiffManifests:
    def test_identical_trees_have_no_differences(self, tmp_path: Path):
        m = build_manifest(_tree(tmp_path, {"a.csv": "xy"}))
        assert diff_manifests(m, m.entries) == []

    def test_short_file_is_reported(self, tmp_path: Path):
        m = build_manifest(_tree(tmp_path, {"a.csv": "xy"}))
        problems = diff_manifests(m, (("a.csv", 1),))
        assert problems == ["size mismatch: a.csv (expected 2, got 1)"]

    def test_missing_and_extra_files_are_reported(self, tmp_path: Path):
        m = build_manifest(_tree(tmp_path, {"a.csv": "xy"}))
        problems = diff_manifests(m, (("b.csv", 2),))
        assert "missing: a.csv" in problems
        assert "unexpected: b.csv" in problems

    def test_long_diffs_are_truncated(self, tmp_path: Path):
        files = {f"f{i}.csv": "x" for i in range(30)}
        m = build_manifest(_tree(tmp_path, files))
        problems = diff_manifests(m, ())
        assert len(problems) == 11
        assert problems[-1].startswith("... and 20 more")

    def test_unparseable_listing_raises(self):
        with pytest.raises(DatasetError, match="Unparseable"):
            parse_remote_listing("no-tab-here\n")


# ---------- layout and env helpers ---------------------------------------


class TestLayout:
    def test_dataset_path_shape(self):
        assert (
            dataset_path("/scratch/w", "acq", "9f2c1ab30d44")
            == "/scratch/w/acq/9f2c1ab30d44"
        )

    def test_dataset_path_tolerates_trailing_slash(self):
        assert dataset_path("/scratch/w/", "acq", "abc") == "/scratch/w/acq/abc"

    def test_staging_path_is_per_run_and_beside_dest(self):
        dest = "/scratch/w/acq/abc"
        staged = staging_path(dest, "run123")
        assert staged.startswith(dest)
        assert staged != dest
        assert "run123" in staged
        # The glob used to clear leftovers must match it.
        assert staged.startswith(staging_glob(dest).rstrip("*"))

    def test_env_var_naming(self):
        assert data_env_var("acquihired") == "DATA_ACQUIHIRED"
        assert data_env_var("acq-raw") == "DATA_ACQ_RAW"

    def test_env_var_avoids_the_reserved_scripthut_namespace(self):
        # The env resolver drops any rule setting a SCRIPTHUT_* key, and the
        # cache strips that prefix from the key; either would break these.
        assert not data_env_var("acq").startswith("SCRIPTHUT_")
        assert DATA_DIR_VAR == "DATA_DIR"


class TestValidateRemoteRoot:
    def test_accepts_absolute_path(self):
        assert validate_remote_root("/scratch/wiemann/", origin="config") == (
            "/scratch/wiemann"
        )

    def test_accepts_tilde_relative_path(self):
        # Expanded against the probed home later; the default dataset_dir is
        # ~/scripthut-data, so this form has to survive validation.
        assert validate_remote_root("~/scripthut-data", origin="config") == (
            "~/scripthut-data"
        )

    @pytest.mark.parametrize(
        "bad",
        ["relative/path", "/scratch/$USER", "/scratch/a b", "~scratch", "~", ""],
    )
    def test_rejects_unsafe_roots(self, bad: str):
        with pytest.raises(DatasetError):
            validate_remote_root(bad, origin="config")

    def test_error_names_the_origin(self):
        with pytest.raises(DatasetError, match="backend env"):
            validate_remote_root("/scratch/$USER", origin="backend env")


# ---------- workflow document parsing ------------------------------------


class TestParseDataDeps:
    def test_object_form(self):
        assert parse_data_deps({"data": [{"name": "acq"}], "tasks": []}) == ["acq"]

    def test_string_shorthand(self):
        assert parse_data_deps({"data": ["acq"], "tasks": []}) == ["acq"]

    def test_bare_list_document_has_no_data(self):
        assert parse_data_deps([{"id": "a"}]) == []

    def test_absent_key_is_empty(self):
        assert parse_data_deps({"tasks": []}) == []

    def test_missing_name_raises(self):
        with pytest.raises(ValueError, match="non-empty 'name'"):
            parse_data_deps({"data": [{}], "tasks": []})

    def test_duplicate_name_raises(self):
        with pytest.raises(ValueError, match="twice"):
            parse_data_deps({"data": ["acq", "acq"], "tasks": []})

    def test_declaring_a_destination_raises(self):
        with pytest.raises(ValueError, match="derived"):
            parse_data_deps({"data": [{"name": "acq", "dest": "/tmp/x"}]})

    def test_non_list_raises(self):
        with pytest.raises(ValueError, match="must be a list"):
            parse_data_deps({"data": {"name": "acq"}})


class TestTaskDataDep:
    def test_workflow_json_cannot_forge_a_staging_item(self):
        task = TaskDefinition.from_dict(
            {
                "id": "t",
                "name": "t",
                "command": "echo hi",
                "data_dep": {
                    "name": "evil",
                    "local_path": "/etc",
                    "dest": "/scratch/x",
                    "hash": "0" * 12,
                },
            }
        )
        assert task.data_dep is None

    def test_data_dep_round_trips_through_storage(self):
        dep = DataDep(
            name="acq", local_path="/data/acq", dest="/scratch/acq/abc", hash="abc",
        )
        task = TaskDefinition(id="_data.acq", name="stage acq", command=":")
        task.data_dep = dep
        assert DataDep.from_dict(task.to_dict()["data_dep"]) == dep


# ---------- configuration -------------------------------------------------


class TestDatasetConfig:
    def test_get_dataset(self):
        cfg = ScriptHutConfig.model_validate(
            {"datasets": [{"name": "acq", "path": "/data/acq"}]}
        )
        assert cfg.get_dataset("acq") is not None
        assert cfg.get_dataset("nope") is None

    def test_default_timeout_is_generous(self):
        cfg = ScriptHutConfig.model_validate(
            {"datasets": [{"name": "acq", "path": "/data/acq"}]}
        )
        assert cfg.datasets[0].timeout == 86400

    def test_duplicate_names_rejected(self):
        with pytest.raises(ValueError, match="Duplicate dataset name"):
            ScriptHutConfig.model_validate(
                {
                    "datasets": [
                        {"name": "acq", "path": "/one"},
                        {"name": "acq", "path": "/two"},
                    ]
                }
            )

    @pytest.mark.parametrize("bad", ["1leading-digit", "has space", "-dash", ""])
    def test_invalid_names_rejected(self, bad: str):
        with pytest.raises(ValueError):
            ScriptHutConfig.model_validate(
                {"datasets": [{"name": bad, "path": "/data"}]}
            )

    def test_project_local_datasets_rejected(self, tmp_path: Path):
        with pytest.raises(ConfigError, match="datasets"):
            _validate_project_local_yaml(
                {"datasets": [{"name": "acq", "path": "/data"}]},
                tmp_path / "scripthut.yaml",
            )


class TestDatasetPathResolution:
    def test_relative_path_resolves_against_config_file_not_cwd(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        cfg_dir = tmp_path / "conf"
        cfg_dir.mkdir()
        (cfg_dir / "scripthut.yaml").write_text(
            "datasets:\n  - name: acq\n    path: ./raw\n"
        )
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        monkeypatch.chdir(elsewhere)

        cfg = load_yaml_config(cfg_dir / "scripthut.yaml")
        assert cfg.datasets[0].path == (cfg_dir / "raw").resolve()

    def test_tilde_expands(self, tmp_path: Path):
        (tmp_path / "scripthut.yaml").write_text(
            "datasets:\n  - name: acq\n    path: ~/acq-raw\n"
        )
        cfg = load_yaml_config(tmp_path / "scripthut.yaml")
        assert cfg.datasets[0].path == Path.home() / "acq-raw"

    def test_absolute_path_is_untouched(self, tmp_path: Path):
        (tmp_path / "scripthut.yaml").write_text(
            "datasets:\n  - name: acq\n    path: /data/acq\n"
        )
        cfg = load_yaml_config(tmp_path / "scripthut.yaml")
        assert cfg.datasets[0].path == Path("/data/acq")
