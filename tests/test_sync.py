"""Tests for sync-source file list and local workflow discovery."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from pydantic import ValidationError

from scripthut.config_schema import (
    LocalBackendConfig,
    ScriptHutConfig,
    SyncSourceConfig,
)
from scripthut.runs.models import TaskDefinition
from scripthut.runs.sync import (
    SYNC_RETURN_ID,
    SYNC_UPLOAD_ID,
    SyncError,
    apply_return,
    apply_upload,
    discover_workflows,
    list_upload_paths,
    resolve_dest,
)


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.email=t@e.st", "-c", "user.name=t", *args],
        cwd=repo, check=True, capture_output=True,
    )


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    return repo


class TestListUploadPaths:
    def test_tracked_and_dirty_included_untracked_and_ignored_not(self, tmp_path):
        repo = _repo(tmp_path)
        (repo / "keep.py").write_text("v1")
        (repo / ".gitignore").write_text("skip.dta\n")
        (repo / "skip.dta").write_text("data")
        (repo / "untracked.py").write_text("nope")
        _git(repo, "add", "keep.py", ".gitignore")
        (repo / "keep.py").write_text("v2-dirty")

        paths = list_upload_paths(repo)
        assert paths == [".gitignore", "keep.py"]

    def test_drops_tracked_return_dir(self, tmp_path):
        repo = _repo(tmp_path)
        (repo / "code.do").write_text("do")
        out = repo / "output"
        out.mkdir()
        (out / "table.csv").write_text("1")
        _git(repo, "add", "-A")

        assert list_upload_paths(repo) == ["code.do"]
        assert list_upload_paths(repo, return_dir="output") == ["code.do"]

    def test_rejects_not_a_git_repo(self, tmp_path):
        d = tmp_path / "plain"
        d.mkdir()
        (d / "a.py").write_text("x")
        with pytest.raises(SyncError, match="not a git repository"):
            list_upload_paths(d)

    def test_rejects_tracked_symlink(self, tmp_path):
        repo = _repo(tmp_path)
        (repo / "real.py").write_text("x")
        (repo / "link.py").symlink_to("real.py")
        _git(repo, "add", "-A")
        with pytest.raises(SyncError, match="symlink"):
            list_upload_paths(repo)


class TestDiscoverWorkflows:
    def test_finds_uncommitted_json(self, tmp_path):
        root = tmp_path / "proj"
        wf = root / ".hut" / "workflows"
        wf.mkdir(parents=True)
        (wf / "train.json").write_text(json.dumps({"title": "T", "tasks": []}))
        found = discover_workflows(root, source_name="wl")
        assert len(found) == 1
        assert found[0].name == "wl/train"
        assert found[0].filename == "train.json"
        assert found[0].title == "T"

    def test_skips_invalid_json(self, tmp_path):
        root = tmp_path / "proj"
        wf = root / ".hut" / "workflows"
        wf.mkdir(parents=True)
        (wf / "bad.json").write_text("{")
        (wf / "ok.json").write_text("{}")
        found = discover_workflows(root, source_name="s")
        assert [f.filename for f in found] == ["ok.json"]


class TestSyncSourceSchema:
    def test_loads_with_return_alias(self):
        cfg = ScriptHutConfig.model_validate({
            "sources": [{
                "name": "wl",
                "type": "sync",
                "path": "/tmp/wl",
                "backend": "mercury",
                "return": "results",
            }],
        })
        src = cfg.sources[0]
        assert isinstance(src, SyncSourceConfig)
        assert src.return_dir == "results"
        assert src.backend == "mercury"

    def test_missing_backend_fails(self):
        with pytest.raises(ValidationError):
            ScriptHutConfig.model_validate({
                "sources": [{
                    "name": "wl",
                    "type": "sync",
                    "path": "/tmp/wl",
                }],
            })


class TestResolveDest:
    def test_default_under_sync_dir(self):
        src = SyncSourceConfig(name="wl", path="/tmp/wl", backend="local")
        be = LocalBackendConfig(name="local")
        dest = resolve_dest(src, backend_name="local", backend_cfg=be, home="/home/u")
        assert dest == "/home/u/scripthut-sync/wl"

    def test_explicit_dest(self):
        src = SyncSourceConfig(
            name="wl", path="/tmp/wl", backend="local", dest="/scratch/wl",
        )
        be = LocalBackendConfig(name="local")
        dest = resolve_dest(src, backend_name="local", backend_cfg=be, home="/home/u")
        assert dest == "/scratch/wl"

    def test_rejects_dest_under_clone_dir(self):
        src = SyncSourceConfig(
            name="wl", path="/tmp/wl", backend="local",
            dest="/home/u/scripthut-repos/wl",
        )
        be = LocalBackendConfig(name="local")
        with pytest.raises(SyncError, match="clone directory"):
            resolve_dest(src, backend_name="local", backend_cfg=be, home="/home/u")

    def test_rejects_dest_under_dataset_dir(self):
        src = SyncSourceConfig(
            name="wl", path="/tmp/wl", backend="local",
            dest="/home/u/scripthut-data/wl",
        )
        be = LocalBackendConfig(name="local")
        with pytest.raises(SyncError, match="dataset directory"):
            resolve_dest(src, backend_name="local", backend_cfg=be, home="/home/u")


class TestApplyUpload:
    def test_prepends_item_and_roots_wait(self):
        tasks = [
            TaskDefinition(id="a", name="a", command="true"),
            TaskDefinition(id="b", name="b", command="true", dependencies=["a"]),
        ]
        out = apply_upload(
            tasks, local_path=Path("/tmp/wl"), dest="/scratch/wl", return_dir="output",
        )
        assert out[0].id == SYNC_UPLOAD_ID
        assert out[0].sync_dep is not None
        assert out[0].sync_dep.kind == "upload"
        assert out[0].sync_dep.dest == "/scratch/wl"
        assert out[1].dependencies == [SYNC_UPLOAD_ID]
        assert out[2].dependencies == ["a"]

    def test_apply_return_appends_with_no_deps(self):
        tasks = [
            TaskDefinition(id="a", name="a", command="true"),
        ]
        out = apply_upload(
            tasks, local_path=Path("/tmp/wl"), dest="/scratch/wl", return_dir="output",
        )
        out = apply_return(
            out, local_path=Path("/tmp/wl"), dest="/scratch/wl", return_dir="output",
        )
        assert out[-1].id == SYNC_RETURN_ID
        assert out[-1].sync_dep is not None
        assert out[-1].sync_dep.kind == "return"
        assert out[-1].dependencies == []
        assert out[0].id == SYNC_UPLOAD_ID

    def test_reserves_prefix(self):
        tasks = [TaskDefinition(id="_sync.upload", name="x", command="true")]
        with pytest.raises(ValueError, match="_sync"):
            apply_upload(
                tasks, local_path=Path("/tmp/wl"), dest="/d", return_dir="output",
            )


class TestReturnDirValidation:
    """``return`` is normalized at the schema boundary, not at each consumer."""

    @pytest.mark.parametrize(
        "raw,expected",
        [("output", "output"), ("/output/", "output"), ("results/final", "results/final")],
    )
    def test_normalized(self, raw: str, expected: str):
        src = SyncSourceConfig.model_validate({
            "name": "wl", "type": "sync", "path": "/tmp/wl",
            "backend": "local", "return": raw,
        })
        assert src.return_dir == expected

    @pytest.mark.parametrize("raw", ["..", "../shared", "output/../..", "", "/", "."])
    def test_escaping_or_empty_refused(self, raw: str):
        with pytest.raises(ValidationError, match="return dir"):
            SyncSourceConfig.model_validate({
                "name": "wl", "type": "sync", "path": "/tmp/wl",
                "backend": "local", "return": raw,
            })


class TestDataDepsLeaveSyncItemsAlone:
    """``_apply_data_deps`` must not rewire the sync items' dependencies.

    ``_sync.upload`` is independent of dataset staging, and ``_sync.return``
    is gated by ``_sync_return_ready`` rather than ``dependencies`` so that it
    still pulls after a failure. A dataset dependency on either would break
    both properties — the return item would cascade to DEP_FAILED and the
    output would never come back.
    """

    def _plan(self, tmp_path: Path):
        from scripthut.runs.datasets import DatasetPlan, build_manifest

        d = tmp_path / "ds"
        d.mkdir()
        (d / "a.txt").write_text("x")
        return DatasetPlan(
            name="raw",
            local_path=d,
            manifest=build_manifest(d),
            dest="/scratch/data/raw/abc123abc123",
            reused=False,
        )

    def test_sync_items_keep_empty_dependencies(self, tmp_path: Path):
        from scripthut.runs.manager import RunManager

        tasks = [TaskDefinition(id="a", name="a", command="true")]
        tasks = apply_upload(
            tasks, local_path=Path("/tmp/wl"), dest="/scratch/wl", return_dir="output",
        )
        tasks = apply_return(
            tasks, local_path=Path("/tmp/wl"), dest="/scratch/wl", return_dir="output",
        )
        out, _env = RunManager._apply_data_deps([self._plan(tmp_path)], tasks, [])

        by_id = {t.id: t for t in out}
        assert by_id[SYNC_UPLOAD_ID].dependencies == []
        assert by_id[SYNC_RETURN_ID].dependencies == []
        # The real root task still waits on the upload, which waits on nothing.
        assert by_id["a"].dependencies == [SYNC_UPLOAD_ID]
        # And the staging item was still injected for the user task to inherit.
        assert "_data.raw" in by_id
