"""Tests for the cluster-facing half of dataset staging.

Covers destination resolution (config only -- the dataset's own root, else
the backend's ``dataset_dir``), the presence probe, and ``stage_dataset``'s
verify-then-publish sequence. All against a scripted SSH mock; no live
cluster.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from scripthut.config_schema import DatasetConfig
from scripthut.runs.datasets import (
    BackendPaths,
    DatasetError,
    DatasetPlan,
    build_manifest,
    probe_backend_paths,
    probe_presence,
    resolve_root,
    stage_dataset,
)


class _ScriptedSSH:
    """SSH mock that returns pre-canned responses and records every command."""

    def __init__(self, responses: list[tuple[str, str, int]] | None = None):
        self.responses = list(responses or [])
        self.commands: list[str] = []
        self.transfers: list[tuple[Path, str, int]] = []
        self.put_error: Exception | None = None
        self.run_command = self._run_command

    async def _run_command(self, cmd: str, timeout: int = 30):
        self.commands.append(cmd)
        if self.responses:
            return self.responses.pop(0)
        return ("", "", 0)

    async def put_tree(self, local_path, remote_path, *, timeout=86400, progress=None):
        self.transfers.append((local_path, remote_path, timeout))
        if self.put_error is not None:
            raise self.put_error
        return 4096


def _scripted(responses: list[tuple[str, str, int]] | None = None) -> Any:
    """Typed Any so the duck-typed stand-in satisfies SSHClient parameters."""
    return _ScriptedSSH(responses)


def _dataset(**kw) -> DatasetConfig:
    base = {"name": "acq", "path": Path("/data/acq")}
    return DatasetConfig(**{**base, **kw})


# ---------- probing -------------------------------------------------------


class TestProbeBackendPaths:
    @pytest.mark.asyncio
    async def test_parses_home(self):
        ssh = _scripted([("HOME\t/home/w\n", "", 0)])
        assert await probe_backend_paths(ssh) == BackendPaths(home="/home/w")

    @pytest.mark.asyncio
    async def test_does_not_need_a_login_shell(self):
        # The root comes from config now, so nothing here depends on profile
        # scripts -- a plain exec channel keeps the probe cheap.
        ssh = _scripted([("HOME\t/home/w\n", "", 0)])
        await probe_backend_paths(ssh)
        assert "bash -lc" not in ssh.commands[0]

    @pytest.mark.asyncio
    async def test_failed_probe_yields_nothing_rather_than_guessing(self):
        ssh = _scripted([("", "boom", 1)])
        assert await probe_backend_paths(ssh) == BackendPaths()


# ---------- root resolution ----------------------------------------------


class _Backend:
    def __init__(self, dataset_dir: str | None = None, clone_dir: str | None = None):
        if dataset_dir is not None:
            self.dataset_dir = dataset_dir
        if clone_dir is not None:
            self.clone_dir = clone_dir


class TestResolveRoot:
    def test_dataset_root_wins_over_the_backend(self):
        root = resolve_root(
            _dataset(root="/scratch/explicit"),
            backend_name="mercury",
            backend_cfg=_Backend(dataset_dir="/scratch/backend"),
            paths=BackendPaths(home="/home/w"),
        )
        assert root == "/scratch/explicit"

    def test_backend_dataset_dir_is_used_when_the_dataset_is_silent(self):
        root = resolve_root(
            _dataset(),
            backend_name="mercury",
            backend_cfg=_Backend(dataset_dir="/scratch/backend"),
            paths=BackendPaths(home="/home/w"),
        )
        assert root == "/scratch/backend"

    def test_default_is_a_home_subdirectory(self):
        # Mirrors clone_dir's ~/scripthut-repos: always an answer, never a
        # failure, and never the home directory itself.
        root = resolve_root(
            _dataset(),
            backend_name="mercury",
            backend_cfg=_Backend(),
            paths=BackendPaths(home="/home/w"),
        )
        assert root == "/home/w/scripthut-data"

    def test_tilde_is_expanded_so_the_destination_is_absolute(self):
        # DATA_DIR is exported into the job; export DATA_DIR="~/x" would not
        # expand the tilde, so it has to be resolved here.
        root = resolve_root(
            _dataset(root="~/mydata"),
            backend_name="mercury",
            backend_cfg=_Backend(),
            paths=BackendPaths(home="/home/w"),
        )
        assert root == "/home/w/mydata"

    def test_tilde_without_a_known_home_is_an_error_not_a_guess(self):
        with pytest.raises(DatasetError, match="could not"):
            resolve_root(
                _dataset(root="~/mydata"),
                backend_name="mercury",
                backend_cfg=_Backend(),
                paths=BackendPaths(),
            )

    def test_home_subdirectory_is_allowed(self):
        root = resolve_root(
            _dataset(root="/home/w/data"),
            backend_name="mercury",
            backend_cfg=_Backend(),
            paths=BackendPaths(home="/home/w"),
        )
        assert root == "/home/w/data"

    def test_the_home_directory_itself_is_rejected(self):
        with pytest.raises(DatasetError, match="home directory itself"):
            resolve_root(
                _dataset(root="/home/w"),
                backend_name="mercury",
                backend_cfg=_Backend(),
                paths=BackendPaths(home="/home/w"),
            )

    def test_root_under_clone_dir_is_rejected(self):
        with pytest.raises(DatasetError, match="clone directory"):
            resolve_root(
                _dataset(root="/scratch/w/clones/data"),
                backend_name="mercury",
                backend_cfg=_Backend(),
                paths=BackendPaths(home="/home/w"),
                clone_dirs=["/scratch/w/clones"],
            )

    def test_clone_dir_guard_matters_most_under_home(self):
        # ~/scripthut-repos and ~/scripthut-data are siblings; staging into
        # the former would drop data on top of checked-out code.
        with pytest.raises(DatasetError, match="clone directory"):
            resolve_root(
                _dataset(),
                backend_name="mercury",
                backend_cfg=_Backend(dataset_dir="~/scripthut-repos/data"),
                paths=BackendPaths(home="/home/w"),
                clone_dirs=["~/scripthut-repos"],
            )

    def test_clone_dir_guard_applies_even_when_home_is_unknown(self):
        with pytest.raises(DatasetError, match="clone directory"):
            resolve_root(
                _dataset(root="/scratch/w/clones"),
                backend_name="mercury",
                backend_cfg=_Backend(),
                paths=BackendPaths(),
                clone_dirs=["/scratch/w/clones"],
            )

    def test_shell_metacharacter_in_root_is_rejected(self):
        with pytest.raises(DatasetError, match="not usable"):
            resolve_root(
                _dataset(root="/scratch/$USER"),
                backend_name="mercury",
                backend_cfg=_Backend(),
                paths=BackendPaths(home="/home/w"),
            )

    def test_sibling_prefix_is_not_treated_as_a_parent(self):
        # /home/wiemann-data is not inside /home/wiemann.
        root = resolve_root(
            _dataset(root="/home/wiemann-data"),
            backend_name="mercury",
            backend_cfg=_Backend(),
            paths=BackendPaths(home="/home/wiemann"),
        )
        assert root == "/home/wiemann-data"


# ---------- presence ------------------------------------------------------


class TestProbePresence:
    @pytest.mark.asyncio
    async def test_present_with_siblings(self):
        ssh = _scripted([(
            "__SCRIPTHUT_PRESENT__\n__SCRIPTHUT_SIBLINGS__\n"
            "aaaaaaaaaaaa\nbbbbbbbbbbbb\n",
            "", 0,
        )])
        present, siblings = await probe_presence(
            ssh, "/scratch/w/acq/aaaaaaaaaaaa", hash12="aaaaaaaaaaaa",
        )
        assert present is True
        assert siblings == ("bbbbbbbbbbbb",)

    @pytest.mark.asyncio
    async def test_absent_with_no_parent(self):
        ssh = _scripted([("__SCRIPTHUT_SIBLINGS__\n", "", 0)])
        present, siblings = await probe_presence(
            ssh, "/scratch/w/acq/aaaaaaaaaaaa", hash12="aaaaaaaaaaaa",
        )
        assert present is False
        assert siblings == ()

    @pytest.mark.asyncio
    async def test_non_hash_entries_are_not_reported_as_siblings(self):
        ssh = _scripted([(
            "__SCRIPTHUT_SIBLINGS__\n"
            "aaaaaaaaaaaa.staging-run1\nREADME\ncccccccccccc\n",
            "", 0,
        )])
        _, siblings = await probe_presence(
            ssh, "/scratch/w/acq/aaaaaaaaaaaa", hash12="aaaaaaaaaaaa",
        )
        assert siblings == ("cccccccccccc",)

    @pytest.mark.asyncio
    async def test_probe_failure_raises_rather_than_assuming_absent(self):
        ssh = _scripted([("", "Permission denied", 1)])
        with pytest.raises(DatasetError, match="Permission denied"):
            await probe_presence(
                ssh, "/scratch/w/acq/aaaaaaaaaaaa", hash12="aaaaaaaaaaaa",
            )


# ---------- transfer ------------------------------------------------------


def _plan(tmp_path: Path, files: dict[str, str] | None = None) -> DatasetPlan:
    local = tmp_path / "acq"
    local.mkdir(parents=True, exist_ok=True)
    for rel, body in (files or {"a.csv": "xy"}).items():
        p = local / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(body)
    manifest = build_manifest(local)
    return DatasetPlan(
        name="acq",
        local_path=local,
        manifest=manifest,
        dest=f"/scratch/w/acq/{manifest.short}",
        reused=False,
    )


class TestStageDataset:
    @pytest.mark.asyncio
    async def test_happy_path_publishes_after_verifying(self, tmp_path: Path):
        plan = _plan(tmp_path)
        ssh = _scripted([
            ("", "", 0),                      # prepare
            (plan.manifest.render(), "", 0),  # verification listing
            ("", "", 0),                      # finalize
        ])

        copied = await stage_dataset(ssh, plan, run_id="run1")

        assert copied == 4096
        staging = f"{plan.dest}.staging-run1"
        assert ssh.transfers == [(plan.local_path, staging, 86400)]
        assert f"mv \"{staging}\" \"{plan.dest}\"" in ssh.commands[-1]

    @pytest.mark.asyncio
    async def test_clears_stale_staging_and_creates_the_parent(self, tmp_path: Path):
        plan = _plan(tmp_path)
        ssh = _scripted([
            ("", "", 0), (plan.manifest.render(), "", 0), ("", "", 0),
        ])

        await stage_dataset(ssh, plan, run_id="run1")

        prepare = ssh.commands[0]
        assert f'rm -rf "{plan.dest}".staging-*' in prepare
        assert 'mkdir -p "/scratch/w/acq"' in prepare

    @pytest.mark.asyncio
    async def test_short_file_blocks_the_publish(self, tmp_path: Path):
        plan = _plan(tmp_path)
        ssh = _scripted([
            ("", "", 0),
            ("a.csv\t1\n", "", 0),  # truncated on the far side
        ])

        with pytest.raises(DatasetError, match="size mismatch: a.csv"):
            await stage_dataset(ssh, plan, run_id="run1")

        assert not any("mv " in c for c in ssh.commands)

    @pytest.mark.asyncio
    async def test_missing_file_blocks_the_publish(self, tmp_path: Path):
        plan = _plan(tmp_path, {"a.csv": "xy", "b.csv": "zzz"})
        ssh = _scripted([("", "", 0), ("a.csv\t2\n", "", 0)])

        with pytest.raises(DatasetError, match="missing: b.csv"):
            await stage_dataset(ssh, plan, run_id="run1")

        assert not any("mv " in c for c in ssh.commands)

    @pytest.mark.asyncio
    async def test_bsd_style_listing_paths_are_normalized(self, tmp_path: Path):
        plan = _plan(tmp_path)
        ssh = _scripted([
            ("", "", 0),
            ("./a.csv\t2\n", "", 0),  # what `stat -f %N` emits
            ("", "", 0),
        ])

        assert await stage_dataset(ssh, plan, run_id="run1") == 4096

    @pytest.mark.asyncio
    async def test_losing_the_race_discards_the_copy_instead_of_nesting(
        self, tmp_path: Path
    ):
        plan = _plan(tmp_path)
        ssh = _scripted([
            ("", "", 0),
            (plan.manifest.render(), "", 0),
            ("RACED\n", "", 0),  # another run published first
        ])

        assert await stage_dataset(ssh, plan, run_id="run1") == 0
        finalize = ssh.commands[-1]
        assert f'if [ -d "{plan.dest}" ]' in finalize
        assert "rm -rf" in finalize

    @pytest.mark.asyncio
    async def test_transfer_failure_propagates_and_never_publishes(
        self, tmp_path: Path
    ):
        plan = _plan(tmp_path)
        ssh = _scripted([("", "", 0)])
        ssh.put_error = RuntimeError("Transfer to ... timed out after 86400s")

        with pytest.raises(RuntimeError, match="timed out"):
            await stage_dataset(ssh, plan, run_id="run1")

        assert not any("mv " in c for c in ssh.commands)

    @pytest.mark.asyncio
    async def test_dataset_timeout_is_passed_to_the_transfer(self, tmp_path: Path):
        base = _plan(tmp_path)
        plan = DatasetPlan(
            name=base.name,
            local_path=base.local_path,
            manifest=base.manifest,
            dest=base.dest,
            reused=False,
            timeout=120,
        )
        ssh = _scripted([
            ("", "", 0), (plan.manifest.render(), "", 0), ("", "", 0),
        ])

        await stage_dataset(ssh, plan, run_id="run1")

        assert ssh.transfers[0][2] == 120
