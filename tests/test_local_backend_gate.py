"""Platform gate for the local backend (runs on every platform).

The local backend is POSIX-only: its task scripts, exit-code supervisor,
and cancel path all assume a POSIX shell, which ``cmd.exe`` silently
mangles (jobs launch but never record a verdict). These tests pin the
runtime behavior on unsupported hosts — skip with a warning rather than
register an executor whose every job would hang — by driving the gate
directly, so they exercise the Windows branch even on POSIX CI.
"""

from __future__ import annotations

import pytest

import scripthut.runtime as runtime_mod
from scripthut.config_schema import (
    GlobalSettings,
    LocalBackendConfig,
    ScriptHutConfig,
)


class TestLocalBackendPlatformGate:
    @pytest.mark.asyncio
    async def test_unsupported_platform_skips_explicit_local_backend(
        self, tmp_path, monkeypatch, caplog,
    ):
        monkeypatch.setattr(
            runtime_mod, "local_backend_supported", lambda: False,
        )
        cfg = ScriptHutConfig(
            backends=[LocalBackendConfig(name="laptop")],
            settings=GlobalSettings(data_dir=str(tmp_path / "data")),
        )
        runtime = await runtime_mod.init_runtime(cfg, restore_runs=False)
        try:
            assert "laptop" not in runtime.backends
            assert "not" in " ".join(
                r.message for r in caplog.records if "laptop" in r.message
            )
        finally:
            await runtime_mod.shutdown_runtime(runtime)

    @pytest.mark.asyncio
    async def test_unsupported_platform_skips_auto_registration(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setattr(
            runtime_mod, "local_backend_supported", lambda: False,
        )
        cfg = ScriptHutConfig(
            settings=GlobalSettings(data_dir=str(tmp_path / "data")),
        )
        runtime = await runtime_mod.init_runtime(cfg, restore_runs=False)
        try:
            assert runtime.backends == {}
            # The auto-registered config entry must not appear either —
            # otherwise `backend list` would advertise a backend that
            # can never run anything.
            assert cfg.get_backend("local") is None
        finally:
            await runtime_mod.shutdown_runtime(runtime)

    def test_supported_matches_platform(self):
        """On the machines that run this suite the gate must agree with
        the OS: True on POSIX, False on Windows."""
        import os

        from scripthut.backends.local import local_backend_supported

        assert local_backend_supported() == (os.name != "nt")
