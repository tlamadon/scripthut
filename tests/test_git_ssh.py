"""The ssh options scripthut hands to git, local and remote.

These exist because `StrictHostKeyChecking=accept-new` was hardcoded on
both paths. It is an OpenSSH 7.6+ spelling, and older clients do not
ignore it — they abort with `unsupported option "accept-new"` before
connecting, so every clone on a RHEL/CentOS 7 login node (OpenSSH 7.4)
died with nothing but `fatal: Could not read from remote repository`.
Nothing tested the flags at all.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripthut.runs.manager import RunManager
from scripthut.sources.git import GitSourceManager
from scripthut.ssh.git_ssh import (
    build_git_ssh_command,
    host_key_checking_option,
    parse_openssh_version,
)


@pytest.mark.parametrize(
    ("banner", "expected"),
    [
        ("OpenSSH_9.6p1, LibreSSL 3.3.6", (9, 6)),
        ("OpenSSH_7.4p1, OpenSSL 1.0.2k-fips  26 Jan 2017", (7, 4)),
        ("OpenSSH_7.6p1 Ubuntu-4ubuntu0.3, OpenSSL 1.0.2n", (7, 6)),
        ("OpenSSH_10.0p1, OpenSSL 3.0.13", (10, 0)),
        ("dropbear 2022.83", None),
        ("", None),
    ],
)
def test_parse_openssh_version(banner: str, expected: tuple[int, int] | None):
    assert parse_openssh_version(banner) == expected


def test_openssh_10_is_not_read_as_older_than_7():
    """Guards against a string compare sneaking back in: "10" < "7"."""
    assert host_key_checking_option(parse_openssh_version("OpenSSH_10.0p1")) == "accept-new"


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ((7, 6), "accept-new"),  # the version that introduced it
        ((9, 6), "accept-new"),
        ((7, 5), "no"),  # one release too old
        ((7, 4), "no"),  # RHEL/CentOS 7 — the case that broke
        ((6, 6), "no"),
        (None, "no"),  # unreadable banner: assume the worst
    ],
)
def test_host_key_checking_option(version: tuple[int, int] | None, expected: str):
    assert host_key_checking_option(version) == expected


@pytest.mark.parametrize("version", [(7, 4), (6, 6), None])
def test_old_ssh_never_sees_accept_new(version: tuple[int, int] | None):
    """The regression itself: the option that aborts must not be emitted."""
    assert "accept-new" not in build_git_ssh_command(openssh_version=version)


def test_command_is_non_interactive():
    """A clone runs unattended; nothing may stop to ask for input."""
    cmd = build_git_ssh_command(openssh_version=(9, 6))
    assert "-o BatchMode=yes" in cmd
    assert "-o PasswordAuthentication=no" in cmd


def test_deploy_key_is_used_exclusively():
    """Without IdentitiesOnly, ssh offers every agent key and can hit
    the server's MaxAuthTries before reaching the one we passed."""
    cmd = build_git_ssh_command(key_path="/tmp/key", openssh_version=(9, 6))
    assert "-i /tmp/key" in cmd
    assert "-o IdentitiesOnly=yes" in cmd


def test_no_key_means_no_identity_flags():
    cmd = build_git_ssh_command(openssh_version=(9, 6))
    assert " -i " not in cmd
    assert "IdentitiesOnly" not in cmd


def test_paths_are_quoted_by_default():
    cmd = build_git_ssh_command(
        key_path="/has space/key", known_hosts="/has space/kh", openssh_version=(9, 6)
    )
    assert "'/has space/key'" in cmd
    assert "UserKnownHostsFile='/has space/kh'" in cmd


def test_tilde_survives_when_quoting_is_off():
    """A remote clone_dir is still `~/...`; quoting it would make the
    shell create a directory named `~` instead of expanding it."""
    cmd = build_git_ssh_command(
        known_hosts="~/repos/.known_hosts", openssh_version=(9, 6), quote_paths=False
    )
    assert "UserKnownHostsFile=~/repos/.known_hosts" in cmd


class _FakeSSHClient:
    """Stands in for a backend whose ssh is a given version."""

    def __init__(self, version: tuple[int, int] | None) -> None:
        self._version = version
        self.probes = 0

    async def openssh_version(self) -> tuple[int, int] | None:
        self.probes += 1
        return self._version


async def _remote_cmd(version: tuple[int, int] | None, key: str | None = "/tmp/k") -> str:
    # The builder never touches self; None keeps the test free of a
    # fully-wired RunManager.
    return await RunManager._build_remote_git_ssh_command(
        None, _FakeSSHClient(version), key, "~/scripthut-repos"
    )


async def test_remote_uses_the_backends_ssh_version_not_ours():
    """The clone runs on the cluster, so the cluster's ssh decides."""
    assert "StrictHostKeyChecking=no" in await _remote_cmd((7, 4))
    assert "StrictHostKeyChecking=accept-new" in await _remote_cmd((9, 6))


async def test_remote_keeps_host_keys_beside_the_clones():
    """Not ~/.ssh/known_hosts: a cluster home is often read-only, over
    quota, or shared across nodes in ways that make writing there fail."""
    cmd = await _remote_cmd((9, 6))
    assert "UserKnownHostsFile=~/scripthut-repos/.known_hosts" in cmd
    assert ".ssh/known_hosts" not in cmd


async def test_remote_creates_the_known_hosts_directory():
    """ssh writes the file but will not create its parent."""
    assert "mkdir -p ~/scripthut-repos;" in await _remote_cmd((9, 6))


async def test_remote_exports_for_the_following_git_command():
    cmd = await _remote_cmd((9, 6))
    assert 'export GIT_SSH_COMMAND="ssh ' in cmd
    assert cmd.endswith('"; ')


async def test_remote_is_empty_without_a_deploy_key():
    """Those clones are rewritten to HTTPS and never invoke ssh."""
    assert await _remote_cmd((7, 4), key=None) == ""


async def test_remote_does_not_probe_when_there_is_no_key():
    client = _FakeSSHClient((9, 6))
    await RunManager._build_remote_git_ssh_command(None, client, None, "~/repos")
    assert client.probes == 0


def test_local_does_not_discard_host_keys(tmp_path: Path):
    """The old command paired accept-new with UserKnownHostsFile=/dev/null,
    which cancels it out: nothing is ever remembered, so a host whose key
    changed always looks new."""
    cmd = GitSourceManager(tmp_path)._build_ssh_command(None)
    assert "/dev/null" not in cmd
    assert f"UserKnownHostsFile={tmp_path / 'known_hosts'}" in cmd


def test_local_creates_its_cache_dir(tmp_path: Path):
    """ssh will not create the directory the known_hosts file lives in."""
    cache = tmp_path / "not-yet"
    GitSourceManager(cache)._build_ssh_command(None)
    assert cache.is_dir()


async def test_probe_is_cached_but_a_dropped_connection_is_not():
    """A failed probe must not pin the client to "unknown" forever, but a
    successful one should not cost a round trip on every clone."""
    from scripthut.ssh.client import SSHClient

    client = SSHClient(host="h", user="u", key_path=Path("/k"))
    calls: list[str] = []

    async def flaky(command: str, timeout: int = 30) -> tuple[str, str, int]:
        calls.append(command)
        if len(calls) == 1:
            raise RuntimeError("connection lost")
        return ("", "OpenSSH_9.6p1, LibreSSL 3.3.6", 0)

    client.run_command = flaky  # type: ignore[assignment]

    assert await client.openssh_version() is None  # failed, not cached
    assert await client.openssh_version() == (9, 6)  # retried, succeeded
    assert await client.openssh_version() == (9, 6)  # served from cache
    assert len(calls) == 2


async def test_unreadable_banner_is_cached():
    """That answer cannot change, so don't re-probe on every clone."""
    from scripthut.ssh.client import SSHClient

    client = SSHClient(host="h", user="u", key_path=Path("/k"))
    calls: list[str] = []

    async def unknown(command: str, timeout: int = 30) -> tuple[str, str, int]:
        calls.append(command)
        return ("", "dropbear 2022.83", 0)

    client.run_command = unknown  # type: ignore[assignment]

    assert await client.openssh_version() is None
    assert await client.openssh_version() is None
    assert len(calls) == 1


def test_local_passes_the_deploy_key(tmp_path: Path):
    cmd = GitSourceManager(tmp_path)._build_ssh_command(Path("/keys/deploy"))
    assert "-i /keys/deploy" in cmd
    assert "-o IdentitiesOnly=yes" in cmd
