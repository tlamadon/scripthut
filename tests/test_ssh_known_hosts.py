"""The known_hosts path has to reach asyncssh in a form it accepts.

scripthut stores it as a ``Path`` all the way from config, and asyncssh
wants a ``str``. Handed a ``Path`` it raises ``'PosixPath' object is not
subscriptable`` from deep inside ``match_known_hosts`` — so every
backend that turned host key verification on stopped connecting, and
switching verification off was the only way to get a working config.
Nothing caught it because nothing exercised the non-None branch.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import asyncssh
import pytest
from asyncssh.known_hosts import match_known_hosts

from scripthut.ssh.client import SSHClient


class _StopBeforeNetworkError(Exception):
    """Raised by the stubbed connect once the kwargs are captured."""


@pytest.fixture
def known_hosts_file(tmp_path: Path) -> Path:
    """A real known_hosts file listing one key for ``testhost``."""
    pub = asyncssh.generate_private_key("ssh-ed25519").export_public_key().decode()
    path = tmp_path / "known_hosts"
    path.write_text(f"testhost {pub.strip()}\n")
    return path


async def _capture_connect_kwargs(monkeypatch, client: SSHClient) -> dict:
    captured: dict = {}

    async def fake_connect(**kwargs):
        captured.update(kwargs)
        raise _StopBeforeNetworkError

    monkeypatch.setattr(asyncssh, "connect", fake_connect)
    with pytest.raises(_StopBeforeNetworkError):
        await client.connect(timeout=5)
    return captured


async def test_known_hosts_reaches_asyncssh_as_something_it_can_read(
    monkeypatch, known_hosts_file: Path
):
    """The regression: assert the value asyncssh receives is one its own
    matcher accepts, rather than trusting our idea of the contract."""
    client = SSHClient(
        host="testhost", user="u", key_path=Path("/k"), known_hosts=known_hosts_file
    )
    captured = await _capture_connect_kwargs(monkeypatch, client)

    assert isinstance(captured["known_hosts"], str)
    # Would raise TypeError if we had handed over the Path. The first
    # element of asyncssh's 7-tuple is the trusted host keys.
    trusted = match_known_hosts(captured["known_hosts"], "testhost", None, 22)[0]
    assert len(trusted) == 1


async def test_none_still_disables_verification(monkeypatch):
    """The documented escape hatch has to keep working."""
    client = SSHClient(host="h", user="u", key_path=Path("/k"), known_hosts=None)
    captured = await _capture_connect_kwargs(monkeypatch, client)
    assert captured["known_hosts"] is None


async def test_disabled_verification_is_announced(monkeypatch, caplog):
    """The default is off, and now that it can be switched on, saying so
    is the difference between a choice and an accident."""
    client = SSHClient(host="h", user="u", key_path=Path("/k"), known_hosts=None)
    with caplog.at_level("WARNING"):
        await _capture_connect_kwargs(monkeypatch, client)
    assert "Host key verification is disabled for h" in caplog.text


async def test_enabled_verification_is_not_warned_about(
    monkeypatch, caplog, known_hosts_file: Path
):
    client = SSHClient(
        host="h", user="u", key_path=Path("/k"), known_hosts=known_hosts_file
    )
    with caplog.at_level("WARNING"):
        await _capture_connect_kwargs(monkeypatch, client)
    assert "verification is disabled" not in caplog.text


def test_asyncssh_rejects_a_bare_path(known_hosts_file: Path):
    """Documents *why* the conversion exists, so nobody reverts it.

    If asyncssh ever grows Path support this fails, which is the right
    prompt to revisit — it means the conversion can go.
    """
    with pytest.raises(TypeError):
        match_known_hosts(known_hosts_file, "testhost", None, 22)


def test_asyncssh_signature_does_not_offer_path():
    """asyncssh's own annotation is the contract: str or bytes, no Path."""
    accepted = str(inspect.signature(match_known_hosts).parameters["known_hosts"])
    assert "str" in accepted
    assert "Path" not in accepted


def test_a_wrong_key_is_not_trusted(tmp_path: Path):
    """Verification is only worth enabling if it actually refuses."""
    wrong = asyncssh.generate_private_key("ssh-ed25519").export_public_key().decode()
    path = tmp_path / "known_hosts"
    path.write_text(f"testhost {wrong.strip()}\n")

    trusted = match_known_hosts(str(path), "testhost", None, 22)[0]
    other = asyncssh.generate_private_key("ssh-ed25519").export_public_key()
    assert asyncssh.import_public_key(other) not in trusted
