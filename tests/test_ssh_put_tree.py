"""Tests for ``SSHClient.put_tree`` (dataset transfer over SFTP).

The important invariants, none of which need a real cluster:
- it runs on its own connection, never the pooled one the poller shares
- it asks for a recursive, symlink-preserving put at the exact destination
- timeouts and SFTP errors surface as ``RuntimeError`` with the path named,
  and never leave the caller thinking the transfer succeeded
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest

from scripthut.ssh.client import SSHClient


class _FakeSFTP:
    def __init__(self, behaviour=None):
        self.behaviour = behaviour
        self.calls: list[tuple[tuple, dict]] = []

    async def put(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.behaviour is not None:
            await self.behaviour(kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, sftp: _FakeSFTP):
        self._sftp = sftp
        self.closed = False

    def start_sftp_client(self):
        return self._sftp

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True
        return False


class _FakeConnectManager:
    """Stands in for ``asyncssh.connect(...)`` — awaitable and an async CM."""

    def __init__(self, conn: _FakeConn):
        self._conn = conn

    def __await__(self):
        async def _inner():
            return self._conn

        return _inner().__await__()

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        self._conn.closed = True
        return False


def _client() -> SSHClient:
    return SSHClient(host="mercury", user="wiemann", key_path=Path("/keys/id"))


@pytest.fixture
def sftp_and_connect():
    sftp = _FakeSFTP()
    conn = _FakeConn(sftp)
    calls: list[dict] = []

    def _connect(**kwargs):
        calls.append(kwargs)
        return _FakeConnectManager(conn)

    with patch("asyncssh.connect", side_effect=_connect):
        yield sftp, conn, calls


class TestPutTree:
    @pytest.mark.asyncio
    async def test_uses_a_dedicated_connection(self, sftp_and_connect, tmp_path):
        sftp, conn, calls = sftp_and_connect
        client = _client()

        await client.put_tree(tmp_path, "/scratch/w/acq/abc")

        assert len(calls) == 1, "should open exactly one connection"
        assert conn.closed, "dedicated connection must be closed after transfer"
        # The pooled connection the status poller uses is untouched.
        assert client._connection is None
        assert not client.is_connected

    @pytest.mark.asyncio
    async def test_connection_matches_the_pooled_settings(
        self, sftp_and_connect, tmp_path
    ):
        _, _, calls = sftp_and_connect
        client = SSHClient(
            host="mercury",
            user="wiemann",
            key_path=Path("/keys/id"),
            port=2222,
            known_hosts=Path("/keys/known"),
        )

        await client.put_tree(tmp_path, "/scratch/w/acq/abc")

        kwargs = calls[0]
        assert kwargs["host"] == "mercury"
        assert kwargs["port"] == 2222
        assert kwargs["username"] == "wiemann"
        assert kwargs["known_hosts"] == Path("/keys/known")
        assert kwargs["preferred_auth"] == ["publickey"]

    @pytest.mark.asyncio
    async def test_requests_recursive_put_without_following_symlinks(
        self, sftp_and_connect, tmp_path
    ):
        sftp, _, _ = sftp_and_connect

        await _client().put_tree(tmp_path, "/scratch/w/acq/abc")

        args, kwargs = sftp.calls[0]
        assert args == (str(tmp_path), "/scratch/w/acq/abc")
        assert kwargs["recurse"] is True
        assert kwargs["follow_symlinks"] is False

    @pytest.mark.asyncio
    async def test_reports_bytes_copied_via_callback_and_return(
        self, sftp_and_connect, tmp_path
    ):
        sftp, _, _ = sftp_and_connect
        seen: list[tuple[int, int]] = []

        async def _emit(kwargs):
            handler = kwargs["progress_handler"]
            handler(b"src", b"dst", 512, 2048)
            handler(b"src", b"dst", 2048, 2048)

        sftp.behaviour = _emit

        copied = await _client().put_tree(
            tmp_path, "/scratch/w/acq/abc", progress=lambda d, t: seen.append((d, t))
        )

        assert copied == 2048
        assert seen == [(512, 2048), (2048, 2048)]

    @pytest.mark.asyncio
    async def test_timeout_raises_naming_the_destination(
        self, sftp_and_connect, tmp_path
    ):
        sftp, _, _ = sftp_and_connect

        async def _hang(_kwargs):
            await asyncio.sleep(5)

        sftp.behaviour = _hang

        with pytest.raises(RuntimeError, match="/scratch/w/acq/abc.*timed out"):
            await _client().put_tree(
                tmp_path, "/scratch/w/acq/abc", timeout=1,
            )

    @pytest.mark.asyncio
    async def test_sftp_error_raises_naming_the_destination(
        self, sftp_and_connect, tmp_path
    ):
        sftp, _, _ = sftp_and_connect

        async def _fail(_kwargs):
            raise OSError("Disk quota exceeded")

        sftp.behaviour = _fail

        with pytest.raises(RuntimeError, match="Disk quota exceeded"):
            await _client().put_tree(tmp_path, "/scratch/w/acq/abc")


class TestLocalExecClientPutTree:
    """The local backend puts a LocalExecClient in the ssh_client slot, so a
    `data:` workflow reaches put_tree there too. It used to be absent, which
    made staging on a local backend an AttributeError at transfer time.
    """

    @pytest.mark.asyncio
    async def test_copies_the_tree_and_reports_bytes(self, tmp_path):
        from scripthut.backends.local import LocalExecClient

        src = tmp_path / "src"
        (src / "nested").mkdir(parents=True)
        (src / "a.txt").write_bytes(b"x" * 10)
        (src / "nested" / "b.txt").write_bytes(b"y" * 5)
        dest = tmp_path / "out" / "copy"
        dest.parent.mkdir()

        copied = await LocalExecClient().put_tree(src, str(dest))

        assert copied == 15
        assert (dest / "a.txt").read_bytes() == b"x" * 10
        assert (dest / "nested" / "b.txt").read_bytes() == b"y" * 5

    @pytest.mark.asyncio
    async def test_refuses_an_existing_destination(self, tmp_path):
        # Same precondition as the SFTP path: the caller owns mkdir/mv, and a
        # nested copy would silently disagree with the manifest.
        from scripthut.backends.local import LocalExecClient

        src = tmp_path / "src"
        src.mkdir()
        (src / "a.txt").write_text("hi")
        dest = tmp_path / "existing"
        dest.mkdir()

        with pytest.raises(FileExistsError):
            await LocalExecClient().put_tree(src, str(dest))
