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

from scripthut.ssh import client as ssh_client
from scripthut.ssh.client import SSHClient


class _FakeSFTP:
    def __init__(self, behaviour=None):
        self.behaviour = behaviour
        self.calls: list[tuple[tuple, dict]] = []
        self.makedirs_calls: list[tuple[str, bool]] = []
        self.get_calls: list[tuple[tuple, dict]] = []

    async def put(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.behaviour is not None:
            await self.behaviour(kwargs)

    async def get(self, *args, **kwargs):
        self.get_calls.append((args, kwargs))
        if self.behaviour is not None:
            await self.behaviour(kwargs)

    async def makedirs(self, path, exist_ok=False):
        self.makedirs_calls.append((path, exist_ok))

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


def test_sftp_block_fits_openssh_message_cap() -> None:
    """Payload plus WRITE headers must stay under OpenSSH's 256 KiB cap.

    A 256 KiB *payload* is too big: sftp-server cleanup_exit(11) and the
    client reports Connection closed. Confirmed against Mercury with a 1 MiB
    dummy: 256 KiB blocks fail, 32 KiB blocks and scp succeed.
    """
    # SSH_FXP_WRITE overhead is tens of bytes (type, id, handle, offset, len).
    assert ssh_client._SFTP_BLOCK_SIZE + 4096 < ssh_client._OPENSSH_SFTP_MAX_MSG


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
        assert kwargs["block_size"] == ssh_client._SFTP_BLOCK_SIZE
        assert kwargs["max_requests"] == ssh_client._SFTP_MAX_REQUESTS

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
    async def test_reports_tree_total_not_last_file(
        self, sftp_and_connect, tmp_path
    ):
        """asyncssh's handler is per-file; the last ``done`` is not the tree."""
        (tmp_path / "a.bin").write_bytes(b"x" * 1000)
        (tmp_path / "b.bin").write_bytes(b"y" * 50)
        sftp, _, _ = sftp_and_connect
        seen: list[tuple[int, int]] = []

        async def _emit(kwargs):
            handler = kwargs["progress_handler"]
            handler(b"a.bin", b"da", 400, 1000)
            handler(b"a.bin", b"da", 1000, 1000)
            handler(b"b.bin", b"db", 50, 50)

        sftp.behaviour = _emit

        copied = await _client().put_tree(
            tmp_path, "/scratch/w/acq/abc", progress=lambda d, t: seen.append((d, t))
        )

        assert copied == 1050
        assert seen == [(400, 1050), (1000, 1050), (1050, 1050)]

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


class TestPutFiles:
    @pytest.mark.asyncio
    async def test_empty_list_opens_no_connection(self, sftp_and_connect, tmp_path):
        _, conn, calls = sftp_and_connect
        copied = await _client().put_files(tmp_path, "/scratch/w/code", [])
        assert copied == 0
        assert calls == []
        assert not conn.closed  # never opened

    @pytest.mark.asyncio
    async def test_uses_a_dedicated_connection(self, sftp_and_connect, tmp_path):
        sftp, conn, calls = sftp_and_connect
        (tmp_path / "a.py").write_text("x")
        client = _client()

        await client.put_files(tmp_path, "/scratch/w/code", ["a.py"])

        assert len(calls) == 1
        assert conn.closed
        assert client._connection is None
        assert sftp.makedirs_calls == [("/scratch/w/code", True)]
        args, kwargs = sftp.calls[0]
        assert args == (str(tmp_path / "a.py"), "/scratch/w/code/a.py")
        assert kwargs["follow_symlinks"] is False

    @pytest.mark.asyncio
    async def test_creates_nested_parents(self, sftp_and_connect, tmp_path):
        sftp, _, _ = sftp_and_connect
        nested = tmp_path / "src"
        nested.mkdir()
        (nested / "b.py").write_text("y")

        await _client().put_files(tmp_path, "/d", ["src/b.py"])

        assert sftp.makedirs_calls == [("/d/src", True)]
        assert sftp.calls[0][0][1] == "/d/src/b.py"

    @pytest.mark.asyncio
    async def test_rejects_dotdot(self, sftp_and_connect, tmp_path):
        with pytest.raises(ValueError, match="Unsafe"):
            await _client().put_files(tmp_path, "/d", ["../etc/passwd"])

    @pytest.mark.asyncio
    async def test_timeout_names_the_destination(self, sftp_and_connect, tmp_path):
        sftp, _, _ = sftp_and_connect
        (tmp_path / "a.py").write_text("x")

        async def _hang(_kwargs):
            await asyncio.sleep(5)

        sftp.behaviour = _hang
        with pytest.raises(RuntimeError, match="/scratch/w/code.*timed out"):
            await _client().put_files(
                tmp_path, "/scratch/w/code", ["a.py"], timeout=1,
            )


class TestGetFiles:
    @pytest.mark.asyncio
    async def test_dedicated_connection_and_overwrite_contract(
        self, sftp_and_connect, tmp_path
    ):
        sftp, conn, calls = sftp_and_connect
        client = _client()

        await client.get_files("/scratch/w/code", tmp_path, ["output/a.csv"])

        assert len(calls) == 1
        assert conn.closed
        assert client._connection is None
        args, kwargs = sftp.get_calls[0]
        assert args[0] == "/scratch/w/code/output/a.csv"
        assert args[1] == str(tmp_path / "output" / "a.csv")
        assert kwargs["follow_symlinks"] is False
        assert (tmp_path / "output").is_dir()


class TestLocalExecClientPutGetFiles:
    @pytest.mark.asyncio
    async def test_round_trip_overwrite_without_delete(self, tmp_path):
        from scripthut.backends.local import LocalExecClient

        src = tmp_path / "src"
        (src / "sub").mkdir(parents=True)
        (src / "a.txt").write_text("new-a")
        (src / "sub" / "b.txt").write_text("b")
        dest = tmp_path / "dest"
        dest.mkdir()
        (dest / "stale.txt").write_text("keep")

        client = LocalExecClient()
        copied = await client.put_files(
            src, str(dest), ["a.txt", "sub/b.txt"],
        )
        assert copied == len("new-a") + len("b")
        assert (dest / "a.txt").read_text() == "new-a"
        assert (dest / "sub" / "b.txt").read_text() == "b"
        assert (dest / "stale.txt").read_text() == "keep"

        pulled = tmp_path / "pulled"
        (pulled / "output").mkdir(parents=True)
        (pulled / "output" / "old.csv").write_text("local-old")
        (dest / "output").mkdir()
        (dest / "output" / "new.csv").write_text("from-cluster")

        await client.get_files(
            str(dest), pulled, ["output/new.csv"],
        )
        assert (pulled / "output" / "new.csv").read_text() == "from-cluster"
        assert (pulled / "output" / "old.csv").read_text() == "local-old"

    @pytest.mark.asyncio
    async def test_empty_list_is_noop(self, tmp_path):
        from scripthut.backends.local import LocalExecClient

        assert await LocalExecClient().put_files(tmp_path, str(tmp_path / "d"), []) == 0
        assert await LocalExecClient().get_files(str(tmp_path), tmp_path, []) == 0

    @pytest.mark.asyncio
    async def test_list_files_skips_missing_dir_and_symlinks(self, tmp_path):
        from scripthut.backends.local import LocalExecClient

        root = tmp_path / "tree"
        (root / "sub").mkdir(parents=True)
        (root / "a.txt").write_text("a")
        (root / "sub" / "b.txt").write_text("b")
        (root / "link.txt").symlink_to("a.txt")
        client = LocalExecClient()
        assert await client.list_files(str(root)) == ["a.txt", "sub/b.txt"]
        assert await client.list_files(str(tmp_path / "nope")) == []

