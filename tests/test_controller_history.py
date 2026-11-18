"""CLI and server use accounting history for controllers that ended earlier."""

import shlex
from types import SimpleNamespace

import pytest

from scripthut.scriptflow import RecordReader
from scripthut.scriptflow_launch import ControllerBackend, LaunchStore, control, register_launch
from scripthut.scriptflow_server import CombinedRead
from tests.test_scriptflow_launch import Backend, LocalTransport, Transport, setup


@pytest.mark.parametrize("query_failed", [False, True])
async def test_old_controller_agrees_between_server_and_cli(setup, tmp_path, query_failed):
    _, _, launch, _ = setup
    launch.job_id = "1234"
    launch.created_at = "2001-01-01T00:00:00+00:00"
    bins = tmp_path / "bin"
    bins.mkdir()
    accounting = bins / "sacct"
    accounting.write_text('''#!/bin/sh
case "$*" in *--starttime*2000-12-31*) ;; *) exit 0;; esac
case "$*" in *--format=State*) printf 'COMPLETED|\\n';; *) printf '1234|%s|COMPLETED\\n' "$JOB_NAME";; esac
''')
    queue = bins / "squeue"
    queue.write_text('''#!/bin/sh
case "$*" in *--jobs*) exit 1;; esac
exit ''' + ("1" if query_failed else "0") + '\n')
    for path in (accounting, queue):
        path.chmod(0o755)
    local = LocalTransport()
    calls = []

    async def run(command, timeout=30):
        calls.append(command)
        prefix = f"export PATH={shlex.quote(str(bins))}:$PATH JOB_NAME={shlex.quote(launch.job_name)}\n"
        return await local.run_command(prefix + command)

    transport = SimpleNamespace(run_command=run)
    combined = CombinedRead(transport, launch.job_id, job_name=launch.job_name, since=launch.created_at)
    await combined.run_command("printf record")
    assert len(calls) == 1
    if query_failed:
        assert combined.controller["state"] == "unknown"
        with pytest.raises(Exception, match="query failed"):
            await ControllerBackend(transport).query(launch.job_name, launch.created_at)
    else:
        assert combined.controller["state"] == "ended"
        assert await ControllerBackend(transport).query(launch.job_name, launch.created_at) == {"1234": "COMPLETED"}


async def test_release_persists_ended_observation(setup):
    config, _, launch, store = setup
    launch.job_id = "1234"
    await LaunchStore(store.directory).save(launch)
    cached = await register_launch(store, launch)
    cached.controller = {"state": "running", "observed_at": "earlier"}
    await store.save(cached)
    backend = Backend(store, launch)
    backend.jobs = {"1234": "COMPLETED"}
    result = await control(config, cached, store, backend, RecordReader(Transport()), "release")
    assert result["status"] == "already_ended"
    saved = await store.get(cached.reference.id)
    assert saved.controller["state"] == "ended"
    assert saved.controller["observed_at"] != "earlier"
    assert not backend.cancelled and not backend.scripts
