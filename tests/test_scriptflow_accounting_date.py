"""Controller history includes launches across scheduler timezone boundaries."""

import shlex
from types import SimpleNamespace

import pytest

from scripthut import scriptflow_launch, scriptflow_server
from scripthut.scriptflow import ObservationError
from tests.test_scriptflow_launch import LocalTransport


@pytest.mark.parametrize(
    "stamp,expected",
    [
        ("2026-11-17T00:01:00+00:00", "2026-11-16"),
        ("2026-01-01T00:01:00Z", "2025-12-31"),
        ("2024-03-01T00:01:00+00:00", "2024-02-29"),
    ],
)
def test_accounting_start_date(stamp, expected):
    assert scriptflow_launch.accounting_start_date(stamp) == expected
    assert scriptflow_server.accounting_start_date is scriptflow_launch.accounting_start_date


@pytest.mark.parametrize("accounting_code", [0, 1])
async def test_both_queries_use_previous_day_and_rejection_stays_unknown(tmp_path, accounting_code):
    bins = tmp_path / "bin"
    bins.mkdir()
    (bins / "sacct").write_text(
        "#!/bin/sh\n"
        'case " $* " in *"--starttime 2026-11-16"*) ;; *) exit 9 ;; esac\n'
        'case " $* " in\n'
        '  *"--format=State"*) printf "COMPLETED\\n" ;;\n'
        '  *) printf "1234|sf-example|COMPLETED\\n" ;;\n'
        "esac\n"
        f"exit {accounting_code}\n"
    )
    (bins / "squeue").write_text("#!/bin/sh\nexit 0\n")
    for path in bins.iterdir():
        path.chmod(0o755)
    local = LocalTransport()
    calls = []

    async def run(command, timeout=30):
        calls.append(command)
        return await local.run_command(
            "export PATH=" + shlex.quote(str(bins)) + ":$PATH\n" + command
        )

    transport = SimpleNamespace(run_command=run)
    backend = scriptflow_launch.ControllerBackend(transport)
    stamp = "2026-11-17T00:01:00+00:00"
    if accounting_code:
        with pytest.raises(ObservationError) as error:
            await backend.query("sf-example", stamp)
        assert error.value.kind == "unknown"
    else:
        assert await backend.query("sf-example", stamp) == {"1234": "COMPLETED"}
    assert "--starttime 2026-11-16" in calls[0]
    combined = scriptflow_server.CombinedRead(transport, "1234", since=stamp)
    assert await combined.run_command("printf record") == ("record", "", 0)
    assert "--starttime 2026-11-16" in calls[-1]
    assert combined.controller["state"] == ("unknown" if accounting_code else "ended")
