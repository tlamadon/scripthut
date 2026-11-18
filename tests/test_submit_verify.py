"""Controller submission acknowledgements and error diagnostics."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from scripthut.scriptflow_launch import ControllerBackend


# --- Slurm submission verification ---------------------------------------


def _slurm_with_responses(responses: list[tuple[str, str, int]]) -> ControllerBackend:
    """ControllerBackend whose SSH client returns ``responses`` in order."""
    ssh = AsyncMock()
    iterator = iter(responses)

    async def run_command(cmd: str, timeout: int = 30):  # noqa: ARG001
        try:
            return next(iterator)
        except StopIteration:
            return ("", "", 0)

    ssh.run_command = AsyncMock(side_effect=run_command)
    return ControllerBackend(ssh)


class TestSlurmSubmitParse:
    @pytest.mark.asyncio
    async def test_strict_regex_parses_clean_output(self):
        backend = _slurm_with_responses([
            ("Submitted batch job 12345\n", "", 0),  # sbatch
        ])
        result = await backend.submit_job("#!/bin/bash\necho hi")
        assert result == "12345"

    @pytest.mark.asyncio
    async def test_strict_regex_ignores_warnings_after_id_line(self):
        # Wrapper warnings can follow the acknowledgement line.
        backend = _slurm_with_responses([
            (
                "Submitted batch job 99999\n"
                "sbatch: warning: some-cluster: hint about something\n",
                "",
                0,
            ),
        ])
        result = await backend.submit_job("#!/bin/bash")
        assert result == "99999"

    @pytest.mark.asyncio
    async def test_unparseable_output_raises_with_raw_text(self):
        backend = _slurm_with_responses([
            ("not a real sbatch response\n", "warning: weird\n", 0),
        ])
        with pytest.raises(RuntimeError, match="Could not parse job ID"):
            await backend.submit_job("#!/bin/bash")

    @pytest.mark.asyncio
    async def test_sbatch_nonzero_exit_raises_with_stderr(self):
        backend = _slurm_with_responses([
            ("", "sbatch: error: invalid partition\n", 1),
        ])
        with pytest.raises(RuntimeError, match="sbatch failed"):
            await backend.submit_job("#!/bin/bash")
