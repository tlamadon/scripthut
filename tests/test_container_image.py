"""Slurm container support: task `image:` -> apptainer exec.

Covers the two pure pieces (script wrapping, URI -> .sif filename) and the
submit-time pull, which is where the design decision lives: the pull runs on
the *login* node over SSH, not inside the job, because compute nodes often
have no route to a registry.
"""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from scripthut.backends.utils import (
    CONTAINER_CMD_EOF,
    generate_script_body,
    wrap_command_in_container,
)
from scripthut.runs.manager import RunManager
from scripthut.runs.models import TaskDefinition


class TestWrapCommand:
    def test_runs_command_inside_the_image(self):
        wrapped = wrap_command_in_container("Rscript run.R", "/home/u/img.sif")
        assert 'apptainer exec "/home/u/img.sif"' in wrapped
        assert '"$_scripthut_cmd"' in wrapped

    def test_prefers_bash_but_tolerates_an_image_without_it(self):
        # Alpine ships busybox ash: hardcoding bash failed with
        # `"bash": executable file not found in $PATH` (run 54e2cb07).
        wrapped = wrap_command_in_container("true", "/i.sif")
        assert "/bin/sh -c" in wrapped
        assert 'command -v bash' in wrapped
        assert 'exec bash "$0"' in wrapped
        assert 'exec sh "$0"' in wrapped

    def test_apptainer_exec_is_last_so_exit_code_survives(self):
        # generate_script_body appends `EXIT_CODE=$?` right after the command;
        # anything executed after the exec would clobber the task's status.
        wrapped = wrap_command_in_container("false", "/i.sif")
        lines = [ln for ln in wrapped.strip().splitlines() if ln.strip()]
        assert lines[-1].strip() == '"$_scripthut_cmd"'
        assert any(ln.startswith("apptainer exec") for ln in lines[-3:])

    def test_temp_file_is_cleaned_up_without_touching_exit_code(self):
        wrapped = wrap_command_in_container("true", "/i.sif")
        assert "trap 'rm -f \"$_scripthut_cmd\"' EXIT" in wrapped

    def test_command_goes_through_a_file_not_the_containers_stdin(self):
        # `apptainer exec ... bash -s <<EOF` would eat the container's stdin.
        wrapped = wrap_command_in_container("cat", "/i.sif")
        assert "bash -s" not in wrapped
        assert 'cat > "$_scripthut_cmd"' in wrapped

    def test_multiline_command_with_quotes_survives_verbatim(self):
        command = 'echo "it\'s $HOME" && R -e \'cat(1)\'\nexit 0'
        wrapped = wrap_command_in_container(command, "/i.sif")
        assert command in wrapped

    def test_tilde_in_image_path_still_expands(self):
        wrapped = wrap_command_in_container("true", "~/scripthut-images/x.sif")
        assert '"$HOME/scripthut-images/x.sif"' in wrapped

    def test_command_containing_the_delimiter_is_rejected(self):
        with pytest.raises(ValueError, match="delimit"):
            wrap_command_in_container(f"echo hi\n{CONTAINER_CMD_EOF}\n", "/i.sif")

    def test_host_ld_preload_is_dropped_before_entering_the_image(self):
        # A host login profile may set LD_PRELOAD to a path absent in the
        # image; against a foreign distro every binary then writes
        # "ld.so: object ... cannot be preloaded: ignored" to stderr.
        wrapped = wrap_command_in_container("true", "/i.sif")
        assert "unset LD_PRELOAD" in wrapped
        assert wrapped.index("unset LD_PRELOAD") < wrapped.index("apptainer exec")

    def test_host_python_env_is_dropped_before_entering_the_image(self):
        # Host PYTHONHOME pointed at a cluster module makes the image's
        # python3 look for encodings under the host path and die.
        wrapped = wrap_command_in_container("true", "/i.sif")
        assert "unset LD_PRELOAD PYTHONHOME PYTHONPATH" in wrapped
        assert wrapped.index("PYTHONHOME") < wrapped.index("apptainer exec")

    def test_no_binds_means_no_bind_flag(self):
        assert "--bind" not in wrap_command_in_container("true", "/i.sif")
        assert "--bind" not in wrap_command_in_container("true", "/i.sif", [])

    def test_each_bind_becomes_a_bind_flag_before_the_image(self):
        wrapped = wrap_command_in_container(
            "true", "/i.sif", ["/data", "/scratch/data"]
        )
        exec_line = next(
            ln for ln in wrapped.splitlines() if ln.startswith("apptainer exec")
        )
        assert exec_line.index("--bind") < exec_line.index("/i.sif")
        assert '--bind "/data"' in exec_line
        assert '--bind "/scratch/data"' in exec_line


class TestGenerateScriptBody:
    def test_no_image_means_no_container_machinery(self):
        body = generate_script_body(
            task_name="t", task_id="t", command="Rscript run.R", working_dir="/w",
        )
        assert "apptainer" not in body
        assert "\nRscript run.R\n" in body

    def test_image_wraps_the_command_but_not_the_env_setup(self):
        body = generate_script_body(
            task_name="t",
            task_id="t",
            command="Rscript run.R",
            working_dir="/w",
            extra_init="module load R/4.5/4.5.3",
            env_vars={"PROJECT_WORK": "/home/u/work"},
            image_sif="/home/u/img.sif",
        )
        # module load / exports / cd stay on the host; only the command moves.
        init_at = body.index("module load")
        export_at = body.index('export PROJECT_WORK="/home/u/work"')
        cd_at = body.index("cd /w")
        exec_at = body.index("apptainer exec")
        assert init_at < export_at < cd_at < exec_at

    def test_exit_code_is_still_captured_after_the_exec(self):
        body = generate_script_body(
            task_name="t", task_id="t", command="run", working_dir="/w",
            image_sif="/i.sif",
        )
        exec_at = body.index("apptainer exec")
        assert body.index("EXIT_CODE=$?", exec_at) > exec_at


class TestSecretUpload:
    """The credential must never reach a remote command line."""

    @pytest.mark.asyncio
    async def test_secret_goes_over_sftp_at_mode_600(
        self, manager, tmp_path: Path
    ):
        secret = tmp_path / "ghcr-token"
        secret.write_text("s3cret-value")

        ssh = _FakeSSH(present=False)
        ssh.write_file = AsyncMock()  # type: ignore[method-assign]

        remote = await manager._upload_secret_file(ssh, secret)

        ssh.write_file.assert_awaited_once()
        args, kwargs = ssh.write_file.await_args
        assert args[0] == remote
        assert args[1] == "s3cret-value"
        assert kwargs["mode"] == 0o600
        # No shell command carried the payload.
        assert not any("s3cret-value" in c for c in ssh.commands)

    @pytest.mark.asyncio
    async def test_trailing_newline_is_stripped_from_a_token(
        self, manager, tmp_path: Path
    ):
        # Editors add one on save; a newline inside the password makes the
        # registry answer 401 with nothing to go on.
        secret = tmp_path / "ghcr-token"
        secret.write_text("ghp_abc123\n")
        ssh = _FakeSSH(present=False)
        ssh.write_file = AsyncMock()  # type: ignore[method-assign]

        await manager._upload_secret_file(ssh, secret, strip=True)

        assert ssh.write_file.await_args.args[1] == "ghp_abc123"

    @pytest.mark.asyncio
    async def test_ssh_key_keeps_its_trailing_newline(
        self, manager, tmp_path: Path
    ):
        # PEM requires it; stripping makes the key unreadable.
        key = tmp_path / "id_ed25519"
        key.write_text("-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n")
        ssh = _FakeSSH(present=False)
        ssh.write_file = AsyncMock()  # type: ignore[method-assign]

        await manager._upload_secret_file(ssh, key)

        assert ssh.write_file.await_args.args[1].endswith("\n")

    @pytest.mark.asyncio
    async def test_missing_secret_file_fails_before_touching_the_backend(
        self, manager, tmp_path: Path
    ):
        ssh = _FakeSSH(present=False)
        ssh.write_file = AsyncMock()  # type: ignore[method-assign]
        with pytest.raises(ValueError, match="Secret file not found"):
            await manager._upload_secret_file(ssh, tmp_path / "nope")
        ssh.write_file.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_each_upload_gets_its_own_remote_path(
        self, manager, tmp_path: Path
    ):
        secret = tmp_path / "tok"
        secret.write_text("x")
        ssh = _FakeSSH(present=False)
        ssh.write_file = AsyncMock()  # type: ignore[method-assign]

        first = await manager._upload_secret_file(ssh, secret)
        second = await manager._upload_secret_file(ssh, secret)
        assert first != second


class TestImageSifName:
    @pytest.mark.parametrize(
        "uri,expected",
        [
            (
                "ghcr.io/example/app:tag-abc123",
                "ghcr.io_example_app_tag-abc123.sif",
            ),
            # A transport prefix must not change the cache key.
            (
                "docker://ghcr.io/o/r:tag",
                "ghcr.io_o_r_tag.sif",
            ),
        ],
    )
    def test_uri_maps_to_a_safe_filename(self, uri, expected):
        assert RunManager._image_sif_name(uri) == expected

    def test_distinct_tags_get_distinct_files(self):
        a = RunManager._image_sif_name("ghcr.io/o/r:tag-aaa")
        b = RunManager._image_sif_name("ghcr.io/o/r:tag-bbb")
        assert a != b

    def test_no_path_separators_escape_the_image_dir(self):
        assert "/" not in RunManager._image_sif_name("ghcr.io/o/r:t")


class _FakeSSH:
    """Records commands and SFTP writes; answers the existence probe."""

    def __init__(self, present: bool):
        self._present = present
        self.commands: list[str] = []
        self.written: list[tuple[str, str, int]] = []

    async def run_command(self, cmd, timeout=None):
        self.commands.append(cmd)
        if "test -f" in cmd:
            return ("present" if self._present else "absent", "", 0)
        return ("", "", 0)

    async def write_file(self, remote_path, content, *, mode=0o600):
        self.written.append((remote_path, content, mode))


@pytest.fixture
def manager():
    # _ensure_image touches no instance state, so an unconstructed shell is
    # enough to exercise it without standing up a whole server config.
    return RunManager.__new__(RunManager)


class TestEnsureImage:
    @pytest.mark.asyncio
    async def test_present_image_is_not_pulled_again(self, manager):
        ssh = _FakeSSH(present=True)
        path, pulled = await manager._ensure_image(
            ssh, "ghcr.io/o/r:t", image_dir="~/scripthut-images",
        )
        assert path == "~/scripthut-images/ghcr.io_o_r_t.sif"
        assert pulled is False
        assert not any("apptainer pull" in c for c in ssh.commands)

    @pytest.mark.asyncio
    async def test_pull_is_serialised_with_flock_and_rechecks_inside_it(
        self, manager,
    ):
        # Two callers racing on one path would leave a truncated .sif that
        # every later `apptainer exec` fails on.
        ssh = _FakeSSH(present=False)
        await manager._ensure_image(ssh, "ghcr.io/o/r:t", image_dir="~/i")
        pull = next(c for c in ssh.commands if "apptainer pull" in c)
        assert "flock 9" in pull
        assert pull.index("flock 9") < pull.index("apptainer pull")
        # Re-check inside the lock: the loser of the race must not re-pull.
        assert "if [ -f" in pull and pull.index("if [ -f") < pull.index("apptainer pull")

    def test_default_cpus_is_one_to_satisfy_interactive_job_caps(self):
        # mercury: "interactive job requests 2 cpu, exceeds 1 cpu limit".
        from scripthut.config_schema import ImagePullConfig

        assert ImagePullConfig().cpus == 1

    @pytest.mark.asyncio
    async def test_pull_runs_on_a_worker_with_requested_memory(self, manager):
        # mksquashfs aborted on mercury's login node with "malloc():
        # corrupted top size"; a worker allocation gets the memory it asks.
        from scripthut.config_schema import ImagePullConfig

        ssh = _FakeSSH(present=False)
        await manager._ensure_image(
            ssh,
            "ghcr.io/o/r:t",
            image_dir="~/i",
            pull_config=ImagePullConfig(cpus=4, memory="16G", time_limit="2:00:00"),
            default_partition="standard",
        )
        pull = next(c for c in ssh.commands if "apptainer pull" in c)
        assert "srun" in pull
        assert "--mem=16G" in pull
        assert "--cpus-per-task=4" in pull
        assert "--time=2:00:00" in pull
        assert "--partition=standard" in pull

    @pytest.mark.asyncio
    async def test_pull_env_is_exported_inside_the_job(self, manager):
        # APPTAINER_TMPDIR must apply where mksquashfs actually runs.
        from scripthut.config_schema import ImagePullConfig

        ssh = _FakeSSH(present=False)
        await manager._ensure_image(
            ssh,
            "ghcr.io/o/r:t",
            image_dir="~/i",
            pull_config=ImagePullConfig(env={"APPTAINER_TMPDIR": "/scratch/tmp"}),
        )
        pull = next(c for c in ssh.commands if "apptainer pull" in c)
        assert "export APPTAINER_TMPDIR=/scratch/tmp" in pull
        assert pull.index("export APPTAINER_TMPDIR") < pull.index("apptainer pull")

    @pytest.mark.asyncio
    async def test_credentials_are_exported_outside_srun(self, manager, tmp_path: Path):
        # The token file lives in the login node's /tmp, which the worker
        # cannot see; $(cat ...) has to run before srun starts the step.
        token = tmp_path / "tok"
        token.write_text("x")
        ssh = _FakeSSH(present=False)
        await manager._ensure_image(
            ssh, "ghcr.io/o/r:t", image_dir="~/i",
            registry_user="alice", registry_token=token,
        )
        pull = next(c for c in ssh.commands if "apptainer pull" in c)
        assert pull.index("APPTAINER_DOCKER_PASSWORD") < pull.index("srun")

    @pytest.mark.asyncio
    async def test_force_repulls_over_an_existing_image(self, manager):
        ssh = _FakeSSH(present=True)
        _, pulled = await manager._ensure_image(
            ssh, "ghcr.io/o/r:t", image_dir="~/i", force=True,
        )
        assert pulled is True
        pull = next(c for c in ssh.commands if "apptainer pull" in c)
        assert "--force" in pull
        assert "if [ -f" not in pull

    @pytest.mark.asyncio
    async def test_absent_image_is_pulled_with_a_docker_transport(self, manager):
        ssh = _FakeSSH(present=False)
        await manager._ensure_image(
            ssh, "ghcr.io/o/r:t", image_dir="~/scripthut-images",
        )
        pull = next(c for c in ssh.commands if "apptainer pull" in c)
        assert "docker://ghcr.io/o/r:t" in pull
        assert "mkdir -p" in pull

    @pytest.mark.asyncio
    async def test_no_token_configured_means_no_auth_prefix(self, manager):
        ssh = _FakeSSH(present=False)
        await manager._ensure_image(ssh, "ghcr.io/o/r:t", image_dir="~/i")
        pull = next(c for c in ssh.commands if "apptainer pull" in c)
        assert "APPTAINER_DOCKER_PASSWORD" not in pull

    @pytest.mark.asyncio
    async def test_token_is_read_on_the_backend_never_interpolated(
        self, manager, tmp_path: Path
    ):
        token = tmp_path / "ghcr-token"
        token.write_text("s3cret-value")
        ssh = _FakeSSH(present=False)

        await manager._ensure_image(
            ssh,
            "ghcr.io/o/r:t",
            image_dir="~/i",
            registry_user="alice",
            registry_token=token,
        )

        pull = next(c for c in ssh.commands if "apptainer pull" in c)
        # The secret itself must never reach the remote process list.
        assert "s3cret-value" not in pull
        assert 'APPTAINER_DOCKER_PASSWORD="$(cat ' in pull
        assert "APPTAINER_DOCKER_USERNAME=alice" in pull

    @pytest.mark.asyncio
    async def test_docker_hub_pull_skips_private_registry_credentials(
        self, manager, tmp_path: Path
    ):
        # mercury has a GHCR token for private packages; sending it to
        # Docker Hub makes `python:3.12-slim` fail unauthorized.
        token = tmp_path / "ghcr-token"
        token.write_text("s3cret-value")
        ssh = _FakeSSH(present=False)

        await manager._ensure_image(
            ssh,
            "python:3.12-slim",
            image_dir="~/i",
            registry_user="alice",
            registry_token=token,
        )

        pull = next(c for c in ssh.commands if "apptainer pull" in c)
        assert "APPTAINER_DOCKER_PASSWORD" not in pull
        assert "APPTAINER_DOCKER_USERNAME" not in pull
        assert ssh.written == []

    def test_image_registry_host_short_name_is_docker_hub(self):
        assert RunManager._image_registry_host("python:3.12-slim") == "docker.io"
        assert RunManager._image_registry_host("library/python:3.12") == "docker.io"
        assert RunManager._image_registry_host("ghcr.io/o/r:t") == "ghcr.io"
        assert RunManager._image_registry_host(
            "docker://ghcr.io/o/r:t"
        ) == "ghcr.io"

    @pytest.mark.asyncio
    async def test_token_is_deleted_even_when_the_pull_fails(
        self, manager, tmp_path: Path
    ):
        token = tmp_path / "tok"
        token.write_text("x")
        ssh = _FakeSSH(present=False)

        async def failing(cmd, timeout=None):
            ssh.commands.append(cmd)
            if "test -f" in cmd:
                return ("absent", "", 0)
            if "apptainer pull" in cmd:
                return ("", "unauthorized", 1)
            return ("/tmp/scripthut_key_x", "", 0)

        ssh.run_command = failing  # type: ignore[method-assign]

        with pytest.raises(ValueError, match="Failed to pull image"):
            await manager._ensure_image(
                ssh, "ghcr.io/o/r:t", image_dir="~/i", registry_token=token,
            )
        assert any(c.startswith("rm -f") for c in ssh.commands)


class _Cfg:
    name = "mercury"
    image_dir = "~/i"
    registry_user = None
    registry_token_resolved = None


class TestResolveImages:
    """Submit-time resolution locates images; it must never pull them."""

    @pytest.mark.asyncio
    async def test_present_image_is_stamped_on_every_task_that_shares_it(
        self, manager,
    ):
        tasks = [
            TaskDefinition(id="a", name="a", command="x", image="ghcr.io/o/r:t"),
            TaskDefinition(id="b", name="b", command="y", image="ghcr.io/o/r:t"),
            TaskDefinition(id="c", name="c", command="z"),
        ]
        ssh = _FakeSSH(present=True)

        await manager._resolve_images(tasks, ssh, _Cfg())

        assert tasks[0].image_sif == tasks[1].image_sif == "~/i/ghcr.io_o_r_t.sif"
        assert tasks[2].image_sif is None
        # One probe for the one distinct URI, and no pull.
        assert sum("test -f" in c for c in ssh.commands) == 1
        assert not any("apptainer pull" in c for c in ssh.commands)

    @pytest.mark.asyncio
    async def test_missing_image_fails_submit_naming_the_command_to_run(
        self, manager,
    ):
        # Pulling here would die with the client's read timeout and cache
        # nothing, so no retry could ever succeed (run 54e2cb07 onwards).
        tasks = [
            TaskDefinition(id="a", name="a", command="x", image="ghcr.io/o/r:t")
        ]
        ssh = _FakeSSH(present=False)

        with pytest.raises(ValueError) as exc:
            await manager._resolve_images(tasks, ssh, _Cfg())

        msg = str(exc.value)
        assert "scripthut image ensure ghcr.io/o/r:t --backend mercury" in msg
        assert not any("apptainer pull" in c for c in ssh.commands)

    @pytest.mark.asyncio
    async def test_tasks_without_images_skip_the_backend_entirely(self, manager):
        tasks = [TaskDefinition(id="a", name="a", command="x")]
        ssh = _FakeSSH(present=True)
        await manager._resolve_images(tasks, ssh, _Cfg())
        assert ssh.commands == []
