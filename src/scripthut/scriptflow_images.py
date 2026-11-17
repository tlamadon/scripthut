"""Compute-node image preparation and credential-free launch metadata."""

from __future__ import annotations

import hashlib
import shlex
import tempfile
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, cast

from scripthut.config_schema import ScriptHutConfig, image_reference
from scripthut.scriptflow import ObservationError, RecordReader, absolute_path
from scripthut.scriptflow_sources import Revision, remote_path
from scripthut.sources.git import GitSourceManager
from scripthut.ssh.client import SSHClient

if TYPE_CHECKING:
    from scripthut.scriptflow_launch import Launch

q = shlex.quote


async def resolve_images(config: ScriptHutConfig, revision: Revision) -> list[dict[str, str]]:
    source = revision.source
    if not source.images:
        return []
    manager = GitSourceManager(config.settings.sources_cache_dir_resolved)
    with tempfile.TemporaryDirectory(prefix="scripthut-image-refs-") as temporary:
        repository = source.local_path_resolved
        if repository is None and any(image.ref_file for image in source.images):
            repository = Path(temporary)
            for args in (
                ["init", "--bare"],
                ["fetch", "--depth", "1", source.url, revision.commit],
            ):
                _, _, code = await manager._run_git(
                    args, cwd=repository, deploy_key=source.deploy_key_resolved
                )
                if code:
                    raise ValueError("Cannot read image references from the selected source commit")
        result = []
        for image in source.images:
            ref = image.ref
            if image.ref_file:
                ref, _, code = await manager._run_git(
                    ["show", f"{revision.commit}:{image.ref_file}"], cwd=repository
                )
                if code:
                    raise ValueError(f"Image {image.name}: ref_file unavailable at selected commit")
            assert ref is not None
            ref = image_reference(ref, image.allow_tag)
            filename = (
                "sha256-" + ref.rsplit("@sha256:", 1)[1].lower()
                if "@sha256:" in ref
                else "tag-" + hashlib.sha256(ref.encode()).hexdigest()
            ) + ".sif"
            result.append({"name": image.name, "ref": ref, "filename": filename})
        return result


async def image_paths(
    config: ScriptHutConfig, reader: RecordReader, backend: str, images: list[dict[str, str]]
) -> list[dict[str, str]]:
    if not images:
        return []
    settings = config.get_backend(backend)
    if settings is None:
        raise ValueError("Unknown image backend")
    directory = await remote_path(reader, settings.image_dir or settings.clone_dir + "/images")
    directory = absolute_path(
        (await reader.execute(shlex.join(["realpath", "-m", "--", directory]))).strip()
    )
    return [
        {"name": image["name"], "ref": image["ref"], "path": directory + "/" + image["filename"]}
        for image in images
    ]


async def presence(reader: RecordReader, images: list[dict[str, str]]) -> list[dict[str, Any]]:
    results = []
    for image in images:
        script = """set -eu
root=$(realpath -m -- "$1")
target=$(realpath -m -- "$2")
[ "$root" = "$1" ] && [ "$target" = "$2" ] || exit 43
[ ! -L "$2" ] || exit 43
if [ -e "$2" ]; then
    [ -f "$2" ] && [ -s "$2" ] && [ -r "$2" ] || exit 43
    printf present
else printf absent; fi
"""
        state = await reader.execute(
            shlex.join(
                [
                    "sh",
                    "-c",
                    script,
                    "image-check",
                    str(PurePosixPath(image["path"]).parent),
                    image["path"],
                ]
            )
        )
        if state not in ("present", "absent"):
            raise ObservationError("malformed", "Unrecognized image presence response")
        results.append({**image, "present": state == "present"})
    return results


def token_path(launch: Launch) -> str:
    return launch.workdir + "/.sf/.token-" + launch.request_id


def status_path(launch: Launch) -> str:
    return launch.workdir + "/.sf/.scripthut-images-" + launch.request_id + ".status"


async def upload_token(config: ScriptHutConfig, reader: RecordReader, launch: Launch) -> None:
    if not launch.images:
        return
    settings = config.get_backend(launch.backend)
    assert settings is not None
    if settings.registry_token_file is None:
        return
    if all(row["present"] for row in await presence(reader, launch.images)):
        return
    try:
        secret = settings.registry_token_file.expanduser().read_text().strip()
    except (OSError, UnicodeError):
        raise ValueError("Cannot read local registry_token_file") from None
    if not secret or any(c in secret for c in "\x00\r\n"):
        raise ValueError("Registry token file must contain one nonempty line")
    directory = str(PurePosixPath(token_path(launch)).parent)
    script = """set -eu
umask 077
[ "$(realpath -m -- "$1")" = "$1" ] || exit 43
mkdir -p -- "$1"
[ "$(realpath -e -- "$1")" = "$1" ] || exit 43
tmp=$(mktemp "$1/.token-upload-XXXXXXXX")
trap 'rm -f -- "$tmp"' EXIT
cat > "$tmp"
chmod 600 -- "$tmp"
if [ -e "$2" ] || [ -L "$2" ]; then
    [ -f "$2" ] && [ ! -L "$2" ] && [ "$(stat -c %a -- "$2")" = 600 ] || exit 43
    cmp -s -- "$tmp" "$2"
else
    ln -- "$tmp" "$2"
fi
"""
    command = shlex.join(["sh", "-c", script, "image-token", directory, token_path(launch)])
    await cast(SSHClient, reader.transport).write_private_file(command, secret + "\n")


def token_cleanup(launch: Launch) -> str:
    path = token_path(launch)
    root = str(PurePosixPath(path).parent)
    return f'[ "$(realpath -m -- {q(root)})" = {q(root)} ] && rm -f -- {q(path)}'


def token_start(launch: Launch) -> str:
    return "\n".join(
        [
            "set +x",
            "set -e",
            "umask 077",
            "trap " + q(token_cleanup(launch)) + " EXIT",
            'trap "exit 143" TERM',
            'trap "exit 130" INT',
            "image_secret=",
            "image_token=" + q(token_path(launch)),
            '[ "$(realpath -m -- "$image_token")" = "$image_token" ] || exit 43',
            'if [ -e "$image_token" ]; then',
            '  [ -f "$image_token" ] && [ ! -L "$image_token" ] || exit 43',
            '  exec {image_token_fd}< "$image_token"',
            '  rm -f -- "$image_token"',
            '  IFS= read -r -u "$image_token_fd" image_secret',
            '  exec {image_token_fd}<&-',
            "fi",
        ]
    )


def preparation_script(config: ScriptHutConfig, launch: Launch) -> str:
    if not launch.images:
        return ""
    settings = config.get_backend(launch.backend)
    assert settings is not None
    root = str(PurePosixPath(launch.images[0]["path"]).parent)
    assignments = "".join(
        f"export SCRIPTHUT_IMAGE_{image['name'].upper()}={q(image['path'])}\n"
        for image in launch.images
    )
    worker = r"""set -euo pipefail
export APPTAINER_TMPDIR="$scratch" TMPDIR="$scratch"
export APPTAINER_CONFIGDIR="$scratch/config" APPTAINER_CACHEDIR="$scratch/cache"
export APPTAINER_DISABLE_CACHE=true
mkdir -- "$scratch/config" "$scratch/cache"
pull_image() (
    target=$1; ref=$2; copy=$3
    [ "$(realpath -m -- "$target")" = "$target" ] && [ ! -L "$target" ] || exit 43
    [ ! -L "$target.lock" ] || exit 43
    exec 9>> "$target.lock"
    flock -x 9
    [ "$(realpath -m -- "$target")" = "$target" ] && [ ! -L "$target" ] || exit 43
    if [ -e "$target" ]; then [ -f "$target" ] && [ -s "$target" ]; exit; fi
    local_image=$(mktemp -d "$scratch/pull-XXXXXXXX")/image.sif
    apptainer pull --disable-cache "$local_image" "$ref" 2>&1 |
        while IFS= read -r line || [ -n "$line" ]; do
            if [ -n "${APPTAINER_DOCKER_PASSWORD:-}" ]; then
                line=${line//"$APPTAINER_DOCKER_PASSWORD"/[redacted]}
            fi
            printf '%s\n' "$line"
        done >> "$image_log"
    [ -s "$local_image" ] || {
        printf 'Runtime did not create an image.\n' >> "$image_log"; exit 1;
    }
    cp -- "$local_image" "$copy"
    [ ! -L "$target" ] || exit 43
    mv -n -T -- "$copy" "$target"
    [ -f "$target" ] && [ -s "$target" ]
)
"""
    for index, image in enumerate(launch.images):
        ref = image["ref"]
        uri = ref if ref.startswith("docker://") else "docker://" + ref
        worker += shlex.join(["pull_image", image["path"], uri]) + f' "${{copies[{index}]}}"\n'
    # The worker receives paths as positional arguments, credentials only through its environment.
    worker = 'copies=("$@")\n' + worker
    script = (
        r"""set +x
set -euo pipefail
umask 077
"""
        + "\n".join(
            [
                "image_root=" + q(root),
                "image_sf=" + q(launch.workdir + "/.sf"),
                "image_status=" + q(status_path(launch)),
                "image_log=" + q(status_path(launch) + ".log"),
                "scratch_base=" + q(settings.image_scratch_dir or ""),
                "image_limit=" + str(settings.image_pull_timeout_seconds),
            ]
        )
        + r"""
[ "$(realpath -m -- "$image_root")" = "$image_root" ] || exit 43
[ "$(realpath -m -- "$image_sf")" = "$image_sf" ] || exit 43
mkdir -p -- "$image_root" "$image_sf"
[ ! -L "$image_log" ] && [ ! -L "$image_status" ] || exit 43
: > "$image_log"
image_started=$(date +%s)
scratch=; image_pid=; copies=(); image_reason=
image_state() {
    tmp=$(mktemp "$image_sf/.image-status-XXXXXXXX")
    { printf '%s\nstarted_at=%s\n' "$1" "$image_started"
      if [ "$1" = failed ]; then
          printf '%s\n' "$image_reason"
          tail -c 65536 -- "$image_log"
      fi
    } > "$tmp"
    mv -T -- "$tmp" "$image_status"
}
image_finish() {
    rc=$?
    trap - EXIT TERM INT
    if [ -n "$image_pid" ]; then
        kill -TERM "$image_pid" 2>/dev/null || true
        wait "$image_pid" 2>/dev/null || true
        # Runtime children can outlive the shell monitored by timeout.
        kill -KILL -- "-$image_pid" 2>/dev/null || true
    fi
    for copy in "${copies[@]}"; do rm -f -- "$copy"; done
    if [ "$rc" != 0 ]; then
        image_reason=${image_reason:-Image preparation failed (exit $rc).}
        image_state failed
    fi
    [ -z "$scratch" ] || rm -rf -- "$scratch"
    exit "$rc"
}
trap image_finish EXIT
trap 'image_reason="Image preparation terminated by scheduler signal."; exit 143' TERM
trap 'image_reason="Image preparation interrupted."; exit 130' INT
image_state preparing
if [ -z "$scratch_base" ]; then
    if [ -n "${SLURM_TMPDIR:-}" ] && [ -d "$SLURM_TMPDIR" ] && [ -w "$SLURM_TMPDIR" ]; then
        scratch_base=$SLURM_TMPDIR
    elif [ -n "${TMPDIR:-}" ] && [ -d "$TMPDIR" ] && [ -w "$TMPDIR" ]; then
        scratch_base=$TMPDIR
    else
        scratch_base=/tmp
    fi
fi
[ -d "$scratch_base" ] && [ -w "$scratch_base" ] || {
    image_reason="Image scratch directory is not writable."; exit 1;
}
scratch=$(mktemp -d "$scratch_base/scripthut-images-XXXXXXXX")
export scratch image_log
"""
    )
    for _ in launch.images:
        script += 'copies+=("$(mktemp "$image_root/.image-copy-XXXXXXXX")")\n'
    script += "\n".join(
        [
            "(",
            "  unset DOCKER_PASSWORD DOCKER_USERNAME",
            "  unset APPTAINER_DOCKER_PASSWORD APPTAINER_DOCKER_USERNAME",
            '  if [ -n "$image_secret" ]; then',
            '    export APPTAINER_DOCKER_PASSWORD="$image_secret"',
            "    export APPTAINER_DOCKER_USERNAME=" + q(settings.registry_user or ""),
            "  fi",
            '  exec timeout --signal=TERM --kill-after=1s "$image_limit" bash -c '
            + q(worker)
            + ' image-worker "${copies[@]}"',
            ") &",
            "image_pid=$!",
            "unset image_secret",
            'rc=0; wait "$image_pid" || rc=$?',
            'kill -KILL -- "-$image_pid" 2>/dev/null || true',
            "image_pid=",
            'if [ "$rc" = 124 ] || [ "$rc" = 137 ]; then',
            '  image_reason="Image preparation timed out after $image_limit seconds."',
            "fi",
            '[ "$rc" = 0 ] || exit "$rc"',
            'for copy in "${copies[@]}"; do rm -f -- "$copy"; done',
            'rm -rf -- "$scratch"; scratch=',
            'tmp=$(mktemp "$image_sf/.images-env-XXXXXXXX")',
            "printf %s " + q(assignments) + ' > "$tmp"',
            'mv -T -- "$tmp" "$image_sf/images.sh"',
            assignments,
            "image_state ready",
            "trap - EXIT TERM INT",
        ]
    )
    return script


def observation_command(launch: Launch) -> str | None:
    if not launch.images:
        return None
    script = """set -eu
root=$(realpath -m -- "$1")
target=$(realpath -m -- "$2")
[ "$root" = "$1" ] && [ "$target" = "$2" ] || exit 43
if [ -e "$target" ]; then
    head -c 66000 -- "$target"
    if [ "$(head -n 1 -- "$target")" = preparing ]; then
        printf '\n__SCRIPTHUT_PULL_LOG__\n'
        [ "$(realpath -m -- "$target.log")" = "$target.log" ] || exit 43
        if [ -f "$target.log" ]; then tail -c 16384 -- "$target.log" | tail -n 20; fi
    fi
fi
"""
    return shlex.join(
        ["sh", "-c", script, "image-status", launch.workdir + "/.sf", status_path(launch)]
    )


def parse_status(content: str) -> dict[str, Any] | None:
    if not content:
        return None
    status, _, error = content.partition("\n")
    if status not in ("preparing", "ready", "failed"):
        raise ObservationError("malformed", "Malformed image preparation status")
    body, _, tail = error.partition("\n__SCRIPTHUT_PULL_LOG__\n")
    result: dict[str, Any] = {"status": status, "error": ""}
    if body.startswith("started_at="):
        started, _, body = body.partition("\n")
        try:
            result["started_at"] = int(started.removeprefix("started_at="))
        except ValueError:
            raise ObservationError("malformed", "Malformed image preparation start time") from None
    if status == "failed":
        result["error"] = body.strip()
    if status == "preparing":
        result["log_tail"] = tail.rstrip()
    return result


async def observe_images(reader: RecordReader, launch: Launch) -> dict[str, Any] | None:
    command = observation_command(launch)
    return parse_status(await reader.execute(command)) if command else None
