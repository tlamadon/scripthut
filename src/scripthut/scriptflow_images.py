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
    return str(PurePosixPath(launch.images[0]["path"]).parent / (".token-" + launch.request_id))


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
    directory = str(PurePosixPath(launch.images[0]["path"]).parent)
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


def preparation_script(config: ScriptHutConfig, launch: Launch) -> str:
    if not launch.images:
        return ""
    settings = config.get_backend(launch.backend)
    assert settings is not None
    root = str(PurePosixPath(launch.images[0]["path"]).parent)
    sf = launch.workdir + "/.sf"
    status = status_path(launch)
    log = status + ".log"
    assignments = (
        "\n".join(
            f"export SCRIPTHUT_IMAGE_{image['name'].upper()}={q(image['path'])}"
            for image in launch.images
        )
        + "\n"
    )
    lines = [
        "set +x",
        "set -euo pipefail",
        "umask 077",
        f"image_root={q(root)}",
        f"image_sf={q(sf)}",
        f"image_status={q(status)}",
        f"image_log={q(log)}",
        f"image_token={q(token_path(launch))}",
        '[ "$(realpath -m -- "$image_root")" = "$image_root" ] || exit 43',
        '[ "$(realpath -m -- "$image_sf")" = "$image_sf" ] || exit 43',
        'mkdir -p -- "$image_root" "$image_sf"',
        '[ ! -L "$image_log" ] && [ ! -L "$image_status" ] || exit 43',
        ': > "$image_log"',
        "image_finish() {",
        "  rc=$?",
        "  trap - EXIT",
        '  if [ "$(realpath -m -- "$image_token")" = "$image_token" ]; then',
        '    rm -f -- "$image_token"',
        "  fi",
        '  if [ "$rc" != 0 ]; then',
        '    tmp=$(mktemp "$image_sf/.image-status-XXXXXXXX")',
        '    { printf "failed\\nImage preparation failed (exit %s).\\n" "$rc"; '
        'head -c 65536 -- "$image_log"; } > "$tmp"',
        '    mv -T -- "$tmp" "$image_status"',
        "  fi",
        '  exit "$rc"',
        "}",
        "trap image_finish EXIT",
        'trap "exit 143" TERM',
        'trap "exit 130" INT',
        'tmp=$(mktemp "$image_sf/.image-status-XXXXXXXX")',
        'printf "preparing\\n" > "$tmp"; mv -T -- "$tmp" "$image_status"',
        "pull_image() (",
        "  trap - EXIT TERM INT",
        "  target=$1; ref=$2",
        '  [ "$(realpath -m -- "$target")" = "$target" ] && [ ! -L "$target" ] || exit 43',
        '  [ ! -L "$target.lock" ] || exit 43',
        '  exec 9> "$target.lock"',
        "  flock -x 9",
        '  [ "$(realpath -m -- "$target")" = "$target" ] && [ ! -L "$target" ] || exit 43',
        '  [ ! -L "$image_token" ] || exit 43',
        '  if [ -e "$target" ]; then [ -f "$target" ] && [ -s "$target" ]; exit; fi',
        '  scratch=$(mktemp -d "$image_root/.pull-XXXXXXXX")',
        '  export APPTAINER_TMPDIR="$scratch" TMPDIR="$scratch"',
        '  export APPTAINER_CONFIGDIR="$scratch/config"',
        '  export APPTAINER_CACHEDIR="$scratch/cache"',
        "  export APPTAINER_DISABLE_CACHE=true",
        '  mkdir -p -- "$APPTAINER_CONFIGDIR" "$APPTAINER_CACHEDIR"',
        "  unset APPTAINER_DOCKER_PASSWORD APPTAINER_DOCKER_USERNAME",
        "  unset DOCKER_PASSWORD DOCKER_USERNAME",
        '  if [ -f "$image_token" ] && [ ! -L "$image_token" ]; then',
        '    export APPTAINER_DOCKER_PASSWORD="$(cat -- "$image_token")"',
        "    export APPTAINER_DOCKER_USERNAME=" + q(settings.registry_user or ""),
        "  fi",
        '  apptainer pull --disable-cache "$scratch/image.sif" "$ref" 2>&1 |',
        # The redactor inherits the credential, never an argv secret.
        '    awk \'BEGIN { secret=ENVIRON["APPTAINER_DOCKER_PASSWORD"] }',
        '      { line=$0; if (length(secret)) { out=""; while (i=index(line,secret)) {',
        '          out=out substr(line,1,i-1) "[redacted]"; line=substr(line,i+length(secret));',
        '        } line=out line; } print line; }\' >> "$image_log"',
        '  [ -s "$scratch/image.sif" ] || {',
        '    printf "Runtime did not create an image.\\n" >> "$image_log"; exit 1;',
        "  }",
        '  [ ! -L "$target" ] || exit 43',
        '  mv -n -T -- "$scratch/image.sif" "$target"',
        '  [ -f "$target" ] && [ -s "$target" ]',
        ")",
    ]
    for image in launch.images:
        ref = image["ref"]
        uri = ref if ref.startswith("docker://") else "docker://" + ref
        lines.append(shlex.join(["pull_image", image["path"], uri]))
    lines += [
        'tmp=$(mktemp "$image_sf/.images-env-XXXXXXXX")',
        "printf %s " + q(assignments) + ' > "$tmp"',
        'mv -T -- "$tmp" "$image_sf/images.sh"',
        assignments,
        'tmp=$(mktemp "$image_sf/.image-status-XXXXXXXX")',
        'printf "ready\\n" > "$tmp"; mv -T -- "$tmp" "$image_status"',
        'rm -f -- "$image_token"',
        "trap - EXIT TERM INT",
    ]
    return "\n".join(lines)


def observation_command(launch: Launch) -> str | None:
    if not launch.images:
        return None
    script = """set -eu
root=$(realpath -m -- "$1")
target=$(realpath -m -- "$2")
[ "$root" = "$1" ] && [ "$target" = "$2" ] || exit 43
if [ -e "$target" ]; then head -c 66000 -- "$target"; fi
"""
    return shlex.join(
        ["sh", "-c", script, "image-status", launch.workdir + "/.sf", status_path(launch)]
    )


def parse_status(content: str) -> dict[str, str] | None:
    if not content:
        return None
    status, _, error = content.partition("\n")
    if status not in ("preparing", "ready", "failed"):
        raise ObservationError("malformed", "Malformed image preparation status")
    return {"status": status, "error": error.strip() if status == "failed" else ""}


async def observe_images(reader: RecordReader, launch: Launch) -> dict[str, str] | None:
    command = observation_command(launch)
    return parse_status(await reader.execute(command)) if command else None
