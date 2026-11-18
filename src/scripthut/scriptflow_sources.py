"""Git-backed ScriptFlow working directories and text-only flow discovery."""

from __future__ import annotations

import hashlib
import re
import shlex
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote

from scripthut.config_schema import GitSourceConfig, ScriptHutConfig
from scripthut.scriptflow import RecordReader, ReferenceStore, absolute_path
from scripthut.sources.delivery import SourceDelivery
from scripthut.sources.git import GitSourceManager, is_safe_branch_name
from scripthut.ssh.client import SSHClient

if TYPE_CHECKING:
    from scripthut.scriptflow_launch import Launch

q = shlex.quote


@dataclass
class Revision:
    source: GitSourceConfig
    branch: str
    commit: str
    tip: str

    @property
    def detached(self) -> bool:
        return self.commit != self.tip


def source_config(config: ScriptHutConfig, name: str) -> GitSourceConfig:
    source = next((s for s in config.sources if s.name == name), None)
    if not isinstance(source, GitSourceConfig):
        raise ValueError(f"Source {name!r} is not a configured git source")
    return source


def select_backend(config: ScriptHutConfig, name: str | None) -> str:
    if name:
        if config.get_backend(name) is None:
            raise ValueError(f"Unknown backend: {name}")
        return name
    if len(config.backends) != 1:
        raise ValueError("Select --backend when there is not exactly one configured backend")
    return config.backends[0].name


async def resolve_revision(
    config: ScriptHutConfig, source: GitSourceConfig, branch: str | None, commit: str | None
) -> Revision:
    branch = branch or source.branch
    if not is_safe_branch_name(branch):
        raise ValueError(f"Invalid branch name: {branch!r}")
    if commit and not re.fullmatch(r"[a-fA-F0-9]{7,40}", commit):
        raise ValueError("--commit must be a Git commit SHA")
    manager = GitSourceManager(config.settings.sources_cache_dir_resolved)
    local = source.local_path_resolved
    if local:
        tip, error, code = await manager._run_git(
            ["rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}"], cwd=local
        )
    else:
        tip, error, code = await manager._run_git(
            ["ls-remote", "--exit-code", source.url, f"refs/heads/{branch}"],
            deploy_key=source.deploy_key_resolved,
        )
        tip = tip.split()[0] if tip else ""
    if code or not re.fullmatch(r"[a-f0-9]{40}", tip):
        raise ValueError(f"Cannot resolve source {source.name} branch {branch}: {error}")
    resolved = tip
    if commit:
        if local:
            resolved, error, code = await manager._run_git(
                ["rev-parse", "--verify", f"{commit}^{{commit}}"], cwd=local
            )
            if code:
                raise ValueError(f"Cannot resolve commit {commit}: {error}")
        elif len(commit) != 40:
            raise ValueError("Remote sources require a full 40-character --commit SHA")
        else:
            resolved = commit.lower()
    return Revision(source, branch, resolved, tip)


async def remote_path(reader: RecordReader, path: str) -> str:
    if path.startswith("~/"):
        home = (await reader.execute("printf '%s' \"$HOME\"")).strip()
        path = home + path[1:]
    return absolute_path(path)


async def workdir_for(
    reader: RecordReader, source: GitSourceConfig, commit: str | None = None
) -> str:
    path = f"{source.clone_dir}/{commit[:12]}" if commit else source.working_directory
    return await remote_path(reader, path)


def flow_workdir(workdir: str, source: GitSourceConfig) -> str:
    return str(PurePosixPath(workdir) / source.flow_dir)


def flow_names(text: str) -> list[str]:
    return list(
        dict.fromkeys(re.findall(r"^(?:async\s+)?def\s+flow_([A-Za-z0-9_]+)\s*\(", text, re.M))
    )


class SourceCheckout(SourceDelivery):
    async def checked(self, reader: RecordReader, command: str) -> str:
        out, error, code = await reader.transport.run_command(command, timeout=600)
        if code:
            raise ValueError(error.strip() or f"Git source command failed (exit {code})")
        return out

    async def origin(self, reader: RecordReader, source: GitSourceConfig) -> str:
        if source.local_path is not None:
            root = await remote_path(reader, source.clone_dir)
            local = source.local_path_resolved
            assert local is not None
            identity = hashlib.sha256(str(local.resolve()).encode()).hexdigest()[:12]
            return f"{root}/.sources/source-{quote(source.name, safe='')}-{identity}/.mirror.git"
        return source.url if source.deploy_key else self._to_https_url(source.url)

    async def inspect_checkout(
        self, reader: RecordReader, directory: str, origin: str
    ) -> dict[str, Any]:
        script = r"""set -eu
export GIT_OPTIONAL_LOCKS=0
if [ ! -e "$1" ] && [ ! -L "$1" ]; then printf 'absent\n'; exit; fi
[ -e "$1/.git" ] || { echo "$1: not a repository checkout" >&2; exit 1; }
[ "$(git -C "$1" remote get-url origin)" = "$2" ] || {
    echo "$1: not this source's repository" >&2; exit 1;
}
dirty=$(git -C "$1" status --porcelain --untracked-files=no) || {
    echo "$1: cannot inspect tracked files" >&2; exit 1;
}
[ -z "$dirty" ] || { echo "$1: modified tracked files" >&2; exit 1; }
printf 'present\n'
git -C "$1" rev-parse HEAD
if branch=$(git -C "$1" symbolic-ref -q --short HEAD); then printf '%s\n' "$branch";
else rc=$?; [ "$rc" = 1 ] || exit "$rc"; fi
"""
        text = await self.checked(
            reader, shlex.join(["sh", "-c", script, "source-check", directory, origin])
        )
        fields = text.splitlines()
        return {
            "present": fields[0] == "present",
            "head": fields[1] if len(fields) > 1 else None,
            "branch": fields[2] if len(fields) > 2 else None,
        }

    async def check_runs(
        self, reader: RecordReader, backend: str, directory: str, runs: str
    ) -> None:
        from scripthut.scriptflow_launch import discover

        exists = await self.checked(
            reader, f"if [ -e {q(runs)} ] || [ -L {q(runs)} ]; then printf yes; fi"
        )
        if not exists:
            return
        entries = await discover(
            reader,
            ReferenceStore(self.config.settings.data_dir_resolved / "scriptflow"),
            backend,
            directory,
            runs,
            False,
        )
        for entry in entries:
            workflow = entry["workflow"]
            if workflow is None:
                raise ValueError(
                    f"{directory}: cannot verify run {entry['run_id']}: "
                    f"{entry['observation']['status']}"
                )
            if workflow["state"] == "running":
                stale = " (stale heartbeat)" if entry["observation"]["stale"] else ""
                raise ValueError(
                    f"{directory}: running ScriptFlow run {entry['run_id']}{stale}; "
                    f"resolve the run or run scriptflow cleanup {entry['run_id']} on a compute node"
                )

    async def prepare(self, reader: RecordReader, launch: Launch, revision: Revision) -> None:
        source = revision.source
        branch_ref = q("refs/heads/" + revision.branch)
        commit_arg = q(revision.commit)
        directory = launch.repository_workdir
        assert directory is not None
        transport = cast(SSHClient, reader.transport)
        origin = await self.origin(reader, source)
        before = await self.inspect_checkout(reader, directory, origin)
        await self.check_runs(reader, launch.backend, directory, launch.runs_dir)
        default_runs = flow_workdir(directory, source) + "/.sf/runs"
        if default_runs != launch.runs_dir:
            await self.check_runs(reader, launch.backend, directory, default_runs)
        if before["present"] and before["head"] == revision.commit:
            launch.detached = before["branch"] is None
            await self.check_flow_dir(reader, directory, source)
            return
        remote_key = None
        stage = None
        try:
            if source.local_path is not None:
                mirror_source = source.model_copy(
                    update={"clone_dir": origin.rsplit("/", 1)[0], "branch": revision.branch}
                )
                await self._push_local_repo(
                    transport,
                    mirror_source,
                    launch.backend,
                    commit=revision.commit,
                    objects_only=True,
                )
            if source.deploy_key and source.local_path is None:
                remote_key = await self._upload_deploy_key(transport, source.deploy_key)
            prefix = await self._build_remote_git_ssh_command(
                transport,
                remote_key,
                await remote_path(reader, source.clone_dir),
                quoted_paths=True,
            )
            stage = (await self.checked(reader, "mktemp -d /tmp/scripthut-source-XXXXXXXX")).strip()
            repo = stage + "/repo"
            await self.checked(
                reader,
                f"{prefix}GIT_TERMINAL_PROMPT=0 git clone --bare --no-hardlinks -- "
                f"{q(origin)} {q(repo)}",
            )
            await self.checked(
                reader, f"git -C {q(repo)} cat-file -e {q(revision.commit + '^{commit}')} "
            )
            if before["present"]:
                await self.checked(reader, f"git -C {q(repo)} fetch -- {q(directory)} HEAD")
                heads = [before["head"]]
                if not revision.detached:
                    branch_head = (
                        await self.checked(
                            reader,
                            f"if git -C {q(directory)} show-ref --verify --quiet {branch_ref}; "
                            f"then git -C {q(directory)} rev-parse --verify {branch_ref}; "
                            'else rc=$?; [ "$rc" = 1 ] || exit "$rc"; fi',
                        )
                    ).strip()
                    if branch_head:
                        heads.append(branch_head)
                        await self.checked(
                            reader, f"git -C {q(repo)} fetch -- {q(directory)} {branch_ref}"
                        )
                    for head in heads:
                        _, _, code = await reader.transport.run_command(
                            f"git -C {q(repo)} merge-base --is-ancestor {q(head)} {commit_arg}"
                        )
                        if code:
                            raise ValueError(
                                f"{directory}: cannot fast-forward; "
                                "push or reconcile commits on the cluster"
                            )
                untracked = await self.checked(
                    reader, f"git -C {q(directory)} ls-files --others -z"
                )
                changed = ""
                for head in heads:
                    changed += await self.checked(
                        reader,
                        f"git -C {q(repo)} diff --name-only -z {q(head)} {commit_arg}",
                    )
                for path in filter(None, untracked.split("\0")):
                    if any(
                        path == other
                        or path.startswith(other + "/")
                        or other.startswith(path + "/")
                        for other in filter(None, changed.split("\0"))
                    ):
                        raise ValueError(
                            f"{directory}: checkout would overwrite untracked path {path}"
                        )
            # Target-tree checks precede checkout mutations.
            await self.checked(
                reader,
                f"mkdir -- {q(stage + '/tree')} && "
                f"git -C {q(repo)} archive {commit_arg} | tar -x -C {q(stage + '/tree')}",
            )
            await self.check_flow_dir(reader, stage + "/tree", source)
            again = await self.inspect_checkout(reader, directory, origin)
            if again != before:
                raise ValueError(f"{directory}: checkout changed during preparation")
            await self.check_runs(reader, launch.backend, directory, launch.runs_dir)
            if not before["present"]:
                await self.checked(
                    reader,
                    f"git -C {q(repo)} update-ref {branch_ref} {commit_arg} && "
                    f"git -C {q(repo)} symbolic-ref HEAD {branch_ref} && "
                    f"git clone --no-checkout -- {q(repo)} {q(directory)} && "
                    f"git -C {q(directory)} remote set-url origin {q(origin)}",
                )
            else:
                await self.checked(
                    reader,
                    f"git -C {q(directory)} fetch --no-write-fetch-head -- {q(repo)} {commit_arg}",
                )
            if revision.detached:
                command = (
                    f"git -C {q(directory)} checkout --no-overwrite-ignore --detach {commit_arg}"
                )
            else:
                command = (
                    f"if git -C {q(directory)} show-ref --verify --quiet {branch_ref}; "
                    f"then git -C {q(directory)} checkout --no-overwrite-ignore "
                    f"{q(revision.branch)}; "
                    f"else git -C {q(directory)} checkout --no-overwrite-ignore "
                    f"-b {q(revision.branch)} {commit_arg}; fi && "
                    f"git -C {q(directory)} merge --ff-only {commit_arg} && "
                    f"git -C {q(directory)} config "
                    f"{q('branch.' + revision.branch + '.remote')} origin && "
                    f"git -C {q(directory)} config "
                    f"{q('branch.' + revision.branch + '.merge')} {branch_ref}"
                )
            await self.checked(reader, command)
            if source.local_path is not None and not revision.detached:
                await self.checked(
                    reader,
                    f"git -C {q(origin)} update-ref {branch_ref} {commit_arg}",
                )
            if not before["present"] and source.postclone:
                await self.checked(reader, f"cd -- {q(directory)} && {source.postclone}")
        except ValueError as exc:
            raise ValueError(f"{directory}: {exc}") from exc
        finally:
            if remote_key:
                await self._cleanup_deploy_key(transport, remote_key)
            if stage:
                await self.checked(reader, f"rm -rf -- {q(stage)}")

    async def check_flow_dir(
        self, reader: RecordReader, directory: str, source: GitSourceConfig
    ) -> None:
        script = """set -eu
root=$(realpath -e -- "$1")
flow=$(realpath -e -- "$1/$2")
case "$flow/" in "$root/"*) ;; *) echo "$1: flow_dir escapes repository" >&2; exit 1;; esac
file=$(realpath -e -- "$flow/sflow.py")
case "$file/" in "$root/"*) ;; *) echo "$1: sflow.py escapes repository" >&2; exit 1;; esac
[ -d "$flow" ] && [ -f "$flow/sflow.py" ] || {
    echo "$1: missing sflow.py in flow_dir" >&2; exit 1;
}
"""
        await self.checked(
            reader, shlex.join(["sh", "-c", script, "source-flow", directory, source.flow_dir])
        )


async def list_flows(
    config: ScriptHutConfig, revision: Revision, reader: RecordReader | None = None
) -> list[str]:
    source = revision.source
    path = str(PurePosixPath(source.flow_dir) / "sflow.py")
    if source.local_path_resolved:
        manager = GitSourceManager(config.settings.sources_cache_dir_resolved)
        text, error, code = await manager._run_git(
            ["show", f"{revision.commit}:{path}"], cwd=source.local_path_resolved
        )
        if code:
            raise ValueError(f"{source.name}: missing or unreadable {path}: {error}")
    else:
        if reader is None:
            raise ValueError("Backend connection is unavailable")
        directory = await workdir_for(reader, source)
        text = await SourceCheckout(config).checked(
            reader, f"git -C {q(directory)} show {q(revision.commit + ':' + path)}"
        )
    return flow_names(text)


async def directory_present(reader: RecordReader, directory: str) -> bool:
    script = """if [ -d "$1" ]; then exit 0; fi
if [ -e "$1" ] || [ -L "$1" ]; then echo "Not a directory: $1" >&2; exit 1; fi
parent=$(dirname -- "$1")
while [ ! -e "$parent" ] && [ "$parent" != / ]; do parent=$(dirname -- "$parent"); done
[ -d "$parent" ] && [ -x "$parent" ] || { echo "Cannot access: $1" >&2; exit 1; }
exit 3
"""
    _, error, code = await reader.transport.run_command(
        shlex.join(["sh", "-c", script, "source-directory", directory])
    )
    if code not in (0, 3):
        raise ValueError(error.strip() or f"Cannot observe directory: {directory}")
    return code == 0


async def source_rows(config: ScriptHutConfig, backends: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in config.sources:
        if not isinstance(source, GitSourceConfig):
            rows.append({"config": source})
            continue
        manager = GitSourceManager(config.settings.sources_cache_dir_resolved)
        manager.add_source(source)
        local_path = manager._get_source_path(source.name)
        local_source = source.model_copy(update={"local_path": local_path})
        has_local = source.local_path is not None or local_path.is_dir()
        local_flows = []
        local_error = None
        if has_local:
            try:
                revision = await resolve_revision(config, local_source, None, None)
                local_flows = await list_flows(config, revision)
            except (ValueError, OSError, RuntimeError) as exc:
                local_error = str(exc)
        for name in list(backends) or [None]:
            row: dict[str, Any] = {
                "config": source,
                "backend": name,
                "workdir": source.working_directory,
                "flow_dir": source.flow_dir,
                "flows": local_flows,
                "error": local_error,
                "status": "available",
            }
            try:
                if name is None and has_local:
                    rows.append(row)
                    continue
                connection = backends.get(name) if name else None
                if (
                    not connection or not connection.ssh_client or not connection.enabled
                    or (
                        getattr(connection, "status", None) is not None
                        and not connection.status.connected
                    )
                ):
                    raise ValueError("Backend copy unavailable; select a connected backend")
                reader = RecordReader(connection.ssh_client)
                directory = await workdir_for(reader, source)
                row["workdir"] = directory
                if not await directory_present(reader, directory):
                    row["status"] = "not_delivered"
                elif not has_local:
                    commit = (
                        await SourceCheckout(config).checked(
                            reader, f"git -C {q(directory)} rev-parse --verify "
                            f"{q('refs/heads/' + source.branch + '^{commit}')}",
                        )
                    ).strip()
                    row["flows"] = await list_flows(
                        config, Revision(source, source.branch, commit, commit), reader
                    )
            except (ValueError, OSError, RuntimeError) as exc:
                row["error"] = str(exc)
            rows.append(row)
    return rows
