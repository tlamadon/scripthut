"""Git source delivery independent of workflow execution."""
from __future__ import annotations

import asyncio
import base64
import logging
import shlex
from pathlib import Path

from scripthut.config_schema import GitSourceConfig, ScriptHutConfig
from scripthut.sources.git import is_safe_branch_name
from scripthut.ssh.client import SSHClient
from scripthut.ssh.git_ssh import build_git_ssh_command, local_openssh_version

logger = logging.getLogger(__name__)
_MIRROR_DIRNAME = ".mirror.git"

async def _run_local_shell(command: str, timeout: float = 60.0) -> tuple[str, str, int]:
    """Run ``command`` locally (``sh -c``) and return ``(stdout, stderr, exit_code)``.

    Used for workflows whose backend has no SSH connection (e.g. AWS Batch) —
    the command runs on the scripthut host instead of a remote login node.
    """
    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise ValueError(f"Local command timed out after {timeout}s: {command}")
    return (
        stdout_b.decode("utf-8", errors="replace"),
        stderr_b.decode("utf-8", errors="replace"),
        proc.returncode if proc.returncode is not None else -1,
    )



class SourceDelivery:
    """Repository delivery operations with configuration and transport inputs."""

    def __init__(self, config: ScriptHutConfig) -> None:
        self.config = config

    _source_shell = staticmethod(_run_local_shell)

    async def _get_git_root(self, ssh_client: SSHClient, working_dir: str) -> str:
        """Detect the git repository root for a working directory on the backend."""
        stdout, stderr, exit_code = await ssh_client.run_command(
            f"cd {working_dir} && git rev-parse --show-toplevel"
        )
        if exit_code != 0:
            raise ValueError(
                f"'{working_dir}' is not inside a git repository. "
                f"ScriptHut requires workflows to live in git repos."
            )
        return stdout.strip()

    @staticmethod
    def _to_https_url(repo: str) -> str:
        """Convert git SSH URLs to HTTPS when possible.

        e.g. git@github.com:org/repo.git -> https://github.com/org/repo.git
        """
        if repo.startswith("git@"):
            # git@github.com:org/repo.git -> https://github.com/org/repo.git
            host_path = repo[4:]  # strip "git@"
            host, _, path = host_path.partition(":")
            if path:
                return f"https://{host}/{path}"
        return repo

    async def _build_remote_git_ssh_command(
        self,
        ssh_client: SSHClient,
        remote_key_path: str | None,
        clone_dir: str,
    ) -> str:
        """Build the GIT_SSH_COMMAND prefix for git commands run on a backend.

        The host-key policy is chosen from the *backend's* ssh version
        rather than hardcoded: ``accept-new`` aborts outright on the
        OpenSSH 7.4 that older cluster login nodes still ship. See
        :mod:`scripthut.ssh.git_ssh`.

        Learned host keys go next to the clones instead of into
        ``~/.ssh/known_hosts``, because a cluster home is often
        read-only, over quota, or shared across nodes in ways that make
        writing there fail or race.

        Args:
            ssh_client: Connected client for the backend that will run git.
            remote_key_path: Path to the deploy key on the backend, or None.
            clone_dir: Where clones live; the known_hosts file sits beside them.

        Returns:
            Shell prefix to prepend to a git command, or an empty string
            when no deploy key is in play (those clones use HTTPS and
            never touch ssh).
        """
        if not remote_key_path:
            return ""
        command = build_git_ssh_command(
            key_path=remote_key_path,
            known_hosts=f"{clone_dir}/.known_hosts",
            openssh_version=await ssh_client.openssh_version(),
            # clone_dir may still be `~/...`; quoting would stop the remote
            # shell expanding it and create a directory literally named `~`.
            quote_paths=False,
        )
        # ssh writes the known_hosts file but will not create its directory.
        return f'mkdir -p {clone_dir}; export GIT_SSH_COMMAND="{command}"; '

    async def _upload_deploy_key(
        self, ssh_client: SSHClient, local_key_path: Path
    ) -> str:
        """Upload a local deploy key to a temp file on the backend.

        The key content is base64-encoded before transmission over the
        encrypted SSH channel, then decoded into a temp file with 600
        permissions.

        Returns:
            The remote temp file path.
        """
        resolved = local_key_path.expanduser()
        if not resolved.exists():
            raise ValueError(f"Deploy key not found: {resolved}")
        key_content = resolved.read_text()
        key_b64 = base64.b64encode(key_content.encode()).decode()
        cmd = (
            "TMPKEY=$(mktemp /tmp/scripthut_key_XXXXXX) && "
            f"echo '{key_b64}' | base64 -d > $TMPKEY && "
            "chmod 600 $TMPKEY && echo $TMPKEY"
        )
        stdout, stderr, exit_code = await ssh_client.run_command(cmd)
        if exit_code != 0:
            raise ValueError(f"Failed to upload deploy key: {stderr}")
        return stdout.strip()

    async def _cleanup_deploy_key(
        self, ssh_client: SSHClient, remote_key_path: str
    ) -> None:
        """Remove a temporary deploy key from the backend."""
        await ssh_client.run_command(f"rm -f {remote_key_path}")

    async def _clone_git_repo(
        self,
        ssh_client: SSHClient,
        *,
        repo: str,
        branch: str,
        deploy_key: Path | None,
        clone_dir: str,
        postclone: str | None,
    ) -> tuple[str, str]:
        """Clone a git repo on the backend if not already present.

        Steps:
            1. Upload the local deploy key (if any) to a temp file on the backend.
            2. ``git ls-remote`` to resolve the branch HEAD commit hash.
            3. Clone into ``<clone_dir>/<short_hash>/`` if absent.
            4. Clean up the temp deploy key.

        Returns:
            ``(clone_path, short_hash)`` — the remote clone directory and the
            12-char commit hash prefix.
        """
        remote_key: str | None = None
        try:
            # 1. Upload deploy key if configured
            if deploy_key is not None:
                key_path = deploy_key.expanduser()
                remote_key = await self._upload_deploy_key(
                    ssh_client, key_path
                )

            git_ssh = await self._build_remote_git_ssh_command(
                ssh_client, remote_key, clone_dir
            )
            # Use HTTPS for public repos (no deploy key) to avoid SSH key issues
            effective_repo = repo if remote_key else self._to_https_url(repo)

            # 2. Resolve HEAD commit hash
            cmd = f"{git_ssh}GIT_TERMINAL_PROMPT=0 git ls-remote {effective_repo} refs/heads/{branch}"
            stdout, stderr, exit_code = await ssh_client.run_command(cmd, timeout=30)
            if exit_code != 0 or not stdout.strip():
                raise ValueError(
                    f"Failed to resolve branch '{branch}' from "
                    f"'{repo}': {stderr}"
                )
            commit_hash = stdout.split()[0]
            short_hash = commit_hash[:12]

            # 3. Clone if not already present
            clone_path = f"{clone_dir}/{short_hash}"
            stdout, _, _ = await ssh_client.run_command(
                f"test -d {clone_path} && echo exists"
            )
            if "exists" not in stdout:
                logger.info(
                    f"Cloning {repo}@{branch} ({short_hash}) "
                    f"to {clone_path}"
                )
                cmd = (
                    f"{git_ssh}GIT_TERMINAL_PROMPT=0 git clone --branch {branch} "
                    f"--single-branch --depth 1 {effective_repo} {clone_path}"
                )
                _, stderr, exit_code = await ssh_client.run_command(
                    cmd, timeout=300
                )
                if exit_code != 0:
                    raise ValueError(f"Git clone failed: {stderr}")

                # Run postclone command if configured
                if postclone:
                    logger.info(f"Running postclone command in {clone_path}")
                    cmd = f"cd {clone_path} && {postclone}"
                    _, stderr, exit_code = await ssh_client.run_command(
                        cmd, timeout=300
                    )
                    if exit_code != 0:
                        raise ValueError(f"Postclone command failed: {stderr}")
            else:
                logger.info(
                    f"Reusing existing clone at {clone_path} ({short_hash})"
                )

            return clone_path, short_hash
        finally:
            # 4. Always clean up the temp deploy key
            if remote_key is not None:
                await self._cleanup_deploy_key(ssh_client, remote_key)

    def _local_push_target(
        self, source: GitSourceConfig, backend_name: str,
    ) -> tuple[str, str]:
        """Where to push a local repo for ``backend_name``, and how.

        Returns ``(push_url, env_prefix)``. The whole local-vs-SSH
        difference lives here: a local backend is a filesystem path with
        no ssh in the picture, an SSH backend is an scp-style URL plus a
        ``GIT_SSH_COMMAND``. Every other step runs through
        ``ssh_client.run_command``, which ``LocalExecClient`` already
        makes transport-transparent.

        Raises:
            ValueError: the backend has no filesystem to push to (the
                API-only backends), or is unknown.
        """
        from scripthut.config_schema import LocalBackendConfig

        backend_cfg = self.config.get_backend(backend_name)
        if backend_cfg is None:
            raise ValueError(f"Unknown backend: {backend_name}")

        mirror = f"{source.clone_dir}/{_MIRROR_DIRNAME}"

        if isinstance(backend_cfg, LocalBackendConfig):
            # Pushing to a path, not a host: git hands it to no shell, so
            # `~` has to be expanded here rather than left for the far end.
            return str(Path(mirror).expanduser()), ""

        ssh_cfg = getattr(backend_cfg, "ssh", None)
        if ssh_cfg is None:
            raise ValueError(
                f"Source '{source.name}' has local_path set, but backend "
                f"'{backend_name}' ({backend_cfg.type}) has no filesystem to "
                "push a repo to. Use an SSH or local backend, or give the "
                "source a 'url' so the backend can clone it itself."
            )

        # known_hosts: the backend's own file when configured, otherwise the
        # same cache-dir file the server-side source clones learn into.
        known_hosts = ssh_cfg.known_hosts_resolved
        if known_hosts is None:
            known_hosts = (
                self.config.settings.sources_cache_dir_resolved / "known_hosts"
            )
            known_hosts.parent.mkdir(parents=True, exist_ok=True)
        git_ssh = build_git_ssh_command(
            key_path=str(ssh_cfg.key_path_resolved),
            known_hosts=str(known_hosts),
            openssh_version=local_openssh_version(),
            port=ssh_cfg.port if ssh_cfg.port != 22 else None,
        )
        # scp-style URL: git passes the path through the far-side shell, so a
        # leading `~` expands there.
        push_url = f"{ssh_cfg.user}@{ssh_cfg.host}:{mirror}"
        return push_url, f'GIT_SSH_COMMAND="{git_ssh}" '

    async def _push_local_repo(
        self,
        ssh_client: SSHClient,
        source: GitSourceConfig,
        backend_name: str,
    ) -> tuple[str, str]:
        """Materialise a local git repo on the backend by pushing to it.

        The counterpart to :meth:`_clone_git_repo` for sources that carry a
        ``local_path``: instead of the backend fetching from a git remote
        (and needing a deploy key to do it), the commit travels over the
        connection scripthut already holds.

            1. Resolve the commit locally — no ``git ls-remote``.
            2. Ensure a bare mirror at ``<clone_dir>/.mirror.git``.
            3. Push ``<sha>`` there as ``refs/heads/sh-<short_hash>``.
            4. Clone that ref into ``<clone_dir>/<short_hash>`` if absent.

        The mirror is shared by every run of this source, so only the first
        push carries history; later ones send deltas. Step 4 is a local
        clone on the backend, so git hardlinks the objects.

        Returns:
            ``(clone_path, short_hash)`` — same contract as
            :meth:`_clone_git_repo`.
        """
        local_path = source.local_path_resolved
        if local_path is None:  # pragma: no cover — guarded by the caller
            raise ValueError(f"Source '{source.name}' has no local_path")
        if not local_path.exists():
            raise ValueError(
                f"Source '{source.name}' local_path does not exist: {local_path}"
            )

        repo = shlex.quote(str(local_path))

        # 1. Resolve the commit from the local repo.
        rev = "HEAD" if not source.branch else f"refs/heads/{source.branch}"
        if source.branch and not is_safe_branch_name(source.branch):
            raise ValueError(f"Invalid branch name: {source.branch!r}")
        stdout, stderr, code = await self._source_shell(
            f"git -C {repo} rev-parse --verify {rev}", timeout=30.0,
        )
        if code != 0 or not stdout.strip():
            raise ValueError(
                f"Could not resolve '{rev}' in local repo {local_path} for "
                f"source '{source.name}': {stderr.strip() or 'rev-parse failed'}"
            )
        commit_hash = stdout.strip().split("\n")[0]
        short_hash = commit_hash[:12]

        # Uncommitted work is not what runs — say so rather than let the
        # difference go unnoticed.
        dirty, _, _ = await self._source_shell(
            f"git -C {repo} status --porcelain", timeout=30.0,
        )
        if dirty.strip():
            logger.warning(
                f"Source '{source.name}': working tree at {local_path} has "
                f"uncommitted changes; submitting committed {short_hash}"
            )

        push_url, env_prefix = self._local_push_target(source, backend_name)
        mirror = f"{source.clone_dir}/{_MIRROR_DIRNAME}"
        clone_path = f"{source.clone_dir}/{short_hash}"
        ref = f"refs/heads/sh-{short_hash}"

        # 2. Ensure the bare mirror exists on the backend.
        _, stderr, code = await ssh_client.run_command(
            f"mkdir -p {source.clone_dir} && "
            f"{{ test -d {mirror} || git init --bare -q {mirror}; }}",
            timeout=60,
        )
        if code != 0:
            raise ValueError(
                f"Could not create mirror {mirror} on '{backend_name}': {stderr}"
            )

        # 3. Push the commit. --force because the ref is content-addressed:
        # re-pushing the same commit is a no-op, and there is nothing to
        # preserve if it somehow differs.
        logger.info(
            f"Pushing {local_path}@{short_hash} to {backend_name}:{mirror}"
        )
        _, stderr, code = await self._source_shell(
            f"{env_prefix}git -C {repo} push --force "
            f"{shlex.quote(push_url)} {commit_hash}:{ref}",
            timeout=1800.0,
        )
        if code != 0:
            raise ValueError(
                f"Failed to push {local_path} to '{backend_name}' "
                f"({push_url}): {stderr.strip()}"
            )

        # 4. Clone that ref out of the mirror, unless this commit is already
        # materialised (content-addressed, so re-runs reuse it).
        stdout, _, _ = await ssh_client.run_command(
            f"test -d {clone_path} && echo exists"
        )
        if "exists" in stdout:
            logger.info(f"Reusing existing clone at {clone_path} ({short_hash})")
            return clone_path, short_hash

        _, stderr, code = await ssh_client.run_command(
            f"git clone -q --branch sh-{short_hash} --single-branch "
            f"{mirror} {clone_path}",
            timeout=600,
        )
        if code != 0:
            raise ValueError(
                f"Failed to clone {mirror} to {clone_path} on "
                f"'{backend_name}': {stderr}"
            )

        if source.postclone:
            logger.info(f"Running postclone command in {clone_path}")
            _, stderr, code = await ssh_client.run_command(
                f"cd {clone_path} && {source.postclone}", timeout=300,
            )
            if code != 0:
                raise ValueError(f"Postclone command failed: {stderr}")

        return clone_path, short_hash

    async def _ls_remote_commit(
        self, repo: str, branch: str,
    ) -> str | None:
        """Resolve a remote git ref to a commit hash via local ``git ls-remote``.

        Used by API-only backends (Batch) so they can pass a concrete SHA to
        their container's runtime clone step.  Returns None on failure; the
        container will fall back to the branch name.
        """
        cmd = f"git ls-remote {repo} {branch}"
        try:
            stdout, stderr, code = await self._source_shell(cmd, timeout=30.0)
        except Exception as e:
            logger.warning(f"Local git ls-remote failed: {e}")
            return None
        if code != 0:
            logger.warning(f"Local git ls-remote exit {code}: {stderr.strip()}")
            return None
        line = stdout.strip().split("\n", 1)[0]
        return line.split()[0] if line else None
