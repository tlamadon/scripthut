"""Task-level result caching against an object store (S3-compatible).

The cache lets a task that declares ``outputs`` reuse a previous run's
artifacts instead of recomputing them. Two distinct hashes do the work,
Bazel-style:

* The **input hash** (a.k.a. action hash) is computed *before* a task runs
  from its command, resolved environment, git commit, and the content
  hashes of its declared ``inputs``. It answers "have I done this work?"
  and is the lookup key into the *action cache* (``<store>/ac/<key>.json``).

* The **content hash** is computed *after* a task runs, over the tarball of
  its declared output paths. The blob is stored content-addressed in the
  *CAS* (``<store>/cas/<content_hash>.tar.gz``) so identical artifacts are
  uploaded once and shared across runs, branches, and clusters.

A small JSON **manifest** at ``ac/<key>.json`` bridges the two: it records
the content hash + blob URI to restore, the exit code (so failures are
never reused), and provenance for debugging.

All file hashing and artifact transfer happen *cluster-side over SSH* — the
authoritative copy of inputs/outputs lives on the backend filesystem, and
the object store is the durable, run-independent tier. The scripthut host
only orchestrates: it tells the backend to "hash these paths", "fetch this
manifest", "store these outputs", "restore this blob". This mirrors the
task-outputs feature, which is likewise SSH-only; API-only backends (Batch,
EC2 without SSH) simply don't cache.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import shlex
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from scripthut.config_schema import CacheConfig
    from scripthut.ssh.client import SSHClient

logger = logging.getLogger(__name__)

# Env keys excluded from the cache key. ``SCRIPTHUT_*`` are runtime metadata
# (run id, created-at timestamp, git sha/branch) — volatile or already
# captured separately by ``commit_hash`` — so folding them in would make
# every run a unique key and defeat the cache entirely.
_VOLATILE_ENV_PREFIX = "SCRIPTHUT_"

# Portable content hasher, resolved on the *executing* machine: prefer
# coreutils' ``sha256sum``, fall back to ``shasum -a 256`` (ships with
# macOS/BSD via Perl). Both print "<hex>  <path>", so parsing is identical
# either way. This must stay a shell-side resolution — hashing runs where
# the files live (the cluster over SSH, or the host for the local
# backend), so a Python-side hashlib can never stand in for the remote
# case and the pipeline has to find its own tool.
_SHA256_RESOLVE = (
    "if command -v sha256sum >/dev/null 2>&1; "
    "then _scripthut_sha=sha256sum; "
    "else _scripthut_sha='shasum -a 256'; fi"
)


class CacheManager:
    """Computes cache keys and moves artifacts between a backend and the store.

    Stateless aside from its config; every method that touches files takes an
    :class:`SSHClient` so the same manager serves every backend.
    """

    def __init__(self, config: CacheConfig) -> None:
        self.config = config

    @property
    def enabled(self) -> bool:
        """True when caching is switched on and a store URI is configured."""
        return bool(self.config.enabled and self.config.store)

    # --- Key computation (pure) -------------------------------------------

    @staticmethod
    def _env_for_key(env: dict[str, str]) -> dict[str, str]:
        """Drop volatile SCRIPTHUT_* keys; keep the user-meaningful env."""
        return {
            k: v for k, v in env.items()
            if not k.startswith(_VOLATILE_ENV_PREFIX)
        }

    def compute_key(
        self,
        *,
        command: str,
        env: dict[str, str],
        commit_hash: str | None,
        input_hashes: dict[str, str],
    ) -> str:
        """Return the input/action hash for a task.

        Deterministic across machines: identical (command, non-volatile env,
        commit, per-input content hashes) → identical key. Resource knobs
        (cpus/memory/partition) are deliberately excluded — resizing a job
        must not bust the cache.
        """
        payload = json.dumps(
            {
                "v": 1,
                "command": command,
                "env": self._env_for_key(env),
                "commit": commit_hash or "",
                "inputs": input_hashes,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    # --- Store-URI / command helpers --------------------------------------

    def _uri(self, *parts: str) -> str:
        """Join the configured store base with ``parts`` using '/'."""
        base = self.config.store.rstrip("/")  # type: ignore[union-attr]
        return "/".join([base, *(p.strip("/") for p in parts)])

    # The four helpers below take shell tokens that are *already quoted* by
    # the caller (``shlex.quote(uri)`` for fixed URIs, or a double-quoted
    # ``"$VAR"`` when the value is a remote shell variable that must expand).

    def _cmd_cat(self, q_uri: str) -> str:
        """Command that writes the object at ``q_uri`` to stdout (nonzero if absent)."""
        if self.config.tool == "rclone":
            return f"rclone cat {q_uri}"
        return f"aws s3 cp {q_uri} -"

    def _cmd_put_stdin(self, q_uri: str) -> str:
        """Command that uploads stdin to ``q_uri``."""
        if self.config.tool == "rclone":
            return f"rclone rcat {q_uri}"
        return f"aws s3 cp - {q_uri}"

    def _cmd_put_file(self, q_local: str, q_uri: str) -> str:
        """Command that uploads the local file ``q_local`` to ``q_uri``."""
        if self.config.tool == "rclone":
            return f"rclone copyto {q_local} {q_uri}"
        return f"aws s3 cp {q_local} {q_uri}"

    def _cmd_exists(self, q_uri: str) -> str:
        """Command that exits 0 iff an object exists at ``q_uri``."""
        if self.config.tool == "rclone":
            return f"rclone lsf {q_uri}"
        return f"aws s3 ls {q_uri}"

    # --- Input hashing ----------------------------------------------------

    async def hash_inputs(
        self,
        ssh: SSHClient,
        working_dir: str,
        inputs: list[str],
    ) -> dict[str, str] | None:
        """Return ``{relative_path: sha256}`` for every file under ``inputs``.

        ``inputs`` are paths/globs relative to ``working_dir`` (absolute paths
        also work but make the key cluster-specific). Globs are expanded by
        the remote shell; directories are walked. Files are listed in a stable
        (sorted) order so the resulting map is deterministic.

        An empty ``inputs`` list returns ``{}`` (a valid, stable key
        component — the task is then keyed on command + env + commit only).
        Returns ``None`` if the remote command errors or any declared pattern
        matches nothing, so the caller treats it as "cannot verify inputs"
        and runs the task rather than risk a stale hit.
        """
        if not inputs:
            return {}

        # Patterns are inserted unquoted so the remote shell expands globs.
        # ``working_dir`` is likewise interpolated raw (not shlex-quoted) so
        # a ``~`` working dir expands — matching ``generate_script_body``'s
        # ``cd {working_dir}`` convention, which already assumes the path is
        # shell-safe. HPC paths with spaces are vanishingly rare.
        patterns = " ".join(inputs)
        # `find` walks dirs and resolves the expanded patterns; missing
        # patterns make the shell pass the literal through and `find` warns
        # on stderr (captured) and exits nonzero — surfaced below.
        cmd = (
            f"cd {working_dir} && "
            f"{_SHA256_RESOLVE} && "
            f"find {patterns} -type f -print0 2>/dev/null "
            f"| LC_ALL=C sort -z | xargs -0 -r $_scripthut_sha 2>/dev/null"
        )
        try:
            stdout, stderr, exit_code = await ssh.run_command(cmd, timeout=120)
        except Exception as e:  # noqa: BLE001 — never let hashing crash a submit
            logger.warning(f"cache: input hashing failed over SSH: {e}")
            return None
        if exit_code != 0:
            logger.info(
                f"cache: input hashing returned exit {exit_code} for "
                f"{inputs} in {working_dir}: {stderr.strip()[:200]}"
            )
            return None

        hashes: dict[str, str] = {}
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            # `sha256sum` prints "<hex>  <path>" (two spaces).
            parts = line.split(None, 1)
            if len(parts) != 2:
                continue
            digest, path = parts
            # Strip the leading "./" find adds for relative patterns so the
            # key is stable regardless of how the pattern was written.
            if path.startswith("./"):
                path = path[2:]
            hashes[path] = digest

        if not hashes:
            # Declared inputs that resolve to zero files is suspicious — the
            # data the task depends on isn't there. Don't cache on an empty
            # input set the user clearly didn't intend.
            logger.info(
                f"cache: declared inputs {inputs} matched no files in "
                f"{working_dir}; skipping cache for this task"
            )
            return None
        return hashes

    # --- Action-cache lookup ----------------------------------------------

    async def lookup(
        self, ssh: SSHClient, key: str
    ) -> dict[str, Any] | None:
        """Fetch and parse the manifest at ``ac/<key>.json``, or ``None`` on miss."""
        uri = self._uri("ac", f"{key}.json")
        cmd = f"{self._cmd_cat(shlex.quote(uri))} 2>/dev/null"
        try:
            stdout, _stderr, exit_code = await ssh.run_command(cmd, timeout=60)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"cache: manifest lookup failed for {key}: {e}")
            return None
        if exit_code != 0 or not stdout.strip():
            return None
        try:
            manifest = json.loads(stdout)
        except json.JSONDecodeError as e:
            logger.warning(f"cache: manifest at {uri} is not valid JSON: {e}")
            return None
        if not isinstance(manifest, dict) or "blob" not in manifest:
            logger.warning(f"cache: manifest at {uri} is malformed; ignoring")
            return None
        return manifest

    # --- Restore ----------------------------------------------------------

    async def restore(
        self, ssh: SSHClient, working_dir: str, manifest: dict[str, Any]
    ) -> bool:
        """Download the manifest's blob and extract it into ``working_dir``.

        Returns ``True`` on success. Any failure returns ``False`` so the
        caller falls back to running the task — a cache that can't deliver
        its artifacts must never silently leave the working dir half-populated.
        """
        blob = manifest.get("blob")
        if not blob:
            return False
        cmd = (
            "set -e\n"
            f"cd {working_dir}\n"
            'TMP=$(mktemp /tmp/scripthut_cache_XXXXXX.tar.gz)\n'
            f"{self._cmd_cat(shlex.quote(blob))} > \"$TMP\"\n"
            'tar xzf "$TMP" -C .\n'
            'rm -f "$TMP"\n'
        )
        try:
            _stdout, stderr, exit_code = await ssh.run_command(cmd, timeout=600)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"cache: restore failed over SSH: {e}")
            return False
        if exit_code != 0:
            logger.warning(
                f"cache: restore of {blob} into {working_dir} failed "
                f"(exit {exit_code}): {stderr.strip()[:200]}"
            )
            return False
        return True

    # --- Store ------------------------------------------------------------

    async def store(
        self,
        ssh: SSHClient,
        working_dir: str,
        *,
        key: str,
        outputs: list[str],
        meta: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Tar the declared ``outputs``, upload them to the CAS, and write the
        action-cache manifest at ``ac/<key>.json``.

        ``meta`` carries the provenance fields scripthut already knows
        (command, commit, run id, created-at, exit code); the content hash,
        blob URI and file list are filled in from the cluster-side build.
        Returns the stored manifest dict, or ``None`` if anything failed
        (storing is best-effort — a failed store must not fail the run).
        """
        if not outputs:
            return None

        patterns = " ".join(outputs)  # unquoted → remote shell expands globs
        # Round-trip A: build the tarball, content-hash it, upload the blob
        # to the CAS only if absent (dedup), and report the hash + members.
        # ``BLOB`` holds the CAS URI with the just-computed hash; it's a shell
        # variable, so the tool commands reference the double-quoted "$BLOB"
        # (which expands) rather than a shlex-quoted literal (which wouldn't).
        cas_dir = self._uri("cas")  # admin-controlled; safe to interpolate
        exists_blob = self._cmd_exists('"$BLOB"')
        put_blob = self._cmd_put_file('"$TMP"', '"$BLOB"')
        build = (
            "set -e\n"
            f"cd {working_dir}\n"
            f"{_SHA256_RESOLVE}\n"
            'TMP=$(mktemp /tmp/scripthut_cache_XXXXXX.tar.gz)\n'
            # Deterministic-ish tar: sort members so identical trees produce
            # identical archives across runs (helps CAS dedup).
            f"tar czf \"$TMP\" --sort=name {patterns} 2>/dev/null "
            f"|| tar czf \"$TMP\" {patterns}\n"
            'H=$($_scripthut_sha "$TMP" | cut -d" " -f1)\n'
            f'BLOB="{cas_dir}/$H.tar.gz"\n'
            f'if ! {exists_blob} >/dev/null 2>&1; then {put_blob}; fi\n'
            'echo "SCRIPTHUT_CACHE_HASH=$H"\n'
            'echo "SCRIPTHUT_CACHE_BLOB=$BLOB"\n'
            'echo "---MEMBERS---"\n'
            'tar tzf "$TMP"\n'
            'rm -f "$TMP"\n'
        )
        try:
            stdout, stderr, exit_code = await ssh.run_command(build, timeout=900)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"cache: store (build/upload) failed over SSH: {e}")
            return None
        if exit_code != 0:
            logger.warning(
                f"cache: store build for key {key} failed "
                f"(exit {exit_code}): {stderr.strip()[:200]}"
            )
            return None

        content_hash: str | None = None
        blob_uri: str | None = None
        members: list[str] = []
        in_members = False
        for line in stdout.splitlines():
            if line.startswith("SCRIPTHUT_CACHE_HASH="):
                content_hash = line.split("=", 1)[1].strip()
            elif line.startswith("SCRIPTHUT_CACHE_BLOB="):
                blob_uri = line.split("=", 1)[1].strip()
            elif line.strip() == "---MEMBERS---":
                in_members = True
            elif in_members:
                entry = line.rstrip()
                if entry and not entry.endswith("/"):  # skip directory entries
                    members.append(entry)
        if not content_hash or not blob_uri:
            logger.warning(
                f"cache: store for key {key} produced no content hash; "
                f"output: {stdout[:200]!r}"
            )
            return None

        manifest: dict[str, Any] = {
            "version": 1,
            "key": key,
            "content_hash": content_hash,
            "blob": blob_uri,
            "outputs": members,
            **meta,
        }

        # Round-trip B: upload the manifest JSON to the action cache. Base64
        # over the wire so arbitrary JSON (quotes, newlines) survives the
        # shell heredoc-free pipe intact.
        manifest_json = json.dumps(manifest, sort_keys=True)
        b64 = base64.b64encode(manifest_json.encode("utf-8")).decode("ascii")
        ac_uri = self._uri("ac", f"{key}.json")
        put = (
            f"printf %s {shlex.quote(b64)} | base64 -d "
            f"| {self._cmd_put_stdin(shlex.quote(ac_uri))}"
        )
        try:
            _stdout, stderr, exit_code = await ssh.run_command(put, timeout=120)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"cache: manifest upload failed over SSH: {e}")
            return None
        if exit_code != 0:
            logger.warning(
                f"cache: manifest upload for key {key} failed "
                f"(exit {exit_code}): {stderr.strip()[:200]}"
            )
            return None

        logger.info(
            f"cache: stored {len(members)} output file(s) for key {key[:12]} "
            f"(content {content_hash[:12]})"
        )
        return manifest
