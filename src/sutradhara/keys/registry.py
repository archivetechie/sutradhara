"""Local key registry for encrypted archive copy epochs.

Sutradhara mints archive encryption epochs and persists root-key material in a
protected local registry shared with the scenario harness. Root keys are only
materialized into short-lived 0600 files for RAO CLI calls; retiring an epoch
marks it inactive for new writes but preserves the key for archival reads.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_DEFAULT_REGISTRY_DIR = Path("/var/lib/replica/sutradhara-key-registry")
KEY_DOMAIN_ARCHIVE = "archive"
KEY_DOMAIN_HDCACHE = "hdcache"
KEY_DOMAINS = frozenset({KEY_DOMAIN_ARCHIVE, KEY_DOMAIN_HDCACHE})
_TEST_SEED = bytes.fromhex(
    "73797374656d2d6861726e6573733a737574726164686172612d6b65792d7365"
    "616d3a616d6265722d616561642d6465763a7631"
)


@dataclass(frozen=True)
class KeyEpoch:
    """Typed Sutradhara return for one encryption-key epoch."""

    key_id: str
    created_at: str
    active: bool


class KeyRegistry:
    """Persistent local registry for encrypted archive root-key epochs."""

    def __init__(self, registry_dir: Path | str | None = None) -> None:
        selected_dir = (
            registry_dir
            if registry_dir is not None
            else os.environ.get("SUTRADHARA_KEY_REGISTRY_DIR") or _DEFAULT_REGISTRY_DIR
        )
        self._registry_dir = Path(selected_dir)

    @property
    def registry_dir(self) -> Path:
        """Directory containing key state and root-key files."""
        return self._registry_dir

    def create_epoch(
        self,
        domain: str = KEY_DOMAIN_ARCHIVE,
        *,
        purpose: str | None = None,
    ) -> KeyEpoch:
        """Create or reactivate the deterministic development epoch for a key domain."""
        if purpose is not None:
            domain = purpose
        domain = _validate_domain(domain)
        self._ensure_registry_dir()
        key_id, root_key = _derive_test_epoch(domain)
        root_path = self._root_key_path(key_id)
        state_path = self._state_path(key_id)

        if not root_path.exists():
            _write_private_bytes(root_path, root_key)
        elif root_path.read_bytes() != root_key:
            raise RuntimeError(f"dev key registry root material mismatch for {key_id}")

        state = self._read_state(key_id)
        created_at = str(state.get("created_at") or datetime.now(UTC).isoformat())
        state = {
            "key_id": key_id,
            "domain": domain,
            "created_at": created_at,
            "active": True,
            "backend": "local-registry",
            "deterministic_test_seed": _TEST_SEED.decode("ascii"),
        }
        _write_private_json(state_path, state)
        return KeyEpoch(key_id=key_id, created_at=created_at, active=True)

    def get_epoch(self, key_id: str) -> KeyEpoch:
        """Return persisted state for an existing epoch."""
        key_id = _validate_key_id(key_id)
        self._ensure_registry_dir()
        if not self._root_key_path(key_id).exists():
            raise KeyError(f"unknown key epoch: {key_id}")
        state = self._read_state(key_id)
        created_at = str(state.get("created_at") or "")
        active = bool(state.get("active", False))
        return KeyEpoch(key_id=key_id, created_at=created_at, active=active)

    @contextlib.contextmanager
    def materialized_root_key(self, key_id: str) -> Iterator[Path]:
        """Yield a short-lived 0600 file containing the raw root key."""
        path = self._write_temp_root_key(key_id)
        try:
            yield path
        finally:
            with contextlib.suppress(FileNotFoundError):
                path.unlink()

    def retire_epoch(self, key_id: str) -> dict[str, Any]:
        """Mark an epoch inactive for new writes without deleting its root key."""
        key_id = _validate_key_id(key_id)
        self._ensure_registry_dir()
        root_path = self._root_key_path(key_id)
        if not root_path.exists():
            raise KeyError(f"unknown key epoch: {key_id}")

        state = self._read_state(key_id)
        created_at = str(state.get("created_at") or datetime.now(UTC).isoformat())
        retired_at = datetime.now(UTC).isoformat()
        state = {
            **state,
            "key_id": key_id,
            "created_at": created_at,
            "active": False,
            "retired_at": retired_at,
        }
        _write_private_json(self._state_path(key_id), state)
        return {
            "key_id": key_id,
            "active": False,
            "retired_at": retired_at,
            "root_key_preserved": True,
        }

    def _write_temp_root_key(self, key_id: str) -> Path:
        key_id = _validate_key_id(key_id)
        root_key = self._read_root_key(key_id)
        fd, raw_path = tempfile.mkstemp(
            prefix=f"rao-root-{key_id[:8]}-",
            suffix=".key",
            dir=_secure_temp_dir(),
        )
        path = Path(raw_path)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as handle:
                handle.write(root_key)
        except Exception:
            with contextlib.suppress(OSError):
                os.close(fd)
            with contextlib.suppress(FileNotFoundError):
                path.unlink()
            raise
        return path

    def _read_root_key(self, key_id: str) -> bytes:
        self._ensure_registry_dir()
        path = self._root_key_path(key_id)
        try:
            data = path.read_bytes()
        except FileNotFoundError as exc:
            raise KeyError(f"unknown key epoch: {key_id}") from exc
        if len(data) != 32:
            raise RuntimeError(f"registry root key for {key_id} is {len(data)} bytes")
        return data

    def _read_state(self, key_id: str) -> dict[str, Any]:
        path = self._state_path(key_id)
        try:
            data = json.loads(path.read_text())
        except FileNotFoundError:
            return {}
        if not isinstance(data, dict):
            raise RuntimeError(f"registry state file is not a JSON object: {path}")
        return data

    def _ensure_registry_dir(self) -> None:
        self._registry_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(self._registry_dir, 0o700)
        except PermissionError as exc:
            raise PermissionError(
                f"cannot protect key registry directory {self._registry_dir}"
            ) from exc

    def _root_key_path(self, key_id: str) -> Path:
        return self._registry_dir / f"{key_id}.root"

    def _state_path(self, key_id: str) -> Path:
        return self._registry_dir / f"{key_id}.json"


def _derive_test_epoch_for_domain(domain: str) -> tuple[str, bytes]:
    if domain == KEY_DOMAIN_ARCHIVE:
        key_id = hashlib.sha256(_TEST_SEED + b":key-id").digest()[:16].hex()
        root_key = hashlib.sha256(_TEST_SEED + b":root-key").digest()
    else:
        domain_bytes = domain.encode("ascii")
        key_suffix = hashlib.sha256(_TEST_SEED + b":" + domain_bytes + b":key-id")
        key_id = f"{domain}-{key_suffix.digest()[:16].hex()}"
        root_key = hashlib.sha256(_TEST_SEED + b":" + domain_bytes + b":root-key").digest()
    if key_id == "0" * 32 or key_id.endswith("-" + "0" * 32):
        raise RuntimeError("deterministic dev key_id must not be all zero")
    return key_id, root_key


def _derive_test_epoch(domain: str = KEY_DOMAIN_ARCHIVE) -> tuple[str, bytes]:
    domain = _validate_domain(domain)
    return _derive_test_epoch_for_domain(domain)


def key_domain(key_id: str) -> str:
    """Return the key domain encoded in a registry key id."""
    key_id = _validate_key_id(key_id)
    if key_id.startswith(f"{KEY_DOMAIN_HDCACHE}-"):
        return KEY_DOMAIN_HDCACHE
    return KEY_DOMAIN_ARCHIVE


def assert_key_epoch_domain(
    epoch: KeyEpoch | str,
    expected_domain: str,
    *,
    context: str,
) -> None:
    """Fail closed when a seal/open path receives a key from the wrong domain."""
    expected_domain = _validate_domain(expected_domain)
    key_id = epoch.key_id if isinstance(epoch, KeyEpoch) else epoch
    actual_domain = key_domain(key_id)
    if actual_domain != expected_domain:
        raise ValueError(
            f"{context} requires {expected_domain} key epochs; got {actual_domain} epoch {key_id!r}"
        )


def _validate_domain(domain: str) -> str:
    if domain not in KEY_DOMAINS:
        raise ValueError(f"key domain must be one of {sorted(KEY_DOMAINS)!r}; got {domain!r}")
    return domain


def _validate_key_id(key_id: str) -> str:
    if not isinstance(key_id, str):
        raise TypeError(f"key_id must be str, got {type(key_id).__name__}")
    if key_id.startswith(f"{KEY_DOMAIN_HDCACHE}-"):
        suffix = key_id.removeprefix(f"{KEY_DOMAIN_HDCACHE}-")
        if len(suffix) != 32:
            raise ValueError(f"hdcache key_id suffix must be 32 hex characters, got {key_id!r}")
        _validate_hex_id(suffix, key_id)
        return key_id
    if len(key_id) != 32:
        raise ValueError(f"key_id must be 32 hex characters or hdcache-*; got {key_id!r}")
    _validate_hex_id(key_id, key_id)
    return key_id


def _validate_hex_id(hex_id: str, original: str) -> None:
    try:
        bytes.fromhex(hex_id)
    except ValueError as exc:
        raise ValueError(f"key_id must be lowercase hex, got {original!r}") from exc
    if hex_id != hex_id.lower():
        raise ValueError(f"key_id must be lowercase hex, got {original!r}")
    if hex_id == "0" * 32:
        raise ValueError("key_id must not be all zero")


def _secure_temp_dir() -> Path:
    for candidate in (
        os.environ.get("XDG_RUNTIME_DIR"),
        f"/run/user/{os.getuid()}",
        "/dev/shm",
    ):
        if not candidate:
            continue
        base = Path(candidate)
        if base.is_dir() and os.access(base, os.W_OK):
            path = base / f"sutradhara-rao-keys-{os.getuid()}"
            path.mkdir(mode=0o700, exist_ok=True)
            os.chmod(path, 0o700)
            return path

    fallback = Path(tempfile.gettempdir()) / f"sutradhara-rao-keys-{os.getuid()}"
    fallback.mkdir(mode=0o700, exist_ok=True)
    os.chmod(fallback, 0o700)
    return fallback


def _write_private_bytes(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
    except Exception:
        with contextlib.suppress(OSError):
            os.close(fd)
        raise


def _write_private_json(path: Path, data: dict[str, Any]) -> None:
    payload = json.dumps(data, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    _write_private_bytes(path, payload)
