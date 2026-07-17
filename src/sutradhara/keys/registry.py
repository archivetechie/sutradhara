"""Local X25519 recipient-key registry for encrypted RAO epochs.

Serving hosts mint OS-random keypairs for hot archive, hdcache, and backup
domains.  Recovery epochs are minted offline and imported public-only.  The
registry exposes persistent non-secret RAOR public files for sealing and
materializes canonical RAOP private files only for the lifetime of an open.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import tempfile
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)

_DEFAULT_REGISTRY_DIR = Path("/var/lib/replica/sutradhara-key-registry")
_PRODUCTION_ROOT = Path("/var/lib")
KEY_DOMAIN_ARCHIVE = "archive"
KEY_DOMAIN_HDCACHE = "hdcache"
KEY_DOMAIN_BACKUP = "backup"
KEY_DOMAIN_RECOVERY = "recovery"
KEY_DOMAINS = frozenset(
    {
        KEY_DOMAIN_ARCHIVE,
        KEY_DOMAIN_HDCACHE,
        KEY_DOMAIN_BACKUP,
        KEY_DOMAIN_RECOVERY,
    }
)
_TEST_SEED = bytes.fromhex(
    "73797374656d2d6861726e6573733a737574726164686172612d6b65792d7365"
    "616d3a616d6265722d616561642d6465763a7631"
)
_PUBLIC_MAGIC = b"RAOR"
_PRIVATE_MAGIC = b"RAOP"


@dataclass(frozen=True)
class KeyEpoch:
    """Typed Sutradhara return for one recipient-key epoch."""

    key_id: str
    created_at: str
    active: bool


class KeyRegistry:
    """Persistent local registry for RAO X25519 recipient epochs."""

    def __init__(
        self,
        registry_dir: Path | str | None = None,
        *,
        deterministic_test: bool = False,
    ) -> None:
        selected_dir = (
            registry_dir
            if registry_dir is not None
            else os.environ.get("SUTRADHARA_KEY_REGISTRY_DIR") or _DEFAULT_REGISTRY_DIR
        )
        resolved = Path(selected_dir).expanduser().resolve(strict=False)
        if deterministic_test and _is_production_registry_path(resolved):
            raise ValueError(
                "deterministic_test key registry must resolve outside /var/lib and must not "
                f"use {_DEFAULT_REGISTRY_DIR}"
            )
        self._registry_dir = resolved
        self._deterministic_test = deterministic_test

    @property
    def registry_dir(self) -> Path:
        """Directory containing key state and persistent key files."""

        return self._registry_dir

    def create_epoch(
        self,
        domain: str = KEY_DOMAIN_ARCHIVE,
        *,
        purpose: str | None = None,
    ) -> KeyEpoch:
        """Create or return the active hot keypair for ``domain``.

        Recovery keypairs must be minted offline with
        :func:`mint_recovery_keypair` and imported public-only.
        """

        if purpose is not None:
            domain = purpose
        domain = _validate_domain(domain)
        if domain == KEY_DOMAIN_RECOVERY:
            raise ValueError("recovery epochs must be minted offline and imported public-only")
        self._ensure_registry_dir()
        existing = self._active_epoch_for_domain(domain)
        if existing is not None:
            return existing

        generation = self._next_generation(domain)
        key_id, private_key, public_key = self._new_keypair(domain, generation=generation)
        private_path = self._private_key_path(key_id)
        public_path = self._public_key_path(key_id)
        state_path = self._state_path(key_id)
        created_at = datetime.now(UTC).isoformat()
        public_payload = _serialize_public_key(
            key_id,
            public_key,
            slot_index=_slot_index_for_domain(domain),
        )
        created_paths: list[Path] = []
        try:
            _write_new_bytes(private_path, private_key, mode=0o600)
            created_paths.append(private_path)
            _write_new_bytes(public_path, public_payload, mode=0o644)
            created_paths.append(public_path)
            _write_private_json(
                state_path,
                {
                    "key_id": key_id,
                    "domain": domain,
                    "generation": generation,
                    "created_at": created_at,
                    "active": True,
                    "backend": "local-registry",
                    "key_kind": "keypair",
                    "deterministic_test": self._deterministic_test,
                },
            )
        except Exception:
            for path in reversed(created_paths):
                if path == private_path:
                    _best_effort_zeroize(path)
                with contextlib.suppress(FileNotFoundError):
                    path.unlink()
            raise
        return KeyEpoch(key_id=key_id, created_at=created_at, active=True)

    def import_public_epoch(self, public_key_file: Path | str) -> KeyEpoch:
        """Import one canonical recovery RAOR file without private material."""

        source = Path(public_key_file)
        try:
            payload = source.read_bytes()
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"recovery public-key file does not exist: {source}") from exc
        parsed = _parse_public_key(payload)
        key_id = parsed.key_id
        if key_domain(key_id) != KEY_DOMAIN_RECOVERY:
            raise ValueError("only recovery-domain public epochs may be imported")
        if parsed.slot_index != _slot_index_for_domain(KEY_DOMAIN_RECOVERY):
            raise ValueError("recovery public-key file must use recipient slot 1")

        self._ensure_registry_dir()
        private_path = self._private_key_path(key_id)
        if private_path.exists():
            raise RuntimeError(
                f"recovery epoch {key_id} has forbidden private material under registry_dir"
            )
        destination = self._public_key_path(key_id)
        previous_state = self._read_state(key_id)
        if previous_state:
            self.get_epoch(key_id)
        if destination.exists():
            if destination.read_bytes() != payload:
                raise RuntimeError(f"registry public material mismatch for {key_id}")
        else:
            _write_new_bytes(destination, payload, mode=0o644)

        generation = (
            _state_generation(previous_state)
            if previous_state
            else self._next_generation(KEY_DOMAIN_RECOVERY)
        )
        created_at = str(previous_state.get("created_at") or datetime.now(UTC).isoformat())
        _write_private_json(
            self._state_path(key_id),
            {
                "key_id": key_id,
                "domain": KEY_DOMAIN_RECOVERY,
                "generation": generation,
                "created_at": created_at,
                "active": True,
                "backend": "offline-public-import",
                "key_kind": "public-only",
                # The imported recovery key was minted with OS randomness by
                # the offline helper, even when the receiving registry is a
                # deterministic test registry.
                "deterministic_test": False,
            },
        )
        for other_key_id, state in self._states_for_domain(KEY_DOMAIN_RECOVERY):
            if other_key_id == key_id or not bool(state.get("active", False)):
                continue
            _write_private_json(
                self._state_path(other_key_id),
                {
                    **state,
                    "active": False,
                    "retired_at": datetime.now(UTC).isoformat(),
                },
            )
        return KeyEpoch(key_id=key_id, created_at=created_at, active=True)

    def get_epoch(self, key_id: str) -> KeyEpoch:
        """Return validated persisted state for an existing epoch."""

        key_id = _validate_key_id(key_id)
        self._ensure_registry_dir()
        state = self._read_state(key_id)
        if not state:
            raise KeyError(f"unknown key epoch: {key_id}")
        domain = key_domain(key_id)
        if state.get("key_id") != key_id or state.get("domain") != domain:
            raise RuntimeError(f"registry state identity mismatch for {key_id}")
        created_at = state.get("created_at")
        if not isinstance(created_at, str) or not created_at:
            raise RuntimeError(f"registry state created_at is invalid for {key_id}")
        active = state.get("active")
        if not isinstance(active, bool):
            raise RuntimeError(f"registry state active marker is invalid for {key_id}")
        _state_generation(state)
        marker = state.get("deterministic_test")
        if not isinstance(marker, bool):
            raise RuntimeError(f"registry state lacks deterministic_test marker for {key_id}")
        if marker and not self._deterministic_test:
            raise RuntimeError(
                f"deterministic test epoch {key_id} is refused by a production registry"
            )
        public_path = self._public_key_path(key_id)
        try:
            public = _parse_public_key(public_path.read_bytes())
        except FileNotFoundError as exc:
            raise KeyError(f"unknown key epoch: {key_id}") from exc
        if public.key_id != key_id or public.slot_index != _slot_index_for_domain(domain):
            raise RuntimeError(f"registry public-key identity mismatch for {key_id}")

        kind = state.get("key_kind")
        private_path = self._private_key_path(key_id)
        if domain == KEY_DOMAIN_RECOVERY:
            if kind != "public-only":
                raise RuntimeError(f"recovery epoch {key_id} is not public-only")
            if private_path.exists():
                raise RuntimeError(
                    f"recovery epoch {key_id} has forbidden private material under registry_dir"
                )
        else:
            if kind != "keypair":
                raise RuntimeError(f"hot epoch {key_id} is not a keypair")
            private = X25519PrivateKey.from_private_bytes(self._read_private_key(key_id))
            if _public_bytes(private.public_key()) != public.public_key:
                raise RuntimeError(f"registry keypair material mismatch for {key_id}")
        return KeyEpoch(
            key_id=key_id,
            created_at=created_at,
            active=active,
        )

    def active_epoch(self, domain: str) -> KeyEpoch:
        """Return the single active epoch for a domain, failing if absent."""

        domain = _validate_domain(domain)
        self._ensure_registry_dir()
        epoch = self._active_epoch_for_domain(domain)
        if epoch is None:
            raise KeyError(f"no active {domain} key epoch")
        return epoch

    def recipients_for_seal(self, hot_key_id: str, *, domain: str) -> tuple[KeyEpoch, KeyEpoch]:
        """Resolve the active hot and recovery epochs for one encrypted seal."""

        domain = _validate_domain(domain)
        if domain == KEY_DOMAIN_RECOVERY:
            raise ValueError("recovery is not a hot sealing domain")
        assert_key_epoch_domain(hot_key_id, domain, context=f"{domain} sealing")
        hot = self.get_epoch(hot_key_id)
        if not hot.active:
            raise KeyError(f"hot {domain} key epoch is retired: {hot_key_id}")
        recovery = self.active_epoch(KEY_DOMAIN_RECOVERY)
        return hot, recovery

    def select_private_epoch(
        self,
        recipient_epochs: Sequence[str],
        *,
        domain: str,
    ) -> KeyEpoch:
        """Select this host's private epoch for a copy-domain recipient list."""

        domain = _validate_domain(domain)
        if isinstance(recipient_epochs, (str, bytes)) or not recipient_epochs:
            raise ValueError("recipient_epochs must be a non-empty sequence of registry ids")
        validated = tuple(_validate_key_id(value) for value in recipient_epochs)
        if len(set(validated)) != len(validated):
            raise ValueError("recipient_epochs must not contain duplicates")
        for key_id in validated:
            if key_domain(key_id) != domain:
                continue
            if not self._private_key_path(key_id).is_file():
                continue
            # Once a candidate private file exists, any corrupt/mismatched
            # state is an integrity failure, not permission to skip ahead.
            return self.get_epoch(key_id)
        raise KeyError(f"no {domain} recipient epoch has resolvable private material on this host")

    def public_key_path(self, key_id: str) -> Path:
        """Return the persistent non-secret canonical RAOR file for an epoch."""

        epoch = self.get_epoch(key_id)
        return self._public_key_path(epoch.key_id)

    @contextlib.contextmanager
    def materialized_private_key(self, key_id: str) -> Iterator[Path]:
        """Yield a short-lived 0600 RAOP file, then zeroize and remove it."""

        path = self._write_temp_private_key(key_id)
        try:
            yield path
        finally:
            _best_effort_zeroize(path)
            with contextlib.suppress(FileNotFoundError):
                path.unlink()

    def retire_epoch(self, key_id: str) -> dict[str, Any]:
        """Mark an epoch inactive without deleting its retained key material."""

        epoch = self.get_epoch(key_id)
        state = self._read_state(epoch.key_id)
        retired_at = datetime.now(UTC).isoformat()
        _write_private_json(
            self._state_path(epoch.key_id),
            {
                **state,
                "active": False,
                "retired_at": retired_at,
            },
        )
        return {
            "key_id": epoch.key_id,
            "active": False,
            "retired_at": retired_at,
            "private_key_preserved": self._private_key_path(epoch.key_id).exists(),
            "public_key_preserved": True,
        }

    def _new_keypair(self, domain: str, *, generation: int) -> tuple[str, bytes, bytes]:
        if self._deterministic_test:
            return _derive_test_keypair(domain, generation=generation)
        while True:
            key_id = f"{domain}-{os.urandom(16).hex()}"
            if not self._state_path(key_id).exists():
                break
        private = X25519PrivateKey.generate()
        return key_id, _private_bytes(private), _public_bytes(private.public_key())

    def _write_temp_private_key(self, key_id: str) -> Path:
        key_id = self.get_epoch(key_id).key_id
        private_key = self._read_private_key(key_id)
        payload = _serialize_private_key(key_id, private_key)
        fd, raw_path = tempfile.mkstemp(
            prefix=f"rao-private-{key_domain(key_id)}-",
            suffix=".raop",
            dir=_secure_temp_dir(),
        )
        path = Path(raw_path)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
        except Exception:
            with contextlib.suppress(OSError):
                os.close(fd)
            with contextlib.suppress(FileNotFoundError):
                path.unlink()
            raise
        return path

    def _read_private_key(self, key_id: str) -> bytes:
        self._ensure_registry_dir()
        path = self._private_key_path(key_id)
        try:
            data = path.read_bytes()
        except FileNotFoundError as exc:
            raise KeyError(f"private key unavailable for epoch: {key_id}") from exc
        if len(data) != 32:
            raise RuntimeError(f"registry private key for {key_id} is {len(data)} bytes")
        try:
            X25519PrivateKey.from_private_bytes(data)
        except ValueError as exc:
            raise RuntimeError(f"registry private key for {key_id} is invalid") from exc
        return data

    def _read_state(self, key_id: str) -> dict[str, Any]:
        path = self._state_path(key_id)
        try:
            data = json.loads(path.read_text())
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"registry state file is invalid JSON: {path}") from exc
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

    def _active_epoch_for_domain(self, domain: str) -> KeyEpoch | None:
        active: list[KeyEpoch] = []
        for key_id, state in self._states_for_domain(domain):
            if bool(state.get("active", False)):
                active.append(self.get_epoch(key_id))
        if len(active) > 1:
            raise RuntimeError(f"key registry has multiple active {domain} epochs")
        return active[0] if active else None

    def _next_generation(self, domain: str) -> int:
        generations = [
            _state_generation(state) for _key_id, state in self._states_for_domain(domain)
        ]
        return max(generations, default=-1) + 1

    def _states_for_domain(self, domain: str) -> list[tuple[str, dict[str, Any]]]:
        states: list[tuple[str, dict[str, Any]]] = []
        for path in sorted(self._registry_dir.glob("*.json")):
            key_id = _validate_key_id(path.stem)
            state = self._read_state(key_id)
            self.get_epoch(key_id)
            if state.get("domain") == domain:
                states.append((key_id, state))
        return states

    def _private_key_path(self, key_id: str) -> Path:
        return self._registry_dir / f"{key_id}.private"

    def _public_key_path(self, key_id: str) -> Path:
        return self._registry_dir / f"{key_id}.public"

    def _state_path(self, key_id: str) -> Path:
        return self._registry_dir / f"{key_id}.json"


@dataclass(frozen=True)
class _ParsedPublicKey:
    key_id: str
    slot_index: int
    public_key: bytes


def mint_recovery_keypair(
    *,
    public_key_path: Path | str,
    private_key_path: Path | str,
) -> KeyEpoch:
    """Mint an offline recovery keypair to operator-selected RAOR/RAOP paths."""

    public_path = Path(public_key_path).expanduser().resolve(strict=False)
    private_path = Path(private_key_path).expanduser().resolve(strict=False)
    if public_path == private_path:
        raise ValueError("recovery public and private key paths must differ")
    configured_registry = (
        Path(os.environ.get("SUTRADHARA_KEY_REGISTRY_DIR") or _DEFAULT_REGISTRY_DIR)
        .expanduser()
        .resolve(strict=False)
    )
    if private_path == configured_registry or private_path.is_relative_to(configured_registry):
        raise ValueError("recovery private key path must be outside registry_dir")
    for path in (public_path, private_path):
        if not path.parent.is_dir():
            raise FileNotFoundError(
                f"recovery key destination parent does not exist: {path.parent}"
            )
    key_id = f"{KEY_DOMAIN_RECOVERY}-{os.urandom(16).hex()}"
    private = X25519PrivateKey.generate()
    private_raw = _private_bytes(private)
    public_raw = _public_bytes(private.public_key())
    created_at = datetime.now(UTC).isoformat()
    private_payload = _serialize_private_key(key_id, private_raw)
    public_payload = _serialize_public_key(
        key_id,
        public_raw,
        slot_index=_slot_index_for_domain(KEY_DOMAIN_RECOVERY),
    )
    _write_new_bytes(private_path, private_payload, mode=0o600)
    try:
        _write_new_bytes(public_path, public_payload, mode=0o644)
    except Exception:
        _best_effort_zeroize(private_path)
        with contextlib.suppress(FileNotFoundError):
            private_path.unlink()
        raise
    return KeyEpoch(key_id=key_id, created_at=created_at, active=True)


def key_domain(key_id: str) -> str:
    """Return the explicit domain prefix encoded in a registry epoch id."""

    key_id = _validate_key_id(key_id)
    for domain in KEY_DOMAINS:
        if key_id.startswith(f"{domain}-"):
            return domain
    raise ValueError(f"key epoch has no recognized domain prefix: {key_id!r}")


def assert_key_epoch_domain(
    epoch: KeyEpoch | str,
    expected_domain: str,
    *,
    context: str,
) -> None:
    """Fail closed when a seal/open path receives an epoch from another domain."""

    expected_domain = _validate_domain(expected_domain)
    key_id = epoch.key_id if isinstance(epoch, KeyEpoch) else epoch
    actual_domain = key_domain(key_id)
    if actual_domain != expected_domain:
        raise ValueError(
            f"{context} requires {expected_domain} key epochs; got {actual_domain} epoch {key_id!r}"
        )


def _derive_test_keypair(domain: str, *, generation: int = 0) -> tuple[str, bytes, bytes]:
    if generation < 0:
        raise ValueError("generation must be non-negative")
    domain = _validate_domain(domain)
    generation_bytes = str(generation).encode("ascii")
    prefix = _TEST_SEED + b":" + domain.encode("ascii") + b":" + generation_bytes
    epoch_hex = hashlib.sha256(prefix + b":epoch-id").digest()[:16].hex()
    if epoch_hex == "0" * 32:
        raise RuntimeError("deterministic test epoch id must not be all zero")
    private_raw = hashlib.sha256(prefix + b":x25519-private").digest()
    private = X25519PrivateKey.from_private_bytes(private_raw)
    return f"{domain}-{epoch_hex}", private_raw, _public_bytes(private.public_key())


def _serialize_public_key(key_id: str, public_key: bytes, *, slot_index: int) -> bytes:
    key_id = _validate_key_id(key_id)
    X25519PublicKey.from_public_bytes(public_key)
    # The installed Remanence P1 parser caps RAOR labels at 32 bytes, while a
    # registry id is 39-41 bytes. Keep the domain in the wire label and the
    # exact 16-byte epoch payload on wire; report parsing reconstructs the
    # canonical ``<domain>-<32hex>`` registry id.
    label = key_domain(key_id).encode("ascii")
    return (
        _PUBLIC_MAGIC
        + bytes([slot_index])
        + _wire_epoch_id(key_id)
        + bytes([len(label)])
        + label
        + public_key
    )


def _parse_public_key(payload: bytes) -> _ParsedPublicKey:
    if payload[:4] != _PUBLIC_MAGIC or len(payload) < 54:
        raise ValueError("invalid RAO recipient public-key file")
    slot_index = payload[4]
    wire_id = payload[5:21]
    label_len = payload[21]
    if len(payload) != 54 + label_len:
        raise ValueError("invalid RAO recipient public-key file length")
    try:
        domain = payload[22 : 22 + label_len].decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError("invalid RAO recipient public-key label") from exc
    domain = _validate_domain(domain)
    key_id = _validate_key_id(f"{domain}-{wire_id.hex()}")
    if wire_id != _wire_epoch_id(key_id):
        raise ValueError("RAO recipient public-key epoch id does not match its label")
    public_key = payload[22 + label_len :]
    try:
        X25519PublicKey.from_public_bytes(public_key)
    except ValueError as exc:
        raise ValueError("invalid X25519 recipient public key") from exc
    return _ParsedPublicKey(key_id=key_id, slot_index=slot_index, public_key=public_key)


def _serialize_private_key(key_id: str, private_key: bytes) -> bytes:
    key_id = _validate_key_id(key_id)
    X25519PrivateKey.from_private_bytes(private_key)
    label = key_domain(key_id).encode("ascii")
    return _PRIVATE_MAGIC + _wire_epoch_id(key_id) + bytes([len(label)]) + label + private_key


def _wire_epoch_id(key_id: str) -> bytes:
    return bytes.fromhex(_validate_key_id(key_id).rsplit("-", 1)[1])


def _slot_index_for_domain(domain: str) -> int:
    return 1 if domain == KEY_DOMAIN_RECOVERY else 0


def _private_bytes(private: X25519PrivateKey) -> bytes:
    return private.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _public_bytes(public: X25519PublicKey) -> bytes:
    return public.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )


def _state_generation(state: dict[str, Any]) -> int:
    raw = state.get("generation", 0)
    if type(raw) is int and raw >= 0:
        return raw
    raise RuntimeError("registry state generation must be a non-negative integer")


def _validate_domain(domain: str) -> str:
    if domain not in KEY_DOMAINS:
        raise ValueError(f"key domain must be one of {sorted(KEY_DOMAINS)!r}; got {domain!r}")
    return domain


def _validate_key_id(key_id: str) -> str:
    if not isinstance(key_id, str):
        raise TypeError(f"key_id must be str, got {type(key_id).__name__}")
    for domain in KEY_DOMAINS:
        prefix = f"{domain}-"
        if key_id.startswith(prefix):
            suffix = key_id.removeprefix(prefix)
            if len(suffix) != 32:
                raise ValueError(
                    f"{domain} key_id suffix must be 32 hex characters, got {key_id!r}"
                )
            _validate_hex_id(suffix, key_id)
            return key_id
    raise ValueError(
        f"key_id must use one of the prefixes {sorted(KEY_DOMAINS)!r} followed by 32 hex characters; got {key_id!r}"
    )


def _validate_hex_id(hex_id: str, original: str) -> None:
    try:
        bytes.fromhex(hex_id)
    except ValueError as exc:
        raise ValueError(f"key_id must be lowercase hex, got {original!r}") from exc
    if hex_id != hex_id.lower():
        raise ValueError(f"key_id must be lowercase hex, got {original!r}")
    if hex_id == "0" * 32:
        raise ValueError("key_id must not be all zero")


def _is_production_registry_path(path: Path) -> bool:
    production_root = _PRODUCTION_ROOT.resolve(strict=False)
    default = _DEFAULT_REGISTRY_DIR.resolve(strict=False)
    return path in (default, production_root) or path.is_relative_to(production_root)


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


def _best_effort_zeroize(path: Path) -> None:
    """Overwrite a materialized key before unlinking when the file still exists."""

    try:
        size = path.stat().st_size
        with path.open("r+b", buffering=0) as handle:
            handle.write(b"\0" * size)
            os.fsync(handle.fileno())
    except OSError:
        pass


def _write_new_bytes(path: Path, data: bytes, *, mode: int) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
    except Exception:
        with contextlib.suppress(OSError):
            os.close(fd)
        if mode & 0o077 == 0:
            _best_effort_zeroize(path)
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
        raise


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
