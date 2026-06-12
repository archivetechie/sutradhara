"""Construct runtime `StorageBackend` instances from catalog `Backend` rows.

The DB row is the operator-visible registration (kind + config + name);
the factory turns it into a live adapter that implements the Protocol.
"""

from __future__ import annotations

from sutradhara.backend.d2tape import D2TapeBackend
from sutradhara.backend.memory import MemoryBackend
from sutradhara.backend.port import StorageBackend
from sutradhara.backend.remanence import RemanenceBackend
from sutradhara.catalog.models import Backend as BackendRow
from sutradhara.catalog.types import BackendKind


class BackendNotConfigured(Exception):
    """Raised when a backend row's `config` is missing required fields."""


class UnsupportedBackendKind(Exception):
    """The backend's `kind` has no factory registered yet."""


def backend_from_row(row: BackendRow) -> StorageBackend:
    """Instantiate a `StorageBackend` from a persisted `Backend` row.

    Day-1 supports:
      - `memory`     — in-process test backend (config ignored)
      - `rem_tape`   — Remanence daemon Catalog (`daemon_endpoint`) or dev fixture
      - `d2_tape`    — d2tape CLI adapter with registration-declared placements

    Future kinds (`rem_disk`, `s3`, `gcs`, `azure_blob`, `plain_disk`)
    will land alongside their adapter implementations.
    """
    cfg: dict[str, object] = row.config or {}

    if row.kind == BackendKind.MEMORY:
        placements_raw = cfg.get("placements")
        if placements_raw is None:
            return MemoryBackend(row.name)
        if not isinstance(placements_raw, list):
            raise BackendNotConfigured(
                f"backend {row.name!r} (kind=memory) config.placements must be a list"
            )
        return MemoryBackend(row.name, placements=placements_raw)

    if row.kind == BackendKind.REM_TAPE:
        daemon_endpoint = cfg.get("daemon_endpoint")
        fixture = cfg.get("fixture_path")
        if isinstance(daemon_endpoint, str) and isinstance(fixture, str):
            raise BackendNotConfigured(
                f"backend {row.name!r} (kind=rem_tape) has both 'daemon_endpoint' "
                "and 'fixture_path'; configure exactly one"
            )
        if isinstance(daemon_endpoint, str):
            return RemanenceBackend.from_grpc(row.name, daemon_endpoint)
        if isinstance(fixture, str):
            return RemanenceBackend.from_fixture_file(row.name, fixture)
        raise BackendNotConfigured(
            f"backend {row.name!r} (kind=rem_tape) needs config.fixture_path "
            "(dev fixture) or config.daemon_endpoint (live Remanence daemon catalog)"
        )

    if row.kind == BackendKind.D2_TAPE:
        placements_raw = cfg.get("placements")
        if placements_raw is not None and not isinstance(placements_raw, list):
            raise BackendNotConfigured(
                f"backend {row.name!r} (kind=d2_tape) config.placements must be a list"
            )
        return D2TapeBackend(
            row.name,
            jar_path=_optional_str(cfg, "jar_path"),
            java_home=_optional_str(cfg, "java_home"),
            java_bin=_optional_str(cfg, "java_bin"),
            device_env_path=str(
                cfg.get("device_env_path", "/var/lib/replica/d2tape/device.env")
            ),
            state_dir=str(
                cfg.get("state_dir", "/var/lib/replica/d2tape/volumes")
            ),
            placements=placements_raw,
            timeout_seconds=_optional_float(cfg, "timeout_seconds", 300.0),
            file_backed=bool(cfg.get("file_backed", False)),
            temp_dir=_optional_str(cfg, "temp_dir"),
            stinit_script=_optional_str(cfg, "stinit_script"),
            volume_uuid=_optional_str(cfg, "volume_uuid"),
        )

    raise UnsupportedBackendKind(
        f"backend {row.name!r}: kind={row.kind} has no factory yet"
    )


def _optional_str(cfg: dict[str, object], key: str) -> str | None:
    value = cfg.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise BackendNotConfigured(f"config.{key} must be a string")
    return value


def _optional_float(
    cfg: dict[str, object],
    key: str,
    default: float,
) -> float:
    value = cfg.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise BackendNotConfigured(f"config.{key} must be a number")
    return float(value)
