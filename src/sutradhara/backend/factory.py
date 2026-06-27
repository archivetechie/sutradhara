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

    Supported adapters:
      - `memory`     — in-process test backend (config ignored)
      - `rem_tape`   — Remanence daemon Catalog (`daemon_endpoint`) or dev fixture
      - `d2_tape`    — d2tape CLI adapter
      - `s3`         — S3-compatible object store
      - `ssh_disk`   — rsync/SSH object store rooted on a LAN file server

    Future kinds (`rem_disk`, `gcs`, `azure_blob`, `plain_disk`)
    will land alongside their adapter implementations.
    """
    cfg: dict[str, object] = row.config or {}
    _reject_obsolete_placements(row, cfg)

    if row.kind == BackendKind.MEMORY:
        return MemoryBackend(row.name)

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
        return D2TapeBackend(
            row.name,
            jar_path=_optional_str(cfg, "jar_path"),
            java_home=_optional_str(cfg, "java_home"),
            java_bin=_optional_str(cfg, "java_bin"),
            device_env_path=str(cfg.get("device_env_path", "/var/lib/replica/d2tape/device.env")),
            state_dir=str(cfg.get("state_dir", "/var/lib/replica/d2tape/volumes")),
            timeout_seconds=_optional_float(cfg, "timeout_seconds", 300.0),
            file_backed=bool(cfg.get("file_backed", False)),
            temp_dir=_optional_str(cfg, "temp_dir"),
            stinit_script=_optional_str(cfg, "stinit_script"),
            volume_uuid=_optional_str(cfg, "volume_uuid"),
        )

    if row.kind == BackendKind.S3:
        from sutradhara.backend.s3 import S3Backend

        bucket = _optional_str(cfg, "bucket")
        if bucket is None:
            raise BackendNotConfigured(f"backend {row.name!r} (kind=s3) needs config.bucket")
        return S3Backend(
            row.name,
            bucket=bucket,
            prefix=_optional_str(cfg, "prefix") or "",
            endpoint_url=_optional_str(cfg, "endpoint_url"),
            storage_class=_optional_str(cfg, "storage_class"),
        )

    if row.kind == BackendKind.SSH_DISK:
        from sutradhara.backend.ssh_disk import SshDiskBackend

        host = _optional_str(cfg, "host")
        root = _optional_str(cfg, "root")
        if not host or not root:
            raise BackendNotConfigured(
                f"backend {row.name!r} (kind=ssh_disk) needs config.host and config.root"
            )
        return SshDiskBackend(
            row.name,
            host=host,
            root=root,
            user=_optional_str(cfg, "user"),
            identity_file=_optional_str(cfg, "identity_file"),
            ssh_options=_optional_str_list(cfg, "ssh_options"),
        )

    raise UnsupportedBackendKind(f"backend {row.name!r}: kind={row.kind} has no factory yet")


def _optional_str(cfg: dict[str, object], key: str) -> str | None:
    value = cfg.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise BackendNotConfigured(f"config.{key} must be a string")
    return value


def _optional_str_list(cfg: dict[str, object], key: str) -> list[str]:
    value = cfg.get(key)
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise BackendNotConfigured(f"config.{key} must be a list of strings")
    return list(value)


def _reject_obsolete_placements(row: BackendRow, cfg: dict[str, object]) -> None:
    if "placements" not in cfg:
        return
    raise BackendNotConfigured(
        f"backend {row.name!r}: config.placements is obsolete; declare pool "
        "and artifactclass_pool rows in the catalog"
    )


def _optional_float(
    cfg: dict[str, object],
    key: str,
    default: float,
) -> float:
    value = cfg.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise BackendNotConfigured(f"config.{key} must be a number")
    return float(value)
