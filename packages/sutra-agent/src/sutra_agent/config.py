"""Local configuration for the Sutradhara edge receive agent.

The agent runs outside the server process, so it needs a small durable config:
where landing lives, which operator/source defaults to use, and where the local
receive ledger is stored. This module keeps that file format explicit and free
of server-side dependencies.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CONFIG_SCHEMA = "sutra-agent-config-v1"
DEFAULT_SOURCE_KIND = "card"
DEFAULT_ARTIFACTCLASS = "camera-original"
DEFAULT_CONFIRM_INTERVAL_SECONDS = 1.0
DEFAULT_PARALLELISM = 8
DEFAULT_CHUNK_BYTES = 4 * 1024 * 1024


class AgentConfigError(ValueError):
    """Raised when an agent config file is missing or invalid."""


@dataclass(frozen=True)
class AgentConfig:
    """Resolved receive-agent configuration."""

    landing: Path | None = None
    operator: str | None = None
    source_kind: str = DEFAULT_SOURCE_KIND
    artifactclass: str = DEFAULT_ARTIFACTCLASS
    ledger_path: Path | None = None
    confirm_interval_seconds: float = DEFAULT_CONFIRM_INTERVAL_SECONDS
    server_address: str | None = None
    client_cert: Path | None = None
    client_key: Path | None = None
    ca_cert: Path | None = None
    device_id: str | None = None
    parallelism: int = DEFAULT_PARALLELISM
    chunk_bytes: int = DEFAULT_CHUNK_BYTES

    def __post_init__(self) -> None:
        if self.server_address and self.landing is not None:
            raise AgentConfigError("landing and server_address are mutually exclusive")
        if self.server_address:
            if not self.client_cert or not self.client_key or not self.ca_cert:
                raise AgentConfigError("streaming mode requires client_cert, client_key, and ca_cert")
            if not self.device_id:
                raise AgentConfigError("streaming mode requires device_id")
        else:
            if self.landing is None:
                raise AgentConfigError("landing is required in legacy receive mode")
            if not self.operator:
                raise AgentConfigError("operator must be non-empty in legacy receive mode")
        if not self.source_kind:
            raise AgentConfigError("source_kind must be non-empty")
        if not self.artifactclass:
            raise AgentConfigError("artifactclass must be non-empty")
        if self.confirm_interval_seconds <= 0:
            raise AgentConfigError("confirm_interval_seconds must be a positive number")
        if self.parallelism < 1 or self.parallelism > 8:
            raise AgentConfigError("parallelism must be between 1 and 8")
        if self.chunk_bytes <= 0:
            raise AgentConfigError("chunk_bytes must be positive")

    @property
    def streaming_enabled(self) -> bool:
        """Return true when receive should use the gRPC streaming path."""

        return self.server_address is not None

    def resolved_ledger_path(self) -> Path:
        """Return the ledger path, falling back to the platform state directory."""

        return self.ledger_path or default_ledger_path()


def default_config_path() -> Path:
    """Return the default config path, honoring `SUTRA_AGENT_CONFIG`."""

    configured = os.environ.get("SUTRA_AGENT_CONFIG")
    if configured:
        return Path(configured).expanduser()
    config_home = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return config_home / "sutra-agent" / "config.json"


def default_state_dir() -> Path:
    """Return the default state directory, honoring `SUTRA_AGENT_STATE_DIR`."""

    configured = os.environ.get("SUTRA_AGENT_STATE_DIR")
    if configured:
        return Path(configured).expanduser()
    state_home = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
    return state_home / "sutra-agent"


def default_ledger_path() -> Path:
    """Return the default JSON receive-ledger path."""

    return default_state_dir() / "ledger.json"


def load_config(path: Path | None = None) -> AgentConfig:
    """Load a JSON agent config from disk."""

    config_path = path or default_config_path()
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise AgentConfigError(f"agent config not found: {config_path}") from exc
    except json.JSONDecodeError as exc:
        raise AgentConfigError(f"agent config is not valid JSON: {config_path}") from exc
    if not isinstance(payload, dict):
        raise AgentConfigError(f"agent config must be a JSON object: {config_path}")
    if payload.get("schema") != CONFIG_SCHEMA:
        raise AgentConfigError(
            f"agent config schema mismatch: expected {CONFIG_SCHEMA}, "
            f"actual {payload.get('schema')!r}"
        )
    return _config_from_payload(payload, base_dir=config_path.parent)


def write_config(
    config: AgentConfig,
    path: Path | None = None,
    *,
    overwrite: bool = False,
) -> Path:
    """Write a JSON agent config atomically and return its path."""

    config_path = path or default_config_path()
    if config_path.exists() and not overwrite:
        raise AgentConfigError(f"agent config already exists: {config_path}")
    payload = config_payload(config)
    _atomic_write_json(config_path, payload)
    return config_path


def config_payload(config: AgentConfig) -> dict[str, Any]:
    """Return the stable JSON payload for an agent config."""

    payload: dict[str, Any] = {
        "schema": CONFIG_SCHEMA,
        "source_kind": config.source_kind,
        "artifactclass": config.artifactclass,
        "confirm_interval_seconds": config.confirm_interval_seconds,
    }
    if config.server_address is not None:
        payload.update(
            {
                "server_address": config.server_address,
                "client_cert": str(config.client_cert),
                "client_key": str(config.client_key),
                "ca_cert": str(config.ca_cert),
                "device_id": config.device_id,
                "parallelism": config.parallelism,
                "chunk_bytes": config.chunk_bytes,
            }
        )
    else:
        payload["landing"] = str(config.landing)
        payload["operator"] = config.operator
    if config.ledger_path is not None:
        payload["ledger_path"] = str(config.ledger_path)
    return payload


def resolve_config(
    *,
    config_path: Path | None = None,
    landing: Path | None = None,
    operator: str | None = None,
    source_kind: str | None = None,
    artifactclass: str | None = None,
    ledger_path: Path | None = None,
    confirm_interval_seconds: float | None = None,
    server_address: str | None = None,
    client_cert: Path | None = None,
    client_key: Path | None = None,
    ca_cert: Path | None = None,
    device_id: str | None = None,
    parallelism: int | None = None,
    chunk_bytes: int | None = None,
) -> AgentConfig:
    """Resolve config from a file plus command-line overrides.

    If no config file exists and no explicit `config_path` was provided, callers
    may still pass the required values directly. This keeps CI/dev invocations
    simple while ensuring operator installs can persist defaults.
    """

    loaded: AgentConfig | None = None
    candidate = config_path or default_config_path()
    if config_path is not None or candidate.exists():
        loaded = load_config(candidate)

    resolved_server = server_address or (loaded.server_address if loaded else None)
    resolved_landing = landing or (loaded.landing if loaded else None)
    resolved_operator = operator or (loaded.operator if loaded else None)

    return AgentConfig(
        landing=resolved_landing,
        operator=resolved_operator,
        source_kind=source_kind or (loaded.source_kind if loaded else DEFAULT_SOURCE_KIND),
        artifactclass=artifactclass or (loaded.artifactclass if loaded else DEFAULT_ARTIFACTCLASS),
        ledger_path=ledger_path or (loaded.ledger_path if loaded else None),
        confirm_interval_seconds=(
            confirm_interval_seconds
            if confirm_interval_seconds is not None
            else (loaded.confirm_interval_seconds if loaded else DEFAULT_CONFIRM_INTERVAL_SECONDS)
        ),
        server_address=resolved_server,
        client_cert=client_cert or (loaded.client_cert if loaded else None),
        client_key=client_key or (loaded.client_key if loaded else None),
        ca_cert=ca_cert or (loaded.ca_cert if loaded else None),
        device_id=device_id or (loaded.device_id if loaded else None),
        parallelism=parallelism if parallelism is not None else (loaded.parallelism if loaded else DEFAULT_PARALLELISM),
        chunk_bytes=chunk_bytes if chunk_bytes is not None else (loaded.chunk_bytes if loaded else DEFAULT_CHUNK_BYTES),
    )


def _config_from_payload(payload: dict[str, Any], *, base_dir: Path) -> AgentConfig:
    landing = _optional_string(payload, "landing")
    operator = _optional_string(payload, "operator")
    server_address = _optional_string(payload, "server_address")
    source_kind = _optional_string(payload, "source_kind") or DEFAULT_SOURCE_KIND
    artifactclass = _optional_string(payload, "artifactclass") or DEFAULT_ARTIFACTCLASS
    ledger_value = _optional_string(payload, "ledger_path")
    ledger_path = _resolve_path(ledger_value, base_dir=base_dir) if ledger_value else None
    client_cert = _optional_path(payload, "client_cert", base_dir=base_dir)
    client_key = _optional_path(payload, "client_key", base_dir=base_dir)
    ca_cert = _optional_path(payload, "ca_cert", base_dir=base_dir)
    device_id = _optional_string(payload, "device_id")
    interval_value = payload.get("confirm_interval_seconds", DEFAULT_CONFIRM_INTERVAL_SECONDS)
    if not isinstance(interval_value, int | float) or interval_value <= 0:
        raise AgentConfigError("confirm_interval_seconds must be a positive number")
    parallelism = payload.get("parallelism", DEFAULT_PARALLELISM)
    if not isinstance(parallelism, int):
        raise AgentConfigError("parallelism must be an integer")
    chunk_bytes = payload.get("chunk_bytes", DEFAULT_CHUNK_BYTES)
    if not isinstance(chunk_bytes, int):
        raise AgentConfigError("chunk_bytes must be an integer")
    return AgentConfig(
        landing=_resolve_path(landing, base_dir=base_dir) if landing else None,
        operator=operator,
        source_kind=source_kind,
        artifactclass=artifactclass,
        ledger_path=ledger_path,
        confirm_interval_seconds=float(interval_value),
        server_address=server_address,
        client_cert=client_cert,
        client_key=client_key,
        ca_cert=ca_cert,
        device_id=device_id,
        parallelism=parallelism,
        chunk_bytes=chunk_bytes,
    )


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise AgentConfigError(f"agent config field {key!r} must be a non-empty string")
    return value


def _optional_string(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise AgentConfigError(f"agent config field {key!r} must be a non-empty string")
    return value


def _resolve_path(value: str | Path, *, base_dir: Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return base_dir / path


def _optional_path(payload: dict[str, Any], key: str, *, base_dir: Path) -> Path | None:
    value = _optional_string(payload, key)
    return _resolve_path(value, base_dir=base_dir) if value else None


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()
