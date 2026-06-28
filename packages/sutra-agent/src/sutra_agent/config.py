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


class AgentConfigError(ValueError):
    """Raised when an agent config file is missing or invalid."""


@dataclass(frozen=True)
class AgentConfig:
    """Resolved receive-agent configuration."""

    landing: Path
    operator: str
    source_kind: str = DEFAULT_SOURCE_KIND
    artifactclass: str = DEFAULT_ARTIFACTCLASS
    ledger_path: Path | None = None
    confirm_interval_seconds: float = DEFAULT_CONFIRM_INTERVAL_SECONDS

    def __post_init__(self) -> None:
        if not self.operator:
            raise AgentConfigError("operator must be non-empty")
        if not self.source_kind:
            raise AgentConfigError("source_kind must be non-empty")
        if not self.artifactclass:
            raise AgentConfigError("artifactclass must be non-empty")
        if self.confirm_interval_seconds <= 0:
            raise AgentConfigError("confirm_interval_seconds must be a positive number")

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
        "landing": str(config.landing),
        "operator": config.operator,
        "source_kind": config.source_kind,
        "artifactclass": config.artifactclass,
        "confirm_interval_seconds": config.confirm_interval_seconds,
    }
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

    resolved_landing = landing or (loaded.landing if loaded else None)
    resolved_operator = operator or (loaded.operator if loaded else None)
    if resolved_landing is None:
        raise AgentConfigError("landing is required; pass --landing or initialize config")
    if resolved_operator is None:
        raise AgentConfigError("operator is required; pass --operator or initialize config")

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
    )


def _config_from_payload(payload: dict[str, Any], *, base_dir: Path) -> AgentConfig:
    landing = _required_string(payload, "landing")
    operator = _required_string(payload, "operator")
    source_kind = _optional_string(payload, "source_kind") or DEFAULT_SOURCE_KIND
    artifactclass = _optional_string(payload, "artifactclass") or DEFAULT_ARTIFACTCLASS
    ledger_value = _optional_string(payload, "ledger_path")
    ledger_path = _resolve_path(ledger_value, base_dir=base_dir) if ledger_value else None
    interval_value = payload.get("confirm_interval_seconds", DEFAULT_CONFIRM_INTERVAL_SECONDS)
    if not isinstance(interval_value, int | float) or interval_value <= 0:
        raise AgentConfigError("confirm_interval_seconds must be a positive number")
    return AgentConfig(
        landing=_resolve_path(landing, base_dir=base_dir),
        operator=operator,
        source_kind=source_kind,
        artifactclass=artifactclass,
        ledger_path=ledger_path,
        confirm_interval_seconds=float(interval_value),
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
