"""Strict artifactclass policy document parsing.

The policy document is intentionally small. It names an artifactclass ruleset,
the pool memberships to activate, bundling thresholds, restore preference, and
whether incoming material is expected to be compliant or messy.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tomllib
import warnings
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import ArtifactClassPolicyRecord, ArtifactClassPool, Pool


class ArtifactClassPolicyError(ValueError):
    """A policy document is missing required data or has unknown keys."""


class UnknownPolicyPool(ArtifactClassPolicyError):
    """A policy references a pool that is not declared in the catalog."""


class ArtifactClassPolicyWarning(UserWarning):
    """A policy is valid but needs operator attention."""


@dataclass(frozen=True)
class PlacementPolicy:
    pool: str
    role: str | None = None
    active: bool = True


@dataclass(frozen=True)
class BundlingPolicy:
    target_gb: float
    max_age_seconds: int

    @property
    def target_bytes(self) -> int:
        """Bundling target rounded to bytes."""
        return int(self.target_gb * 1024**3)


@dataclass(frozen=True)
class AppleDoubleStagingPolicy:
    action: str = "off"
    tool: str = "sutradhara-parser"
    on_error: str = "hold"
    record: bool = True

    def to_json(self) -> dict[str, object]:
        return {
            "action": self.action,
            "tool": self.tool,
            "on_error": self.on_error,
            "record": self.record,
        }


@dataclass(frozen=True)
class CompressionStagingPolicy:
    codec: str = "off"
    level: int | None = None
    globs: tuple[str, ...] = ()
    min_bytes: int | None = None

    def to_json(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "codec": self.codec,
            "globs": list(self.globs),
        }
        if self.level is not None:
            payload["level"] = self.level
        if self.min_bytes is not None:
            payload["min_bytes"] = self.min_bytes
        return payload


@dataclass(frozen=True)
class StagingPolicy:
    appledouble: AppleDoubleStagingPolicy = AppleDoubleStagingPolicy()
    compression: CompressionStagingPolicy = CompressionStagingPolicy()

    def to_json(self) -> dict[str, object]:
        return {
            "appledouble": self.appledouble.to_json(),
            "compression": self.compression.to_json(),
        }


@dataclass(frozen=True)
class HdcachePolicy:
    enabled: bool = False
    privacy_level: str = "none"

    def to_json(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "privacy_level": self.privacy_level,
        }


@dataclass(frozen=True)
class ArtifactClassPolicy:
    ruleset: str
    placements: tuple[PlacementPolicy, ...]
    bundling: BundlingPolicy
    restore_preference: tuple[str, ...]
    expect: str
    staging: StagingPolicy = StagingPolicy()
    hdcache: HdcachePolicy = HdcachePolicy()


_DURATION_RE = re.compile(r"^(?P<value>[1-9][0-9]*)(?P<unit>s|m|h|d)$")
_DURATION_MULTIPLIERS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def load_artifactclass_policy(path: Path | str) -> ArtifactClassPolicy:
    """Parse and validate an artifactclass policy TOML file."""
    policy_path = Path(path)
    return parse_artifactclass_policy(
        policy_path.read_text(encoding="utf-8"),
        source=str(policy_path),
    )


def get_artifactclass_policy(
    session: Session,
    artifactclass: str,
) -> ArtifactClassPolicyRecord:
    """Return the active persisted policy for an artifactclass."""
    policy = session.get(ArtifactClassPolicyRecord, artifactclass)
    if policy is None:
        raise ArtifactClassPolicyError(f"artifactclass {artifactclass!r} has no applied policy")
    return policy


def staging_policy_from_json(raw: object) -> StagingPolicy:
    """Hydrate a persisted normalized staging policy JSON document."""
    return _parse_staging(raw, "staging_config")


def hdcache_policy_from_json(raw: object) -> HdcachePolicy:
    """Hydrate a persisted normalized hdcache policy JSON document."""
    return _parse_hdcache(raw, "hdcache_config")


def hdcache_privacy_capability_map_from_env() -> dict[str, str]:
    """Load the fail-closed hdcache privacy-level capability map."""

    raw = os.environ.get("SUTRADHARA_HDCACHE_PRIVACY_CAPABILITIES")
    if raw is None or raw == "":
        return {"p2": "can_restore_p2", "p3": "can_restore_p3"}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ArtifactClassPolicyError(
            "SUTRADHARA_HDCACHE_PRIVACY_CAPABILITIES must be a JSON object"
        ) from exc
    if not isinstance(parsed, dict):
        raise ArtifactClassPolicyError(
            "SUTRADHARA_HDCACHE_PRIVACY_CAPABILITIES must be a JSON object"
        )
    result: dict[str, str] = {}
    for level, capability in parsed.items():
        if not isinstance(level, str) or not level:
            raise ArtifactClassPolicyError("hdcache privacy capability levels must be strings")
        if not isinstance(capability, str) or not capability:
            raise ArtifactClassPolicyError("hdcache privacy capabilities must be strings")
        result[level] = capability
    return result


def parse_artifactclass_policy(
    text: str,
    *,
    source: str = "<string>",
) -> ArtifactClassPolicy:
    """Parse a strict artifactclass policy TOML string."""
    try:
        raw = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise ArtifactClassPolicyError(f"{source}: invalid TOML: {exc}") from exc

    _require_keys(
        raw,
        {"ruleset", "placements", "bundling", "restore", "expect"},
        source,
    )
    _reject_keys(
        raw,
        {"ruleset", "placements", "bundling", "restore", "expect", "staging", "hdcache"},
        source,
    )

    ruleset = _required_str(raw["ruleset"], f"{source}: ruleset")
    expect = _required_str(raw["expect"], f"{source}: expect")
    if expect not in {"compliant", "messy"}:
        raise ArtifactClassPolicyError(f"{source}: expect must be 'compliant' or 'messy'")

    placements_raw = raw["placements"]
    if not isinstance(placements_raw, list) or not placements_raw:
        raise ArtifactClassPolicyError(f"{source}: placements must be a non-empty array")
    placements = tuple(
        _parse_placement(item, f"{source}: placements[{index}]")
        for index, item in enumerate(placements_raw)
    )
    _reject_duplicate_pools((placement.pool for placement in placements), source)

    bundling_raw = _required_table(raw["bundling"], f"{source}: bundling")
    _require_keys(bundling_raw, {"target_gb", "max_age"}, f"{source}: bundling")
    _reject_keys(bundling_raw, {"target_gb", "max_age"}, f"{source}: bundling")
    bundling = BundlingPolicy(
        target_gb=_positive_number(
            bundling_raw["target_gb"],
            f"{source}: bundling.target_gb",
        ),
        max_age_seconds=_duration_seconds(
            bundling_raw["max_age"],
            f"{source}: bundling.max_age",
        ),
    )

    restore_raw = _required_table(raw["restore"], f"{source}: restore")
    _require_keys(restore_raw, {"preference"}, f"{source}: restore")
    _reject_keys(restore_raw, {"preference"}, f"{source}: restore")
    restore_preference = _str_tuple(
        restore_raw["preference"],
        f"{source}: restore.preference",
    )
    if not restore_preference:
        raise ArtifactClassPolicyError(f"{source}: restore.preference must not be empty")

    staging = _parse_staging(raw.get("staging"), f"{source}: staging")
    hdcache = _parse_hdcache(raw.get("hdcache"), f"{source}: hdcache")

    return ArtifactClassPolicy(
        ruleset=ruleset,
        placements=placements,
        bundling=bundling,
        restore_preference=restore_preference,
        expect=expect,
        staging=staging,
        hdcache=hdcache,
    )


def apply_artifactclass_policy(
    session: Session,
    artifactclass: str,
    policy: ArtifactClassPolicy,
    *,
    source: str | None = None,
    source_text: str | None = None,
) -> None:
    """Apply placement membership from a validated policy document.

    Pools must already exist. Memberships listed in the document are upserted in
    document order. Existing memberships for the artifactclass that are absent
    from the document are marked inactive.
    """
    pool_ids = [placement.pool for placement in policy.placements]
    referenced_pool_ids = set(pool_ids) | set(policy.restore_preference)
    pools = {
        pool.id: pool
        for pool in session.scalars(select(Pool).where(Pool.id.in_(referenced_pool_ids)))
    }
    missing = sorted(set(pool_ids) - set(pools))
    if missing:
        raise UnknownPolicyPool(
            f"artifactclass {artifactclass!r} references unknown pools: " + ", ".join(missing)
        )
    _validate_restore_preference_pools(policy, pools)
    _validate_hdcache_privacy_mapping(policy)
    _warn_if_appledouble_ruleset_preservation_is_unproven(policy)

    existing = {
        membership.pool_id: membership
        for membership in session.scalars(
            select(ArtifactClassPool).where(ArtifactClassPool.artifactclass == artifactclass)
        )
    }
    active_pool_ids = set(pool_ids)
    for sort_order, placement in enumerate(policy.placements):
        membership = existing.get(placement.pool)
        if membership is None:
            membership = ArtifactClassPool(
                artifactclass=artifactclass,
                pool_id=placement.pool,
            )
            session.add(membership)
        membership.active = placement.active
        membership.role = placement.role
        membership.sort_order = sort_order

    for pool_id, membership in existing.items():
        if pool_id not in active_pool_ids:
            membership.active = False

    record = session.get(ArtifactClassPolicyRecord, artifactclass)
    if record is None:
        record = ArtifactClassPolicyRecord(artifactclass=artifactclass)
        session.add(record)
    record.ruleset = policy.ruleset
    record.expect = policy.expect
    record.target_bytes = policy.bundling.target_bytes
    record.max_age_seconds = policy.bundling.max_age_seconds
    record.restore_preference = list(policy.restore_preference)
    record.staging_config = policy.staging.to_json()
    record.hdcache_config = policy.hdcache.to_json()
    record.policy_source = source
    record.policy_sha256 = (
        hashlib.sha256(source_text.encode("utf-8")).hexdigest() if source_text is not None else None
    )
    session.flush()


def _warn_if_appledouble_ruleset_preservation_is_unproven(
    policy: ArtifactClassPolicy,
) -> None:
    if policy.staging.appledouble.action != "merge-to-xattrs":
        return
    warnings.warn(
        "AppleDouble merge writes user.com.apple.* xattrs before rem ruleset "
        f"{policy.ruleset!r}; verify that the ruleset preserves com.apple.* metadata",
        ArtifactClassPolicyWarning,
        stacklevel=3,
    )


def _validate_restore_preference_pools(
    policy: ArtifactClassPolicy,
    pools: dict[str, Pool],
) -> None:
    missing = sorted(set(policy.restore_preference) - set(pools))
    if missing:
        raise ArtifactClassPolicyError(
            "restore.preference references unknown pools: " + ", ".join(missing)
        )
    # M3/D3: warn here for restore-preference pools that are write-fenced.


def apply_artifactclass_policy_file(
    session: Session,
    artifactclass: str,
    path: Path | str,
) -> ArtifactClassPolicy:
    """Load, validate, and persist an artifactclass policy file."""
    policy_path = Path(path)
    text = policy_path.read_text(encoding="utf-8")
    policy = parse_artifactclass_policy(text, source=str(policy_path))
    apply_artifactclass_policy(
        session,
        artifactclass,
        policy,
        source=str(policy_path),
        source_text=text,
    )
    return policy


def _parse_placement(raw: object, label: str) -> PlacementPolicy:
    table = _required_table(raw, label)
    _require_keys(table, {"pool"}, label)
    _reject_keys(table, {"pool", "role", "active"}, label)
    active = table.get("active", True)
    if not isinstance(active, bool):
        raise ArtifactClassPolicyError(f"{label}.active must be a boolean")
    role_raw = table.get("role")
    return PlacementPolicy(
        pool=_required_str(table["pool"], f"{label}.pool"),
        role=None if role_raw is None else _required_str(role_raw, f"{label}.role"),
        active=active,
    )


def _parse_staging(raw: object, label: str) -> StagingPolicy:
    if raw is None:
        return StagingPolicy()
    table = _required_table(raw, label)
    _reject_keys(table, {"appledouble", "compression"}, label)
    return StagingPolicy(
        appledouble=_parse_appledouble(table.get("appledouble"), f"{label}.appledouble"),
        compression=_parse_compression(table.get("compression"), f"{label}.compression"),
    )


def _parse_hdcache(raw: object, label: str) -> HdcachePolicy:
    if raw is None:
        return HdcachePolicy()
    table = _required_table(raw, label)
    _reject_keys(table, {"enabled", "privacy_level"}, label)
    enabled = table.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ArtifactClassPolicyError(f"{label}.enabled must be a boolean")
    privacy_level = _optional_str(table.get("privacy_level"), f"{label}.privacy_level", default="none")
    if privacy_level != "none" and re.fullmatch(r"p[1-9][0-9]*", privacy_level) is None:
        raise ArtifactClassPolicyError(f"{label}.privacy_level must be 'none' or a p<N> level")
    return HdcachePolicy(enabled=enabled, privacy_level=privacy_level)


def _validate_hdcache_privacy_mapping(policy: ArtifactClassPolicy) -> None:
    privacy_level = policy.hdcache.privacy_level
    if privacy_level == "none":
        return
    mapping = hdcache_privacy_capability_map_from_env()
    if privacy_level not in mapping:
        raise ArtifactClassPolicyError(
            f"hdcache privacy_level {privacy_level!r} has no configured restore capability"
        )


def _parse_appledouble(raw: object, label: str) -> AppleDoubleStagingPolicy:
    if raw is None:
        return AppleDoubleStagingPolicy()
    table = _required_table(raw, label)
    _reject_keys(table, {"action", "tool", "on_error", "record"}, label)
    action = _optional_str(table.get("action"), f"{label}.action", default="off")
    if action not in {"off", "merge-to-xattrs"}:
        raise ArtifactClassPolicyError(f"{label}.action must be 'off' or 'merge-to-xattrs'")
    tool = _optional_str(table.get("tool"), f"{label}.tool", default="sutradhara-parser")
    if tool not in {"netatalk-ad", "sutradhara-parser"}:
        raise ArtifactClassPolicyError(f"{label}.tool must be 'netatalk-ad' or 'sutradhara-parser'")
    on_error = _optional_str(table.get("on_error"), f"{label}.on_error", default="hold")
    if on_error not in {"hold", "fail"}:
        raise ArtifactClassPolicyError(f"{label}.on_error must be 'hold' or 'fail'")
    record = table.get("record", True)
    if not isinstance(record, bool):
        raise ArtifactClassPolicyError(f"{label}.record must be a boolean")
    if action == "merge-to-xattrs" and not record:
        raise ArtifactClassPolicyError(f"{label}.record must be true when merging AppleDouble")
    return AppleDoubleStagingPolicy(
        action=action,
        tool=tool,
        on_error=on_error,
        record=record,
    )


def _parse_compression(raw: object, label: str) -> CompressionStagingPolicy:
    if raw is None:
        return CompressionStagingPolicy()
    table = _required_table(raw, label)
    _reject_keys(table, {"codec", "level", "globs", "min_bytes"}, label)
    codec = _optional_str(table.get("codec"), f"{label}.codec", default="off")
    if codec not in {"off", "zstd"}:
        raise ArtifactClassPolicyError(f"{label}.codec must be 'off' or 'zstd'")
    level = _optional_int(table.get("level"), f"{label}.level")
    if codec == "zstd" and level is None:
        raise ArtifactClassPolicyError(f"{label}.level is required when codec='zstd'")
    if level is not None and not 1 <= level <= 22:
        raise ArtifactClassPolicyError(f"{label}.level must be between 1 and 22")
    globs = _str_tuple(table.get("globs", []), f"{label}.globs")
    min_bytes = _optional_int(table.get("min_bytes"), f"{label}.min_bytes")
    if min_bytes is not None and min_bytes < 0:
        raise ArtifactClassPolicyError(f"{label}.min_bytes must be non-negative")
    return CompressionStagingPolicy(
        codec=codec,
        level=level,
        globs=globs,
        min_bytes=min_bytes,
    )


def _require_keys(raw: dict[str, object], required: set[str], label: str) -> None:
    missing = sorted(required - set(raw))
    if missing:
        raise ArtifactClassPolicyError(f"{label}: missing required key(s): {', '.join(missing)}")


def _reject_keys(raw: dict[str, object], allowed: set[str], label: str) -> None:
    extra = sorted(set(raw) - allowed)
    if extra:
        raise ArtifactClassPolicyError(f"{label}: unknown key(s): {', '.join(extra)}")


def _required_table(raw: object, label: str) -> dict[str, object]:
    if not isinstance(raw, dict):
        raise ArtifactClassPolicyError(f"{label} must be a table")
    return raw


def _required_str(raw: object, label: str) -> str:
    if not isinstance(raw, str) or not raw:
        raise ArtifactClassPolicyError(f"{label} must be a non-empty string")
    return raw


def _optional_str(raw: object, label: str, *, default: str) -> str:
    if raw is None:
        return default
    return _required_str(raw, label)


def _optional_int(raw: object, label: str) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ArtifactClassPolicyError(f"{label} must be an integer")
    return raw


def _positive_number(raw: object, label: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ArtifactClassPolicyError(f"{label} must be a positive number")
    value = float(raw)
    if value <= 0:
        raise ArtifactClassPolicyError(f"{label} must be positive")
    return value


def _duration_seconds(raw: object, label: str) -> int:
    if isinstance(raw, int) and not isinstance(raw, bool):
        if raw <= 0:
            raise ArtifactClassPolicyError(f"{label} must be positive")
        return raw
    if not isinstance(raw, str):
        raise ArtifactClassPolicyError(f"{label} must be a duration like '48h' or integer seconds")
    match = _DURATION_RE.match(raw)
    if match is None:
        raise ArtifactClassPolicyError(
            f"{label} must be a duration like '48h', '7d', '30m', or '60s'"
        )
    return int(match.group("value")) * _DURATION_MULTIPLIERS[match.group("unit")]


def _str_tuple(raw: object, label: str) -> tuple[str, ...]:
    if isinstance(raw, str):
        return (raw,)
    if not isinstance(raw, list):
        raise ArtifactClassPolicyError(f"{label} must be a string or string array")
    values: list[str] = []
    for index, item in enumerate(raw):
        values.append(_required_str(item, f"{label}[{index}]"))
    return tuple(values)


def _reject_duplicate_pools(pool_ids: Iterable[str], source: str) -> None:
    seen: set[str] = set()
    for pool_id in pool_ids:
        if pool_id in seen:
            raise ArtifactClassPolicyError(
                f"{source}: placements contain duplicate pool {pool_id!r}"
            )
        seen.add(pool_id)
