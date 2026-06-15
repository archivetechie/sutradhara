"""Strict artifactclass policy document parsing.

The policy document is intentionally small. It names an artifactclass ruleset,
the pool memberships to activate, bundling thresholds, restore preference, and
whether incoming material is expected to be compliant or messy.
"""

from __future__ import annotations

import hashlib
import re
import tomllib
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
class ArtifactClassPolicy:
    ruleset: str
    placements: tuple[PlacementPolicy, ...]
    bundling: BundlingPolicy
    restore_preference: tuple[str, ...]
    expect: str


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
        {"ruleset", "placements", "bundling", "restore", "expect"},
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

    return ArtifactClassPolicy(
        ruleset=ruleset,
        placements=placements,
        bundling=bundling,
        restore_preference=restore_preference,
        expect=expect,
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
    pools = {pool.id: pool for pool in session.scalars(select(Pool).where(Pool.id.in_(pool_ids)))}
    missing = sorted(set(pool_ids) - set(pools))
    if missing:
        raise UnknownPolicyPool(
            f"artifactclass {artifactclass!r} references unknown pools: " + ", ".join(missing)
        )

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
    record.policy_source = source
    record.policy_sha256 = (
        hashlib.sha256(source_text.encode("utf-8")).hexdigest() if source_text is not None else None
    )
    session.flush()


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
