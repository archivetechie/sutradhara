"""Virtual arrangement catalog primitives.

P3.1 organizes already-archived logical assets into permanently mutable
catalog-only views. The member identity is the archived asset
``(logical_asset_hash, artifactclass)``; callers own transactions and these
helpers never commit or roll back.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from sqlalchemy import distinct, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from sutradhara.arrangement import canonical_member_path
from sutradhara.catalog.models import (
    AssetLocator,
    AssetTag,
    BundleMember,
    Copy,
    LogicalAsset,
    VirtualArrangement,
    VirtualArrangementHistory,
    VirtualArrangementMember,
)
from sutradhara.catalog.types import CopyHealth, is_content_hash


class VirtualArrangementError(ValueError):
    """Base class for virtual-arrangement validation failures."""


class VirtualArrangementNotArchived(VirtualArrangementError):
    """The requested asset has no healthy archived locator."""


class VirtualArrangementAmbiguousClass(VirtualArrangementError):
    """The requested asset is archived under several artifactclasses."""


@dataclass(frozen=True)
class VirtualMemberSummary:
    """Stable listing row for a virtual arrangement member."""

    id: int
    path: str
    logical_asset_hash: str
    artifactclass: str
    excluded: bool
    rejected: bool


def create_view(
    session: Session,
    name: str,
    *,
    description: str | None = None,
    created_by: str,
) -> VirtualArrangement:
    """Create a named virtual arrangement view."""

    if not name:
        raise VirtualArrangementError("virtual arrangement name must be non-empty")
    if not created_by:
        raise VirtualArrangementError("created_by must be non-empty")
    view = VirtualArrangement(
        name=name,
        description=description,
        created_by=created_by,
        created_at=_utcnow(),
        updated_at=_utcnow(),
    )
    session.add(view)
    try:
        session.flush()
    except IntegrityError as exc:
        raise VirtualArrangementError(f"virtual arrangement {name!r} already exists") from exc
    return view


def add_member(
    session: Session,
    view: int | str | VirtualArrangement,
    asset_hash: bytes,
    path: str,
    *,
    artifactclass: str | None = None,
    added_by: str,
) -> VirtualArrangementMember:
    """Place one archived asset at a virtual path inside a view."""

    if not added_by:
        raise VirtualArrangementError("added_by must be non-empty")
    _require_asset_hash(asset_hash)
    loaded = _get_view(session, view)
    asset = session.get(LogicalAsset, asset_hash)
    if asset is None:
        raise VirtualArrangementNotArchived(f"asset {asset_hash.hex()} is not in the catalog")
    resolved_class = _resolve_archived_artifactclass(session, asset_hash, artifactclass)
    virtual_path = canonical_member_path(path)
    _assert_no_member_identity_conflict(loaded, asset_hash, resolved_class)
    _assert_no_active_path_conflict(loaded, virtual_path)

    now = _utcnow()
    member = VirtualArrangementMember(
        va_id=loaded.id,
        logical_asset_hash=asset_hash,
        artifactclass=resolved_class,
        path=virtual_path,
        excluded=False,
        added_by=added_by,
        added_at=now,
        updated_at=now,
    )
    session.add(member)
    loaded.members.append(member)
    asset.virtual_members.append(member)
    loaded.updated_at = now
    try:
        session.flush()
    except IntegrityError as exc:
        raise VirtualArrangementError(
            f"asset {asset_hash.hex()} in {resolved_class!r} is already in view {loaded.name!r}"
        ) from exc
    return member


def move_member(
    session: Session,
    view: int | str | VirtualArrangement,
    from_path: str,
    to_path: str,
    *,
    actor: str,
) -> VirtualArrangementMember:
    """Move one live member and append a self-identifying history row."""

    if not actor:
        raise VirtualArrangementError("actor must be non-empty")
    loaded = _get_view(session, view)
    source_path = canonical_member_path(from_path)
    target_path = canonical_member_path(to_path)
    member = _one_member_by_path(loaded, source_path, excluded=False)
    if any(
        row.id != member.id and not row.excluded and row.path == target_path
        for row in loaded.members
    ):
        raise VirtualArrangementError(f"duplicate active virtual path {target_path!r}")

    now = _utcnow()
    old_path = member.path
    member.path = target_path
    member.updated_at = now
    loaded.updated_at = now
    session.add(
        VirtualArrangementHistory(
            va_id=loaded.id,
            va_member_id=member.id,
            logical_asset_hash=member.logical_asset_hash,
            artifactclass=member.artifactclass,
            old_path=old_path,
            new_path=target_path,
            actor=actor,
            changed_at=now,
        )
    )
    try:
        session.flush()
    except IntegrityError as exc:
        raise VirtualArrangementError(f"duplicate active virtual path {target_path!r}") from exc
    return member


def exclude_member(
    session: Session,
    view: int | str | VirtualArrangement,
    path: str,
) -> VirtualArrangementMember:
    """Hide one active member from a single view."""

    loaded = _get_view(session, view)
    member = _one_member_by_path(loaded, canonical_member_path(path), excluded=False)
    now = _utcnow()
    member.excluded = True
    member.updated_at = now
    loaded.updated_at = now
    session.flush()
    return member


def include_member(
    session: Session,
    view: int | str | VirtualArrangement,
    path: str,
) -> VirtualArrangementMember:
    """Re-show one excluded member in a single view."""

    loaded = _get_view(session, view)
    virtual_path = canonical_member_path(path)
    member = _one_member_by_path(loaded, virtual_path, excluded=True)
    if any(
        row.id != member.id and not row.excluded and row.path == virtual_path
        for row in loaded.members
    ):
        raise VirtualArrangementError(f"duplicate active virtual path {virtual_path!r}")
    now = _utcnow()
    member.excluded = False
    member.updated_at = now
    loaded.updated_at = now
    try:
        session.flush()
    except IntegrityError as exc:
        raise VirtualArrangementError(f"duplicate active virtual path {virtual_path!r}") from exc
    return member


def reject_asset(
    session: Session,
    asset_hash: bytes,
    *,
    actor: str,
    reason: str | None = None,
) -> LogicalAsset:
    """Mark one logical asset rejected without deleting any bytes or locators."""

    if not actor:
        raise VirtualArrangementError("actor must be non-empty")
    asset = _get_asset(session, asset_hash)
    asset.rejected_at = _utcnow()
    asset.rejected_by = actor
    asset.rejection_reason = reason
    session.flush()
    return asset


def unreject_asset(session: Session, asset_hash: bytes) -> LogicalAsset:
    """Clear the reject marker for one logical asset."""

    asset = _get_asset(session, asset_hash)
    asset.rejected_at = None
    asset.rejected_by = None
    asset.rejection_reason = None
    session.flush()
    return asset


def add_tag(
    session: Session,
    asset_hash: bytes,
    tag: str,
    *,
    actor: str,
) -> AssetTag:
    """Add or reactivate one governance tag for a logical asset."""

    if not tag:
        raise VirtualArrangementError("tag must be non-empty")
    if not actor:
        raise VirtualArrangementError("actor must be non-empty")
    _get_asset(session, asset_hash)
    existing = _active_tag(session, asset_hash, tag)
    if existing is not None:
        return existing
    row = AssetTag(
        logical_asset_hash=asset_hash,
        tag=tag,
        added_by=actor,
        added_at=_utcnow(),
    )
    session.add(row)
    try:
        session.flush()
    except IntegrityError as exc:
        raise VirtualArrangementError(f"active tag {tag!r} already exists") from exc
    return row


def remove_tag(
    session: Session,
    asset_hash: bytes,
    tag: str,
    *,
    actor: str,
) -> AssetTag:
    """Soft-delete one active governance tag."""

    if not actor:
        raise VirtualArrangementError("actor must be non-empty")
    row = _active_tag(session, asset_hash, tag)
    if row is None:
        raise VirtualArrangementError(f"asset {asset_hash.hex()} has no active tag {tag!r}")
    row.removed_by = actor
    row.removed_at = _utcnow()
    session.flush()
    return row


def list_view(
    session: Session,
    view: int | str | VirtualArrangement,
    *,
    include_hidden: bool = False,
) -> list[VirtualMemberSummary]:
    """Return member summaries for one view."""

    loaded = _get_view(session, view)
    rows = loaded.members if include_hidden else _visible_members(loaded.members)
    return [_summarize_member(row) for row in sorted(rows, key=lambda member: member.path)]


def show_view(
    session: Session,
    view: int | str | VirtualArrangement,
) -> VirtualArrangement:
    """Load one virtual arrangement with members for inspection."""

    return _get_view(session, view)


def resolve(
    session: Session,
    view: int | str | VirtualArrangement,
    path: str,
    *,
    include_hidden: bool = False,
) -> tuple[bytes, str]:
    """Resolve one virtual path to ``(logical_asset_hash, artifactclass)``."""

    loaded = _get_view(session, view)
    member = _one_member_by_path(
        loaded,
        canonical_member_path(path),
        excluded=None if include_hidden else False,
    )
    return member.logical_asset_hash, member.artifactclass


def _get_view(session: Session, view: int | str | VirtualArrangement) -> VirtualArrangement:
    if isinstance(view, VirtualArrangement):
        loaded = session.get(VirtualArrangement, view.id)
        if loaded is None:
            raise VirtualArrangementError(f"virtual arrangement {view.id} does not exist")
        return loaded
    stmt = select(VirtualArrangement).options(
        selectinload(VirtualArrangement.members).selectinload(
            VirtualArrangementMember.logical_asset
        )
    )
    if isinstance(view, int):
        stmt = stmt.where(VirtualArrangement.id == view)
        label = str(view)
    else:
        stmt = stmt.where(VirtualArrangement.name == view)
        label = view
    loaded = session.scalars(stmt).first()
    if loaded is None:
        raise VirtualArrangementError(f"virtual arrangement {label!r} does not exist")
    return loaded


def _get_asset(session: Session, asset_hash: bytes) -> LogicalAsset:
    _require_asset_hash(asset_hash)
    asset = session.get(LogicalAsset, asset_hash)
    if asset is None:
        raise VirtualArrangementError(f"asset {asset_hash.hex()} does not exist")
    return asset


def _require_asset_hash(asset_hash: bytes) -> None:
    if not is_content_hash(asset_hash):
        raise VirtualArrangementError("asset_hash must be a 32-byte SHA-256 hash")


def _resolve_archived_artifactclass(
    session: Session,
    asset_hash: bytes,
    artifactclass: str | None,
) -> str:
    available = _healthy_archived_artifactclasses(session, asset_hash)
    if not available:
        raise VirtualArrangementNotArchived(
            f"asset {asset_hash.hex()} has no healthy archived locator"
        )
    if artifactclass is not None:
        if artifactclass not in available:
            raise VirtualArrangementNotArchived(
                f"asset {asset_hash.hex()} is not archived under artifactclass {artifactclass!r}"
            )
        return artifactclass
    if len(available) > 1:
        raise VirtualArrangementAmbiguousClass(
            f"asset {asset_hash.hex()} is archived under multiple artifactclasses: "
            + ", ".join(repr(value) for value in available)
        )
    return available[0]


def _healthy_archived_artifactclasses(session: Session, asset_hash: bytes) -> list[str]:
    # Member grain (§5): the class comes from the asset's own member row in
    # the locator's bundle (hash + class) — co-resident classes never leak in.
    rows = session.scalars(
        select(distinct(BundleMember.artifactclass))
        .join(AssetLocator, AssetLocator.bundle_id == BundleMember.bundle_id)
        .join(Copy, Copy.id == AssetLocator.copy_id)
        .where(
            AssetLocator.logical_asset_hash == asset_hash,
            BundleMember.logical_asset_hash == asset_hash,
            Copy.health == CopyHealth.OK,
            Copy.deleted_at.is_(None),
        )
        .order_by(BundleMember.artifactclass)
    )
    return list(rows)


def _one_member_by_path(
    view: VirtualArrangement,
    path: str,
    *,
    excluded: bool | None,
) -> VirtualArrangementMember:
    matches = [
        member
        for member in view.members
        if member.path == path and (excluded is None or member.excluded is excluded)
    ]
    if not matches:
        state = (
            "member" if excluded is None else ("excluded member" if excluded else "active member")
        )
        raise VirtualArrangementError(f"view {view.name!r} has no {state} {path!r}")
    if len(matches) > 1:
        raise VirtualArrangementError(f"view {view.name!r} has ambiguous member path {path!r}")
    return matches[0]


def _assert_no_active_path_conflict(view: VirtualArrangement, path: str) -> None:
    if any(not member.excluded and member.path == path for member in view.members):
        raise VirtualArrangementError(f"duplicate active virtual path {path!r}")


def _assert_no_member_identity_conflict(
    view: VirtualArrangement,
    asset_hash: bytes,
    artifactclass: str,
) -> None:
    if any(
        member.logical_asset_hash == asset_hash and member.artifactclass == artifactclass
        for member in view.members
    ):
        raise VirtualArrangementError(
            f"asset {asset_hash.hex()} in {artifactclass!r} is already in view {view.name!r}"
        )


def _visible_members(
    members: list[VirtualArrangementMember],
) -> list[VirtualArrangementMember]:
    return [
        member
        for member in members
        if not member.excluded and member.logical_asset.rejected_at is None
    ]


def _summarize_member(member: VirtualArrangementMember) -> VirtualMemberSummary:
    return VirtualMemberSummary(
        id=member.id,
        path=member.path,
        logical_asset_hash=member.logical_asset_hash.hex(),
        artifactclass=member.artifactclass,
        excluded=member.excluded,
        rejected=member.logical_asset.rejected_at is not None,
    )


def _active_tag(session: Session, asset_hash: bytes, tag: str) -> AssetTag | None:
    _require_asset_hash(asset_hash)
    return session.scalars(
        select(AssetTag).where(
            AssetTag.logical_asset_hash == asset_hash,
            AssetTag.tag == tag,
            AssetTag.removed_at.is_(None),
        )
    ).first()


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


__all__ = [
    "VirtualArrangementAmbiguousClass",
    "VirtualArrangementError",
    "VirtualArrangementNotArchived",
    "VirtualMemberSummary",
    "add_member",
    "add_tag",
    "create_view",
    "exclude_member",
    "include_member",
    "list_view",
    "move_member",
    "reject_asset",
    "remove_tag",
    "resolve",
    "show_view",
    "unreject_asset",
]
