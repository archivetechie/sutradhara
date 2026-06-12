"""Placement-tagged replication orchestration.

This module is the first-cut fan-out library the harness drives for rem_tape
multi-pool replication. The policy layer speaks only in tagged placements:
`content_class` selects an asset family and `copy_class` selects durable copy
slots. Backend-native details such as rem_tape `pool_id` and `tape_uuid` remain
inside `Copy.native_locator`.
"""

from __future__ import annotations

import contextlib
import hashlib
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any, Protocol, TypedDict, TypeVar

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.backend.port import (
    ByteRange,
    CopyRecord,
    StorageBackend,
    TaggedPlacement,
)
from sutradhara.catalog.copies import add_copy
from sutradhara.catalog.models import Copy, PlacementTagPin
from sutradhara.catalog.types import BackendKind, CopyHealth, CopySource
from sutradhara.keys import KeyEpoch, KeyRegistry
from sutradhara.sealing.policy import DEFAULT_POLICY, RepresentationPolicy
from sutradhara.sealing.port import Opener, Representation, Sealer, SealResult
from sutradhara.sealing.rao import RAO_CHUNK_SIZE, RaoCliOpener, RaoCliSealer


class ReplicationError(Exception):
    """Base class for replication policy and completeness errors."""


class DuplicatePlacementClass(ReplicationError):
    """Two target placements claim the same content/copy tag pair."""


class ReplicationInvariantError(ReplicationError):
    """A durability invariant was violated by existing catalog rows."""


class PlacementTagDrift(ReplicationInvariantError):
    """A discovered placement's routing tags no longer match its pin."""


class SelfHealUnavailable(ReplicationError):
    """A missing copy cannot be rebuilt from the available healthy copies."""


class WritableStorageBackend(StorageBackend, Protocol):
    """Storage backend surface needed by the first fan-out writer."""

    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        """Write `source` to this backend's placement id and return a copy."""
        ...


class ReplicationStatus(TypedDict):
    complete: bool
    have: set[TaggedPlacement]
    want: set[TaggedPlacement]
    missing: set[TaggedPlacement]


TBackend = TypeVar("TBackend", bound=StorageBackend)
BackendMap = Mapping[int, StorageBackend]
WritableBackendMap = Mapping[int, WritableStorageBackend]
PlacementTarget = tuple[TBackend, TaggedPlacement]


def target_placements(
    content_type: str,
    backends: Mapping[int, TBackend],
    *,
    policy: RepresentationPolicy = DEFAULT_POLICY,
    key_epoch: str | None = None,
) -> set[PlacementTarget[TBackend]]:
    """Return every placement matching `content_type`, one per copy class.

    This is the swappable policy point. Today's fixed policy is "all placements
    whose `content_class` matches the asset content type, with duplicate
    `copy_class` values rejected instead of guessed."
    """
    targets: set[PlacementTarget[TBackend]] = set()
    by_copy_class: dict[str, tuple[TBackend, TaggedPlacement]] = {}
    for backend in backends.values():
        for placement in backend.list_tagged_placements():
            if placement.content_class != content_type:
                continue
            existing = by_copy_class.get(placement.copy_class)
            if existing is not None:
                _, other = existing
                raise DuplicatePlacementClass(
                    f"content_type {content_type!r} has duplicate copy_class "
                    f"{placement.copy_class!r}: {other.backend_name}/"
                    f"{other.placement_id} and {placement.backend_name}/"
                    f"{placement.placement_id}"
                )
            target = (backend, _apply_representation_policy(placement, policy, key_epoch))
            by_copy_class[placement.copy_class] = target
            targets.add(target)
    return targets


def replicate_asset(
    session: Session,
    asset_hash: bytes,
    source_path: Path | str,
    content_type: str,
    *,
    backends: WritableBackendMap,
    sealer: Sealer | None = None,
    policy: RepresentationPolicy = DEFAULT_POLICY,
    key_epoch: str | None = None,
) -> list[Copy]:
    """Replicate one asset to every target placement and record Copy rows.

    Existing healthy copies in target placements are reused, so a rerun does not
    create extra physical writes. New copies are written via the backend's
    existing `write_object_to_pool` method and recorded through `add_copy`.
    """
    validate_placement_tags(session, backends)
    targets = _sorted_targets(
        target_placements(
            content_type,
            backends,
            policy=policy,
            key_epoch=key_epoch,
        )
    )
    existing = _healthy_copies_by_placement(session, asset_hash, targets)
    backend_ids = {id(backend): backend_id for backend_id, backend in backends.items()}
    sealer = sealer or RaoCliSealer(KeyRegistry())

    copies: list[Copy] = []
    for backend, placement in targets:
        backend_id = backend_ids[id(backend)]
        existing_copy = existing.get(placement)
        if existing_copy is not None:
            _pin_or_validate_placement(session, backend_id, placement)
            copies.append(existing_copy)
            continue

        representation = Representation(placement.representation)
        with sealer.seal(
            source_path,
            representation,
            key_epoch=_epoch_for(placement, representation),
        ) as sealed:
            record = backend.write_object_to_pool(
                sealed.sealed_path,
                placement.placement_id,
            )
            _assert_copy_integrity(asset_hash, record, sealed, placement)
        copy, _ = add_copy(
            session,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator=record.native_locator,
            integrity_hash=sealed.stored_digest,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
            storage_metadata=_copy_storage_metadata(representation),
        )
        _pin_or_validate_placement(session, backend_id, placement)
        copies.append(copy)
    return copies


def repair(
    session: Session,
    asset_hash: bytes,
    source_path: Path | str,
    content_type: str,
    *,
    backends: WritableBackendMap,
    sealer: Sealer | None = None,
    policy: RepresentationPolicy = DEFAULT_POLICY,
    key_epoch: str | None = None,
) -> list[Copy]:
    """Write copies for placements currently missing from replication status."""
    status = replication_status(
        session,
        asset_hash,
        content_type,
        backends,
        policy=policy,
        key_epoch=key_epoch,
    )
    if not status["missing"]:
        return []

    targets = _sorted_targets(
        target_placements(
            content_type,
            backends,
            policy=policy,
            key_epoch=key_epoch,
        )
    )
    missing = status["missing"]
    backend_ids = {id(backend): backend_id for backend_id, backend in backends.items()}
    sealer = sealer or RaoCliSealer(KeyRegistry())

    repaired: list[Copy] = []
    for backend, placement in targets:
        if placement not in missing:
            continue
        backend_id = backend_ids[id(backend)]
        representation = Representation(placement.representation)
        with sealer.seal(
            source_path,
            representation,
            key_epoch=_epoch_for(placement, representation),
        ) as sealed:
            record = backend.write_object_to_pool(
                sealed.sealed_path,
                placement.placement_id,
            )
            _assert_copy_integrity(asset_hash, record, sealed, placement)
        copy, _ = add_copy(
            session,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator=record.native_locator,
            integrity_hash=sealed.stored_digest,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
            storage_metadata=_copy_storage_metadata(representation),
        )
        _pin_or_validate_placement(session, backend_id, placement)
        repaired.append(copy)
    return repaired


def self_heal(
    session: Session,
    asset_hash: bytes,
    content_type: str,
    *,
    backends: WritableBackendMap,
    opener: Opener | None = None,
    sealer: Sealer | None = None,
    policy: RepresentationPolicy = DEFAULT_POLICY,
    key_epoch: str | None = None,
    chooser: Callable[[Sequence[Copy]], Copy] | None = None,
) -> list[Copy]:
    """Rebuild missing target copies from a surviving healthy copy.

    The source is read from an existing backend copy, opened to plaintext under
    that copy's representation, checked against the logical asset hash, then
    passed through `repair()` so the missing placement is sealed according to
    its own representation policy.
    """
    status = replication_status(
        session,
        asset_hash,
        content_type,
        backends,
        policy=policy,
        key_epoch=key_epoch,
    )
    if not status["missing"]:
        return []

    source = select_restore_source(session, asset_hash, chooser=chooser)
    if source is None:
        raise SelfHealUnavailable(
            f"cannot self-heal {asset_hash.hex()}: no healthy source copy"
        )

    source_backend = backends.get(source.backend_id)
    if source_backend is None:
        raise SelfHealUnavailable(
            f"cannot self-heal {asset_hash.hex()}: source copy id={source.id} "
            f"uses backend_id={source.backend_id}, which is not available"
        )

    source_placement = _placement_for_copy(
        source,
        content_type,
        backends,
        policy=policy,
        key_epoch=key_epoch,
    )
    if source_placement is None:
        raise SelfHealUnavailable(
            f"cannot self-heal {asset_hash.hex()}: source copy id={source.id} "
            "does not belong to a target placement"
        )

    opener = opener or RaoCliOpener(KeyRegistry())
    representation = Representation(source_placement.representation)
    with _materialized_copy_path(source_backend, source) as stored_path, opener.open(
        stored_path,
        representation,
        key_epoch=_epoch_for(source_placement, representation),
    ) as plaintext_path:
        plaintext_digest = _sha256_file(plaintext_path)
        if plaintext_digest != asset_hash:
            raise ReplicationInvariantError(
                "self-heal source plaintext hash differs from requested asset "
                f"for copy id={source.id}: {plaintext_digest.hex()} != "
                f"{asset_hash.hex()}"
            )
        return repair(
            session,
            asset_hash,
            plaintext_path,
            content_type,
            backends=backends,
            sealer=sealer,
            policy=policy,
            key_epoch=key_epoch,
        )


def replication_status(
    session: Session,
    asset_hash: bytes,
    content_type: str,
    backends: BackendMap,
    *,
    policy: RepresentationPolicy = DEFAULT_POLICY,
    key_epoch: str | None = None,
) -> ReplicationStatus:
    """Report whether an asset has healthy copies in all target placements."""
    validate_placement_tags(session, backends)
    targets = target_placements(
        content_type,
        backends,
        policy=policy,
        key_epoch=key_epoch,
    )
    target_placements_by_key = {
        _placement_key(placement): placement for _, placement in targets
    }
    want = set(target_placements_by_key.values())
    have: set[TaggedPlacement] = set()
    media_id_by_placement: dict[TaggedPlacement, str] = {}

    for copy in _healthy_copies(session, asset_hash):
        key = _copy_placement_key(copy)
        if key is None:
            continue
        placement = target_placements_by_key.get(key)
        if placement is None:
            continue
        have.add(placement)
        media_id = _copy_media_id(copy)
        if media_id:
            media_id_by_placement[placement] = media_id

    _assert_distinct_media(have, media_id_by_placement)
    missing = want - have
    return {
        "complete": not missing,
        "have": have,
        "want": want,
        "missing": missing,
    }


def validate_placement_tags(
    session: Session,
    backends: BackendMap,
) -> None:
    """Compare discovered placement tags against any existing pins.

    Missing pins are allowed during discovery. A mismatch is treated as a
    reconciliation halt because silently acting on changed tags can mis-route
    future copies.
    """
    for backend_id, backend in backends.items():
        for placement in backend.list_tagged_placements():
            pin = _get_placement_pin(session, backend_id, placement.placement_id)
            if pin is None:
                continue
            _assert_pin_matches(pin, placement)


def select_restore_source(
    session: Session,
    asset_hash: bytes,
    *,
    chooser: Callable[[Sequence[Copy]], Copy] | None = None,
) -> Copy | None:
    """Select a healthy copy for restore.

    The default is deterministic: lowest copy id among healthy copies. Callers
    can supply a chooser for locality, media preference, or future cost policy.
    """
    candidates = _healthy_copies(session, asset_hash)
    if not candidates:
        return None
    if chooser is None:
        return candidates[0]
    selected = chooser(candidates)
    if selected not in candidates:
        raise ReplicationInvariantError("restore chooser returned a non-candidate copy")
    return selected


def _sorted_targets(
    targets: set[PlacementTarget[TBackend]],
) -> list[PlacementTarget[TBackend]]:
    return sorted(
        targets,
        key=lambda target: (
            target[1].copy_class,
            target[1].backend_name,
            target[1].placement_id,
        ),
    )


def _healthy_copies_by_placement(
    session: Session,
    asset_hash: bytes,
    targets: list[PlacementTarget[WritableStorageBackend]],
) -> dict[TaggedPlacement, Copy]:
    by_key = {_placement_key(placement): placement for _, placement in targets}
    result: dict[TaggedPlacement, Copy] = {}
    for copy in _healthy_copies(session, asset_hash):
        key = _copy_placement_key(copy)
        if key is None:
            continue
        placement = by_key.get(key)
        if placement is not None and placement not in result:
            result[placement] = copy
    return result


def _healthy_copies(session: Session, asset_hash: bytes) -> list[Copy]:
    return list(
        session.scalars(
            select(Copy)
            .where(
                Copy.logical_asset_hash == asset_hash,
                Copy.health == CopyHealth.OK,
            )
            .order_by(Copy.id)
        )
    )


def _placement_key(placement: TaggedPlacement) -> tuple[str, str]:
    return (placement.backend_name, placement.placement_id)


def _placement_for_copy(
    copy: Copy,
    content_type: str,
    backends: BackendMap,
    *,
    policy: RepresentationPolicy,
    key_epoch: str | None,
) -> TaggedPlacement | None:
    key = _copy_placement_key(copy)
    if key is None:
        return None
    targets = target_placements(
        content_type,
        backends,
        policy=policy,
        key_epoch=key_epoch,
    )
    return {_placement_key(placement): placement for _, placement in targets}.get(key)


def _apply_representation_policy(
    placement: TaggedPlacement,
    policy: RepresentationPolicy,
    key_epoch: str | None,
) -> TaggedPlacement:
    representation = policy.get(
        (placement.content_class, placement.copy_class),
        placement.representation,
    )
    Representation(representation)
    placement_key_epoch = (
        key_epoch
        if representation == Representation.RAO_AEAD_V1.value
        else placement.key_epoch
    )
    return replace(
        placement,
        representation=representation,
        key_epoch=placement_key_epoch,
    )


def _get_placement_pin(
    session: Session,
    backend_id: int,
    placement_id: str,
) -> PlacementTagPin | None:
    return session.scalars(
        select(PlacementTagPin).where(
            PlacementTagPin.backend_id == backend_id,
            PlacementTagPin.placement_id == placement_id,
        )
    ).one_or_none()


def _pin_or_validate_placement(
    session: Session,
    backend_id: int,
    placement: TaggedPlacement,
) -> PlacementTagPin:
    pin = _get_placement_pin(session, backend_id, placement.placement_id)
    if pin is not None:
        _assert_pin_matches(pin, placement)
        return pin

    pin = PlacementTagPin(
        backend_id=backend_id,
        placement_id=placement.placement_id,
        content_class=placement.content_class,
        copy_class=placement.copy_class,
    )
    session.add(pin)
    session.flush()
    return pin


def _assert_pin_matches(pin: PlacementTagPin, placement: TaggedPlacement) -> None:
    if pin.content_class == placement.content_class and pin.copy_class == placement.copy_class:
        return
    raise PlacementTagDrift(
        "reconciliation halt: placement tag drift for "
        f"{placement.backend_name}/{placement.placement_id}; pinned "
        f"content_class={pin.content_class!r}, copy_class={pin.copy_class!r}; "
        f"discovered content_class={placement.content_class!r}, "
        f"copy_class={placement.copy_class!r}"
    )


def _copy_placement_key(copy: Copy) -> tuple[str, str] | None:
    placement_id = copy.native_locator.get("pool_id")
    if not isinstance(placement_id, str):
        return None
    return (copy.backend.name, placement_id)


def _copy_media_id(copy: Copy) -> str | None:
    value = copy.native_locator.get("tape_uuid")
    if isinstance(value, str) and value:
        return f"tape:{value}"
    if copy.backend.kind == BackendKind.D2_TAPE:
        value = copy.native_locator.get("volume_uuid")
        if isinstance(value, str) and value:
            return f"d2_tape:{value}"
        value = copy.native_locator.get("barcode")
        if isinstance(value, str) and value:
            return f"d2_tape:{value}"
    return None


@contextlib.contextmanager
def _materialized_copy_path(
    backend: StorageBackend,
    copy: Copy,
) -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="sutradhara-self-heal-") as temp_dir_raw:
        path = Path(temp_dir_raw) / "stored-copy.bin"
        path.write_bytes(backend.read_range(copy.native_locator, ByteRange(0, 0)))
        yield path


def _epoch_for(
    placement: TaggedPlacement,
    representation: Representation,
) -> KeyEpoch | None:
    if representation is not Representation.RAO_AEAD_V1:
        return None
    if placement.key_epoch is None:
        raise ReplicationInvariantError(
            "encrypted placement requires key_epoch for "
            f"{placement.backend_name}/{placement.placement_id}"
        )
    return KeyEpoch(key_id=placement.key_epoch, created_at="", active=True)


def _copy_storage_metadata(representation: Representation) -> dict[str, Any]:
    if representation in {Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1}:
        return {
            "representation": representation.value,
            "chunk_size": RAO_CHUNK_SIZE,
        }
    return {}


def _assert_copy_integrity(
    asset_hash: bytes,
    record: CopyRecord,
    seal_result: SealResult,
    placement: TaggedPlacement,
) -> None:
    if seal_result.representation is Representation.RAW_BYTES:
        if record.logical_id == asset_hash and record.integrity_hash == asset_hash:
            return
        raise ReplicationInvariantError(
            "raw-bytes copy hash differs from asset for "
            f"{placement.backend_name}/{placement.placement_id}"
        )

    if seal_result.plaintext_digest != asset_hash:
        raise ReplicationInvariantError(
            "sealed plaintext_digest differs from requested asset for "
            f"{placement.backend_name}/{placement.placement_id}"
        )
    if record.integrity_hash == seal_result.stored_digest:
        return
    raise ReplicationInvariantError(
        "backend stored bytes differ from sealed representation for "
        f"{placement.backend_name}/{placement.placement_id}"
    )


def _sha256_file(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()


def _assert_distinct_media(
    have: set[TaggedPlacement],
    media_id_by_placement: dict[TaggedPlacement, str],
) -> None:
    missing_media_id = have - set(media_id_by_placement)
    if missing_media_id:
        [placement] = _sorted_placements(missing_media_id)[:1]
        raise ReplicationInvariantError(
            "target placement copies must include tape_uuid or volume_uuid to "
            "assert durability; "
            f"{placement.backend_name}/{placement.placement_id} is missing tape_uuid"
        )

    seen: dict[str, TaggedPlacement] = {}
    for placement, media_id in media_id_by_placement.items():
        other = seen.get(media_id)
        if other is not None:
            raise ReplicationInvariantError(
                "target placements must resolve to distinct tape_uuid/media "
                "identifiers; "
                f"{other.backend_name}/{other.placement_id} and "
                f"{placement.backend_name}/{placement.placement_id} both use "
                f"{media_id}"
            )
        seen[media_id] = placement


def _sorted_placements(placements: set[TaggedPlacement]) -> list[TaggedPlacement]:
    return sorted(
        placements,
        key=lambda placement: (
            placement.copy_class,
            placement.backend_name,
            placement.placement_id,
        ),
    )
