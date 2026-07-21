"""Pool-backed replication orchestration.

Replication policy is catalog data:

* ``pool`` owns the backend-native destination id and byte representation.
* ``artifactclass_pool`` declares the active pools for an artifactclass.
* ``copy.pool_id`` records which pool produced each materialized copy.

Backends still own physical write/read mechanics, but they no longer advertise
scenario-era content/copy tags.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, TypedDict, TypeVar

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from sutradhara.backend.port import BackendError, CopyRecord, StorageBackend
from sutradhara.catalog.copies import add_copy
from sutradhara.catalog.models import ArtifactClassPool, Bundle, Copy, Pool
from sutradhara.catalog.types import CopyHealth, CopySource
from sutradhara.jobs.engine import submit
from sutradhara.keys import KEY_DOMAIN_ARCHIVE, KeyEpoch, KeyRegistry, assert_key_epoch_domain
from sutradhara.restore import RestoreError, RestoreIntegrityError, restore_copy
from sutradhara.sealing.port import Opener, Representation, Sealer, SealResult
from sutradhara.sealing.rao import RAO_CHUNK_SIZE, RaoCliOpener, RaoCliSealer


class ReplicationError(Exception):
    """Base class for replication policy and completeness errors."""


class ReplicationPolicyMissing(ReplicationError):
    """No active pool membership exists for an artifactclass."""


class PoolBackendUnavailable(ReplicationError):
    """A target pool's backend was not supplied to the operation."""


class ReplicationInvariantError(ReplicationError):
    """A durability invariant was violated by existing catalog rows."""


class PoolRepresentationError(ReplicationInvariantError):
    """A copy's stored representation metadata disagrees with its pool."""


class SelfHealUnavailable(ReplicationError):
    """A missing copy cannot be rebuilt from the available healthy copies."""


class WritableStorageBackend(StorageBackend, Protocol):
    """Storage backend surface needed by the fan-out writer."""

    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        """Return only after `source` is durable and copy-accountable.

        Checkpoint-capable implementations must return the copy selected from
        the checkpoint/close committed-copy set, never from a WRITTEN append.
        """
        ...


@dataclass(frozen=True)
class PoolTarget:
    """One active replication destination for an artifactclass."""

    pool_id: str
    artifactclass: str
    backend_id: int
    backend_name: str
    representation: str
    key_epoch: str | None = None
    location: str = ""
    offsite_gate: bool = False
    tier: str = ""
    sort_order: int = 0


class ReplicationStatus(TypedDict):
    complete: bool
    have: set[PoolTarget]
    want: set[PoolTarget]
    missing: set[PoolTarget]


TBackend = TypeVar("TBackend", bound=StorageBackend)
BackendMap = Mapping[int, StorageBackend]
WritableBackendMap = Mapping[int, WritableStorageBackend]
PoolTargetEntry = tuple[TBackend, PoolTarget]


def target_pools(
    session: Session,
    artifactclass: str,
    backends: Mapping[int, TBackend],
    *,
    key_epoch: str | None = None,
    write_eligible_only: bool = True,
) -> list[PoolTargetEntry[TBackend]]:
    """Return pool targets for ``artifactclass`` in catalog order.

    The default is the write path view: active memberships whose pools currently
    accept writes. Reader/status callers pass ``write_eligible_only=False`` to
    keep existing placements visible.
    """
    predicates = [
        ArtifactClassPool.artifactclass == artifactclass,
        ArtifactClassPool.active.is_(True),
    ]
    if write_eligible_only:
        predicates.append(ArtifactClassPool.pool.has(Pool.accepts_writes.is_(True)))
    memberships = list(
        session.scalars(
            select(ArtifactClassPool)
            .options(joinedload(ArtifactClassPool.pool).joinedload(Pool.backend))
            .where(*predicates)
            .order_by(ArtifactClassPool.sort_order, ArtifactClassPool.pool_id)
        )
    )
    if not memberships:
        raise ReplicationPolicyMissing(
            f"artifactclass {artifactclass!r} has no active pool memberships"
        )

    targets: list[PoolTargetEntry[TBackend]] = []
    seen: set[str] = set()
    for membership in memberships:
        pool = membership.pool
        if pool.id in seen:
            raise ReplicationInvariantError(
                f"artifactclass {artifactclass!r} has duplicate pool {pool.id!r}"
            )
        seen.add(pool.id)

        Representation(pool.representation)
        backend = backends.get(pool.backend_id)
        if backend is None:
            raise PoolBackendUnavailable(
                f"pool {pool.id!r} targets backend_id={pool.backend_id}, which was not supplied"
            )
        targets.append(
            (
                backend,
                PoolTarget(
                    pool_id=pool.id,
                    artifactclass=artifactclass,
                    backend_id=pool.backend_id,
                    backend_name=pool.backend.name,
                    representation=pool.representation,
                    key_epoch=(
                        key_epoch
                        if pool.representation == Representation.RAO_AEAD_V1.value
                        else None
                    ),
                    location=pool.location,
                    offsite_gate=pool.offsite_gate,
                    tier=pool.tier,
                    sort_order=membership.sort_order,
                ),
            )
        )
    return targets


def replicate_asset(
    session: Session,
    asset_hash: bytes,
    source_path: Path | str,
    artifactclass: str,
    *,
    backends: WritableBackendMap,
    sealer: Sealer | None = None,
    key_epoch: str | None = None,
) -> list[Copy]:
    """Replicate one asset to every active write-eligible pool for an artifactclass."""
    targets = target_pools(session, artifactclass, backends, key_epoch=key_epoch)
    existing = _healthy_copies_by_pool(session, asset_hash, targets)
    sealer = sealer or RaoCliSealer(KeyRegistry())

    copies: list[Copy] = []
    for backend, target in targets:
        existing_copy = existing.get(target)
        if existing_copy is not None:
            _assert_copy_matches_pool(existing_copy, target)
            copies.append(existing_copy)
            continue

        representation = Representation(target.representation)
        seal_epoch = _epoch_for(target, representation)
        with sealer.seal(
            source_path,
            representation,
            key_epoch=seal_epoch,
        ) as sealed:
            committed_record = backend.write_object_to_pool(
                sealed.sealed_path,
                target.pool_id,
            )
            _assert_copy_integrity(asset_hash, committed_record, sealed, target)
        copy, created = add_copy(
            session,
            logical_asset_hash=asset_hash,
            backend_id=target.backend_id,
            pool_id=target.pool_id,
            native_locator=committed_record.native_locator,
            integrity_hash=sealed.stored_digest,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
            storage_metadata=_copy_storage_metadata(
                representation,
                recipient_epochs=sealed.recipient_epochs,
            ),
        )
        if created:
            _enqueue_copy_verify(session, copy)
        _assert_copy_matches_pool(copy, target)
        copies.append(copy)
    return copies


def repair(
    session: Session,
    asset_hash: bytes,
    source_path: Path | str,
    artifactclass: str,
    *,
    backends: WritableBackendMap,
    sealer: Sealer | None = None,
    key_epoch: str | None = None,
) -> list[Copy]:
    """Write copies for write-eligible pools currently missing from replication status."""
    status = replication_status(
        session,
        asset_hash,
        artifactclass,
        backends,
        key_epoch=key_epoch,
    )
    if not status["missing"]:
        return []

    targets = target_pools(session, artifactclass, backends, key_epoch=key_epoch)
    missing = status["missing"]
    sealer = sealer or RaoCliSealer(KeyRegistry())

    repaired: list[Copy] = []
    for backend, target in targets:
        if target not in missing:
            continue
        representation = Representation(target.representation)
        seal_epoch = _epoch_for(target, representation)
        with sealer.seal(
            source_path,
            representation,
            key_epoch=seal_epoch,
        ) as sealed:
            committed_record = backend.write_object_to_pool(
                sealed.sealed_path,
                target.pool_id,
            )
            _assert_copy_integrity(asset_hash, committed_record, sealed, target)
        copy, created = add_copy(
            session,
            logical_asset_hash=asset_hash,
            backend_id=target.backend_id,
            pool_id=target.pool_id,
            native_locator=committed_record.native_locator,
            integrity_hash=sealed.stored_digest,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
            storage_metadata=_copy_storage_metadata(
                representation,
                recipient_epochs=sealed.recipient_epochs,
            ),
        )
        if created:
            _enqueue_copy_verify(session, copy)
        _assert_copy_matches_pool(copy, target)
        repaired.append(copy)
    return repaired


def self_heal(
    session: Session,
    asset_hash: bytes,
    artifactclass: str,
    *,
    backends: WritableBackendMap,
    opener: Opener | None = None,
    sealer: Sealer | None = None,
    key_epoch: str | None = None,
    chooser: Callable[[Sequence[Copy]], Copy] | None = None,
    execution_id: str,
) -> list[Copy]:
    """Rebuild missing pool copies from a surviving healthy copy."""
    if not execution_id:
        raise ValueError("execution_id must be non-empty")
    from sutradhara.durability import AssetTarget

    status = replication_status(
        session,
        asset_hash,
        artifactclass,
        backends,
        key_epoch=key_epoch,
    )
    if not status["missing"]:
        return []

    candidates = select_source_candidates(
        session,
        AssetTarget(asset_hash, artifactclass),
        purpose="self_heal",
    )
    if chooser is not None:
        healthy = [copy for copy in candidates if copy.health == CopyHealth.OK]
        selected = chooser(healthy)
        if selected not in healthy:
            raise ReplicationInvariantError("restore chooser returned a non-candidate copy")
        candidates = [selected, *[copy for copy in candidates if copy.id != selected.id]]
    if not candidates:
        raise SelfHealUnavailable(f"cannot self-heal {asset_hash.hex()}: no healthy source copy")

    opener = opener or RaoCliOpener(KeyRegistry())
    errors: list[str] = []
    for source in candidates:
        if source.health != CopyHealth.OK:
            errors.append(f"copy id={source.id}: health={source.health.value}")
            continue
        source_backend = backends.get(source.backend_id)
        if source_backend is None:
            errors.append(f"copy id={source.id}: backend_id={source.backend_id} is not available")
            continue

        source_target = _pool_for_copy(
            session,
            source,
            artifactclass,
            backends,
            key_epoch=key_epoch,
        )
        if source_target is None:
            errors.append(f"copy id={source.id}: not in an active target pool")
            continue

        _assert_copy_matches_pool(source, source_target)
        try:
            restored = restore_copy(
                session,
                source,
                backend=source_backend,
                opener=opener,
                execution_id=execution_id,
            )
            with restored as result:
                if result.sha256 != asset_hash:
                    _mark_copy_suspect(source)
                    errors.append(
                        "copy id="
                        f"{source.id}: source plaintext hash differs from requested asset "
                        f"{result.sha256.hex()} != {asset_hash.hex()}"
                    )
                    continue
                return repair(
                    session,
                    asset_hash,
                    result.path,
                    artifactclass,
                    backends=backends,
                    sealer=sealer,
                    key_epoch=key_epoch,
                )
        except RestoreIntegrityError as exc:
            if _is_content_digest_mismatch(exc):
                _mark_copy_suspect(source)
            errors.append(f"copy id={source.id}: {exc}")
            continue
        except (RestoreError, BackendError, OSError) as exc:
            errors.append(f"copy id={source.id}: {exc}")
            continue

    detail = "; ".join(errors) if errors else "no healthy source copy"
    raise SelfHealUnavailable(f"cannot self-heal {asset_hash.hex()}: {detail}")


def replication_status(
    session: Session,
    asset_hash: bytes,
    artifactclass: str,
    backends: BackendMap,
    *,
    key_epoch: str | None = None,
) -> ReplicationStatus:
    """Report whether an asset has healthy copies in all active pools."""
    from sutradhara.durability import direct_copies, pending_verification_copy_ids

    targets = target_pools(
        session,
        artifactclass,
        backends,
        key_epoch=key_epoch,
        write_eligible_only=False,
    )
    targets_by_key = {_pool_key(target): target for _, target in targets}
    want = set(targets_by_key.values())
    have: set[PoolTarget] = set()
    media_id_by_target: dict[PoolTarget, str] = {}

    measured = direct_copies(session, asset_hash, require_verified=True)
    pending_ids = pending_verification_copy_ids(session)
    pending = [
        copy
        for copy in direct_copies(session, asset_hash)
        if copy.id in pending_ids and copy not in measured
    ]
    for copy in [*measured, *pending]:
        key = _copy_pool_key(copy)
        if key is None:
            continue
        target = targets_by_key.get(key)
        if target is None:
            continue
        _assert_copy_matches_pool(copy, target)
        have.add(target)
        media_id = _copy_media_id(copy)
        if media_id:
            media_id_by_target[target] = media_id

    _assert_distinct_media(have, media_id_by_target)
    missing = want - have
    return {
        "complete": not missing,
        "have": have,
        "want": want,
        "missing": missing,
    }


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


SourcePurpose = Literal["user_restore", "self_heal"]


def select_source(
    session: Session,
    target: Any,
    *,
    purpose: SourcePurpose,
) -> Copy | None:
    """Select a source copy for a target and purpose.

    ``user_restore`` follows artifactclass restore preference. ``self_heal``
    uses trust-first ordering and is deterministic across equal-cost copies.
    """

    candidates = select_source_candidates(session, target, purpose=purpose)
    if purpose == "self_heal":
        for candidate in candidates:
            if candidate.health == CopyHealth.OK:
                return candidate
        return None
    return candidates[0] if candidates else None


def select_source_candidates(
    session: Session,
    target: Any,
    *,
    purpose: SourcePurpose,
) -> list[Copy]:
    """Return ordered source candidates for restore or healing fallback."""

    if purpose == "user_restore":
        return _user_restore_candidates(session, target)
    if purpose == "self_heal":
        return _self_heal_candidates(session, target)
    raise ValueError(f"unsupported source-selection purpose {purpose!r}")


def _healthy_copies_by_pool(
    session: Session,
    asset_hash: bytes,
    targets: list[PoolTargetEntry[WritableStorageBackend]],
) -> dict[PoolTarget, Copy]:
    by_key = {_pool_key(target): target for _, target in targets}
    result: dict[PoolTarget, Copy] = {}
    for copy in _healthy_copies(session, asset_hash):
        key = _copy_pool_key(copy)
        if key is None:
            continue
        target = by_key.get(key)
        if target is not None and target not in result:
            _assert_copy_matches_pool(copy, target)
            result[target] = copy
    return result


def _healthy_copies(session: Session, asset_hash: bytes) -> list[Copy]:
    from sutradhara.durability import direct_copies

    return direct_copies(session, asset_hash)


def _user_restore_candidates(session: Session, target: Any) -> list[Copy]:
    from sutradhara.archive_restore import _restore_pool_order
    from sutradhara.artifactclass_policy import get_artifactclass_policy
    from sutradhara.durability import AssetTarget, BundleTarget, durable_placements

    if isinstance(target, AssetTarget):
        artifactclass = target.artifactclass
        copies = durable_placements(
            session,
            target,
            require_verified=False,
            artifactclass=artifactclass,
        )
    elif isinstance(target, BundleTarget):
        bundle = session.get(Bundle, target.bundle_id)
        if bundle is None:
            return []
        artifactclass = bundle.artifactclass
        copies = durable_placements(
            session,
            target,
            require_verified=False,
            artifactclass=artifactclass,
        )
    else:
        raise TypeError(f"unsupported source target {target!r}")

    policy = get_artifactclass_policy(session, artifactclass)
    pool_order = _restore_pool_order(session, artifactclass, policy.restore_preference)
    order_by_pool = {pool_id: index for index, pool_id in enumerate(pool_order)}
    return sorted(
        copies,
        key=lambda copy: (
            order_by_pool.get(copy.pool_id or "", len(order_by_pool)),
            copy.id,
        ),
    )


def _self_heal_candidates(session: Session, target: Any) -> list[Copy]:
    from sutradhara.durability import AssetTarget, BundleTarget

    if isinstance(target, AssetTarget):
        query = select(Copy).where(Copy.logical_asset_hash == target.asset_hash)
    elif isinstance(target, BundleTarget):
        query = select(Copy).where(Copy.bundle_id == target.bundle_id)
    else:
        raise TypeError(f"unsupported source target {target!r}")

    copies = list(
        session.scalars(
            query.options(joinedload(Copy.backend), joinedload(Copy.pool))
            .where(
                Copy.deleted_at.is_(None),
                Copy.health.in_((CopyHealth.OK, CopyHealth.SUSPECT)),
            )
            .order_by(Copy.id)
        )
    )
    return sorted(copies, key=_self_heal_sort_key)


def _self_heal_sort_key(copy: Copy) -> tuple[int, float, int, int, int]:
    verified = _timestamp_sort_value(copy.last_measured_at)
    return (
        0 if copy.health == CopyHealth.OK else 1,
        -verified,
        _representation_cost(copy),
        _location_cost(copy),
        copy.id,
    )


def _timestamp_sort_value(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value.timestamp())
    except (AttributeError, OSError, ValueError):
        return 0.0


def _representation_cost(copy: Copy) -> int:
    representation = copy.storage_metadata.get("representation")
    if representation == Representation.RAO_PLAIN_V1.value:
        return 0
    if representation == Representation.RAO_AEAD_V1.value:
        return 1
    return 2


def _location_cost(copy: Copy) -> int:
    pool = copy.pool
    if pool is not None and pool.offsite_gate:
        return 1
    return 0


def _mark_copy_suspect(copy: Copy) -> None:
    copy.health = CopyHealth.SUSPECT


def _enqueue_copy_verify(session: Session, copy: Copy) -> None:
    submit(
        session,
        "verify",
        {"copy_id": copy.id},
        dedupe_key=f"verify:copy:{copy.id}",
    )


def _is_content_digest_mismatch(exc: RestoreIntegrityError) -> bool:
    """Return whether opening, rather than stored-byte measurement, failed."""

    return str(exc).startswith("content-corrupt:")


def _pool_key(target: PoolTarget) -> tuple[int, str]:
    return (target.backend_id, target.pool_id)


def _copy_pool_key(copy: Copy) -> tuple[int, str] | None:
    if copy.pool_id is None:
        return None
    return (copy.backend_id, copy.pool_id)


def _pool_for_copy(
    session: Session,
    copy: Copy,
    artifactclass: str,
    backends: BackendMap,
    *,
    key_epoch: str | None,
) -> PoolTarget | None:
    key = _copy_pool_key(copy)
    if key is None:
        return None
    targets = target_pools(
        session,
        artifactclass,
        backends,
        key_epoch=key_epoch,
        write_eligible_only=False,
    )
    return {_pool_key(target): target for _, target in targets}.get(key)


def _copy_media_id(copy: Copy) -> str | None:
    from sutradhara.durability import DurabilityMediaIdentityError, copy_media_id

    try:
        return copy_media_id(copy)
    except DurabilityMediaIdentityError:
        return None


def _epoch_for(
    target: PoolTarget,
    representation: Representation,
) -> KeyEpoch | None:
    if representation is not Representation.RAO_AEAD_V1:
        return None
    if target.key_epoch is None:
        raise ReplicationInvariantError(
            f"encrypted pool requires key_epoch for {target.backend_name}/{target.pool_id}"
        )
    try:
        assert_key_epoch_domain(
            target.key_epoch,
            KEY_DOMAIN_ARCHIVE,
            context=f"pool sealing for {target.backend_name}/{target.pool_id}",
        )
    except ValueError as exc:
        raise ReplicationInvariantError(str(exc)) from exc
    return KeyEpoch(key_id=target.key_epoch, created_at="", active=True)


def _copy_storage_metadata(
    representation: Representation,
    *,
    recipient_epochs: Sequence[str] = (),
) -> dict[str, Any]:
    metadata: dict[str, Any] = {"representation": representation.value}
    if representation in {Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1}:
        metadata["chunk_size"] = RAO_CHUNK_SIZE
    if representation is Representation.RAO_AEAD_V1:
        if not recipient_epochs:
            raise ReplicationInvariantError("encrypted copy is missing recipient epochs")
        metadata["recipient_epochs"] = list(recipient_epochs)
    return metadata


def _assert_copy_matches_pool(copy: Copy, target: PoolTarget) -> None:
    copy_representation = copy.storage_metadata.get("representation")
    if copy_representation == target.representation:
        return
    raise PoolRepresentationError(
        f"copy id={copy.id} has representation {copy_representation!r}; "
        f"pool {target.pool_id!r} requires {target.representation!r}"
    )


def _assert_copy_integrity(
    asset_hash: bytes,
    record: CopyRecord,
    seal_result: SealResult,
    target: PoolTarget,
) -> None:
    if seal_result.representation is Representation.RAW_BYTES:
        if record.logical_id == asset_hash and record.integrity_hash == asset_hash:
            return
        raise ReplicationInvariantError(
            f"raw-bytes copy hash differs from asset for {target.backend_name}/{target.pool_id}"
        )

    if seal_result.plaintext_digest != asset_hash:
        raise ReplicationInvariantError(
            "sealed plaintext_digest differs from requested asset for "
            f"{target.backend_name}/{target.pool_id}"
        )
    if record.integrity_hash == seal_result.stored_digest:
        return
    raise ReplicationInvariantError(
        "backend stored bytes differ from sealed representation for "
        f"{target.backend_name}/{target.pool_id}"
    )


def _assert_distinct_media(
    have: set[PoolTarget],
    media_id_by_target: dict[PoolTarget, str],
) -> None:
    missing_media_id = have - set(media_id_by_target)
    if missing_media_id:
        [target] = _sorted_targets(missing_media_id)[:1]
        raise ReplicationInvariantError(
            "target pool copies must include tape_uuid or volume_uuid to "
            "assert durability; "
            f"{target.backend_name}/{target.pool_id} is missing tape_uuid"
        )

    seen: dict[str, PoolTarget] = {}
    for target, media_id in media_id_by_target.items():
        other = seen.get(media_id)
        if other is not None:
            raise ReplicationInvariantError(
                "target pools must resolve to distinct tape_uuid/media "
                "identifiers; "
                f"{other.backend_name}/{other.pool_id} and "
                f"{target.backend_name}/{target.pool_id} both use {media_id}"
            )
        seen[media_id] = target


def _sorted_targets(targets: set[PoolTarget]) -> list[PoolTarget]:
    return sorted(
        targets,
        key=lambda target: (
            target.sort_order,
            target.backend_name,
            target.pool_id,
        ),
    )
