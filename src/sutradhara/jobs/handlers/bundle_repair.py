"""`bundle-repair` job: rebuild missing bundle pool copies from a healthy copy.

The handler is the bundle-grain counterpart to asset self-heal. It never uses
staging paths or reconstituted logical bytes. Instead, it extracts each stored
bundle member from a surviving copy into a scratch tree and passes those stored
member bytes to the archive fan-out primitive for each missing pool.
"""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path, PurePosixPath
from typing import cast

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from sutradhara.archive_bundle import bundle_artifactclasses
from sutradhara.archive_fanout import (
    ArchiveBuilder,
    ArchiveFanoutError,
    MemberInput,
    RemArchiveBuilder,
    SealKeyUnavailable,
    build_bundle_copy_for_pool,
    preflight_seal_keys,
)
from sutradhara.archive_restore import (
    ArchiveRestoreError,
    read_member_to_path,
    select_private_epoch_for_copy,
)
from sutradhara.backend import factory
from sutradhara.backend.port import BackendError
from sutradhara.catalog.models import ArtifactClassPool, AssetLocator, Backend, Bundle, Pool
from sutradhara.catalog.types import CopyHealth
from sutradhara.durability import BundleTarget, bundle_replication_status
from sutradhara.jobs.components import touch_asset, touch_copy_tape
from sutradhara.jobs.reconcilers import bundle_copy
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    CONDITION_BLOCKED,
    OBSERVED_MISSING,
    record_observation,
)
from sutradhara.jobs.registry import ConditionProjection, JobContext, JobResult, register_handler
from sutradhara.keys import KeyRegistry
from sutradhara.replication import (
    PoolTargetEntry,
    WritableStorageBackend,
    select_source_candidates,
    target_pools,
)
from sutradhara.resource_control import ResourceControlUnavailable
from sutradhara.sealing.port import Representation

DOMAIN = "bundle_copy"
_HOST_KEY_GUIDANCE = (
    "the key registry or the rem tool on this host could not be used; the repair is retried"
)


@register_handler("bundle-repair")
def handle_bundle_repair(ctx: JobContext) -> JobResult:
    params = ctx.job.params
    bundle_id = params.get("bundle_id")
    if not isinstance(bundle_id, str) or not bundle_id:
        raise ValueError(f"bundle-repair requires params.bundle_id (str); got {bundle_id!r}")
    key_epoch = params.get("key_epoch")
    if key_epoch is not None and not isinstance(key_epoch, str):
        raise ValueError(f"bundle-repair params.key_epoch must be a string; got {key_epoch!r}")
    rem_bin = params.get("rem_bin")
    if rem_bin is not None and not isinstance(rem_bin, str):
        raise ValueError(f"bundle-repair params.rem_bin must be a string; got {rem_bin!r}")

    bundle = _load_sealed_bundle(ctx, bundle_id)
    for member in bundle.members:
        touch_asset(ctx, member.logical_asset_hash)
    # Member grain (§5): repair converges toward the same want-set the
    # bundle_copy reconciler demands — the union over the bundle's member
    # classes' live write-eligible pool sets, so a policy edit propagates to
    # sealed bundles and reconcile/repair can never disagree about "missing".
    classes = bundle_artifactclasses(ctx.session, bundle)
    if not classes:
        raise ValueError(f"bundle {bundle_id!r} has no member artifactclass for repair")
    backends = _classes_target_backends(ctx, classes)
    targets = _classes_target_pools(ctx.session, classes, backends, key_epoch=key_epoch)
    missing = _missing_pool_ids(ctx, bundle.id)
    blocked_projection = bundle_copy.blocked_projection_for_bundle(ctx.session, bundle.id)
    if blocked_projection is not None:
        reason, message = blocked_projection
        return JobResult(
            ok=False,
            detail=message,
            condition=ConditionProjection(
                condition=CONDITION_BLOCKED,
                reason=reason,
                message=message,
            ),
        )
    if not missing:
        return JobResult(ok=True, detail=f"bundle {bundle.id} already has complete placement")

    builder = make_archive_builder(rem_bin=rem_bin)
    keys = make_key_registry()
    missing_targets = [
        (backend, target) for backend, target in targets if target.pool_id in missing
    ]
    try:
        preflight_seal_keys(missing_targets, builder)
    except SealKeyUnavailable as exc:
        message = (
            f"cannot repair bundle {bundle.id}, missing pools={sorted(missing)}: {exc}; "
            f"retry with sutra jobs submit bundle-repair -p bundle_id={bundle.id} "
            "-p key_epoch=<archive epoch>"
        )
        return JobResult(
            ok=False,
            detail=message,
            condition=ConditionProjection(
                condition=CONDITION_BLOCKED, reason=exc.reason, message=message
            ),
        )
    except (OSError, ResourceControlUnavailable) as exc:
        return JobResult(
            ok=False,
            detail=(
                f"cannot repair bundle {bundle.id}, missing pools={sorted(missing)}: {exc}; "
                f"{_HOST_KEY_GUIDANCE}"
            ),
        )
    if not missing_targets and _write_fenced_pool_ids(ctx, missing) == missing:
        return _fenced_missing_result(ctx, bundle.id, repaired=set(), fenced_missing=missing)

    sources = list(
        select_source_candidates(ctx.session, BundleTarget(bundle.id), purpose="self_heal")
    )
    unreadable_sources: dict[int, str] = {}
    if missing_targets:
        candidates = [
            source
            for source in sources
            if source.health == CopyHealth.OK and source.backend_id in backends
        ]
        host_fault = False
        for source in candidates:
            locators = ctx.session.scalars(
                select(AssetLocator).where(
                    AssetLocator.bundle_id == bundle.id,
                    AssetLocator.copy_id == source.id,
                    AssetLocator.representation == Representation.REM_ENCRYPT_V1.value,
                )
            )
            needs_key = any(_sealed_locator_needs_private_key(locator) for locator in locators)
            if not needs_key:
                continue
            try:
                select_private_epoch_for_copy(keys, source.storage_metadata)
            except (ArchiveRestoreError, OSError, ResourceControlUnavailable) as exc:
                unreadable_sources[source.id] = (
                    f"copy id={source.id} recipient_epochs={source.storage_metadata.get('recipient_epochs')!r}: {exc}"
                )
                host_fault = (
                    host_fault
                    or isinstance(exc, (OSError, ResourceControlUnavailable))
                    or isinstance(exc.__cause__, (OSError, ResourceControlUnavailable))
                )
        if candidates and len(unreadable_sources) == len(candidates):
            guidance = (
                _HOST_KEY_GUIDANCE
                if host_fault
                else "a private key for one of the named recipient epochs must be in this host's key registry"
            )
            message = (
                f"cannot repair bundle {bundle.id}: {'; '.join(unreadable_sources.values())}; "
                f"{guidance}"
            )
            return JobResult(
                ok=False,
                detail=message,
                condition=None
                if host_fault
                else ConditionProjection(
                    condition=CONDITION_BLOCKED, reason="source-key-unavailable", message=message
                ),
            )

    errors: list[str] = []
    for source in sources:
        if source.health != CopyHealth.OK:
            errors.append(f"copy id={source.id}: health={source.health.value}")
            continue
        source_backend = backends.get(source.backend_id)
        if source_backend is None:
            errors.append(f"copy id={source.id}: backend_id={source.backend_id} unavailable")
            continue
        if source.id in unreadable_sources:
            errors.append(unreadable_sources[source.id])
            continue
        touch_copy_tape(ctx, source)
        try:
            with tempfile.TemporaryDirectory(
                prefix=f"sutradhara-bundle-repair-{bundle.id}-"
            ) as raw:
                temp_root = Path(raw)
                member_sources = _extract_member_sources(
                    ctx=ctx,
                    bundle=bundle,
                    source_copy_id=source.id,
                    source_backend=source_backend,
                    scratch=temp_root / "scratch",
                    rem_bin=rem_bin or "rem",
                    keys=keys,
                )
                repaired = _repair_missing_targets(
                    ctx=ctx,
                    bundle=bundle,
                    targets=targets,
                    member_sources=member_sources,
                    builder=builder,
                    key_epoch=key_epoch,
                    work_dir=temp_root / "build",
                )
        except _SourceDigestMismatch as exc:
            source.health = CopyHealth.SUSPECT
            errors.append(f"copy id={source.id}: {exc}")
            continue
        except (ArchiveFanoutError, ArchiveRestoreError, BackendError, OSError) as exc:
            errors.append(f"copy id={source.id}: {exc}")
            continue

        remaining_missing = _missing_pool_ids(ctx, bundle.id)
        fenced_missing = _write_fenced_pool_ids(ctx, remaining_missing)
        if remaining_missing and remaining_missing == fenced_missing:
            return _fenced_missing_result(
                ctx,
                bundle.id,
                repaired=repaired,
                fenced_missing=fenced_missing,
                source_copy_id=source.id,
            )

        return JobResult(
            ok=True,
            detail=f"bundle {bundle.id} repaired pools={sorted(repaired)}",
            step_state={
                "bundle_repair": {
                    "bundle_id": bundle.id,
                    "source_copy_id": source.id,
                    "repaired_pools": sorted(repaired),
                }
            },
        )

    detail = "; ".join(errors) if errors else "no healthy source copy"
    return JobResult(ok=False, detail=f"cannot repair bundle {bundle.id}: {detail}")


def make_archive_builder(*, rem_bin: str | None = None) -> ArchiveBuilder:
    """Return the production archive builder; tests may monkeypatch this seam."""

    return RemArchiveBuilder(rem_bin or "rem", keys=make_key_registry())


def make_key_registry() -> KeyRegistry:
    """Use the host registry for repair builds, readability checks and reads."""

    return KeyRegistry()


def _sealed_locator_needs_private_key(locator: AssetLocator) -> bool:
    """Defer malformed sizes to the read primitive and its existing fallback lane."""

    if "offset" in locator.native_locator:
        return False
    try:
        return int(locator.native_locator.get("size_bytes", 0)) != 0
    except (TypeError, ValueError):
        return False


def _load_sealed_bundle(ctx: JobContext, bundle_id: str) -> Bundle:
    bundle = (
        ctx.session.scalars(
            select(Bundle).options(joinedload(Bundle.members)).where(Bundle.id == bundle_id)
        )
        .unique()
        .one_or_none()
    )
    if bundle is None:
        raise ValueError(f"no Bundle with id={bundle_id!r}")
    if bundle.status != "sealed":
        raise ArchiveFanoutError(f"bundle {bundle.id!r} is not sealed")
    if not bundle.members:
        raise ArchiveFanoutError(f"bundle {bundle.id!r} has no members")
    return bundle


def _classes_target_backends(
    ctx: JobContext, classes: list[str]
) -> dict[int, WritableStorageBackend]:
    rows = list(
        ctx.session.scalars(
            select(Backend)
            .join(Pool, Pool.backend_id == Backend.id)
            .join(ArtifactClassPool, ArtifactClassPool.pool_id == Pool.id)
            .where(
                ArtifactClassPool.artifactclass.in_(classes),
                ArtifactClassPool.active.is_(True),
                Pool.accepts_writes.is_(True),
            )
            .order_by(Backend.id)
        ).unique()
    )
    result: dict[int, WritableStorageBackend] = {}
    for row in rows:
        ctx.touch(f"backend:{row.name}")
        backend = factory.backend_from_row(row)
        if not hasattr(backend, "write_object_to_pool"):
            raise ArchiveFanoutError(
                f"backend {row.name!r} does not implement write_object_to_pool"
            )
        result[row.id] = cast(WritableStorageBackend, backend)
    return result


def _classes_target_pools(
    session: Session,
    classes: list[str],
    backends: dict[int, WritableStorageBackend],
    *,
    key_epoch: str | None,
) -> list[PoolTargetEntry[WritableStorageBackend]]:
    """Union of per-class target pools, deduped by (backend, pool) — the same
    want-set the reconciler's member-grain observation demands."""
    targets: list[PoolTargetEntry[WritableStorageBackend]] = []
    seen: set[tuple[int, str]] = set()
    for artifactclass in classes:
        for backend, target in target_pools(session, artifactclass, backends, key_epoch=key_epoch):
            key = (target.backend_id, target.pool_id)
            if key in seen:
                continue
            seen.add(key)
            targets.append((backend, target))
    return targets


def _missing_pool_ids(ctx: JobContext, bundle_id: str) -> set[str]:
    return {
        target.pool_id for target in bundle_replication_status(ctx.session, bundle_id)["missing"]
    }


def _write_fenced_pool_ids(ctx: JobContext, pool_ids: set[str]) -> set[str]:
    if not pool_ids:
        return set()
    return set(
        ctx.session.scalars(
            select(Pool.id).where(
                Pool.id.in_(pool_ids),
                Pool.accepts_writes.is_(False),
            )
        )
    )


def _write_fenced_missing_message(
    bundle_id: str,
    *,
    repaired: set[str],
    fenced_missing: set[str],
) -> str:
    return (
        f"bundle {bundle_id} repaired pools={sorted(repaired)}; "
        f"remaining missing pools are write-fenced: {sorted(fenced_missing)}"
    )


def _fenced_missing_result(
    ctx: JobContext,
    bundle_id: str,
    *,
    repaired: set[str],
    fenced_missing: set[str],
    source_copy_id: int | None = None,
) -> JobResult:
    """Record the existing fenced outcome, including a source only if one was read."""

    message = _write_fenced_missing_message(
        bundle_id, repaired=repaired, fenced_missing=fenced_missing
    )
    record_observation(
        ctx.session,
        domain=DOMAIN,
        target_key=bundle_id,
        desired=True,
        observed_state=OBSERVED_MISSING,
        reason="fenced-missing",
        message=message,
    )
    state: dict[str, object] = {
        "bundle_id": bundle_id,
        "repaired_pools": sorted(repaired),
        "remaining_write_fenced_pools": sorted(fenced_missing),
    }
    if source_copy_id is not None:
        state["source_copy_id"] = source_copy_id
    return JobResult(
        ok=True,
        detail=message,
        step_state={"bundle_repair": state},
        condition=ConditionProjection(
            condition=CONDITION_BACKOFF, reason="fenced-missing", message=message
        ),
    )


def _extract_member_sources(
    *,
    ctx: JobContext,
    bundle: Bundle,
    source_copy_id: int,
    source_backend: WritableStorageBackend,
    scratch: Path,
    rem_bin: str,
    keys: KeyRegistry,
) -> list[MemberInput]:
    locators = {
        (locator.logical_asset_hash, locator.member_path): locator
        for locator in ctx.session.scalars(
            select(AssetLocator).where(
                AssetLocator.bundle_id == bundle.id,
                AssetLocator.copy_id == source_copy_id,
            )
        )
    }
    source_copy = next(copy for copy in bundle.copies if copy.id == source_copy_id)
    member_sources: list[MemberInput] = []
    for member in bundle.members:
        locator = locators.get((member.logical_asset_hash, member.member_path))
        if locator is None:
            raise ArchiveRestoreError(
                f"source copy id={source_copy_id} has no locator for {member.member_path!r}"
            )
        dest = _scratch_member_path(scratch, member.member_path)
        read_member_to_path(
            backend=source_backend,
            copy=source_copy,
            asset_locator=locator,
            dest=dest,
            rem_bin=rem_bin,
            keys=keys,
        )
        if dest.stat().st_size != member.size_bytes:
            raise ArchiveRestoreError(
                f"member {member.member_path!r} short read {dest.stat().st_size} != "
                f"{member.size_bytes}"
            )
        actual = hashlib.sha256(dest.read_bytes()).digest()
        if actual != member.file_sha256:
            raise _SourceDigestMismatch(
                f"member {member.member_path!r} digest {actual.hex()} != {member.file_sha256.hex()}"
            )
        member_sources.append(
            MemberInput(
                logical_asset_hash=member.logical_asset_hash,
                member_path=member.member_path,
                source_path=dest,
                size_bytes=member.size_bytes,
                file_sha256=member.file_sha256,
            )
        )
    return member_sources


def _repair_missing_targets(
    *,
    ctx: JobContext,
    bundle: Bundle,
    targets: list[PoolTargetEntry[WritableStorageBackend]],
    member_sources: list[MemberInput],
    builder: ArchiveBuilder,
    key_epoch: str | None,
    work_dir: Path,
) -> set[str]:
    repaired: set[str] = set()
    work_dir.mkdir(parents=True, exist_ok=True)
    for backend, target in targets:
        if target.pool_id not in _missing_pool_ids(ctx, bundle.id):
            continue
        copy = build_bundle_copy_for_pool(
            ctx.session,
            bundle=bundle,
            target=target,
            member_sources=member_sources,
            builder=builder,
            backend=backend,
            key_epoch=key_epoch,
            work_dir=work_dir,
        )
        touch_copy_tape(ctx, copy)
        repaired.add(target.pool_id)
    return repaired


def _scratch_member_path(root: Path, member_path: str) -> Path:
    rel = PurePosixPath(member_path)
    if rel.is_absolute() or ".." in rel.parts:
        raise ArchiveRestoreError(f"unsafe bundle member path {member_path!r}")
    return root.joinpath(*rel.parts)


class _SourceDigestMismatch(ArchiveRestoreError):
    """A source copy produced stored member bytes that disagree with catalog digests."""
