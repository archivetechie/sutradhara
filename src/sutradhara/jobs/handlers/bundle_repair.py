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
from sqlalchemy.orm import joinedload

from sutradhara.archive_fanout import (
    ArchiveBuilder,
    ArchiveFanoutError,
    MemberInput,
    RemArchiveBuilder,
    build_bundle_copy_for_pool,
)
from sutradhara.archive_restore import ArchiveRestoreError, read_member_to_path
from sutradhara.backend import factory
from sutradhara.backend.port import BackendError
from sutradhara.catalog.models import ArtifactClassPool, AssetLocator, Backend, Bundle, Pool
from sutradhara.catalog.types import CopyHealth
from sutradhara.durability import BundleTarget, bundle_replication_status
from sutradhara.jobs.reconcilers import bundle_copy
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    CONDITION_BLOCKED,
    OBSERVED_MISSING,
    record_observation,
)
from sutradhara.jobs.registry import ConditionProjection, JobContext, JobResult, register_handler
from sutradhara.replication import (
    PoolTargetEntry,
    SelfHealUnavailable,
    WritableStorageBackend,
    select_source_candidates,
    target_pools,
)

DOMAIN = "bundle_copy"


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
    backends = _target_backends(ctx, bundle.artifactclass)
    targets = target_pools(ctx.session, bundle.artifactclass, backends, key_epoch=key_epoch)
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
    errors: list[str] = []
    for source in select_source_candidates(ctx.session, BundleTarget(bundle.id), purpose="self_heal"):
        if source.health != CopyHealth.OK:
            errors.append(f"copy id={source.id}: health={source.health.value}")
            continue
        source_backend = backends.get(source.backend_id)
        if source_backend is None:
            errors.append(f"copy id={source.id}: backend_id={source.backend_id} unavailable")
            continue
        try:
            with tempfile.TemporaryDirectory(prefix=f"sutradhara-bundle-repair-{bundle.id}-") as raw:
                temp_root = Path(raw)
                member_sources = _extract_member_sources(
                    ctx=ctx,
                    bundle=bundle,
                    source_copy_id=source.id,
                    source_backend=source_backend,
                    scratch=temp_root / "scratch",
                    rem_bin=rem_bin or "rem",
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
        except (ArchiveRestoreError, BackendError, OSError) as exc:
            errors.append(f"copy id={source.id}: {exc}")
            continue

        remaining_missing = _missing_pool_ids(ctx, bundle.id)
        fenced_missing = _write_fenced_pool_ids(ctx, remaining_missing)
        if remaining_missing and remaining_missing == fenced_missing:
            message = _write_fenced_missing_message(
                bundle.id,
                repaired=repaired,
                fenced_missing=fenced_missing,
            )
            record_observation(
                ctx.session,
                domain=DOMAIN,
                target_key=bundle.id,
                desired=True,
                observed_state=OBSERVED_MISSING,
                reason="fenced-missing",
                message=message,
            )
            return JobResult(
                ok=True,
                detail=message,
                step_state={
                    "bundle_repair": {
                        "bundle_id": bundle.id,
                        "source_copy_id": source.id,
                        "repaired_pools": sorted(repaired),
                        "remaining_write_fenced_pools": sorted(fenced_missing),
                    }
                },
                condition=ConditionProjection(
                    condition=CONDITION_BACKOFF,
                    reason="fenced-missing",
                    message=message,
                ),
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
    raise SelfHealUnavailable(f"cannot repair bundle {bundle.id}: {detail}")


def make_archive_builder(*, rem_bin: str | None = None) -> ArchiveBuilder:
    """Return the production archive builder; tests may monkeypatch this seam."""

    return RemArchiveBuilder(rem_bin or "rem")


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


def _target_backends(ctx: JobContext, artifactclass: str) -> dict[int, WritableStorageBackend]:
    rows = list(
        ctx.session.scalars(
            select(Backend)
            .join(Pool, Pool.backend_id == Backend.id)
            .join(ArtifactClassPool, ArtifactClassPool.pool_id == Pool.id)
            .where(
                ArtifactClassPool.artifactclass == artifactclass,
                ArtifactClassPool.active.is_(True),
                Pool.accepts_writes.is_(True),
            )
            .order_by(Backend.id)
        ).unique()
    )
    result: dict[int, WritableStorageBackend] = {}
    for row in rows:
        backend = factory.backend_from_row(row)
        if not hasattr(backend, "write_object_to_pool"):
            raise ArchiveFanoutError(
                f"backend {row.name!r} does not implement write_object_to_pool"
            )
        result[row.id] = cast(WritableStorageBackend, backend)
    return result


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


def _extract_member_sources(
    *,
    ctx: JobContext,
    bundle: Bundle,
    source_copy_id: int,
    source_backend: WritableStorageBackend,
    scratch: Path,
    rem_bin: str,
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
        )
        if dest.stat().st_size != member.size_bytes:
            raise ArchiveRestoreError(
                f"member {member.member_path!r} short read {dest.stat().st_size} != "
                f"{member.size_bytes}"
            )
        actual = hashlib.sha256(dest.read_bytes()).digest()
        if actual != member.file_sha256:
            raise _SourceDigestMismatch(
                f"member {member.member_path!r} digest {actual.hex()} != "
                f"{member.file_sha256.hex()}"
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
        build_bundle_copy_for_pool(
            ctx.session,
            bundle=bundle,
            target=target,
            member_sources=member_sources,
            builder=builder,
            backend=backend,
            key_epoch=key_epoch,
            work_dir=work_dir,
        )
        repaired.add(target.pool_id)
    return repaired


def _scratch_member_path(root: Path, member_path: str) -> Path:
    rel = PurePosixPath(member_path)
    if rel.is_absolute() or ".." in rel.parts:
        raise ArchiveRestoreError(f"unsafe bundle member path {member_path!r}")
    return root.joinpath(*rel.parts)


class _SourceDigestMismatch(ArchiveRestoreError):
    """A source copy produced stored member bytes that disagree with catalog digests."""
