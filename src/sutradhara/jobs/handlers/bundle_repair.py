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
    PoolWriteFailure,
    RemArchiveBuilder,
    SealKeyUnavailable,
    TransientPoolFanoutError,
    _artifact_from_copy,
    _copy_problem,
    _copy_run,
    _current_bundle_members,
    _pool_slug,
    _project_copy_problems,
    _read_copy_result,
    _ReadAssetLocatorView,
    _ReadCopyView,
    _record_existing_copy_result,
    _remember_copy_problem,
    _reset_copy_problems,
    build_bundle_copy_for_pool,
    preflight_seal_keys,
)
from sutradhara.archive_restore import (
    ArchiveRestoreError,
    read_member_to_path,
    select_private_epoch_for_copy,
)
from sutradhara.backend import factory
from sutradhara.backend.port import BackendError, BackendStoredCorruptError
from sutradhara.bundle_copy_ledger import identity_attempts, resolve_identity
from sutradhara.catalog.models import (
    ArtifactClassPool,
    AssetLocator,
    Backend,
    Bundle,
    BundleCopyWriteAttempt,
    Copy,
    Pool,
)
from sutradhara.catalog.types import CopyHealth
from sutradhara.durability import BundleTarget, bundle_replication_status
from sutradhara.jobs.commit_point import commit_point_for
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
    PoolTarget,
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
    commit_point_for(ctx.session)
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

    return _repair_after_preflight(
        ctx,
        bundle=bundle,
        targets=targets,
        backends=backends,
        sources=sources,
        unreadable_sources=unreadable_sources,
        builder=builder,
        keys=keys,
        key_epoch=key_epoch,
        rem_bin=rem_bin or "rem",
    )


def _repair_after_preflight(
    ctx: JobContext,
    *,
    bundle: Bundle,
    targets: list[PoolTargetEntry[WritableStorageBackend]],
    backends: dict[int, WritableStorageBackend],
    sources: list[Copy],
    unreadable_sources: dict[int, str],
    builder: ArchiveBuilder,
    keys: KeyRegistry,
    key_epoch: str | None,
    rem_bin: str,
) -> JobResult:
    """Resolve first, re-read doubtful/possibly missing copies, then build once."""
    bundle_id = bundle.id
    point = commit_point_for(ctx.session)
    run, _, _, _ = _copy_run(ctx.session)
    _reset_copy_problems(ctx.session, bundle_id)
    writable = {target.pool_id: (backend, target) for backend, target in targets}
    resolved = dict(writable)

    def pool_entry(pool_id: str) -> PoolTargetEntry[WritableStorageBackend]:
        if pool_id not in resolved:
            pool = ctx.session.get(Pool, pool_id)
            if pool is None:
                raise RuntimeError(f"repair ledger names missing pool {pool_id!r}")
            row = ctx.session.get(Backend, pool.backend_id)
            if row is None:
                raise RuntimeError(f"repair pool {pool_id!r} has no backend")
            backend = backends.get(row.id)
            if backend is None:
                backend = cast(WritableStorageBackend, factory.backend_from_row(row))
            resolved[pool_id] = (
                backend,
                PoolTarget(
                    pool_id=pool_id,
                    artifactclass="",
                    backend_id=row.id,
                    backend_name=row.name,
                    representation=pool.representation,
                    key_epoch=key_epoch,
                ),
            )
        return resolved[pool_id]

    repaired = set()
    rebuild = set()
    reread_copy_ids: set[int] = set()
    resolved_pool_ids: set[str] = set()
    with tempfile.TemporaryDirectory(prefix=f"sutradhara-bundle-repair-{bundle_id}-") as raw:
        temp_root = Path(raw)
        pool_ids = list(
            ctx.session.scalars(
                select(BundleCopyWriteAttempt.pool_id)
                .where(
                    BundleCopyWriteAttempt.bundle_id == bundle_id,
                    BundleCopyWriteAttempt.generation == 0,
                )
                .distinct()
            )
        )
        for pool_id in pool_ids:
            attempts = identity_attempts(
                ctx.session, bundle_id=bundle_id, pool_id=pool_id, generation=0
            )
            if not any(outcome is None for _, outcome in attempts):
                continue
            backend, target = pool_entry(pool_id)
            read_dir = temp_root / f"resolve-{_pool_slug(pool_id)}"
            read_dir.mkdir(exist_ok=True)
            copy, problem, may_rebuild = resolve_identity(
                ctx.session,
                bundle_id=bundle_id,
                target=target,
                backend=backend,
                builder=builder,
                work_dir=read_dir,
            )
            _remember_copy_problem(ctx.session, bundle_id, pool_id, problem)
            if copy is not None:
                reread_copy_ids.add(copy.id)
            if copy is not None or problem is not None:
                resolved_pool_ids.add(pool_id)
            if may_rebuild:
                rebuild.add(pool_id)
                if copy is None:
                    raise RuntimeError(f"resolution permits rebuilding {pool_id!r} without a copy")
            elif copy is not None and problem is None:
                repaired.add(pool_id)

        copies = list(
            ctx.session.scalars(select(Copy).where(Copy.bundle_id == bundle_id).order_by(Copy.id))
        )
        for copy in copies:
            if copy.id in reread_copy_ids:
                # Step 1 already checked this copy, including any read error.
                continue
            if copy.health != CopyHealth.SUSPECT and not (
                copy.health == CopyHealth.OK
                and (copy.last_measured_at is None or copy.last_measured_digest is None)
            ):
                continue
            if copy.pool_id is None:
                raise RuntimeError(f"bundle copy {copy.id} has no pool")
            pool_id = copy.pool_id
            backend, target = pool_entry(pool_id)
            read_dir = temp_root / f"reread-{copy.id}"
            read_dir.mkdir()
            artifact = _artifact_from_copy(ctx.session, copy, read_dir)
            locator, metadata = dict(copy.native_locator), dict(copy.storage_metadata)
            with point.window():
                result, error = _read_copy_result(
                    backend=backend,
                    locator=locator,
                    artifact=artifact,
                    representation=target.representation,
                    metadata=metadata,
                    builder=builder,
                    work_dir=read_dir,
                )
            if error is not None and not isinstance(
                error, (ArchiveFanoutError, BackendStoredCorruptError)
            ):
                if not isinstance(error, (BackendError, OSError)):
                    raise error
                problem = _copy_problem(pool_id, "transient-backend-failure", str(error))
            else:
                attempts = identity_attempts(
                    ctx.session, bundle_id=bundle_id, pool_id=pool_id, generation=0
                )
                reread_run = f"{run}:reread:step2:{attempts[-1][0].id if attempts else 0}"
                with ctx.session.begin_nested():
                    _record_existing_copy_result(ctx.session, copy, result, error, run=reread_run)
                problem = (
                    None
                    if error is None and result is not None and result.ok
                    else _copy_problem(
                        pool_id,
                        "post-write-pool-failure",
                        str(error or (result.detail if result is not None else "no read-back")),
                        blocked=True,
                    )
                )
            _remember_copy_problem(ctx.session, bundle_id, pool_id, problem)

        missing = _missing_pool_ids(ctx, bundle_id)
        for pool_id in sorted(missing):
            if pool_id in rebuild or pool_id in resolved_pool_ids:
                continue
            pool_copies = [copy for copy in copies if copy.pool_id == pool_id]
            if not pool_copies or any(copy.health == CopyHealth.SUSPECT for copy in pool_copies):
                continue
            newest = pool_copies[-1]
            if newest.health not in {CopyHealth.MISSING, CopyHealth.CORRUPT}:
                continue
            backend, target = pool_entry(pool_id)
            read_dir = temp_root / f"lookup-{newest.id}"
            read_dir.mkdir()
            copy, problem, may_rebuild = resolve_identity(
                ctx.session,
                bundle_id=bundle_id,
                target=target,
                backend=backend,
                builder=builder,
                work_dir=read_dir,
                copy_id=newest.id,
            )
            _remember_copy_problem(ctx.session, bundle_id, pool_id, problem)
            if may_rebuild:
                rebuild.add(pool_id)
            elif copy is not None and problem is None:
                repaired.add(pool_id)

        write_targets = []
        for backend, target in targets:
            outcomes = [
                outcome
                for _, outcome in identity_attempts(
                    ctx.session, bundle_id=bundle_id, pool_id=target.pool_id, generation=0
                )
            ]
            if "conflict" in outcomes or None in outcomes:
                continue
            pool_copies = [copy for copy in copies if copy.pool_id == target.pool_id]
            if target.pool_id in rebuild or (not pool_copies and "landed" not in outcomes):
                write_targets.append((backend, target))

        source_id = None
        if write_targets:
            errors = []
            member_sources = None
            for source in sources:
                if source.health != CopyHealth.OK:
                    errors.append(f"copy id={source.id}: health={source.health}")
                    continue
                source_backend = backends.get(source.backend_id)
                if source_backend is None:
                    errors.append(
                        f"copy id={source.id}: backend_id={source.backend_id} unavailable"
                    )
                    continue
                if source.id in unreadable_sources:
                    errors.append(unreadable_sources[source.id])
                    continue
                touch_copy_tape(ctx, source)
                try:
                    member_sources = _extract_member_sources(
                        ctx=ctx,
                        bundle=bundle,
                        source_copy_id=source.id,
                        source_backend=source_backend,
                        scratch=temp_root / f"source-{source.id}",
                        rem_bin=rem_bin,
                        keys=keys,
                    )
                except _SourceDigestMismatch as exc:
                    source.health = CopyHealth.SUSPECT
                    errors.append(f"copy id={source.id}: {exc}")
                    continue
                except (ArchiveFanoutError, ArchiveRestoreError, BackendError, OSError) as exc:
                    errors.append(f"copy id={source.id}: {exc}")
                    continue
                source_id = source.id
                break
            if member_sources is None:
                projection, _ = _project_copy_problems(ctx.session, bundle_id)
                return JobResult(
                    ok=False,
                    detail=f"cannot repair bundle {bundle_id}: "
                    + ("; ".join(errors) if errors else "no healthy source copy"),
                    condition=projection,
                )
            repaired.update(
                _repair_missing_targets(
                    ctx=ctx,
                    bundle=bundle,
                    targets=write_targets,
                    member_sources=member_sources,
                    builder=builder,
                    key_epoch=key_epoch,
                    work_dir=temp_root / "build",
                )
            )

    projection, _ = _project_copy_problems(ctx.session, bundle_id)
    if projection is not None:
        return JobResult(ok=False, detail=projection.message or "", condition=projection)
    remaining = _missing_pool_ids(ctx, bundle_id)
    fenced = _write_fenced_pool_ids(ctx, remaining)
    if remaining and remaining == fenced:
        return _fenced_missing_result(
            ctx, bundle_id, repaired=repaired, fenced_missing=fenced, source_copy_id=source_id
        )
    return JobResult(
        ok=True,
        detail=f"bundle {bundle_id} repaired pools={sorted(repaired)}",
        step_state={
            "bundle_repair": {
                "bundle_id": bundle_id,
                "source_copy_id": source_id,
                "repaired_pools": sorted(repaired),
            }
        },
    )


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
    source_copy = ctx.session.scalars(
        select(Copy)
        .where(
            Copy.id == source_copy_id,
            Copy.bundle_id == bundle.id,
        )
        .execution_options(populate_existing=True)
    ).one()
    copy_view = _ReadCopyView(
        native_locator=dict(source_copy.native_locator),
        storage_metadata=dict(source_copy.storage_metadata),
        id=source_copy.id,
    )
    inputs: list[tuple[MemberInput, _ReadAssetLocatorView]] = []
    for member in _current_bundle_members(ctx.session, bundle.id):
        locator = locators.get((member.logical_asset_hash, member.member_path))
        if locator is None:
            raise ArchiveRestoreError(
                f"source copy id={source_copy_id} has no locator for {member.member_path!r}"
            )
        dest = _scratch_member_path(scratch, member.member_path)
        inputs.append(
            (
                MemberInput(
                    logical_asset_hash=member.logical_asset_hash,
                    member_path=member.member_path,
                    source_path=dest,
                    size_bytes=member.size_bytes,
                    file_sha256=member.file_sha256,
                ),
                _ReadAssetLocatorView(
                    representation=locator.representation,
                    native_locator=dict(locator.native_locator),
                    member_path=locator.member_path,
                ),
            )
        )
    with commit_point_for(ctx.session).window():
        for member_input, locator_view in inputs:
            dest = member_input.source_path
            read_member_to_path(
                backend=source_backend,
                copy=copy_view,
                asset_locator=locator_view,
                dest=dest,
                rem_bin=rem_bin,
                keys=keys,
            )
            if dest.stat().st_size != member_input.size_bytes:
                raise ArchiveRestoreError(
                    f"member {member_input.member_path!r} short read {dest.stat().st_size} != "
                    f"{member_input.size_bytes}"
                )
            actual = hashlib.sha256(dest.read_bytes()).digest()
            if actual != member_input.file_sha256:
                raise _SourceDigestMismatch(
                    f"member {member_input.member_path!r} digest {actual.hex()} != "
                    f"{member_input.file_sha256.hex()}"
                )
    return [member_input for member_input, _ in inputs]


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
    for index, (backend, target) in enumerate(targets):
        if target.pool_id not in _missing_pool_ids(ctx, bundle.id):
            continue
        target_dir = work_dir / f"target-{index:02d}-{_pool_slug(target.pool_id)}"
        target_dir.mkdir(parents=True, exist_ok=True)
        try:
            copy = build_bundle_copy_for_pool(
                ctx.session,
                bundle=bundle,
                target=target,
                member_sources=member_sources,
                builder=builder,
                backend=backend,
                key_epoch=key_epoch,
                work_dir=target_dir,
            )
        except (PoolWriteFailure, TransientPoolFanoutError):
            continue
        except (ArchiveFanoutError, ArchiveRestoreError, BackendError, OSError) as exc:
            _remember_copy_problem(
                ctx.session,
                bundle.id,
                target.pool_id,
                _copy_problem(target.pool_id, "transient-backend-failure", str(exc)),
            )
            continue
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
