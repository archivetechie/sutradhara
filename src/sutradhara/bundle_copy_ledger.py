"""Append write attempts and resolutions without taking transaction ownership.

The caller commits the attempt before storage I/O. Resolutions are immutable
facts; readers derive the outcome from the latest resolution, including a
lookup that overturns an earlier not-landed response.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import BundleCopyWriteAttempt, BundleCopyWriteResolution
from sutradhara.jobs.attempts import default_worker_id

if TYPE_CHECKING:
    from sutradhara.archive_fanout import ArchiveBuilder, BuildArtifact
    from sutradhara.catalog.models import Copy
    from sutradhara.jobs.registry import ConditionProjection
    from sutradhara.replication import PoolTarget, WritableStorageBackend

BUNDLE_COPY_FAST_PATH_KEY = "sutradhara_bundle_copy_fast_path"


def record_attempt(
    session: Session,
    *,
    bundle_id: str,
    pool_id: str,
    backend_id: int,
    generation: int,
    object_name: str,
    digest: bytes,
    size_bytes: int,
    member_layout: dict[str, Any],
    recipient_epochs: list[str] | None = None,
    job_id: int | None = None,
    job_attempt: int | None = None,
    issued_by: str | None = None,
) -> BundleCopyWriteAttempt:
    """Add and flush one issued write; never commit the caller's transaction."""
    row = BundleCopyWriteAttempt(
        bundle_id=bundle_id,
        pool_id=pool_id,
        backend_id=backend_id,
        generation=generation,
        object_name=object_name,
        digest_algorithm="sha256",
        digest=digest,
        size_bytes=size_bytes,
        recipient_epochs=recipient_epochs,
        member_layout=member_layout,
        job_id=job_id,
        job_attempt=job_attempt,
        issued_by=default_worker_id() if issued_by is None else issued_by,
        issued_at=dt.datetime.now(dt.UTC),
    )
    session.add(row)
    session.flush([row])
    return row


def record_resolution(
    session: Session,
    *,
    attempt_id: int,
    outcome: str,
    learned_by: str,
    copy_id: int | None = None,
    observed_hash: bytes | None = None,
    job_id: int | None = None,
    job_attempt: int | None = None,
    resolved_by: str | None = None,
) -> BundleCopyWriteResolution:
    """Add and flush a conclusion, constrained by the ledger's transition trigger."""
    row = BundleCopyWriteResolution(
        attempt_id=attempt_id,
        outcome=outcome,
        learned_by=learned_by,
        copy_id=copy_id,
        observed_hash=observed_hash,
        job_id=job_id,
        job_attempt=job_attempt,
        resolved_by=default_worker_id() if resolved_by is None else resolved_by,
        resolved_at=dt.datetime.now(dt.UTC),
    )
    session.add(row)
    session.flush([row])
    return row


def attempt_outcome(session: Session, attempt_id: int) -> str | None:
    """Return the latest resolution's outcome, or None for an unresolved attempt."""
    return session.scalar(
        select(BundleCopyWriteResolution.outcome)
        .where(BundleCopyWriteResolution.attempt_id == attempt_id)
        .order_by(BundleCopyWriteResolution.id.desc())
        .limit(1)
    )


def identity_attempts(
    session: Session, *, bundle_id: str, pool_id: str, generation: int
) -> list[tuple[BundleCopyWriteAttempt, str | None]]:
    """Return the identity's attempts in id order, paired with their current outcome."""
    latest = (
        select(BundleCopyWriteResolution.outcome)
        .where(BundleCopyWriteResolution.attempt_id == BundleCopyWriteAttempt.id)
        .order_by(BundleCopyWriteResolution.id.desc())
        .limit(1)
        .scalar_subquery()
    )
    rows = session.execute(
        select(BundleCopyWriteAttempt, latest)
        .where(
            BundleCopyWriteAttempt.bundle_id == bundle_id,
            BundleCopyWriteAttempt.pool_id == pool_id,
            BundleCopyWriteAttempt.generation == generation,
        )
        .order_by(BundleCopyWriteAttempt.id)
    )
    return [(attempt, outcome) for attempt, outcome in rows]


def resolve_identity(
    session: Session,
    *,
    bundle_id: str,
    target: PoolTarget,
    backend: WritableStorageBackend,
    builder: ArchiveBuilder,
    work_dir: Path,
    write_artifact: BuildArtifact | None = None,
    write_attempt_id: int | None = None,
    copy_id: int | None = None,
) -> tuple[Copy | None, ConditionProjection | None, bool]:
    """Resolve a pool identity with one window and guarded ledger transitions.

    A writer may supply its just-recorded attempt and artifact; a refused write
    then falls through to lookup in that same window. Repair may supply an old
    copy for the pre-rebuild lookup. The third result permits that rebuild.
    Every ORM value needed by media work is snapshotted before committing.
    """
    from sutradhara.archive_fanout import (
        _artifact_from_attempt,
        _artifact_from_copy,
        _copy_problem,
        _copy_run,
        _copy_storage_metadata,
        _read_copy_result,
        _record_copy_result,
        _record_existing_copy_result,
    )
    from sutradhara.backend.port import BackendWriteInDoubtError
    from sutradhara.catalog.models import Copy
    from sutradhara.jobs.commit_point import commit_point_for
    from sutradhara.jobs.engine import AttemptBoundaryError, AttemptSuperseded, is_sqlite_lock_error
    from sutradhara.replication import PoolObject

    point = commit_point_for(session)
    run, job_id, job_attempt, issued_by = _copy_run(session)
    before = identity_attempts(session, bundle_id=bundle_id, pool_id=target.pool_id, generation=0)
    snapshots = {
        row.id: (row.digest, outcome, _artifact_from_attempt(row, work_dir), row.object_name)
        for row, outcome in before
    }
    landed_copies = {}
    for row, outcome in before:
        if outcome != "landed":
            continue
        landed_copy_id = session.scalars(
            select(BundleCopyWriteResolution.copy_id).where(
                BundleCopyWriteResolution.attempt_id == row.id,
                BundleCopyWriteResolution.outcome == "landed",
            )
        ).one()
        landed_copy = session.get(Copy, landed_copy_id)
        if landed_copy is None:
            raise RuntimeError(f"landed attempt {row.id} has no copy")
        landed_copies[row.id] = (
            _artifact_from_copy(session, landed_copy, work_dir),
            dict(landed_copy.native_locator),
            dict(landed_copy.storage_metadata),
        )
    copy = None if copy_id is None else session.get(Copy, copy_id)
    copy_digest = None if copy is None else copy.integrity_hash
    copy_artifact = None if copy is None else _artifact_from_copy(session, copy, work_dir)
    copy_locator = {} if copy is None else dict(copy.native_locator)
    copy_metadata = {} if copy is None else dict(copy.storage_metadata)
    name = next(reversed(snapshots.values()))[3] if snapshots else ""
    if copy is not None:
        name = str(
            copy_locator.get("caller_object_id")
            or str(copy_locator.get("key") or "").rsplit("/", 1)[-1]
            or name
        )
    candidates = tuple(
        dict.fromkeys(
            [value[0] for value in snapshots.values()]
            + ([] if copy_digest is None else [copy_digest])
        )
    )
    pool_object = None
    problem = None
    read_result = None
    read_error = None
    matched_id = None
    matched_copy = False
    response = False
    failure_detail = ""
    with point.window():
        lookup = True
        if write_artifact is not None:
            try:
                record = backend.write_object_to_pool(
                    write_artifact.artifact_path,
                    target.pool_id,
                    content_sha256=write_artifact.stored_digest,
                )
            except (AttemptSuperseded, AttemptBoundaryError):
                raise
            except BackendWriteInDoubtError as exc:
                problem = _copy_problem(target.pool_id, "write-outcome-unknown", str(exc))
                lookup = False
            except Exception as exc:
                if is_sqlite_lock_error(exc):
                    raise
                failure_detail = str(exc)
            else:
                pool_object = PoolObject(record, write_artifact.stored_digest)
                matched_id = write_attempt_id
                response = True
                lookup = False
        if lookup:
            try:
                pool_object = backend.lookup_pool_object(
                    target.pool_id, name, candidate_digests=candidates
                )
            except (AttemptSuperseded, AttemptBoundaryError):
                raise
            except Exception as exc:
                problem = _copy_problem(target.pool_id, "write-outcome-unknown", str(exc))
            else:
                if pool_object is not None and pool_object.content_hash is None:
                    problem = _copy_problem(
                        target.pool_id, "write-outcome-unknown", "pool returned no content hash"
                    )
                elif pool_object is not None:
                    matches = [
                        identifier
                        for identifier, value in snapshots.items()
                        if value[0] == pool_object.content_hash
                    ]
                    matches.sort(key=lambda identifier: snapshots[identifier][1] != "landed")
                    if copy_digest == pool_object.content_hash:
                        matched_copy = True
                    elif matches:
                        matched_id = matches[0]
        if pool_object is not None and problem is None and (matched_id is not None or matched_copy):
            if matched_copy:
                artifact = copy_artifact
            else:
                if matched_id is None:
                    raise RuntimeError("read-back has no matched attempt")
                artifact = snapshots[matched_id][2]
            if artifact is None:
                raise RuntimeError("matched copy has no read-back layout")
            metadata = (
                copy_metadata
                if matched_copy
                else _copy_storage_metadata(
                    target.representation,
                    recipient_epochs=artifact.recipient_epochs,
                    stored_size_bytes=pool_object.copy_record.size_bytes,
                )
            )
            locator = pool_object.copy_record.native_locator
            if not response and matched_id in landed_copies:
                artifact, locator, metadata = landed_copies[matched_id]
            read_result, read_error = _read_copy_result(
                backend=backend,
                locator=locator,
                artifact=artifact,
                representation=target.representation,
                metadata=metadata,
                builder=builder,
                work_dir=work_dir,
            )

    # BEGIN IMMEDIATE now serializes this re-read with other resolvers. Never
    # append transitions using the pre-window outcomes or affect new attempts.
    current = identity_attempts(session, bundle_id=bundle_id, pool_id=target.pool_id, generation=0)
    unresolved = [row for row, outcome in current if outcome is None and row.id in snapshots]
    if any(outcome == "conflict" for _, outcome in current):
        return (
            None,
            _copy_problem(
                target.pool_id, "copy-identity-conflict", "identity already conflicts", blocked=True
            ),
            False,
        )
    if (
        not response
        and any(value[1] is None for value in snapshots.values())
        and not any(outcome is None for _, outcome in current)
    ):
        landed_ids = [row.id for row, outcome in current if outcome == "landed"]
        if landed_ids and (
            problem is not None or pool_object is None or (matched_id is None and not matched_copy)
        ):
            from sutradhara.catalog.types import CopyHealth

            landed_copy_id = session.scalar(
                select(BundleCopyWriteResolution.copy_id)
                .where(
                    BundleCopyWriteResolution.attempt_id.in_(landed_ids),
                    BundleCopyWriteResolution.outcome == "landed",
                )
                .order_by(BundleCopyWriteResolution.id.desc())
                .limit(1)
            )
            landed_copy = session.get(Copy, landed_copy_id)
            if landed_copy is None:
                raise RuntimeError("concurrently landed attempt has no copy")
            problem = (
                None
                if landed_copy.health == CopyHealth.OK
                else _copy_problem(
                    target.pool_id,
                    "post-write-pool-failure",
                    "resolved copy requires verification",
                    blocked=True,
                )
            )
            return landed_copy, problem, False
    if problem is not None:
        return None, problem, False
    if pool_object is None:
        if copy is not None:
            return None, None, True
        if not backend.reissue_after_absent:
            return (
                None,
                _copy_problem(
                    target.pool_id,
                    "write-outcome-unknown",
                    "pool cannot certify absence",
                    blocked=True,
                ),
                False,
            )
        with session.begin_nested():
            for row in unresolved:
                record_resolution(
                    session,
                    attempt_id=row.id,
                    outcome="not-landed",
                    learned_by="lookup",
                    job_id=job_id,
                    job_attempt=job_attempt,
                    resolved_by=issued_by,
                )
        return (
            None,
            _copy_problem(
                target.pool_id, "transient-backend-failure", failure_detail or "object absent"
            ),
            False,
        )
    observed_hash = pool_object.content_hash
    if observed_hash is None:
        raise RuntimeError("resolved pool object has no observed hash")
    matches_now = [(row, outcome) for row, outcome in current if row.digest == observed_hash]
    matches_now.sort(key=lambda value: value[1] != "landed")
    if matches_now and not response and not matched_copy:
        row, _ = matches_now[0]
        if row.id not in snapshots:
            return (
                None,
                _copy_problem(
                    target.pool_id,
                    "write-unresolved",
                    "object matches an attempt recorded during lookup",
                ),
                False,
            )
        matched_id = row.id
    if matched_id is None and not matched_copy:
        with session.begin_nested():
            for row in unresolved:
                record_resolution(
                    session,
                    attempt_id=row.id,
                    outcome="conflict",
                    learned_by="lookup",
                    observed_hash=observed_hash,
                    job_id=job_id,
                    job_attempt=job_attempt,
                    resolved_by=issued_by,
                )
        return (
            None,
            _copy_problem(
                target.pool_id,
                "copy-identity-conflict",
                f"pool holds {observed_hash.hex()}",
                blocked=True,
            ),
            False,
        )
    from sutradhara.archive_fanout import ArchiveFanoutError
    from sutradhara.backend.port import BackendError, BackendStoredCorruptError

    verification_failed = read_error is not None or read_result is None or not read_result.ok
    verification_detail = str(read_error or (read_result.detail if read_result else "no read-back"))
    read_unavailable = read_error is not None and not isinstance(
        read_error, (ArchiveFanoutError, BackendStoredCorruptError)
    )
    landed_match = matched_id is not None and any(
        row.id == matched_id and outcome == "landed" for row, outcome in current
    )
    if landed_match:
        landed_copy_id = session.scalars(
            select(BundleCopyWriteResolution.copy_id).where(
                BundleCopyWriteResolution.attempt_id == matched_id,
                BundleCopyWriteResolution.outcome == "landed",
            )
        ).one()
        copy = session.get(Copy, landed_copy_id)
        if matched_id not in landed_copies:
            # This window read the pool record before learning that another
            # resolver had landed the attempt. It did not read this copy's
            # own locator, so its result must not become evidence on the copy.
            from sutradhara.catalog.types import CopyHealth

            if copy is None:
                raise RuntimeError("concurrently landed attempt has no copy")
            with session.begin_nested():
                for row in unresolved:
                    record_resolution(
                        session,
                        attempt_id=row.id,
                        outcome="not-landed",
                        learned_by="lookup",
                        job_id=job_id,
                        job_attempt=job_attempt,
                        resolved_by=issued_by,
                    )
            problem = (
                None
                if copy.health == CopyHealth.OK
                else _copy_problem(
                    target.pool_id,
                    "post-write-pool-failure",
                    "resolved copy requires verification",
                    blocked=True,
                )
            )
            return copy, problem, False
    if matched_copy or landed_match:
        if copy is None:
            raise RuntimeError("matched existing copy is missing from the catalogue")
        if (
            read_error is not None
            and read_unavailable
            and not isinstance(read_error, (BackendError, OSError))
        ):
            if landed_match:
                return (
                    copy,
                    _copy_problem(target.pool_id, "write-unresolved", verification_detail),
                    False,
                )
            raise read_error
        # Replays of this phase share a receipt; another phase or a later
        # attempt keeps its evidence separate from this read and the write.
        phase = "copy" if matched_copy else "landed"
        reread_run = f"{run}:reread:{phase}:{current[-1][0].id if current else 0}"
        with session.begin_nested():
            if landed_match:
                for row in unresolved:
                    record_resolution(
                        session,
                        attempt_id=row.id,
                        outcome="not-landed",
                        learned_by="lookup",
                        job_id=job_id,
                        job_attempt=job_attempt,
                        resolved_by=issued_by,
                    )
            if not read_unavailable:
                _record_existing_copy_result(session, copy, read_result, read_error, run=reread_run)
        if read_unavailable:
            return (
                copy,
                _copy_problem(target.pool_id, "transient-backend-failure", verification_detail),
                False,
            )
        if verification_failed and not backend.holds_names:
            if write_artifact is not None:
                return (
                    copy,
                    _copy_problem(target.pool_id, "transient-backend-failure", verification_detail),
                    False,
                )
            return copy, None, True
        if verification_failed:
            return (
                copy,
                _copy_problem(
                    target.pool_id,
                    "copy-rewrite-needs-generation",
                    verification_detail,
                    blocked=True,
                ),
                False,
            )
        return copy, None, False
    if read_unavailable:
        return None, _copy_problem(target.pool_id, "write-unresolved", verification_detail), False
    if matched_id is None:
        raise RuntimeError("adoption has no matched attempt")
    matched = next(row for row, _ in current if row.id == matched_id)
    outcome = next(outcome for row, outcome in current if row.id == matched_id)
    try:
        with session.begin_nested():
            copy = _record_copy_result(
                session,
                bundle_id=bundle_id,
                target=target,
                record=pool_object.copy_record,
                artifact=_artifact_from_attempt(matched, work_dir),
                result=read_result,
                error=read_error,
                run=run,
            )
            if outcome is None or (outcome == "not-landed" and not response):
                record_resolution(
                    session,
                    attempt_id=matched_id,
                    outcome="landed",
                    learned_by="write-response" if response else "lookup",
                    copy_id=copy.id,
                    observed_hash=None if response else observed_hash,
                    job_id=job_id,
                    job_attempt=job_attempt,
                    resolved_by=issued_by,
                )
            if not response:
                for row in unresolved:
                    if row.id != matched_id:
                        record_resolution(
                            session,
                            attempt_id=row.id,
                            outcome="not-landed",
                            learned_by="lookup",
                            job_id=job_id,
                            job_attempt=job_attempt,
                            resolved_by=issued_by,
                        )
    except Exception as exc:
        if (
            not response
            or isinstance(exc, (AttemptSuperseded, AttemptBoundaryError))
            or is_sqlite_lock_error(exc)
        ):
            raise
        return None, _copy_problem(target.pool_id, "write-unresolved", str(exc)), False
    if verification_failed:
        return (
            copy,
            _copy_problem(
                target.pool_id,
                "post-write-pool-failure",
                verification_detail,
                blocked=True,
            ),
            False,
        )
    return copy, None, False
