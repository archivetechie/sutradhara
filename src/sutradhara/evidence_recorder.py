"""Own the current read-back measurement projection and its atomic receipts.

Only ``record_measured`` and ``record_unmeasured_promotion`` may mutate a
copy's ``last_measured_digest``/``last_measured_at`` pair.  Callers own the
transaction, so the projection, receipt, and any verification enqueue commit
or roll back together.
"""

from __future__ import annotations

import datetime as dt
import os
import socket

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.backend.port import VerifyResult
from sutradhara.catalog.models import Copy, VerifyReceipt
from sutradhara.catalog.types import CopyHealth, IntegrityHashProvenance, is_content_hash
from sutradhara.jobs.engine import submit
from sutradhara.jobs.models import Job

MEASUREMENT_SOURCES = frozenset({"fanout", "verify-job", "restore"})
RECEIPT_SOURCES = MEASUREMENT_SOURCES | {"scrub"}


def record_measured(
    session: Session,
    copy: Copy,
    result: VerifyResult,
    *,
    source: str,
    execution_id: str,
    actor: str | None = None,
    measured_at: dt.datetime | None = None,
) -> VerifyReceipt:
    """Record one actual byte read-back and update the operational projection."""

    if source not in MEASUREMENT_SOURCES:
        raise ValueError(f"measurement source must be one of {sorted(MEASUREMENT_SOURCES)}")
    if not execution_id:
        raise ValueError("execution_id must be non-empty")
    if not result.measured or result.actual_hash is None:
        raise ValueError("record_measured requires measured=True and an actual_hash")
    measured = bytes(result.actual_hash)
    if not is_content_hash(measured):
        raise ValueError("measured digest must be a 32-byte SHA-256 hash")

    receipt = _receipt_for_execution(session, copy, source, execution_id)
    if receipt is not None:
        return receipt

    at = measured_at or _utcnow()
    copy.last_checked_at = at
    copy.last_measured_digest = measured
    copy.last_measured_at = at
    if measured != copy.integrity_hash:
        copy.health = CopyHealth.CORRUPT
        failure_kind = "digest-mismatch"
    elif not result.ok:
        copy.health = CopyHealth.SUSPECT
        failure_kind = "backend-failure"
    elif (
        copy.logical_asset_hash is not None
        and copy.integrity_hash != copy.logical_asset_hash
        and copy.integrity_hash_provenance != IntegrityHashProvenance.LOCALLY_COMPUTED
    ):
        copy.health = CopyHealth.SUSPECT
        failure_kind = "identity-unproven"
    else:
        copy.health = CopyHealth.OK
        failure_kind = None

    receipt = VerifyReceipt(
        copy_id=copy.id,
        backend_id=copy.backend_id,
        expected_digest=copy.integrity_hash,
        measured_digest=measured,
        backend_ok=result.ok,
        failure_kind=failure_kind,
        failure_detail=result.detail or None,
        source=source,
        execution_id=execution_id,
        producer_process=_producer_process(),
        actor=actor,
        recorded_at=at,
    )
    session.add(receipt)
    session.flush()
    return receipt


def record_unmeasured_promotion(
    session: Session,
    copy: Copy,
    result: VerifyResult,
    *,
    source: str,
    execution_id: str,
    actor: str | None = None,
    checked_at: dt.datetime | None = None,
) -> tuple[VerifyReceipt, Job]:
    """Invalidate stale measurement evidence when a trust-only check restores OK."""

    if source not in RECEIPT_SOURCES:
        raise ValueError(f"receipt source must be one of {sorted(RECEIPT_SOURCES)}")
    if not execution_id:
        raise ValueError("execution_id must be non-empty")
    if result.measured:
        raise ValueError("unmeasured promotion requires measured=False")
    if not result.ok:
        raise ValueError("unmeasured promotion requires a successful backend check")

    receipt = _receipt_for_execution(session, copy, source, execution_id)
    if receipt is not None:
        job = _verify_job_for_copy(session, copy)
        if job is None:
            raise RuntimeError(
                "an invalidation receipt exists without its transactionally enqueued verify job"
            )
        return receipt, job

    at = checked_at or _utcnow()
    copy.last_checked_at = at
    copy.last_measured_digest = None
    copy.last_measured_at = None
    copy.health = CopyHealth.OK
    receipt = VerifyReceipt(
        copy_id=copy.id,
        backend_id=copy.backend_id,
        expected_digest=copy.integrity_hash,
        measured_digest=None,
        backend_ok=True,
        failure_kind="measurement-invalidated",
        failure_detail=result.detail or None,
        source=source,
        execution_id=execution_id,
        producer_process=_producer_process(),
        actor=actor,
        recorded_at=at,
    )
    session.add(receipt)
    job = _enqueue_verify(session, copy)
    session.flush()
    return receipt, job


def _enqueue_verify(session: Session, copy: Copy) -> Job:
    if copy.id is None:
        session.flush()
    return submit(
        session,
        "verify",
        {"copy_id": copy.id},
        dedupe_key=f"verify:remeasure:{copy.id}",
    )


def _verify_job_for_copy(session: Session, copy: Copy) -> Job | None:
    return session.scalars(
        select(Job)
        .where(Job.dedupe_key == f"verify:remeasure:{copy.id}")
        .order_by(Job.id.desc())
    ).first()


def _receipt_for_execution(
    session: Session,
    copy: Copy,
    source: str,
    execution_id: str,
) -> VerifyReceipt | None:
    if copy.id is None:
        session.flush()
    return session.scalars(
        select(VerifyReceipt).where(
            VerifyReceipt.copy_id == copy.id,
            VerifyReceipt.source == source,
            VerifyReceipt.execution_id == execution_id,
        )
    ).one_or_none()


def _producer_process() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)
