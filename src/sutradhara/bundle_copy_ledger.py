"""Append write attempts and resolutions without taking transaction ownership.

The caller commits the attempt before storage I/O. Resolutions are immutable
facts; readers derive the outcome from the latest resolution, including a
lookup that overturns an earlier not-landed response.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import BundleCopyWriteAttempt, BundleCopyWriteResolution
from sutradhara.jobs.attempts import default_worker_id


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
