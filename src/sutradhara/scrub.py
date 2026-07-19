"""Reconciliation scrub — the load-bearing demo of the rebuildable index.

The scrub re-enumerates a backend and reconciles its catalog representation
against what the backend actually holds. See docs/spec-v0.1.md §7
(reconciliation scrub).

Day-1 reconciliation policy (a subset of the full §7 policy):
  - Copy found on backend AND in catalog       → update `last_checked_at`.
  - Copy found on backend BUT NOT in catalog   → insert; insert the logical
    asset row too if it's the first time we've seen this content hash.
  - Copy in catalog BUT NOT on backend         → mark `health = MISSING`.
    (Do not delete the row — that erases history.)

Hash-conflict detection (a backend yields an integrity_hash that differs
from the recorded logical_id) is reported but does not auto-resolve;
that's a `health = SUSPECT` event for human attention.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import PurePosixPath

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.backend.port import CopyRecord, StorageBackend
from sutradhara.catalog.copies import add_copy
from sutradhara.catalog.models import Backend, Copy, LogicalAsset, Pool
from sutradhara.catalog.session import locator_key
from sutradhara.catalog.types import (
    BackendKind,
    CopyHealth,
    CopySource,
    IntegrityHashProvenance,
)
from sutradhara.backend.port import VerifyResult
from sutradhara.evidence_recorder import record_unmeasured_promotion
from sutradhara.jobs.engine import submit
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RAO_CHUNK_SIZE


class ScrubInvariantError(Exception):
    """A backend enumeration row violates catalog storage-policy invariants."""


@dataclass
class ScrubReport:
    """What the scrub did. Returned by `scrub_backend` for CLI display."""

    backend_name: str
    assets_added: int = 0
    copies_added: int = 0
    copies_updated: int = 0
    copies_marked_missing: int = 0
    unknown_objects: int = 0
    unknown_object_locators: list[str] = field(default_factory=list)
    integrity_warnings: list[str] = field(default_factory=list)

    @property
    def total_observed(self) -> int:
        return self.copies_added + self.copies_updated


def scrub_backend(
    session: Session,
    backend_row: Backend,
    backend: StorageBackend,
    *,
    now: dt.datetime | None = None,
    run_id: str | None = None,
) -> ScrubReport:
    """Reconcile `backend.enumerate()` against the catalog under `session`.

    Caller is responsible for `session.commit()` on success; this function
    only flushes the work into the session.
    """
    now = now or dt.datetime.now(dt.UTC)
    execution_id = run_id or f"scrub-{uuid.uuid4()}"
    report = ScrubReport(backend_name=backend_row.name)

    # Snapshot every catalog copy on this backend up front, so we can find
    # the missing-on-backend set by removing the seen ones.
    catalog_copies: dict[str, Copy] = {
        c.native_locator_key: c
        for c in session.scalars(select(Copy).where(Copy.backend_id == backend_row.id))
    }

    for record in backend.enumerate():
        _ingest_record(
            session=session,
            backend_row=backend_row,
            record=record,
            catalog_copies=catalog_copies,
            now=now,
            report=report,
            run_id=execution_id,
        )

    # Anything still in catalog_copies wasn't returned by enumerate(): mark
    # missing. (We do NOT delete — losing the row would erase history.)
    for stranded in catalog_copies.values():
        if stranded.health != CopyHealth.MISSING:
            stranded.health = CopyHealth.MISSING
            report.copies_marked_missing += 1

    return report


def _ingest_record(
    *,
    session: Session,
    backend_row: Backend,
    record: CopyRecord,
    catalog_copies: dict[str, Copy],
    now: dt.datetime,
    report: ScrubReport,
    run_id: str,
) -> None:
    key = locator_key(record.native_locator)
    existing = catalog_copies.pop(key, None)
    record_health = _health_for_record(record, backend_row, report)
    pool = _pool_for_record(session, backend_row, record)
    storage_metadata = _storage_metadata_for_record(record, pool)

    if existing is not None:
        _update_existing_copy(
            existing=existing,
            record=record,
            backend_row=backend_row,
            record_health=record_health,
            pool=pool,
            now=now,
            report=report,
            session=session,
            run_id=run_id,
        )
        return

    # Logical asset: insert if first time we've seen this content hash. For
    # existing sealed copies, locator matching above keeps the original
    # plaintext logical asset and avoids inserting a stored-digest pseudo asset.
    asset = session.get(LogicalAsset, record.logical_id)
    if asset is None:
        if _is_recognizable_bundle_container(record):
            report.unknown_objects += 1
            if len(report.unknown_object_locators) < 20:
                report.unknown_object_locators.append(key)
            return
        asset = LogicalAsset(
            content_sha256=record.logical_id,
            size_bytes=record.size_bytes,
            first_seen_at=now,
        )
        session.add(asset)
        report.assets_added += 1
        session.flush()  # so the FK on Copy is satisfiable

    copy, created = add_copy(
        session,
        logical_asset_hash=record.logical_id,
        backend_id=backend_row.id,
        pool_id=pool.id if pool is not None else None,
        native_locator=record.native_locator,
        integrity_hash=record.integrity_hash,
        source=CopySource.SCRUB,
        health=record_health,
        integrity_hash_provenance=IntegrityHashProvenance.BACKEND_DISCOVERED,
        first_observed_at=now,
        storage_metadata=storage_metadata,
    )
    if created:
        copy.last_checked_at = now
        submit(
            session,
            "verify",
            {"copy_id": copy.id},
            dedupe_key=f"verify:copy:{copy.id}",
        )
        report.copies_added += 1


def _update_existing_copy(
    *,
    existing: Copy,
    record: CopyRecord,
    backend_row: Backend,
    record_health: CopyHealth,
    pool: Pool | None,
    now: dt.datetime,
    report: ScrubReport,
    session: Session,
    run_id: str,
) -> None:
    """Refresh a cataloged copy found again by backend-native locator."""
    if pool is not None:
        if existing.pool_id != pool.id:
            raise ScrubInvariantError(
                f"copy id={existing.id} locator belongs to pool {existing.pool_id!r}, "
                f"but backend enumerated pool {pool.id!r}"
            )
        _assert_copy_representation_matches_pool(existing, pool)
    existing.last_checked_at = now
    if existing.integrity_hash != record.integrity_hash:
        existing.health = CopyHealth.SUSPECT
        report.integrity_warnings.append(
            f"copy id={existing.id} on backend {backend_row.name!r}: "
            f"recorded integrity_hash={existing.integrity_hash.hex()[:12]}… "
            f"but enumerate returned {record.integrity_hash.hex()[:12]}…"
        )
    elif existing.health == CopyHealth.MISSING and record_health == CopyHealth.OK:
        record_unmeasured_promotion(
            session,
            existing,
            VerifyResult(ok=True, measured=False, detail="scrub rediscovery"),
            source="scrub",
            execution_id=run_id,
            checked_at=now,
        )
    elif existing.health == CopyHealth.MISSING:
        existing.health = record_health
    elif record_health == CopyHealth.SUSPECT:
        existing.health = CopyHealth.SUSPECT
    elif existing.health == CopyHealth.SUSPECT and record_health == CopyHealth.OK:
        record_unmeasured_promotion(
            session,
            existing,
            VerifyResult(ok=True, measured=False, detail="scrub enumerate refresh"),
            source="scrub",
            execution_id=run_id,
            checked_at=now,
        )
    report.copies_updated += 1


def _health_for_record(record: CopyRecord, backend_row: Backend, report: ScrubReport) -> CopyHealth:
    if record.logical_id == record.integrity_hash:
        return CopyHealth.OK

    report.integrity_warnings.append(
        f"backend {backend_row.name!r} yielded locator "
        f"{locator_key(record.native_locator)} with logical_id="
        f"{record.logical_id.hex()[:12]}… but integrity_hash="
        f"{record.integrity_hash.hex()[:12]}…"
    )
    return CopyHealth.SUSPECT


def _pool_for_record(
    session: Session,
    backend_row: Backend,
    record: CopyRecord,
) -> Pool | None:
    pool_id = record.native_locator.get("pool_id")
    if pool_id in {None, ""}:
        return None
    if not isinstance(pool_id, str) or not pool_id:
        raise ScrubInvariantError(
            f"backend {backend_row.name!r} yielded invalid pool_id {pool_id!r}"
        )
    pool = session.get(Pool, pool_id)
    if pool is None:
        raise ScrubInvariantError(
            f"backend {backend_row.name!r} yielded unknown pool_id {pool_id!r}"
        )
    if pool.backend_id != backend_row.id:
        raise ScrubInvariantError(
            f"pool {pool_id!r} belongs to backend_id={pool.backend_id}, "
            f"not backend_id={backend_row.id}"
        )
    return pool


def _storage_metadata_for_record(
    record: CopyRecord,
    pool: Pool | None,
) -> dict[str, object]:
    metadata: dict[str, object] = dict(record.metadata)
    if pool is None:
        return metadata

    observed = metadata.get("representation")
    if observed is not None and observed != pool.representation:
        raise ScrubInvariantError(
            f"backend record for pool {pool.id!r} declares representation "
            f"{observed!r}, but pool requires {pool.representation!r}"
        )

    representation = Representation(pool.representation)
    metadata["representation"] = representation.value
    if representation in {Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1}:
        metadata.setdefault("chunk_size", RAO_CHUNK_SIZE)
    return metadata


def _is_recognizable_bundle_container(record: CopyRecord) -> bool:
    body_format = _record_string(record, "body_format")
    if body_format is not None and _is_archive_body_format(body_format):
        return True

    for key in ("caller_object_id", "key", "artifact_name", "object_id", "path"):
        value = _record_string(record, key)
        if value is not None and _looks_like_bundle_container_name(value):
            return True
    return False


def _record_string(record: CopyRecord, key: str) -> str | None:
    value = record.metadata.get(key)
    if value is None:
        value = record.native_locator.get(key)
    return value if isinstance(value, str) and value else None


def _is_archive_body_format(value: str) -> bool:
    normalized = value.lower()
    return normalized in {
        "sutradhara-local-archive-v1",
        "sutradhara-archive-bundle-v1",
        "rem-archive-v1",
        "rao-archive-v1",
    } or normalized.startswith(("rem-archive-", "rao-archive-"))


def _looks_like_bundle_container_name(value: str) -> bool:
    name = PurePosixPath(value).name
    return name.endswith(
        (
            "-rao-plain-v1.rao",
            "-rao-aead-v1.rao",
            "-d2tar-raw.tar",
            "-rao-plain-v1.sra",
            "-rao-aead-v1.sra",
            "-d2tar-raw.sra",
        )
    )


def _assert_copy_representation_matches_pool(copy: Copy, pool: Pool) -> None:
    observed = copy.storage_metadata.get("representation")
    if observed == pool.representation:
        return
    raise ScrubInvariantError(
        f"copy id={copy.id} has representation {observed!r}, "
        f"but pool {pool.id!r} requires {pool.representation!r}"
    )


# --- helpers for non-CLI callers / tests ---------------------------------


def known_backend_kinds() -> Iterable[BackendKind]:
    return list(BackendKind)
