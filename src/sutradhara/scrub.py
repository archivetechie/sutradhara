"""Reconciliation scrub — the load-bearing demo of the rebuildable index.

The scrub re-enumerates a backend and reconciles its catalog representation
against what the backend actually holds. See docs/spec-v0.1.md §7
(reconciliation scrub).

Day-1 reconciliation policy (a subset of the full §7 policy):
  - Copy found on backend AND in catalog       → update `last_verified_at`.
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
from collections.abc import Iterable
from dataclasses import dataclass, field

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.backend.port import CopyRecord, StorageBackend
from sutradhara.catalog.copies import add_copy
from sutradhara.catalog.models import Backend, Copy, LogicalAsset
from sutradhara.catalog.session import locator_key
from sutradhara.catalog.types import (
    BackendKind,
    CopyHealth,
    CopySource,
)


@dataclass
class ScrubReport:
    """What the scrub did. Returned by `scrub_backend` for CLI display."""

    backend_name: str
    assets_added: int = 0
    copies_added: int = 0
    copies_updated: int = 0
    copies_marked_missing: int = 0
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
) -> ScrubReport:
    """Reconcile `backend.enumerate()` against the catalog under `session`.

    Caller is responsible for `session.commit()` on success; this function
    only flushes the work into the session.
    """
    now = now or dt.datetime.now(dt.UTC)
    report = ScrubReport(backend_name=backend_row.name)

    # Snapshot every catalog copy on this backend up front, so we can find
    # the missing-on-backend set by removing the seen ones.
    catalog_copies: dict[str, Copy] = {
        c.native_locator_key: c
        for c in session.scalars(
            select(Copy).where(Copy.backend_id == backend_row.id)
        )
    }

    for record in backend.enumerate():
        _ingest_record(
            session=session,
            backend_row=backend_row,
            record=record,
            catalog_copies=catalog_copies,
            now=now,
            report=report,
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
) -> None:
    key = locator_key(record.native_locator)
    existing = catalog_copies.pop(key, None)
    record_health = _health_for_record(record, backend_row, report)

    if existing is not None:
        _update_existing_copy(
            existing=existing,
            record=record,
            backend_row=backend_row,
            record_health=record_health,
            now=now,
            report=report,
        )
        return

    # Logical asset: insert if first time we've seen this content hash. For
    # existing sealed copies, locator matching above keeps the original
    # plaintext logical asset and avoids inserting a stored-digest pseudo asset.
    asset = session.get(LogicalAsset, record.logical_id)
    if asset is None:
        asset = LogicalAsset(
            content_sha256=record.logical_id,
            size_bytes=record.size_bytes,
            first_seen_at=now,
        )
        session.add(asset)
        report.assets_added += 1
        session.flush()  # so the FK on Copy is satisfiable

    _, created = add_copy(
        session,
        logical_asset_hash=record.logical_id,
        backend_id=backend_row.id,
        native_locator=record.native_locator,
        integrity_hash=record.integrity_hash,
        source=CopySource.SCRUB,
        health=record_health,
        last_verified_at=now,
        first_observed_at=now,
        storage_metadata=record.metadata,
    )
    if created:
        report.copies_added += 1


def _update_existing_copy(
    *,
    existing: Copy,
    record: CopyRecord,
    backend_row: Backend,
    record_health: CopyHealth,
    now: dt.datetime,
    report: ScrubReport,
) -> None:
    """Refresh a cataloged copy found again by backend-native locator."""
    existing.last_verified_at = now
    if existing.health == CopyHealth.MISSING:
        existing.health = record_health
    elif record_health == CopyHealth.SUSPECT:
        existing.health = CopyHealth.SUSPECT
    if existing.integrity_hash != record.integrity_hash:
        existing.health = CopyHealth.SUSPECT
        report.integrity_warnings.append(
            f"copy id={existing.id} on backend {backend_row.name!r}: "
            f"recorded integrity_hash={existing.integrity_hash.hex()[:12]}… "
            f"but enumerate returned {record.integrity_hash.hex()[:12]}…"
        )
    report.copies_updated += 1


def _health_for_record(
    record: CopyRecord, backend_row: Backend, report: ScrubReport
) -> CopyHealth:
    if record.logical_id == record.integrity_hash:
        return CopyHealth.OK

    report.integrity_warnings.append(
        f"backend {backend_row.name!r} yielded locator "
        f"{locator_key(record.native_locator)} with logical_id="
        f"{record.logical_id.hex()[:12]}… but integrity_hash="
        f"{record.integrity_hash.hex()[:12]}…"
    )
    return CopyHealth.SUSPECT


# --- helpers for non-CLI callers / tests ---------------------------------


def known_backend_kinds() -> Iterable[BackendKind]:
    return list(BackendKind)
