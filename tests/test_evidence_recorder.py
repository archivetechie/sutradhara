"""Closed-writer and transactional tests for deletion-evidence measurements."""

from __future__ import annotations

import ast
import datetime as dt
import hashlib
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, func, select

from sutradhara.backend.port import VerifyResult
from sutradhara.catalog.models import Backend, Copy, LogicalAsset, VerifyReceipt
from sutradhara.catalog.session import (
    create_all,
    locator_key,
    make_engine,
    make_session_factory,
    session_scope,
)
from sutradhara.catalog.types import (
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
    IntegrityHashProvenance,
    content_hash,
)
from sutradhara.evidence_recorder import record_measured


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_measurement_projection_has_one_production_writer_module() -> None:
    """The five historical writers cannot assign the measurement pair directly."""

    source_root = Path(__file__).resolve().parents[1] / "src" / "sutradhara"
    writers: set[Path] = set()
    for path in source_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            target = getattr(node, "target", None)
            if (
                isinstance(target, ast.Attribute)
                and isinstance(target.ctx, ast.Store)
                and target.attr in {"last_measured_digest", "last_measured_at"}
            ):
                writers.add(path.relative_to(source_root))
            for target in getattr(node, "targets", ()):
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.ctx, ast.Store)
                    and target.attr in {"last_measured_digest", "last_measured_at"}
                ):
                    writers.add(path.relative_to(source_root))

    assert writers == {Path("evidence_recorder.py")}


def test_record_measured_projection_and_receipt_commit_or_rollback_together(
    engine: Engine,
) -> None:
    copy_id = _seed_copy(engine)
    factory = make_session_factory(engine)
    session = factory()
    try:
        copy = session.get(Copy, copy_id)
        assert copy is not None
        record_measured(
            session,
            copy,
            VerifyResult(ok=True, measured=True, actual_hash=copy.integrity_hash),
            source="verify-job",
            execution_id="verify-job-rollback",
        )
        session.rollback()
    finally:
        session.close()

    with session_scope(engine) as check:
        copy = check.get(Copy, copy_id)
        assert copy is not None
        assert copy.last_measured_digest is None
        assert check.scalar(select(func.count()).select_from(VerifyReceipt)) == 0

    measured_at = dt.datetime(2026, 7, 19, 12, 0, tzinfo=dt.UTC)
    with session_scope(engine) as session:
        copy = session.get(Copy, copy_id)
        assert copy is not None
        receipt = record_measured(
            session,
            copy,
            VerifyResult(ok=True, measured=True, actual_hash=copy.integrity_hash),
            source="verify-job",
            execution_id="verify-job-commit",
            measured_at=measured_at,
        )
        assert receipt.measured_digest == copy.integrity_hash

    with session_scope(engine) as check:
        copy = check.get(Copy, copy_id)
        assert copy is not None
        assert copy.last_measured_digest == copy.integrity_hash
        assert copy.last_measured_at is not None
        assert copy.last_measured_at.replace(tzinfo=dt.UTC) == measured_at
        assert check.scalar(select(func.count()).select_from(VerifyReceipt)) == 1


def test_record_measured_retry_deduplicates_without_rewriting_projection(
    engine: Engine,
) -> None:
    copy_id = _seed_copy(engine)
    good_at = dt.datetime(2026, 7, 19, 12, 0, tzinfo=dt.UTC)
    with session_scope(engine) as session:
        copy = session.get(Copy, copy_id)
        assert copy is not None
        first = record_measured(
            session,
            copy,
            VerifyResult(ok=True, measured=True, actual_hash=copy.integrity_hash),
            source="fanout",
            execution_id="fanout-attempt-1",
            measured_at=good_at,
        )
        wrong = content_hash(hashlib.sha256(b"different bytes").digest())
        retried = record_measured(
            session,
            copy,
            VerifyResult(ok=False, measured=True, actual_hash=wrong),
            source="fanout",
            execution_id="fanout-attempt-1",
            measured_at=good_at + dt.timedelta(minutes=1),
        )
        assert retried.event_id == first.event_id
        assert copy.health == CopyHealth.OK
        assert copy.last_measured_digest == copy.integrity_hash
        assert copy.last_measured_at == good_at
        assert session.scalar(select(func.count()).select_from(VerifyReceipt)) == 1


def test_backend_discovered_identity_conflict_cannot_reach_ok_via_readback(
    engine: Engine,
) -> None:
    asset_digest = content_hash(hashlib.sha256(b"asset identity").digest())
    discovered_digest = content_hash(hashlib.sha256(b"backend claim").digest())
    with session_scope(engine) as session:
        backend = Backend(
            name="discovered-memory",
            kind=BackendKind.MEMORY,
            tier=BackendTier.SELF_DESCRIBING,
        )
        session.add_all([backend, LogicalAsset(content_sha256=asset_digest, size_bytes=14)])
        session.flush()
        locator = {"object": "discovered-conflict"}
        copy = Copy(
            logical_asset_hash=asset_digest,
            backend_id=backend.id,
            native_locator=locator,
            native_locator_key=locator_key(locator),
            integrity_hash=discovered_digest,
            integrity_hash_provenance=IntegrityHashProvenance.BACKEND_DISCOVERED,
            source=CopySource.SCRUB,
            health=CopyHealth.SUSPECT,
        )
        session.add(copy)
        session.flush()

        receipt = record_measured(
            session,
            copy,
            VerifyResult(ok=True, measured=True, actual_hash=discovered_digest),
            source="verify-job",
            execution_id="verify-discovered-conflict",
        )

        assert copy.last_measured_digest == discovered_digest
        assert copy.health == CopyHealth.SUSPECT
        assert receipt.failure_kind == "identity-unproven"


def _seed_copy(engine: Engine) -> int:
    digest = content_hash(hashlib.sha256(b"evidence bytes").digest())
    locator = {"hash_hex": digest.hex()}
    with session_scope(engine) as session:
        backend = Backend(
            name="evidence-memory",
            kind=BackendKind.MEMORY,
            tier=BackendTier.SELF_DESCRIBING,
        )
        session.add_all([backend, LogicalAsset(content_sha256=digest, size_bytes=14)])
        session.flush()
        copy = Copy(
            logical_asset_hash=digest,
            backend_id=backend.id,
            native_locator=locator,
            native_locator_key=locator_key(locator),
            integrity_hash=digest,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
        )
        session.add(copy)
        session.flush()
        return copy.id
