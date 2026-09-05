"""Tests for reconciliation scrub edge cases.

These cover catalog health transitions that are easier to exercise with a
small fake backend than through the day-1 CLI fixture adapter.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, select

from sutradhara.backend import factory as backend_factory
from sutradhara.backend.port import (
    BackendLocator,
    ByteRange,
    CopyRecord,
    VerifyResult,
)
from sutradhara.catalog.copies import add_copy
from sutradhara.catalog.models import (
    ArtifactClassPool,
    Backend,
    Copy,
    IngestItem,
    Intake,
    LogicalAsset,
    Pool,
    VerifyReceipt,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import (
    BackendKind,
    BackendTier,
    ContentHash,
    CopyHealth,
    CopySource,
    IntakeSourceKind,
    IntakeStatus,
    RetentionState,
    content_hash,
)
from sutradhara.jobs.engine import run_one
from sutradhara.jobs.handlers import verify as _verify_handler  # noqa: F401
from sutradhara.jobs.models import Job, JobStatus
from sutradhara.jobs.reconcilers import copy as copy_reconciler
from sutradhara.jobs.reconcilers.conditions import OBSERVED_PRESENT
from sutradhara.scrub import scrub_backend
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def _hash(value: bytes) -> ContentHash:
    return content_hash(hashlib.sha256(value).digest())


class _MismatchedIntegrityBackend:
    def __init__(self) -> None:
        self.logical_id = _hash(b"logical bytes")
        self.integrity_hash = _hash(b"different bytes")

    @property
    def name(self) -> str:
        return "bad-integrity"

    def enumerate(self) -> Iterator[CopyRecord]:
        yield CopyRecord(
            logical_id=self.logical_id,
            native_locator={"hash_hex": self.logical_id.hex()},
            integrity_hash=self.integrity_hash,
            size_bytes=13,
        )

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        raise AssertionError("read_range is not used by scrub")

    def verify(self, locator: BackendLocator) -> VerifyResult:
        raise AssertionError("verify is not used by scrub")


class _DuplicateLocatorBackend:
    """Yields the same (logical_id, locator) record twice in one enumerate pass."""

    def __init__(self) -> None:
        self.logical_id = _hash(b"dup locator body")
        self.locator = {"tape_uuid": "uuidA", "tape_file_number": 5}

    @property
    def name(self) -> str:
        return "dup-tape"

    def enumerate(self) -> Iterator[CopyRecord]:
        record = CopyRecord(
            logical_id=self.logical_id,
            native_locator=self.locator,
            integrity_hash=self.logical_id,
            size_bytes=17,
        )
        yield record
        yield record

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        raise AssertionError("read_range is not used by scrub")

    def verify(self, locator: BackendLocator) -> VerifyResult:
        raise AssertionError("verify is not used by scrub")


def test_scrub_marks_new_hash_conflict_suspect(engine: Engine) -> None:
    backend = _MismatchedIntegrityBackend()
    now = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)

    with session_scope(engine) as s:
        row = Backend(
            name=backend.name,
            kind=BackendKind.MEMORY,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(row)
        s.flush()

        report = scrub_backend(s, row, backend, now=now)
        copy = s.scalars(select(Copy)).one()

        assert report.copies_added == 1
        assert report.integrity_warnings
        assert copy.health == CopyHealth.SUSPECT


def test_scrub_keeps_existing_hash_conflict_suspect(engine: Engine) -> None:
    backend = _MismatchedIntegrityBackend()
    now = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)

    with session_scope(engine) as s:
        row = Backend(
            name=backend.name,
            kind=BackendKind.MEMORY,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(row)
        s.flush()
        scrub_backend(s, row, backend, now=now)

    with session_scope(engine) as s:
        row = s.scalars(select(Backend).where(Backend.name == backend.name)).one()
        report = scrub_backend(s, row, backend, now=now + dt.timedelta(hours=1))
        copy = s.scalars(select(Copy)).one()

        assert report.copies_updated == 1
        assert report.integrity_warnings
        assert copy.health == CopyHealth.SUSPECT


def test_scrub_verified_good_clears_suspect_copy(engine: Engine) -> None:
    data_hash = _hash(b"recoverable copy")
    locator = {"hash_hex": data_hash.hex()}
    now = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)

    class _GoodBackend:
        @property
        def name(self) -> str:
            return "good"

        def enumerate(self) -> Iterator[CopyRecord]:
            yield CopyRecord(
                logical_id=data_hash,
                native_locator=locator,
                integrity_hash=data_hash,
                size_bytes=16,
            )

        def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
            raise AssertionError("read_range is not used by scrub")

        def verify(self, locator: BackendLocator) -> VerifyResult:
            raise AssertionError("verify is not used by scrub")

    with session_scope(engine) as s:
        row = Backend(name="good", kind=BackendKind.MEMORY, tier=BackendTier.SELF_DESCRIBING)
        s.add(row)
        s.add(LogicalAsset(content_sha256=data_hash, size_bytes=16))
        s.flush()
        copy, _ = add_copy(
            s,
            logical_asset_hash=data_hash,
            backend_id=row.id,
            native_locator=locator,
            integrity_hash=data_hash,
            source=CopySource.INGEST,
            health=CopyHealth.SUSPECT,
        )
        copy.last_measured_digest = data_hash
        copy.last_measured_at = now - dt.timedelta(days=1)

        report = scrub_backend(s, row, _GoodBackend(), now=now, run_id="scrub-good-1")

        assert report.copies_updated == 1
        assert copy.health == CopyHealth.OK
        assert copy.last_measured_digest is None
        assert copy.last_measured_at is None
        receipt = s.scalars(select(VerifyReceipt).where(VerifyReceipt.copy_id == copy.id)).one()
        assert receipt.source == "scrub"
        assert receipt.execution_id == "scrub-good-1"
        assert receipt.failure_kind == "measurement-invalidated"
        verify_job = s.scalars(select(Job).where(Job.kind == "verify")).one()
        assert verify_job.params == {"copy_id": copy.id}


def test_scrub_discovery_is_satisfied_pending_until_verify_runs(
    engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A newly discovered OK copy must not launch a duplicate repair loop."""

    payload = b"scrub-discovered pending verification"
    data_hash = _hash(payload)
    pool_id = "scrub-pool"
    locator = {"pool_id": pool_id, "hash_hex": data_hash.hex()}

    class _DiscoveredBackend:
        @property
        def name(self) -> str:
            return "scrub-discovered"

        def enumerate(self) -> Iterator[CopyRecord]:
            yield CopyRecord(
                logical_id=data_hash,
                native_locator=locator,
                integrity_hash=data_hash,
                size_bytes=len(payload),
            )

        def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
            del locator
            return (
                payload
                if byte_range.is_whole_object
                else payload[byte_range.start : byte_range.end]
            )

        def verify(self, locator: BackendLocator) -> VerifyResult:
            actual = _hash(self.read_range(locator, ByteRange(0, 0)))
            return VerifyResult(
                ok=actual == data_hash,
                measured=True,
                actual_hash=actual,
            )

    with session_scope(engine) as session:
        backend = Backend(
            name="scrub-discovered",
            kind=BackendKind.MEMORY,
            tier=BackendTier.SELF_DESCRIBING,
        )
        session.add(backend)
        session.flush()
        session.add(
            Pool(
                id=pool_id,
                backend_id=backend.id,
                representation=Representation.RAW_BYTES.value,
            )
        )
        session.add(ArtifactClassPool(artifactclass="masters", pool_id=pool_id))
        session.add(LogicalAsset(content_sha256=data_hash, size_bytes=len(payload)))
        session.add(
            Intake(
                intake_id="scrub-intake",
                operator="tester",
                source_kind=IntakeSourceKind.CARD,
                source_ref="card",
                artifactclass="masters",
                status=IntakeStatus.REGISTERED,
                retention_state=RetentionState.HELD,
            )
        )
        session.flush()
        session.add(
            IngestItem(
                intake_id="scrub-intake",
                logical_asset_hash=data_hash,
                as_received_path="asset.bin",
                virtual_path="asset.bin",
                size_bytes=len(payload),
                artifactclass="masters",
            )
        )
        session.flush()

        report = scrub_backend(
            session,
            backend,
            _DiscoveredBackend(),
            run_id="scrub-discovery-1",
        )
        copy = session.scalars(select(Copy)).one()
        assert report.copies_added == 1
        assert copy.last_measured_digest is None
        verify_job = session.scalars(select(Job).where(Job.kind == "verify")).one()
        assert verify_job.params == {"copy_id": copy.id}
        target_key = copy_reconciler.make_target_key(data_hash, pool_id)
        observation = copy_reconciler.observe(session, target_key)
        assert observation.observed_state == OBSERVED_PRESENT

        measuring_backend = _DiscoveredBackend()
        monkeypatch.setattr(
            backend_factory,
            "backend_from_row",
            lambda _row: measuring_backend,
        )
        assert run_one(session, verify_job.id).ok
        receipt = session.scalars(
            select(VerifyReceipt).where(VerifyReceipt.copy_id == copy.id)
        ).one()
        assert receipt.source == "verify-job"
        assert receipt.execution_id == str(verify_job.id)
        assert receipt.measured_digest == data_hash
        assert copy.last_measured_digest == data_hash
        assert verify_job.status == JobStatus.SUCCEEDED


def test_scrub_quarantines_recognizable_bundle_container_unknown(engine: Engine) -> None:
    stored_digest = _hash(b"bundle container")
    now = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)

    class _BundleObjectBackend:
        @property
        def name(self) -> str:
            return "bundle-object"

        def enumerate(self) -> Iterator[CopyRecord]:
            yield CopyRecord(
                logical_id=stored_digest,
                native_locator={
                    "caller_object_id": "bundle-a-rao-plain-v1.rao",
                    "content_sha256": stored_digest.hex(),
                },
                integrity_hash=stored_digest,
                size_bytes=99,
                metadata={"body_format": "rem-archive-v1"},
            )

        def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
            raise AssertionError("read_range is not used by scrub")

        def verify(self, locator: BackendLocator) -> VerifyResult:
            raise AssertionError("verify is not used by scrub")

    with session_scope(engine) as s:
        row = Backend(
            name="bundle-object",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(row)
        s.flush()

        report = scrub_backend(s, row, _BundleObjectBackend(), now=now)

        assert report.unknown_objects == 1
        assert report.unknown_object_locators
        assert s.get(LogicalAsset, stored_digest) is None
        assert list(s.scalars(select(Copy))) == []


def test_scrub_still_adopts_unknown_non_container_object(engine: Engine) -> None:
    logical_id = _hash(b"ordinary unknown")
    now = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)

    class _OrdinaryBackend:
        @property
        def name(self) -> str:
            return "ordinary"

        def enumerate(self) -> Iterator[CopyRecord]:
            yield CopyRecord(
                logical_id=logical_id,
                native_locator={"hash_hex": logical_id.hex()},
                integrity_hash=logical_id,
                size_bytes=15,
            )

        def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
            raise AssertionError("read_range is not used by scrub")

        def verify(self, locator: BackendLocator) -> VerifyResult:
            raise AssertionError("verify is not used by scrub")

    with session_scope(engine) as s:
        row = Backend(name="ordinary", kind=BackendKind.MEMORY, tier=BackendTier.SELF_DESCRIBING)
        s.add(row)
        s.flush()

        report = scrub_backend(s, row, _OrdinaryBackend(), now=now)

        assert report.assets_added == 1
        assert report.copies_added == 1
        assert report.unknown_objects == 0
        assert s.get(LogicalAsset, logical_id) is not None


def test_scrub_existing_rao_copy_does_not_create_stored_digest_asset(
    engine: Engine,
) -> None:
    asset_hash = _hash(b"plaintext asset")
    stored_digest = _hash(b"stored rao bytes")
    locator = {"pool_id": "o-copy-1-pool", "object_id": "stored-rao"}
    now = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)

    class _ExistingRaoBackend:
        @property
        def name(self) -> str:
            return "rao-backend"

        def enumerate(self) -> Iterator[CopyRecord]:
            yield CopyRecord(
                logical_id=stored_digest,
                native_locator=locator,
                integrity_hash=stored_digest,
                size_bytes=99,
            )

        def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
            raise AssertionError("read_range is not used by scrub")

        def verify(self, locator: BackendLocator) -> VerifyResult:
            raise AssertionError("verify is not used by scrub")

    with session_scope(engine) as s:
        row = Backend(
            name="rao-backend",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(row)
        s.flush()
        s.add(
            Pool(
                id="o-copy-1-pool",
                backend_id=row.id,
                representation=Representation.RAO_PLAIN_V1.value,
            )
        )
        s.add(LogicalAsset(content_sha256=asset_hash, size_bytes=15))
        s.flush()
        add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=row.id,
            pool_id="o-copy-1-pool",
            native_locator=locator,
            integrity_hash=stored_digest,
            source=CopySource.INGEST,
            storage_metadata={"representation": Representation.RAO_PLAIN_V1.value},
        )

        report = scrub_backend(s, row, _ExistingRaoBackend(), now=now)

        assert report.assets_added == 0
        assert report.copies_updated == 1
        assert s.get(LogicalAsset, stored_digest) is None
        [copy] = list(s.scalars(select(Copy)))
        assert copy.logical_asset_hash == asset_hash
        assert copy.health == CopyHealth.OK


def test_scrub_tolerates_duplicate_locator_in_one_enumerate(engine: Engine) -> None:
    backend = _DuplicateLocatorBackend()
    now = dt.datetime(2026, 6, 1, tzinfo=dt.UTC)

    with session_scope(engine) as s:
        row = Backend(
            name=backend.name,
            kind=BackendKind.MEMORY,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(row)
        s.flush()

        report = scrub_backend(s, row, backend, now=now)

        # No IntegrityError; the duplicate collapses to a single copy row.
        assert report.copies_added == 1
        assert len(list(s.scalars(select(Copy)))) == 1


def test_scrub_during_open_batch_neither_marks_missing_nor_synthesizes(
    engine: Engine,
) -> None:
    """The backend port hides WRITTEN objects on both scrub reconciliation legs."""

    committed_hash = _hash(b"already checkpointed")
    written_hash = _hash(b"written only")
    committed_locator = {"object_id": "committed"}

    class _CheckpointVisibilityBackend:
        def __init__(self) -> None:
            self.checkpointed = [
                CopyRecord(
                    logical_id=committed_hash,
                    native_locator=committed_locator,
                    integrity_hash=committed_hash,
                    size_bytes=20,
                )
            ]
            self.written: list[CopyRecord] = []

        @property
        def name(self) -> str:
            return "checkpoint-visibility"

        def append_open_batch(self) -> None:
            self.written.append(
                CopyRecord(
                    logical_id=written_hash,
                    native_locator={"object_id": "provisional"},
                    integrity_hash=written_hash,
                    size_bytes=12,
                )
            )

        def enumerate(self) -> Iterator[CopyRecord]:
            # The contract deliberately excludes self.written.
            return iter(self.checkpointed)

        def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
            raise AssertionError("read_range is not used by scrub")

        def verify(self, locator: BackendLocator) -> VerifyResult:
            raise AssertionError("verify is not used by scrub")

    backend = _CheckpointVisibilityBackend()
    with session_scope(engine) as session:
        row = Backend(
            name=backend.name,
            kind=BackendKind.MEMORY,
            tier=BackendTier.SELF_DESCRIBING,
        )
        session.add(row)
        session.add(LogicalAsset(content_sha256=committed_hash, size_bytes=20))
        session.flush()
        existing, _ = add_copy(
            session,
            logical_asset_hash=committed_hash,
            backend_id=row.id,
            native_locator=committed_locator,
            integrity_hash=committed_hash,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
        )
        backend.append_open_batch()

        report = scrub_backend(session, row, backend)

        assert report.copies_marked_missing == 0
        assert report.copies_added == 0
        assert existing.health == CopyHealth.OK
        assert list(session.scalars(select(Copy))) == [existing]
        assert session.get(LogicalAsset, written_hash) is None


def test_scrub_against_live_backend_surfaces_unavailable(engine: Engine, tmp_path: Path) -> None:
    from sutradhara.backend.port import BackendUnavailableError
    from sutradhara.backend.remanence import RemanenceBackend

    live = RemanenceBackend.from_grpc("primary-tape", f"unix:{tmp_path / 'missing.sock'}")
    with session_scope(engine) as s:
        row = Backend(
            name="primary-tape",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(row)
        s.flush()
        with pytest.raises(BackendUnavailableError):
            scrub_backend(s, row, live)
