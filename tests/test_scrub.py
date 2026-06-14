"""Tests for reconciliation scrub edge cases.

These cover catalog health transitions that are easier to exercise with a
small fake backend than through the day-1 CLI fixture adapter.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, select

from sutradhara.backend.port import (
    BackendLocator,
    ByteRange,
    CopyRecord,
    VerifyResult,
)
from sutradhara.catalog.copies import add_copy
from sutradhara.catalog.models import Backend, Copy, LogicalAsset, Pool
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import (
    BackendKind,
    BackendTier,
    ContentHash,
    CopyHealth,
    CopySource,
    content_hash,
)
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


def test_scrub_against_live_backend_surfaces_unavailable(engine: Engine) -> None:
    from sutradhara.backend.port import BackendUnavailableError
    from sutradhara.backend.remanence import RemanenceBackend

    live = RemanenceBackend.from_grpc("primary-tape", "127.0.0.1:1")
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
