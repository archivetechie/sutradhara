"""Catalog model tests — round-trip, dedup, FK enforcement."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, select
from sqlalchemy.exc import IntegrityError

from sutradhara.catalog.models import Backend, Copy, LogicalAsset
from sutradhara.catalog.session import (
    create_all,
    locator_key,
    make_engine,
    session_scope,
)
from sutradhara.catalog.types import (
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
    MediaKind,
)


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def _hash(s: str) -> bytes:
    return hashlib.sha256(s.encode()).digest()


def _add_backend(engine: Engine, name: str = "primary-tape") -> int:
    with session_scope(engine) as s:
        b = Backend(
            name=name,
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
            config={"library_uuid": "L91234L9"},
        )
        s.add(b)
        s.flush()
        return b.id


def test_logical_asset_roundtrip(engine: Engine) -> None:
    h = _hash("hello")
    with session_scope(engine) as s:
        s.add(
            LogicalAsset(
                content_sha256=h,
                size_bytes=5,
                human_label="hello.txt",
                media_kind=MediaKind.DOCUMENT,
            )
        )

    with session_scope(engine) as s:
        loaded = s.get(LogicalAsset, h)
        assert loaded is not None
        assert loaded.content_sha256 == h
        assert loaded.size_bytes == 5
        assert loaded.human_label == "hello.txt"
        assert loaded.media_kind == MediaKind.DOCUMENT


def test_same_hash_is_same_row_full_dedup(engine: Engine) -> None:
    """spec-v0.1.md §2 principle 3: same hash = same logical asset."""
    h = _hash("identical")

    with session_scope(engine) as s:
        s.add(LogicalAsset(content_sha256=h, size_bytes=9))

    # Second insert with the same hash MUST raise — the PK enforces dedup.
    with pytest.raises(IntegrityError), session_scope(engine) as s:
        s.add(LogicalAsset(content_sha256=h, size_bytes=9))


def test_backend_name_is_unique(engine: Engine) -> None:
    _add_backend(engine, name="primary-tape")
    with pytest.raises(IntegrityError), session_scope(engine) as s:
        s.add(
            Backend(
                name="primary-tape",
                kind=BackendKind.S3,
                tier=BackendTier.SELF_DESCRIBING,
            )
        )


def test_copy_requires_existing_asset_and_backend(engine: Engine) -> None:
    """FK constraints are enforced (SQLite PRAGMA foreign_keys=ON)."""
    h = _hash("orphan")
    locator = {"tape_uuid": "abcd", "tape_file_number": 7}

    with pytest.raises(IntegrityError), session_scope(engine) as s:
        s.add(
            Copy(
                logical_asset_hash=h,  # no LogicalAsset with this PK
                backend_id=999,  # no Backend with this PK
                native_locator=locator,
                native_locator_key=locator_key(locator),
                integrity_hash=h,
                source=CopySource.SCRUB,
            )
        )


def test_copy_locator_is_unique_per_backend(engine: Engine) -> None:
    h = _hash("locked")
    backend_id = _add_backend(engine)
    locator = {"tape_uuid": "abcd", "tape_file_number": 1}

    with session_scope(engine) as s:
        s.add(LogicalAsset(content_sha256=h, size_bytes=12))
        s.add(
            Copy(
                logical_asset_hash=h,
                backend_id=backend_id,
                native_locator=locator,
                native_locator_key=locator_key(locator),
                integrity_hash=h,
                source=CopySource.INGEST,
            )
        )

    # Second copy with the same (backend_id, locator) is rejected.
    with pytest.raises(IntegrityError), session_scope(engine) as s:
        s.add(
            Copy(
                logical_asset_hash=h,
                backend_id=backend_id,
                native_locator=locator,
                native_locator_key=locator_key(locator),
                integrity_hash=h,
                source=CopySource.SCRUB,
            )
        )


def test_same_asset_can_have_copies_on_two_backends(engine: Engine) -> None:
    """Multi-copy across backends: one asset, two backends, two copy rows."""
    h = _hash("multi-copy")
    primary = _add_backend(engine, name="primary-tape")
    secondary = _add_backend(engine, name="cloud-mirror")
    loc1 = {"tape_uuid": "tape-1", "tape_file_number": 3}
    loc2 = {"bucket": "archive-primary", "key": "assets/ab/cd/abcd/object.bin"}

    with session_scope(engine) as s:
        s.add(LogicalAsset(content_sha256=h, size_bytes=42))
        s.add(
            Copy(
                logical_asset_hash=h,
                backend_id=primary,
                native_locator=loc1,
                native_locator_key=locator_key(loc1),
                integrity_hash=h,
                source=CopySource.INGEST,
                health=CopyHealth.OK,
            )
        )
        s.add(
            Copy(
                logical_asset_hash=h,
                backend_id=secondary,
                native_locator=loc2,
                native_locator_key=locator_key(loc2),
                integrity_hash=h,
                source=CopySource.SCRUB,
                health=CopyHealth.OK,
            )
        )

    with session_scope(engine) as s:
        asset = s.get(LogicalAsset, h)
        assert asset is not None
        assert len(asset.copies) == 2
        backends_seen = {c.backend.name for c in asset.copies}
        assert backends_seen == {"primary-tape", "cloud-mirror"}


def test_locator_key_is_deterministic_regardless_of_dict_order() -> None:
    """spec discipline: the UNIQUE key must collide for semantically-equal locators."""
    a = {"tape_uuid": "X", "tape_file_number": 7}
    b = {"tape_file_number": 7, "tape_uuid": "X"}
    assert locator_key(a) == locator_key(b)


def test_cascade_delete_removes_copies(engine: Engine) -> None:
    h = _hash("cascade")
    backend_id = _add_backend(engine)
    locator = {"path": "/tmp/cascade"}

    with session_scope(engine) as s:
        s.add(LogicalAsset(content_sha256=h, size_bytes=1))
        s.add(
            Copy(
                logical_asset_hash=h,
                backend_id=backend_id,
                native_locator=locator,
                native_locator_key=locator_key(locator),
                integrity_hash=h,
                source=CopySource.INGEST,
            )
        )

    with session_scope(engine) as s:
        asset = s.get(LogicalAsset, h)
        assert asset is not None
        s.delete(asset)

    with session_scope(engine) as s:
        remaining = s.scalars(select(Copy).where(Copy.logical_asset_hash == h)).all()
        assert remaining == []
