"""Catalog copy API tests: generic add_copy + lookup_by_hash."""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import Engine, select

from sutradhara.catalog.copies import (
    UnknownBundle,
    UnknownLogicalAsset,
    add_bundle_copy,
    add_copy,
    lookup_by_hash,
)
from sutradhara.catalog.models import Backend, Bundle, Copy, LogicalAsset, Pool
from sutradhara.catalog.session import create_all, locator_key, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def _hash(value: str) -> bytes:
    return hashlib.sha256(value.encode()).digest()


def _add_backend(
    engine: Engine,
    name: str = "archive-tape",
    kind: BackendKind = BackendKind.REM_TAPE,
) -> int:
    with session_scope(engine) as s:
        b = Backend(
            name=name,
            kind=kind,
            tier=BackendTier.SELF_DESCRIBING,
            config={"library_uuid": "00112233445566778899aabbccddeeff"},
        )
        s.add(b)
        s.flush()
        return b.id


def _add_asset(engine: Engine, content: bytes) -> bytes:
    asset_hash = hashlib.sha256(content).digest()
    with session_scope(engine) as s:
        s.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(content)))
    return asset_hash


def _locator(content_hash: bytes, *, tape_file_number: int = 7) -> dict[str, Any]:
    return {
        "tape_uuid": "b62f35b7f2694d4a8d0c2ffdd6e0a101",
        "tape_file_number": tape_file_number,
        "object_id": f"obj-{tape_file_number:06d}",
        "pool_id": "pool-main",
        "content_sha256": content_hash.hex(),
        "body_format": "remanence.body.v1",
    }


def test_add_copy_inserts_new_copy_and_reports_created(engine: Engine) -> None:
    backend_id = _add_backend(engine, "tape-primary")
    asset_hash = _add_asset(engine, b"asset body")
    locator = _locator(asset_hash)
    integrity = _hash("verified body")
    now = dt.datetime(2026, 6, 1, 12, 0, tzinfo=dt.UTC)

    with session_scope(engine) as s:
        copy, created = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator=locator,
            integrity_hash=integrity,
            source=CopySource.INGEST,
            last_checked_at=now,
            first_observed_at=now,
        )
        assert created is True
        assert copy.id is not None
        assert copy.logical_asset_hash == asset_hash
        assert copy.backend_id == backend_id
        assert copy.native_locator == locator
        assert copy.native_locator_key == locator_key(locator)
        assert copy.integrity_hash == integrity
        assert copy.source == CopySource.INGEST
        assert copy.health == CopyHealth.OK
        assert copy.last_checked_at == now
        assert copy.first_observed_at == now

    with session_scope(engine) as s:
        assert len(list(s.scalars(select(Copy)))) == 1


def test_add_copy_health_defaults_to_ok_and_honors_override(engine: Engine) -> None:
    backend_id = _add_backend(engine)
    ok_hash = _add_asset(engine, b"ok body")
    suspect_hash = _add_asset(engine, b"suspect body")

    with session_scope(engine) as s:
        ok_copy, _ = add_copy(
            s,
            logical_asset_hash=ok_hash,
            backend_id=backend_id,
            native_locator=_locator(ok_hash, tape_file_number=1),
            integrity_hash=ok_hash,
            source=CopySource.SCRUB,
        )
        suspect_copy, _ = add_copy(
            s,
            logical_asset_hash=suspect_hash,
            backend_id=backend_id,
            native_locator=_locator(suspect_hash, tape_file_number=2),
            integrity_hash=_hash("mismatch"),
            source=CopySource.SCRUB,
            health=CopyHealth.SUSPECT,
        )
        assert ok_copy.health == CopyHealth.OK
        assert suspect_copy.health == CopyHealth.SUSPECT


def test_add_copy_is_idempotent_and_does_not_mutate_existing(engine: Engine) -> None:
    backend_id = _add_backend(engine)
    asset_hash = _add_asset(engine, b"idempotent body")
    locator = _locator(asset_hash)
    original_integrity = _hash("original")

    with session_scope(engine) as s:
        first, first_created = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator=locator,
            integrity_hash=original_integrity,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
        )
        second, second_created = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator=locator,
            integrity_hash=_hash("ignored"),
            source=CopySource.SCRUB,
            health=CopyHealth.SUSPECT,
        )
        assert first_created is True
        assert second_created is False
        assert second.id == first.id
        # existing row is returned UNMUTATED
        assert second.integrity_hash == original_integrity
        assert second.source == CopySource.INGEST
        assert second.health == CopyHealth.OK

    with session_scope(engine) as s:
        copies = list(s.scalars(select(Copy)))
        assert len(copies) == 1
        assert copies[0].integrity_hash == original_integrity


def test_add_bundle_copy_records_bundle_without_logical_asset(engine: Engine) -> None:
    backend_id = _add_backend(engine)
    locator = {
        "pool_id": "archive-pool",
        "object_id": "bundle-001",
        "content_sha256": _hash("bundle").hex(),
    }

    with session_scope(engine) as s:
        s.add(Bundle(id="bundle-001", artifactclass="o-archive"))
        s.add(
            Pool(
                id="archive-pool",
                backend_id=backend_id,
                representation="rao-plain-v1",
            )
        )
        s.flush()
        first, created = add_bundle_copy(
            s,
            bundle_id="bundle-001",
            backend_id=backend_id,
            pool_id="archive-pool",
            native_locator=locator,
            integrity_hash=_hash("bundle"),
            source=CopySource.INGEST,
            storage_metadata={"representation": "rao-plain-v1"},
        )
        second, second_created = add_bundle_copy(
            s,
            bundle_id="bundle-001",
            backend_id=backend_id,
            pool_id="archive-pool",
            native_locator=locator,
            integrity_hash=_hash("ignored"),
            source=CopySource.SCRUB,
            storage_metadata={"representation": "ignored"},
        )
        assert created is True
        assert second_created is False
        assert second.id == first.id
        assert first.logical_asset_hash is None
        assert first.bundle_id == "bundle-001"


def test_add_bundle_copy_rejects_unknown_bundle(engine: Engine) -> None:
    backend_id = _add_backend(engine)

    with session_scope(engine) as s, pytest.raises(UnknownBundle, match="no Bundle"):
        add_bundle_copy(
            s,
            bundle_id="missing",
            backend_id=backend_id,
            pool_id="archive-pool",
            native_locator={"object_id": "bundle-001"},
            integrity_hash=_hash("bundle"),
            source=CopySource.INGEST,
        )


def test_add_copy_same_locator_different_backend_is_separate_copy(
    engine: Engine,
) -> None:
    tape_id = _add_backend(engine, "tape-a")
    disk_id = _add_backend(engine, "disk-b", kind=BackendKind.PLAIN_DISK)
    asset_hash = _add_asset(engine, b"two backends")
    locator = _locator(asset_hash)

    with session_scope(engine) as s:
        _, c1 = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=tape_id,
            native_locator=locator,
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
        )
        _, c2 = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=disk_id,
            native_locator=locator,
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
        )
        assert c1 is True
        assert c2 is True

    with session_scope(engine) as s:
        assert len(list(s.scalars(select(Copy)))) == 2


def test_add_copy_different_locator_same_backend_is_separate_copy(
    engine: Engine,
) -> None:
    backend_id = _add_backend(engine)
    asset_hash = _add_asset(engine, b"two locators")

    with session_scope(engine) as s:
        _, c1 = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator=_locator(asset_hash, tape_file_number=3),
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
        )
        _, c2 = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator=_locator(asset_hash, tape_file_number=4),
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
        )
        assert c1 is True
        assert c2 is True

    with session_scope(engine) as s:
        assert len(list(s.scalars(select(Copy)))) == 2


def test_add_copy_raises_when_asset_missing_and_stages_nothing(
    engine: Engine,
) -> None:
    backend_id = _add_backend(engine)
    missing = _hash("missing asset")

    with (
        session_scope(engine) as s,
        pytest.raises(UnknownLogicalAsset, match="no LogicalAsset"),
    ):
        add_copy(
            s,
            logical_asset_hash=missing,
            backend_id=backend_id,
            native_locator=_locator(missing),
            integrity_hash=missing,
            source=CopySource.INGEST,
        )

    with session_scope(engine) as s:
        assert list(s.scalars(select(Copy))) == []


def test_add_copy_rejects_non_content_hash(engine: Engine) -> None:
    backend_id = _add_backend(engine)
    with (
        session_scope(engine) as s,
        pytest.raises(ValueError, match="32-byte"),
    ):
        add_copy(
            s,
            logical_asset_hash=b"too short",
            backend_id=backend_id,
            native_locator={"k": "v"},
            integrity_hash=b"too short",
            source=CopySource.INGEST,
        )


def test_lookup_by_hash_round_trips_added_locator(engine: Engine) -> None:
    backend_id = _add_backend(engine, "tape-primary")
    asset_hash = _add_asset(engine, b"lookup body")
    locator = _locator(asset_hash)

    with session_scope(engine) as s:
        add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend_id,
            native_locator=locator,
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
        )
        result = lookup_by_hash(s, asset_hash)

    assert result == {
        "id": asset_hash,
        "copies": [
            {
                "locator": locator,
                "integrity_hash": asset_hash,
                "health": "ok",
                "backend": "tape-primary",
            }
        ],
    }


def test_lookup_by_hash_orders_multiple_copies_by_copy_id(engine: Engine) -> None:
    tape_id = _add_backend(engine, "tape-primary")
    disk_id = _add_backend(engine, "disk-mirror", kind=BackendKind.PLAIN_DISK)
    asset_hash = _add_asset(engine, b"multi-copy body")
    first_locator = _locator(asset_hash, tape_file_number=11)
    second_locator = {"path": "/mirror/obj", "replica": "disk"}

    with session_scope(engine) as s:
        first, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=tape_id,
            native_locator=first_locator,
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
        )
        second, _ = add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=disk_id,
            native_locator=second_locator,
            integrity_hash=asset_hash,
            source=CopySource.SCRUB,
        )
        assert first.id < second.id
        result = lookup_by_hash(s, asset_hash)

    assert result["id"] == asset_hash
    assert [c["backend"] for c in result["copies"]] == [
        "tape-primary",
        "disk-mirror",
    ]
    assert result["copies"][0]["locator"] == first_locator
    assert result["copies"][1]["locator"] == second_locator


def test_lookup_by_hash_raises_when_asset_unknown(engine: Engine) -> None:
    unknown = _hash("unknown")
    with (
        session_scope(engine) as s,
        pytest.raises(UnknownLogicalAsset, match="no LogicalAsset"),
    ):
        lookup_by_hash(s, unknown)


def test_add_copy_importable_from_catalog_package() -> None:
    import sutradhara.catalog as catalog

    assert catalog.add_copy is add_copy
    assert catalog.lookup_by_hash is lookup_by_hash
    assert issubclass(catalog.UnknownLogicalAsset, catalog.CatalogError)
