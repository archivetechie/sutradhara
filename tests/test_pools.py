"""Tests for pool mutation invariants."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine

from sutradhara.catalog.copies import add_copy
from sutradhara.catalog.models import Backend, LogicalAsset, Pool
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopySource
from sutradhara.pools import (
    PoolRepresentationImmutable,
    UnknownPool,
    set_pool_representation,
)
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def _digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def test_pool_representation_can_change_until_first_copy(engine: Engine) -> None:
    with session_scope(engine) as s:
        backend = Backend(
            name="rem",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(backend)
        s.flush()
        s.add(
            Pool(
                id="archive-pool",
                backend_id=backend.id,
                representation=Representation.RAW_BYTES.value,
            )
        )
        s.flush()

        pool = set_pool_representation(
            s,
            "archive-pool",
            Representation.RAO_PLAIN_V1,
        )
        assert pool.representation == Representation.RAO_PLAIN_V1.value


def test_pool_representation_is_immutable_after_first_copy(engine: Engine) -> None:
    asset_hash = _digest(b"asset")
    with session_scope(engine) as s:
        backend = Backend(
            name="rem",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        s.add(backend)
        s.flush()
        s.add(
            Pool(
                id="archive-pool",
                backend_id=backend.id,
                representation=Representation.RAO_PLAIN_V1.value,
            )
        )
        s.add(LogicalAsset(content_sha256=asset_hash, size_bytes=5))
        s.flush()
        add_copy(
            s,
            logical_asset_hash=asset_hash,
            backend_id=backend.id,
            pool_id="archive-pool",
            native_locator={"pool_id": "archive-pool", "object_id": "one"},
            integrity_hash=asset_hash,
            source=CopySource.INGEST,
            storage_metadata={"representation": Representation.RAO_PLAIN_V1.value},
        )

        same = set_pool_representation(
            s,
            "archive-pool",
            Representation.RAO_PLAIN_V1,
        )
        assert same.representation == Representation.RAO_PLAIN_V1.value
        with pytest.raises(PoolRepresentationImmutable):
            set_pool_representation(s, "archive-pool", Representation.D2TAR_RAW)


def test_set_pool_representation_rejects_unknown_pool(engine: Engine) -> None:
    with session_scope(engine) as s, pytest.raises(UnknownPool):
        set_pool_representation(s, "missing", Representation.RAO_PLAIN_V1)
