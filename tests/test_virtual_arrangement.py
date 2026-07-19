"""Virtual arrangement tests for content-level organize-forever views."""

from __future__ import annotations

import contextlib
import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

from sutradhara.archive_bundle import enqueue_artifact
from sutradhara.archive_fanout import LocalArchiveBuilder, flush_bundle
from sutradhara.archive_restore import RestoreRejectedAsset, RestoreSuspectAsset, restore_asset
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    DurabilityPolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
    get_artifactclass_policy,
)
from sutradhara.backend.memory import MemoryBackend
from sutradhara.backend.port import BackendLocator, ByteRange, CopyRecord, VerifyResult
from sutradhara.catalog.copies import add_copy
from sutradhara.catalog.models import (
    AssetLocator,
    AssetTag,
    Backend,
    Copy,
    IngestItem,
    Intake,
    LogicalAsset,
    Pool,
    VirtualArrangementHistory,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import (
    AssetValidity,
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
    IntakeSourceKind,
    IntakeStatus,
    content_hash,
)
from sutradhara.restore import restore_copy
from sutradhara.sealing.port import Representation
from sutradhara.virtual_arrangement import (
    VirtualArrangementAmbiguousClass,
    VirtualArrangementError,
    VirtualArrangementNotArchived,
    add_member,
    add_tag,
    create_view,
    exclude_member,
    include_member,
    list_view,
    move_member,
    reject_asset,
    remove_tag,
    resolve,
    unreject_asset,
)


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


class _ArchiveBackend:
    def __init__(self, name: str) -> None:
        self.name = name
        self.objects: dict[str, bytes] = {}
        self._counter = 0

    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        data = Path(source).read_bytes()
        self._counter += 1
        object_id = f"{self.name}-{self._counter}"
        digest = content_hash(hashlib.sha256(data).digest())
        self.objects[object_id] = data
        return CopyRecord(
            logical_id=digest,
            native_locator={
                "object_id": object_id,
                "content_sha256": digest.hex(),
            },
            integrity_hash=digest,
            size_bytes=len(data),
        )

    def enumerate(self) -> Iterator[CopyRecord]:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        data = self.objects[str(locator["object_id"])]
        if byte_range.is_whole_object:
            return data
        return data[byte_range.start : byte_range.end]

    def verify(self, locator: BackendLocator) -> VerifyResult:
        data = self.read_range(locator, ByteRange(0, 0))
        actual = content_hash(hashlib.sha256(data).digest())
        expected = content_hash(bytes.fromhex(str(locator["content_sha256"])))
        return VerifyResult(ok=actual == expected, measured=True, actual_hash=actual)


class _RawOpener:
    @contextlib.contextmanager
    def open(
        self,
        source_path: Path | str,
        representation: Representation,
        **_: Any,
    ) -> Iterator[Path]:
        assert representation is Representation.RAW_BYTES
        yield Path(source_path)


def test_mv_catalog_only_and_restore_by_virtual_path(engine: Engine, tmp_path: Path) -> None:
    backend = _ArchiveBackend("masters")
    source = tmp_path / "source" / "clip-a.mov"
    source.parent.mkdir()
    source.write_bytes(b"master-a")
    with session_scope(engine) as session:
        backend_id = _install_policy(session, "s-masters")
        asset_hash = _archive_asset(session, "s-masters", source, backend_id, backend)
        _add_ingest_occurrence(session, asset_hash, "intake-a", "DCIM/A001.MOV")
        view = create_view(session, "programs", created_by="operator")
        add_member(
            session,
            view,
            asset_hash,
            "raw/A001.MOV",
            added_by="operator",
        )
        object_snapshot = dict(backend.objects)
        item = session.scalar(select(IngestItem).where(IngestItem.logical_asset_hash == asset_hash))
        assert item is not None
        item_snapshot = (item.as_received_path, item.virtual_path)

        moved = move_member(
            session,
            view,
            "raw/A001.MOV",
            "programs/day-1/opening.MOV",
            actor="operator",
        )

        assert moved.path == "programs/day-1/opening.MOV"
        assert (item.as_received_path, item.virtual_path) == item_snapshot
        assert source.read_bytes() == b"master-a"
        assert backend.objects == object_snapshot
        history = session.scalars(select(VirtualArrangementHistory)).one()
        assert history.va_member_id == moved.id
        assert history.logical_asset_hash == asset_hash
        assert history.artifactclass == "s-masters"
        assert history.old_path == "raw/A001.MOV"
        assert history.new_path == "programs/day-1/opening.MOV"

        resolved_hash, artifactclass = resolve(session, "programs", "programs/day-1/opening.MOV")
        restored = restore_asset(
            session,
            asset_hash=resolved_hash,
            artifactclass=artifactclass,
            destination=tmp_path / "restored.mov",
            backends={backend_id: backend},
        )
        assert restored.output_path.read_bytes() == b"master-a"


def test_multi_view_exclude_include_and_one_path_per_asset(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = _ArchiveBackend("masters")
    source_a = tmp_path / "a.mov"
    source_b = tmp_path / "b.mov"
    source_a.write_bytes(b"a")
    source_b.write_bytes(b"b")
    with session_scope(engine) as session:
        backend_id = _install_policy(session, "s-masters")
        asset_a = _archive_asset(session, "s-masters", source_a, backend_id, backend)
        asset_b = _archive_asset(session, "s-masters", source_b, backend_id, backend)
        programs = create_view(session, "programs", created_by="operator")
        speakers = create_view(session, "speakers", created_by="operator")
        add_member(session, programs, asset_a, "clips/a.mov", added_by="operator")
        add_member(session, speakers, asset_a, "talks/a.mov", added_by="operator")

        assert resolve(session, "programs", "clips/a.mov") == (asset_a, "s-masters")
        assert resolve(session, "speakers", "talks/a.mov") == (asset_a, "s-masters")
        with pytest.raises(VirtualArrangementError, match="already in view"):
            add_member(session, programs, asset_a, "aliases/a.mov", added_by="operator")

        exclude_member(session, programs, "clips/a.mov")
        assert list_view(session, programs) == []
        include_member(session, programs, "clips/a.mov")
        assert [row.path for row in list_view(session, programs)] == ["clips/a.mov"]

        exclude_member(session, programs, "clips/a.mov")
        add_member(session, programs, asset_b, "clips/a.mov", added_by="operator")
        with pytest.raises(VirtualArrangementError, match="duplicate active"):
            include_member(session, programs, "clips/a.mov")


def test_reject_gates_restore_globally_and_preservation_is_ungated(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = _ArchiveBackend("masters")
    source = tmp_path / "source.mov"
    source.write_bytes(b"reject me")
    with session_scope(engine) as session:
        backend_id = _install_policy(session, "s-masters")
        asset_hash = _archive_asset(session, "s-masters", source, backend_id, backend)
        view = create_view(session, "programs", created_by="operator")
        add_member(session, view, asset_hash, "clips/rejected.mov", added_by="operator")
        memory_backend_id, memory_backend, copy_id = _asset_copy_for_restore_copy(
            session,
            asset_hash,
            b"reject me",
        )
        assert memory_backend_id > 0

        reject_asset(session, asset_hash, actor="operator", reason="bad take")
        assert list_view(session, view) == []
        with pytest.raises(RestoreRejectedAsset, match="bad take"):
            restore_asset(
                session,
                asset_hash=asset_hash,
                artifactclass="s-masters",
                destination=tmp_path / "blocked.mov",
                backends={backend_id: backend},
            )
        forced = restore_asset(
            session,
            asset_hash=asset_hash,
            artifactclass="s-masters",
            destination=tmp_path / "forced.mov",
            backends={backend_id: backend},
            force_rejected=True,
        )
        assert forced.output_path.read_bytes() == b"reject me"

        copy = session.get(Copy, copy_id)
        assert copy is not None
        with restore_copy(
            session,
            copy,
            backend=memory_backend,
            opener=_RawOpener(),
            execution_id="restore-test:rejected-asset",
        ) as restored:
            assert restored.path.read_bytes() == b"reject me"

        asset = session.get(LogicalAsset, asset_hash)
        assert asset is not None
        asset.validity = AssetValidity.SUSPECT
        with pytest.raises(RestoreRejectedAsset):
            restore_asset(
                session,
                asset_hash=asset_hash,
                artifactclass="s-masters",
                destination=tmp_path / "force-suspect-only.mov",
                backends={backend_id: backend},
                force_suspect=True,
            )
        with pytest.raises(RestoreSuspectAsset):
            restore_asset(
                session,
                asset_hash=asset_hash,
                artifactclass="s-masters",
                destination=tmp_path / "force-rejected-only.mov",
                backends={backend_id: backend},
                force_rejected=True,
            )
        both = restore_asset(
            session,
            asset_hash=asset_hash,
            artifactclass="s-masters",
            destination=tmp_path / "both.mov",
            backends={backend_id: backend},
            force_suspect=True,
            force_rejected=True,
        )
        assert both.output_path.read_bytes() == b"reject me"
        unreject_asset(session, asset_hash)
        assert len(list_view(session, view)) == 1


def test_multiclass_add_requires_artifactclass_and_restores_that_class(
    engine: Engine,
    tmp_path: Path,
) -> None:
    masters_backend = _ArchiveBackend("masters")
    proxy_backend = _ArchiveBackend("proxy")
    source = tmp_path / "shared.mov"
    source.write_bytes(b"same bytes")
    with session_scope(engine) as session:
        masters_backend_id = _install_policy(session, "s-masters")
        proxy_backend_id = _install_policy(session, "s-proxy")
        asset_hash = _archive_asset(
            session, "s-masters", source, masters_backend_id, masters_backend
        )
        _archive_asset(session, "s-proxy", source, proxy_backend_id, proxy_backend)
        view = create_view(session, "mixed", created_by="operator")

        with pytest.raises(VirtualArrangementAmbiguousClass):
            add_member(session, view, asset_hash, "shared.mov", added_by="operator")

        member = add_member(
            session,
            view,
            asset_hash,
            "shared.mov",
            artifactclass="s-proxy",
            added_by="operator",
        )
        assert member.artifactclass == "s-proxy"
        resolved_hash, artifactclass = resolve(session, view, "shared.mov")
        result = restore_asset(
            session,
            asset_hash=resolved_hash,
            artifactclass=artifactclass,
            destination=tmp_path / "restored-proxy.mov",
            backends={masters_backend_id: masters_backend, proxy_backend_id: proxy_backend},
        )
        assert result.pool_id == "s-proxy-pool"
        assert result.output_path.read_bytes() == b"same bytes"


def test_tags_soft_delete_and_add_requires_archived(engine: Engine, tmp_path: Path) -> None:
    backend = _ArchiveBackend("masters")
    source = tmp_path / "tagged.mov"
    source.write_bytes(b"tagged")
    orphan_hash = hashlib.sha256(b"not archived").digest()
    with session_scope(engine) as session:
        backend_id = _install_policy(session, "s-masters")
        asset_hash = _archive_asset(session, "s-masters", source, backend_id, backend)
        session.add(LogicalAsset(content_sha256=orphan_hash, size_bytes=len(b"not archived")))
        view = create_view(session, "tags", created_by="operator")

        tag_1 = add_tag(session, asset_hash, "speaker:ada", actor="operator")
        remove_tag(session, asset_hash, "speaker:ada", actor="operator")
        tag_2 = add_tag(session, asset_hash, "speaker:ada", actor="operator")
        rows = list(
            session.scalars(
                select(AssetTag)
                .where(AssetTag.logical_asset_hash == asset_hash)
                .order_by(AssetTag.id)
            )
        )
        assert [row.id for row in rows] == [tag_1.id, tag_2.id]
        assert rows[0].removed_at is not None
        assert rows[1].removed_at is None

        with pytest.raises(VirtualArrangementNotArchived):
            add_member(session, view, orphan_hash, "missing.mov", added_by="operator")


def _install_policy(session: Session, artifactclass: str) -> int:
    backend = Backend(
        name=f"{artifactclass}-backend",
        kind=BackendKind.MEMORY,
        tier=BackendTier.SELF_DESCRIBING,
    )
    session.add(backend)
    session.flush()
    pool_id = f"{artifactclass}-pool"
    session.add(
        Pool(
            id=pool_id,
            backend_id=backend.id,
            representation=Representation.RAO_PLAIN_V1.value,
        )
    )
    session.flush()
    apply_artifactclass_policy(
        session,
        artifactclass,
        ArtifactClassPolicy(
            ruleset=f"{artifactclass}.rules",
            placements=(PlacementPolicy(pool_id, role="working"),),
            bundling=BundlingPolicy(target_gb=1, max_age_seconds=60),
            restore_preference=(pool_id,),
            expect="compliant",
            durability=DurabilityPolicy(min_copies=1, min_impl_families=1),
        ),
    )
    return backend.id


def _archive_asset(
    session: Session,
    artifactclass: str,
    source: Path,
    backend_id: int,
    backend: _ArchiveBackend,
) -> bytes:
    asset_hash = hashlib.sha256(source.read_bytes()).digest()
    session.merge(LogicalAsset(content_sha256=asset_hash, size_bytes=source.stat().st_size))
    policy = get_artifactclass_policy(session, artifactclass)
    bundle, _, _ = enqueue_artifact(
        session,
        artifactclass=artifactclass,
        policy=policy,
        logical_asset_hash=asset_hash,
        source_path=source,
        member_path=source.name,
    )
    flush_bundle(
        session,
        bundle_id=bundle.id,
        backends={backend_id: backend},
        builder=LocalArchiveBuilder(),
    )
    assert (
        session.scalar(
            select(func.count())
            .select_from(AssetLocator)
            .where(AssetLocator.logical_asset_hash == asset_hash)
        )
        is not None
    )
    return asset_hash


def _add_ingest_occurrence(
    session: Session,
    asset_hash: bytes,
    intake_id: str,
    path: str,
) -> IngestItem:
    session.add(
        Intake(
            intake_id=intake_id,
            operator="operator",
            source_kind=IntakeSourceKind.CARD,
            artifactclass="s-masters",
            status=IntakeStatus.REGISTERED,
        )
    )
    item = IngestItem(
        intake_id=intake_id,
        logical_asset_hash=asset_hash,
        as_received_path=path,
        virtual_path=path,
        size_bytes=0,
        artifactclass="s-masters",
        item_metadata={},
    )
    session.add(item)
    session.flush()
    return item


def _asset_copy_for_restore_copy(
    session: Session,
    asset_hash: bytes,
    payload: bytes,
) -> tuple[int, MemoryBackend, int]:
    backend = Backend(
        name="restore-copy-memory",
        kind=BackendKind.MEMORY,
        tier=BackendTier.SELF_DESCRIBING,
    )
    session.add(backend)
    session.flush()
    memory = MemoryBackend("restore-copy-memory")
    stored_hash = memory.add(payload)
    copy, _ = add_copy(
        session,
        logical_asset_hash=asset_hash,
        backend_id=backend.id,
        native_locator={"hash_hex": stored_hash.hex()},
        integrity_hash=stored_hash,
        source=CopySource.INGEST,
        health=CopyHealth.OK,
        storage_metadata={"representation": Representation.RAW_BYTES.value},
    )
    return backend.id, memory, copy.id
