"""Hdcache M4 restore seam, gate, request, and cache-serve tests."""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner
from sqlalchemy import Engine, select

from sutradhara.api.identity import parse_identity
from sutradhara.archive_restore import RestoreRejectedAsset, RestoreSuspectAsset
from sutradhara.backend.memory import MemoryBackend
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    ArtifactClassPool,
    AssetLocator,
    Backend,
    Bundle,
    BundleMember,
    Copy,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, locator_key, make_engine, session_scope
from sutradhara.catalog.types import (
    AssetValidity,
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
)
from sutradhara.cli.archive import archive_group
from sutradhara.hdcache.manager import (
    ITEM_DENIED,
    ITEM_DONE,
    ITEM_QUEUED,
    REQUEST_COMPLETED_WITH_ERRORS,
    REQUEST_PENDING,
    InvalidRestoreDestination,
    RestoreConfig,
    RestoreDenied,
    RestoreDestination,
    RestoreEvent,
    RestoreItemSpec,
    UnknownRestoreDestination,
    admit_restore_request,
    canonicalize_restore_destination,
    destination_for_request_item,
    resolve_read_source,
    restore_to_path,
    serve_restore_item,
    serve_restore_request,
)
from sutradhara.hdcache.models import CacheDisk, CacheEntry, RestoreRequest, RestoreRequestItem
from sutradhara.hdcache.store import RAW_REPRESENTATION, SENTINEL_NAME, write_entry
from sutradhara.sealing.port import Representation


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = make_engine(f"sqlite:///{tmp_path / 'hdcache-m4.db'}")
    create_all(eng)
    yield eng
    eng.dispose()


def test_identity_restore_grants_are_additive_and_admin_not_implicit() -> None:
    p3_operator = parse_identity(
        {
            "X-Authentik-Username": "owner",
            "X-Authentik-Groups": "sutradhara-operator|sutradhara-restore-p3",
        }
    )
    assert p3_operator.role == "operator"
    assert p3_operator.capabilities == (
        "can_view",
        "can_receive",
        "can_restore_p2",
        "can_restore_p3",
    )

    admin = parse_identity(
        {
            "X-Authentik-Username": "owner",
            "X-Authentik-Groups": "sutradhara-admin",
        }
    )
    assert admin.capabilities == ("can_view", "can_receive", "can_admin")


@pytest.mark.parametrize(
    ("privacy", "groups", "allowed", "detail"),
    [
        ("p2", "sutradhara-operator", False, "requires sutradhara-restore-p2"),
        (
            "p2",
            "sutradhara-operator|sutradhara-restore-p2",
            True,
            None,
        ),
        (
            "p2",
            "sutradhara-operator|sutradhara-restore-p3",
            True,
            None,
        ),
        (
            "p3",
            "sutradhara-operator|sutradhara-restore-p2",
            False,
            "requires sutradhara-restore-p3",
        ),
        (
            "p3",
            "sutradhara-operator|sutradhara-restore-p3",
            True,
            None,
        ),
        ("p3", "sutradhara-admin", False, "requires sutradhara-restore-p3"),
    ],
)
def test_privacy_gate_matrix(
    engine: Engine,
    tmp_path: Path,
    privacy: str,
    groups: str,
    allowed: bool,
    detail: str | None,
) -> None:
    data = b"private clip"
    identity = _identity(groups)
    with session_scope(engine) as session:
        digest, _backend_id, _memory = _seed_archived_asset(
            session,
            data=data,
            artifactclass="private",
            privacy=privacy,
        )
        if allowed:
            plan = resolve_read_source(
                session,
                asset_hash=digest,
                artifactclass="private",
                destination=tmp_path / "out.mov",
                identity_or_override=identity,
            )
            assert plan.source == "tape"
        else:
            with pytest.raises(RestoreDenied) as excinfo:
                resolve_read_source(
                    session,
                    asset_hash=digest,
                    artifactclass="private",
                    destination=tmp_path / "out.mov",
                    identity_or_override=identity,
                )
            assert excinfo.value.detail == detail


def test_unmapped_privacy_denies_and_emits_alarm(engine: Engine, tmp_path: Path) -> None:
    events: list[RestoreEvent] = []
    config = RestoreConfig(privacy_capability_map={}, event_sink=events.append)
    with session_scope(engine) as session:
        digest, _backend_id, _memory = _seed_archived_asset(
            session,
            data=b"p4 clip",
            artifactclass="private",
            privacy="p4",
        )
        with pytest.raises(RestoreDenied) as excinfo:
            resolve_read_source(
                session,
                asset_hash=digest,
                artifactclass="private",
                destination=tmp_path / "out.mov",
                identity_or_override=_identity("sutradhara-operator|sutradhara-restore-p3"),
                config=config,
            )
    assert excinfo.value.detail == "privacy level p4 unmapped (config error)"
    assert [event.code for event in events] == ["privacy-unmapped"]


def test_strictest_privacy_wins_across_classes(engine: Engine, tmp_path: Path) -> None:
    with session_scope(engine) as session:
        digest, _backend_id, _memory = _seed_archived_asset(
            session,
            data=b"shared bytes",
            artifactclass="public",
            privacy="none",
        )
        _add_class_membership(session, digest, "p3-class", privacy="p3")

        with pytest.raises(RestoreDenied) as excinfo:
            resolve_read_source(
                session,
                asset_hash=digest,
                artifactclass="public",
                destination=tmp_path / "out.mov",
                identity_or_override=_identity("sutradhara-operator|sutradhara-restore-p2"),
            )
    assert excinfo.value.detail == "requires sutradhara-restore-p3"


@pytest.mark.parametrize(
    ("state", "force_kwargs", "error_type"),
    [
        ("suspect", {"force_suspect": True}, RestoreSuspectAsset),
        ("rejected", {"force_rejected": True}, RestoreRejectedAsset),
    ],
)
def test_validity_gate_applies_to_cache_and_tape_branches(
    engine: Engine,
    tmp_path: Path,
    state: str,
    force_kwargs: dict[str, bool],
    error_type: type[Exception],
) -> None:
    data = f"{state} bytes".encode()
    with session_scope(engine) as session:
        digest, backend_id, memory = _seed_archived_asset(session, data=data)
        asset = session.get(LogicalAsset, digest)
        assert asset is not None
        if state == "suspect":
            asset.validity = AssetValidity.SUSPECT
            asset.validity_note = "decode warning"
        else:
            asset.rejected_at = dt.datetime.now(dt.UTC)
            asset.rejected_by = "operator"
            asset.rejection_reason = "bad take"

        with pytest.raises(error_type):
            resolve_read_source(
                session,
                asset_hash=digest,
                artifactclass="s-masters",
                destination=tmp_path / "tape.mov",
                identity_or_override=_identity("sutradhara-operator"),
            )
        tape_plan = resolve_read_source(
            session,
            asset_hash=digest,
            artifactclass="s-masters",
            destination=tmp_path / "tape.mov",
            identity_or_override=_identity("sutradhara-operator"),
            **force_kwargs,
        )
        assert tape_plan.source == "tape"

        _seed_cache_entry(session, tmp_path / "d001", digest, data)
        with pytest.raises(error_type):
            resolve_read_source(
                session,
                asset_hash=digest,
                artifactclass="s-masters",
                destination=tmp_path / "cache.mov",
                identity_or_override=_identity("sutradhara-operator"),
            )
        restored = restore_to_path(
            session,
            asset_hash=digest,
            artifactclass="s-masters",
            destination=tmp_path / "cache.mov",
            identity_or_override=_identity("sutradhara-operator"),
            backends={backend_id: memory},
            **force_kwargs,
        )
        assert restored.source == "cache"
        assert (tmp_path / "cache.mov").read_bytes() == data


def test_destination_confinement_rejects_escape_overwrite_and_unknown_id(
    engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "export"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "link").symlink_to(outside, target_is_directory=True)

    assert canonicalize_restore_destination("clip.mov", root=root) == root / "clip.mov"
    with pytest.raises(InvalidRestoreDestination, match="traversal"):
        canonicalize_restore_destination("../escape.mov", root=root)
    with pytest.raises(InvalidRestoreDestination, match="escapes"):
        canonicalize_restore_destination("link/escape.mov", root=root)
    (root / "exists.mov").write_bytes(b"old")
    with pytest.raises(InvalidRestoreDestination, match="already exists"):
        canonicalize_restore_destination("exists.mov", root=root)

    with session_scope(engine) as session:
        digest, _backend_id, _memory = _seed_archived_asset(session, data=b"clip")
        request = RestoreRequest(id="r1", identity="owner", destination_id="unknown")
        request.items.append(
            RestoreRequestItem(
                content_sha256=digest,
                artifactclass="s-masters",
                state=ITEM_QUEUED,
            )
        )
        session.add(request)
        session.flush()
        with pytest.raises(UnknownRestoreDestination):
            destination_for_request_item(RestoreConfig(destinations={}), "unknown", request.items[0])


def test_request_admission_and_sequential_serve_persist_contract_states(
    engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    config = _config(root)
    with session_scope(engine) as session:
        public_digest, backend_id, memory = _seed_archived_asset(session, data=b"public bytes")
        private_digest, _backend_id, _memory = _seed_archived_asset(
            session,
            data=b"private",
            artifactclass="private",
            privacy="p3",
        )
        config = _config(root, restore_backends={backend_id: memory})
        request = admit_restore_request(
            session,
            identity=_identity("sutradhara-operator"),
            destination_id="media-server",
            items=[
                RestoreItemSpec(public_digest, "s-masters"),
                RestoreItemSpec(private_digest, "private"),
            ],
            config=config,
        )
        assert request.state != REQUEST_PENDING
        states = {item.content_sha256: item.state for item in request.items}
        assert states[public_digest] == ITEM_QUEUED
        assert states[private_digest] == ITEM_DENIED
        denied = next(item for item in request.items if item.content_sha256 == private_digest)
        assert denied.detail == "requires sutradhara-restore-p3"

        serve_restore_request(
            session,
            request,
            identity_or_override=_identity("sutradhara-operator"),
            config=config,
        )
        session.flush()
        assert request.state == REQUEST_COMPLETED_WITH_ERRORS
        assert (root / public_digest.hex()).read_bytes() == b"public bytes"
        assert {item.content_sha256: item.state for item in request.items} == {
            public_digest: ITEM_DONE,
            private_digest: ITEM_DENIED,
        }


def test_untrusted_cache_hit_promotes_after_verified_serve(
    engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    with session_scope(engine) as session:
        digest, _backend_id, _memory = _seed_archived_asset(session, data=b"cache hit")
        entry = _seed_cache_entry(session, tmp_path / "d001", digest, b"cache hit", trusted=False)
        request, item = _request_item(session, digest, root)
        result = serve_restore_item(
            session,
            item,
            identity_or_override=_identity("sutradhara-operator"),
            config=_config(root),
        )
        assert result.source == "cache"
        assert (root / digest.hex()).read_bytes() == b"cache hit"
        assert session.get(CacheEntry, digest).trusted is True
        assert item.state == ITEM_DONE
        assert request.state in {"pending", "active"}
        assert entry.last_read_at is not None


@pytest.mark.parametrize("file_present", [True, False])
def test_cache_failure_falls_back_to_tape_with_audit_and_lost_marking(
    engine: Engine,
    tmp_path: Path,
    file_present: bool,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    events: list[RestoreEvent] = []
    with session_scope(engine) as session:
        digest, backend_id, memory = _seed_archived_asset(session, data=b"tape truth")
        entry = _seed_cache_entry(session, tmp_path / "d001", digest, b"tape truth")
        path = tmp_path / "d001" / "hdcache" / "v1" / digest.hex()[:2] / digest.hex()
        if file_present:
            path.write_bytes(b"corrupt")
        else:
            path.unlink()
        _request, item = _request_item(session, digest, root)
        result = serve_restore_item(
            session,
            item,
            identity_or_override=_identity("sutradhara-operator"),
            config=_config(root, restore_backends={backend_id: memory}, events=events),
        )
        assert result.source == "tape"
        assert (root / digest.hex()).read_bytes() == b"tape truth"
        assert item.state == ITEM_DONE
        assert session.get(CacheEntry, digest).state == "lost"
        assert session.get(CacheDisk, entry.disk_id).filled_bytes == 0
    assert [event.code for event in events] == ["cache-fallback:read-failed"]


def test_absent_disk_falls_back_without_lost_marking_or_accounting_release(
    engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    missing_mount = tmp_path / "missing-disk"
    with session_scope(engine) as session:
        digest, backend_id, memory = _seed_archived_asset(session, data=b"tape truth")
        session.add(
            CacheDisk(
                disk_id="d001",
                serial="SER001",
                fs_uuid="fs-001",
                mount=str(missing_mount),
                state="active",
                capacity_bytes=1000,
                filled_bytes=len(b"stale"),
            )
        )
        session.add(
            CacheEntry(
                content_sha256=digest,
                artifactclass="s-masters",
                disk_id="d001",
                relpath=f"{digest.hex()[:2]}/{digest.hex()}",
                size_bytes=len(b"stale"),
                state="present",
                representation=RAW_REPRESENTATION,
                trusted=True,
            )
        )
        _request, item = _request_item(session, digest, root)
        result = serve_restore_item(
            session,
            item,
            identity_or_override=_identity("sutradhara-operator"),
            config=_config(root, restore_backends={backend_id: memory}),
        )
        assert result.source == "tape"
        assert session.get(CacheDisk, "d001").state == "absent"
        assert session.get(CacheDisk, "d001").filled_bytes == len(b"stale")
        assert session.get(CacheEntry, digest).state == "present"


def test_gated_request_item_tape_serve_uses_admission_validity(
    engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    with session_scope(engine) as session:
        digest, backend_id, memory = _seed_archived_asset(session, data=b"suspect tape bytes")
        asset = session.get(LogicalAsset, digest)
        assert asset is not None
        asset.validity = AssetValidity.SUSPECT
        asset.validity_note = "forced at admission"
        _request, item = _request_item(session, digest, root)

        result = serve_restore_item(
            session,
            item,
            gates_already_admitted=True,
            config=_config(root, restore_backends={backend_id: memory}),
        )

        assert result.source == "tape"
        assert item.state == ITEM_DONE
        assert (root / digest.hex()).read_bytes() == b"suspect tape bytes"


def test_archive_cli_private_assets_fail_closed_and_override_audits(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    runner = CliRunner()
    with session_scope(engine) as session:
        digest, backend_id, memory = _seed_archived_asset(
            session,
            data=b"private bytes",
            artifactclass="private",
            privacy="p2",
        )
    monkeypatch.setattr("sutradhara.cli.archive.make_engine", lambda: engine)
    monkeypatch.setattr(
        "sutradhara.cli.archive._restore_backends",
        lambda _session, _artifactclass, _pool_ids: {backend_id: memory},
    )

    denied = runner.invoke(
        archive_group,
        [
            "restore",
            digest.hex(),
            "--artifactclass",
            "private",
            "--dest",
            str(tmp_path / "denied.mov"),
        ],
    )
    assert denied.exit_code != 0
    assert "requires sutradhara-restore-p2" in denied.output
    assert not (tmp_path / "denied.mov").exists()

    with caplog.at_level("WARNING", logger="sutradhara.hdcache.manager"):
        allowed = runner.invoke(
            archive_group,
            [
                "restore",
                digest.hex(),
                "--artifactclass",
                "private",
                "--dest",
                str(tmp_path / "allowed.mov"),
                "--privacy-override",
                "supervised export",
            ],
        )
    assert allowed.exit_code == 0, allowed.output
    assert (tmp_path / "allowed.mov").read_bytes() == b"private bytes"
    assert "privacy-override" in caplog.text


def _identity(groups: str):
    return parse_identity(
        {
            "X-Authentik-Username": "owner",
            "X-Authentik-Name": "Ada Operator",
            "X-Authentik-Groups": groups,
        }
    )


def _config(
    root: Path,
    *,
    restore_backends: dict[int, MemoryBackend] | None = None,
    events: list[RestoreEvent] | None = None,
) -> RestoreConfig:
    return RestoreConfig(
        destinations={
            "media-server": RestoreDestination(
                id="media-server",
                root=root,
                label="Media server /restore",
            )
        },
        restore_backends=restore_backends,
        event_sink=None if events is None else events.append,
    )


def _seed_archived_asset(
    session,
    *,
    data: bytes,
    artifactclass: str = "s-masters",
    privacy: str = "none",
) -> tuple[bytes, int, MemoryBackend]:
    digest = hashlib.sha256(data).digest()
    session.merge(LogicalAsset(content_sha256=digest, size_bytes=len(data)))
    backend = session.scalar(select(Backend).where(Backend.name == "mem"))
    if backend is None:
        backend = Backend(name="mem", kind=BackendKind.MEMORY, tier=BackendTier.SELF_DESCRIBING)
        session.add(backend)
        session.flush()
    memory = MemoryBackend("mem")
    memory.add(data)
    pool = session.get(Pool, "mem-pool")
    if pool is None:
        pool = Pool(
            id="mem-pool",
            backend_id=backend.id,
            representation=Representation.RAW_BYTES.value,
        )
        session.add(pool)
    _add_policy(session, artifactclass, privacy=privacy)
    bundle_id = f"bundle-{artifactclass}-{digest.hex()[:12]}"
    if session.get(Bundle, bundle_id) is None:
        session.add(
            Bundle(
                id=bundle_id,
                artifactclass=artifactclass,
                status="sealed",
                target_bytes=1024,
                max_age_seconds=3600,
            )
        )
        session.add(
            BundleMember(
                bundle_id=bundle_id,
                logical_asset_hash=digest,
                member_path=f"{digest.hex()}.mov",
                size_bytes=len(data),
                file_sha256=digest,
            )
        )
    locator = {"hash_hex": digest.hex()}
    copy = Copy(
        bundle_id=bundle_id,
        backend_id=backend.id,
        pool_id="mem-pool",
        native_locator=locator,
        native_locator_key=locator_key(locator),
        storage_metadata={"representation": Representation.RAW_BYTES.value},
        integrity_hash=digest,
        health=CopyHealth.OK,
        source=CopySource.INGEST,
    )
    session.add(copy)
    session.flush()
    session.add(
        AssetLocator(
            logical_asset_hash=digest,
            pool_id="mem-pool",
            copy_id=copy.id,
            bundle_id=bundle_id,
            native_locator={
                "member_path": f"{digest.hex()}.mov",
                "offset": 0,
                "size_bytes": len(data),
            },
            member_path=f"{digest.hex()}.mov",
            representation=Representation.RAW_BYTES.value,
        )
    )
    session.flush()
    return digest, backend.id, memory


def _add_policy(session, artifactclass: str, *, privacy: str) -> None:
    session.merge(
        ArtifactClassPolicyRecord(
            artifactclass=artifactclass,
            ruleset="test.rules",
            expect="messy",
            target_bytes=1024,
            max_age_seconds=3600,
            restore_preference=["mem-pool"],
            staging_config={},
            hdcache_config={"enabled": True, "privacy_level": privacy},
        )
    )
    if (
        session.scalar(
            select(ArtifactClassPool).where(
                ArtifactClassPool.artifactclass == artifactclass,
                ArtifactClassPool.pool_id == "mem-pool",
            )
        )
        is None
    ):
        session.add(
            ArtifactClassPool(
                artifactclass=artifactclass,
                pool_id="mem-pool",
                active=True,
                sort_order=0,
            )
        )
    session.flush()


def _add_class_membership(
    session,
    digest: bytes,
    artifactclass: str,
    *,
    privacy: str,
) -> None:
    _add_policy(session, artifactclass, privacy=privacy)
    bundle_id = f"bundle-{artifactclass}-{digest.hex()[:12]}"
    session.add(
        Bundle(
            id=bundle_id,
            artifactclass=artifactclass,
            status="sealed",
            target_bytes=1024,
            max_age_seconds=3600,
        )
    )
    session.add(
        BundleMember(
            bundle_id=bundle_id,
            logical_asset_hash=digest,
            member_path=f"{digest.hex()}.mov",
            size_bytes=0,
            file_sha256=digest,
        )
    )
    session.flush()


def _seed_cache_entry(
    session,
    mount: Path,
    digest: bytes,
    data: bytes,
    *,
    trusted: bool = True,
) -> CacheEntry:
    mount.mkdir(parents=True, exist_ok=True)
    (mount / SENTINEL_NAME).write_text("{}", encoding="utf-8")
    disk = session.get(CacheDisk, "d001")
    if disk is None:
        disk = CacheDisk(
            disk_id="d001",
            serial="SER001",
            fs_uuid="fs-001",
            mount=str(mount),
            state="active",
            capacity_bytes=1000,
            filled_bytes=0,
        )
        session.add(disk)
        session.flush()
    result = write_entry(mount, digest, data, representation=RAW_REPRESENTATION)
    entry = session.get(CacheEntry, digest)
    if entry is None:
        entry = CacheEntry(
            content_sha256=digest,
            artifactclass="s-masters",
            disk_id=disk.disk_id,
            relpath=result.relpath,
            size_bytes=result.size_bytes,
            state="present",
            representation=RAW_REPRESENTATION,
            trusted=trusted,
        )
        session.add(entry)
    else:
        entry.disk_id = disk.disk_id
        entry.relpath = result.relpath
        entry.size_bytes = result.size_bytes
        entry.state = "present"
        entry.representation = RAW_REPRESENTATION
        entry.trusted = trusted
    disk.filled_bytes = result.size_bytes
    session.flush([disk, entry])
    return entry


def _request_item(
    session,
    digest: bytes,
    root: Path,
) -> tuple[RestoreRequest, RestoreRequestItem]:
    request = RestoreRequest(
        id=f"r-{digest.hex()[:8]}",
        identity="owner",
        destination_id="media-server",
        state="active",
    )
    item = RestoreRequestItem(
        content_sha256=digest,
        artifactclass="s-masters",
        state=ITEM_QUEUED,
    )
    request.items.append(item)
    session.add(request)
    session.flush()
    return request, item
