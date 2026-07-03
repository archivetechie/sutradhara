"""Hdcache M4 restore seam, gate, request, and cache-serve tests."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner
from sqlalchemy import Engine, select

import sutradhara.hdcache.manager as restore_manager
from sutradhara.api.identity import parse_identity
from sutradhara.archive_restore import RestoreRejectedAsset, RestoreResult, RestoreSuspectAsset
from sutradhara.artifactclass_policy import ArtifactClassPolicyError
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
from sutradhara.catalog.session import (
    create_all,
    locator_key,
    make_engine,
    make_session_factory,
    session_scope,
)
from sutradhara.catalog.types import (
    AssetValidity,
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
)
from sutradhara.cli.archive import archive_group
from sutradhara.hdcache.manager import (
    DiskCircuitBreaker,
    ITEM_DENIED,
    ITEM_DONE,
    ITEM_QUEUED,
    ITEM_STREAMING,
    ITEM_WAKING_DISK,
    REQUEST_ACTIVE,
    REQUEST_COMPLETED,
    REQUEST_COMPLETED_WITH_ERRORS,
    REQUEST_PENDING,
    RESTORE_DESTINATIONS_ENV,
    InvalidRestoreDestination,
    RestoreAdmissionInvalid,
    RestoreConfig,
    RestoreDenied,
    RestoreDestination,
    RestoreEvent,
    RestoreItemSpec,
    UnknownRestoreDestination,
    admit_restore_request,
    canonicalize_restore_destination,
    configured_destinations,
    destination_for_request_item,
    resolve_read_source,
    restore_to_path,
    serve_restore_item,
    serve_restore_request,
)
from sutradhara.hdcache.models import CacheDisk, CacheEntry, RestoreRequest, RestoreRequestItem
from sutradhara.hdcache.store import (
    AEAD_REPRESENTATION,
    RAW_REPRESENTATION,
    SENTINEL_NAME,
    ExpectedDiskIdentity,
    ObservedBlockIdentity,
    entry_path,
    write_disk_sentinel,
    write_entry,
)
from sutradhara.sealing.port import Representation

TEST_HDCACHE_HMAC_SECRET = b"restore-manager-test-secret"


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
            config=_config(tmp_path),
            **force_kwargs,
        )
        assert restored.source == "cache"
        assert (tmp_path / "cache.mov").read_bytes() == data


@pytest.mark.parametrize(
    ("state", "flag_text", "note"),
    [
        ("suspect", "--force", "decode warning"),
        ("rejected", "--force-rejected", "bad take"),
    ],
)
def test_admission_validity_denial_details_are_api_safe(
    engine: Engine,
    tmp_path: Path,
    state: str,
    flag_text: str,
    note: str,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    with session_scope(engine) as session:
        digest, _backend_id, _memory = _seed_archived_asset(session, data=f"{state} bytes".encode())
        asset = session.get(LogicalAsset, digest)
        assert asset is not None
        if state == "suspect":
            asset.validity = AssetValidity.SUSPECT
            asset.validity_note = note
        else:
            asset.rejected_at = dt.datetime.now(dt.UTC)
            asset.rejected_by = "operator"
            asset.rejection_reason = note

        request = admit_restore_request(
            session,
            identity=_identity("sutradhara-operator"),
            destination_id="media-server",
            items=[RestoreItemSpec(digest, "s-masters")],
            config=_config(root),
        )

        item = request.items[0]
        assert item.state == ITEM_DENIED
        assert item.detail is not None
        assert note in item.detail
        assert flag_text not in item.detail
        assert "restore anyway" not in item.detail


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


def test_configured_destinations_do_not_expose_raw_root_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_root = tmp_path / "first-root"
    second_root = tmp_path / "second-root"
    monkeypatch.setenv(
        RESTORE_DESTINATIONS_ENV,
        json.dumps(
            {
                "media-server": str(first_root),
                "review-station": {"root": str(second_root), "writable": True},
            }
        ),
    )

    payload = configured_destinations()

    assert payload == [
        {"id": "media-server", "label": "media-server", "writable": True},
        {"id": "review-station", "label": "review-station", "writable": True},
    ]
    encoded = json.dumps(payload)
    assert str(first_root) not in encoded
    assert str(second_root) not in encoded


@pytest.mark.parametrize("destination_id", ["media/server", r"media\server", "C:restore"])
def test_path_like_destination_id_is_rejected_at_config_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    destination_id: str,
) -> None:
    monkeypatch.setenv(
        RESTORE_DESTINATIONS_ENV,
        json.dumps({destination_id: str(tmp_path / "restore-root")}),
    )

    with pytest.raises(ArtifactClassPolicyError, match="opaque"):
        configured_destinations()


def test_path_like_destination_label_is_rejected_at_config_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "restore-root"
    monkeypatch.setenv(
        RESTORE_DESTINATIONS_ENV,
        json.dumps({"media-server": {"root": str(root), "label": "exports/restore"}}),
    )

    with pytest.raises(ArtifactClassPolicyError, match="label must not be a raw path"):
        configured_destinations()


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
        assert request.admitted_by == "owner"
        assert request.admitted_at is not None
        assert request.admitted_capabilities == ["can_view", "can_receive"]
        assert request.state == REQUEST_PENDING
        states = {item.content_sha256: item.state for item in request.items}
        assert states[public_digest] == ITEM_QUEUED
        assert states[private_digest] == ITEM_DENIED
        denied = next(item for item in request.items if item.content_sha256 == private_digest)
        assert denied.detail == "requires sutradhara-restore-p3"
        admitted = next(item for item in request.items if item.content_sha256 == public_digest)
        assert admitted.admitted_force_suspect is False
        assert admitted.admitted_force_rejected is False

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


def test_forged_queued_row_without_admission_inputs_is_refused(
    engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    with session_scope(engine) as session:
        digest, _backend_id, _memory = _seed_archived_asset(session, data=b"public bytes")
        request = RestoreRequest(
            id="forged",
            identity="owner",
            destination_id="media-server",
            state=REQUEST_PENDING,
        )
        request.items.append(
            RestoreRequestItem(
                content_sha256=digest,
                artifactclass="s-masters",
                state=ITEM_QUEUED,
            )
        )
        session.add(request)
        session.flush()
        item = request.items[0]

        with pytest.raises(RestoreAdmissionInvalid, match="missing admission inputs"):
            serve_restore_item(
                session,
                item,
                identity_or_override=_identity("sutradhara-operator"),
                force_suspect=True,
                force_rejected=True,
                config=_config(root),
            )

        assert item.state == "failed"
        assert item.detail is not None
        assert "missing admission inputs" in item.detail


def test_admitted_force_flags_are_used_at_serve_when_caller_differs(
    engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    with session_scope(engine) as session:
        digest, backend_id, memory = _seed_archived_asset(session, data=b"suspect bytes")
        asset = session.get(LogicalAsset, digest)
        assert asset is not None
        asset.validity = AssetValidity.SUSPECT
        asset.validity_note = "operator accepted warning"
        request = admit_restore_request(
            session,
            identity=_identity("sutradhara-operator"),
            destination_id="media-server",
            items=[RestoreItemSpec(digest, "s-masters")],
            force_suspect=True,
            config=_config(root),
        )

        serve_restore_request(
            session,
            request,
            identity_or_override=_identity("sutradhara-operator"),
            force_suspect=False,
            force_rejected=True,
            config=_config(root, restore_backends={backend_id: memory}),
        )

        item = request.items[0]
        assert item.admitted_force_suspect is True
        assert item.admitted_force_rejected is False
        assert item.state == ITEM_DONE
        assert (root / digest.hex()).read_bytes() == b"suspect bytes"


def test_force_flags_requested_while_asset_ok_do_not_waive_later_rejection(
    engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    with session_scope(engine) as session:
        digest, backend_id, memory = _seed_archived_asset(session, data=b"later rejected")
        request = admit_restore_request(
            session,
            identity=_identity("sutradhara-operator"),
            destination_id="media-server",
            items=[RestoreItemSpec(digest, "s-masters")],
            force_suspect=True,
            force_rejected=True,
            config=_config(root),
        )
        item = request.items[0]
        assert item.admitted_force_suspect is False
        assert item.admitted_force_rejected is False
        asset = session.get(LogicalAsset, digest)
        assert asset is not None
        asset.rejected_at = dt.datetime.now(dt.UTC)
        asset.rejected_by = "supervisor"
        asset.rejection_reason = "post-admission revoke"

        serve_restore_request(
            session,
            request,
            identity_or_override=_identity("sutradhara-operator"),
            force_rejected=True,
            config=_config(root, restore_backends={backend_id: memory}),
        )

        assert item.state == ITEM_DENIED
        assert item.detail is not None
        assert "post-admission revoke" in item.detail
        assert not (root / digest.hex()).exists()
        assert request.state == REQUEST_COMPLETED_WITH_ERRORS


def test_suspect_waiver_at_admission_does_not_waive_later_rejection(
    engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    with session_scope(engine) as session:
        digest, backend_id, memory = _seed_archived_asset(session, data=b"suspect then rejected")
        asset = session.get(LogicalAsset, digest)
        assert asset is not None
        asset.validity = AssetValidity.SUSPECT
        asset.validity_note = "decode warning"
        request = admit_restore_request(
            session,
            identity=_identity("sutradhara-operator"),
            destination_id="media-server",
            items=[RestoreItemSpec(digest, "s-masters")],
            force_suspect=True,
            config=_config(root),
        )
        item = request.items[0]
        assert item.admitted_force_suspect is True
        assert item.admitted_force_rejected is False

        asset.rejected_at = dt.datetime.now(dt.UTC)
        asset.rejected_by = "supervisor"
        asset.rejection_reason = "post-admission reject"

        serve_restore_request(
            session,
            request,
            identity_or_override=_identity("sutradhara-operator"),
            config=_config(root, restore_backends={backend_id: memory}),
        )

        assert item.state == ITEM_DENIED
        assert item.detail is not None
        assert "post-admission reject" in item.detail
        assert not (root / digest.hex()).exists()
        assert request.state == REQUEST_COMPLETED_WITH_ERRORS


def test_request_state_is_active_only_while_item_is_serving(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    observed: list[tuple[str, str]] = []
    with session_scope(engine) as session:
        digest, _backend_id, _memory = _seed_archived_asset(session, data=b"public bytes")
        config = _config(root)
        request = admit_restore_request(
            session,
            identity=_identity("sutradhara-operator"),
            destination_id="media-server",
            items=[RestoreItemSpec(digest, "s-masters")],
            config=config,
        )
        item = request.items[0]
        assert request.state == REQUEST_PENDING

        def fake_serve_from_tape(
            _session,
            asset_hash,
            _artifactclass,
            destination,
            _config,
            *,
            force_suspect,
            force_rejected,
        ):
            assert force_suspect is False
            assert force_rejected is False
            observed.append((request.state, item.state))
            destination.write_bytes(b"public bytes")
            return RestoreResult(
                asset_hash=asset_hash,
                pool_id="mem-pool",
                copy_id=1,
                output_path=destination,
                size_bytes=len(b"public bytes"),
            )

        monkeypatch.setattr(restore_manager, "_serve_from_tape", fake_serve_from_tape)

        serve_restore_request(
            session,
            request,
            identity_or_override=_identity("sutradhara-operator"),
            config=config,
        )

        assert observed == [(REQUEST_ACTIVE, ITEM_STREAMING)]
        assert request.state == REQUEST_COMPLETED
        assert item.state == ITEM_DONE


def test_wake_window_marks_only_rolling_cache_items(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    observed: list[list[str]] = []

    def fake_cache(_session, _entry, destination, _config):
        observed.append([item.state for item in request.items])
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"cache")
        return restore_manager.ServeResult(None, "cache", destination, 5)

    monkeypatch.setattr(restore_manager, "_serve_from_cache", fake_cache)
    with session_scope(engine) as session:
        digests = []
        for index in range(5):
            digest, _backend_id, _memory = _seed_archived_asset(
                session,
                data=f"cache-{index}".encode(),
            )
            _seed_cache_entry(session, tmp_path / "d001", digest, f"cache-{index}".encode())
            digests.append(digest)
        request = admit_restore_request(
            session,
            identity=_identity("sutradhara-operator"),
            destination_id="media-server",
            items=[RestoreItemSpec(digest, "s-masters") for digest in digests],
            config=_config(root),
        )

        serve_restore_request(
            session,
            request,
            identity_or_override=_identity("sutradhara-operator"),
            config=RestoreConfig(
                destinations=_config(root).destinations,
                stream_pool_size=2,
                wake_window_size=4,
            ),
        )

    assert observed[0] == [
        ITEM_STREAMING,
        ITEM_WAKING_DISK,
        ITEM_WAKING_DISK,
        ITEM_WAKING_DISK,
        ITEM_QUEUED,
    ]
    assert any(states[4] == ITEM_WAKING_DISK for states in observed[1:])


def test_parallel_serve_respects_stream_pool_and_aead_subcap(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    lock = threading.Lock()
    active = 0
    active_aead = 0
    max_active = 0
    max_aead = 0

    def fake_cache(_session, entry, destination, _config):
        nonlocal active, active_aead, max_active, max_aead
        with lock:
            active += 1
            if entry.representation == AEAD_REPRESENTATION:
                active_aead += 1
            max_active = max(max_active, active)
            max_aead = max(max_aead, active_aead)
        time.sleep(0.02)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(entry.content_sha256)
        with lock:
            if entry.representation == AEAD_REPRESENTATION:
                active_aead -= 1
            active -= 1
        return restore_manager.ServeResult(None, "cache", destination, len(entry.content_sha256))

    monkeypatch.setattr(restore_manager, "_serve_from_cache", fake_cache)
    with session_scope(engine) as session:
        digests = []
        for index in range(50):
            digest, _backend_id, _memory = _seed_archived_asset(
                session,
                data=f"parallel-{index}".encode(),
            )
            entry = _seed_cache_entry(session, tmp_path / "d001", digest, f"parallel-{index}".encode())
            if index < 20:
                entry.representation = AEAD_REPRESENTATION
                entry.key_epoch = "hdcache-test-epoch"
                entry.stored_digest = b"0" * 32
            digests.append(digest)
        request = admit_restore_request(
            session,
            identity=_identity("sutradhara-operator"),
            destination_id="media-server",
            items=[RestoreItemSpec(digest, "s-masters") for digest in digests],
            config=_config(root),
        )
        request_id = request.id

    factory = make_session_factory(engine)
    with session_scope(engine) as session:
        request = session.get(RestoreRequest, request_id)
        assert request is not None
        serve_restore_request(
            session,
            request,
            identity_or_override=_identity("sutradhara-operator"),
            config=RestoreConfig(
                destinations=_config(root).destinations,
                stream_pool_size=6,
                aead_stream_cap=2,
                worker_session_factory=factory,
            ),
        )

    assert 1 < max_active <= 6
    assert 1 < max_aead <= 2
    with session_scope(engine) as session:
        assert set(session.scalars(select(RestoreRequestItem.state))) == {ITEM_DONE}


def test_deadline_fallback_trips_breaker_without_lost_marking(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    events: list[RestoreEvent] = []
    read_started = threading.Event()
    unblock_read = threading.Event()

    class BlockingReader:
        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def read(self, _size: int = -1) -> bytes:
            read_started.set()
            unblock_read.wait()
            return b""

    with session_scope(engine) as session:
        digest, backend_id, memory = _seed_archived_asset(session, data=b"tape truth")
        entry = _seed_cache_entry(session, tmp_path / "d001", digest, b"tape truth")
        blocked_path = entry_path(tmp_path / "d001", digest, representation=RAW_REPRESENTATION)
        original_open = Path.open

        def open_maybe_blocking(path: Path, mode: str = "r", *args, **kwargs):
            if path == blocked_path and "r" in mode and "b" in mode:
                return BlockingReader()
            return original_open(path, mode, *args, **kwargs)

        monkeypatch.setattr(Path, "open", open_maybe_blocking)
        _request, item = _request_item(session, digest, root)
        started = time.monotonic()
        result = serve_restore_item(
            session,
            item,
            identity_or_override=_identity("sutradhara-operator"),
            config=_config(
                root,
                restore_backends={backend_id: memory},
                events=events,
                read_deadline_seconds=0.05,
                breaker=DiskCircuitBreaker(failure_threshold=1, window_seconds=60),
            ),
        )
        elapsed = time.monotonic() - started

        assert result.source == "tape"
        assert item.state == ITEM_DONE
        assert (root / digest.hex()).read_bytes() == b"tape truth"
        assert session.get(CacheEntry, digest).state == "present"
        assert session.get(CacheDisk, entry.disk_id).filled_bytes == len(b"tape truth")
        assert read_started.is_set()
        assert elapsed < 0.5
    unblock_read.set()

    assert [event.code for event in events] == [
        "cache-fallback:read-deadline",
        "disk-circuit-open",
        "cache-fallback:lost-mark-skipped-breaker",
    ]


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


def test_unverified_cache_disk_identity_falls_back_without_lost_or_delete(
    engine: Engine,
    tmp_path: Path,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    events: list[RestoreEvent] = []
    with session_scope(engine) as session:
        digest, backend_id, memory = _seed_archived_asset(session, data=b"tape truth")
        entry = _seed_cache_entry(session, tmp_path / "d001", digest, b"tape truth")
        path = entry_path(tmp_path / "d001", digest, representation=RAW_REPRESENTATION)
        path.write_bytes(b"corrupt-but-unverified")
        (tmp_path / "d001" / SENTINEL_NAME).write_text("{}", encoding="utf-8")
        _request, item = _request_item(session, digest, root)

        result = serve_restore_item(
            session,
            item,
            identity_or_override=_identity("sutradhara-operator"),
            config=_config(root, restore_backends={backend_id: memory}, events=events),
        )

        assert result.source == "tape"
        assert (root / digest.hex()).read_bytes() == b"tape truth"
        assert path.read_bytes() == b"corrupt-but-unverified"
        assert session.get(CacheEntry, digest).state == "present"
        assert session.get(CacheDisk, entry.disk_id).filled_bytes == len(b"tape truth")

    assert [event.code for event in events] == [
        "disk-identity-unverified",
        "cache-fallback:disk-identity-unverified",
    ]


def test_cache_fallback_state_is_observable_before_tape_work(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    observed: list[str] = []

    with session_scope(engine) as session:
        digest, _backend_id, _memory = _seed_archived_asset(session, data=b"tape truth")
        _seed_cache_entry(session, tmp_path / "d001", digest, b"tape truth")
        entry_path(tmp_path / "d001", digest, representation=RAW_REPRESENTATION).write_bytes(b"bad")
        _request, item = _request_item(session, digest, root)

        def fake_serve_from_tape(
            _session,
            asset_hash,
            _artifactclass,
            destination,
            _config,
            *,
            force_suspect,
            force_rejected,
        ):
            assert asset_hash == digest
            assert force_suspect is False
            assert force_rejected is False
            observed.append(item.state)
            destination.write_bytes(b"tape truth")
            return RestoreResult(
                asset_hash=digest,
                pool_id="mem-pool",
                copy_id=1,
                output_path=destination,
                size_bytes=len(b"tape truth"),
            )

        monkeypatch.setattr(restore_manager, "_serve_from_tape", fake_serve_from_tape)

        result = serve_restore_item(
            session,
            item,
            identity_or_override=_identity("sutradhara-operator"),
            config=_config(root),
        )

        assert result.source == "tape"
        assert observed == [restore_manager.ITEM_FELL_BACK_TO_TAPE]
        assert item.state == ITEM_DONE


def test_cache_lost_mark_failure_audits_and_still_falls_back_to_tape(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "restore-root"
    root.mkdir()
    events: list[RestoreEvent] = []

    def fail_lost_mark(_session, _entry) -> None:
        raise OSError("delete failed")

    monkeypatch.setattr(restore_manager, "mark_entry_lost_and_delete", fail_lost_mark)

    with session_scope(engine) as session:
        digest, backend_id, memory = _seed_archived_asset(session, data=b"tape truth")
        entry = _seed_cache_entry(session, tmp_path / "d001", digest, b"tape truth")
        path = tmp_path / "d001" / "hdcache" / "v1" / digest.hex()[:2] / digest.hex()
        path.write_bytes(b"corrupt")
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
        assert session.get(CacheEntry, digest).state == "present"
        assert session.get(CacheDisk, entry.disk_id).filled_bytes == len(b"tape truth")
    assert [event.code for event in events] == [
        "cache-fallback:read-failed",
        "cache-fallback:lost-mark-failed",
    ]


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
        assert session.get(CacheDisk, "d001").state == "active"
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
        request = admit_restore_request(
            session,
            identity=_identity("sutradhara-operator"),
            destination_id="media-server",
            items=[RestoreItemSpec(digest, "s-masters")],
            force_suspect=True,
            config=_config(root),
        )
        item = request.items[0]

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


class FakeDiskIdentityProbe:
    def __init__(
        self,
        *,
        mounted: bool = True,
        serial: str = "SER001",
        fs_uuid: str = "fs-001",
        wwn: str | None = None,
    ) -> None:
        self.observed = ObservedBlockIdentity(
            mounted=mounted,
            serial=serial,
            fs_uuid=fs_uuid,
            wwn=wwn,
        )

    def observe(self, _mount: Path) -> ObservedBlockIdentity:
        return self.observed


def _config(
    root: Path,
    *,
    restore_backends: dict[int, MemoryBackend] | None = None,
    events: list[RestoreEvent] | None = None,
    read_deadline_seconds: float = restore_manager.DEFAULT_READ_DEADLINE_SECONDS,
    breaker: DiskCircuitBreaker | None = None,
    identity_probe: "FakeDiskIdentityProbe | None" = None,
) -> RestoreConfig:
    return RestoreConfig(
        destinations={
            "media-server": RestoreDestination(
                id="media-server",
                root=root,
                label="Media server restore",
            )
        },
        restore_backends=restore_backends,
        event_sink=None if events is None else events.append,
        read_deadline_seconds=read_deadline_seconds,
        breaker=breaker or DiskCircuitBreaker(),
        hmac_secret=TEST_HDCACHE_HMAC_SECRET,
        identity_probe=identity_probe or FakeDiskIdentityProbe(),
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
    write_disk_sentinel(
        mount,
        ExpectedDiskIdentity("d001", "SER001", "fs-001"),
        hmac_secret=TEST_HDCACHE_HMAC_SECRET,
    )
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
        admitted_by="owner",
        admitted_at=dt.datetime.now(dt.UTC),
        admitted_capabilities=["can_view", "can_receive"],
    )
    item = RestoreRequestItem(
        content_sha256=digest,
        artifactclass="s-masters",
        state=ITEM_QUEUED,
        admitted_force_suspect=False,
        admitted_force_rejected=False,
    )
    request.items.append(item)
    session.add(request)
    session.flush()
    return request, item
