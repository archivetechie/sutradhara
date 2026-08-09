"""HTTP contract tests for the library console read models."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any

import grpc
from fastapi.testclient import TestClient
from sqlalchemy import Engine

from sutradhara._proto import layer5_pb2
from sutradhara.api.routes_library import MAX_LIMIT
from sutradhara.catalog.models import Backend, Copy, LogicalAsset, OffsiteConfirmation, Pool
from sutradhara.catalog.session import locator_key, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource, MediaKind
from sutradhara.hdcache.models import CacheDisk, CacheEntry
from sutradhara.retention import confirm_offsite
from tests.api.conftest import auth_headers, make_api_app

DISK_PUBLIC_KEYS = {
    "disk_id",
    "state",
    "capacity_bytes",
    "filled_bytes",
    "capacity_state",
    "last_walk_at",
    "entry_count",
    "lost_count",
}
DISK_ADMIN_KEYS = {
    "serial",
    "wwn",
    "fs_uuid",
    "enclosure",
    "slot",
    "smart_status",
}
TAPE_KEYS = {
    "tape_key",
    "media_id",
    "display_label",
    "backend_name",
    "backend_kind",
    "pool_id",
    "library",
    "object_count",
    "health_rollup",
    "last_checked_at",
    "offsite_confirmed",
}
DRIVE_PUBLIC_KEYS = {"bay", "status"}
DRIVE_ADMIN_KEYS = {"serial", "voltag"}
SLOT_PUBLIC_KEYS = {"address", "full"}
SLOT_ADMIN_KEYS = {"voltag"}


def test_library_disks_whitelist_admin_shaping_and_drill_mapping(
    api_engine: Engine,
) -> None:
    base = dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    drill_id = "d001:20260704T080000Z"
    with session_scope(api_engine) as session:
        _add_disk(session, "d001", state="dead", last_walk_at=base)
        _add_disk(session, "d002", state="active", last_walk_at=base)
        lost = _add_asset(session, "lost", 10)
        refilled = _add_asset(session, "refilled", 20)
        _add_cache_entry(
            session,
            digest=lost,
            disk_id="d001",
            state="lost",
            size_bytes=10,
            lost_origin_disk_id="d001",
            lost_drill_id=drill_id,
            lost_at=base,
        )
        _add_cache_entry(
            session,
            digest=refilled,
            disk_id="d002",
            state="present",
            size_bytes=20,
            lost_origin_disk_id="d001",
            lost_drill_id=drill_id,
            lost_at=base,
            refilled_at=base + dt.timedelta(hours=1),
        )
    client = TestClient(make_api_app(api_engine))

    viewer = client.get("/api/ui/library/disks", headers=auth_headers("viewer"))
    admin = client.get("/api/ui/library/disks/d001", headers=auth_headers("admin"))

    assert viewer.status_code == 200
    viewer_body = viewer.json()
    assert set(viewer_body) == {"total", "truncated", "disks"}
    assert viewer_body["total"] == 2
    assert viewer_body["truncated"] is False
    viewer_disk = next(row for row in viewer_body["disks"] if row["disk_id"] == "d001")
    assert set(viewer_disk) == DISK_PUBLIC_KEYS
    assert viewer_disk["entry_count"] == 1
    assert viewer_disk["lost_count"] == 1
    assert "mount" not in json.dumps(viewer_body)

    assert admin.status_code == 200
    admin_body = admin.json()
    assert set(admin_body) == DISK_PUBLIC_KEYS | DISK_ADMIN_KEYS | {"drills"}
    assert admin_body["serial"] == "SER-d001"
    assert admin_body["fs_uuid"] == "FS-d001"
    assert admin_body["smart_status"] == "ok"
    assert admin_body["drills"] == [
        {
            "drill_id": drill_id,
            "lost_at": base.isoformat(),
            "entries_lost": 2,
            "entries_refilled": 1,
        }
    ]
    assert "mount" not in json.dumps(admin_body)
    assert "/mnt/cache" not in json.dumps(admin_body)


def test_library_disks_cap_total_and_truncated(api_engine: Engine) -> None:
    with session_scope(api_engine) as session:
        for index in range(MAX_LIMIT + 3):
            _add_disk(session, f"d{index:03d}")
    client = TestClient(make_api_app(api_engine))

    response = client.get("/api/ui/library/disks?limit=999", headers=auth_headers("viewer"))

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == MAX_LIMIT + 3
    assert body["truncated"] is True
    assert len(body["disks"]) == MAX_LIMIT


def test_library_tapes_grouping_opacity_admin_shaping_and_offsite_match(
    api_engine: Engine,
) -> None:
    verified = dt.datetime(2026, 7, 4, 9, 0, tzinfo=dt.UTC)
    tape_uuid = "a" * 32
    d2_uuid = "00000000-0000-4000-8000-00000000000f"
    with session_scope(api_engine) as session:
        rem_backend, rem_pool = _add_backend_pool(
            session,
            "rem-main",
            BackendKind.REM_TAPE,
            "main-pool",
        )
        d2_backend, d2_pool = _add_backend_pool(
            session,
            "d2-main",
            BackendKind.D2_TAPE,
            "d2-pool",
        )
        _add_copy(
            session,
            rem_backend,
            rem_pool,
            "asset-a",
            locator={"tape_uuid": tape_uuid, "tape_file_number": 1},
            health=CopyHealth.OK,
            last_checked_at=verified,
        )
        _add_copy(
            session,
            rem_backend,
            rem_pool,
            "asset-b",
            locator={"tape_uuid": tape_uuid, "tape_file_number": 2},
            health=CopyHealth.SUSPECT,
            last_checked_at=verified + dt.timedelta(minutes=5),
        )
        _add_copy(
            session,
            d2_backend,
            d2_pool,
            "asset-c",
            locator={"barcode": "D2T001L7", "volume_uuid": d2_uuid},
            health=CopyHealth.OK,
        )
        session.add(
            OffsiteConfirmation(
                media_id=f"tape:{tape_uuid}",
                confirmed_by="ops",
                confirmed_at=verified,
            )
        )
        session.add(
            OffsiteConfirmation(
                media_id=f"d2tape:{d2_uuid}",
                confirmed_by="ops",
                confirmed_at=verified,
                revoked_at=verified + dt.timedelta(hours=1),
                revoked_by="reviewer",
            )
        )
    app = make_api_app(api_engine)
    app.state.remanence_tape_catalog = [
        {"tape_uuid": tape_uuid, "media_id": "VOL001", "library": "mainlib"},
        {"volume_uuid": d2_uuid, "media_id": "D2T001L7", "library": "d2lib"},
    ]
    client = TestClient(app)

    viewer = client.get("/api/ui/library/tapes", headers=auth_headers("viewer"))
    admin = client.get("/api/ui/library/tapes", headers=auth_headers("admin"))

    assert viewer.status_code == 200
    assert admin.status_code == 200
    viewer_body = viewer.json()
    admin_body = admin.json()
    assert set(viewer_body) == {"total", "truncated", "tapes"}
    assert viewer_body["total"] == 2
    assert viewer_body["truncated"] is False
    assert all(set(row) == TAPE_KEYS for row in viewer_body["tapes"])
    assert all(row["media_id"] is None for row in viewer_body["tapes"])
    assert all(row["display_label"] is None for row in viewer_body["tapes"])
    assert "VOL001" not in json.dumps(viewer_body)
    assert "D2T001L7" not in json.dumps(viewer_body)

    admin_by_media = {row["media_id"]: row for row in admin_body["tapes"]}
    assert set(admin_by_media) == {f"tape:{tape_uuid}", f"d2tape:{d2_uuid}"}
    rem_row = admin_by_media[f"tape:{tape_uuid}"]
    assert rem_row["display_label"] == "VOL001"
    assert rem_row["object_count"] == 2
    assert rem_row["health_rollup"] == "suspect"
    assert rem_row["library"] == "mainlib"
    assert rem_row["last_checked_at"] == (verified + dt.timedelta(minutes=5)).isoformat()
    assert rem_row["offsite_confirmed"] is True
    d2_row = admin_by_media[f"d2tape:{d2_uuid}"]
    assert d2_row["display_label"] == "D2T001L7"
    assert d2_row["offsite_confirmed"] is False
    assert rem_row["tape_key"].startswith("tape_")
    assert "VOL001" not in rem_row["tape_key"]
    viewer_keys = {row["backend_name"]: row["tape_key"] for row in viewer_body["tapes"]}
    admin_keys = {row["backend_name"]: row["tape_key"] for row in admin_body["tapes"]}
    assert viewer_keys == admin_keys

    with session_scope(api_engine) as session:
        row, changed = confirm_offsite(
            session,
            media_id=f"d2tape:{d2_uuid}",
            confirmed_by="ops-2",
        )
        assert changed is True
        assert row.revoked_at is None
        assert row.revoked_by is None
    reconfirmed = client.get("/api/ui/library/tapes", headers=auth_headers("admin"))
    assert reconfirmed.status_code == 200
    reconfirmed_by_media = {row["media_id"]: row for row in reconfirmed.json()["tapes"]}
    assert reconfirmed_by_media[f"d2tape:{d2_uuid}"]["offsite_confirmed"] is True


def test_library_drives_two_vtl_filter_and_admin_shaping(api_engine: Engine) -> None:
    main_uuid = b"\x01" * 16
    d2_uuid = b"\x02" * 16
    ignored_uuid = b"\x03" * 16
    loaded_tape = b"\xaa" * 16
    app = make_api_app(api_engine)
    app.state.remanence_library_client = _FakeLibraryClient(
        {
            main_uuid: layer5_pb2.LibraryState(
                library=layer5_pb2.Library(
                    library_serial="mainlib",
                    product_revision="D.00",
                    library_uuid=main_uuid,
                ),
                drives=[
                    layer5_pb2.Drive(
                        element_address=1,
                        drive_serial="DRV-MAIN-1",
                        loaded_tape_uuid=loaded_tape,
                        status=layer5_pb2.Drive.DRIVE_STATUS_LOADED,
                    )
                ],
                slots=[
                    layer5_pb2.Slot(
                        element_address=101,
                        voltag="VOL001",
                        tape_uuid=loaded_tape,
                    )
                ],
            ),
            d2_uuid: layer5_pb2.LibraryState(
                library=layer5_pb2.Library(
                    library_serial="d2lib",
                    product_revision="D2D0",
                    library_uuid=d2_uuid,
                ),
                drives=[
                    layer5_pb2.Drive(
                        element_address=2,
                        drive_serial="DRV-D2-1",
                        status=layer5_pb2.Drive.DRIVE_STATUS_UNREACHABLE,
                    ),
                    layer5_pb2.Drive(
                        status=layer5_pb2.Drive.DRIVE_STATUS_UNREACHABLE,
                    ),
                ],
                slots=[layer5_pb2.Slot(element_address=201)],
            ),
            ignored_uuid: layer5_pb2.LibraryState(
                library=layer5_pb2.Library(
                    library_serial="otherlib",
                    product_revision="X.00",
                    library_uuid=ignored_uuid,
                ),
            ),
        }
    )
    client = TestClient(app)

    viewer = client.get("/api/ui/library/drives", headers=auth_headers("viewer"))
    admin = client.get("/api/ui/library/drives", headers=auth_headers("admin"))

    assert viewer.status_code == 200
    assert admin.status_code == 200
    viewer_libraries = viewer.json()["libraries"]
    admin_libraries = admin.json()["libraries"]
    assert [row["library"] for row in viewer_libraries] == ["mainlib", "d2lib"]
    assert [row["changer_revision"] for row in viewer_libraries] == ["D.00", "D2D0"]

    main_viewer = viewer_libraries[0]
    assert set(main_viewer["drives"][0]) == DRIVE_PUBLIC_KEYS
    assert set(main_viewer["slots"][0]) == SLOT_PUBLIC_KEYS
    assert main_viewer["drives"][0] == {"bay": "1", "status": "loaded"}
    assert main_viewer["slots"][0] == {"address": 101, "full": True}

    main_admin = admin_libraries[0]
    assert set(main_admin["drives"][0]) == DRIVE_PUBLIC_KEYS | DRIVE_ADMIN_KEYS
    assert set(main_admin["slots"][0]) == SLOT_PUBLIC_KEYS | SLOT_ADMIN_KEYS
    assert main_admin["drives"][0]["serial"] == "DRV-MAIN-1"
    assert main_admin["drives"][0]["voltag"] == "VOL001"
    assert main_admin["slots"][0]["voltag"] == "VOL001"
    assert admin_libraries[1]["drives"][0]["status"] == "unreachable"
    assert admin_libraries[1]["drives"][0]["bay"] == "2"
    assert admin_libraries[1]["drives"][1]["bay"] is None


def test_library_drives_unreachable_uses_nested_error_envelope(api_engine: Engine) -> None:
    app = make_api_app(api_engine)
    app.state.remanence_library_client = _UnavailableLibraryClient()
    client = TestClient(app)

    response = client.get("/api/ui/library/drives", headers=auth_headers("viewer"))

    assert response.status_code == 503
    assert response.json() == {
        "detail": {
            "error": "unavailable",
            "detail": "remanence library service unreachable",
        }
    }


def test_library_read_models_require_can_view(api_engine: Engine) -> None:
    client = TestClient(make_api_app(api_engine))

    for path in (
        "/api/ui/library/disks",
        "/api/ui/library/disks/d001",
        "/api/ui/library/tapes",
        "/api/ui/library/drives",
    ):
        response = client.get(path, headers=auth_headers("restore-p2"))
        assert response.status_code == 403
        assert response.json() == {
            "detail": {"error": "forbidden", "detail": "operator has no sutradhara role"}
        }


class _FakeLibraryClient:
    def __init__(self, states: dict[bytes, layer5_pb2.LibraryState]) -> None:
        self.states = states

    def ListLibraries(
        self, request: object, timeout: float | None = None
    ) -> layer5_pb2.ListLibrariesResponse:
        del request, timeout
        return layer5_pb2.ListLibrariesResponse(
            libraries=[state.library for state in self.states.values()]
        )

    def GetLibrary(
        self,
        request: layer5_pb2.GetLibraryRequest,
        timeout: float | None = None,
    ) -> layer5_pb2.LibraryState:
        del timeout
        return self.states[request.library_uuid]


class _UnavailableLibraryClient:
    def ListLibraries(self, request: object, timeout: float | None = None) -> object:
        del request, timeout
        raise _UnavailableRpcError()


class _UnavailableRpcError(grpc.RpcError):
    pass


def _digest(label: str) -> bytes:
    return hashlib.sha256(label.encode("utf-8")).digest()


def _add_asset(session: Any, label: str, size: int) -> bytes:
    digest = _digest(label)
    session.add(
        LogicalAsset(
            content_sha256=digest,
            size_bytes=size,
            media_kind=MediaKind.VIDEO,
        )
    )
    session.flush()
    return digest


def _add_disk(
    session: Any,
    disk_id: str,
    *,
    state: str = "active",
    last_walk_at: dt.datetime | None = None,
) -> CacheDisk:
    disk = CacheDisk(
        disk_id=disk_id,
        serial=f"SER-{disk_id}",
        wwn=f"WWN-{disk_id}",
        fs_uuid=f"FS-{disk_id}",
        enclosure="enc-a",
        slot=f"slot-{disk_id}",
        mount=f"/mnt/cache/{disk_id}",
        state=state,
        capacity_bytes=1000,
        filled_bytes=250,
        capacity_state="ok",
        smart_status="ok",
        enrolled_at=dt.datetime(2026, 7, 4, 7, 0, tzinfo=dt.UTC),
        last_walk_at=last_walk_at,
    )
    session.add(disk)
    session.flush([disk])
    return disk


def _add_cache_entry(
    session: Any,
    *,
    digest: bytes,
    disk_id: str,
    state: str,
    size_bytes: int,
    lost_origin_disk_id: str | None = None,
    lost_drill_id: str | None = None,
    lost_at: dt.datetime | None = None,
    refilled_at: dt.datetime | None = None,
) -> CacheEntry:
    entry = CacheEntry(
        content_sha256=digest,
        artifactclass="s-masters",
        disk_id=disk_id,
        relpath=f"{digest.hex()}.rao",
        size_bytes=size_bytes,
        state=state,
        lost_origin_disk_id=lost_origin_disk_id,
        lost_drill_id=lost_drill_id,
        lost_at=lost_at,
        refilled_at=refilled_at,
    )
    session.add(entry)
    session.flush([entry])
    return entry


def _add_backend_pool(
    session: Any,
    backend_name: str,
    kind: BackendKind,
    pool_id: str,
) -> tuple[Backend, Pool]:
    backend = Backend(
        name=backend_name,
        kind=kind,
        tier=BackendTier.SELF_DESCRIBING,
    )
    session.add(backend)
    session.flush([backend])
    pool = Pool(
        id=pool_id,
        backend_id=backend.id,
        representation="RAO_PLAIN",
        location="test",
        tier="archive",
    )
    session.add(pool)
    session.flush([pool])
    return backend, pool


def _add_copy(
    session: Any,
    backend: Backend,
    pool: Pool,
    label: str,
    *,
    locator: dict[str, Any],
    health: CopyHealth,
    last_checked_at: dt.datetime | None = None,
) -> Copy:
    digest = _add_asset(session, label, 100)
    copy = Copy(
        logical_asset_hash=digest,
        backend_id=backend.id,
        pool_id=pool.id,
        native_locator=locator,
        native_locator_key=locator_key(locator),
        storage_metadata={},
        integrity_hash=_digest(f"copy-{label}"),
        health=health,
        source=CopySource.INGEST,
        last_checked_at=last_checked_at,
    )
    session.add(copy)
    session.flush([copy])
    return copy
