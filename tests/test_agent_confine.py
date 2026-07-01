"""Helper-side confinement and directory-listing tests."""

from __future__ import annotations

import contextlib
from pathlib import Path

import pytest

from sutra_agent._proto import device_pb2
from sutra_agent.confine import (
    DevicePathConfinementError,
    DevicePathIoError,
    DevicePathPermissionError,
    directory_listing_message,
    resolve_directory,
)
from sutra_agent.mounts import MountedCard


def test_resolve_directory_confines_root_subpath_and_package_final_segment(tmp_path: Path) -> None:
    mount = tmp_path / "card"
    subdir = mount / "DCIM" / "100MEDIA"
    package = mount / "A001.fcpbundle"
    subdir.mkdir(parents=True)
    package.mkdir(parents=True)

    assert resolve_directory(mount, "").path == mount.resolve()
    assert resolve_directory(mount, "DCIM/100MEDIA").path == subdir.resolve()
    assert resolve_directory(mount, "A001.fcpbundle").path == package.resolve()


@pytest.mark.parametrize(
    "rel_path",
    [
        "../outside",
        "/tmp/outside",
        "C:/DCIM",
        "DCIM\\100MEDIA",
        "DCIM/../PRIVATE",
        "DCIM//100MEDIA",
        "DCIM/./100MEDIA",
        ".",
        "a" * 1025,
    ],
)
def test_resolve_directory_rejects_unsafe_or_non_normalized_paths(
    tmp_path: Path,
    rel_path: str,
) -> None:
    mount = tmp_path / "card"
    mount.mkdir()

    with pytest.raises(DevicePathConfinementError):
        resolve_directory(mount, rel_path)


def test_resolve_directory_rejects_symlink_escape(tmp_path: Path) -> None:
    mount = tmp_path / "card"
    outside = tmp_path / "outside"
    mount.mkdir()
    outside.mkdir()
    try:
        (mount / "escape").symlink_to(outside, target_is_directory=True)
    except OSError as exc:  # pragma: no cover - platform privilege dependent
        pytest.skip(f"symlink unavailable: {exc}")

    with pytest.raises(DevicePathConfinementError):
        resolve_directory(mount, "escape")


def test_resolve_directory_rejects_package_interior_but_allows_package_root(
    tmp_path: Path,
) -> None:
    mount = tmp_path / "card"
    event = mount / "A001.fcpbundle" / "Event"
    event.mkdir(parents=True)

    assert resolve_directory(mount, "A001.fcpbundle").path == event.parent.resolve()
    with pytest.raises(DevicePathConfinementError):
        resolve_directory(mount, "A001.fcpbundle/Event")


def test_directory_listing_is_folders_first_package_flagged_and_symlink_free(
    tmp_path: Path,
) -> None:
    mount = tmp_path / "card"
    mount.mkdir()
    (mount / "Folder").mkdir()
    (mount / "A001.fcpbundle").mkdir()
    (mount / "clip-b.mov").write_bytes(b"bb")
    (mount / "clip-a.mov").write_bytes(b"a")
    with contextlib.suppress(OSError):
        (mount / "linked").symlink_to(mount / "Folder", target_is_directory=True)
    card = _card(mount)

    message = directory_listing_message(
        card,
        device_pb2.ListDirectory(request_id="req-1", card_id=card.card_id, rel_path=""),
    )

    listing = message.directory_listing
    assert listing.status == device_pb2.DIR_STATUS_OK
    assert [(entry.name, entry.is_dir, entry.size_bytes, entry.is_package) for entry in listing.entries] == [
        ("A001.fcpbundle", True, 0, True),
        ("Folder", True, 0, False),
        ("clip-a.mov", False, 1, False),
        ("clip-b.mov", False, 2, False),
    ]
    assert listing.truncated is False


def test_directory_listing_caps_files_after_folders(tmp_path: Path) -> None:
    mount = tmp_path / "card"
    mount.mkdir()
    (mount / "Folder").mkdir()
    for index in range(501):
        (mount / f"{index:03}.mov").write_bytes(b"x")
    card = _card(mount)

    listing = directory_listing_message(
        card,
        device_pb2.ListDirectory(request_id="req-1", card_id=card.card_id, rel_path=""),
    ).directory_listing

    assert listing.truncated is True
    assert len(listing.entries) == 501
    assert listing.entries[0].name == "Folder"
    assert listing.entries[-1].name == "499.mov"


def test_directory_listing_caps_folders_before_files(tmp_path: Path) -> None:
    mount = tmp_path / "card"
    mount.mkdir()
    for index in range(5001):
        (mount / f"dir-{index:04}").mkdir()
    (mount / "clip.mov").write_bytes(b"x")
    card = _card(mount)

    listing = directory_listing_message(
        card,
        device_pb2.ListDirectory(request_id="req-1", card_id=card.card_id, rel_path=""),
    ).directory_listing

    assert listing.truncated is True
    assert len(listing.entries) == 5000
    assert all(entry.is_dir for entry in listing.entries)


def test_directory_listing_maps_path_failures_without_absolute_detail(tmp_path: Path) -> None:
    mount = tmp_path / "card"
    mount.mkdir()
    (mount / "clip.mov").write_bytes(b"x")
    card = _card(mount)

    missing = directory_listing_message(
        card,
        device_pb2.ListDirectory(request_id="missing", card_id=card.card_id, rel_path="missing"),
    ).directory_listing
    not_dir = directory_listing_message(
        card,
        device_pb2.ListDirectory(request_id="file", card_id=card.card_id, rel_path="clip.mov"),
    ).directory_listing
    escaped = directory_listing_message(
        card,
        device_pb2.ListDirectory(request_id="escape", card_id=card.card_id, rel_path=str(tmp_path)),
    ).directory_listing
    unavailable = directory_listing_message(
        None,
        device_pb2.ListDirectory(request_id="card", card_id=card.card_id, rel_path="DCIM"),
    ).directory_listing

    assert missing.status == device_pb2.DIR_STATUS_NOT_FOUND
    assert missing.detail == "missing"
    assert not_dir.status == device_pb2.DIR_STATUS_NOT_A_DIRECTORY
    assert not_dir.detail == "clip.mov"
    assert escaped.status == device_pb2.DIR_STATUS_CONFINEMENT_VIOLATION
    assert str(tmp_path) not in escaped.detail
    assert unavailable.status == device_pb2.DIR_STATUS_CARD_UNAVAILABLE
    assert unavailable.detail == "DCIM"


@pytest.mark.parametrize(
    ("exc", "status"),
    [
        (DevicePathPermissionError("permission denied", detail="Private"), device_pb2.DIR_STATUS_PERMISSION_DENIED),
        (DevicePathIoError("directory cannot be read", detail="Private"), device_pb2.DIR_STATUS_IO_ERROR),
    ],
)
def test_directory_listing_maps_permission_and_io_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exc: Exception,
    status: int,
) -> None:
    mount = tmp_path / "card"
    mount.mkdir()
    card = _card(mount)

    def fail_resolve(_mount_path: Path, _rel_path: str) -> object:
        raise exc

    monkeypatch.setattr("sutra_agent.confine.resolve_directory", fail_resolve)

    listing = directory_listing_message(
        card,
        device_pb2.ListDirectory(request_id="req-1", card_id=card.card_id, rel_path="Private"),
    ).directory_listing

    assert listing.status == status
    assert listing.detail == "Private"
    assert str(tmp_path) not in listing.detail


def _card(mount: Path) -> MountedCard:
    return MountedCard(
        card_id="card-1",
        label="Card",
        kind="drive",
        size_bytes=1,
        status="available",
        mount_path=mount,
    )
