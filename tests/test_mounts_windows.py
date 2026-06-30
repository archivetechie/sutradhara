"""Windows mount enumeration behavior for the operator-console helper."""

from __future__ import annotations

from pathlib import Path

import pytest

from sutra_agent import mounts
from sutra_agent.controld import card_snapshot_message
from sutra_agent.mounts import (
    DRIVE_CDROM,
    DRIVE_FIXED,
    DRIVE_NO_ROOT_DIR,
    DRIVE_REMOTE,
    DRIVE_REMOVABLE,
    MountInfo,
    card_from_mount,
    current_mounts,
)


def test_current_mounts_dispatches_to_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = [MountInfo(mount_path=Path("D:\\"), label="Camera")]

    monkeypatch.setattr(mounts.platform, "system", lambda: "Windows")
    monkeypatch.setattr(mounts, "_windows_mounts", lambda: sentinel)

    assert current_mounts() == sentinel


def test_windows_removable_drive_becomes_stable_card(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mounts, "_win_logical_drive_letters", lambda: ["D:"])
    monkeypatch.setattr(mounts, "_win_drive_type", lambda _letter: DRIVE_REMOVABLE)
    monkeypatch.setattr(mounts, "_win_volume_info", lambda _letter: ("CAMERA", 0x1A2B3C4D))

    first = mounts._windows_mounts()[0]
    second = mounts._windows_mounts()[0]
    card = card_from_mount(first)

    assert first.removable is True
    assert first.mount_path == Path("D:\\")
    assert first.source == "D:"
    assert first.volume_uuid == "1A2B-3C4D"
    assert second.volume_uuid == first.volume_uuid
    assert card.kind == "card"
    assert card.card_id == "volume:1A2B-3C4D"


def test_windows_fixed_non_system_drive_becomes_drive_and_excludes_system_and_others(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    drive_types = {
        "C:": DRIVE_FIXED,
        "E:": DRIVE_FIXED,
        "F:": DRIVE_CDROM,
        "G:": DRIVE_NO_ROOT_DIR,
        "Z:": DRIVE_REMOTE,
    }
    volume_info = {
        "C:": ("System", 0x11112222),
        "E:": ("Shuttle", 0xAABBCCDD),
        "F:": ("Disc", 0x01020304),
        "G:": ("Broken", 0x05060708),
        "Z:": ("Share", 0x090A0B0C),
    }

    monkeypatch.setenv("SYSTEMDRIVE", "C:")
    monkeypatch.setattr(mounts, "_win_logical_drive_letters", lambda: list(drive_types))
    monkeypatch.setattr(mounts, "_win_drive_type", lambda letter: drive_types[letter])
    monkeypatch.setattr(mounts, "_win_volume_info", lambda letter: volume_info[letter])

    infos = mounts._windows_mounts()
    cards = [card_from_mount(info) for info in infos]

    assert [info.source for info in infos] == ["E:"]
    assert infos[0].removable is False
    assert cards[0].kind == "drive"
    assert cards[0].card_id == "volume:AABB-CCDD"


def test_windows_drive_with_no_media_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mounts, "_win_logical_drive_letters", lambda: ["H:"])
    monkeypatch.setattr(mounts, "_win_drive_type", lambda _letter: DRIVE_REMOVABLE)
    monkeypatch.setattr(mounts, "_win_volume_info", lambda _letter: None)

    assert mounts._windows_mounts() == []


def test_windows_mount_path_stays_out_of_outbound_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mounts, "_win_logical_drive_letters", lambda: ["D:"])
    monkeypatch.setattr(mounts, "_win_drive_type", lambda _letter: DRIVE_REMOVABLE)
    monkeypatch.setattr(mounts, "_win_volume_info", lambda _letter: ("CAMERA", 0x1A2B3C4D))

    card = card_from_mount(mounts._windows_mounts()[0])
    message = card_snapshot_message([card])

    assert "D:\\" not in str(message)
    assert b"D:\\" not in message.SerializeToString()
