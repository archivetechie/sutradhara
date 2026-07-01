"""Server-side device-relative path validator tests."""

from __future__ import annotations

import pytest

from sutradhara.api.paths import DevicePathError, canonical_device_rel_path


@pytest.mark.parametrize("value", [None, ""])
def test_device_path_root_canonicalizes_to_empty(value: str | None) -> None:
    assert canonical_device_rel_path(value) == ""


@pytest.mark.parametrize(
    "value",
    [
        "/DCIM",
        "../DCIM",
        "DCIM/../PRIVATE",
        "DCIM\\100MEDIA",
        "C:",
        "C:/DCIM",
        "DCIM//100MEDIA",
        "DCIM/./100MEDIA",
        "DCIM/",
        ".",
        "a" * 1025,
    ],
)
def test_device_path_rejects_unsafe_or_non_normalized_input(value: str) -> None:
    with pytest.raises(DevicePathError):
        canonical_device_rel_path(value)


@pytest.mark.parametrize("value", ["DCIM", "DCIM/100MEDIA", "A001.fcpbundle"])
def test_device_path_accepts_normalized_relative_paths(value: str) -> None:
    assert canonical_device_rel_path(value) == value
