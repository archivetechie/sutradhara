"""Shared payload planner tests for streaming receive."""

from __future__ import annotations

import hashlib
import os

import pytest

from sutradhara_receive import SourceMutationError, plan_payload_units
from sutradhara_receive import core as receive_core


def test_planner_yields_package_as_one_unit_with_index(tmp_path) -> None:
    source = tmp_path / "source"
    package = source / "Edit.fcpbundle"
    (package / "Media").mkdir(parents=True)
    (package / "Media" / "clip.mov").write_bytes(b"clip")

    plan = plan_payload_units(source)

    assert len(plan.units) == 1
    unit = plan.units[0]
    assert unit.relpath == "Edit.fcpbundle.tar"
    assert unit.hint_size == 0
    data = b"".join(unit.byte_chunks(1024))
    package_index = unit.package_index(hashlib.sha256(data).hexdigest())
    assert package_index is not None
    assert package_index["logical_member_path"] == "Edit.fcpbundle"
    assert package_index["stored_member_path"] == "Edit.fcpbundle.tar"


def test_planner_packages_package_boundary_stream_root(tmp_path) -> None:
    package = tmp_path / "Edit.fcpbundle"
    (package / "Media").mkdir(parents=True)
    (package / "Media" / "clip.mov").write_bytes(b"clip")

    plan = plan_payload_units(package)

    assert len(plan.units) == 1
    unit = plan.units[0]
    assert unit.relpath == "Edit.fcpbundle.tar"
    assert unit.logical_relpath == "Edit.fcpbundle"
    assert unit.is_package is True


def test_planner_relpaths_are_relative_to_stream_root(tmp_path) -> None:
    selected = tmp_path / "DCIM" / "100MEDIA"
    selected.mkdir(parents=True)
    (selected / "IMG001.JPG").write_bytes(b"image")

    plan = plan_payload_units(selected)

    assert [unit.relpath for unit in plan.units] == ["IMG001.JPG"]


def test_planner_uses_native_scanner_when_available(tmp_path, monkeypatch) -> None:
    assert receive_core._native is not None
    source = tmp_path / "source"
    source.mkdir()
    (source / "clip.mov").write_bytes(b"clip")

    def fail_scan(_source_root):
        raise AssertionError("pure Python scanner should not be called")

    monkeypatch.setattr(receive_core, "_scan_source", fail_scan)

    plan = plan_payload_units(source)

    assert [unit.relpath for unit in plan.units] == ["clip.mov"]


def test_native_planner_preserves_invalid_source_path_bytes(tmp_path) -> None:
    assert receive_core._native is not None
    source = tmp_path / "source"
    source.mkdir()
    raw_path = os.fsencode(source) + b"/bad_\xff.bin"
    fd = os.open(raw_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        os.write(fd, b"legacy")
    finally:
        os.close(fd)

    plan = plan_payload_units(source)

    assert [unit.relpath for unit in plan.units] == ["bad_\\xff.bin"]
    assert plan.units[0].source_path.read_bytes() == b"legacy"


def test_file_unit_stat_guard_fails_if_source_mutates_mid_read(tmp_path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    payload = source / "clip.mov"
    payload.write_bytes(b"a" * 2048)
    unit = plan_payload_units(source).units[0]
    chunks = unit.byte_chunks(1024)
    assert next(chunks) == b"a" * 1024
    payload.write_bytes(b"changed")
    with pytest.raises(SourceMutationError):
        list(chunks)
