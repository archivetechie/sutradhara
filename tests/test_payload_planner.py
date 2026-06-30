"""Shared payload planner tests for streaming receive."""

from __future__ import annotations

import hashlib

import pytest

from sutradhara_receive import SourceMutationError, plan_payload_units


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
