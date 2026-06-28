"""Tests for rem-compatible archive member-name escaping."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from sutradhara_receive.member_name import (
    MemberNameError,
    escape_member_name,
    escape_path_name,
    escape_path_text,
    unescape_member_name,
)


def test_legacy_member_name_import_aliases_extracted_package() -> None:
    import sutradhara.member_name as legacy_member_name
    import sutradhara_receive.member_name as extracted_member_name

    assert legacy_member_name is extracted_member_name


def test_member_name_escape_round_trips_utf8_backslash_controls_and_invalid_bytes() -> None:
    raw = b"dir/name\\with\x00controls\x7f-and-invalid-\xff-\xe2\x82\xac"

    escaped = escape_member_name(raw)

    assert escaped == r"dir/name\\with\x00controls\x7f-and-invalid-\xff-€"
    assert unescape_member_name(escaped) == raw


def test_member_name_decoder_rejects_noncanonical_escapes() -> None:
    with pytest.raises(MemberNameError):
        unescape_member_name(r"bad\q")
    with pytest.raises(MemberNameError):
        unescape_member_name(r"bad\xFF")
    with pytest.raises(MemberNameError):
        unescape_member_name("bad\nname")


def test_path_helpers_preserve_surrogateescaped_filesystem_bytes(tmp_path: Path) -> None:
    raw_name = b"legacy-\xff\\name.bin"
    raw_path = os.fsencode(tmp_path) + b"/" + raw_name
    fd = os.open(raw_path, os.O_CREAT | os.O_WRONLY, 0o644)
    os.close(fd)
    path_text = os.fsdecode(raw_path)

    escaped = escape_path_name(Path(path_text))

    assert escaped == r"legacy-\xff\\name.bin"
    assert escape_path_text(os.fsdecode(raw_name)) == escaped
    assert unescape_member_name(escaped) == raw_name
