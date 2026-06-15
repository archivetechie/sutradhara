"""Reversible archive member-name escaping shared with rem indexes.

Sutradhara stores member names in text columns, but POSIX paths are bytes and
may contain legacy non-UTF-8 names. The catalog representation is therefore the
same escaped string rem uses in `.remwrap.idx`: valid UTF-8 text passes through,
literal backslashes are doubled, and invalid/control bytes are written as
lowercase `\\xhh`.
"""

from __future__ import annotations

import os
from pathlib import Path


class MemberNameError(ValueError):
    """A member-name string is not in canonical escaped form."""


def escape_member_name(raw: bytes) -> str:
    """Return the catalog/customer escaped form for raw member-name bytes."""
    parts: list[str] = []
    index = 0
    while index < len(raw):
        byte = raw[index]
        if byte == 0x5C:
            parts.append(r"\\")
            index += 1
            continue
        if _must_escape_byte(byte):
            parts.append(_hex_escape(byte))
            index += 1
            continue
        sequence_len = _utf8_sequence_len(byte)
        if sequence_len == 0:
            parts.append(_hex_escape(byte))
            index += 1
            continue
        if sequence_len == 1:
            parts.append(chr(byte))
            index += 1
            continue
        candidate = raw[index : index + sequence_len]
        if len(candidate) == sequence_len and _is_valid_utf8(candidate):
            parts.append(candidate.decode("utf-8"))
            index += sequence_len
            continue
        parts.append(_hex_escape(byte))
        index += 1
    return "".join(parts)


def unescape_member_name(text: str) -> bytes:
    """Decode a catalog/customer member-name string back to raw bytes."""
    output = bytearray()
    index = 0
    while index < len(text):
        char = text[index]
        if char == "\\":
            if index + 1 >= len(text):
                raise MemberNameError("member name ends with a bare backslash")
            marker = text[index + 1]
            if marker == "\\":
                output.append(0x5C)
                index += 2
                continue
            if marker == "x" and index + 3 < len(text):
                raw_hex = text[index + 2 : index + 4]
                if _is_lower_hex_pair(raw_hex):
                    output.append(int(raw_hex, 16))
                    index += 4
                    continue
            raise MemberNameError(f"invalid escape at character {index}")
        if ord(char) < 0x20 or ord(char) == 0x7F:
            raise MemberNameError("member name contains an unescaped control character")
        output.extend(char.encode("utf-8"))
        index += 1
    return bytes(output)


def escape_path_name(path: Path | str) -> str:
    """Escape a single filesystem path name using raw OS bytes."""
    return escape_member_name(os.fsencode(Path(path).name))


def escape_path_text(path: str) -> str:
    """Escape a supplied member path string, preserving surrogateescaped bytes."""
    return escape_member_name(path.encode("utf-8", "surrogateescape"))


def _must_escape_byte(byte: int) -> bool:
    return byte < 0x20 or byte == 0x7F


def _hex_escape(byte: int) -> str:
    return f"\\x{byte:02x}"


def _utf8_sequence_len(first: int) -> int:
    if first < 0x80:
        return 1
    if 0xC2 <= first <= 0xDF:
        return 2
    if 0xE0 <= first <= 0xEF:
        return 3
    if 0xF0 <= first <= 0xF4:
        return 4
    return 0


def _is_valid_utf8(raw: bytes) -> bool:
    try:
        raw.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


def _is_lower_hex_pair(text: str) -> bool:
    if len(text) != 2:
        return False
    return all(char in "0123456789abcdef" for char in text)
