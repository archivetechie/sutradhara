"""Bind-safety tests for the API serving command."""

from __future__ import annotations

import stat
from pathlib import Path

import pytest
from click import ClickException

from sutradhara.cli.api import _bind_unix_socket, _parse_socket_mode, validate_tcp_host


def test_tcp_mode_accepts_loopback() -> None:
    validate_tcp_host("127.0.0.1")
    validate_tcp_host("::1")
    validate_tcp_host("localhost")


def test_tcp_mode_rejects_wildcard_and_tailnet() -> None:
    for host in ("0.0.0.0", "::", "100.81.52.26"):
        with pytest.raises(ClickException):
            validate_tcp_host(host)


def test_socket_mode_parser_accepts_octal_modes() -> None:
    assert _parse_socket_mode("660") == 0o660
    assert _parse_socket_mode("0600") == 0o600


def test_socket_mode_parser_rejects_invalid_modes() -> None:
    for mode in ("abc", "888", "1000", "-1"):
        with pytest.raises(ClickException):
            _parse_socket_mode(mode)


def test_unix_socket_bind_applies_requested_mode(tmp_path: Path) -> None:
    socket_path = tmp_path / "api.sock"
    sock = _bind_unix_socket(socket_path, mode=0o660)
    try:
        socket_stat = socket_path.lstat()
        assert stat.S_ISSOCK(socket_stat.st_mode)
        assert stat.S_IMODE(socket_stat.st_mode) == 0o660
    finally:
        sock.close()
        socket_path.unlink(missing_ok=True)


def test_unix_socket_bind_refuses_existing_non_socket(tmp_path: Path) -> None:
    socket_path = tmp_path / "api.sock"
    socket_path.write_text("not a socket", encoding="utf-8")

    with pytest.raises(ClickException, match="refusing to replace non-socket path"):
        _bind_unix_socket(socket_path, mode=0o660)
