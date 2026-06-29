"""Bind-safety tests for the API serving command."""

from __future__ import annotations

import pytest
from click import ClickException

from sutradhara.cli.api import validate_tcp_host


def test_tcp_mode_accepts_loopback() -> None:
    validate_tcp_host("127.0.0.1")
    validate_tcp_host("::1")
    validate_tcp_host("localhost")


def test_tcp_mode_rejects_wildcard_and_tailnet() -> None:
    for host in ("0.0.0.0", "::", "100.81.52.26"):
        with pytest.raises(ClickException):
            validate_tcp_host(host)
