"""Tests for backend_from_row routing (rem_tape live vs fixture)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from sutradhara.backend.d2tape import D2TapeBackend
from sutradhara.backend.factory import BackendNotConfigured, backend_from_row
from sutradhara.backend.memory import MemoryBackend
from sutradhara.backend.remanence import RemanenceBackend
from sutradhara.catalog.models import Backend
from sutradhara.catalog.types import BackendKind, BackendTier


def _rem_tape_row(config: dict[str, Any] | None) -> Backend:
    return Backend(
        name="primary-tape",
        kind=BackendKind.REM_TAPE,
        tier=BackendTier.SELF_DESCRIBING,
        config=config,
    )


def _memory_row(config: dict[str, Any] | None) -> Backend:
    return Backend(
        name="mem",
        kind=BackendKind.MEMORY,
        tier=BackendTier.SELF_DESCRIBING,
        config=config,
    )


def _d2_tape_row(config: dict[str, Any] | None) -> Backend:
    return Backend(
        name="d2-tape",
        kind=BackendKind.D2_TAPE,
        tier=BackendTier.SELF_DESCRIBING,
        config=config,
    )


def test_memory_backend_ignores_empty_config() -> None:
    backend = backend_from_row(_memory_row({}))

    assert isinstance(backend, MemoryBackend)
    assert backend.name == "mem"


def test_obsolete_placements_config_is_rejected() -> None:
    with pytest.raises(BackendNotConfigured, match="obsolete"):
        backend_from_row(
            _memory_row(
                {
                    "placements": [
                        {
                            "placement_id": "mem-copy-1",
                            "content_class": "video-priv",
                            "copy_class": "copy-1",
                        }
                    ]
                }
            )
        )


def test_daemon_endpoint_builds_live_adapter() -> None:
    backend = backend_from_row(
        _rem_tape_row({"daemon_endpoint": "http://localhost:50051"})
    )
    assert isinstance(backend, RemanenceBackend)
    assert backend.name == "primary-tape"


def test_fixture_path_builds_fixture_adapter(tmp_path: Path) -> None:
    fixture = tmp_path / "objs.json"
    fixture.write_text("[]")
    backend = backend_from_row(_rem_tape_row({"fixture_path": str(fixture)}))
    assert isinstance(backend, RemanenceBackend)
    assert list(backend.enumerate()) == []


def test_both_keys_raises_not_configured() -> None:
    row = _rem_tape_row(
        {"daemon_endpoint": "http://x", "fixture_path": "/tmp/f.json"}
    )
    with pytest.raises(BackendNotConfigured, match="both"):
        backend_from_row(row)


def test_neither_key_raises_not_configured() -> None:
    with pytest.raises(BackendNotConfigured):
        backend_from_row(_rem_tape_row({}))


def test_d2_tape_backend_builds_adapter(tmp_path: Path) -> None:
    jar = tmp_path / "d2tape.jar"
    fake_java = tmp_path / "fake-java"
    jar.write_text("fake\n")
    fake_java.write_text("#!/bin/sh\nexit 0\n")
    backend = backend_from_row(
        _d2_tape_row(
            {
                "jar_path": str(jar),
                "java_bin": str(fake_java),
                "device_env_path": str(tmp_path / "device.env"),
                "state_dir": str(tmp_path / "state"),
            }
        )
    )

    assert isinstance(backend, D2TapeBackend)


def test_d2_tape_rejects_obsolete_placements_config(tmp_path: Path) -> None:
    jar = tmp_path / "d2tape.jar"
    jar.write_text("fake\n")
    with pytest.raises(BackendNotConfigured, match="obsolete"):
        backend_from_row(_d2_tape_row({"jar_path": str(jar), "placements": {}}))
