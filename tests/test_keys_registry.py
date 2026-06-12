"""Tests for Sutradhara's local key epoch registry."""

from __future__ import annotations

import hashlib
import stat
from pathlib import Path

from sutradhara.keys import KeyRegistry

_TEST_SEED = bytes.fromhex(
    "73797374656d2d6861726e6573733a737574726164686172612d6b65792d7365"
    "616d3a616d6265722d616561642d6465763a7631"
)


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_key_registry_create_materialize_and_retire_preserves_root(
    tmp_path: Path,
) -> None:
    registry = KeyRegistry(tmp_path / "keys")
    expected_key_id = hashlib.sha256(_TEST_SEED + b":key-id").digest()[:16].hex()
    expected_root = hashlib.sha256(_TEST_SEED + b":root-key").digest()

    epoch = registry.create_epoch()

    assert epoch.key_id == expected_key_id
    assert epoch.active is True
    assert _mode(registry.registry_dir) == 0o700
    root_path = registry.registry_dir / f"{epoch.key_id}.root"
    state_path = registry.registry_dir / f"{epoch.key_id}.json"
    assert root_path.read_bytes() == expected_root
    assert _mode(root_path) == 0o600
    assert _mode(state_path) == 0o600

    with registry.materialized_root_key(epoch.key_id) as key_path:
        materialized = key_path
        assert materialized.read_bytes() == expected_root
        assert _mode(materialized) == 0o600
    assert not materialized.exists()

    retired = registry.retire_epoch(epoch.key_id)
    assert retired["root_key_preserved"] is True
    assert root_path.read_bytes() == expected_root

    with registry.materialized_root_key(epoch.key_id) as key_path:
        assert key_path.read_bytes() == expected_root
    assert registry.get_epoch(epoch.key_id).active is False


def test_key_registry_create_epoch_is_idempotent(tmp_path: Path) -> None:
    registry = KeyRegistry(tmp_path / "keys")

    first = registry.create_epoch()
    second = registry.create_epoch()

    assert second == first
