"""Shared deterministic recipient-registry fixtures for RAO tests."""

from __future__ import annotations

from pathlib import Path

from sutradhara.keys import KeyEpoch, KeyRegistry, mint_recovery_keypair


def registry_with_recovery(path: Path) -> tuple[KeyRegistry, KeyEpoch]:
    """Create a deterministic hot-key registry with one public recovery epoch."""

    registry = KeyRegistry(path, deterministic_test=True)
    public_path = path.parent / f"{path.name}-recovery.raor"
    private_path = path.parent / f"{path.name}-recovery.raop"
    mint_recovery_keypair(public_key_path=public_path, private_key_path=private_path)
    recovery = registry.import_public_epoch(public_path)
    return registry, recovery
