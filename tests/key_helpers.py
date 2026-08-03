"""Shared deterministic recipient-registry fixtures for REM-OBJECT tests."""

from __future__ import annotations

import hashlib
from pathlib import Path

from sutradhara.keys import KeyEpoch, KeyRegistry, mint_recovery_keypair
from sutradhara.keys.remanence import (
    RecipientKeyCodec,
    RecipientPublicIdentity,
    RecipientPublicMaterial,
    parse_public_structure,
)


class DeterministicRecipientKeyCodec:
    """Hermetic structural codec double, permitted only in test registries."""

    def derive_public(self, private_key_path: Path, *, slot_index: int) -> RecipientPublicMaterial:
        payload = private_key_path.read_bytes()
        if payload[:4] != b"REMP" or len(payload) < 53:
            raise ValueError("invalid test REMP file")
        epoch_id = payload[4:20]
        label_length = payload[20]
        if len(payload) != 53 + label_length:
            raise ValueError("invalid test REMP file length")
        label = payload[21 : 21 + label_length].decode("ascii")
        seed = payload[21 + label_length :]
        public = hashlib.shake_256(b"test-xwing-public\0" + seed).digest(1216)
        canonical = (
            b"REMR"
            + bytes([slot_index])
            + epoch_id
            + bytes([label_length])
            + label.encode("ascii")
            + public
        )
        return RecipientPublicMaterial(
            canonical_bytes=canonical,
            recipient_epoch_id=epoch_id,
            epoch_label=label,
            slot_index=slot_index,
        )

    def inspect_public(self, public_key_path: Path) -> RecipientPublicIdentity:
        return parse_public_structure(public_key_path.read_bytes())


TEST_RECIPIENT_CODEC = DeterministicRecipientKeyCodec()


def make_test_key_registry(path: Path, *, deterministic_test: bool = True) -> KeyRegistry:
    """Build a hermetic registry whose crypto boundary is an explicit test double."""

    return KeyRegistry(
        path,
        deterministic_test=deterministic_test,
        recipient_codec=TEST_RECIPIENT_CODEC,
        allow_test_codec=True,
    )


def registry_with_recovery(
    path: Path,
    *,
    recipient_codec: RecipientKeyCodec = TEST_RECIPIENT_CODEC,
) -> tuple[KeyRegistry, KeyEpoch]:
    """Create a deterministic hot-key registry with one public recovery epoch."""

    registry = KeyRegistry(
        path,
        deterministic_test=True,
        recipient_codec=recipient_codec,
    )
    public_path = path.parent / f"{path.name}-recovery.remr"
    private_path = path.parent / f"{path.name}-recovery.remp"
    mint_recovery_keypair(
        public_key_path=public_path,
        private_key_path=private_path,
        recipient_codec=recipient_codec,
        allow_test_codec=True,
    )
    recovery = registry.import_public_epoch(public_path)
    return registry, recovery
