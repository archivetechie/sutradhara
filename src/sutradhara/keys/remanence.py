"""Remanence CLI boundary for canonical REM-ENCRYPT recipient keys.

Sutradhara owns recipient-epoch lifecycle and custody, while Remanence owns
X-Wing derivation and canonical REMR/REMP validation.  This module is the one
subprocess adapter between those responsibilities.
"""

from __future__ import annotations

import base64
import binascii
import json
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from sutradhara.resource_control import run_managed


@dataclass(frozen=True)
class RecipientPublicMaterial:
    """Canonical public file plus the identity Remanence validated."""

    canonical_bytes: bytes
    recipient_epoch_id: bytes
    epoch_label: str
    slot_index: int


@dataclass(frozen=True)
class RecipientPublicIdentity:
    """Identity parsed from a canonical public file by Remanence."""

    recipient_epoch_id: bytes
    epoch_label: str
    slot_index: int
    public_key_bytes: int


class RecipientKeyCodec(Protocol):
    """Port for X-Wing derivation and canonical public-file validation."""

    def derive_public(self, private_key_path: Path, *, slot_index: int) -> RecipientPublicMaterial:
        """Derive one canonical REMR record from a canonical REMP file."""

    def inspect_public(self, public_key_path: Path) -> RecipientPublicIdentity:
        """Parse and validate one canonical REMR record."""


class RemRecipientKeyCodec:
    """Recipient-key codec backed by the installed Remanence CLI."""

    def __init__(self, rem_bin: str | Path | None = None) -> None:
        self._rem_bin = rem_bin

    def derive_public(self, private_key_path: Path, *, slot_index: int) -> RecipientPublicMaterial:
        """Ask Remanence to derive the X-Wing public recipient record."""

        if not 0 <= slot_index <= 255:
            raise ValueError("recipient slot_index must fit in one byte")
        report = self._run(
            [
                "archive",
                "recipient",
                "derive",
                "--private-key",
                str(private_key_path),
                "--slot-index",
                str(slot_index),
            ],
            failure_label="rem archive recipient derive",
        )
        encoded = report.get("canonical_public_key_base64")
        if not isinstance(encoded, str):
            raise RuntimeError("Remanence recipient derive omitted canonical_public_key_base64")
        try:
            canonical = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise RuntimeError(
                "Remanence recipient derive returned invalid standard base64"
            ) from exc
        identity = _identity_from_report(report, include_public_size=False)
        material = RecipientPublicMaterial(
            canonical_bytes=canonical,
            recipient_epoch_id=identity.recipient_epoch_id,
            epoch_label=identity.epoch_label,
            slot_index=identity.slot_index,
        )
        parsed = parse_public_structure(canonical)
        if (
            parsed.recipient_epoch_id != material.recipient_epoch_id
            or parsed.epoch_label != material.epoch_label
            or parsed.slot_index != material.slot_index
        ):
            raise RuntimeError("Remanence recipient derive report differs from its REMR payload")
        return material

    def inspect_public(self, public_key_path: Path) -> RecipientPublicIdentity:
        """Ask Remanence to cryptographically validate a public recipient record."""

        report = self._run(
            [
                "archive",
                "recipient",
                "inspect",
                "--public-key",
                str(public_key_path),
            ],
            failure_label="rem archive recipient inspect",
        )
        return _identity_from_report(report, include_public_size=True)

    def _run(self, args: list[str], *, failure_label: str) -> dict[str, object]:
        # Lazy import avoids making the archive adapter depend on the key package
        # during module initialization while preserving one executable resolver.
        from sutradhara.rem_archive_cli import resolve_rem_bin

        command = [resolve_rem_bin(self._rem_bin), *args]
        completed = run_managed(
            command,
            role="medium",
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"{failure_label} failed (exit {completed.returncode}): "
                f"command={shlex.join(command)!r} "
                f"stdout={completed.stdout.strip()[:500]!r} "
                f"stderr={completed.stderr.strip()[:500]!r}"
            )
        for line in completed.stdout.splitlines():
            line = line.strip()
            if not (line.startswith("{") and line.endswith("}")):
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"{failure_label} emitted invalid JSON") from exc
            if isinstance(value, dict):
                return value
        raise RuntimeError(
            f"{failure_label} emitted no JSON object: "
            f"stdout={completed.stdout.strip()[:500]!r} "
            f"stderr={completed.stderr.strip()[:500]!r}"
        )


def parse_public_structure(payload: bytes) -> RecipientPublicIdentity:
    """Parse REMR framing so CLI report bytes can be cross-checked locally."""

    fixed_length = 4 + 1 + 16 + 1 + 1216
    if payload[:4] != b"REMR" or len(payload) < fixed_length:
        raise ValueError("invalid REM-OBJECT recipient public-key file")
    slot_index = payload[4]
    recipient_epoch_id = payload[5:21]
    label_length = payload[21]
    if len(payload) != fixed_length + label_length:
        raise ValueError("invalid REM-OBJECT recipient public-key file length")
    try:
        label = payload[22 : 22 + label_length].decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError("invalid REM-OBJECT recipient public-key label") from exc
    return RecipientPublicIdentity(
        recipient_epoch_id=recipient_epoch_id,
        epoch_label=label,
        slot_index=slot_index,
        public_key_bytes=1216,
    )


def _identity_from_report(
    report: dict[str, object],
    *,
    include_public_size: bool,
) -> RecipientPublicIdentity:
    epoch_hex = report.get("recipient_epoch_id")
    label = report.get("epoch_label")
    slot_index = report.get("slot_index")
    if not isinstance(epoch_hex, str) or len(epoch_hex) != 32:
        raise RuntimeError("Remanence recipient report has invalid recipient_epoch_id")
    try:
        epoch_id = bytes.fromhex(epoch_hex)
    except ValueError as exc:
        raise RuntimeError("Remanence recipient report epoch id is not hexadecimal") from exc
    if epoch_hex != epoch_hex.lower() or epoch_id == bytes(16):
        raise RuntimeError("Remanence recipient report has non-canonical recipient_epoch_id")
    if not isinstance(label, str) or not label:
        raise RuntimeError("Remanence recipient report has invalid epoch_label")
    if type(slot_index) is not int or not 0 <= slot_index <= 255:
        raise RuntimeError("Remanence recipient report has invalid slot_index")
    public_key_bytes = report.get("public_key_bytes") if include_public_size else 1216
    if public_key_bytes != 1216:
        raise RuntimeError("Remanence recipient report has invalid public_key_bytes")
    return RecipientPublicIdentity(
        recipient_epoch_id=epoch_id,
        epoch_label=label,
        slot_index=slot_index,
        public_key_bytes=1216,
    )
