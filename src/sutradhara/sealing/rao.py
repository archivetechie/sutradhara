"""Remanence RAO CLI implementation of the Sutradhara sealing port.

This module wraps `rem archive build/inspect/extract` as Sutradhara's
stateless file codec. It seals one local plaintext file into deterministic RAO
objects for backend storage, opens stored RAO objects back to plaintext for
self-heal, and maps Remanence's JSON reports into the catalog representation
strings used by the replication policy.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import subprocess
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sutradhara.keys import KeyEpoch, KeyRegistry
from sutradhara.rem_archive_cli import (
    resolve_rem_bin as _resolve_rem_bin,
)
from sutradhara.rem_archive_cli import (
    run_rem_archive_build,
)
from sutradhara.resource_control import run_managed
from sutradhara.sealing.port import Representation, SealResult

RAO_CHUNK_SIZE = 262144
RAO_TIMESTAMP = "2026-01-01T00:00:00Z"

_DIGEST_SIZE = 32
_REM_REPRESENTATIONS = {
    "plaintext": Representation.RAO_PLAIN_V1,
    "encrypted": Representation.RAO_AEAD_V1,
}
_ID_LABELS = {
    "object_id": b"sutradhara:rao:object-id:v1\0",
    "caller_object_id": b"sutradhara:rao:caller-object-id:v1\0",
    "manifest_file_id": b"sutradhara:rao:manifest-file-id:v1\0",
}


@dataclass(frozen=True)
class RaoInspection:
    """Keyless Remanence RAO inspection mapped into Sutradhara vocabulary."""

    representation: Representation
    key_id: str | None
    report: dict[str, Any]


class RaoCliSealer:
    """Seal local files by shelling out to Remanence's RAO CLI."""

    def __init__(self, keys: KeyRegistry | None = None) -> None:
        self._keys = keys or KeyRegistry()

    @contextlib.contextmanager
    def seal(
        self,
        source_path: Path | str,
        representation: Representation,
        *,
        key_epoch: KeyEpoch | None = None,
    ) -> Iterator[SealResult]:
        """Yield a local sealed representation, removing temp files on exit."""
        source = Path(source_path)
        plaintext_digest = _sha256_file(source)

        if representation in {Representation.RAW_BYTES, Representation.D2TAR_RAW}:
            yield SealResult(
                sealed_path=source,
                stored_digest=plaintext_digest,
                plaintext_digest=plaintext_digest,
                representation=representation,
            )
            return

        with tempfile.TemporaryDirectory(prefix="sutradhara-rao-") as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            os.chmod(temp_dir, 0o700)
            sealed_path = temp_dir / "sealed.rao"

            if representation is Representation.RAO_PLAIN_V1:
                report = _build_rao(
                    source,
                    sealed_path,
                    representation=representation,
                    plaintext_digest=plaintext_digest,
                )
            elif representation is Representation.RAO_AEAD_V1:
                if key_epoch is None:
                    raise ValueError("rao-aead-v1 sealing requires key_epoch")
                with self._keys.materialized_root_key(key_epoch.key_id) as key_file:
                    report = _build_rao(
                        source,
                        sealed_path,
                        representation=representation,
                        plaintext_digest=plaintext_digest,
                        key_id=key_epoch.key_id,
                        key_file=key_file,
                    )
            else:  # pragma: no cover - enum exhaustiveness
                raise ValueError(f"unsupported representation: {representation}")

            _assert_report_representation(report, representation)
            file_digest = _single_member_digest_from_build_report(report, source.name)
            if file_digest != plaintext_digest:
                raise RuntimeError(
                    "RAO build file_sha256 differs from source digest: "
                    f"{file_digest.hex()} != {plaintext_digest.hex()}"
                )
            report_stored_digest = _digest_from_report(report, "stored_digest")
            local_stored_digest = _sha256_file(sealed_path)
            if report_stored_digest != local_stored_digest:
                raise RuntimeError(
                    "RAO build stored_digest differs from sealed bytes: "
                    f"{report_stored_digest.hex()} != {local_stored_digest.hex()}"
                )
            yield SealResult(
                sealed_path=sealed_path,
                stored_digest=local_stored_digest,
                plaintext_digest=plaintext_digest,
                representation=representation,
            )


class RaoCliOpener:
    """Open local Remanence RAO objects back to plaintext via the CLI."""

    def __init__(self, keys: KeyRegistry | None = None) -> None:
        self._keys = keys or KeyRegistry()

    @contextlib.contextmanager
    def open(
        self,
        source_path: Path | str,
        representation: Representation,
        *,
        key_epoch: KeyEpoch | None = None,
    ) -> Iterator[Path]:
        """Yield a local plaintext file for one stored representation."""
        source = Path(source_path)

        if representation in {Representation.RAW_BYTES, Representation.D2TAR_RAW}:
            yield source
            return

        with tempfile.TemporaryDirectory(prefix="sutradhara-rao-open-") as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            os.chmod(temp_dir, 0o700)
            if representation is Representation.RAO_PLAIN_V1:
                _extract_rao(source, temp_dir, representation=representation)
            elif representation is Representation.RAO_AEAD_V1:
                if key_epoch is None:
                    raise ValueError("rao-aead-v1 opening requires key_epoch")
                with self._keys.materialized_root_key(key_epoch.key_id) as key_file:
                    _extract_rao(
                        source,
                        temp_dir,
                        representation=representation,
                        key_file=key_file,
                    )
            else:  # pragma: no cover - enum exhaustiveness
                raise ValueError(f"unsupported representation: {representation}")

            yield _single_restored_member(temp_dir)


def inspect_rao(path: Path | str) -> RaoInspection:
    """Inspect a RAO object and recover its Sutradhara representation.

    Encrypted RAO headers expose the key identifier without key material.
    Plaintext RAO inspection needs the contract chunk size because the stored
    object is a fixed-block tar stream.
    """
    source = Path(path)
    args = ["archive", "inspect", "--object", str(source)]
    if not _looks_encrypted(source):
        args.extend(["--chunk-size", str(RAO_CHUNK_SIZE)])
    report = _json_report(_run_rem(args, role="medium"))
    rem_representation = report.get("representation")
    if not isinstance(rem_representation, str):
        raise RuntimeError(f"RAO inspect did not report representation: {report!r}")
    try:
        representation = _REM_REPRESENTATIONS[rem_representation]
    except KeyError as exc:
        raise RuntimeError(f"unknown RAO representation {rem_representation!r}") from exc
    key_id = report.get("key_id")
    if representation is Representation.RAO_AEAD_V1:
        if not isinstance(key_id, str) or not key_id:
            raise RuntimeError(f"encrypted RAO inspect did not report key_id: {report!r}")
        return RaoInspection(representation=representation, key_id=key_id, report=report)
    return RaoInspection(representation=representation, key_id=None, report=report)


def resolve_rem_bin() -> str:
    """Resolve the Remanence CLI path used by Sutradhara."""
    return _resolve_rem_bin()


def _build_rao(
    source: Path,
    sealed_path: Path,
    *,
    representation: Representation,
    plaintext_digest: bytes,
    key_id: str | None = None,
    key_file: Path | None = None,
) -> dict[str, Any]:
    ids = _deterministic_ids(
        plaintext_digest=plaintext_digest,
        basename=source.name,
        representation=representation,
        key_id=key_id,
    )
    if representation is Representation.RAO_AEAD_V1:
        if key_file is None or key_id is None:
            raise ValueError("encrypted RAO build requires key_file and key_id")
    elif key_file is not None or key_id is not None:
        raise ValueError("key_file/key_id are only valid for encrypted RAO builds")
    result = run_rem_archive_build(
        inputs=[source],
        ruleset=None,
        output_path=sealed_path,
        chunk_size=RAO_CHUNK_SIZE,
        object_id=ids["object_id"],
        caller_object_id=ids["caller_object_id"],
        manifest_file_id=ids["manifest_file_id"],
        timestamp=RAO_TIMESTAMP,
        encrypt=representation is Representation.RAO_AEAD_V1,
        key_id=key_id,
        key_file=key_file,
        failure_label="rem archive build",
    )
    return result.stdout_report


def _extract_rao(
    source: Path,
    dest: Path,
    *,
    representation: Representation,
    key_file: Path | None = None,
) -> dict[str, Any]:
    args = [
        "archive",
        "extract",
        "--object",
        str(source),
        "--dest",
        str(dest),
    ]
    if representation is Representation.RAO_PLAIN_V1:
        args.extend(["--chunk-size", str(RAO_CHUNK_SIZE)])
    elif representation is Representation.RAO_AEAD_V1:
        if key_file is None:
            raise ValueError("encrypted RAO extract requires key_file")
        args.extend(["--key-file", str(key_file)])
    else:
        raise ValueError(f"unsupported RAO representation: {representation}")
    return _json_report(_run_rem(args, role="high"))


def _deterministic_ids(
    *,
    plaintext_digest: bytes,
    basename: str,
    representation: Representation,
    key_id: str | None,
) -> dict[str, str]:
    return {
        name: _uuid_from_contract_seed(
            label,
            plaintext_digest=plaintext_digest,
            basename=basename,
            representation=representation,
            key_id=key_id,
        )
        for name, label in _ID_LABELS.items()
    }


def _uuid_from_contract_seed(
    label: bytes,
    *,
    plaintext_digest: bytes,
    basename: str,
    representation: Representation,
    key_id: str | None,
) -> str:
    h = hashlib.sha256()
    h.update(label)
    h.update(plaintext_digest)
    h.update(b"\0")
    h.update(basename.encode("utf-8"))
    h.update(b"\0")
    h.update(representation.value.encode("ascii"))
    h.update(b"\0")
    h.update((key_id or "").encode("ascii"))
    raw = bytearray(h.digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    text = raw.hex()
    return f"{text[:8]}-{text[8:12]}-{text[12:16]}-{text[16:20]}-{text[20:]}"


def _assert_report_representation(
    report: dict[str, Any],
    expected: Representation,
) -> None:
    rem_value = report.get("representation")
    if not isinstance(rem_value, str):
        raise RuntimeError(f"RAO build reported unexpected representation: {report!r}")
    try:
        actual = _REM_REPRESENTATIONS[rem_value]
    except KeyError as exc:
        raise RuntimeError(f"RAO build reported unexpected representation: {report!r}") from exc
    if actual is not expected:
        raise RuntimeError(f"RAO build representation drift: {actual.value} != {expected.value}")
    chunk_size = report.get("chunk_size")
    if chunk_size != RAO_CHUNK_SIZE:
        raise RuntimeError(f"RAO build chunk_size drift: {chunk_size!r} != {RAO_CHUNK_SIZE}")


def _single_member_digest_from_build_report(report: dict[str, Any], basename: str) -> bytes:
    files = report.get("files")
    if not isinstance(files, list):
        raise RuntimeError(f"RAO build report has no file list: {report!r}")
    matches = [
        row
        for row in files
        if isinstance(row, dict)
        and row.get("entry_type") == "regular"
        and row.get("path") == basename
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"RAO build expected one regular member {basename!r}, got {len(matches)}"
        )
    digest = matches[0].get("file_sha256")
    if not isinstance(digest, str):
        raise RuntimeError(f"RAO build file row missing file_sha256: {matches[0]!r}")
    return _digest_from_hex(digest, field="files[].file_sha256")


def _single_restored_member(dest: Path) -> Path:
    files = [path for path in dest.rglob("*") if path.is_file()]
    if len(files) != 1:
        raise RuntimeError(f"RAO extract expected one regular file, got {len(files)} under {dest}")
    return files[0]


def _looks_encrypted(path: Path) -> bool:
    with path.open("rb") as handle:
        return handle.read(4) == b"RAO1"


def _run_rem(
    args: list[str],
    *,
    role: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    cmd = [resolve_rem_bin(), *args]
    result = run_managed(cmd, role=role, capture_output=True, text=True, check=False)
    if check and result.returncode != 0:
        raise RuntimeError(
            f"rem {' '.join(args[:2])} failed (exit {result.returncode}): "
            f"stdout={result.stdout.strip()[:500]!r} "
            f"stderr={result.stderr.strip()[:500]!r}"
        )
    return result


def _json_report(result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            data = json.loads(line)
            if isinstance(data, dict):
                return data
    raise RuntimeError(
        "Remanence RAO command emitted no JSON object: "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )


def _digest_from_report(report: dict[str, Any], key: str) -> bytes:
    value = report.get(key)
    if not isinstance(value, str):
        raise RuntimeError(f"RAO report missing {key!r}: {report!r}")
    return _digest_from_hex(value, field=key)


def _digest_from_hex(value: str, *, field: str) -> bytes:
    try:
        digest = bytes.fromhex(value)
    except ValueError as exc:
        raise RuntimeError(f"RAO report {field!r} is not hex: {value!r}") from exc
    if len(digest) != _DIGEST_SIZE:
        raise RuntimeError(f"RAO report {field!r} must be {_DIGEST_SIZE} bytes, got {len(digest)}")
    return digest


def _sha256_file(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()
