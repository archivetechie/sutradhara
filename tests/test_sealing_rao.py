"""Remanence RAO CLI sealing tests.

The unit tests drive a fake `rem` binary so the sealing port's command
construction, digest mapping, inspection, cleanup, and pass-through behavior
stay hermetic. The integration test uses the real binary when available to
prove plaintext/encrypted round trips, missing-key failure, and byte-stable
re-sealing.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import textwrap
from pathlib import Path

import pytest

from sutradhara.keys import KeyRegistry
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import (
    RAO_CHUNK_SIZE,
    RaoCliOpener,
    RaoCliSealer,
    inspect_rao,
    resolve_rem_bin,
)
from tests.key_helpers import registry_with_recovery


def _rem_bin_or_skip() -> str:
    try:
        resolved = resolve_rem_bin()
    except FileNotFoundError as exc:
        pytest.skip(str(exc))
    if not os.access(resolved, os.X_OK):
        pytest.skip(f"rem is not executable: {resolved}")
    return resolved


def _fake_rem_bin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    script = tmp_path / "rem"
    script.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            import hashlib
            import json
            import sys
            from pathlib import Path

            def opt(name):
                try:
                    return sys.argv[sys.argv.index(name) + 1]
                except ValueError:
                    return None

            def opts(name):
                return [sys.argv[index + 1] for index, value in enumerate(sys.argv) if value == name]

            def recipient(path):
                raw = Path(path).read_bytes()
                label_len = raw[21]
                return {
                    "epoch_id": raw[5:21].hex(),
                    "label": raw[22:22 + label_len].decode(),
                }

            def emit(value):
                print(json.dumps(value, sort_keys=True))

            command = sys.argv[1:3]
            if command == ["archive", "build"]:
                source = Path(opt("--inputs"))
                out = Path(opt("--out"))
                recipients = [recipient(path) for path in opts("--recipient")]
                encrypt = bool(recipients)
                data = source.read_bytes()
                body = {
                    "data_hex": data.hex(),
                    "name": source.name,
                    "object_id": opt("--object-id"),
                    "recipient_epochs": recipients,
                    "representation": "encrypted" if encrypt else "plaintext",
                }
                payload = json.dumps(body, sort_keys=True).encode()
                if encrypt:
                    payload = b"RAO1" + payload
                out.write_bytes(payload)
                stored_digest = hashlib.sha256(payload).hexdigest()
                file_digest = hashlib.sha256(data).hexdigest()
                emit({
                    "body_format": "rao-v1",
                    "chunk_size": int(opt("--chunk-size")),
                    "encryption": "RAO1" if encrypt else "none",
                    "files": [{
                        "entry_type": "regular",
                        "file_sha256": file_digest,
                        "path": source.name,
                        "size_bytes": len(data),
                    }],
                    "format_version": 2 if encrypt else None,
                    "recipient_epochs": recipients if encrypt else None,
                    "plaintext_digest": hashlib.sha256(b"inner:" + data).hexdigest(),
                    "representation": "encrypted" if encrypt else "plaintext",
                    "stored_digest": stored_digest,
                })
            elif command == ["archive", "inspect"]:
                obj = Path(opt("--object")).read_bytes()
                encrypted = obj.startswith(b"RAO1")
                body = json.loads((obj[4:] if encrypted else obj).decode())
                emit({
                    "body_format": "rao-v1",
                    "chunk_size": int(opt("--chunk-size") or 262144),
                    "encryption": "RAO1" if encrypted else "none",
                    "format_version": 2 if encrypted else None,
                    "recipient_epochs": body["recipient_epochs"] if encrypted else None,
                    "keyed": False if encrypted else None,
                    "representation": body["representation"],
                    "stored_digest": hashlib.sha256(obj).hexdigest(),
                })
            elif command == ["archive", "extract"]:
                obj = Path(opt("--object")).read_bytes()
                encrypted = obj.startswith(b"RAO1")
                if encrypted and opt("--private-key") is None:
                    print("error: encrypted RAO extract requires --private-key", file=sys.stderr)
                    sys.exit(1)
                body = json.loads((obj[4:] if encrypted else obj).decode())
                dest = Path(opt("--dest"))
                dest.mkdir(parents=True, exist_ok=True)
                (dest / body["name"]).write_bytes(bytes.fromhex(body["data_hex"]))
                emit({
                    "bytes_written": len(bytes.fromhex(body["data_hex"])),
                    "files_written": 1,
                    "representation": body["representation"],
                    "stored_digest": hashlib.sha256(obj).hexdigest(),
                })
            else:
                print(f"unexpected command: {sys.argv}", file=sys.stderr)
                sys.exit(2)
            """
        )
    )
    script.chmod(0o700)
    monkeypatch.setenv("REM_BIN", str(script))
    return script


def _sha256(path: Path) -> bytes:
    return hashlib.sha256(path.read_bytes()).digest()


def test_rao_cli_sealer_plain_round_trip_with_fake_cli(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_rem_bin(tmp_path, monkeypatch)
    source = tmp_path / "asset.bin"
    source.write_bytes(b"rao plain round trip")
    source_digest = _sha256(source)
    sealer = RaoCliSealer(KeyRegistry(tmp_path / "keys"))
    opener = RaoCliOpener(KeyRegistry(tmp_path / "keys"))

    with sealer.seal(source, Representation.RAO_PLAIN_V1) as result:
        sealed_path = result.sealed_path
        sealed_parent = sealed_path.parent
        assert result.plaintext_digest == source_digest
        assert result.stored_digest == _sha256(sealed_path)
        inspection = inspect_rao(sealed_path)
        assert inspection.representation is Representation.RAO_PLAIN_V1
        assert inspection.format_version is None
        assert inspection.recipient_epochs == ()
        assert inspection.report["chunk_size"] == RAO_CHUNK_SIZE
        with opener.open(sealed_path, Representation.RAO_PLAIN_V1) as opened:
            assert opened.read_bytes() == source.read_bytes()

    assert not sealed_path.exists()
    assert not sealed_parent.exists()


def test_rao_cli_sealer_uses_configured_work_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_rem_bin(tmp_path, monkeypatch)
    source = tmp_path / "asset.bin"
    source.write_bytes(b"work dir")
    work_dir = tmp_path / "scratch"
    sealer = RaoCliSealer(KeyRegistry(tmp_path / "keys"), work_dir=work_dir)

    with sealer.seal(source, Representation.RAO_PLAIN_V1) as result:
        assert result.sealed_path.is_relative_to(work_dir)
        assert result.sealed_path.exists()

    assert work_dir.is_dir()


def test_rao_cli_sealer_encrypted_round_trip_and_key_id_with_fake_cli(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_rem_bin(tmp_path, monkeypatch)
    source = tmp_path / "asset.bin"
    source.write_bytes(b"rao encrypted round trip")
    registry, recovery = registry_with_recovery(tmp_path / "keys")
    epoch = registry.create_epoch()
    sealer = RaoCliSealer(registry)
    opener = RaoCliOpener(registry)

    with sealer.seal(source, Representation.RAO_AEAD_V1, key_epoch=epoch) as result:
        sealed_path = result.sealed_path
        assert result.plaintext_digest == _sha256(source)
        assert result.stored_digest == _sha256(sealed_path)
        inspection = inspect_rao(sealed_path)
        assert inspection.representation is Representation.RAO_AEAD_V1
        assert inspection.format_version == 2
        assert [item.label for item in inspection.recipient_epochs] == [
            epoch.key_id,
            recovery.key_id,
        ]
        assert result.recipient_epochs == (epoch.key_id, recovery.key_id)
        with opener.open(
            sealed_path,
            Representation.RAO_AEAD_V1,
            recipient_epochs=result.recipient_epochs,
        ) as opened:
            assert opened.read_bytes() == source.read_bytes()


def test_rao_cli_sealer_cleans_temp_file_on_body_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_rem_bin(tmp_path, monkeypatch)
    source = tmp_path / "asset.bin"
    source.write_bytes(b"cleanup")
    sealer = RaoCliSealer(KeyRegistry(tmp_path / "keys"))
    paths: dict[str, Path] = {}

    def force_body_exception() -> None:
        with sealer.seal(source, Representation.RAO_PLAIN_V1) as result:
            paths["sealed_path"] = result.sealed_path
            paths["sealed_parent"] = result.sealed_path.parent
            raise RuntimeError("forced")

    with pytest.raises(RuntimeError, match="forced"):
        force_body_exception()

    assert not paths["sealed_path"].exists()
    assert not paths["sealed_parent"].exists()


def test_rao_cli_sealer_passes_through_d2tar_representation(tmp_path: Path) -> None:
    source = tmp_path / "asset.bin"
    source.write_bytes(b"d2tar plaintext")
    sealer = RaoCliSealer(KeyRegistry(tmp_path / "keys"))
    opener = RaoCliOpener(KeyRegistry(tmp_path / "keys"))
    source_digest = _sha256(source)

    with sealer.seal(source, Representation.D2TAR_RAW) as result:
        assert result.sealed_path == source
        assert result.stored_digest == source_digest
        assert result.plaintext_digest == source_digest
        assert result.representation is Representation.D2TAR_RAW

    with opener.open(source, Representation.D2TAR_RAW) as opened:
        assert opened == source


def test_rao_real_binary_round_trips_and_v2_recipients(tmp_path: Path) -> None:
    rem_bin = _rem_bin_or_skip()
    source = tmp_path / "asset.bin"
    source.write_bytes(b"real remanence rao integration")
    registry, recovery = registry_with_recovery(tmp_path / "keys")
    epoch = registry.create_epoch()
    sealer = RaoCliSealer(registry)
    opener = RaoCliOpener(registry)

    with sealer.seal(source, Representation.RAO_PLAIN_V1) as plain:
        plain_bytes = plain.sealed_path.read_bytes()
        assert plain.plaintext_digest == _sha256(source)
        assert plain.stored_digest == _sha256(plain.sealed_path)
        assert inspect_rao(plain.sealed_path).representation is Representation.RAO_PLAIN_V1
        with opener.open(plain.sealed_path, Representation.RAO_PLAIN_V1) as opened:
            assert opened.read_bytes() == source.read_bytes()

    with sealer.seal(source, Representation.RAO_PLAIN_V1) as resealed_plain:
        assert resealed_plain.sealed_path.read_bytes() == plain_bytes

    with sealer.seal(source, Representation.RAO_AEAD_V1, key_epoch=epoch) as encrypted:
        encrypted_bytes = encrypted.sealed_path.read_bytes()
        encrypted_path = tmp_path / "encrypted.rao"
        encrypted_path.write_bytes(encrypted_bytes)
        assert encrypted.plaintext_digest == _sha256(source)
        assert encrypted.stored_digest == _sha256(encrypted.sealed_path)
        inspection = inspect_rao(encrypted.sealed_path)
        assert inspection.representation is Representation.RAO_AEAD_V1
        assert inspection.format_version == 2
        assert encrypted.recipient_epochs == (epoch.key_id, recovery.key_id)
        assert [item.epoch_id for item in inspection.recipient_epochs] == [
            epoch.key_id.rsplit("-", 1)[1],
            recovery.key_id.rsplit("-", 1)[1],
        ]
        with opener.open(
            encrypted.sealed_path,
            Representation.RAO_AEAD_V1,
            recipient_epochs=encrypted.recipient_epochs,
        ) as opened:
            assert opened.read_bytes() == source.read_bytes()

    keyless = subprocess.run(
        [
            rem_bin,
            "archive",
            "extract",
            "--object",
            str(encrypted_path),
            "--dest",
            str(tmp_path / "keyless"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert keyless.returncode != 0
    assert "key" in (keyless.stdout + keyless.stderr).lower()
