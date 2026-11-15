"""Tests for the d2tape CLI backend adapter.

The adapter shells out to a Java CLI in production. These tests use a tiny
subprocess fake that accepts the same ``java -jar ...`` command shape, records
arguments, and writes deterministic JSON responses so the Sutradhara sidecar,
locator, read, and verify behavior is exercised without a tape drive.
"""

from __future__ import annotations

import hashlib
import json
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from sutradhara.backend.d2tape import D2TapeBackend
from sutradhara.backend.port import ByteRange, StorageBackend
from sutradhara.catalog.types import content_hash
from sutradhara.jobs.runtime_observations import bind_tape_locator_observer


def test_d2tape_backend_satisfies_storagebackend_protocol(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend, _ = _backend(tmp_path, monkeypatch)
    assert isinstance(backend, StorageBackend)
    assert backend.name == "d2-tape"


def test_write_read_verify_round_trip_and_sidecar_progression(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend, log_path = _backend(tmp_path, monkeypatch)
    first = tmp_path / "first.bin"
    second = tmp_path / "second.bin"
    first.write_bytes(b"first payload")
    second.write_bytes(b"second payload")
    first_hash = content_hash(hashlib.sha256(first.read_bytes()).digest())
    second_hash = content_hash(hashlib.sha256(second.read_bytes()).digest())
    touched_locators: list[dict[str, Any]] = []

    def observe_locator(
        locator: Mapping[str, Any],
        *,
        library: bytes | str | None = None,
    ) -> None:
        del library
        touched_locators.append(dict(locator))

    with bind_tape_locator_observer(observe_locator):
        first_record = backend.write_object_to_pool(first, "n-copy-3")
        second_record = backend.write_object_to_pool(second, "n-copy-3")

    assert first_record.logical_id == first_hash
    assert first_record.integrity_hash == first_hash
    assert first_record.native_locator == {
        "barcode": "D2T002L7",
        "volume_uuid": "00000000-0000-4000-8000-00000000000f",
        "artifact_name": f"n-{first_hash.hex()[:16]}",
        "start_block": 2,
        "end_block": 5,
        "volume_blocksize": 256000,
        "pool_id": "n-copy-3",
    }
    assert second_record.logical_id == second_hash
    assert second_record.native_locator["start_block"] == 9
    assert second_record.native_locator["end_block"] == 12

    with bind_tape_locator_observer(observe_locator):
        assert backend.read_range(first_record.native_locator, ByteRange(0, 0)) == b"first payload"
        assert backend.read_range(first_record.native_locator, ByteRange(6, 13)) == b"payload"
        with backend.open_materialized_range_chunks(
            first_record.native_locator,
            ByteRange(2, 11),
            chunk_bytes=3,
        ) as chunks:
            assert list(chunks) == [b"rst", b" pa", b"ylo"]
        verify = backend.verify(first_record.native_locator)
    assert verify.ok
    assert verify.measured is True
    assert verify.actual_hash == first_hash
    assert any(call["verb"] == "restore" for call in _calls(log_path))
    assert len(touched_locators) == 6
    assert all(locator["barcode"] == "D2T002L7" for locator in touched_locators)

    state = json.loads((tmp_path / "state" / "D2T002L7.json").read_text())
    assert state["last_end_block"] == 12
    assert [artifact["start_block"] for artifact in state["artifacts"]] == [2, 9]
    assert [artifact["end_block"] for artifact in state["artifacts"]] == [5, 12]
    assert all(artifact["verified"] is True for artifact in state["artifacts"])

    # The d2 sidecar remains truthful to the original write while restored
    # bytes are corrupted.  Verification must measure those bytes, not echo
    # the unchanged sidecar digest.
    sidecar_before = json.loads((tmp_path / "state" / "D2T002L7.json").read_text())
    store_path = tmp_path / "d2tape-store.json"
    store = json.loads(store_path.read_text())
    store[first_record.native_locator["artifact_name"]]["payload_hex"] = b"corrupt".hex()
    store_path.write_text(json.dumps(store, sort_keys=True) + "\n")
    corrupt_verify = backend.verify(first_record.native_locator)
    assert corrupt_verify.ok is False
    assert corrupt_verify.measured is True
    assert corrupt_verify.actual_hash != first_hash
    assert json.loads((tmp_path / "state" / "D2T002L7.json").read_text()) == sidecar_before
    assert [call["verb"] for call in _calls(log_path)[-2:]] == ["verify", "restore"]

    records = list(backend.enumerate())
    assert [record.logical_id for record in records] == [first_hash, second_hash]
    assert [record.native_locator["start_block"] for record in records] == [2, 9]

    writes = [_call for _call in _calls(log_path) if _call["verb"] == "write"]
    assert [_arg_after(call["args"], "--prev-end-block") for call in writes] == [
        "none",
        "5",
    ]


def _backend(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[D2TapeBackend, Path]:
    java, jar, log_path = _fake_d2_cli(tmp_path, monkeypatch)
    device_env = tmp_path / "device.env"
    device_env.write_text(
        "\n".join(
            [
                "D2TAPE_DEVICE=/dev/nst-test",
                "D2TAPE_BARCODE=D2T002L7",
                "D2TAPE_VOLUME_UUID=00000000-0000-4000-8000-00000000000f",
                "D2TAPE_VOLUME_BLOCKSIZE=256000",
                "D2TAPE_ARCHIVE_BLOCKSIZE=512",
                "",
            ]
        )
    )
    return (
        D2TapeBackend(
            "d2-tape",
            jar_path=jar,
            java_bin=java,
            device_env_path=device_env,
            state_dir=tmp_path / "state",
            timeout_seconds=10,
        ),
        log_path,
    )


def _fake_d2_cli(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, Path]:
    log_path = tmp_path / "d2tape-calls.jsonl"
    store_path = tmp_path / "d2tape-store.json"
    java = tmp_path / "fake-java"
    jar = tmp_path / "fake-d2tape.jar"
    jar.write_text("fake jar marker\n")
    java.write_text(
        """#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

args = sys.argv[1:]
if args[:2] == ["-jar", args[1] if len(args) > 1 else ""]:
    args = args[2:]
verb, rest = args[0], args[1:]

log_path = Path(os.environ["D2_FAKE_LOG"])
store_path = Path(os.environ["D2_FAKE_STORE"])
with log_path.open("a") as handle:
    handle.write(json.dumps({"verb": verb, "args": rest}) + "\\n")

def value(name, default=None):
    if name not in rest:
        return default
    return rest[rest.index(name) + 1]

def load_store():
    if store_path.exists():
        return json.loads(store_path.read_text())
    return {}

def save_store(store):
    store_path.write_text(json.dumps(store, sort_keys=True) + "\\n")

if verb == "write":
    source_dir = Path(value("--source-dir"))
    artifact = value("--artifact-name")
    hashes = json.loads(Path(value("--hashes")).read_text())
    relpath, digest = next(iter(hashes.items()))
    payload = (source_dir / relpath).read_bytes()
    prev = value("--prev-end-block")
    start = 2 if prev == "none" else int(prev) + 4
    end = start + 3
    store = load_store()
    store[artifact] = {"relpath": relpath, "payload_hex": payload.hex()}
    save_store(store)
    print(json.dumps({
        "artifactName": artifact,
        "artifactStartVolumeBlock": start,
        "artifactEndVolumeBlock": end,
        "artifactEndVolumeBlockOldWays": end,
        "artifactEndVolumeBlockNewWays": end,
        "files": [{
            "path": relpath,
            "linkName": None,
            "archiveBlock": 0,
            "volumeStartBlock": start,
            "volumeEndBlock": end,
            "size": len(payload),
            "headerBlocks": 3,
            "sha256": digest,
        }],
        "nativeLocators": [],
    }))
elif verb == "verify":
    hashes = json.loads(Path(value("--hashes")).read_text())
    print(json.dumps({
        "ok": True,
        "perFile": [{"path": path, "ok": True} for path in hashes],
    }))
elif verb == "restore":
    artifact = value("--artifact-name")
    dest = Path(value("--dest"))
    store = load_store()[artifact]
    out = dest / store["relpath"]
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(bytes.fromhex(store["payload_hex"]))
    print(json.dumps({"ok": True, "restoredPaths": [artifact]}))
else:
    print(f"unsupported verb {verb}", file=sys.stderr)
    sys.exit(2)
"""
    )
    java.chmod(java.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("D2_FAKE_LOG", str(log_path))
    monkeypatch.setenv("D2_FAKE_STORE", str(store_path))
    return java, jar, log_path


def _calls(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def _arg_after(args: list[str], flag: str) -> str:
    return args[args.index(flag) + 1]


def _lookup_artifact(digest, *, pool="pool", start=2, verified=False):
    """Complete sidecar entry, including writes not yet verified."""
    return {
        "artifact_name": f"n-{digest.hex()[:16]}",
        "barcode": "D2T002L7",
        "volume_uuid": "00000000-0000-4000-8000-00000000000f",
        "start_block": start,
        "end_block": start + 3,
        "volume_blocksize": 256000,
        "archive_blocksize": 512,
        "pool_id": pool,
        "logical_id": digest.hex(),
        "integrity_hash": digest.hex(),
        "size_bytes": 7,
        "verified": verified,
        "hashes": {},
        "relative_path": "payload.bin",
    }


def _lookup_backend(tmp_path, monkeypatch):
    """Lookup requires neither a device config nor a Java process."""
    backend = D2TapeBackend(
        "d2",
        jar_path=tmp_path / "unused.jar",
        java_bin="unused-java",
        device_env_path=tmp_path / "unused.env",
        state_dir=tmp_path / "sidecars",
    )

    def no_jar(*args, **kwargs):
        pytest.fail("pool lookup invoked the jar")

    monkeypatch.setattr(backend, "_run_d2tape", no_jar)
    (tmp_path / "sidecars").mkdir()
    return backend


def test_d2_pool_lookup_unverified_pool_filter_and_earliest(tmp_path, monkeypatch):
    """T10/M10: find unverified entries by digest within the requested pool only."""
    backend = _lookup_backend(tmp_path, monkeypatch)
    digest = hashlib.sha256(b"stored bytes").digest()
    # Another pool's earlier entry must not be chosen, even with the same digest.
    (tmp_path / "sidecars" / "a.json").write_text(
        json.dumps(
            {
                "artifacts": [
                    _lookup_artifact(digest, pool="other", start=1),
                    _lookup_artifact(digest, start=9),
                    _lookup_artifact(digest, start=20, verified=True),
                ]
            }
        )
    )
    (tmp_path / "sidecars" / "b.json").write_text(
        json.dumps(
            {
                "artifacts": [
                    _lookup_artifact(digest, start=1),
                ]
            }
        )
    )
    found = backend.lookup_pool_object("pool", "caller-name-not-stored", candidate_digests=[digest])
    assert found is not None
    assert found.copy_record.native_locator["pool_id"] == "pool"
    assert found.copy_record.native_locator["start_block"] == 9
    assert found.content_hash == digest
    assert backend.reissue_after_absent is False
    assert backend.holds_names is False
    assert backend.lookup_pool_object("absent-pool", "ignored", candidate_digests=[digest]) is None
    assert backend.lookup_pool_object("pool", "ignored", candidate_digests=[b"x" * 32]) is None


def test_d2_pool_lookup_conflicting_digests_and_empty_candidates(tmp_path, monkeypatch):
    """T10: two plausible identities or no candidates cannot answer absence."""
    from sutradhara.backend.port import BackendUnavailableError

    backend = _lookup_backend(tmp_path, monkeypatch)
    first, second = b"a" * 32, b"b" * 32
    (tmp_path / "sidecars" / "a.json").write_text(
        json.dumps({"artifacts": [_lookup_artifact(first)]})
    )
    (tmp_path / "sidecars" / "b.json").write_text(
        json.dumps({"artifacts": [_lookup_artifact(second)]})
    )
    with pytest.raises(BackendUnavailableError, match="multiple candidate"):
        backend.lookup_pool_object("pool", "ignored", candidate_digests=[first, second])
    with pytest.raises(BackendUnavailableError, match="requires candidate"):
        backend.lookup_pool_object("pool", "ignored", candidate_digests=[])


@pytest.mark.parametrize(
    "sidecar",
    ["not-json", "[]", "{}", '{"artifacts":{}}', '{"artifacts":[null]}', '{"artifacts":[{}]}'],
)
def test_d2_pool_lookup_malformed_sidecar_is_unavailable(tmp_path, monkeypatch, sidecar):
    from sutradhara.backend.port import BackendUnavailableError

    backend = _lookup_backend(tmp_path, monkeypatch)
    # A valid matching entry does not hide a later malformed sidecar.
    (tmp_path / "sidecars" / "a.json").write_text(
        json.dumps({"artifacts": [_lookup_artifact(b"a" * 32)]})
    )
    (tmp_path / "sidecars" / "z.json").write_text(sidecar)
    with pytest.raises(BackendUnavailableError):
        backend.lookup_pool_object("pool", "ignored", candidate_digests=[b"a" * 32])


def test_d2_pool_lookup_unreadable_directory_is_unavailable(tmp_path, monkeypatch):
    """Listing failure cannot be mistaken for absence; a new empty state can."""
    import os

    from sutradhara.backend.port import BackendUnavailableError

    backend = _lookup_backend(tmp_path, monkeypatch)
    assert backend.lookup_pool_object("pool", "ignored", candidate_digests=[b"a" * 32]) is None
    (tmp_path / "sidecars").rmdir()
    assert backend.lookup_pool_object("pool", "ignored", candidate_digests=[b"a" * 32]) is None
    (tmp_path / "sidecars").mkdir()
    original = os.scandir
    original_listdir = os.listdir

    def refuse_sidecars(path):
        if Path(path) == tmp_path / "sidecars":
            raise PermissionError("sidecar directory unreadable")
        return original(path)

    def refuse_listdir(path):
        if Path(path) == tmp_path / "sidecars":
            raise PermissionError("sidecar directory unreadable")
        return original_listdir(path)

    monkeypatch.setattr(os, "scandir", refuse_sidecars)
    monkeypatch.setattr(os, "listdir", refuse_listdir)
    with pytest.raises(BackendUnavailableError):
        backend.lookup_pool_object("pool", "ignored", candidate_digests=[b"a" * 32])
