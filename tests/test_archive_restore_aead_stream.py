"""Integration tests for bounded, fail-closed RAO AEAD plan restores."""

from __future__ import annotations

import hashlib
import stat
import subprocess
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import Engine

from sutradhara import archive_restore as archive_restore_module
from sutradhara.archive_restore import (
    RemArchiveExtractor,
    RestoreIntegrityError,
    build_restore_plan,
    restore_asset,
)
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    DurabilityPolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
)
from sutradhara.backend.port import BackendLocator, ByteRange, CopyRecord, StreamKind, VerifyResult
from sutradhara.catalog.models import (
    AssetLocator,
    Backend,
    Bundle,
    BundleMember,
    Copy,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, locator_key, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource
from sutradhara.keys import KeyRegistry
from sutradhara.rem_archive_cli import resolve_rem_bin
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RaoCliSealer


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


class _StreamingObjectBackend:
    """Test backend that exposes objects only through bounded range chunks."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.max_chunk_seen = 0
        self.whole_reads = 0

    @property
    def name(self) -> str:
        return "aead-stream"

    @property
    def stream_kind(self) -> StreamKind:
        return StreamKind.native_stream

    def add(self, object_id: str, data: bytes) -> BackendLocator:
        self.objects[object_id] = data
        return {"object_id": object_id}

    def enumerate(self) -> Iterator[CopyRecord]:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        if byte_range.is_whole_object:
            self.whole_reads += 1
        data = self.objects[str(locator["object_id"])]
        if byte_range.is_whole_object:
            return data
        return data[byte_range.start : byte_range.end]

    @contextmanager
    def open_range_chunks(
        self,
        locator: BackendLocator,
        byte_range: ByteRange,
        *,
        chunk_bytes: int,
    ) -> Iterator[Iterator[bytes]]:
        data = self.objects[str(locator["object_id"])]
        end = len(data) if byte_range.is_whole_object else byte_range.end

        def chunks() -> Iterator[bytes]:
            for cursor in range(byte_range.start, end, chunk_bytes):
                chunk = data[cursor : min(cursor + chunk_bytes, end)]
                self.max_chunk_seen = max(self.max_chunk_seen, len(chunk))
                yield chunk

        yield chunks()

    def verify(self, locator: BackendLocator) -> VerifyResult:
        del locator
        return VerifyResult(ok=True)


def _sha(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _write_stream_helper(path: Path) -> Path:
    path.write_text(
        """#!/usr/bin/env python3
import sys

while True:
    chunk = sys.stdin.buffer.read(65536)
    if not chunk:
        break
    if chunk.startswith((b"CORRUPT", b"TRUNCATED")):
        sys.stdout.buffer.write(b"untrusted-prefix")
        sys.stdout.buffer.flush()
        sys.stdin.buffer.read()
        sys.stderr.write("error: archive extract-stream: invalid encrypted object\\n")
        raise SystemExit(1)
    sys.stdout.buffer.write(chunk)
    sys.stdout.buffer.flush()
sys.stderr.write('{"command":"archive extract-stream","status":"ok"}\\n')
""",
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _install_candidates(
    engine: Engine,
    backend: _StreamingObjectBackend,
    *,
    logical: bytes,
    stored_candidates: list[bytes],
    key_epoch: str,
) -> tuple[int, list[int]]:
    asset_hash = _sha(logical)
    with session_scope(engine) as session:
        backend_row = Backend(
            name="aead-stream",
            kind=BackendKind.REM_TAPE,
            tier=BackendTier.SELF_DESCRIBING,
        )
        session.add(backend_row)
        session.flush()
        pool_ids = [f"aead-pool-{index}" for index in range(len(stored_candidates))]
        session.add_all(
            Pool(
                id=pool_id,
                backend_id=backend_row.id,
                representation=Representation.RAO_AEAD_V1.value,
            )
            for pool_id in pool_ids
        )
        session.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(logical)))
        session.add(
            Bundle(
                id="aead-test-bundle",
                artifactclass="aead-test",
                status="sealed",
                total_bytes=len(logical),
                member_count=1,
            )
        )
        session.flush()
        session.add(
            BundleMember(
                bundle_id="aead-test-bundle",
                logical_asset_hash=asset_hash,
                member_path="asset.bin",
                size_bytes=len(logical),
                file_sha256=asset_hash,
            )
        )
        apply_artifactclass_policy(
            session,
            "aead-test",
            ArtifactClassPolicy(
                ruleset="rao.aead.stream.test",
                placements=tuple(PlacementPolicy(pool_id) for pool_id in pool_ids),
                bundling=BundlingPolicy(target_gb=1, max_age_seconds=60),
                restore_preference=tuple(pool_ids),
                expect="messy",
                durability=DurabilityPolicy(min_copies=1, min_impl_families=1),
            ),
        )
        copy_ids: list[int] = []
        for index, (pool_id, stored) in enumerate(zip(pool_ids, stored_candidates, strict=True)):
            native = backend.add(f"object-{index}", stored)
            copy = Copy(
                logical_asset_hash=None,
                bundle_id="aead-test-bundle",
                backend_id=backend_row.id,
                pool_id=pool_id,
                native_locator=native,
                native_locator_key=locator_key(native),
                storage_metadata={
                    "representation": Representation.RAO_AEAD_V1.value,
                    "key_epoch": key_epoch,
                    "stored_size_bytes": len(stored),
                },
                integrity_hash=_sha(stored),
                source=CopySource.INGEST,
                health=CopyHealth.OK,
            )
            session.add(copy)
            session.flush()
            copy_ids.append(copy.id)
            session.add(
                AssetLocator(
                    logical_asset_hash=asset_hash,
                    pool_id=pool_id,
                    copy_id=copy.id,
                    bundle_id="aead-test-bundle",
                    member_path="asset.bin",
                    native_locator={
                        "member_path": "asset.bin",
                        # RAO reserves chunk 0 for the pax/rao header; the member payload
                        # begins at chunk 1 (member_byte_base = 1 * RAO_CHUNK_SIZE), matching
                        # the real sealed object's data_offset. (Was 0 — which sliced the
                        # framing, not the member, and only surfaced against a real rem.)
                        "first_chunk_lba": 1,
                        "size_bytes": len(logical),
                    },
                    representation=Representation.RAO_AEAD_V1.value,
                )
            )
        return backend_row.id, copy_ids


def test_real_encrypted_copy_round_trips_through_unbuffered_plan(
    engine: Engine,
    tmp_path: Path,
) -> None:
    rem_bin = _extract_stream_rem_bin_or_skip()
    source = tmp_path / "source.bin"
    logical = b"real encrypted streaming restore" * 4096
    source.write_bytes(logical)
    registry = KeyRegistry(tmp_path / "keys")
    epoch = registry.create_epoch()
    backend = _StreamingObjectBackend()
    with RaoCliSealer(registry).seal(
        source,
        Representation.RAO_AEAD_V1,
        key_epoch=epoch,
    ) as sealed:
        stored = sealed.sealed_path.read_bytes()
    backend_id, _copy_ids = _install_candidates(
        engine,
        backend,
        logical=logical,
        stored_candidates=[stored],
        key_epoch=epoch.key_id,
    )

    with session_scope(engine) as session:
        extractor = RemArchiveExtractor(rem_bin, keys=registry)
        with build_restore_plan(
            session,
            asset_hash=_sha(logical),
            artifactclass="aead-test",
            backends={backend_id: backend},
            extractor=extractor,
        ) as plan:
            [member] = list(plan.iter_members())
            assert member.buffered is False
        result = restore_asset(
            session,
            asset_hash=_sha(logical),
            artifactclass="aead-test",
            destination=tmp_path / "restored.bin",
            backends={backend_id: backend},
            extractor=extractor,
        )

    assert result.output_path.read_bytes() == logical
    assert _sha(result.output_path.read_bytes()) == _sha(logical)
    assert backend.whole_reads == 0


@pytest.mark.parametrize("broken", [b"CORRUPT ciphertext", b"TRUNCATED object"])
def test_helper_failure_discards_stdout_and_leaves_no_destination(
    engine: Engine,
    tmp_path: Path,
    broken: bytes,
) -> None:
    logical = b"verified fallback"
    backend = _StreamingObjectBackend()
    registry = KeyRegistry(tmp_path / "keys")
    epoch = registry.create_epoch()
    backend_id, copy_ids = _install_candidates(
        engine,
        backend,
        logical=logical,
        stored_candidates=[broken],
        key_epoch=epoch.key_id,
    )
    destination = tmp_path / "fallback.bin"

    with session_scope(engine) as session:
        with pytest.raises(RestoreIntegrityError, match="extract-stream failed"):
            restore_asset(
                session,
                asset_hash=_sha(logical),
                artifactclass="aead-test",
                destination=destination,
                backends={backend_id: backend},
                extractor=RemArchiveExtractor(
                    _write_stream_helper(tmp_path / "fake-rem"), keys=registry
                ),
            )
        first = session.get(Copy, copy_ids[0])
        assert first is not None
        assert first.health == CopyHealth.OK

    assert not destination.exists()
    assert list(tmp_path.glob(".fallback.bin.*.tmp")) == []


def test_helper_failure_falls_through_to_next_candidate(engine: Engine, tmp_path: Path) -> None:
    logical = b"verified fallback"
    backend = _StreamingObjectBackend()
    registry = KeyRegistry(tmp_path / "keys")
    epoch = registry.create_epoch()
    backend_id, copy_ids = _install_candidates(
        engine,
        backend,
        logical=logical,
        stored_candidates=[b"CORRUPT ciphertext", logical],
        key_epoch=epoch.key_id,
    )

    with session_scope(engine) as session:
        result = restore_asset(
            session,
            asset_hash=_sha(logical),
            artifactclass="aead-test",
            destination=tmp_path / "fallback-success.bin",
            backends={backend_id: backend},
            extractor=RemArchiveExtractor(
                _write_stream_helper(tmp_path / "fallback-rem"), keys=registry
            ),
        )
        first = session.get(Copy, copy_ids[0])
        assert first is not None
        assert first.health == CopyHealth.OK

    assert result.copy_id == copy_ids[1]
    assert result.output_path.read_bytes() == logical


def test_hung_helper_is_terminated_with_no_reveal(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    helper = tmp_path / "hung-rem"
    helper.write_text(
        """#!/usr/bin/env python3
import sys
import time
sys.stdin.buffer.read()
time.sleep(60)
""",
        encoding="utf-8",
    )
    helper.chmod(helper.stat().st_mode | stat.S_IXUSR)
    logical = b"must never appear"
    backend = _StreamingObjectBackend()
    registry = KeyRegistry(tmp_path / "keys")
    epoch = registry.create_epoch()
    backend_id, _copy_ids = _install_candidates(
        engine,
        backend,
        logical=logical,
        stored_candidates=[logical],
        key_epoch=epoch.key_id,
    )
    monkeypatch.setattr(archive_restore_module, "_REM_STREAM_INACTIVITY_TIMEOUT_SECONDS", 0.1)
    destination = tmp_path / "hung.bin"

    with (
        session_scope(engine) as session,
        pytest.raises(RestoreIntegrityError, match="inactivity timeout"),
    ):
        restore_asset(
            session,
            asset_hash=_sha(logical),
            artifactclass="aead-test",
            destination=destination,
            backends={backend_id: backend},
            extractor=RemArchiveExtractor(helper, keys=registry),
        )

    assert not destination.exists()
    assert list(tmp_path.glob(".hung.bin.*.tmp")) == []


def test_large_duplex_restore_has_bounded_rss_and_does_not_deadlock(
    engine: Engine,
    tmp_path: Path,
) -> None:
    logical = b"z" * (16 * 1024 * 1024)
    backend = _StreamingObjectBackend()
    registry = KeyRegistry(tmp_path / "keys")
    epoch = registry.create_epoch()
    backend_id, _copy_ids = _install_candidates(
        engine,
        backend,
        logical=logical,
        stored_candidates=[logical],
        key_epoch=epoch.key_id,
    )
    baseline_rss = _rss_bytes()
    peak_rss = [baseline_rss]
    stop = threading.Event()

    def sample_rss() -> None:
        while not stop.wait(0.002):
            peak_rss[0] = max(peak_rss[0], _rss_bytes())

    sampler = threading.Thread(target=sample_rss, daemon=True)
    sampler.start()
    try:
        with session_scope(engine) as session:
            result = restore_asset(
                session,
                asset_hash=_sha(logical),
                artifactclass="aead-test",
                destination=tmp_path / "large.bin",
                backends={backend_id: backend},
                extractor=RemArchiveExtractor(
                    _write_stream_helper(tmp_path / "duplex-rem"), keys=registry
                ),
            )
    finally:
        stop.set()
        sampler.join()

    assert result.output_path.stat().st_size == len(logical)
    assert _sha(result.output_path.read_bytes()) == _sha(logical)
    assert backend.max_chunk_seen <= 256 * 1024
    assert backend.whole_reads == 0
    assert peak_rss[0] - baseline_rss < len(logical) // 2


def _rss_bytes() -> int:
    """Read this process's resident set without including the helper child."""

    for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("/proc/self/status did not report VmRSS")


def _extract_stream_rem_bin_or_skip() -> str:
    """Require a built Remanence binary containing the RM0.3a command."""

    rem_bin = resolve_rem_bin()
    result = subprocess.run(
        [rem_bin, "archive", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    if "extract-stream" not in result.stdout:
        pytest.skip(f"built Remanence binary lacks extract-stream: {rem_bin}")
    return rem_bin
