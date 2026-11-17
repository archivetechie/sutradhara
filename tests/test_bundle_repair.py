"""Bundle-copy repair and reconciler tests."""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pytest
from sqlalchemy import Engine, select

from sutradhara.archive_bundle import add_bundle_member
from sutradhara.archive_fanout import (
    BuildArtifact,
    BuiltBlobRoot,
    BuiltExclusion,
    LocalArchiveBuilder,
    RemArchiveBuilder,
    flush_bundle,
)
from sutradhara.archive_restore import read_member_bytes
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    DurabilityPolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
)
from sutradhara.backend.port import (
    BackendError,
    BackendLocator,
    ByteRange,
    CopyRecord,
    StreamKind,
    VerifyObserver,
    VerifyResult,
)
from sutradhara.catalog.copies import add_bundle_copy
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    AssetLocator,
    Backend,
    BlobRoot,
    Bundle,
    Copy,
    ExclusionRecord,
    LogicalAsset,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, CopyHealth, CopySource, content_hash
from sutradhara.durability import BundleTarget, bundle_replication_status
from sutradhara.jobs import handlers as _handlers  # noqa: F401 -- register bundle-repair
from sutradhara.jobs.engine import run_one, submit
from sutradhara.jobs.handlers import bundle_repair
from sutradhara.jobs.models import Job, JobAttempt, JobStatus, ReconciliationCondition
from sutradhara.jobs.reconcilers import bundle_copy
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    CONDITION_BLOCKED,
    CONDITION_SATISFIED,
    OBSERVED_MISSING,
    OBSERVED_PRESENT,
)
from sutradhara.resource_control import ResourceControlUnavailable
from sutradhara.sealing.port import Representation
from tests.bundle_group_helpers import bundle_kwargs_for_class
from tests.key_helpers import make_test_key_registry


@pytest.mark.parametrize(
    "fault",
    [
        "missing",
        "not-regular",
        "not-directory",
        "size",
        "digest",
        "xattr",
        "permission",
        "io",
        "unsupported",
        "value",
    ],
)
def test_rba2d_member_check_fault_classes(tmp_path, monkeypatch, fault):
    """The shared check distinguishes unavailable bytes from host and programming faults."""
    import errno

    from sutradhara.archive_fanout import MemberInput
    from sutradhara.archive_restore import ArchiveRestoreError

    path = tmp_path / "member.bin"
    data = b"stored member"
    path.write_bytes(data)
    expected = ArchiveRestoreError
    injected = None
    xattrs = ()
    if fault == "missing":
        path.unlink()
    elif fault == "not-regular":
        path.unlink()
        path.mkdir()
    elif fault == "not-directory":
        path = path / "child"
    elif fault == "size":
        path.write_bytes(data + b"!")
    elif fault == "digest":
        path.write_bytes(b"x" * len(data))
        expected = bundle_repair._SourceDigestMismatch
    elif fault == "xattr":
        xattrs = ("user.required",)
        monkeypatch.setattr(bundle_repair.os, "listxattr", lambda path: [])
    else:
        injected = {
            "permission": PermissionError("denied"),
            "io": OSError(errno.EIO, "read fault"),
            "unsupported": OSError(errno.ENOTSUP, "xattr unsupported"),
            "value": ValueError("embedded null byte"),
        }[fault]
        expected = type(injected)

        def fail(path):
            raise injected

        if fault == "unsupported":
            xattrs = ("user.required",)
            monkeypatch.setattr(bundle_repair.os, "listxattr", fail)
        else:
            monkeypatch.setattr(bundle_repair, "_sha256_file", fail)
    member = MemberInput(
        logical_asset_hash=hashlib.sha256(data).digest(),
        member_path="member.bin",
        source_path=path,
        size_bytes=len(data),
        file_sha256=hashlib.sha256(data).digest(),
    )
    with pytest.raises(expected) as caught:
        bundle_repair._check_member_file(member, xattrs=xattrs, staging=True)
    if injected is not None:
        assert caught.value is injected
    else:
        assert "member.bin" in str(caught.value)


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


class _Backend:
    reissue_after_absent = True
    holds_names = False

    def lookup_pool_object(self, pool, object_name, *, candidate_digests):
        return None

    def __init__(self, *, kind: BackendKind = BackendKind.REM_TAPE) -> None:
        self.kind = kind
        self._counter = 0
        self.objects: dict[str, bytes] = {}
        self.writes: list[str] = []
        self.read_failures: set[str] = set()
        self.reads: list[str] = []
        self.fail_next_verify = False

    @property
    def name(self) -> str:
        return "rem"

    def write_object_to_pool(
        self,
        source: Path | str,
        pool: str,
        *,
        caller_object_id: str | None = None,
        content_sha256: bytes | None = None,
    ) -> CopyRecord:
        self._counter += 1
        data = Path(source).read_bytes()
        digest = content_hash(hashlib.sha256(data).digest())
        object_id = f"obj-{self._counter}"
        self.objects[object_id] = data
        self.writes.append(pool)
        media_locator = (
            {"barcode": f"D2BAR{self._counter:06d}"}
            if self.kind == BackendKind.D2_TAPE
            else {"tape_uuid": f"{self._counter:032x}"}
        )
        return CopyRecord(
            logical_id=digest,
            native_locator={
                "pool_id": pool,
                "object_id": object_id,
                "content_sha256": digest.hex(),
                "tape_file_number": self._counter,
                **media_locator,
            },
            integrity_hash=digest,
            size_bytes=len(data),
        )

    def enumerate(self) -> Iterator[CopyRecord]:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        object_id = str(locator["object_id"])
        self.reads.append(object_id)
        if object_id in self.read_failures:
            raise BackendError(f"transport unavailable for {object_id}")
        data = self.objects[object_id]
        if byte_range.is_whole_object:
            return data
        return data[byte_range.start : byte_range.end]

    def verify(
        self, locator: BackendLocator, *, observe: VerifyObserver | None = None
    ) -> VerifyResult:
        if self.fail_next_verify:
            self.fail_next_verify = False
            return VerifyResult(ok=False, measured=False, detail="forced verify failure")
        data = self.read_range(locator, ByteRange(0, 0))
        if observe is not None:
            observe(0, data)
        actual = content_hash(hashlib.sha256(data).digest())
        expected = content_hash(bytes.fromhex(str(locator["content_sha256"])))
        return VerifyResult(ok=actual == expected, measured=True, actual_hash=actual)

    def corrupt_member(self, copy: Copy, locator: AssetLocator) -> None:
        object_id = str(copy.native_locator["object_id"])
        data = bytearray(self.objects[object_id])
        header_len = int.from_bytes(data[:8], "big")
        offset = 8 + header_len + int(locator.native_locator["offset"])
        data[offset] ^= 0x01
        self.objects[object_id] = bytes(data)


class _OutputsBuilder(LocalArchiveBuilder):
    def build(self, **kwargs: object) -> BuildArtifact:
        artifact = super().build(**kwargs)
        return replace(
            artifact,
            blob_roots=(
                BuiltBlobRoot(
                    root_path="root",
                    native_locator={"member_path": "root", "offset": 0},
                ),
            ),
            exclusions=(BuiltExclusion(path="skip.tmp", reason="test-exclusion"),),
        )


def test_bundle_repair_rebuilds_missing_pool_after_staging_purge(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _Backend(kind=BackendKind.D2_TAPE)
    bundle_id, _backend_id, assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        old_exclusion_count = len(list(s.scalars(select(ExclusionRecord))))
        p2_copy = s.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        p2_copy.health = CopyHealth.MISSING
        bundle = s.get(Bundle, bundle_id)
        assert bundle is not None
        for member in bundle.members:
            member.source_path = str(tmp_path / "purged" / member.member_path)

        monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _row: backend)
        monkeypatch.setattr(
            bundle_repair,
            "make_archive_builder",
            lambda rem_bin=None: _OutputsBuilder(),
        )
        job = submit(s, "bundle-repair", {"bundle_id": bundle_id})
        result = run_one(s, job.id)

        assert result.ok
        repaired = s.scalars(
            select(Copy).where(Copy.pool_id == "p2", Copy.health == CopyHealth.OK)
        ).one()
        locators = list(s.scalars(select(AssetLocator).where(AssetLocator.copy_id == repaired.id)))
        assert len(locators) == len(assets)
        assert len(list(s.scalars(select(BlobRoot).where(BlobRoot.copy_id == repaired.id)))) == 1
        assert len(list(s.scalars(select(ExclusionRecord)))) == old_exclusion_count
        for locator in locators:
            assert (
                read_member_bytes(backend, repaired, locator, work_dir=tmp_path)
                == assets[locator.logical_asset_hash]
            )
        attempt = s.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id)).one()
        assert "tape:D2BAR000003" in attempt.detail["components"]


class _StreamingRepairBackend(_Backend):
    """Expose source member range sessions separately from target verification reads."""

    def __init__(self) -> None:
        super().__init__()
        self.streams: list[tuple[str, ByteRange]] = []

    @property
    def stream_kind(self) -> StreamKind:
        return StreamKind.native_stream

    @contextmanager
    def open_range_chunks(
        self, locator: BackendLocator, byte_range: ByteRange, *, chunk_bytes: int
    ) -> Iterator[Iterator[bytes]]:
        object_id = str(locator["object_id"])
        self.streams.append((object_id, byte_range))
        data = self.objects[object_id]
        end = len(data) if byte_range.is_whole_object else byte_range.end

        def chunks() -> Iterator[bytes]:
            for cursor in range(byte_range.start, end, chunk_bytes):
                yield data[cursor : min(cursor + chunk_bytes, end)]

        yield chunks()


def test_bundle_repair_reads_each_source_member_in_one_range(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T5: the repair handler materializes each source member in one opener session."""
    backend = _StreamingRepairBackend()
    bundle_id, _, assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))
    backend.streams.clear()
    with session_scope(engine) as session:
        source_copy = session.scalars(select(Copy).where(Copy.pool_id == "p1")).one()
        source_id = str(source_copy.native_locator["object_id"])
        header_end = 8 + int.from_bytes(backend.objects[source_id][:8], "big")
        source_locators = list(
            session.scalars(select(AssetLocator).where(AssetLocator.copy_id == source_copy.id))
        )
        expected_ranges = [
            ByteRange(
                header_end + int(locator.native_locator["offset"]),
                header_end
                + int(locator.native_locator["offset"])
                + int(locator.native_locator["size_bytes"]),
            )
            for locator in source_locators
        ]
        missing = session.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        missing.health = CopyHealth.MISSING
        bundle = session.get(Bundle, bundle_id)
        assert bundle is not None
        for member in bundle.members:
            member.source_path = str(tmp_path / "purged" / member.member_path)
        monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _: backend)
        monkeypatch.setattr(
            bundle_repair, "make_archive_builder", lambda rem_bin=None: _OutputsBuilder()
        )
        job = submit(session, "bundle-repair", {"bundle_id": bundle_id})
        result = run_one(session, job.id)
        assert result.ok
        source_ranges = [span for object_id, span in backend.streams if object_id == source_id]
        assert sorted(source_ranges, key=lambda span: span.start) == sorted(
            expected_ranges, key=lambda span: span.start
        )
        assert len(source_ranges) == len(assets)
        assert all(not span.is_whole_object for span in source_ranges)
        repaired = session.scalars(
            select(Copy).where(Copy.pool_id == "p2", Copy.health == CopyHealth.OK)
        ).one()
        assert repaired.last_measured_digest == repaired.integrity_hash


def test_bundle_repair_marks_corrupt_source_suspect_and_falls_back(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2", "p3"))

    with session_scope(engine) as s:
        p1_copy = s.scalars(select(Copy).where(Copy.pool_id == "p1")).one()
        p1_locator = s.scalars(
            select(AssetLocator).where(AssetLocator.copy_id == p1_copy.id)
        ).first()
        assert p1_locator is not None
        backend.corrupt_member(p1_copy, p1_locator)
        p1_copy.last_measured_at = dt.datetime(2026, 1, 2, tzinfo=dt.UTC)
        p2_copy = s.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        p2_copy.last_measured_at = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
        p3_copy = s.scalars(select(Copy).where(Copy.pool_id == "p3")).one()
        p3_copy.health = CopyHealth.MISSING

        monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _row: backend)
        monkeypatch.setattr(
            bundle_repair,
            "make_archive_builder",
            lambda rem_bin=None: _OutputsBuilder(),
        )
        job = submit(s, "bundle-repair", {"bundle_id": bundle_id})
        result = run_one(s, job.id)

        assert result.ok
        assert p1_copy.health == CopyHealth.SUSPECT
        assert s.scalars(
            select(Copy).where(Copy.pool_id == "p3", Copy.health == CopyHealth.OK)
        ).one()


def test_bundle_repair_transport_error_falls_back_without_suspect_latch(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2", "p3"))

    with session_scope(engine) as s:
        p1_copy = s.scalars(select(Copy).where(Copy.pool_id == "p1")).one()
        p1_copy.last_measured_at = dt.datetime(2026, 1, 2, tzinfo=dt.UTC)
        p2_copy = s.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        p2_copy.last_measured_at = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
        p3_copy = s.scalars(select(Copy).where(Copy.pool_id == "p3")).one()
        p3_copy.health = CopyHealth.MISSING
        backend.read_failures.add(str(p1_copy.native_locator["object_id"]))

        monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _row: backend)
        monkeypatch.setattr(
            bundle_repair,
            "make_archive_builder",
            lambda rem_bin=None: _OutputsBuilder(),
        )
        job = submit(s, "bundle-repair", {"bundle_id": bundle_id})
        result = run_one(s, job.id)

        assert result.ok
        assert p1_copy.health == CopyHealth.OK
        assert s.scalars(
            select(Copy).where(Copy.pool_id == "p3", Copy.health == CopyHealth.OK)
        ).one()


def test_bundle_repair_failed_target_verification_commits_suspect_copy_then_reruns(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        p2_copy = s.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        p2_copy.health = CopyHealth.MISSING
        backend.fail_next_verify = True

        monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _row: backend)
        monkeypatch.setattr(
            bundle_repair,
            "make_archive_builder",
            lambda rem_bin=None: _OutputsBuilder(),
        )
        first_job = submit(s, "bundle-repair", {"bundle_id": bundle_id})
        first_result = run_one(s, first_job.id)

        assert not first_result.ok
        assert first_job.status == JobStatus.FAILED
        suspect = s.scalars(
            select(Copy).where(Copy.pool_id == "p2", Copy.health == CopyHealth.SUSPECT)
        ).one()
        assert suspect.last_checked_at is None
        status = bundle_replication_status(s, bundle_id)
        assert status["complete"] is False
        assert {target.pool_id for target in status["missing"]} == {"p2"}

        second_job = submit(s, "bundle-repair", {"bundle_id": bundle_id})
        second_result = run_one(s, second_job.id)

        assert second_result.ok
        assert second_job.status == JobStatus.SUCCEEDED
        status = bundle_replication_status(s, bundle_id)
        assert status["complete"] is True


def test_bundle_repair_reports_write_fenced_missing_pools(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        p2 = s.get(Pool, "p2")
        assert p2 is not None
        p2.accepts_writes = False
        p2_copy = s.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        p2_copy.health = CopyHealth.MISSING
        policy = s.get(ArtifactClassPolicyRecord, "class-a")
        assert policy is not None
        policy.min_copies = 1
        condition = s.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        ).one()
        assert condition.condition == CONDITION_SATISFIED

        monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _row: backend)
        monkeypatch.setattr(
            bundle_repair,
            "make_archive_builder",
            lambda rem_bin=None: _OutputsBuilder(),
        )
        job = submit(
            s,
            "bundle-repair",
            {"bundle_id": bundle_id},
            recon_domain="bundle_copy",
            recon_target_key=bundle_id,
            dedupe_key=f"bundle_copy:{bundle_id}",
        )
        result = run_one(s, job.id)

        assert result.ok
        assert "write-fenced" in result.detail
        assert "p2" in result.detail
        assert result.step_state["bundle_repair"]["repaired_pools"] == []
        assert result.step_state["bundle_repair"]["remaining_write_fenced_pools"] == ["p2"]
        status = bundle_replication_status(s, bundle_id)
        assert status["complete"] is False
        assert {target.pool_id for target in status["missing"]} == {"p2"}
        assert condition.condition == CONDITION_BACKOFF
        assert condition.reason == "fenced-missing"
        assert condition.observed_state == OBSERVED_MISSING
        assert condition.message is not None
        assert "p2" in condition.message


def test_bundle_copy_reconciler_observes_placement_complete_without_default_floor(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        p2_copy = s.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        p2_copy.health = CopyHealth.MISSING

        missing = bundle_copy.observe(s, bundle_id)
        assert missing.observed_state == OBSERVED_MISSING
        assert bundle_copy.enumerate_targets(s, None, 100)[0].observed_state == OBSERVED_MISSING

        p2_copy.health = CopyHealth.OK
        present = bundle_copy.observe(s, bundle_id)
        assert present.observed_state == OBSERVED_PRESENT

        bundle_copy.reconcile_target(s, bundle_id)
        job = s.scalars(select(Job).where(Job.kind == "bundle-repair")).one()
        assert job.params == {"bundle_id": bundle_id}
        assert job.recon_domain == "bundle_copy"
        assert job.recon_target_key == bundle_id


def test_flush_bundle_outbox_fast_path_resolves_condition(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        condition = s.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        ).one()
        assert condition.condition == CONDITION_SATISFIED
        assert condition.observed_state == OBSERVED_PRESENT
        assert condition.reason is None


def test_bundle_copy_structural_floor_blocks_without_duplicate_write(
    engine: Engine,
    tmp_path: Path,
) -> None:
    backend = _Backend()
    bundle_id, _backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        policy = s.get(ArtifactClassPolicyRecord, "class-a")
        assert policy is not None
        policy.min_impl_families = 2
        before = len(list(s.scalars(select(Copy).where(Copy.bundle_id == bundle_id))))

        condition = bundle_copy.refresh_condition(s, bundle_id)

        after = len(list(s.scalars(select(Copy).where(Copy.bundle_id == bundle_id))))
        assert after == before
        assert condition.condition == CONDITION_BLOCKED
        assert condition.reason == "durability-floor-unsatisfiable"


def test_bundle_copy_duplicate_alarm_logs_once(
    engine: Engine,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = _Backend()
    bundle_id, backend_id, _assets = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))

    with session_scope(engine) as s:
        existing = s.scalars(
            select(Copy).where(Copy.bundle_id == bundle_id, Copy.pool_id == "p1")
        ).one()
        duplicate, _ = add_bundle_copy(
            s,
            bundle_id=bundle_id,
            backend_id=backend_id,
            pool_id="p1",
            native_locator={
                "pool_id": "p1",
                "object_id": "duplicate",
                "tape_uuid": "f" * 32,
            },
            integrity_hash=existing.integrity_hash,
            source=CopySource.INGEST,
            health=CopyHealth.OK,
            storage_metadata=existing.storage_metadata,
        )
        submit(
            s,
            "verify",
            {"copy_id": duplicate.id},
            dedupe_key=f"verify:copy:{duplicate.id}",
        )
        s.flush()
        caplog.set_level("ERROR", logger="sutradhara.jobs.reconcilers.bundle_copy")

        first = bundle_copy.refresh_condition(s, bundle_id)
        second = bundle_copy.refresh_condition(s, bundle_id)

        assert first.condition == CONDITION_BLOCKED
        assert second.condition == CONDITION_BLOCKED
        assert second.reason == "duplicate-copy"
        assert (
            sum(1 for record in caplog.records if record.message == "bundle_copy_condition_blocked")
            == 1
        )


def _sealed_pool_copy(session, pool_id: str, *, offset: bool = False) -> Copy:
    """Turn a seeded copy into a sealed source with unavailable recipient keys."""
    pool = session.get(Pool, pool_id)
    assert pool is not None
    pool.representation = Representation.REM_ENCRYPT_V1.value
    copy = session.scalars(select(Copy).where(Copy.pool_id == pool_id)).one()
    copy.storage_metadata = {
        **copy.storage_metadata,
        "representation": Representation.REM_ENCRYPT_V1.value,
        "recipient_epochs": ["archive-" + "e" * 32, "recovery-" + "d" * 32],
    }
    for locator in session.scalars(select(AssetLocator).where(AssetLocator.copy_id == copy.id)):
        locator.representation = Representation.REM_ENCRYPT_V1.value
        if not offset:
            locator.native_locator = {
                key: value for key, value in locator.native_locator.items() if key != "offset"
            }
            locator.native_locator = {**locator.native_locator, "first_chunk_lba": 0}
    session.flush()
    return copy


@pytest.mark.parametrize("reconciled", [True, False], ids=["reconciled", "manual"])
@pytest.mark.parametrize("epoch", [None, "archive-" + "f" * 32], ids=["missing", "unknown"])
def test_repair_checks_destination_keys_before_source_read(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    epoch: str | None,
    reconciled: bool,
) -> None:
    """T4/T5: both repair submissions refuse destination keys without I/O or build."""
    backend = _Backend()
    bundle_id, _, _ = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))
    registry = make_test_key_registry(tmp_path / "keys")
    builds: list[object] = []

    class RecordingBuilder(_OutputsBuilder):
        def build(self, **kwargs: object) -> BuildArtifact:
            builds.append(kwargs)
            return super().build(**kwargs)

    monkeypatch.setattr(bundle_repair, "make_key_registry", lambda: registry)
    monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _row: backend)
    if epoch is None:
        monkeypatch.setattr(
            bundle_repair, "make_archive_builder", lambda rem_bin=None: RecordingBuilder()
        )
    else:
        # The production factory stays intact; record its build method without
        # bypassing the production preflight's isinstance/registry check.
        original_build = RemArchiveBuilder.build

        def build(self, **kwargs):
            builds.append(kwargs)
            return original_build(self, **kwargs)

        monkeypatch.setattr(RemArchiveBuilder, "build", build)

    with session_scope(engine) as session:
        _sealed_pool_copy(session, "p2").health = CopyHealth.MISSING
        params = {"bundle_id": bundle_id}
        if epoch is not None:
            params["key_epoch"] = epoch
        job = submit(
            session,
            "bundle-repair",
            params,
            recon_domain="bundle_copy" if reconciled else None,
            recon_target_key=bundle_id if reconciled else None,
        )
        backend.reads.clear()
        result = run_one(session, job.id)
        assert backend.reads == [], "no source read before destination key validation"
        assert builds == [], "no build before destination key validation"
        assert not result.ok
        assert result.condition is not None
        assert result.condition.condition == CONDITION_BLOCKED
        assert result.condition.reason == (
            "key-epoch-missing" if epoch is None else "key-epoch-unusable"
        )
        assert bundle_id in result.detail
        assert "p2" in result.detail
        assert "sutra jobs submit bundle-repair" in result.detail
        assert "-p key_epoch=" in result.detail
        assert result.condition.reason in result.detail
        assert "bundle flush" not in result.detail
        assert "bundle sweep" not in result.detail
        condition = session.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        ).one()
        if reconciled:
            assert condition.condition == CONDITION_BLOCKED
            assert condition.reason == result.condition.reason
        else:
            assert job.status == JobStatus.FAILED
            assert condition.condition == CONDITION_SATISFIED


@pytest.mark.parametrize("fault_type", [OSError, ResourceControlUnavailable])
def test_repair_destination_host_fault_returns_handler_failure(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_type: type[Exception],
) -> None:
    """Destination host faults return a retried handler result before any source I/O."""
    backend = _Backend()
    bundle_id, _, _ = _flushed_bundle(engine, tmp_path, backend, ("p1", "p2"))
    registry = make_test_key_registry(tmp_path / "keys")
    builds: list[object] = []

    def host_fault(*_args, **_kwargs):
        raise fault_type("destination registry unavailable on this host")

    def build(self, **kwargs):
        builds.append(kwargs)
        raise AssertionError("no build before destination key validation")

    monkeypatch.setattr(registry, "recipients_for_seal", host_fault)
    monkeypatch.setattr(bundle_repair, "make_key_registry", lambda: registry)
    monkeypatch.setattr(RemArchiveBuilder, "build", build)
    monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _row: backend)
    with session_scope(engine) as session:
        _sealed_pool_copy(session, "p2").health = CopyHealth.MISSING
        job = submit(
            session,
            "bundle-repair",
            {"bundle_id": bundle_id, "key_epoch": "archive-" + "f" * 32},
            recon_domain="bundle_copy",
            recon_target_key=bundle_id,
        )
        backend.reads.clear()
        result = run_one(session, job.id)
        assert backend.reads == [], "no source read before destination key validation"
        assert builds == []
        assert not result.ok
        assert result.condition is None
        attempt = session.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id)).one()
        assert attempt.error == result.detail, "attempt records the handler's failure result"
        assert attempt.outcome == JobStatus.FAILED
        assert bundle_id in result.detail
        assert "missing pools=['p2']" in result.detail
        assert "destination registry unavailable on this host" in result.detail
        assert result.detail.endswith(
            "the key registry or the rem tool on this host could not be used; the repair is retried"
        )
        assert "private key" not in result.detail
        condition = session.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        ).one()
        assert condition.condition == CONDITION_BACKOFF
        assert condition.reason == "unclassified"


@pytest.mark.parametrize(
    "case",
    ["key", "mixed", "host", "wrapped-scope", "scope", "fenced", "offset", "malformed"],
)
def test_repair_checks_source_keys_before_reading(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    """T6: key-less sources are skipped; host faults retry and fencing reads nothing."""
    backend = _Backend()
    pools = ("p1", "p2", "p3") if case in {"mixed", "malformed"} else ("p1", "p2")
    bundle_id, _, _ = _flushed_bundle(engine, tmp_path, backend, pools)
    registry = make_test_key_registry(tmp_path / "keys")
    builds: list[object] = []

    class RecordingBuilder(_OutputsBuilder):
        def build(self, **kwargs: object) -> BuildArtifact:
            builds.append(kwargs)
            return super().build(**kwargs)

    monkeypatch.setattr(bundle_repair, "make_key_registry", lambda: registry)
    monkeypatch.setattr(
        bundle_repair, "make_archive_builder", lambda rem_bin=None: RecordingBuilder()
    )
    monkeypatch.setattr(bundle_repair.factory, "backend_from_row", lambda _row: backend)
    if case in {"host", "wrapped-scope", "scope"}:

        def host_fault(*_args, **_kwargs):
            if case == "host":
                raise OSError("registry unavailable on this host")
            raise ResourceControlUnavailable("rem scope unavailable on this host")

        if case == "wrapped-scope":
            # Raise inside the registry so the real restore selector supplies
            # the ArchiveRestoreError wrapper and its ResourceControlUnavailable cause.
            monkeypatch.setattr(registry, "select_private_epoch", host_fault)
        else:
            monkeypatch.setattr(bundle_repair, "select_private_epoch_for_copy", host_fault)
    original_read = bundle_repair.read_member_to_path

    def read_with_registry(*args, **kwargs):
        assert kwargs["keys"] is registry
        return original_read(*args, **kwargs)

    monkeypatch.setattr(bundle_repair, "read_member_to_path", read_with_registry)
    with session_scope(engine) as session:
        sealed = _sealed_pool_copy(session, "p1", offset=case == "offset")
        if case == "malformed":
            for locator in session.scalars(
                select(AssetLocator).where(AssetLocator.copy_id == sealed.id)
            ):
                locator.native_locator = {**locator.native_locator, "size_bytes": None}
        missing_copy = session.scalars(select(Copy).where(Copy.pool_id == "p2")).one()
        missing_copy.health = CopyHealth.MISSING
        if case in {"mixed", "malformed"}:
            # Prefer this source despite its representation cost: the test must
            # prove it is skipped, rather than just selecting plaintext first.
            sealed.last_measured_at = dt.datetime.now(dt.UTC) + dt.timedelta(days=1)
            sources = bundle_repair.select_source_candidates(
                session, BundleTarget(bundle_id), purpose="self_heal"
            )
            assert sources[0].id == sealed.id
        if case == "fenced":
            pool = session.get(Pool, "p2")
            assert pool is not None
            pool.accepts_writes = False
            policy = session.get(ArtifactClassPolicyRecord, "class-a")
            assert policy is not None
            policy.min_copies = 1
        job = submit(
            session,
            "bundle-repair",
            {"bundle_id": bundle_id},
            recon_domain="bundle_copy",
            recon_target_key=bundle_id,
        )
        backend.reads.clear()
        result = run_one(session, job.id)
        if case in {"key", "mixed"}:
            attempt = session.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id)).one()
            assert (
                f"tape:{sealed.native_locator['tape_uuid']}" not in attempt.detail["components"]
            ), "unreadable source tape is not touched"
        if case in {"mixed", "offset", "malformed"}:
            if case in {"mixed", "malformed"}:
                assert str(sealed.native_locator["object_id"]) not in backend.reads, (
                    "no read of a source without private keys"
                )
            assert result.ok, result.detail
            assert builds
            source_id = result.step_state["bundle_repair"]["source_copy_id"]
            if case in {"mixed", "malformed"}:
                assert source_id != sealed.id
            else:
                assert source_id == sealed.id
            return
        assert backend.reads == [], "no source read before source key validation"
        assert builds == []
        if case == "fenced":
            assert result.ok
            assert result.condition is not None
            assert result.condition.reason == "fenced-missing"
            state = result.step_state["bundle_repair"]
            assert state["repaired_pools"] == []
            assert state["remaining_write_fenced_pools"] == ["p2"]
            assert "source_copy_id" not in state
            return
        assert not result.ok
        assert f"copy id={sealed.id}" in result.detail
        assert "archive-" + "e" * 32 in result.detail
        if case in {"host", "wrapped-scope", "scope"}:
            assert result.condition is None
            assert (
                "registry unavailable on this host"
                if case == "host"
                else "rem scope unavailable on this host"
            ) in result.detail
            assert result.detail.endswith(
                "the key registry or the rem tool on this host could not be used; the repair is retried"
            )
            assert "private key" not in result.detail
        else:
            assert "private key" in result.detail
            assert "this host's key registry" in result.detail
            assert result.condition is not None
            assert result.condition.condition == CONDITION_BLOCKED
            assert result.condition.reason == "source-key-unavailable"
        condition = session.scalars(
            select(ReconciliationCondition).where(
                ReconciliationCondition.domain == "bundle_copy",
                ReconciliationCondition.target_key == bundle_id,
            )
        ).one()
        assert (condition.condition == CONDITION_BLOCKED) == (case == "key")


def _flushed_bundle(
    engine: Engine,
    tmp_path: Path,
    backend: _Backend,
    pool_ids: tuple[str, ...],
) -> tuple[str, int, dict[bytes, bytes]]:
    paths = {
        "a.bin": b"alpha",
        "nested/b.bin": b"beta",
    }
    with session_scope(engine) as s:
        row = Backend(name="rem", kind=backend.kind, tier=BackendTier.SELF_DESCRIBING)
        s.add(row)
        s.flush()
        for pool_id in pool_ids:
            s.add(
                Pool(
                    id=pool_id,
                    backend_id=row.id,
                    representation=Representation.REM_OBJECT_V1.value,
                )
            )
        s.flush()
        apply_artifactclass_policy(
            s,
            "class-a",
            ArtifactClassPolicy(
                ruleset="",
                placements=tuple(PlacementPolicy(pool_id) for pool_id in pool_ids),
                bundling=BundlingPolicy(target_gb=1, max_age_seconds=60),
                restore_preference=pool_ids,
                expect="messy",
                durability=DurabilityPolicy(
                    min_copies=len(pool_ids),
                    min_impl_families=1,
                ),
            ),
        )
        # The basis must name the real pools: fan-out and repair both read
        # their target list from group_basis, in basis order (§2/§5).
        bundle = Bundle(
            id="bundle-a",
            **bundle_kwargs_for_class(s, "class-a"),
            status="open",
        )
        s.add(bundle)
        assets: dict[bytes, bytes] = {}
        for member_path, data in paths.items():
            asset_hash = hashlib.sha256(data).digest()
            source = tmp_path / member_path.replace("/", "_")
            source.write_bytes(data)
            s.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(data)))
            s.flush()
            add_bundle_member(
                s,
                bundle=bundle,
                artifactclass="class-a",
                logical_asset_hash=asset_hash,
                member_path=member_path,
                size_bytes=len(data),
                file_sha256=asset_hash,
                source_path=str(source),
            )
            assets[asset_hash] = data
        flush_bundle(
            s,
            bundle_id=bundle.id,
            backends={row.id: backend},
            builder=_OutputsBuilder(),
        )
        return bundle.id, row.id, assets
