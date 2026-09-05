"""Restore read ordering: planning pass, dispatcher gate, §5 table, re-plans.

Failure paths outweigh happy paths (design-restore-read-ordering §5-§6).
All tests here run against a faked plan client; the live-wire proof against
a spawned read-only rem-daemon lives in test_read_ordering_live.py.

covers: rem.plan.batch_read (hermetic; sutradhara consumer side)
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import grpc
import pytest
from google.protobuf import any_pb2
from sqlalchemy import Engine, select

from sutradhara._proto import layer5_pb2
from sutradhara._proto.google.rpc import error_details_pb2, status_pb2
from sutradhara.api.identity import parse_identity
from sutradhara.backend.memory import MemoryBackend
from sutradhara.backend.remanence import TapeReadFacts
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    ArtifactClassPool,
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
from sutradhara.hdcache import read_ordering
from sutradhara.hdcache.manager import (
    RestoreConfig,
    RestoreDestination,
    RestoreItemSpec,
    ServeResult,
    admit_restore_request,
)
from sutradhara.hdcache.models import (
    CacheDisk,
    CacheEntry,
    RestoreOrderingOutcome,
    RestoreReadPlanSlot,
    RestoreRequestItem,
)
from sutradhara.hdcache.read_ordering import (
    generate_item_tag,
    restore_release_allowed,
)
from sutradhara.jobs.engine import pending_candidates, run_pending, submit
from sutradhara.jobs.models import Job, JobStatus
from sutradhara.sealing.port import Representation

# Ensure the restore handler (and its dispatch gate) is registered.
import sutradhara.jobs.handlers  # noqa: F401  isort: skip
from tests.bundle_group_helpers import bundle_kwargs

TAPE_A = bytes.fromhex("aa" * 16)
TAPE_B = bytes.fromhex("bb" * 16)

BLOCK_SIZE = 262_144
WRITTEN_EXTENT = 50_000


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    eng = make_engine(f"sqlite:///{tmp_path / 'read-ordering.db'}")
    create_all(eng)
    yield eng
    eng.dispose()


def _identity(groups: str = "sutradhara-restore"):
    return parse_identity(
        {
            "X-Authentik-Username": "ada",
            "X-Authentik-Name": "Ada Operator",
            "X-Authentik-Groups": groups,
        }
    )


class FakeRpcError(grpc.RpcError):
    """A grpc.RpcError shaped like the surface the §5 mapping reads."""

    def __init__(
        self,
        code: grpc.StatusCode,
        details: str = "",
        trailers: tuple[tuple[str, bytes], ...] = (),
    ) -> None:
        self._code = code
        self._details = details
        self._trailers = trailers

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details

    def trailing_metadata(self):
        return self._trailers


def _invalid_argument_error() -> FakeRpcError:
    bad_request = error_details_pb2.BadRequest()
    violation = bad_request.field_violations.add()
    violation.field = "targets[1].end_block"
    violation.description = "end_block below start_block"
    packed = any_pb2.Any()
    packed.Pack(bad_request)
    status = status_pb2.Status(code=3, message="malformed plan request")
    status.details.append(packed)
    return FakeRpcError(
        grpc.StatusCode.INVALID_ARGUMENT,
        "malformed plan request",
        trailers=(("grpc-status-details-bin", status.SerializeToString()),),
    )


class FakePlanner:
    """Faked plan client implementing the ReadOrderingPlanner protocol."""

    def __init__(self) -> None:
        self.tape_facts: dict[bytes, TapeReadFacts] = {}
        self.spans: dict[tuple[str, int], tuple[int, int] | None] = {}
        self.responses: list[Any] = []
        self.plan_requests: list[layer5_pb2.PlanBatchReadRequest] = []
        self.facts_errors: list[Exception] = []

    def get_tape_facts(self, tape_uuid: bytes) -> TapeReadFacts:
        if self.facts_errors:
            raise self.facts_errors.pop(0)
        return self.tape_facts.get(
            tape_uuid,
            TapeReadFacts(
                voltag="RO0001L8",
                block_size_bytes=BLOCK_SIZE,
                written_extent_lba=WRITTEN_EXTENT,
            ),
        )

    def get_copy_read_span(self, locator: Any) -> tuple[int, int] | None:
        key = (str(locator.get("tape_uuid")), int(locator.get("tape_file_number", -1)))
        return self.spans.get(key)

    def plan_batch_read(
        self, request: layer5_pb2.PlanBatchReadRequest
    ) -> layer5_pb2.PlanBatchReadResponse:
        self.plan_requests.append(request)
        if not self.responses:
            return _ok_response(request)
        action = self.responses.pop(0)
        if isinstance(action, Exception):
            raise action
        if callable(action):
            return action(request)
        return action


def _ok_response(
    request: layer5_pb2.PlanBatchReadRequest,
    *,
    tag_order: list[bytes] | None = None,
    status: int = layer5_pb2.OK,
    basis: int = layer5_pb2.PRIORS,
    calibration_generation: int = 7,
) -> layer5_pb2.PlanBatchReadResponse:
    by_tag = {bytes(target.tag): target for target in request.targets}
    order = tag_order if tag_order is not None else [bytes(t.tag) for t in request.targets]
    response = layer5_pb2.PlanBatchReadResponse(
        status=status,
        cost_model_basis=basis,
        max_targets=2730,
        calibration_generation=calibration_generation,
    )
    for tag in order:
        hop = response.hops.add()
        hop.target.CopyFrom(by_tag[tag])
        hop.estimated_locate_ns = 1_000_000
    return response


def _unavailable_response(status: int, *, calibration_generation: int = 0):
    return layer5_pb2.PlanBatchReadResponse(
        status=status,
        detail=layer5_pb2.PlanStatus.Name(status),
        max_targets=2730,
        calibration_generation=calibration_generation,
    )


class PlanMemoryBackend(MemoryBackend):
    """MemoryBackend that exposes the live plan surface via a FakePlanner."""

    def __init__(self, name: str, planner: FakePlanner) -> None:
        super().__init__(name)
        self._planner = planner

    def read_ordering_planner(self) -> FakePlanner:
        return self._planner


def _config(root: Path, backends: dict[int, MemoryBackend]) -> RestoreConfig:
    return RestoreConfig(
        destinations={
            "media-server": RestoreDestination(
                id="media-server",
                root=root,
                label="Media server restore",
            )
        },
        restore_backends=backends,
        scratch_root=root / ".scratch",
    )


def _add_policy(session, artifactclass: str) -> None:
    session.merge(
        ArtifactClassPolicyRecord(
            artifactclass=artifactclass,
            ruleset="test.rules",
            expect="messy",
            target_bytes=1024,
            max_age_seconds=3600,
            restore_preference=["tape-pool"],
            staging_config={},
            hdcache_config={"enabled": True, "privacy_level": "none"},
        )
    )
    if (
        session.scalar(
            select(ArtifactClassPool).where(
                ArtifactClassPool.artifactclass == artifactclass,
                ArtifactClassPool.pool_id == "tape-pool",
            )
        )
        is None
    ):
        session.add(
            ArtifactClassPool(
                artifactclass=artifactclass,
                pool_id="tape-pool",
                active=True,
                sort_order=0,
            )
        )
    session.flush()


def _seed_tape_asset(
    session,
    backend_obj: MemoryBackend,
    *,
    data: bytes,
    tape_uuid: bytes,
    tape_file_number: int,
    artifactclass: str = "s-masters",
) -> tuple[bytes, int, int]:
    """Seed one archived asset whose only copy sits on a tape-shaped locator.

    Returns (digest, backend_id, copy_id).
    """

    digest = hashlib.sha256(data).digest()
    session.merge(LogicalAsset(content_sha256=digest, size_bytes=len(data)))
    backend = session.scalar(select(Backend).where(Backend.name == backend_obj.name))
    if backend is None:
        backend = Backend(
            name=backend_obj.name,
            kind=BackendKind.MEMORY,
            tier=BackendTier.SELF_DESCRIBING,
        )
        session.add(backend)
        session.flush()
    if session.get(Pool, "tape-pool") is None:
        session.add(
            Pool(
                id="tape-pool",
                backend_id=backend.id,
                representation=Representation.RAW_BYTES.value,
            )
        )
    _add_policy(session, artifactclass)
    backend_obj.add(data)
    bundle_id = f"bundle-{artifactclass}-{digest.hex()[:12]}-{tape_file_number}"
    if session.get(Bundle, bundle_id) is None:
        session.add(
            Bundle(
                id=bundle_id,
                **bundle_kwargs(seed=artifactclass),
                status="sealed",
                target_bytes=1024,
                max_age_seconds=3600,
            )
        )
        session.add(
            BundleMember(
                bundle_id=bundle_id,
                logical_asset_hash=digest,
                artifactclass=artifactclass,
                member_path=f"{digest.hex()}.mov",
                size_bytes=len(data),
                file_sha256=digest,
            )
        )
    locator = {
        "hash_hex": digest.hex(),
        "tape_uuid": tape_uuid.hex(),
        "tape_file_number": tape_file_number,
        "object_id": hashlib.sha256(tape_uuid + bytes([tape_file_number])).hexdigest()[:32],
    }
    copy = Copy(
        bundle_id=bundle_id,
        backend_id=backend.id,
        pool_id="tape-pool",
        native_locator=locator,
        native_locator_key=locator_key(locator),
        storage_metadata={"representation": Representation.RAW_BYTES.value},
        integrity_hash=digest,
        health=CopyHealth.OK,
        source=CopySource.INGEST,
    )
    session.add(copy)
    session.flush()
    session.add(
        AssetLocator(
            logical_asset_hash=digest,
            pool_id="tape-pool",
            copy_id=copy.id,
            bundle_id=bundle_id,
            native_locator={
                "member_path": f"{digest.hex()}.mov",
                "offset": 0,
                "size_bytes": len(data),
            },
            member_path=f"{digest.hex()}.mov",
            representation=Representation.RAW_BYTES.value,
        )
    )
    session.flush()
    assert copy.id is not None
    assert backend.id is not None
    return digest, backend.id, copy.id


def _span_for(tape_file_number: int) -> tuple[int, int]:
    """Distinct, sane [start, end) spans keyed by tape file number."""

    start = 1_000 * tape_file_number
    return (start, start + 400)


def _seed_world(
    session,
    planner: FakePlanner,
    *,
    count: int = 3,
    tape_uuid: bytes = TAPE_A,
    unspanned: set[int] | None = None,
) -> tuple[PlanMemoryBackend, list[bytes], dict[bytes, int]]:
    """Seed `count` tape assets on one volume; returns backend, digests, copy ids."""

    backend_obj = PlanMemoryBackend("plan-mem", planner)
    digests: list[bytes] = []
    copy_ids: dict[bytes, int] = {}
    for index in range(count):
        tape_file_number = index + 1
        digest, _backend_id, copy_id = _seed_tape_asset(
            session,
            backend_obj,
            data=b"payload-%d" % index,
            tape_uuid=tape_uuid,
            tape_file_number=tape_file_number,
        )
        digests.append(digest)
        copy_ids[digest] = copy_id
        if unspanned is not None and tape_file_number in unspanned:
            planner.spans[(tape_uuid.hex(), tape_file_number)] = None
        else:
            planner.spans[(tape_uuid.hex(), tape_file_number)] = _span_for(tape_file_number)
    return backend_obj, digests, copy_ids


def _backend_map(session, backend_obj: MemoryBackend) -> dict[int, MemoryBackend]:
    backend = session.scalar(select(Backend).where(Backend.name == backend_obj.name))
    assert backend is not None
    assert backend.id is not None
    return {backend.id: backend_obj}


def _admit(session, config: RestoreConfig, digests: list[bytes]):
    return admit_restore_request(
        session,
        identity=_identity(),
        destination_id="media-server",
        items=[RestoreItemSpec(digest, "s-masters") for digest in digests],
        config=config,
    )


def _slots(session, request_id: str, tape_uuid: bytes | None = None):
    query = (
        select(RestoreReadPlanSlot)
        .where(RestoreReadPlanSlot.request_id == request_id)
        .order_by(RestoreReadPlanSlot.tape_uuid, RestoreReadPlanSlot.position)
    )
    if tape_uuid is not None:
        query = query.where(RestoreReadPlanSlot.tape_uuid == tape_uuid)
    return list(session.scalars(query))


def _outcomes(session, request_id: str):
    return list(
        session.scalars(
            select(RestoreOrderingOutcome)
            .where(RestoreOrderingOutcome.request_id == request_id)
            .order_by(RestoreOrderingOutcome.id)
        )
    )


def _item_by_digest(request, digest: bytes) -> RestoreRequestItem:
    for item in request.items:
        if item.content_sha256 == digest:
            return item
    raise AssertionError("item not found")


def _submit_restore_jobs(session, request) -> dict[int, int]:
    """Mimic the API route: one restore job per queued item. item_id -> job_id."""

    jobs: dict[int, int] = {}
    for item in request.items:
        if item.state == "queued" and item.id is not None:
            job = submit(session, "restore", {"restore_request_item_id": item.id})
            jobs[item.id] = job.id
    session.flush()
    return jobs


def _install_fake_serve(
    monkeypatch: pytest.MonkeyPatch,
    config: RestoreConfig,
    copy_id_by_item: dict[int, int | None],
    order_log: list[int],
    outcomes: dict[int, str] | None = None,
) -> None:
    """Replace the serve seam under the real handler; the gate and hooks stay real."""

    plans = outcomes or {}

    def fake_serve(session, item, *, gates_already_admitted=False, config=None, _runtime=None):
        assert item.id is not None
        order_log.append(item.id)
        outcome = plans.get(item.id, "done")
        if outcome == "failed":
            item.state = "failed"
            item.detail = "simulated read failure"
            return ServeResult(item.id, "tape", Path(f"/nonexistent/{item.id}"), 0)
        if outcome == "cache":
            item.state = "done"
            item.source = "cache"
            item.bytes_restored = 1
            return ServeResult(item.id, "cache", Path(f"/nonexistent/{item.id}"), 1)
        if outcome == "fell_back":
            item.state = "fell_back_to_tape"
            item.source = "tape"
            item.state = "done"
            return ServeResult(
                item.id,
                "tape",
                Path(f"/nonexistent/{item.id}"),
                1,
                copy_id=copy_id_by_item.get(item.id),
            )
        item.state = "done"
        item.source = "tape"
        item.bytes_restored = 1
        return ServeResult(
            item.id,
            "tape",
            Path(f"/nonexistent/{item.id}"),
            1,
            copy_id=copy_id_by_item.get(item.id),
        )

    monkeypatch.setattr("sutradhara.jobs.handlers.restore.serve_restore_item", fake_serve)
    monkeypatch.setattr("sutradhara.jobs.handlers.restore.restore_config_from_env", lambda: config)


# ---------------------------------------------------------------------------
# The planning pass at the real call site (n > 1 is the load-bearing case)
# ---------------------------------------------------------------------------


def test_planning_pass_sends_n3_batch_and_persists_returned_order(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: every other test could pass at n=1 with ordering never exercised.

    Three targets go out in ONE PlanBatchRead; the permuted returned order is
    what gets persisted, not the submission order.
    """

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        planner.responses.append(
            lambda request: _ok_response(
                request,
                tag_order=[
                    bytes(request.targets[2].tag),
                    bytes(request.targets[0].tag),
                    bytes(request.targets[1].tag),
                ],
            )
        )
        request = _admit(session, config, digests)

        assert len(planner.plan_requests) == 1
        wire = planner.plan_requests[0]
        assert len(wire.targets) == 3, "the batch must carry n>1 targets"
        assert wire.tape_uuid == TAPE_A
        assert wire.cartridge.voltag == "RO0001L8"
        assert wire.cartridge.block_size_bytes == BLOCK_SIZE
        assert wire.cartridge.written_extent_lba == WRITTEN_EXTENT
        assert wire.cartridge.compression == layer5_pb2.COMPRESSION_DISABLED
        assert wire.cartridge.cartridge_generation == ""
        assert wire.cartridge.recording_format == ""
        assert not wire.HasField("start_position"), "initial plan originates at load point"
        for index, target in enumerate(wire.targets):
            start, end_exclusive = _span_for(index + 1)
            assert target.start_block == start
            assert target.end_block == end_exclusive - 1, (
                "ReadTarget.end_block must be span_end_exclusive - 1"
            )
            item = _item_by_digest(request, digests[index])
            assert bytes(target.tag) == generate_item_tag(item.id)

        slots = _slots(session, request.id)
        assert [slot.planned for slot in slots] == [True, True, True]
        expected_items = [
            _item_by_digest(request, digests[2]).id,
            _item_by_digest(request, digests[0]).id,
            _item_by_digest(request, digests[1]).id,
        ]
        assert [slot.item_id for slot in slots] == expected_items
        assert [slot.position for slot in slots] == [0, 1, 2]
        outcome_rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in outcome_rows] == [("initial", "ok")]
        assert outcome_rows[0].calibration_generation == 7


def test_dispatcher_releases_volume_items_in_planned_order(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards: a persisted plan nobody enforces — jobs must run in plan order."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        planner.responses.append(
            lambda request: _ok_response(
                request,
                tag_order=[
                    bytes(request.targets[2].tag),
                    bytes(request.targets[0].tag),
                    bytes(request.targets[1].tag),
                ],
            )
        )
        request = _admit(session, config, digests)
        items = [_item_by_digest(request, digest) for digest in digests]
        jobs = _submit_restore_jobs(session, request)
        session.commit()

        # Before anything settles, only the head of the list is claimable.
        candidates = [job.id for job in pending_candidates(session)]
        assert candidates == [jobs[items[2].id]]

        order_log: list[int] = []
        _install_fake_serve(
            monkeypatch,
            config,
            {item.id: copies[item.content_sha256] for item in items},
            order_log,
        )
        run_pending(session, limit=0)
        assert order_log == [items[2].id, items[0].id, items[1].id]


def test_cache_served_item_skips_its_hop_harmlessly(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards: a tape-planned item that serves from cache must not wedge the list."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        request = _admit(session, config, digests)
        items = [_item_by_digest(request, digest) for digest in digests]
        _submit_restore_jobs(session, request)
        session.commit()

        order_log: list[int] = []
        _install_fake_serve(
            monkeypatch,
            config,
            {item.id: copies[item.content_sha256] for item in items},
            order_log,
            outcomes={items[0].id: "cache"},  # planned for tape, served from cache
        )
        run_pending(session, limit=0)
        # All three ran, in plan (identity) order; the cache serve consumed
        # its slot turn without blocking successors.
        assert order_log == [items[0].id, items[1].id, items[2].id]
        session.expire_all()
        assert _item_by_digest(request, digests[0]).state == "done"
        assert _item_by_digest(request, digests[0]).source == "cache"
        assert all(job.status == JobStatus.SUCCEEDED for job in session.scalars(select(Job)))


def test_cache_predicted_item_has_no_slot_and_fallback_joins_unordered_tail(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards: mid-serve cache->tape fallback must not enter (or wait for) the list."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, copies = _seed_world(session, planner)
        # Give digest[0] a present cache entry so prediction says cache.
        disk = CacheDisk(
            disk_id="d1",
            serial="SER-RO",
            fs_uuid="fs-ro",
            mount=str(tmp_path / "cache"),
            state="active",
            capacity_bytes=10_000_000,
        )
        session.add(disk)
        session.flush()
        session.add(
            CacheEntry(
                content_sha256=digests[0],
                artifactclass="s-masters",
                disk_id="d1",
                relpath="a/b",
                size_bytes=9,
                state="present",
            )
        )
        session.flush()
        config = _config(tmp_path, _backend_map(session, backend_obj))
        request = _admit(session, config, digests)
        items = [_item_by_digest(request, digest) for digest in digests]

        # Cache-predicted item is in no list.
        assert len(planner.plan_requests) == 1
        assert len(planner.plan_requests[0].targets) == 2
        slot_items = {slot.item_id for slot in _slots(session, request.id)}
        assert items[0].id not in slot_items

        _submit_restore_jobs(session, request)
        session.commit()
        order_log: list[int] = []
        _install_fake_serve(
            monkeypatch,
            config,
            {item.id: copies[item.content_sha256] for item in items},
            order_log,
            outcomes={items[0].id: "fell_back"},
        )
        run_pending(session, limit=0)
        # The fallback item ran first (submission order, no gate) even though
        # the volume's ordered list had not settled: the unordered tail.
        assert order_log[0] == items[0].id
        assert set(order_log) == {item.id for item in items}


def test_unspanned_items_append_after_the_ordered_ones(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards: absent spans guessed or zeroed, and tail items jumping the order."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, copies = _seed_world(session, planner, unspanned={2})
        config = _config(tmp_path, _backend_map(session, backend_obj))
        request = _admit(session, config, digests)
        items = [_item_by_digest(request, digest) for digest in digests]

        wire = planner.plan_requests[0]
        assert len(wire.targets) == 2, "the unspanned item must never reach the wire"
        slots = _slots(session, request.id)
        assert [slot.planned for slot in slots] == [True, True, False]
        tail = slots[2]
        assert tail.item_id == items[1].id
        assert tail.tag is None
        assert tail.start_block is None
        assert tail.end_block is None

        jobs = _submit_restore_jobs(session, request)
        session.commit()
        # The tail job is not claimable while planned slots are unsettled.
        assert jobs[items[1].id] not in [job.id for job in pending_candidates(session)]
        order_log: list[int] = []
        _install_fake_serve(
            monkeypatch,
            config,
            {item.id: copies[item.content_sha256] for item in items},
            order_log,
        )
        run_pending(session, limit=0)
        assert order_log[-1] == items[1].id, "unspanned item releases after the ordered ones"


# ---------------------------------------------------------------------------
# The tag rule
# ---------------------------------------------------------------------------


def test_duplicate_content_items_get_distinct_tags_and_both_route(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: tags collapsing to object/copy identity (the Sony card split)."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner, count=1)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        # The same asset twice in one request: legal duplicate content.
        request = _admit(session, config, [digests[0], digests[0]])

        wire = planner.plan_requests[0]
        assert len(wire.targets) == 2
        tags = {bytes(target.tag) for target in wire.targets}
        assert len(tags) == 2, "duplicate content must still get distinct tags"
        item_ids = [item.id for item in request.items]
        assert tags == {generate_item_tag(item_ids[0]), generate_item_tag(item_ids[1])}
        slots = _slots(session, request.id)
        assert {slot.item_id for slot in slots} == set(item_ids)


def test_forced_tag_collision_is_loud_and_falls_back_unordered(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Guards: the same-tag defect being silently resolved by picking one."""

    planner = FakePlanner()
    monkeypatch.setattr(read_ordering, "generate_item_tag", lambda item_id: b"same")
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        with caplog.at_level(logging.ERROR, logger="sutradhara.hdcache.read_ordering"):
            request = _admit(session, config, digests)

        assert any("tag collision" in record.message for record in caplog.records)
        assert planner.plan_requests == [], "no plan may be attempted on a collision"
        assert _slots(session, request.id) == []
        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [("initial", "tag_collision")]
        # The restore itself is untouched: every item still dispatches.
        for item in request.items:
            assert item.state == "queued"


# ---------------------------------------------------------------------------
# Every §5 row
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "status",
    [
        layer5_pb2.UNAVAILABLE_UNKNOWN_BLOCK_SIZE,
        layer5_pb2.UNAVAILABLE_COMPRESSION_ENABLED,
        layer5_pb2.UNAVAILABLE_UNKNOWN_COMPRESSION,
        layer5_pb2.UNAVAILABLE_UNSUPPORTED_FORMAT,
        layer5_pb2.UNAVAILABLE_UNKNOWN_FORMAT,
        layer5_pb2.UNAVAILABLE_UNKNOWN_EXTENT,
        layer5_pb2.UNAVAILABLE_UNCALIBRATED,
        layer5_pb2.UNAVAILABLE_MAP_STALE,
    ],
)
def test_every_unavailable_status_reads_unordered_and_records(
    engine: Engine, tmp_path: Path, status: int
) -> None:
    """Guards: any UNAVAILABLE_* failing the restore instead of degrading."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        planner.responses.append(_unavailable_response(status, calibration_generation=3))
        request = _admit(session, config, digests)

        assert _slots(session, request.id) == []
        rows = _outcomes(session, request.id)
        expected = layer5_pb2.PlanStatus.Name(status).lower()
        assert [(row.phase, row.status) for row in rows] == [("initial", expected)]
        assert rows[0].calibration_generation == 3
        # Unordered means unordered dispatch, not failure: all items queued
        # and (with no slots) all claimable.
        jobs = _submit_restore_jobs(session, request)
        session.commit()
        assert sorted(job.id for job in pending_candidates(session)) == sorted(jobs.values())


def test_unimplemented_is_the_transport_row_old_rem_is_not_an_error(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: an old rem without ReadPlanService failing or disabling restores."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        planner.responses.append(FakeRpcError(grpc.StatusCode.UNIMPLEMENTED, "no such service"))
        request = _admit(session, config, digests)

        assert _slots(session, request.id) == []
        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [("initial", "rpc_unimplemented")]
        for item in request.items:
            assert item.state == "queued"


def test_rpc_transport_failure_reads_unordered_and_records(engine: Engine, tmp_path: Path) -> None:
    """Guards: a dead daemon failing admission instead of degrading."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        planner.responses.append(FakeRpcError(grpc.StatusCode.UNAVAILABLE, "connection refused"))
        request = _admit(session, config, digests)

        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [("initial", "rpc_transport_error")]
        assert "UNAVAILABLE" in (rows[0].detail or "")
        assert _slots(session, request.id) == []


def test_gettape_transport_failure_reads_unordered_and_records(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: fact-assembly RPC failures escaping the degradation envelope."""

    planner = FakePlanner()
    planner.facts_errors.append(FakeRpcError(grpc.StatusCode.UNAVAILABLE, "daemon down"))
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        request = _admit(session, config, digests)

        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [("initial", "rpc_transport_error")]
        assert _slots(session, request.id) == []


def test_invalid_argument_logs_loudly_with_violations_and_proceeds(
    engine: Engine, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Guards: a malformed request (sutradhara defect) killing the user's restore."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        planner.responses.append(_invalid_argument_error())
        with caplog.at_level(logging.ERROR, logger="sutradhara.hdcache.read_ordering"):
            request = _admit(session, config, digests)

        loud = [r for r in caplog.records if "rejected the request as malformed" in r.message]
        assert loud, "INVALID_ARGUMENT must be logged loudly"
        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [("initial", "rpc_invalid_argument")]
        assert "targets[1].end_block" in (rows[0].detail or ""), (
            "the violation detail must name the offending target"
        )
        assert _slots(session, request.id) == []
        for item in request.items:
            assert item.state == "queued"


def test_unknown_plan_status_downgrades_to_unordered(engine: Engine, tmp_path: Path) -> None:
    """Guards: a newer rem's unknown status being treated as success."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        response = layer5_pb2.PlanBatchReadResponse(max_targets=2730)
        response.status = 99  # proto3 open enum: a value this build does not know
        planner.responses.append(response)
        request = _admit(session, config, digests)

        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [("initial", "unknown_plan_status")]
        assert "99" in (rows[0].detail or "")
        assert _slots(session, request.id) == []


def test_unspecified_plan_status_sentinel_downgrades_too(engine: Engine, tmp_path: Path) -> None:
    """Guards: the never-emitted wire sentinel slipping through as success."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        planner.responses.append(
            layer5_pb2.PlanBatchReadResponse(
                status=layer5_pb2.PLAN_STATUS_UNSPECIFIED, max_targets=2730
            )
        )
        request = _admit(session, config, digests)
        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [("initial", "unknown_plan_status")]


def test_unknown_cost_model_basis_downgrades_to_unordered(engine: Engine, tmp_path: Path) -> None:
    """Guards: an unrecognised CostModelBasis passing as success (§5)."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))

        def respond(request: layer5_pb2.PlanBatchReadRequest):
            response = _ok_response(request)
            response.cost_model_basis = 77
            return response

        planner.responses.append(respond)
        request = _admit(session, config, digests)

        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [
            ("initial", "unknown_cost_model_basis")
        ]
        assert _slots(session, request.id) == []


def test_degraded_ascending_fallback_reads_in_returned_order(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: DEGRADED being treated as unavailable instead of an order."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        planner.responses.append(
            lambda request: _ok_response(request, status=layer5_pb2.DEGRADED_ASCENDING_FALLBACK)
        )
        request = _admit(session, config, digests)

        slots = _slots(session, request.id)
        assert len(slots) == 3
        assert all(slot.planned for slot in slots)
        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [
            ("initial", "degraded_ascending_fallback")
        ]


def test_all_unspanned_volume_records_no_spanned_targets(engine: Engine, tmp_path: Path) -> None:
    """Guards: a tape-bound volume with no spans going invisibly unordered."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner, unspanned={1, 2, 3})
        config = _config(tmp_path, _backend_map(session, backend_obj))
        request = _admit(session, config, digests)

        assert planner.plan_requests == [], "nothing to plan without spans"
        assert _slots(session, request.id) == []
        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [("initial", "no_spanned_targets")]


# ---------------------------------------------------------------------------
# Post-mount re-plan: exactly once per volume per job
# ---------------------------------------------------------------------------


def test_uncalibrated_then_post_mount_replan_exactly_once(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards: never re-planning after harvest, and re-planning more than once."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        planner.responses.append(_unavailable_response(layer5_pb2.UNAVAILABLE_UNCALIBRATED))
        request = _admit(session, config, digests)
        items = [_item_by_digest(request, digest) for digest in digests]
        assert _slots(session, request.id) == []

        _submit_restore_jobs(session, request)
        session.commit()
        order_log: list[int] = []
        _install_fake_serve(
            monkeypatch,
            config,
            {item.id: copies[item.content_sha256] for item in items},
            order_log,
        )
        # Next plan (the post-mount one) orders the remainder [item3, item2].
        planner.responses.append(
            lambda request: _ok_response(
                request,
                tag_order=[bytes(request.targets[1].tag), bytes(request.targets[0].tag)],
            )
        )
        run_pending(session, limit=0)

        # Item 1 ran first (unordered), its completion triggered the single
        # post-mount re-plan, and the remainder ran in the re-planned order.
        assert order_log == [items[0].id, items[2].id, items[1].id]
        assert len(planner.plan_requests) == 2, "exactly one post-mount re-plan"
        replan = planner.plan_requests[1]
        assert replan.HasField("start_position"), "the re-plan must not assume load point"
        served_span = _span_for(1)
        assert replan.start_position.block == served_span[1] - 1, (
            "the re-plan originates at the last completed target's end"
        )
        assert len(replan.targets) == 2
        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [
            ("initial", "unavailable_uncalibrated"),
            ("post_mount", "ok"),
        ]


def test_post_mount_replan_still_unavailable_stays_unordered_no_third_plan(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards: the exactly-once bound — still-unavailable must not retry forever."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        planner.responses.append(_unavailable_response(layer5_pb2.UNAVAILABLE_UNCALIBRATED))
        request = _admit(session, config, digests)
        items = [_item_by_digest(request, digest) for digest in digests]
        _submit_restore_jobs(session, request)
        session.commit()
        order_log: list[int] = []
        _install_fake_serve(
            monkeypatch,
            config,
            {item.id: copies[item.content_sha256] for item in items},
            order_log,
        )
        planner.responses.append(
            _unavailable_response(layer5_pb2.UNAVAILABLE_UNCALIBRATED, calibration_generation=4)
        )
        run_pending(session, limit=0)

        # One initial plan + exactly one post-mount re-plan, despite three
        # completions that could each have tried again.
        assert len(planner.plan_requests) == 2
        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [
            ("initial", "unavailable_uncalibrated"),
            ("post_mount", "unavailable_uncalibrated"),
        ]
        assert rows[1].calibration_generation == 4
        assert order_log == [items[0].id, items[1].id, items[2].id], "unordered = today's order"


# ---------------------------------------------------------------------------
# Read-failure re-plan
# ---------------------------------------------------------------------------


def test_read_failure_with_completed_target_replans_from_its_end(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards: re-planning from the load point (the origin the parent forbids)
    and tag instability across re-plans."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        request = _admit(session, config, digests)  # ordered [1, 2, 3]
        items = [_item_by_digest(request, digest) for digest in digests]
        original_tags = {slot.item_id: slot.tag for slot in _slots(session, request.id)}
        _submit_restore_jobs(session, request)
        session.commit()
        order_log: list[int] = []
        _install_fake_serve(
            monkeypatch,
            config,
            {item.id: copies[item.content_sha256] for item in items},
            order_log,
            outcomes={items[1].id: "failed"},
        )
        run_pending(session, limit=0)

        assert len(planner.plan_requests) == 2
        replan = planner.plan_requests[1]
        span_item1 = _span_for(1)
        assert replan.HasField("start_position")
        assert replan.start_position.block == span_item1[1] - 1, (
            "origin must be the last completed target's end, not the load point"
        )
        assert [bytes(target.tag) for target in replan.targets] == [original_tags[items[2].id]], (
            "tags must be stable across re-plans"
        )
        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [
            ("initial", "ok"),
            ("read_failure", "ok"),
        ]
        assert order_log == [items[0].id, items[1].id, items[2].id]
        session.expire_all()
        assert _item_by_digest(request, digests[1]).state == "failed"
        assert _item_by_digest(request, digests[2]).state == "done"


def test_read_failure_with_nothing_completed_never_replans(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guards: planning from an unknown head position — the forbidden origin."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, copies = _seed_world(session, planner)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        request = _admit(session, config, digests)
        items = [_item_by_digest(request, digest) for digest in digests]
        _submit_restore_jobs(session, request)
        session.commit()
        order_log: list[int] = []
        _install_fake_serve(
            monkeypatch,
            config,
            {item.id: copies[item.content_sha256] for item in items},
            order_log,
            outcomes={items[0].id: "failed"},  # the FIRST ordered read fails
        )
        run_pending(session, limit=0)

        assert len(planner.plan_requests) == 1, "no re-plan when nothing has completed"
        rows = _outcomes(session, request.id)
        assert [(row.phase, row.status) for row in rows] == [
            ("initial", "ok"),
            ("read_failure", "read_failure_unordered"),
        ]
        assert _slots(session, request.id) == [], "the remainder reads unordered"
        # The remainder still restored.
        session.expire_all()
        assert _item_by_digest(request, digests[1]).state == "done"
        assert _item_by_digest(request, digests[2]).state == "done"
        assert order_log == [items[0].id, items[1].id, items[2].id]


# ---------------------------------------------------------------------------
# Gate robustness
# ---------------------------------------------------------------------------


def test_gate_releases_when_predecessor_job_died_without_item_state(
    engine: Engine, tmp_path: Path
) -> None:
    """Guards: an engine-side crash (item still queued, job dead) wedging the
    volume forever — the deadlock valve."""

    planner = FakePlanner()
    with session_scope(engine) as session:
        backend_obj, digests, _copies = _seed_world(session, planner, count=2)
        config = _config(tmp_path, _backend_map(session, backend_obj))
        request = _admit(session, config, digests)
        items = [_item_by_digest(request, digest) for digest in digests]
        jobs = _submit_restore_jobs(session, request)
        session.commit()

        second_job = session.get(Job, jobs[items[1].id])
        assert second_job is not None
        assert restore_release_allowed(session, second_job) is False

        # Kill the head's job engine-side; its item stays 'queued'.
        first_job = session.get(Job, jobs[items[0].id])
        assert first_job is not None
        first_job.status = JobStatus.FAILED
        session.flush()
        assert restore_release_allowed(session, second_job) is True


def test_items_in_no_list_dispatch_as_today(engine: Engine, tmp_path: Path) -> None:
    """Guards: the gate leaking beyond planned volumes."""

    with session_scope(engine) as session:
        # A plain memory backend with no plan surface: no planning happens.
        backend_obj = MemoryBackend("plain-mem")
        digest, _backend_id, _copy_id = _seed_tape_asset(
            session,
            backend_obj,
            data=b"unplanned",
            tape_uuid=TAPE_B,
            tape_file_number=1,
        )
        config = _config(tmp_path, _backend_map(session, backend_obj))
        request = _admit(session, config, [digest])
        assert _slots(session, request.id) == []
        assert _outcomes(session, request.id) == []
        jobs = _submit_restore_jobs(session, request)
        session.commit()
        assert sorted(job.id for job in pending_candidates(session)) == sorted(jobs.values())
