"""Restore read ordering — sutradhara's `PlanBatchRead` consumer.

Design of record: design-restore-read-ordering.md §§4-5 (system journal);
wire contract: contract-read-ordering.md. Three pieces live here:

- the **request-level planning pass** (`plan_restore_request_read_order`),
  run after a restore request's items are accepted and before item
  dispatch: predict per item tape-vs-cache and the copy with the same
  choice logic serving uses, group tape-bound items by volume, call
  `PlanBatchRead` once per volume, persist the ordered list;
- the **dispatcher release gate** (`restore_release_allowed`), consulted
  by the jobs engine before claiming a `restore` job: a volume's items
  are released per the persisted list; items in no list dispatch as
  today;
- the **runtime hooks** (`note_restore_item_outcome`) implementing the
  post-mount re-plan (exactly once per volume per job) and the
  read-failure re-plan (one fresh plan from the last completed target's
  end; with nothing completed, no re-plan — the head's position is
  unknown and sutradhara does not control the head).

Ordering is an optimisation. Every path in this module is wrapped so a
restore can never fail — or deadlock — because ordering could not be
computed; failures degrade loudly to unordered and are recorded on the
`restore_ordering_outcome` ledger.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

import grpc
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from sutradhara._proto import layer5_pb2
from sutradhara._proto.google.rpc import error_details_pb2, status_pb2
from sutradhara.hdcache.models import (
    RestoreOrderingOutcome,
    RestoreReadPlanSlot,
    RestoreRequest,
    RestoreRequestItem,
)

if TYPE_CHECKING:
    from sutradhara.hdcache.manager import RestoreConfig
    from sutradhara.jobs.models import Job

LOG = logging.getLogger(__name__)

PHASE_INITIAL = "initial"
PHASE_POST_MOUNT = "post_mount"
PHASE_READ_FAILURE = "read_failure"

#: Item states after which a release slot no longer blocks its successors.
_SETTLED_ITEM_STATES = {"done", "denied", "failed", "sent"}

#: PlanStatus wire name -> ledger status. `PLAN_STATUS_UNSPECIFIED` is a
#: sentinel the daemon never emits; it is deliberately absent so receiving
#: it downgrades to `unknown_plan_status` like any unrecognised value.
_PLAN_STATUS_TO_OUTCOME = {
    "OK": "ok",
    "DEGRADED_ASCENDING_FALLBACK": "degraded_ascending_fallback",
    "UNAVAILABLE_UNKNOWN_BLOCK_SIZE": "unavailable_unknown_block_size",
    "UNAVAILABLE_COMPRESSION_ENABLED": "unavailable_compression_enabled",
    "UNAVAILABLE_UNKNOWN_COMPRESSION": "unavailable_unknown_compression",
    "UNAVAILABLE_UNSUPPORTED_FORMAT": "unavailable_unsupported_format",
    "UNAVAILABLE_UNKNOWN_FORMAT": "unavailable_unknown_format",
    "UNAVAILABLE_UNKNOWN_EXTENT": "unavailable_unknown_extent",
    "UNAVAILABLE_UNCALIBRATED": "unavailable_uncalibrated",
    "UNAVAILABLE_MAP_STALE": "unavailable_map_stale",
}

_ORDERED_OUTCOMES = {"ok", "degraded_ascending_fallback"}

_KNOWN_COST_MODEL_BASES = frozenset(layer5_pb2.CostModelBasis.values())


class ReadOrderingPlanner(Protocol):
    """The three metadata-only calls the planning pass consumes.

    `RemanenceBackend` implements this for live daemons via
    `read_ordering_planner()`; tests substitute fakes. gRPC errors
    propagate out of all three — this module owns the §5 mapping.
    """

    def get_tape_facts(self, tape_uuid: bytes) -> Any: ...

    def get_copy_read_span(self, locator: Any) -> tuple[int, int] | None: ...

    def plan_batch_read(
        self, request: layer5_pb2.PlanBatchReadRequest
    ) -> layer5_pb2.PlanBatchReadResponse: ...


def generate_item_tag(item_id: int) -> bytes:
    """The tag rule (normative): a sutradhara-generated correlation token.

    Unique per restore item within one request (item ids are globally
    unique), stable across re-plans (deterministic in the item id), and
    never merely an object/copy/span id — duplicate content is legal and
    all three are ambiguous; the restore item is not.
    """

    return b"rri-%d" % item_id


@dataclass
class _TapePrediction:
    """One item predicted to read tape, with the copy serving would pick."""

    item: RestoreRequestItem
    tape_uuid: bytes
    locator: dict[str, Any]
    planner: ReadOrderingPlanner
    tag: bytes


@dataclass(frozen=True)
class _PlannedTarget:
    """One ReadTarget as sent on the wire (end_block inclusive)."""

    item_id: int
    tag: bytes
    start_block: int
    end_block: int


# ---------------------------------------------------------------------------
# The planning pass
# ---------------------------------------------------------------------------


def plan_restore_request_read_order(
    session: Session,
    request: RestoreRequest,
    *,
    config: RestoreConfig | None = None,
) -> None:
    """Request-level planning pass: runs post-acceptance, pre-dispatch.

    Never raises: any failure degrades to unordered dispatch, loudly.
    The caller owns the transaction, matching `admit_restore_request`.
    """

    try:
        _plan_request(session, request, config)
    except Exception:
        LOG.exception(
            "read-ordering planning pass failed for restore request %s; "
            "all items dispatch unordered",
            request.id,
        )


def _plan_request(
    session: Session,
    request: RestoreRequest,
    config: RestoreConfig | None,
) -> None:
    if request.delivery_mode != "server_local":
        # The dispatcher below only releases server-local worker jobs; the
        # agent-delivery pull surface has no ordering enforcement yet
        # (reported gap, not silently wired).
        return
    items = [item for item in request.items if item.state == "queued" and item.id is not None]
    if not items:
        return
    predictions = _predict_tape_items(session, items, config)
    if not predictions:
        return

    # The tag rule's defect path: two distinct items producing the same tag
    # is a loud internal defect — log, count, fall back to unordered for
    # every volume involved; never silently pick one.
    tags_seen: dict[bytes, int] = {}
    collision = False
    for prediction in predictions:
        earlier = tags_seen.get(prediction.tag)
        if earlier is not None and earlier != prediction.item.id:
            collision = True
            LOG.error(
                "read-ordering tag collision (internal defect): tag %r generated for "
                "both item %d and item %d in request %s; dispatching unordered",
                prediction.tag,
                earlier,
                prediction.item.id,
                request.id,
            )
        tags_seen[prediction.tag] = prediction.item.id or 0
    volumes: dict[bytes, list[_TapePrediction]] = {}
    for prediction in predictions:
        volumes.setdefault(prediction.tape_uuid, []).append(prediction)
    if collision:
        for tape_uuid in volumes:
            _record_outcome(
                session,
                request.id,
                tape_uuid,
                phase=PHASE_INITIAL,
                status="tag_collision",
                detail="duplicate correlation tag generated; internal defect",
            )
        return

    for tape_uuid, volume_predictions in volumes.items():
        try:
            _plan_volume_initial(session, request, tape_uuid, volume_predictions)
        except Exception:
            LOG.exception(
                "read-ordering planning failed for request %s volume %s; "
                "that volume dispatches unordered",
                request.id,
                tape_uuid.hex(),
            )
            _record_outcome(
                session,
                request.id,
                tape_uuid,
                phase=PHASE_INITIAL,
                status="planning_error",
                detail="unexpected error during planning; see logs",
            )


def _predict_tape_items(
    session: Session,
    items: list[RestoreRequestItem],
    config: RestoreConfig | None,
) -> list[_TapePrediction]:
    """Predict tape-vs-cache and the copy with serving's own choice logic.

    The prediction is advice, like the plan itself: an item predicted for
    tape that serves from cache skips its hop harmlessly; an item that
    falls back from cache to tape mid-serve joins the unordered tail.
    """

    from sutradhara.hdcache.manager import _select_cache_entry, restore_config_from_env

    final_config = config or restore_config_from_env()
    predictions: list[_TapePrediction] = []
    for item in items:
        try:
            entry = _select_cache_entry(
                session,
                item.content_sha256,
                config=final_config,
                # Prediction must be side-effect free; serve time probes.
                allow_recovery_probe=False,
            )
            if entry is not None:
                continue  # cache-predicted: in no list, dispatches as today
            member = _first_restore_member(session, item, final_config)
            if member is None:
                continue
            planner_factory = getattr(member.backend, "read_ordering_planner", None)
            planner = planner_factory() if callable(planner_factory) else None
            if planner is None:
                continue  # backend has no live plan surface
            # The copy's locator carries the tape identity (tape_uuid,
            # tape_file_number, object_id); the asset locator is member-
            # scoped and may not.
            locator = dict(member.copy.native_locator)
            tape_uuid_hex = locator.get("tape_uuid")
            if not isinstance(tape_uuid_hex, str):
                continue
            tape_uuid = bytes.fromhex(tape_uuid_hex)
            if len(tape_uuid) != 16:
                continue
            if item.id is None:
                raise RuntimeError("persisted restore item has no id")
            predictions.append(
                _TapePrediction(
                    item=item,
                    tape_uuid=tape_uuid,
                    locator=locator,
                    planner=planner,
                    tag=generate_item_tag(item.id),
                )
            )
        except Exception:
            LOG.debug(
                "read-ordering prediction failed for item %s; it dispatches as today",
                item.id,
                exc_info=True,
            )
    return predictions


def _first_restore_member(
    session: Session,
    item: RestoreRequestItem,
    config: RestoreConfig,
) -> Any:
    """The first candidate serving would try (`manager._serve_from_tape`)."""

    from sutradhara.archive_restore import build_restore_plan
    from sutradhara.hdcache.manager import restore_backends_for_artifactclass

    backends = config.restore_backends
    if backends is None:
        resolver = config.restore_backend_resolver or restore_backends_for_artifactclass
        backends = resolver(session, item.artifactclass)
    if not backends:
        return None
    plan = build_restore_plan(
        session,
        asset_hash=item.content_sha256,
        artifactclass=item.artifactclass,
        backends=backends,
        extractor=config.extractor,
    )
    return next(iter(plan.iter_members()), None)


def _plan_volume_initial(
    session: Session,
    request: RestoreRequest,
    tape_uuid: bytes,
    predictions: list[_TapePrediction],
) -> None:
    planner = predictions[0].planner
    targets: list[_PlannedTarget] = []
    tail_item_ids: list[int] = []
    try:
        for prediction in predictions:
            span = planner.get_copy_read_span(prediction.locator)
            item_id = prediction.item.id
            if item_id is None:
                raise RuntimeError("persisted restore item has no id")
            if span is None:
                tail_item_ids.append(item_id)
                continue
            start, end_exclusive = span
            if end_exclusive <= start:
                LOG.warning(
                    "read-ordering: degenerate span [%d, %d) for item %d; treating as absent",
                    start,
                    end_exclusive,
                    item_id,
                )
                tail_item_ids.append(item_id)
                continue
            targets.append(
                _PlannedTarget(
                    item_id=item_id,
                    tag=prediction.tag,
                    start_block=start,
                    # The planner's contract is inclusive; the catalog span
                    # is exclusive. One token never names both fenceposts:
                    # ReadTarget.end_block = span_end_exclusive - 1.
                    end_block=end_exclusive - 1,
                )
            )
    except grpc.RpcError as error:
        _record_rpc_failure(session, request.id, tape_uuid, PHASE_INITIAL, error)
        return

    _plan_volume(
        session,
        request,
        tape_uuid,
        planner,
        targets,
        tail_item_ids,
        phase=PHASE_INITIAL,
        start_block=None,
    )


def _plan_volume(
    session: Session,
    request: RestoreRequest,
    tape_uuid: bytes,
    planner: ReadOrderingPlanner,
    targets: list[_PlannedTarget],
    tail_item_ids: list[int],
    *,
    phase: str,
    start_block: int | None,
) -> None:
    """Plan one volume and adopt or degrade per the §5 table."""

    if not targets:
        _rewrite_volume_slots(session, request.id, tape_uuid, [], [])
        _record_outcome(
            session,
            request.id,
            tape_uuid,
            phase=phase,
            status="no_spanned_targets",
            detail="no copy on this volume carries a global block span",
        )
        return

    try:
        facts = planner.get_tape_facts(tape_uuid)
        wire_request = layer5_pb2.PlanBatchReadRequest(
            cartridge=layer5_pb2.CartridgeFacts(
                # Generation and recording format are left empty on purpose:
                # rem resolves them from the barcode (contract §6.3).
                voltag=facts.voltag,
                block_size_bytes=facts.block_size_bytes,
                # Sutradhara's own write policy: it declares what it knows,
                # never assumes. Silence (UNSPECIFIED) is not permission.
                compression=layer5_pb2.COMPRESSION_DISABLED,
                # In CartridgeFacts 0 means unknown (the contract's own
                # encoding); the R1 Tape field's absence maps to it here.
                written_extent_lba=facts.written_extent_lba or 0,
            ),
            targets=[
                layer5_pb2.ReadTarget(
                    partition=0,
                    start_block=target.start_block,
                    end_block=target.end_block,
                    tag=target.tag,
                )
                for target in targets
            ],
            objective=layer5_pb2.MIN_TOTAL_TIME,
            tape_uuid=tape_uuid,
        )
        if start_block is not None:
            wire_request.start_position.partition = 0
            wire_request.start_position.block = start_block
        response = planner.plan_batch_read(wire_request)
    except grpc.RpcError as error:
        _rewrite_volume_slots(session, request.id, tape_uuid, [], [])
        _record_rpc_failure(session, request.id, tape_uuid, phase, error)
        return

    status_name = _PLAN_STATUS_TO_OUTCOME.get(_enum_name(layer5_pb2.PlanStatus, response.status))
    calibration_generation = int(response.calibration_generation)
    if status_name is None:
        _rewrite_volume_slots(session, request.id, tape_uuid, [], [])
        _record_outcome(
            session,
            request.id,
            tape_uuid,
            phase=phase,
            status="unknown_plan_status",
            detail=f"unrecognised PlanStatus value {int(response.status)}; "
            "unknown values are never treated as success",
            calibration_generation=calibration_generation,
        )
        return
    if (
        status_name in _ORDERED_OUTCOMES
        and int(response.cost_model_basis) not in _KNOWN_COST_MODEL_BASES
    ):
        # cost_model_basis is read (estimates stay unused for scheduling);
        # an unrecognised value downgrades rather than passing as success.
        _rewrite_volume_slots(session, request.id, tape_uuid, [], [])
        _record_outcome(
            session,
            request.id,
            tape_uuid,
            phase=phase,
            status="unknown_cost_model_basis",
            detail=f"unrecognised CostModelBasis value {int(response.cost_model_basis)}",
            calibration_generation=calibration_generation,
        )
        return
    if status_name not in _ORDERED_OUTCOMES:
        _rewrite_volume_slots(session, request.id, tape_uuid, [], [])
        _record_outcome(
            session,
            request.id,
            tape_uuid,
            phase=phase,
            status=status_name,
            detail=response.detail or None,
            calibration_generation=calibration_generation,
        )
        return

    # OK or DEGRADED_ASCENDING_FALLBACK: read in the returned order.
    by_tag = {target.tag: target for target in targets}
    ordered: list[_PlannedTarget] = []
    for hop in response.hops:
        target = by_tag.pop(bytes(hop.target.tag), None)
        if target is None:
            LOG.warning(
                "read-ordering: plan for request %s volume %s returned unknown tag %r; ignoring",
                request.id,
                tape_uuid.hex(),
                bytes(hop.target.tag),
            )
            continue
        ordered.append(target)
    if by_tag:
        # The contract says the returned order is a permutation of the
        # supplied targets; if a target went missing anyway, its item joins
        # the unordered tail rather than being stranded.
        LOG.warning(
            "read-ordering: plan for request %s volume %s omitted %d supplied target(s); "
            "those items join the unordered tail",
            request.id,
            tape_uuid.hex(),
            len(by_tag),
        )
        tail_item_ids = [*tail_item_ids, *(target.item_id for target in by_tag.values())]
    _rewrite_volume_slots(session, request.id, tape_uuid, ordered, tail_item_ids)
    _record_outcome(
        session,
        request.id,
        tape_uuid,
        phase=phase,
        status=status_name,
        detail=response.detail or None,
        calibration_generation=calibration_generation,
    )


def _rewrite_volume_slots(
    session: Session,
    request_id: str,
    tape_uuid: bytes,
    ordered: list[_PlannedTarget],
    tail_item_ids: list[int],
) -> None:
    """Replace this volume's release slots with the current plan (whiteboard)."""

    session.execute(
        delete(RestoreReadPlanSlot).where(
            RestoreReadPlanSlot.request_id == request_id,
            RestoreReadPlanSlot.tape_uuid == tape_uuid,
        )
    )
    position = 0
    for target in ordered:
        session.add(
            RestoreReadPlanSlot(
                request_id=request_id,
                tape_uuid=tape_uuid,
                position=position,
                item_id=target.item_id,
                planned=True,
                tag=target.tag,
                start_block=target.start_block,
                end_block=target.end_block,
            )
        )
        position += 1
    # Unspanned items append after the ordered ones, in today's order
    # (ascending item id = submission order). They release together once
    # the planned slots settle — today's concurrency, after the order.
    for item_id in sorted(tail_item_ids):
        session.add(
            RestoreReadPlanSlot(
                request_id=request_id,
                tape_uuid=tape_uuid,
                position=position,
                item_id=item_id,
                planned=False,
            )
        )
        position += 1
    session.flush()


def _record_rpc_failure(
    session: Session,
    request_id: str,
    tape_uuid: bytes,
    phase: str,
    error: grpc.RpcError,
) -> None:
    code = error.code()
    if code == grpc.StatusCode.UNIMPLEMENTED:
        # The transport row for an old rem without ReadPlanService: not an
        # error, must not fail or disable restores.
        _record_outcome(
            session,
            request_id,
            tape_uuid,
            phase=phase,
            status="rpc_unimplemented",
            detail="remanence daemon predates ReadPlanService; reading unordered",
        )
        return
    if code == grpc.StatusCode.INVALID_ARGUMENT:
        # Almost always a sutradhara defect (a fetch-order race can produce
        # one innocently): log loudly with the violation detail, count it,
        # and proceed unordered — the user's restore proceeds.
        violations = _bad_request_violations(error)
        detail = str(error.details() or "")
        if violations:
            detail = f"{detail}; violations: {violations}"
        LOG.error(
            "read-ordering: PlanBatchRead rejected the request as malformed "
            "(request %s volume %s): %s",
            request_id,
            tape_uuid.hex(),
            detail,
        )
        _record_outcome(
            session,
            request_id,
            tape_uuid,
            phase=phase,
            status="rpc_invalid_argument",
            detail=detail or None,
        )
        return
    _record_outcome(
        session,
        request_id,
        tape_uuid,
        phase=phase,
        status="rpc_transport_error",
        detail=f"{getattr(code, 'name', code)}: {error.details() or ''}".strip(": "),
    )


def _bad_request_violations(error: grpc.RpcError) -> list[dict[str, str]]:
    """Decode `google.rpc.BadRequest` field violations from the trailers."""

    try:
        metadata = error.trailing_metadata() or ()
        for key, value in metadata:
            if key != "grpc-status-details-bin":
                continue
            status = status_pb2.Status()
            status.MergeFromString(value if isinstance(value, bytes) else value.encode())
            violations: list[dict[str, str]] = []
            for detail in status.details:
                bad_request = error_details_pb2.BadRequest()
                if detail.Unpack(bad_request):
                    violations.extend(
                        {"field": violation.field, "description": violation.description}
                        for violation in bad_request.field_violations
                    )
            return violations
    except Exception:
        LOG.debug("failed to decode BadRequest details", exc_info=True)
    return []


def _record_outcome(
    session: Session,
    request_id: str,
    tape_uuid: bytes,
    *,
    phase: str,
    status: str,
    detail: str | None = None,
    calibration_generation: int | None = None,
) -> None:
    session.add(
        RestoreOrderingOutcome(
            request_id=request_id,
            tape_uuid=tape_uuid,
            phase=phase,
            status=status,
            detail=detail,
            calibration_generation=calibration_generation,
        )
    )
    session.flush()


def _enum_name(enum_type: Any, value: int) -> str:
    try:
        return str(enum_type.Name(value))
    except ValueError:
        return f"<unknown:{int(value)}>"


# ---------------------------------------------------------------------------
# The dispatcher release gate
# ---------------------------------------------------------------------------


def restore_release_allowed(session: Session, job: Job) -> bool:
    """Release a volume's items per the persisted list.

    Items in no list release as today. A planned slot releases when every
    earlier slot on its volume has settled; a tail slot releases when all
    planned slots have settled (then today's concurrency applies). Any
    gate failure releases — ordering must never wedge a restore.
    """

    try:
        params = job.params or {}
        item_id = params.get("restore_request_item_id")
        if not isinstance(item_id, int):
            return True
        slot = session.scalars(
            select(RestoreReadPlanSlot).where(RestoreReadPlanSlot.item_id == item_id)
        ).one_or_none()
        if slot is None:
            return True
        predecessors_query = select(RestoreReadPlanSlot).where(
            RestoreReadPlanSlot.request_id == slot.request_id,
            RestoreReadPlanSlot.tape_uuid == slot.tape_uuid,
            RestoreReadPlanSlot.planned.is_(True),
        )
        if slot.planned:
            predecessors_query = predecessors_query.where(
                RestoreReadPlanSlot.position < slot.position
            )
        for predecessor in session.scalars(predecessors_query):
            if predecessor.item_id == item_id:
                continue
            if not _slot_item_settled(session, predecessor):
                return False
        return True
    except Exception:
        LOG.exception(
            "read-ordering release gate failed for job %s; releasing",
            getattr(job, "id", None),
        )
        return True


def _slot_item_settled(session: Session, slot: RestoreReadPlanSlot) -> bool:
    from sutradhara.jobs.models import LIVE_JOB_STATUS_VALUES, Job

    item = session.get(RestoreRequestItem, slot.item_id)
    if item is None:
        return True
    if item.state in _SETTLED_ITEM_STATES:
        return True
    if item.state == "queued":
        # Deadlock valve: a predecessor whose restore job died engine-side
        # (or was never submitted) must not wedge the volume forever. With
        # no live job left to serve it, successors release.
        live = session.scalars(
            select(Job.id)
            .where(
                Job.kind == "restore",
                Job.status.in_(LIVE_JOB_STATUS_VALUES),
                Job.params["restore_request_item_id"].as_integer() == slot.item_id,
            )
            .limit(1)
        ).first()
        return live is None
    # waking_disk / streaming / fell_back_to_tape: actively being served.
    return False


# ---------------------------------------------------------------------------
# Runtime hooks: post-mount re-plan and read-failure re-plan
# ---------------------------------------------------------------------------


def note_restore_item_outcome(
    session: Session,
    item: RestoreRequestItem,
    *,
    served_copy_id: int | None = None,
    config: RestoreConfig | None = None,
) -> None:
    """Observe one served item and apply the §4 re-plan rules. Never raises."""

    try:
        request = item.request
        if request is None or request.delivery_mode != "server_local":
            return
        if item.state == "done" and item.source == "tape":
            _maybe_post_mount_replan(session, request, item, served_copy_id, config)
        elif item.state == "failed":
            _maybe_read_failure_replan(session, request, item, config)
    except Exception:
        LOG.exception(
            "read-ordering runtime hook failed for item %s; continuing unordered",
            item.id,
        )


def _maybe_post_mount_replan(
    session: Session,
    request: RestoreRequest,
    item: RestoreRequestItem,
    served_copy_id: int | None,
    config: RestoreConfig | None,
) -> None:
    """Exactly one post-mount re-plan per volume per job.

    `UNAVAILABLE_UNCALIBRATED` before mount is the expected first-restore
    case: the volume proceeded unordered, the first tape read mounted the
    volume (which harvests — read-mounts DO calibrate), and this hook
    re-plans the remainder once. Still unavailable: unordered, recorded.
    """

    if served_copy_id is None:
        return
    from sutradhara.catalog.models import Copy

    copy = session.get(Copy, served_copy_id)
    if copy is None:
        return
    tape_uuid_hex = (copy.native_locator or {}).get("tape_uuid")
    if not isinstance(tape_uuid_hex, str):
        return
    tape_uuid = bytes.fromhex(tape_uuid_hex)
    if len(tape_uuid) != 16:
        return

    outcomes = list(
        session.scalars(
            select(RestoreOrderingOutcome).where(
                RestoreOrderingOutcome.request_id == request.id,
                RestoreOrderingOutcome.tape_uuid == tape_uuid,
            )
        )
    )
    initial_uncalibrated = any(
        outcome.phase == PHASE_INITIAL and outcome.status == "unavailable_uncalibrated"
        for outcome in outcomes
    )
    already_replanned = any(outcome.phase == PHASE_POST_MOUNT for outcome in outcomes)
    if not initial_uncalibrated or already_replanned:
        return

    # The re-plan origin is the last completed target's end. The copy just
    # served is that target; its span comes from rem at re-plan time (the
    # serve is complete, so the span is the one actually read). Without a
    # span the head position is unknown and no re-plan happens — the
    # exactly-once bound is only consumed by a performed re-plan.
    queued = [i for i in request.items if i.state == "queued" and i.id is not None]
    if not queued:
        return
    predictions = [
        prediction
        for prediction in _predict_tape_items(session, queued, config)
        if prediction.tape_uuid == tape_uuid
    ]
    if not predictions:
        return
    planner = predictions[0].planner
    span = planner.get_copy_read_span(copy.native_locator or {})
    if span is None:
        return
    origin_end_block = span[1] - 1

    targets: list[_PlannedTarget] = []
    tail_item_ids: list[int] = []
    try:
        for prediction in predictions:
            item_span = planner.get_copy_read_span(prediction.locator)
            item_id = prediction.item.id
            if item_id is None:
                raise RuntimeError("persisted restore item has no id")
            if item_span is None or item_span[1] <= item_span[0]:
                tail_item_ids.append(item_id)
                continue
            targets.append(
                _PlannedTarget(
                    item_id=item_id,
                    tag=prediction.tag,
                    start_block=item_span[0],
                    end_block=item_span[1] - 1,
                )
            )
    except grpc.RpcError as error:
        _record_rpc_failure(session, request.id, tape_uuid, PHASE_POST_MOUNT, error)
        return

    _plan_volume(
        session,
        request,
        tape_uuid,
        planner,
        targets,
        tail_item_ids,
        phase=PHASE_POST_MOUNT,
        start_block=origin_end_block,
    )


def _maybe_read_failure_replan(
    session: Session,
    request: RestoreRequest,
    item: RestoreRequestItem,
    config: RestoreConfig | None,
) -> None:
    """One fresh plan from the last completed target's end after a failed read.

    With nothing completed the head's position is genuinely unknown and
    sutradhara does not control the head: no re-plan — the remainder reads
    unordered, recorded. Planning from an origin the head is not at is the
    one option the parent design forbids.
    """

    slot = session.scalars(
        select(RestoreReadPlanSlot).where(RestoreReadPlanSlot.item_id == item.id)
    ).one_or_none()
    if slot is None or not slot.planned:
        # Unordered/tail failures leave nothing to re-plan: tail reads have
        # unknown spans, so the head position is unknown by construction.
        return
    tape_uuid = slot.tape_uuid
    volume_slots = list(
        session.scalars(
            select(RestoreReadPlanSlot)
            .where(
                RestoreReadPlanSlot.request_id == request.id,
                RestoreReadPlanSlot.tape_uuid == tape_uuid,
            )
            .order_by(RestoreReadPlanSlot.position)
        )
    )
    items_by_id = {i.id: i for i in request.items if i.id is not None}

    completed_end: int | None = None
    for volume_slot in volume_slots:
        if not volume_slot.planned or volume_slot.end_block is None:
            continue
        slot_item = items_by_id.get(volume_slot.item_id) or session.get(
            RestoreRequestItem, volume_slot.item_id
        )
        if slot_item is not None and slot_item.state == "done" and slot_item.source == "tape":
            completed_end = volume_slot.end_block

    remaining_targets: list[_PlannedTarget] = []
    remaining_tail: list[int] = []
    for volume_slot in volume_slots:
        slot_item = items_by_id.get(volume_slot.item_id) or session.get(
            RestoreRequestItem, volume_slot.item_id
        )
        if slot_item is None or slot_item.state != "queued":
            continue
        if (
            volume_slot.planned
            and volume_slot.tag is not None
            and volume_slot.start_block is not None
            and volume_slot.end_block is not None
        ):
            remaining_targets.append(
                _PlannedTarget(
                    item_id=volume_slot.item_id,
                    tag=volume_slot.tag,
                    start_block=volume_slot.start_block,
                    end_block=volume_slot.end_block,
                )
            )
        else:
            remaining_tail.append(volume_slot.item_id)

    if completed_end is None:
        _rewrite_volume_slots(session, request.id, tape_uuid, [], [])
        _record_outcome(
            session,
            request.id,
            tape_uuid,
            phase=PHASE_READ_FAILURE,
            status="read_failure_unordered",
            detail="read failed with no completed target; head position unknown, "
            "remainder reads unordered",
        )
        return
    if not remaining_targets and not remaining_tail:
        return

    planner = _planner_for_remaining(session, request, remaining_targets, remaining_tail, config)
    if planner is None:
        _rewrite_volume_slots(session, request.id, tape_uuid, [], [])
        _record_outcome(
            session,
            request.id,
            tape_uuid,
            phase=PHASE_READ_FAILURE,
            status="planning_error",
            detail="no plan-capable backend available for the re-plan",
        )
        return
    # The re-plan re-sends the targets actually adopted (same tags — the
    # tag rule requires stability across re-plans) from the current
    # position: the end block of the last completed target.
    _plan_volume(
        session,
        request,
        slot.tape_uuid,
        planner,
        remaining_targets,
        remaining_tail,
        phase=PHASE_READ_FAILURE,
        start_block=completed_end,
    )


def _planner_for_remaining(
    session: Session,
    request: RestoreRequest,
    remaining_targets: list[_PlannedTarget],
    remaining_tail: list[int],
    config: RestoreConfig | None,
) -> ReadOrderingPlanner | None:
    remaining_ids = {target.item_id for target in remaining_targets} | set(remaining_tail)
    queued = [i for i in request.items if i.id in remaining_ids and i.state == "queued"]
    for prediction in _predict_tape_items(session, queued, config):
        return prediction.planner
    return None
