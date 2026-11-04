"""Explicit Remanence maintenance requests over caller-owned RPC clients.

The caller owns operator confirmation and persists the closeout idempotency key
for retries. These helpers neither select media nor create network channels.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator

import grpc

from ._proto import layer5_pb2 as pb
from ._proto.layer5_pb2_grpc import CatalogStub, DaemonStub


def supersede_tail(
    catalog: CatalogStub,
    tape_uuid: uuid.UUID,
    *,
    confirm: Callable[[pb.SupersedeTailResponse], bool],
    timeout: float = 900,
    max_previews: int = 3,
) -> pb.SupersedeTailResponse:
    """Preview and confirm the exact native sequence; ask again after stale preview."""
    if max_previews < 1:
        raise ValueError("max_previews must be positive")
    for attempt in range(max_previews):
        preview: pb.SupersedeTailResponse = catalog.SupersedeTail(
            pb.SupersedeTailRequest(tape_uuid=tape_uuid.bytes, ack_tape_uuid=tape_uuid.bytes),
            timeout=timeout,
        )
        # Copy before invoking caller code so confirmation cannot mutate the binding.
        request = pb.SupersedeTailRequest(
            tape_uuid=tape_uuid.bytes,
            ack_tape_uuid=tape_uuid.bytes,
            confirm_supersede=preview.required_confirmation,
            expected_found=preview.found,
            observed_tape_log_sequence=preview.tape_log_sequence,
        )
        if not confirm(preview):
            raise PermissionError("supersede-tail was not confirmed")
        try:
            response: pb.SupersedeTailResponse = catalog.SupersedeTail(request, timeout=timeout)
            return response
        except grpc.RpcError as exc:
            if (
                exc.code() != grpc.StatusCode.FAILED_PRECONDITION
                or not (exc.details() or "").startswith("TailPreviewStale:")
                or attempt + 1 == max_previews
            ):
                raise
    raise AssertionError("unreachable preview loop")


def closeout_tape(
    catalog: CatalogStub,
    daemon: DaemonStub,
    tape_uuid: uuid.UUID,
    *,
    idempotency_key: uuid.UUID,
    expected_pool_id: str | None = None,
    timeout: float = 900,
) -> pb.OperationStatus:
    """Wait for checked closeout, reusing the caller's key for any later retry."""
    if idempotency_key.int == 0:
        raise ValueError("CloseoutTape idempotency_key must not be the nil UUID")
    request = pb.CloseoutTapeRequest(
        tape_uuid=tape_uuid.bytes, idempotency_key=pb.IdempotencyKey(value=idempotency_key.bytes)
    )
    if expected_pool_id is not None:
        request.expected_pool_id = expected_pool_id
    operation: pb.OperationRef = catalog.CloseoutTape(request, timeout=timeout)
    statuses: Iterator[pb.OperationStatus] = daemon.WatchOperation(
        pb.GetOperationRequest(operation_id=operation.operation_id), timeout=timeout
    )
    for status in statuses:
        if (
            status.operation_id != operation.operation_id
            or status.operation_kind != "tape_closeout"
        ):
            raise RuntimeError("closeout operation identity mismatch")
        if status.state == pb.OPERATION_STATE_SUCCEEDED:
            return status
        if status.state in (
            pb.OPERATION_STATE_FAILED,
            pb.OPERATION_STATE_CANCELLED,
            pb.OPERATION_STATE_COMPLETION_UNKNOWN,
        ):
            raise RuntimeError(status.error_summary or "closeout operation did not succeed")
    raise RuntimeError("closeout operation watch ended without a terminal status")
