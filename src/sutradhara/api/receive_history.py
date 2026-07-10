"""Durable card-identity history projection for receive duplicate handling.

The projection joins the existing HTTP receive intent, gRPC intake, and catalog
intake rows.  It deliberately derives ``verified`` only from registered catalog
state; watcher marker files are never treated as durable verification evidence.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from sutradhara.api import store as api_store
from sutradhara.catalog.models import IngestItem, Intake
from sutradhara.catalog.types import IntakeStatus
from sutradhara.grpc import store as grpc_store
from sutradhara.grpc.status import intake_receipt_summary

HistoryState = Literal["verifying", "verified", "failed", "quarantined", "revoked"]


@dataclass(frozen=True)
class ReceiveHistoryMatch:
    """One authorization-scoped latest history match for a card identity."""

    intake_id: str
    card_label: str | None
    device_id: str | None
    received_at: dt.datetime
    state: HistoryState
    file_count: int
    visible: bool

    def warning_payload(self) -> dict[str, object]:
        """Return the normative duplicate-warning wire payload."""

        payload: dict[str, object] = {
            "matchKind": "card_identity",
            "visible": self.visible,
        }
        if self.visible:
            payload["priorIntake"] = {
                "intakeId": self.intake_id,
                "cardLabel": self.card_label,
                "deviceId": self.device_id,
                "receivedAt": _aware(self.received_at).isoformat(),
                "state": self.state,
                "fileCount": self.file_count,
            }
        return payload


@dataclass
class _Attempt:
    intake_id: str
    operator: str
    received_at: dt.datetime
    card_label: str | None = None
    device_id: str | None = None
    catalog: Intake | None = None
    grpc: grpc_store.GrpcIntake | None = None
    intent: api_store.IdempotencyRecord | None = None
    file_count: int = 0


def latest_card_history(
    session: Session,
    *,
    card_identity: str,
    requester: str,
    exclude_intent_id: int | None = None,
) -> ReceiveHistoryMatch | None:
    """Return the newest non-revoked attempt matching only the card identity."""

    attempts: dict[str, _Attempt] = {}
    catalog_rows = list(session.scalars(select(Intake).where(Intake.card_id == card_identity)))
    counts = _catalog_file_counts(session, [row.intake_id for row in catalog_rows])
    for row in catalog_rows:
        attempt = _attempt(
            attempts,
            intake_id=row.intake_id,
            operator=row.operator,
            received_at=row.created_at,
        )
        attempt.catalog = row
        attempt.card_label = row.label
        attempt.device_id = row.device_id
        attempt.file_count = counts.get(row.intake_id, 0)

    grpc_rows = list(
        session.scalars(
            select(grpc_store.GrpcIntake).where(grpc_store.GrpcIntake.card_id == card_identity)
        )
    )
    for row in grpc_rows:
        attempt = _attempt(
            attempts,
            intake_id=row.intake_id,
            operator=row.operator,
            received_at=row.created_at,
        )
        attempt.grpc = row
        attempt.card_label = attempt.card_label or row.label
        attempt.device_id = attempt.device_id or row.device_id
        if attempt.catalog is None:
            receipt = intake_receipt_summary(row)
            attempt.file_count = 0 if receipt is None else len(receipt.relpaths)

    intent_query = select(api_store.IdempotencyRecord).where(
        api_store.IdempotencyRecord.endpoint == api_store.DEVICE_RECEIVE_ENDPOINT,
        api_store.IdempotencyRecord.card_identity == card_identity,
        api_store.IdempotencyRecord.intake_id.is_not(None),
        api_store.IdempotencyRecord.status.in_(
            ("started", "committed", "aborted", "quarantined", "failed")
        ),
    )
    if exclude_intent_id is not None:
        intent_query = intent_query.where(api_store.IdempotencyRecord.id != exclude_intent_id)
    for row in session.scalars(intent_query):
        assert row.intake_id is not None
        attempt = _attempt(
            attempts,
            intake_id=row.intake_id,
            operator=row.operator_username,
            received_at=row.started_at or row.created_at,
        )
        attempt.intent = row
        attempt.card_label = attempt.card_label or row.card_label
        attempt.device_id = attempt.device_id or row.device_id

    projected = [
        ReceiveHistoryMatch(
            intake_id=attempt.intake_id,
            card_label=attempt.card_label,
            device_id=attempt.device_id,
            received_at=attempt.received_at,
            state=_attempt_state(attempt),
            file_count=attempt.file_count,
            visible=attempt.operator == requester,
        )
        for attempt in attempts.values()
    ]
    non_revoked = [match for match in projected if match.state != "revoked"]
    if not non_revoked:
        return None
    return max(non_revoked, key=lambda match: (_aware(match.received_at), match.intake_id))


def _attempt(
    attempts: dict[str, _Attempt],
    *,
    intake_id: str,
    operator: str,
    received_at: dt.datetime,
) -> _Attempt:
    existing = attempts.get(intake_id)
    if existing is None:
        existing = _Attempt(
            intake_id=intake_id,
            operator=operator,
            received_at=received_at,
        )
        attempts[intake_id] = existing
    elif _aware(received_at) < _aware(existing.received_at):
        existing.received_at = received_at
    return existing


def _attempt_state(attempt: _Attempt) -> HistoryState:
    intent_state = attempt.intent.status if attempt.intent is not None else None
    if intent_state in {"aborted", "failed"}:
        return "failed"
    if intent_state == "quarantined":
        return "quarantined"
    if attempt.catalog is not None:
        if attempt.catalog.status == IntakeStatus.REGISTERED:
            return "verified"
        if attempt.catalog.status == IntakeStatus.QUARANTINED:
            return "quarantined"
    if attempt.grpc is not None and attempt.grpc.state == "aborted":
        return "failed"
    return "verifying"


def _catalog_file_counts(session: Session, intake_ids: list[str]) -> dict[str, int]:
    if not intake_ids:
        return {}
    return {
        str(intake_id): int(file_count)
        for intake_id, file_count in session.execute(
            select(IngestItem.intake_id, func.count(IngestItem.id))
            .where(IngestItem.intake_id.in_(intake_ids))
            .group_by(IngestItem.intake_id)
        )
    }


def _aware(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)
