"""Reopen abandoned agent restores after their delivery lease expires.

The delivery attempt may finish at ``sent`` while the restore item remains
active. This reconciler marks an expired, unrevealed attempt reopenable without
completing the item or dispatching the server-local restore handler.
"""

from __future__ import annotations

import datetime as dt

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload

from sutradhara.hdcache.manager import ITEM_SENT, REQUEST_ACTIVE
from sutradhara.hdcache.models import RestoreOpenSession, RestoreRequestItem
from sutradhara.jobs.reconcilers.conditions import OBSERVED_MISSING, OBSERVED_PRESENT
from sutradhara.jobs.reconcilers.registry import Reconciler, TargetObservation, register_reconciler

DOMAIN = "restore_open"
REOPENABLE_DETAIL = "delivery lease expired; restore is reopenable"


def enumerate_targets(
    session: Session,
    cursor: int | None,
    batch: int,
) -> list[TargetObservation]:
    """Enumerate bounded expired ``sent`` attempts that still await reveal."""

    now = dt.datetime.now(dt.UTC)
    query = (
        select(RestoreRequestItem)
        .join(RestoreRequestItem.open_session)
        .options(selectinload(RestoreRequestItem.checkpoint))
        .where(
            RestoreRequestItem.state == ITEM_SENT,
            RestoreOpenSession.expires_at <= now,
            or_(
                RestoreRequestItem.checkpoint == None,  # noqa: E711
                RestoreRequestItem.checkpoint.has(revealed=False),
            ),
        )
        .order_by(RestoreRequestItem.id)
        .limit(batch)
    )
    if cursor is not None:
        query = query.where(RestoreRequestItem.id > cursor)
    return [_observation(item) for item in session.scalars(query)]


def observe(session: Session, target_key: str) -> TargetObservation:
    """Re-observe one item before the spine acts on its condition."""

    item = session.scalar(
        select(RestoreRequestItem)
        .options(
            selectinload(RestoreRequestItem.checkpoint),
            selectinload(RestoreRequestItem.open_session),
        )
        .where(RestoreRequestItem.id == int(target_key))
    )
    if item is None or not _awaits_expired_reopen(item):
        return TargetObservation(
            target_key=target_key, desired=False, observed_state=OBSERVED_PRESENT
        )
    return _observation(item)


def reconcile_target(session: Session, target_key: str) -> None:
    """Make the expired delivery generation explicitly reopenable in place."""

    item = session.scalar(
        select(RestoreRequestItem)
        .options(
            selectinload(RestoreRequestItem.checkpoint),
            selectinload(RestoreRequestItem.open_session),
            selectinload(RestoreRequestItem.request),
        )
        .where(RestoreRequestItem.id == int(target_key))
    )
    if item is None or not _awaits_expired_reopen(item):
        return
    item.detail = REOPENABLE_DETAIL
    item.updated_at = dt.datetime.now(dt.UTC)
    if item.request is not None:
        item.request.state = REQUEST_ACTIVE


def _observation(item: RestoreRequestItem) -> TargetObservation:
    return TargetObservation(
        target_key=str(item.id),
        desired=True,
        observed_state=(OBSERVED_PRESENT if item.detail == REOPENABLE_DETAIL else OBSERVED_MISSING),
    )


def _awaits_expired_reopen(item: RestoreRequestItem) -> bool:
    session = item.open_session
    checkpoint = item.checkpoint
    return (
        item.state == ITEM_SENT
        and session is not None
        and _as_utc(session.expires_at) <= dt.datetime.now(dt.UTC)
        and (checkpoint is None or not checkpoint.revealed)
    )


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


register_reconciler(DOMAIN)(
    Reconciler(
        enumerate_targets=enumerate_targets,
        observe=observe,
        reconcile_target=reconcile_target,
    )
)
