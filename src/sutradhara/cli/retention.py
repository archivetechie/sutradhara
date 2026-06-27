"""Retention and offsite-confirmation CLI commands."""

from __future__ import annotations

import dataclasses
import json
import os
from collections.abc import Sequence
from typing import Any

import click
from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import Intake
from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.catalog.types import RetentionState
from sutradhara.retention import (
    DEFAULT_STAGING_GRACE_DAYS,
    RetentionError,
    confirm_offsite,
    retention_status,
    run_retention,
    sweep_staging,
)


@click.group("offsite")
def offsite_group() -> None:
    """Record offsite media confirmations."""


@offsite_group.command("confirm")
@click.option("--tape", default=None, help="Tape UUID/barcode to confirm.")
@click.option("--media-id", default=None, help="Exact media id recorded on Copy locators.")
@click.option("--shipment", "shipment_id", default=None, help="Optional shipment identifier.")
@click.option("--confirmed-by", default=None, help="Operator recording the confirmation.")
def offsite_confirm(
    tape: str | None,
    media_id: str | None,
    shipment_id: str | None,
    confirmed_by: str | None,
) -> None:
    """Confirm one media id as offsite."""
    try:
        resolved = _resolve_media_id(tape=tape, media_id=media_id)
        actor = confirmed_by or os.environ.get("USER") or "operator"
        engine = make_engine()
        with session_scope(engine) as session:
            row, created = confirm_offsite(
                session,
                media_id=resolved,
                confirmed_by=actor,
                shipment_id=shipment_id,
            )
    except (RetentionError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    action = "confirmed" if created else "already confirmed"
    click.echo(f"{action} {row.media_id}")


@click.group("retention")
def retention_group() -> None:
    """Run the retention release gate and staging sweep."""


@retention_group.command("run")
@click.option("--intake", "intake_id", default=None, help="Restrict to one intake id.")
@click.option("--actor", default=None, help="Operator running the gate.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def retention_run_cmd(intake_id: str | None, actor: str | None, as_json: bool) -> None:
    """Release held intakes whose durable copies pass the gate."""
    operator = actor or os.environ.get("USER") or "operator"
    try:
        engine = make_engine()
        with session_scope(engine) as session:
            results = [
                run_retention(session, intake, actor=operator)
                for intake in _retention_intakes(session, intake_id, RetentionState.HELD)
            ]
    except RetentionError as exc:
        raise click.ClickException(str(exc)) from exc
    _emit_results(results, as_json=as_json)


@retention_group.command("sweep-staging")
@click.option("--intake", "intake_id", default=None, help="Restrict to one intake id.")
@click.option("--actor", default=None, help="Operator running the sweep.")
@click.option(
    "--grace-days",
    type=int,
    default=DEFAULT_STAGING_GRACE_DAYS,
    show_default=True,
    help="Days after release before landing bytes may be deleted.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def retention_sweep_cmd(
    intake_id: str | None,
    actor: str | None,
    grace_days: int,
    as_json: bool,
) -> None:
    """Delete released intake landing bytes after the grace period."""
    operator = actor or os.environ.get("USER") or "operator"
    try:
        engine = make_engine()
        with session_scope(engine) as session:
            results = [
                sweep_staging(session, intake, actor=operator, grace_days=grace_days)
                for intake in _retention_intakes(session, intake_id, RetentionState.RELEASED)
            ]
    except RetentionError as exc:
        raise click.ClickException(str(exc)) from exc
    _emit_results(results, as_json=as_json)


@retention_group.command("status")
@click.option("--intake", "intake_id", default=None, help="Restrict to one intake id.")
@click.option(
    "--grace-days",
    type=int,
    default=DEFAULT_STAGING_GRACE_DAYS,
    show_default=True,
    help="Days after release before landing bytes may be deleted.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def retention_status_cmd(intake_id: str | None, grace_days: int, as_json: bool) -> None:
    """Show the retention gate truth for intakes."""
    try:
        engine = make_engine()
        with session_scope(engine) as session:
            rows = [
                retention_status(session, intake, grace_days=grace_days)
                for intake in _status_intakes(session, intake_id)
            ]
    except RetentionError as exc:
        raise click.ClickException(str(exc)) from exc
    payload = [dataclasses.asdict(row) for row in rows]
    if as_json:
        click.echo(json.dumps(payload[0] if len(payload) == 1 else payload, indent=2, default=str))
        return
    for row in rows:
        holds = f" holds={len(row.holds)}" if row.holds else ""
        click.echo(
            f"{row.intake_id}: state={row.retention_state} releasable={row.releasable}{holds}"
        )


def _retention_intakes(
    session: Session,
    intake_id: str | None,
    state: RetentionState,
) -> list[Intake]:
    query = select(Intake).order_by(Intake.intake_id)
    if intake_id is not None:
        query = query.where(Intake.intake_id == intake_id)
    else:
        query = query.where(Intake.retention_state == state)
    return list(session.scalars(query))


def _status_intakes(session: Session, intake_id: str | None) -> list[Intake]:
    query = select(Intake).order_by(Intake.intake_id)
    if intake_id is not None:
        query = query.where(Intake.intake_id == intake_id)
    return list(session.scalars(query))


def _resolve_media_id(*, tape: str | None, media_id: str | None) -> str:
    if bool(tape) == bool(media_id):
        raise ValueError("provide exactly one of --tape or --media-id")
    if media_id:
        return media_id
    assert tape is not None
    return tape if ":" in tape else f"tape:{tape}"


def _emit_results(rows: Sequence[Any], *, as_json: bool) -> None:
    payload = [dataclasses.asdict(row) for row in rows]
    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
        return
    for row in payload:
        action = "changed" if row.get("released") or row.get("purged") else "no-op"
        click.echo(f"{row['intake_id']}: {action} reason={row['reason']}")
