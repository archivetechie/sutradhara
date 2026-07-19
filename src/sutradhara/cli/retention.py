"""Retention, offsite-confirmation, and correction CLI commands."""

from __future__ import annotations

import dataclasses
import json
import os
from typing import Any

import click
from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import Intake
from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.retention import (
    DEFAULT_STAGING_GRACE_DAYS,
    RetentionBatchResult,
    RetentionError,
    abandon_retention,
    confirm_offsite,
    retention_status,
    revoke_offsite,
    run_retention_batch,
    sweep_staging_batch,
)


@click.group("offsite")
def offsite_group() -> None:
    """Record and correct offsite media confirmations."""


@offsite_group.command("confirm")
@click.option("--tape", default=None, help="Tape UUID/barcode to confirm.")
@click.option("--media-id", default=None, help="Exact media id recorded on Copy locators.")
@click.option("--shipment", "shipment_id", default=None, help="Optional shipment identifier.")
@click.option("--actor", "actor", default=None, help="Operator recording the confirmation.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def offsite_confirm(
    tape: str | None,
    media_id: str | None,
    shipment_id: str | None,
    actor: str | None,
    as_json: bool,
) -> None:
    """Confirm one known media id as offsite."""
    try:
        resolved = _resolve_media_id(tape=tape, media_id=media_id)
        operator = _actor(actor)
        engine = make_engine()
        with session_scope(engine) as session:
            row, created = confirm_offsite(
                session,
                media_id=resolved,
                confirmed_by=operator,
                shipment_id=shipment_id,
            )
    except (RetentionError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    payload = {"media_id": row.media_id, "confirmed": created, "actor": operator}
    _emit_one(
        payload,
        as_json=as_json,
        human=f"{resolved}: {'confirmed' if created else 'already-confirmed'}",
    )


@offsite_group.command("revoke")
@click.option("--tape", default=None, help="Tape UUID/barcode to revoke.")
@click.option("--media-id", default=None, help="Exact media id to revoke.")
@click.option("--reason", required=True, help="Reason for the correction.")
@click.option("--actor", default=None, help="Operator recording the correction.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def offsite_revoke(
    tape: str | None,
    media_id: str | None,
    reason: str,
    actor: str | None,
    as_json: bool,
) -> None:
    """Revoke an incorrect offsite confirmation without deleting its row."""
    try:
        resolved = _resolve_media_id(tape=tape, media_id=media_id)
        operator = _actor(actor)
        engine = make_engine()
        with session_scope(engine) as session:
            row, changed = revoke_offsite(
                session,
                media_id=resolved,
                actor=operator,
                reason=reason,
            )
    except (RetentionError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    payload = {
        "media_id": row.media_id,
        "revoked": changed,
        "revoked_at": row.revoked_at,
        "revoked_by": row.revoked_by,
        "reason": reason,
    }
    _emit_one(
        payload, as_json=as_json, human=f"{resolved}: {'revoked' if changed else 'already-revoked'}"
    )


@click.group("retention")
def retention_group() -> None:
    """Run the retention release gate, staging sweep, and corrections."""


@retention_group.command("run")
@click.option("--intake", "intake_id", default=None, help="Restrict to one intake id.")
@click.option("--actor", default=None, help="Operator running the gate.")
@click.option(
    "--batch-limit", type=int, default=None, help="Deliberately override the 25-intake brake."
)
@click.option("--dry-run", is_flag=True, default=False, help="List evidence without acting.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def retention_run_cmd(
    intake_id: str | None,
    actor: str | None,
    batch_limit: int | None,
    dry_run: bool,
    as_json: bool,
) -> None:
    """Release held intakes whose durable copies pass the gate."""
    try:
        engine = make_engine()
        with session_scope(engine) as session:
            result = run_retention_batch(
                session,
                actor=_actor(actor),
                intake_id=intake_id,
                batch_limit=batch_limit,
                dry_run=dry_run,
            )
    except (RetentionError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    _emit_batch(result, as_json=as_json)
    if result.exit_code:
        raise click.exceptions.Exit(result.exit_code)


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
@click.option(
    "--break-glass", is_flag=True, default=False, help="Allow a sub-default grace period."
)
@click.option(
    "--batch-limit", type=int, default=None, help="Deliberately override the 25-intake brake."
)
@click.option("--dry-run", is_flag=True, default=False, help="List evidence without acting.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def retention_sweep_cmd(
    intake_id: str | None,
    actor: str | None,
    grace_days: int,
    break_glass: bool,
    batch_limit: int | None,
    dry_run: bool,
    as_json: bool,
) -> None:
    """Re-gate and purge released intake landing bytes."""
    try:
        engine = make_engine()
        with session_scope(engine) as session:
            result = sweep_staging_batch(
                session,
                actor=_actor(actor),
                intake_id=intake_id,
                batch_limit=batch_limit,
                dry_run=dry_run,
                grace_days=grace_days,
                break_glass=break_glass,
            )
    except (RetentionError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    _emit_batch(result, as_json=as_json)
    if result.exit_code:
        raise click.exceptions.Exit(result.exit_code)


@retention_group.command("status")
@click.option("--intake", "intake_id", default=None, help="Restrict to one intake id.")
@click.option("--held", is_flag=True, default=False, help="Show only intakes with gate holds.")
@click.option(
    "--grace-days",
    type=int,
    default=DEFAULT_STAGING_GRACE_DAYS,
    show_default=True,
    help="Days after release before landing bytes may be deleted.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def retention_status_cmd(
    intake_id: str | None,
    held: bool,
    grace_days: int,
    as_json: bool,
) -> None:
    """Show per-pool retention evidence and purge disposition."""
    try:
        engine = make_engine()
        with session_scope(engine) as session:
            rows = [
                retention_status(session, intake, grace_days=grace_days)
                for intake in _status_intakes(session, intake_id)
            ]
    except RetentionError as exc:
        raise click.ClickException(str(exc)) from exc
    if held:
        rows = [row for row in rows if row.holds]
    if as_json:
        payload = [dataclasses.asdict(row) for row in rows]
        click.echo(json.dumps(payload[0] if len(payload) == 1 else payload, indent=2, default=str))
        return
    for row in rows:
        click.echo(
            f"{row.intake_id}: state={row.retention_state} "
            f"release={'eligible' if row.releasable else 'held'} "
            f"purge={row.purge_status.status}"
        )
        for reason in row.holds:
            click.echo(f"  hold: {reason}")


@retention_group.command("abandon")
@click.option("--intake", "intake_id", required=True, help="Intake id to abandon.")
@click.option("--reason", required=True, help="Why retention must never purge this intake.")
@click.option("--actor", default=None, help="Operator recording the correction.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def retention_abandon_cmd(
    intake_id: str,
    reason: str,
    actor: str | None,
    as_json: bool,
) -> None:
    """Terminally abandon retention while preserving staging bytes."""
    try:
        engine = make_engine()
        with session_scope(engine) as session:
            changed = abandon_retention(
                session,
                intake_id,
                actor=_actor(actor),
                reason=reason,
            )
    except (RetentionError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    payload = {"intake_id": intake_id, "abandoned": changed, "reason": reason}
    _emit_one(
        payload,
        as_json=as_json,
        human=f"{intake_id}: {'abandoned' if changed else 'already-abandoned'}",
    )


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


def _actor(value: str | None) -> str:
    return value or os.environ.get("USER") or "operator"


def _emit_batch(result: RetentionBatchResult, *, as_json: bool) -> None:
    if as_json:
        click.echo(json.dumps(dataclasses.asdict(result), indent=2, default=str))
        return
    click.echo(
        f"batch {result.action}: candidates={result.candidate_count} "
        f"limit={result.limit} reason={result.reason}"
    )
    for candidate in result.candidates:
        recent = " recent-health-flip" if candidate.recent_flip else ""
        click.echo(f"  candidate {candidate.intake_id}:{recent}")
        for evidence in candidate.evidence:
            click.echo(f"    evidence: {evidence}")
    for held in result.holds:
        click.echo(f"  held {held.intake_id}:")
        for reason in held.reasons:
            click.echo(f"    hold: {reason}")
    for row in result.results:
        action = (
            "changed"
            if getattr(row, "released", False) or getattr(row, "purged", False)
            else "no-op"
        )
        click.echo(f"  {row.intake_id}: {action} reason={row.reason}")


def _emit_one(payload: dict[str, Any], *, as_json: bool, human: str) -> None:
    click.echo(json.dumps(payload, indent=2, default=str) if as_json else human)
