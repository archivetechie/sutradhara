"""Retention, offsite-confirmation, and correction CLI commands."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import os
from contextlib import suppress
from typing import Any

import click
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import Copy, Intake
from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.catalog.types import RetentionState
from sutradhara.replication import _copy_media_id
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
from sutradhara.retention_journal import (
    RUNBOOK_TEXT,
    JournalError,
    check_journal,
    configured_dr_destination,
    export_journal,
    project_journal_alarm,
    record_journal_correction,
    refresh_staleness_alarm,
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
        operator = _actor(actor)
        engine = make_engine()
        with session_scope(engine) as session:
            resolved = _resolve_media_id(session, tape=tape, media_id=media_id)
            row, created = confirm_offsite(
                session,
                media_id=resolved,
                confirmed_by=operator,
                shipment_id=shipment_id,
            )
        _export_after_mutation(engine)
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
        operator = _actor(actor)
        engine = make_engine()
        with session_scope(engine) as session:
            resolved = _resolve_media_id(session, tape=tape, media_id=media_id)
            row, changed = revoke_offsite(
                session,
                media_id=resolved,
                actor=operator,
                reason=reason,
            )
        _export_after_mutation(engine)
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
        _export_after_mutation(engine)
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
        _export_after_mutation(engine)
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
        rows = [row for row in rows if row.holds or row.purge_status.status.startswith("blocked:")]
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
        if row.purge_status.status.startswith("blocked:"):
            click.echo(f"  hold: purge:{row.purge_status.status.removeprefix('blocked:')}")


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
        _export_after_mutation(engine)
    except (RetentionError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    payload = {"intake_id": intake_id, "abandoned": changed, "reason": reason}
    _emit_one(
        payload,
        as_json=as_json,
        human=f"{intake_id}: {'abandoned' if changed else 'already-abandoned'}",
    )


@retention_group.group("journal")
def retention_journal_group() -> None:
    """Export, verify, and correct the emit-only receipt journal."""


@retention_journal_group.command("export")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def retention_journal_export_cmd(as_json: bool) -> None:
    """Run the singleton exporter and append-only DR shipment."""

    engine = make_engine()
    try:
        destination = configured_dr_destination(engine)
        result = export_journal(engine, destination=destination)
    except Exception as exc:
        _record_export_failure(engine, exc)
        raise click.ClickException(str(exc)) from exc
    payload = dataclasses.asdict(result)
    human = (
        f"journal export: entries={result.entry_count} published={result.published} "
        f"sequence={result.state.global_sequence} shipped={result.shipped_segments}"
    )
    if result.shipping_error:
        human += f" ALARM shipping-failed={result.shipping_error}"
    _emit_one(payload, as_json=as_json, human=human)
    if result.shipping_error:
        raise click.exceptions.Exit(1)


@retention_journal_group.command("check")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def retention_journal_check_cmd(as_json: bool) -> None:
    """Walk the journal chain and compare its off-box head and DB projection."""

    engine = make_engine()
    destination = None
    destination_error: str | None = None
    try:
        destination = configured_dr_destination(engine)
    except JournalError as exc:
        destination_error = str(exc)
    try:
        result = check_journal(engine, destination=destination)
    except Exception as exc:
        result_error = str(exc)
        with suppress(Exception):
            project_journal_alarm(
                engine,
                target_key="check-failed",
                active=True,
                reason="check-failed",
                message=result_error,
            )
        if as_json:
            click.echo(json.dumps({"ok": False, "issues": [result_error], "runbook": RUNBOOK_TEXT}))
        else:
            click.echo(f"journal check: FAIL\n  {result_error}\n{RUNBOOK_TEXT}")
        raise click.exceptions.Exit(1) from exc
    issues = list(result.issues)
    if destination_error:
        issues.append(f"offbox-head-read-failed: {destination_error}")
    ok = result.ok and not destination_error
    try:
        project_journal_alarm(
            engine,
            target_key="check-failed",
            active=not ok,
            reason="check-failed",
            message="; ".join([*issues, *result.projection_mismatches]) or "journal check passed",
        )
    except Exception as exc:
        issues.append(f"alarm-write-failed: {exc}")
        ok = False
    if as_json:
        payload = dataclasses.asdict(result)
        payload["ok"] = ok
        payload["issues"] = issues
        payload["runbook"] = None if ok else RUNBOOK_TEXT
        click.echo(json.dumps(payload, indent=2, default=str))
    else:
        click.echo(
            f"journal check: {'PASS' if ok else 'FAIL'} files={result.file_count} "
            f"entries={result.entry_count} sequence={result.state.global_sequence}"
        )
        for issue in issues:
            click.echo(f"  break: {issue}")
        for mismatch in result.projection_mismatches:
            click.echo(f"  break: {mismatch}")
        if not ok:
            click.echo(RUNBOOK_TEXT)
    if not ok:
        raise click.exceptions.Exit(1)


@retention_journal_group.command("correct")
@click.option(
    "--source",
    type=click.Choice(["verify_receipt", "retention_event"]),
    required=True,
)
@click.option("--event-id", type=int, required=True, help="Receipt event_id to supersede.")
@click.option("--reason", required=True, help="Why the immutable receipt is incorrect.")
@click.option("--actor", default=None, help="Operator recording the correction.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def retention_journal_correct_cmd(
    source: str,
    event_id: int,
    reason: str,
    actor: str | None,
    as_json: bool,
) -> None:
    """Append a superseding correction; never rewrite a published receipt."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            row = record_journal_correction(
                session,
                source=source,
                event_id=event_id,
                actor=_actor(actor),
                reason=reason,
            )
        _export_after_mutation(engine)
    except (JournalError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    _emit_one(
        {
            "event_id": row.event_id,
            "supersedes_source": source,
            "supersedes_event_id": event_id,
            "reason": reason,
        },
        as_json=as_json,
        human=f"correction event {row.event_id} supersedes {source}:{event_id}",
    )


@retention_group.command("sitrep")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def retention_sitrep_cmd(as_json: bool) -> None:
    """Print standing purge holds and receipt-export staleness for ops sitrep."""

    engine = make_engine()
    now = dt.datetime.now(dt.UTC)
    standing: dict[str, list[str]] = {}
    with session_scope(engine) as session:
        for intake in _status_intakes(session, None):
            if intake.retention_state != RetentionState.RELEASED or intake.released_at is None:
                continue
            released_at = intake.released_at
            if released_at.tzinfo is None:
                released_at = released_at.replace(tzinfo=dt.UTC)
            if released_at + dt.timedelta(days=DEFAULT_STAGING_GRACE_DAYS) >= now:
                continue
            status = retention_status(session, intake)
            if status.purge_status.status.startswith("blocked:"):
                standing.setdefault(intake.intake_id, []).append(status.purge_status.status)
    export_status = refresh_staleness_alarm(engine, now=now)
    payload = {
        "standing_holds": [
            {"intake_id": intake_id, "reasons": sorted(set(reasons))}
            for intake_id, reasons in sorted(standing.items())
        ],
        "export": dataclasses.asdict(export_status),
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
        return
    if standing:
        for intake_id, reasons in sorted(standing.items()):
            click.echo(f"retention hold: {intake_id}: {', '.join(sorted(set(reasons)))}")
    else:
        click.echo("retention holds: none")
    age = f"{export_status.stale_seconds}s" if export_status.oldest_pending_at else "current"
    click.echo(
        f"retention journal: pending={export_status.pending_entries} age={age} "
        f"status={'STALE' if export_status.stale else 'ok'}"
    )


def _status_intakes(session: Session, intake_id: str | None) -> list[Intake]:
    query = select(Intake).order_by(Intake.intake_id)
    if intake_id is not None:
        query = query.where(Intake.intake_id == intake_id)
    return list(session.scalars(query))


def _resolve_media_id(
    session: Session,
    *,
    tape: str | None,
    media_id: str | None,
) -> str:
    """Resolve an operator label to one known canonical copy-media identity."""

    if bool(tape) == bool(media_id):
        raise ValueError("provide exactly one of --tape or --media-id")
    if media_id:
        return media_id
    if tape is None:
        raise ValueError("provide exactly one of --tape or --media-id")
    matches: set[str] = set()
    for copy in session.scalars(select(Copy).where(Copy.deleted_at.is_(None))):
        canonical = _copy_media_id(copy)
        if canonical is None:
            continue
        locator_labels = {
            value
            for value in (copy.native_locator or {}).values()
            if isinstance(value, str) and value
        }
        if tape == canonical or tape in locator_labels:
            matches.add(canonical)
    if not matches:
        raise RetentionError(f"tape label {tape!r} matches no known canonical media identity")
    if len(matches) > 1:
        choices = ", ".join(sorted(matches))
        raise RetentionError(
            f"tape label {tape!r} is ambiguous; matching canonical media ids: {choices}"
        )
    return matches.pop()


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
    conditions = sorted({candidate.release_condition for candidate in result.candidates})
    for condition in conditions:
        click.echo(f"  releasing-condition {condition}:")
        for candidate in result.candidates:
            if candidate.release_condition != condition:
                continue
            recent = " recent-health-flip" if candidate.recent_flip else ""
            click.echo(f"    candidate {candidate.intake_id}:{recent}")
            for evidence in candidate.evidence:
                click.echo(f"      evidence: {evidence}")
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


def _export_after_mutation(engine: Engine) -> None:
    """Best-effort post-commit hook: alarm on failure, never change gate outcome."""

    try:
        destination = configured_dr_destination(engine)
        result = export_journal(engine, destination=destination)
        if result.shipping_error:
            click.echo(
                f"warning: retention journal shipping alarm: {result.shipping_error}",
                err=True,
            )
    except Exception as exc:
        _record_export_failure(engine, exc)
        click.echo(f"warning: retention journal export alarm: {exc}", err=True)


def _record_export_failure(engine: Engine, exc: Exception) -> None:
    try:
        project_journal_alarm(
            engine,
            target_key="export-failed",
            active=True,
            reason="export-failed",
            message=str(exc),
        )
    except Exception:
        # The catalog itself may be the failing component.  The CLI warning is
        # still emitted by the caller; journal trouble never rewrites a gate result.
        return
