"""`sutra arrangement` commands for P2.3a arrange and submit."""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any

import click

from sutradhara.arrangement import (
    ArrangementError,
    abandon_arrangement,
    create_from_arrangement,
    create_from_intake,
    exclude_member,
    list_arrangements,
    move_member,
    show_arrangement,
    submit_arrangement,
)
from sutradhara.catalog.models import Arrangement, ArrangementMember
from sutradhara.catalog.session import make_engine, session_scope


@click.group("arrangement")
def arrangement_group() -> None:
    """Create, edit, inspect, and submit arrangement workspaces."""


@arrangement_group.command("create")
@click.option("--from-intake", "from_intake", default=None, help="Registered intake id to arrange.")
@click.option(
    "--from-arrangement",
    "from_arrangement",
    type=int,
    default=None,
    help="Existing arrangement id to clone.",
)
@click.option("--label", required=True, help="Human label for the new arrangement.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def create_cmd(
    from_intake: str | None,
    from_arrangement: int | None,
    label: str,
    as_json: bool,
) -> None:
    """Create a draft arrangement from a registered intake or existing arrangement."""

    if (from_intake is None) == (from_arrangement is None):
        raise click.ClickException("provide exactly one of --from-intake or --from-arrangement")
    engine = make_engine()
    try:
        with session_scope(engine) as session:
            if from_intake is not None:
                arrangement = create_from_intake(session, from_intake, label=label)
            else:
                assert from_arrangement is not None
                arrangement = create_from_arrangement(session, from_arrangement, label=label)
            payload = _arrangement_payload(arrangement)
    except ArrangementError as exc:
        raise click.ClickException(str(exc)) from exc
    _emit(payload, as_json=as_json)


@arrangement_group.command("mv")
@click.argument("arrangement_id", type=int)
@click.argument("from_path")
@click.argument("to_path")
def mv_cmd(arrangement_id: int, from_path: str, to_path: str) -> None:
    """Move one active arrangement member to a new archive path."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            member = move_member(session, arrangement_id, from_path, to_path)
            message = f"moved arrangement {arrangement_id}: {from_path!r} -> {member.member_path!r}"
    except ArrangementError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(message)


@arrangement_group.command("exclude")
@click.argument("arrangement_id", type=int)
@click.argument("member_path")
def exclude_cmd(arrangement_id: int, member_path: str) -> None:
    """Exclude one active member from submit output."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            member = exclude_member(session, arrangement_id, member_path)
            message = f"excluded arrangement {arrangement_id}: {member.member_path!r}"
    except ArrangementError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(message)


@arrangement_group.command("abandon")
@click.argument("arrangement_id", type=int)
@click.option("--actor", default=None, help="Operator recorded on the abandonment.")
@click.option("--reason", default=None, help="Reason recorded on the abandonment.")
def abandon_cmd(arrangement_id: int, actor: str | None, reason: str | None) -> None:
    """Abandon a draft arrangement."""

    operator = actor or os.environ.get("USER") or "unknown"
    engine = make_engine()
    try:
        with session_scope(engine) as session:
            arrangement = abandon_arrangement(
                session, arrangement_id, actor=operator, reason=reason
            )
            message = f"abandoned arrangement {arrangement.id}"
    except ArrangementError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(message)


@arrangement_group.command("submit")
@click.argument("arrangement_id", type=int)
@click.option(
    "--submission-root",
    type=click.Path(path_type=Path, file_okay=False),
    default=Path("/replica/submissions"),
    show_default=True,
    help="Directory where submission id directories are written.",
)
@click.option("--submitted-by", default=None, help="Operator recorded on the submission.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def submit_cmd(
    arrangement_id: int,
    submission_root: Path,
    submitted_by: str | None,
    as_json: bool,
) -> None:
    """Freeze an arrangement into a source-map submission."""

    operator = submitted_by or os.environ.get("USER") or "unknown"
    engine = make_engine()
    try:
        with session_scope(engine) as session:
            result = submit_arrangement(
                session,
                arrangement_id,
                submitted_by=operator,
                submission_root=submission_root,
            )
            payload = {
                "submission_id": result.submission_id,
                "arrangement_id": result.arrangement_id,
                "source_map_path": str(result.source_map_path),
                "manifest_digest": result.manifest_digest,
                "member_count": result.member_count,
            }
    except ArrangementError as exc:
        raise click.ClickException(str(exc)) from exc
    _emit(payload, as_json=as_json)


@arrangement_group.command("list")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def list_cmd(as_json: bool) -> None:
    """List arrangement workspaces."""

    engine = make_engine()
    with session_scope(engine) as session:
        rows = [asdict(row) for row in list_arrangements(session)]
    if as_json:
        click.echo(json.dumps(rows, indent=2, sort_keys=True, default=str))
        return
    for row in rows:
        parent = (
            f" cloned_from={row['cloned_from_arrangement_id']}"
            if row["cloned_from_arrangement_id"] is not None
            else ""
        )
        click.echo(
            f"{row['id']}: {row['status']} intake={row['intake_id']} "
            f"members={row['active_member_count']}/{row['member_count']}{parent} "
            f"label={row['label']!r}"
        )


@arrangement_group.command("show")
@click.argument("arrangement_id", type=int)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def show_cmd(arrangement_id: int, as_json: bool) -> None:
    """Show one arrangement and its members."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            arrangement = show_arrangement(session, arrangement_id)
            payload = _arrangement_payload(arrangement, include_members=True)
    except ArrangementError as exc:
        raise click.ClickException(str(exc)) from exc
    _emit(payload, as_json=as_json)


def _arrangement_payload(
    arrangement: Arrangement,
    *,
    include_members: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": arrangement.id,
        "label": arrangement.label,
        "intake_id": arrangement.intake_id,
        "artifactclass": arrangement.artifactclass,
        "status": str(arrangement.status),
        "submission_id": arrangement.submission_id,
        "cloned_from_arrangement_id": arrangement.cloned_from_arrangement_id,
        "member_count": len(arrangement.members),
        "active_member_count": sum(1 for member in arrangement.members if not member.excluded),
    }
    if include_members:
        payload["members"] = [_member_payload(member) for member in arrangement.members]
    return payload


def _member_payload(member: ArrangementMember) -> dict[str, Any]:
    return {
        "id": member.id,
        "ingest_item_id": member.ingest_item_id,
        "member_path": member.member_path,
        "excluded": member.excluded,
    }


def _emit(payload: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))
        return
    if "source_map_path" in payload:
        click.echo(
            f"submitted arrangement {payload['arrangement_id']}: "
            f"{payload['submission_id']} {payload['source_map_path']}"
        )
        return
    parent = (
        f" cloned_from={payload['cloned_from_arrangement_id']}"
        if payload["cloned_from_arrangement_id"] is not None
        else ""
    )
    click.echo(
        f"arrangement {payload['id']}: {payload['status']} "
        f"members={payload['active_member_count']}/{payload['member_count']}{parent} "
        f"label={payload['label']!r}"
    )
    if "members" in payload:
        for member in payload["members"]:
            flag = " excluded" if member["excluded"] else ""
            click.echo(f"  {member['member_path']} item={member['ingest_item_id']}{flag}")
