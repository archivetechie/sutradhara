"""`sutra virtual`, `sutra tag`, and reject commands for virtual arrangements."""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from typing import Any

import click
from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.archive_restore import (
    ArchiveRestoreError,
    RemArchiveExtractor,
)
from sutradhara.backend.factory import backend_from_row
from sutradhara.backend.port import StorageBackend
from sutradhara.catalog.models import ArtifactClassPool, Backend, Pool, VirtualArrangement
from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.hdcache.manager import (
    PrivacyOverride,
    RestoreDenied,
    RestoreManagerError,
    restore_to_path,
)
from sutradhara.virtual_arrangement import (
    VirtualArrangementError,
    add_member,
    add_tag,
    create_view,
    exclude_member,
    include_member,
    list_view,
    move_member,
    reject_asset,
    remove_tag,
    resolve,
    show_view,
    unreject_asset,
)


@click.group("virtual")
def virtual_group() -> None:
    """Create, edit, inspect, and restore virtual arrangements."""


@virtual_group.command("create")
@click.argument("name")
@click.option("--description", default=None, help="Optional view description.")
@click.option("--created-by", default=None, help="Operator recorded on the view.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def create_cmd(name: str, description: str | None, created_by: str | None, as_json: bool) -> None:
    """Create a virtual arrangement view."""

    actor = created_by or _operator()
    engine = make_engine()
    try:
        with session_scope(engine) as session:
            view = create_view(session, name, description=description, created_by=actor)
            payload = _view_payload(view)
    except VirtualArrangementError as exc:
        raise click.ClickException(str(exc)) from exc
    _emit(payload, as_json=as_json, kind="view")


@virtual_group.command("add")
@click.argument("name")
@click.argument("asset_hash_hex")
@click.argument("path")
@click.option(
    "--artifactclass",
    default=None,
    help="Required when the hash is archived under several classes.",
)
@click.option("--added-by", default=None, help="Operator recorded on the member.")
def add_cmd(
    name: str,
    asset_hash_hex: str,
    path: str,
    artifactclass: str | None,
    added_by: str | None,
) -> None:
    """Place one archived asset in a virtual arrangement."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            member = add_member(
                session,
                name,
                _asset_hash(asset_hash_hex),
                path,
                artifactclass=artifactclass,
                added_by=added_by or _operator(),
            )
            message = (
                f"added {member.logical_asset_hash.hex()} "
                f"class={member.artifactclass} path={member.path!r} to {name!r}"
            )
    except (ValueError, VirtualArrangementError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(message)


@virtual_group.command("mv")
@click.argument("name")
@click.argument("from_path")
@click.argument("to_path")
@click.option("--actor", default=None, help="Operator recorded on the history row.")
def mv_cmd(name: str, from_path: str, to_path: str, actor: str | None) -> None:
    """Move one active virtual arrangement member."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            member = move_member(session, name, from_path, to_path, actor=actor or _operator())
            message = f"moved {name!r}: {from_path!r} -> {member.path!r}"
    except VirtualArrangementError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(message)


@virtual_group.command("exclude")
@click.argument("name")
@click.argument("path")
def exclude_cmd(name: str, path: str) -> None:
    """Hide one active member from a virtual arrangement."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            member = exclude_member(session, name, path)
            message = f"excluded {name!r}: {member.path!r}"
    except VirtualArrangementError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(message)


@virtual_group.command("include")
@click.argument("name")
@click.argument("path")
def include_cmd(name: str, path: str) -> None:
    """Re-show one excluded member in a virtual arrangement."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            member = include_member(session, name, path)
            message = f"included {name!r}: {member.path!r}"
    except VirtualArrangementError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(message)


@virtual_group.command("ls")
@click.argument("name")
@click.option(
    "--all", "include_hidden", is_flag=True, default=False, help="Show excluded/rejected members."
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def ls_cmd(name: str, include_hidden: bool, as_json: bool) -> None:
    """List members in one virtual arrangement."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            rows = [asdict(row) for row in list_view(session, name, include_hidden=include_hidden)]
    except VirtualArrangementError as exc:
        raise click.ClickException(str(exc)) from exc
    if as_json:
        click.echo(json.dumps(rows, indent=2, sort_keys=True))
        return
    for row in rows:
        flags = []
        if row["excluded"]:
            flags.append("excluded")
        if row["rejected"]:
            flags.append("rejected")
        suffix = f" [{' '.join(flags)}]" if flags else ""
        click.echo(
            f"{row['path']} {row['logical_asset_hash']} class={row['artifactclass']}{suffix}"
        )


@virtual_group.command("show")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def show_cmd(name: str, as_json: bool) -> None:
    """Show one virtual arrangement and its members."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            view = show_view(session, name)
            payload = _view_payload(view, include_members=True)
    except VirtualArrangementError as exc:
        raise click.ClickException(str(exc)) from exc
    _emit(payload, as_json=as_json, kind="view")


@virtual_group.command("restore")
@click.argument("name")
@click.argument("path")
@click.option("--dest", "destination", required=True, type=click.Path(dir_okay=False))
@click.option("--rem-bin", default="rem", show_default=True, help="rem CLI binary.")
@click.option("--force", "force_suspect", is_flag=True, help="Restore suspect assets.")
@click.option("--force-rejected", is_flag=True, help="Restore rejected assets.")
@click.option(
    "--privacy-override",
    default=None,
    help="Trusted CLI reason for restoring private hdcache assets without API grants.",
)
def restore_cmd(
    name: str,
    path: str,
    destination: str,
    rem_bin: str,
    force_suspect: bool,
    force_rejected: bool,
    privacy_override: str | None,
) -> None:
    """Restore one member by virtual path."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            asset_hash, artifactclass = resolve(session, name, path)
            backends = _restore_backends(session, artifactclass)
            result = restore_to_path(
                session,
                asset_hash=asset_hash,
                artifactclass=artifactclass,
                destination=destination,
                identity_or_override=(
                    PrivacyOverride(privacy_override) if privacy_override is not None else None
                ),
                backends=backends,
                extractor=RemArchiveExtractor(rem_bin),
                force_suspect=force_suspect,
                force_rejected=force_rejected,
            )
    except RestoreDenied as exc:
        raise click.ClickException(exc.detail) from exc
    except (ArchiveRestoreError, RestoreManagerError, VirtualArrangementError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        f"restored {asset_hash.hex()} from {result.source} to {result.output_path}"
    )


@click.command("reject")
@click.argument("asset_hash_hex")
@click.option("--reason", default=None, help="Reason shown when restore is rejected.")
@click.option("--actor", default=None, help="Operator recorded on the asset.")
def reject_cmd(asset_hash_hex: str, reason: str | None, actor: str | None) -> None:
    """Reject one logical asset without deleting it."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            asset = reject_asset(
                session,
                _asset_hash(asset_hash_hex),
                actor=actor or _operator(),
                reason=reason,
            )
    except (ValueError, VirtualArrangementError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"rejected {asset.content_sha256.hex()}")


@click.command("unreject")
@click.argument("asset_hash_hex")
@click.option("--reason", default=None, help="Reason recorded with the decision.")
@click.option("--actor", default=None, help="Operator recorded with the decision.")
def unreject_cmd(asset_hash_hex: str, reason: str | None, actor: str | None) -> None:
    """Clear the reject marker for one logical asset."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            asset = unreject_asset(
                session,
                _asset_hash(asset_hash_hex),
                actor=actor or _operator(),
                reason=reason,
            )
    except (ValueError, VirtualArrangementError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"unrejected {asset.content_sha256.hex()}")


@click.group("tag")
def tag_group() -> None:
    """Manage content-level governance tags."""


@tag_group.command("add")
@click.argument("tag")
@click.argument("asset_hash_hex")
@click.option("--actor", default=None, help="Operator recorded on the tag.")
def tag_add_cmd(tag: str, asset_hash_hex: str, actor: str | None) -> None:
    """Add one governance tag."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            row = add_tag(session, _asset_hash(asset_hash_hex), tag, actor=actor or _operator())
    except (ValueError, VirtualArrangementError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"tagged {row.logical_asset_hash.hex()} {row.tag!r}")


@tag_group.command("rm")
@click.argument("tag")
@click.argument("asset_hash_hex")
@click.option("--actor", default=None, help="Operator recorded on the tag tombstone.")
def tag_rm_cmd(tag: str, asset_hash_hex: str, actor: str | None) -> None:
    """Soft-delete one governance tag."""

    engine = make_engine()
    try:
        with session_scope(engine) as session:
            row = remove_tag(session, _asset_hash(asset_hash_hex), tag, actor=actor or _operator())
    except (ValueError, VirtualArrangementError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"removed tag {row.logical_asset_hash.hex()} {row.tag!r}")


def _restore_backends(session: Session, artifactclass: str) -> dict[int, StorageBackend]:
    rows = list(
        session.scalars(
            select(Backend)
            .join(Backend.pools)
            .join(ArtifactClassPool, ArtifactClassPool.pool_id == Pool.id)
            .where(
                ArtifactClassPool.artifactclass == artifactclass,
                ArtifactClassPool.active.is_(True),
            )
        )
    )
    return {row.id: backend_from_row(row) for row in rows}


def _asset_hash(value: str) -> bytes:
    return bytes.fromhex(value)


def _operator() -> str:
    return os.environ.get("USER") or "unknown"


def _view_payload(
    view: VirtualArrangement,
    *,
    include_members: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": view.id,
        "name": view.name,
        "description": view.description,
        "created_by": view.created_by,
        "member_count": len(view.members),
        "active_member_count": sum(
            1
            for member in view.members
            if not member.excluded and member.logical_asset.rejected_at is None
        ),
    }
    if include_members:
        payload["members"] = [
            {
                "id": member.id,
                "logical_asset_hash": member.logical_asset_hash.hex(),
                "artifactclass": member.artifactclass,
                "path": member.path,
                "excluded": member.excluded,
                "rejected": member.logical_asset.rejected_at is not None,
            }
            for member in view.members
        ]
    return payload


def _emit(payload: dict[str, Any], *, as_json: bool, kind: str) -> None:
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))
        return
    if kind == "view":
        click.echo(
            f"virtual {payload['name']}: "
            f"members={payload['active_member_count']}/{payload['member_count']}"
        )
        for member in payload.get("members", []):
            flag = " excluded" if member["excluded"] else ""
            rejected = " rejected" if member["rejected"] else ""
            click.echo(
                f"  {member['path']} {member['logical_asset_hash']} "
                f"class={member['artifactclass']}{flag}{rejected}"
            )
