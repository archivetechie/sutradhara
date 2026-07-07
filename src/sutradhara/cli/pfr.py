"""`sutra pfr` commands for sidecar status, reindex, and cut operations."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import json
from pathlib import Path

import click
from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.archive_restore import RestoreNameError, resolve_member_asset_hash
from sutradhara.backend.factory import backend_from_row
from sutradhara.backend.port import StorageBackend
from sutradhara.catalog.models import ArtifactClassPool, Backend, IngestItem, Pool
from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.jobs.config import WorkerConfig, derivation_cache_root
from sutradhara.jobs.engine import submit
from sutradhara.jobs.leases import LeaseError, LeaseManager
from sutradhara.jobs.reconcilers import derivation as derivation_reconciler
from sutradhara.jobs.reconcilers.conditions import OBSERVED_MISSING, record_observation
from sutradhara.pfr import (
    PFRUnavailable,
    current_pfr_recipe_version,
    cut_pfr_asset,
    pfr_status,
    sidecar_for_asset,
)


@click.group("pfr")
def pfr_group() -> None:
    """Partial file restore sidecars and cuts."""


@pfr_group.command("status")
@click.argument("asset_hash_hex", required=False)
@click.option("--artifactclass", required=True, help="Artifactclass restore policy.")
@click.option("--member-name", default=None, help="Escaped customer manifest member name.")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON.")
def status_cmd(
    asset_hash_hex: str | None,
    artifactclass: str,
    member_name: str | None,
    as_json: bool,
) -> None:
    """Show PFR readiness for one asset or member selector."""

    engine = make_engine()
    with session_scope(engine) as session:
        asset_hash = _resolve_asset_hash(session, artifactclass, asset_hash_hex, member_name)
        payload = pfr_status(session, asset_hash=asset_hash, artifactclass=artifactclass)
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    sidecar = payload["sidecar"]
    sidecar_text = (
        "missing"
        if sidecar is None
        else (
            f"{sidecar['grammar_id']} recipe={sidecar['recipe_version']} "
            f"blobs={sidecar['blobs_ok']}"
        )
    )
    ranged = sum(1 for locator in payload["locators"] if locator["ranged_pfr"])
    click.echo(
        f"{payload['asset_hash']}: sidecar={sidecar_text}; "
        f"locators={len(payload['locators'])}; ranged={ranged}"
    )


@pfr_group.command("cut")
@click.argument("asset_hash_hex", required=False)
@click.option("--artifactclass", required=True, help="Artifactclass restore policy.")
@click.option("--member-name", default=None, help="Escaped customer manifest member name.")
@click.option("--from", "from_time", required=True, type=float, help="File-relative in time.")
@click.option("--to", "to_time", required=True, type=float, help="File-relative out time.")
@click.option("-o", "--output", type=click.Path(dir_okay=False), default=None)
@click.option("--rem-bin", default="rem", show_default=True, help="rem CLI binary.")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON.")
def cut_cmd(
    asset_hash_hex: str | None,
    artifactclass: str,
    member_name: str | None,
    from_time: float,
    to_time: float,
    output: str | None,
    rem_bin: str,
    as_json: bool,
) -> None:
    """Cut a clip, falling back to whole-member restore when needed."""

    engine = make_engine()
    with session_scope(engine) as session:
        asset_hash = _resolve_asset_hash(session, artifactclass, asset_hash_hex, member_name)
        destination = (
            Path(output)
            if output
            else Path(f"{asset_hash.hex()}-{from_time:g}-{to_time:g}.mxf")
        )
        try:
            with _inline_io_lease():
                result = cut_pfr_asset(
                    session,
                    asset_hash=asset_hash,
                    artifactclass=artifactclass,
                    destination=destination,
                    backends=_restore_backends(session, artifactclass),
                    t_in=from_time,
                    t_out=to_time,
                    rem_bin=rem_bin,
                )
        except (LeaseError, PFRUnavailable, ValueError) as exc:
            raise click.ClickException(str(exc)) from exc
    if as_json:
        click.echo(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return
    click.echo(
        f"pfr rung={result.rung} reason={result.reason} "
        f"asset={result.asset_hash.hex()} output={result.output_path}"
    )


@pfr_group.command("reindex")
@click.argument("asset_hash_hex", required=False)
@click.option("--artifactclass", default=None, help="Resolve ASSET_HASH_HEX through a member name.")
@click.option("--member-name", default=None, help="Escaped customer manifest member name.")
@click.option("--grammar", type=click.Choice(["fallback"]), default=None)
@click.option("--all", "all_sidecars", is_flag=True, help="Reindex every current PFR sidecar.")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON.")
def reindex_cmd(
    asset_hash_hex: str | None,
    artifactclass: str | None,
    member_name: str | None,
    grammar: str | None,
    all_sidecars: bool,
    as_json: bool,
) -> None:
    """Enqueue forced pfr-index jobs without the presence-gated reconciler."""

    if (grammar is None) == (not all_sidecars):
        raise click.ClickException("provide exactly one of --grammar fallback or --all")
    engine = make_engine()
    recipe_version = current_pfr_recipe_version()
    with session_scope(engine) as session:
        asset_hash = (
            _resolve_asset_hash(session, artifactclass or "", asset_hash_hex, member_name)
            if asset_hash_hex is not None or member_name is not None
            else None
        )
        jobs = []
        for item in _reindex_items(session, asset_hash=asset_hash, grammar=grammar):
            target_key = derivation_reconciler.make_target_key(item.id, "pfr-index")
            record_observation(
                session,
                domain=derivation_reconciler.DOMAIN,
                target_key=target_key,
                desired=True,
                observed_state=OBSERVED_MISSING,
                reason="pfr-reindex",
                message="forced PFR reindex requested",
            )
            job = submit(
                session,
                "pfr-index",
                {
                    "ingest_item_id": item.id,
                    "output_class": None,
                    "cache_root": str(derivation_cache_root()),
                    "force": True,
                    "recipe_version": recipe_version,
                },
                required_resources=[{"pool": "io", "count": 1}, {"pool": "cpu", "count": 1}],
                dedupe_key=f"pfr-reindex:{item.id}:{recipe_version}",
                recon_domain=derivation_reconciler.DOMAIN,
                recon_target_key=target_key,
            )
            jobs.append(job.id)
    payload = {"recipe_version": recipe_version, "jobs": jobs, "count": len(jobs)}
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(f"enqueued {len(jobs)} pfr-index job(s) for recipe {recipe_version}")


def _resolve_asset_hash(
    session: Session,
    artifactclass: str,
    asset_hash_hex: str | None,
    member_name: str | None,
) -> bytes:
    if (asset_hash_hex is None) == (member_name is None):
        raise click.ClickException("provide exactly one of ASSET_HASH_HEX or --member-name")
    if asset_hash_hex is not None:
        try:
            return bytes.fromhex(asset_hash_hex)
        except ValueError as exc:
            raise click.ClickException("ASSET_HASH_HEX must be hex") from exc
    if not artifactclass:
        raise click.ClickException("--artifactclass is required with --member-name")
    try:
        return resolve_member_asset_hash(
            session,
            artifactclass=artifactclass,
            member_name=member_name or "",
        )
    except RestoreNameError as exc:
        raise click.ClickException(str(exc)) from exc


def _restore_backends(session: Session, artifactclass: str) -> dict[int, StorageBackend]:
    active_pool_ids = [
        row.pool_id
        for row in session.scalars(
            select(ArtifactClassPool)
            .where(
                ArtifactClassPool.artifactclass == artifactclass,
                ArtifactClassPool.active.is_(True),
            )
            .order_by(ArtifactClassPool.sort_order, ArtifactClassPool.pool_id)
        )
    ]
    rows = list(
        session.scalars(select(Backend).join(Backend.pools).where(Pool.id.in_(active_pool_ids)))
    )
    return {row.id: backend_from_row(row) for row in rows}


@contextmanager
def _inline_io_lease() -> Iterator[dict[str, int]]:
    manager = LeaseManager(WorkerConfig.defaults().capacities)
    granted = manager.reserve({"io": 1})
    try:
        yield granted
    finally:
        manager.release(granted)


def _reindex_items(
    session: Session,
    *,
    asset_hash: bytes | None,
    grammar: str | None,
) -> list[IngestItem]:
    query = select(IngestItem).order_by(IngestItem.id)
    if asset_hash is not None:
        query = query.where(IngestItem.logical_asset_hash == asset_hash)
    items = list(session.scalars(query))
    if grammar is None:
        return [
            item
            for item in items
            if isinstance((item.item_metadata or {}).get("pfr_sidecar_path"), str)
        ]
    selected: list[IngestItem] = []
    for item in items:
        record = sidecar_for_asset(session, item.logical_asset_hash)
        if record is not None and record.sidecar.grammar_id == grammar:
            selected.append(item)
    return selected
