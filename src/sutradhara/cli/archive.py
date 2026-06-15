"""`sutra archive` commands for RAO artifactclass bundling and restore."""

from __future__ import annotations

import json
from pathlib import Path

import click
from sqlalchemy import select

from sutradhara.archive_bundle import record_review_decision
from sutradhara.archive_fanout import (
    BundleHeld,
    HmacManifestSigner,
    ManifestSigningError,
    RemArchiveBuilder,
    flush_bundle,
)
from sutradhara.archive_restore import (
    RemArchiveExtractor,
    RestoreNameError,
    resolve_member_asset_hash,
    restore_asset,
)
from sutradhara.artifactclass_policy import (
    apply_artifactclass_policy_file,
    get_artifactclass_policy,
)
from sutradhara.backend.factory import backend_from_row
from sutradhara.catalog.models import ArtifactClassPool, Backend, Bundle, Pool
from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.staging import StagingHeld, stage_and_enqueue_artifact


@click.group("archive")
def archive_group() -> None:
    """Archive artifactclass policy, bundles, review, and restore."""


@archive_group.group("artifactclass")
def artifactclass_group() -> None:
    """Manage artifactclass policy documents."""


@artifactclass_group.command("apply")
@click.argument("artifactclass")
@click.argument("policy_path", type=click.Path(exists=True, dir_okay=False))
def artifactclass_apply(artifactclass: str, policy_path: str) -> None:
    """Strict-validate and apply an artifactclass TOML policy."""
    engine = make_engine()
    with session_scope(engine) as session:
        policy = apply_artifactclass_policy_file(session, artifactclass, policy_path)
    click.echo(
        f"Applied {artifactclass!r}: ruleset={policy.ruleset!r}, pools={len(policy.placements)}"
    )


@archive_group.group("bundle")
def bundle_group() -> None:
    """Manage durable bundle accumulators."""


@bundle_group.command("enqueue")
@click.argument("artifactclass")
@click.argument("asset_hash_hex")
@click.argument("source_path", type=click.Path(exists=True, dir_okay=False))
@click.option("--member-path", default=None, help="Path stored inside the archive.")
@click.option(
    "--staging-dir",
    type=click.Path(file_okay=False),
    default=None,
    help="Directory for copy-on-write staging transforms.",
)
def bundle_enqueue(
    artifactclass: str,
    asset_hash_hex: str,
    source_path: str,
    member_path: str | None,
    staging_dir: str | None,
) -> None:
    """Stage and add an existing logical asset to an artifactclass open bundle."""
    expected_hash = bytes.fromhex(asset_hash_hex)
    engine = make_engine()
    with session_scope(engine) as session:
        policy = get_artifactclass_policy(session, artifactclass)
        try:
            staged = stage_and_enqueue_artifact(
                session,
                artifactclass=artifactclass,
                policy=policy,
                source_path=source_path,
                staging_root=_staging_root(source_path, staging_dir),
                member_path=member_path,
            )
        except StagingHeld as exc:
            raise click.ClickException(json.dumps(exc.summary, indent=2, sort_keys=True)) from exc
        if staged.logical_sha256 != expected_hash:
            raise click.ClickException(
                f"source hash {staged.logical_sha256.hex()} does not match {asset_hash_hex}"
            )
        bundle = session.scalars(
            select(Bundle).where(
                Bundle.artifactclass == artifactclass,
                Bundle.status == "open",
            )
        ).first()
        if bundle is None:
            raise click.ClickException("staging did not create an open bundle")
        click.echo(
            f"enqueued {staged.stored_member_path!r} in bundle {bundle.id}"
        )


@bundle_group.command("flush")
@click.argument("bundle_id")
@click.option(
    "--deliverables-dir",
    type=click.Path(file_okay=False),
    default=None,
    help="Directory for customer manifest receipts.",
)
@click.option("--rem-bin", default="rem", show_default=True, help="rem CLI binary.")
@click.option("--key-epoch", default=None, help="Key epoch for rao-aead-v1 pools.")
@click.option(
    "--manifest-signing-key-file",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Raw HMAC key file for customer manifest receipts.",
)
def bundle_flush(
    bundle_id: str,
    deliverables_dir: str | None,
    rem_bin: str,
    key_epoch: str | None,
    manifest_signing_key_file: str | None,
) -> None:
    """Flush one open bundle to all active artifactclass pools."""
    engine = make_engine()
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        if bundle is None:
            raise click.ClickException(f"no bundle {bundle_id!r}")
        backends = _target_backends(session, bundle.artifactclass)
        try:
            result = flush_bundle(
                session,
                bundle_id=bundle_id,
                backends=backends,
                builder=RemArchiveBuilder(rem_bin),
                key_epoch=key_epoch,
                deliverables_dir=None if deliverables_dir is None else Path(deliverables_dir),
                manifest_signer=_manifest_signer(manifest_signing_key_file),
            )
        except (BundleHeld, ManifestSigningError) as exc:
            raise click.ClickException(str(exc)) from exc
    click.echo(
        f"sealed {result.bundle_id}: copies={list(result.copy_ids)} "
        f"manifest={result.manifest_path or '(none)'}"
    )


@archive_group.command("review")
@click.argument("bundle_id")
@click.option(
    "--action",
    type=click.Choice(["wrap", "blob", "exclude", "fix-source-and-rescan", "abort"]),
    default=None,
    help="Record a review action. Omit to show the held summary.",
)
@click.option(
    "--scope",
    type=click.Choice(["just-this-ingest", "persist-rule"]),
    default="just-this-ingest",
    show_default=True,
)
@click.option("--subtree", default=None, help="Subtree/prefix this action covers.")
@click.option("--why", default=None, help="Reason for the review decision.")
@click.option("--who", default=None, help="Reviewer/operator name.")
def review_cmd(
    bundle_id: str,
    action: str | None,
    scope: str,
    subtree: str | None,
    why: str | None,
    who: str | None,
) -> None:
    """Show or record a held-bundle review decision."""
    engine = make_engine()
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        if bundle is None:
            raise click.ClickException(f"no bundle {bundle_id!r}")
        if action is None:
            click.echo(json.dumps(bundle.review_summary or {}, indent=2, sort_keys=True))
            return
        if bundle.status != "held":
            raise click.ClickException(
                f"bundle {bundle_id!r} is {bundle.status!r}; only held bundles can be reviewed"
            )
        if not who:
            raise click.ClickException("--who is required when recording a review decision")
        if not why:
            raise click.ClickException("--why is required when recording a review decision")
        decision = record_review_decision(
            session,
            bundle_id=bundle_id,
            action=action,
            scope=scope,
            subtree=subtree,
            reason=why,
            reviewer=who,
            persisted_rule=(
                {"action": action, "subtree": subtree} if scope == "persist-rule" else None
            ),
        )
        if action == "abort":
            bundle.status = "aborted"
        click.echo(f"recorded review decision {decision.id} for bundle {bundle_id}")


@archive_group.command("restore")
@click.argument("asset_hash_hex", required=False)
@click.option("--artifactclass", required=True, help="Artifactclass restore policy.")
@click.option("--dest", "destination", required=True, type=click.Path(dir_okay=False))
@click.option("--rem-bin", default="rem", show_default=True, help="rem CLI binary.")
@click.option("--member-name", default=None, help="Escaped customer manifest member name.")
def restore_cmd(
    asset_hash_hex: str | None,
    artifactclass: str,
    destination: str,
    rem_bin: str,
    member_name: str | None,
) -> None:
    """Restore one asset using artifactclass pool preference."""
    engine = make_engine()
    with session_scope(engine) as session:
        if (asset_hash_hex is None) == (member_name is None):
            raise click.ClickException("provide exactly one of ASSET_HASH_HEX or --member-name")
        asset_hash = (
            bytes.fromhex(asset_hash_hex)
            if asset_hash_hex is not None
            else _resolve_member_hash(
                session=session,
                artifactclass=artifactclass,
                member_name=member_name,
            )
        )
        policy = get_artifactclass_policy(session, artifactclass)
        backends = _restore_backends(session, artifactclass, policy.restore_preference)
        result = restore_asset(
            session,
            asset_hash=asset_hash,
            artifactclass=artifactclass,
            destination=destination,
            backends=backends,
            extractor=RemArchiveExtractor(rem_bin),
        )
    click.echo(
        f"restored {result.asset_hash.hex()} from pool {result.pool_id} copy {result.copy_id} "
        f"to {result.output_path}"
    )


def _target_backends(session, artifactclass: str):
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


def _resolve_member_hash(session, artifactclass: str, member_name: str | None) -> bytes:
    try:
        return resolve_member_asset_hash(
            session,
            artifactclass=artifactclass,
            member_name=member_name or "",
        )
    except RestoreNameError as exc:
        raise click.ClickException(str(exc)) from exc


def _staging_root(source_path: str, staging_dir: str | None) -> Path:
    if staging_dir is not None:
        return Path(staging_dir)
    return Path(source_path).resolve().parent / ".sutradhara-stage"


def _restore_backends(session, artifactclass: str, pool_ids: list[str]):
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
    wanted = [*pool_ids, *(pool_id for pool_id in active_pool_ids if pool_id not in pool_ids)]
    rows = list(session.scalars(select(Backend).join(Backend.pools).where(Pool.id.in_(wanted))))
    return {row.id: backend_from_row(row) for row in rows}


def _manifest_signer(path: str | None) -> HmacManifestSigner | None:
    if path is None:
        return None
    key_path = Path(path)
    return HmacManifestSigner(
        key=key_path.read_bytes().rstrip(b"\r\n"),
        key_id=key_path.name,
    )
