"""`sutra archive` commands for RAO artifactclass bundling and restore."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import cast

import click
from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.archive_bundle import bundle_primary_artifactclass, record_review_decision
from sutradhara.archive_enqueue import (
    ArchiveEnqueueError,
    BatchScanHeld,
    EnqueueItem,
    enqueue_intake_batch,
    scan_enqueue_batch,
)
from sutradhara.archive_fanout import (
    FlushPreflightShort,
    HmacManifestSigner,
    ManifestSigningError,
    RemArchiveBuilder,
    flush_bundle,
)
from sutradhara.archive_predicate import build_archive_predicate_audit
from sutradhara.archive_restore import (
    ArchiveRestoreError,
    RemArchiveExtractor,
    RestoreNameError,
    resolve_member_asset_hash,
)
from sutradhara.archive_submission import ArchiveSubmissionError, archive_submission
from sutradhara.artifactclass_policy import (
    apply_artifactclass_policy_file,
    get_artifactclass_policy,
)
from sutradhara.backend.factory import backend_from_row
from sutradhara.backend.port import StorageBackend
from sutradhara.catalog.models import ArtifactClassPool, Backend, Bundle, Pool, Submission
from sutradhara.catalog.session import make_engine, make_read_only_engine, session_scope
from sutradhara.hdcache.manager import (
    PrivacyOverride,
    RestoreDenied,
    RestoreManagerError,
    restore_to_path,
)
from sutradhara.replication import WritableStorageBackend
from sutradhara.staging import StagingHeld


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


@archive_group.group("submission")
def submission_group() -> None:
    """Archive frozen arrangement submissions."""


@archive_group.command("predicate-audit")
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Write the reusable JSON audit artifact to this path.",
)
@click.option("--force", is_flag=True, help="Replace an existing output artifact.")
def predicate_audit(output: Path, force: bool) -> None:
    """Audit retention-passed intakes before enabling ALL semantics."""

    if output.exists() and not force:
        raise click.ClickException(f"output already exists: {output}; pass --force to replace it")
    engine = make_read_only_engine()
    try:
        with Session(engine) as session:
            report = build_archive_predicate_audit(session)
    finally:
        engine.dispose()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)
    summary = report["summary"]
    assert isinstance(summary, dict)
    click.echo(
        f"wrote {output}: audited={summary['audited_intakes']} "
        f"affected={summary['affected_intakes']} gate_safe={summary['gate_safe']}"
    )


@submission_group.command("flush")
@click.argument("submission_id")
@click.option("--rem-bin", default="rem", show_default=True, help="rem CLI binary.")
@click.option("--key-epoch", default=None, help="Key epoch for rao-aead-v1 pools.")
def submission_flush(submission_id: str, rem_bin: str, key_epoch: str | None) -> None:
    """Flush one pending arrangement submission to its artifactclass pools."""
    engine = make_engine()
    with session_scope(engine) as session:
        submission = session.get(Submission, submission_id)
        if submission is None:
            raise click.ClickException(f"no submission {submission_id!r}")
        backends = _target_backends(session, submission.artifactclass)
        try:
            result = archive_submission(
                session,
                submission_id,
                backends=backends,
                builder=RemArchiveBuilder(rem_bin),
                key_epoch=key_epoch,
            )
        except (ArchiveSubmissionError, ManifestSigningError) as exc:
            raise click.ClickException(str(exc)) from exc
    action = "already archived" if result.noop else "archived"
    click.echo(
        f"{action} {result.submission_id}: bundle={result.bundle_id} copies={list(result.copy_ids)}"
    )


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
@click.option("--rem-bin", default="rem", show_default=True, help="rem CLI binary.")
def bundle_enqueue(
    artifactclass: str,
    asset_hash_hex: str,
    source_path: str,
    member_path: str | None,
    staging_dir: str | None,
    rem_bin: str,
) -> None:
    """Stage and add an existing logical asset to an artifactclass open bundle.

    A wrapper over a one-member enqueue batch: the class ruleset scan runs at
    enqueue-batch grain against the file's tree root, and the reported bundle
    is the one the member actually landed in — an include-alone member routes
    to its own funnel bundle, not the group accumulator.
    """
    expected_hash = bytes.fromhex(asset_hash_hex)
    source = Path(source_path).resolve()
    engine = make_engine()
    held_summary: dict[str, object] | None = None
    message: str | None = None
    with session_scope(engine) as session:
        policy = get_artifactclass_policy(session, artifactclass)
        item = EnqueueItem(
            logical_asset_hash=expected_hash,
            source_path=source,
            member_path=member_path or source.name,
        )
        try:
            result = scan_enqueue_batch(
                session,
                artifactclass=artifactclass,
                policy=policy,
                scan_root=source.parent,
                items=[item],
                staging_root=_staging_root(source_path, staging_dir),
                rem_bin=rem_bin,
            )
        except StagingHeld as exc:
            held_summary = exc.summary
            result = None
        except ArchiveEnqueueError as exc:
            raise click.ClickException(_enqueue_error_text(exc)) from exc
        if result is not None:
            if result.enqueued:
                member = result.enqueued[0]
                message = f"enqueued {member.member_path!r} in bundle {member.bundle_id}"
            else:
                message = (
                    f"not enqueued: {item.member_path!r} matched scan verdicts "
                    f"(excluded={list(result.excluded_prefixes)} "
                    f"blob={list(result.blob_prefixes)})"
                )
    if held_summary is not None:
        raise click.ClickException(json.dumps(held_summary, indent=2, sort_keys=True))
    if message is not None:
        click.echo(message)


@bundle_group.command("enqueue-intake")
@click.argument("intake_id")
@click.option(
    "--staging-dir",
    type=click.Path(file_okay=False),
    default=None,
    help="Directory for copy-on-write staging transforms.",
)
@click.option("--rem-bin", default="rem", show_default=True, help="rem CLI binary.")
def bundle_enqueue_intake(
    intake_id: str,
    staging_dir: str | None,
    rem_bin: str,
) -> None:
    """Enqueue a registered intake's items, one ruleset scan per (class, tree root)."""
    engine = make_engine()
    lines: list[str] = []
    with session_scope(engine) as session:
        try:
            results = enqueue_intake_batch(
                session,
                intake_id=intake_id,
                staging_root=None if staging_dir is None else Path(staging_dir),
                rem_bin=rem_bin,
            )
        except StagingHeld as exc:
            raise click.ClickException(
                json.dumps(exc.summary, indent=2, sort_keys=True)
            ) from exc
        except ArchiveEnqueueError as exc:
            raise click.ClickException(_enqueue_error_text(exc)) from exc
        for result in results:
            bundle_ids = sorted({member.bundle_id for member in result.enqueued})
            lines.append(
                f"{result.artifactclass}: enqueued={len(result.enqueued)} "
                f"bundles={bundle_ids} "
                f"excluded={list(result.excluded_prefixes)} "
                f"blob={list(result.blob_prefixes)}"
            )
    for line in lines:
        click.echo(line)


def _enqueue_error_text(exc: Exception) -> str:
    if isinstance(exc, BatchScanHeld):
        return json.dumps(exc.summary, indent=2, sort_keys=True)
    return str(exc)


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
        # BG-P4: representative member class; P4 reads group_basis pool order.
        hop_class = bundle_primary_artifactclass(session, bundle)
        if hop_class is None:
            raise click.ClickException(f"bundle {bundle_id!r} has no member artifactclass")
        backends = _target_backends(session, hop_class)
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
        # FlushPreflightShort is the §8 skip-and-alarm verdict: the work dir
        # cannot hold bundle x targets, nothing was mutated. That is an
        # operator message ("come back with more space"), not a traceback.
        except (ManifestSigningError, FlushPreflightShort) as exc:
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
@click.option(
    "--force",
    "force_suspect",
    is_flag=True,
    help="Restore even when the logical asset is flagged suspect.",
)
@click.option(
    "--force-rejected",
    is_flag=True,
    help="Restore even when the logical asset is rejected.",
)
@click.option(
    "--privacy-override",
    default=None,
    help="Trusted CLI reason for restoring private hdcache assets without API grants.",
)
def restore_cmd(
    asset_hash_hex: str | None,
    artifactclass: str,
    destination: str,
    rem_bin: str,
    member_name: str | None,
    force_suspect: bool,
    force_rejected: bool,
    privacy_override: str | None,
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
        try:
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
        except (ArchiveRestoreError, RestoreManagerError) as exc:
            raise click.ClickException(str(exc)) from exc
    click.echo(f"restored {asset_hash.hex()} from {result.source} to {result.output_path}")


def _target_backends(session: Session, artifactclass: str) -> dict[int, WritableStorageBackend]:
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
    return {row.id: cast(WritableStorageBackend, backend_from_row(row)) for row in rows}


def _resolve_member_hash(session: Session, artifactclass: str, member_name: str | None) -> bytes:
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


def _restore_backends(
    session: Session, artifactclass: str, pool_ids: list[str]
) -> dict[int, StorageBackend]:
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
