"""`sutra archive` commands for RAO artifactclass bundling and restore."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import cast

import click
from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.archive_bundle import bundle_artifactclasses, record_review_decision
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
from sutradhara.archive_sweeper import sweep_bundles
from sutradhara.artifactclass_policy import (
    apply_artifactclass_policy_file,
    get_artifactclass_policy,
)
from sutradhara.backend.factory import backend_from_row
from sutradhara.backend.port import StorageBackend
from sutradhara.bundle_group import basis_pool_ids
from sutradhara.bundle_group_report import render_policy_apply_report
from sutradhara.catalog.models import ArtifactClassPool, Backend, Bundle, Pool, Submission
from sutradhara.catalog.session import make_engine, make_read_only_engine, session_scope
from sutradhara.hdcache.manager import (
    PrivacyOverride,
    RestoreDenied,
    RestoreManagerError,
    restore_to_path,
)
from sutradhara.jobs.handlers.bundle_sweep import sweep_backends
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
        policy, report = apply_artifactclass_policy_file(session, artifactclass, policy_path)
        # Rendered inside the session: the report is a value built from catalog
        # rows, but it is only meaningful next to the apply that produced it.
        rendered = render_policy_apply_report(report)
    click.echo(
        f"Applied {artifactclass!r}: ruleset={policy.ruleset!r}, pools={len(policy.placements)}"
    )
    # Bundle groups are derived, never declared — this is the operator's only
    # read-back of which classes now share a crate (§2).
    click.echo(rendered)


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
    """Report retention-passed intakes with an incomplete archive state."""

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
        f"affected={summary['affected_intakes']} clean={summary['clean']}"
    )


@submission_group.command("accumulate")
@click.argument("submission_id")
def submission_accumulate(submission_id: str) -> None:
    """Append one arrangement submission to its bundle group's accumulator.

    A submission no longer builds an object of its own. It converges on the
    group accumulator and the sweeper (`sutra archive bundle sweep`) seals that
    accumulator when it is full or overdue, so this command writes catalog rows
    and touches no backend. The submission reports `archived` only once every
    member sits in a sealed bundle with enough verified copies.
    """
    engine = make_engine()
    with session_scope(engine) as session:
        if session.get(Submission, submission_id) is None:
            raise click.ClickException(f"no submission {submission_id!r}")
        try:
            result = archive_submission(session, submission_id)
        except ArchiveSubmissionError as exc:
            raise click.ClickException(str(exc)) from exc
        lines = [
            f"{'already accumulated' if result.noop else 'accumulated'} "
            f"{result.submission_id}: archived={result.archived}"
        ]
        for bundle_id in result.bundle_ids:
            bundle = session.get(Bundle, bundle_id)
            status = "?" if bundle is None else bundle.status
            copies = list(result.copies_by_bundle.get(bundle_id, ()))
            lines.append(f"  bundle {bundle_id} status={status} copies={copies}")
    for line in lines:
        click.echo(line)


@bundle_group.command("enqueue")
@click.argument("artifactclass")
@click.argument("asset_hash_hex")
@click.argument("source_path", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--scan-root",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help=(
        "Source tree root the class ruleset is written against. "
        "SOURCE_PATH must live under it; the member's rule-matched path is "
        "its path relative to this root."
    ),
)
@click.option(
    "--member-path",
    default=None,
    help="Override the name stored inside the archive (default: the scan-root-relative path).",
)
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
    scan_root: Path,
    member_path: str | None,
    staging_dir: str | None,
    rem_bin: str,
) -> None:
    """Stage and add an existing logical asset to an artifactclass open bundle.

    A wrapper over a one-member enqueue batch: the class ruleset scan runs at
    enqueue-batch grain against ``--scan-root``, and the reported bundle is the
    one the member actually landed in — an include-alone member routes to its
    own funnel bundle, not the group accumulator.

    ``--scan-root`` is required, deliberately. Rules match paths relative to
    the scan root, so deriving it from the file (its parent directory) would
    hand rem a root under which ``proxies/x.mov`` is just ``x.mov`` and a rule
    scoped ``proxies/**`` would never fire — the member would be silently
    enqueued and archived. Only the operator knows which tree the class's
    ruleset was written against, so the command asks instead of guessing.
    """
    expected_hash = bytes.fromhex(asset_hash_hex)
    source = Path(source_path).resolve()
    root = Path(scan_root).resolve()
    try:
        relative_path = source.relative_to(root)
    except ValueError as exc:
        raise click.ClickException(
            f"source {source} is not under --scan-root {root}; "
            "rules match paths relative to the scan root, so a source outside "
            "it has no path a path-scoped rule could match"
        ) from exc
    engine = make_engine()
    held_summary: dict[str, object] | None = None
    message: str | None = None
    with session_scope(engine) as session:
        policy = get_artifactclass_policy(session, artifactclass)
        item = EnqueueItem(
            logical_asset_hash=expected_hash,
            source_path=source,
            member_path=relative_path.as_posix(),
            archive_path=member_path,
        )
        try:
            result = scan_enqueue_batch(
                session,
                artifactclass=artifactclass,
                policy=policy,
                scan_root=root,
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
                # Member grain (§5, P1 residue F6): report the bundle this
                # member actually landed in — `EnqueuedMember.bundle_id` is
                # copied straight off the member's own catalog row, never a
                # group-wide accumulator lookup, which would misreport an
                # include-alone funnel routing.
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
    held_summary: dict[str, object] | None = None
    with session_scope(engine) as session:
        try:
            results = enqueue_intake_batch(
                session,
                intake_id=intake_id,
                staging_root=None if staging_dir is None else Path(staging_dir),
                rem_bin=rem_bin,
            )
        except StagingHeld as exc:
            # A hold must SURVIVE the failure that produced it: staging wrote
            # `held` on the bundle before raising, and raising from inside the
            # session scope would roll that write back with everything else —
            # the operator would then see a hold message with no held bundle to
            # review. The summary is carried out and raised after the commit.
            held_summary = exc.summary
            results = []
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
    if held_summary is not None:
        raise click.ClickException(json.dumps(held_summary, indent=2, sort_keys=True))


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
    """Force-flush one open bundle to its bundle group's basis pools.

    Force-flush is **group grain**, not class grain: the bundle holds every
    class whose storage placement matches, so sealing it here seals other
    classes' material too, at whatever fill it has reached. The command reports
    that fill and those classes before it seals, because an operator sealing a
    12%-full accumulator to get one class's material onto tape is spending the
    whole group's tape efficiency to do it.
    """
    engine = make_engine()
    with session_scope(engine) as session:
        bundle = session.get(Bundle, bundle_id)
        if bundle is None:
            raise click.ClickException(f"no bundle {bundle_id!r}")
        for line in _force_flush_fill_warning(session, bundle):
            click.echo(line, err=True)
        # Group grain (§5): the flush's backends come from the bundle's frozen
        # group_basis pool set, not from any member class's live policy.
        backends = _bundle_basis_backends(session, bundle)
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


def _force_flush_fill_warning(session: Session, bundle: Bundle) -> list[str]:
    """The §4 force-flush disclosure: current fill and the classes it seals."""
    if bundle.status != "open":
        return []
    classes = bundle_artifactclasses(session, bundle)
    fill = (
        f"{100 * bundle.total_bytes / bundle.target_bytes:.1f}%"
        if bundle.target_bytes
        else "n/a (no target)"
    )
    return [
        f"force-flush is group grain: bundle {bundle.id} is the accumulator for "
        f"bundle group {bundle.bundle_group[:12]}…",
        f"  fill: {bundle.total_bytes} / {bundle.target_bytes} bytes ({fill}), "
        f"{bundle.member_count} member(s)",
        f"  member classes sealed by this flush: {classes}",
    ]


@bundle_group.command("sweep")
@click.option("--rem-bin", default="rem", show_default=True, help="rem CLI binary.")
@click.option("--key-epoch", default=None, help="Key epoch for rao-aead-v1 pools.")
@click.option(
    "--no-reap",
    is_flag=True,
    help="Skip the stuck-claim reaper (diagnosis only; the sweep still flushes).",
)
def bundle_sweep(rem_bin: str, key_epoch: str | None, no_reap: bool) -> None:
    """Run one sweeper pass: reap, void-seal orphans, drain, flush what is due.

    The same pass the `bundle-sweep` job runs. This is the only caller of
    `bundle_due`'s age arm, so a quiet class's accumulator seals here or not at
    all.
    """
    engine = make_engine()
    with session_scope(engine) as session:
        result = sweep_bundles(
            session,
            backends=sweep_backends(session),
            builder=RemArchiveBuilder(rem_bin),
            key_epoch=key_epoch,
            reap=not no_reap,
        )
    click.echo(
        f"sweep: reaped={list(result.reaped)} voided={list(result.voided)} "
        f"drained={list(result.drained)} flushed={list(result.flushed)}"
    )
    for bundle_id, reason in result.failed:
        click.echo(f"  failed {bundle_id}: {reason}", err=True)
    if result.failed:
        raise SystemExit(1)


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


def _bundle_basis_backends(
    session: Session, bundle: Bundle
) -> dict[int, WritableStorageBackend]:
    """Backends for the pools in a bundle's frozen group_basis (§5)."""
    pool_ids = basis_pool_ids(bundle.group_basis)
    if not pool_ids:
        raise click.ClickException(f"bundle {bundle.id!r} has an empty group_basis")
    rows = list(
        session.scalars(
            select(Backend).join(Backend.pools).where(Pool.id.in_(pool_ids)).distinct()
        )
    )
    return {row.id: cast(WritableStorageBackend, backend_from_row(row)) for row in rows}


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
