"""Durable RAO archive bundle bookkeeping.

This module owns the sutradhara-side accumulator state: one open bundle per
**bundle group** (the derived fingerprint of a class's active pool set — see
``sutradhara.bundle_group``), its pending member set, effective flush
thresholds frozen at open from the group's declared class set, the canonical
member-naming ladder both producers call, per-copy asset locators, blob-root
pointers, exclusion records, and held-bundle review decisions.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import os
import re
import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError, InvalidRequestError
from sqlalchemy.orm import Session

from sutradhara.bundle_group import (
    BASIS_SOURCE_DERIVED,
    compute_bundle_group,
    effective_group_thresholds,
    group_basis_document,
)
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    AssetLocator,
    BlobRoot,
    Bundle,
    BundleMember,
    Copy,
    ExclusionRecord,
    LogicalAsset,
    Pool,
    ReviewDecision,
    StagingTransform,
)
from sutradhara.catalog.types import is_content_hash
from sutradhara.jobs.attempts import default_worker_id
from sutradhara.structured_logs import emit_structured_event
from sutradhara_receive.member_name import escape_path_name, escape_path_text


class ArchiveBundleError(Exception):
    """Base class for archive bundle catalog errors."""


class UnknownBundleAsset(ArchiveBundleError):
    """A bundle operation referenced an unknown logical asset."""


class UnknownBundlePool(ArchiveBundleError):
    """A locator operation referenced an unknown pool."""


class UnknownBundleCopy(ArchiveBundleError):
    """A locator operation referenced an unknown copy."""


class AssetLocatorError(ArchiveBundleError):
    """An asset locator is malformed."""


class BundleStateError(ArchiveBundleError):
    """A bundle operation was requested in the wrong lifecycle state."""


class StagingTransformError(ArchiveBundleError):
    """A staging transform record is inconsistent with its bundle member."""


class BundleClaimLost(ArchiveBundleError):
    """A guarded bundle claim transition did not apply to exactly one row.

    Either the ``open -> flushing`` claim lost the race (another flusher, or a
    seal that already happened), or a ``flushing -> sealed`` close found the
    claim gone — the reaper returned the bundle to ``open`` and something else
    may already be building it. Sealing on a lost claim would record a member
    set that is not on media, so the close fails loudly instead.
    """


class MemberNamingError(ArchiveBundleError):
    """The canonical member-naming ladder could not produce a name.

    The terminal rung carries the member's full content hash plus the class
    slug seeded with a hash of the raw class name — the slug alone is not
    injective (``photo.raw`` and ``photo-raw`` share a slug; the seed is
    injective up to a SHA-256 collision on class names). Exhaustion therefore
    requires a SHA-256 collision, or a co-resident member whose literal
    recorded name equals this member's terminal-rung name — not merely two
    classes with lookalike names.
    """


# Progressive hash-prefix rungs for the collision ladder; the terminal rung is
# the full hash plus the seeded class slug (appended by _name_ladder itself).
_NAME_LADDER_PREFIXES = (10, 20, 32, 48, 64)

# The submission linkage design §4 names: SubmissionMember -> bundle_member ->
# bundle. It is a LIST because one member row can legitimately serve several
# submissions: the group accumulator converges producers, so a submission whose
# (class, member_path, content hash) is already present — a co-resident intake
# enqueue, or a second submission of identical content under the duplicate-
# content workflow — lands on the existing row rather than a new one. A single
# submission_id field made that last-writer-wins, and the loser then never
# found its own members again: no noop branch, the whole submission re-hashed
# on every call, and its bundle missing from the reported result.
SUBMISSION_LINKS_KEY = "submission_links"


def submission_links(metadata: dict[str, Any] | None) -> list[tuple[str, int]]:
    """Return the ``(submission_id, submission_member_id)`` pairs a member carries."""
    raw = (metadata or {}).get(SUBMISSION_LINKS_KEY)
    if not isinstance(raw, list):
        return []
    links: list[tuple[str, int]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        submission_id = entry.get("submission_id")
        member_id = entry.get("submission_member_id")
        if isinstance(submission_id, str) and isinstance(member_id, int):
            links.append((submission_id, member_id))
    return links


def submission_link_metadata(submission_id: str, submission_member_id: int) -> dict[str, Any]:
    """Render one submission linkage as ``source_metadata`` for an append."""
    return {
        SUBMISSION_LINKS_KEY: [
            {"submission_id": submission_id, "submission_member_id": submission_member_id}
        ]
    }


def merge_member_source_metadata(
    session: Session,
    member: BundleMember,
    incoming: dict[str, Any] | None,
) -> bool:
    """Fold an idempotent append's metadata into the member row it landed on.

    Submission links accumulate; every other key is first-writer-wins, because
    the row's other metadata (``source_path_bytes_hex``) describes the source
    the row was created from and a later producer's view of it is not better
    information. Returns whether anything changed.
    """
    if not incoming:
        return False
    merged = dict(member.source_metadata or {})
    changed = False
    for key, value in incoming.items():
        if key == SUBMISSION_LINKS_KEY:
            existing = submission_links(merged)
            added = [
                entry
                for entry in submission_links({SUBMISSION_LINKS_KEY: value})
                if entry not in existing
            ]
            if not added:
                continue
            merged[key] = [
                {"submission_id": submission_id, "submission_member_id": member_id}
                for submission_id, member_id in [*existing, *added]
            ]
            changed = True
        elif key not in merged:
            merged[key] = value
            changed = True
    if changed:
        # Reassigned, not mutated in place: the JSON column is not a
        # MutableDict, so an in-place edit would never reach the database.
        member.source_metadata = merged
        session.flush()
    return changed


def _find_open_accumulator(session: Session, fingerprint: str) -> Bundle | None:
    return session.scalars(
        select(Bundle)
        .where(
            Bundle.bundle_group == fingerprint,
            Bundle.status == "open",
            # Only true accumulators are adoptable. Externally-identified
            # funnels (cloud-blob, quarantine, include-alone, per-submission
            # bundles) carry archive_id from creation and must never absorb
            # ordinary archive enqueues.
            Bundle.archive_id.is_(None),
        )
        .order_by(Bundle.opened_at, Bundle.id)
    ).first()


def get_or_create_open_bundle(
    session: Session,
    *,
    artifactclass: str,
    policy: ArtifactClassPolicyRecord,
    bundle_id: str | None = None,
    now: dt.datetime | None = None,
) -> tuple[Bundle, bool]:
    """Return the durable open accumulator for an artifactclass's bundle group."""
    fingerprint, basis = compute_bundle_group(session, artifactclass)
    existing = _find_open_accumulator(session, fingerprint)
    if existing is not None:
        _assert_open_witness(existing)
        return existing, False

    target_bytes, max_age_seconds = effective_group_thresholds(
        session,
        artifactclass=artifactclass,
        policy=policy,
        fingerprint=fingerprint,
        basis=basis,
    )
    bundle = _new_bundle(
        fingerprint=fingerprint,
        basis=basis,
        target_bytes=target_bytes,
        max_age_seconds=max_age_seconds,
        bundle_id=bundle_id,
        now=now,
    )
    try:
        with session.begin_nested():
            session.add(bundle)
            session.flush()
    except IntegrityError as exc:
        # Check-then-insert race on the one-open-accumulator partial index:
        # the loser adopts the winner's bundle. Bounded (single re-read);
        # a raw IntegrityError never escapes.
        _discard_pending(session, bundle)
        winner = _find_open_accumulator(session, fingerprint)
        if winner is None:
            raise BundleStateError(
                f"could not open accumulator for bundle group {fingerprint!r}: {exc.orig}"
            ) from exc
        return winner, False
    return bundle, True


def _new_bundle(
    *,
    fingerprint: str,
    basis: list[dict[str, Any]],
    target_bytes: int,
    max_age_seconds: int,
    bundle_id: str | None,
    now: dt.datetime | None,
) -> Bundle:
    resolved_id = bundle_id or f"bundle-{uuid.uuid4().hex}"
    document = group_basis_document(
        basis,
        basis_source=BASIS_SOURCE_DERIVED,
        target_bytes=target_bytes,
        max_age_seconds=max_age_seconds,
    )
    bundle = Bundle(
        id=resolved_id,
        bundle_group=fingerprint,
        group_basis=document,
        status="open",
        target_bytes=target_bytes,
        max_age_seconds=max_age_seconds,
        opened_at=now or dt.datetime.now(dt.UTC),
    )
    _assert_open_witness(bundle)
    return bundle


def _assert_open_witness(bundle: Bundle) -> None:
    """Open-time assert: the typed threshold columns equal the basis witness."""
    effective = (bundle.group_basis or {}).get("effective") or {}
    if (
        effective.get("target_bytes") != bundle.target_bytes
        or effective.get("max_age_seconds") != bundle.max_age_seconds
    ):
        raise BundleStateError(
            f"bundle {bundle.id!r} typed thresholds "
            f"({bundle.target_bytes}, {bundle.max_age_seconds}) do not equal "
            f"the group_basis witness {effective!r}"
        )


def _discard_pending(session: Session, instance: object) -> None:
    """Detach an instance whose savepoint-scoped insert was rolled back."""
    # Already-detached (by the savepoint rollback) is fine.
    with contextlib.suppress(InvalidRequestError):
        session.expunge(instance)


def enqueue_artifact(
    session: Session,
    *,
    artifactclass: str,
    policy: ArtifactClassPolicyRecord,
    logical_asset_hash: bytes,
    source_path: Path | str,
    member_path: str | None = None,
    member_path_is_escaped: bool = False,
    bundle_id: str | None = None,
    now: dt.datetime | None = None,
    source_metadata: dict[str, Any] | None = None,
    size_bytes: int | None = None,
    file_sha256: bytes | None = None,
) -> tuple[Bundle, BundleMember, bool]:
    """Add one asset to the open accumulator for ``artifactclass``'s group.

    A member whose size meets or exceeds the group's effective ``target_bytes``
    is routed include-alone: a fresh funnel-style bundle of its own, minted
    non-adoptable (``archive_id`` set at creation, so it never collides with
    the one-open-accumulator index) and immediately due for flush. The group
    accumulator is untouched by an include-alone routing.

    ``size_bytes``/``file_sha256`` let a caller that has *just* measured the
    source hand those facts in rather than have them re-derived — the
    submission convergence path verifies every member's digest against the
    frozen manifest immediately before appending, and re-hashing there would
    read the whole submission a second time for no new information. Omitted,
    both are measured here as before.
    """
    _require_asset(session, logical_asset_hash)
    source = Path(source_path)
    if size_bytes is None:
        size_bytes = source.stat().st_size
    if member_path is None:
        path_in_bundle = escape_path_name(source)
    elif member_path_is_escaped:
        path_in_bundle = member_path
    else:
        path_in_bundle = escape_path_text(member_path)
    source_path_text = str(source)
    stored_source_path: str | None = source_path_text
    metadata = dict(source_metadata or {})
    try:
        source_path_text.encode("utf-8")
    except UnicodeEncodeError:
        metadata["source_path_bytes_hex"] = os.fsencode(source).hex()
        stored_source_path = None

    fingerprint, basis = compute_bundle_group(session, artifactclass)
    target_bytes, max_age_seconds = effective_group_thresholds(
        session,
        artifactclass=artifactclass,
        policy=policy,
        fingerprint=fingerprint,
        basis=basis,
    )
    if size_bytes >= target_bytes:
        # Crash-retry idempotency: a re-enqueue of the same oversized member
        # while its include-alone funnel is still open lands on its own row.
        existing = session.execute(
            select(Bundle, BundleMember)
            .join(BundleMember, BundleMember.bundle_id == Bundle.id)
            .where(
                Bundle.bundle_group == fingerprint,
                Bundle.status == "open",
                Bundle.archive_id.is_not(None),
                BundleMember.member_path == path_in_bundle,
                BundleMember.artifactclass == artifactclass,
                BundleMember.logical_asset_hash == logical_asset_hash,
            )
            .order_by(Bundle.opened_at, Bundle.id)
        ).first()
        if existing is not None:
            # Fall through to `add_bundle_member` rather than returning here:
            # its idempotency rung lands on this same row and it is the one
            # place that folds the incoming metadata in, so the funnel probe
            # and the accumulator path cannot drift apart on the linkage.
            bundle = existing[0]
            return _append(
                session,
                bundle=bundle,
                artifactclass=artifactclass,
                logical_asset_hash=logical_asset_hash,
                member_path=path_in_bundle,
                source_path=stored_source_path,
                size_bytes=size_bytes,
                file_sha256=file_sha256,
                source=source,
                metadata=metadata,
            )
        bundle = _new_bundle(
            fingerprint=fingerprint,
            basis=basis,
            target_bytes=target_bytes,
            max_age_seconds=max_age_seconds,
            bundle_id=bundle_id,
            now=now,
        )
        bundle.archive_id = f"archive-{bundle.id}"
        minted_id = bundle.id
        try:
            with session.begin_nested():
                session.add(bundle)
                session.flush()
        except IntegrityError as exc:
            # F8: the funnel mint gets the same savepoint guard as the other
            # two surfaces — §3's "raw IntegrityError never escapes" holds
            # unqualified. The only unique surface here is the bundle id
            # (explicit bundle_id on a crash-retry); adopt the existing open
            # funnel, bounded to one re-read.
            _discard_pending(session, bundle)
            winner = session.get(Bundle, minted_id)
            if (
                winner is None
                or winner.status != "open"
                or winner.bundle_group != fingerprint
                or winner.archive_id is None
            ):
                raise BundleStateError(
                    f"could not mint include-alone funnel {minted_id!r} for bundle "
                    f"group {fingerprint!r}: {exc.orig}"
                ) from exc
            bundle = winner
    else:
        bundle, _ = get_or_create_open_bundle(
            session,
            artifactclass=artifactclass,
            policy=policy,
            bundle_id=bundle_id,
            now=now,
        )
    return _append(
        session,
        bundle=bundle,
        artifactclass=artifactclass,
        logical_asset_hash=logical_asset_hash,
        member_path=path_in_bundle,
        source_path=stored_source_path,
        size_bytes=size_bytes,
        file_sha256=file_sha256,
        source=source,
        metadata=metadata,
    )


def _append(
    session: Session,
    *,
    bundle: Bundle,
    artifactclass: str,
    logical_asset_hash: bytes,
    member_path: str,
    source_path: str | None,
    size_bytes: int,
    file_sha256: bytes | None,
    source: Path,
    metadata: dict[str, Any],
) -> tuple[Bundle, BundleMember, bool]:
    """The single append funnel both enqueue routings end in."""
    member, created = add_bundle_member(
        session,
        bundle=bundle,
        artifactclass=artifactclass,
        logical_asset_hash=logical_asset_hash,
        member_path=member_path,
        source_path=source_path,
        size_bytes=size_bytes,
        file_sha256=_sha256_file(source) if file_sha256 is None else file_sha256,
        source_metadata=metadata or None,
    )
    return bundle, member, created


def bundle_due(
    bundle: Bundle,
    *,
    now: dt.datetime | None = None,
    force: bool = False,
) -> bool:
    """Return whether an open bundle should be flushed.

    The age arm reads ``opened_at`` through ``_as_utc``. SQLite does not store
    the offset behind ``DateTime(timezone=True)``, so a bundle *re-read* from
    the catalog comes back naive while a freshly-constructed one is aware —
    and subtracting the two raises. Every caller until the sweeper compared a
    bundle it had just built in the same session, which is why this never
    surfaced; the first production caller of the age arm would have hit it on
    its first pass.
    """
    if bundle.status != "open":
        return False
    if force:
        return bundle.member_count > 0
    if bundle.member_count == 0:
        return False
    if bundle.target_bytes and bundle.total_bytes >= bundle.target_bytes:
        return True
    if not bundle.max_age_seconds:
        return False
    reference = _as_utc(now) or dt.datetime.now(dt.UTC)
    opened_at = _as_utc(bundle.opened_at)
    if opened_at is None:  # pragma: no cover - opened_at is NOT NULL
        return False
    return (reference - opened_at).total_seconds() >= bundle.max_age_seconds


def _as_utc(value: dt.datetime | None) -> dt.datetime | None:
    """Read a catalog timestamp as UTC-aware; naive values are UTC by convention."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def claim_bundle_for_flush(
    session: Session,
    bundle: Bundle,
    *,
    worker_id: str | None = None,
    now: dt.datetime | None = None,
) -> str:
    """Claim one open bundle for flushing and return the claim token.

    The guarded ``open -> flushing`` compare-and-set (design-bundle-groups §4).
    SQLite is the default dialect, where ``FOR UPDATE`` compiles to nothing, so
    the race is closed by rowcount on a conditional ``UPDATE``, never a row
    lock. The caller runs this **ahead of the member load**: claiming after the
    snapshot would let an appender slip a member into a sealing bundle, where
    it would sit unmaterialised.

    The claim, the ``archive_id`` assignment, and everything up to the first
    physical write belong to one transaction — the caller's. Any
    pre-physical-write failure rolls back, and that rollback IS the un-claim:
    the bundle returns to ``open``, visible again to ``bundle_due``, the drain
    check, and the one-open-accumulator partial index.
    """
    token = worker_id or default_worker_id()
    stamp = now or dt.datetime.now(dt.UTC)
    result = session.execute(
        update(Bundle)
        .where(Bundle.id == bundle.id, Bundle.status == "open")
        .values(status="flushing", claimed_by=token, flushed_at=stamp)
    )
    if result.rowcount != 1:
        session.expire(bundle, ["status", "claimed_by", "flushed_at"])
        raise BundleClaimLost(
            f"bundle {bundle.id!r} could not be claimed for flush; "
            f"status is {bundle.status!r}, not 'open'"
        )
    session.expire(bundle, ["status", "claimed_by", "flushed_at"])
    return token


def close_bundle(session: Session, bundle: Bundle, *, claim_token: str) -> Bundle:
    """Seal a claimed bundle after all copy materialisations are verified.

    A guarded compare-and-set on ``status = 'flushing' AND claimed_by = :token``
    (design-bundle-groups §4). A flusher whose claim the reaper already took
    back fails here rather than sealing a member set that is not on media —
    the reaped bundle may have been re-claimed and rebuilt meanwhile, and its
    member set is no longer the one this flusher wrote.

    ``claimed_by`` is deliberately left in place on the sealed row: it is the
    evidence of who sealed it, and the reaper only ever looks at ``flushing``.
    """
    sealed_at = dt.datetime.now(dt.UTC)
    result = session.execute(
        update(Bundle)
        .where(
            Bundle.id == bundle.id,
            Bundle.status == "flushing",
            Bundle.claimed_by == claim_token,
        )
        .values(status="sealed", sealed_at=sealed_at)
    )
    if result.rowcount != 1:
        session.expire(bundle, ["status", "claimed_by", "sealed_at"])
        raise BundleClaimLost(
            f"bundle {bundle.id!r} lost its flush claim {claim_token!r} before seal "
            f"(status={bundle.status!r}, claimed_by={bundle.claimed_by!r}); "
            "refusing to seal a member set that may not be on media"
        )
    session.expire(bundle, ["status", "claimed_by", "sealed_at"])
    return bundle


def hold_bundle(
    session: Session,
    bundle: Bundle,
    *,
    summary: dict[str, Any],
) -> Bundle:
    """Mark a bundle as held for human review."""
    bundle.status = "held"
    bundle.held_at = dt.datetime.now(dt.UTC)
    bundle.review_summary = summary
    session.flush()
    return bundle


def tag_member_path(member_path: str, tag: str) -> str:
    """Insert a disambiguation tag into a member path's final segment.

    The tag lands after the first-dot stem of the basename, so tagging
    commutes with suffix-appending transforms (``name.ext`` → ``name.TAG.ext``,
    ``name.ext.zst`` → ``name.TAG.ext.zst``) and the staging-transform chain
    stays linked after a re-key.
    """
    head, slash, base = member_path.rpartition("/")
    stem, dot, rest = base.partition(".")
    tagged = f"{stem}.{tag}.{rest}" if dot else f"{stem}.{tag}"
    return f"{head}{slash}{tagged}"


def extract_member_tag(requested: str, actual: str) -> str | None:
    """Recover the tag that maps ``requested`` onto ``actual``, if any.

    Catalog names are authoritative and are read, never re-derived; this
    reads the recorded name back into the tag the ladder inserted.
    """
    if actual == requested:
        return None
    r_head, _, r_base = requested.rpartition("/")
    a_head, _, a_base = actual.rpartition("/")
    if r_head != a_head:
        return None
    stem, dot, rest = r_base.partition(".")
    prefix = f"{stem}."
    if not a_base.startswith(prefix):
        return None
    remainder = a_base[len(prefix) :]
    if dot:
        suffix = f".{rest}"
        if not remainder.endswith(suffix) or len(remainder) <= len(suffix):
            return None
        return remainder[: -len(suffix)]
    return remainder or None


def _class_slug(artifactclass: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "-", artifactclass) or "class"


def _class_seed(artifactclass: str) -> str:
    """A short hash of the raw class name; injective where the slug is not.

    ``_class_slug`` collapses distinct class names (``photo.raw`` and
    ``photo-raw`` share a slug), so the terminal rung seeds it with the hash
    of the raw name (F5) — two distinct classes can never share a terminal
    rung short of a SHA-256 collision.
    """
    return hashlib.sha256(artifactclass.encode("utf-8")).hexdigest()[:12]


def _name_ladder(requested: str, file_sha256: bytes, artifactclass: str) -> list[str]:
    """The complete deterministic collision ladder for one requested name.

    Rung 0 is the requested name; the following rungs insert progressively
    longer prefixes of the member's own content hash (never arrival order);
    the terminal rung is the full hash plus the seeded class slug, so the
    ladder always ends and every rung is arrival-order-independent.
    """
    digest = file_sha256.hex()
    candidates = [requested]
    candidates.extend(
        tag_member_path(requested, digest[:length]) for length in _NAME_LADDER_PREFIXES
    )
    candidates.append(
        tag_member_path(
            requested,
            f"{digest}.{_class_slug(artifactclass)}-{_class_seed(artifactclass)}",
        )
    )
    return candidates


def _resolve_member_name(
    session: Session,
    *,
    bundle_id: str,
    artifactclass: str,
    requested: str,
    file_sha256: bytes,
    logical_asset_hash: bytes,
) -> tuple[str, BundleMember | None, BundleMember | None]:
    """Run the canonical naming ladder for one requested member name.

    Returns ``(final_path, existing, first_occupant)``: ``existing`` is the
    caller's own row when the idempotency check hits (same class, same
    logical hash) — re-checked at every rung so a crash-retry lands on its
    own row; ``first_occupant`` is the row occupying the requested name when
    disambiguation happened (for the recorded event).
    """
    first_occupant: BundleMember | None = None
    for candidate in _name_ladder(requested, file_sha256, artifactclass):
        row = session.scalars(
            select(BundleMember).where(
                BundleMember.bundle_id == bundle_id,
                BundleMember.member_path == candidate,
            )
        ).one_or_none()
        if row is None:
            return candidate, None, first_occupant
        if (
            row.artifactclass == artifactclass
            and row.logical_asset_hash == logical_asset_hash
        ):
            return candidate, row, first_occupant
        if first_occupant is None:
            first_occupant = row
    # The terminal rung carries the full content hash and the seeded class
    # slug; reaching this line means a SHA-256 collision or a co-resident
    # literal name equal to the terminal rung (see MemberNamingError).
    raise MemberNamingError(
        f"member naming ladder exhausted for {requested!r} in bundle "
        f"{bundle_id!r} (class {artifactclass!r})"
    )


def add_bundle_member(
    session: Session,
    *,
    bundle: Bundle,
    artifactclass: str,
    logical_asset_hash: bytes,
    member_path: str,
    size_bytes: int,
    file_sha256: bytes,
    source_path: str | None = None,
    source_metadata: dict[str, Any] | None = None,
) -> tuple[BundleMember, bool]:
    """Add a logical asset to a bundle through the canonical naming ladder.

    A requested ``(bundle, member_path)`` already present with the same class
    and same logical hash is the idempotent no-op — **except for
    ``source_metadata``, which is folded into the existing row**. The linkage a
    submission append carries is the whole reason the append happened; dropping
    it on the idempotent path silently unlinked every submission whose content
    a co-resident had already enqueued. Any other hit gets a deterministic
    disambiguated name from the collision ladder, recorded as an event. Counter
    bumps are atomic SQL updates; a check-then-insert race on the
    ``(bundle_id, member_path)`` unique surface recovers by re-running the
    ladder against the now-visible winner (bounded, one retry) — a raw
    ``IntegrityError`` never escapes.
    """
    if bundle.status != "open":
        raise BundleStateError(f"bundle {bundle.id!r} is not open")
    _require_asset(session, logical_asset_hash)

    member: BundleMember | None = None
    final_path = member_path
    first_occupant: BundleMember | None = None
    for attempt in range(2):
        final_path, existing, first_occupant = _resolve_member_name(
            session,
            bundle_id=bundle.id,
            artifactclass=artifactclass,
            requested=member_path,
            file_sha256=file_sha256,
            logical_asset_hash=logical_asset_hash,
        )
        if existing is not None:
            merge_member_source_metadata(session, existing, source_metadata)
            return existing, False
        candidate = BundleMember(
            bundle_id=bundle.id,
            logical_asset_hash=logical_asset_hash,
            artifactclass=artifactclass,
            member_path=final_path,
            source_path=source_path,
            size_bytes=size_bytes,
            file_sha256=file_sha256,
            source_metadata=source_metadata,
        )
        try:
            with session.begin_nested():
                session.add(candidate)
                session.flush()
        except IntegrityError as exc:
            _discard_pending(session, candidate)
            if attempt == 0:
                # Naming race loser: re-run the ladder against the winner.
                continue
            raise BundleStateError(
                f"could not add member {member_path!r} to bundle {bundle.id!r}: {exc.orig}"
            ) from exc
        member = candidate
        break
    if member is None:  # pragma: no cover - loop invariant
        raise BundleStateError(f"could not add member {member_path!r} to bundle {bundle.id!r}")

    session.execute(
        update(Bundle)
        .where(Bundle.id == bundle.id)
        .values(
            total_bytes=Bundle.total_bytes + size_bytes,
            member_count=Bundle.member_count + 1,
        )
    )
    session.expire(bundle, ["total_bytes", "member_count"])

    if final_path != member_path:
        emit_structured_event(
            "bundle_member_name_disambiguated",
            bundle_id=bundle.id,
            requested_path=member_path,
            final_path=final_path,
            artifactclass=artifactclass,
            logical_asset_hash=logical_asset_hash.hex(),
            occupant_path=None if first_occupant is None else first_occupant.member_path,
            occupant_artifactclass=(
                None if first_occupant is None else first_occupant.artifactclass
            ),
            occupant_logical_asset_hash=(
                None if first_occupant is None else first_occupant.logical_asset_hash.hex()
            ),
        )
    return member, True


def record_asset_locator(
    session: Session,
    *,
    logical_asset_hash: bytes,
    pool_id: str,
    native_locator: dict[str, Any],
    representation: str,
    copy_id: int,
    bundle_id: str,
    member_path: str | None = None,
) -> AssetLocator:
    """Record a concrete per-copy locator for an asset in a bundle."""
    _require_asset(session, logical_asset_hash)
    if session.get(Pool, pool_id) is None:
        raise UnknownBundlePool(f"no Pool with id {pool_id!r}")
    copy = session.get(Copy, copy_id)
    if copy is None:
        raise UnknownBundleCopy(f"no Copy with id={copy_id}")
    resolved_member_path = member_path or native_locator.get("member_path")
    if not isinstance(resolved_member_path, str) or not resolved_member_path:
        raise AssetLocatorError("asset locator requires a non-empty member_path")
    locator = AssetLocator(
        logical_asset_hash=logical_asset_hash,
        pool_id=pool_id,
        native_locator=native_locator,
        member_path=resolved_member_path,
        representation=representation,
        copy_id=copy_id,
        bundle_id=bundle_id,
    )
    session.add(locator)
    session.flush()
    return locator


def record_blob_root(
    session: Session,
    *,
    bundle_id: str,
    copy_id: int,
    pool_id: str,
    root_path: str,
    native_locator: dict[str, Any],
    archive_id: str | None = None,
) -> BlobRoot:
    """Record a coarse blob-root pointer for single-file restore from blobs."""
    if session.get(Bundle, bundle_id) is None:
        raise BundleStateError(f"no Bundle with id {bundle_id!r}")
    if session.get(Copy, copy_id) is None:
        raise UnknownBundleCopy(f"no Copy with id={copy_id}")
    if session.get(Pool, pool_id) is None:
        raise UnknownBundlePool(f"no Pool with id {pool_id!r}")
    root = BlobRoot(
        bundle_id=bundle_id,
        copy_id=copy_id,
        pool_id=pool_id,
        root_path=root_path,
        native_locator=native_locator,
        archive_id=archive_id,
    )
    session.add(root)
    session.flush()
    return root


def record_staging_transform(
    session: Session,
    *,
    member: BundleMember,
    artifactclass: str,
    step_order: int,
    kind: str,
    reversible: bool,
    original_member_path: str,
    stored_member_path: str,
    original_size_bytes: int,
    stored_size_bytes: int,
    original_sha256: bytes,
    stored_sha256: bytes,
    parameters: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
    is_final: bool = True,
) -> StagingTransform:
    """Record one ordered staging transform for a bundle member."""
    if is_final and stored_member_path != member.member_path:
        raise StagingTransformError(
            f"transform stored path {stored_member_path!r} does not match "
            f"bundle member {member.member_path!r}"
        )
    if step_order == 0 and original_sha256 != member.logical_asset_hash:
        raise StagingTransformError(
            "first transform original_sha256 must match the member logical asset hash"
        )
    if is_final and stored_sha256 != member.file_sha256:
        raise StagingTransformError(
            f"transform stored sha256 for {stored_member_path!r} does not match "
            "bundle member file_sha256"
        )
    transform = StagingTransform(
        bundle_member_id=member.id,
        bundle_id=member.bundle_id,
        logical_asset_hash=member.logical_asset_hash,
        artifactclass=artifactclass,
        step_order=step_order,
        kind=kind,
        reversible=reversible,
        original_member_path=original_member_path,
        stored_member_path=stored_member_path,
        original_size_bytes=original_size_bytes,
        stored_size_bytes=stored_size_bytes,
        original_sha256=original_sha256,
        stored_sha256=stored_sha256,
        parameters=parameters or {},
        result=result or {},
    )
    session.add(transform)
    session.flush()
    return transform


def record_exclusion(
    session: Session,
    *,
    artifactclass: str,
    reason: str,
    bundle_id: str | None = None,
    logical_asset_hash: bytes | None = None,
    path: str | None = None,
    count: int = 1,
    bytes_total: int = 0,
    ruleset_name: str | None = None,
    ruleset_hash: str | None = None,
    detail: dict[str, Any] | None = None,
) -> ExclusionRecord:
    """Record why a candidate was excluded from archive bundling."""
    if logical_asset_hash is not None:
        _require_asset(session, logical_asset_hash)
    exclusion = ExclusionRecord(
        bundle_id=bundle_id,
        artifactclass=artifactclass,
        reason=reason,
        logical_asset_hash=logical_asset_hash,
        path=path,
        count=count,
        bytes_total=bytes_total,
        ruleset_name=ruleset_name,
        ruleset_hash=ruleset_hash,
        detail=detail,
    )
    session.add(exclusion)
    session.flush()
    return exclusion


def record_review_decision(
    session: Session,
    *,
    bundle_id: str,
    action: str,
    scope: str,
    subtree: str | None = None,
    reason: str | None = None,
    reviewer: str | None = None,
    persisted_rule: dict[str, Any] | None = None,
) -> ReviewDecision:
    """Record a held-bundle human review decision."""
    bundle = session.get(Bundle, bundle_id)
    if bundle is None:
        raise BundleStateError(f"no Bundle with id {bundle_id!r}")
    decision = ReviewDecision(
        bundle_id=bundle_id,
        action=action,
        scope=scope,
        subtree=subtree,
        reason=reason,
        reviewer=reviewer,
        persisted_rule=persisted_rule,
    )
    session.add(decision)
    session.flush()
    return decision


def bundle_artifactclasses(session: Session, bundle: Bundle) -> list[str]:
    """Return the sorted distinct member classes of a bundle (member grain, §5).

    A bundle may hold several classes; every §5 consumer that used to read the
    dropped ``bundle.artifactclass`` column reads the member rows instead. A
    memberless funnel bundle (e.g. cloud-blob before its wrap lands) falls
    back to the classes whose policy projection derives this bundle's group —
    identical pool sets by fingerprint construction.
    """
    classes = sorted(
        set(
            session.scalars(
                select(BundleMember.artifactclass).where(
                    BundleMember.bundle_id == bundle.id
                )
            )
        )
    )
    if classes:
        return classes
    return sorted(
        set(
            session.scalars(
                select(ArtifactClassPolicyRecord.artifactclass).where(
                    ArtifactClassPolicyRecord.bundle_group == bundle.bundle_group
                )
            )
        )
    )


def _require_asset(session: Session, logical_asset_hash: bytes) -> LogicalAsset:
    if not is_content_hash(logical_asset_hash):
        raise ValueError("logical_asset_hash must be a 32-byte SHA-256 hash")
    asset = session.get(LogicalAsset, logical_asset_hash)
    if asset is None:
        raise UnknownBundleAsset(f"no LogicalAsset with content hash {logical_asset_hash.hex()}")
    return asset


def _sha256_file(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()
