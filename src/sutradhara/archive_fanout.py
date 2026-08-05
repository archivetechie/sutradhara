"""RAO archive bundle flush and fan-out orchestration.

Sutradhara owns policy, accumulator state, fan-out, and catalog records. The
archive mechanics are delegated through ``ArchiveBuilder``: remanence implements
the canonical RAO builder, while tests can inject an in-process deterministic
builder. d2 copies are materialized as ordinary tar files here because they are
the rem-independent shelf copy.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import logging
import os
import re
import tarfile
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import event, select, update
from sqlalchemy.orm import Session, noload

from sutradhara.archive_bundle import (
    claim_bundle_for_flush,
    close_bundle,
    record_asset_locator,
    record_blob_root,
    record_exclusion,
    submission_links,
)
from sutradhara.archive_restore import ArchiveRestoreError, member_byte_base, read_member_bytes
from sutradhara.arrangement import SourceMapEntry, render_source_map
from sutradhara.backend.port import BackendError, ByteRange, VerifyResult
from sutradhara.catalog.copies import add_bundle_copy
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    Bundle,
    BundleMember,
    Copy,
    StagingTransform,
    SubmissionMember,
)
from sutradhara.catalog.types import CopyHealth, CopySource
from sutradhara.evidence_recorder import record_measured, record_unmeasured_promotion
from sutradhara.hdcache.fill import enqueue_post_flush_hdcache_fills
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    CONDITION_BLOCKED,
    OBSERVED_MISSING,
    record_condition,
    record_observation,
)
from sutradhara.jobs.runtime_observations import report_tape_locator
from sutradhara.keys import KEY_DOMAIN_ARCHIVE, KeyRegistry, assert_key_epoch_domain
from sutradhara.rem_archive_cli import (
    recipient_registry_ids,
    resolve_rem_bin,
    run_rem_archive_build,
)
from sutradhara.replication import (
    PoolTarget,
    WritableStorageBackend,
    bundle_group_targets,
)
from sutradhara.resource_control import run_managed
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RAO_CHUNK_SIZE
from sutradhara.structured_logs import emit_structured_event

LOGGER = logging.getLogger(__name__)
_BUNDLE_COPY_FAST_PATH_KEY = "sutradhara_bundle_copy_fast_path"


class ArchiveFanoutError(Exception):
    """Base class for archive fan-out errors."""


class FlushPreflightShort(ArchiveFanoutError):
    """The flush work directory lacks space for bundle x target materialisation."""


class BundleOversize(ArchiveFanoutError):
    """A single artifact exceeds the configured tape capacity."""


class ManifestSigningError(ArchiveFanoutError):
    """A customer receipt could not be signed with a real keyed signature."""


class TransientPoolFanoutError(BackendError, ArchiveFanoutError):
    """A target pool failed with a retryable backend transport error."""

    def __init__(self, pool_id: str, backend_name: str, cause: BaseException) -> None:
        self.pool_id = pool_id
        self.backend_name = backend_name
        self.cause = cause
        super().__init__(
            f"transient backend failure for pool {pool_id!r} on backend {backend_name!r}: {cause}"
        )


class PoolWriteFailure(ArchiveFanoutError):
    """A target pool failed **after** its bytes reached media, non-transiently.

    The readback verify refused the object, or a post-write catalog step
    rejected it. Distinct from ``TransientPoolFanoutError`` in both directions:
    it is not retried on a backoff (a tape append is unreclaimable, so an
    automatic retry appends a *second* object rather than replacing the first),
    and it does not abort the flush — the design's "post-write failures seal
    partial rather than roll back" applies to every post-write failure, not
    only the retryable ones. Raising it out of the fan-out would roll back the
    sibling targets whose copies are already on media.
    """

    def __init__(self, pool_id: str, backend_name: str, cause: BaseException) -> None:
        self.pool_id = pool_id
        self.backend_name = backend_name
        self.cause = cause
        super().__init__(
            f"post-write failure for pool {pool_id!r} on backend {backend_name!r}: {cause}"
        )


@dataclass(frozen=True)
class MemberInput:
    """One source member sent to an archive builder.

    ``ingest_item_id`` is the submission lineage the map's last column carries,
    resolved once at flush time from the recorded submission linkage. ``None``
    means no lineage — the routine intake-accumulator case — and renders as the
    empty string, never the literal ``"None"``.
    """

    logical_asset_hash: bytes
    member_path: str
    source_path: Path
    size_bytes: int
    file_sha256: bytes
    ingest_item_id: int | None = None


@dataclass(frozen=True)
class DeviationCluster:
    """Clustered conformance-scan deviation summary."""

    prefix: str
    reason: str
    count: int
    bytes_total: int = 0
    samples: tuple[str, ...] = ()
    proposed_default: str | None = None


@dataclass(frozen=True)
class ConformanceScan:
    """Conformance scan output consumed by the expect gate."""

    clusters: tuple[DeviationCluster, ...] = ()
    exclusions: tuple[DeviationCluster, ...] = ()

    @property
    def has_deviations(self) -> bool:
        # Mirror rem's is_noncompliant_reason (archive_ingest.rs): native
        # entries, rule-driven exclusions, and rule-driven blob wraps are
        # compliant informational clusters, not deviations — only reasons
        # outside that set hold a compliant-expect bundle.
        compliant = {"native", "exclude-rule", "blob-rule"}
        return any(c.reason not in compliant for c in self.clusters) or bool(
            self.exclusions
        )

    def to_summary(self) -> dict[str, Any]:
        return {
            "clusters": [_cluster_json(cluster) for cluster in self.clusters],
            "exclusions": [_cluster_json(cluster) for cluster in self.exclusions],
        }


@dataclass(frozen=True)
class BuiltMember:
    """A built member locator emitted by an archive builder."""

    logical_asset_hash: bytes
    member_path: str
    size_bytes: int
    file_sha256: bytes
    native_locator: dict[str, Any]
    ingest_item_id: str | None = None


@dataclass(frozen=True)
class _ReadCopyView:
    """Copy-shaped view used before fan-out rows are committed."""

    native_locator: dict[str, Any]
    storage_metadata: dict[str, Any]
    id: int | None = None


@dataclass(frozen=True)
class _ReadAssetLocatorView:
    """AssetLocator-shaped view over a built member locator."""

    representation: str
    native_locator: dict[str, Any]
    member_path: str


@dataclass(frozen=True)
class BuiltBlobRoot:
    """A coarse blob-root locator emitted by an archive builder."""

    root_path: str
    native_locator: dict[str, Any]


@dataclass(frozen=True)
class BuiltExclusion:
    """An exclusion emitted by an archive builder."""

    path: str
    reason: str
    count: int = 1
    bytes_total: int = 0
    detail: dict[str, Any] | None = None


@dataclass(frozen=True)
class BuildArtifact:
    """One built archive object ready to write to a pool."""

    artifact_path: Path
    stored_digest: bytes
    members: tuple[BuiltMember, ...]
    manifest_path: Path | None = None
    blob_roots: tuple[BuiltBlobRoot, ...] = ()
    exclusions: tuple[BuiltExclusion, ...] = ()
    recipient_epochs: tuple[str, ...] = ()


class ArchiveBuilder(Protocol):
    """Archive builder boundary owned by remanence in production.

    Conformance scanning is not part of this boundary: scanning happens at
    enqueue-batch time, per (artifactclass, source tree root) — see
    ``sutradhara.archive_enqueue``. The build consumes a flush-rendered map.
    """

    def build(
        self,
        *,
        bundle: Bundle,
        members: Sequence[MemberInput],
        representation: Representation,
        ruleset: str,
        key_epoch: str | None,
        work_dir: Path,
        map_path: Path | None = None,
        source_root: Path | None = None,
        map_sha256: str | None = None,
    ) -> BuildArtifact:
        """Build one archive object for a pool representation."""
        ...


class ManifestSigner(Protocol):
    """Keyed signer for customer-facing archive receipts."""

    def sign(self, payload: Mapping[str, Any]) -> dict[str, str]:
        """Return a detached signature over the canonical payload."""
        ...


@dataclass(frozen=True)
class HmacManifestSigner:
    """HMAC-SHA256 signer for customer manifest receipts."""

    key: bytes
    key_id: str

    def __post_init__(self) -> None:
        if not self.key:
            raise ManifestSigningError("manifest signing key must not be empty")
        if not self.key_id:
            raise ManifestSigningError("manifest signing key_id must not be empty")

    def sign(self, payload: Mapping[str, Any]) -> dict[str, str]:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return {
            "algorithm": "hmac-sha256",
            "key_id": self.key_id,
            "digest": hmac.new(self.key, canonical, hashlib.sha256).hexdigest(),
        }


@dataclass(frozen=True)
class FanoutResult:
    """Summary of a bundle fan-out that reached sealed state."""

    bundle_id: str
    copy_ids: tuple[int, ...]
    manifest_path: str | None
    partial: bool = False
    failed_pools: tuple[str, ...] = ()
    condition_reason: str | None = None
    condition_message: str | None = None



class LocalArchiveBuilder:
    """Deterministic archive builder for tests and local dry-runs.

    The object format is intentionally simple and self-describing:
    ``8-byte header length`` + JSON header + concatenated member bytes. It is
    not RAO; production callers should use ``RemArchiveBuilder``.

    WARNING: this builder reads member bytes from ``members`` directly and
    IGNORES ``map_path``/``source_root``/``map_sha256`` — a map defect cannot
    fail a LocalArchiveBuilder test. Map-route coverage must execute through
    ``RemArchiveBuilder`` with the process boundary mocked (the twice-flagged
    ships-green hazard).
    """

    _TEST_RECOVERY_EPOCH = (
        "recovery-" + hashlib.sha256(b"sutradhara-local-archive-builder-recovery").hexdigest()[:32]
    )

    def build(
        self,
        *,
        bundle: Bundle,
        members: Sequence[MemberInput],
        representation: Representation,
        ruleset: str,
        key_epoch: str | None,
        work_dir: Path,
        map_path: Path | None = None,
        source_root: Path | None = None,
        map_sha256: str | None = None,
    ) -> BuildArtifact:
        archive_path = work_dir / f"{bundle.id}-{representation.value}.sra"
        manifest_path = work_dir / f"{bundle.id}-{representation.value}.manifest.json"
        payload = bytearray()
        built_members: list[BuiltMember] = []
        for member in members:
            data = member.source_path.read_bytes()
            offset = len(payload)
            payload.extend(data)
            built_members.append(
                BuiltMember(
                    logical_asset_hash=member.logical_asset_hash,
                    member_path=member.member_path,
                    size_bytes=len(data),
                    file_sha256=hashlib.sha256(data).digest(),
                    native_locator={
                        "member_path": member.member_path,
                        "offset": offset,
                        "size_bytes": len(data),
                    },
                )
            )
        header = {
            "format": "sutradhara-local-archive-v1",
            "bundle_id": bundle.id,
            "representation": representation.value,
            "ruleset": ruleset,
            "members": [
                {
                    "path": member.member_path,
                    "sha256": member.file_sha256.hex(),
                    "size_bytes": member.size_bytes,
                    **member.native_locator,
                }
                for member in built_members
            ],
        }
        header_bytes = json.dumps(header, sort_keys=True).encode("utf-8")
        archive_path.write_bytes(len(header_bytes).to_bytes(8, "big") + header_bytes + payload)
        manifest_path.write_text(json.dumps(header, sort_keys=True, indent=2) + "\n")
        return BuildArtifact(
            artifact_path=archive_path,
            stored_digest=hashlib.sha256(archive_path.read_bytes()).digest(),
            members=tuple(built_members),
            manifest_path=manifest_path,
            recipient_epochs=(key_epoch, self._TEST_RECOVERY_EPOCH)
            if representation is Representation.RAO_AEAD_V1 and key_epoch is not None
            else (),
        )


class RemArchiveBuilder:
    """Subprocess adapter for ``rem archive build``.

    The command is deliberately thin: sutradhara passes a flush-rendered
    catalog map, then consumes the manifest emitted by rem. The exact rem
    manifest shape is normalized permissively so tests can cover the sutradhara
    side without depending on rem internals.

    Builds go by ``--map`` only (design-bundle-groups §4): on the ``--inputs``
    route rem derives member names from the filesystem itself and hard-errors
    on duplicate basenames — sutradhara owns member names only on the map
    route, and the retired segment-counting root derivation
    (``_rem_input_paths``) must not come back.
    """

    def __init__(
        self,
        rem_bin: str | Path | None = None,
        *,
        keys: KeyRegistry | None = None,
    ) -> None:
        self._rem_bin = None if rem_bin is None else str(rem_bin)
        self._keys = keys or KeyRegistry()

    def build(
        self,
        *,
        bundle: Bundle,
        members: Sequence[MemberInput],
        representation: Representation,
        ruleset: str,
        key_epoch: str | None,
        work_dir: Path,
        map_path: Path | None = None,
        source_root: Path | None = None,
        map_sha256: str | None = None,
    ) -> BuildArtifact:
        output_path = work_dir / f"{bundle.id}-{representation.value}.rao"
        manifest_path = work_dir / f"{bundle.id}-{representation.value}.manifest.json"
        expected_recipient_epochs: tuple[str, ...] = ()
        if map_path is None:
            raise ArchiveFanoutError(
                "RemArchiveBuilder builds by map only; the --inputs route is retired"
            )
        if source_root is None:
            raise ArchiveFanoutError("map archive build requires source_root")
        if representation is Representation.RAO_AEAD_V1:
            if key_epoch is None:
                raise ArchiveFanoutError("encrypted RAO archive build requires key_epoch")
            try:
                assert_key_epoch_domain(
                    key_epoch,
                    KEY_DOMAIN_ARCHIVE,
                    context=f"pool sealing for bundle {bundle.id}",
                )
            except ValueError as exc:
                raise ArchiveFanoutError(str(exc)) from exc
            recipients = self._keys.recipients_for_seal(
                key_epoch,
                domain=KEY_DOMAIN_ARCHIVE,
            )
            expected_recipient_epochs = tuple(epoch.key_id for epoch in recipients)
            result = run_rem_archive_build(
                inputs=None,
                ruleset=None,
                map_path=map_path,
                source_root=source_root,
                map_sha256=map_sha256,
                output_path=output_path,
                manifest_path=manifest_path,
                rem_bin=self._rem_bin,
                recipients=tuple(self._keys.public_key_path(epoch.key_id) for epoch in recipients),
                failure_label="rem archive build",
            )
        else:
            result = run_rem_archive_build(
                inputs=None,
                ruleset=None,
                map_path=map_path,
                source_root=source_root,
                map_sha256=map_sha256,
                output_path=output_path,
                manifest_path=manifest_path,
                rem_bin=self._rem_bin,
                failure_label="rem archive build",
            )
        manifest = _normalized_rem_build_report(result.stdout_report)
        recipient_epochs = (
            recipient_registry_ids(result.stdout_report, failure_label="rem archive build")
            if representation is Representation.RAO_AEAD_V1
            else ()
        )
        if (
            representation is Representation.RAO_AEAD_V1
            and recipient_epochs != expected_recipient_epochs
        ):
            raise ArchiveFanoutError(
                "rem archive build recipient epochs differ from registry selection"
            )
        return BuildArtifact(
            artifact_path=output_path,
            stored_digest=result.stored_digest,
            members=tuple(_members_from_manifest(manifest, members)),
            manifest_path=manifest_path,
            blob_roots=tuple(_blob_roots_from_manifest(manifest)),
            exclusions=tuple(_exclusions_from_manifest(manifest)),
            recipient_epochs=recipient_epochs,
        )

    def verify_member_copy(
        self,
        *,
        backend: WritableStorageBackend,
        copy_locator: dict[str, Any],
        member: BuiltMember,
        representation: Representation,
        storage_metadata: Mapping[str, Any],
        work_dir: Path,
    ) -> bytes:
        """Extract one member from the stored copy through rem for verification."""
        if representation not in {Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1}:
            raise ArchiveFanoutError(
                f"RemArchiveBuilder cannot verify representation {representation.value!r}"
            )
        object_path = (
            work_dir / f"verify-{hashlib.sha256(member.member_path.encode()).hexdigest()}.rao"
        )
        _materialize_copy_to_path(backend, copy_locator, storage_metadata, object_path)
        verify_id = hashlib.sha256(member.member_path.encode() + b"\0" + member.file_sha256)
        dest = work_dir / f"verify-out-{verify_id.hexdigest()}"
        dest.mkdir()
        cmd = [
            resolve_rem_bin(self._rem_bin),
            "archive",
            "extract",
            "--object",
            str(object_path),
            "--dest",
            str(dest),
            "--path",
            member.member_path,
            "--first-chunk-lba",
            str(_member_first_chunk_lba(member.native_locator)),
            "--file-size-bytes",
            str(member.size_bytes),
            "--range",
            f"0:{member.size_bytes}",
            "--overwrite",
        ]
        if representation is Representation.RAO_PLAIN_V1:
            cmd.extend(["--chunk-size", str(RAO_CHUNK_SIZE)])
            _run_rem(cmd)
        else:
            recipient_epochs = _metadata_recipient_epochs(storage_metadata)
            try:
                selected = self._keys.select_private_epoch(
                    recipient_epochs,
                    domain=KEY_DOMAIN_ARCHIVE,
                )
            except (KeyError, ValueError) as exc:
                raise ArchiveFanoutError(str(exc)) from exc
            with self._keys.materialized_private_key(selected.key_id) as key_file:
                cmd.extend(["--private-key", str(key_file)])
                _run_rem(cmd)
        return _single_restored_member(dest, member.member_path)


def flush_bundle(
    session: Session,
    *,
    bundle_id: str,
    backends: Mapping[int, WritableStorageBackend],
    builder: ArchiveBuilder,
    key_epoch: str | None = None,
    deliverables_dir: Path | str | None = None,
    manifest_signer: ManifestSigner | None = None,
    tape_capacity_bytes: int | None = None,
) -> FanoutResult:
    """Flush one open bundle, build each pool copy, and record catalog state.

    Every flush renders its build map from the bundle's own member rows (the
    flush-time catalog map of design-bundle-groups §4) and builds by map with
    ``--source-root /``. The map's digest column carries the catalog
    ``file_sha256`` of the staged bytes; rem's writer verifies the streamed
    payload against it, which IS the pre-write content check — no separate
    re-hash pass exists here by design.

    A rem failure that names a member (the map line for missing-source and
    size-drift, the archive path for the writer digest refusal) is caught
    inside this claim: the member is quarantined into a held funnel bundle,
    the map is re-rendered without it, and the build retries — an internal
    loop, never a re-entry into ``flush_bundle``. Non-member failures
    propagate as before.
    """
    if deliverables_dir is not None and manifest_signer is None:
        raise ManifestSigningError("deliverables_dir requires a manifest_signer")
    # noload: the member snapshot must be read AFTER the claim, never before.
    # Bundle.members is lazy="selectin", so an ordinary load would take the
    # very snapshot the claim exists to serialise.
    bundle = (
        session.scalars(
            select(Bundle).options(noload(Bundle.members)).where(Bundle.id == bundle_id)
        )
        .unique()
        .one()
    )
    if bundle.status != "open":
        raise ArchiveFanoutError(f"bundle {bundle.id!r} is not open")
    if bundle.member_count == 0:
        raise ArchiveFanoutError(f"bundle {bundle.id!r} has no members")

    # Scanning does not happen here at all: the member-grain scan contract
    # lives at enqueue-batch time (``sutradhara.archive_enqueue``), per
    # (class, source tree root) — ``--map`` conflicts with ``--rules``, and a
    # single-file scan would match rules against the bare basename.
    #
    # Fan-out targets come from the bundle's frozen group_basis, in basis
    # order (§2/§5) — never from a member class's live policy and never from a
    # representative-class hop.
    targets = bundle_group_targets(session, bundle, backends, key_epoch=key_epoch)
    _require_key_epoch(targets)

    with tempfile.TemporaryDirectory(prefix=f"sutradhara-bundle-{bundle.id}-") as raw:
        work_dir = Path(raw)
        # §8 preflight: the flush materialises the bundle once per pool target.
        # Skip-and-alarm BEFORE anything is mutated — the claim, the archive_id
        # mint and the status transition all sit after it, so a short work dir
        # leaves the bundle exactly as it found it (and un-claimed).
        _preflight_flush_work_dir(
            work_dir,
            bundle_id=bundle.id,
            required_bytes=bundle.total_bytes * max(len(targets), 1),
            target_count=len(targets),
        )
        # The claim comes FIRST, ahead of the member load: round 3 showed a
        # claim taken after the snapshot lets an appender slip a member into a
        # sealing bundle, silently unmaterialised. Everything from here to the
        # first physical write is one transaction (the caller's), so a
        # pre-write failure rolls back and the rollback IS the un-claim.
        claim_token = claim_bundle_for_flush(session, bundle)
        if bundle.archive_id is None:
            bundle.archive_id = f"archive-{bundle.id}"
        # The claim is taken; ``bundle.members`` may now be loaded normally.
        # The flush's own snapshot is ``_member_rows_for_flush``; this
        # collection serves the consumers that want *current* membership —
        # the customer manifest and the build-exclusion recorder, both of
        # which must see a quarantine move.
        session.expire(bundle, ["members"])

        quarantine: Bundle | None = None
        attempt = 0
        max_attempts: int | None = None
        while True:
            member_rows = _member_rows_for_flush(session, bundle)
            if not member_rows:
                raise ArchiveFanoutError(
                    f"bundle {bundle.id!r} has no members left to flush; "
                    f"all were quarantined"
                )
            if max_attempts is None:
                # Each retry quarantines exactly one member, so the loop is
                # bounded by the claimed snapshot's member count.
                max_attempts = len(member_rows)
            if tape_capacity_bytes is not None:
                for row in member_rows:
                    if row.size_bytes > tape_capacity_bytes:
                        raise BundleOversize(
                            f"member {row.member_path!r} exceeds tape capacity; "
                            "# TODO: oversize split"
                        )
            # C3 (P4): member identity is validated HERE — once, before any
            # physical write, for every representation. See the function's
            # docstring for why this cannot live in the per-target
            # artifact_validator slot.
            validate_submission_member_identity(session, member_rows)
            members = _member_inputs_for_flush(session, member_rows)
            # §8 work-dir hygiene: each quarantine retry gets its own
            # subdirectory. Reusing per-target dirs across attempts leaves an
            # earlier attempt's artifact in place, and a target that built then
            # failed transiently on the backend write would meet its own stale
            # `--out already exists` on the retry.
            attempt_dir = work_dir / f"attempt-{attempt}"
            attempt_dir.mkdir()
            try:
                # Rendering is inside the try because a member whose source
                # path is not UTF-8 encodable is a member verdict, and the
                # quarantine loop below is what moves it out of the way.
                map_text = _render_flush_map(session, members, member_rows=member_rows)
                # Strict: the renderer has already rejected every member the
                # map could not carry, so a surviving surrogate is a hole in
                # that check and must fail loudly rather than reach rem, whose
                # --map loader is strict UTF-8 over the whole file.
                map_bytes = map_text.encode("utf-8")
                map_path = attempt_dir / f"{bundle.id}.map.tsv"
                map_path.write_bytes(map_bytes)
                map_digest = hashlib.sha256(map_bytes).hexdigest()
                bundle.scan_summary = {
                    "mode": "map",
                    "map_sha256": map_digest,
                    "member_count": len(member_rows),
                }
                (
                    copy_ids,
                    transient_failures,
                    write_failures,
                    manifest_receipt,
                ) = _fan_out_targets(
                    session,
                    bundle=bundle,
                    targets=targets,
                    members=members,
                    member_rows=member_rows,
                    builder=builder,
                    key_epoch=key_epoch,
                    work_dir=attempt_dir,
                    map_path=map_path,
                    map_sha256=map_digest,
                    deliverables_dir=deliverables_dir,
                    manifest_signer=manifest_signer,
                )
            except _MemberBuildFailure as failure:
                attempt += 1
                if attempt > max_attempts:  # pragma: no cover - structural bound
                    raise ArchiveFanoutError(
                        f"bundle {bundle.id!r} flush exceeded its quarantine bound"
                    ) from failure
                quarantine = _quarantine_member(
                    session,
                    bundle=bundle,
                    member_row=failure.member_row,
                    quarantine=quarantine,
                    message=failure.message,
                )
                continue
            break

    close_bundle(session, bundle, claim_token=claim_token)
    condition_message: str | None = None
    condition_reason: str | None = None
    if transient_failures:
        condition_message = _record_bundle_copy_transient_backoff(
            session,
            bundle.id,
            transient_failures,
        )
        condition_reason = "transient-backend-failure"
    if write_failures:
        # Recorded second, deliberately: a post-write failure blocks where a
        # transient one backs off, and the more severe verdict must own the
        # condition row. Bytes on media that failed their own check are a
        # human's call, not a retry timer's.
        condition_message = _record_bundle_copy_write_failure(
            session,
            bundle.id,
            write_failures,
        )
        condition_reason = "post-write-pool-failure"
    if not transient_failures and not write_failures:
        _record_bundle_copy_outbox(session, bundle.id)
    _schedule_bundle_copy_fast_path(session, bundle.id)
    enqueue_post_flush_hdcache_fills(session, bundle.id)
    return FanoutResult(
        bundle.id,
        tuple(copy_ids),
        manifest_receipt,
        partial=bool(transient_failures or write_failures),
        failed_pools=tuple(
            failure.pool_id for failure in (*transient_failures, *write_failures)
        ),
        condition_reason=condition_reason,
        condition_message=condition_message,
    )


# The map route requires one containing root and a group bundle's sources are
# scattered by construction (submission data dirs, per-source staging dirs).
# The anchoring contract exists to bound hand-authored maps; a catalog-rendered
# map is not that case — the compensating control is rem's writer verifying
# every streamed payload against the map's digest column. (Design §4.)
_MAP_SOURCE_ROOT = Path("/")

# rem names the map *line* for missing-source and size-drift refusals
# (line 1 is the header, data rows start at line 2)...
_MAP_LINE_FAILURE = re.compile(r"source map line (\d+)")
# ...and the archive path for the writer's streamed-digest refusal
# (remanence-format/src/writer.rs).
_WRITER_DIGEST_FAILURE = re.compile(r"streamed data hash for (.+?) does not match spec")


class _MemberBuildFailure(Exception):
    """Internal: a rem build failure identified one member row as the cause."""

    def __init__(self, member_row: BundleMember, message: str) -> None:
        super().__init__(message)
        self.member_row = member_row
        self.message = message


def _member_rows_for_flush(session: Session, bundle: Bundle) -> list[BundleMember]:
    """Return the bundle's member rows in map order (sorted by archive path).

    The sort is Python's, not SQL's, on purpose: design §4 gives sutradhara
    ownership of on-media member order, and a SQL ``ORDER BY`` would hand that
    order to whatever collation the database happens to be configured with —
    the same catalog could then lay members down in one order on SQLite and
    another on Postgres. Python compares by codepoint, everywhere.
    """
    rows = list(
        session.scalars(select(BundleMember).where(BundleMember.bundle_id == bundle.id))
    )
    return sorted(rows, key=lambda row: row.member_path)


def _render_flush_map(
    session: Session,
    members: Sequence[MemberInput],
    *,
    member_rows: Sequence[BundleMember] | None = None,
) -> str:
    """Render the flush-time catalog map for one build attempt.

    Rows arrive sorted by archive path (rem's own ``--inputs`` convention —
    sutradhara owns on-media member order now, so it is declared, not
    incidental). ``sha256`` is the catalog ``file_sha256`` — the digest of the
    staged bytes, which rem's writer verifies against the streamed payload.
    ``ingest_item_id`` comes from the member's own recorded submission linkage
    when the caller resolved one (the flush path), else from the
    ``(archive_path, staged digest)`` join for callers that did not (the
    bundle-repair rebuild path), else the empty string. The recorded linkage
    is preferred because it survives the naming ladder: a disambiguated member
    no longer matches its submission row on ``archive_path``, and the join
    alone would silently drop that member's lineage from the map.

    ``member_rows`` is the flush's row snapshot, index-aligned with
    ``members``. When it is supplied a member the map cannot carry is reported
    as a ``_MemberBuildFailure`` against its row, so the flush's quarantine
    loop moves that one member aside and the rest of the bundle still builds.
    Without it (the bundle-repair rebuild path, which has no quarantine loop)
    the same condition raises a plain, named ``ArchiveFanoutError``.
    """
    for index, member in enumerate(members):
        _reject_unmappable_member(
            member,
            None if member_rows is None else member_rows[index],
        )
    unresolved = [member for member in members if member.ingest_item_id is None]
    ingest_ids = _submission_ingest_item_ids(session, unresolved) if unresolved else {}
    entries = [
        SourceMapEntry(
            archive_path=member.member_path,
            source_path=str(member.source_path),
            sha256=member.file_sha256,
            size_bytes=member.size_bytes,
            ingest_item_id=(
                member.ingest_item_id
                if member.ingest_item_id is not None
                else ingest_ids.get((member.member_path, member.file_sha256))
            ),
        )
        for member in members
    ]
    return render_source_map(entries)


def _reject_unmappable_member(
    member: MemberInput,
    member_row: BundleMember | None,
) -> None:
    """Refuse one member whose paths cannot survive the map's UTF-8 contract.

    rem parses the map with ``std::str::from_utf8`` over the WHOLE file
    (``remanence-cli/src/archive_map.rs``), so a single non-UTF-8-encodable
    byte rejects every row: the error names no map line and no archive path,
    ``_identify_member_failure`` cannot attribute it, and one odd filename
    kills a whole multi-class bundle — precisely what the quarantine loop
    exists to prevent. This is a live case, not a hypothetical: members whose
    source path is not UTF-8 encodable carry it as
    ``source_metadata['source_path_bytes_hex']`` (``archive_bundle.py``,
    ``archive_submission.py``) and ``_member_source_path`` surrogate-decodes
    it back.

    The member is quarantined rather than rescued. Rescuing it would mean
    republishing the bytes under a UTF-8-safe name in the flush work dir, and
    a hardlink only works within one filesystem — across the staging/work-dir
    boundary that degrades to a full copy of a possibly very large member, at
    flush time, to paper over a filename the operator should see. A
    non-UTF-8 filename is a data-hygiene fact worth surfacing; the split hold
    surfaces it while the rest of the bundle reaches media.
    """
    for label, value in (
        ("source_path", str(member.source_path)),
        ("member_path", member.member_path),
    ):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            message = (
                f"member {member.member_path!r} has a {label} that is not UTF-8 "
                f"encodable ({os.fsencode(value)!r}); rem's --map loader is "
                f"strict UTF-8 over the whole file, so this row would reject "
                f"the entire map"
            )
            if member_row is None:
                raise ArchiveFanoutError(message) from None
            raise _MemberBuildFailure(member_row, message) from None


def validate_submission_member_identity(
    session: Session,
    member_rows: Sequence[BundleMember],
) -> None:
    """Assert every submission-linked member row still matches its source map.

    This is the pre-write identity gate (design §4, P4 gate condition C3). It
    runs **once per flush attempt, before any build and before any physical
    write, for every representation** — which is why it lives here and not in
    a per-target artifact hook. The old per-submission ``artifact_validator``
    early-returned for representations outside the RAO family, and
    basis-ordered fan-out sorts a D2 shelf pool first, so an identity mismatch
    was caught only after the shelf write and left a media-only orphan.

    The check is catalog-grain and representation-blind: for every member the
    submission convergence recorded a linkage for, the ``SubmissionMember`` row
    must still exist and still agree on content hash, staged digest, size, and
    the ``ingest_item_id`` the map render will carry. Members with no
    submission linkage are **co-residents** — intake enqueues, other bundles'
    material — and are tolerated by construction: a group bundle holds whatever
    the group accumulated.
    """
    linked: dict[int, BundleMember] = {}
    for row in member_rows:
        for _submission_id, member_id in submission_links(row.source_metadata):
            linked[member_id] = row
    if not linked:
        return
    found = {
        record.id: record
        for record in session.scalars(
            select(SubmissionMember).where(SubmissionMember.id.in_(linked))
        )
    }
    for member_id, row in sorted(linked.items()):
        record = found.get(member_id)
        if record is None:
            raise ArchiveFanoutError(
                f"bundle member {row.member_path!r} names submission member "
                f"{member_id}, which is not in the catalog"
            )
        if row.logical_asset_hash != record.sha256:
            raise ArchiveFanoutError(
                f"bundle member {row.member_path!r} has logical hash "
                f"{row.logical_asset_hash.hex()}, but submission member "
                f"{member_id} records {record.sha256.hex()}"
            )
        if row.file_sha256 != record.sha256:
            raise ArchiveFanoutError(
                f"bundle member {row.member_path!r} has staged digest "
                f"{row.file_sha256.hex()}, but submission member {member_id} "
                f"records {record.sha256.hex()}"
            )
        if row.size_bytes != record.size_bytes:
            raise ArchiveFanoutError(
                f"bundle member {row.member_path!r} has size {row.size_bytes}, "
                f"but submission member {member_id} records {record.size_bytes}"
            )
        if record.ingest_item_id is None:
            raise ArchiveFanoutError(
                f"submission member {member_id} ({row.member_path!r}) has no "
                "ingest_item_id to cross-check"
            )


def validate_built_members(
    target: PoolTarget,
    artifact: BuildArtifact,
    members: Sequence[MemberInput],
) -> None:
    """Assert one built artifact carries exactly the members the map asked for.

    Runs for **every** representation, and — since ``_fan_out_targets`` builds
    and validates all targets before writing any — always before any physical
    write. Scope stated honestly: for ``D2TAR_RAW`` the artifact is assembled
    by our own ``_build_d2_tar`` from these same inputs, so the comparison is
    near-tautological there; the load-bearing checks on that leg are
    ``validate_submission_member_identity`` before the build and the readback
    verify after the write. For the RAO family the artifact comes back from
    rem, and this is where a builder that mis-associated a member surfaces.
    """
    expected = {member.member_path: member for member in members}
    seen: set[str] = set()
    for built in artifact.members:
        member = expected.get(built.member_path)
        if member is None:
            raise ArchiveFanoutError(
                f"archive build for pool {target.pool_id!r} returned unexpected "
                f"member {built.member_path!r}"
            )
        seen.add(built.member_path)
        if built.logical_asset_hash != member.logical_asset_hash:
            raise ArchiveFanoutError(
                f"archive build member {built.member_path!r} has logical hash "
                f"{built.logical_asset_hash.hex()}, expected "
                f"{member.logical_asset_hash.hex()}"
            )
        if built.file_sha256 != member.file_sha256:
            raise ArchiveFanoutError(
                f"archive build member {built.member_path!r} has sha256 "
                f"{built.file_sha256.hex()}, expected {member.file_sha256.hex()}"
            )
        if built.size_bytes != member.size_bytes:
            raise ArchiveFanoutError(
                f"archive build member {built.member_path!r} has size "
                f"{built.size_bytes}, expected {member.size_bytes}"
            )
        # A builder that reports no lineage at all is tolerated (rem's report
        # may omit the column); one that reports a *different* lineage is the
        # mis-association this check exists to catch.
        if member.ingest_item_id is not None and built.ingest_item_id not in {
            None,
            str(member.ingest_item_id),
        }:
            raise ArchiveFanoutError(
                f"archive build member {built.member_path!r} echoed ingest_item_id "
                f"{built.ingest_item_id!r}, expected {member.ingest_item_id!r}"
            )
    missing = sorted(set(expected) - seen)
    if missing:
        raise ArchiveFanoutError(
            f"archive build for pool {target.pool_id!r} omitted members: {missing!r}"
        )


def _submission_ingest_item_ids(
    session: Session,
    members: Sequence[MemberInput],
) -> dict[tuple[str, bytes], int]:
    """Map (archive path, staged digest) to ingest_item_id via SubmissionMember."""
    digests = {member.file_sha256 for member in members}
    if not digests:
        return {}
    wanted = {(member.member_path, member.file_sha256) for member in members}
    resolved: dict[tuple[str, bytes], int] = {}
    rows = session.execute(
        select(
            SubmissionMember.archive_path,
            SubmissionMember.sha256,
            SubmissionMember.ingest_item_id,
        )
        .where(SubmissionMember.sha256.in_(digests))
        .order_by(SubmissionMember.id)
    )
    for archive_path, sha256, ingest_item_id in rows:
        key = (archive_path, sha256)
        if key in wanted and ingest_item_id is not None:
            # Later submissions win deterministically (rows ordered by id).
            resolved[key] = ingest_item_id
    return resolved


def _preflight_flush_work_dir(
    work_dir: Path,
    *,
    bundle_id: str,
    required_bytes: int,
    target_count: int,
) -> None:
    """§8 statvfs preflight sized to the bundle x target count; skip-and-alarm."""
    stats = os.statvfs(work_dir)
    available = stats.f_bavail * stats.f_frsize
    if available < required_bytes:
        emit_structured_event(
            "bundle_flush_preflight_short",
            bundle_id=bundle_id,
            required_bytes=required_bytes,
            available_bytes=available,
            target_count=target_count,
            work_dir=str(work_dir),
        )
        raise FlushPreflightShort(
            f"bundle {bundle_id!r} flush skipped: work dir has {available} bytes free, "
            f"needs {required_bytes} ({target_count} targets)"
        )


def _fan_out_targets(
    session: Session,
    *,
    bundle: Bundle,
    targets: Sequence[tuple[WritableStorageBackend, PoolTarget]],
    members: Sequence[MemberInput],
    member_rows: Sequence[BundleMember],
    builder: ArchiveBuilder,
    key_epoch: str | None,
    work_dir: Path,
    map_path: Path,
    map_sha256: str,
    deliverables_dir: Path | str | None,
    manifest_signer: ManifestSigner | None,
) -> tuple[list[int], list[TransientPoolFanoutError], list[PoolWriteFailure], str | None]:
    """Build and validate EVERY target, then write them.

    The two phases are separate on purpose (design §4, C3): with build and
    write interleaved per target, an identity defect on the second
    representation was only caught after the first representation's bytes were
    already on media — a media-only orphan. Every artifact is now built and
    checked against the flush's member snapshot before the first
    ``write_object_to_pool``, so an identity failure costs nothing physical.

    The phase split is free on disk: the work dir already holds one artifact
    per target for the life of the flush (each target owns a subdirectory) and
    ``_preflight_flush_work_dir`` already reserves ``bundle x targets``.

    It also removes a stated limitation: a member-attributable build failure
    used to propagate instead of quarantining once any copy existed, because a
    retry would have rebuilt the written targets. No copy exists during the
    build phase, so every member failure now reaches the quarantine loop.

    In the write phase **no** failure propagates, transient or not. Design §4's
    "post-write failures seal partial rather than roll back" is a statement
    about physics, not about error taxonomy: once ``write_object_to_pool``
    returns, a tape append is unreclaimable, so an exception that escaped here
    would undo the *sibling* targets whose bytes are already on media too. The
    two failure lists carry the verdicts back out, and the flush seals what it
    placed.
    """
    prepared: list[tuple[WritableStorageBackend, PoolTarget, Path, BuildArtifact]] = []
    for index, (backend, target) in enumerate(targets):
        # §8 work-dir hygiene: two same-representation pool targets used to
        # collide on the work-dir artifact filename; every target now owns a
        # subdirectory, so artifact filenames are per-target.
        target_dir = work_dir / f"target-{index:02d}-{_pool_slug(target.pool_id)}"
        target_dir.mkdir(exist_ok=True)
        try:
            artifact = _build_for_target(
                bundle=bundle,
                members=members,
                target=target,
                builder=builder,
                key_epoch=key_epoch,
                work_dir=target_dir,
                map_path=map_path,
                source_root=_MAP_SOURCE_ROOT,
                map_sha256=map_sha256,
            )
        except RuntimeError as exc:
            member_row = _identify_member_failure(str(exc), member_rows)
            if member_row is not None:
                # The quarantine-re-render-retry loop in flush_bundle handles
                # this. Nothing is on media yet, so every attributable member
                # failure can be quarantined and retried.
                raise _MemberBuildFailure(member_row, str(exc)) from exc
            raise
        validate_built_members(target, artifact, members)
        prepared.append((backend, target, target_dir, artifact))

    copy_ids: list[int] = []
    transient_failures: list[TransientPoolFanoutError] = []
    write_failures: list[PoolWriteFailure] = []
    manifest_receipt: str | None = None
    for backend, target, target_dir, artifact in prepared:
        # Each target owns a savepoint so retryable backend failures do not
        # erase catalog rows for earlier successful placements.
        copy: Copy | None = None
        try:
            with session.begin_nested():
                try:
                    copy = write_bundle_copy_for_pool(
                        session,
                        bundle=bundle,
                        target=target,
                        artifact=artifact,
                        builder=builder,
                        backend=backend,
                        work_dir=target_dir,
                    )
                except TransientPoolFanoutError:
                    # Retryable: let it out of the savepoint, which discards
                    # this target's rows so the copy outbox can re-place it.
                    raise
                except ArchiveFanoutError as exc:
                    # Post-write by construction: the first line of
                    # `write_bundle_copy_for_pool` is the physical write, so
                    # everything that raises out of it happened after the bytes
                    # were committed to media. Caught INSIDE the savepoint so
                    # the SUSPECT copy row and the measurement evidence survive
                    # — the same handling `jobs/handlers/bundle_repair.py` gives
                    # this failure, and the reason is preservation, not
                    # symmetry: discarding the row would leave an unrecorded
                    # object on unreclaimable media and let a later repair
                    # append a second one.
                    write_failures.append(
                        PoolWriteFailure(target.pool_id, target.backend_name, exc)
                    )
                else:
                    # Member grain (§5): class and ruleset come from the
                    # matching member row, not from a representative-class hop.
                    _record_build_exclusions(
                        session,
                        bundle=bundle,
                        artifact=artifact,
                    )
        except TransientPoolFanoutError as exc:
            transient_failures.append(exc)
            continue
        if copy is None:
            continue
        copy_ids.append(copy.id)
        if (
            deliverables_dir is not None
            and artifact.manifest_path is not None
            and manifest_receipt is None
            and Representation(target.representation)
            in {Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1}
        ):
            manifest_receipt = str(
                # Member grain (§5): the receipt carries per-member classes as
                # a `member_classes` roll-up, not one bundle-level class.
                emit_customer_manifest(
                    bundle=bundle,
                    manifest_path=artifact.manifest_path,
                    destination_dir=Path(deliverables_dir),
                    signer=manifest_signer,
                )
            )
            bundle.customer_manifest_path = manifest_receipt
    return copy_ids, transient_failures, write_failures, manifest_receipt


def _pool_slug(pool_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", pool_id) or "pool"


def _identify_member_failure(
    message: str,
    member_rows: Sequence[BundleMember],
) -> BundleMember | None:
    """Map a rem build failure back to the member row it names, if any."""
    match = _MAP_LINE_FAILURE.search(message)
    if match:
        # Header is line 1; the renderer preserves row order, so data line N
        # is sorted row index N - 2.
        index = int(match.group(1)) - 2
        if 0 <= index < len(member_rows):
            return member_rows[index]
        return None
    match = _WRITER_DIGEST_FAILURE.search(message)
    if match:
        wanted = match.group(1)
        for row in member_rows:
            if row.member_path == wanted:
                return row
        return None
    return None


def _quarantine_member(
    session: Session,
    *,
    bundle: Bundle,
    member_row: BundleMember,
    quarantine: Bundle | None,
    message: str,
) -> Bundle:
    """Move one failed member into the flush's held quarantine bundle.

    The quarantine bundle is minted funnel-style — ``archive_id`` set at
    creation — so it never sits ``open`` under the group fingerprint and
    cannot collide with the one-open-accumulator partial index or be adopted.
    The member row and its staging-transform rows move by direct UPDATE, and
    both bundles' counters move atomically (design §4, hold-split mechanics).
    """
    if quarantine is None:
        stamp = dt.datetime.now(dt.UTC)
        quarantine_id = f"bundle-{uuid.uuid4().hex}"
        quarantine = Bundle(
            id=quarantine_id,
            bundle_group=bundle.bundle_group,
            group_basis=bundle.group_basis,
            status="held",
            target_bytes=bundle.target_bytes,
            max_age_seconds=bundle.max_age_seconds,
            archive_id=f"archive-{quarantine_id}",
            opened_at=stamp,
            held_at=stamp,
            review_summary={"quarantined_members": []},
        )
        session.add(quarantine)
        session.flush()
    session.execute(
        update(BundleMember)
        .where(BundleMember.id == member_row.id)
        .values(bundle_id=quarantine.id)
    )
    session.execute(
        update(StagingTransform)
        .where(StagingTransform.bundle_member_id == member_row.id)
        .values(bundle_id=quarantine.id)
    )
    session.execute(
        update(Bundle)
        .where(Bundle.id == bundle.id)
        .values(
            total_bytes=Bundle.total_bytes - member_row.size_bytes,
            member_count=Bundle.member_count - 1,
        )
    )
    session.execute(
        update(Bundle)
        .where(Bundle.id == quarantine.id)
        .values(
            total_bytes=Bundle.total_bytes + member_row.size_bytes,
            member_count=Bundle.member_count + 1,
        )
    )
    summary = dict(quarantine.review_summary or {})
    quarantined = list(summary.get("quarantined_members", []))
    quarantined.append(
        {
            "member_path": member_row.member_path,
            "artifactclass": member_row.artifactclass,
            "reason": message[:500],
        }
    )
    summary["quarantined_members"] = quarantined
    quarantine.review_summary = summary
    emit_structured_event(
        "bundle_member_quarantined",
        bundle_id=bundle.id,
        quarantine_bundle_id=quarantine.id,
        member_path=member_row.member_path,
        artifactclass=member_row.artifactclass,
        reason=message[:500],
    )
    # Flush pending ORM state BEFORE expiring: expire() discards unflushed
    # changes on the instance, and the flush's own status/claim writes must
    # not be lost to the counter refresh.
    session.flush()
    session.expire(member_row)
    session.expire(bundle)
    session.expire(quarantine, ["total_bytes", "member_count"])
    return quarantine


def _record_bundle_copy_outbox(session: Session, bundle_id: str) -> None:
    record_observation(
        session,
        domain="bundle_copy",
        target_key=bundle_id,
        desired=True,
        observed_state=OBSERVED_MISSING,
        reason="durability-unverified",
        message="bundle fan-out committed copies; post-commit durability check pending",
    )


def _record_bundle_copy_transient_backoff(
    session: Session,
    bundle_id: str,
    failures: Sequence[TransientPoolFanoutError],
) -> str:
    detail = "; ".join(f"pool {failure.pool_id}: {failure.cause}" for failure in failures)
    message = (
        f"bundle {bundle_id} sealed with partial fan-out; transient backend failure for {detail}"
    )
    record_observation(
        session,
        domain="bundle_copy",
        target_key=bundle_id,
        desired=True,
        observed_state=OBSERVED_MISSING,
        reason="transient-backend-failure",
        message=message,
    )
    record_condition(
        session,
        domain="bundle_copy",
        target_key=bundle_id,
        condition=CONDITION_BACKOFF,
        reason="transient-backend-failure",
        message=message,
    )
    return message


def _record_bundle_copy_write_failure(
    session: Session,
    bundle_id: str,
    failures: Sequence[PoolWriteFailure],
) -> str:
    """Alarm on pools whose bytes reached media and then failed their check.

    ``CONDITION_BLOCKED``, not ``CONDITION_BACKOFF``: the write is not known to
    have failed — it is known to have *landed and not verified*, and on tape
    that is unreclaimable. An automatic retry would append a second object; a
    human decides whether to re-place, re-verify, or condemn the media.
    """
    detail = "; ".join(f"pool {failure.pool_id}: {failure.cause}" for failure in failures)
    message = (
        f"bundle {bundle_id} sealed with partial fan-out; post-write failure for {detail}"
    )
    record_observation(
        session,
        domain="bundle_copy",
        target_key=bundle_id,
        desired=True,
        observed_state=OBSERVED_MISSING,
        reason="post-write-pool-failure",
        message=message,
    )
    record_condition(
        session,
        domain="bundle_copy",
        target_key=bundle_id,
        condition=CONDITION_BLOCKED,
        reason="post-write-pool-failure",
        message=message,
    )
    emit_structured_event(
        "bundle_copy_post_write_failure",
        bundle_id=bundle_id,
        pools=[failure.pool_id for failure in failures],
        message=message[:500],
    )
    return message


def _schedule_bundle_copy_fast_path(session: Session, bundle_id: str) -> None:
    pending = session.info.setdefault(_BUNDLE_COPY_FAST_PATH_KEY, set())
    pending.add(bundle_id)


@event.listens_for(Session, "after_commit")
def _run_bundle_copy_fast_path(session: Session) -> None:
    bundle_ids = session.info.pop(_BUNDLE_COPY_FAST_PATH_KEY, set())
    if not bundle_ids:
        return
    try:
        bind = session.get_bind()
        with Session(bind=bind, future=True) as fast_session:
            from sutradhara.jobs.reconcilers import bundle_copy

            for bundle_id in sorted(bundle_ids):
                bundle_copy.refresh_condition(fast_session, str(bundle_id))
            fast_session.commit()
    except Exception:
        LOGGER.exception(
            "bundle_copy_fast_path_failed",
            extra={"bundle_ids": sorted(str(bundle_id) for bundle_id in bundle_ids)},
        )


def build_bundle_copy_for_pool(
    session: Session,
    *,
    bundle: Bundle,
    target: PoolTarget,
    member_sources: Sequence[MemberInput],
    builder: ArchiveBuilder,
    backend: WritableStorageBackend,
    key_epoch: str | None,
    work_dir: Path,
    map_path: Path | None = None,
    source_root: Path | None = None,
    map_sha256: str | None = None,
) -> Copy:
    """Build, validate, write, verify, and record one pool's bundle copy.

    This primitive records the bundle ``Copy`` plus its ``AssetLocator`` and
    ``BlobRoot`` rows only. Bundle lifecycle, customer manifests, and
    ``ExclusionRecord`` rows stay with ``flush_bundle``. When the caller does
    not hand in a map (the bundle-repair rebuild path), one is rendered here
    from ``member_sources`` — every RAO build goes through the map route.

    ``flush_bundle`` does not use this composed form: it builds and validates
    every target first, then writes them, so no representation's bytes reach
    media before every representation has been checked. Single-target callers
    (bundle repair) get the same guarantee from the composition here.
    """

    if map_path is None:
        ordered = sorted(member_sources, key=lambda member: member.member_path)
        map_text = _render_flush_map(session, ordered)
        map_bytes = map_text.encode("utf-8")
        map_path = work_dir / f"{bundle.id}.map.tsv"
        map_path.write_bytes(map_bytes)
        map_sha256 = hashlib.sha256(map_bytes).hexdigest()
        source_root = _MAP_SOURCE_ROOT
        member_sources = ordered
    artifact = _build_for_target(
        bundle=bundle,
        members=member_sources,
        target=target,
        builder=builder,
        key_epoch=key_epoch,
        work_dir=work_dir,
        # The map route retires build-time rules entirely: `--map` conflicts
        # with `--rules`, and the member-grain scan already ran at enqueue
        # under each class's own ruleset. No bundle-level ruleset is derived
        # here — there is no honest one for a multi-class group bundle.
        ruleset="",
        map_path=map_path,
        source_root=source_root,
        map_sha256=map_sha256,
    )
    validate_built_members(target, artifact, member_sources)
    return write_bundle_copy_for_pool(
        session,
        bundle=bundle,
        target=target,
        artifact=artifact,
        builder=builder,
        backend=backend,
        work_dir=work_dir,
    )


def write_bundle_copy_for_pool(
    session: Session,
    *,
    bundle: Bundle,
    target: PoolTarget,
    artifact: BuildArtifact,
    builder: ArchiveBuilder,
    backend: WritableStorageBackend,
    work_dir: Path,
) -> Copy:
    """Write one already-built, already-validated artifact and record its copy.

    The first line of this function is the flush's first physical write. Every
    identity check the design places "before any physical write" has run by the
    time control reaches here; everything after it is post-write, where a
    rollback would orphan media (a tape append is unreclaimable and costs a
    bootstrap row) and the flush therefore seals partial instead.
    """
    try:
        committed_record = backend.write_object_to_pool(
            artifact.artifact_path,
            target.pool_id,
        )
    except (BackendError, OSError) as exc:
        raise TransientPoolFanoutError(target.pool_id, target.backend_name, exc) from exc
    report_tape_locator(committed_record.native_locator)
    storage_metadata = _copy_storage_metadata(
        target.representation,
        recipient_epochs=artifact.recipient_epochs,
        stored_size_bytes=committed_record.size_bytes,
    )
    copy, _ = add_bundle_copy(
        session,
        bundle_id=bundle.id,
        backend_id=target.backend_id,
        pool_id=target.pool_id,
        native_locator=committed_record.native_locator,
        integrity_hash=artifact.stored_digest,
        source=CopySource.INGEST,
        health=CopyHealth.OK,
        storage_metadata=storage_metadata,
    )
    _record_build_locators_and_roots(
        session,
        bundle=bundle,
        target=target,
        copy_id=copy.id,
        artifact=artifact,
    )
    try:
        verify_result = _verify_members_from_copy(
            backend=backend,
            copy_locator=copy.native_locator,
            members=artifact.members,
            representation=Representation(target.representation),
            storage_metadata=storage_metadata,
            builder=builder,
            work_dir=work_dir,
        )
    except (BackendError, OSError) as exc:
        copy.health = CopyHealth.SUSPECT
        raise TransientPoolFanoutError(target.pool_id, target.backend_name, exc) from exc
    except Exception:
        copy.health = CopyHealth.SUSPECT
        raise
    execution_id = f"{bundle.id}:{target.pool_id}:{copy.native_locator_key}"
    if verify_result.measured and verify_result.actual_hash is not None:
        record_measured(
            session,
            copy,
            verify_result,
            source="fanout",
            execution_id=execution_id,
        )
    elif verify_result.ok:
        record_unmeasured_promotion(
            session,
            copy,
            verify_result,
            source="fanout",
            execution_id=execution_id,
        )
    else:
        copy.health = CopyHealth.SUSPECT
    if not verify_result.ok:
        raise ArchiveFanoutError(f"backend verify failed: {verify_result.detail}")
    return copy


def emit_customer_manifest(
    *,
    bundle: Bundle,
    manifest_path: Path,
    destination_dir: Path,
    signer: ManifestSigner | None,
) -> Path:
    """Wrap rem's manifest with an archive id, timestamp, and keyed signature.

    The receipt is member-grain (§5): each member entry carries its own
    ``artifactclass``, ``stored_member_name`` carries any disambiguation tag,
    and ``member_name`` is the logical name — two co-resident same-named
    members share a ``member_name`` and are distinguished by
    ``stored_member_name``, so the receipt is not misread as a duplicate.
    """
    if signer is None:
        raise ManifestSigningError("customer manifest requires a keyed signer")
    destination_dir.mkdir(parents=True, exist_ok=True)
    source = _read_json(manifest_path)
    archive_id = bundle.archive_id or f"archive-{bundle.id}"
    payload = {
        "archive_id": archive_id,
        "bundle_id": bundle.id,
        "member_classes": sorted({member.artifactclass for member in bundle.members}),
        "issued_at": dt.datetime.now(dt.UTC).isoformat(),
        "manifest": source,
        "members": _customer_manifest_members(bundle),
        "exclusion_summary": bundle.scan_summary.get("exclusions", [])
        if bundle.scan_summary
        else [],
    }
    payload["signature"] = signer.sign(payload)
    destination = destination_dir / f"{archive_id}.manifest.json"
    destination.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n")
    bundle.archive_id = archive_id
    return destination


def _customer_manifest_members(bundle: Bundle) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for member in sorted(bundle.members, key=lambda item: item.member_path):
        metadata = member.source_metadata or {}
        logical_name = metadata.get("logical_path")
        if not isinstance(logical_name, str) or not logical_name:
            logical_name = member.member_path
        transforms = sorted(member.transforms, key=lambda item: item.step_order)
        entries.append(
            {
                "member_name": logical_name,
                "stored_member_name": member.member_path,
                "artifactclass": member.artifactclass,
                "logical_sha256": member.logical_asset_hash.hex(),
                "stored_sha256": member.file_sha256.hex(),
                "transforms": [transform.kind for transform in transforms],
                "pfr_original": not any(
                    transform.kind == "zstd-file-v1" for transform in transforms
                ),
            }
        )
    return entries


def _build_for_target(
    *,
    bundle: Bundle,
    members: Sequence[MemberInput],
    target: PoolTarget,
    builder: ArchiveBuilder,
    key_epoch: str | None,
    work_dir: Path,
    ruleset: str = "",
    map_path: Path | None = None,
    source_root: Path | None = None,
    map_sha256: str | None = None,
) -> BuildArtifact:
    representation = Representation(target.representation)
    if representation is Representation.D2TAR_RAW:
        # D2TAR_RAW stays map-blind by design (§4): _build_d2_tar tars the
        # staged sources directly and carries only the post-write readback
        # check, so a legacy-pool leg pays one wasted write on a bad source,
        # never silent corruption. The map's pre-write digest check is the
        # RAO writer's.
        return _build_d2_tar(bundle, members, work_dir)
    return builder.build(
        bundle=bundle,
        members=members,
        representation=representation,
        ruleset=ruleset,
        key_epoch=key_epoch if representation is Representation.RAO_AEAD_V1 else None,
        work_dir=work_dir,
        map_path=map_path,
        source_root=source_root,
        map_sha256=map_sha256,
    )


def _require_key_epoch(
    targets: Sequence[tuple[WritableStorageBackend, PoolTarget]],
) -> None:
    for _, target in targets:
        if target.representation == Representation.RAO_AEAD_V1.value and target.key_epoch is None:
            raise ArchiveFanoutError(f"encrypted pool {target.pool_id!r} requires key_epoch")


def _copy_storage_metadata(
    representation: str,
    *,
    recipient_epochs: Sequence[str],
    stored_size_bytes: int,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "representation": representation,
        "stored_size_bytes": stored_size_bytes,
    }
    if representation in {
        Representation.RAO_PLAIN_V1.value,
        Representation.RAO_AEAD_V1.value,
    }:
        metadata["chunk_size"] = RAO_CHUNK_SIZE
    if representation == Representation.RAO_AEAD_V1.value:
        if not recipient_epochs:
            raise ArchiveFanoutError("encrypted archive artifact is missing recipient epochs")
        metadata["recipient_epochs"] = list(recipient_epochs)
    return metadata


def _build_d2_tar(
    bundle: Bundle,
    members: Sequence[MemberInput],
    work_dir: Path,
) -> BuildArtifact:
    tar_path = work_dir / f"{bundle.id}-d2tar-raw.tar"
    with tarfile.open(tar_path, "w") as tar:
        for member in members:
            tar.add(member.source_path, arcname=member.member_path, recursive=False)
    built_members: list[BuiltMember] = []
    with tarfile.open(tar_path, "r") as tar:
        for member in members:
            info = tar.getmember(member.member_path)
            built_members.append(
                BuiltMember(
                    logical_asset_hash=member.logical_asset_hash,
                    member_path=member.member_path,
                    size_bytes=member.size_bytes,
                    file_sha256=member.file_sha256,
                    native_locator={
                        "member_path": member.member_path,
                        "block_range": [info.offset_data, info.offset_data + info.size],
                        "size_bytes": info.size,
                    },
                    # Carried through so the built-member check reads the same
                    # lineage column on every representation, not just RAO.
                    ingest_item_id=(
                        None
                        if member.ingest_item_id is None
                        else str(member.ingest_item_id)
                    ),
                )
            )
    manifest_path = work_dir / f"{bundle.id}-d2tar-raw.manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "format": "d2tar-raw",
                "bundle_id": bundle.id,
                "members": [
                    {
                        "path": member.member_path,
                        "sha256": member.file_sha256.hex(),
                        "size_bytes": member.size_bytes,
                    }
                    for member in built_members
                ],
            },
            sort_keys=True,
            indent=2,
        )
        + "\n"
    )
    return BuildArtifact(
        artifact_path=tar_path,
        stored_digest=_sha256_file(tar_path),
        members=tuple(built_members),
        manifest_path=manifest_path,
    )


def _record_build_locators_and_roots(
    session: Session,
    *,
    bundle: Bundle,
    target: PoolTarget,
    copy_id: int,
    artifact: BuildArtifact,
) -> None:
    for member in artifact.members:
        record_asset_locator(
            session,
            logical_asset_hash=member.logical_asset_hash,
            pool_id=target.pool_id,
            native_locator=member.native_locator,
            representation=target.representation,
            copy_id=copy_id,
            bundle_id=bundle.id,
            member_path=member.member_path,
        )
    for root in artifact.blob_roots:
        record_blob_root(
            session,
            bundle_id=bundle.id,
            copy_id=copy_id,
            pool_id=target.pool_id,
            root_path=root.root_path,
            native_locator=root.native_locator,
            archive_id=bundle.archive_id,
        )


def _record_build_exclusions(
    session: Session,
    *,
    bundle: Bundle,
    artifact: BuildArtifact,
) -> None:
    """Record build exclusions with member-sourced class and ruleset (§5).

    An exclusion whose path matches a member row exactly is a per-asset
    exclusion: it records the member's class, that class's ruleset, and the
    member's logical hash (joined through members by hash — the Sony-split
    duplicate resolves to its own class's row). A cluster exclusion with no
    matching member keys to the bundle's sole member class where one exists;
    a multi-class bundle records it classless — the producing class is not
    recoverable from the build artifact, and P2's enqueue-time scan keys
    these by (class, root) scan identity instead.
    """
    members_by_path = {member.member_path: member for member in bundle.members}
    classes = sorted({member.artifactclass for member in bundle.members})
    sole_class = classes[0] if len(classes) == 1 else None
    rulesets: dict[str, str | None] = {}

    def _ruleset_for(artifactclass: str | None) -> str | None:
        if not artifactclass:
            return None
        if artifactclass not in rulesets:
            policy = session.get(ArtifactClassPolicyRecord, artifactclass)
            rulesets[artifactclass] = None if policy is None else (policy.ruleset or None)
        return rulesets[artifactclass]

    for exclusion in artifact.exclusions:
        member = (
            members_by_path.get(exclusion.path) if exclusion.count <= 1 else None
        )
        artifactclass = member.artifactclass if member is not None else sole_class
        record_exclusion(
            session,
            bundle_id=bundle.id,
            artifactclass=artifactclass or "",
            logical_asset_hash=None if member is None else member.logical_asset_hash,
            path=exclusion.path,
            reason=exclusion.reason,
            count=exclusion.count,
            bytes_total=exclusion.bytes_total,
            ruleset_name=_ruleset_for(artifactclass),
            detail=exclusion.detail,
        )


def _verify_members_from_copy(
    *,
    backend: WritableStorageBackend,
    copy_locator: dict[str, Any],
    members: Sequence[BuiltMember],
    representation: Representation,
    storage_metadata: Mapping[str, Any],
    builder: ArchiveBuilder,
    work_dir: Path,
) -> VerifyResult:
    result = backend.verify(copy_locator)
    if not result.ok:
        return result
    cached_container: bytes | None = None
    for member in members:
        data, cached_container = _verified_member_bytes(
            backend=backend,
            copy_locator=copy_locator,
            member=member,
            representation=representation,
            storage_metadata=storage_metadata,
            builder=builder,
            work_dir=work_dir,
            cached_container=cached_container,
        )
        digest = hashlib.sha256(data).digest()
        if digest != member.file_sha256:
            raise ArchiveFanoutError(
                f"member verification failed for {member.member_path!r}: "
                f"{digest.hex()} != {member.file_sha256.hex()}"
            )
    return result


def _verified_member_bytes(
    *,
    backend: WritableStorageBackend,
    copy_locator: dict[str, Any],
    member: BuiltMember,
    representation: Representation,
    storage_metadata: Mapping[str, Any],
    builder: ArchiveBuilder,
    work_dir: Path,
    cached_container: bytes | None,
) -> tuple[bytes, bytes | None]:
    try:
        data = read_member_bytes(
            backend,
            _ReadCopyView(
                native_locator=dict(copy_locator),
                storage_metadata=dict(storage_metadata),
            ),
            _ReadAssetLocatorView(
                representation=representation.value,
                native_locator=dict(member.native_locator),
                member_path=member.member_path,
            ),
            work_dir=work_dir,
        )
    except ArchiveRestoreError as exc:
        if representation is not Representation.RAO_AEAD_V1:
            raise ArchiveFanoutError(str(exc)) from exc
        fallback_error = exc
    else:
        return data, cached_container

    verifier = getattr(builder, "verify_member_copy", None)
    if verifier is None:
        raise ArchiveFanoutError(
            f"member verification for {representation.value!r} requires builder support: "
            f"{fallback_error}"
        )
    data = verifier(
        backend=backend,
        copy_locator=copy_locator,
        member=member,
        representation=representation,
        storage_metadata=storage_metadata,
        work_dir=work_dir,
    )
    return data, cached_container


def _member_input(
    member: BundleMember,
    *,
    ingest_item_id: int | None = None,
) -> MemberInput:
    source_path = _member_source_path(member)
    if source_path is None:
        raise ArchiveFanoutError(
            f"bundle member {member.id} has no source_path; cannot materialize"
        )
    return MemberInput(
        logical_asset_hash=member.logical_asset_hash,
        member_path=member.member_path,
        source_path=source_path,
        size_bytes=member.size_bytes,
        file_sha256=member.file_sha256,
        ingest_item_id=ingest_item_id,
    )


def _member_inputs_for_flush(
    session: Session,
    member_rows: Sequence[BundleMember],
) -> list[MemberInput]:
    """Build the flush's member inputs with submission lineage resolved once.

    Lineage comes from the linkage the submission append recorded on the member
    row, so it survives the naming ladder — the ``archive_path`` join that
    predates the linkage misses a disambiguated member and would drop its
    ``ingest_item_id`` from the map.
    """
    linked = {
        member_id: row
        for row in member_rows
        for _submission_id, member_id in submission_links(row.source_metadata)
    }
    lineage: dict[int, int] = {}
    if linked:
        lineage = {
            record.id: record.ingest_item_id
            for record in session.scalars(
                select(SubmissionMember).where(SubmissionMember.id.in_(linked))
            )
            if record.ingest_item_id is not None
        }
    resolved: dict[int, int] = {}
    for member_id, row in linked.items():
        if member_id in lineage:
            resolved[row.id] = lineage[member_id]
    return [_member_input(row, ingest_item_id=resolved.get(row.id)) for row in member_rows]


def _member_source_path(member: BundleMember) -> Path | None:
    if member.source_path is not None:
        return Path(member.source_path)
    metadata = member.source_metadata or {}
    raw_hex = metadata.get("source_path_bytes_hex")
    if isinstance(raw_hex, str) and raw_hex:
        try:
            return Path(os.fsdecode(bytes.fromhex(raw_hex)))
        except ValueError as exc:
            raise ArchiveFanoutError(
                f"bundle member {member.id} has invalid source_path_bytes_hex"
            ) from exc
    return None


def _scan_from_json(raw: dict[str, Any]) -> ConformanceScan:
    clusters = tuple(_cluster_from_json(item) for item in raw.get("clusters", []))
    exclusions = tuple(_cluster_from_json(item) for item in raw.get("exclusions", []))
    return ConformanceScan(clusters=clusters, exclusions=exclusions)


def _normalized_rem_scan_report(report: dict[str, Any]) -> dict[str, Any]:
    scan = report.get("scan")
    if not isinstance(scan, dict):
        return report
    normalized = dict(report)
    normalized["clusters"] = scan.get("clusters", normalized.get("clusters", []))
    normalized["exclusions"] = scan.get("exclusions", normalized.get("exclusions", []))
    return normalized


def _cluster_from_json(raw: object) -> DeviationCluster:
    if not isinstance(raw, dict):
        raise ArchiveFanoutError("scan cluster must be an object")
    samples = raw.get("samples", [])
    bytes_value = raw.get("bytes_total")
    if bytes_value is None:
        bytes_value = raw.get("bytes", 0)
    return DeviationCluster(
        prefix=str(raw.get("prefix", "")),
        reason=str(raw.get("reason", "unknown")),
        count=int(raw.get("count", 0)),
        bytes_total=int(str(bytes_value)),
        samples=tuple(str(sample) for sample in samples if isinstance(sample, str)),
        proposed_default=(
            None if raw.get("proposed_default") is None else str(raw.get("proposed_default"))
        ),
    )


def _cluster_json(cluster: DeviationCluster) -> dict[str, Any]:
    return {
        "prefix": cluster.prefix,
        "reason": cluster.reason,
        "count": cluster.count,
        "bytes_total": cluster.bytes_total,
        "samples": list(cluster.samples),
        "proposed_default": cluster.proposed_default,
    }


def _normalized_rem_build_report(report: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(report)
    if normalized.get("members"):
        return normalized
    files = normalized.get("files")
    if isinstance(files, list):
        members: list[dict[str, Any]] = []
        for item in files:
            if not isinstance(item, dict):
                continue
            path = item.get("path")
            if not isinstance(path, str):
                continue
            members.append(
                {
                    "path": path,
                    "size_bytes": item.get("size_bytes"),
                    "sha256": item.get("file_sha256") or item.get("sha256"),
                    "first_chunk_lba": item.get("first_chunk_lba"),
                    "ingest_item_id": item.get("ingest_item_id"),
                }
            )
        normalized["members"] = members
    return normalized


def _members_from_manifest(
    manifest: dict[str, Any],
    inputs: Sequence[MemberInput],
) -> Sequence[BuiltMember]:
    """Join manifest locators to source-owned member identity facts."""

    by_path = {member.member_path: member for member in inputs}
    if len(by_path) != len(inputs):
        raise ArchiveFanoutError("archive inputs contain duplicate member paths")
    raw_members = manifest.get("members", [])
    if not isinstance(raw_members, list) or not raw_members:
        raise ArchiveFanoutError("rem manifest did not include member locators")
    built: list[BuiltMember] = []
    seen: set[str] = set()
    for item in raw_members:
        if not isinstance(item, dict):
            raise ArchiveFanoutError("rem manifest member must be an object")
        raw_path = item.get("path") or item.get("member_path")
        if not isinstance(raw_path, str) or not raw_path:
            raise ArchiveFanoutError("rem manifest member is missing its path")
        path = raw_path
        if path not in by_path:
            raise ArchiveFanoutError(f"rem manifest returned unknown member path {path!r}")
        if path in seen:
            raise ArchiveFanoutError(f"rem manifest duplicated member path {path!r}")
        seen.add(path)
        source = by_path[path]
        reported_size = item.get("size_bytes")
        if reported_size != source.size_bytes:
            raise ArchiveFanoutError(
                f"rem manifest resized member {path!r}: "
                f"{reported_size!r} != {source.size_bytes}"
            )
        reported_sha256 = item.get("sha256")
        try:
            reported_digest = bytes.fromhex(reported_sha256)
        except (TypeError, ValueError):
            raise ArchiveFanoutError(
                f"rem manifest returned invalid file_sha256 for member {path!r}"
            ) from None
        if reported_digest != source.file_sha256:
            raise ArchiveFanoutError(f"rem manifest rehashed member {path!r}")
        try:
            first_chunk_lba = int(item.get("first_chunk_lba", 0))
        except (TypeError, ValueError):
            raise ArchiveFanoutError(
                f"rem manifest returned invalid first_chunk_lba for member {path!r}"
            ) from None
        built.append(
            BuiltMember(
                logical_asset_hash=source.logical_asset_hash,
                member_path=source.member_path,
                size_bytes=source.size_bytes,
                file_sha256=source.file_sha256,
                native_locator={
                    "member_path": source.member_path,
                    "first_chunk_lba": first_chunk_lba,
                    "size_bytes": source.size_bytes,
                },
                ingest_item_id=(
                    None if item.get("ingest_item_id") is None else str(item.get("ingest_item_id"))
                ),
            )
        )
    omitted = sorted(by_path.keys() - seen)
    if omitted:
        raise ArchiveFanoutError(f"rem manifest omitted member paths: {omitted!r}")
    return built


def _blob_roots_from_manifest(manifest: dict[str, Any]) -> Sequence[BuiltBlobRoot]:
    roots = []
    for item in manifest.get("blob_roots", []):
        if isinstance(item, dict):
            roots.append(
                BuiltBlobRoot(
                    root_path=str(item.get("root_path", "")),
                    native_locator=dict(item.get("native_locator", item)),
                )
            )
    return roots


def _exclusions_from_manifest(manifest: dict[str, Any]) -> Sequence[BuiltExclusion]:
    exclusions = []
    for item in manifest.get("exclusions", []):
        if isinstance(item, dict):
            exclusions.append(
                BuiltExclusion(
                    path=str(item.get("path", "")),
                    reason=str(item.get("reason", "excluded")),
                    count=int(item.get("count", 1)),
                    bytes_total=int(item.get("bytes_total", 0)),
                    detail=dict(item),
                )
            )
    return exclusions


def _read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ArchiveFanoutError(f"{path} JSON root is not an object")
    return data


def _member_first_chunk_lba(locator: Mapping[str, Any]) -> int:
    try:
        return member_byte_base(locator) // RAO_CHUNK_SIZE
    except ArchiveRestoreError as exc:
        raise ArchiveFanoutError(str(exc)) from exc


def _metadata_recipient_epochs(storage_metadata: Mapping[str, Any]) -> tuple[str, ...]:
    value = storage_metadata.get("recipient_epochs")
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(epoch, str) or not epoch for epoch in value)
    ):
        raise ArchiveFanoutError("encrypted copy metadata has invalid recipient_epochs")
    if len(set(value)) != len(value):
        raise ArchiveFanoutError("encrypted copy metadata has duplicate recipient_epochs")
    return tuple(value)


def _materialize_copy_to_path(
    backend: WritableStorageBackend,
    copy_locator: Mapping[str, Any],
    storage_metadata: Mapping[str, Any],
    destination: Path,
) -> None:
    size = storage_metadata.get("stored_size_bytes")
    if isinstance(size, int) and size >= 0:
        with destination.open("wb") as handle:
            for start in range(0, size, RAO_CHUNK_SIZE):
                end = min(start + RAO_CHUNK_SIZE, size)
                handle.write(backend.read_range(dict(copy_locator), ByteRange(start, end)))
        return
    destination.write_bytes(backend.read_range(dict(copy_locator), ByteRange(0, 0)))


def _single_restored_member(dest_dir: Path, member_path: str) -> bytes:
    candidate = dest_dir / member_path
    if candidate.is_file():
        return candidate.read_bytes()
    files = [path for path in dest_dir.rglob("*") if path.is_file()]
    if len(files) != 1:
        raise ArchiveFanoutError(
            f"rem member verification expected one file for {member_path!r}, found {len(files)}"
        )
    return files[0].read_bytes()


def _run_rem(cmd: Sequence[str]) -> None:
    result = run_managed(list(cmd), role="medium", capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise ArchiveFanoutError(
            f"rem command failed (exit {result.returncode}): "
            f"stdout={result.stdout.strip()[:500]!r} "
            f"stderr={result.stderr.strip()[:500]!r}"
        )


def _sha256_file(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()
