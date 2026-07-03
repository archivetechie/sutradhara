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
import tarfile
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from sqlalchemy import event, select
from sqlalchemy.orm import Session, joinedload

from sutradhara.archive_bundle import (
    close_bundle,
    hold_bundle,
    record_asset_locator,
    record_blob_root,
    record_exclusion,
)
from sutradhara.archive_restore import ArchiveRestoreError, read_member_bytes
from sutradhara.backend.port import BackendError, ByteRange
from sutradhara.catalog.copies import add_bundle_copy
from sutradhara.catalog.models import Bundle, BundleMember, Copy
from sutradhara.catalog.types import CopyHealth, CopySource
from sutradhara.hdcache.fill import enqueue_post_flush_hdcache_fills
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    OBSERVED_MISSING,
    record_condition,
    record_observation,
)
from sutradhara.keys import KEY_DOMAIN_ARCHIVE, KeyRegistry, assert_key_epoch_domain
from sutradhara.rem_archive_cli import (
    resolve_rem_bin,
    run_rem_archive_build,
    run_rem_archive_scan,
)
from sutradhara.replication import (
    PoolTarget,
    WritableStorageBackend,
    target_pools,
)
from sutradhara.resource_control import run_managed
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RAO_CHUNK_SIZE

LOGGER = logging.getLogger(__name__)
_BUNDLE_COPY_FAST_PATH_KEY = "sutradhara_bundle_copy_fast_path"


class ArchiveFanoutError(Exception):
    """Base class for archive fan-out errors."""


class BundleHeld(ArchiveFanoutError):
    """The conformance gate held a bundle for review."""


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
            f"transient backend failure for pool {pool_id!r} "
            f"on backend {backend_name!r}: {cause}"
        )


@dataclass(frozen=True)
class MemberInput:
    """One source member sent to an archive builder."""

    logical_asset_hash: bytes
    member_path: str
    source_path: Path
    size_bytes: int
    file_sha256: bytes


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
        return bool(self.clusters or self.exclusions)

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


class ArchiveBuilder(Protocol):
    """Archive builder boundary owned by remanence in production."""

    def scan(
        self,
        *,
        bundle: Bundle,
        members: Sequence[MemberInput],
        ruleset: str,
    ) -> ConformanceScan:
        """Run the ruleset scan-only pass and return clustered deviations."""
        ...

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


ArtifactObserver = Callable[[BuildArtifact], None]


class LocalArchiveBuilder:
    """Deterministic archive builder for tests and local dry-runs.

    The object format is intentionally simple and self-describing:
    ``8-byte header length`` + JSON header + concatenated member bytes. It is
    not RAO; production callers should use ``RemArchiveBuilder``.
    """

    def scan(
        self,
        *,
        bundle: Bundle,
        members: Sequence[MemberInput],
        ruleset: str,
    ) -> ConformanceScan:
        return ConformanceScan()

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
        )


class RemArchiveBuilder:
    """Subprocess adapter for ``rem archive build``.

    The command is deliberately thin: sutradhara passes the ruleset name/path and
    member paths, then consumes the manifest emitted by rem. The exact rem
    manifest shape is normalized permissively so tests can cover the sutradhara
    side without depending on rem internals.
    """

    def __init__(self, rem_bin: str | Path | None = None) -> None:
        self._rem_bin = None if rem_bin is None else str(rem_bin)

    def scan(
        self,
        *,
        bundle: Bundle,
        members: Sequence[MemberInput],
        ruleset: str,
    ) -> ConformanceScan:
        report = run_rem_archive_scan(
            inputs=_rem_input_paths(members),
            ruleset=ruleset or None,
            rem_bin=self._rem_bin,
            failure_label="rem archive scan",
        )
        return _scan_from_json(_normalized_rem_scan_report(report))

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
        rem_ruleset: str | None = ruleset or None
        rem_inputs = _rem_input_paths(members) if map_path is None else None
        if map_path is not None and source_root is None:
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
            with KeyRegistry().materialized_root_key(key_epoch) as key_file:
                result = run_rem_archive_build(
                    inputs=rem_inputs,
                    ruleset=None if map_path is not None else rem_ruleset,
                    map_path=map_path,
                    source_root=source_root,
                    map_sha256=map_sha256,
                    output_path=output_path,
                    manifest_path=manifest_path,
                    rem_bin=self._rem_bin,
                    encrypt=True,
                    key_id=key_epoch,
                    key_file=key_file,
                    failure_label="rem archive build",
                )
        else:
            result = run_rem_archive_build(
                inputs=rem_inputs,
                ruleset=None if map_path is not None else rem_ruleset,
                map_path=map_path,
                source_root=source_root,
                map_sha256=map_sha256,
                output_path=output_path,
                manifest_path=manifest_path,
                rem_bin=self._rem_bin,
                failure_label="rem archive build",
            )
        manifest = _normalized_rem_build_report(result.stdout_report)
        return BuildArtifact(
            artifact_path=output_path,
            stored_digest=result.stored_digest,
            members=tuple(_members_from_manifest(manifest, members)),
            manifest_path=manifest_path,
            blob_roots=tuple(_blob_roots_from_manifest(manifest)),
            exclusions=tuple(_exclusions_from_manifest(manifest)),
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
            str(_first_chunk_lba(member.native_locator)),
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
            key_epoch = _metadata_key_epoch(storage_metadata)
            with KeyRegistry().materialized_root_key(key_epoch) as key_file:
                cmd.extend(["--key-file", str(key_file)])
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
    map_path: Path | str | None = None,
    source_root: Path | str | None = None,
    map_sha256: str | None = None,
    artifact_validator: Callable[[PoolTarget, BuildArtifact], None] | None = None,
) -> FanoutResult:
    """Flush one open bundle, build each pool copy, and record catalog state."""
    if deliverables_dir is not None and manifest_signer is None:
        raise ManifestSigningError("deliverables_dir requires a manifest_signer")
    resolved_map_path = None if map_path is None else Path(map_path)
    resolved_source_root = None if source_root is None else Path(source_root)
    if resolved_map_path is not None and resolved_source_root is None:
        raise ArchiveFanoutError("map flush requires source_root")
    if resolved_map_path is None and resolved_source_root is not None:
        raise ArchiveFanoutError("source_root is only valid for map flush")
    if resolved_map_path is None and map_sha256 is not None:
        raise ArchiveFanoutError("map_sha256 is only valid for map flush")
    bundle = (
        session.scalars(
            select(Bundle).options(joinedload(Bundle.members)).where(Bundle.id == bundle_id)
        )
        .unique()
        .one()
    )
    if bundle.status != "open":
        raise ArchiveFanoutError(f"bundle {bundle.id!r} is not open")
    if not bundle.members:
        raise ArchiveFanoutError(f"bundle {bundle.id!r} has no members")
    if tape_capacity_bytes is not None:
        for member in bundle.members:
            if member.size_bytes > tape_capacity_bytes:
                raise BundleOversize(
                    f"member {member.member_path!r} exceeds tape capacity; # TODO: oversize split"
                )

    members = [_member_input(member) for member in bundle.members]
    ruleset = bundle.ruleset or ""
    if resolved_map_path is None:
        scan = builder.scan(bundle=bundle, members=members, ruleset=ruleset)
        bundle.scan_summary = scan.to_summary()
        if bundle.expect == "compliant" and scan.has_deviations:
            hold_bundle(session, bundle, summary=scan.to_summary())
            raise BundleHeld(f"bundle {bundle.id!r} held for conformance review")
    else:
        bundle.scan_summary = {
            "mode": "map",
            "source_map_path": str(resolved_map_path),
        }

    if bundle.archive_id is None:
        bundle.archive_id = f"archive-{bundle.id}"
    targets = target_pools(session, bundle.artifactclass, backends, key_epoch=key_epoch)
    _require_key_epoch(targets)
    copy_ids: list[int] = []
    transient_failures: list[TransientPoolFanoutError] = []
    manifest_receipt: str | None = None
    bundle.status = "flushing"
    bundle.flushed_at = dt.datetime.now(dt.UTC)

    with tempfile.TemporaryDirectory(prefix=f"sutradhara-bundle-{bundle.id}-") as raw:
        work_dir = Path(raw)
        for backend, target in targets:
            # Each target owns a savepoint so retryable backend failures do not
            # erase catalog rows for earlier successful placements.
            built: list[BuildArtifact] = []
            try:
                with session.begin_nested():
                    copy = build_bundle_copy_for_pool(
                        session,
                        bundle=bundle,
                        target=target,
                        member_sources=members,
                        builder=builder,
                        backend=backend,
                        key_epoch=key_epoch,
                        work_dir=work_dir,
                        map_path=resolved_map_path,
                        source_root=resolved_source_root,
                        map_sha256=map_sha256,
                        artifact_validator=artifact_validator,
                        artifact_observer=built.append,
                    )
                    [artifact] = built
                    _record_build_exclusions(
                        session,
                        bundle=bundle,
                        artifact=artifact,
                    )
            except TransientPoolFanoutError as exc:
                transient_failures.append(exc)
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
                    emit_customer_manifest(
                        bundle=bundle,
                        manifest_path=artifact.manifest_path,
                        destination_dir=Path(deliverables_dir),
                        signer=manifest_signer,
                    )
                )
                bundle.customer_manifest_path = manifest_receipt

    close_bundle(session, bundle)
    if transient_failures:
        condition_message = _record_bundle_copy_transient_backoff(
            session,
            bundle.id,
            transient_failures,
        )
        condition_reason = "transient-backend-failure"
    else:
        _record_bundle_copy_outbox(session, bundle.id)
        condition_message = None
        condition_reason = None
    _schedule_bundle_copy_fast_path(session, bundle.id)
    enqueue_post_flush_hdcache_fills(session, bundle.id)
    return FanoutResult(
        bundle.id,
        tuple(copy_ids),
        manifest_receipt,
        partial=bool(transient_failures),
        failed_pools=tuple(failure.pool_id for failure in transient_failures),
        condition_reason=condition_reason,
        condition_message=condition_message,
    )


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
    detail = "; ".join(
        f"pool {failure.pool_id}: {failure.cause}" for failure in failures
    )
    message = (
        f"bundle {bundle_id} sealed with partial fan-out; "
        f"transient backend failure for {detail}"
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
    artifact_validator: Callable[[PoolTarget, BuildArtifact], None] | None = None,
    artifact_observer: ArtifactObserver | None = None,
) -> Copy:
    """Build, write, verify, and record one pool's bundle copy.

    This primitive records the bundle ``Copy`` plus its ``AssetLocator`` and
    ``BlobRoot`` rows only. Bundle lifecycle, conformance gates, customer
    manifests, and ``ExclusionRecord`` rows stay with ``flush_bundle``.
    Optional map/validator/observer arguments exist so ``flush_bundle`` can use
    the same write path without changing its current behavior.
    """

    artifact = _build_for_target(
        bundle=bundle,
        members=member_sources,
        target=target,
        builder=builder,
        key_epoch=key_epoch,
        work_dir=work_dir,
        map_path=map_path,
        source_root=source_root,
        map_sha256=map_sha256,
    )
    if artifact_validator is not None:
        artifact_validator(target, artifact)
    try:
        record = backend.write_object_to_pool(artifact.artifact_path, target.pool_id)
    except (BackendError, OSError) as exc:
        raise TransientPoolFanoutError(target.pool_id, target.backend_name, exc) from exc
    storage_metadata = _copy_storage_metadata(
        target.representation,
        key_epoch=target.key_epoch,
        stored_size_bytes=record.size_bytes,
    )
    copy, _ = add_bundle_copy(
        session,
        bundle_id=bundle.id,
        backend_id=target.backend_id,
        pool_id=target.pool_id,
        native_locator=record.native_locator,
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
        _verify_members_from_copy(
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
    copy.last_verified_at = dt.datetime.now(dt.UTC)
    if artifact_observer is not None:
        artifact_observer(artifact)
    return copy


def emit_customer_manifest(
    *,
    bundle: Bundle,
    manifest_path: Path,
    destination_dir: Path,
    signer: ManifestSigner | None,
) -> Path:
    """Wrap rem's manifest with an archive id, timestamp, and keyed signature."""
    if signer is None:
        raise ManifestSigningError("customer manifest requires a keyed signer")
    destination_dir.mkdir(parents=True, exist_ok=True)
    source = _read_json(manifest_path)
    archive_id = bundle.archive_id or f"archive-{bundle.id}"
    payload = {
        "archive_id": archive_id,
        "bundle_id": bundle.id,
        "artifactclass": bundle.artifactclass,
        "ruleset": bundle.ruleset,
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
    map_path: Path | None = None,
    source_root: Path | None = None,
    map_sha256: str | None = None,
) -> BuildArtifact:
    representation = Representation(target.representation)
    if representation is Representation.D2TAR_RAW:
        return _build_d2_tar(bundle, members, work_dir)
    return builder.build(
        bundle=bundle,
        members=members,
        representation=representation,
        ruleset=bundle.ruleset or "",
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
    key_epoch: str | None,
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
    if representation == Representation.RAO_AEAD_V1.value and key_epoch is not None:
        metadata["key_epoch"] = key_epoch
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
    for exclusion in artifact.exclusions:
        record_exclusion(
            session,
            bundle_id=bundle.id,
            artifactclass=bundle.artifactclass,
            path=exclusion.path,
            reason=exclusion.reason,
            count=exclusion.count,
            bytes_total=exclusion.bytes_total,
            ruleset_name=bundle.ruleset,
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
) -> None:
    result = backend.verify(copy_locator)
    if not result.ok:
        raise ArchiveFanoutError(f"backend verify failed: {result.detail}")
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


def _member_input(member: BundleMember) -> MemberInput:
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
    )


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


def _rem_input_paths(members: Sequence[MemberInput]) -> list[Path]:
    """Return build roots that make rem member paths match the catalog paths."""

    roots: list[Path] = []
    seen: set[Path] = set()
    for member in members:
        source = Path(member.source_path)
        parts = PurePosixPath(member.member_path).parts
        root = source
        if parts and len(source.parents) >= len(parts):
            root = source.parents[len(parts) - 1]
        if root not in seen:
            roots.append(root)
            seen.add(root)
    return roots


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
    by_path = {member.member_path: member for member in inputs}
    raw_members = manifest.get("members", [])
    if not raw_members:
        raise ArchiveFanoutError("rem manifest did not include member locators")
    built: list[BuiltMember] = []
    for item in raw_members:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or item.get("member_path"))
        if path not in by_path:
            raise ArchiveFanoutError(f"rem manifest returned unknown member path {path!r}")
        source = by_path[path]
        built.append(
            BuiltMember(
                logical_asset_hash=source.logical_asset_hash,
                member_path=path,
                size_bytes=int(item.get("size_bytes", source.size_bytes)),
                file_sha256=bytes.fromhex(str(item.get("sha256", source.file_sha256.hex()))),
                native_locator={
                    "member_path": path,
                    "first_chunk_lba": int(item.get("first_chunk_lba", 0)),
                    "size_bytes": int(item.get("size_bytes", source.size_bytes)),
                },
                ingest_item_id=(
                    None if item.get("ingest_item_id") is None else str(item.get("ingest_item_id"))
                ),
            )
        )
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


def _first_chunk_lba(locator: Mapping[str, Any]) -> int:
    value = locator.get("first_chunk_lba")
    if value is None:
        raise ArchiveFanoutError("RAO locator requires first_chunk_lba")
    result = int(value)
    if result < 0:
        raise ArchiveFanoutError(f"invalid first_chunk_lba {value!r}")
    return result


def _metadata_key_epoch(storage_metadata: Mapping[str, Any]) -> str:
    value = storage_metadata.get("key_epoch")
    if not isinstance(value, str) or not value:
        raise ArchiveFanoutError("encrypted copy metadata is missing key_epoch")
    return value


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
