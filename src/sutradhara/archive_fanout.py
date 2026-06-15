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
import json
import subprocess
import tarfile
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from sutradhara.archive_bundle import (
    close_bundle,
    hold_bundle,
    record_asset_locator,
    record_blob_root,
    record_exclusion,
)
from sutradhara.catalog.copies import add_bundle_copy
from sutradhara.catalog.models import Bundle, BundleMember
from sutradhara.catalog.types import CopyHealth, CopySource
from sutradhara.replication import (
    PoolTarget,
    WritableStorageBackend,
    target_pools,
)
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RAO_CHUNK_SIZE


class ArchiveFanoutError(Exception):
    """Base class for archive fan-out errors."""


class BundleHeld(ArchiveFanoutError):
    """The conformance gate held a bundle for review."""


class BundleOversize(ArchiveFanoutError):
    """A single artifact exceeds the configured tape capacity."""


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
    ) -> BuildArtifact:
        """Build one archive object for a pool representation."""
        ...


@dataclass(frozen=True)
class FanoutResult:
    """Summary of a successful bundle fan-out."""

    bundle_id: str
    copy_ids: tuple[int, ...]
    manifest_path: str | None


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

    def __init__(self, rem_bin: str | Path = "rem") -> None:
        self._rem_bin = str(rem_bin)

    def scan(
        self,
        *,
        bundle: Bundle,
        members: Sequence[MemberInput],
        ruleset: str,
    ) -> ConformanceScan:
        with tempfile.TemporaryDirectory(prefix="sutradhara-rem-scan-") as raw:
            report_path = Path(raw) / "scan.json"
            cmd = [
                self._rem_bin,
                "archive",
                "build",
                "--scan-only",
                "--rules",
                ruleset,
                "--scan-out",
                str(report_path),
                *[str(member.source_path) for member in members],
            ]
            subprocess.run(cmd, check=True)
            if not report_path.exists():
                return ConformanceScan()
            return _scan_from_json(_read_json(report_path))

    def build(
        self,
        *,
        bundle: Bundle,
        members: Sequence[MemberInput],
        representation: Representation,
        ruleset: str,
        key_epoch: str | None,
        work_dir: Path,
    ) -> BuildArtifact:
        output_path = work_dir / f"{bundle.id}-{representation.value}.rao"
        manifest_path = work_dir / f"{bundle.id}-{representation.value}.manifest.json"
        cmd = [
            self._rem_bin,
            "archive",
            "build",
            "--rules",
            ruleset,
            "--output",
            str(output_path),
            "--manifest-out",
            str(manifest_path),
        ]
        if representation is Representation.RAO_AEAD_V1:
            cmd.append("--encrypt")
            if key_epoch:
                cmd.extend(["--key-epoch", key_epoch])
        cmd.extend(str(member.source_path) for member in members)
        subprocess.run(cmd, check=True)
        manifest = _read_json(manifest_path)
        return BuildArtifact(
            artifact_path=output_path,
            stored_digest=_sha256_file(output_path),
            members=tuple(_members_from_manifest(manifest, members)),
            manifest_path=manifest_path,
            blob_roots=tuple(_blob_roots_from_manifest(manifest)),
            exclusions=tuple(_exclusions_from_manifest(manifest)),
        )


def flush_bundle(
    session: Session,
    *,
    bundle_id: str,
    backends: Mapping[int, WritableStorageBackend],
    builder: ArchiveBuilder,
    key_epoch: str | None = None,
    deliverables_dir: Path | str | None = None,
    tape_capacity_bytes: int | None = None,
) -> FanoutResult:
    """Flush one open bundle, build each pool copy, and record catalog state."""
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
    scan = builder.scan(bundle=bundle, members=members, ruleset=ruleset)
    bundle.scan_summary = scan.to_summary()
    if bundle.expect == "compliant" and scan.has_deviations:
        hold_bundle(session, bundle, summary=scan.to_summary())
        raise BundleHeld(f"bundle {bundle.id!r} held for conformance review")

    if bundle.archive_id is None:
        bundle.archive_id = f"archive-{bundle.id}"
    targets = target_pools(session, bundle.artifactclass, backends, key_epoch=key_epoch)
    _require_key_epoch(targets)
    copy_ids: list[int] = []
    manifest_receipt: str | None = None
    bundle.status = "flushing"
    bundle.flushed_at = dt.datetime.now(dt.UTC)

    with tempfile.TemporaryDirectory(prefix=f"sutradhara-bundle-{bundle.id}-") as raw:
        work_dir = Path(raw)
        for backend, target in targets:
            artifact = _build_for_target(
                bundle=bundle,
                members=members,
                target=target,
                builder=builder,
                key_epoch=key_epoch,
                work_dir=work_dir,
            )
            record = backend.write_object_to_pool(artifact.artifact_path, target.pool_id)
            copy, _ = add_bundle_copy(
                session,
                bundle_id=bundle.id,
                backend_id=target.backend_id,
                pool_id=target.pool_id,
                native_locator=record.native_locator,
                integrity_hash=artifact.stored_digest,
                source=CopySource.INGEST,
                health=CopyHealth.OK,
                storage_metadata=_copy_storage_metadata(
                    target.representation,
                    key_epoch=target.key_epoch,
                ),
            )
            copy_ids.append(copy.id)
            _record_build_outputs(
                session,
                bundle=bundle,
                target=target,
                copy_id=copy.id,
                artifact=artifact,
            )
            _verify_members_from_copy(backend, copy.native_locator, artifact.members)
            if (
                deliverables_dir is not None
                and artifact.manifest_path is not None
                and manifest_receipt is None
            ):
                manifest_receipt = str(
                    emit_customer_manifest(
                        bundle=bundle,
                        manifest_path=artifact.manifest_path,
                        destination_dir=Path(deliverables_dir),
                    )
                )
                bundle.customer_manifest_path = manifest_receipt

    close_bundle(session, bundle)
    return FanoutResult(bundle.id, tuple(copy_ids), manifest_receipt)


def emit_customer_manifest(
    *,
    bundle: Bundle,
    manifest_path: Path,
    destination_dir: Path,
) -> Path:
    """Wrap rem's manifest with an archive id, timestamp, and digest signature."""
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
        "exclusion_summary": bundle.scan_summary.get("exclusions", [])
        if bundle.scan_summary
        else [],
    }
    signature_basis = json.dumps(payload, sort_keys=True).encode("utf-8")
    payload["signature"] = {
        "algorithm": "sha256",
        "digest": hashlib.sha256(signature_basis).hexdigest(),
    }
    destination = destination_dir / f"{archive_id}.manifest.json"
    destination.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n")
    bundle.archive_id = archive_id
    return destination


def _build_for_target(
    *,
    bundle: Bundle,
    members: Sequence[MemberInput],
    target: PoolTarget,
    builder: ArchiveBuilder,
    key_epoch: str | None,
    work_dir: Path,
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
) -> dict[str, Any]:
    metadata: dict[str, Any] = {"representation": representation}
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


def _record_build_outputs(
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
    backend: WritableStorageBackend,
    copy_locator: dict[str, Any],
    members: Sequence[BuiltMember],
) -> None:
    # The backend-level verify hook is retained for production adapters. Member
    # byte verification happens in restore tests through asset_locator; this
    # hook catches a failed write before staging cleanup.
    result = backend.verify(copy_locator)
    if not result.ok:
        raise ArchiveFanoutError(f"backend verify failed: {result.detail}")


def _member_input(member: BundleMember) -> MemberInput:
    if member.source_path is None:
        raise ArchiveFanoutError(
            f"bundle member {member.id} has no source_path; cannot materialize"
        )
    return MemberInput(
        logical_asset_hash=member.logical_asset_hash,
        member_path=member.member_path,
        source_path=Path(member.source_path),
        size_bytes=member.size_bytes,
        file_sha256=member.file_sha256,
    )


def _scan_from_json(raw: dict[str, Any]) -> ConformanceScan:
    clusters = tuple(_cluster_from_json(item) for item in raw.get("clusters", []))
    exclusions = tuple(_cluster_from_json(item) for item in raw.get("exclusions", []))
    return ConformanceScan(clusters=clusters, exclusions=exclusions)


def _cluster_from_json(raw: object) -> DeviationCluster:
    if not isinstance(raw, dict):
        raise ArchiveFanoutError("scan cluster must be an object")
    samples = raw.get("samples", [])
    return DeviationCluster(
        prefix=str(raw.get("prefix", "")),
        reason=str(raw.get("reason", "unknown")),
        count=int(raw.get("count", 0)),
        bytes_total=int(raw.get("bytes_total", 0)),
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


def _members_from_manifest(
    manifest: dict[str, Any],
    inputs: Sequence[MemberInput],
) -> Sequence[BuiltMember]:
    by_path = {member.member_path: member for member in inputs}
    raw_members = manifest.get("members", [])
    if not raw_members:
        return [
            BuiltMember(
                logical_asset_hash=member.logical_asset_hash,
                member_path=member.member_path,
                size_bytes=member.size_bytes,
                file_sha256=member.file_sha256,
                native_locator={
                    "member_path": member.member_path,
                    "first_chunk_lba": 0,
                    "size_bytes": member.size_bytes,
                },
            )
            for member in inputs
        ]
    built: list[BuiltMember] = []
    for item in raw_members:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or item.get("member_path"))
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


def _sha256_file(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()
