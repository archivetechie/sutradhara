"""Artifactclass-aware restore from bundle asset locators.

Restore policy is copy-independent: choose the first healthy locator according
to the artifactclass policy's ordered pool preference, extract bytes from that
copy's representation, and verify the plaintext SHA-256 against the logical
asset identity.
"""

from __future__ import annotations

import hashlib
import io
import json
import shutil
import tarfile
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Protocol, cast

import zstandard as zstd
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload
from typing_extensions import Buffer

from sutradhara.artifactclass_policy import get_artifactclass_policy
from sutradhara.backend.port import BackendError, ByteRange, StorageBackend, StreamingStorageBackend
from sutradhara.catalog.models import (
    ArtifactClassPool,
    AssetLocator,
    Bundle,
    BundleMember,
    Copy,
    LogicalAsset,
    StagingTransform,
)
from sutradhara.catalog.types import AssetValidity, CopyHealth, is_content_hash
from sutradhara.durability import locator_artifactclass_filter
from sutradhara.keys import KeyRegistry
from sutradhara.resource_control import run_managed
from sutradhara.restore import (
    RestoreIntegrityError as ChunkRestoreIntegrityError,
)
from sutradhara.restore import (
    atomic_write_verified_chunks,
)
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RAO_CHUNK_SIZE
from sutradhara.staging import StagingError
from sutradhara_receive.member_name import (
    MemberNameError,
    escape_member_name,
    unescape_member_name,
)

_MAX_LOCAL_ARCHIVE_HEADER_BYTES = 16 * 1024 * 1024


class ArchiveRestoreError(Exception):
    """Base class for archive restore errors."""


class RestoreSourceUnavailable(ArchiveRestoreError):
    """No healthy copy could satisfy a restore request."""


class RestoreIntegrityError(ArchiveRestoreError):
    """Restored bytes do not match the logical asset hash."""


class StoredMemberIntegrityError(ArchiveRestoreError):
    """A selected member range does not match its staged/member digest."""


class LogicalMemberIntegrityError(ArchiveRestoreError):
    """A clean stored member does not recover to its logical asset identity."""


class RestoreNameError(ArchiveRestoreError):
    """A customer member-name restore request could not be resolved."""


class RestoreSuspectAsset(ArchiveRestoreError):
    """A normal restore was refused because the asset is flagged suspect."""


class RestoreRejectedAsset(ArchiveRestoreError):
    """A normal restore was refused because the asset is rejected."""


@dataclass(frozen=True)
class RestoreResult:
    """Completed restore details."""

    asset_hash: bytes
    pool_id: str
    copy_id: int
    output_path: Path
    size_bytes: int


@dataclass(frozen=True)
class PlannedMember:
    """One typed copy/locator/transform candidate in a restore plan."""

    asset_hash: bytes
    expected_logical_size: int
    expected_stored_sha256: bytes
    pool_id: str
    copy: Copy
    locator: AssetLocator
    transforms: tuple[StagingTransform, ...]
    backend: StorageBackend
    buffered: bool


class _ChunkIteratorReader(io.RawIOBase):
    """Adapt pull-driven chunks to the ``readinto`` API used by zstandard."""

    def __init__(self, chunks: Iterator[bytes]) -> None:
        self._chunks = chunks
        self._current = memoryview(b"")

    def readable(self) -> bool:
        return True

    def readinto(self, target: Buffer) -> int:
        view = memoryview(target).cast("B")
        while not self._current:
            try:
                chunk = next(self._chunks)
            except StopIteration:
                return 0
            if not isinstance(chunk, bytes):
                raise TypeError("archive member stream yielded a non-bytes chunk")
            self._current = memoryview(chunk)
        count = min(len(view), len(self._current))
        view[:count] = self._current[:count]
        self._current = self._current[count:]
        return count


class RestorePlan:
    """Built restore selection whose member streams execute only when opened.

    Single-asset plans contain ordered copy/locator candidates. Bundle plans
    contain the members of exactly one all-covering copy group. Streamable
    representations stay pull-driven; AEAD and D2 tar members lacking a block
    range retain the existing scratch/extractor machinery.
    """

    def __init__(
        self,
        members: list[PlannedMember],
        *,
        extractor: ArchiveExtractor,
        bundle_group: bool,
    ) -> None:
        self._members = tuple(members)
        self._extractor = extractor
        self._bundle_group = bundle_group
        self._bundle_temp: tempfile.TemporaryDirectory[str] | None = None
        self._bundle_paths: dict[bytes, Path] | None = None

    def __enter__(self) -> RestorePlan:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Release any shared buffered bundle materialization."""

        if self._bundle_temp is not None:
            self._bundle_temp.cleanup()
            self._bundle_temp = None
            self._bundle_paths = None

    def iter_members(self) -> Iterator[PlannedMember]:
        """Yield candidates in their selection order."""

        return iter(self._members)

    @contextmanager
    def open_member_stream(self, member: PlannedMember) -> Iterator[Iterator[bytes]]:
        """Yield verified logical plaintext chunks for one planned member."""

        if member not in self._members:
            raise ValueError("planned member does not belong to this restore plan")
        with self._open_stored_stream(member) as stored_chunks:
            identity_is_logical = not any(item.reversible for item in member.transforms) and (
                member.expected_stored_sha256 == member.asset_hash
            )
            verified_stored = _verify_stored_chunks(
                stored_chunks,
                expected_sha256=member.expected_stored_sha256,
                copy_id=member.copy.id,
                mismatch_is_logical=identity_is_logical,
            )
            logical_chunks = _reverse_transform_chunks(verified_stored, member.transforms)
            yield _verify_logical_chunks(
                logical_chunks,
                expected_sha256=member.asset_hash,
                expected_size=member.expected_logical_size,
                copy_id=member.copy.id,
            )

    @contextmanager
    def _open_stored_stream(self, member: PlannedMember) -> Iterator[Iterator[bytes]]:
        if self._bundle_group and any(item.buffered for item in self._members):
            paths = self._ensure_buffered_bundle()
            with paths[member.asset_hash].open("rb") as handle:
                yield _file_chunks(handle)
            return
        if member.buffered:
            with tempfile.TemporaryDirectory(prefix=".sutradhara-plan-member-") as raw:
                path = Path(raw) / "stored"
                self._extractor.extract_to_path(
                    locator=member.locator,
                    copy=member.copy,
                    backend=member.backend,
                    destination=path,
                )
                with path.open("rb") as handle:
                    yield _file_chunks(handle)
            return
        with _open_locator_range_chunks(member) as chunks:
            yield chunks

    def _ensure_buffered_bundle(self) -> dict[bytes, Path]:
        if self._bundle_paths is not None:
            return self._bundle_paths
        self._bundle_temp = tempfile.TemporaryDirectory(prefix=".sutradhara-plan-bundle-")
        root = Path(self._bundle_temp.name)
        paths = {member.asset_hash: root / member.asset_hash.hex() for member in self._members}
        first = self._members[0]
        _extract_bundle_to_paths(
            self._extractor,
            locators=[member.locator for member in self._members],
            copy=first.copy,
            backend=first.backend,
            destinations=paths,
        )
        self._bundle_paths = paths
        return paths


class ArchiveExtractor(Protocol):
    """Per-copy archive extraction boundary."""

    def extract_to_path(
        self,
        *,
        locator: AssetLocator,
        copy: Copy,
        backend: StorageBackend,
        destination: Path,
    ) -> None:
        """Write the stored member bytes for one locator from one stored copy."""
        ...


class BundleArchiveExtractor(ArchiveExtractor, Protocol):
    """Extractor variant that can share copy materialization across members."""

    def extract_bundle_to_paths(
        self,
        *,
        locators: list[AssetLocator],
        copy: Copy,
        backend: StorageBackend,
        destinations: dict[bytes, Path],
    ) -> None:
        """Write several stored members from one bundle copy to paths keyed by hash."""
        ...


class LocalArchiveExtractor:
    """Extractor for d2 tar copies, local test archives, and offset locators."""

    def extract_to_path(
        self,
        *,
        locator: AssetLocator,
        copy: Copy,
        backend: StorageBackend,
        destination: Path,
    ) -> None:
        read_member_to_path(
            backend=backend,
            copy=copy,
            asset_locator=locator,
            dest=destination,
        )

    def extract_bundle_to_paths(
        self,
        *,
        locators: list[AssetLocator],
        copy: Copy,
        backend: StorageBackend,
        destinations: dict[bytes, Path],
    ) -> None:
        for locator in locators:
            self.extract_to_path(
                locator=locator,
                copy=copy,
                backend=backend,
                destination=destinations[locator.logical_asset_hash],
            )


class RemArchiveExtractor(LocalArchiveExtractor):
    """Extractor that delegates RAO bundle extraction to the rem CLI."""

    def __init__(
        self,
        rem_bin: str | Path = "rem",
        *,
        keys: KeyRegistry | None = None,
    ) -> None:
        self._rem_bin = str(rem_bin)
        self._keys = keys or KeyRegistry()

    def extract_to_path(
        self,
        *,
        locator: AssetLocator,
        copy: Copy,
        backend: StorageBackend,
        destination: Path,
    ) -> None:
        read_member_to_path(
            backend=backend,
            copy=copy,
            asset_locator=locator,
            dest=destination,
            rem_bin=self._rem_bin,
            keys=self._keys,
        )

    def extract_bundle_to_paths(
        self,
        *,
        locators: list[AssetLocator],
        copy: Copy,
        backend: StorageBackend,
        destinations: dict[bytes, Path],
    ) -> None:
        if not locators:
            return
        representation = Representation(locators[0].representation)
        if representation is Representation.D2TAR_RAW:
            _extract_d2_bundle_to_paths(locators, copy, backend, destinations)
            return
        if representation in {Representation.RAO_PLAIN_V1, Representation.RAO_AEAD_V1}:
            _extract_rao_bundle_with_rem_to_paths(
                backend=backend,
                copy=copy,
                locators=locators,
                destinations=destinations,
                rem_bin=self._rem_bin,
                keys=self._keys,
            )
            return
        super().extract_bundle_to_paths(
            locators=locators,
            copy=copy,
            backend=backend,
            destinations=destinations,
        )


def read_member_to_path(
    backend: StorageBackend,
    copy: Any,
    asset_locator: Any,
    dest: Path,
    *,
    rem_bin: str | Path | None = None,
    keys: KeyRegistry | None = None,
    work_dir: Path | None = None,
) -> int:
    """Write one archive member's stored bytes to ``dest``.

    This is the shared member-read primitive for restore and build-time
    verification. It streams through a destination path so large members do not
    have to be held in memory; the bytes wrapper below is only for callers that
    already need an in-memory verification value.
    """
    native_locator = dict(asset_locator.native_locator)
    size = _size_bytes(native_locator)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if size == 0:
        dest.write_bytes(b"")
        return 0

    representation = Representation(asset_locator.representation)
    if representation is Representation.D2TAR_RAW:
        _extract_d2_to_path(asset_locator, copy, backend, dest)
        return size

    if "offset" in native_locator:
        if _try_extract_from_local_archive_to_path(backend, copy, asset_locator, dest):
            return size
        offset = int(native_locator["offset"])
        _copy_backend_range_to_path(
            backend,
            dict(copy.native_locator),
            offset,
            offset + size,
            dest,
        )
        return size

    if representation is Representation.RAO_PLAIN_V1:
        start = member_byte_base(native_locator)
        _copy_backend_range_to_path(
            backend,
            dict(copy.native_locator),
            start,
            start + size,
            dest,
        )
        return size

    if representation is Representation.RAO_AEAD_V1:
        if rem_bin is None:
            copy_id = getattr(copy, "id", None)
            label = f"copy id={copy_id}" if copy_id is not None else "copy"
            raise ArchiveRestoreError(
                f"{label} representation {representation.value!r} requires a RAO restore adapter"
            )
        _extract_rao_with_rem_to_path(
            backend=backend,
            copy=copy,
            locator=asset_locator,
            representation=representation,
            destination=dest,
            rem_bin=rem_bin,
            keys=keys or KeyRegistry(),
            work_dir=work_dir,
        )
        return size

    raise ArchiveRestoreError(f"unsupported archive representation {representation.value!r}")


def read_member_bytes(
    backend: StorageBackend,
    copy: Any,
    asset_locator: Any,
    *,
    work_dir: Path | None = None,
    rem_bin: str | Path | None = None,
    keys: KeyRegistry | None = None,
) -> bytes:
    """Return member bytes via ``read_member_to_path`` for verification tests."""
    with tempfile.TemporaryDirectory(
        prefix="sutradhara-member-read-",
        dir=work_dir,
    ) as raw_tmp:
        member_path = Path(raw_tmp) / "member"
        read_member_to_path(
            backend=backend,
            copy=copy,
            asset_locator=asset_locator,
            dest=member_path,
            rem_bin=rem_bin,
            keys=keys,
            work_dir=work_dir,
        )
        return member_path.read_bytes()


def build_restore_plan(
    session: Session,
    *,
    asset_hash: bytes,
    artifactclass: str,
    backends: dict[int, StorageBackend],
    extractor: ArchiveExtractor | None = None,
) -> RestorePlan:
    """Build ordered typed candidates using the user-restore selector."""

    from sutradhara.durability import AssetTarget
    from sutradhara.replication import select_source_candidates

    archive_extractor = extractor or LocalArchiveExtractor()
    locators = list(
        session.scalars(
            select(AssetLocator)
            .outerjoin(Bundle, AssetLocator.bundle_id == Bundle.id)
            .where(
                AssetLocator.logical_asset_hash == asset_hash,
                locator_artifactclass_filter(session, asset_hash, artifactclass),
            )
            .order_by(AssetLocator.id)
        )
    )
    locators_by_copy: dict[int, list[AssetLocator]] = {}
    for locator in locators:
        if locator.copy_id is not None:
            locators_by_copy.setdefault(locator.copy_id, []).append(locator)

    members: list[PlannedMember] = []
    # Parity with pre-RM0.2 restore_asset: only locators whose pool is in the
    # artifactclass restore pool order are eligible. The user-restore selector ranks
    # (never excludes) non-preferred pools, so without this gate a copy in an
    # excluded/retired pool would leak in as a last-resort restore source — a policy
    # divergence (not a corruption; bytes are still verified). RM0.2 diff-gate defect.
    policy = get_artifactclass_policy(session, artifactclass)
    pool_order = set(_restore_pool_order(session, artifactclass, policy.restore_preference))
    candidates = select_source_candidates(
        session,
        AssetTarget(asset_hash=asset_hash, artifactclass=artifactclass),
        purpose="user_restore",
    )
    for copy in candidates:
        if copy.health != CopyHealth.OK or copy.deleted_at is not None:
            continue
        backend = backends.get(copy.backend_id)
        if backend is None:
            continue
        for locator in locators_by_copy.get(copy.id, []):
            if locator.pool_id not in pool_order:
                continue
            members.append(
                _planned_member(
                    session,
                    locator=locator,
                    copy=copy,
                    backend=backend,
                    extractor=archive_extractor,
                )
            )
    return RestorePlan(members, extractor=archive_extractor, bundle_group=False)


def build_bundle_restore_plan(
    session: Session,
    *,
    asset_hashes: list[bytes],
    artifactclass: str,
    backends: dict[int, StorageBackend],
    extractor: BundleArchiveExtractor | ArchiveExtractor | None = None,
) -> RestorePlan:
    """Build one all-covering bundle group without cross-group retry."""

    archive_extractor = extractor or LocalArchiveExtractor()
    chosen = _choose_bundle_restore_group(
        session,
        asset_hashes,
        artifactclass,
        backends=backends,
    )
    if chosen is None:
        return RestorePlan([], extractor=archive_extractor, bundle_group=True)
    _pool_id, copy, backend, locator_by_hash = chosen
    members = [
        _planned_member(
            session,
            locator=locator_by_hash[asset_hash],
            copy=copy,
            backend=backend,
            extractor=archive_extractor,
        )
        for asset_hash in asset_hashes
    ]
    return RestorePlan(members, extractor=archive_extractor, bundle_group=True)


def _planned_member(
    session: Session,
    *,
    locator: AssetLocator,
    copy: Copy,
    backend: StorageBackend,
    extractor: ArchiveExtractor,
) -> PlannedMember:
    asset = session.get(LogicalAsset, locator.logical_asset_hash)
    if asset is None:
        raise RestoreSourceUnavailable(
            f"logical asset {locator.logical_asset_hash.hex()} disappeared while planning restore"
        )
    transforms = tuple(_locator_transforms(session, locator))
    stored_sha256 = _expected_stored_member_sha256(session, locator, transforms)
    representation = Representation(locator.representation)
    trusted_extractor = isinstance(extractor, (LocalArchiveExtractor, RemArchiveExtractor))
    buffered = (
        not trusted_extractor
        or representation is Representation.RAO_AEAD_V1
        or (
            representation is Representation.D2TAR_RAW
            and "block_range" not in locator.native_locator
        )
    )
    return PlannedMember(
        asset_hash=locator.logical_asset_hash,
        expected_logical_size=asset.size_bytes,
        expected_stored_sha256=stored_sha256,
        pool_id=locator.pool_id,
        copy=copy,
        locator=locator,
        transforms=transforms,
        backend=backend,
        buffered=buffered,
    )


def _expected_stored_member_sha256(
    session: Session,
    locator: AssetLocator,
    transforms: tuple[StagingTransform, ...],
) -> bytes:
    reversible = [transform for transform in transforms if transform.reversible]
    if reversible:
        return max(reversible, key=lambda item: item.step_order).stored_sha256
    if locator.bundle_id is not None:
        digest = session.scalar(
            select(BundleMember.file_sha256).where(
                BundleMember.bundle_id == locator.bundle_id,
                BundleMember.logical_asset_hash == locator.logical_asset_hash,
                BundleMember.member_path == locator.member_path,
            )
        )
        if digest is not None:
            return digest
    return locator.logical_asset_hash


def restore_asset(
    session: Session,
    *,
    asset_hash: bytes,
    artifactclass: str,
    destination: Path | str,
    backends: dict[int, StorageBackend],
    extractor: ArchiveExtractor | None = None,
    force_suspect: bool = False,
    force_rejected: bool = False,
) -> RestoreResult:
    """Restore one asset from the first candidate that reaches verified EOF."""
    if not is_content_hash(asset_hash):
        raise ValueError("asset_hash must be a 32-byte SHA-256 hash")
    check_asset_restore_allowed(
        session,
        asset_hash,
        force_suspect=force_suspect,
        force_rejected=force_rejected,
    )
    output_path = Path(destination).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plan = build_restore_plan(
        session,
        asset_hash=asset_hash,
        artifactclass=artifactclass,
        backends=backends,
        extractor=extractor,
    )

    integrity_errors: list[str] = []
    with plan:
        for member in plan.iter_members():
            try:
                with plan.open_member_stream(member) as chunks:
                    atomic_write_verified_chunks(
                        chunks,
                        output_path,
                        expected_sha256=member.asset_hash,
                        expected_size_bytes=member.expected_logical_size,
                    )
                return RestoreResult(
                    asset_hash=asset_hash,
                    pool_id=member.pool_id,
                    copy_id=member.copy.id,
                    output_path=output_path,
                    size_bytes=member.expected_logical_size,
                )
            except LogicalMemberIntegrityError as exc:
                member.copy.health = CopyHealth.SUSPECT
                integrity_errors.append(f"copy id={member.copy.id} pool={member.pool_id}: {exc}")
                continue
            except (
                ArchiveRestoreError,
                BackendError,
                StagingError,
                ChunkRestoreIntegrityError,
            ) as exc:
                integrity_errors.append(f"copy id={member.copy.id} pool={member.pool_id}: {exc}")
                continue

    if integrity_errors:
        raise RestoreIntegrityError(
            f"all candidate restores for asset {asset_hash.hex()} failed integrity: "
            + "; ".join(integrity_errors)
        )
    raise RestoreSourceUnavailable(
        f"no healthy locator for asset {asset_hash.hex()} in artifactclass {artifactclass!r}"
    )


def restore_assets_from_bundle(
    session: Session,
    *,
    asset_hashes: list[bytes],
    artifactclass: str,
    destination_dir: Path | str,
    backends: dict[int, StorageBackend],
    extractor: BundleArchiveExtractor | ArchiveExtractor | None = None,
    force_suspect: bool = False,
    force_rejected: bool = False,
) -> list[RestoreResult]:
    """Restore several members from one bundle copy with shared materialization.

    The caller supplies members that should be co-located in archive truth. The
    function chooses the first healthy copy, in artifactclass restore-pool order,
    that has locators for every requested hash. Each member still reverses its
    own staging transforms and verifies against its logical asset hash before a
    result is returned.
    """

    if not asset_hashes:
        return []
    unique_hashes: list[bytes] = []
    seen: set[bytes] = set()
    for asset_hash in asset_hashes:
        if not is_content_hash(asset_hash):
            raise ValueError("asset_hashes must contain 32-byte SHA-256 hashes")
        if asset_hash in seen:
            continue
        check_asset_restore_allowed(
            session,
            asset_hash,
            force_suspect=force_suspect,
            force_rejected=force_rejected,
        )
        seen.add(asset_hash)
        unique_hashes.append(asset_hash)

    destination_root = Path(destination_dir).resolve()
    destination_root.mkdir(parents=True, exist_ok=True)
    plan = build_bundle_restore_plan(
        session,
        asset_hashes=unique_hashes,
        artifactclass=artifactclass,
        backends=backends,
        extractor=extractor,
    )
    members = list(plan.iter_members())
    if not members:
        hashes = ", ".join(asset_hash.hex() for asset_hash in unique_hashes)
        raise RestoreSourceUnavailable(
            f"no healthy bundle copy can restore all requested assets for "
            f"artifactclass {artifactclass!r}: {hashes}"
        )
    with plan:
        results_by_hash: dict[bytes, RestoreResult] = {}
        for member in members:
            output_path = destination_root / member.asset_hash.hex()
            try:
                with plan.open_member_stream(member) as chunks:
                    atomic_write_verified_chunks(
                        chunks,
                        output_path,
                        expected_sha256=member.asset_hash,
                        expected_size_bytes=member.expected_logical_size,
                    )
            except (
                ArchiveRestoreError,
                BackendError,
                StagingError,
                ChunkRestoreIntegrityError,
            ) as exc:
                raise RestoreIntegrityError(
                    f"bundle restore copy id={member.copy.id} pool={member.pool_id}: {exc}"
                ) from exc
            results_by_hash[member.asset_hash] = RestoreResult(
                asset_hash=member.asset_hash,
                pool_id=member.pool_id,
                copy_id=member.copy.id,
                output_path=output_path,
                size_bytes=member.expected_logical_size,
            )
    return [results_by_hash[asset_hash] for asset_hash in asset_hashes]


def check_asset_restore_allowed(
    session: Session,
    asset_hash: bytes,
    *,
    force_suspect: bool,
    force_rejected: bool,
) -> None:
    """Apply the operator restore validity gate shared by tape and hdcache serves."""

    _check_asset_restore_allowed(
        session,
        asset_hash,
        force_suspect=force_suspect,
        force_rejected=force_rejected,
    )


def _check_asset_restore_allowed(
    session: Session,
    asset_hash: bytes,
    *,
    force_suspect: bool,
    force_rejected: bool,
) -> None:
    asset = session.get(LogicalAsset, asset_hash)
    if asset is None:
        return
    if asset.validity == AssetValidity.SUSPECT and not force_suspect:
        note = f": {asset.validity_note}" if asset.validity_note else ""
        raise RestoreSuspectAsset(
            f"asset {asset_hash.hex()} is flagged suspect{note}; use --force to restore anyway"
        )
    if asset.rejected_at is not None and not force_rejected:
        note = f": {asset.rejection_reason}" if asset.rejection_reason else ""
        raise RestoreRejectedAsset(
            f"asset {asset_hash.hex()} is rejected{note}; use --force-rejected to restore anyway"
        )


def resolve_member_asset_hash(
    session: Session,
    *,
    artifactclass: str,
    member_name: str,
) -> bytes:
    """Resolve a customer escaped member name to a logical asset hash."""
    try:
        canonical = escape_member_name(unescape_member_name(member_name))
    except MemberNameError as exc:
        raise RestoreNameError(f"invalid escaped member name {member_name!r}: {exc}") from exc

    hashes: set[bytes] = set(
        session.scalars(
            select(StagingTransform.logical_asset_hash).where(
                StagingTransform.artifactclass == artifactclass,
                (
                    (StagingTransform.original_member_path == canonical)
                    | (StagingTransform.stored_member_path == canonical)
                ),
            )
        )
    )
    hashes.update(
        session.scalars(
            select(BundleMember.logical_asset_hash)
            .join(Bundle, Bundle.id == BundleMember.bundle_id)
            .where(
                Bundle.artifactclass == artifactclass,
                BundleMember.member_path == canonical,
            )
        )
    )
    if not hashes:
        raise RestoreNameError(
            f"no catalog member {member_name!r} in artifactclass {artifactclass!r}"
        )
    if len(hashes) > 1:
        raise RestoreNameError(
            f"member name {member_name!r} is ambiguous in artifactclass {artifactclass!r}"
        )
    return next(iter(hashes))


def _locator_transforms(
    session: Session,
    locator: AssetLocator,
) -> list[StagingTransform]:
    if locator.bundle_id is None:
        return []
    member_id = session.scalar(
        select(StagingTransform.bundle_member_id)
        .where(
            StagingTransform.bundle_id == locator.bundle_id,
            StagingTransform.logical_asset_hash == locator.logical_asset_hash,
            StagingTransform.stored_member_path == locator.member_path,
        )
        .order_by(StagingTransform.step_order.desc())
        .limit(1)
    )
    if member_id is None:
        return []
    return list(
        session.scalars(
            select(StagingTransform)
            .where(
                StagingTransform.bundle_member_id == member_id,
            )
            .order_by(StagingTransform.step_order)
        )
    )


def _choose_bundle_restore_group(
    session: Session,
    asset_hashes: list[bytes],
    artifactclass: str,
    *,
    backends: dict[int, StorageBackend],
) -> tuple[str, Copy, StorageBackend, dict[bytes, AssetLocator]] | None:
    policy = get_artifactclass_policy(session, artifactclass)
    pool_order = _restore_pool_order(session, artifactclass, policy.restore_preference)
    pool_rank = {pool_id: index for index, pool_id in enumerate(pool_order)}
    if not pool_rank:
        return None
    locators = list(
        session.scalars(
            select(AssetLocator)
            .options(joinedload(AssetLocator.copy).joinedload(Copy.backend))
            .outerjoin(Bundle, AssetLocator.bundle_id == Bundle.id)
            .where(
                AssetLocator.logical_asset_hash.in_(asset_hashes),
                Bundle.artifactclass == artifactclass,
            )
        )
    )
    grouped: dict[tuple[int, str, int, str], dict[bytes, AssetLocator]] = {}
    copy_by_key: dict[tuple[int, str, int, str], Copy] = {}
    for locator in locators:
        copy = locator.copy
        if (
            copy is None
            or copy.health != CopyHealth.OK
            or copy.deleted_at is not None
            or copy.bundle_id is None
            or locator.bundle_id is None
            or locator.pool_id not in pool_rank
            or copy.backend_id not in backends
        ):
            continue
        key = (pool_rank[locator.pool_id], locator.pool_id, copy.id, locator.bundle_id)
        grouped.setdefault(key, {}).setdefault(locator.logical_asset_hash, locator)
        copy_by_key[key] = copy
    wanted = set(asset_hashes)
    for key in sorted(grouped):
        locator_by_hash = grouped[key]
        if set(locator_by_hash) >= wanted:
            copy = copy_by_key[key]
            return key[1], copy, backends[copy.backend_id], locator_by_hash
    return None


def _extract_bundle_to_paths(
    extractor: ArchiveExtractor,
    *,
    locators: list[AssetLocator],
    copy: Copy,
    backend: StorageBackend,
    destinations: dict[bytes, Path],
) -> None:
    batch_method = getattr(extractor, "extract_bundle_to_paths", None)
    if callable(batch_method):
        batch_method(
            locators=locators,
            copy=copy,
            backend=backend,
            destinations=destinations,
        )
        return
    for locator in locators:
        extractor.extract_to_path(
            locator=locator,
            copy=copy,
            backend=backend,
            destination=destinations[locator.logical_asset_hash],
        )


def _restore_pool_order(
    session: Session,
    artifactclass: str,
    restore_preference: list[str],
) -> list[str]:
    order: list[str] = []
    seen: set[str] = set()
    for pool_id in restore_preference:
        if pool_id not in seen:
            seen.add(pool_id)
            order.append(pool_id)
    memberships = list(
        session.scalars(
            select(ArtifactClassPool)
            .where(
                ArtifactClassPool.artifactclass == artifactclass,
                ArtifactClassPool.active.is_(True),
            )
            .order_by(ArtifactClassPool.sort_order, ArtifactClassPool.pool_id)
        )
    )
    for membership in memberships:
        if membership.pool_id not in seen:
            seen.add(membership.pool_id)
            order.append(membership.pool_id)
    return order


def _extract_d2_to_path(
    locator: AssetLocator,
    copy: Copy,
    backend: StorageBackend,
    destination: Path,
) -> None:
    if "block_range" in locator.native_locator:
        raw_range = locator.native_locator["block_range"]
        if (
            isinstance(raw_range, list)
            and len(raw_range) == 2
            and "size_bytes" in locator.native_locator
        ):
            data_start = int(raw_range[0])
            size = int(locator.native_locator["size_bytes"])
            _copy_backend_range_to_path(
                backend,
                copy.native_locator,
                data_start,
                data_start + size,
                destination,
            )
            return
    with tempfile.TemporaryDirectory(prefix="sutradhara-d2-restore-") as raw_tmp:
        object_path = Path(raw_tmp) / "copy.tar"
        _materialize_copy_to_path(backend, copy, object_path)
        _extract_tar_member_to_path(object_path, locator.member_path, destination)


def _extract_d2_bundle_to_paths(
    locators: list[AssetLocator],
    copy: Copy,
    backend: StorageBackend,
    destinations: dict[bytes, Path],
) -> None:
    if all("block_range" in locator.native_locator for locator in locators):
        for locator in locators:
            _extract_d2_to_path(locator, copy, backend, destinations[locator.logical_asset_hash])
        return
    with tempfile.TemporaryDirectory(prefix="sutradhara-d2-bundle-restore-") as raw_tmp:
        object_path = Path(raw_tmp) / "copy.tar"
        _materialize_copy_to_path(backend, copy, object_path)
        for locator in locators:
            _extract_tar_member_to_path(
                object_path,
                locator.member_path,
                destinations[locator.logical_asset_hash],
            )


def _extract_tar_member_to_path(
    tar_path: Path,
    member_path: str,
    destination: Path,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, mode="r:*") as tar:
        handle = tar.extractfile(member_path)
        if handle is None:
            raise ArchiveRestoreError(f"tar member {member_path!r} is not a file")
        with destination.open("wb") as raw_out:
            shutil.copyfileobj(handle, raw_out, length=1024 * 1024)


def _try_extract_from_local_archive_to_path(
    backend: StorageBackend,
    copy: Copy,
    locator: AssetLocator,
    destination: Path,
) -> bool:
    header_len_raw = backend.read_range(copy.native_locator, ByteRange(0, 8))
    if len(header_len_raw) != 8:
        return False
    header_len = int.from_bytes(header_len_raw, "big")
    if header_len <= 0 or header_len > _MAX_LOCAL_ARCHIVE_HEADER_BYTES:
        return False
    try:
        header_bytes = backend.read_range(copy.native_locator, ByteRange(8, 8 + header_len))
        header = json.loads(header_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return False
    if not isinstance(header, dict) or header.get("format") != "sutradhara-local-archive-v1":
        return False
    if "offset" not in locator.native_locator or "size_bytes" not in locator.native_locator:
        raise ArchiveRestoreError("local archive locator is missing offset/size_bytes")
    offset = int(locator.native_locator["offset"])
    size = int(locator.native_locator["size_bytes"])
    if offset < 0 or size < 0:
        raise ArchiveRestoreError("local archive locator has negative offset/size_bytes")
    payload_start = 8 + header_len
    _copy_backend_range_to_path(
        backend,
        copy.native_locator,
        payload_start + offset,
        payload_start + offset + size,
        destination,
    )
    return True


@contextmanager
def _open_locator_range_chunks(member: PlannedMember) -> Iterator[Iterator[bytes]]:
    """Open the exact stored-member range and structurally own its source."""

    native = dict(member.locator.native_locator)
    size = _size_bytes(native)
    representation = Representation(member.locator.representation)
    if size == 0:
        yield iter(())
        return
    if representation is Representation.D2TAR_RAW:
        raw_range = native.get("block_range")
        if not isinstance(raw_range, list) or len(raw_range) != 2:
            raise ArchiveRestoreError("ranged D2 member is missing a valid block_range")
        start = int(raw_range[0])
    elif "offset" in native:
        start = _local_archive_member_start(member.backend, member.copy, member.locator)
    elif representation is Representation.RAO_PLAIN_V1:
        start = member_byte_base(native)
    elif representation is Representation.RAW_BYTES and "block_range" in native:
        raw_range = native["block_range"]
        if not isinstance(raw_range, list) or len(raw_range) != 2:
            raise ArchiveRestoreError("RAW member has an invalid block_range")
        start = int(raw_range[0])
    else:
        start = 0
    if start < 0:
        raise ArchiveRestoreError(f"invalid backend member start {start}")
    byte_range = ByteRange(start, start + size)
    with _open_backend_range_chunks(
        member.backend,
        dict(member.copy.native_locator),
        byte_range,
    ) as chunks:
        yield _require_exact_range(chunks, byte_range.length)


def _local_archive_member_start(
    backend: StorageBackend,
    copy: Copy,
    locator: AssetLocator,
) -> int:
    native = dict(locator.native_locator)
    offset = int(native["offset"])
    if offset < 0:
        raise ArchiveRestoreError("local archive locator has a negative offset")
    header_len_raw = backend.read_range(copy.native_locator, ByteRange(0, 8))
    if len(header_len_raw) != 8:
        return offset
    header_len = int.from_bytes(header_len_raw, "big")
    if header_len <= 0 or header_len > _MAX_LOCAL_ARCHIVE_HEADER_BYTES:
        return offset
    try:
        header_bytes = backend.read_range(copy.native_locator, ByteRange(8, 8 + header_len))
        header = json.loads(header_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return offset
    if isinstance(header, dict) and header.get("format") == "sutradhara-local-archive-v1":
        return 8 + header_len + offset
    return offset


@contextmanager
def _open_backend_range_chunks(
    backend: StorageBackend,
    locator: dict[str, Any],
    byte_range: ByteRange,
) -> Iterator[Iterator[bytes]]:
    if isinstance(backend, StreamingStorageBackend):
        with backend.open_range_chunks(
            locator,
            byte_range,
            chunk_bytes=RAO_CHUNK_SIZE,
        ) as chunks:
            yield chunks
        return
    materialized = getattr(backend, "open_materialized_range_chunks", None)
    if callable(materialized):
        with materialized(locator, byte_range, chunk_bytes=RAO_CHUNK_SIZE) as chunks:
            yield chunks
        return

    @contextmanager
    def legacy_range_reader() -> Iterator[Iterator[bytes]]:
        def chunks() -> Iterator[bytes]:
            for cursor in range(byte_range.start, byte_range.end, RAO_CHUNK_SIZE):
                end = min(cursor + RAO_CHUNK_SIZE, byte_range.end)
                yield backend.read_range(locator, ByteRange(cursor, end))

        yield chunks()

    with legacy_range_reader() as chunks:
        yield chunks


def _require_exact_range(chunks: Iterator[bytes], expected_size: int) -> Iterator[bytes]:
    seen = 0
    for chunk in chunks:
        seen += len(chunk)
        if seen > expected_size:
            raise ArchiveRestoreError(
                f"backend member stream exceeded expected range size {expected_size}"
            )
        if chunk:
            yield chunk
    if seen != expected_size:
        raise ArchiveRestoreError(
            f"backend member stream returned {seen} bytes, expected {expected_size}"
        )


def _verify_stored_chunks(
    chunks: Iterator[bytes],
    *,
    expected_sha256: bytes,
    copy_id: int,
    mismatch_is_logical: bool = False,
) -> Iterator[bytes]:
    digest = hashlib.sha256()
    for chunk in chunks:
        digest.update(chunk)
        yield chunk
    actual = digest.digest()
    if actual != expected_sha256:
        message = (
            f"copy id={copy_id} stored-member SHA-256 {actual.hex()} != {expected_sha256.hex()}"
        )
        if mismatch_is_logical:
            raise LogicalMemberIntegrityError(message)
        raise StoredMemberIntegrityError(message)


def _reverse_transform_chunks(
    chunks: Iterator[bytes],
    transforms: tuple[StagingTransform, ...],
) -> Iterator[bytes]:
    current = chunks
    for transform in sorted(transforms, key=lambda item: item.step_order, reverse=True):
        if not transform.reversible:
            continue
        if transform.kind == "zstd-file-v1":
            current = _decompress_zstd_chunks(current)
            continue
        raise StagingError(f"unsupported reversible transform {transform.kind!r}")
    return current


def _decompress_zstd_chunks(chunks: Iterator[bytes]) -> Iterator[bytes]:
    reader = _ChunkIteratorReader(chunks)
    try:
        with zstd.ZstdDecompressor().stream_reader(
            cast(BinaryIO, reader), closefd=False
        ) as decompressed:
            while chunk := decompressed.read(RAO_CHUNK_SIZE):
                yield chunk
    except zstd.ZstdError as exc:
        raise StagingError("zstd decompression failed during restore") from exc


def _verify_logical_chunks(
    chunks: Iterator[bytes],
    *,
    expected_sha256: bytes,
    expected_size: int,
    copy_id: int,
) -> Iterator[bytes]:
    digest = hashlib.sha256()
    size = 0
    for chunk in chunks:
        digest.update(chunk)
        size += len(chunk)
        yield chunk
    actual = digest.digest()
    if size != expected_size:
        raise LogicalMemberIntegrityError(
            f"copy id={copy_id} logical size {size} != expected {expected_size}"
        )
    if actual != expected_sha256:
        raise LogicalMemberIntegrityError(
            f"copy id={copy_id} logical SHA-256 {actual.hex()} != {expected_sha256.hex()}"
        )


def _file_chunks(handle: Any) -> Iterator[bytes]:
    while chunk := handle.read(RAO_CHUNK_SIZE):
        yield chunk


def _copy_backend_range_to_path(
    backend: StorageBackend,
    locator: dict[str, Any],
    start: int,
    end: int,
    destination: Path,
) -> None:
    if start < 0 or end < start:
        raise ArchiveRestoreError(f"invalid backend byte range [{start}, {end})")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as handle:
        for cursor in range(start, end, RAO_CHUNK_SIZE):
            chunk_end = min(cursor + RAO_CHUNK_SIZE, end)
            chunk = backend.read_range(locator, ByteRange(cursor, chunk_end))
            expected = chunk_end - cursor
            if len(chunk) != expected:
                raise ArchiveRestoreError(
                    f"backend returned {len(chunk)} bytes for range "
                    f"[{cursor}, {chunk_end}), expected {expected}"
                )
            handle.write(chunk)


def _read_whole(backend: StorageBackend, copy: Any) -> bytes:
    return backend.read_range(copy.native_locator, ByteRange(0, 0))


def _materialize_copy_to_path(
    backend: StorageBackend,
    copy: Any,
    destination: Path,
) -> None:
    size = copy.storage_metadata.get("stored_size_bytes")
    if isinstance(size, int) and size >= 0:
        _copy_backend_range_to_path(backend, copy.native_locator, 0, size, destination)
        return
    destination.write_bytes(_read_whole(backend, copy))


def _extract_rao_with_rem_to_path(
    *,
    backend: StorageBackend,
    copy: Any,
    locator: Any,
    representation: Representation,
    destination: Path,
    rem_bin: str | Path,
    keys: KeyRegistry,
    work_dir: Path | None = None,
) -> None:
    with tempfile.TemporaryDirectory(prefix="sutradhara-restore-", dir=work_dir) as raw:
        temp_dir = Path(raw)
        object_path = temp_dir / "bundle.rao"
        _materialize_copy_to_path(backend, copy, object_path)
        _extract_rao_materialized_member_to_path(
            object_path=object_path,
            copy=copy,
            locator=locator,
            representation=representation,
            destination=destination,
            rem_bin=rem_bin,
            keys=keys,
        )


def _extract_rao_bundle_with_rem_to_paths(
    *,
    backend: StorageBackend,
    copy: Copy,
    locators: list[AssetLocator],
    destinations: dict[bytes, Path],
    rem_bin: str | Path,
    keys: KeyRegistry,
) -> None:
    if not locators:
        return
    with tempfile.TemporaryDirectory(prefix="sutradhara-bundle-restore-") as raw:
        object_path = Path(raw) / "bundle.rao"
        _materialize_copy_to_path(backend, copy, object_path)
        for locator in locators:
            _extract_rao_materialized_member_to_path(
                object_path=object_path,
                copy=copy,
                locator=locator,
                representation=Representation(locator.representation),
                destination=destinations[locator.logical_asset_hash],
                rem_bin=rem_bin,
                keys=keys,
            )


def _extract_rao_materialized_member_to_path(
    *,
    object_path: Path,
    copy: Any,
    locator: Any,
    representation: Representation,
    destination: Path,
    rem_bin: str | Path,
    keys: KeyRegistry,
) -> None:
    member_path = _member_path(locator.native_locator)
    size = _size_bytes(locator.native_locator)
    with tempfile.TemporaryDirectory(prefix="sutradhara-rem-member-") as raw:
        dest_dir = Path(raw) / "out"
        dest_dir.mkdir()
        cmd = [
            str(rem_bin),
            "archive",
            "extract",
            "--object",
            str(object_path),
            "--dest",
            str(dest_dir),
            "--path",
            member_path,
            "--first-chunk-lba",
            str(_first_chunk_lba(locator.native_locator)),
            "--file-size-bytes",
            str(size),
            "--range",
            f"0:{size}",
            "--overwrite",
        ]
        format_plugin = _format_plugin(copy.storage_metadata)
        if format_plugin is not None:
            cmd.extend(["--format", format_plugin])
        if representation is Representation.RAO_PLAIN_V1:
            cmd.extend(["--chunk-size", str(RAO_CHUNK_SIZE)])
            _run_rem(cmd)
        elif representation is Representation.RAO_AEAD_V1:
            key_epoch = _key_epoch(copy.storage_metadata)
            with keys.materialized_root_key(key_epoch) as key_file:
                cmd.extend(["--key-file", str(key_file)])
                _run_rem(cmd)
        else:
            raise ArchiveRestoreError(f"unsupported RAO representation {representation.value!r}")
        _copy_restored_member(dest_dir, member_path, destination)


def _member_path(locator: dict[str, Any]) -> str:
    value = locator.get("member_path")
    if not isinstance(value, str) or not value:
        raise ArchiveRestoreError("asset locator is missing member_path")
    return value


def member_byte_base(locator: dict[str, Any] | Mapping[str, Any]) -> int:
    """Return the object-relative byte offset where one RAO member begins."""

    return _first_chunk_lba(locator) * RAO_CHUNK_SIZE


def _first_chunk_lba(locator: dict[str, Any] | Mapping[str, Any]) -> int:
    value = locator.get("first_chunk_lba")
    if value is None:
        raise ArchiveRestoreError("RAO asset locator is missing first_chunk_lba")
    result = int(value)
    if result < 0:
        raise ArchiveRestoreError(f"invalid first_chunk_lba {value!r}")
    return result


def _size_bytes(locator: dict[str, Any]) -> int:
    value = locator.get("size_bytes")
    if value is None:
        raise ArchiveRestoreError("asset locator is missing size_bytes")
    result = int(value)
    if result < 0:
        raise ArchiveRestoreError(f"invalid size_bytes {value!r}")
    return result


def _key_epoch(storage_metadata: dict[str, Any]) -> str:
    value = storage_metadata.get("key_epoch")
    if not isinstance(value, str) or not value:
        raise ArchiveRestoreError("encrypted archive copy is missing key_epoch")
    return value


def _format_plugin(storage_metadata: dict[str, Any]) -> str | None:
    value = storage_metadata.get("format_plugin")
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ArchiveRestoreError("copy format_plugin metadata must be a non-empty string")
    return value


def _run_rem(cmd: list[str]) -> None:
    result = run_managed(cmd, role="high", capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise ArchiveRestoreError(
            f"rem archive extract failed (exit {result.returncode}): "
            f"stdout={result.stdout.strip()[:500]!r} "
            f"stderr={result.stderr.strip()[:500]!r}"
        )


def _copy_restored_member(dest_dir: Path, member_path: str, destination: Path) -> None:
    candidate = dest_dir / member_path if member_path else None
    if candidate is not None and candidate.is_file():
        source = candidate
    else:
        files = [path for path in dest_dir.rglob("*") if path.is_file()]
        if len(files) != 1:
            raise ArchiveRestoreError(
                f"rem restore expected one file for member {member_path!r}, found {len(files)}"
            )
        source = files[0]
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as raw_in, destination.open("wb") as raw_out:
        shutil.copyfileobj(raw_in, raw_out, length=1024 * 1024)
