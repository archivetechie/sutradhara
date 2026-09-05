"""Partial-file-restore wiring over pfr_core sidecars and RAO archive copies.

This module is the Sutradhara integration boundary for the standalone
``format-anatomy``/``pfr_core`` package.  It keeps the ingest-time sidecar
contract, blob validation, RAO byte-source adapter, fallback ladder, and CLI
result envelopes in one place so the job handler and CLI do not duplicate PFR
semantics.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from types import TracebackType
from typing import Any, Protocol

from pfr_core import PFRSidecar, default_registry, make_fallback_sidecar
from pfr_core.cut import CutRefusal, cut_from_sidecar
from pfr_core.failure import ReasonId, ScrapeFailure
from pfr_core.isolation import SourceSpec, run_scrape_isolated
from pfr_core.registry import HEAD_BYTES
from pfr_core.schema import Provenance
from pfr_core.source import ByteRangeSource, LocalFile, SourceChanged
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from sutradhara.archive_restore import (
    ArchiveRestoreError,
    _restore_pool_order,
    member_byte_base,
    read_member_to_path,
)
from sutradhara.artifactclass_policy import get_artifactclass_policy
from sutradhara.backend.port import (
    BackendError,
    BackendSessionInvalidatedError,
    BackendTransientError,
    ByteRange,
    StorageBackend,
)
from sutradhara.catalog.models import AssetLocator, Bundle, Copy, IngestItem
from sutradhara.catalog.types import CopyHealth, is_content_hash
from sutradhara.durability import locator_artifactclass_filter
from sutradhara.jobs.config import derivation_cache_root
from sutradhara.resource_control import run_managed
from sutradhara.sealing.port import Representation

PFR_INDEX_KIND = "pfr-index-v1"
PFR_RECIPE_METADATA_KEY = "pfr_recipe_version"
PFR_SIDECAR_METADATA_KEY = "pfr_sidecar_path"
PFR_SCRAPE_WALL_CLOCK_SECONDS = 120.0
DEFAULT_PFR_BLOB_CACHE_BYTES = 20 * 1024 * 1024 * 1024


class PFRUnavailable(Exception):
    """No PFR or fallback restore path could satisfy a request."""


class PFRBusy(PFRUnavailable):
    """Another local process is already cutting the same item."""


class PFRSourceDrift(PFRUnavailable):
    """The sidecar/source or locator/rem member table no longer agrees."""


class PFRCutRefused(PFRUnavailable):
    """A deterministic PFR plan/rewrap refusal from pfr_core."""

    def __init__(self, refusal: CutRefusal) -> None:
        self.failure = refusal.failure
        super().__init__(
            f"{refusal.failure.reason_id.value}: {refusal.failure.message or 'cut refused'}"
        )


class _ReadSession(Protocol):
    def read_range(self, byte_range: ByteRange) -> bytes: ...


@dataclass(frozen=True)
class SidecarRecord:
    """Parsed PFR sidecar plus its catalog path."""

    path: Path
    sidecar: PFRSidecar
    blobs_ok: bool


@dataclass(frozen=True)
class PFRRungAttempt:
    """One rung tried by the PFR restore ladder."""

    rung: int
    state: str
    reason: str
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "rung": self.rung,
            "state": self.state,
            "reason": self.reason,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class PFRCutResult:
    """Operator-facing result for a `sutra pfr cut` request."""

    asset_hash: bytes
    output_path: Path
    rung: int
    reason: str
    attempts: tuple[PFRRungAttempt, ...] = field(default_factory=tuple)
    sidecar_path: Path | None = None
    cut_result: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_hash": self.asset_hash.hex(),
            "output_path": str(self.output_path),
            "rung": self.rung,
            "reason": self.reason,
            "sidecar_path": str(self.sidecar_path) if self.sidecar_path else None,
            "attempts": [attempt.to_dict() for attempt in self.attempts],
            "cut_result": dict(self.cut_result or {}),
        }


class RaoObject(ByteRangeSource):
    """pfr_core ByteRangeSource backed by one RAO member over one read session."""

    def __init__(
        self,
        *,
        reader: _ReadSession,
        copy: Copy,
        locator: AssetLocator,
        size_cross_check: int | None = None,
    ) -> None:
        native_locator = dict(locator.native_locator)
        self._reader = reader
        self._base = member_byte_base(native_locator)
        self._size = _locator_size(native_locator)
        self._identity = {
            "kind": "rao_object",
            "copy_id": copy.id,
            "locator_id": locator.id,
            "pool_id": locator.pool_id,
            "representation": locator.representation,
            "member_path": locator.member_path,
            "object_id": dict(copy.native_locator).get("object_id"),
            "tape_uuid": dict(copy.native_locator).get("tape_uuid"),
            "member_byte_base": self._base,
            "size_bytes": self._size,
        }
        if size_cross_check is not None and int(size_cross_check) != self._size:
            raise SourceChanged(
                self._identity,
                {
                    **self._identity,
                    "size_bytes": int(size_cross_check),
                    "source": "remanence_get_file",
                },
            )

    def read(self, offset: int, length: int) -> bytes:
        if length < 0:
            raise ValueError("length must be non-negative")
        resolved = self._size + offset if offset < 0 else offset
        if resolved < 0:
            raise ValueError("negative offset resolves before logical member EOF")
        if resolved >= self._size or length == 0:
            return b""
        end = min(self._size, resolved + length)
        try:
            data = self._reader.read_range(ByteRange(self._base + resolved, self._base + end))
        except BackendSessionInvalidatedError as exc:
            raise SourceChanged(
                self._identity,
                {
                    **self._identity,
                    "source": "remanence_read_session",
                    "error_class": exc.__class__.__name__,
                    "message": str(exc),
                },
            ) from exc
        expected = end - resolved
        if len(data) != expected:
            raise SourceChanged(
                self._identity,
                {
                    **self._identity,
                    "short_read_offset": resolved,
                    "expected_bytes": expected,
                    "actual_bytes": len(data),
                },
            )
        return data

    def size(self) -> int:
        return self._size

    def identity(self) -> Mapping[str, Any]:
        return dict(self._identity)


def pfr_core_version() -> str:
    """Return the installed distribution version used for blocked-tool records."""

    try:
        return version("format-anatomy")
    except PackageNotFoundError:
        return "unknown"


def scrape_path_isolated_120(
    source_path: Path,
    *,
    blob_dir: Path,
    role: str = "medium",
    cpu_lease: int | None = None,
) -> PFRSidecar | ScrapeFailure:
    """Run the pfr_core isolated scrape composition under resource control."""

    cmd = [
        sys.executable,
        "-m",
        "sutradhara._pfr_worker",
        str(source_path),
        str(blob_dir),
    ]
    try:
        completed = run_managed(
            cmd,
            role=role,
            cpu_lease=cpu_lease,
            check=False,
            capture_output=True,
            text=True,
            timeout=PFR_SCRAPE_WALL_CLOCK_SECONDS + 30.0,
        )
    except subprocess.TimeoutExpired:
        return ScrapeFailure(
            plugin="registry",
            stage="isolation",
            reason_id=ReasonId.BUDGET_EXCEEDED,
            timeout=True,
            exception_class="TimeoutExpired",
            source_identity={"kind": "local_file", "path": str(source_path)},
            message=(
                "managed pfr_core scrape exceeded wall-clock limit of "
                f"{PFR_SCRAPE_WALL_CLOCK_SECONDS} seconds"
            ),
        )
    except OSError as exc:
        return ScrapeFailure(
            plugin="registry",
            stage="isolation",
            reason_id=ReasonId.EXCEPTION,
            exception_class=exc.__class__.__name__,
            source_identity={"kind": "local_file", "path": str(source_path)},
            message=str(exc),
        )

    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        return ScrapeFailure(
            plugin="registry",
            stage="isolation",
            reason_id=ReasonId.EXCEPTION,
            exception_class=f"ChildExit{completed.returncode}",
            source_identity={"kind": "local_file", "path": str(source_path)},
            message=(stderr or stdout or "pfr_core scrape worker failed"),
        )

    try:
        payload = json.loads(completed.stdout or "{}")
        return _scrape_payload_from_dict(payload)
    except Exception as exc:
        return ScrapeFailure(
            plugin="registry",
            stage="isolation",
            reason_id=ReasonId.EXCEPTION,
            exception_class=exc.__class__.__name__,
            source_identity={"kind": "local_file", "path": str(source_path)},
            message=f"pfr_core scrape worker returned invalid JSON: {exc}",
        )


def _scrape_path_isolated_worker(argv: list[str] | None = None) -> int:
    """Subprocess entry point used by ``scrape_path_isolated_120``."""

    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 2:
        sys.stderr.write("usage: _scrape_path_isolated_worker SOURCE BLOB_DIR\n")
        return 2
    result = _scrape_path_isolated_in_process(Path(args[0]), blob_dir=Path(args[1]))
    sys.stdout.write(result.to_json())
    sys.stdout.write("\n")
    return 0


def _scrape_payload_from_dict(payload: Mapping[str, Any]) -> PFRSidecar | ScrapeFailure:
    if payload.get("record_type") == "pfr_scrape_attempt":
        return ScrapeFailure.from_dict(payload)
    return PFRSidecar.from_dict(payload)


def _scrape_path_isolated_in_process(
    source_path: Path,
    *,
    blob_dir: Path,
) -> PFRSidecar | ScrapeFailure:
    """Run the pfr_core parent-sniff + isolated-child scrape path."""

    try:
        source = LocalFile(source_path)
    except Exception as exc:
        return ScrapeFailure(
            plugin="registry",
            stage="source_open",
            reason_id=ReasonId.EXCEPTION,
            exception_class=exc.__class__.__name__,
            source_identity={"kind": "local_file", "path": str(source_path)},
            message=str(exc),
        )

    registry = default_registry(blob_dir=blob_dir)
    try:
        head = source.read(0, min(HEAD_BYTES, source.size()))
    except SourceChanged as exc:
        return ScrapeFailure(
            plugin="registry",
            stage="sniff",
            reason_id=ReasonId.SOURCE_CHANGED,
            exception_class=exc.__class__.__name__,
            source_identity=exc.expected_identity,
            message=str(exc),
        )
    except Exception as exc:
        return ScrapeFailure(
            plugin="registry",
            stage="sniff",
            reason_id=ReasonId.EXCEPTION,
            exception_class=exc.__class__.__name__,
            source_identity=source.identity(),
            message=str(exc),
        )

    selected = registry.sniff(head)
    if isinstance(selected, ScrapeFailure):
        return selected
    registration, _sniff = selected
    plugin = registration.plugin
    clone = getattr(plugin, "with_blob_store_dir", None)
    if callable(clone):
        plugin = clone(blob_dir)
    result = run_scrape_isolated(
        plugin,
        SourceSpec(
            kind="local_file",
            path=str(source_path),
            parent_identity=dict(source.identity()),
        ),
        wall_clock_seconds=PFR_SCRAPE_WALL_CLOCK_SECONDS,
    )
    if isinstance(result, ScrapeFailure):
        if result.reason_id in {
            ReasonId.CAP_EXCEEDED_FALLBACK,
            ReasonId.OP_ATOM_UNSUPPORTED,
        }:
            return make_fallback_sidecar(source, result)
        return result
    if plugin.plugin_id == "fallback" and result.provenance.attempt_reason_id is None:
        result = result.with_provenance(
            Provenance(
                read_ranges_used=result.provenance.read_ranges_used,
                validation_status=result.provenance.validation_status,
                attempt_reason_id=ReasonId.PLUGIN_MISSING,
                failures=result.provenance.failures,
            )
        )
    return result


def sidecar_with_catalog_identity(sidecar: PFRSidecar, item: IngestItem) -> PFRSidecar:
    """Inject catalog back-links into a pfr_core sidecar source identity."""

    return PFRSidecar(
        grammar_id=sidecar.grammar_id,
        schema_version=sidecar.schema_version,
        plugin_version=sidecar.plugin_version,
        recipe_version=sidecar.recipe_version,
        measured_facts=dict(sidecar.measured_facts),
        source_identity={
            **dict(sidecar.source_identity),
            "ingest_item_id": item.id,
            "logical_asset_hash": item.logical_asset_hash.hex(),
        },
        capability_snapshot=sidecar.capability_snapshot,
        provenance=sidecar.provenance,
        blobs=sidecar.blobs,
        sidecar_kind=sidecar.sidecar_kind,
    )


def atomic_write_sidecar(sidecar: PFRSidecar, path: Path) -> None:
    """Publish a sidecar with temp-write, fsync, and rename."""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(sidecar.to_dict(), indent=2, sort_keys=True) + "\n"
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
        _fsync_dir(path.parent)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def load_sidecar_record(path: Path) -> SidecarRecord:
    """Parse a sidecar and verify its blob references."""

    sidecar = PFRSidecar.from_json(path.read_text(encoding="utf-8"))
    blob_dir = path.parent / "blobs"
    return SidecarRecord(
        path=path,
        sidecar=sidecar,
        blobs_ok=sidecar_blobs_complete(sidecar, blob_dir=blob_dir),
    )


def pfr_sidecar_complete(path: Path) -> bool:
    """Return True only when a PFR sidecar parses and all blobs verify."""

    try:
        return load_sidecar_record(path).blobs_ok
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False


def sidecar_blobs_complete(sidecar: PFRSidecar, *, blob_dir: Path) -> bool:
    """Verify every sidecar blob path/content-address is present and access-touched."""

    for blob in sidecar.blobs:
        path = _resolve_blob_path(blob.sha256, explicit_path=blob.path, blob_dir=blob_dir)
        if path is None or not path.is_file():
            return False
        try:
            stat = path.stat()
        except OSError:
            return False
        if stat.st_size != blob.size:
            return False
        if _sha256_file(path) != blob.sha256:
            return False
        _touch_blob_access(path, stat)
    return True


def enforce_blob_lru(
    blob_dir: Path,
    *,
    max_bytes: int | None = None,
    protect_sidecar: PFRSidecar | None = None,
) -> None:
    """Trim a content-addressed blob directory to the configured LRU budget."""

    budget = pfr_blob_cache_bytes() if max_bytes is None else max_bytes
    if budget <= 0 or not blob_dir.exists():
        return
    protected = _protected_blob_paths(protect_sidecar, blob_dir=blob_dir)
    all_files = [
        path for path in blob_dir.rglob("*") if path.is_file() and not path.name.endswith(".tmp")
    ]
    files = [path for path in all_files if path.resolve() not in protected]
    total = sum(path.stat().st_size for path in all_files)
    if total <= budget:
        return
    for path in sorted(files, key=lambda item: item.stat().st_atime_ns):
        try:
            size = path.stat().st_size
            path.unlink()
            total -= size
        except OSError:
            continue
        if total <= budget:
            return


def pfr_blob_cache_bytes() -> int:
    raw = os.environ.get("SUTRADHARA_PFR_BLOB_CACHE_BYTES")
    if not raw:
        return DEFAULT_PFR_BLOB_CACHE_BYTES
    value = int(raw)
    if value < 0:
        raise ValueError("SUTRADHARA_PFR_BLOB_CACHE_BYTES must be non-negative")
    return value


def pfr_scratch_root() -> Path:
    raw = os.environ.get("SUTRADHARA_PFR_SCRATCH_ROOT")
    if raw:
        return Path(raw).expanduser().resolve()
    return Path("/var/lib/replica/pfr-scratch")


def current_pfr_recipe_version() -> str:
    registry = default_registry()
    for registration in registry._plugins.values():
        if registration.plugin.plugin_id == "mxf":
            return str(registration.plugin.recipe_version)
    return "unknown"


def sidecar_for_asset(session: Session, asset_hash: bytes) -> SidecarRecord | None:
    """Return the first parseable PFR sidecar for a logical asset."""

    items = list(
        session.scalars(
            select(IngestItem)
            .where(IngestItem.logical_asset_hash == asset_hash)
            .order_by(IngestItem.id)
        )
    )
    for item in items:
        raw = (item.item_metadata or {}).get(PFR_SIDECAR_METADATA_KEY)
        if not isinstance(raw, str) or not raw:
            continue
        path = Path(raw)
        if not path.exists():
            continue
        try:
            return load_sidecar_record(path)
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
    return None


def pfr_status(
    session: Session,
    *,
    asset_hash: bytes,
    artifactclass: str,
) -> dict[str, Any]:
    """Return a JSON-compatible PFR readiness snapshot for one asset."""

    sidecar = sidecar_for_asset(session, asset_hash)
    locators = _candidate_locators(session, asset_hash=asset_hash, artifactclass=artifactclass)
    return {
        "asset_hash": asset_hash.hex(),
        "artifactclass": artifactclass,
        "sidecar": None
        if sidecar is None
        else {
            "path": str(sidecar.path),
            "grammar_id": sidecar.sidecar.grammar_id,
            "recipe_version": sidecar.sidecar.recipe_version,
            "blobs_ok": sidecar.blobs_ok,
        },
        "locators": [
            {
                "locator_id": locator.id,
                "copy_id": locator.copy_id,
                "pool_id": locator.pool_id,
                "representation": locator.representation,
                "member_path": locator.member_path,
                "ranged_pfr": locator.representation == Representation.RAO_PLAIN_V1.value,
            }
            for locator in locators
        ],
    }


def cut_pfr_asset(
    session: Session,
    *,
    asset_hash: bytes,
    artifactclass: str,
    destination: Path,
    backends: Mapping[int, StorageBackend],
    t_in: float,
    t_out: float,
    rem_bin: str | Path = "rem",
    scratch_root: Path | None = None,
    cache_root: Path | None = None,
) -> PFRCutResult:
    """Cut an asset through the three-rung PFR restore ladder."""

    if not is_content_hash(asset_hash):
        raise ValueError("asset_hash must be a 32-byte SHA-256 hash")
    if t_out <= t_in:
        raise ValueError("--to must be greater than --from")

    output_path = destination.resolve()
    attempts: list[PFRRungAttempt] = []
    with pfr_item_lock(asset_hash, cache_root=cache_root):
        sidecar = sidecar_for_asset(session, asset_hash)
        locators = _candidate_locators(
            session,
            asset_hash=asset_hash,
            artifactclass=artifactclass,
        )
        if sidecar is not None and sidecar.sidecar.grammar_id == "mxf":
            ranged = _first_supported_locator(
                locators,
                representation=Representation.RAO_PLAIN_V1,
                backends=backends,
            )
            if ranged is not None:
                locator, copy, backend = ranged
                try:
                    result = _cut_ranged_rao(
                        sidecar=sidecar,
                        locator=locator,
                        copy=copy,
                        backend=backend,
                        output_path=output_path,
                        t_in=t_in,
                        t_out=t_out,
                    )
                    attempts.append(PFRRungAttempt(1, "completed", "ranged-pfr"))
                    return PFRCutResult(
                        asset_hash=asset_hash,
                        output_path=output_path,
                        rung=1,
                        reason="ranged-pfr",
                        attempts=tuple(attempts),
                        sidecar_path=sidecar.path,
                        cut_result=result,
                    )
                except PFRUnavailable as exc:
                    reason = (
                        exc.failure.reason_id.value
                        if isinstance(exc, PFRCutRefused)
                        else exc.__class__.__name__
                    )
                    attempts.append(PFRRungAttempt(1, "fallback", reason, str(exc)))
            else:
                attempts.append(PFRRungAttempt(1, "skipped", "no-rao-plain-locator"))
        elif sidecar is None:
            attempts.append(PFRRungAttempt(1, "skipped", "sidecar-missing"))
        else:
            attempts.append(
                PFRRungAttempt(
                    1,
                    "skipped",
                    f"sidecar-grammar-{sidecar.sidecar.grammar_id}",
                )
            )

        non_aead = _first_non_aead_locator(locators, backends=backends)
        if non_aead is not None:
            locator, copy, backend = non_aead
            _restore_member_whole(
                locator=locator,
                copy=copy,
                backend=backend,
                output_path=output_path,
                rem_bin=rem_bin,
            )
            attempts.append(PFRRungAttempt(2, "completed", "member-whole-file"))
            return PFRCutResult(
                asset_hash=asset_hash,
                output_path=output_path,
                rung=2,
                reason="member-whole-file",
                attempts=tuple(attempts),
                sidecar_path=sidecar.path if sidecar else None,
            )
        attempts.append(PFRRungAttempt(2, "skipped", "no-restorable-non-aead-locator"))

        aead = _first_supported_locator(
            locators,
            representation=Representation.RAO_AEAD_V1,
            backends=backends,
        )
        if aead is not None:
            locator, copy, backend = aead
            final_scratch = scratch_root or pfr_scratch_root()
            _preflight_aead_scratch(copy=copy, locator=locator, scratch_root=final_scratch)
            _restore_member_whole(
                locator=locator,
                copy=copy,
                backend=backend,
                output_path=output_path,
                rem_bin=rem_bin,
                work_dir=final_scratch,
            )
            attempts.append(PFRRungAttempt(3, "completed", "aead_ranged_unsupported"))
            return PFRCutResult(
                asset_hash=asset_hash,
                output_path=output_path,
                rung=3,
                reason="aead_ranged_unsupported",
                attempts=tuple(attempts),
                sidecar_path=sidecar.path if sidecar else None,
            )

    raise PFRUnavailable(f"no healthy archive locator can restore {asset_hash.hex()}")


@contextmanager
def pfr_item_lock(asset_hash: bytes, *, cache_root: Path | None = None) -> Iterator[None]:
    """Acquire a non-blocking advisory lock for one PFR output item."""

    root = cache_root or derivation_cache_root()
    lock_dir = root / "locks" / "pfr-cut"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{asset_hash.hex()}.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PFRBusy(f"PFR cut is already running for {asset_hash.hex()}") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class _BackendReadSession:
    """Context wrapper for non-Remanence backends used by unit tests."""

    def __init__(self, backend: StorageBackend, locator: Mapping[str, Any]) -> None:
        self._backend = backend
        self._locator = dict(locator)

    def __enter__(self) -> _BackendReadSession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    def read_range(self, byte_range: ByteRange) -> bytes:
        return self._backend.read_range(self._locator, byte_range)


def _cut_ranged_rao(
    *,
    sidecar: SidecarRecord,
    locator: AssetLocator,
    copy: Copy,
    backend: StorageBackend,
    output_path: Path,
    t_in: float,
    t_out: float,
) -> Mapping[str, Any]:
    blob_dir = sidecar.path.parent / "blobs"
    last_error: BaseException | None = None
    for _attempt in range(2):
        try:
            with _open_read_session(backend, copy.native_locator) as reader:
                source = RaoObject(
                    reader=reader,
                    copy=copy,
                    locator=locator,
                    size_cross_check=_rem_file_size(backend, copy, locator),
                )
                if not sidecar_blobs_complete(sidecar.sidecar, blob_dir=blob_dir):
                    regenerated = default_registry(blob_dir=blob_dir).scrape_source(
                        source,
                        blob_dir=blob_dir,
                    )
                    if isinstance(regenerated, ScrapeFailure):
                        raise PFRUnavailable(
                            f"blob regeneration failed: "
                            f"{regenerated.reason_id.value}: {regenerated.message}"
                        )
                    if not sidecar_blobs_complete(sidecar.sidecar, blob_dir=blob_dir):
                        raise PFRUnavailable(
                            "blob regeneration did not recreate the sidecar blob references"
                        )
                    enforce_blob_lru(blob_dir, protect_sidecar=sidecar.sidecar)
                    if not sidecar_blobs_complete(sidecar.sidecar, blob_dir=blob_dir):
                        raise PFRUnavailable(
                            "blob regeneration references were evicted during cache trim"
                        )
                return _cut_to_temp_then_publish(
                    sidecar=sidecar.sidecar,
                    source=source,
                    output_path=output_path,
                    t_in=t_in,
                    t_out=t_out,
                    blob_dir=blob_dir,
                )
        except BackendTransientError as exc:
            last_error = exc
            continue
        except CutRefusal as exc:
            raise PFRCutRefused(exc) from exc
        except SourceChanged as exc:
            raise PFRSourceDrift(str(exc)) from exc
        except BackendError as exc:
            raise PFRUnavailable(str(exc)) from exc
        except RuntimeError as exc:
            raise PFRUnavailable(str(exc)) from exc
    raise PFRUnavailable(str(last_error or "RAO read failed after retry"))


def _cut_to_temp_then_publish(
    *,
    sidecar: PFRSidecar,
    source: ByteRangeSource,
    output_path: Path,
    t_in: float,
    t_out: float,
    blob_dir: Path,
) -> Mapping[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "wb",
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=output_path.parent,
        delete=False,
    ) as handle:
        temp_path = Path(handle.name)
    try:
        result = cut_from_sidecar(
            sidecar,
            source,
            t_in=t_in,
            t_out=t_out,
            out_path=temp_path,
            blob_dir=blob_dir,
        )
        os.replace(temp_path, output_path)
        _fsync_dir(output_path.parent)
        return result.to_dict()
    finally:
        temp_path.unlink(missing_ok=True)


def _restore_member_whole(
    *,
    locator: AssetLocator,
    copy: Copy,
    backend: StorageBackend,
    output_path: Path,
    rem_bin: str | Path,
    work_dir: Path | None = None,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "wb",
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=output_path.parent,
        delete=False,
    ) as handle:
        temp_path = Path(handle.name)
    try:
        read_member_to_path(
            backend=backend,
            copy=copy,
            asset_locator=locator,
            dest=temp_path,
            rem_bin=rem_bin,
            keys=None,
            work_dir=work_dir,
        )
        os.replace(temp_path, output_path)
        _fsync_dir(output_path.parent)
    except (ArchiveRestoreError, BackendError) as exc:
        raise PFRUnavailable(str(exc)) from exc
    finally:
        temp_path.unlink(missing_ok=True)


def _candidate_locators(
    session: Session,
    *,
    asset_hash: bytes,
    artifactclass: str,
) -> list[AssetLocator]:
    policy = get_artifactclass_policy(session, artifactclass)
    pool_order = _restore_pool_order(session, artifactclass, policy.restore_preference)
    pool_rank = {pool_id: index for index, pool_id in enumerate(pool_order)}
    rows = list(
        session.scalars(
            select(AssetLocator)
            .options(joinedload(AssetLocator.copy).joinedload(Copy.backend))
            .outerjoin(Bundle, AssetLocator.bundle_id == Bundle.id)
            .where(
                AssetLocator.logical_asset_hash == asset_hash,
                locator_artifactclass_filter(session, asset_hash, artifactclass),
            )
        )
    )
    return sorted(
        [
            locator
            for locator in rows
            if locator.copy is not None
            and locator.copy.health == CopyHealth.OK
            and locator.copy.deleted_at is None
            and locator.pool_id in pool_rank
        ],
        key=lambda locator: (pool_rank[locator.pool_id], locator.copy_id or 0, locator.id),
    )


def _first_supported_locator(
    locators: list[AssetLocator],
    *,
    representation: Representation,
    backends: Mapping[int, StorageBackend],
) -> tuple[AssetLocator, Copy, StorageBackend] | None:
    for locator in locators:
        copy = locator.copy
        if copy is None or locator.representation != representation.value:
            continue
        backend = backends.get(copy.backend_id)
        if backend is not None:
            return locator, copy, backend
    return None


def _first_non_aead_locator(
    locators: list[AssetLocator],
    *,
    backends: Mapping[int, StorageBackend],
) -> tuple[AssetLocator, Copy, StorageBackend] | None:
    for locator in locators:
        if locator.representation == Representation.RAO_AEAD_V1.value:
            continue
        copy = locator.copy
        if copy is None:
            continue
        backend = backends.get(copy.backend_id)
        if backend is not None:
            return locator, copy, backend
    return None


@contextmanager
def _open_read_session(
    backend: StorageBackend,
    locator: Mapping[str, Any],
) -> Iterator[_ReadSession]:
    opener = getattr(backend, "open_read_session", None)
    if callable(opener):
        with opener(dict(locator)) as reader:
            yield reader
        return
    with _BackendReadSession(backend, locator) as reader:
        yield reader


def _rem_file_size(backend: StorageBackend, copy: Copy, locator: AssetLocator) -> int | None:
    getter = getattr(backend, "get_file", None)
    if not callable(getter):
        return None
    if getattr(backend, "has_live_catalog", True) is False:
        return None
    record = getter(dict(copy.native_locator), path=locator.member_path)
    return int(record.size_bytes)


def _preflight_aead_scratch(
    *,
    copy: Copy,
    locator: AssetLocator,
    scratch_root: Path,
) -> None:
    scratch_root.mkdir(parents=True, exist_ok=True)
    size = _locator_size(locator.native_locator)
    stored = copy.storage_metadata.get("stored_size_bytes")
    stored_size = int(stored) if isinstance(stored, int) and stored >= 0 else size
    required = stored_size + size + size
    free = shutil.disk_usage(scratch_root).free
    if free < required:
        raise PFRUnavailable(
            f"scratch root {scratch_root} has {free} bytes free; "
            f"{required} bytes required for AEAD fallback"
        )


def _locator_size(locator: Mapping[str, Any]) -> int:
    value = locator.get("size_bytes")
    if value is None:
        raise PFRUnavailable("asset locator is missing size_bytes")
    result = int(value)
    if result < 0:
        raise PFRUnavailable(f"invalid locator size_bytes {value!r}")
    return result


def _resolve_blob_path(
    sha256: str,
    *,
    explicit_path: str | None,
    blob_dir: Path,
) -> Path | None:
    if explicit_path:
        path = Path(explicit_path)
        if path.exists():
            return path
    candidate = blob_dir / sha256[:2] / sha256
    return candidate if candidate.exists() else None


def _protected_blob_paths(sidecar: PFRSidecar | None, *, blob_dir: Path) -> set[Path]:
    if sidecar is None:
        return set()
    paths: set[Path] = set()
    for blob in sidecar.blobs:
        path = _resolve_blob_path(blob.sha256, explicit_path=blob.path, blob_dir=blob_dir)
        if path is not None:
            paths.add(path.resolve())
    return paths


def _touch_blob_access(path: Path, stat: os.stat_result | None = None) -> None:
    try:
        current = stat or path.stat()
        now_ns = time.time_ns()
        os.utime(path, ns=(now_ns, current.st_mtime_ns))
    except OSError:
        return


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
