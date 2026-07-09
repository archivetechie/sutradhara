"""IntakeService implementation for streaming card/drive intake.

The servicer writes streamed payload units into a landing directory and seals a
standard receive BagIt bag at commit. Verification, catalog registration, and
terminal markers remain owned by ``sutra intake watch``.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import itertools
import json
import os
import shutil
import threading
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

import grpc
from sqlalchemy import Engine, select

from sutradhara._proto import intake_pb2, intake_pb2_grpc
from sutradhara.api import store as api_store
from sutradhara.artifactclass_policy import ArtifactClassPolicyError, get_artifactclass_policy
from sutradhara.catalog.session import make_session_factory
from sutradhara.grpc import assembly
from sutradhara.grpc import ca as grpc_ca
from sutradhara.grpc import store as grpc_store
from sutradhara.grpc.progress import ReceiveProgressRegistry
from sutradhara.grpc.status import intake_status
from sutradhara_receive import (
    CANONICALIZATION_VERSION,
    DATA_DIR_NAME,
    ReceiveError,
    canonicalize_manifest_path,
    safe_payload_path,
    slug_operator,
)


@dataclass(frozen=True)
class GrpcIntakeConfig:
    """Runtime dependencies for the IntakeService."""

    engine: Engine
    landing_root: Path
    validate_artifactclass: bool = True
    progress_registry: ReceiveProgressRegistry | None = None


@dataclass
class _RuntimeState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    ledger_lock: threading.Lock = field(default_factory=threading.Lock)
    in_flight: int = 0


class IntakeServicer(intake_pb2_grpc.IntakeServiceServicer):
    """gRPC servicer for streaming intake."""

    def __init__(self, config: GrpcIntakeConfig) -> None:
        self.config = config
        self.progress_registry = config.progress_registry or ReceiveProgressRegistry()
        self._runtime: dict[str, _RuntimeState] = {}
        self._runtime_lock = threading.Lock()

    def StartIntake(self, request: Any, context: Any) -> Any:
        identity = self._identity(context)
        self._validate_start_request(request, context)
        request_hash = _start_request_hash(request)
        decision = api_store.begin_idempotency(
            self.config.engine,
            operator_username=identity.operator,
            endpoint=grpc_store.GRPC_START_ENDPOINT,
            idempotency_key=request.idempotency_key,
            request_hash=request_hash,
        )
        if decision.state == "completed":
            response = decision.response_json or {}
            intake_id = str(response.get("intake_id", ""))
            self._assert_resume_source_plan(intake_id, identity, request.source_plan_digest, context)
            self.progress_registry.start(
                intake_id,
                planned_bytes_total=_planned_bytes_total(request),
            )
            return intake_pb2.StartIntakeResponse(intake_id=intake_id)
        if decision.state == "conflict":
            self._abort_if_resume_source_changed(request, identity, context)
            _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "idempotency key conflict")
        if decision.state != "claimed":
            _abort(context, grpc.StatusCode.ALREADY_EXISTS, "intake start already in progress")

        now = dt.datetime.now(dt.UTC)
        intake_id = _mint_intake_id(identity.operator, now, self.config.landing_root)
        intake_dir = self.config.landing_root / intake_id
        try:
            intake_dir.mkdir(parents=True, mode=0o755)
            (intake_dir / DATA_DIR_NAME).mkdir()
            (intake_dir / ".incoming").mkdir()
            _write_json(
                intake_dir / ".receiving.json",
                {
                    "intake_id": intake_id,
                    "landing": str(self.config.landing_root),
                    "source_kind": request.source_kind,
                    "operator": identity.operator,
                    "device_id": identity.device_id,
                    "source_ref": request.source_ref,
                    "artifactclass": request.artifactclass,
                    "label": request.label,
                    "started_at": now.isoformat(),
                    "receive_version": "grpc-stream-v1",
                    "canonicalization_version": CANONICALIZATION_VERSION,
                    "transport": "grpc-stream",
                    "state": "streaming",
                },
            )
            factory = make_session_factory(self.config.engine)
            with factory.begin() as session:
                grpc_store.insert_intake(
                    session,
                    intake_id=intake_id,
                    operator=identity.operator,
                    device_id=identity.device_id,
                    idempotency_key=request.idempotency_key,
                    source_plan_digest=request.source_plan_digest,
                    artifactclass=request.artifactclass,
                    source_kind=request.source_kind,
                    source_ref=request.source_ref or None,
                    label=request.label or None,
                    landing_root=str(self.config.landing_root),
                )
            api_store.complete_idempotency(
                self.config.engine,
                operator_username=identity.operator,
                endpoint=grpc_store.GRPC_START_ENDPOINT,
                idempotency_key=request.idempotency_key,
                intake_id=intake_id,
                response_json={"intake_id": intake_id},
            )
            self.progress_registry.start(
                intake_id,
                planned_bytes_total=_planned_bytes_total(request),
            )
        except Exception:
            api_store.abandon_idempotency(
                self.config.engine,
                operator_username=identity.operator,
                endpoint=grpc_store.GRPC_START_ENDPOINT,
                idempotency_key=request.idempotency_key,
            )
            if intake_dir.exists():
                shutil.rmtree(intake_dir)
            raise
        return intake_pb2.StartIntakeResponse(intake_id=intake_id)

    def UploadFile(self, request_iterator: Iterable[Any], context: Any) -> Any:
        iterator = iter(request_iterator)
        try:
            first = next(iterator)
        except StopIteration:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, "UploadFile requires chunks")
        relpath = _validate_wire_relpath(first.relpath, context)
        runtime = self._runtime_for(first.intake_id)
        with runtime.lock:
            row = self._owned_row(first.intake_id, context)
            if row.state != "streaming":
                _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "intake is not streaming")
            runtime.in_flight += 1
        try:
            return self._receive_file(row, relpath, first, iterator, runtime, context)
        finally:
            with runtime.lock:
                runtime.in_flight = max(0, runtime.in_flight - 1)

    def ListIntakeFiles(self, request: Any, context: Any) -> Any:
        row = self._owned_row(request.intake_id, context)
        runtime = self._runtime_for(row.intake_id)
        with runtime.lock:
            if row.state == "streaming" and runtime.in_flight == 0:
                _clear_incoming(_intake_dir(row))
        records = _read_receipts(_intake_dir(row))
        return intake_pb2.ListIntakeFilesResponse(
            files=[
                intake_pb2.FileRecord(relpath=relpath, server_sha256=digest, bytes=size)
                for relpath, (digest, size) in sorted(records.items())
            ]
        )

    def CommitIntake(self, request: Any, context: Any) -> Any:
        row = self._owned_row(request.intake_id, context)
        if row.state == "committed":
            if row.manifest_digest != request.manifest_digest:
                _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "manifest digest conflict")
            return intake_pb2.CommitIntakeResponse(
                intake_id=row.intake_id,
                status=self._live_status(row),
            )
        if row.state == "aborted":
            _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "intake is aborted")
        runtime = self._runtime_for(row.intake_id)
        with runtime.lock:
            if runtime.in_flight:
                _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "upload in flight")
            factory = make_session_factory(self.config.engine)
            with factory.begin() as session:
                if not grpc_store.compare_and_set_state(
                    session,
                    row.intake_id,
                    expect="streaming",
                    update="committing",
                ):
                    _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "intake is not streaming")

        missing = _reupload_relpaths(_read_receipts(_intake_dir(row)), request.files)
        if missing:
            self._rollback_to_streaming(row.intake_id)
            return intake_pb2.CommitIntakeResponse(
                intake_id=row.intake_id,
                status="streaming",
                reupload_relpaths=missing,
            )

        try:
            if assembly.manifest_digest(request.files) != request.manifest_digest:
                raise assembly.AssemblyError("manifest_digest does not match commit files")
            assembly.assemble_committed_bag(
                _intake_dir(row),
                row=row,
                files=request.files,
                receive_facts=request.receive_facts,
                package_indexes=request.package_indexes,
            )
        except ReceiveError as exc:
            self._rollback_to_streaming(row.intake_id)
            _abort(context, grpc.StatusCode.FAILED_PRECONDITION, str(exc))

        factory = make_session_factory(self.config.engine)
        with factory.begin() as session:
            grpc_store.set_committed_digest(session, row.intake_id, request.manifest_digest)
        (_intake_dir(row) / ".receiving.json").unlink(missing_ok=True)
        _fsync_dir(_intake_dir(row))
        self.progress_registry.discard(row.intake_id)
        return intake_pb2.CommitIntakeResponse(intake_id=row.intake_id, status="verifying")

    def GetIntakeStatus(self, request: Any, context: Any) -> Any:
        row = self._owned_row(request.intake_id, context)
        view = intake_status(row)
        return intake_pb2.IntakeStatusResponse(
            intake_id=row.intake_id,
            status=view.status,
            errors=view.errors,
        )

    def AbortIntake(self, request: Any, context: Any) -> Any:
        row = self._owned_row(request.intake_id, context)
        if row.state == "committed":
            _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "committed intake cannot be aborted")
        if row.state != "aborted":
            intake_dir = _intake_dir(row)
            if intake_dir.exists():
                shutil.rmtree(intake_dir)
            factory = make_session_factory(self.config.engine)
            with factory.begin() as session:
                grpc_store.set_state(session, row.intake_id, "aborted")
            api_store.release_idempotency(
                self.config.engine,
                operator_username=row.operator,
                endpoint=grpc_store.GRPC_START_ENDPOINT,
                idempotency_key=row.idempotency_key,
            )
        self.progress_registry.discard(row.intake_id)
        return intake_pb2.AbortIntakeResponse(intake_id=row.intake_id, status="aborted")

    def _receive_file(
        self,
        row: grpc_store.GrpcIntake,
        relpath: str,
        first: Any,
        iterator: Iterable[Any],
        runtime: _RuntimeState,
        context: Any,
    ) -> Any:
        intake_dir = _intake_dir(row)
        incoming = intake_dir / ".incoming"
        incoming.mkdir(exist_ok=True)
        temp_path = incoming / f"{uuid.uuid4().hex}.tmp"
        destination = safe_payload_path(intake_dir / DATA_DIR_NAME, relpath)
        digest = hashlib.sha256()
        received = 0
        total = max(0, int(first.file_size))
        last_seen = False
        self.progress_registry.update_file(
            row.intake_id,
            relpath=relpath,
            bytes_received=0,
            bytes_total=total,
        )
        try:
            with temp_path.open("xb") as handle:
                for chunk in itertools.chain((first,), iterator):
                    if chunk.intake_id != row.intake_id or chunk.relpath != relpath:
                        _abort(context, grpc.StatusCode.INVALID_ARGUMENT, "chunk identity changed")
                    if chunk.file_size > 0:
                        total = max(total, int(chunk.file_size))
                    if chunk.data:
                        if chunk.offset != received:
                            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, "unexpected chunk offset")
                        digest.update(chunk.data)
                        handle.write(chunk.data)
                        received += len(chunk.data)
                        self.progress_registry.update_file(
                            row.intake_id,
                            relpath=relpath,
                            bytes_received=received,
                            bytes_total=total,
                        )
                    if chunk.is_last:
                        last_seen = True
                        break
                if not last_seen:
                    _abort(context, grpc.StatusCode.INVALID_ARGUMENT, "stream ended before is_last")
                handle.flush()
                os.fsync(handle.fileno())
            destination.parent.mkdir(parents=True, exist_ok=True)
            temp_path.replace(destination)
            _fsync_dir(destination.parent)
            server_sha = digest.hexdigest()
            self.progress_registry.update_file(
                row.intake_id,
                relpath=relpath,
                bytes_received=received,
                bytes_total=max(total, received),
            )
            with runtime.ledger_lock:
                _append_receipt(intake_dir, relpath=relpath, digest=server_sha, size=received)
            return intake_pb2.FileReceipt(
                relpath=relpath,
                server_sha256=server_sha,
                received_bytes=received,
            )
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise

    def _identity(self, context: Any) -> grpc_store.DeviceIdentity:
        try:
            return grpc_ca.resolve_peer_identity(self.config.engine, context)
        except PermissionError as exc:
            _abort(context, grpc.StatusCode.UNAUTHENTICATED, str(exc))

    def _owned_row(self, intake_id: str, context: Any) -> grpc_store.GrpcIntake:
        identity = self._identity(context)
        factory = make_session_factory(self.config.engine)
        with factory() as session:
            row = grpc_store.get_intake(session, intake_id)
            if row is None:
                _abort(context, grpc.StatusCode.NOT_FOUND, "unknown intake")
            if row.operator != identity.operator or row.device_id != identity.device_id:
                _abort(context, grpc.StatusCode.PERMISSION_DENIED, "intake owner mismatch")
            session.expunge(row)
            return row

    def _runtime_for(self, intake_id: str) -> _RuntimeState:
        with self._runtime_lock:
            return self._runtime.setdefault(intake_id, _RuntimeState())

    def _rollback_to_streaming(self, intake_id: str) -> None:
        factory = make_session_factory(self.config.engine)
        with factory.begin() as session:
            grpc_store.set_state(session, intake_id, "streaming")

    def _live_status(self, row: grpc_store.GrpcIntake) -> str:
        return intake_status(row).status

    def _validate_start_request(self, request: Any, context: Any) -> None:
        if not request.idempotency_key:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required")
        if not request.artifactclass:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, "artifactclass is required")
        if not request.source_plan_digest:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, "source_plan_digest is required")
        if request.source_kind not in {"card", "drive", "upload", "handoff", "download", "other"}:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, "invalid source_kind")
        if self.config.validate_artifactclass:
            try:
                factory = make_session_factory(self.config.engine)
                with factory() as session:
                    get_artifactclass_policy(session, request.artifactclass)
            except ArtifactClassPolicyError as exc:
                _abort(context, grpc.StatusCode.INVALID_ARGUMENT, str(exc))

    def _assert_resume_source_plan(
        self,
        intake_id: str,
        identity: grpc_store.DeviceIdentity,
        source_plan_digest: str,
        context: Any,
    ) -> None:
        if not intake_id:
            _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "resume has no intake id")
        factory = make_session_factory(self.config.engine)
        with factory() as session:
            row = grpc_store.get_intake(session, intake_id)
            if row is None:
                _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "resume intake is unknown")
            if row.operator != identity.operator or row.device_id != identity.device_id:
                _abort(context, grpc.StatusCode.PERMISSION_DENIED, "intake owner mismatch")
            if row.source_plan_digest != source_plan_digest:
                _abort(
                    context,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "source changed; start a new intake",
                )

    def _abort_if_resume_source_changed(
        self,
        request: Any,
        identity: grpc_store.DeviceIdentity,
        context: Any,
    ) -> None:
        factory = make_session_factory(self.config.engine)
        with factory() as session:
            record = session.scalars(
                select(api_store.IdempotencyRecord).where(
                    api_store.IdempotencyRecord.operator_username == identity.operator,
                    api_store.IdempotencyRecord.endpoint == grpc_store.GRPC_START_ENDPOINT,
                    api_store.IdempotencyRecord.idempotency_key == request.idempotency_key,
                )
            ).one_or_none()
            if record is None or record.status != "completed" or record.intake_id is None:
                return
            row = grpc_store.get_intake(session, record.intake_id)
            if row is None or row.operator != identity.operator or row.device_id != identity.device_id:
                return
            if row.source_plan_digest != request.source_plan_digest:
                _abort(
                    context,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "source changed; start a new intake",
                )


def _start_request_hash(request: Any) -> str:
    payload = {
        "artifactclass": request.artifactclass,
        "source_kind": request.source_kind,
        "source_ref": request.source_ref,
        "label": request.label,
        "source_plan_digest": request.source_plan_digest,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _validate_wire_relpath(raw: str, context: Any) -> str:
    if raw.startswith(f"{DATA_DIR_NAME}/"):
        _abort(context, grpc.StatusCode.INVALID_ARGUMENT, "relpath must not start with data/")
    pure = PurePosixPath(raw)
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        _abort(context, grpc.StatusCode.INVALID_ARGUMENT, "unsafe relpath")
    canonical = canonicalize_manifest_path(raw)
    if raw != canonical:
        _abort(context, grpc.StatusCode.INVALID_ARGUMENT, "relpath is not canonical")
    return canonical


def _read_receipts(intake_dir: Path) -> dict[str, tuple[str, int]]:
    path = intake_dir / "receive-receipts.jsonl"
    records: dict[str, tuple[str, int]] = {}
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        payload = json.loads(line)
        relpath = canonicalize_manifest_path(str(payload["relpath"]))
        records[relpath] = (str(payload["server_sha256"]), int(payload["bytes"]))
    return records


def _clear_incoming(intake_dir: Path) -> None:
    incoming = intake_dir / ".incoming"
    if not incoming.exists():
        return
    for child in incoming.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink(missing_ok=True)


def _append_receipt(intake_dir: Path, *, relpath: str, digest: str, size: int) -> None:
    path = intake_dir / "receive-receipts.jsonl"
    payload = {"relpath": relpath, "server_sha256": digest, "bytes": size}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _reupload_relpaths(
    receipts: dict[str, tuple[str, int]],
    files: Iterable[Any],
) -> list[str]:
    missing: list[str] = []
    for item in files:
        relpath = canonicalize_manifest_path(str(item.relpath))
        landed = receipts.get(relpath)
        if landed is None:
            missing.append(relpath)
            continue
        digest, size = landed
        if digest != str(item.client_sha256).lower() or size != int(item.bytes):
            missing.append(relpath)
    return sorted(missing)


def _intake_dir(row: grpc_store.GrpcIntake) -> Path:
    return Path(row.landing_root) / row.intake_id


def _planned_bytes_total(request: Any) -> int | None:
    value = int(getattr(request, "planned_bytes_total", 0) or 0)
    return value if value > 0 else None


def _mint_intake_id(operator: str, now: dt.datetime, landing_root: Path) -> str:
    prefix = f"{now:%Y%m%d}-{slug_operator(operator)}"
    for _ in range(100):
        candidate = f"{prefix}-{uuid.uuid4().hex}"
        if not (landing_root / candidate).exists():
            return candidate
    raise RuntimeError("could not mint unique intake id")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _fsync_dir(path.parent)


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _abort(context: Any, code: grpc.StatusCode, message: str) -> Any:
    context.abort(code, message)
    raise RuntimeError(message)
