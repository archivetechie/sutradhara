"""Streaming gRPC receive client for ``sutra-agent``.

The client reuses ``sutradhara_receive`` payload planning, reads each source
unit through that planner exactly once for upload, uploads units in parallel
with synchronous gRPC stubs, and waits for the server/watch registrar status
before declaring a source release decision.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import grpc

from sutra_agent._proto import intake_pb2, intake_pb2_grpc
from sutra_agent.config import AgentConfig
from sutra_agent.ledger import (
    ConfirmationSnapshot,
    lookup_stream_resume,
    record_stream_resume,
)
from sutradhara_receive import (
    CANONICALIZATION_VERSION,
    PACKAGE_PROFILE_VERSION,
    PayloadPlan,
    PayloadUnit,
    plan_payload_units,
)


class TransitCorruptionError(RuntimeError):
    """Raised when the server receipt digest differs from the client digest."""


@dataclass(frozen=True)
class StreamReceiveResult:
    """Summary of a completed streaming receive attempt."""

    intake_id: str
    file_count: int
    total_bytes: int
    skipped_count: int
    confirmation: ConfirmationSnapshot
    idempotency_key: str
    plan_digest: str


def stream_source(
    source: Path,
    *,
    config: AgentConfig,
    source_ref: str | None = None,
    label: str | None = None,
    idempotency_key: str | None = None,
    confirm_timeout: float | None = None,
    confirm_interval: float | None = None,
) -> StreamReceiveResult:
    """Stream a source tree to the configured gRPC intake server."""

    if not config.streaming_enabled:
        raise ValueError("stream_source requires streaming AgentConfig")
    key = idempotency_key or str(uuid.uuid4())
    plan = plan_payload_units(source)
    plan_digest = plan.source_plan_digest()
    ledger_path = config.resolved_ledger_path()

    with _channel(config) as channel:
        stub = intake_pb2_grpc.IntakeServiceStub(channel)
        start = stub.StartIntake(
            intake_pb2.StartIntakeRequest(
                idempotency_key=key,
                artifactclass=config.artifactclass,
                source_kind=config.source_kind,
                source_ref=source_ref or "",
                label=label or "",
                source_plan_digest=plan_digest,
            )
        )
        intake_id = start.intake_id
        local_resume = lookup_stream_resume(ledger_path, key)
        trusted = (
            local_resume is not None
            and local_resume.intake_id == intake_id
            and local_resume.plan_digest == plan_digest
        )
        landed = {
            item.relpath: (item.server_sha256, int(item.bytes))
            for item in stub.ListIntakeFiles(
                intake_pb2.ListIntakeFilesRequest(intake_id=intake_id)
            ).files
        }
        units_by_relpath = {unit.relpath: unit for unit in plan.units}
        package_indexes: dict[str, dict[str, Any]] = (
            dict(local_resume.package_indexes) if local_resume and trusted else {}
        )
        manifest: dict[str, tuple[str, int]] = {}
        if trusted:
            manifest.update(landed)
        else:
            for relpath, landed_record in landed.items():
                unit = units_by_relpath.get(relpath)
                if unit is None:
                    continue
                digest, size, package_index = _hash_unit(unit, chunk_bytes=config.chunk_bytes)
                if (digest, size) == landed_record:
                    manifest[relpath] = landed_record
                    if package_index is not None:
                        package_indexes[relpath] = package_index

        record_stream_resume(
            ledger_path,
            idempotency_key=key,
            intake_id=intake_id,
            plan_digest=plan_digest,
            package_indexes=package_indexes,
        )

        remaining = [unit for unit in plan.units if unit.relpath not in manifest]
        _upload_units(
            stub,
            intake_id=intake_id,
            units=remaining,
            config=config,
            manifest=manifest,
            package_indexes=package_indexes,
        )
        response = _commit(stub, intake_id, plan, manifest, package_indexes)
        while response.reupload_relpaths:
            reupload = [units_by_relpath[relpath] for relpath in response.reupload_relpaths]
            _upload_units(
                stub,
                intake_id=intake_id,
                units=reupload,
                config=config,
                manifest=manifest,
                package_indexes=package_indexes,
            )
            response = _commit(stub, intake_id, plan, manifest, package_indexes)
        record_stream_resume(
            ledger_path,
            idempotency_key=key,
            intake_id=intake_id,
            plan_digest=plan_digest,
            package_indexes=package_indexes,
        )
        confirmation = _poll_status(
            stub,
            intake_id,
            timeout=confirm_timeout or 0,
            interval=confirm_interval or config.confirm_interval_seconds,
        )
        return StreamReceiveResult(
            intake_id=intake_id,
            file_count=len(manifest),
            total_bytes=sum(size for _digest, size in manifest.values()),
            skipped_count=plan.skipped_count,
            confirmation=confirmation,
            idempotency_key=key,
            plan_digest=plan_digest,
        )


def get_stream_status(config: AgentConfig, intake_id: str) -> ConfirmationSnapshot:
    """Read current gRPC intake/watch status."""

    with _channel(config) as channel:
        stub = intake_pb2_grpc.IntakeServiceStub(channel)
        response = stub.GetIntakeStatus(intake_pb2.IntakeStatusRequest(intake_id=intake_id))
    return _confirmation_from_status(response.status, response.errors)


def _upload_units(
    stub: Any,
    *,
    intake_id: str,
    units: list[PayloadUnit],
    config: AgentConfig,
    manifest: dict[str, tuple[str, int]],
    package_indexes: dict[str, dict[str, Any]],
) -> None:
    if not units:
        return
    with ThreadPoolExecutor(max_workers=config.parallelism) as pool:
        futures = {
            pool.submit(_upload_one, stub, intake_id, unit, config.chunk_bytes): unit
            for unit in units
        }
        for future in as_completed(futures):
            relpath, digest, size, package_index = future.result()
            manifest[relpath] = (digest, size)
            if package_index is not None:
                package_indexes[relpath] = package_index


def _upload_one(
    stub: Any,
    intake_id: str,
    unit: PayloadUnit,
    chunk_bytes: int,
) -> tuple[str, str, int, dict[str, Any] | None]:
    digest = hashlib.sha256()
    total = 0

    def chunks() -> Any:
        nonlocal total
        first = True
        for data in unit.byte_chunks(chunk_bytes):
            offset = total
            total += len(data)
            digest.update(data)
            yield intake_pb2.FileChunk(
                intake_id=intake_id,
                relpath=unit.relpath,
                data=data,
                offset=offset,
                is_last=False,
                file_size=unit.hint_size if first else 0,
            )
            first = False
        yield intake_pb2.FileChunk(
            intake_id=intake_id,
            relpath=unit.relpath,
            data=b"",
            offset=total,
            is_last=True,
            file_size=unit.hint_size if first else 0,
        )

    receipt = stub.UploadFile(chunks())
    client_sha = digest.hexdigest()
    if receipt.server_sha256 != client_sha or int(receipt.received_bytes) != total:
        raise TransitCorruptionError(
            f"{unit.relpath}: client={client_sha}/{total}, "
            f"server={receipt.server_sha256}/{receipt.received_bytes}"
        )
    return unit.relpath, client_sha, total, unit.package_index(client_sha)


def _hash_unit(unit: PayloadUnit, *, chunk_bytes: int) -> tuple[str, int, dict[str, Any] | None]:
    digest = hashlib.sha256()
    total = 0
    for data in unit.byte_chunks(chunk_bytes):
        digest.update(data)
        total += len(data)
    sha = digest.hexdigest()
    return sha, total, unit.package_index(sha)


def _commit(
    stub: Any,
    intake_id: str,
    plan: PayloadPlan,
    manifest: dict[str, tuple[str, int]],
    package_indexes: dict[str, dict[str, Any]],
) -> Any:
    entries = [
        intake_pb2.ManifestEntry(relpath=relpath, client_sha256=digest, bytes=size)
        for relpath, (digest, size) in sorted(manifest.items())
    ]
    return stub.CommitIntake(
        intake_pb2.CommitIntakeRequest(
            intake_id=intake_id,
            files=entries,
            receive_facts=intake_pb2.ReceiveFacts(
                canonicalization_version=CANONICALIZATION_VERSION,
                skipped_count=plan.skipped_count,
                package_profile_version=PACKAGE_PROFILE_VERSION if package_indexes else "",
            ),
            package_indexes=[
                _package_index_proto(package_indexes[relpath])
                for relpath in sorted(package_indexes)
            ],
            manifest_digest=_manifest_digest(entries),
        )
    )


def _package_index_proto(payload: dict[str, Any]) -> Any:
    return intake_pb2.PackageIndex(
        logical_member_path=str(payload["logical_member_path"]),
        stored_member_path=str(payload["stored_member_path"]),
        sha256=str(payload["sha256"]),
        members=[_package_member_proto(item) for item in payload.get("members", [])],
    )


def _package_member_proto(payload: dict[str, Any]) -> Any:
    member_type = str(payload["type"])
    kwargs: dict[str, Any] = {
        "member": str(payload["member"]),
        "type": member_type,
        "length": int(payload.get("length", 0)),
    }
    if member_type == "file":
        kwargs["sha256"] = str(payload["sha256"])
        kwargs["data_offset"] = int(payload["data_offset"])
    if member_type == "symlink" and payload.get("linkname") is not None:
        kwargs["linkname"] = str(payload["linkname"])
    return intake_pb2.PackageMemberEntry(**kwargs)


def _manifest_digest(entries: list[Any]) -> str:
    payload = [
        {
            "relpath": item.relpath,
            "client_sha256": item.client_sha256,
            "bytes": int(item.bytes),
        }
        for item in entries
    ]
    return hashlib.sha256(
        json.dumps(sorted(payload, key=lambda item: item["relpath"]), sort_keys=True).encode("utf-8")
    ).hexdigest()


def _poll_status(
    stub: Any,
    intake_id: str,
    *,
    timeout: float,
    interval: float,
) -> ConfirmationSnapshot:
    deadline = time.monotonic() + max(0.0, timeout)
    while True:
        response = stub.GetIntakeStatus(intake_pb2.IntakeStatusRequest(intake_id=intake_id))
        if response.status in {"verified", "quarantined", "discrepancy"}:
            return _confirmation_from_status(response.status, response.errors)
        if time.monotonic() >= deadline:
            return ConfirmationSnapshot(status="pending", release_ok=False)
        time.sleep(interval)


def _confirmation_from_status(status: str, errors: Any) -> ConfirmationSnapshot:
    if status == "verified":
        return ConfirmationSnapshot(status="verified", release_ok=True)
    if status == "quarantined":
        return ConfirmationSnapshot(
            status="quarantined",
            release_ok=False,
            detail={"errors": list(errors)},
        )
    if status == "discrepancy":
        return ConfirmationSnapshot(
            status="discrepancy",
            release_ok=False,
            detail={"errors": list(errors)},
        )
    return ConfirmationSnapshot(status="pending", release_ok=False)


def _channel(config: AgentConfig) -> grpc.Channel:
    if config.server_address is None or config.client_cert is None or config.client_key is None or config.ca_cert is None:
        raise ValueError("streaming config is incomplete")
    credentials = grpc.ssl_channel_credentials(
        root_certificates=config.ca_cert.read_bytes(),
        private_key=config.client_key.read_bytes(),
        certificate_chain=config.client_cert.read_bytes(),
    )
    return grpc.secure_channel(config.server_address, credentials)
