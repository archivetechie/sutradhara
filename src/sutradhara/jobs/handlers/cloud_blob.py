"""`cloud-blob` job: write one encrypted RAO object for an intake."""

from __future__ import annotations

import datetime as dt
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Protocol, cast

from sqlalchemy import select

from sutradhara.archive_fanout import MemberInput
from sutradhara.backend import factory
from sutradhara.backend.port import CopyRecord
from sutradhara.catalog.copies import add_bundle_copy
from sutradhara.catalog.models import Backend, Bundle, Copy, IngestItem, Intake, Pool
from sutradhara.catalog.types import CopyHealth, CopySource
from sutradhara.jobs.registry import JobContext, JobResult, register_handler
from sutradhara.keys import KeyRegistry
from sutradhara.rem_archive_cli import run_rem_archive_build, sha256_file
from sutradhara.sealing.port import Representation


class KeyedObjectWriter(Protocol):
    def write_object(
        self, source: Path | str, *, key: str, pool: str | None = None
    ) -> CopyRecord: ...


@register_handler("cloud-blob")
def handle_cloud_blob(ctx: JobContext) -> JobResult:
    params = ctx.job.params
    intake_id = params.get("intake_id")
    if not isinstance(intake_id, str) or not intake_id:
        raise ValueError("cloud-blob job requires params.intake_id")
    intake = ctx.session.get(Intake, intake_id)
    if intake is None:
        raise ValueError(f"no Intake with id={intake_id!r}")

    backend_name = str(params.get("backend_name") or "cloud-temp")
    pool_id = str(params.get("pool_id") or "cloud-temp")
    backend_row = ctx.session.scalars(
        select(Backend).where(Backend.name == backend_name).limit(1)
    ).one_or_none()
    if backend_row is None:
        raise ValueError(f"no Backend named {backend_name!r}")
    pool = ctx.session.get(Pool, pool_id)
    if pool is None:
        raise ValueError(f"no Pool with id={pool_id!r}")
    if pool.backend_id != backend_row.id:
        raise ValueError(
            f"pool {pool_id!r} belongs to backend_id={pool.backend_id}, "
            f"not backend {backend_name!r}"
        )

    bundle_id = _bundle_id(intake_id)
    existing = ctx.session.scalars(
        select(Copy).where(Copy.bundle_id == bundle_id).limit(1)
    ).one_or_none()
    if existing is not None:
        return JobResult(
            ok=True,
            detail="cloud blob already copied",
            step_state={"cloud_blob": {"kind": "already_copied", "copy_id": existing.id}},
        )

    payload_root = Path(str(params.get("payload_root") or "")).resolve()
    if not payload_root.is_dir():
        raise ValueError(f"cloud-blob payload_root is not a directory: {payload_root}")
    intake_root = Path(str(params.get("intake_root") or payload_root.parent)).resolve()
    if not intake_root.is_dir():
        raise ValueError(f"cloud-blob intake_root is not a directory: {intake_root}")
    cache_root = Path(str(params.get("cache_root") or ".sutradhara-cache")).resolve()
    blob_dir = cache_root / "intakes" / intake.intake_id / "cloud"
    blob_dir.mkdir(parents=True, exist_ok=True)
    blob_path = blob_dir / f"{intake.intake_id}.rao"
    key_epoch = _cloud_key_epoch(params.get("key_epoch"))

    members = _member_inputs_for_intake(ctx, intake, intake_root, payload_root)
    bundle = _upsert_cloud_bundle(
        ctx,
        intake,
        bundle_id,
        total_bytes=sum(member.size_bytes for member in members),
        member_count=len(members),
    )
    stored_digest = _build_cloud_blob(
        bundle=bundle,
        members=members,
        intake_root=intake_root,
        payload_root=payload_root,
        destination=blob_path,
        key_epoch=key_epoch,
    )

    backend = factory.backend_from_row(backend_row)
    key = f"intakes/{intake.intake_id}.rao"
    if hasattr(backend, "write_object"):
        record = cast(KeyedObjectWriter, backend).write_object(blob_path, key=key, pool=pool_id)
    elif hasattr(backend, "write_object_to_pool"):
        record = backend.write_object_to_pool(blob_path, pool_id)
    else:
        raise ValueError(f"backend {backend_name!r} does not support object writes")

    copy, created = add_bundle_copy(
        ctx.session,
        bundle_id=bundle.id,
        backend_id=backend_row.id,
        pool_id=pool_id,
        native_locator=record.native_locator,
        integrity_hash=record.integrity_hash,
        source=CopySource.INGEST,
        health=CopyHealth.OK,
        storage_metadata={
            **record.metadata,
            "representation": Representation.RAO_AEAD_V1.value,
            "key_epoch": key_epoch,
            "payload_root": str(payload_root),
            "intake_root": str(intake_root),
            "member_count": len(members),
            "stored_digest": stored_digest.hex(),
        },
        last_verified_at=dt.datetime.now(dt.UTC),
    )
    bundle.status = "sealed"
    bundle.sealed_at = bundle.sealed_at or dt.datetime.now(dt.UTC)
    return JobResult(
        ok=True,
        detail="cloud blob copied" if created else "cloud blob copy already existed",
        step_state={
            "cloud_blob": {
                "kind": "ok",
                "copy_id": copy.id,
                "bundle_id": bundle.id,
                "native_locator": copy.native_locator,
                "blob_path": str(blob_path),
            }
        },
    )


def _build_cloud_blob(
    *,
    bundle: Bundle,
    members: list[MemberInput],
    intake_root: Path,
    payload_root: Path,
    destination: Path,
    key_epoch: str,
) -> bytes:
    if os.environ.get("SUTRADHARA_FAKE_CLOUD_BLOB") == "1":
        payload = {
            "representation": Representation.RAO_AEAD_V1.value,
            "intake_bundle_id": bundle.id,
            "payload_root": str(payload_root),
            "key_epoch": key_epoch,
            "members": [
                {
                    "member_path": member.member_path,
                    "sha256": member.file_sha256.hex(),
                    "size_bytes": member.size_bytes,
                }
                for member in members
            ],
        }
        destination.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return sha256_file(destination)

    with tempfile.TemporaryDirectory(prefix="sutradhara-cloud-blob-") as raw:
        work_dir = Path(raw)
        rules_path = work_dir / "rules.rem"
        manifest_path = work_dir / "manifest.json"
        rules_path.write_text("blob **/\n", encoding="utf-8")
        with KeyRegistry().materialized_root_key(key_epoch) as key_file:
            result = run_rem_archive_build(
                inputs=[intake_root],
                ruleset=rules_path,
                output_path=destination,
                manifest_path=manifest_path,
                encrypt=True,
                key_id=key_epoch,
                key_file=key_file,
                failure_label="rem archive build for cloud blob",
            )
        return result.stored_digest


def _cloud_key_epoch(value: Any) -> str:
    raw = _optional_str(value) or _optional_str(os.environ.get("SUTRADHARA_CLOUD_KEY_EPOCH"))
    if raw is not None:
        return raw
    return KeyRegistry().create_epoch().key_id


def _member_inputs_for_intake(
    ctx: JobContext,
    intake: Intake,
    intake_root: Path,
    payload_root: Path,
) -> list[MemberInput]:
    intake_root = intake_root.resolve()
    payload_root = payload_root.resolve()
    item_hashes = _payload_item_hashes(ctx, intake, payload_root)
    members: list[MemberInput] = []
    for source_path in sorted(path for path in intake_root.rglob("*") if path.is_file()):
        source_path = source_path.resolve()
        if not _is_under(source_path, intake_root):
            continue
        if not source_path.exists() or not source_path.is_file():
            raise ValueError(f"payload source path is unavailable: {source_path}")
        digest = item_hashes.get(source_path) or sha256_file(source_path)
        stat = source_path.stat()
        members.append(
            MemberInput(
                logical_asset_hash=digest,
                member_path=source_path.relative_to(intake_root).as_posix(),
                source_path=source_path,
                size_bytes=stat.st_size,
                file_sha256=digest,
            )
        )
    if not members:
        raise ValueError(f"intake {intake.intake_id!r} has no intake members")
    return members


def _payload_item_hashes(ctx: JobContext, intake: Intake, payload_root: Path) -> dict[Path, bytes]:
    rows = list(
        ctx.session.scalars(
            select(IngestItem)
            .where(IngestItem.intake_id == intake.intake_id)
            .order_by(IngestItem.as_received_path)
        )
    )
    result: dict[Path, bytes] = {}
    for item in rows:
        source_path = _item_source_path(item)
        if source_path is None:
            continue
        resolved = source_path.resolve()
        if _is_under(resolved, payload_root):
            result[resolved] = item.logical_asset_hash
    return result


def _upsert_cloud_bundle(
    ctx: JobContext,
    intake: Intake,
    bundle_id: str,
    *,
    total_bytes: int,
    member_count: int,
) -> Bundle:
    bundle = ctx.session.get(Bundle, bundle_id)
    if bundle is None:
        bundle = Bundle(
            id=bundle_id,
            artifactclass=intake.artifactclass,
            status="open",
            total_bytes=total_bytes,
            member_count=member_count,
            target_bytes=total_bytes,
            max_age_seconds=0,
            ruleset="blob **/",
            expect="compliant",
            archive_id=bundle_id,
        )
        ctx.session.add(bundle)
    else:
        bundle.artifactclass = intake.artifactclass
        bundle.total_bytes = total_bytes
        bundle.member_count = member_count
        bundle.target_bytes = total_bytes
        bundle.ruleset = "blob **/"
        bundle.expect = "compliant"
    ctx.session.flush()
    return bundle


def _item_source_path(item: IngestItem) -> Path | None:
    raw = (item.item_metadata or {}).get("source_path")
    return Path(raw) if isinstance(raw, str) and raw else None


def _is_under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _bundle_id(intake_id: str) -> str:
    return f"cloud-blob:{intake_id}"


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None
