"""Manifest of schema-wide vocabulary, identity, ownership, and clock conventions.

The Wave 1 reflection gate imports this module and compares every declared
column and foreign key with SQLAlchemy metadata.  Application models also build
their vocabulary CHECK constraints from these values, so a closed set has one
authoritative declaration rather than copied SQL literals.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ForeignKeyRole = Literal["ownership", "reference", "audit-subject"]


@dataclass(frozen=True)
class Vocabulary:
    """One closed string set and whether SQL NULL is also admissible."""

    values: tuple[str, ...]
    nullable: bool = False


@dataclass(frozen=True)
class ForeignKeyConvention:
    """The semantic role assigned to one exact foreign-key constraint."""

    role: ForeignKeyRole


VOCABULARIES: dict[str, Vocabulary] = {
    "arrangement_status": Vocabulary(("draft", "submitted", "abandoned")),
    "asset_derivation_kind": Vocabulary(("mezz", "preview")),
    "asset_review_action": Vocabulary(("reject", "unreject")),
    "asset_validity": Vocabulary(("ok", "suspect", "unvalidated")),
    "backend_tier": Vocabulary(("self_describing", "catalog_authoritative")),
    "bundle_status": Vocabulary(("open", "flushing", "sealed", "held", "aborted")),
    "cache_capacity_state": Vocabulary(("ok", "over_reserve")),
    "cache_disk_state": Vocabulary(("active", "absent", "retiring", "dead")),
    "cache_entry_representation": Vocabulary(("raw-bytes", "rao-aead-v1")),
    "cache_entry_state": Vocabulary(("filling", "present", "lost")),
    "copy_health": Vocabulary(("ok", "suspect", "corrupt", "missing")),
    "copy_source": Vocabulary(("ingest", "scrub", "manual_import")),
    "derivation_condition": Vocabulary(("open", "backoff", "blocked", "suppressed", "satisfied")),
    "grpc_intake_state": Vocabulary(("streaming", "committing", "committed", "aborted")),
    "idempotency_status": Vocabulary(
        (
            "in_progress",
            "completed",
            "warned",
            "authorized",
            "started",
            "committed",
            "aborted",
            "quarantined",
            "failed",
        )
    ),
    "ingest_disposition": Vocabulary(
        ("new", "known_durable", "known_under_durable", "reverified", "legacy_unknown")
    ),
    "integrity_hash_provenance": Vocabulary(("locally_computed", "backend_discovered")),
    "intake_source_kind": Vocabulary(("card", "drive", "upload", "handoff", "download", "other")),
    "intake_status": Vocabulary(("verifying", "quarantined", "registered")),
    "job_attempt_outcome": Vocabulary(("succeeded", "failed", "cancelled")),
    "job_status": Vocabulary(("pending", "queued", "running", "succeeded", "failed", "cancelled")),
    "media_kind": Vocabulary(("video", "audio", "image", "document", "other"), nullable=True),
    "observed_state": Vocabulary(("present", "missing")),
    "operator_capability": Vocabulary(
        (
            "can_view",
            "can_receive",
            "can_restore",
            "can_logs",
            "can_admin",
            "can_restore_p2",
            "can_restore_p3",
        )
    ),
    "restore_delivery_mode": Vocabulary(("server_local", "agent")),
    "restore_item_denial_kind": Vocabulary(
        ("capability", "privacy_unmapped", "suspect", "rejected"), nullable=True
    ),
    "restore_item_source": Vocabulary(("cache", "tape"), nullable=True),
    "restore_item_state": Vocabulary(
        (
            "queued",
            "waking_disk",
            "streaming",
            "sent",
            "done",
            "fell_back_to_tape",
            "denied",
            "failed",
        )
    ),
    "restore_request_state": Vocabulary(
        ("pending", "active", "completed", "completed_with_errors")
    ),
    "retention_action": Vocabulary(
        (
            "released",
            "cloud_blob_deleted",
            "staging_deleted",
            "release_attempted",
            "purge_attempted",
            "staging_tombstoned",
            "staging_purge_held",
            "batch_invoked",
            "batch_refused",
            "grace_overridden",
            "abandoned",
            "correction_recorded",
            "offsite_confirmed",
        )
    ),
    "retention_state": Vocabulary(
        ("not_applicable", "held", "released", "tombstoned", "abandoned", "purged")
    ),
    "review_action": Vocabulary(("wrap", "blob", "exclude", "fix-source-and-rescan", "abort")),
    "review_scope": Vocabulary(("just-this-ingest", "persist-rule")),
    "submission_status": Vocabulary(("pending_archive", "archived")),
    "verify_receipt_source": Vocabulary(("fanout", "verify-job", "restore", "scrub")),
}


CLOSED_VOCABULARY_COLUMNS: dict[str, str] = {
    "arrangement.status": "arrangement_status",
    "asset_derivation.kind": "asset_derivation_kind",
    "asset_review_event.action": "asset_review_action",
    "backend.tier": "backend_tier",
    "bundle.status": "bundle_status",
    "cache_disk.capacity_state": "cache_capacity_state",
    "cache_disk.state": "cache_disk_state",
    "cache_entry.representation": "cache_entry_representation",
    "cache_entry.state": "cache_entry_state",
    "copy.health": "copy_health",
    "copy.integrity_hash_provenance": "integrity_hash_provenance",
    "copy.source": "copy_source",
    "grpc_intake.state": "grpc_intake_state",
    "idempotency_record.status": "idempotency_status",
    "ingest_item.disposition": "ingest_disposition",
    "intake.retention_state": "retention_state",
    "intake.source_kind": "intake_source_kind",
    "intake.status": "intake_status",
    "job.status": "job_status",
    "job_attempt.outcome": "job_attempt_outcome",
    "logical_asset.media_kind": "media_kind",
    "logical_asset.validity": "asset_validity",
    "operator_live_capability.capability": "operator_capability",
    "reconciliation_condition.condition": "derivation_condition",
    "reconciliation_condition.observed_state": "observed_state",
    "retention_event.action": "retention_action",
    "restore_request.delivery_mode": "restore_delivery_mode",
    "restore_request.state": "restore_request_state",
    "restore_request_item.denial_kind": "restore_item_denial_kind",
    "restore_request_item.source": "restore_item_source",
    "restore_request_item.state": "restore_item_state",
    "review_decision.action": "review_action",
    "review_decision.scope": "review_scope",
    "submission.status": "submission_status",
    "verify_receipt.source": "verify_receipt_source",
}


def vocabulary_check_sql(column: str, vocabulary: str) -> str:
    """Render the portable SQL fragment used by one vocabulary CHECK."""

    declaration = VOCABULARIES[vocabulary]
    quoted = ", ".join(f"'{value}'" for value in declaration.values)
    check = f"{column} IN ({quoted})"
    return f"{column} IS NULL OR {check}" if declaration.nullable else check


ALLOWED_ON_DELETE_BY_ROLE: dict[ForeignKeyRole, frozenset[str]] = {
    "ownership": frozenset({"CASCADE"}),
    "reference": frozenset({"RESTRICT", "SET NULL"}),
    "audit-subject": frozenset({"RESTRICT"}),
}

# Explicit semantic role for every metadata FK. The key format is
# ``source_table.source_columns->target_table.target_columns``; composite
# columns are comma-separated in constraint order. No role is guessed from a
# table or column name.
FOREIGN_KEYS: dict[str, ForeignKeyConvention] = {
    "arrangement.artifactclass->artifactclass.name": ForeignKeyConvention("reference"),
    "arrangement.cloned_from_arrangement_id->arrangement.id": ForeignKeyConvention("reference"),
    "arrangement.intake_id->intake.intake_id": ForeignKeyConvention("ownership"),
    "arrangement.submission_id->submission.id": ForeignKeyConvention("reference"),
    "arrangement_member.arrangement_id->arrangement.id": ForeignKeyConvention("ownership"),
    "arrangement_member.ingest_item_id->ingest_item.id": ForeignKeyConvention("audit-subject"),
    "artifactclass_policy.artifactclass->artifactclass.name": ForeignKeyConvention("reference"),
    "artifactclass_pool.artifactclass->artifactclass.name": ForeignKeyConvention("reference"),
    "artifactclass_pool.pool_id->pool.id": ForeignKeyConvention("ownership"),
    "asset_derivation.derived_item_id->ingest_item.id": ForeignKeyConvention("audit-subject"),
    "asset_derivation.source_item_id->ingest_item.id": ForeignKeyConvention("audit-subject"),
    "asset_locator.bundle_id->bundle.id": ForeignKeyConvention("audit-subject"),
    "asset_locator.bundle_id,logical_asset_hash,member_path->bundle_member.bundle_id,logical_asset_hash,member_path": ForeignKeyConvention(
        "audit-subject"
    ),
    "asset_locator.copy_id,pool_id,bundle_id->copy.id,pool_id,bundle_id": ForeignKeyConvention(
        "audit-subject"
    ),
    "asset_locator.logical_asset_hash->logical_asset.content_sha256": ForeignKeyConvention(
        "audit-subject"
    ),
    "asset_locator.pool_id->pool.id": ForeignKeyConvention("audit-subject"),
    "asset_review_event.logical_asset_hash->logical_asset.content_sha256": ForeignKeyConvention(
        "audit-subject"
    ),
    "asset_tag.logical_asset_hash->logical_asset.content_sha256": ForeignKeyConvention(
        "audit-subject"
    ),
    "bundle.artifactclass->artifactclass.name": ForeignKeyConvention("reference"),
    "bundle_member.bundle_id->bundle.id": ForeignKeyConvention("ownership"),
    "bundle_member.logical_asset_hash->logical_asset.content_sha256": ForeignKeyConvention(
        "audit-subject"
    ),
    "cache_entry.artifactclass->artifactclass.name": ForeignKeyConvention("reference"),
    "cache_entry.content_sha256->logical_asset.content_sha256": ForeignKeyConvention("reference"),
    "cache_entry.disk_id->cache_disk.disk_id": ForeignKeyConvention("ownership"),
    "condition_component.condition_id->reconciliation_condition.id": ForeignKeyConvention(
        "ownership"
    ),
    "copy.backend_id->backend.id": ForeignKeyConvention("audit-subject"),
    "copy.bundle_id->bundle.id": ForeignKeyConvention("audit-subject"),
    "copy.logical_asset_hash->logical_asset.content_sha256": ForeignKeyConvention("audit-subject"),
    "copy.pool_id->pool.id": ForeignKeyConvention("audit-subject"),
    "exclusion_record.artifactclass->artifactclass.name": ForeignKeyConvention("reference"),
    "exclusion_record.bundle_id->bundle.id": ForeignKeyConvention("reference"),
    "exclusion_record.logical_asset_hash->logical_asset.content_sha256": ForeignKeyConvention(
        "reference"
    ),
    "grpc_device_destination_grant.device_id->grpc_logical_device.device_id": ForeignKeyConvention(
        "ownership"
    ),
    "grpc_device_enrollment.device_id->grpc_logical_device.device_id": ForeignKeyConvention(
        "ownership"
    ),
    "grpc_intake.artifactclass->artifactclass.name": ForeignKeyConvention("reference"),
    "ingest_item.artifactclass->artifactclass.name": ForeignKeyConvention("reference"),
    "ingest_item.intake_id->intake.intake_id": ForeignKeyConvention("ownership"),
    "ingest_item.logical_asset_hash->logical_asset.content_sha256": ForeignKeyConvention(
        "audit-subject"
    ),
    "ingest_item.prior_intake_id->intake.intake_id": ForeignKeyConvention("audit-subject"),
    "intake.artifactclass->artifactclass.name": ForeignKeyConvention("reference"),
    "job_attempt.job_id->job.id": ForeignKeyConvention("reference"),
    "operator_live_capability.operator->operator_capability_sync.operator": ForeignKeyConvention(
        "ownership"
    ),
    "pool.backend_id->backend.id": ForeignKeyConvention("ownership"),
    "reconciliation_condition.last_attempt_id->job_attempt.id": ForeignKeyConvention("reference"),
    "restore_item_checkpoint.restore_request_item_id->restore_request_item.id": ForeignKeyConvention(
        "ownership"
    ),
    "restore_open_session.receiver_device_id->grpc_logical_device.device_id": ForeignKeyConvention(
        "ownership"
    ),
    "restore_open_session.restore_request_item_id->restore_request_item.id": ForeignKeyConvention(
        "ownership"
    ),
    "restore_request.receiver_device_id->grpc_logical_device.device_id": ForeignKeyConvention(
        "reference"
    ),
    "restore_request_item.artifactclass->artifactclass.name": ForeignKeyConvention("reference"),
    "restore_request_item.content_sha256->logical_asset.content_sha256": ForeignKeyConvention(
        "reference"
    ),
    "restore_request_item.request_id->restore_request.id": ForeignKeyConvention("ownership"),
    "retention_event.intake_id->intake.intake_id": ForeignKeyConvention("audit-subject"),
    "review_decision.bundle_id->bundle.id": ForeignKeyConvention("audit-subject"),
    "staging_transform.artifactclass->artifactclass.name": ForeignKeyConvention("reference"),
    "staging_transform.bundle_id->bundle.id": ForeignKeyConvention("ownership"),
    "staging_transform.bundle_member_id,bundle_id,logical_asset_hash->bundle_member.id,bundle_id,logical_asset_hash": ForeignKeyConvention(
        "audit-subject"
    ),
    "staging_transform.logical_asset_hash->logical_asset.content_sha256": ForeignKeyConvention(
        "audit-subject"
    ),
    "submission.arrangement_id->arrangement.id": ForeignKeyConvention("ownership"),
    "submission.artifactclass->artifactclass.name": ForeignKeyConvention("reference"),
    "submission_member.ingest_item_id->ingest_item.id": ForeignKeyConvention("audit-subject"),
    "submission_member.submission_id->submission.id": ForeignKeyConvention("ownership"),
    "verify_receipt.backend_id->backend.id": ForeignKeyConvention("audit-subject"),
    "verify_receipt.copy_id->copy.id": ForeignKeyConvention("audit-subject"),
    "virtual_arrangement_history.artifactclass->artifactclass.name": ForeignKeyConvention(
        "reference"
    ),
    "virtual_arrangement_history.logical_asset_hash->logical_asset.content_sha256": ForeignKeyConvention(
        "audit-subject"
    ),
    "virtual_arrangement_history.va_id->virtual_arrangement.id": ForeignKeyConvention(
        "audit-subject"
    ),
    "virtual_arrangement_history.va_member_id->virtual_arrangement_member.id": ForeignKeyConvention(
        "audit-subject"
    ),
    "virtual_arrangement_member.artifactclass->artifactclass.name": ForeignKeyConvention(
        "reference"
    ),
    "virtual_arrangement_member.logical_asset_hash->logical_asset.content_sha256": ForeignKeyConvention(
        "reference"
    ),
    "virtual_arrangement_member.va_id->virtual_arrangement.id": ForeignKeyConvention("ownership"),
}

REGISTRY_TABLES: dict[str, tuple[str, ...]] = {
    "artifactclass": ("name",),
}

# Every current-state projection clock named ``updated_at`` is explicitly
# listed. The reflection gate requires SQLAlchemy ``onupdate`` behavior on all
# of them and rejects undeclared additions.
UPDATED_AT_COLUMNS = frozenset(
    {
        "arrangement.updated_at",
        "arrangement_member.updated_at",
        "artifactclass_policy.updated_at",
        "grpc_intake.updated_at",
        "grpc_logical_device.updated_at",
        "idempotency_record.updated_at",
        "intake.updated_at",
        "reconciliation_condition.updated_at",
        "restore_item_checkpoint.updated_at",
        "restore_open_session.updated_at",
        "restore_request_item.updated_at",
        "source_claim.updated_at",
        "virtual_arrangement.updated_at",
        "virtual_arrangement_member.updated_at",
    }
)

# These groups document same-shaped columns that actually share meaning. They
# intentionally keep unrelated ``id``, ``kind``, and ``status`` columns apart.
SEMANTIC_COLUMN_GROUPS: dict[str, frozenset[str]] = {
    "content_sha256_binary": frozenset(
        {
            "logical_asset.content_sha256",
            "ingest_item.logical_asset_hash",
            "bundle_member.logical_asset_hash",
            "copy.logical_asset_hash",
            "asset_locator.logical_asset_hash",
            "submission_member.sha256",
        }
    ),
    "document_sha256_hex": frozenset(
        {
            "intake.manifest_digest",
            "grpc_intake.manifest_digest",
            "submission.manifest_digest",
            "artifactclass_policy.policy_sha256",
            "exclusion_record.ruleset_hash",
        }
    ),
    "versioned_fingerprint": frozenset({"intake.release_policy_fingerprint"}),
    "soft_exclusion_projection": frozenset(
        {"arrangement_member.excluded", "virtual_arrangement_member.excluded"}
    ),
    "attributed_removal": frozenset({"asset_tag.removed_at", "asset_tag.removed_by"}),
    "physical_copy_deletion": frozenset({"copy.deleted_at"}),
    "pool_retirement": frozenset({"pool.retired"}),
}

IDENTIFIER_CONVENTIONS: dict[str, tuple[str, ...]] = {
    "arrangement": ("id",),
    "arrangement_member": ("id",),
    "artifactclass": ("name",),
    "artifactclass_policy": ("artifactclass",),
    "artifactclass_pool": ("id",),
    "asset_derivation": ("id",),
    "asset_locator": ("id",),
    "asset_review_event": ("id",),
    "asset_tag": ("id",),
    "backend": ("id",),
    "bundle": ("id",),
    "bundle_member": ("id",),
    "cache_disk": ("disk_id",),
    "cache_entry": ("content_sha256",),
    "condition_component": ("id",),
    "copy": ("id",),
    "exclusion_record": ("id",),
    "grpc_device_destination_grant": ("id",),
    "grpc_device_enrollment": ("id",),
    "grpc_enroll_token": ("token",),
    "grpc_intake": ("intake_id",),
    "grpc_logical_device": ("device_id",),
    "idempotency_record": ("id",),
    "ingest_item": ("id",),
    "intake": ("intake_id",),
    "job": ("id",),
    "job_attempt": ("id",),
    "logical_asset": ("content_sha256",),
    "offsite_confirmation": ("media_id",),
    "operator_capability_sync": ("operator",),
    "operator_live_capability": ("id",),
    "pool": ("id",),
    "reconciliation_condition": ("id",),
    "restore_item_checkpoint": ("restore_request_item_id",),
    "restore_open_session": ("restore_request_item_id",),
    "restore_request": ("id",),
    "restore_request_item": ("id",),
    "retention_event": ("event_id",),
    "retention_journal_checkpoint": ("id",),
    "review_decision": ("id",),
    "source_claim": ("source_id",),
    "staging_transform": ("id",),
    "submission": ("id",),
    "submission_member": ("id",),
    "verify_receipt": ("event_id",),
    "virtual_arrangement": ("id",),
    "virtual_arrangement_history": ("id",),
    "virtual_arrangement_member": ("id",),
}

JSON_COLUMN_CONTRACTS: dict[str, dict[str, object]] = {
    "asset_locator.native_locator": {
        "forbidden_keys": ("member_path",),
        "meaning": "backend-native coordinates only; member_path is typed",
    },
    "ingest_item.metadata": {
        "forbidden_keys": ("source_path", "pfr_sidecar_path", "sha256"),
        "meaning": "non-identity occurrence metadata only; promoted facts are typed",
    },
    "job.prerequisites": {
        "item_type": "job_id",
        "meaning": "ordered prerequisite job-id list; attempt-fact promotion is Wave 2",
    },
}

TIMESTAMP_CONVENTIONS: dict[str, str] = {
    "reconciliation_condition.observed_at": "latest observation look",
    "reconciliation_condition.condition_changed_at": "latest condition value transition",
    "reconciliation_condition.updated_at": "latest projection mutation",
    "reconciliation_condition.reopened_at": "latest attributed reopen event",
    "retention_event.occurred_at": "retention event occurrence",
    "verify_receipt.recorded_at": "receipt recording time",
    "copy.health_changed_at": "database-maintained health transition",
}
