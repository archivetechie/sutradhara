# Database schema reference

This is the reference for Sutradhara's application database. Think of the
database as the archive's working card catalogue: it records what the system
knows, what it intends to do, and the evidence behind those decisions. It does
not replace stored bytes. For self-describing backends, the catalogue can be
rebuilt by enumerating the backend.

The live source of truth is the SQLAlchemy model set in
`src/sutradhara/catalog/models.py`, `jobs/models.py`, and
`hdcache/models.py`, applied in order by `alembic/`. This page was checked
against those models and the Alembic head. Do not hand-edit a production
database: use the CLI/API and Alembic migrations. When this reference and the
models disagree, the models and migrations win; please fix this page.

## Reading this reference

- `PK` means primary key. `FK -> table.column` means a foreign key. Fields not
  marked optional are required (`NOT NULL`).
- `hash` is always a raw 32-byte SHA-256 value in the database, not a hex
  string. CLI and API boundaries normally render it as hex.
- `time` is a timezone-aware UTC timestamp. `json` is structured,
  feature-specific data rather than an untyped dumping ground.
- Rows called `copy` describe stored objects. A copy is either an asset copy or
  a bundle copy, never both. `asset_locator` is the bridge that lets a member
  of a stored bundle count as coverage for one logical asset.

## Why these boundaries exist

The most important modelling choice is to keep *content identity* separate
from *occurrence identity*. `logical_asset` says "these bytes exist"; an
`ingest_item` says "these bytes appeared at this path in this intake." That
preserves provenance when two cards contain the same file without paying to
store it twice. The same separation appears in storage: a `copy` names an
object written to a backend, while an `asset_locator` says where an individual
asset lives inside that object. This is what lets one bundled archive object
provide durable coverage for many assets without claiming that every asset was
written as an independent object.

The catalogue is operational state, not the only evidence of archive truth.
Self-describing backends can be enumerated to reconstruct it, while
catalog-authoritative ones are explicitly marked so their database backup
requirements are visible. That trade-off is described in more depth in
[`architecture-overview.md`](architecture-overview.md).

## Relationship map

`logical_asset` is the content identity. `ingest_item` records each appearance
of that content in an `intake`. An `arrangement` turns intake items into a
mutable archive namespace; a `submission` freezes that namespace. `bundle` is
the other archive unit: a synthetic object that holds `bundle_member` rows.
Backends contain policy-facing `pool` rows; `copy` records what was written and
`asset_locator` connects bundle members to those writes.

The job, reconciliation, cache, and restore tables share the same database so
operators can explain a decision without joining separate operational stores.

## Content and intake

### `logical_asset`

One row per distinct byte sequence. It is the anchor for content-level facts;
two files with the same SHA-256 share this row but retain separate provenance
through `ingest_item`.

| Field | Type / key | Meaning |
|---|---|---|
| `content_sha256` | `hash`, PK | Content identity; exactly 32 SHA-256 bytes. |
| `size_bytes` | bigint | Byte length of that content. |
| `first_seen_at` | time | When the catalogue first recorded this hash. |
| `human_label` | text, optional | Operator-friendly label; not identity. |
| `media_kind` | enum, optional | Coarse classification: `video`, `audio`, `image`, `document`, or `other`. |
| `media_info` | json, optional | Non-authoritative extracted media metadata. |
| `validity` | enum | Decode/parse verdict: `ok`, `suspect`, or `unvalidated`; defaults to `unvalidated`. |
| `validity_note` | text, optional | Explanation of the validity verdict. |
| `rejected_at` | time, optional | When an operator rejected restore/use of this asset. |
| `rejected_by` | text, optional | Actor who set the reject marker. |
| `rejection_reason` | text, optional | Reason for the reject marker. |

### `intake`

A landing batch at the acceptance boundary. A quarantined intake is retained
as evidence but creates no registered `ingest_item` rows.

| Field | Type / key | Meaning |
|---|---|---|
| `intake_id` | text, PK | Stable identifier for the received batch. |
| `operator` | text | Operator associated with receipt. |
| `source_kind` | enum | `card`, `drive`, `upload`, `handoff`, `download`, or `other`. |
| `source_ref` | text, optional | Human/source-system reference. |
| `card_id` | text, optional, indexed | Opaque physical-card identity used for duplicate-receive history. |
| `device_id` | text, optional | Enrolled helper/device that reported the card. |
| `artifactclass` | text, indexed | Policy class assigned to this intake. |
| `label` | text, optional | Human batch label. |
| `manifest_path` | text, optional | Landing manifest location. |
| `manifest_digest` | text, optional | SHA-256 digest of that manifest, rendered as hex. |
| `requested_profile` | text, optional | Requested derivation/prepare profile. |
| `status` | enum | `receiving`, `verifying`, `quarantined`, or `registered`; defaults to `receiving`. |
| `created_at`, `updated_at` | time | Creation and last state-update times. |
| `registered_at`, `quarantined_at` | time, optional | Terminal acceptance or quarantine time. |
| `retention_state` | enum | Landing-byte lifecycle: `held`, `released`, or `purged`; defaults to `held`. |
| `released_at` | time, optional | When policy made source staging releasable. |
| `staging_deleted_at` | time, optional | When temporary landing bytes were actually deleted. |

### `ingest_item`

One occurrence of a logical asset inside an intake. This is why repeated cards
can preserve their own provenance even when their bytes deduplicate.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Occurrence identifier. |
| `intake_id` | text, FK -> `intake.intake_id` | Intake that supplied the occurrence. |
| `logical_asset_hash` | hash, FK -> `logical_asset.content_sha256` | Content represented by this occurrence. |
| `as_received_path` | text | Path exactly as received; unique within an intake. |
| `virtual_path` | text | Normalized working path used before archive arrangement. |
| `st_dev`, `st_ino` | bigint, optional | Source filesystem device/inode evidence when available. |
| `size_bytes` | bigint | Observed item size. |
| `artifactclass` | text, indexed | Item's archive-policy class. |
| `metadata` | json | Per-occurrence metadata captured at registration. |
| `created_at` | time | Registration time. |

### `asset_derivation`

Provenance edge between two ingested occurrences. The uniqueness rule prevents
the same kind of edge being recorded twice for one source/derived pair.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Edge identifier. |
| `derived_item_id` | integer, FK -> `ingest_item.id` | Derived occurrence. |
| `source_item_id` | integer, FK -> `ingest_item.id` | Source occurrence. |
| `kind` | text | Derivation kind, such as a transcode profile. |
| `created_at` | time | When the provenance edge was recorded. |

## Arrangement and submission

### `arrangement`

A mutable pre-archive workspace over one intake. The submission link is set
only after the arrangement is frozen.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Workspace identifier. |
| `label` | text | Human workspace label. |
| `intake_id` | text, FK -> `intake.intake_id` | Source intake. |
| `artifactclass` | text, indexed | Policy class the workspace targets. |
| `status` | enum | `draft`, `pending_derivatives`, `ready`, `submitted`, or `abandoned`. |
| `submission_id` | text, optional, FK -> `submission.id` | Frozen submission produced from this workspace. |
| `cloned_from_arrangement_id` | integer, optional, FK -> `arrangement.id` | Earlier workspace copied to revise it. |
| `created_at`, `updated_at`, `submitted_at` | time | Creation, last edit, and freeze times; `submitted_at` is optional. |

### `arrangement_member`

One editable path in an arrangement. Its lifecycle is controlled by its parent
arrangement rather than a separate state field.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Member identifier. |
| `arrangement_id` | integer, FK -> `arrangement.id` | Parent workspace. |
| `ingest_item_id` | integer, FK -> `ingest_item.id` | Source occurrence. |
| `member_path` | text | Requested archive namespace path. |
| `excluded` | boolean | Whether this member is omitted from a submission. |
| `created_at`, `updated_at` | time | Member creation and last edit. |

### `submission`

An immutable source-map created by `sutra arrangement submit`. Revision means
cloning the arrangement, never editing this record.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | text, PK | Submission identifier. |
| `arrangement_id` | integer, unique FK -> `arrangement.id` | Arrangement frozen into this submission. |
| `artifactclass` | text, indexed | Policy class used for archive fan-out. |
| `source_map_path` | text | Filesystem path of the frozen source map. |
| `manifest_digest` | text | SHA-256 hex digest of the source map. |
| `member_count` | integer | Number of frozen members. |
| `status` | enum | `pending_archive` or `archived`; defaults to `pending_archive`. |
| `archived_at` | time, optional | When fan-out completed. |
| `submitted_by` | text | Actor who froze the source map. |
| `submitted_at` | time | Freeze time. |

### `submission_member`

The immutable member rows represented by a submission's source map.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Member identifier. |
| `submission_id` | text, FK -> `submission.id` | Parent submission. |
| `ingest_item_id` | integer, optional FK -> `ingest_item.id` | Original occurrence, when retained. |
| `archive_path` | text | Frozen archive path; unique within the submission. |
| `source_path` | text | Original source path used at archive time. |
| `sha256` | hash | Content hash expected for this member. |
| `size_bytes` | bigint | Member byte length. |
| `ord` | integer | Stable source-map order. |

## Storage policy and archive objects

### `backend`

A registered storage implementation, such as a Remanence library, d2tape, S3,
or an SSH disk. `implementation_family` is derived from `kind`, not selected
freely by an operator. This supports the durability rule that copies must span
independent implementation families rather than merely different locations.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Backend identifier. |
| `name` | text, unique | Operator-facing backend name. |
| `kind` | enum | Registered backend kind: `rem_tape`, `d2_tape`, disk/cloud kinds, or test-only `memory`. |
| `implementation_family` | text | Durability-family grouping such as `tape`, `d2tape`, `disk`, or `cloud`. |
| `config` | json, optional | Adapter configuration. |
| `tier` | enum | `self_describing` or `catalog_authoritative` recovery tier. |
| `added_at` | time | Registration time. |

### `pool`

The policy-facing write target beneath a backend. A backend describes how to
talk to storage; a pool describes where a policy is allowed to place data.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | text, PK | Pool identifier. |
| `backend_id` | integer, FK -> `backend.id` | Owning backend. |
| `representation` | text | Stored representation, for example raw bytes or RAO. |
| `location` | text | Operator-defined location label. |
| `offsite_gate` | boolean | Whether offsite confirmation gates retention. |
| `tier` | text | Policy tier label. |
| `accepts_writes` | boolean | Write fence; false prevents new placements. |
| `retired` | boolean | Whether the pool is retired from ordinary selection. |
| `media_generation` | text, optional | Media-generation or compatibility label. |
| `created_at` | time | Creation time. |

### `artifactclass_policy`

The compiled record of a strict artifactclass policy TOML. The policy itself
defines routing; this table keeps the validated values the runtime uses.

| Field | Type / key | Meaning |
|---|---|---|
| `artifactclass` | text, PK | Policy class name. |
| `ruleset`, `expect` | text | Bundling ruleset and expected input shape. |
| `target_bytes`, `max_age_seconds` | bigint / integer | Bundle size and age flush thresholds. |
| `restore_preference` | json | Ordered pool preference for restore. |
| `min_copies`, `min_impl_families` | integer | Durability floor; defaults are 3 copies and 2 implementation families. |
| `staging_config`, `hdcache_config` | json | Validated feature-specific policy sections. |
| `policy_source` | text, optional | Source file/path used when applied. |
| `policy_sha256` | text, optional | SHA-256 hex digest of the applied policy. |
| `updated_at` | time | Last policy application time. |

### `artifactclass_pool`

Joins an artifactclass to a pool, with the policy ordering and role.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Placement-row identifier. |
| `artifactclass` | text, indexed | Referenced policy class. |
| `pool_id` | text, FK -> `pool.id` | Eligible pool. |
| `active` | boolean | Whether normal routing selects it. |
| `sort_order` | integer | Deterministic routing order. |
| `role` | text, optional | Policy-defined placement role. |
| `created_at` | time | Creation time. |

### `bundle`

A synthetic archive object that groups multiple logical assets before fan-out.
Bundling makes tape and object-store writes tractable while the locator layer
keeps individual-file restoration possible.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | text, PK | Bundle identifier. |
| `artifactclass` | text, indexed | Bundle policy class. |
| `status` | text | Lifecycle state; starts `open`, then may be flushed, sealed, or held. |
| `total_bytes`, `member_count` | bigint / integer | Current aggregate size and member count. |
| `target_bytes`, `max_age_seconds` | bigint / integer | Policy thresholds captured for this bundle. |
| `ruleset`, `expect` | text, optional | Captured policy descriptors. |
| `archive_id` | text, optional | Backend/archive identifier after write. |
| `scan_summary`, `review_summary` | json, optional | Scanner and operator-review results. |
| `customer_manifest_path` | text, optional | Generated customer manifest receipt. |
| `opened_at`, `flushed_at`, `sealed_at`, `held_at` | time | Lifecycle timestamps; all but `opened_at` are optional. |

### `bundle_member`

A logical asset represented inside one bundle. `(bundle_id, member_path)` is
unique so an archive object cannot contain two entries at the same path.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Member identifier. |
| `bundle_id` | text, FK -> `bundle.id` | Parent bundle. |
| `logical_asset_hash` | hash, FK -> `logical_asset.content_sha256` | Content represented in the bundle. |
| `member_path` | text | Path inside the bundle. |
| `source_path` | text, optional | Original path used to build it. |
| `size_bytes`, `file_sha256` | bigint / hash | Stored member size and content hash. |
| `source_metadata` | json, optional | Source-specific context retained with the member. |
| `added_at` | time | Addition time. |

### `staging_transform`

An auditable pre-fan-out transformation of a bundle member. The two uniqueness
constraints keep step order unambiguous per member and output path.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Transform identifier. |
| `bundle_member_id`, `bundle_id` | FK | Member and bundle that own the transform. |
| `logical_asset_hash`, `artifactclass` | hash / text | Content and policy context. |
| `step_order`, `kind`, `reversible` | integer / text / boolean | Ordered transform description and whether it can be reversed. |
| `original_member_path`, `stored_member_path` | text | Paths before and after transformation. |
| `original_size_bytes`, `stored_size_bytes` | bigint | Sizes before and after transformation. |
| `original_sha256`, `stored_sha256` | hash | Digests before and after transformation. |
| `parameters`, `result` | json | Validated inputs and recorded outcome. |
| `created_at` | time | Record time. |

### `copy`

One stored realization on a backend. A database check enforces exactly one of
`logical_asset_hash` or `bundle_id`; this avoids pretending a bundle object is
an individual-file copy.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Copy identifier. |
| `logical_asset_hash` | hash, optional FK -> `logical_asset.content_sha256` | Asset represented by a direct copy. |
| `bundle_id` | text, optional FK -> `bundle.id` | Bundle represented by a bundle copy. |
| `backend_id`, `pool_id` | FK | Backend and policy pool that hold it. |
| `native_locator` | json | Adapter-specific address for read, verify, and delete. |
| `native_locator_key` | text | Canonical indexed locator; unique with `backend_id`. |
| `storage_metadata` | json | Representation-specific facts, such as RAO metadata. |
| `integrity_hash` | hash, optional | Digest of the stored representation. |
| `health` | enum | `ok`, `suspect`, `corrupt`, or `missing`; defaults to `ok`. |
| `last_verified_at` | time, optional | Last successful verification time. |
| `deleted_at` | time, optional | Tombstone time after physical deletion. |
| `first_observed_at` | time | When the catalogue first learned of it. |
| `source` | enum | Discovery path: `ingest`, `scrub`, or `manual_import`. |

### `asset_locator`

The per-asset pointer into a bundle copy. This is the important distinction
between an object on storage and the file a restore operator asked for.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Locator identifier. |
| `logical_asset_hash` | hash, FK -> `logical_asset.content_sha256` | Restorable asset. |
| `pool_id` | text, FK -> `pool.id` | Policy pool containing the pointer. |
| `copy_id` | integer, optional FK -> `copy.id` | Stored copy that contains the asset. |
| `bundle_id` | text, optional FK -> `bundle.id` | Bundle context for the member. |
| `native_locator` | json | Adapter-native location details. |
| `member_path` | text | Path of the member in the stored object. |
| `representation` | text | Representation needed to open it. |
| `created_at` | time | Creation time. |

### `blob_root`

Root-level metadata for blob-style bundle storage, separate from member-level
`asset_locator` rows.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Root record identifier. |
| `bundle_id`, `copy_id`, `pool_id` | FK | Bundle, its stored copy, and policy pool. |
| `root_path` | text | Root path; unique with `copy_id`. |
| `native_locator` | json | Native root location. |
| `archive_id` | text, optional | Backend archive identifier. |
| `created_at` | time | Creation time. |

## Organization, retention, and review

### `virtual_arrangement`

A permanently mutable, catalogue-only view of archived material. It does not
move stored bytes; the separation means an operator can reorganize a catalogue
without risking a new tape write.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | View identifier. |
| `name` | text, unique | View name. |
| `description` | text, optional | Operator description. |
| `created_by` | text | Creating actor. |
| `created_at`, `updated_at` | time | Creation and last edit. |

### `virtual_arrangement_member`

One asset path in a virtual arrangement. Asset and artifactclass together are
the durable member identity; `path` is allowed to change and is audited.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Member identifier. |
| `va_id` | integer, FK -> `virtual_arrangement.id` | Parent view. |
| `logical_asset_hash` | hash, FK -> `logical_asset.content_sha256` | Referenced asset. |
| `artifactclass` | text | Policy class of that reference. |
| `path` | text | Current virtual path. |
| `excluded` | boolean | Whether the view hides the member. |
| `added_by`, `added_at`, `updated_at` | text / time | Audit actor and times. |

### `virtual_arrangement_history`

Append-only path-change audit for virtual arrangements.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Audit record identifier. |
| `va_id`, `va_member_id` | FK | Parent view and optional current member record. |
| `logical_asset_hash`, `artifactclass` | hash / text | Asset identity and policy context. |
| `old_path`, `new_path` | text | Path before and after the change. |
| `actor`, `changed_at` | text / time | Actor and change time. |

### `asset_tag`

Soft-deletable content-level tag. The partial unique index allows one active
tag of a given name per asset while preserving removal history.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Tag record identifier. |
| `logical_asset_hash` | hash, FK -> `logical_asset.content_sha256` | Tagged asset. |
| `tag` | text | Tag label. |
| `added_by`, `added_at` | text / time | Addition audit. |
| `removed_by`, `removed_at` | text / time, optional | Soft-removal audit. |

### `offsite_confirmation`

One operator attestation for a media identifier. Retention policy can require
this before it releases landing bytes.

| Field | Type / key | Meaning |
|---|---|---|
| `media_id` | text, PK | Confirmed medium identity. |
| `confirmed_at`, `confirmed_by` | time / text | Confirmation audit. |
| `shipment_id` | text, optional | Shipment or transfer reference. |

### `retention_event`

Append-only record of a retention action for an intake.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Event identifier. |
| `intake_id` | text, FK -> `intake.intake_id` | Intake affected. |
| `action` | text | Action such as release or staging deletion. |
| `actor`, `at` | text / time | Who acted and when. |
| `detail` | json, optional | Action evidence/details. |

### `exclusion_record`

Persistent explanation for material excluded from a bundle or policy result.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Record identifier. |
| `bundle_id` | text, optional FK -> `bundle.id` | Related bundle, if one exists. |
| `artifactclass` | text, indexed | Policy class. |
| `logical_asset_hash` | hash, optional FK -> `logical_asset.content_sha256` | Related asset. |
| `path` | text, optional | Related source/member path. |
| `reason` | text | Machine-readable exclusion reason. |
| `count`, `bytes_total` | integer / bigint | Aggregate affected items and bytes. |
| `ruleset_name`, `ruleset_hash` | text, optional | Rule set and its digest. |
| `detail` | json, optional | Structured explanation. |
| `created_at` | time | Record time. |

### `review_decision`

Operator decision for a held bundle. `scope` tells the runtime whether it
applies only to this ingest or becomes a persisted rule.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Decision identifier. |
| `bundle_id` | text, FK -> `bundle.id` | Held bundle reviewed. |
| `action`, `scope` | text | Review action and its application scope. |
| `subtree` | text, optional | Path prefix covered by the decision. |
| `reason`, `reviewer` | text, optional | Human rationale and actor. |
| `persisted_rule` | json, optional | Rule created for future matching input. |
| `decided_at` | time | Decision time. |

## Jobs and reconciliation

### `job`

The live queue/current-state row. Handler-specific state belongs in JSON so
the dispatch registry can evolve without turning every new job kind into a DB
migration.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Job identifier. |
| `kind` | text, indexed | Registered handler dispatch key. |
| `params` | json | Handler input. |
| `required_resources` | json | Counted resource-pool requirements. |
| `prerequisites` | json | Required predecessor job identifiers. |
| `status` | enum | `pending`, `queued`, `running`, `succeeded`, `failed`, or `cancelled`. |
| `step_state` | json | Checkpointed handler progress for retry/resume. |
| `attempts` | integer | Number of dispatch attempts. |
| `not_before`, `priority` | time / integer | Eligibility time and scheduling priority. |
| `dedupe_key` | text, optional | Live-job idempotency key; unique while the job is live. |
| `recon_domain`, `recon_target_key` | text, optional | Reconciler identity for a condition-backed job. |
| `last_error` | text, optional | Current human-readable failure reason. |
| `created_at`, `started_at`, `finished_at` | time | Queue lifecycle timestamps; later two optional. |

### `job_attempt`

Append-only transcript of completed job runs. It retains history after the
live job row is pruned.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Attempt identifier. |
| `job_id` | integer, optional FK -> `job.id` | Original job; null after job-row deletion. |
| `job_kind`, `attempt_number`, `outcome` | text / integer / enum | Handler kind, ordinal, and terminal status. |
| `error` | text, optional | Failure text. |
| `started_at`, `finished_at`, `created_at` | time | Attempt timing and record time. |
| `granted_leases` | json | Resources granted to this attempt. |
| `worker_id`, `code_version` | text, optional | Executing worker and code identity. |
| `detail` | json | Structured attempt result. |

### `reconciliation_condition`

One indexed worklist row per `(domain, target_key)`. It summarizes observed
reality and the latest attempt so reconcilers do not scan the full job history.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Condition identifier. |
| `domain`, `target_key` | text, unique pair | Reconciler namespace and target identity. |
| `observed_state`, `condition` | text | What exists and current disposition. |
| `reason`, `message` | text, optional | Machine-readable reason and human detail. |
| `attempt_count`, `next_eligible_at` | integer / time | Retry count and next allowable attempt. |
| `blocked_tool_name`, `blocked_tool_version` | text, optional | Tool evidence that can reopen a blocked condition after change. |
| `last_attempt_id` | integer, optional FK -> `job_attempt.id` | Latest supporting attempt. |
| `last_attempt_at`, `last_success_at`, `updated_at` | time | Attempt, success, and row-update times. |

## HD cache and restore

The HD cache is expendable operational state. These tables intentionally do
not make a cache disk a durable backend or pool.

### `cache_disk`

| Field | Type / key | Meaning |
|---|---|---|
| `disk_id` | text, PK | Short enrolled-cache-disk identifier. |
| `serial` | text, unique | Physical disk serial. |
| `wwn`, `enclosure`, `slot` | text, optional | Additional physical-location identity. |
| `fs_uuid`, `mount` | text | Expected filesystem UUID and mount path. |
| `state` | enum | `active`, `absent`, `retiring`, or `dead`. |
| `capacity_bytes`, `filled_bytes` | bigint | Usable capacity and tracked cache occupancy. |
| `capacity_state` | enum | `ok` or `over_reserve`. |
| `smart_status` | text, optional | Latest SMART summary. |
| `enrolled_at`, `last_walk_at` | time | Enrollment and most recent inventory-walk times. |

### `cache_entry`

One cached asset. Its primary key permits at most one cache realization per
logical asset across the current cache inventory.

| Field | Type / key | Meaning |
|---|---|---|
| `content_sha256` | hash, PK/FK -> `logical_asset.content_sha256` | Cached asset. |
| `artifactclass` | text | Policy class. |
| `bundle_key`, `group_key` | text, optional | Cache placement grouping keys. |
| `disk_id` | text, FK -> `cache_disk.disk_id` | Physical cache disk. |
| `relpath` | text | Relative path beneath the cache mount. |
| `size_bytes` | bigint | Cached byte count. |
| `state` | enum | `filling`, `present`, or `lost`. |
| `representation` | enum | `raw-bytes` or `rao-aead-v1`. |
| `key_epoch` | text, optional | Encryption-key epoch for encrypted cache content. |
| `stored_digest` | hash, optional | Digest of stored representation. |
| `trusted` | boolean | Whether the cache result is trusted for use. |
| `placed_at`, `last_read_at` | time | Placement and last-read times. |
| `lost_origin_disk_id`, `lost_drill_id`, `lost_at`, `refilled_at` | text / time, optional | Loss-drill provenance and refill audit. |

### `restore_request`

One persisted operator request, independently tracking the cache and tape
branches. The idempotency fields prevent an HTTP retry from creating a second
request with different content.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | text, PK | Request identifier. |
| `identity` | text | Requesting operator/identity. |
| `created_at` | time | Request time. |
| `destination_id` | text | Configured destination identity. |
| `state` | enum | `pending`, `active`, `completed`, or `completed_with_errors`. |
| `admitted_by`, `admitted_at` | text / time, optional | Authorization-admission audit. |
| `admitted_capabilities` | json, optional | Capabilities accepted at admission. |
| `idempotency_key`, `idempotency_body_hash` | text, optional | Request replay key and body digest. |

### `restore_request_item`

One requested asset within a restore request.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Item identifier. |
| `request_id` | text, FK -> `restore_request.id` | Parent request. |
| `content_sha256` | hash, FK -> `logical_asset.content_sha256` | Asset to restore. |
| `artifactclass` | text | Restore-policy class. |
| `state` | enum | `queued`, `waking_disk`, `streaming`, `done`, `fell_back_to_tape`, `denied`, or `failed`. |
| `detail` | text, optional | Operator-visible outcome detail. |
| `denial_kind` | enum, optional | `capability`, `privacy_unmapped`, `suspect`, or `rejected`. |
| `size_bytes`, `bytes_restored` | bigint | Expected and completed byte counts. |
| `source` | enum, optional | `cache` or `tape` source actually used. |
| `admitted_force_suspect`, `admitted_force_rejected` | boolean, optional | Recorded admission overrides. |
| `updated_at` | time | Last progress update. |

## Integrity constraints and migration practice

Besides the primary and foreign keys shown above, the implementation enforces
state-value checks, unique path/identity pairs, the `copy` asset-or-bundle XOR,
and partial unique indexes for active tags and live job deduplication. Those
are part of the application contract, not mere performance hints.

For a production database, apply changes with `alembic upgrade head` under the
deployment procedure. `sutra db init` is a local-development convenience only.
For a machine-readable view of the installed schema, inspect it through the
database's native tooling after migrations; it includes dialect-specific index
and constraint syntax that this explanatory reference intentionally avoids.
