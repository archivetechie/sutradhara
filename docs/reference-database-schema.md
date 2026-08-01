# Database schema reference

This is the reference for Sutradhara's application database. Think of the
database as the archive's working card catalogue: it records what the system
knows, what it intends to do, and the evidence behind those decisions. It does
not replace stored bytes. For self-describing backends, the catalogue can be
rebuilt by enumerating the backend.

The live source of truth is the SQLAlchemy model set in
`src/sutradhara/catalog/models.py`, `jobs/models.py`, `api/store.py`,
`api/live_capabilities.py`, `grpc/store.py`, and `hdcache/models.py`, applied
in order by `alembic/`. This page was checked against the full model set
loaded by `catalog.session.create_all` and the Alembic head. Do not hand-edit a
production database: use the CLI/API and Alembic migrations. When this
reference and the models disagree, the models and migrations win; please fix
this page.

<!-- code-anchor: src/sutradhara/catalog/models.py @ 5688438 -->
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

<!-- code-anchor: src/sutradhara/catalog/models.py @ 5688438 -->
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

<!-- code-anchor: src/sutradhara/catalog/models.py @ 5688438 -->
## Relationship map

`logical_asset` is the content identity. `ingest_item` records each appearance
of that content in an `intake`. An `arrangement` turns intake items into a
mutable archive namespace; a `submission` freezes that namespace. `bundle` is
the other archive unit: a synthetic object that holds `bundle_member` rows.
Backends contain policy-facing `pool` rows; `copy` records what was written and
`asset_locator` connects bundle members to those writes.

The job, reconciliation, cache, and restore tables share the same database so
operators can explain a decision without joining separate operational stores.

<!-- code-anchor: src/sutradhara/catalog/models.py @ 5688438 -->
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
| `retention_state` | enum | Landing-byte lifecycle: `held`, `released`, `tombstoned`, `abandoned`, or `purged`; defaults to `held`. |
| `released_at` | time, optional | When policy made source staging releasable. |
| `release_policy_fingerprint` | text, optional | Versioned digest of the gate-relevant policy snapshot frozen at release. |
| `staging_tombstoned_at`, `staging_tombstone_path` | time/text, optional pair | Atomic-rename recovery marker committed before tombstone garbage collection. |
| `staging_deleted_at` | time, optional | When temporary landing bytes were actually deleted. |

### `ingest_item`

One occurrence of a logical asset inside an intake. This is why repeated cards
can preserve their own provenance even when their bytes deduplicate.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Occurrence identifier. |
| `intake_id` | text, FK -> `intake.intake_id`, indexed | Intake that supplied the occurrence. |
| `logical_asset_hash` | hash, FK -> `logical_asset.content_sha256`, indexed | Content represented by this occurrence. |
| `as_received_path` | text | Path exactly as received; unique within an intake. |
| `virtual_path` | text | Normalized working path used before archive arrangement. |
| `st_dev`, `st_ino` | bigint, optional | Source filesystem device/inode evidence when available. |
| `size_bytes` | bigint | Observed item size. |
| `artifactclass` | text, indexed | Item's archive-policy class. |
| `disposition` | enum | Immutable registration verdict: `new`, `known_durable`, `known_under_durable`, `reverified`, or `legacy_unknown`. |
| `disposition_evaluated_at` | time, optional | When server-sha novelty and durability were evaluated; null only for legacy backfill. |
| `disposition_policy_generation` | text, optional | Policy SHA/generation used for the evaluated-at verdict. |
| `disposition_evidence` | json, optional | Immutable server hash and policy-qualified durability facts supporting the verdict. |
| `prior_intake_id` | text, optional FK -> `intake.intake_id`, indexed | Most recent prior verified occurrence linked by the server hash. |
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

<!-- code-anchor: src/sutradhara/api/store.py src/sutradhara/grpc/store.py @ 5688438 -->
## Receive API and device relay

These tables make duplicate-receive decisions, live source ownership, and
workstation enrollment durable. They deliberately sit beside the catalogue:
an HTTP retry, a helper reconnect, or a server restart must not turn one card
receive into a second uncontrolled copy.

### `idempotency_record`

One durable API request/receive-intent record, scoped by operator, endpoint,
and client key. The unique scope protects normal retries; the card index makes
the explicit duplicate-warning workflow fast enough to use at receive time.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Record identifier. |
| `operator_username`, `endpoint`, `idempotency_key` | text, unique triple | Actor, API endpoint, and client retry key. |
| `request_hash` | text | SHA-256 hex digest of the canonical request body; detects key reuse with different input. |
| `status` | enum | Durable state: `in_progress`, `completed`, `warned`, `authorized`, `started`, `committed`, `aborted`, `quarantined`, or `failed`. |
| `intake_id` | text, optional | Intake created or associated with the request. |
| `response_json` | json, optional | Stored response for a safe replay. |
| `device_id`, `card_identity`, `card_label` | text, optional | Device/card context used for the duplicate-receive decision. |
| `duplicate_warning` | json, optional | Stored `nothing_new` estimate warning shown before an override (legacy column name retained). |
| `duplicate_acknowledged` | boolean | Whether the operator explicitly accepted that content warning; defaults to false. |
| `lease_source_id` | text, optional | Source claim held while the receive is active. |
| `warned_at`, `authorized_at`, `started_at`, `terminal_at` | time, optional | Receive-intent state-transition audit. |
| `created_at`, `updated_at`, `last_heartbeat` | time | Record lifecycle and liveness timestamps. |

### `source_claim`

One durable receive lease per source identity. It stops two live requests from
reading the same card/source simultaneously; an expired or terminal request
can be reconciled rather than guessed away.

| Field | Type / key | Meaning |
|---|---|---|
| `source_id` | text, PK | Claimed source/card identity. |
| `operator_username` | text | Current claim owner. |
| `idempotency_key` | text | Intent that holds the claim. |
| `intake_id` | text, optional | Intake created once receiving begins. |
| `created_at`, `updated_at`, `last_heartbeat` | time | Claim audit and liveness times. |

### `grpc_intake`

Durable ownership and state for a streamed workstation intake. The filesystem
marker is only a watcher/sweep hint; this row is the authority through commit.

| Field | Type / key | Meaning |
|---|---|---|
| `intake_id` | text, PK | Streaming-intake identifier. |
| `operator`, `device_id` | text | Authorized operator and enrolled helper. |
| `state` | enum | `streaming`, `committing`, `committed`, or `aborted`. |
| `manifest_digest` | text, optional | SHA-256 hex digest after commit. |
| `card_id` | text, optional, indexed | Card identity for history/projection. |
| `idempotency_key`, `source_plan_digest` | text | Receive intent and the selected-source plan digest. |
| `artifactclass`, `source_kind` | text | Archive policy class and declared source category. |
| `source_ref`, `label` | text, optional | Operator-visible selected-folder/source context. |
| `landing_root` | text | Server-configured landing destination. |
| `created_at`, `updated_at` | time | Lifecycle timestamps. |

### `grpc_logical_device`

The stable device identity that survives a certificate rotation. Enrollment
rows, enrollment tokens, and restore-destination grants all reference this
identity rather than a fingerprint, so a device keeps its authorized scopes
across a re-issued certificate.

| Field | Type / key | Meaning |
|---|---|---|
| `device_id` | text, PK | Logical device identity. |
| `scopes` | json | Authorized capability set: `["ingest"]`, `["restore"]`, or `["ingest", "restore"]`; defaults to `["ingest"]`. |
| `created_at`, `updated_at` | time | Creation and last scope-update time. |

### `grpc_device_enrollment`

Maps an mTLS client-certificate fingerprint to its authorized device and
operator. Keeping enrollment server-side means a browser cannot nominate a
device merely by supplying its identifier.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Enrollment-row identifier. |
| `device_id` | text, FK -> `grpc_logical_device.device_id`, unique with `cert_fingerprint` | Enrolled logical device. |
| `cert_fingerprint` | text | Certificate fingerprint, unique with `device_id`. |
| `operator` | text | Owning operator. |
| `revoked` | boolean | Whether the certificate may still authenticate; defaults to false. |
| `created_at`, `revoked_at` | time | Enrollment and optional revocation time. |

### `grpc_enroll_token`

One-use, short-lived authority for a device CSR. The token itself is the
primary key because it is the secret bearer value and may only be consumed once.

| Field | Type / key | Meaning |
|---|---|---|
| `token` | text, PK | Enrollment bearer token. |
| `created_at`, `expires_at`, `used_at` | time | Creation, expiry, and optional consumption time. |
| `operator`, `device_id` | text | Intended owner and device binding. |
| `scopes` | json | Capability set granted on consumption; same closed vocabulary as `grpc_logical_device.scopes`, defaults to `["ingest"]`. |
| `rotation_authority` | text, optional | `self` or `admin` authority for a re-enrollment. |
| `rotation_fingerprint` | text, optional | Existing certificate fingerprint required for a self-rotation. |

### `grpc_device_destination_grant`

One opaque restore-destination binding authorized for a logical device, used
by the agent restore-delivery path.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Grant identifier. |
| `device_id` | text, FK -> `grpc_logical_device.device_id`, unique with `destination_id` | Authorized device. |
| `destination_id` | text, indexed | Opaque destination identity, unique with `device_id`. |
| `dest_root` | text | Destination root path for delivered restores. |
| `created_at` | time | Grant time. |

<!-- code-anchor: src/sutradhara/catalog/models.py @ 5688438 -->
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
arrangement rather than a separate state field. A partial unique index on
`(arrangement_id, member_path)` where `excluded` is false stops two active
members from claiming the same path; an excluded member does not block reuse
of its old path.

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

<!-- code-anchor: src/sutradhara/catalog/models.py @ 5688438 -->
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

Joins an artifactclass to a pool, with the policy ordering and role. The
`(artifactclass, pool_id)` pair is unique, so a class cannot list the same
pool twice.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Placement-row identifier. |
| `artifactclass` | text, indexed, unique with `pool_id` | Referenced policy class. |
| `pool_id` | text, FK -> `pool.id`, unique with `artifactclass` | Eligible pool. |
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
| `backend_id` | integer, FK -> `backend.id` | Backend that holds it. |
| `pool_id` | text, optional FK -> `pool.id` | Policy pool when the copy is policy-routed; null for legacy or discovered copies without one. |
| `native_locator` | json | Adapter-specific address for read, verify, and delete. |
| `native_locator_key` | text | Canonical indexed locator; unique with `backend_id`. |
| `storage_metadata` | json | Representation-specific facts, such as RAO metadata. |
| `integrity_hash` | hash | Required digest of the stored representation. |
| `integrity_hash_provenance` | enum | `locally_computed` or `backend_discovered`. |
| `health` | enum | `ok`, `suspect`, `corrupt`, or `missing`; defaults to `ok`. |
| `health_changed_at` | time | Transition clock maintained by a SQLite trigger for ORM and raw-SQL updates. |
| `last_checked_at` | time, optional | Last backend check, measured or trust-based. |
| `last_measured_digest` / `last_measured_at` | hash/time, optional pair | Last byte read-back measurement; both fields are null or both are set. |
| `deleted_at` | time, optional | Tombstone time after physical deletion. |
| `first_observed_at` | time | When the catalogue first learned of it. |
| `source` | enum | Discovery path: `ingest`, `scrub`, or `manual_import`. |

### `asset_locator`

The per-asset pointer into a bundle copy. This is the important distinction
between an object on storage and the file a restore operator asked for. The
triple `(copy_id, logical_asset_hash, member_path)` is unique, so the same
asset cannot be pointed at the same member path in the same copy twice.

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

<!-- code-anchor: src/sutradhara/catalog/models.py alembic/versions/e1f2a3b4c5d6_add_deletion_evidence_gate.py alembic/versions/f2a3b4c5d6e7_add_retention_journal.py @ 5688438 -->
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
the durable member identity; `path` is allowed to change and is audited. A
partial unique index on `(va_id, path)` where `excluded` is false stops two
active members from sharing a path within the same view, the same pattern
used for `arrangement_member`.

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
| `revoked_at`, `revoked_by` | time / text, optional pair | Attributed revocation; active gates require both to be null. |

### `retention_event`

Append-only record of an intake, media, or batch retention decision. A
partial unique index on `(action, operation_id)` covers the six idempotency-
sensitive actions (`release_attempted`, `cloud_blob_deleted`, `released`,
`purge_attempted`, `staging_tombstoned`, `staging_deleted`) so an operation
retry cannot double-record the same outcome; other actions, including
`correction_recorded`, are unconstrained by this index.

| Field | Type / key | Meaning |
|---|---|---|
| `event_id` | integer, PK | Event identifier. |
| `intake_id` | text, optional FK -> `intake.intake_id`, `ON DELETE RESTRICT` | Relational intake target for intake-subject events. |
| `subject_type`, `subject_id` | text | Explicit `intake`, `media`, `batch`, or correction-only `receipt` subject identity. |
| `action` | text | Closed action vocabulary for attempts, outcomes, holds, tripwires, and corrections. |
| `operation_id` | required text | Correlation key shared by an attempt and its outcomes. |
| `actor`, `at` | text / time | Who acted and when. |
| `detail` | json, optional | Action evidence/details. |
| `supersedes_source`, `supersedes_event_id` | optional source enum / integer pair | Immutable receipt target for `correction_recorded`; both are null for ordinary events. |

The retention recorder strictly validates the payload keys for each action
before appending an event.

### `verify_receipt`

Append-only audit receipt written in the same transaction as a copy's current
measurement projection or measurement invalidation. The triple `(source,
execution_id, copy_id)` is unique, so retrying a fan-out, verify-job, restore,
or scrub execution cannot record the same copy's outcome twice.

| Field | Type / key | Meaning |
|---|---|---|
| `event_id` | integer, PK | Receipt identifier. |
| `copy_id`, `backend_id` | integer, restricted FKs | Copy and backend measured. |
| `expected_digest`, `measured_digest` | hash / optional hash | Catalog expectation and actual read-back result; null measured digest means invalidation. |
| `backend_ok`, `failure_kind`, `failure_detail` | boolean / optional text | Backend verdict and separate failure facts. |
| `source`, `execution_id` | enum / text | `fanout`, `verify-job`, `restore`, or scrub invalidation plus its retry-deduplication identity. |
| `producer_process`, `actor`, `at` | text / optional text / time | Producer and attribution metadata. |

### `retention_journal_checkpoint`

Singleton optimization mirror of the latest authoritative published journal
footer. The exporter always resumes from files, so this row can be stale after a
crash without causing duplicate or omitted receipts.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK constrained to `1` | Singleton identity. |
| `envelope_id`, `hash_algorithm_id` | text | Versioned encoding and hash identifiers. |
| `global_sequence`, `head_hash` | non-negative integer / hash | Latest published sequence and chain head. |
| `verify_receipt_cursor`, `retention_event_cursor` | non-negative integers | Inclusive source-table cursors. |
| `published_filename`, `published_at` | text / time | Authoritative segment/footer mirrored by this checkpoint. |

Downgrading through the deletion-evidence revision uses an export-then-transform
policy. Events and receipts that the legacy schema cannot represent are written
to a JSON sidecar beside the SQLite catalog, and Alembic prints that path.
`staging_tombstoned` maps to the legacy `staging_deleted` action; other new
actions or non-intake subjects are removed after export. Intake states map
`abandoned` to `held` and `tombstoned` to `purged` without discarding their
existing timestamps.

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

<!-- code-anchor: src/sutradhara/jobs/models.py @ 5688438 -->
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
| `detail` | json | Structured attempt result, including context-accumulated `components`, optional `component_parents`, and raw `observations`. |

### `reconciliation_condition`

One indexed worklist row per `(domain, target_key)`. It summarizes observed
reality and the latest attempt so reconcilers do not scan the full job history.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Condition identifier. |
| `domain`, `target_key` | text, unique pair | Reconciler namespace and target identity. |
| `observed_state`, `condition` | text | What exists and current disposition. |
| `reason`, `message` | text, optional | Machine-readable reason and human detail. |
| `attempt_count` | integer | Retry count. |
| `next_eligible_at` | time, optional | Next allowable attempt. |
| `blocked_tool_name`, `blocked_tool_version` | text, optional | Tool evidence that can reopen a blocked condition after change. |
| `last_attempt_id` | integer, optional FK -> `job_attempt.id` | Latest supporting attempt. |
| `last_attempt_at`, `last_success_at` | time, optional | Latest attempt and success times. |
| `updated_at` | time | Row-update time. |

### `condition_component`

An indexed, attempt-independent snapshot of exact component strings captured
when a reconciliation condition transitions to `blocked`. Attempt pruning can
null `reconciliation_condition.last_attempt_id` without removing this lookup.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Snapshot row identifier. |
| `condition_id` | integer, FK -> `reconciliation_condition.id` | Parked condition; cascades on condition deletion. |
| `component` | text, indexed | Exact component string used by `record-fix`; unique per condition. |

<!-- code-anchor: src/sutradhara/hdcache/models.py @ 5688438 -->
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
| `enrolled_at` | time | Enrollment time. |
| `last_walk_at` | time, optional | Most recent inventory-walk time. |

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
| `placed_at` | time | Placement time. |
| `last_read_at` | time, optional | Last-read time. |
| `lost_origin_disk_id`, `lost_drill_id`, `lost_at`, `refilled_at` | text / time, optional | Loss-drill provenance and refill audit. |

### `restore_request`

One persisted operator request, independently tracking the cache and tape
branches. The idempotency fields prevent an HTTP retry from creating a second
request with different content. `delivery_mode` and `receiver_device_id` are
paired by a check constraint: a `server_local` request must leave
`receiver_device_id` null, and an `agent` request must set it.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | text, PK | Request identifier. |
| `identity` | text | Requesting operator/identity. |
| `created_at` | time | Request time. |
| `destination_id` | text | Configured destination identity. |
| `delivery_mode` | enum | `server_local` or `agent`; defaults to `server_local`. |
| `receiver_device_id` | text, optional FK -> `grpc_logical_device.device_id` | Delivery-agent device; required when `delivery_mode` is `agent`, forbidden otherwise. |
| `state` | enum | `pending`, `active`, `completed`, or `completed_with_errors`. |
| `admitted_by`, `admitted_at` | text / time, optional | Authorization-admission audit. |
| `admitted_capabilities` | json, optional | Capabilities accepted at admission. |
| `idempotency_key` | text, optional, unique | Request replay key. |
| `idempotency_body_hash` | text, optional | Body digest paired with the replay key. |

### `restore_request_item`

One requested asset within a restore request.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Item identifier. |
| `request_id` | text, FK -> `restore_request.id` | Parent request. |
| `content_sha256` | hash, FK -> `logical_asset.content_sha256` | Asset to restore. |
| `artifactclass` | text | Restore-policy class. |
| `final_rel_path` | text, optional | Destination-relative path once delivery completes. |
| `state` | enum | `queued`, `waking_disk`, `streaming`, `sent`, `done`, `fell_back_to_tape`, `denied`, or `failed`. |
| `detail` | text, optional | Operator-visible outcome detail. |
| `denial_kind` | enum, optional | `capability`, `privacy_unmapped`, `suspect`, or `rejected`. |
| `size_bytes` | bigint, optional | Expected byte count. |
| `bytes_restored` | bigint | Completed byte count. |
| `source` | enum, optional | `cache` or `tape` source actually used. |
| `admitted_force_suspect`, `admitted_force_rejected` | boolean, optional | Recorded admission overrides. |
| `updated_at` | time | Last progress update. |

### `restore_item_checkpoint`

Durable per-item staged/revealed progress for agent restore delivery, keyed
1:1 by `restore_request_item`. `revealed` can only be true once at least one
chunk has been committed.

| Field | Type / key | Meaning |
|---|---|---|
| `restore_request_item_id` | integer, PK, FK -> `restore_request_item.id` | Parent restore item. |
| `manifest_sha256` | hash | Digest of the delivery manifest this checkpoint tracks. |
| `committed_index` | integer | Last committed chunk index; defaults to 0. |
| `revealed` | boolean | Whether the destination path has been exposed to the receiver; defaults to false. |
| `updated_at` | time | Last checkpoint update. |

### `restore_open_session`

An exclusive, expiring generation lease for opening one agent restore item, so
a stale or duplicate agent session cannot race a live one.

| Field | Type / key | Meaning |
|---|---|---|
| `restore_request_item_id` | integer, PK, FK -> `restore_request_item.id` | Restore item the session opens. |
| `receiver_device_id` | text, FK -> `grpc_logical_device.device_id` | Device holding the lease. |
| `manifest_sha256` | hash | Digest of the delivery manifest for this open. |
| `generation` | integer | Monotonically increasing lease generation, starting at 1. |
| `expires_at` | time | Lease expiry. |
| `created_at`, `updated_at` | time | Lease creation and last renewal time. |

<!-- code-anchor: src/sutradhara/api/live_capabilities.py @ 5688438 -->
## Operator capability cache

Restore admission trusts an HTTP session's capability headers as a snapshot,
but an agent restore open can happen well after admission. These two tables
give the agent-open path a separately revocable, authoritative source, so a
capability revoked after admission cannot still be exercised through an open
agent session.

### `operator_capability_sync`

The freshness boundary for one operator's synchronized capability snapshot.

| Field | Type / key | Meaning |
|---|---|---|
| `operator` | text, PK | Synchronized operator. |
| `synchronized_at` | time | When this snapshot was last refreshed. |
| `valid_until` | time | When this snapshot must be refreshed again to stay authoritative. |

### `operator_live_capability`

One synchronized, currently effective capability grant. `(operator,
capability)` is unique.

| Field | Type / key | Meaning |
|---|---|---|
| `id` | integer, PK | Grant identifier. |
| `operator` | text, FK -> `operator_capability_sync.operator`, unique with `capability` | Granted operator. |
| `capability` | enum, unique with `operator` | One of `can_view`, `can_receive`, `can_restore`, `can_logs`, `can_admin`, `can_restore_p2`, `can_restore_p3`. |

<!-- code-anchor: src/sutradhara/catalog/models.py alembic @ 5688438 -->
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
