# Schema conventions and persistent-field ownership

Wave 1 makes the cross-table conventions executable in
`src/sutradhara/schema_conventions.py`. The reflection test loads the complete
SQLAlchemy metadata and requires an explicit manifest entry for every foreign
key, closed vocabulary, registry, and current-state `updated_at` column; it
does not infer meaning from table or column names.

## P1 manifest conventions

- Each foreign key has one declared role: `ownership`, `reference`, or
  `audit-subject`; the role determines the allowed `ON DELETE` behavior, and
  audit/history/provenance subjects always use `RESTRICT`.
- Each closed vocabulary is declared once in `VOCABULARIES`; table `CHECK`
  constraints are rendered from that declaration through
  `vocabulary_check_sql`.
- Registry-backed values are represented by a real table and foreign keys;
  `artifactclass` rows are created only while applying policy administration.
- `SEMANTIC_COLUMN_GROUPS`, `IDENTIFIER_CONVENTIONS`,
  `JSON_COLUMN_CONTRACTS`, and `TIMESTAMP_CONVENTIONS` record distinctions
  which identical SQL types cannot express.
- Every current-state column named `updated_at` is listed in
  `UPDATED_AT_COLUMNS` and has ORM `onupdate` behavior.

The gate is intentionally exhaustive: a new foreign key, vocabulary `CHECK`,
registry, or `updated_at` column fails reflection until its convention is
declared, while a stale manifest entry fails because its schema object no
longer exists.

## P5 writer/reader declarations

`copy.media_id` and `copy.media_family` mean the canonical physical-medium identity and implementation family, are written only by `_copy_media_id` at copy registration and by the Wave 1 backfill, and are read by the retention gate, offsite matching, component tagging, durability and replication queries, the library projection, and the schema-hardening query battery.

`reconciliation_condition.observed_at` means the latest observation look and is written on every look by `record_observation`; `condition_changed_at` means the latest condition-value transition and is written only by `_set_condition` when the value changes; `updated_at` means the latest projection mutation and is maintained by ORM `onupdate`; the blocked-condition CLI orders and reports “since” from `condition_changed_at`, while the behavioral query gate reads all three clocks.

`reconciliation_condition.reopened_by` and `reopened_at` mean the immutable actor and time of the latest explicit reopen event, are written together once by `reopen_condition` for that event, and are read by operational forensics and the history behavioral gate without being rewritten by later observations or duplicated into the mutable message.

`ingest_item.source_path` and `pfr_sidecar_path` mean the typed registered staging and PFR-sidecar locations, are written by registration/fact writers, and are read by staging-purge root derivation, arrangement source-map validation, the cloud/transcode/PFR job handlers, PFR lookup and CLI projection, and the derivation reconciler; migration backfills them from legacy metadata and then removes the `source_path`, `pfr_sidecar_path`, and `sha256` JSON mirrors.

`artifactclass.name` means a policy-administered archive class, is written only by artifactclass policy administration, and is read through foreign keys from every artifactclass-bearing table so an unknown class is rejected at write time.

## Identity and lifecycle notes

`asset_locator` has one complete relational chain: its required
`(copy_id, pool_id, bundle_id)` must identify the same bundle copy, and its
required `(bundle_id, logical_asset_hash, member_path)` must identify the
corresponding bundle member. `member_path` is therefore stored only in the
typed column and not duplicated in `native_locator`.

Unregistered intakes use `retention_state = not_applicable`; registered
intakes use an applicable retention state. Arrangement lifecycle values are
only `draft`, `submitted`, and `abandoned`, with explicit attributed abandon
fields. `blob_root` is absent until a real consumer establishes its contract,
and physical-copy deletion retains the established `copy.deleted_at` name.
