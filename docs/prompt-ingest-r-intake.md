# Codex prompt — Ingest v2 Phase R: intake service (sutradhara engine)

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`.**
> Companion: `~/system/docs/prompt-ingest-r-harness.md` (scenario R + bringup) —
> **identical Shared contract**. Master design: `~/system/docs/design-ingest-flow.md`
> (§S0–S2 + Verification + Data model) is the *why*; this is the work order. Read
> `CLAUDE.md` + `AGENTS.md` first.
>
> This **re-cuts Phase R of the superseded `prompt-ingest-v2-sutradhara.md`** for
> the post-RAO reality. Phase S (bundling/fan-out/policy/restore) is **DONE** — do
> not rebuild it; build R *on top of* the existing archive layer. Phases U and T
> follow in their own prompts.

## What already exists — BUILD ON IT, do not rebuild
The archive/storage spine is live and green (scenarios RAP/RAB/RAS):
- **Catalog** (`src/sutradhara/catalog/models.py`): `LogicalAsset`
  (content-addressed, PK = `content_sha256`), `Backend`, `Pool`,
  `ArtifactClassPool`, `ArtifactClassPolicyRecord`, `Bundle`, `BundleMember`,
  `StagingTransform`, `AssetLocator`, `BlobRoot`, `ExclusionRecord`,
  `ReviewDecision`, `Copy`. **`BackendKind.S3 = "s3"` already exists**
  (`catalog/types.py`) — but there is **no `backend/s3.py` adapter yet**.
- **Policy** (`artifactclass_policy.py`): strict TOML accessor +
  `ArtifactClassPool` many-to-many; `apply_artifactclass_policy_file`.
- **Archive** (`archive_bundle.py` / `archive_fanout.py` / `archive_restore.py`):
  durable bundle accumulator, 3-copy fan-out, per-asset PFR restore.
- **Sealing** (`sealing/rao.py`): `RaoCliSealer` / `RaoCliOpener` over `rem
  archive build/inspect/extract`; representations `rao-plain-v1` / `rao-aead-v1`.
- **Backends** (`backend/`): `remanence.py`, `d2tape.py`, `memory.py`, `factory.py`,
  `port.py` (the `StorageBackend` Protocol R's S3 adapter must satisfy).

## The model refinement R introduces (read this first)
The design says "asset += `virtual_path`, `intake_id`, `(st_dev,st_ino)`". That
**cannot** hang off `LogicalAsset` — it is content-addressed and *shared*: the
same bytes ingested from two sources are ONE `LogicalAsset`, but two distinct
occurrences at two paths in two intakes. Per-occurrence facts therefore live in a
**new `ingest_item` table** (one row per received file), FK → `LogicalAsset` by
hash. `LogicalAsset` stays pure bytes-identity; `ingest_item` carries the
mutable, occurrence-specific facts. T (virtual segregation) later edits
`ingest_item.virtual_path`; tags attach to ingest_items.

## Schema (new)
1. **`intake`** — a batch of data **received somehow** (NOT necessarily a camera
   card — could be a drive, an upload, an rsync from another facility, a handed-over
   folder). `intake_id` (PK, `YYYYMMDD-<operator>-<4hex>` — **source-agnostic, no
   card token**), `operator`, `source_kind` (`card|drive|upload|handoff|download|
   other`), `source_ref` (nullable free text: card serial / drive label / sender /
   URL), `artifactclass`, `label` (nullable — shoot/event/job/donor name),
   `manifest_path` (**nullable** — a checksum manifest only if one arrived with the
   data), `status` (`receiving|verifying|quarantined|registered`), timestamps.
2. **`ingest_item`** — PK surrogate; `intake_id` FK; `logical_asset_hash` FK →
   `logical_asset.content_sha256`; `as_received_path` (relative to the received
   tree root); `virtual_path` (default = `as_received_path`); `st_dev`, `st_ino`,
   `size_bytes`; `artifactclass` (this item's archival class — originals take the
   intake's class, derived artifacts the policy-derived proxy class); per-item
   metadata as needed. Unique `(intake_id, as_received_path)` for idempotent
   re-scan. **No `role` column** — original-vs-derived is answered structurally by
   `asset_derivation` (below), and the archival class is on `artifactclass`; a
   `role` field would just denormalize both and risk drifting out of sync.
3. **`asset_derivation`** — self-referential link between **ingest_items**:
   `(derived_item_id, source_item_id, kind)`, `kind` extensible
   (`mezz|preview|…`). The presence of an **incoming** edge IS the "this is a
   derived artifact" marker (the design's `proxy_of`); an item with no incoming
   edge is an original received file. Edges are between occurrences, not bare hashes.

Use the repo's existing migration/`create_all` convention (match how the archive
tables were added). No change to `LogicalAsset`/`Copy`/`Bundle` schemas.

## The work, in order (each step has tests; commit per logical step)
1. **`sutra intake scan <landing-root>`** (`src/sutradhara/intake.py` + a
   `cli/intake.py` group, runnable by cron/harness):
   - Detect completed intakes: a subdir with the `intake.json` sentinel present
     (written LAST per the Shared contract). Ignore in-flight ones.
   - **Hash every payload file (sha256) and CREATE the authoritative hash record
     for the intake.** The manifest is something WE produce on receipt — the source
     never provides it (a camera card carries no hash list). The catalog rows
     (`LogicalAsset` + `ingest_item`) ARE that authoritative record; optionally also
     emit a portable MHL hash-list file into the intake dir for provenance.
   - **Cross-check against a PRIOR offload manifest if one exists.** When the data
     was brought in by a hashing offload tool (e.g. torana, which hashes the source
     *as it reads the card/drive* and drops a `manifest.*`), re-hash the landed bytes
     and compare to that prior manifest — a match confirms the offload→staging
     *transfer* was clean. Any mismatch / missing / extra file ⇒ `status=quarantined`,
     write `intake.quarantined.json` listing the offenders, register **nothing**,
     alert. (Load-bearing negative path.) Prefer the `ascmhl` package if it
     integrates cleanly, else a minimal compliant reader (document the choice); a
     provided manifest MUST carry sha256 — never silently downgrade hash strength.
   - **No prior manifest ⇒ baseline.** Data simply copied in (drive, upload, a card
     copied by hand) has no earlier hash to check against, so intake's freshly
     computed hashes ARE the baseline. NEVER reject data for lacking a prior manifest
     (archive-everything); record that the transfer-in hop is unverified (integrity
     is cryptographically locked from the seal onward).
   - On success (cross-checked or baseline) ⇒ for each file: upsert a `LogicalAsset`
     by sha256 (dedup — same bytes reuse the row), insert an `ingest_item`
     (virtual_path = as-received, inode + size, `artifactclass` = the intake's class,
     intake metadata; no derivation edge — it's an original). Idempotent re-scan (the
     unique key) adds no duplicates. `status=registered`; write
     `intake.verified.json`. **Only when `source_kind=card`** does this double as the
     "removable media may be released" signal (a custody hook the offload client
     polls); other sources have no card to hold.
2. **Proxy job** (`src/sutradhara/proxy.py`): per **video** master item, ONE
   ffmpeg invocation producing TWO outputs — mezz (1080p h264 ~50 Mbps) + preview
   (~360p h264 ~1.5 Mbps) — to the cache-shard path (config). Register each output
   as a `LogicalAsset` + `ingest_item(role=proxy)` + `asset_derivation` edge to the
   master. **ffmpeg absent or a per-file decode failure ⇒ flag the master
   `no-proxy` and continue — proxies NEVER block intake/registration.** Add an
   ffmpeg-presence check the harness can preflight (don't inline-assert in the job).
3. **S3 backend adapter** (`src/sutradhara/backend/s3.py`, `BackendKind.S3`):
   implement the `StorageBackend` port over boto3. Config
   `{endpoint_url?, bucket, prefix, storage_class?}` — `DEEP_ARCHIVE` in prod,
   omitted/standard under dev MinIO. Multipart upload; object get for verify;
   record `stored_digest`. Register it in `backend/factory.py`.
4. **Cloud-temp blob** (`src/sutradhara/cloud_blob.py`): for a registered intake,
   build **one** RAO object over the whole intake dir — `rem archive build` with a
   whole-tree `blob **` ruleset → `rao-aead-v1` sealed under the epoch key
   (`RaoCliSealer`, key registry unchanged) → S3 put at `intakes/<intake-id>.rao`
   → record **one `Copy` row**, placement `cloud-temp`, keyed to the intake.
   (Reuse the proven RAO archive machinery; do not hand-roll a separate tar.) One
   object per intake — Deep Archive per-object overhead is the reason.
5. **Tests** (DoD gate): MHL parse/verify good + corrupted (byte-flip after MHL ⇒
   quarantine, zero registration); registration idempotency (re-scan = no dup);
   **dedup** (identical bytes in two intakes ⇒ one `LogicalAsset`, two
   `ingest_item`s); derivation edges; S3 adapter against MinIO (skip cleanly if
   absent); cloud-blob round-trip (build → put → get → `rem archive extract` →
   per-member `file_sha256` == registered). `pytest`, `ruff`/format, type-check —
   paste output.

## Shared contract (IDENTICAL in the harness prompt)
- **Landing layout:** `/landing/<intake-id>/` containing `payload/…` (the received
  tree, structure preserved), an OPTIONAL `manifest.*` (a PRIOR hash list — present
  only when a hashing offload tool such as torana already produced one; **absent**
  for data simply copied in), and `intake.json` written LAST as the completion
  sentinel: `{intake_id, operator, source_kind, source_ref?, artifactclass, label?,
  created_at}`. intake-id format: `YYYYMMDD-<operator>-<4 hex>` (source-agnostic).
  Intake creates the authoritative hash record regardless.
- **Asset identity:** `LogicalAsset` is content-addressed and shared; the
  per-occurrence row is `ingest_item` (intake_id, as_received_path, virtual_path,
  inode, artifactclass). Dedup on bytes; never put per-occurrence facts on
  `LogicalAsset`. Original-vs-derived is the `asset_derivation` graph, not a role
  column.
- **Cloud:** backend name `cloud-temp`, kind `s3`; blob key
  `intakes/<intake-id>.rao`; always `rao-aead-v1`; `storage_class=DEEP_ARCHIVE`
  in prod, omitted under dev MinIO. Recorded as a `Copy` row like any other copy.
- **Proxies:** one ffmpeg decode → two outputs (mezz 1080p ~50 Mbps, preview
  ~360p ~1.5 Mbps); `asset_derivation` edges; ffmpeg failure ⇒ `no-proxy`, never
  blocks. (Proxies are bundled, never loose tiny artifacts — the shoeshine lesson;
  that bundling is the archive layer's job, already built.)
- **Verification chain:** intake hashes every file and **creates the authoritative
  manifest** (we make it on receipt — the source never supplies one). If a prior
  offload manifest exists (torana hashed the source on read), the re-hash is
  cross-checked against it → quarantine on mismatch (transfer verification);
  otherwise intake's hashes are the baseline (transfer-in unverified). → seal-time
  RAO per-member `file_sha256` == registered asset hash (archive layer, done). No
  separate re-hash at archive time; send-matching by `(st_dev, st_ino)` + size with
  hash-match fallback (Phase S send-scan, future).
- **Key registry:** unchanged (`$SUTRADHARA_KEY_REGISTRY_DIR`).
- **Artifactclass policy / classes:** the live `artifactclass_policy` accessor +
  `ArtifactClassPool` model; test classes per the harness bringup (`s-masters`,
  `s-proxy`). Governance keys on tags (Phase T), never on classes.

## Constraints
- Existing archive/RAP/RAB/RAS behavior and unit suites stay green; no changes to
  the bundling/fan-out/restore APIs.
- Env discipline: ffmpeg / MinIO / landing root are harness preflight + config,
  never inline asserts in engine code.
- DoD per `AGENTS.md`: tests per step (paste output), commit per step, never leave
  the tree dirty, add the INDEX row. **This doc lives in this repo's `docs/`** and
  is registered in `docs/INDEX.md`.

## Acceptance
- `sutra intake scan` drives a fabricated intake to `registered` with proxies +
  cloud blob, and a corrupted one to `quarantined` with zero registration.
- Engine unit suites green; the harness `scenario-r` (companion prompt) goes green
  on top of this. Report what's covered vs MinIO/ffmpeg-gated.
