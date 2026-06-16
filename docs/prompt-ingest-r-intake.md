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
same bytes ingested from two cards are ONE `LogicalAsset`, but two distinct
occurrences at two paths in two intakes. Per-occurrence facts therefore live in a
**new `ingest_item` table** (one row per received file), FK → `LogicalAsset` by
hash. `LogicalAsset` stays pure bytes-identity; `ingest_item` carries the
mutable, occurrence-specific facts. T (virtual segregation) later edits
`ingest_item.virtual_path`; tags attach to ingest_items.

## Schema (new)
1. **`intake`** — `intake_id` (PK, `YYYYMMDD-<operator>-<card>-<4hex>`),
   `operator`, `card_id`, `artifactclass`, `shoot_label`, `mhl_path`, `status`
   (`receiving|verifying|quarantined|registered`), timestamps.
2. **`ingest_item`** — PK surrogate; `intake_id` FK; `logical_asset_hash` FK →
   `logical_asset.content_sha256`; `as_received_path` (relative to the card root);
   `virtual_path` (default = `as_received_path`); `st_dev`, `st_ino`, `size_bytes`;
   `role` (`master|proxy`); per-item shoot metadata as needed. Unique
   `(intake_id, as_received_path)` for idempotent re-scan.
3. **`asset_derivation`** — `(derived_item_id, source_item_id, kind=mezz|preview)`;
   the proxy→master edge (design `proxy_of`). Keep the edge between **ingest_items**
   (occurrences), not bare hashes.

Use the repo's existing migration/`create_all` convention (match how the archive
tables were added). No change to `LogicalAsset`/`Copy`/`Bundle` schemas.

## The work, in order (each step has tests; commit per logical step)
1. **`sutra intake scan <landing-root>`** (`src/sutradhara/intake.py` + a
   `cli/intake.py` group, runnable by cron/harness):
   - Detect completed intakes: a subdir with the `intake.json` sentinel present
     (written LAST per the Shared contract). Ignore in-flight ones.
   - Parse the **ASC MHL** `manifest.mhl` (sha256 entries REQUIRED). Prefer the
     `ascmhl` package if it integrates cleanly; otherwise a minimal compliant
     reader (document the choice). Reject an MHL lacking sha256 — never silently
     downgrade hash strength.
   - **Re-hash every payload file**, compare to the MHL (hop-1 verification). Any
     mismatch / missing / extra file ⇒ `status=quarantined`, write
     `intake.quarantined.json` listing the offending files, register **nothing**,
     emit an alert record. (This is the load-bearing negative path.)
   - All-match ⇒ for each file: upsert a `LogicalAsset` by sha256 (dedup — same
     bytes seen before reuse the row), insert an `ingest_item` (virtual_path =
     as-received, inode + size recorded, `role=master`, intake metadata). Idempotent
     re-scan (the unique key) must add no duplicates. `status=registered`; write
     `intake.verified.json` (torana polls it to tell the operator "card may be
     released").
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
- **Landing layout:** `/landing/<intake-id>/` containing `card/…` (payload,
  structure preserved), `manifest.mhl` (ASC MHL; sha256 REQUIRED per file), and
  `intake.json` written LAST as the completion sentinel:
  `{intake_id, operator, card_id, artifactclass, shoot_label, created_at}`.
  intake-id format: `YYYYMMDD-<operator>-<card>-<4 hex>`.
- **Asset identity:** `LogicalAsset` is content-addressed and shared; the
  per-occurrence row is `ingest_item` (intake_id, as_received_path, virtual_path,
  inode, role). Dedup on bytes; never put per-occurrence facts on `LogicalAsset`.
- **Cloud:** backend name `cloud-temp`, kind `s3`; blob key
  `intakes/<intake-id>.rao`; always `rao-aead-v1`; `storage_class=DEEP_ARCHIVE`
  in prod, omitted under dev MinIO. Recorded as a `Copy` row like any other copy.
- **Proxies:** one ffmpeg decode → two outputs (mezz 1080p ~50 Mbps, preview
  ~360p ~1.5 Mbps); `asset_derivation` edges; ffmpeg failure ⇒ `no-proxy`, never
  blocks. (Proxies are bundled, never loose tiny artifacts — the shoeshine lesson;
  that bundling is the archive layer's job, already built.)
- **Verification chain:** card-stream sha256 (MHL) → **intake re-hash vs MHL** (R's
  job) → seal-time RAO per-member `file_sha256` == registered asset hash (the
  archive layer's job, done). No separate re-hash pass at archive time;
  send-matching by `(st_dev, st_ino)` + size with hash-match fallback (that match
  is Phase S's send-scan, future).
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
