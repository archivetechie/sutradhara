# Codex prompt — Ingest v2 (sutradhara): bundler, intake, lifecycle, VS

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`.**
> Companions: `~/system/docs/prompt-ingest-v2-harness.md` (scenarios R/S/T/U) and
> `~/system/docs/prompt-torana-offload-client.md` (Mac client). All three share the
> **Shared contract** below verbatim. Master design:
> `~/system/docs/design-ingest-flow.md` (read it first; this prompt is the work
> order, that doc is the why).
>
> **PHASED — implement and commit per phase, in this order. Each phase is
> independently shippable and verified by its harness scenario.**
>
> **RAO REFRESH / PARTIAL SUPERSESSION (2026-06-14).** amber was merged into
> remanence as RAO (migration complete). Sealing/opening is now
> `sutradhara/sealing/rao.py` (`RaoCliSealer`/`RaoCliOpener` over `rem-debug
> archive build/inspect/extract`); representations `rao-plain-v1` / `rao-aead-v1`;
> `AmberCliSealer`/`sealing/amber.py` are deleted. **Phase S's bundler
> (`sutradhara/bundle.py` GNU-tar + `TarInfo.offset_data` PFR + the
> `rem-tar-v1[AOF1[tar]]` nesting) is SUPERSEDED by
> `~/system/docs/design-ingest-v2-rao-archive.md` (approved 2026-06-12):** files
> become first-class RAO entries (native manifest PFR); small/non-compliant
> trees wrap as `.remwrap.tar` entries inside one RAO object via a ruleset
> (`rem archive build --rules`, verbs `granular`/`blob`/`exclude`); ranged
> single-file restore is RAO's native member range-extract (+ an optional
> catalog blob inner-index). Phases R/T/U + the catalog/VS/lifecycle model stand
> on RAO. **Re-cut Phase S against the ruleset design before implementing it** —
> the Phase S section below is the obsolete GNU-tar-bundle plan.

## Phase S — Bundling + data-driven policies (storage-critical, do first)

1. **Artifactclass policy documents** (replace the hardcoded `o_archive_policy()`
   / `n_archive_policy()` pattern): per-class TOML documents with
   `schema_version`, ARCHIVAL sections only —
   `placements = [{placement_id, backend, copy_class, representation,
   offsite_gate (default false)}]`, `proxies = {preview, mezz}` (bools),
   `bundling = {target_gb}`. **Strict validation: unknown keys/sections are
   errors** (a typo'd policy must never silently default to wrong durability).
   ONE accessor module (`sutradhara/policy.py`, e.g. `policy.for_class(...)`) —
   no consumer reads the TOML directly; every future policy dimension lands as a
   named section + accessor change with the feature that enforces it (no rules
   engine). Governance (access/approval) keys on TAGS (phase T), never here.
   Keep the o/n functions as compat shims reading the documents. Source path via
   env `SUTRADHARA_CLASSES` (harness bringup writes it).
2. **Bundler** (`sutradhara/bundle.py`): input = ordered list of assets
   (paths + expected sha256); output = one tar + per-asset PFR records.
   - Python `tarfile`, `format=PAX_FORMAT`, members sorted by relative path,
     **no compression** (media doesn't compress; PFR needs raw offsets).
   - Record per member: `TarInfo.offset_data` + size → catalog PFR rows
     `(asset_id, bundle_id, offset, length)`.
   - Packing policy: 1 artifact → 1 bundle default; **N small same-class
     artifacts → 1 bundle** up to `BUNDLE_TARGET_GB` (config, default 64);
     oversize-artifact *splitting is deferred* — raise a clear error if an
     artifact exceeds the max, with a TODO marker.
   - While taring, verify each asset's streamed sha256 == registered hash
     (the no-extra-read verification — tar reads every byte anyway).
3. **Fan-out integration**: `replicate`/fan_out accepts an *artifact* (asset list)
   — bundles per policy, seals the bundle per placement representation
   (aof-raw / aof-aead exactly as today, the tar is just the payload), records
   copies per bundle + PFR per asset.
4. **D2TapeBackend multi-file artifacts**: extend `write_object_to_pool` to accept
   a directory (the artifact's files, relative paths preserved; hashes.json per
   file — the CLI already supports per-file hashes and returns per-file block
   records = d2-native PFR). copy-3 of a bundle = d2tape's own tar of the same
   files, NOT our tar — same content, native format (per the N design).
5. **Per-asset restore**: resolve asset → bundle copy → `read_range(offset,length)`
   on the working copy (rem read_range is proven) → verify sha256.
6. Unit tests: deterministic offsets, packing boundaries, oversize error,
   per-asset restore round-trip with memory backend, policy registry.

## Phase R — Intake service

1. **Intake model** (`intake` table: intake_id, operator, card_id, artifactclass,
   shoot_label, mhl_path, status receiving|verifying|quarantined|registered).
2. **Watcher/CLI** (superseded for Phase R by P1.1's explicit
   `sutra intake inspect` / `register` / `accept` lifecycle):
   detect completed intakes (sentinel `intake.json` present, per Shared contract)
   → parse **ASC MHL** manifest (sha256 entries REQUIRED; use the `ascmhl` package
   if it fits, else a minimal compliant reader) → re-hash all files → mismatch ⇒
   status=quarantined + report; match ⇒ register each file as an asset
   (content-addressed, `virtual_path` = as-received path, `(st_dev, st_ino)`
   recorded, intake metadata attached) → status=registered. On success, write
   `intake.verified.json` into the intake dir (torana polls for it to show the
   operator "card may be released"); on quarantine, write
   `intake.quarantined.json` with the mismatching files.
3. **Proxy job**: per video asset, ONE ffmpeg invocation, TWO outputs
   (mezz 1080p h264 ~50 Mbps; preview ~360p h264 ~1.5 Mbps) to the cache shard
   path (config). Register both as assets with **derivation edges**
   (`asset_derivation(derived, source, kind=mezz|preview)`).
   ffmpeg absent or per-file failure ⇒ flag `no-proxy`, never blocks archiving.
4. **Cloud temp blob**: tar the whole intake dir → RAO `rao-aead-v1` seal
   (`RaoCliSealer`, epoch key from the registry) → upload to the **S3
   backend** (new `backend/s3.py`, `BackendKind.S3`, boto3; config
   `{endpoint_url?, bucket, prefix, storage_class}` — `DEEP_ARCHIVE` in prod,
   omitted under dev MinIO) → one copy row, placement `cloud-temp`, keyed to the
   intake. Multipart upload; record stored_digest.
5. Tests: MHL parse/verify (good + corrupted), registration idempotency,
   derivation edges, S3 adapter against MinIO (skip if absent), blob round-trip.

## Phase U — Lifecycle + retention

1. Copy/tape lifecycle states: `written → verified → ejected → in_transit →
   offsite_confirmed` (+ events table: who/when).
2. `sutra offsite confirm --tape <barcode>` (and `--shipment <id>` grouping) →
   marks copies on those tapes offsite_confirmed.
3. **Retention engine** (`sutra retention run`): **recipe-relative gate** — an
   asset is *releasable* when every copy_class in its class's recipe is verified
   AND every placement flagged `offsite_gate` is offsite_confirmed. Classes with
   no offsite_gate placements (proxies) release on verification alone — never
   wait for an offsite event that can't come. When ALL assets of an intake are
   releasable:
   delete the cloud blob (S3 delete; record), mark staging originals deletable
   (actual deletion after `STAGING_GRACE_DAYS`, separate job, logged).
   **Nothing deletes before the gate — make the negative path explicit.**
4. Tests: gate truth table (incl. a proxy-only class releasing with ZERO
   offsite events), hook idempotency, no-delete-before-gate.

## Phase T — Virtual segregation

1. `asset.virtual_path` (default = as-received) + `vs_history(asset, old, new,
   actor, at)`.
2. CLI v1: `sutra vs mv <asset-id|virtual-path> <new-virtual-path>` (file or
   subtree), `sutra vs ls <virtual-prefix>`, `sutra vs history <asset>`.
   `rejected` = a reserved virtual subtree (`/.rejected/...`), bits untouched.
3. **Tags** (the governance subjects): `asset_tags` many-to-many + history
   table; `sutra tag add|rm|ls <asset-id|virtual-path>` (subtree tagging tags
   the contained assets); `vs ls --tag <t>` filtering. Governance ENFORCEMENT
   (ACLs, restore approval) is deferred until an identity model exists — model
   the subjects only, and say so in the module docstring. Tag-driven *storage*
   effects are future per-asset policy evaluation reconciled via want/have
   repair; transformative changes (re-encryption) are explicit migrations.
4. Search/browse resolve virtual paths; restore resolves physical PFR locators
   (and orders multi-asset restores by tape/bundle/offset).
5. Tests: move/history, subtree moves, tag add/filter/history, virtual-vs-
   physical resolution, restore ordering.

## Shared contract (IDENTICAL in all three prompts)
- **Landing layout:** `/landing/<intake-id>/` containing `card/…` (payload,
  structure preserved), `manifest.mhl` (ASC MHL; sha256 REQUIRED per file),
  and `intake.json` written LAST as the completion sentinel:
  `{intake_id, operator, card_id, artifactclass, shoot_label, created_at}`.
  intake-id format: `YYYYMMDD-<operator>-<card>-<4 hex>`.
- **Artifactclass policy documents** — archival scope ONLY (placements,
  proxies, bundling), strict versioned validation (unknown keys = error), one
  accessor module. Governance policies (access, approval) key on TAGS, never on
  classes. Test classes: `s-masters` → s-copy-1 (rem, rao-plain-v1, copy-1) +
  s-copy-2 (rem, rao-aead-v1, copy-2, offsite_gate=true) + s-copy-3
  (d2-tape, d2tar-raw, copy-3); `s-proxy` → s-proxy pool (rem, rao-plain-v1,
  copy-1; no offsite_gate). Production classes are config, not code.
- **Bundle:** PAX tar, path-sorted, uncompressed; PFR = `(asset, bundle, offset,
  length)` from `TarInfo.offset_data`; bundle copies recorded like any object;
  copy-3 = d2tape-native tar of the same files (its per-file blocks are its PFR).
- **Cloud:** backend name `cloud-temp`, kind s3; blob key
  `intakes/<intake-id>.rao`; encrypted (rao-aead-v1) always; storage_class
  DEEP_ARCHIVE in prod, plain under dev MinIO.
- **Lifecycle gate (recipe-relative):** releasable(asset) = every copy_class in
  its class's recipe verified AND every `offsite_gate` placement
  `offsite_confirmed` (via `sutra offsite confirm`). Proxy-only assets (no
  offsite_gate) release without any offsite event. Cloud blob expires when ALL
  intake assets releasable; staging deletes after grace. Never earlier.
- **Tags (governance subjects):** `asset_tags` many-to-many + history, assignable
  anytime (`sutra tag add|rm|ls`, `vs ls --tag`). ACL/approval ENFORCEMENT is
  deferred until an identity model exists — model the subjects, not the authz.
- **Proxies:** one ffmpeg decode → two outputs (mezz 1080p ~50 Mbps, preview
  ~360p ~1.5 Mbps); derivation edges `proxy_of`; proxies archived bundled
  (never as loose tiny artifacts — legacy shoeshine lesson).
- **Key registry:** unchanged (`$SUTRADHARA_KEY_REGISTRY_DIR`).
- **Verification chain:** card-stream sha256 (MHL) → intake re-hash → seal-time
  RAO per-member `file_sha256` == registered (the asset hash — NOT RAO's
  tar-body `plaintext_digest`); send-matching by `(st_dev, st_ino)`+size with
  hash-match fallback — no separate re-hash pass.

## Constraints
- O/N/Q/J behavior and tests unchanged; policy registry must reproduce their
  placements exactly (compat shims).
- DoD per the harness `AGENTS.md`: run tests per phase, paste output, commit per
  phase, never leave the tree dirty, update your docs/INDEX entry.

## Acceptance (per phase = its harness scenario green)
S → scenario-s; R → scenario-r; U → scenario-u; T → scenario-t. Unit suites green
throughout.
