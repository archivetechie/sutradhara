# Codex prompt — RAO ingest archive (sutradhara): bundling, placement, catalog, restore, receipt

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`**
> — the orchestration *policy* + catalog. One of a trio; companions:
> `~/remanence/docs/prompt-rao-archive-rem.md` (the `rem archive --rules` engine)
> and `~/system/docs/prompt-rao-archive-harness.md` (scenario). The **Shared
> contract** below is identical in all three. **Source of truth:**
> `~/system/docs/design-ingest-v2-rao-archive.md` (Part B is yours). This is the
> **Phase-S re-cut** — it supersedes the Phase-S sections of
> `prompt-ingest-v2-sutradhara.md` (R/T/U intake/VS/lifecycle stay there). Read
> `CLAUDE.md` + `AGENTS.md` first.

## Scope (design §B), phased

### P1 — Placement: the flat pool model (design §B6)
Replace the scenario-era placement (exact `content_class` match +
`PlacementTagPin` + `(class,copy)→representation` policy) with:
- **`pool`** — self-describing `{ id, backend, representation, location,
  offsite_gate, tier }`; `representation` **immutable once non-empty** (config
  check); the pool **owns** format/encryption.
- **`artifactclass_pool`** — `artifactclass ↔ pool` **many-to-many**, `active`
  flag. A class's copies are its active memberships; **many classes share a pool**.
- Drop the pin; its drift-detection becomes (1) pool-immutability, (2) scrub
  asserts `copy.representation == pool.representation`.
- **Compat:** O/N/Q/J reproduce their exact placements through the pool model
  (same objects, byte-identical); their tests stay green.

### P2 — Artifactclass policy documents
Per-class TOML, strict validation (unknown keys/sections = error), one accessor:
`ruleset = "<name>"`, `[[placements]]` (→ pools), `[bundling]` (`target_gb`,
`max_age`), restore-preference (ordered pools/copy-roles), `expect`
(`compliant`|`messy`). The named `ruleset` is passed to `rem archive build
--rules`; sutradhara never reimplements the ruleset engine.

### P3 — Bundling + fan-out (design §B1–B4)
- **Open accumulator**, per artifactclass: durable (survives restart; answers
  pending/age/force-flush). Flush on `size ≥ target_gb` **or** `age ≥ max_age`
  (age from the oldest pending artifact). Single-class. Oversize → own bundle;
  > tape capacity → clear error + `# TODO: oversize split`.
- **`fan_out_artifact`**: on flush, materialise the bundle into **each pool's**
  copy from `pool.representation` — `rem archive build [--rules]` for
  `rao-plain-v1`/`rao-aead-v1`, d2 tar for `d2tar-raw` (optional cloud PUT for
  `s3`/`gcs` pools, P6). Record `bundle` (synthetic id) + `bundle_member` +
  `asset_locator` (per copy) + `blob_root` (coarse, per blob entry) +
  `exclusion_record`. Verify each member on each copy before deleting scratch.

### P4 — Conformance gate + review (design §A4)
- Run rem `archive build --scan-only --rules <file>` to get the clustered
  classification. Apply **`expect`**: `messy` → auto-pass (record); `compliant`
  → on any wrap-fallback/unexpected-exclusion **halt the bundle + alert** (the
  intake-quarantine pattern). The gate auto-passes only a scan matching the
  expectation; never silently auto-remediate a surprise.
- **`sutra review <bundle-id>`** (held-bundle review): present context + the
  **clustered** deviation summary (prefix × reason + samples + counts — never an
  enumeration) + the proposed default. Actions at **subtree** granularity:
  `wrap` / `blob` / `exclude` (scope: just-this-ingest **or** persist the rule
  into the class ruleset, version-bumped), `fix-source-&-rescan`, `abort`. Record
  every decision (who/when/what/why). A held bundle **never auto-proceeds**.

### P5 — Restore + customer manifest (design §A5/§B5)
- **Restore engine:** per-artifactclass **ordered pool preference**; walk it,
  take the first available+healthy copy, dispatch the per-copy ranged extract
  (native RAO for rem copies, d2 block locator for d2), **verify restored bytes
  against the asset plaintext sha256** (copy-independent). Single-file-from-blob:
  `blob_root` → object/copy → read on-object `.idx` → member range → extract.
- **Customer-manifest receipt:** from rem `--manifest-out`, add an **archive ID**,
  sign/timestamp, and route to a configured **deliverables destination**. It is
  the customer's table-of-contents + proof-of-archival; re-issuable from the
  on-object `.idx` if lost.

### P6 — Cloud copy backend (optional, design §B6 "Cloud as a pool")
A pluggable `s3`/`gcs`/s3-compatible backend (authenticated ranged GET/PUT)
beside `rem`/`d2tape`. A cloud copy is the **same `rao-aead-v1` object** on a pool
with `backend=gcs|s3`, `tier=archive|instant|deep`. Byte-range restore reuses the
existing `asset_locator` coords + key — only the byte source changes. **GCS
Archive** is the recommended cheap + partial-without-thaw tier; Deep Archive is
whole-thaw DR-only.

## Shared contract (IDENTICAL in all three prompts)
The full design is `~/system/docs/design-ingest-v2-rao-archive.md` (cited "design
§X"); it is the source of truth. These invariants bind all three repos:
1. **Representations** (the only ones): `rao-plain-v1`, `rao-aead-v1`,
   `d2tar-raw`, `raw-bytes`. Encryption is **part of** the representation
   (`rao-aead-v1` = encrypted) — no separate flag, no `(class,copy)→representation`
   policy.
2. **RAO geometry:** 256 KiB chunks; `rao-aead-v1` is per-chunk AEAD
   (independently decryptable; encrypted ranged extract is proven).
3. **Ruleset (§A2):** ordered, **first-match-wins** (rsync/borg-style; the
   documented deviation from gitignore — lint unreachable rules). Verbs **`blob`,
   `exclude`** only; **granular is the implicit default**. `rem archive build
   --rules` is the canonical engine; sutradhara names the ruleset via the
   artifactclass policy `ruleset` field. Re-include deferred.
4. **Wrapping (§A3–A5):** `.remwrap.tar` via a pinned mainstream tar engine
   (never a Rust codec; dialect pinned by the §A3.5 round-trip test); each blob
   carries an on-tape sibling **`.remwrap.idx`** (member → offset/len + sha256),
   default-on, `--no-index` to disable.
5. **Asset identity (§B4):** the per-file **plaintext sha256**, copy-independent;
   restored bytes always verify against it.
6. **Per-copy locator (§B4/B5):** granular member →
   `{member_path, first_chunk_lba, size_bytes}` (rem) / `{member_path,
   block_range}` (d2) in `asset_locator`; a file *inside* a blob → coords from the
   on-object `.idx` (never per-member DB rows), found via the coarse `blob_root`.
7. **Bundle (§B1–B4):** single-artifactclass; **synthetic `bundle_id`**; strict
   container discriminator; `open → sealed`.
8. **Placement (§B6):** **`artifactclass ↔ pool` many-to-many** (`active`); a
   **pool is self-describing** `{id, backend, representation, location,
   offsite_gate, tier}` and **owns** representation, immutable once non-empty. **No
   `content_class` tier, no `PlacementTagPin`** — invariants are pool-immutability
   + scrub `copy.representation == pool.representation`. Restore = ordered pool
   preference.
9. **Conformance + `expect` (§A4):** scan classifies native/wrap-fallback/excluded,
   output aggregated/clustered (prefix × reason + samples + counts) with the
   density+count rule (blob-suggest / straggler / sanity-ceiling). Per-ruleset
   **`expect`** (`compliant`|`messy`) decides halt-on-deviation vs auto-wrap. Scan
   suggests; reviewer confirms.
10. **Customer manifest (§A5):** on a blob/archive emit a receipt (archive ID +
    listing + exclusion summary, signed) — re-issuable from the on-object `.idx`.

## Constraints / DoD
- O/N/Q/J behaviour + tests unchanged (pool model reproduces their placements).
- Encrypted copies aren't byte-deterministic — assert plaintext round-trip +
  within-run digest stability, never a fixed ciphertext digest.
- Per `AGENTS.md`: run `uv run pytest -q` (paste), commit per phase, update
  `docs/INDEX.md`.

## Sequencing
P1–P5 are buildable now against today's `rem archive build` (multi-file + native
PFR) for the clean path; the messy-source path + the gate's clustered review use
rem's `--rules`/`--scan-only` (companion prompt). P6 is optional/later.
