# Design — Ingest v2 Phase S: RAO re-cut (bundling · catalog · restore)

> **Status:** current. **Supersedes** `design-ingest-v2-phase-s.md` (the
> GNU-tar-bundle + harness-offset-PFR + AOF-nesting design, written before the
> amber→RAO merge). Brainstorm 2026-06-14 (the owner + Claude). The ruleset /
> wrapping layer lives in `~/remanence/docs/ingest-policy-design-v0.1.md`
> (referenced here, **not** duplicated). Implementation by codex per the §8
> split; verified by the harness Phase-S scenario. Master flow:
> `~/system/docs/design-ingest-flow.md`.

## 0. What this re-cut changes and why

amber merged into remanence as **RAO**, whose archive objects are
*tar-native with a built-in member manifest* and optional AEAD. That dissolves
most of the old Phase S: there is no separate GNU-tar bundle sealed opaquely,
no harness-computed tar offsets, no `rem-tar-v1[AOF1[tar]]` nesting. A bundle's
rem copies are produced by `rem archive build` straight from the member files,
and RAO's own per-member manifest *is* the PFR index.

This doc covers the **sutradhara-side storage mechanics** on that model:
bundling, per-copy fan-out, the catalog, and restore. The **ruleset** that
decides per-file `blob`/`exclude`/granular and creates `.remwrap.tar` wrappers
is remanence's `rem archive build --rules` (see ingest-policy-design); this doc
consumes it, it does not re-specify it.

## 1. Locked decisions (brainstorm output)

1. **One logical bundle, materialised independently per copy.** A bundle is a
   grouping of same-class artifacts. On flush it is built *separately* for each
   copy in that copy's own container — there is **no** shared serialized
   intermediate. (§2)
2. **Copy set:** copy-1 `rao-plain-v1` (rem, working/online), copy-2
   `rao-aead-v1` (rem, encrypted, offsite), copy-3 `d2tar-raw` (d2tape, a plain
   commodity tar — **independent of remanence/RAO entirely**). (§2)
3. **Accumulate → flush.** Per-class open accumulator; flush when
   `size ≥ target_gb` **or** `age ≥ max_age` (age measured from the *oldest*
   pending artifact). (§3)
4. **RAO-native entries.** Each member file is a first-class RAO entry (or a
   `blob`/fallback `.remwrap.tar`, per the ruleset); RAO's native member
   manifest gives per-asset PFR. No separate tar-with-offsets layer. (§4)
5. **Bundle = first-class catalog entity, synthetic id**, strict container
   discriminator (no asset semantics, excluded from every asset query, never
   itself a restore target). Lifecycle: `open → sealed`. (§5)
6. **Asset identity = plaintext file sha256, copy-independent.** Per-copy
   locators map an asset into each copy's object. Restored bytes verify against
   the one asset digest regardless of source copy. (§5, §6)
7. **Restore = ordered copy-class preference per class** (overridable), engine
   picks the first available + healthy copy; PFR works on all three. (§6)
8. **`exclude` stays in the unified ruleset** (it is policy authored by
   sutradhara, executed by rem's engine — not a rem curation decision). (refs
   ingest-policy-design)

## 2. The model: one logical bundle, three independent materialisations

```
            ┌─ open accumulator (staging files + bookkeeping) ─┐
artifacts ─▶│  artifactclass = X · running size · oldest-age   │
            └──────────────────────┬───────────────────────────┘
                       flush (size ≥ target_gb OR age ≥ max_age)
                                   │
        ┌──────────────────────────┼──────────────────────────┐
        ▼                          ▼                          ▼
  rem archive build        rem archive build --encrypt    system tar (via d2tape)
  → copy-1 rao-plain-v1    → copy-2 rao-aead-v1           → copy-3 d2tar-raw
  (online working)         (offsite, encrypted)           (commodity shelf, no rem)
```

The **bundling policy** (which artifacts group, when to flush) is
copy-independent sutradhara state. The **container** differs per copy: same
member files in, three different serialized byte streams out, three different
`stored_digest`s. The shared identity lives one level down, at the **per-file
asset hash** (§5), which every copy preserves.

Why no shared serialized object (unlike the old design): RAO-native entries
mean rem builds from the *files* to emit per-member PFR, rather than sealing one
opaque pre-built tar. We traded the old shared-bundle-tar hash for native PFR;
the shared identity moved down to the file level, which is the asset identity
anyway.

## 3. Bundling: accumulate → flush

- **Open accumulator, per artifactclass.** Holds the pending artifacts, a
  running total size, and `opened_at`. Durable (survives a sutradhara restart;
  answers "what is pending / how old / force-flush"). This is the `open` state
  of the bundle entity (§5).
- **Flush triggers:** `size ≥ target_gb` **or** `age ≥ max_age`, where age is
  measured from the **oldest pending artifact** so no artifact waits longer than
  `max_age` for tape. Both come from the artifactclass policy `[bundling]`
  section: `target_gb`, `max_age`.
- **Single-class.** Every member shares one copy recipe; one bundle, one policy,
  one fan-out.
- **Oversize:** a single artifact ≥ `target_gb` flushes immediately as its own
  bundle. A single artifact exceeding tape capacity → clear error + `# TODO:
  oversize split` (M-way split deferred, as before).
- **The timeout is durability-safe.** Bundling delay only affects the *tape*
  copies. Interim durability is already provided by the per-intake encrypted
  **cloud blob** (S3, created at intake) plus the staging copy — both exist
  within hours, independent of tape bundling. So `max_age` (24–48 h) costs
  nothing on durability; it only sets when tape copies land (and therefore a
  floor on how long staging + cloud must persist before the deletion gate).
- **Small timeout-flushed bundles are a non-issue.** Worst case is one
  small bundle per class per window — on the order of ~180/year — versus
  thousands of unbundled files. No mount-batching machinery needed; a plain
  size-or-timeout flush suffices.

## 4. RAO-native entries and PFR

On flush, the rem copies are `rem archive build [--rules <ruleset>] --inputs
<member files…>`. Within the object:

- Each member file is a **first-class RAO entry** (granular default), or a
  `blob`-wrapped / fallback `.remwrap.tar` entry per the ruleset. The ruleset is
  evaluated by rem; sutradhara only binds source-type → ruleset.
- RAO's **native member manifest** (`path`, `file_sha256`, `first_chunk_lba`,
  `size_bytes`) is the per-asset PFR. No catalog-side offset arithmetic.
- Per-asset restore = `rem archive extract --path … --first-chunk-lba …
  --file-size-bytes … --range …` (proven, incl. the encrypted copy — see the
  RAO scenario).

The **d2 copy** is a plain system `tar` of the *same member files*, written as
one tape object by d2tape (no remanence involvement). Its per-asset locator is
the member name + the tape block range d2tape-cli records; ranged single-file
restore is whole-then-slice in v1 (fine for a fallback copy).

Files inside a `.remwrap.tar` are reachable per the optional **blob inner-index**
(member → offset/length within the wrapper, catalog-side, derived state) — see
ingest-policy-design §5.

## 5. Catalog model

The existing `Copy` / replication / scrub / status machinery is reused — a
bundle's copies are ordinary `Copy` rows. New/changed entities:

- **`asset`** (unchanged): content-addressed file, identity = plaintext sha256.
- **`bundle`** (new): the grouping + lifecycle. `bundle_id` **synthetic** (PK),
  `artifactclass`, `status` (`open` | `sealed`), `opened_at`, `flushed_at`,
  `member_count`, `target_gb`, `max_age`. **Strict container discriminator:** no
  `virtual_path`, tags, or proxies; excluded from every asset-facing query;
  never itself a restore target.
- **`bundle_member`** (new): `bundle_id → asset_hash` (+ the source artifact
  reference). The membership of what was *stored*.
- **`copy`** (extended): a copy is now of a **bundle** (Phase-S path) or a single
  asset (O/N/Q/J path). For bundle copies: reference `bundle_id`, plus the
  copy's own `integrity_hash` (= that container's `stored_digest`),
  `native_locator`, `representation`. Single-asset copies are unchanged.
- **`asset_locator`** (new): `(asset_hash, copy) → locator`. For rem copies:
  `{member_path, first_chunk_lba, size_bytes}`. For the d2 copy:
  `{member_path, block_range}`. This is what restore dispatches on.
- **`exclusion_record`** (new): per bundle/ingest, the deliberately-dropped set —
  path roots + file counts + bytes + `ruleset_name`/`ruleset_hash`. Satisfies
  "never silent" (ingest-policy §1.2). **Separate** from `bundle_member`:
  excluded files are not members and never enter any object.

Three cleanly separated records, as established in the brainstorm: **bundle id**
(synthetic, allocated at open), **member manifest** (per-member asset hashes +
locators, known at build), **exclusion record** (what was dropped).

## 6. Restore

- **Copy-independent identity.** `asset_locator` resolves an asset within any
  chosen copy; the recovered bytes always verify against the single asset
  plaintext sha256. Restore correctness never depends on which copy was pulled —
  only efficiency and availability do.
- **PFR works on all three copies:**
  - copy-1 `rao-plain-v1` — native RAO range-extract, **no key**, online. Default.
  - copy-2 `rao-aead-v1` — native RAO range-extract **with key** (registry),
    offsite → DR path.
  - copy-3 `d2tar-raw` — tar member extract via d2 block locator;
    whole-then-slice acceptable v1.
- **Source selection = a per-artifactclass, overridable *ordered preference* of
  copy_classes** (not a single "primary" flag — the ordered list *is* the
  fallback story). Default for masters: `[copy-1, copy-3, copy-2]`. The restore
  engine walks the order and takes the first copy that is available + healthy,
  then dispatches the per-copy ranged extract and verifies.
- Multi-asset restores order by `(copy/tape, object, offset)`.

## 7. What carries over vs what is replaced

**Carried over from the superseded Phase S (intent intact):** single-class
containers; anti-shoeshine packing of small same-class artifacts; per-file asset
identity preserved across copies via PFR; the d2 shelf copy is commodity-tar
readable; two-integrity-layers / one-read (build verifies each member's
`file_sha256`; verify-after-write per copy).

**Replaced:** GNU-tar bundle → independent per-copy `rem archive build`;
harness-computed `TarInfo.offset_data` PFR → RAO native member manifest;
`bundle`/`pfr` tables keyed on a bundle content-hash → `bundle` (synthetic id) +
`bundle_member` + `asset_locator`; `rem-tar-v1[AOF1[tar]]` nesting → direct RAO
object; "one build feeds all three copies" → one *policy*, three independent
materialisations.

## 8. Cross-repo work split (prompt map)

| Repo | Work | Notes |
|---|---|---|
| **remanence** | `rem archive build --rules` (ruleset engine + `blob`/`exclude` + `.remwrap.tar` wrapping + conformance scan), `rem restore` unwrap, blob inner-index hooks | Per `ingest-policy-design-v0.1.md`. Native multi-file build + member PFR **mostly exist today**; `--rules`/wrapping is the new piece. |
| **sutradhara** | open-accumulator + size/age flush; policy-document `[bundling]` (`target_gb`, `max_age`) + restore-preference order; `fan_out_artifact` (materialise per copy: 2× `rem archive build`, 1× d2 tar); catalog (`bundle`, `bundle_member`, `asset_locator`, `exclusion_record`); restore engine (preference order → per-copy dispatch → verify) | The bulk of this doc. O/N/Q/J single-asset path stays byte-identical. |
| **d2tape-cli** | ingest-a-provided-tar-as-one-object write mode (bundle-level, returns start block); per-file block locators; whole-then-slice ranged read | Same companion as the old §7; unchanged by RAO. |
| **system (harness)** | Phase-S scenario(s): rough-seg send → bundle accumulation/flush → 3-copy fan-out → per-asset PFR restore from each copy → exclusion/ruleset assertions | New scenario id; suite-wired. |

**Sequencing:** sutradhara bundling + fan-out + catalog + restore are buildable
and unit-testable **now** against today's `rem archive build` (multi-file +
native PFR) — `--rules`/wrapping can land in parallel and is only needed for
messy non-compliant sources, not the clean media-card flow. d2 copy-3 goes green
when the d2tape-cli companion lands.

## 9. Open items / deferred

- **Re-include verb** (rsync include-before-exclude) — deferred (ingest-policy
  Refinements #3); add on concrete demand, purely additive.
- **d2 efficient ranged read** — whole-then-slice in v1; true ranged later.
- **Oversize artifact M-way split** — deferred; clear error meanwhile.
- **Wrapper tar engine + dialect** — pinned by the round-trip test
  (ingest-policy §3.5 / §8.1); lean bsdtar/libarchive.
- **Bundle `max_age` defaults** per class; **restore-preference** defaults per
  class — policy-table values, set at bringup.
- **Encrypted copies aren't byte-deterministic** — assert plaintext round-trip +
  within-run digest stability, never a fixed ciphertext digest.

## 10. Acceptance

Phase S = its harness scenario green from a clean slate: rough-seg send →
inode-match send-scan → same-class artifacts accumulate → flush (size or age) →
3-copy fan-out (rao-plain + rao-aead + d2tar-raw on distinct media) →
per-asset PFR restore proven from the working copy (and from the encrypted +
d2 copies) → restored bytes equal the asset plaintext digest → exclusion record
present and counted. O/N/Q/J unchanged. Unit suites green throughout.
