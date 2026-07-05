# Design — PFR wiring, MXF end-to-end (sutradhara ⇄ format-anatomy ⇄ rem)

Date: 2026-07-05 · Status: draft (pre-panel)
Parent designs: `design-pfr-restore.md` (policy/tiers; its §5 "not built" claims are stale —
P2.1/P2.2 restore paths exist), `~/format-anatomy/docs/design-pfr-plugin-architecture.md`
(the plugin core; §9 scope correction: **rem working-copy tapes only**, D2 frozen).
Ground truth survey: `~/format-anatomy/analysis/2026-07-05-sutradhara-wiring-survey.md`
(file:line refs for every surface named here).

## 1. Goal and v1 scope

Wire the implemented `pfr_core` (M1 core + M2 mxf scrape/plan + M5 rewrap/cut, all gated;
MXF proven end-to-end on real fixture slices) into sutradhara so that:

ingest → real `pfr-index` sidecar (replaces ffprobe stub, no schema churn) →
`sutra pfr cut <item> --from/--to` → ranged read of the RAO working copy via rem
`ReadObjectRange` → playable sub-clip MXF, delivery-validated.

v1 boundaries: MXF grammar only (others scrape as `fallback` and cut refuses politely);
`RAO_PLAIN` copies only (AEAD → existing whole-member restore + local cut, see §4 ladder);
`file_relative` time basis; CLI surface only (web later).

## 2. Packaging (prerequisite)

format-anatomy's distribution is renamed `format-anatomy` (import package stays `pfr_core`;
the frozen `d2_format_observer` stays in-repo but OUT of the distribution — it is legacy-D2
tooling per §9 and must not ride into sutradhara). sutradhara adds it as an editable uv path
dep exactly like the `sutradhara-receive` precedent (`[tool.uv.sources]` path dep), then
`uv sync` in `~/sutradhara` **and** `~/system` (the known land-the-seam-or-silently-regress
trap — harness memo). Python 3.12 everywhere; pfr_core is stdlib-only.

## 3. Component A — real `pfr-index` handler

Swap the internals of `jobs/handlers/pfr_index.py` (keep: `ingest_item_id` signature,
`source_path` resolution, sidecar path convention `<cache_root>/intakes/<id>/pfr/<item>.pfr.json`,
`record_index(index_kind="pfr-index-v1")` + `pfr_sidecar_path` pointer, reconciler enqueue +
dedupe key, worker rails):

- Body: `registry.scrape_path(source_path)` (subprocess-isolated variant — parser on
  arbitrary bytes stays sandboxed even at ingest). Result serialization IS the sidecar
  payload (pfr_core's `pfr-index-v1` JSON — the design's "replaces the stub without schema
  churn" promise, now literal).
- Blob refs: pfr_core writes out-of-line blobs (header metadata + index segments) to a blob
  dir; for sutradhara the blob root is `<cache_root>/intakes/<id>/pfr/blobs/` — same lifecycle
  as the sidecar, referenced by content address from the payload. (No blobs in the catalog.)
- Failure mapping: `ScrapeFailure.reason_id` → job outcome: `source_changed`/IO →
  **retryable** (backoff); parser/structure reasons (`index_unavailable`,
  `op_atom_unsupported`, malformed) → **completed-with-fallback-sidecar** (grammar
  `fallback`, reason recorded) — NOT a blocked job: a file we can't index is a fact, not an
  error, and the reconciler must not spin on it. Timeout/RSS kill → retryable once, then
  fallback sidecar. (Matches the plugin-architecture failure schema; keeps the 2026-07-03
  loud-degradation rails meaningful.)
- ffprobe stub retirement: the old stub payload is superseded; existing stub sidecars in dev
  catalogs are re-scraped naturally by the derivation reconciler when the handler version
  bumps the derivation key (survey: dedupe key is target-scoped — bump `index_kind` stays
  `pfr-index-v1`; version lives INSIDE the payload per the three-axis rule; reconciler
  re-derivation policy = existing sidecar present ⇒ no re-run, so dev catalogs need one
  manual re-enqueue; note in prompt, not a mechanism).

## 4. Component B — `RaoObject` ByteRangeSource + the cut path

New module in sutradhara (implements pfr_core's `ByteRangeSource` ABC):

- Construction from the catalog: item → `AssetLocator.native_locator`
  (`first_chunk_lba`-derived member base, `size_bytes`, `member_path`) → member-relative
  coordinates per the plugin-architecture coordinate contract. **v1 uses the locator
  transcription** (already in the catalog, zero new rem calls); rem's live
  `Catalog.ListFilesInObject` gRPC stays a cross-check tool, not a dependency — recorded
  choice, revisit if locator drift is ever observed.
- `read(offset, length)`: one `ReadObjectRange(session, object_id, start, end)` streamed
  call per read, member base applied; `size()` = locator `size_bytes` (logical member
  length). Identity snapshot: object id + copy id + locator digest; recheck per read is a
  no-op for immutable RAO objects but `session` invalidation surfaces as `source_changed`.
- Read efficiency: `cut()` reads the head window (sidecar already has the index — scrape is
  NOT re-run at cut time; plan comes from the stored sidecar) and then streams the essence
  ranges. Chunk size = `stream_chunk_bytes` aligned (default from rem), sequential.
- **Fallback ladder (explicit, each rung recorded in the result):**
  1. sidecar grammar `mxf` + `RAO_PLAIN` copy → ranged sub-clip cut (the point of all this);
  2. sidecar missing/fallback-grammar or plan/rewrap refuses (e.g. `gop_rewrap_unsupported`)
     → existing member-level whole-file restore (`read_member_to_path`), reason surfaced;
  3. `RAO_AEAD`-only copies → existing whole-object materialize + CLI decrypt + (optional)
     LOCAL cut of the decrypted file via `pfr_core.cut(LocalFile…)` so the operator still
     gets a sub-clip, just without the I/O savings.
- CLI: `sutra pfr cut <item-ref> --from S --to S [-o PATH]` (click group per `cli/archive.py`
  template); prints the CutResult JSON (plan, snap deltas, validation report, rung used).
  `sutra pfr status <item-ref>` shows sidecar presence/grammar/capability snapshot.

## 5. Verification member (ships in the same prompt set)

- **Scenario PFR** (hermetic, ~/system): generate a small real OP1a MXF with ffmpeg
  (mpeg2video → CBE-indexed — deliberately exercises the CBE path our real-fixture tests
  don't), ingest it through intake, let the derivation reconciler enqueue `pfr-index`,
  assert: sidecar exists, `index_kind=pfr-index-v1`, grammar `mxf`, capability snapshot
  sane; archive to the hermetic rem backend; `sutra pfr cut --from 1 --to 3`; assert output
  ffprobe-parses with duration ≈2 s and the ranged-read path (rung 1) was used. Negative
  legs: cut on a fallback-grammar item → rung 2 whole-member; AEAD-only → rung 3.
- sutradhara unit tests: `RaoObject` against a fake ReadObjectRange server (range math,
  member base, half-open semantics, stream reassembly, session-invalid → source_changed);
  handler failure-mapping matrix (reason_id → retryable/fallback).
- Registry `covers` declaration for the new scenario in the harness registry.

## 6. Prompt set (contract = THIS doc, referenced not inlined)

1. `format-anatomy`: packaging rename + distribution split (small; may be done directly).
2. `~/sutradhara/docs/prompt-pfr-wiring-sutradhara.md`: components A + B + CLI + unit tests.
3. `~/system/docs/prompt-pfr-scenario.md`: scenario PFR + registry/covers + Makefile target.
Server-first ordering (2 before 3). Each implements against this design; diff gate each.

## 7. Out of scope (recorded)

Web request surface; significance/priority integration; AEAD ranged decrypt (needs rem-side
design); observer-v2 deep probe (deprioritized per §9); M3 grammars (next arc); economics
gate constants (trivial on ingest-indexed path; revisit only for physical-MSL working sets).
