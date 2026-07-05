# Design — PFR wiring, MXF end-to-end (sutradhara ⇄ format-anatomy ⇄ rem)

Date: 2026-07-05 · Status: **frozen** (panel 3 lenses → fold → verify (1 blocker found) → micro-fold → re-verify PASS, 2026-07-05)
Panel 2026-07-05: 3 blind lenses (failure/codex, contract/opus, cost/opus ×2 passes);
~35 findings (3 convergent blockers, ~20 majors); folded in one pass. Core discoveries:
the draft assumed pfr_core entry points that don't exist (scrape-free cut, composed
isolated scrape, blob-root threading, CBE support) — those are now **M6 (integration
seams) in format-anatomy**, a prerequisite prompt of this wiring — and the draft's failure
taxonomy silently overrode derivation-reconciler §3.4a (now conforms).
Parents: `design-pfr-restore.md` (policy; its §5 "not built" is stale — P2.1/P2.2 exist;
a stale banner on that section ships with this arc),
`~/format-anatomy/docs/design-pfr-plugin-architecture.md` (plugin core; §9: rem working
copies only), `design-derivation-reconciler.md` §3.4a (failure/validity contract).
Survey: `~/format-anatomy/analysis/2026-07-05-sutradhara-wiring-survey.md`.

## 1. Goal and v1 scope

ingest → real `pfr-index` sidecar → `sutra pfr cut <ref> --from/--to` → ranged read of the
RAO working copy through ONE rem read session → playable sub-clip MXF, delivery-validated.
Boundaries: MXF grammar cuts only; `RAO_PLAIN` ranged path (AEAD → rung 3 handover);
`file_relative`; CLI only.

## 2. M6 — pfr_core integration seams (format-anatomy, prerequisite prompt)

1. **`cut_from_sidecar(sidecar, source, …) -> CutResult`** — plan()+rewrap() from a stored
   `PFRSidecar`, NO scrape (today `cut()` always re-scrapes — all three lenses; without
   this, Component A's ingest spend is dead weight and every cut pays scrape twice).
2. **`registry.scrape_path_isolated(path, blob_dir)`** — sniff in parent + isolated child
   scrape of the selected plugin (no composing API exists today, so the draft's
   "sandboxed at ingest" was unreachable).
3. **Blob-root threading** through plugin/registry/cut construction (today hardwired to
   /tmp — sidecar blob refs would dangle after tmp reaping, silently killing rung 1).
   Index-blob externalization gated on plannability. Rewrap's validation-scrape writes its
   blobs to a throwaway dir (today it deposits unreferenced blobs into the store per cut).
4. **CBE support + entry-size generalization**: plan() stride math for
   `edit_unit_byte_count != 0`, CBE index-segment serialization in rewrap (today raises
   NotImplemented), and VBE entry sizes per the ST 377-1 formula (11 + 4×SliceCount…) —
   Sony's 15 is one point, ffmpeg's 11 is another. Without this NO generated fixture can
   go rung-1 green (cost lens, verified against plugins_mxf source).
5. **Serialization + synthesis**: `CutResult.to_dict()`/`RestorePlan.to_dict()`;
   `make_fallback_sidecar(source, failure)` exposed for handler-side use (registry only
   auto-falls-back on cap-exceed today).
5b. **`registry.scrape_source(source, blob_dir)`** public seam (blob-dir-threaded,
   in-process) — used by cut-time blob regeneration over a live `RaoObject` session, where
   subprocess isolation can't inherit the gRPC session. Rationale recorded: cut is
   operator-attended and reads our own archived bytes (bounded 8 MiB budget still applies);
   the parent-brokered-range child is the noted hardening path if that posture changes.
   M6 acceptance includes a regeneration test through this seam.
6. M6 acceptance: cut_from_sidecar ≡ cut() on the real XAVC-I fixture (identical output
   hash); a **pre-verified generated fixture** — `ffmpeg … mxf_d10` D-10 file scraped and
   cut green via the CLI in M6's own tests; the same generation command is then reused by
   the scenario (§5), so the fixture premise is proven before the scenario exists.

## 3. Component A — real `pfr-index` handler (sutradhara)

Keep: signature, `source_path` resolution, sidecar path convention,
`record_index(index_kind="pfr-index-v1")` + `pfr_sidecar_path` pointer, reconciler
enqueue/dedupe, worker rails. Swap the body:

- `scrape_path_isolated(source_path, blob_dir=<cache_root>/intakes/<id>/pfr/blobs/)`,
  run **under the job's granted lease** via the same `run_managed`/cpu-lease plumbing that
  wraps ffprobe today (raw multiprocessing outside the lease scheduler is not acceptable).
  Child wall-clock **120 s** (pfr_core's 10 s default + lease contention would strand
  healthy items on sticky fallback — cost lens).
- Payload = pfr_core sidecar JSON with `ingest_item_id` + `logical_asset_hash` injected
  into `source_identity` (catalog back-link preserved). "No schema churn" means the
  RECORDING contract (pointer + index_kind) — the file body is intentionally replaced;
  verified no consumer reads the old body.
- **Atomic publication**: blobs (content-addressed temp+rename) → sidecar temp+fsync+
  rename → `record_index` + pointer. `_has_pfr_sidecar` upgraded to parse JSON + verify
  blob refs (presence-only `exists()` admits partial states).
- `SUTRADHARA_FAKE_FFPROBE` short-circuit is retired with the stub; hermetic scenarios pay
  real scrapes on tiny fixtures (better signal; decided, stated so suite behavior doesn't
  change by accident).
- **Failure matrix — exhaustive over ReasonId, conforming to §3.4a:**

| Outcome | Reasons |
|---|---|
| retryable (backoff) | `source_changed`; IO/`exception` at read; timeout/RSS kill; `budget_exceeded` (wall-clock/RSS-shaped — transient contention, NOT structural) |
| **blocked(unsupported-source) + SUSPECT + blocked_tool=(pfr_core, version)** — §3.4a preserved, and applied only on a **determination**, not a single event: two consecutive attempts failing at the same stage with a parse-level reason (`malformed` structure; `index_unavailable` on a file that sniffed MXF) after backoff retries exhaust. A one-off parser crash is retryable until it reproduces. | reproducible parse-unyielding sources only |
| completed with **fallback sidecar** (handler-side `make_fallback_sidecar`; a fact, not an error) | `op_atom_unsupported`; non-MXF grammars; `cap_exceeded_fallback` (byte-budget structural — the ONLY budget reason that falls back) |
| loud stop (job error) | anything unmapped — matrix is closed; new ReasonIds fail tests |

- **Staleness (decided)**: no automatic re-derivation exists (target key has no version
  axis; reconciler is presence-only — verified). Stub→real and future grammar upgrades =
  **operator-triggered**: `sutra pfr reindex --grammar fallback|--all` ships in this arc
  (runbook line in prompt). **Force semantics defined**: reindex enqueues pfr-index jobs
  DIRECTLY (not via the presence-gated reconciler) with dedupe key
  `pfr-reindex:{item}:{recipe_version}`; the handler in force mode writes the new sidecar
  temp+fsync+rename OVER the existing path (pointer unchanged, atomic replace — no window
  with a missing sidecar) and re-runs `record_index` (idempotent). `recipe_version` is persisted in the fact metadata now, so the
  deferred reconciler §9(4) reopen-on-mismatch has data when built.
- **No size/artifactclass gate on scrape** (deliberate, recorded): real scrape ≈ stub cost
  (≤8 MiB bounded reads, ~1.6 MB blobs ≈ 0.03% of item ingest I/O), and a wrong gate +
  no-re-run policy would permanently strand items on rung 2.

## 4. Component B — `RaoObject` source + cut path (sutradhara)

- **Coordinate math has one owner**: extract `member_byte_base(native_locator)`
  (= `first_chunk_lba × RAO_CHUNK_SIZE`; today inlined at archive_restore.py:253, private
  getter duplicated in archive_fanout.py) — `read_member_to_path` AND `RaoObject` both
  call it; prompt forbids re-inlining. `size()` = locator `size_bytes`. **Negative offsets
  resolve against the member, not the object.** Extend `RemanenceBackend` with a
  session-scoped reader rather than growing a second gRPC client.
- **Construction cross-check**: locator `size_bytes` vs rem `GetFile` once per cut (cheap
  drift detection vs self-heal/rebuild); mismatch → `source_changed` → rung 2, loudly.
- **One read session per cut**: context-managed OpenReadSession…CloseReadSession (close in
  finally), reused across blob-regeneration reads + all essence reads — NOT the per-call
  open/read/close helper (≈3× RPCs). gRPC mapping: LOST/UNAVAILABLE → retry once →
  rung 2; DEADLINE_EXCEEDED → retry once; mid-cut session invalidation → SourceChanged.
  Sessions pin a drive — held only for the streaming phase.
- **Cut = `cut_from_sidecar`** (M6): zero scrape reads on the happy path; essence streamed
  in ≤8 MiB chunks. (Per-window coalesced ReadObjectRange deferred — §7.)
- **Blob cache, not archive**: blobs are regenerable from the working copy. The pfr blob
  dir is a size-capped LRU cache (default 20 GiB, config key; retention.py does NOT manage
  it today — verified). Missing blob at cut → one bounded re-scrape through the same
  session, then proceed. Small facts sidecar kept indefinitely.
- **Fallback ladder = explicit state machine**, every rung attempt recorded:
  1. grammar mxf + plannable + RAO_PLAIN → ranged `cut_from_sidecar`;
  2. sidecar missing/fallback-grammar/plan-rewrap refusal/blob regeneration failed/session
     errors exhausted → member whole-file restore (`read_member_to_path`), reason recorded;
  3. AEAD-only → existing whole-object materialize + CLI decrypt → **hand over the
     decrypted member** with reason `aead_ranged_unsupported` (a `ffmpeg -ss/-to -c copy`
     clip is a documented operator step, NOT a code path — the I/O savings are already
     gone and design-pfr-restore §2.2 owns that pattern). **Scratch preflight before any
     tape I/O**: free space ≥ stored + decrypted + output estimates; configurable root.
- **Concurrency**: `sutra pfr cut` acquires the worker io-lease path inline (cuts contend
  fairly for drives), a per-item advisory lock (second concurrent cut → `busy`), and
  temp-write+rename on output.
- **CLI**: `sutra pfr cut <asset-selector> --from S --to S [-o PATH] [--json]`,
  `sutra pfr status <asset-selector>`, `sutra pfr reindex …`. Selector = existing
  `resolve_member_asset_hash` grammar (asset hash + `--artifactclass` + member selector) —
  consistent with `sutra archive restore`; no new resolver. Default output = human summary
  (rung, snap deltas, path); `--json` = full envelope via M6 `to_dict()`s.

## 5. Verification member

- **Scenario PFR (hermetic, ~/system)**: fixture = the **M6 pre-verified** `mxf_d10`
  generation command (CBE; the draft's "mpeg2video → CBE" premise was wrong twice over —
  default mpeg2 is long-GOP, and pfr_core had no CBE support at all until M6). Scenario
  asserts `index_shape == CBE` and `edit_unit_byte_count > 0` on the sidecar BEFORE
  cutting. Flow: ingest → drive reconciler+worker synchronously (scenario-R pattern, no
  timer waits) → sidecar asserts (index_kind, grammar, capability, blob refs verified) →
  archive → cut through a **fake in-process ReadObjectRange server** (rung-1 mechanics
  proven hermetically; live-VTL confirmation is a separate suite-gated leg per the
  clean-slate rule) → ffprobe duration ≈ window. Negative legs: fallback-grammar item →
  rung 2; AEAD-only → rung 3 handover + reason + scratch-preflight exercised; concurrent
  second cut → `busy`.
- sutradhara units: RaoObject (session reuse, shared member-base primitive, half-open
  ranges at member boundaries, negative offsets vs member, stream reassembly,
  session-invalid → SourceChanged); handler matrix (every ReasonId row incl. §3.4a
  blocked+SUSPECT leg; atomic-publication crash points); blob-cache eviction+regeneration;
  reindex helper.
- format-anatomy M6 units: cut_from_sidecar ≡ cut(); isolated-scrape composition; blob-dir
  threading; CBE plan/rewrap on the generated D-10 fixture; VBE entry-size generalization
  (11-byte entries).
- Registry `covers` + Makefile target in ~/system.

## 6. Prompt set (contract = THIS doc; server-first)

1. `~/format-anatomy/docs/prompt-pfr-core-m6-seams.md` — §2 (prerequisite).
2. `~/sutradhara/docs/prompt-pfr-wiring-sutradhara.md` — §3+§4; plus stale banner on
   design-pfr-restore §5; packaging: dist rename `format-anatomy` via import hygiene (a
   packages.find exclusion of d2_format_observer would break its own tests and doesn't
   confine an editable install — verified), uv path dep + `uv sync` in ~/sutradhara AND
   ~/system.
3. `~/system/docs/prompt-pfr-scenario.md` — §5 scenario.

## 7. Out of scope (recorded)

Web surface; AEAD ranged decrypt; automatic re-derivation (deferred reconciler §9(4);
`recipe_version` persisted now); observer-v2 deep probe; per-window coalesced
ReadObjectRange (needs a pfr_core streaming sink; session reuse is the material win);
economics-gate constants (`estimated_saved_time` hardwired 0 — revisit only on physical
MSL working sets); blob-store readback double-hash (kept for durability; operator-tunable).
