# Prompt RM3.3b — sutradhara consumes the remanence ranged-ciphertext extract (O(N×object)→O(object))

**Status:** pending (gpt-5.6-sol). Completes RM3.3: the remanence ranged extract + `archive
covering-range` query are LANDED (remanence main); this wires sutradhara's AEAD ciphertext pump to feed
ONLY each member's covering stored range instead of the whole object. **A prior stale patch broke RM3.0's
tests — build FRESH against CURRENT main; the RM3.0 two-phase timeout + bounded-RSS behavior MUST stay
green.** Physical throughput acceptance is the MSL3040 window.
**Normative:** `~/remanence/docs/design-restore-tape-leg-v0.1.md` §6.6 (mapping stays in Rust; sutradhara
passes the PLAINTEXT member range; option (c): the remanence `archive covering-range` query returns the
covering STORED byte-range, then a bounded `ReadObjectRange(start,end)` feeds a trimming ranged extract;
**N independent ranged opens per member**, no multi-member single pass). **Read the landed remanence
surface** (`~/remanence` main: the new `archive covering-range` CLI/query added in RM3.3 — its exact args +
output; grep `covering-range`/`CoveringRange`) and the CURRENT sutradhara AEAD path.
**Read the real sutradhara code you change (verify, cite):** `src/sutradhara/archive_restore.py` — the AEAD
ciphertext pump `pump_ciphertext` (~1294) + `_stored_object_range(copy)` (~1292, currently the WHOLE object
`ByteRange(0, stored_size)`), `_open_backend_range_chunks` (~1211), and RM3.0's two-phase clock
`streaming_started` (~1291) + `_REM_STREAM_MOUNT_GRACE_SECONDS` (~69) + the inactivity/mount-grace reader —
**PRESERVE all of it byte-compatibly** (do NOT restructure the pump; the RM3.0 tests
`test_mount_error_fails_fast_without_waiting_for_grace`,
`test_large_duplex_restore_has_bounded_rss_and_does_not_deadlock` MUST stay green).

## Scope
1. For an AEAD MEMBER extract, replace the whole-object `_stored_object_range(copy)` with the member's
   **covering STORED byte-range**, obtained from the landed remanence `archive covering-range` query for
   the member's plaintext `(object_id, file_id, plaintext_start, len)`. Issue a bounded
   `ReadObjectRange(start, end)` for ONLY those covering bytes and pump them into the ranged extract
   (`extract-stream` ranged mode). Do NOT reimplement the plaintext→ciphertext mapping in Python — call the
   Rust query. N independent ranged opens per member (each reads only its covering range) → O(object) tape
   bytes for a bundle, not O(N×object). Fall back to the existing whole-object path for a whole-object
   (non-member) restore or if the covering-range query is unavailable (degrade — do not fail).
2. Keep RM3.0's two-phase mount-grace / streaming-inactivity clock + fail-fast on mount error UNCHANGED,
   and keep the bounded-RSS/duplex-pump behavior (the ranged feed is smaller, never larger).

## Binding invariants
- The plaintext→ciphertext mapping stays in Rust (the covering-range query is the single source of truth).
- RM3.0's timeout behavior + bounded RSS UNCHANGED (its tests stay green — this is why the stale patch was
  rejected). Whole-object + buffered paths still work. Per-chunk AEAD auth intact (the ranged extract
  authenticates). No proto change. Disk-tier restores unaffected.

## Tests (verification member — REQUIRED, non-vacuous, no skip)
- A member extract from an AEAD object reads only its covering stored range (assert the `ReadObjectRange`
  bounds ≈ the covering range, NOT the whole object) and returns byte-identical plaintext.
- An N-member encrypted bundle reads ≈ Σ member covering ranges (O(object)), not N×object — a test that
  would fail under the whole-object path.
- **ALL existing RM3.0 tests stay green** (`test_mount_error_fails_fast_without_waiting_for_grace`,
  `test_large_duplex_restore_has_bounded_rss_and_does_not_deadlock`) + the encrypted-restore plumbing test
  (update its expected args to the ranged args if the extractor invocation changed — the assertion must
  still pin the key-epoch + ranged args non-vacuously).
- Whole-object AEAD restore + the buffered path unchanged.

## Definition of done
`uv run pytest -q` green (paste tallies), `uv run ruff format --check`/`ruff check` + `uv run mypy` clean
on touched files. Summary: files touched; each test → scope item; explicit statement that (a) the mapping
stays in Rust (covering-range query), (b) N ranged opens per member → O(object), (c) RM3.0's timeout +
bounded RSS are unchanged (tests green), (d) the whole-object path still works, (e) physical acceptance is
deferred to the MSL3040 window. Build FRESH against current main — do NOT reuse the stale patch.
