# Prompt RM1.2b — bounded hdcache producer + unified cache-first source-selection (B4)

**Status:** implemented 2026-07-12 (gpt-5.6-sol). RM1.2 part b. Builds on RM1.2a (landed sutradhara main @a2301ff:
OpenRestore streaming over archive-backed items) + RM0. **This is the SECOND safety funnel of RM1.2:
source-SELECTION. Additive — it deepens where OpenRestore gets its bytes; it does NOT change the proto,
the auth gate, the lease, the frame protocol, the frozen-manifest rule, or `sent`-not-`done`.**
**Normative (read FIRST, binding — do NOT inline):** `docs/design-restore-agent-protocol-v0.1.md`
**§7.1-B4** (the disk tier is hdcache, which RestorePlan does NOT represent; need a bounded verified
hdcache producer + unified cache-first-then-archive selection reproducing `restore_asset`'s candidate
fallback + SUSPECT loop — NOT just opening the first member) and **§7.5 RM1.2** (the bounded verified
hdcache producer; reproduce archive candidate fallback; tape excluded/rejected until RM3);
`docs/design-hd-disk-tier.md` (the cache tier: private footage AEAD-only at rest, digest-verified on
every serve, `resolve_read_source` = access gate ABOVE cache/tape branch, cache failure degrades to
archive). **Read the real code you extend (verify, cite file:line):**
- `src/sutradhara/grpc/restore_service.py` (RM1.2a — `_prepare_open` ~221-289 calls `build_restore_plan`
  ~270 then `_first_matching_member` ~284 = OPEN-FIRST, the gap; the frozen manifest, auth gate, lease,
  `_stream`, `sent` transition all STAY).
- `src/sutradhara/archive_restore.py` (`restore_asset` ~668-736 = THE candidate-fallback + SUSPECT loop
  to reproduce: `plan.iter_members()` → per-member try `open_member_stream` → on
  `LogicalMemberIntegrityError` mark `member.copy.health = CopyHealth.SUSPECT` + continue → next
  candidate; `build_restore_plan` ~509; `PlannedMember` ~111; `RestoreSourceUnavailable`/
  `RestoreIntegrityError`).
- `src/sutradhara/hdcache/manager.py` (`resolve_read_source` ~419 = cache-first access gate;
  `_select_cache_entry` ~1603 = get `CacheEntry` for an asset_hash; `_serve_from_cache` ~1098 +
  `_publish_cache_plaintext` ~1817 = the cache read+verify path, but it PUBLISHES TO A FILE — you need a
  STREAMING producer; `_cache_key_epoch` ~1821 + the AEAD-at-rest open for private footage; `CacheDisk`/
  `CacheEntry` liveness/health).
- `src/sutradhara/hdcache/models.py` (`CacheEntry`, `CacheDisk`).

## Scope
1. **Bounded verified hdcache plaintext PRODUCER (B4).** Add a producer — analogous to RM0's
   `open_member_stream`/`open_range_chunks` — that, given a healthy `CacheEntry` (+ the expected
   `content_sha256`, size, and artifactclass privacy), yields **bounded, digest-verified plaintext
   chunks** from the cache disk: AEAD-decrypt for private footage (reuse the EXISTING cache AEAD open /
   `_cache_key_epoch` key-epoch logic — do NOT reimplement crypto), verify the streamed plaintext against
   `content_sha256` + size (a cache read is NEVER less verified than an archival restore — hd-disk-tier
   invariant), bounded memory (no whole-object buffer). Factor the verify/AEAD out of `_serve_from_cache`
   (which publishes to a file) so BOTH the file-publish path and this stream share one verified reader —
   do NOT fork a second, less-verified cache read.
2. **Unified cache-first source-selection in `_prepare_open`.** Replace the open-first
   `_first_matching_member` with a selector that:
   - **(a) hdcache FIRST:** `_select_cache_entry(asset_hash)`; if a healthy CacheEntry exists on a live
     disk, stream via the new cache producer.
   - **(b) archive DISK candidates with FALLBACK + SUSPECT:** on cache miss OR a cache integrity failure
     (degrade-to-archive per hd-disk-tier), drive `build_restore_plan` and reproduce `restore_asset`'s
     loop — `iter_members()`, try each candidate, on `LogicalMemberIntegrityError` mark
     `copy.health = SUSPECT` + fall through to the next candidate; NOT `_first_matching_member`. Exhaustion
     → the same `RestoreSourceUnavailable`/`RestoreIntegrityError` semantics (aborted PRE-STREAM per
     RM1.2a's `_prepare_open` reconcile, so no frame is emitted for an unsatisfiable item).
   - **(c) TAPE EXCLUDED until RM3:** a tape-backed copy/pool is skipped/rejected as a candidate (RM1 is
     disk-only). If the ONLY sources are tape, abort cleanly (FAILED_PRECONDITION / a typed "tape source
     deferred to RM3") — do NOT open a tape read here.
   Because a candidate may fail integrity, selection that involves actually opening a stream must happen
   such that a failed candidate does NOT emit partial frames — either probe/verify the chosen source
   before `manifest_head`, or ensure the fallback loop runs entirely within `_prepare_open` before the
   frozen manifest + first frame (mirror RM1.2a's pre-stream reconcile).

## Binding invariants
- **SINGLE FUNNEL preserved:** OpenRestore still frames a bounded, verified plaintext chunk iterator; you
  are only widening the SOURCE of that iterator (cache producer OR the RM0 plan's member stream). Do NOT
  add a parallel restore/decode path; do NOT touch the auth gate, the SQL-CAS lease, the frozen manifest
  digest (it is source-INDEPENDENT — same digest whether served from cache or archive), the frame
  protocol, or `sent`-not-`done`. NEVER enter `serve_restore_item`/`_serve_from_cache`'s FILE publish /
  any server-local write — the agent stream is bytes-only.
- **server_local restore is byte-for-byte UNCHANGED** (golden baseline — existing restore suite is the
  oracle; the operator `restore_asset`/`resolve_read_source` file-publish path is untouched).
- Cache read ≥ archival verification (digest-verified, AEAD for private). Tape deferred to RM3. No runtime
  compat flag.

## Tests (verification member — REQUIRED, non-vacuous, no skip)
- **Cache-hit streams from cache:** a healthy CacheEntry for an item → OpenRestore streams verified
  plaintext from the cache producer (assert it read the cache, not the archive); plain + AEAD-at-rest
  (private) both; bounded memory (no whole-object buffer, as RM1.2a's memory assertion).
- **Cache-miss falls back to archive disk candidate** and streams verified.
- **SUSPECT fallback:** a primary archive DISK candidate that fails integrity → the loop marks its copy
  `SUSPECT` and streams from the NEXT healthy candidate; assert the SHA still matches and the SUSPECT mark
  persisted. (Reproduces `restore_asset`'s behavior — a test that would pass with open-first is vacuous.)
- **Cache integrity failure degrades to archive** (a corrupt/failed cache read → archive fallback, verified).
- **Tape-only asset is rejected** (no tape read opened; clean abort → RM3).
- **Manifest digest source-independent:** the frozen `manifest_sha256` is identical whether the item is
  served from cache or from the archive (reuse the RM1.2a golden/cross-frame assertions).
- **server_local restore suite still green;** all RM1.2a tests still green.

## Definition of done (this repo's AGENTS.md)
`uv run pytest -q` green (paste tallies), `uv run ruff format --check`/`ruff check` + `uv run mypy` clean
on touched files. Summary: files touched; each test → the scope item it covers; explicit statement that
(a) server_local + the operator file-publish path are unchanged, (b) the source selector reproduces
`restore_asset`'s candidate-fallback + SUSPECT loop (not open-first), (c) tape is excluded/deferred to
RM3, (d) the cache producer is digest-verified + bounded + shares the verified reader with the
file-publish path (no second less-verified cache read), (e) auth gate / lease / frozen manifest / frame
protocol / `sent`-not-`done` are unchanged. Do NOT implement durable commit/resume (RM1.3) or a tape leg
(RM3).
