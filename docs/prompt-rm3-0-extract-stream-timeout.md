# Prompt RM3.0 — extract-stream cold-mount timeout fix (two-phase clock; sutradhara-only; LIVE-PATH BUG)

**Status:** pending (gpt-5.6-sol). First RM3 item (pulled FIRST per the folded design — a CURRENT
live-path availability bug, sutradhara-only, no remanence/proto change). RM1+RM2 are COMPLETE.
**Normative (read FIRST, binding — do NOT inline):** `~/remanence/docs/design-restore-tape-leg-v0.1.md`
**§6.5** (the two-phase mount-grace / streaming-inactivity split — build to this; it explicitly REJECTS
the async `OperationRef` option and requires fail-fast on a library mount error) + §6 context.
**Read the real code you fix (verify, cite file:line):** `src/sutradhara/archive_restore.py` —
`_REM_STREAM_INACTIVITY_TIMEOUT_SECONDS = 120.0` (~63); the AEAD ciphertext-pump path
`_open_backend_range_chunks` (~1202); `pump_ciphertext` (~1284 — opens the backend range chunks =
the drive MOUNT + `space()` seek at ~1286, then writes chunks; the FIRST `_write_pipe_chunk` (~1299)
is the first `last_activity` stamp); `_iter_rem_plaintext` (the reader loop, ~1383-1414 — bumps
`last_activity` on plaintext reads ~1400/1407 and enforces the inactivity timeout at ~1411);
`last_activity` (~1281). `backend/remanence.py` `open_read_session` (the synchronous mount the pump
blocks on).

## The bug
`pump_ciphertext` performs the cold LTO drive **mount + locate** (via `_open_backend_range_chunks` →
`open_read_session`) BEFORE the first pipe byte, while `_iter_rem_plaintext`'s poll loop ticks the SAME
120 s inactivity clock (seeded at Popen). A cold LTO load + locate-to-far-filemark under robot contention
can exceed 120 s → the reader false-kills `rem archive extract-stream` before the first plaintext byte.
Every physical encrypted-tape restore is exposed.

## The fix (§6.5 — two clocks, NO proto/remanence change, NO async OperationRef)
1. **Add a MOUNT-PHASE grace** distinct from the streaming inactivity: a configurable
   `_REM_STREAM_MOUNT_GRACE_SECONDS` (env-overridable, e.g. `SUTRADHARA_REM_STREAM_MOUNT_GRACE_SECONDS`),
   default GENEROUS — cover worst-case LTO load + locate-to-far-filemark under robot contention (set the
   default from the MSL3040 leg's MEASURED mount+locate latencies, not a guess; document the basis in a
   comment). This grace applies from Popen until the backend session is open.
2. **Stamp the phase transition:** when `_open_backend_range_chunks(...)` RETURNS (the backend session is
   open = mount+seek DONE) in `pump_ciphertext`, record the transition to the streaming phase — set a
   shared `streaming_started` flag AND stamp `last_activity`. From that point the existing 120 s
   `_REM_STREAM_INACTIVITY_TIMEOUT_SECONDS` governs (unchanged).
3. **The reader picks the grace by phase:** in `_iter_rem_plaintext`'s timeout check (~1411), use
   `_REM_STREAM_MOUNT_GRACE_SECONDS` while `streaming_started` is False, else
   `_REM_STREAM_INACTIVITY_TIMEOUT_SECONDS`. The mount phase gets the generous grace; the streaming phase
   keeps the tight 120 s.
4. **Fail-fast on a mount ERROR (do NOT wait out the grace):** if `_open_backend_range_chunks`/
   `open_read_session` RAISES a library/mount error, it must surface to the reader promptly (via the
   existing `writer_errors` path) and abort — the reader must check `writer_errors` and raise immediately,
   NOT wait for the mount grace to elapse on a genuinely dead tape/library.

## Binding invariants
- **No proto change, no remanence change, no async OperationRef** (§6.5 rejects it). The plaintext/AEAD
  restore behavior is otherwise unchanged. The streaming-phase 120 s inactivity is unchanged. The
  buffered (non-streaming) extract path is untouched. Thread-safety: `streaming_started`/`last_activity`
  are shared between the pump thread and the reader — use the existing list-cell pattern (`last_activity`
  is already a `list[float]`), no new races.

## Tests (verification member — REQUIRED, non-vacuous, no skip)
- **Slow mount does NOT false-kill:** simulate `_open_backend_range_chunks` taking LONGER than the 120 s
  streaming inactivity but WITHIN the mount grace (use a fake backend that sleeps/blocks the session-open,
  with the grace/inactivity constants patched small so the test is fast) → the stream is NOT killed; once
  the session opens + bytes flow, it completes. This test would FAIL under the old single-clock behavior.
- **Streaming inactivity STILL fires:** after the session opens, a stall exceeding the 120 s streaming
  inactivity (patched small) → the stream IS killed (the tight clock still protects a hung stream).
- **Fail-fast on mount error:** `_open_backend_range_chunks`/`open_read_session` raising a mount/library
  error → the reader aborts PROMPTLY (well within the mount grace), surfacing the error — it does NOT wait
  out the grace.

## Definition of done (this repo's AGENTS.md)
`uv run pytest -q` green (paste tallies), `uv run ruff format --check`/`ruff check` + `uv run mypy` clean
on touched files. Summary: files touched; each test → the scope item; explicit statement that (a) no proto/
remanence change, (b) the streaming-phase 120 s inactivity is unchanged, (c) a slow mount no longer
false-kills, (d) a mount error fails fast (no grace wait), (e) the buffered extract path is untouched.
