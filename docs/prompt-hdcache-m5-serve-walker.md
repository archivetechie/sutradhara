# Codex prompt — hdcache M5: parallel serve orchestration + walker + rebuild — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`.**
> Read `CLAUDE.md` + `AGENTS.md` first. **Authoritative design:
> `docs/design-hd-disk-tier.md` §6.3–6.4, §8.2, §10 — read in full.**
> Depends on: M1–M4. Consumed by: M6.
>
> **What this is.** The performance and disk-health slice: the parallel restore
> orchestrator (stream pool, windowed wake-ahead, deadlines, liveness + circuit breaker,
> AEAD cap), the hardened walker, and `rebuild`. NO repop planner / drain / alarms-wiring
> (M6 — but this milestone EMITS the events M6 wires up).

## What already exists — BUILD ON IT
- M4 request model + single-stream serve (this milestone parallelizes it in place).
- M1 `verify_disk_identity`, store enumerate; M3 sweep (walker feeds it lost entries).

## Build order

### A. Orchestrator (design §6.4)
- Bounded stream pool (default 24; config) sized guidance `min(NIC, destination absorb)`;
  **separate AEAD stream cap** (default 4; file-staged via scratch — design §6.3).
- **Windowed wake-ahead**: rolling ~2× pool size in planned stream order (config window;
  on/off knob retained); item state `waking_disk` while pending spin-up.
- **Per-stream read deadline** (config, ~2× the 35s spin-up budget): timeout ⇒ read
  failure ⇒ tape fallback for that asset + breaker feed. Per-asset fallback never blocks
  the set.

### B. Disk liveness + circuit breaker (design §6.3)
Before any `lost` mark: statfs + sentinel liveness; failure ⇒ disk → `absent` + event, no
entry-state change, tape fallback. Breaker: N failures/timeouts per disk in a window
(config) ⇒ disk-level event, stop per-entry lost-marking, subsequent hits on that disk go
straight to fallback until the disk recovers or an operator acts.

### C. Walker (`hdcache/walker.py`, design §8.2 — the table IS the spec)
Per-disk, one at a time. Destructive mode gated on the M1 identity check; read-only +
alarm-event otherwise; deletions confined to `hdcache/v1`. Implement every row of the §8.2
table including: **unknown-file tripwire** (threshold ⇒ halt disk + "run rebuild?" event);
tmp GC age+liveness guard; `filling` young/live-job skip; `filling`+final-file-present ⇒
verify size, flip present. Corrects `filled_bytes`, refreshes slot/enclosure + SMART via
provisioner port. Optional sampled re-hash knob, off by default.

### D. Rebuild (`sutra hdcache rebuild`, design §10)
Sequential per-disk walk (one spin-up at a time), per-disk progress output (k/N, entries,
elapsed); inserts **untrusted** rows only after the catalog cross-check (asset exists,
cacheable, size/representation/bundle match — failures deleted from disk? NO: failures are
left untouched on disk and reported; only rows are withheld — file deletion is the
walker's job under its tripwire rules); recovers `artifactclass`/`bundle_key`/`group_key`
from the catalog. Promotion beyond cross-check: verification sweep (walker machinery, low
priority) hash-verifies untrusted entries and promotes; inline verify-then-serve promotion
already landed in M4 — keep them consistent.

## Must-be-exact
- "Never mark lost from absence alone" — enforced at every call site that can mark `lost`.
- Walker halts, never guesses, on identity mismatch (foreign/mis-mounted disk).
- Events emitted with stable reason codes (M6 wires them to the gap board): fallback
  reasons, breaker trips, tripwire halts, identity mismatches, reserve breaches.

## Definition of done
- `uv run pytest -q` green. Tests: pool respects caps (incl. AEAD sub-cap) under a mixed
  50-asset request; wake window ordering; deadline ⇒ fallback + breaker; flap simulation
  (mount vanishes: entries NOT lost, disk absent, storm prevented); walker table matrix
  row-by-row incl. tripwire halt + GC races (fill in flight); rebuild on fake disks:
  spoofed sentinel/foreign hash rejected, untrusted→promoted flow, class/bundle recovery;
  event emission with reason codes.
- Covers: harness scenario disk-death + rebuild legs —
  `~/system/docs/prompt-hdcache-harness-scenario.md`.
- `docs/INDEX.md` + journal per AGENTS.md.
