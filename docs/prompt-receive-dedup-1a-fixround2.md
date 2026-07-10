# Prompt: receive-dedup 1a fix round 2 — gate findings on f6479eb (2026-07-10)

**Status: implemented 2026-07-10; gate pending.** The fix-round diff gate (22-agent review) confirmed 12
distinct defects, clustered on the stale-reclaim path that fix-round-1 introduced.
**Design decision (Claude, recorded): the in-place reclaim is REVERTED.** Stale
authorized/started intents with an identical-hash replay go back to the simpler
pre-fix semantics — terminalize as `failed` and answer **409 `receive_terminal`
with a new `retryable: true` field** in the error body. The "never hard-block a
retry" promise moves up a layer: the client mints a fresh idempotency key and
re-runs the full handshake, which structurally re-runs the duplicate-history gate
(the hidden guarantee the reclaim broke — gate finding: an already-archived card
could silently re-receive). This reversal eliminates the orphaned-intake,
history-gate-skip, and StartIntake-first-ordering defects in one move.

Design/contract remain normative (`docs/design-receive-dedup.md`,
`docs/contract-receive-dedup-phase1.md`); this prompt adds `retryable` to the
contract §1 replay semantics — update the contract file accordingly.

## Fixes, in order

1. **Revert the reclaim** (store.py ~801-840): remove the reclaim branch entirely;
   stale authorized/started + identical hash → terminalize `failed` (durable),
   release lease, return `terminal` with `terminal_state="failed"`. The route's
   `receive_terminal` 409 body gains `"retryable": true`. Never null `intake_id`
   on any path. Tests: identical-hash stale replay → 409 receive_terminal
   retryable; different hash → conflict; a subsequent FRESH-key request for the
   same card runs the full history gate (assert the duplicate warning fires when
   history exists).
2. **Never terminalize a live started receive** (routes_devices.py ~376-390): the
   RuntimeError/StreamClosed ack-wait handler must first check the intent state —
   if StartIntake has already claimed `started` (or the grpc intake row is live),
   do NOT fail the intent or release the lease; return 409 `already_in_progress`.
   Only fail intents still in `authorized`. Test: stream drop after StartIntake
   claim → intent stays started, lease held, upload continues to registration.
3. **Leaseless streams must abort, not continue** (servicer.py ~335-365 +
   store.py renew): when renewal discovers the SourceClaim is gone or owned by a
   different intent, abort the upload stream (fail the intake with a distinct
   status detail) — the current lease holder wins. A resumed >TTL-gap stream must
   never continue unprotected. Test included.
4. **Orphan reconciliation** (new, small): a reconcile pass (piggyback on
   `reconcile_device_receive_leases`) that terminalizes `grpc_intake` rows in
   streaming/committing whose linked intent is terminal or absent AND whose
   stream/landing shows no activity beyond TTL — bounded batch, logged. Cures
   phantom "verifying" history rows. Test: orphaned streaming row → terminalized
   → history projects `failed`, duplicate warnings stop citing it.
5. **Peek verdict completeness + order** (store.py ~715-735): stored-response
   replay branch precedes the stale-skip (a stale started intent WITH a stored
   response replays it, per the docstring); `committed`-without-response returns
   the terminal verdict. Tests for both corners with the card ejected.
6. **Receipt-summary cache robustness** (grpc/status.py ~80): never cache a
   failed/None read (retry next poll); cache a bounded summary (file COUNT +
   bytes, not the relpath frozenset) so 1024 entries cannot hold unbounded path
   sets. Adjust the projection to consume the bounded summary.
7. **Guard per-chunk renewal** (servicer.py ~361): a transient DB error in
   `_renew_lease_on_activity` must not abort the upload — log, continue; the
   floor timer retries on the next chunk. (Distinct from fix 3: claim LOST →
   abort; renewal ERRORED → tolerate.)
8. Low (include if cheap): busy pre-check ordering + duplicated lease predicate
   (the two sub-cap cleanups from the gate).

## Definition of done (AGENTS.md applies)

Full suite green (863 baseline) + the named tests; contract §1 updated with
`retryable`; no proto/agent changes. This diff gets its own gate; scenario RDD
holds until it passes.
