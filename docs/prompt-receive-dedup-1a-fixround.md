# Prompt: receive-dedup 1a fix round — diff-gate findings (2026-07-10)

**Status: pending (dispatch at codex quota reset).** The 1a diff gate (33-agent
workflow review of 6cf48d8^..HEAD against the frozen design + contract) confirmed
10 findings. Finding 1 (CARD_ID_PATTERN rejecting `volume:` ids) is already fixed
on main with a regression test. Fix the remaining nine, in this order. The design
`docs/design-receive-dedup.md` and contract `docs/contract-receive-dedup-phase1.md`
remain normative; the design's "never hard-block a retry" promise binds several of
these fixes.

## Correctness (must fix)

2. **Lease/intent heartbeat starves on long single files** (grpc/servicer.py:335):
   renewal happens only per COMMITTED file receipt, so one >30-min file (180 GB
   event tape) loses the lease mid-transfer; a concurrent same-card request then
   marks the live intent failed and starts the exact duplicate receive the lease
   exists to prevent, and the registered intake projects "failed" forever.
   → Renew on streamed activity: per received chunk batch or a periodic renewal
   tick while the upload stream is live (floor-timer throttled), not per commit.
   Test: simulated transfer exceeding TTL with zero committed receipts keeps its
   lease; concurrent same-card request gets source_busy throughout.
3. **StreamClosed/RuntimeError ack-wait path strands the lease**
   (api/routes_devices.py:376): unlike sibling error paths it raises 409 without
   `_fail_device_intent`, leaving the intent authorized + lease held for the full
   TTL. → Terminalize + release exactly like DeviceOffline/CardUnavailable paths.
   Test: stream drop between dispatch and ack → immediate same-card retry works.
4. **Correlation failure after 'started' has no cleanup** (api/store.py:379):
   `fail_device_receive_intent` only acts on status=='authorized'. → Handle
   'started' too (terminalize + release lease) so correlation_failed doesn't lock
   the card for the TTL. Test included.
5. **Stale same-key replay is permanently terminal** (api/store.py:801): old
   machinery re-claimed a stale in_progress record so the same stored key
   restarted a stalled receive; now stale authorized/started → failed + terminal
   forever, violating "never hard-block a retry" for untouched clients replaying
   their stored key. → On stale intent + IDENTICAL request_hash: reclaim (fresh
   lease, back to authorized, relay restarts). Different hash stays conflict.
6. **StartIntake stale-expiry writes silently rolled back** (grpc/servicer.py:88):
   the `_abort(FAILED_PRECONDITION)` fires inside the same `factory.begin()`
   transaction that wrote status='failed'/lease-release, undoing them. → Commit
   the expiry side effects in their own transaction before aborting the RPC.
7. **Migration backfill fakes liveness** (alembic d4e5f6a7b8c9:77): every migrated
   row gets `last_heartbeat = CURRENT_TIMESTAMP`, so long-dead streaming intents
   look live to `reconcile_device_receive_leases` at first startup. → Backfill
   heartbeat from the row's own updated_at (or terminalize rows with no live
   grpc/catalog evidence). Migration up/down test covers a dead-intent row.
8. **peek_device_receive_intent omits terminal verdicts** (api/store.py:728): a
   same-key replay after a terminal attempt with the card ejected 409s
   device_unavailable instead of receive_terminal — the exact verdict-precedes-
   card-resolution class the peek exists for. → Add
   aborted/quarantined/failed → ("terminal", terminal_state) to the peek and
   handle it in the route before card resolution. Test with ejected card.

## Contract/efficiency

9. **'revoked' history state unreachable** (api/receive_history.py:173): no state
   maps to it and the `!= "revoked"` filter is dead. → Keep the enum value as
   reserved vocabulary, delete the dead filter, and note in the contract §4 that
   `revoked` awaits a durable revocation source (phase 2+).
10. **GET /api/devices does per-poll landing-file I/O** (api/routes_devices.py:140):
    `latest_card_history` → `intake_receipt_summary` re-reads and JSON-parses the
    receipts ledger for historical intakes on EVERY 30s poll per mounted card.
    → Memoize per (intake, terminal) — terminal summaries are immutable — or
    precompute into the projection at terminalization. Bound memory.

Optional cleanups if cheap while there: session-lock held across file I/O in the
projection path; legacy-status fallthrough in `_attempt_state`; duplicated
helpers between store.py and receive_history.py; unused telemetry API surface.

## Definition of done (AGENTS.md applies)

Full suite green (855+ baseline) plus the named new tests; migration up/down
verified; no proto/agent changes. The fix-round diff gets its own gate before
scenario RDD dispatches.
