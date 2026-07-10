# Prompt: receive-dedup phase 1a — server (sutradhara)

**Status: pending.** Design: `docs/design-receive-dedup.md` (FROZEN 2026-07-10) —
§P0, §P0 handshake (rev 2), §Invariants, §Two verdicts, §Contract notes, §Build
order phase 1a are normative. Wire shapes: **read
`docs/contract-receive-dedup-phase1.md`; it is normative.** Do not restate either —
implement them.

## Scope (phase 1a only)

1. **Receive-intent state machine** (contract §1) — durable, with the base-body-hash
   idempotency rule. Extend the existing intent-key machinery; do not build a
   parallel one.
2. **P0 handshake** on `/api/devices/{device}/receive` (contract §2): CardSnapshot
   identity resolution, atomic history recheck, 409 + stored `duplicateWarning`,
   `acknowledge_duplicate` transition, telemetry events.
3. **`StartIntake` linkage** (contract §3) — proto UNCHANGED; `FAILED_PRECONDITION`
   when no authorized intent.
4. **History projection** (contract §4) with the five states, precedence, authz
   scoping; "verified" from durable catalog state.
5. **Source lease** (contract §5) with renewal-on-receipt + floor timer and
   intent-based restart reconciliation.
6. **Identity fields** (contract §6): migration for durable indexed identity on
   `Intake` (copied at registration), interim `grpc_intake(card_id)` index, ingress
   validation/bounding of `card_id`/`label`.
7. **`archive_state`** additive read-model enum + `archiveSemantics: 2` (contract
   §7) — ALL-semantics computation (indexed anti-join over nonempty distinct
   relevant hashes). **The legacy `archived` boolean and its ANY predicate are NOT
   touched in this prompt** (that is phase 1c).
8. **`receivedBefore`** on `/api/devices` card entries (contract §8).
9. **Completed-intent lock terminal-state fix**: the HTTP intent unlocks on
   abort/quarantine/discrepancy (linked to intake terminal state); verified repeats
   remain overridable via the handshake — retry after failure must never be trapped.

## Hard constraints

- gRPC proto unchanged; agent (~/sutra-agent) untouched — phase 1 is
  agent-independent.
- Invariants I1–I4 in the design bind every implementation choice here.
- `/api/receive` (server sources) behavior unchanged except where the shared intent
  machinery refactor requires mechanical adaptation.
- No novelty/disposition columns in this prompt (phase 2).

## Definition of done (AGENTS.md applies)

- Alembic migration(s); all existing tests green; new hermetic tests covering: the
  state machine (warned/authorized replays, other-body conflict), 409 + ack flow,
  FAILED_PRECONDITION on unlinked StartIntake, projection precedence (verified vs
  most-recent-failed), lease exclusion + restart reconciliation from intents,
  archive_state none/partial/complete incl. empty-intake, intent unlock on abort.
- `scenario_dr` fixture updates per contract §10 — deliberate, in the same change.

## Verification member

Harness scenario prompt `~/system/docs/prompt-scenario-receive-dedup.md` (cut with
this set) — scenario RDD must go green against this implementation from a clean
slate before this prompt is archived implemented.

## Diff gate note

The independent diff review MUST re-verify the P0 handshake section specifically
against the design (it absorbed two verify rounds; residual risk is concentrated
there — see `panel-receive-dedup-2026-07-10.md` cap note).
