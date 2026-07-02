# Codex prompt — activity read-model API (sutradhara)

> Status: **pending** — **dependency root** for the graphite console redesign
> (`~/system-ui/docs/prompt-console-graphite-redesign-system-ui.md` consumes this;
> build this first). **Contract:** read
> `~/system-ui/docs/contract-activity-api.md` — **it is normative**; do not restate
> it here or diverge from it. Design rationale:
> `~/system-ui/docs/superpowers/specs/2026-07-02-console-graphite-redesign-design.md`
> §9. The endpoint lives on the existing operator-identity FastAPI app
> (`design-operator-console-relay.md` §3.3 siblings: `/api/session`,
> `/api/devices`, `/api/intake/{id}/status`).

Per `AGENTS.md`: run `uv run pytest -q` (the **full suite**) and paste output at every
milestone; commit at every green milestone; update `docs/INDEX.md` on completion.

## Scope

**In:** the `GET /api/activity` route + its read-model query. **Out:** any UI (sibling
prompt), any schema/state additions beyond a read-model (if a contract field has no
durable source, it is `null` — never invent storage this pass), any change to existing
endpoints.

## Sourcing guidance (verify against the actual store; the contract's nullability is
the escape hatch)

- `grpc_intake` already carries operator, `device_id`, `card_id`, status, and
  timestamps; the receive `label` and `artifactclass` are intake metadata.
- `sourceLabel`: card label where known (the relay's card correlation), else the
  server-source label, else fall back to `card_id`/`source_ref` — a stable, opaque,
  human-readable string; never a path.
- `operator`: display name as the session layer resolves it; if only the username is
  stored, return the username (it is the stable id — do not join to Authentik).
- Byte counters (`bytesTotal`/`bytesReceived`, `bytesVerifiedToday`): from the receipt
  ledger **where durably recorded**; else `null`. State in your summary which fields
  came out null and why.
- `openDiscrepancies`: `quarantined | discrepancy` rows in the window without a
  recorded resolution (if no resolution concept exists yet, all such rows in the
  window count — say so).

## Milestones (TDD — each ends green: `uv run pytest -q` + commit)

1. **Read-model query** (store layer): window filter on `startedAt` (`days` 1..30,
   server-local day boundaries), newest-first ordering, 200-row cap, and the three
   summary aggregates. Tests: rows materialize with the contract's fields; day
   boundary correctness; the cap; aggregate correctness including the
   `bytesVerifiedToday: null` path; `openDiscrepancies` counts only terminal-bad in
   window.
2. **HTTP route** on the operator-identity app: `can_view` required (403 without a
   role; the existing capability machinery — no new authz logic), `days` validation
   (400 outside 1..30, default 7), camelCase response exactly per the contract,
   standard `{error, detail}` errors. Tests: authz matrix (viewer 200 / groupless
   403), shape golden test, param validation.
3. **Cross-operator visibility:** a viewer session sees intakes started by two
   different operators in one response (the read-model is not operator-scoped).
   Test proves it.
4. **Regression:** full suite green; `docs/INDEX.md` row added; commit.

## Verification member (process §5)

No harness scenario covers the console HTTP surface; **pytest is the verification
member for this prompt** (same posture as the existing `/api/devices` +
`/api/intake/{id}/status` routes). The consuming UI prompt carries the browser-QA
gate for the end-to-end path.

## Definition of done

`GET /api/activity` serves the contract exactly (shape, auth, validation, caps,
honest nulls) from existing durable state only; full suite green; INDEX updated.
