# Codex prompt — `sutra intake watch`: server-side landing registrar

> Status: **implemented** (2026-06-30). Design: `docs/design-intake-watch.md` (the authority — read it
> first; this prompt sequences its §13 plan into green+commit milestones and pins the
> load-bearing invariants). Single repo (`sutra-agent` lives at
> `packages/sutra-agent/`). **Prerequisite for** `docs/design-streaming-intake-grpc.md`
> (the gRPC streaming intake is built on top of this — it writes no terminal markers of
> its own and relies on this watcher as the single registrar).

Per `AGENTS.md`: run `uv run pytest -q` (the **full suite**) and paste the output at
every milestone; commit at every green milestone (never leave the tree dirty); update
`docs/INDEX.md` on completion.

---

## Goal

A foreground, level-triggered server registrar: poll one landing root, discover
completed BagIt intakes (`intake.json` present, no `.receiving.json`, stable), and drive
the **existing** `register_intake` / `accept_intake` boundary once per candidate. It
adds **no** new validation, marker format, schema, or HTTP/RPC surface. The landing
filesystem is the durable queue.

## Scope

**In:** the marker-lifecycle refactor on `sutradhara.intake` (deferred + atomic +
post-commit publication, typed discrepancy exception); updating **every** existing
caller of `register_intake`/`accept_intake` to publish markers post-commit; the
`sutra-agent` / `wait_for_server_confirmation` marker-precedence + fail-closed reader;
the new `intake_watch.py` core + `sutra intake watch` CLI; tests.

**Out:** inotify/FSEvents (polling only); a daemon supervisor (foreground; systemd
later); deletion/retention; client-notification RPC; any new validation path or catalog
schema change; the gRPC streaming work (separate prompt).

---

## Load-bearing invariants (do not weaken — design §6, §11)

1. **A terminal marker present on disk ⟹ its registering transaction durably
   committed.** One-way, *not* a biconditional: committed-but-marker-missing is a legal,
   self-healing interim state (next scan re-registers → `already-registered` → republish).
2. **Deferred publication, never an internal commit.** `register_intake` /
   `accept_intake` are session-taking and the **caller owns the transaction**
   (`accept_intake` keeps `register_intake` + optional `prepare_intake` in one caller
   transaction). The helpers **return** the marker payload+path; the **caller publishes
   after its own commit**. Committing inside the helper would let an `accept --prepare`
   prepare-failure strand a registered intake + verified marker.
3. **Atomic marker writes.** Publish every marker temp-file → `fsync` → atomic `rename`
   (+ `fsync` the intake dir). The marker is the source-release signal; its filename must
   never be visible with partial JSON.
4. **Fail closed on read.** A `verified` marker that is unreadable or not a JSON object
   is **not** release-safe. Blocking markers (`quarantined`, `discrepancy`) outrank a
   stale `verified`. Marker **filename is authoritative**, never the JSON `status` field.
5. **Stability snapshot covers the full `data/` tree**, not just manifest-named files —
   `validate_bag` flags non-manifest files under `data/` as `extra` (invalid), so a
   transient extra file must register as a snapshot change, not "stable invalid".

---

## Work items (each ends green: `uv run pytest -q` + a commit)

### 1. `intake.py` — deferred, atomic, post-commit markers + typed discrepancy

Files: `src/sutradhara/intake.py` (+ its tests under `tests/`).

- Add `IntakeDiscrepancyError(ValueError)` that **carries the discrepancy marker payload
  + path** (and structured details for events). The discrepancy branches **raise it**
  instead of calling `_write_discrepancy_receipt`.
- Stop writing receipts inside `register_intake`/`accept_intake` (today
  `_write_verified_receipt`/`_write_quarantine_receipt`/`_write_discrepancy_receipt` run
  on `flush()`). Instead **return the marker payload + path** on `IntakeRegisterOutcome`
  (e.g. an `IntakeMarker { path, payload }` field). Do **not** commit inside the helper.
- `accept_intake` **rebuilds `outcome.marker` after `prepare_intake` succeeds** so the
  verified payload's `requested_profile` reflects the prepared profile (it is set on the
  intake *after* `register_intake` returns), not the raw register result.
- Add a marker-publish helper (e.g. `publish_intake_marker(marker)`) that writes
  **atomically** (temp → `fsync` → `rename` → dir `fsync`). Make `_write_json` atomic, or
  add a marker-specific atomic writer.
- Fix the discrepancy receipt payload so its `status` is **not** the misleading
  `"registered"` — use a `"discrepancy"`-class value carrying the registered-time
  details. (Filename precedence is the contract; the payload must not contradict it.)
- Tests: helpers **return** markers and write nothing on `flush()`; `publish_intake_marker`
  is atomic (no partial-JSON filename observable); `accept_intake` marker carries the
  prepared `requested_profile`; `IntakeDiscrepancyError` carries payload+path; discrepancy
  payload `status` is the corrected value.

### 2. Update **every** existing caller to publish markers post-commit

Files: `src/sutradhara/cli/intake.py`, `src/sutradhara/intake.py`
(`register_landing_root`/`accept_landing_root`), + their tests.

Deferring the marker out of the helpers means a caller that does not publish would commit
catalog rows with **no** terminal marker. Update all of them:

- `sutra intake register` and `sutra intake accept` CLIs: after the `session_scope`
  commit, call `publish_intake_marker(outcome.marker)` (and on `IntakeDiscrepancyError`,
  publish the carried marker after the transaction resolves, then surface the error).
- `register_landing_root` / `accept_landing_root`: collect each candidate's marker and
  publish them **after** the single batch commit (these batch in one caller transaction —
  per design they are *not* the watcher primitive, but they remain a public surface and
  must stay correct).
- Update existing tests that asserted markers appear as a side effect of the helper to
  assert the **caller** publishes them post-commit; add a regression test that a manual
  `sutra intake register`/`accept` writes the terminal marker after commit.

### 3. Edge-side marker precedence + fail-closed reader

Files: `packages/sutra-agent/src/sutra_agent/ledger.py`,
`packages/sutradhara-receive/src/sutradhara_receive/core.py`
(`wait_for_server_confirmation`), + their tests.

- `sutra-agent`: add a `discrepancy` confirmation status; make `intake.discrepancy.json`
  **release-blocking**; check discrepancy/quarantine **before** `verified`; key on the
  marker **filename**, never the JSON `status`.
- **Fail closed:** an `intake.verified.json` whose JSON is unreadable or not an object
  yields `release_ok=False` (today `_read_json_object` returns `None` but the verified
  branch still reports `release_ok=True`).
- `wait_for_server_confirmation` (`sutradhara_receive`): same precedence — a blocking
  marker outranks a coexisting `verified`; fail closed on an unreadable verified marker.
- Tests: discrepancy blocks release and outranks a coexisting verified; unreadable /
  non-object verified marker → blocked; precedence holds in both `sutra-agent` and
  `wait_for_server_confirmation`.

### 4. `intake_watch.py` — the watcher core

Files: `src/sutradhara/intake_watch.py` (new) + tests.

- Candidate iterator (design §5): landing root or immediate child with `intake.json`;
  **skip** `.receiving.json` (unconditional active-write), fresh `intake.json` younger
  than `--settle-seconds`, hidden cache/state dirs, and dirs with any terminal marker.
- **Stability snapshot over the full `data/` tree** (path/size/mtime/type for
  `intake.json`, BagIt tag files, manifest files, and **every file under `data/`**), with
  explicit `absent` entries for manifest-named-but-missing payload; require it unchanged
  across `--stable-polls` before any mutating call.
- Read-only `inspect_intake` preflight with bounded `--validation-attempts` (**the
  initial inspect is attempt #1**); route the five `InspectReport.status` values exactly
  per design §6 (ready/already-registered/quarantined → register; incomplete → retry/exit-0;
  stable invalid → terminal quarantine).
- Per-candidate **isolated DB session/transaction**; on success **commit, then publish**
  `outcome.marker`; catch `IntakeDiscrepancyError` → publish its carried marker
  post-boundary + emit `intake-discrepancy`; classify a generic `ValueError`/unexpected
  exception as `intake-error` (per-path in-memory backoff). Do **not** use
  `register_landing_root`/`accept_landing_root` (they cannot isolate one candidate's
  discrepancy from another's success).
- Best-effort `<cache-root>/intake-watch.lock` singleton; `WatchEvent` dataclass;
  one-shot `process_landing_once(...)`; loop `watch_landing(...)` with injectable
  sleep/stop predicate.
- Tests from design §12, including: post-commit marker durability (inject a commit
  failure → **no** marker left); deferred discrepancy marker via the exception path;
  transient extra `data/` file stays unstable (not quarantined); terminal-marker skip;
  failure isolation (candidate A discrepancy, candidate B still registers); discrepancy
  vs generic-error classification; one-shot exit codes (1 for terminal
  quarantine/discrepancy, 0 for active/unstable/incomplete); atomic-marker and
  fail-closed-reader assertions; loop stoppable via `max_iterations`/stop predicate.

### 5. `sutra intake watch` CLI

Files: `src/sutradhara/cli/intake.py` + tests.

- Add the command with the design §4 options: `--landing-root` (canonical; `--landing`
  alias), `--once`, `--interval` (5), `--settle-seconds` (2), `--stable-polls` (2),
  `--validation-attempts` (2, total incl. the first inspect, `>= 1`), `--artifactclass`
  (legacy-only), `--prepare PROFILE` (→ `accept_intake`, default off), `--cache-root` /
  `--cloud-backend` / `--cloud-pool` pass-through, `--json-lines`.
- Exit codes per design §4/§8: `0` clean/`come-back-later`; `1` one-shot wrote a
  quarantine/discrepancy or hit an error; `2` usage/config error.
- Plain + `--json-lines` event output (design §10 event names/records).
- Tests: agent-loop closure (`sutra-agent receive` → `watch --once` → `sutra-agent
  status` flips `pending`→`verified`); `--prepare` records the profile only for
  registered intakes; `--once` exit codes; lock contention emits a skipped/locked event.

### 6. INDEX + deployment note

- Flip `docs/design-intake-watch.md` status `for review` → `current` (implemented core),
  and this prompt's INDEX status `pending` → `implemented`.
- Add a short deployment note / example systemd unit (`--once` via timer, or
  `watch_landing` as a `Type=simple` foreground unit) after the CLI is proven.

---

## Definition of done

`uv run pytest -q` (the **full suite**, per `AGENTS.md`) green with the output pasted;
`sutra intake watch --landing-root <root> --once` discovers a completed receive bag,
registers it, and **publishes the verified marker only after the commit** (atomically);
a `sutra-agent receive` → `watch --once` → `sutra-agent status` loop flips
`pending`→`verified`; manual `sutra intake register`/`accept` still publish their markers
post-commit; blocking markers outrank a stale verified everywhere and unreadable verified
markers fail closed; the tree is committed; `docs/INDEX.md` updated. A cross-repo
`~/system` harness scenario exercising the live watch loop is a **follow-up** (note it in
INDEX), not part of this prompt.
