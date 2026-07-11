# Prompt RM1.3 — durable CommitRestore CAS + resume re-drive + two-lifecycle reconciliation

**Status:** pending (gpt-5.6-sol). Final RM1 milestone. Builds on RM1.1 (tables/states, landed @ba04c6e),
RM1.2a (OpenRestore streaming + CommitRestore/resume STUBS + the SQL-CAS open lease, landed @a2301ff),
RM1.2b (cache-first source-selection, landed @b1ad4e8).
**Normative (read FIRST, binding — do NOT inline):** `docs/design-restore-agent-protocol-v0.1.md`
**§7.5 RM1.3** (exit criteria), **§7.2** (the CommitRestore-CAS major + the two-lifecycle major — the job
engine can't hold "server done, restore awaiting remote durability"), **§7.4** (idempotency: REVEALED
terminal + "already done" on resume), **§3.4** (CommitRestore), **§3.7** (resume protocol), **§7.1-B6**
(the lease). **Read the real code you extend (verify, cite file:line):**
- `src/sutradhara/grpc/restore_service.py` — RM1.2a: `CommitRestore` STUB ~173-179 (UNIMPLEMENTED); the
  resume STUB in `_prepare_open` ~243-244 (UNIMPLEMENTED); the SQL-CAS open-lease `_acquire_lease`
  (the CAS PATTERN to mirror); the frozen `manifest_sha256`; `_stream`; the `sent` transition (`_mark_sent`).
- `src/sutradhara/grpc/store.py` — `compare_and_set_state` ~324 is the **NON-durable anti-pattern**
  (ORM load/check/set + process-local lock, `servicer.py`); do NOT use it for the commit — use a true
  `UPDATE…WHERE` row-count-arbitrated CAS like `_acquire_lease` already does.
- `src/sutradhara/hdcache/models.py` — `RestoreItemCheckpoint` ~244 (`committed_index` 0..2147483647,
  `revealed` bool, CHECK `revealed=false OR committed_index>=1`, unique per item, FK CASCADE);
  `RestoreOpenSession` ~278 (generation, expires).
- `src/sutradhara/hdcache/manager.py` — `ITEM_SENT`/`ITEM_DONE` ~100; `_update_request_state` ~2050
  (already treats `sent` as active); `serve_restore_item` (server_local UNCHANGED).
- `src/sutradhara/jobs/reconcilers/` (hdcache.py, spine.py, registry.py — the reconcile-timer spine where
  lease-expiry reopen reconciliation registers) and `proto/restore.proto` (CommitRestoreRequest/Reply,
  ResumeToken, the committed_index-divergence header contract — already authored, do NOT change the wire).

## Scope
1. **Durable CommitRestore CAS (§3.4, §7.2).** Implement the RPC as a true durable compare-and-set on
   `RestoreItemCheckpoint` — `UPDATE restore_item_checkpoint SET committed_index=:n[, revealed=true]
   WHERE restore_request_item_id=:id AND <preconditions>` arbitrated by **row-count** (mirror
   `_acquire_lease`), NEVER `compare_and_set_state`. Preconditions enforced atomically:
   - `manifest_sha256` == the SERVER-FROZEN digest for the item (mismatch ⇒ `FAILED_PRECONDITION`, the
     plan changed — the client must restart clean);
   - the `lease_token` generation matches the live `RestoreOpenSession` (a stale/superseded lease ⇒ reject);
   - the item's `receiver_device_id` == the calling mTLS device;
   - **monotonic `committed_index`** — `STAGED` may only ADVANCE it (a lower/equal index is a no-op-OK
     idempotent replay, never a regression that loses progress);
   `STAGED` advances the checkpoint's `committed_index` (durable staged progress). `REVEALED` sets
   `revealed=true` and transitions the item `sent → done`.
2. **REVEALED is terminal + idempotent (§7.4).** `CommitRestore(REVEALED)` is terminal regardless of any
   intermediate STAGED; a REPLAYED REVEALED (lost ack) returns success with `revealed=true` and does NOT
   double-transition or error ("already done"). Specify + implement the replay rules (idempotent on
   `(item, manifest_sha256, revealed)`).
3. **Two-lifecycle model (§7.2 — the hard one).** Separate **the delivery attempt** (the OpenRestore
   stream/`sent` transition — may terminally succeed) from **the restore ITEM** (stays ACTIVE until an
   idempotent REVEALED commit). The OpenRestore stream reaching `sent` is NOT the item's completion — only
   a durable REVEALED → `done`. Ensure `_update_request_state`/console aggregation renders a `sent`
   (uncommitted) item as in-progress, and the request completes only when all items are `done` (revealed).
   The terminal job/stream must not force the item terminal.
4. **Resume re-drive (§3.7).** Implement the OpenRestore resume path (replace the `_prepare_open`
   UNIMPLEMENTED at ~243). On `OpenRestore` with a `resume_token`:
   - validate `resume_token.manifest_sha256` against the current plan's frozen digest (mismatch ⇒ refuse,
     the client restarts clean);
   - re-acquire/verify the open-session lease (generation);
   - **if the item is already `revealed` ⇒ return "already done"** (a terminal marker frame / a clean
     `job_end` with no data, per the proto) — NO re-drive, NO double-reveal;
   - else re-drive the `RestorePlan` (via RM1.2b's cache-first `_prepare_open` selection) from
     `committed_index` at **FILE BOUNDARY**: skip the first `committed_index` fully-staged files; re-stream
     the in-progress file FROM ITS START (per the committed_index-divergence contract — the client
     verifies-on-receive, skips already-durable data, truncates the in-progress file). `committed_index`
     read from the checkpoint row.
5. **Lease-expiry reopen reconciliation (§7.2).** Add a reconciler (register on the reconcile-timer spine)
   that finds `sent`-but-not-`revealed` items whose `RestoreOpenSession` lease has EXPIRED and makes them
   reopenable (so a partitioned/abandoned agent doesn't strand the item) — a fresh `OpenRestore` can then
   acquire a new-generation lease and resume. The item is NOT falsely completed and NOT locally written.

## Binding invariants
- Durable **row-count CAS** (like `_acquire_lease`), NEVER `compare_and_set_state`. Monotonic
  `committed_index`. Server-frozen `manifest_sha256` match on every commit/resume. **Idempotent REVEALED**
  replay + "already done" on resume. **Two-lifecycle:** a `sent` item is NOT `done`; only a durable
  REVEALED commit → `done`; the console never shows a false completion. Resume is FILE-BOUNDARY and uses
  the committed_index-divergence contract (never mid-file). `server_local` restore byte-for-byte UNCHANGED.
  No agent path ever writes locally. No runtime compat flag. Do NOT change the proto wire (RM1.2a authored
  it). Do NOT touch the auth gate, the source selection (RM1.2b), or the frame protocol beyond adding the
  resume/commit behavior.

## Tests (verification member — REQUIRED, non-vacuous, no skip)
- **CommitRestore(STAGED)** advances `committed_index` durably; a lower index is idempotent-no-op (never
  regresses); a manifest mismatch → rejected; a stale/wrong-generation lease → rejected; a wrong receiver
  → rejected; two concurrent commits race safely (row-count CAS, no lost update).
- **CommitRestore(REVEALED)** → item `done`; a REPLAYED REVEALED → success "already done", no double
  transition, no error.
- **Two-lifecycle:** an item whose OpenRestore stream reached `sent` but has NO commit renders in-progress
  (NOT `done`, NOT `completed_with_errors`); the request completes only after every item is REVEALED→`done`.
- **Resume:** stream, `CommitRestore(STAGED)` after K files, KILL/close, then `OpenRestore(resume_token)`
  → re-drives from `committed_index` (skips the K staged files, re-streams file K from its start),
  reassembled result identical, final REVEALED → `done`. An already-`revealed` item resume → "already done"
  (no re-drive). A resume with a mismatched `manifest_sha256` → refused.
- **Lease-expiry reconciliation:** a `sent`-uncommitted item with an expired lease → reconciled to
  reopenable; a fresh OpenRestore acquires a new-generation lease and resumes; no false completion, no
  local write.
- **server_local + all RM1.1/RM1.2 tests still green.**

## Definition of done (this repo's AGENTS.md)
`uv run pytest -q` green (paste tallies), `uv run ruff format --check`/`ruff check` + `uv run mypy` clean
on touched files. Summary: files touched; each test → the scope item it covers; explicit statement that
(a) CommitRestore uses a durable row-count CAS (not `compare_and_set_state`), (b) REVEALED is idempotent +
terminal, (c) a `sent` item is not `done` (two-lifecycle), (d) resume is file-boundary via the
committed_index-divergence contract, (e) lease-expiry items are reconciled reopenable, (f) server_local is
unchanged and no agent path writes locally. RM1.3 completes RM1 — after this, the server restore-agent
protocol is fully durable/resumable; the RM2 Rust client (separate repo) consumes it.
