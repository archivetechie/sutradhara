# Prompt RM1.1 — restore agent-delivery: persisted protocol + admission foundations (NO streaming yet)

**Status:** pending (gpt-5.6-sol). First milestone of RM1 (restore agent-delivery protocol, server side).
**Normative (read FIRST, binding — do NOT inline):** `docs/design-restore-agent-protocol-v0.1.md`
**§7 (the v0.2 fold)** — §7.1 blockers, §7.2 majors, §7.3 minors, **§7.5 RM1.1 scope + exit criterion**;
and `docs/design-restore-agent-v0.1.md` §5 (service/agent split), §17 (security). §3-6 of the protocol
doc are the SUPERSEDED v0.1 — build to §7. **Read the real code you extend (verify, cite file:line):**
`hdcache/models.py` (RestoreRequest/RestoreRequestItem), `hdcache/manager.py` (admit_restore_request,
serve_restore_item ~703-838, destination_for_request_item/canonicalize_restore_destination ~942-985,
_update_request_state ~1838, RestoreDestination ~203), `api/routes_restore.py` (POST /api/ui/restores
admission + job submit ~120), `jobs/handlers/restore.py` + `jobs/engine.py` (terminal-only handlers),
`grpc/store.py` (GrpcDeviceEnrollment ~61-78 — device_id NOT unique; GrpcEnrollToken ~81-96;
issue/consume_enroll_token; record_device_enrollment; compare_and_set_state ~251), `grpc/ca.py`
(sign_device_csr ~185-269), `api/routes_devices.py` (mint_enroll_token/bundle/token endpoints,
can_receive gates ~493/505/615), `api/identity.py` (parse_identity ~35/50, GROUP_CAPABILITIES),
`catalog/session.py` (Alembic, NOT create_all ~88), `contract-hdcache-restore.md` (states ~146,
bytes_restored ~94).

**Scope: the persisted protocol + admission foundations. NO gRPC RestoreService, NO streaming (RM1.2).**

1. **Revise `contract-hdcache-restore.md` (same milestone — no silent semantic change, §7.2):** add
   `delivery_mode` (`server_local`|`agent`); add item states `sent`/`done`-via-agent + a `revealed`
   checkpoint flag (drop `committed` as a distinct in-tx state, §7.3); define `bytes_restored` for agent
   mode explicitly ("plaintext bytes emitted in the current source attempt") + any separate durable
   staged/revealed progress the console needs; add UI/state-aggregation mappings for the new states.
2. **Alembic migrations (NOT create_all):** on `RestoreRequest` add `delivery_mode` (default
   `server_local`) + `receiver_device_id`; on `RestoreRequestItem` add `final_rel_path`; extend the
   state CHECK for the new states; add a `restore_item_checkpoint` table `{restore_request_item_id UNIQUE
   FK CASCADE, manifest_sha256, committed_index (monotonic, range-checked), revealed bool, updated_at}`;
   add a `restore_open_session` (LEASE) table bound to `(item_id, receiver_device_id, manifest_sha256)`
   with a generation + expiry (§7.1-B6).
3. **Logical device model (§7.1-B3):** a new table keyed by `device_id` (unique), with
   `GrpcDeviceEnrollment` cert rows as children (device_id NOT unique there — `(device_id,
   cert_fingerprint)` is). It holds the device's **scopes** (`{ingest,restore}`) and **destination
   grants** (which destination_id / dest_root a device may receive). `receiver_device_id` FKs to THIS
   table. Migrate/backfill existing enrolled device_ids into it with scope `{ingest}`.
4. **Restore-scope enrollment (§7.2):** add nullable `scopes` to the token + enrollment (via the logical
   device), backfill existing token/enrollment rows to exactly `["ingest"]`, then non-null + strict
   validation. Thread `scopes` in the `EnrollTokenGrant` (NOT just the .sutra-enroll bundle — tamper);
   rotation REPLACES scopes from the redeemed token (never unions old). **Authorize each requested scope
   independently** at the endpoints (`can_receive`→ingest, `can_restore`→restore) — a restore-only
   operator must be able to mint a restore enrollment (today the shared mint helper requires
   `can_receive`, `routes_devices.py:615`). The bundle gains a `scopes` field (the Rust consumer tolerates
   unknown fields).
5. **Live operator-capability resolver (§7.1-B2):** implement an authoritative "does operator X hold
   cap C NOW" resolver — a direct Authentik lookup with **fail-closed** availability (unreachable ⇒
   deny), or a deliberately-synchronized server-side grant registry. This is a NEW component (caps today
   come only from trusted HTTP headers, `identity.py:35`); `admitted_capabilities` cannot answer live.
   Not yet WIRED into an open gate (that's RM1.2) — build + unit-test the resolver here.
6. **Delivery-mode-aware admission + dispatch (§7.1-B1 — the race guard):** `POST /api/ui/restores`
   accepts `delivery_mode` + (for agent) a `receiver_device_id`; agent admission requires the device to
   exist, be `restore`-scoped, and have a matching destination grant, and records the binding. Route
   `server_local` items to the existing `restore` job UNCHANGED; **agent items must NEVER submit the
   local-write handler** — decide + implement whether an agent item enqueues a "prepare/lease" job or is
   simply left admitted-and-bound awaiting `OpenRestore` (RM1.2). No agent item may reach
   `serve_restore_item`'s local publish.
7. **Split destination auth from confinement (§7.2):** factor `canonicalize_restore_destination` into
   (1) opaque destination/grant authorization, (2) pure lexical relative-path validation, (3)
   server-local root confinement + overwrite (server_local ONLY). `server_local` path byte-for-byte
   unchanged; `agent` uses (1)+(2) only, never resolves against the archive-server root.

## Binding invariants
- `server_local` restore is byte-for-byte UNCHANGED; existing ingest enrollment flow (sutra-agent)
  UNBROKEN (scopes default/backfill to `{ingest}`). Alembic migrations, not `create_all`. An admitted
  agent item is durably bound + scoped and **cannot trigger any local write, nor be opened twice** (the
  lease table exists for RM1.2 to enforce). No streaming/RPC yet. No runtime compat flag (delivery_mode
  is a persisted attribute). Preserve/re-derive the path-confinement funnel — never bypass.

## Tests
- Migration up/down + backfill: existing enrollments/tokens → `{ingest}`; existing restores →
  `server_local`; the checkpoint/lease/logical-device tables + constraints exist.
- `server_local` admission + restore round-trip UNCHANGED (existing restore suite green).
- Agent admission: requires an existing restore-scoped device + destination grant; binds
  receiver_device_id; an ingest-only device / missing grant / unknown device is refused at admission.
- Agent item does NOT enqueue the local-write handler / never reaches local publish.
- Enrollment scopes: a restore-only operator can mint a restore enrollment; each scope authorized
  independently; rotation replaces (not unions) scopes; the token carries the grant (bundle-only is
  rejected).
- Live-capability resolver: returns live caps; fail-closed (unreachable ⇒ deny); unit-tested.
- Destination split: lexical validation rejects `..`/absolute/traversal; server_local confinement +
  overwrite behavior unchanged; agent path never resolves a server root.

## Definition of done (this repo's AGENTS.md)
`uv run pytest -q` green (paste tallies), `uv run ruff format --check`/`ruff check` + `uv run mypy`
clean on touched files (repo has pre-existing mypy debt elsewhere — don't regress yours). Summary: files
touched, each test → the §7 item it covers, the migration list, and an explicit statement that (a)
server_local is unchanged, (b) ingest enrollment is unbroken, (c) an agent item can't reach a local
write. Do NOT `#[ignore]`/skip the migration/admission/scope tests. NO gRPC service or streaming here.
