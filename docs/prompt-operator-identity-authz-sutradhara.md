# Codex prompt — Operator identity & component authZ: **sutradhara API**

> Status: **implemented** (round-4 codex review folded). Design:
> `docs/design-operator-identity-authz.md` (§15 = the security-review trail every
> acceptance test traces to). Sibling prompts (identical **Shared contract**):
> `~/system-ui/docs/prompt-operator-identity-authz-system-ui.md`,
> `~/dvarapala/docs/prompt-operator-identity-authz-dvarapala.md`. This is the
> **dependency root** — build and unit-test it in isolation with injected headers; it
> does not require the Caddy/Authentik stack to be running.

---

## Shared contract (IDENTICAL in all three prompts — do not diverge)

**Topology & trust invariant.**
- Browser → **Caddy** (edge TLS + forward-auth via Authentik's embedded outpost) →
  path-routed: `/api/*` → the **sutradhara API**, everything else → **system-ui**.
- The API **listens on a Unix domain socket** (default `/run/sutradhara/api.sock`)
  that is **bind-mounted into the Caddy container** — **no TCP listener in the PoC**,
  so there is no port a tailnet/host peer could reach. (Docker `host-gateway` /
  `host.docker.internal` resolves to the **bridge IP** `172.17.0.1`, *not* the host
  loopback, so a TCP `127.0.0.1` bind is **not** Caddy-reachable — the socket avoids
  that trap entirely.) If a TCP bind is ever used for local dev it MUST be loopback
  (`127.0.0.1`/`::1`); `serve-api` **refuses** `0.0.0.0`, `::`, and any
  non-loopback/tailnet address. This confinement is the first half of the trust basis.
- Caddy, inside an **order-pinned `route {}`**, **deletes every inbound
  `X-Authentik-*` request header before `forward_auth`**, then `copy_headers` sets the
  authentic ones from the outpost response. This is the second half.
- Given both (+ the public-NIC default-deny backstop), apps trust `X-Authentik-*`
  **with no token parsing**. If the API is ever exposed off the Caddy path the model
  breaks — a hard invariant, asserted by tests.

**Identity headers** (set by Caddy/outpost, consumed by the API):
`X-Authentik-Username` (stable id), `X-Authentik-Name` (display name),
`X-Authentik-Groups` (**pipe-`|`-separated** by default — confirm against the live
outpost and pin), `X-Authentik-Email`.

**Groups → role → capabilities.** Authentik-**local** groups (self-managed, not AD
groups): `sutradhara-viewer`, `sutradhara-operator`, `sutradhara-admin`.
- Parse `X-Authentik-Groups`: **split on `|`, exact-string-equality match**. Never
  substring/prefix (so `sutradhara-admin-extra` grants nothing). Missing/empty ⇒ **no
  groups ⇒ no access** (fail closed).
- `role` = highest tier present: admin > operator > viewer; none ⇒ denied.
- `capabilities`: `viewer → {can_view}`, `operator → {can_view, can_receive}`,
  `admin → {can_view, can_receive, can_admin}`.

**API surface** (sutradhara serves; system-ui consumes; dvarapala routes `/api/*`):
- `GET /api/session` → `{ operatorUsername, displayName, role, capabilities[] }`.
  **Never returns raw `groups[]`.**
- `GET /api/receive/options` → `{ sources: {sourceId, label, kind, status}[],
  landings: {landingId, label, status}[], artifactclasses: {artifactclass, label}[] }`.
  `sourceId`/`landingId` are **opaque** (never paths); `status ∈ {"available","busy"}`.
- `POST /api/receive` — **JSON only**, `Origin`/`Host` validated — body
  `{ sourceId, landingId, artifactclass, label?, idempotencyKey }` →
  `{ intakeId, status }` or a 4xx `{ error, detail }`. **`idempotencyKey` is a UUID the
  client mints once per "Start receive" intent and REUSES on every retry** (so a
  timeout/error retry dedupes — never minted per HTTP call).

**`POST /api/receive` server-side rules (all mandatory):**
1. **AuthZ:** require `can_receive` (operator|admin) else **403**.
2. **operator** stamped server-side = `X-Authentik-Username`; **any body `operator`
   ignored.**
3. **artifactclass** validated against the server registry
   (`src/sutradhara/artifactclass_policy.py`) else **400** — never trust the client's
   class (it is the storage/retention-policy identity).
4. **Source/landing confinement:** resolve `sourceId` to an allowlisted **source**
   root (under **`/replica/sources`**) and `landingId` to an allowlisted **landing**
   root (the configured landing root, *separate* from source roots); canonicalize and
   reject traversal / symlink-escape / anything outside its root, else **400**. Only
   opaque IDs cross the wire — never a path.
5. **Idempotency:** dedupe record scoped to `(operatorUsername, endpoint,
   idempotencyKey)` binding a **hash of the canonical request body**. Same key + same
   body ⇒ **same `intakeId`** (no second intake); same key + different body ⇒ **409**.
   The record carries an **`in_progress`** state claimed **atomically**, stored
   **durably + process-safely** (SQLite/catalog — **never** an in-process dict). A
   crashed/abandoned `in_progress` is **reclaimable via a TTL against a
   `last_heartbeat` timestamp** — the active receive updates `last_heartbeat`
   periodically (every N seconds, N ≪ TTL); **only** a row whose
   `last_heartbeat + TTL < now` is stale and may be reclaimed; a legitimately
   long-running receive is **never evicted**. The PoC runs uvicorn **single-worker,
   no `--reload`**.
6. **Source claim/lease:** claim the `sourceId` for the receive's duration; a
   concurrent `POST /api/receive` on a claimed source ⇒ **409**; reflected as
   `status:"busy"` in `/api/receive/options`. **Durable + process-safe** (same store),
   **reclaimable via TTL** (same `last_heartbeat` model as idempotency) — never a
   lost in-process lock, and **never evicts a legitimately in-progress receive**.

**Error contract:** JSON `{ error, detail }`; 400 (validation/bad class/bad
source/landing), 403 (capability), 409 (idempotency conflict OR source busy).
401/redirect is Caddy's.

**Cross-repo security invariants:** no `X-Authentik-*` trusted off the Caddy path
(socket confinement + scrub); no paths from the browser (opaque IDs only); JSON-only
mutating requests with `Origin`/`Host` validation; idempotency + claims are durable.

---

## This prompt: sutradhara — the HTTP API

### Scope
**In:** a FastAPI app on a **Unix domain socket**; header→identity; group→
role/capabilities; artifactclass validation; source+landing catalog & confinement;
**durable** idempotency + per-source claim (SQLite); CSRF/Origin guard; full unit
tests. Reuse the existing receive/intake core (`sutradhara_receive` / `intake.py`).
**Out:** Caddy/Authentik wiring (dvarapala); the React UI (system-ui); Kerberos;
persisting a separate display name (v1 stamps `operator=username`); async/job-id
receive (PoC is synchronous behind the idempotency guard); edge-CLI changes.

### Dependencies / run model
- Add server-only deps to `pyproject.toml`: `fastapi`, `uvicorn[standard]`. Keep them
  out of the `sutradhara-receive` edge package (preserve the edge/server split).
- Entry point: a `sutra serve-api` click command running uvicorn on a **UDS**:
  `--uds $SUTRA_API_SOCKET` (default `/run/sutradhara/api.sock`); create the parent
  dir; set socket perms so the Caddy container user can connect. **PoC: single
  worker, no `--reload`.** A `--host/--port` TCP mode may exist for local dev but the
  command **refuses** `0.0.0.0`, `::`, and any non-loopback address (incl.
  `100.81.52.26`).
  - Alternative (document, don't build now): run the API as a compose service on the
    Caddy private network instead of host UDS. Either way: never a tailnet/0.0.0.0 TCP.

### File structure
- Create `src/sutradhara/api/{__init__,app,identity,sources,store,routes_session,routes_receive}.py`
  - `identity.py` — `parse_identity(headers) -> Identity` (`|`-split + exact match +
    fail-closed) + `ROLE_CAPABILITIES`.
  - `sources.py` — source+landing catalogs over `/replica/sources` and the configured
    landing root; `resolve_source(id)`/`resolve_landing(id)` (canonicalize + confine).
  - `store.py` — **SQLite-backed** idempotency + source-claim store (atomic
    `in_progress`/claim via unique constraints; TTL reclamation via **`last_heartbeat`
    column** updated by the active receive every N seconds; only rows where
    `last_heartbeat + TTL < now` are reclaimed — a live receive is never evicted).
  - `app.py` — `create_app()`; mounts routers; CSRF/Origin + JSON-only middleware.
- Modify `src/sutradhara/cli/main.py` — register `serve-api`.
- Modify `src/sutradhara/catalog/session.py` — add
  `import_module("sutradhara.api.store")` in both `create_all()` and `reset_all()`
  alongside the existing `import_module("sutradhara.jobs.models")` call; without this,
  the idempotency + claim tables are invisible to `Base.metadata.create_all()` used by
  tests and the bootstrap CLI (tests do not run Alembic).
- Migration: add an alembic revision for the idempotency + source-claim tables
  (chain from the current head; follow the repo's alembic pattern).
- Tests under `tests/api/`.

### Work items (each ends green + a commit)
1. **`parse_identity` + caps** (`test_identity.py`): operator→`{can_view,can_receive}`;
   `viewer|operator|admin`→admin; **`sutradhara-admin-extra`→denied**; empty header→
   denied; `operatorUsername`=`-Username`, `displayName`=`-Name`.
2. **`GET /api/session`** (`test_session.py`): returns role+capabilities; **asserts
   `groups` absent** from the body.
3. **Catalogs + confinement** (`test_sources.py`): `resolve_source`/`resolve_landing`
   map known IDs to their roots; **reject (400)** outside-root, `..`, symlink-escape,
   and a path-shaped ID (`/etc/passwd`, the DB path, a dotfile) — use tmp roots via
   `SUTRA_RECEIVE_SOURCE_ROOT`/`SUTRA_RECEIVE_LANDING_ROOT`. `GET /api/receive/options`
   returns opaque `sources`+`landings`+`artifactclasses` with per-item `status`.
4. **artifactclass validation** (`test_receive_validation.py`): registry class passes;
   unknown → **400**.
5. **Durable idempotency** (`test_idempotency.py`): same key+body → same `intakeId`,
   receive core invoked once (spy); same key+different body → **409**; concurrent
   same-key (threads) → one run. **Heartbeat guards live receives:** insert a
   synthetic `in_progress` row with a recent `last_heartbeat`; the TTL reclaimer must
   **not** touch it even when its `created_at` is beyond the TTL. **Stale reclaim:**
   insert a row whose `last_heartbeat + TTL < now`; a subsequent call **reclaims and
   proceeds** (no duplicate-intake from a legitimately crashed receive). Also: call
   `create_all()` on a fresh engine and assert both `idempotency_record` and
   `source_claim` tables exist (schema parity — ensures the `session.py` import is
   wired).
6. **Durable source claim** (`test_source_claim.py`): claimed source → second POST
   **409** + `options` shows `status:"busy"`; claim released on completion. **Heartbeat
   guards live claims:** a claim with a recent `last_heartbeat` is **not** reclaimed by
   a concurrent second operator; a claim with `last_heartbeat + TTL < now` **is**
   reclaimable and the second call proceeds.
7. **CSRF/Origin + JSON-only** (`test_csrf.py`): form-encoded POST → **415/400**;
   foreign `Origin` → **403**; correct JSON+Origin → passes.
8. **`POST /api/receive` end-to-end** (`test_receive.py`, headers injected): viewer →
   **403**; operator → drives the receive/intake core against the resolved
   `sourceId`+`landingId` roots, **stamps `operator`=injected `X-Authentik-Username`**
   (assert persisted `Intake.operator`), **ignores body `operator`**, returns
   `{intakeId,status}`.
9. **Bind safety** (`test_bind.py`): default transport is the UDS; the TCP mode
   **rejects** `0.0.0.0`, `::`, `100.81.52.26` (and accepts `127.0.0.1`).

### Definition of done
`uv run pytest -q` (the **full suite**, per `AGENTS.md` — not just `tests/api/`) green;
`sutra serve-api` starts uvicorn on the UDS (single-worker, no reload); idempotency +
claims survive a process restart (durable store); no new deps leak into the edge
package; the receive core is reused. INDEX prompt status → in-progress on start,
implemented on completion.
