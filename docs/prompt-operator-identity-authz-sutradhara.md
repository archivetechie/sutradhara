# Codex prompt — Operator identity & component authZ: **sutradhara API**

> Status: **pending**. Design: `docs/design-operator-identity-authz.md` (read it;
> §15 is the security-review trail every acceptance test below traces to).
> Sibling prompts (identical **Shared contract** section): `~/system-ui/docs/
> prompt-operator-identity-authz-system-ui.md`, `~/dvarapala/docs/
> prompt-operator-identity-authz-dvarapala.md`. This is the **dependency root** —
> build and unit-test it in isolation with injected headers; it does not require the
> Caddy/Authentik stack to be running.

---

## Shared contract (IDENTICAL in all three prompts — do not diverge)

**Topology & trust invariant.**
- Browser → **Caddy** (edge TLS + forward-auth via Authentik's embedded outpost) →
  path-routed: `/api/*` → the **sutradhara API**, everything else → **system-ui**.
- The sutradhara API **binds loopback only (`127.0.0.1:8770`) — NEVER the tailnet IP
  (`100.81.52.26`) and never `0.0.0.0`.** Only Caddy may reach it. This bind is the
  first half of the trust basis.
- Caddy, inside an **order-pinned `route {}`**, **deletes every inbound
  `X-Authentik-*` request header before `forward_auth`**, then `copy_headers` sets
  the authentic ones from the outpost response. This is the second half.
- Given both (+ the public-NIC default-deny backstop), apps trust `X-Authentik-*`
  **with no token parsing**. If the API is ever exposed off the Caddy path the model
  breaks — so the bind is a hard invariant, asserted by tests.

**Identity headers** (set by Caddy/outpost, consumed by the API):
`X-Authentik-Username` (stable id), `X-Authentik-Name` (display name),
`X-Authentik-Groups` (**pipe-`|`-separated** by default — confirm against the live
outpost and pin), `X-Authentik-Email`.

**Groups → role → capabilities.** Authentik-**local** groups (self-managed, not AD
groups): `sutradhara-viewer`, `sutradhara-operator`, `sutradhara-admin`.
- Parse `X-Authentik-Groups`: **split on `|`, exact-string-equality match** to the
  known group names. Never substring/prefix (so `sutradhara-admin-extra` grants
  nothing). Missing/empty header ⇒ **no groups ⇒ no access** (fail closed).
- `role` = highest tier present: admin > operator > viewer; none ⇒ denied.
- `capabilities` derived from role:
  `viewer → {can_view}`, `operator → {can_view, can_receive}`,
  `admin → {can_view, can_receive, can_admin}`.

**API surface** (sutradhara serves; system-ui consumes; dvarapala routes `/api/*`):
- `GET /api/session` → `{ operatorUsername, displayName, role, capabilities[] }`.
  **Never returns raw `groups[]`.**
- `GET /api/receive/options` →
  `{ sources: {sourceId, label, kind, status}[], artifactclasses: {artifactclass, label}[] }`.
  `sourceId` is **opaque** (never a path); `status ∈ {"available","busy"}`.
- `POST /api/receive` — **JSON only**, `Origin`/`Host` validated — body
  `{ sourceId, artifactclass, label?, landingId?, idempotencyKey }` →
  `{ intakeId, status }` or a 4xx error `{ error, detail }`.

**`POST /api/receive` server-side rules (all mandatory):**
1. **AuthZ:** require capability `can_receive` (operator|admin) else **403**.
2. **operator** stamped server-side = `X-Authentik-Username`; **any body `operator`
   is ignored.**
3. **artifactclass** validated against the server registry
   (`src/sutradhara/artifactclass_policy.py`) else **400** — never trust the client's
   class (it is the storage/retention-policy identity).
4. **Source confinement:** resolve `sourceId`/`landingId` server-side to an
   allowlisted root under **`/replica/sources`** (canonicalize, reject traversal,
   symlink-escape, and anything outside the root) else **400**. No path ever crosses
   the wire.
5. **Idempotency:** dedupe record scoped to `(operatorUsername, endpoint,
   idempotencyKey)` binding a **hash of the canonical request body**. Same key + same
   body ⇒ **same `intakeId`** (no second intake). Same key + different body ⇒ **409**.
   Record carries an **`in_progress`** state claimed atomically so concurrent retries
   don't both run receive.
6. **Source claim/lease:** claim the `sourceId` for the receive's duration; a
   concurrent `POST /api/receive` on a claimed source ⇒ **409**; reflected as
   `status:"busy"` in `/api/receive/options`.

**Error contract:** JSON `{ error, detail }`; codes 400 (validation/bad
class/bad source), 403 (capability), 409 (idempotency conflict OR source busy).
401/redirect is Caddy's, not the API's.

**Cross-repo security invariants:** no `X-Authentik-*` trusted off the Caddy path
(bind + scrub); no server paths from the browser (opaque IDs only); JSON-only
mutating requests with `Origin`/`Host` validation.

---

## This prompt: sutradhara — the HTTP API

### Scope
**In:** a new FastAPI app exposing the three endpoints above; header→identity
parsing; group→role/capabilities; artifactclass validation; source catalog +
confinement under `/replica/sources`; idempotency; per-source claim; CSRF/Origin
guard; the loopback bind; full unit tests. Reuse the existing receive/intake core
(`sutradhara_receive` / `intake.py`) — do **not** reimplement receiving.

**Out:** Caddy/Authentik wiring (dvarapala prompt); the React UI (system-ui prompt);
Kerberos/SPNEGO; persisting a separate display name (v1 stamps `operator =
username`); async/job-id receive (PoC is synchronous behind the idempotency guard);
any change to the edge `sutra-receive` CLI.

### Dependencies / setup
- Add server-only deps to `pyproject.toml`: `fastapi`, `uvicorn[standard]`. Keep them
  out of the edge `sutradhara-receive` package (preserve the edge/server split).
- Entry point: a `sutra serve-api` click command (in `src/sutradhara/cli/`) that runs
  uvicorn bound to **`127.0.0.1` port `8770`** (both configurable via
  `SUTRA_API_HOST`/`SUTRA_API_PORT`, defaulting to loopback). A test asserts the
  default host is `127.0.0.1`, never `0.0.0.0`/the tailnet IP.

### File structure
- Create `src/sutradhara/api/__init__.py`
- Create `src/sutradhara/api/app.py` — FastAPI app factory `create_app()`; mounts
  routers; installs the CSRF/Origin + JSON-only middleware.
- Create `src/sutradhara/api/identity.py` — `parse_identity(headers) -> Identity`
  (operatorUsername, displayName, groups, role, capabilities); the `|`-split + exact
  match + fail-closed logic; `ROLE_CAPABILITIES` map.
- Create `src/sutradhara/api/sources.py` — the source catalog (`/replica/sources`
  roots → `{sourceId,label,kind,status}`), `resolve_source(sourceId) -> Path` with
  canonicalize + confinement, and the per-source claim store.
- Create `src/sutradhara/api/idempotency.py` — the dedupe store (scope + body-hash +
  `in_progress` atomic claim + 409-on-conflict).
- Create `src/sutradhara/api/routes_session.py`, `routes_receive.py` — the endpoints.
- Modify `src/sutradhara/cli/main.py` — register `serve-api`.
- Modify `pyproject.toml` — deps.
- Tests under `tests/api/` (one file per module below).

### Work items (each ends green + a commit)

1. **`parse_identity` + role/capabilities.** Pure function over a header mapping.
   Acceptance/tests (`tests/api/test_identity.py`):
   - `sutradhara-operator` alone → role `operator`, caps `{can_view,can_receive}`.
   - `viewer|operator|admin` → role `admin`.
   - **`sutradhara-admin-extra` → role denied** (no substring match).
   - empty/missing `X-Authentik-Groups` → no caps, denied.
   - `operatorUsername`=`X-Authentik-Username`, `displayName`=`X-Authentik-Name`.

2. **`GET /api/session`.** Returns `{operatorUsername, displayName, role,
   capabilities}` from `parse_identity`; **asserts `groups` is NOT in the response**
   (`tests/api/test_session.py`).

3. **Source catalog + confinement** (`tests/api/test_sources.py`):
   - `resolve_source` maps a known `sourceId` to its `/replica/sources/...` path.
   - **Rejects (400):** a `sourceId` resolving outside `/replica/sources`, `..`
     traversal, a symlink escaping the root, and a **path-shaped `sourceId`** (e.g.
     `/etc/passwd`, `../../etc`, the DB path, a dotfile). Use a tmp root in tests via
     a `SUTRA_RECEIVE_ROOT` override.
   - `GET /api/receive/options` returns opaque `sourceId`s + advertised
     `artifactclasses` (from `artifactclass_policy.py`) + per-source `status`.

4. **artifactclass validation** (`tests/api/test_receive_validation.py`):
   - a class in the registry passes; an unknown class → **400**.

5. **Idempotency store** (`tests/api/test_idempotency.py`):
   - same key + same body → same `intakeId`, exactly one intake created (assert the
     receive core is invoked once — patch/spy).
   - same key + different body → **409**.
   - concurrent same-key (two threads) → exactly one runs; the other gets the same id
     / in-progress, never a second run.

6. **Per-source claim** (`tests/api/test_source_claim.py`):
   - while a source is claimed, a second `POST /api/receive` on it → **409**;
     `/api/receive/options` shows that source `status:"busy"`.
   - claim released after the receive completes (or fails) → source `available` again.

7. **CSRF/Origin + JSON-only middleware** (`tests/api/test_csrf.py`):
   - a form-encoded `POST /api/receive` → **415/400** (rejected).
   - a `POST` with a foreign `Origin` → **403**.
   - a well-formed JSON POST with the correct `Origin`/`Host` → passes the guard.

8. **`POST /api/receive` end-to-end** (`tests/api/test_receive.py`, headers injected):
   - operator without `can_receive` (viewer) → **403**.
   - operator with `can_receive` → drives the existing receive/intake core against the
     resolved `/replica/sources` root, **stamps `operator` = injected
     `X-Authentik-Username`** (assert on the persisted `Intake.operator`), **ignores a
     body `operator`**, returns `{intakeId, status}`.

9. **Bind safety** (`tests/api/test_bind.py`): `serve-api` default host is `127.0.0.1`;
   assert it is never `0.0.0.0` or `100.81.52.26`.

### Definition of done
All `tests/api/` green from a clean `pytest`; `sutra serve-api` starts uvicorn on
loopback; no new deps leak into the `sutradhara-receive` edge package; the receive
core is reused (not reimplemented). Update `docs/INDEX.md` prompt status → in-progress
on start, implemented on completion.
