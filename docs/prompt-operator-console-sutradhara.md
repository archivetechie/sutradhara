# Codex prompt — operator console: **sutradhara server relay**

> Status: **implemented 2026-06-30**. Design: `docs/design-operator-console-relay.md` (the authority —
> read it first; 4 review rounds / 19 findings, trail in §11). Sibling prompts (identical
> **Shared contract**): `docs/prompt-operator-console-sutra-agent.md` (the helper daemon,
> same repo), `~/system-ui/docs/prompt-operator-console-system-ui.md` (the browser).
> **This is the dependency root** — build it first; the helper and browser consume its
> proto + endpoints. Extends `design-streaming-intake-grpc.md` (the mTLS gRPC + streaming,
> already built) and `design-operator-identity-authz.md` (the Authentik HTTP API).

Per `AGENTS.md`: run `uv run pytest -q` (the **full suite**) and paste output at every
milestone; commit at every green milestone; update `docs/INDEX.md` on completion.

---

## Shared contract (IDENTICAL in all three prompts — do not diverge)

**Topology.** Browser (system-ui, Authentik) → **server** → (mTLS relay) → **local helper
(sutra-agent)** → (gRPC stream) → server → `sutra intake watch`. The browser is pure view;
the helper is pure outbound (no inbound surface); the **server is the hub** where identity,
authZ, the connected-device registry, and the relay converge. The byte transfer reuses the
existing `IntakeService` streaming + `grpc_client.stream_source`; the design adds only a
thin control plane.

**`DeviceService` (new gRPC service, on the existing mTLS port).**
`rpc Connect(stream DeviceMessage) returns (stream ServerCommand)` — bidirectional; the
helper dials it once (mTLS device cert) and keeps it open (open stream = liveness).
- *Up* `DeviceMessage`: `CardSnapshot{cards:[Card{card_id,label,kind,size_bytes,status}]}`
  (`kind ∈ {card,drive,other}`; full list on connect + on mount change), `Heartbeat{}`,
  `CommandAck{command_id, accepted|rejected, reason, intake_id?}`, and
  **`ActiveReceives{receives:[ActiveReceive{card_id,idempotency_key,intake_id,state}]}`**
  (sent on every `Connect` + on change — the helper's authoritative in-flight set that the
  relay rebuilds correlation/idempotency from after a restart).
- *Down* `ServerCommand`: `StartReceive{command_id, card_id, artifactclass, label?,
  source_ref?, idempotency_key}`. **No `source_kind` on the wire** — the helper derives it
  from the enumerated card.

**HTTP console endpoints** (Authentik-gated, behind Caddy on the existing
operator-identity HTTP surface — **not** the mTLS port):
- `GET /api/devices` → the **authenticated** operator's **online devices + card lists**
  (from the in-memory registry) **and** in-flight receives (`intake_id, device_id,
  card_id, status`) **from the durable `grpc_intake` table** (so refresh/restart re-attach).
- `POST /api/devices/{device_id}/receive` → `{card_id, artifactclass, label?,
  idempotencyKey}` → owner-checked; pushes `StartReceive`; awaits the **early ack** (the
  helper `StartIntake`s, acks the `intake_id`, then background-streams) → returns
  `{intakeId, status:"streaming"}` promptly. Holds a **durable HTTP idempotency record**
  `(operator, "POST /api/devices/receive", key)` binding a `request_hash` over
  `(device_id, card_id, artifactclass, label, source_ref)` — same key+body → replay the
  completed `intakeId`; same key+body while still in progress → `409 already_in_progress`
  without a second `StartReceive`; same key + different body → 409. **Timeout is advisory**
  (a late ack still completes correlation+idempotency, generation-gated; never cancels the helper).
- `POST /api/devices/{device_id}/revoke` → admin-only, Origin-guarded JSON POST; durable
  revoke + live `registry.evict(device_id)` in the running `sutra serve` process.
- `GET /api/intake/{intake_id}/status` → thin HTTP reader over the same
  `grpc_intake`-state → watcher-marker logic the gRPC `GetIntakeStatus` uses.
- `POST /api/enroll/token` (Authentik-gated **and** Origin-guarded) → mints a one-time,
  24h, **operator-scoped + `device_id`-bound** token.
- `POST /api/enroll/csr` (**the one pre-cert surface**) → `{csr_pem, token}` →
  consumes/validates the token, requires the CSR `CN` == the token's `device_id`, reads
  the operator **from the token**, signs, records `(device_id, fingerprint)→operator`,
  returns `{cert_pem, ca_pem}`. **Exempt from BOTH Caddy forward-auth AND the FastAPI
  `_json_origin_guard`** (a helper POST has no browser Origin); token is the auth; JSON-only;
  rate-limited; tailnet-only; CA-pinned by the helper (no TOFU).

**Load-bearing invariants** (design §2–§7, §10): server-brokered relay — **no inbound
surface on the laptop**; **identities meet at the server** (`GET/POST` only expose/command
devices whose `device→operator` mapping equals the Authentik operator); **per-RPC/per-
command owner check** with **per-command `resolve_device` re-resolve** (rejects revoked);
**crash-safe `card_id` correlation** via `ActiveReceives`; server retries do not send a
second command while an idempotency key is still in progress; the helper is still
idempotent on `idempotency_key` as a defense-in-depth sibling contract; **`AbortIntake` on
a terminal background failure** (no stuck `streaming`);
`IntakeService`/`StartIntakeRequest` **unchanged** — `card_id` is **relay-correlated** into
`grpc_intake`.

---

## This prompt: the server relay (sutradhara repo)

### Scope
**In:** the `DeviceService` proto + servicer + the `ConnectedDeviceRegistry`; the
`grpc_intake.card_id` migration; the HTTP console + enrollment endpoints; the `ca.py`
token-scoped signing; the `serve-api`+`serve-grpc` **merge into one `sutra serve`**; the
admin revoke live eviction + durable CLI revoke split; the dvarapala Caddy exemption
(cross-repo, below). **Out:** the helper daemon (sibling prompt), the browser (sibling
prompt), the `IntakeService` streaming (reused unchanged).

### Milestones (each ends green: `uv run pytest -q` + commit)

1. **`proto/device.proto` + stubs.** The `DeviceService` + messages per the Shared
   contract; `scripts/regenerate-proto.sh` → committed stubs in `src/sutradhara/_proto`
   (repo convention). Test: stubs import + message round-trips.
2. **`grpc_intake.card_id` migration** (`src/sutradhara/grpc/store.py`): add the nullable
   `card_id` column + alembic revision (chain from head); `create_all` already imports the
   store. Test: column present; round-trip.
3. **`ConnectedDeviceRegistry`** (`src/sutradhara/grpc/registry.py`): thread-safe; per-stream
   command queue + pending-ack map + **generation/epoch**; **last-writer-wins `register`**
   (replace + close old stream + fail its pending acks); `devices_for(operator)`;
   heartbeat-TTL eviction via a dedicated fast registry sweep loop; `evict(device)`.
   Tests per design §9 (`test_registry.py`, `test_serve_lifecycle.py` concurrency: queue
   delivery, timeout→409, duplicate-stream replacement).
4. **`DeviceService` servicer** (`src/sutradhara/grpc/device_service.py`): `Connect`;
   identity from the peer cert + **per-command re-resolve**; feed `CardSnapshot` → registry;
   push `StartReceive`; on `CommandAck` **correlate `card_id` into `grpc_intake` + complete
   the HTTP idempotency record** (generation-gated for late acks); on `ActiveReceives`
   **rebuild** both. Tests per §9 (`test_device_service.py`, `test_card_id_correlation.py`
   incl. the crash-safety reconcile + late-ack drop).
5. **HTTP console endpoints** (on the operator-identity FastAPI app): `GET /api/devices`
   (registry + durable in-flight from `grpc_intake`), `POST /api/devices/{id}/receive`
   (owner check + early-ack + HTTP idempotency request-hash),
   `POST /api/devices/{id}/revoke` (admin-only live eviction), `GET /api/intake/{id}/status`.
   Tests per §9 (`test_device_routes.py`, `test_receive_idempotency.py`,
   `test_receive_retry.py`).
6. **Enrollment** (`src/sutradhara/grpc/ca.py` + routes): `issue_enroll_token`
   (operator-scoped + `device_id`-bound) behind `POST /api/enroll/token`; `sign_device_csr`
   reads the operator **from the token**, requires `CN==token.device_id`; `POST /api/enroll/csr`
   **exempt from the `_json_origin_guard`** (allowlist the path in `src/sutradhara/api/app.py`)
   + rate-limited + returns `{cert_pem, ca_pem}`. The standalone `serve-grpc --revoke-device`
   path is durable revoke only; live eviction is through the admin HTTP route.
   Tests per §9 (`test_enroll.py`: csr reachable with no Authentik/no Origin yet rejects
   bad/used/expired token + CN-mismatch; admin revoke evicts the live stream).
7. **Merge `sutra serve`** (`src/sutradhara/cli/main.py`): one process hosts the mTLS gRPC
   port (`IntakeService`+`DeviceService`) **and** the Unix-socket HTTP app, **sharing one
   registry**; the §3.0 startup/graceful-shutdown + lock/queue concurrency contract; the
   hourly stale-receive sweep thread plus the fast registry liveness sweep thread. Keep both surfaces' auth distinct. Test: lifecycle
   (`test_serve_lifecycle.py`) start/shutdown; both listeners up; shared registry.
8. **Cross-repo: dvarapala Caddy** — exempt `POST /api/enroll/csr` from the forward-auth
   wall (an explicit `route` ordering / matcher before `forward_auth`, like the
   `request_header` scrub), + a rate-limit. Provide the Caddyfile snippet + a
   `~/dvarapala` `verify.sh` check that an unauthenticated `enroll/csr` reaches the app
   while `enroll/token` still 401s without Authentik. (Commit in `~/dvarapala`.)

### Definition of done
`uv run pytest -q` green (output pasted); `sutra serve` runs both surfaces on one shared
registry; a helper can `Connect` (mTLS), report cards + `ActiveReceives`, and be commanded;
`GET /api/devices` shows online cards + durable in-flight receives; enrollment signs a
device-bound, operator-scoped cert over the CA-pinned, doubly-exempt `enroll/csr`; the
admin revoke API evicts a live stream; the dvarapala Caddy exemption is in place +
verified. INDEX updated.
