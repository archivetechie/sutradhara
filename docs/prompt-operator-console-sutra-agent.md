# Codex prompt — operator console: **sutra-agent helper daemon**

> Status: **pending**. Design: `docs/design-operator-console-relay.md` (the authority).
> Sibling prompts (identical **Shared contract**): `docs/prompt-operator-console-sutradhara.md`
> (the server relay — **the dependency root; build it first**),
> `~/system-ui/docs/prompt-operator-console-system-ui.md` (the browser). Same repo as the
> server (`packages/sutra-agent/`). Reuses the existing `grpc_client.stream_source` +
> `ledger` + `ca`/enrollment from `design-streaming-intake-grpc.md`.

Per `AGENTS.md`: run `uv run pytest -q` (the **full suite**) and paste output at every
milestone; commit at every green milestone; update `docs/INDEX.md` on completion.

---

## Shared contract (IDENTICAL in all three prompts — do not diverge)

**Topology.** Browser (system-ui, Authentik) → **server** → (mTLS relay) → **local helper
(sutra-agent)** → (gRPC stream) → server → `sutra intake watch`. The browser is pure view;
the helper is pure outbound (no inbound surface); the **server is the hub**. The byte
transfer reuses the existing `IntakeService` streaming + `grpc_client.stream_source`.

**`DeviceService` (gRPC, mTLS port).** `rpc Connect(stream DeviceMessage) returns (stream
ServerCommand)` — the helper dials it once (mTLS device cert) and keeps it open.
- *Up* `DeviceMessage`: `CardSnapshot{cards:[Card{card_id,label,kind,size_bytes,status}]}`
  (`kind ∈ {card,drive,other}`; on connect + on mount change), `Heartbeat{}`,
  `CommandAck{command_id, accepted|rejected, reason, intake_id?}`, and
  **`ActiveReceives{receives:[ActiveReceive{card_id,idempotency_key,intake_id,state}]}`**
  (every `Connect` + on change — the helper's authoritative in-flight set).
- *Down* `ServerCommand`: `StartReceive{command_id, card_id, artifactclass, label?,
  source_ref?, idempotency_key}`. **No `source_kind` on the wire** — the helper derives it
  from the enumerated card.

**HTTP console endpoints** (server side; the helper uses only `POST /api/enroll/csr`):
`GET /api/devices`, `POST /api/devices/{id}/receive` (early-ack + HTTP idempotency),
`GET /api/intake/{id}/status`, `POST /api/enroll/token` (browser), `POST /api/enroll/csr`
(the one pre-cert surface — token-gated, CA-pinned, doubly-exempt from Caddy forward-auth
+ the Origin guard; returns `{cert_pem, ca_pem}`).

**Load-bearing invariants:** server-brokered relay — **no inbound surface on the laptop**;
identities meet at the server; **crash-safe `card_id` correlation** via `ActiveReceives`;
helper **idempotent on `idempotency_key`** (same key → re-ack existing intake; different
key on a busy card → busy); **`AbortIntake` on a terminal background failure**;
`StartIntakeRequest` unchanged (`card_id` is relay-correlated); the **mount path never
crosses the wire** (opaque `card_id` only).

---

## This prompt: the local helper daemon (`packages/sutra-agent`)

### Scope
**In:** the control daemon (`Connect` + reconnect + command dispatch), the mount watcher,
the `enroll` change (CSR submit to `/api/enroll/csr`), the `stream_source` `on_started`
callback, and a launchd/login-item template. **Out:** the server relay (sibling prompt),
the browser (sibling prompt), the byte-transfer path (reused unchanged).

### Milestones (each ends green: `uv run pytest -q` + commit)

1. **Mount watcher** (`mounts.py`): enumerate card/drive mounts (FSEvents on macOS, polling
   fallback); each gets an **opaque `card_id` carrying a stable volume identity** (volume
   UUID); map `card_id → local mount path` **locally**; `card.kind ∈ {card,drive,other}`.
   `current_cards()` + change callbacks. Tests (`test_mounts.py`): `card_id` carries volume
   identity; the mount path never appears in any outbound payload.
2. **`stream_source` `on_started` callback** (`grpc_client.py`): fire `on_started(intake_id)`
   right after `StartIntake`; upload/commit otherwise unchanged. Test: the callback fires
   before completion.
3. **Control daemon** (`controld.py`, `sutra-agent serve`): maintain the `Connect` stream
   (reconnect-with-backoff + heartbeat); push `CardSnapshot` on mount change; on
   `StartReceive` — validate the card is mounted + not in flight (**track in-flight by
   `idempotency_key`**: a matching key **re-acks the existing `intake_id`**, a different key
   on a busy card → `rejected "card busy"`), then `StartIntake` (via `stream_source` with a
   per-receive `replace(config, source_kind=card.kind)`), **early-ack the `intake_id`**, and
   **background-stream**; on a **terminal background failure** call **`AbortIntake`**; send
   **`ActiveReceives`** on every `Connect` (built from the `ledger`). Tests
   (`test_controld.py`): reconnect/backoff; early-ack-then-stream; same-key re-ack vs
   different-key busy; `AbortIntake` on terminal failure → `aborted`; `ActiveReceives` lists
   in-flight; `source_kind` == the card's kind.
4. **Enrollment** (`cli.py` `enroll`): `--token <token> --device-id <id>` → generate key+CSR
   locally (`CN=device_id`) → `POST /api/enroll/csr` over a **CA-pinned** TLS connection
   (the helper ships with / is configured with the server CA — **no TOFU**) → store
   `{cert_pem, ca_pem}`. **Drop `--operator`** (operator comes from the token). Test
   (`test_enroll_client.py`): CSR `CN==device_id`; pins the CA; stores the returned cert+CA;
   a token/CN mismatch from the server surfaces a clear error.
5. **Deployment template:** a launchd plist (macOS) / login-item that runs `sutra-agent serve`
   at login; a `sutra-agent serve --status` health check. (Doc + template; not auto-installed.)

### Definition of done
`uv run pytest -q` green (output pasted); `sutra-agent serve` connects to `sutra serve`
(mTLS), reports cards + `ActiveReceives`, runs a relayed receive end-to-end (early-ack →
background stream → commit → handoff to watch), is idempotent on the key, aborts cleanly on
a pulled card, and never emits a local path; `sutra-agent enroll` redeems a device-bound
token over a CA-pinned connection. INDEX updated.
