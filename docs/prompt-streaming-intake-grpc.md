# Codex prompt — streaming card/drive intake over gRPC + mTLS

> Status: **implemented** (2026-06-30). Design: `docs/design-streaming-intake-grpc.md` (the authority —
> read it first; 5 review rounds / 30 findings folded, trail in its §16). Single repo
> (`sutra-agent` lives at `packages/sutra-agent/`). **Prerequisite satisfied:**
> `sutra intake watch` is implemented (`docs/design-intake-watch.md`,
> `src/sutradhara/intake_watch.py`) — the gRPC path hands off to it and writes **no**
> terminal markers itself.

Per `AGENTS.md`: run `uv run pytest -q` (the **full suite**) and paste the output at
every milestone; commit at every green milestone (never leave the tree dirty); update
`docs/INDEX.md` on completion. Two QuadStor VTLs / strict policy validation still apply.

---

## Repo conventions (corrections to the design's assumptions — follow these)

- **Proto source goes in `proto/intake.proto`** (next to the existing `proto/layer5.proto`),
  **NOT** `packages/sutra-agent/proto/`. Generated stubs are committed; regenerate via
  `scripts/regenerate-proto.sh` (extend it to cover `intake.proto`). The server imports
  stubs from `src/sutradhara/_proto/` (ruff-excluded, generated). (Supersedes design §15 Q6.)
- **Keep `sutradhara-receive` grpc-free.** `grpcio`/`protobuf` are already in the main
  `pyproject.toml` (server); `grpcio-tools` is a dev dep — the server needs no new deps.
  The **agent** (`packages/sutra-agent`) gets its own `grpcio` dep and its **own** copy of
  the generated stubs (regenerate into `packages/sutra-agent/src/sutra_agent/_proto/`),
  so the edge/server split holds and the lightweight `sutradhara-receive` never imports
  grpc.
- **Migrations** live in `alembic/versions/`; chain the new revision from the current
  head (`uv run alembic heads`). `alembic/versions` is ruff-excluded.
- Reuse `sutradhara_receive` exports (`BAG_INFO_NAME`, `MANIFEST_NAME`,
  `PACKAGE_INDEX_NAME`, `CANONICALIZATION_VERSION`, `PACKAGE_PROFILE_VERSION`,
  `hash_payload_tree`, `write_bagit_files`, …) for bag assembly — do not reimplement.

## Scope

**In:** the `proto/intake.proto` service; the server (`src/sutradhara/grpc/`: server,
servicer, assembly, ca, store) + `sutra serve-grpc`; the `grpc_intake` durable table +
migration; the device-cert CA / device→operator enrollment; the `sutra-agent`
streaming client + config + `enroll` + ledger plan-digest; the extracted shared payload
planner; tests + one in-repo end-to-end test against the real `sutra intake watch`.

**Out:** the operator GUI; any change to `sutra serve-api` / the HTTP API; Caddy /
Authentik; mid-file byte-level resume (the proto `offset` is reserved for v2); building
`sutra intake watch` (done). A live `~/system` harness scenario is a **follow-up**.

---

## Load-bearing invariants (do not weaken — design §4–§11; each traces to folded findings)

1. **Single pass, no local buffer.** The client reads each source unit **once**, hashes
   in memory, and streams it. Planning is **metadata-only** (`{relpath,size,mtime_ns}`,
   no content read) so a package tar is produced exactly once at upload (§4, §11).
2. **Device identity, server-assigned operator.** The client cert `CN = device_id`; the
   server resolves the operator from a `(device_id, fingerprint) → operator` enrollment
   mapping and stamps **that**. No request carries an `operator` field; never trust a
   client-supplied one (§6).
3. **Per-RPC owner check.** Every RPC that takes an `intake_id` resolves the peer cert →
   `(operator, device_id)` and requires it to match the intake's **stored** owner
   (`PERMISSION_DENIED` otherwise). A valid device cert is not authority over an
   arbitrary intake (§7).
4. **Durable `grpc_intake` state, not `.receiving.json`.** Owner / state /
   committed `manifest_digest` live in the `grpc_intake` table and survive
   `.receiving.json` removal at commit (§7).
5. **Temps outside `data/`, atomic rename, receipt ledger.** `UploadFile` writes to
   `{intake}/.incoming/{uuid}.tmp` (UUID, outside `data/` so a crash can't leave a
   `*.tmp` the receive validator hashes as extra payload), then fsync → atomic rename
   into `data/{relpath}` → append `{relpath, server_sha256, bytes}` to
   `receive-receipts.jsonl` **after** the rename (under a per-intake lock). `relpath` is
   relative to `data/` and a leading `data/` is rejected (§7).
6. **State machine + concurrency.** `streaming → committing → committed` (+ `aborted`) as
   a compare-and-set on the `grpc_intake` row under a per-intake lock; an in-flight
   upload counter; `CommitIntake` rejected while any upload is in flight; a recoverable
   commit failure rolls back `committing → streaming` and returns `reupload_relpaths`
   (§7).
7. **Hand off to `sutra intake watch` — no gRPC verify/register.** `CommitIntake`
   produces a bag **indistinguishable from a local `sutra receive` bag** (sentinel last,
   `.receiving.json` removed), then stops. The watcher verifies fixity, registers, and
   writes the terminal markers. The gRPC server writes **none** (§7, §10). Verified
   against `validate_bag`: the bag's extra root artifacts (`receive-receipts.jsonl`,
   emptied `.incoming/`) are outside `data/` + the tag set, so the validator ignores them.
8. **Skew guards + correct package-index.** Reject a `canonicalization_version` /
   `package_profile_version` that ≠ the server constants **before** writing the bag;
   write `Package-Profile-Version`/`-Hash`/`Canonicalization-Version` **constants**
   unconditionally into `bag-info.txt` (never `""`). Packages land as **one**
   `package-index.json` that `core.read_package_index` accepts (top-level
   profile/profile_hash constants; `packages[]` with logical/stored/sha256/members;
   member JSON mapped **by type** — non-file → `sha256:null`/`data_offset:null`, symlink
   → `linkname`) (§5, §7).
9. **bag-info authority = StartIntake intent.** artifactclass/source_kind/source_ref/label
   are stored server-side at StartIntake and are authoritative for `bag-info.txt`; Commit
   carries only receive-determined facts; the client never restates intent (§5, §7).
10. **Bind safety.** `serve-grpc` binds the LAN/Tailscale interface, never `0.0.0.0`/`::`
    or the public NIC; mTLS `require_client_auth=True` (§3, §7).

---

## Work items (each ends green: `uv run pytest -q` + a commit)

### 1. Proto + generated stubs + regen

`proto/intake.proto` exactly per design §5 (service `IntakeService` with **six** RPCs:
`StartIntake`, `UploadFile` (client-streaming), `ListIntakeFiles`, `CommitIntake`,
`GetIntakeStatus`, `AbortIntake`; messages incl. `StartIntakeRequest.source_plan_digest`,
`FileChunk` (optional `file_size` hint, 0 for packages), `ReceiveFacts`, `PackageIndex` /
`PackageMemberEntry` with `optional` `sha256`/`data_offset`/`linkname`,
`CommitIntakeRequest.manifest_digest`, `reupload_relpaths`, `AbortIntake*`). Extend
`scripts/regenerate-proto.sh` to generate committed stubs into **both**
`src/sutradhara/_proto/` (server) and `packages/sutra-agent/src/sutra_agent/_proto/`
(agent); add `grpcio` to `packages/sutra-agent/pyproject.toml`. Commit the proto + both
stub sets. (Test: stubs import; a trivial round-trip serialize of each message.)

### 2. `grpc_intake` durable store + migration

`src/sutradhara/grpc/store.py`: the `grpc_intake` table (`intake_id PK, operator,
device_id, state, manifest_digest NULL, created_at, updated_at`) + helpers
(insert/get/compare-and-set state/set committed digest). Alembic revision chained from
the current head. `test_grpc_store.py`: owner/state/digest round-trip; CAS state
transition; **after commit the row still resolves owner + status + manifest_digest**; an
`aborted` row blocks reuse.

### 3. CA + device→operator enrollment + revocation

`src/sutradhara/grpc/ca.py`: generate/load the sutradhara CA at `--pki-dir`
(default `/etc/sutradhara/pki`, key mode 0600); sign a device CSR (`CN = device_id`),
**record `(device_id, fingerprint) → operator`** (in `grpc_intake`'s store or a small
sibling table); `revoke_device(device_id)` (drop mapping + block fingerprint); resolve a
peer cert → `(operator, device_id)` or raise. **Enrollment transport (v1 scoping
decision):** use an **out-of-band CSR-signing CLI** — `sutra serve-grpc
--issue-enroll-token` mints a 24h one-time token; the operator runs `sutra-agent enroll`
to produce key+CSR locally and submits the CSR + token; an admin signs with a server-side
command that validates the token and records the mapping. This avoids standing up a new
HTTPS network surface for a 2-operator PoC (the design's HTTPS enrollment endpoint is the
documented future path). Document whichever is built. Tests: sign→resolve round-trip;
revoked device → resolve fails; wrong/expired token → refused.

### 4. Servicer upload path — StartIntake / UploadFile / ListIntakeFiles

`src/sutradhara/grpc/servicer.py` (start it here): the **per-RPC owner check** (inv. 3);
**StartIntake** (resolve operator from cert; validate artifactclass; reuse
`store.begin_idempotency` scoped `(operator, "grpc:StartIntake", key)` hashing
artifactclass/source_kind/source_ref/label/**source_plan_digest**; same key+identical →
existing intake_id, changed → `FAILED_PRECONDITION`; **store intent + owner in
`grpc_intake`**; mint `YYYYMMDD-<operator-slug>-<UUID>`; create the landing dir +
`.receiving.json` in state `streaming`); **UploadFile** (owner-checked; only in
`streaming`; in-flight++; relpath confinement incl. reject leading `data/`; write to
`.incoming/{uuid}.tmp` → fsync → atomic rename into `data/{relpath}` → fsync dir →
append receipt under per-intake lock; in-flight-- in `finally`; transit sha256 returned;
discard temp on disconnect); **ListIntakeFiles** (owner-checked; read the durable
receipt ledger). `test_owner_check.py` + the StartIntake/UploadFile/ListIntakeFiles parts
of `test_servicer.py` from design §14 (incl. UUID-temp no-collision, temp-outside-`data/`,
leading-`data/` reject, not-`streaming` reject).

### 5. assembly.py + CommitIntake / AbortIntake / GetIntakeStatus

`src/sutradhara/grpc/assembly.py`: build the bag from streamed chunks reusing
`sutradhara_receive` writers — **skew-check** canonicalization/profile vs server
constants, build `bag-info.txt` from the **server-stored StartIntake intent** + Commit
`receive_facts` + always-written constants, write the **single `package-index.json`**
(type-mapped members, inv. 8), then `bagit.txt`/`manifest`/`tagmanifest`/`intake.json`
(sentinel last) atomically, empty `.incoming/`, and remove `.receiving.json`.
`servicer.py` **CommitIntake** (owner-checked; idempotent on
`grpc_intake.manifest_digest` — same digest → live status, different → conflict; CAS
`streaming → committing` rejecting in-flight uploads; per-unit transit cross-check;
recoverable failure → rollback `committing → streaming` + `reupload_relpaths`; on success
assemble + set `committed` + store digest); **AbortIntake** (owner-checked;
state-gated to `streaming`/`committing`, `FAILED_PRECONDITION` once `committed`; drop the
dir incl. `.incoming/`; set `aborted`; `release_idempotency`); **GetIntakeStatus**
(owner-checked; read watcher terminal markers, else `grpc_intake.state`:
`committed`→`verifying`). Add `release_idempotency` to `src/sutradhara/api/store.py`
(delete a *completed* `grpc:StartIntake` row — `abandon_idempotency` is `in_progress`-only).
`test_assembly.py` + the Commit/Abort/GetStatus parts of `test_servicer.py` (design §14),
incl. the non-package `sha256:null` member serialization and `inspect_intake` accepts the bag.

### 6. server.py + `sutra serve-grpc` CLI + sweep

`src/sutradhara/grpc/server.py`: `grpc.server` + `ssl_server_credentials`
(`require_client_auth=True`); **bind the LAN/Tailscale interface, refuse `0.0.0.0`/`::`/
non-loopback-non-tailnet** (inv. 10). `sutra serve-grpc` in `src/sutradhara/cli/main.py`
(`--bind`, `--port 50051`, `--pki-dir`, `--issue-enroll-token`, `--revoke-device`); run
the `.receiving.json` stale-sweep as a periodic task (reuse the agent's sweep semantics).
Tests: bind rejects public addresses; serve-grpc starts on a UDS/loopback test port with
a self-signed CA.

### 7. Shared payload planner (extraction) + stat guard

Extract a standalone **"yield payload units"** planner from `sutradhara_receive` (package
normalization, symlink policy, NFC canonicalization, skipped-count) **carrying the source
mutation stat-guard** into `unit.byte_chunks()` (so a source changing mid-read fails the
unit). Reuse the planning *logic*, never duplicate it; no behavioural change to existing
`sutradhara_receive` callers. Each unit exposes: `relpath`, `hint_size` (0 for packages),
`byte_chunks(n)` (regular read or on-the-fly `package-tar-v1`), `package_index(tar_sha)`
(full `PackageIndex` or `None`). Tests: a `.fcpbundle` yields one package unit + index;
a mutated source file fails its unit; planner digest is metadata-only.

### 8. `grpc_client.py` streaming client + config + enroll + resume

`packages/sutra-agent/src/sutra_agent/grpc_client.py`: the **threaded** sync uploader
(`ThreadPoolExecutor(max_workers=parallelism)`, default 8) — plan first → `StartIntake`
with `source_plan_digest` → `ListIntakeFiles` → **warm/cold resume trust gate** (warm =
local `AgentLedger` `idempotency_key → {intake_id, plan_digest}` match → skip; cold →
re-hash skipped units) → parallel `UploadFile` (in-flight sha256, assert == receipt) →
`CommitIntake` (manifest_digest, receive_facts, package_indexes) → poll `GetIntakeStatus`
until verified/quarantined/discrepancy; on `reupload_relpaths` re-upload + re-commit.
`config.py` (`server_address`, cert paths, `device_id`; `landing` optional; **no
`operator`** in streaming mode), `ledger.py` (record `idempotency_key →
{intake_id, plan_digest}`), `receive.py` (route to `grpc_client` when `server_address`
set), `cli.py` (`--server`/`--client-cert`/`--ca-cert`, `enroll`). Tests: happy path vs a
local in-process test server (self-signed CA); warm resume skips without re-hash; cold
resume re-hashes; different card → StartIntake conflict; transit mismatch → error;
parallelism capped; package indexes sent and match the core schema.

### 9. End-to-end + INDEX

One in-repo end-to-end test: `sutra serve-grpc` (self-signed test CA) + the **real
`sutra intake watch`** → `sutra-agent receive --server …` against a `--fake-source` →
assert the watcher writes `intake.verified.json` and the intake registers + passes
`sutra intake inspect`; and a quarantine/discrepancy case blocks "safe to eject". Flip
`docs/design-streaming-intake-grpc.md` and this prompt to `implemented` in INDEX; note
the live `~/system` harness scenario as the remaining follow-up.

---

## Definition of done

`uv run pytest -q` (the **full suite**, per `AGENTS.md`) green with output pasted; a
`sutra-agent receive --server …` against `serve-grpc` lands a bag that the real `sutra
intake watch` registers and marks `intake.verified.json` (card held until then); single
read of the source (planning is zero-read); per-RPC owner checks, the durable
`grpc_intake` state, atomic temps outside `data/`, skew guards, and the single
`package-index.json` all hold; no grpc dep leaks into `sutradhara-receive`; the tree is
committed; `docs/INDEX.md` updated. A live `~/system` harness scenario is a **follow-up**.
