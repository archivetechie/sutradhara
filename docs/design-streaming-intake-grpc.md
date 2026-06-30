# Design — streaming card/drive intake over gRPC + mTLS (`sutra-agent`)

> Status: **current** (brainstorm 2026-06-30, Claude + the owner; **five codex document
> review rounds folded — 30 findings**, trail in §16). **Prerequisite:**
> `sutra intake watch` (`design-intake-watch.md`) must be built first. Companion to
> `design-receive-front-door.md`
> (the BagIt core + payload planner this design reuses) and
> `design-operator-identity-authz.md` (the HTTP API surface this design runs
> alongside, not through). The implementation prompt will live here when cut.

## 1. Goal & non-goals

**Goal:** operator plugs a camera card or USB/Thunderbolt drive into a Mac or Windows
workstation on the 10G LAN (or Tailscale for the PoC), opens a thin GUI or runs
`sutra-agent receive`, and the footage is on the archive server — hashed, bagged,
integrity-verified, ready for `sutra intake accept` — without any command the operator
has to type, any network share they have to mount, or any local temporary copy of the
footage.

**Non-goals now:** the operator GUI (deferred — thin shell over `sutra-agent`
CLI/gRPC, separate design); any change to the existing `sutra serve-api` / HTTP API
surface (it stays as-is for the browser receive flow); RDMA or kernel-bypass
networking (10G TCP/HTTP2 is sufficient; card I/O is the bottleneck not the
protocol); mid-file byte-level resume (v1 re-uploads the partial file; `offset` is in
the proto for v2); **building `sutra intake watch`** — this design *depends* on that
registrar but does not implement it (it is a hard prerequisite, §12, §15 Q21).

## 2. Why gRPC + mTLS (not HTTPS / rsync / SMB)

| Option | Why rejected |
|---|---|
| rsync → server | Two passes: card→laptop local buffer, then laptop→server. Operators complain cards take twice as long before they can hand them back. |
| SMB share + rsync | Network mount required on every operator workstation. No operator should need to mount a share to do their job. |
| HTTPS / HTTP2 chunked upload | Viable transport, but Authentik bearer tokens are the wrong auth model for a headless background agent (token expiry, rotation). The stack already uses gRPC+mTLS in remanence; consistency wins. |
| Fountain codes / QUIC | Fountain codes are for lossy broadcast channels — a wired 10G LAN has near-zero packet loss; encoding overhead is wasted. QUIC benefits lossy/high-latency links; TCP with large buffers is better for sustained large-file transfer on stable LAN. |
| gRPC + mTLS | **Single pass** (card→server direct). Machine identity via cert (no token). Streaming RPCs are purpose-built for large binary transfer. Flow control via HTTP/2 is built in. Consistent with remanence. Works on Mac and Windows (Python `grpcio`). |

## 3. Architecture overview

Two separate ingress surfaces on sutradhara, each with its own auth model:

```
Browser (system-ui)
  → Caddy :443 → Authentik forward-auth → sutradhara HTTP API
    Auth: Authentik session cookie → X-Authentik-* headers
    Surface: /api/session, /api/receive, /api/receive/options

sutra-agent (Mac / Windows workstation)
  → gRPC + mTLS → sutradhara :50051
    Auth: client (device) certificate (CN = device_id) → device→operator mapping (§6)
    Surface: IntakeService (StartIntake, UploadFile, CommitIntake, …)
```

The gRPC port binds to the LAN/Tailscale interface only — never the public NIC —
matching the nftables `public_guard` model already in place.

The HTTP API and gRPC service **share** the catalog (SQLite via SQLAlchemy) and the
BagIt assembly core (`sutradhara_receive`). They do not share auth or ingress. A
committed gRPC intake is **indistinguishable** from a local `sutra receive` bag, so the
**same** `sutra intake watch` registrar verifies and registers both — the gRPC server
never registers or writes terminal markers itself (§7).

## 4. Single-pass streaming — the key design decision

Old model (two passes):

```
Card → hash → write BagIt to laptop local disk   (pass 1: card I/O + laptop disk write)
     → rsync local bag to server                  (pass 2: laptop disk read + network write)
```

New model (one pass):

```
Card → hash in memory → stream chunks over gRPC → server writes BagIt
```

Single read of the card. Single write on the server. No local temp directory. Time =
`card_size / min(card_read_speed, 10G_LAN_bandwidth)`. For a CFexpress card at
1.8 GB/s into a 10G LAN (1.25 GB/s), the network is the ceiling. For slower SD cards
(300 MB/s), the card is the ceiling. Either way: one pass, not two.

The tradeoff: the local verify step (re-read from laptop NVMe, compare sha256 before
sending) is not possible without a local copy. This is replaced by:
1. Server returns `server_sha256` in every `UploadFile` receipt — client cross-checks
   immediately (catches transit corruption).
2. `sutra intake watch` (the existing registrar — not a gRPC-side job, §7) re-reads
   every landed file and checks against the manifest before issuing
   `intake.verified.json` (catches disk write corruption).
3. Card held until (2) completes — the fail-safe release model from
   `design-receive-front-door.md §11.7` is preserved.

This matches the `--server-confirm-only` fast path anticipated in `§11` of the
receive front-door design.

## 5. Proto definition

File: `packages/sutra-agent/proto/intake.proto`. Generated Python stubs committed
alongside.

```protobuf
syntax = "proto3";
package sutradhara.intake;

// EVERY RPC resolves the peer cert → (device_id, operator) and — for all RPCs that
// take an intake_id — requires it to match the intake's stored owner (PERMISSION_DENIED
// otherwise). A valid device cert is not authority over an arbitrary intake.
service IntakeService {
  // Mint an intake id, store its owner (operator, device_id), create the landing dir.
  rpc StartIntake (StartIntakeRequest) returns (StartIntakeResponse);

  // Stream one file from client to server. One RPC per file; N files in parallel
  // via HTTP/2 stream multiplexing on the same gRPC channel.
  rpc UploadFile (stream FileChunk) returns (FileReceipt);

  // Return files already fully received — used by the client on resume.
  rpc ListIntakeFiles (ListIntakeFilesRequest) returns (ListIntakeFilesResponse);

  // Seal the bag: write BagIt tag files + sentinel, hand off to `sutra intake watch`.
  // Idempotent on (intake_id, manifest_digest). A recoverable failure (sha mismatch /
  // missing unit) rolls the intake back to `streaming` so the client can re-upload.
  rpc CommitIntake (CommitIntakeRequest) returns (CommitIntakeResponse);

  // Report status from the watcher's terminal markers, falling back to gRPC state.
  rpc GetIntakeStatus (IntakeStatusRequest) returns (IntakeStatusResponse);

  // Unrecoverable give-up, allowed ONLY before committed (before intake.json exists):
  // drops the partial intake dir + frees the StartIntake idempotency key. After commit
  // the bag belongs to `sutra intake watch` — Abort then returns FAILED_PRECONDITION.
  rpc AbortIntake (AbortIntakeRequest) returns (AbortIntakeResponse);
}

// ── StartIntake ──────────────────────────────────────────────────────────────

message StartIntakeRequest {
  string idempotency_key   = 1;  // UUID — same dedup model as HTTP /api/receive
  string artifactclass     = 2;
  string source_kind       = 3;  // card | drive | upload | handoff | download | other
  string source_ref        = 4;  // optional: card serial / drive label
  string label             = 5;  // optional: human label for this intake
  // Digest binding this intake to a SPECIFIC source. sha256 over the sorted list of
  // {relpath, size, mtime_ns} over planned units — METADATA ONLY, no content read, so
  // planning stays zero-read and the "single pass" claim (§4) holds and a package tar
  // is still produced exactly once (front-door §12.1). {relpath,size} alone was too
  // weak; mtime_ns adds real discrimination but is NOT an absolute guarantee — the
  // mixed-source safety net is the resume trust gate (§11): the client's local resume
  // ledger proves same-run identity (warm), and cold resume re-hashes skipped units.
  string source_plan_digest = 6;
}

// These five intent fields are the AUTHORITY for bag-info.txt — stored server-side
// at StartIntake and NOT restated by the client at CommitIntake (avoids a second,
// divergent source of truth). operator is resolved from the device→operator mapping.

message StartIntakeResponse {
  string intake_id = 1;  // YYYYMMDD-<operator-slug>-<UUID>
}

// ── UploadFile ───────────────────────────────────────────────────────────────

message FileChunk {
  string intake_id = 1;
  // POSIX relpath relative to BagIt data/, e.g. "A001/clip001.mxf".
  // The wire value is ALWAYS relative to data/ and NEVER carries a leading
  // "data/" component — the server writes it under data/{relpath}, so a leading
  // "data/" would produce data/data/... The server rejects a leading "data/".
  // NFC-normalised + escaped per the shared canonicalization (design-receive-front-door §6).
  string relpath   = 2;
  bytes  data      = 3;         // raw bytes; 4 MB default chunk size
  int64  offset    = 4;         // byte offset within file (reserved for v2 mid-file resume)
  bool   is_last   = 5;         // true on the final chunk for this relpath
  // Optional progress HINT only — never load-bearing (the server streams until is_last
  // regardless). First chunk only; 0 = unknown. A regular file sends its stat size; a
  // PACKAGE sends 0 (the deterministic tar size isn't known until it is produced) unless
  // the client cheaply precomputes the pax tar size. Do NOT require it for packages.
  int64  file_size = 6;
}

message FileReceipt {
  string relpath        = 1;
  string server_sha256  = 2;  // sha256 the server computed from received bytes
  int64  received_bytes = 3;
}

// ── ListIntakeFiles ──────────────────────────────────────────────────────────

message ListIntakeFilesRequest  { string intake_id = 1; }
message ListIntakeFilesResponse { repeated FileRecord files = 1; }

message FileRecord {
  string relpath       = 1;
  string server_sha256 = 2;
  int64  bytes         = 3;
}

// ── CommitIntake ─────────────────────────────────────────────────────────────

message CommitIntakeRequest {
  string                 intake_id       = 1;
  repeated ManifestEntry files           = 2;
  // CommitIntake does NOT restate artifactclass/source_kind/source_ref/label —
  // those are the StartIntake intent (server-stored authority for bag-info.txt).
  // Commit carries only receive-DETERMINED facts + package indexes (below).
  ReceiveFacts           receive_facts   = 3;
  repeated PackageIndex  package_indexes = 4;
  // sha256 over the canonical manifest (sorted {relpath, client_sha256, bytes}).
  // Makes Commit idempotent: same (intake_id, manifest_digest) → returns the current
  // status; a DIFFERENT digest for an already-committed intake → FAILED_PRECONDITION.
  string                 manifest_digest = 5;
}

message ManifestEntry {
  string relpath       = 1;  // stored name (a package's is "<name>.<ext>.tar")
  string client_sha256 = 2;  // sha256 the client computed while reading the card
  int64  bytes         = 3;
}

// Receive-determined facts (NOT bound at StartIntake — known only after planning the
// source). Folded into bag-info.txt alongside the server-stored StartIntake intent.
message ReceiveFacts {
  // CANONICALIZATION_VERSION the client planner used. The server REJECTS a value that
  // differs from its own constant BEFORE writing the bag (skew guard) — the receive
  // core validates this field, so a stale value would otherwise quarantine the bag at
  // the watcher. Same discipline as package_profile_version.
  string canonicalization_version = 1;
  int64  skipped_count            = 2;  // symlinks / special files intentionally skipped
  // The planner's package-profile version the client used. The SERVER writes the
  // Package-Profile-Version / -Hash constants into bag-info.txt UNCONDITIONALLY (as
  // the receive core's bag_info_metadata does — never "" , never absent when a package
  // exists). This field is a VERSION-SKEW ASSERTION only: the server rejects a value
  // that differs from its own PACKAGE_PROFILE_VERSION constant. Empty ⇒ "same as
  // server" (no package wrapped); never sent as a literal "".
  string package_profile_version  = 3;
}

// Inner index for the normalized packages in this intake. The server writes these as
// the SINGLE `package-index.json` tag file the receive core expects (out of the
// payload manifest; each package is one manifest entry, per front-door §12.4). The
// schema below matches `core.read_package_index`: top-level profile / profile_hash are
// the server's constants; each package carries logical_member_path, stored_member_path,
// sha256, and members.
message PackageIndex {
  string                      logical_member_path = 1;  // "A001.fcpbundle"  (natural name)
  string                      stored_member_path  = 2;  // "A001.fcpbundle.tar" (== manifest relpath)
  string                      sha256              = 3;  // sha256 of the package tar (its identity)
  repeated PackageMemberEntry members             = 4;
}
// Field names + values mirror the receive core's member record EXACTLY
// (core.py `_PackageTarResult` members): keys member / type / length / sha256 /
// data_offset, plus linkname for symlinks. type ∈ {"file","directory","symlink"}.
// `optional` gives proto3 explicit presence so absent ≠ default ("" / 0) — but the
// server does NOT depend on wire presence; it derives the JSON deterministically BY
// TYPE (see the mapping rule below the message), so a non-file member always lands as
// JSON null regardless of what the client sends.
message PackageMemberEntry {
  string          member      = 1;  // POSIX path inside the package tar (core key: "member")
  string          type        = 2;  // "file" | "directory" | "symlink"  (core's type_name)
  int64           length      = 3;  // file: byte length; non-file: 0
  optional string sha256      = 4;  // file: hex; non-file: → JSON null
  optional int64  data_offset = 5;  // file: tar byte offset; non-file: → JSON null
  optional string linkname    = 6;  // symlink: target; else → JSON absent
}
// Proto → package-index.json member mapping (authoritative — the server applies this,
// it does NOT trust proto presence to decide nullness):
//   type=="file":   {member, type, length, sha256:<hex>, data_offset:<int>}
//   type!="file":   {member, type, length:0, sha256:null, data_offset:null}
//   type=="symlink": additionally  linkname:<target>
// This reproduces core.py's records byte-for-byte (sha256/data_offset None for
// non-file; linkname only on symlinks).

message CommitIntakeResponse {
  string          intake_id = 1;
  string          status    = 2;  // "verifying" on success; current status on idempotent retry
  // On a recoverable failure the server rolls back to `streaming` and returns the
  // units the client must re-upload before re-committing (empty on success).
  repeated string reupload_relpaths = 3;
}

// ── GetIntakeStatus ──────────────────────────────────────────────────────────

message IntakeStatusRequest  { string intake_id = 1; }

message IntakeStatusResponse {
  string          intake_id = 1;
  // gRPC-owned, pre-handoff: streaming | committing | verifying
  // watcher-owned terminal markers: verified | quarantined | discrepancy
  string          status    = 2;
  repeated string errors    = 3;  // populated when quarantined/discrepancy: bad relpaths + reason
}

// ── AbortIntake ──────────────────────────────────────────────────────────────

message AbortIntakeRequest  { string intake_id = 1; }
message AbortIntakeResponse { string intake_id = 1; string status = 2; }  // status = "aborted"
```

## 6. mTLS — certificate model (device identity, mapped to operator)

The client certificate is **device identity**, not operator identity — the two are
deliberately split. A cert authenticates *which enrolled workstation is connecting*;
a server-side enrollment table maps that device to *which operator's intakes it
stamps*. This avoids the earlier contradiction (a cert can't simultaneously be "the
machine" and "CN = the person"), and it means a lost laptop is revoked without
touching the operator, and an operator could have more than one enrolled device.

**CA:** a sutradhara-internal CA. Generated on first `sutra serve-grpc` if absent;
stored at `/etc/sutradhara/pki/{ca.crt,ca.key}` (mode 0600 for key). Separate from
the Caddy internal CA — different trust scope.

**Server certificate:** issued by the sutradhara CA for the server's hostname /
Tailscale IP / LAN IP. Rotated annually.

**Client (device) certificate:** one per workstation. `CN = device_id` (e.g.
`owner-macbook-01`), **not** a person. The cert's SHA-256 fingerprint is the durable
device handle.

**Device → operator mapping (the authority for operator identity):** at enrollment,
the server records a row `(device_id, cert_fingerprint) → operator_username`,
authorized by an admin (the `--admin-token` below). The `IntakeServicer` reads the
verified `device_id`/fingerprint from the peer TLS certificate, looks up the operator
in this table, and stamps **that** server-controlled value as `Intake.operator`. No
request carries an `operator` field (StartIntake binds intent, Commit carries only
receive facts) and the server never trusts a client-supplied one.
This preserves the core invariant from `design-operator-identity-authz.md` — operator
identity is **server-assigned, never client-asserted** — the binding just happens at
enrollment (admin-authorized) instead of per-request (Authentik session).

**v1 assumption (documented):** one device : one operator. Each of the two operators
has a personal Mac, so the mapping is 1:1 and `Intake.operator` is unambiguous. A
**shared** ingest workstation would need per-transfer operator selection (e.g. the GUI
carrying an Authentik-issued operator token alongside the device cert) — a documented
future path, out of scope for v1 (Open Question 1, §15).

**Issuance (enroll command):**
```
# On the operator workstation — key never leaves the machine
sutra-agent enroll \
  --server 100.81.52.26:50051 \
  --device-id owner-macbook-01 \   # CN of the device cert
  --operator owner \               # who this device's intakes are stamped as
  --admin-token <one-time token>   # admin-authorizes the device→operator mapping

# Generates client key + CSR locally (key never leaves the machine)
# POSTs CSR + requested device→operator mapping to a one-time HTTPS enrollment endpoint
# Server CA signs the cert AND records (device_id, fingerprint) → operator
# Returns the signed cert
# Stored: ~/.config/sutra-agent/{client.crt,client.key,ca.crt}  (Mac/Linux)
#         %APPDATA%\sutra-agent\{client.crt,client.key,ca.crt}   (Windows)
```

The `--admin-token` is a short-lived (24h) one-time token generated by the operator
running `sutra serve-grpc --issue-enroll-token`. It gates CSR signing; once used it
is invalidated. Enrollment is a one-time setup per machine.

**Revocation:** on a lost machine, run `sutra serve-grpc --revoke-device owner-macbook-01`
(removes the device→operator mapping and blocks the cert fingerprint; checked on each
RPC). For a lost CA, re-generate and re-enroll all machines — small team, acceptable.
CRL is deferred.

## 7. Server-side gRPC service

### File structure (new)

```
src/sutradhara/grpc/
  __init__.py
  server.py          — gRPC server lifecycle; mTLS channel credentials; bind address
  servicer.py        — IntakeServicer: six RPC implementations (incl. AbortIntake)
  assembly.py        — BagIt bag assembly from streamed chunks; reuses sutradhara_receive writers
  ca.py              — CA / cert issuance helpers (sign CSR, device→operator mapping, revoke device)
packages/sutra-agent/proto/
  intake.proto       — the definition above
  intake_pb2.py      — generated (committed)
  intake_pb2_grpc.py — generated (committed)
```

### `server.py` — bind and mTLS

```python
def make_server(config: GrpcConfig) -> grpc.Server:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    intake_pb2_grpc.add_IntakeServiceServicer_to_server(IntakeServicer(config), server)
    creds = grpc.ssl_server_credentials(
        [(config.server_key, config.server_cert)],
        root_certificates=config.ca_cert,
        require_client_auth=True,   # mTLS: reject any client without a valid cert
    )
    server.add_secure_port(f"{config.bind_address}:{config.port}", creds)
    return server
```

Bind address defaults to the Tailscale/LAN interface IP, not `0.0.0.0`. nftables
already drops traffic on the public NIC; the bind restriction is a defence-in-depth
layer.

### `servicer.py` — RPC implementations

**Owner check on EVERY RPC (not just StartIntake).** `UploadFile`, `ListIntakeFiles`,
`CommitIntake`, `GetIntakeStatus`, and `AbortIntake` all take only an `intake_id`. A
valid mTLS cert authenticates an *enrolled device* — but that is **not** authority to
touch an arbitrary intake. So every RPC resolves the peer cert → `(device_id,
operator)` (§6) and requires it to match the intake's **stored owner**: StartIntake
records `(operator, device_id)` on the intake; every later RPC rejects with
`PERMISSION_DENIED` unless the caller's resolved operator (and device, for v1's 1:1
mapping) equals the stored owner. Without this, a second enrolled operator who learns
or guesses an `intake_id` could upload into, commit, read, or abort someone else's
intake. The `intake_id`'s embedded UUID makes guessing hard, but the check is the
actual control — unguessability is not relied upon.

**Durable intake-state store (survives commit — NOT just `.receiving.json`).** The
authoritative owner + state + committed digest live in a small durable table,
`grpc_intake` (SQLite, same catalog engine): `intake_id PK, operator, device_id,
state, manifest_digest NULL, created_at, updated_at`. This is **separate from**
`.receiving.json`, which is only the filesystem lifecycle hint the watcher/sweep read
and which is **removed at commit**. Owner checks, the `manifest_digest` retry check,
`GetIntakeStatus`, and the Abort state-gate all read this table, so they keep working
**after** `.receiving.json` is gone. (`.receiving.json` mirrors `state` for the
watcher; the table is the source of truth.)

**Per-intake state machine (gates concurrency).** `grpc_intake.state` runs
`streaming → committing → committed` (and `aborted`). `UploadFile` is accepted only in
`streaming`; `CommitIntake` atomically flips `streaming → committing` (compare-and-set
on the row under a per-intake lock) and is **rejected if any `UploadFile` for that
intake is still in flight** (the server tracks an in-flight counter per intake under
the same lock) — so the bag is never sealed mid-write. The `verifying → verified |
quarantined | discrepancy` states are **not gRPC-owned** — they belong to `sutra intake
watch` (the single registrar, below); `GetIntakeStatus` reports them by reading the
landing markers the watcher writes, falling back to `grpc_intake.state`.

**The gRPC server does NOT verify or register — it hands off to `sutra intake watch`.**
A `CommitIntake` produces **exactly** what a local `sutra receive` produces: a
complete BagIt bag with the `intake.json` sentinel present and `.receiving.json`
removed. The existing `sutra intake watch` registrar then inspects, registers, and
writes the terminal marker (`intake.verified.json` / `intake.quarantined.json` /
`intake.discrepancy.json`). The gRPC path must **not** write those markers itself —
if it wrote `intake.verified.json`, the watcher would see a terminal marker and
**never register the intake into the catalog** (`design-intake-watch.md §5`). One
registrar, one place that writes terminal markers.

**`StartIntake`:** resolves the operator from the peer cert (the device→operator
enrollment mapping, §6); validates artifactclass against the registry (same check as
HTTP `POST /api/receive`); applies the **same durable idempotency contract the HTTP
API uses** (`store.begin_idempotency`): the record is scoped to
`(operator_username, method="grpc:StartIntake", idempotency_key)` and binds a
**canonical hash** of the request fields (`artifactclass`, `source_kind`,
`source_ref`, `label`, **`source_plan_digest`**). Same key + identical request ⇒
returns the **existing** `intake_id`. Same key + a **different** request (changed
artifactclass / source_ref / **a different card → different plan digest**) ⇒
`FAILED_PRECONDITION` conflict — never silently attaches to the old intake. **The five
intent fields are stored server-side here** and are the authority for `bag-info.txt`
(the client does not restate them at Commit). A first call mints the intake id
(`YYYYMMDD-<operator-slug>-<UUID>`), **inserts the `grpc_intake` row** (`operator`,
`device_id` from the cert, `state="streaming"`, `manifest_digest=NULL`) — the durable
owner+state every later RPC checks — and creates `/replica/landing/{intake_id}/` with a
`.receiving.json` marker mirroring `state`. Returns `intake_id`.

**`UploadFile`:** owner-checked; accepted only while the intake is `streaming` (else
`FAILED_PRECONDITION`); increments the in-flight counter under the per-intake lock.
Reads the client-streaming `FileChunk` sequence for one unit; validates `relpath`
confinement on the first chunk (POSIX, NFC-normalised, **rejects a leading `data/`
component** — the wire value is relative to `data/` — plus no `..`, no absolute path,
must canonicalize to stay inside `data/`); writes chunks to a **per-RPC UUID temp path
in a staging dir OUTSIDE `data/`** (`{intake}/.incoming/{uuid4}.tmp` — UUID so two
same-relpath streams can't collide, **and outside `data/` so a server crash can't leave
a `*.tmp` under `data/` that the receive validator would hash as an extra payload file
and quarantine the bag**), computes sha256 rolling hash; on `is_last=true`: `fsync`,
atomic `rename` from `.incoming/` into `data/{relpath}`, `fsync` parent dir, then
**appends a receipt line** (`{relpath, server_sha256, bytes}`) to the durable per-file
receipt ledger `receive-receipts.jsonl` **holding the per-intake ledger lock**
(serialized appends; last write for a relpath wins on read); decrements the in-flight
counter; returns `FileReceipt` with `server_sha256`. On disconnect mid-stream: the
`.incoming/` temp is discarded (never renamed, so no receipt line is written) and the
in-flight counter is decremented in a `finally`. **Stale-temp sweep:** `CommitIntake`
(and the resume path) empties `.incoming/` before sealing, and the periodic sweep
clears `.incoming/` of any crash-orphaned temps — `data/` only ever holds renamed,
receipted payload files.

**`ListIntakeFiles`:** owner-checked; returns `{relpath, server_sha256, bytes}` read
from the durable `receive-receipts.jsonl` ledger (a file counts as received only if it
has a receipt line; on duplicate relpath lines the last wins). Used by the client on
resume to skip already-landed units **without re-hashing** the landed bytes. (Crash
window: a file renamed but not yet logged has no receipt line, so the client re-uploads
it and the server overwrites with identical bytes — safe.)

**`CommitIntake`:** owner-checked. **Its idempotency is NOT the HTTP
`begin_idempotency` store** — that store keys on an `idempotency_key` and replays a
**frozen `response_json`**, but `CommitIntakeRequest` carries no such key and a
re-Commit must reflect the **live** watcher status, not a frozen response. So Commit
idempotency is enforced by the **intake's own committed-state record**: the
`committed` intake stores its `manifest_digest`; a re-Commit with the **same**
`manifest_digest` re-reads and returns the **current** status (a retry after a
post-commit timeout is safe); a **different** `manifest_digest` on an already-committed
intake ⇒ `FAILED_PRECONDITION`. (StartIntake still uses `begin_idempotency`, whose
frozen `{intake_id}` response is correct to replay; Commit does not.) On a fresh
commit it atomically flips `streaming → committing` (rejecting if any `UploadFile` is
still in flight) and validates `client_sha256 == server_sha256` for every manifest
file (cross-checks the per-file transit integrity).
- **On a recoverable failure** (sha mismatch, or a unit missing from the receipt
  ledger), the server **rolls back `committing → streaming`** and returns the offending
  relpaths, so the client can re-`UploadFile` the bad/missing units and re-`CommitIntake`
  (uploads are accepted again in `streaming`). Without this rollback a failed commit
  would strand the intake in `committing` with uploads rejected and no repair path.
- **On success**, the server first **rejects skew** — `receive_facts.canonicalization_version`
  and (if non-empty) `package_profile_version` must equal the server's own
  `CANONICALIZATION_VERSION` / `PACKAGE_PROFILE_VERSION` constants, else
  `FAILED_PRECONDITION` **before writing the bag** (the receive core validates both
  fields, so a stale value would otherwise quarantine the bag at the watcher). Then it
  builds `bag-info.txt` **from the server-stored StartIntake intent + the Commit
  `receive_facts`** (operator from the device→operator mapping), **always** writing the
  `Package-Profile-Version` / `Package-Profile-Hash` and `Canonicalization-Version`
  **constants** from its own receive core (never `""`, never absent) — matching
  `core.bag_info_metadata`. When packages were wrapped, it writes the **single**
  `package-index.json` tag file from the `package_indexes` (top-level
  `profile`/`profile_hash` = the constants; one `packages[]` entry per `PackageIndex`,
  members mapped per the proto→JSON rule in §5), exactly the schema
  `core.read_package_index` validates. Then `bagit.txt`, `manifest-sha256.txt`,
  `tagmanifest-sha256.txt`, and `intake.json` (sentinel, written last); **sets
  `grpc_intake.state="committed"` + stores `manifest_digest`**, then **removes
  `.receiving.json`**. Returns `status: "verifying"` (the watcher hasn't processed yet).
- An explicit **`AbortIntake`** path (owner-checked) handles an unrecoverable give-up.
  It is **state-gated: allowed only while `grpc_intake.state ∈ {streaming, committing}`**
  (i.e. before `intake.json` exists); once `committed` the bag has been handed off and
  Abort returns `FAILED_PRECONDITION` — deleting a committed bag would race or break
  `sutra intake watch`. When allowed: drop the partial intake dir (including
  `.incoming/`), set `state="aborted"`, and free the StartIntake idempotency record so
  the **same `idempotency_key` can start a clean intake**. The existing
  `store.abandon_idempotency` only deletes `in_progress` rows and a minted intake's
  record is `completed`, so Abort calls a small new helper (`release_idempotency`:
  delete the `completed` row by `(operator, "grpc:StartIntake", key)`); without it the
  key would forever replay the dead `intake_id`. After release, a re-`StartIntake` with
  the same key re-mints.

**`GetIntakeStatus`:** owner-checked; reports the landing markers written by
`sutra intake watch`: `intake.verified.json` → `verified`,
`intake.quarantined.json` → `quarantined`,
`intake.discrepancy.json` → `discrepancy`. Before any terminal marker, it reads
`grpc_intake.state`: `streaming`/`committing` → as-is; `committed` (bag handed off, no
terminal marker yet) → `verifying`. The durable row means status survives the removal
of `.receiving.json`. Returns status + error list on quarantine/discrepancy.

### Verification & registration — owned by `sutra intake watch`, not the gRPC server

There is **no gRPC-side verify job**. Re-hash-against-manifest fixity, catalog
registration, and the terminal markers are all `sutra intake watch`'s job
(`design-intake-watch.md`) — the same registrar that handles local `sutra receive`
intakes. The gRPC commit lands a complete bag and stops; the watcher (a running
daemon) picks it up by its `intake.json` sentinel. This removes the duplicate verify
logic and the marker race, and means a gRPC intake and a card dropped via local
`sutra receive` are **indistinguishable downstream**. The agent's "card held until
verified" gate polls `GetIntakeStatus` until the watcher's `intake.verified.json`
lands — identical to the existing `sutra-agent` confirmation model.

> **Prerequisite — `sutra intake watch` is not yet implemented.** The intake CLI today
> has only `inspect` / `register` / `accept` / `prepare`; `watch` is designed
> (`design-intake-watch.md`, status *for review*) but unbuilt. This streaming design
> **depends** on it as the registrar that writes the terminal markers. So `intake
> watch` must be approved and implemented **before** this lands — it is a hard
> prerequisite, tracked as such (§15 Q21), not an "unchanged, already-present" reuse.

### CLI

```
sutra serve-grpc [--bind <ip>] [--port 50051] [--pki-dir /etc/sutradhara/pki]
sutra serve-grpc --issue-enroll-token     # prints a 24h one-time token
sutra serve-grpc --revoke-device <id>     # drop device→operator mapping + block fingerprint
```

Registered in `cli/main.py` alongside the existing `serve-api`.

## 8. Client-side — sutra-agent streaming mode

### Config (`~/.config/sutra-agent/config.json`)

Two mutually exclusive modes:

```jsonc
// Streaming mode (new — server required)
{
  "server_address": "100.81.52.26:50051",
  "client_cert": "~/.config/sutra-agent/client.crt",   // device cert (CN = device_id)
  "client_key":  "~/.config/sutra-agent/client.key",
  "ca_cert":     "~/.config/sutra-agent/ca.crt",
  "device_id":   "owner-macbook-01",   // informational; the device cert CN is authoritative
  "source_kind": "card",
  "artifactclass": "s-masters",
  "parallelism": 8,
  "chunk_bytes": 4194304           // 4 MB
}

// Legacy landing mode (existing — kept for local/dev use)
{
  "landing": "/replica/landing",
  ...
}
```

There is **no `operator` field** in streaming mode — the operator is resolved
server-side from the device→operator enrollment mapping (§6), never set by the client.
(Legacy landing mode still carries `operator`, since the local edge CLI has no cert.)

`sutra-agent config init` gains `--server` / `--client-cert` / `--client-key` /
`--ca-cert` / `--parallelism` flags; `--landing` and `--server` are mutually
exclusive.

### Payload planning — reuse the shared planner, do NOT raw-walk

The client must **not** do a naive `os.walk` of the source. The shared
`sutradhara_receive` core already owns the payload-planning contract that the
front-door design defines, and the streaming client reuses it unchanged so the edge
and server agree on what a payload entry *is*:
- **macOS package normalization** (`design-receive-front-door.md §12`): a directory
  matching `package_globs` (`*.fcpbundle`, `*.photoslibrary`, `*.imovielibrary`,
  `*.app`) is **one** payload entry — the pinned deterministic `package-tar-v1`, not
  thousands of inner files. Streaming it raw would explode the manifest and the
  catalog, defeating §12.
- **symlink / special-file policy, skipped-count, NFC canonicalization** (§6, §11.6)
  all live in the planner too.
- **source mutation guard** (`design-receive-front-door.md §11.4`): the front-door
  contract `stat`s each source file (size, mtime, inode) before and after its read and
  fails the unit if anything changed (the receive core's `_hash_source_with_stat_guard`).
  The extracted payload-unit API **carries this same guard** — `unit.byte_chunks()`
  wraps the read in the stat-before/stat-after check, so a card being actively written
  during receive fails that unit (no receipt) instead of producing a bag that disagrees
  with the source.

So the planner yields a list of **payload units**, each either a regular file or a
normalized package. The streaming client turns each unit into one `UploadFile` RPC:
- a regular file streams its bytes directly (under the stat guard);
- a package streams the `package-tar-v1` bytes produced **on the fly in sorted member
  order** (no full local buffer — tar is a streaming format; the member list is walked
  first to fix the deterministic order, then bytes stream). Its `relpath` is the
  stored name (`<name>.<ext>.tar`); its inner index — per-member `{member, type,
  length, sha256, data_offset, linkname?}` (the core's exact member schema) plus the
  package's `logical_member_path` / `stored_member_path` / tar `sha256` — is collected
  during the stream and sent in `CommitIntake` via the **`PackageIndex` repeated
  field**. The server writes these as
  the **single `package-index.json`** tag file the receive core expects
  (`core.read_package_index`: top-level `profile`/`profile_hash` = server constants,
  one `packages[]` entry per package) — NOT per-package tag files, NOT in the payload
  manifest (the package is one manifest entry, per §12.4). The package tar's sha256 is
  its identity (computed in-flight, cross-checked against the server receipt like any
  file). `Package-Profile-Version`/`-Hash` are **server constants** written
  unconditionally into `bag-info.txt`; `receive_facts.package_profile_version` is only
  a skew assertion (§5).

### Streaming loop (`grpc_client.py`) — threaded sync uploader

gRPC's **synchronous** stubs are used with a `ThreadPoolExecutor` (one worker per
parallel stream). This avoids the `grpc.aio`/sync-stub mismatch entirely — the upload
is I/O-bound (card read + socket write), so threads are the right tool and the GIL is
released during both.

```python
def stream_source(source: Path, config: AgentConfig, idempotency_key: str) -> StreamResult:
    channel = grpc.secure_channel(config.server_address, mtls_creds(config))
    stub = IntakeServiceStub(channel)

    # Plan FIRST (shared planner: regular files AND normalized packages, NOT os.walk),
    # so the source identity is bound to StartIntake. The digest is METADATA ONLY
    # (no content read) — planning stays zero-read, so the package tar is still produced
    # exactly once during upload and the single-pass claim holds.
    plan = plan_payload_units(source)                     # units + skipped_count, no byte reads
    plan_digest = digest_plan(plan)                       # sha256 over sorted {relpath, size, mtime_ns}

    intake_id = stub.StartIntake(StartIntakeRequest(
        idempotency_key=idempotency_key,
        artifactclass=config.artifactclass,
        source_kind=config.source_kind,
        source_ref=config.source_ref,
        label=config.label,
        source_plan_digest=plan_digest,
    )).intake_id

    # Resume: skip units already fully received (durable server ledger). Trust the
    # server-landed set only when the LOCAL resume ledger confirms this is the same run
    # (same intake_id + plan digest recorded on the first attempt). On a cold resume
    # (no local ledger — e.g. a different laptop), revalidate: re-hash skipped units
    # locally before trusting them, trading the O(remaining) win for safety (§11).
    local = agent_ledger.lookup(idempotency_key)
    trusted = bool(local and local.plan_digest == plan_digest and local.intake_id == intake_id)
    landed = {f.relpath: (f.server_sha256, f.bytes)
              for f in stub.ListIntakeFiles(ListIntakeFilesRequest(intake_id=intake_id)).files}
    if not trusted:
        landed = {r: v for r, v in landed.items() if local_rehash(plan, r) == v[0]}
    agent_ledger.record(idempotency_key, intake_id=intake_id, plan_digest=plan_digest)
    units = [u for u in plan if u.relpath not in landed]

    manifest = dict(landed)            # relpath -> (sha, bytes)
    package_indexes = {}               # relpath (== stored_member_path) -> PackageIndex

    # N parallel UploadFile RPCs via a thread pool (sync stubs).
    with ThreadPoolExecutor(max_workers=config.parallelism) as pool:
        futures = {pool.submit(_upload_one, stub, intake_id, u): u for u in units}
        for fut in as_completed(futures):
            relpath, sha, nbytes, pkg = fut.result()      # raises on transit mismatch
            manifest[relpath] = (sha, nbytes)
            if pkg is not None:                            # a normalized package
                package_indexes[relpath] = pkg             # full PackageIndex (logical/stored/sha/members)

    entries = [ManifestEntry(relpath=r, client_sha256=h, bytes=b)
               for r, (h, b) in sorted(manifest.items())]
    commit = CommitIntakeRequest(
        intake_id=intake_id,
        files=entries,
        receive_facts=ReceiveFacts(
            canonicalization_version=CANON_VERSION,
            skipped_count=plan.skipped_count,
            # Skew assertion only; the SERVER writes the profile constants into bag-info.
            # Empty when no package was wrapped — never the literal "".
            package_profile_version=PACKAGE_PROFILE_VERSION if package_indexes else "",
        ),
        package_indexes=[package_indexes[r] for r in sorted(package_indexes)],
        manifest_digest=digest_manifest(entries),         # makes Commit retry-idempotent
    )
    resp = stub.CommitIntake(commit)   # server uses StartIntake-stored intent — no restatement
    if resp.reupload_relpaths:         # recoverable failure: server rolled back to streaming
        return retry_after_reupload(stub, intake_id, plan, resp.reupload_relpaths, idempotency_key)
    return poll_until_done(stub, intake_id, timeout=config.confirm_timeout)


def _upload_one(stub, intake_id, unit):
    """Stream one payload unit (regular file or normalized package) → one UploadFile RPC."""
    sha = hashlib.sha256()
    counter = {"total": 0}   # dict, not a bare local — the generator mutates it safely

    def chunk_iter():
        first = True
        # unit.byte_chunks wraps the read in the stat-before/stat-after mutation guard
        # (§8) — raises if the source file changes underfoot (no receipt for that unit).
        for data in unit.byte_chunks(CHUNK_BYTES):   # regular read, or on-the-fly package tar
            sha.update(data)
            counter["total"] += len(data)
            yield FileChunk(
                intake_id=intake_id, relpath=unit.relpath, data=data,
                offset=counter["total"] - len(data), is_last=False,
                # Hint only. A regular file knows its size; a PACKAGE's tar size isn't
                # known until produced → 0 (unknown). unit.hint_size is 0 for packages.
                file_size=unit.hint_size if first else 0,
            )
            first = False
        yield FileChunk(intake_id=intake_id, relpath=unit.relpath,
                        data=b"", offset=counter["total"], is_last=True)

    receipt = stub.UploadFile(chunk_iter())
    client_sha256 = sha.hexdigest()
    if receipt.server_sha256 != client_sha256:
        raise TransitCorruptionError(unit.relpath, client_sha256, receipt.server_sha256)
    # unit.package_index() is a full PackageIndex (logical/stored/sha256/members) for a
    # package, else None. The package tar's own sha256 == client_sha256 here.
    return unit.relpath, client_sha256, receipt.received_bytes, unit.package_index(client_sha256)
```

Fixes carried here: the broken `asyncio.TaskGroup`/sync-stub mix is gone (thread pool);
the nested-generator `UnboundLocalError` is gone (dict counter); package indexes are
transmitted as the **single `package-index.json` schema** the core expects, not
silently dropped; the manifest restates **no** StartIntake intent; the metadata-only
plan digest plus the local resume ledger keep a resume from mixing two cards (without a
content read at planning); `manifest_digest` makes a Commit retry safe; and the source
mutation guard from the front-door contract rides along in `unit.byte_chunks()`.

### Windows

`grpcio` installs via pip on Windows with no native dependencies. Path handling uses
`pathlib.Path` throughout (POSIX conversion applied at the `relpath` construction
step). The cert paths resolve against `%APPDATA%\sutra-agent\` on Windows.

## 9. Parallel streams

Eight concurrent `UploadFile` RPCs (configurable, default 8) on a single gRPC
channel share the underlying HTTP/2 connection. HTTP/2 multiplexes the streams; the
OS TCP stack manages the shared bandwidth fairly.

**Throughput analysis (10G LAN):**

| Source | Read speed | 10G ceiling | Bottleneck | 8-stream effect |
|---|---|---|---|---|
| CFexpress Type B | ~1800 MB/s | 1250 MB/s | Network | Multiple streams keep pipe full |
| SD UHS-II | ~300 MB/s | 1250 MB/s | Card | 8 streams idle-wait on card |
| USB-C SSD | ~1000 MB/s | 1250 MB/s | Card | 4-5 streams saturate |

Two operators offloading simultaneously each see ~600 MB/s (shared 10G). The
`parallelism` setting can be tuned per-workstation; 8 is a safe default.

gRPC's HTTP/2 flow control prevents a fast card reader from overwhelming a slower
server disk — the server's receive window fills and the client pauses naturally
without application-level rate limiting.

## 10. Integrity chain

```
Card (single read per unit, under the stat-before/after mutation guard)
  → client: sha256 rolling hash (in memory, no local write)
  → chunks sent via UploadFile gRPC stream
  → server: receives chunks, writes temp→fsync→rename, computes server_sha256
  → FileReceipt returned: server_sha256
  → client assert: client_sha256 == server_sha256   ← TRANSIT INTEGRITY
       fail → TransitCorruptionError: abort this unit, record in failed set
  → CommitIntake: client_sha256 per unit + manifest_digest
  → server: assert client_sha256 == server_sha256 per unit (double-checks commit)
       mismatch/missing → rollback committing→streaming, return reupload_relpaths
  → server: writes bag (sentinel last), removes .receiving.json  ← HANDOFF
  → sutra intake watch: re-reads every data/ file, recomputes sha256,
       checks against manifest-sha256.txt, registers into the catalog  ← DISK WRITE INTEGRITY
  → clean   → intake.verified.json
  → fixity  → intake.quarantined.json
  → catalog → intake.discrepancy.json
  → client polls GetIntakeStatus
       verified              → "safe to eject" message
       quarantined/discrepancy → "DO NOT eject — bad units: {list}" + operator action
```

Three independent integrity checks. The disk-write verify + catalog registration are
`sutra intake watch`'s job (one registrar, §7), not a gRPC-side verify. Card held
until `intake.verified.json`.

## 11. Resume model

Connection drops mid-transfer (network blip, laptop sleep, operator walks away):

1. Agent restarts (or operator re-runs `sutra-agent receive <source>`)
2. Client **re-plans the source first** and recomputes `source_plan_digest` —
   `{relpath, size, mtime_ns}`, **metadata only** (no content read, so planning stays
   zero-read and the package tar is produced exactly once at upload). mtime adds
   discrimination over names+sizes but is not a guarantee — step 5 is the real net.
3. `StartIntake` with the same `idempotency_key` + the recomputed digest — server
   returns the existing `intake_id` only if the canonical hash matches (§7). A
   **different card or wrong mount → different plan digest → `FAILED_PRECONDITION`**.
4. `ListIntakeFiles` — server returns fully-received units with their sha256
5. **Trust gate (closes the residual mixed-source gap):**
   - **Warm resume** (the client's own `AgentLedger` records this `idempotency_key →
     {intake_id, plan_digest}` from the first attempt, and both match): trust the
     server-landed set, skip those units **without re-hashing the source** — the
     O(remaining) fast path.
   - **Cold resume** (no local ledger entry — e.g. resuming from a *different* laptop,
     so the digest match alone can't prove same-source): **re-hash each skipped unit
     locally** and trust the server's entry only if the local sha256 matches. Safe,
     but O(landed) read — the explicit price of an uncertain identity.
6. Remaining units are uploaded; `CommitIntake` includes the full manifest
   (already-landed + newly-uploaded)

The absolute "never mixed-source" claim is softened to honest layers: the strengthened
digest blocks accidental collisions; the warm-resume ledger proves same-run identity;
cold-resume revalidation catches the rest. `{relpath,size}`-only was insufficient and
is replaced.

**Durable receipt ledger (what makes resume O(remaining), not O(size)).** The
"fully-received" set is **persisted**, not recomputed by re-hashing `data/` on
restart. Each completed `UploadFile` appends one line to `receive-receipts.jsonl` in
the intake dir — `{relpath, server_sha256, bytes}` — fsync'd, **after** the atomic
rename of the data file. `ListIntakeFiles` reads this ledger. Ordering gives the
correctness guarantee: rename-then-log means a crash between the two leaves a data
file with no receipt → it's treated as not-received → re-uploaded → overwritten with
identical bytes (safe and idempotent). The alternative — re-hashing every landed byte
on restart — is O(intake size) and was explicitly rejected for TB-scale offloads.

**Partial files** (dropped mid-`UploadFile` stream): the server discards the temp
file on disconnect (never renamed, never logged). The relpath is absent from the
ledger / `ListIntakeFiles`. The agent re-uploads the full unit on resume. Mid-file
byte-level resume (using the `offset` field) is deferred to v2.

**Stale intakes** (`.receiving.json` older than 24h with no activity): swept by the
**existing** `sutra-agent receive sweep` command — which already centers on
`.receiving.json`. gRPC intakes share that exact lifecycle marker (with a
`transport: "grpc-stream"` field) rather than forking a parallel `.streaming.json`
state, so no sweep/orphan-detection logic is duplicated. `sutra serve-grpc` also runs
the sweep as a periodic background task.

## 12. What changes in the codebase

### New

| Path | Purpose |
|---|---|
| `packages/sutra-agent/proto/intake.proto` | gRPC service definition |
| `packages/sutra-agent/proto/intake_pb2*.py` | Generated stubs (committed) |
| `src/sutradhara/grpc/server.py` | gRPC server + mTLS setup |
| `src/sutradhara/grpc/servicer.py` | Six RPCs (incl. state-gated `AbortIntake`); **per-RPC owner check (cert → durable `(operator, device_id)`)**; `grpc_intake`-backed `streaming→committing→committed`/`aborted` state machine + in-flight counter + ledger lock + commit rollback; `.incoming/` staging (temps outside `data/`); canonicalization/profile skew guards; `.receiving.json` + `receive-receipts.jsonl` lifecycle; **hands off to `sutra intake watch` — no gRPC verify/register** |
| `src/sutradhara/grpc/store.py` | **New durable `grpc_intake` table** (`intake_id PK, operator, device_id, state, manifest_digest NULL, created_at, updated_at`) — the authoritative owner/state/committed-digest that survives `.receiving.json` removal; + alembic revision |
| `src/sutradhara/grpc/assembly.py` | BagIt assembly from streamed chunks (reuses `sutradhara_receive` writers); skew-checks then builds `bag-info.txt` from server-stored intent + `receive_facts` + always-written canonicalization/profile constants; writes the **single `package-index.json`** the receive core validates (member JSON mapped by type per §5) |
| `src/sutradhara/grpc/ca.py` | CA / cert issuance / device→operator mapping / device revocation |
| `packages/sutra-agent/src/sutra_agent/grpc_client.py` | Streaming upload client (threaded sync, reuses shared payload planner + stat guard; warm/cold resume) |

### Modified

| Path | Change |
|---|---|
| `src/sutradhara/cli/main.py` | Add `sutra serve-grpc` command |
| `src/sutradhara/api/store.py` | Reuse `begin_idempotency` for `grpc:StartIntake` only (method scope + plan digest in the hashed body); **add `release_idempotency`** to delete a *completed* StartIntake row on `AbortIntake` (existing `abandon_idempotency` is `in_progress`-only). Commit idempotency is NOT here — it lives in `grpc_intake.manifest_digest` + dynamic status. |
| `packages/sutra-agent/src/sutra_agent/config.py` | Add `server_address`, cert fields, `device_id`; `landing` optional (no `operator` in streaming mode) |
| `packages/sutra-agent/src/sutra_agent/receive.py` | Route to `grpc_client` when `server_address` set |
| `packages/sutra-agent/src/sutra_agent/ledger.py` | Record `idempotency_key → {intake_id, plan_digest}` for the warm-resume trust gate (§11) |
| `packages/sutra-agent/src/sutra_agent/cli.py` | `--server` / `--client-cert` / `--ca-cert` / `enroll` |

### Prerequisite — **now satisfied**

- **`sutra intake watch`** — the single registrar that verifies fixity, registers into
  the catalog, and writes `intake.verified.json` / `intake.quarantined.json` /
  `intake.discrepancy.json`. **Implemented 2026-06-30** (`src/sutradhara/intake_watch.py`,
  `sutra intake watch`; `design-intake-watch.md` is `current`). The gRPC path writes no
  terminal markers itself; it hands off complete bags to this watcher.

### Reused (not reimplemented)

- **Shared `sutradhara_receive` payload planner** — package normalization, symlink
  policy, NFC canonicalization, skipped-count, **and the source mutation stat-guard**
  (`_hash_source_with_stat_guard`). If not yet exposed as a standalone "yield payload
  units" entry point, factoring one out (carrying the stat guard into
  `unit.byte_chunks()`) is in scope; the planning *logic* is reused, never duplicated.

### Unchanged

`routes_receive.py`, `routes_session.py`, `routes_intake.py`, Caddy
config, Authentik config, all existing HTTP API behaviour, `sutra serve-api`.
(`sutradhara_receive` gains only the extracted planner entry point if one isn't already
present — no behavioural change to existing callers.)

## 13. New dependencies

**Server:** `grpcio`, `grpcio-tools` (already in the Python ecosystem; add to
`pyproject.toml` server extras, not the edge `sutradhara-receive` package).

**sutra-agent:** `grpcio` (add to `packages/sutra-agent/pyproject.toml`).

Neither dependency leaks into `sutradhara-receive` (the lightweight edge package).

## 14. Testing

**Unit:**
- `test_owner_check.py`: **a second enrolled device/operator is `PERMISSION_DENIED`**
  on `UploadFile`/`ListIntakeFiles`/`CommitIntake`/`GetIntakeStatus`/`AbortIntake`
  against an intake it does not own (owner stored at StartIntake); the owner passes.
- `test_servicer.py`: `StartIntake` idempotency — same key+identical request returns
  the same intake, **same key + changed artifactclass/source_ref/plan_digest →
  conflict** (not silent attach); `UploadFile` happy path + transit corruption
  (mismatched sha256 → receipt error) + **leading-`data/` relpath rejected** + **two
  same-relpath streams in one process don't collide** (UUID temp names) + **temp lives
  in `.incoming/`, never under `data/`** (a leftover `.incoming/*.tmp` after a simulated
  crash does not appear in the committed `data/` nor fail `inspect_intake`) + rejected
  when intake not `streaming`; `CommitIntake` sha256 cross-check fail + **rejected
  while an upload is in flight** + **idempotent via `grpc_intake.manifest_digest`** (same
  digest → live status, different digest → conflict) + **recoverable failure rolls back
  to `streaming`** and returns `reupload_relpaths` (re-upload + re-commit repairs it) +
  **rejects a `canonicalization_version` / `package_profile_version` skew before writing
  the bag**; `AbortIntake` is **state-gated** — allowed in `streaming`/`committing`
  (drops the dir + `release_idempotency` frees the completed key so a re-`StartIntake`
  re-mints), **`FAILED_PRECONDITION` once `committed`** (won't delete a handed-off bag);
  `ListIntakeFiles` reads the durable ledger and lists only receipt-logged files;
  `GetIntakeStatus` survives `.receiving.json` removal (reads `grpc_intake`), reporting
  watcher markers (incl. `discrepancy`) over `committed`→`verifying`.
- `test_grpc_store.py`: the `grpc_intake` row carries owner/state/`manifest_digest`;
  **after commit (`.receiving.json` gone) owner check + status + manifest-digest retry
  still resolve** from the row; an `aborted` row blocks reuse until `release_idempotency`.
- `test_assembly.py`: streamed chunks produce a valid BagIt bag (`bagit.validate()`);
  `operator`/`artifactclass`/`source_*`/`label` in `bag-info.txt` come from the
  **server-stored StartIntake intent**, never the Commit payload (client can't restate
  them); a `.fcpbundle` payload unit lands as **one** `package-tar-v1` entry, the server
  writes the **single `package-index.json`** that **`core.read_package_index` accepts**
  (profile/profile_hash constants + `packages[]` with logical/stored/sha256/members),
  the **member records match the core by type** — a **non-file member serializes
  `sha256:null`/`data_offset:null`** (not `""`/`0`) and a symlink carries `linkname`,
  per the §5 proto→JSON rule — and a ranged extract of one internal member verifies
  against its sha256; `Package-Profile-Version`/`-Hash` **and `Canonicalization-Version`**
  are the **constants** in `bag-info.txt` even for a **non-package** intake (never `""`,
  which the core rejects); sentinel written last; the bag passes `inspect_intake` so
  `sutra intake watch` accepts it (no double-verify).
- `test_grpc_client.py`: happy path with a local test server; **warm resume** (local
  ledger present) skips ledger-listed units without re-hashing; **cold resume** (no
  local ledger) re-hashes skipped units before trusting them; **resume against a
  different card → StartIntake conflict** (strengthened plan digest); the source
  mutation guard fails a unit whose source file changes mid-read;
  `TransitCorruptionError` on sha256 mismatch; the thread pool caps concurrent RPCs at
  `parallelism`; package indexes are actually sent and match the core schema.
- `test_receipt_ledger.py`: rename-then-log ordering — a data file present without a
  receipt line is re-uploaded (not falsely skipped); a logged file is skipped;
  concurrent appends under the per-intake lock don't interleave/corrupt lines.

**Integration (harness scenario):**
A new scenario (or extension of `scenario_r.py`) that runs `sutra serve-grpc` with a
self-signed test CA **and the `sutra intake watch` registrar**, runs `sutra-agent
receive` against a fake-source directory, and asserts the watcher writes
`intake.verified.json` and the registered intake passes `sutra intake inspect`. Uses
`--fake-source` (already in `sutra-agent` CLI) for CI without real hardware.

## 15. Open / decided

| # | Question | Decision |
|---|---|---|
| 1 | **Does the cert identify a person, a workstation, or both?** (codex) | **Workstation** (`CN = device_id`). Operator is a server-side `(device_id, fingerprint) → operator` mapping set at enrollment (admin-authorized). v1 assumes 1 device : 1 operator (each operator has a personal Mac). A **shared** ingest workstation needs per-transfer operator selection (GUI carries an Authentik operator token alongside the device cert) — future, out of scope for v1. |
| 2 | **Do gRPC intakes share the `.receiving.json` state machine with HTTP/local receive?** (codex) | **Yes.** One lifecycle marker (`.receiving.json` + a `transport` field), one sweep, one orphan-detector. No forked `.streaming.json` state. |
| 3 | **Package normalization: client-side before streaming, or a server planning step?** (codex) | **Client-side**, via the shared `sutradhara_receive` payload planner. The `package-tar-v1` is produced once at receive (its hash is the package identity, per front-door §12.1); it streams on the fly (no full local buffer) as one payload unit; its inner index ships at `CommitIntake`. No server planning RPC. |
| 4 | StartIntake idempotency scope | Reuses the HTTP API's durable store: scoped `(operator, "grpc:StartIntake", key)` + canonical request hash; reused key with a changed request → conflict. |
| 5 | Resume receipt durability | Durable `receive-receipts.jsonl` append-after-rename ledger; O(remaining), not O(size) re-hash on restart. |
| 6 | Proto location | **Corrected at prompt time:** the repo already uses gRPC — `.proto` source lives in `proto/` (`proto/intake.proto`, beside `proto/layer5.proto`); committed stubs regenerate via `scripts/regenerate-proto.sh` into `src/sutradhara/_proto/` (server) **and** `packages/sutra-agent/src/sutra_agent/_proto/` (agent), keeping `sutradhara-receive` grpc-free. (Was: `packages/sutra-agent/proto/`.) See `prompt-streaming-intake-grpc.md`. |
| 7 | Mid-file byte-level resume | Deferred (v2); `offset` field reserved in proto; re-upload of a partial unit on disconnect is acceptable on 10G LAN. |
| 8 | CRL / cert revocation | Device blocklist (drop mapping + block fingerprint) for v1; CRL deferred. |
| 9 | GUI | Deferred — thin shell over `sutra-agent` CLI, separate design. |
| 10 | Parallelism default | 8 concurrent RPCs (thread pool); configurable per workstation. |
| 11 | Chunk size | 4 MB default; configurable; aligns with ZFS recordsize=1M (4 records per chunk write). |
| 12 | gRPC port | 50051 default; bind to LAN/Tailscale interface, blocked on public NIC by existing nftables. |
| 13 | **Who verifies + registers a committed intake?** (codex r3) | **`sutra intake watch`**, not the gRPC server. The gRPC commit lands a complete bag (sentinel last, `.receiving.json` removed) indistinguishable from local `sutra receive`; the existing watcher verifies fixity, registers, and writes terminal markers. A gRPC-side verify would have raced the watcher and (if it wrote `intake.verified.json`) blocked registration. |
| 14 | **Package inner index + profile carriage** (codex r2, corrected r3, r4) | `CommitIntakeRequest.package_indexes` carries the data for the **single `package-index.json`** the receive core validates (`profile`/`profile_hash` = server constants, `packages[]` = logical/stored/sha256/members; member records use the core's exact keys `member`/`type`∈`file|directory|symlink`/`length`/`sha256`/`data_offset`/`linkname`). Profile constants are written into `bag-info.txt` **unconditionally** by the server (never `""`); `receive_facts.package_profile_version` is a skew assertion only. |
| 15 | **bag-info.txt authority** (codex r2) | The five StartIntake intent fields (artifactclass/source_kind/source_ref/label + server-resolved operator) are **server-stored at StartIntake** and authoritative; Commit carries only receive-determined facts. Client cannot restate intent. |
| 16 | **Per-intake concurrency** (codex r2, r3) | UUID temp names (not `<pid>`); per-intake ledger lock for serialized appends; `streaming→committing→committed` state machine; Commit rejected while any upload is in flight; a recoverable Commit failure **rolls back to `streaming`** (no stranding); `AbortIntake` for an unrecoverable give-up. |
| 17 | **Mixed-source resume hazard** (codex r2, r3, corrected r4) | `source_plan_digest` = `{relpath,size,mtime_ns}` — **metadata only, no content read** (the r3 content anchor broke single-read / produce-tar-once, so it was dropped). Mixed-source safety is the resume trust gate: warm resume trusts the server set via the local `AgentLedger`; cold resume re-hashes skipped units. The "never mixed-source" claim is honest layers, not absolute. |
| 18 | **Commit retry safety** (codex r2, clarified r4) | Commit idempotency is the intake's stored `manifest_digest` + live status (NOT the HTTP `begin_idempotency` store, which has no key here and replays a frozen response): same digest → live status; different digest on a committed intake → conflict. |
| 19 | **Non-package profile metadata** (codex r3) | Server **always** writes the `Package-Profile-Version`/`-Hash` constants (matching `core.bag_info_metadata`) even for non-package intakes; the client never sends `""`. |
| 20 | **Source mutation guard** (codex r3) | The extracted payload-unit API carries the front-door stat-before/after guard (`unit.byte_chunks()`); a source changing mid-read fails that unit (no receipt). |
| 21 | **`sutra intake watch` prerequisite** (codex r4) | **Hard prerequisite, not a reuse** — the command doesn't exist yet (`design-intake-watch.md` *for review*; CLI has only inspect/register/accept/prepare). It must be approved + built before this lands; the gRPC path writes no terminal markers itself. |
| 22 | **Per-RPC owner check** (codex r4) | Every RPC resolves the peer cert → `(operator, device_id)` and requires a match against the intake's stored owner (`PERMISSION_DENIED` otherwise). A valid device cert is not authority over an arbitrary `intake_id`. |
| 23 | **Temp-file location** (codex r4) | Temps live in `{intake}/.incoming/` **outside `data/`** (UUID-named), atomically renamed into `data/`. A crash can't leave a `*.tmp` under `data/` that the receive validator would hash as extra payload and quarantine the bag. Swept at commit/resume/sweep. |
| 24 | **Abort frees the idempotency key** (codex r4) | `abandon_idempotency` is `in_progress`-only; a minted intake's record is `completed`. Abort uses a new `release_idempotency` to delete the completed StartIntake row so the same key can re-mint. |
| 25 | **Where does committed owner/state/digest live?** (codex r5) | A durable **`grpc_intake` table** (`intake_id, operator, device_id, state, manifest_digest, timestamps`), NOT `.receiving.json` (which commit removes). Owner checks, the manifest-digest retry, `GetIntakeStatus`, and the Abort gate all read it, so they survive `.receiving.json` removal. |
| 26 | **Abort after commit** (codex r5) | `AbortIntake` is **state-gated to `streaming`/`committing`**; once `committed` it returns `FAILED_PRECONDITION` — the bag belongs to `sutra intake watch` and deleting it would race the registrar. |
| 27 | **Canonicalization skew** (codex r5) | Server rejects `receive_facts.canonicalization_version ≠` its `CANONICALIZATION_VERSION` constant **before writing the bag** (same guard as the package profile); the core validates this field, so a stale value would otherwise quarantine. |
| 28 | **Nullable package member fields** (codex r5) | proto3 scalars can't express null, so `sha256`/`data_offset`/`linkname` are `optional` AND the server derives the JSON **by `type`**: non-file → `sha256:null`/`data_offset:null`, symlink → `linkname`. Reproduces the core records exactly. |
| 29 | **`file_size` for packages** (codex r5) | `file_size` is an optional progress **hint** (0 = unknown); a package sends 0 (its deterministic tar size isn't known until produced) — the server never requires it. |

## 16. Review trail

- **2026-06-30 — brainstorm (Claude + the owner).** Settled: single-pass streaming over
  two-pass rsync; gRPC+mTLS over HTTPS/SMB/QUIC/fountain-codes (rationale §2);
  in-flight hashing with server cross-check; N parallel streams; card held until
  server verify.
- **2026-06-30 — codex document review (7 findings folded).**
  - *High* — relpath contract contradiction (proto said relative-to-`data/`, servicer
    required leading `data/` → `data/data/…`). Fixed: wire value is relative to
    `data/`, server **rejects** a leading `data/` (§5 proto comment, §7 `UploadFile`).
  - *High* — client bypassed package normalization (raw walk would explode a
    `.fcpbundle`). Fixed: client **reuses the shared payload planner**; packages stream
    as one `package-tar-v1` unit (§8 "Payload planning", §15 Q3).
  - *High* — cert identity was self-contradictory (machine identity yet `CN =
    operator`, revoked by device name). Fixed: **device identity** (`CN = device_id`) +
    server `device→operator` enrollment mapping (§6, §15 Q1).
  - *Medium* — StartIntake idempotency underspecified. Fixed: reuse the HTTP API's
    scoped + canonical-hash store; reused key + changed body → conflict (§7, §15 Q4).
  - *Medium* — async pseudocode unimplementable (sync stubs in `asyncio.TaskGroup`;
    `total` `UnboundLocalError`). Fixed: explicit **threaded sync uploader**; counter
    is a dict (§8 streaming loop).
  - *Medium* — marker `.streaming.json` forked the receive lifecycle / sweep. Fixed:
    reuse `.receiving.json` with a `transport` field (§7, §11, §15 Q2).
  - *Medium* — resume receipt durability unspecified. Fixed: durable
    `receive-receipts.jsonl` append-after-rename ledger; explicitly rejects O(size)
    re-hash (§7 `ListIntakeFiles`, §11, §15 Q5).
- **2026-06-30 — codex document review, round 2 (6 findings folded).**
  - *High* — package inner indexes were described but had no proto carriage (Commit
    accumulated them and dropped them). Fixed: `CommitIntakeRequest.package_indexes`
    (`PackageIndex`/`PackageMemberEntry`) + `receive_facts.package_profile_version`/
    `_hash`; the client now sends them and the server writes per-package tag files
    (§5, §8, §15 Q14).
  - *High* — Commit restated metadata already bound at StartIntake (a second,
    divergent source of truth; the sample even dropped `source_ref`). Fixed: the five
    intent fields are server-stored at StartIntake and authoritative for `bag-info.txt`;
    Commit carries only receive-determined facts (§5 `ReceiveFacts`, §7, §15 Q15).
  - *High* — per-intake concurrency underspecified (`<pid>` temp-name collision for
    two same-relpath streams, unlocked JSONL ledger, no seal-vs-write ordering). Fixed:
    UUID temp names, per-intake ledger lock, explicit `streaming→committing→verifying`
    state machine with Commit gated on a zero in-flight counter (§7, §15 Q16).
  - *Medium* — resume could merge two cards under a reused key. Fixed:
    `source_plan_digest` bound into StartIntake's canonical hash; different card →
    conflict (§5, §8, §11, §15 Q17).
  - *Medium* — Commit had no retry/idempotency contract. Fixed: idempotent on
    `(intake_id, manifest_digest)` (§5, §7, §15 Q18).
  - *Low* — `source_kind` comment dropped `download` (front-door includes it). Fixed
    to `card | drive | upload | handoff | download | other` (§5), matching
    `SOURCE_KIND_CHOICES`.
- **2026-06-30 — codex document review, round 3 (6 findings folded).**
  - *High* — `PackageIndex` didn't match the receive core's package-index contract
    (core expects one `package-index.json` with `profile`/`profile_hash`/`packages[]`
    each with `logical_member_path`/`stored_member_path`/`sha256`/`members`; r2's proto
    had only `stored_relpath`+`members` and "per-package tag files"). Fixed: proto
    `PackageIndex` carries the full schema; server writes the **single
    `package-index.json`** `core.read_package_index` validates (§5, §8, §15 Q14).
  - *High* — removing `.receiving.json` then running a gRPC verify job races
    `sutra intake watch`, and writing `intake.verified.json` would block registration.
    Fixed: **no gRPC verify/register** — the commit hands off a complete bag and the
    watcher is the single registrar/marker-writer (§3, §7, §10, §12, §15 Q13).
  - *High* — `source_plan_digest` over `{relpath,size}` was too weak (same-layout cards
    collide). Fixed: digest adds `mtime_ns` + a largest-unit content anchor; warm/cold
    resume trust gate via the local `AgentLedger`; absolute guarantee softened
    (§5, §8, §11, §15 Q17).
  - *Medium* — non-package intakes would write `""` profile metadata, which fails core
    validation (accepts absent or the constant, not `""`). Fixed: server **always**
    writes the constants; client never sends `""` (§5, §7, §15 Q19).
  - *Medium* — a failed Commit stranded the intake in `committing` with uploads
    rejected. Fixed: recoverable failure **rolls back to `streaming`** (returns
    `reupload_relpaths`); `AbortIntake` for unrecoverable (§5, §7, §15 Q16).
  - *Medium* — the streaming loop dropped the front-door source mutation guard. Fixed:
    `unit.byte_chunks()` carries the stat-before/after guard (§8, §15 Q20).
- **2026-06-30 — codex document review, round 4 (6 findings folded).**
  - *High* — only StartIntake resolved the operator; the other RPCs took a bare
    `intake_id`, so any enrolled device that learned an id could act on another's
    intake. Fixed: **per-RPC owner check** — every RPC matches the peer cert against the
    intake's stored `(operator, device_id)` (§5 service comment, §7, §15 Q22).
  - *High* — the r3 content anchor re-read the largest unit at planning, breaking the
    single-read claim and the "tar produced once" rule. Fixed: `source_plan_digest` is
    **metadata-only** (`{relpath,size,mtime_ns}`); mixed-source safety leans on the
    resume trust gate (§4, §5, §8, §11, §15 Q17).
  - *High* — the design leaned on `sutra intake watch`, which **doesn't exist** (CLI has
    only inspect/register/accept/prepare; its design is *for review*). Fixed: marked a
    **hard prerequisite**, not an "unchanged" reuse (§1, §12, §15 Q21).
  - *Medium* — temp files under `data/` would survive a crash and poison the receive
    validator (which hashes every `data/` file). Fixed: temps in `{intake}/.incoming/`
    **outside `data/`**, swept at commit/resume (§7, §15 Q23).
  - *Medium* — Commit/Abort idempotency didn't match the store API (`CommitIntake` has
    no key + needs live status not a frozen response; `abandon_idempotency` is
    `in_progress`-only). Fixed: Commit idempotency = stored `manifest_digest` + live
    status; Abort uses a new `release_idempotency` for the completed row (§7, §12,
    §15 Q18, Q24).
  - *Medium* — `PackageMemberEntry` didn't match the core's member JSON (`internal_member`
    /`"dir"` vs core `member`/`"directory"`/`linkname`). Fixed: proto fields renamed to
    the core schema exactly (§5, §15 Q14).
- **2026-06-30 — codex document review, round 5 (5 findings folded).**
  - *High* — committed owner/state/digest were said to live in `.receiving.json`, which
    commit removes — leaving owner checks, the manifest-digest retry, and GetIntakeStatus
    with nothing durable to read. Fixed: a durable **`grpc_intake` table** is the source
    of truth; `.receiving.json` is only the watcher/sweep hint (§7, §12, §15 Q25).
  - *High* — `AbortIntake` wasn't state-gated, so it could delete a committed,
    handed-off bag and race the watcher. Fixed: Abort allowed only in
    `streaming`/`committing`; `FAILED_PRECONDITION` once `committed` (§5, §7, §15 Q26).
  - *Medium* — `canonicalization_version` was client-supplied with no skew check, though
    the core validates it. Fixed: server rejects a mismatch before writing the bag, like
    the profile guard (§5, §7, §15 Q27).
  - *Medium* — proto3 scalars can't represent the core's null `sha256`/`data_offset` for
    non-file members. Fixed: `optional` fields + an explicit server proto→JSON mapping
    by `type` (non-file → JSON null; symlink → `linkname`) (§5, §15 Q28).
  - *Medium* — required `file_size` on the first chunk conflicts with on-the-fly package
    tar (size unknown until produced). Fixed: `file_size` is an optional hint, 0 for
    packages (§5, §8, §15 Q29).
