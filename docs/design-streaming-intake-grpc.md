# Design — streaming card/drive intake over gRPC + mTLS (`sutra-agent`)

> Status: **current** (brainstorm 2026-06-30, Claude + the owner; **codex document review
> folded — 7 findings**, trail in §16). Companion to `design-receive-front-door.md`
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
the proto for v2).

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
BagIt assembly core (`sutradhara_receive`). They do not share auth or ingress.

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
2. Server verify job re-reads every landed file and checks against the manifest before
   issuing `intake.verified.json` (catches disk write corruption).
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

service IntakeService {
  // Mint an intake id and create the landing directory.
  rpc StartIntake (StartIntakeRequest) returns (StartIntakeResponse);

  // Stream one file from client to server. One RPC per file; N files in parallel
  // via HTTP/2 stream multiplexing on the same gRPC channel.
  rpc UploadFile (stream FileChunk) returns (FileReceipt);

  // Return files already fully received — used by the client on resume.
  rpc ListIntakeFiles (ListIntakeFilesRequest) returns (ListIntakeFilesResponse);

  // Seal the bag: write BagIt tag files and kick the verify job.
  rpc CommitIntake (CommitIntakeRequest) returns (CommitIntakeResponse);

  // Poll until verified or quarantined.
  rpc GetIntakeStatus (IntakeStatusRequest) returns (IntakeStatusResponse);
}

// ── StartIntake ──────────────────────────────────────────────────────────────

message StartIntakeRequest {
  string idempotency_key = 1;  // UUID — same dedup model as HTTP /api/receive
  string artifactclass   = 2;
  string source_kind     = 3;  // card | drive | upload | handoff | other
  string source_ref      = 4;  // optional: card serial / drive label
  string label           = 5;  // optional: human label for this intake
}

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
  int64  file_size = 6;         // total file size in bytes (first chunk only; 0 otherwise)
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
  string                 intake_id = 1;
  repeated ManifestEntry files     = 2;
  BagInfo                bag_info  = 3;
}

message ManifestEntry {
  string relpath       = 1;
  string client_sha256 = 2;  // sha256 the client computed while reading the card
  int64  bytes         = 3;
}

message BagInfo {
  // operator is NOT client-supplied — the servicer stamps it from the
  // device→operator enrollment mapping keyed by the peer cert (§6).
  string source_kind              = 1;
  string source_ref               = 2;
  string artifactclass            = 3;
  string label                    = 4;
  string canonicalization_version = 5;
  int64  skipped_count            = 6;  // files intentionally skipped (symlinks etc.)
}

message CommitIntakeResponse {
  string intake_id = 1;
  string status    = 2;  // always "verifying" on success
}

// ── GetIntakeStatus ──────────────────────────────────────────────────────────

message IntakeStatusRequest  { string intake_id = 1; }

message IntakeStatusResponse {
  string          intake_id = 1;
  // streaming | verifying | verified | quarantined
  string          status    = 2;
  repeated string errors    = 3;  // populated when quarantined: bad relpaths + reason
}
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
in this table, and stamps **that** server-controlled value as `Intake.operator`. The
client-supplied `BagInfo` has no `operator` field and the server never trusts one.
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
  servicer.py        — IntakeServicer: five RPC implementations
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

**`StartIntake`:** resolves the operator from the peer cert (the device→operator
enrollment mapping, §6); validates artifactclass against the registry (same check as
HTTP `POST /api/receive`); applies the **same durable idempotency contract the HTTP
API uses** (`store.begin_idempotency`): the record is scoped to
`(operator_username, method="grpc:StartIntake", idempotency_key)` and binds a
**canonical hash** of the request fields (`artifactclass`, `source_kind`,
`source_ref`, `label`). Same key + identical request ⇒ returns the **existing**
`intake_id`. Same key + a **different** request (changed artifactclass/source_ref/…)
⇒ `FAILED_PRECONDITION` conflict — never silently attaches to the old intake. A first
call mints the intake id (`YYYYMMDD-<operator-slug>-<UUID>`) and creates
`/replica/landing/{intake_id}/` with a `.receiving.json` marker (see below). Returns
`intake_id`.

**`UploadFile`:** reads the client-streaming `FileChunk` sequence for one file;
validates `relpath` confinement on the first chunk (POSIX, NFC-normalised, **rejects a
leading `data/` component** — the wire value is relative to `data/` — plus no `..`,
no absolute path, must canonicalize to stay inside `data/`); writes chunks to a
process-unique temp path (`data/{relpath}.tmp.<pid>`), computes sha256 rolling hash;
on `is_last=true`: `fsync`, atomic `rename` to `data/{relpath}`, `fsync` parent dir,
then **appends a receipt line** (`{relpath, server_sha256, bytes}`) to the durable
per-file receipt ledger `receive-receipts.jsonl` (§11) with `fsync`; returns
`FileReceipt` with `server_sha256`. On disconnect mid-stream: temp file is discarded
(never renamed, so no receipt line is written).

**`ListIntakeFiles`:** returns `{relpath, server_sha256, bytes}` read from the durable
`receive-receipts.jsonl` ledger (a file counts as received only if it has a receipt
line). Used by the client on resume to skip already-landed files **without re-hashing**
the landed bytes. (Crash window: a file renamed but not yet logged has no receipt line,
so the client re-uploads it and the server overwrites with identical bytes — safe.)

**`CommitIntake`:** validates that `client_sha256 == server_sha256` for every file
in the manifest (cross-checks the transit integrity asserted per-file during upload);
any mismatch → return error, do not commit; writes `bagit.txt`, `bag-info.txt`
(stamping `operator` from the cert→operator mapping, ignoring any client-supplied
value), `manifest-sha256.txt`, `tagmanifest-sha256.txt`, `intake.json` (sentinel,
written last); replaces `.receiving.json` (removes it once `intake.json` lands);
enqueues the async verify job. Returns `status: "verifying"`.

**`GetIntakeStatus`:** reads the intake directory for `intake.verified.json` or
`intake.quarantined.json`; falls back to `.receiving.json` presence (vs `intake.json`)
to distinguish `streaming` from `verifying`. Returns status + error list if
quarantined.

### Verify job

Re-reads every file in `data/`, recomputes sha256, compares against
`manifest-sha256.txt`. On clean: writes `intake.verified.json`. On mismatch: writes
`intake.quarantined.json` with the offending relpaths. The verify job reuses the
existing intake verification logic from `sutradhara_receive`; it runs in a thread
pool (not a separate process) so it shares the server's SQLAlchemy engine.

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

So the planner yields a list of **payload units**, each either a regular file or a
normalized package. The streaming client turns each unit into one `UploadFile` RPC:
- a regular file streams its bytes directly;
- a package streams the `package-tar-v1` bytes produced **on the fly in sorted member
  order** (no full local buffer — tar is a streaming format; the member list is walked
  first to fix the deterministic order, then bytes stream). Its `relpath` is the
  stored name (`<name>.<ext>.tar`); its inner index (`internal_member → {offset,
  length, sha256}`) is collected during the stream and sent in `CommitIntake` as a tag
  file, exactly as §12.4 specifies. The package tar's sha256 is its identity (computed
  in-flight, cross-checked against the server receipt like any file).

### Streaming loop (`grpc_client.py`) — threaded sync uploader

gRPC's **synchronous** stubs are used with a `ThreadPoolExecutor` (one worker per
parallel stream). This avoids the `grpc.aio`/sync-stub mismatch entirely — the upload
is I/O-bound (card read + socket write), so threads are the right tool and the GIL is
released during both.

```python
def stream_source(source: Path, config: AgentConfig, idempotency_key: str) -> StreamResult:
    channel = grpc.secure_channel(config.server_address, mtls_creds(config))
    stub = IntakeServiceStub(channel)

    intake_id = stub.StartIntake(StartIntakeRequest(
        idempotency_key=idempotency_key,
        artifactclass=config.artifactclass,
        source_kind=config.source_kind,
        source_ref=config.source_ref,
        label=config.label,
    )).intake_id

    # Resume: skip payload units already fully received (durable server ledger)
    landed = {f.relpath: (f.server_sha256, f.bytes)
              for f in stub.ListIntakeFiles(ListIntakeFilesRequest(intake_id=intake_id)).files}

    # Shared planner: regular files AND normalized packages (NOT a raw os.walk)
    units = [u for u in plan_payload_units(source) if u.relpath not in landed]
    manifest = dict(landed)
    inner_indexes = {}

    # N parallel UploadFile RPCs via a thread pool (sync stubs).
    with ThreadPoolExecutor(max_workers=config.parallelism) as pool:
        futures = {pool.submit(_upload_one, stub, intake_id, u): u for u in units}
        for fut in as_completed(futures):
            relpath, sha, nbytes, inner = fut.result()  # raises on transit mismatch
            manifest[relpath] = (sha, nbytes)
            if inner is not None:
                inner_indexes[relpath] = inner

    stub.CommitIntake(CommitIntakeRequest(
        intake_id=intake_id,
        files=[ManifestEntry(relpath=r, client_sha256=h, bytes=b)
               for r, (h, b) in manifest.items()],
        bag_info=BagInfo(
            source_kind=config.source_kind, artifactclass=config.artifactclass,
            label=config.label, canonicalization_version=CANON_VERSION,
            skipped_count=count_skipped(source),
        ),  # NB: no operator field — server stamps it from the cert mapping
    ))
    return poll_until_done(stub, intake_id, timeout=config.confirm_timeout)


def _upload_one(stub, intake_id, unit):
    """Stream one payload unit (regular file or normalized package) → one UploadFile RPC."""
    sha = hashlib.sha256()
    counter = {"total": 0}   # dict, not a bare local — the generator mutates it safely

    def chunk_iter():
        first = True
        for data in unit.byte_chunks(CHUNK_BYTES):   # regular read, or on-the-fly package tar
            sha.update(data)
            counter["total"] += len(data)
            yield FileChunk(
                intake_id=intake_id, relpath=unit.relpath, data=data,
                offset=counter["total"] - len(data), is_last=False,
                file_size=unit.size if first else 0,
            )
            first = False
        yield FileChunk(intake_id=intake_id, relpath=unit.relpath,
                        data=b"", offset=counter["total"], is_last=True)

    receipt = stub.UploadFile(chunk_iter())
    client_sha256 = sha.hexdigest()
    if receipt.server_sha256 != client_sha256:
        raise TransitCorruptionError(unit.relpath, client_sha256, receipt.server_sha256)
    return unit.relpath, client_sha256, receipt.received_bytes, unit.inner_index()
```

Two bugs from the first draft are fixed here: the broken `asyncio.TaskGroup` + sync
stub mix is gone (explicit thread pool), and the `total += …`-in-a-nested-generator
`UnboundLocalError` is gone (the counter is a dict the generator mutates by key, not a
bare local that `+=` would shadow).

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
Card (single read per file)
  → client: sha256 rolling hash (in memory, no local write)
  → chunks sent via UploadFile gRPC stream
  → server: receives chunks, writes temp→fsync→rename, computes server_sha256
  → FileReceipt returned: server_sha256
  → client assert: client_sha256 == server_sha256   ← TRANSIT INTEGRITY
       fail → TransitCorruptionError: abort this file, record in failed set
  → CommitIntake: client_sha256 per file
  → server: assert client_sha256 == server_sha256 per file (double-checks commit)
  → server verify job: re-reads every data/ file, recomputes sha256,
       checks against manifest-sha256.txt         ← DISK WRITE INTEGRITY
  → all pass → intake.verified.json
  → any fail → intake.quarantined.json + error list
  → client polls GetIntakeStatus
       verified    → "safe to eject" message
       quarantined → "DO NOT eject — bad files: {list}" + operator action required
```

Three independent integrity checks. Card held until all three pass.

## 11. Resume model

Connection drops mid-transfer (network blip, laptop sleep, operator walks away):

1. Agent restarts (or operator re-runs `sutra-agent receive <source>`)
2. `StartIntake` with the same `idempotency_key` — server returns the existing
   `intake_id` (idempotent; the canonical-hash check ensures the resumed request
   matches the original, §7)
3. `ListIntakeFiles` — server returns fully-received units with their sha256
4. Client re-plans the source via the shared planner; units in the server's list are
   skipped (no re-hash of the source bytes for already-landed units)
5. Remaining units are uploaded normally; `CommitIntake` includes the full manifest
   (already-landed + newly-uploaded)

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
| `src/sutradhara/grpc/servicer.py` | Five RPC implementations; `.receiving.json` + `receive-receipts.jsonl` lifecycle |
| `src/sutradhara/grpc/assembly.py` | BagIt assembly from streamed chunks (reuses `sutradhara_receive` writers) |
| `src/sutradhara/grpc/ca.py` | CA / cert issuance / device→operator mapping / device revocation |
| `packages/sutra-agent/src/sutra_agent/grpc_client.py` | Streaming upload client (threaded sync, reuses shared payload planner) |

### Modified

| Path | Change |
|---|---|
| `src/sutradhara/cli/main.py` | Add `sutra serve-grpc` command |
| `src/sutradhara/api/store.py` | Reuse `begin_idempotency` for `grpc:StartIntake` (new method scope); no new table |
| `packages/sutra-agent/src/sutra_agent/config.py` | Add `server_address`, cert fields, `device_id`; `landing` optional (no `operator` in streaming mode) |
| `packages/sutra-agent/src/sutra_agent/receive.py` | Route to `grpc_client` when `server_address` set |
| `packages/sutra-agent/src/sutra_agent/cli.py` | `--server` / `--client-cert` / `--ca-cert` / `enroll` |

### Reused (not modified — depended upon)

The streaming client **reuses the shared `sutradhara_receive` payload planner**
(package normalization, symlink policy, NFC canonicalization, skipped-count) rather
than re-walking the source. If that planner is not yet exposed as a standalone
"yield payload units" entry point, factoring one out of the existing `receive` core
is in scope — but the planning *logic* is reused, never reimplemented (the edge and
server must agree on what a payload entry is).

### Unchanged

`routes_receive.py`, `routes_session.py`, `routes_intake.py`, Caddy config, Authentik
config, all existing HTTP API behaviour, `sutra serve-api`. (`sutradhara_receive` gains
only the extracted planner entry point if one isn't already present — no behavioural
change to existing callers.)

## 13. New dependencies

**Server:** `grpcio`, `grpcio-tools` (already in the Python ecosystem; add to
`pyproject.toml` server extras, not the edge `sutradhara-receive` package).

**sutra-agent:** `grpcio` (add to `packages/sutra-agent/pyproject.toml`).

Neither dependency leaks into `sutradhara-receive` (the lightweight edge package).

## 14. Testing

**Unit:**
- `test_servicer.py`: `StartIntake` idempotency — same key+identical request returns
  the same intake, **same key + changed artifactclass/source_ref → conflict** (not
  silent attach); `UploadFile` happy path + transit corruption (mismatched sha256 →
  receipt error) + **leading-`data/` relpath rejected**; `CommitIntake` sha256
  cross-check fail; `ListIntakeFiles` reads the durable ledger and lists only
  receipt-logged files; `GetIntakeStatus` state machine.
- `test_assembly.py`: streamed chunks produce a valid BagIt bag (`bagit.validate()`);
  `operator` is always from the **device→operator mapping**, never the client payload;
  a `.fcpbundle` payload unit lands as **one** `package-tar-v1` entry with a
  round-tripping inner index (not exploded into inner files); sentinel written last.
- `test_grpc_client.py`: happy path with a local test server; **resume skips
  ledger-listed units without re-hashing**; `TransitCorruptionError` on sha256
  mismatch; the thread pool caps concurrent RPCs at `parallelism`; the payload planner
  (not a raw walk) drives the unit list.
- `test_receipt_ledger.py`: rename-then-log ordering — a data file present without a
  receipt line is re-uploaded (not falsely skipped); a logged file is skipped.

**Integration (harness scenario):**
A new scenario (or extension of `scenario_r.py`) that runs `sutra serve-grpc` with a
self-signed test CA, runs `sutra-agent receive` against a fake-source directory, and
asserts `intake.verified.json` is written and the registered intake passes
`sutra intake inspect`. Uses `--fake-source` (already in `sutra-agent` CLI) for CI
without real hardware.

## 15. Open / decided

| # | Question | Decision |
|---|---|---|
| 1 | **Does the cert identify a person, a workstation, or both?** (codex) | **Workstation** (`CN = device_id`). Operator is a server-side `(device_id, fingerprint) → operator` mapping set at enrollment (admin-authorized). v1 assumes 1 device : 1 operator (each operator has a personal Mac). A **shared** ingest workstation needs per-transfer operator selection (GUI carries an Authentik operator token alongside the device cert) — future, out of scope for v1. |
| 2 | **Do gRPC intakes share the `.receiving.json` state machine with HTTP/local receive?** (codex) | **Yes.** One lifecycle marker (`.receiving.json` + a `transport` field), one sweep, one orphan-detector. No forked `.streaming.json` state. |
| 3 | **Package normalization: client-side before streaming, or a server planning step?** (codex) | **Client-side**, via the shared `sutradhara_receive` payload planner. The `package-tar-v1` is produced once at receive (its hash is the package identity, per front-door §12.1); it streams on the fly (no full local buffer) as one payload unit; its inner index ships at `CommitIntake`. No server planning RPC. |
| 4 | StartIntake idempotency scope | Reuses the HTTP API's durable store: scoped `(operator, "grpc:StartIntake", key)` + canonical request hash; reused key with a changed request → conflict. |
| 5 | Resume receipt durability | Durable `receive-receipts.jsonl` append-after-rename ledger; O(remaining), not O(size) re-hash on restart. |
| 6 | Proto location | `packages/sutra-agent/proto/` — both client and server are Python in the same repo; move to shared location if a native client (Swift/Go) is ever built. |
| 7 | Mid-file byte-level resume | Deferred (v2); `offset` field reserved in proto; re-upload of a partial unit on disconnect is acceptable on 10G LAN. |
| 8 | CRL / cert revocation | Device blocklist (drop mapping + block fingerprint) for v1; CRL deferred. |
| 9 | GUI | Deferred — thin shell over `sutra-agent` CLI, separate design. |
| 10 | Parallelism default | 8 concurrent RPCs (thread pool); configurable per workstation. |
| 11 | Chunk size | 4 MB default; configurable; aligns with ZFS recordsize=1M (4 records per chunk write). |
| 12 | gRPC port | 50051 default; bind to LAN/Tailscale interface, blocked on public NIC by existing nftables. |
| 13 | Verify job execution | Thread pool within `serve-grpc` process (shares SQLAlchemy engine); move to a job queue worker if it contends with tape I/O in production. |

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
