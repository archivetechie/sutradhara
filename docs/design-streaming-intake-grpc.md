# Design — streaming card/drive intake over gRPC + mTLS (`sutra-agent`)

> Status: **current** (brainstorm 2026-06-30, Claude + the owner). Companion to
> `design-receive-front-door.md` (the BagIt core this design reuses) and
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
    Auth: client certificate (CN = operator username)
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
  // operator is NOT client-supplied — the servicer stamps it from the peer cert CN.
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

## 6. mTLS — certificate model

The agent path uses **machine identity** (client certificate) not user identity
(session cookie / bearer token). Different trust domain from Caddy/Authentik.

**CA:** a sutradhara-internal CA. Generated on first `sutra serve-grpc` if absent;
stored at `/etc/sutradhara/pki/{ca.crt,ca.key}` (mode 0600 for key). Separate from
the Caddy internal CA — different trust scope.

**Server certificate:** issued by the sutradhara CA for the server's hostname /
Tailscale IP / LAN IP. Rotated annually.

**Client certificate:** one per operator workstation. `CN = operator-username` (e.g.
`owner`). The `IntakeServicer` reads the verified CN from the peer TLS certificate
and stamps it as `Intake.operator` — the client-supplied `BagInfo` has no `operator`
field and the server never trusts one if provided.

**Issuance (enroll command):**
```
# On the operator workstation — key never leaves the machine
sutra-agent enroll \
  --server 100.81.52.26:50051 \
  --operator owner \
  --admin-token <one-time token>

# Generates client key + CSR locally
# POSTs CSR to a one-time HTTPS enrollment endpoint on the server
# Server CA signs it, returns cert
# Stored: ~/.config/sutra-agent/{client.crt,client.key,ca.crt}  (Mac/Linux)
#         %APPDATA%\sutra-agent\{client.crt,client.key,ca.crt}   (Windows)
```

The `--admin-token` is a short-lived (24h) one-time token generated by the operator
running `sutra serve-grpc --issue-enroll-token`. It gates CSR signing; once used it
is invalidated. Enrollment is a one-time setup per machine.

**Revocation:** on a lost machine, run `sutra serve-grpc --revoke-cn owner-macbook`
(adds CN to a local blocklist checked on each RPC). For a lost CA, re-generate and
re-enroll all machines — small team, acceptable. CRL is deferred.

## 7. Server-side gRPC service

### File structure (new)

```
src/sutradhara/grpc/
  __init__.py
  server.py          — gRPC server lifecycle; mTLS channel credentials; bind address
  servicer.py        — IntakeServicer: five RPC implementations
  assembly.py        — BagIt bag assembly from streamed chunks; reuses sutradhara_receive writers
  ca.py              — CA / cert issuance helpers (sign CSR, revoke CN)
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

**`StartIntake`:** validates artifactclass against the registry (same check as HTTP
`POST /api/receive`); mints intake id (`YYYYMMDD-<operator-slug>-<UUID>`); creates
`/replica/landing/{intake_id}/` with a `.streaming.json` sentinel; records the
idempotency key (same SQLite store used by the HTTP API). Returns `intake_id`.
Idempotent: same `idempotency_key` returns the existing `intake_id`.

**`UploadFile`:** reads the client-streaming `FileChunk` sequence for one file;
validates `relpath` confinement on the first chunk (POSIX, NFC-normalised, no `..`,
no absolute, must begin with `data/`); writes chunks to a process-unique temp path
(`data/{relpath}.tmp.<pid>`), computes sha256 rolling hash; on `is_last=true`:
`fsync`, atomic `rename` to `data/{relpath}`, `fsync` parent dir; returns
`FileReceipt` with `server_sha256`. On disconnect mid-stream: temp file is discarded.

**`ListIntakeFiles`:** returns `{relpath, server_sha256, bytes}` for every file
present and fully written under `data/`. Used by the client on resume to skip
already-landed files.

**`CommitIntake`:** validates that `client_sha256 == server_sha256` for every file
in the manifest (cross-checks the transit integrity asserted per-file during upload);
any mismatch → return error, do not commit; writes `bagit.txt`, `bag-info.txt`
(stamping `operator` from the peer cert CN, ignoring any client-supplied value),
`manifest-sha256.txt`, `tagmanifest-sha256.txt`, `intake.json` (sentinel, written
last); removes `.streaming.json`; enqueues the async verify job. Returns
`status: "verifying"`.

**`GetIntakeStatus`:** reads the intake directory for `intake.verified.json` or
`intake.quarantined.json`; falls back to `.streaming.json` presence to distinguish
`streaming` from `verifying`. Returns status + error list if quarantined.

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
sutra serve-grpc --revoke-cn <cn>         # add CN to blocklist
```

Registered in `cli/main.py` alongside the existing `serve-api`.

## 8. Client-side — sutra-agent streaming mode

### Config (`~/.config/sutra-agent/config.json`)

Two mutually exclusive modes:

```jsonc
// Streaming mode (new — server required)
{
  "server_address": "100.81.52.26:50051",
  "client_cert": "~/.config/sutra-agent/client.crt",
  "client_key":  "~/.config/sutra-agent/client.key",
  "ca_cert":     "~/.config/sutra-agent/ca.crt",
  "operator":    "owner",          // must match cert CN
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

`sutra-agent config init` gains `--server` / `--client-cert` / `--client-key` /
`--ca-cert` / `--parallelism` flags; `--landing` and `--server` are mutually
exclusive.

### Streaming loop (`grpc_client.py`)

```python
async def stream_source(source: Path, config: AgentConfig, ...) -> StreamResult:
    channel = grpc.secure_channel(config.server_address, mtls_creds(config))
    stub = IntakeServiceStub(channel)

    resp = stub.StartIntake(StartIntakeRequest(
        idempotency_key=idempotency_key,
        artifactclass=config.artifactclass,
        source_kind=config.source_kind,
        ...
    ))
    intake_id = resp.intake_id

    # Resume: skip files already on the server
    landed = {f.relpath: f.server_sha256
              for f in stub.ListIntakeFiles(intake_id).files}

    files = [f for f in walk_source(source) if f.relpath not in landed]
    manifest = dict(landed)  # start with already-landed files

    sem = asyncio.Semaphore(config.parallelism)
    async with asyncio.TaskGroup() as tg:
        for relpath, path in files:
            tg.create_task(_upload_one(stub, intake_id, relpath, path, sem, manifest))

    stub.CommitIntake(CommitIntakeRequest(
        intake_id=intake_id,
        files=[ManifestEntry(relpath=r, client_sha256=h, bytes=b)
               for r, (h, b) in manifest.items()],
        bag_info=BagInfo(...),
    ))

    return poll_until_done(stub, intake_id, timeout=config.confirm_timeout)


async def _upload_one(stub, intake_id, relpath, path, sem, manifest):
    async with sem:
        sha = hashlib.sha256()
        total = 0

        def chunk_iter():
            first = True
            with open(path, "rb") as fh:
                while True:
                    data = fh.read(CHUNK_BYTES)
                    if not data:
                        break
                    sha.update(data)
                    total += len(data)
                    yield FileChunk(
                        intake_id=intake_id,
                        relpath=relpath,
                        data=data,
                        offset=total - len(data),
                        is_last=False,
                        file_size=path.stat().st_size if first else 0,
                    )
                    first = False
            # mark last chunk
            yield FileChunk(intake_id=intake_id, relpath=relpath,
                            data=b"", offset=total, is_last=True)

        receipt = stub.UploadFile(chunk_iter())
        client_sha256 = sha.hexdigest()

        if receipt.server_sha256 != client_sha256:
            raise TransitCorruptionError(relpath, client_sha256, receipt.server_sha256)

        manifest[relpath] = (client_sha256, receipt.received_bytes)
```

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
   `intake_id` (idempotent)
3. `ListIntakeFiles` — server returns fully-received files with their sha256
4. Client re-inventories the source; files in the server's list are skipped
5. Remaining files are uploaded normally; `CommitIntake` includes the full manifest
   (already-landed + newly-uploaded)

**Partial files** (dropped mid-`UploadFile` stream): the server discards the temp
file on disconnect. The relpath is absent from `ListIntakeFiles`. The agent
re-uploads the full file on resume. Mid-file byte-level resume (using the `offset`
field) is deferred to v2.

**Stale intakes** (`.streaming.json` older than 24h with no activity): swept by the
existing `sutra-agent receive sweep` command; added to `sutra serve-grpc` as a
periodic background task too.

## 12. What changes in the codebase

### New

| Path | Purpose |
|---|---|
| `packages/sutra-agent/proto/intake.proto` | gRPC service definition |
| `packages/sutra-agent/proto/intake_pb2*.py` | Generated stubs (committed) |
| `src/sutradhara/grpc/server.py` | gRPC server + mTLS setup |
| `src/sutradhara/grpc/servicer.py` | Five RPC implementations |
| `src/sutradhara/grpc/assembly.py` | BagIt assembly from streamed chunks |
| `src/sutradhara/grpc/ca.py` | CA / cert issuance / revocation |
| `packages/sutra-agent/src/sutra_agent/grpc_client.py` | Streaming upload client |

### Modified

| Path | Change |
|---|---|
| `src/sutradhara/cli/main.py` | Add `sutra serve-grpc` command |
| `packages/sutra-agent/src/sutra_agent/config.py` | Add `server_address`, cert fields; `landing` optional |
| `packages/sutra-agent/src/sutra_agent/receive.py` | Route to `grpc_client` when `server_address` set |
| `packages/sutra-agent/src/sutra_agent/cli.py` | `--server` / `--client-cert` / `--ca-cert` flags |

### Unchanged

`sutradhara_receive` (BagIt core), `routes_receive.py`, `routes_session.py`,
`routes_intake.py`, Caddy config, Authentik config, all existing HTTP API behaviour,
`sutra serve-api`.

## 13. New dependencies

**Server:** `grpcio`, `grpcio-tools` (already in the Python ecosystem; add to
`pyproject.toml` server extras, not the edge `sutradhara-receive` package).

**sutra-agent:** `grpcio` (add to `packages/sutra-agent/pyproject.toml`).

Neither dependency leaks into `sutradhara-receive` (the lightweight edge package).

## 14. Testing

**Unit:**
- `test_servicer.py`: `StartIntake` idempotency; `UploadFile` happy path + transit
  corruption (mismatched sha256 → receipt error); `CommitIntake` sha256 cross-check
  fail; `ListIntakeFiles` lists only complete files; `GetIntakeStatus` state machine.
- `test_assembly.py`: streamed chunks produce a valid BagIt bag (`bagit.validate()`);
  `operator` is always from the cert CN, never the client payload; sentinel written
  last.
- `test_grpc_client.py`: happy path with a local test server; resume skips landed
  files; `TransitCorruptionError` on sha256 mismatch; `parallelism` semaphore limits
  concurrent RPCs.

**Integration (harness scenario):**
A new scenario (or extension of `scenario_r.py`) that runs `sutra serve-grpc` with a
self-signed test CA, runs `sutra-agent receive` against a fake-source directory, and
asserts `intake.verified.json` is written and the registered intake passes
`sutra intake inspect`. Uses `--fake-source` (already in `sutra-agent` CLI) for CI
without real hardware.

## 15. Open / decided

| # | Question | Decision |
|---|---|---|
| 1 | Proto location | `packages/sutra-agent/proto/` — both client and server are Python in the same repo; move to shared location if a native client (Swift/Go) is ever built |
| 2 | Mid-file byte-level resume | Deferred (v2); `offset` field reserved in proto; re-upload on disconnect is acceptable on 10G LAN |
| 3 | CRL / cert revocation | CN blocklist for v1 (simple, sufficient for a small team); CRL deferred |
| 4 | GUI | Deferred — thin shell over `sutra-agent` CLI, separate design |
| 5 | Parallelism default | 8 concurrent RPCs; configurable per workstation |
| 6 | Chunk size | 4 MB default; configurable; aligns with ZFS recordsize=1M (4 records per chunk write) |
| 7 | gRPC port | 50051 default; bind to LAN/Tailscale interface, blocked on public NIC by existing nftables |
| 8 | Verify job execution | Thread pool within `serve-grpc` process (shares SQLAlchemy engine); move to a job queue worker if it contends with tape I/O in production |
