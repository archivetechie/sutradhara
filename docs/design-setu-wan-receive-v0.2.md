# Design — Setu WAN receive / Signiant replacement (v0.2)

**Status:** detailed, code-grounded design for panel/owner review (2026-07-13).
The owner-approved v0.1 product and engine decisions are carried forward. This
document is the implementation design for Spec 1, includes the executable Spec 0
measurement plan, and is the source from which the per-repository Codex prompts are
to be cut. No implementation has landed.

**Decision (one paragraph).** Build Setu to Signiant parity by retaining
`sutra-agent`'s outbound, enrolled, mTLS control plane and Sutradhara's one receive
admission/verification/archive funnel, while replacing the bulk upload machinery with
a transport-neutral `DataTransport`: QUIC on `quinn` with BBR first, and a bounded
pool of independent TCP/443 gRPC connections as automatic fallback. Both transports
terminate at one new, transport-neutral `IntakeLandingWriter`; only that writer may
turn authenticated chunks into payload files, and only the existing `CommitIntake`
assembly may publish `intake.json` to the existing watcher. Spec 1 is direct from the
managed US site to Coimbatore, with measured self-tuning, a strict bandwidth schedule,
durable chunk resume, and exactly two public sockets (TCP/443 and UDP/443). Spec 2 adds
the ad-hoc portal through the same admission and writer, and Spec 3 adds an opaque
relay only if Spec 0 proves that the direct route, rather than the endpoints, is the
bottleneck.

## 1. Corrections to v0.1 — load-bearing

The v0.1 direction is sound, but several of its implementation premises are not true
in the current code. These are design corrections, not reasons to revisit the fixed
product or engine decision.

### 1.1 “Swap only the data channel” is not a one-call substitution

**Correction: the architectural boundary is narrow, but extracting it is a real
client-and-server refactor.** On the client, `IntakeApi` currently combines control
operations and the streaming data operation in one trait
(`~/sutra-agent/src/relay/intake.rs:55-80`). `stream_file` creates a four-chunk mpsc
queue and starts both the reader and gRPC upload tasks
(`~/sutra-agent/src/relay/intake.rs:975-1017`); the actual wire attachment is
`api.upload_file(receiver)` at `~/sutra-agent/src/relay/intake.rs:1005`, while the
producer-side backpressure is the queue send at
`~/sutra-agent/src/relay/intake.rs:1117-1129` and
`~/sutra-agent/src/relay/intake.rs:1452-1460`. Replacing only line 1005 would leave the
old queue, memory ownership, retry, and backpressure semantics attached to a different
wire. The client seam therefore has to replace the whole `stream_file`/`send_chunk`
coupling while preserving source planning, mutation checks, hashing, package
preparation, commit, and progress reporting.

On the server, the current byte sink is Python `_receive_file`: it requires an offset
of zero followed by strictly sequential chunks, writes a fresh UUID temporary file,
fsyncs it, atomically renames it into `data/`, and appends a receipt
(`src/sutradhara/grpc/servicer.py:283-356`). Its concurrency and “upload in flight”
state are process-local (`src/sutradhara/grpc/servicer.py:61-80`), so a separate Rust
QUIC process cannot safely write alongside it. Spec 1 must extract those filesystem
semantics into one durable, cross-process landing writer before adding either new wire
leg. That is deeper than v0.1 implied, but it still changes only the bulk-data
boundary: `StartIntake`, authorization, commit, BagIt assembly, watcher verification,
deduplication, policy, and archive submission remain the existing owners.

### 1.2 Current “resume” is completed-file resume, not byte/chunk resume

The client journal stores source identity, plan digest, state, and `intake_id`, but no
file offset, range bitmap, or transport session
(`~/sutra-agent/src/relay/inflight.rs:13-29`). Recovery re-plans the source and reuses
the intake (`~/sutra-agent/src/relay/intake.rs:375-404`), then
`ListIntakeFiles` skips only a whole file whose server receipt matches after the client
rehashes it (`~/sutra-agent/src/relay/intake.rs:647-674` and
`~/sutra-agent/src/relay/intake.rs:755-798`). The server refuses a non-sequential
offset (`src/sutradhara/grpc/servicer.py:308-335`) and even clears `.incoming` when no
process-local stream is active (`src/sutradhara/grpc/servicer.py:183-195`). Therefore a
lost 2 TB file restarts at byte zero today. Spec 1 adds a durable chunk/range ledger on
the server and a v2 client journal; this is not optional for intercontinental parity.

There is a second crash-safety defect for multi-hour WAN work: after the runner's two
bounded attempts (`~/sutra-agent/src/relay/intake.rs:628-645`), any task error causes
the supervisor to abort the server intake and delete the journal
(`~/sutra-agent/src/relay/intake.rs:520-557`). Spec 1 must classify transient transport
loss as recoverable and retain both intake and journal. Explicit operator cancellation,
source mutation, authorization failure, or a terminal server rejection still aborts.

### 1.3 QUIC migration is not durable resume

QUIC can validate a new network path for a *live* connection, and `quinn::Endpoint`
supports rebinding its UDP socket. That covers NAT rebinding and some laptop IP changes.
It does not resurrect a connection after process death, server restart, or a long
offline interval. Spec 1 uses migration opportunistically and uses the durable server
range ledger for every new connection. The journal, not QUIC, is the crash-recovery
authority.

### 1.4 The bandwidth cap is not a `quinn` BBR pacing-rate knob

The current public `quinn` BBR API selects the congestion controller and exposes an
initial window, but no application pacing-rate cap. Spec 1 therefore applies one
aggregate token bucket before chunks enter either QUIC or TCP lane queues. BBR remains
responsible for estimating and filling capacity below that ceiling. This keeps the
cap transport-independent and prevents a fallback or an added QUIC connection from
multiplying the site's allowed rate. The implementation basis is the official
[`BbrConfig`](https://docs.rs/quinn/0.11.11/quinn/congestion/struct.BbrConfig.html) and
[`TransportConfig`](https://docs.rs/quinn/0.11.11/quinn/struct.TransportConfig.html)
APIs; the implementation prompt must pin and test one exact `quinn` release.

### 1.5 “Intake → verify → RAO → copy-3” is a program funnel, not one commit call

`CommitIntake` verifies receipts, assembles the standard BagIt directory, removes the
active marker, and returns `verifying` (`src/sutradhara/grpc/servicer.py:197-251`).
Assembly writes BagIt tags and writes `intake.json` last; it deliberately creates no
terminal verification marker (`src/sutradhara/grpc/assembly.py:1-7` and
`src/sutradhara/grpc/assembly.py:40-105`). The watcher then ignores active receives,
validates the completed bag, and routes it to registration
(`src/sutradhara/intake_watch.py:323-384`). Registration performs shared BagIt
validation, catalog insertion, content-hash deduplication, classification, and policy
work (`src/sutradhara/intake.py:268-380`, `src/sutradhara/intake.py:696-727`, and
`src/sutradhara/intake.py:857-936`).

RAO build and multi-pool placement occur later through an explicit pending archive
submission: source hashes are rechecked before `flush_bundle`
(`src/sutradhara/archive_submission.py:45-119`), the builder invokes the local `rem
archive build` CLI (`src/sutradhara/archive_fanout.py:350-442` and
`src/sutradhara/rem_archive_cli.py:35-56`), and fanout writes and verifies each policy
target (`src/sutradhara/archive_fanout.py:495-635` and
`src/sutradhara/archive_fanout.py:707-791`). The copy reconciler considers only
registered intakes and derives desired pools from active policy
(`src/sutradhara/jobs/reconcilers/copy.py:49-121` and
`src/sutradhara/jobs/reconcilers/copy.py:175-206`). Setu must not claim that network
commit itself seals RAO or establishes three copies. Its invariant is stronger and
more precise: Setu produces exactly the same committed BagIt handoff consumed by that
existing program funnel.

### 1.6 The public landing zone does not exist in the current server configuration

The current Python server hosts Device, Intake, and Restore on one mTLS gRPC server
(`src/sutradhara/grpc/server.py:46-91`) but explicitly rejects wildcard and public
binds (`src/sutradhara/grpc/server.py:94-108`). The checked-out repositories do not
contain the akash `public_guard` nft source; only the fixed v0.1 deployment fact and
historical architecture note that the public NIC is default-deny
(`docs/historical/design-streaming-intake-grpc.md:44-55`). Spec 1 therefore includes an
explicit `~/system`/akash deployment work item and an acceptance audit of the live nft
rules. It must not silently reinterpret the existing private listener as public-ready.

### 1.7 Receive-side socket backpressure ends at landing disk, not live RAO work

The watcher skips a directory while `.receiving.json` exists
(`src/sutradhara/intake_watch.py:337-340`), and only `CommitIntake` publishes the final
sentinel. Thus verification, RAO build, and copy placement do not run concurrently
with the data socket today. The live receive loop can and must backpressure on landing
disk and durable-ledger pressure; post-commit verification/archive has a phase barrier,
not a socket feedback loop. Spec 1 preserves that barrier rather than inventing a
second streaming-to-RAO ingest path.

### 1.8 Throughput physics carried as design inputs

The owner-established link model is binding: US↔Mumbai/Coimbatore RTT is roughly
170–260 ms, and a loss-limited single TCP flow can sit around 1–6 Mbps regardless of a
much wider purchased pipe (the Mathis relation scales as
`MSS / (RTT × sqrt(loss))`). Parallel flow goodput is
`min(sum of useful flow rates, tightest aggregate bottleneck)`, not `N ×` as a general
law. It helps while one loss-controlled flow leaves capacity unused or a shaper meters
per five-tuple; once BBR reaches the aggregate bottleneck, more flows only redivide it
and can increase loss/queueing enough to lower goodput. That is why Setu is BBR-first,
uses one global work queue, and treats 1/4/16 as a diagnosis matrix while production
uses a small measured connection/lane ceiling. Sender uplink, receiver downlink, source
disk, landing disk, CPU, and the policy cap are equally eligible to be the real ceiling.

## 2. Code-grounded invariant and component boundary

The current relay already has the correct authority shape: the agent opens no inbound
socket, presents its enrolled certificate, verifies the enrolled CA, and receives all
server authority through one outbound `DeviceService.Connect` stream
(`~/sutra-agent/docs/architecture.md:41-76`). `StartReceive` is dispatched to the
existing `ReceiveSupervisor` (`~/sutra-agent/src/relay/control.rs:226-242`), which
plans and journals before starting the intake
(`~/sutra-agent/src/relay/intake.rs:447-510`). The shared proto is compiled by the agent
from `../sutradhara/proto` (`~/sutra-agent/build.rs:7-30`), so Sutradhara remains the
single proto source.

The one-funnel design is:

```text
managed source / card
        |
        v
existing ReceiveSupervisor: plan -> journal -> StartIntake
        |
        v
DataTransport (one selected session)
   | QUIC/BBR UDP 443              | N independent gRPC/TCP 443 lanes
   v                               v
Rust QUIC ingress              Python IntakeService.TransferChunks
   |                               |
   +----------- same --------------+
                  |
                  v
     sutradhara_receive::IntakeLandingWriter
       .incoming partials + durable range ledger
       -> fsync/checkpoint -> atomic data/<relpath> + receipt
                  |
                  v
existing CommitIntake -> existing assemble_committed_bag -> intake.json LAST
                  |
                  v
existing intake watcher -> shared validate_bag -> catalog/dedup/policy
                  |
                  v
existing arrangement/submission -> local rem CLI RAO -> policy fanout/copy reconcile
```

**The single byte-landing seam is
`sutradhara_receive::IntakeLandingWriter::accept_chunk`.** Both network servers invoke
that function; neither may write under `data/` or `.incoming/` itself. Only
`finish_file` may atomically reveal a completed payload file, and only the existing
Python `assembly.assemble_committed_bag` may publish `intake.json`. The receive package
is the right owner because it already owns canonical paths, hashing, BagIt encodings,
resume semantics, and server validation (`packages/sutradhara-receive/README.md:3-18`),
and its Rust/PyO3 build already serves Python and Rust consumers
(`packages/sutradhara-receive/pyproject.toml:1-31`). It remains dependency-light: the
writer has filesystem/hash/ledger logic but no database, catalog, backend, QUIC, or
Remanence dependency.

The Rust QUIC ingress is a small Sutradhara-owned binary, not a second ingest service.
It terminates QUIC and calls the same Rust writer directly. It authorizes each data
ticket and renews each intake lease through a private Unix-domain control socket to the
Python server; that socket carries no payload bytes and is accessible only to the
Setu service account. TCP fallback remains a method on the existing Python
`IntakeService`, calls the writer through PyO3, and therefore reaches the same landing
state. This avoids copying multi-TB payloads through a Python/Rust IPC hop while keeping
database admission and revocation in the existing server owner.

Filesystem permissions reinforce the invariant: the landing root is owned by the one
Setu service account used by the Python server and QUIC ingress; no web/portal/relay
account receives write access. A repository test rejects direct new writes to
`<intake>/.incoming` or `<intake>/data` outside `IntakeLandingWriter` and the existing
BagIt assembly. This is a guardrail, not the proof by itself; end-to-end scenario SETU
is the proof.

## 3. The client `DataTransport` seam

### 3.1 Split control from data

Refactor the current `IntakeApi` into an `IntakeControlApi` and one data session. The
control API retains `start_intake`, completed-file/resume queries, session negotiation,
`commit_intake`, status, and abort. It does not accept payload bytes. The current
gRPC calls at `~/sutra-agent/src/relay/intake.rs:105-225` remain their implementation,
with the new negotiation and resume RPCs added.

The exact client interface is:

```rust
#[async_trait]
pub(crate) trait DataTransport: Send + Sync {
    fn kind(&self) -> DataTransportKind;

    async fn register_file(
        &self,
        file: DataFileSpec,
    ) -> Result<FileResumePlan, IntakeError>;

    // Owns `chunk` until the server reports the chunk durably checkpointed.
    // Implementations enqueue into one shared work queue; a ready lane steals it.
    async fn submit_chunk(
        &self,
        chunk: PreparedChunk,
    ) -> Result<DurableChunkAck, IntakeError>;

    async fn finish_file(
        &self,
        file: FinishDataFile,
    ) -> Result<FileReceipt, IntakeError>;

    async fn close(self: Arc<Self>) -> Result<(), IntakeError>;
    fn snapshot(&self) -> DataTransportSnapshot;
}

pub(crate) struct DataFileSpec {
    pub file_id: [u8; 32],       // SHA-256 of canonical wire relpath
    pub relpath: String,
    pub size_bytes: u64,
    pub chunk_bytes: u32,
}

pub(crate) struct PreparedChunk {
    pub file_id: [u8; 32],
    pub offset: u64,
    pub payload: bytes::Bytes,
    pub sha256: [u8; 32],
    pub reservoir_permit: OwnedSemaphorePermit,
}

pub(crate) struct FinishDataFile {
    pub file_id: [u8; 32],
    pub relpath: String,
    pub size_bytes: u64,
    pub sha256: [u8; 32],
}
```

`PreparedChunk` is an owned, fixed-size reservoir buffer. Its permit is released only
when the durable ack arrives or the chunk is requeued after a connection failure.
`QuicBbrTransport` and `ParallelTcpTransport` implement this trait and contain their
own lane tasks, but use the same bounded MPMC work queue, cap, progress counters, and
retry classification. There is no `LegacyGrpcTransport`, runtime compatibility flag,
or “old upload” branch: this pre-production migration removes the old direct
`UploadFile` data path after fixtures have been captured. Backout is `git revert` plus
the previous binary.

### 3.2 Precise call-site replacement

Keep `upload_plan`'s bounded multi-file scheduling
(`~/sutra-agent/src/relay/intake.rs:737-753`), `prepare_unit`, package normalization,
source snapshots, and whole-file SHA-256. Package directories are still materialized
as normalized temporary tar files before upload
(`~/sutra-agent/src/relay/intake.rs:864-917`); Spec 0 measures whether that preparation
is a disk/CPU bottleneck, but Spec 1 does not redesign the package contract.

Replace `stream_file` at `~/sutra-agent/src/relay/intake.rs:975-1024` and the
`mpsc::Sender<FileChunk>` argument to `send_file_chunks` at
`~/sutra-agent/src/relay/intake.rs:1040-1148` with:

1. `transport.register_file(DataFileSpec)` to obtain the server's durable missing
   ranges;
2. a sequential file reader that hashes every source byte in order, but allocates and
   submits only missing chunks;
3. multiple outstanding `submit_chunk` futures, bounded by the global byte reservoir;
4. `finish_file` after every missing chunk has a durable ack and the source
   before/after snapshot still matches; and
5. the existing client/server whole-file receipt comparison at
   `~/sutra-agent/src/relay/intake.rs:815-831`.

Sequential reading preserves the current source-mutation and whole-file hash proof
even though lanes may deliver chunks out of order. On reconnect the client reads the
whole source again to recompute SHA-256 but submits only missing ranges. Serializing a
library-specific SHA-256 internal state is rejected: it would make journal format
depend on a hash implementation and could accept a mutated prefix.

The selected `Arc<dyn DataTransport>` lives for one intake and is passed into
`IntakeRunner`; it is not reconstructed per file. That lifetime is what makes the
work queue global: a fast lane can take the next chunk from any file, while
`max_concurrent_files` continues to bound open source files. The existing one
re-upload round at commit (`~/sutra-agent/src/relay/intake.rs:680-706`) remains, with
the semantics in §9.4.

## 4. Negotiation, capability probe, and fallback

### 4.1 Shared proto additions

`proto/intake.proto` currently exposes `StartIntake`, one client-streaming
`UploadFile`, list, commit, status, and abort (`proto/intake.proto:5-12`), and its chunk
has offset but no chunk digest or durable ack (`proto/intake.proto:28-41`). Replace the
production data method with these shared v1 messages/RPCs:

```proto
rpc OpenDataSession(OpenDataSessionRequest) returns (OpenDataSessionResponse);
rpc GetDataResumePlan(GetDataResumePlanRequest) returns (GetDataResumePlanResponse);
rpc TransferChunks(stream DataPlaneFrame) returns (stream DataPlaneFrame);
rpc CloseDataSession(CloseDataSessionRequest) returns (CloseDataSessionResponse);
```

`OpenDataSessionRequest` contains `intake_id`, client protocol versions, QUIC support,
TCP support, maximum local reservoir, and the current journal session id if any.
`OpenDataSessionResponse` contains:

- selected protocol version and a random 256-bit opaque `data_ticket`;
- ticket expiry and renewal deadline;
- public QUIC authority (`host`, UDP port 443, server name) and TCP authority (the
  existing gRPC authority on TCP 443);
- accepted chunk size, maximum in-flight bytes, QUIC connection ceiling, TCP lane
  ceiling, and server receive-reservoir limit;
- server capabilities `{quic_bbr_v1, parallel_tcp_v1, chunk_resume_v1}`; and
- `resume_epoch`, which changes if the server deliberately invalidates partial state.

The ticket is stored only as a hash and is bound in the server database to the intake,
device id, enrolled certificate fingerprint, protocol version, expiry, and one active
transport kind. It is an authorization capability derived from the existing mTLS
identity, not a new identity. The QUIC server also requires the enrolled client cert;
possessing the ticket alone is insufficient.

`DataPlaneFrame` is a `oneof` of `LaneHello`, `SessionHello`, `FileBegin`, `DataChunk`,
`DurableChunkAck`, `FileFinish`, `FileReceipt`, `Checkpoint`, `Error`, and `Goodbye`.
QUIC carries the same prost message bodies in unsigned-LEB128 length-delimited frames;
gRPC carries them directly. Unknown protocol versions, frame types, or enum values are
rejected. The canonical conformance fixtures live in `packages/sutradhara-receive` and
are consumed by Rust, PyO3/Python, and the agent.

`GetDataResumePlan` is paginated by file and returns completed receipts plus durable
missing ranges for partial files. A range is `[offset, end)` aligned to the negotiated
chunk size except the final range. The server is authoritative; the client journal's
checkpoint is only a recovery hint.

### 4.2 Probe state machine

After the existing authenticated `StartIntake`, the client calls
`OpenDataSession`. The server advertises both transports only when their listeners and
landing writer are healthy. The client always tries QUIC first:

1. Construct QUIC TLS from the same enrolled client cert/key/CA and server name used by
   the existing relay.
2. Complete TLS and send `SessionHello {data_ticket, resume_epoch}` on the first bidi
   stream.
3. Accept QUIC only after an authenticated `SessionAccepted` arrives and a zero-byte
   probe receives an application ack.
4. If the OS reports UDP unreachable/unsupported, or no authenticated application ack
   arrives within the probe budget (two attempts within 3 seconds), close the QUIC
   attempt and open TCP lanes.

Only reachability failures trigger connect-time fallback. Certificate failure, unknown
CA, hostname mismatch, revoked enrollment, expired/wrong ticket, protocol mismatch,
intake ownership error, or policy rejection is terminal. Falling back on those errors
would conceal a security/configuration fault.

During transfer, repeated QUIC PTOs are not by themselves a fallback signal; BBR must
be allowed to adapt. A soft fallback occurs only if QUIC makes **zero new durable
payload progress** for 30 seconds, the configured overall stall timeout has not yet
expired, and a one-chunk authenticated TCP health probe succeeds. The transport closes
the QUIC session, asks the server to atomically switch the ticket's active kind,
queries durable ranges, and requeues only unacknowledged chunks. This covers UDP that
passes a handshake but is later blackholed. It does not race both full transports or
switch based on a guessed bandwidth ratio.

### 4.3 Server symmetry

| Concern | QUIC/BBR | Parallel TCP | Shared owner |
|---|---|---|---|
| Public listener | Rust `sutradhara-setu-ingress`, UDP/443 | Existing Python gRPC server, TCP/443 | `sutra serve-setu` supervision |
| TLS identity | rustls TLS 1.3, client cert required | current gRPC TLS, client cert required | same CA/server cert/client enrollment |
| Ticket authorization | private UDS call into Python DB owner | direct `IntakeServicer` check | `GrpcIntake` owner + enrollment fingerprint |
| Lane framing | length-delimited shared proto on QUIC streams | `TransferChunks` bidi gRPC | `proto/intake.proto` fixtures |
| Payload landing | direct Rust call | PyO3 call | `IntakeLandingWriter::accept_chunk` |
| Lease activity | checkpoint callback over private UDS | direct current store call | existing device-intake lease |
| Commit/handoff | no QUIC commit endpoint | existing gRPC `CommitIntake` | existing assembly/watcher |

The Rust ingress never implements `StartIntake`, commit, abort, policy lookup, catalog
registration, or BagIt assembly. Its private UDS methods are only `authorize_ticket`,
`renew_activity`, `release_session`, and `report_transport_stats`. The UDS path is
root-created, mode `0660`, owned by the Sutradhara service group, validates peer
credentials, and is never exposed through nft.

## 5. QUIC/BBR data plane

### 5.1 `quinn` and TLS configuration

Spec 1 pins the exact `quinn` version proven by the spike (the design baseline is
0.11.11) in both Rust lockfiles. Configuration is explicit:

```rust
let mut transport = quinn::TransportConfig::default();
transport.congestion_controller_factory(Arc::new(
    quinn::congestion::BbrConfig::default()
));
transport.max_concurrent_uni_streams(64_u32.into());
transport.max_concurrent_bidi_streams(8_u32.into());
// receive_window, stream_receive_window, send_window, idle timeout and keepalive
// are then derived from the negotiated reservoir and measured BDP.
```

The client builds a rustls 0.23 `ClientConfig` from
`RelayConfig.client_cert`, `client_key`, and `ca_cert`; those are the same files read
by the current tonic channel (`~/sutra-agent/src/relay/transport.rs:85-99`). It verifies
the same configured server name. The server loads the same CA/server cert/server key
used by gRPC (`src/sutradhara/grpc/ca.py:324-328`), uses rustls
`WebPkiClientVerifier`, requires a client certificate, extracts its device CN and
SHA-256 fingerprint, and asks the existing enrollment store to authorize it. That is
the same resolution the current gRPC server performs from peer CN/fingerprint into an
enrolled device/operator (`src/sutradhara/grpc/ca.py:291-321` and
`src/sutradhara/grpc/store.py:549-561`). The current store binds enrolled device id and
fingerprint and records revocation (`src/sutradhara/grpc/store.py:121-143`); QUIC does
not create another registry.

TLS 1.3 early data is disabled for v1. A chunk is not accepted before the certificate
and ticket are authenticated, so replayable 0-RTT cannot write payload. ALPN is exactly
`sutradhara-setu/1`. The public DNS name must be present in the existing CA-issued
server certificate; a public WebPKI certificate is not a substitute for enrolled-CA
verification.

### 5.2 Connection and stream model

One QUIC connection is the default. It owns one bidi control stream and opens one
unidirectional stream per chunk. A chunk stream contains exactly one `DataChunk` header
and payload, then FIN. The control stream carries file begin/finish, durable checkpoint
acks, ticket renewal, errors, and telemetry. A fresh stream per chunk avoids a lost
chunk head-of-line blocking unrelated chunks; the shared byte reservoir, not the stream
count, is the memory bound.

All streams in one QUIC connection share one UDP five-tuple and one congestion
controller. They **cannot** defeat a per-flow shaper. If Spec 0 diagnoses per-flow
shaping, the same `QuicBbrTransport` may maintain 2 and then at most 4 authenticated
connections for one ticket. A 60-second hill-climb epoch adds a connection only when:

- the site cap is not the limiter;
- neither source nor landing disk reservoir is at its high watermark;
- the new connection improves aggregate durable goodput by at least 15%; and
- smoothed RTT and loss/PTO cost do not deteriorate beyond the Spec 0 guardrail.

It removes the last-added connection when marginal goodput is below 5% for two epochs
or aggregate goodput falls. Sixteen connections are a Spec 0 diagnostic point, not a
Spec 1 default. The production ceiling can be lowered by the measured site profile but
cannot exceed 4 without a new reviewed design. This honors the physics: BBR should fill
an aggregate bottleneck with few flows; additional flows exist only for a measured
per-flow shaper and multi-file work.

Chunk assignment is never fixed to a connection or a file partition. Every connection
worker pulls from the same ready queue; the first worker with congestion and flow-control
credit steals the next chunk. A connection loss therefore creates no straggler barrier:
only its unacknowledged work returns to the queue.

### 5.3 Framing and integrity

`FileBegin` binds `file_id`, canonical relpath, total size, chunk size, and resume epoch.
`DataChunk` binds `file_id`, offset, length, and SHA-256 of its payload. The server
rejects unaligned ranges, overlaps with a different digest, a path/file-id mismatch,
data beyond declared size, and any chunk hash mismatch. An identical duplicate is
idempotently acknowledged.

The server uses positioned writes into a deterministic partial file, then checkpoints
a compact range bitmap plus per-chunk digests. At 2 MiB chunks a 2 TB file has 1,048,576
bits (128 KiB) in its completion bitmap; digests are stored in a paged side ledger so
they need not all reside in RAM. `FileFinish` is accepted only when every range is
durable. The writer then reads the complete partial sequentially, computes the existing
whole-file SHA-256, compares it to `FinishDataFile.sha256`, fsyncs, atomically renames
to the confined `data/<relpath>`, fsyncs the parent, and appends the same completed-file
receipt shape used today. The server's independent whole-file hash preserves the
current receipt proof (`src/sutradhara/grpc/servicer.py:334-353`).

### 5.4 Flow control, BBR, and the cap

QUIC flow control is a memory/safety credit system; BBR is the network congestion and
pacing controller. Do not use a small QUIC window as a bandwidth cap. Configure:

- per-stream receive window = one negotiated chunk plus framing;
- connection receive window <= that connection's share of the receive reservoir;
- send window >= the measured bandwidth-delay product plus one reservoir tranche; and
- stream limits <= available fixed buffers.

`quinn` defaults are not assumed to fit a 1 Gbps/250 ms path; windows are derived after
Spec 0 and their total worst-case allocation is logged at session open. The official
transport docs warn that memory scales with windows and concurrency, which is why the
reservoir budget is the controlling value rather than a “large enough” magic window.

The bandwidth schedule feeds one aggregate token bucket **before** the transport work
queue. Tokens represent payload bytes, refill from a monotonic clock, and have a burst
of two chunks. Every QUIC connection and every TCP lane draws from the same bucket.
Schedule transitions change only the refill rate; they do not reconnect. The server
also enforces the signed session ceiling per enrolled device so a modified managed
client cannot exceed its policy. Transport overhead is reported separately; policy
caps useful payload rate.

### 5.5 Migration

The agent monitors local route/address changes without opening a listener. For a live
connection it rebinds the `quinn::Endpoint` and lets QUIC validate the new path; see the
official [`Endpoint::rebind`](https://docs.rs/quinn/0.11.11/quinn/struct.Endpoint.html#method.rebind)
contract. Until validation succeeds, no chunk is called durable merely because it was
written to a stream. If validation fails or the process restarts, the agent opens a new
connection, presents the same mTLS identity and refreshed ticket, asks for the server's
durable ranges, and continues. No “migration succeeded” event removes the client
journal.

## 6. Parallel-TCP fallback

`ParallelTcpTransport` deliberately reuses the existing mTLS gRPC `IntakeService` for
authentication, admission, session negotiation, lease renewal, and commit. It does not
reuse the current `UploadFile` stream because that RPC maps one whole sequential file
to one HTTP/2 stream and the server requires offsets in order
(`proto/intake.proto:5-12`; `src/sutradhara/grpc/servicer.py:308-335`). It uses the new
bidi `TransferChunks` method and shared `DataPlaneFrame` contract.

Each lane constructs and connects an independent tonic `Channel`; cloning the current
cached channel would merely multiplex HTTP/2 streams on one TCP congestion window.
This is why the current `GrpcIntakeApi` `OnceCell<Channel>`
(`~/sutra-agent/src/relay/intake.rs:82-103`) is retained only for control and is not the
fallback lane pool. Every lane uses the existing cert/key/CA builder, with 30-second
keepalive and 20-second timeout matching the already-proven restore channel tuning
(`~/sutra-agent/src/relay/transport.rs:101-120`). Its h2 windows are accounted against
the same reservoir rather than copied blindly from the server's current 4 MiB values
(`src/sutradhara/grpc/server.py:53-61`).

The pool starts with one lane, then tests the Spec 0-approved candidates (normally 1
then 4; 16 is enabled only if Spec 0 proves a material gain and the owner accepts its
fairness cost). It uses the same 60-second marginal-goodput rule and backs down after
the knee. Hard bounds are part of the strict site policy. Every lane consumes the same
work queue, so fast TCP flows steal chunks and a slow flow cannot own a fixed quarter
of a file. The cap and source/disk watermark checks prevent the lane tuner from
mistaking a local bottleneck for a need for more flows.

A broken lane requeues only work without a durable ack and is replaced within the
approved pool ceiling. Authentication, authorization, protocol, path, hash, and policy
errors fail the session; only network/HTTP2 connection failures are lane-retryable.
The QUIC→TCP fallback trigger is exactly §4.2. TCP never automatically switches back
to QUIC during the same intake: that would churn congestion state and complicate
resume. The next intake probes QUIC afresh.

## 7. Disk ↔ socket backpressure

This directly reuses the TIO design shape: a fixed preallocated ring, bounded queues,
one-in-flight submitter, nonblocking buffer return, and close-before-join poison
protocol (`~/remanence/docs/design-tape-io-pipelined-submission-v0.1.md:130-169`). TIO's
RAM budget is explicit rather than incidental
(`~/remanence/docs/design-tape-io-pipelined-submission-v0.1.md:240-246`), and its read
path returns typed buffers with valid lengths while withholding failed data
(`~/remanence/docs/design-tape-io-pipelined-submission-v0.1.md:287-312`). Setu applies
those invariants to disks and sockets; it does not copy tape command semantics.

### 7.1 Sender reservoir

Default negotiated chunk size is the current 2 MiB
(`~/sutra-agent/src/relay/config.rs:259-268`). The sender preallocates 64 buffers, for a
128 MiB payload reservoir. It stops scheduling disk reads at a 96 MiB high watermark
and resumes only below a 64 MiB low watermark. The byte semaphore is authoritative;
watermarks avoid rapid stop/start around full occupancy. Four concurrent source files
may each hold at most one filling buffer outside the reservoir, so the default payload
memory bound is 136 MiB plus protocol and kernel buffers. The implementation reports
all components and refuses a configuration whose checked sum exceeds the configured
`max_transport_memory_bytes` (default 256 MiB).

The sequence is: acquire buffer/token credit → positioned/sequential read → update
whole-file and chunk SHA-256 → snapshot check point → enqueue → lane write → durable
ack → return buffer. A lane never reads the source itself. On poison/error, close the
work queue before joining lane and reader tasks, drain/requeue unacknowledged items,
and return every permit; no bounded-credit cycle may leave a reader blocked forever.

The cap is acquired after a free buffer but before enqueue. When the cap is low, the
reservoir therefore does not fill with data that cannot yet be sent. When disk is slow,
lanes naturally starve and telemetry says `source_disk_limited`; the tuner does not add
connections.

### 7.2 Receiver reservoir and durable checkpointing

The server has one global 512 MiB payload budget and a default 256 MiB per-intake
budget. For one intake, high/low watermarks are 192/128 MiB. Network handlers obtain
byte credit before retaining a decoded payload. At the high watermark they stop
reading new QUIC streams/gRPC messages until the landing submitter drains below low;
QUIC flow-control credit and TCP receive windows carry the pressure back to the sender.

There is one landing submitter per active intake, with at most one filesystem write
operation in flight, and a global disk semaphore bounds concurrent intakes. It consumes
validated chunk descriptors, performs positioned writes, and groups durability into a
checkpoint at the earlier of 64 MiB newly written or 2 seconds. A `DurableChunkAck`
names only ranges covered by `fdatasync` plus an atomically written/fsynced ledger
checkpoint. Network receipt without that checkpoint is not durable; after a crash the
client may resend it. This batching avoids one fsync per 2 MiB while retaining a
bounded retransmit window.

The on-disk state is:

```text
<intake>/.incoming/setu-v1/
  session.json                 # schema, intake, owner binding, resume epoch
  session.lock                 # cross-process exclusive active-transport lock
  files/<file-id>.partial
  files/<file-id>.ranges       # atomic range/checkpoint ledger
  receipts.jsonl               # completed files, same logical receipt contract
```

All paths are created through `safe_payload_path`/canonical path logic already shared
by the receive package; the current server already applies that confinement before its
atomic rename (`src/sutradhara/grpc/servicer.py:292-297`). The session lock means QUIC
and TCP cannot write one intake concurrently. A fallback first releases the QUIC lease,
then acquires TCP with a bumped transport generation; late frames from the old
generation are rejected.

`ListIntakeFiles` must stop deleting `.incoming`; the writer owns cleanup. Abort removes
the intake only after all writer handles are closed. Commit obtains the exclusive
session lock, verifies no uncheckpointed/in-flight chunks, reads completed receipts,
and then executes the existing compare-and-set and assembly. This replaces the current
process-local `runtime.in_flight` gate (`src/sutradhara/grpc/servicer.py:197-220`) with
a durable cross-process gate.

### 7.3 Phase boundary into verify/RAO

After all files finish, `CommitIntake` performs the same manifest comparison and
one-reupload response, then calls the existing assembly
(`src/sutradhara/grpc/servicer.py:222-249`). `intake.json` remains last
(`src/sutradhara/grpc/assembly.py:97-105`). Only then can the watcher hash the bag and
register it. Slow verification/RAO/copy work therefore cannot OOM the socket reservoir;
it begins after the socket session is closed. Landing disk capacity and later archive
backlog need separate operational alerts, but combining their queues would violate the
one-funnel phase barrier.

## 8. Public ingress and landing zone

### 8.1 Exactly two public sockets

Use a dedicated public Setu IP and DNS name on akash. Open exactly:

1. **TCP/443** — mTLS gRPC carrying `DeviceService.Connect`, Intake control/commit,
   and `ParallelTcpTransport.TransferChunks` fallback.
2. **UDP/443** — mTLS QUIC/`sutradhara-setu/1` carrying bulk chunks.

These are two protocol/socket rules even though both use port number 443. No public
50051, enrollment database, metrics listener, HTTP upload endpoint, UDS, SSH, or admin
port is added. Enrollment continues through its existing protected route; Setu data
connections require an already enrolled cert.

A dedicated IP is an implementation prerequisite because the existing system UI also
uses TCP/443 and may use HTTP/3/UDP/443. Sharing by ALPN is unsafe (web traffic can also
be HTTP/2), and sharing UDP/443 would collide with HTTP/3. The deployment prompt must
either allocate that IP or return to the owner; it must not silently insert a generic
TLS proxy in front of mTLS.

`sutra serve-setu` binds both listeners to that exact configured address and refuses
`0.0.0.0`/`::`. The current general gRPC server's public-bind rejection remains correct
for `serve-grpc` (`src/sutradhara/grpc/server.py:94-108`); the new command has a
Setu-specific exact-address validator and makes the public trust boundary explicit.
The Python and Rust listeners load the same PKI directory, and the supervisor fails
startup if either socket, PKI, landing writer, DB/UDS authorization, or filesystem
permission check fails. During a later QUIC-process failure, existing sessions resume
over TCP by the specified automatic fallback; this is product behavior, not a backout
flag.

### 8.2 `public_guard`

The `~/system` deployment prompt adds destination-address-specific nft accepts for
`SetuIP tcp dport 443` and `SetuIP udp dport 443` inside the existing `public_guard`
default-deny table, with established/related return traffic and rate-limited logging of
drops. IPv6 is either configured equivalently with a dedicated address and tested, or
has no Setu listener/accept rule; never bind IPv6 accidentally. Source-IP allowlisting
may be layered for the managed US site, but mTLS remains mandatory because ad-hoc Spec
2 cannot rely on stable source IPs.

Acceptance captures `nft list ruleset`, listening sockets, and negative probes to every
other host port from an off-tailnet machine. Since the actual akash rules are not in the
mounted repositories, this live audit is a hard deployment gate, not a paper claim.

### 8.3 Direct landing

Spec 1 is direct US→Coimbatore. No cloud staging bucket or POP receives plaintext. The
server checks free space before issuing a ticket: declared planned bytes plus the
configured landing reserve must fit, accounting for partial files. Admission is
per-device and globally bounded. Stale partial cleanup uses the durable session expiry
and explicit aborted/terminal state; it must not reuse the current blanket 24-hour
sweep for a legitimate multi-day transfer (`src/sutradhara/grpc/server.py:111-125`).

## 9. Managed site-to-site path (Spec 1)

### 9.1 Reuse the existing headless daemon and control stream

`sutra-agent serve` is already the headless entry point: it loads `RelayConfig`, takes
the daemon lock, secures identity files, installs rustls, and runs `ControlDaemon`
(`~/sutra-agent/src/main.rs:18-46`). Spec 1 extends that daemon; it does not introduce a
second “Setu agent.” The current long-lived outbound stream sends heartbeat/inventory
and accepts server commands (`~/sutra-agent/src/relay/control.rs:151-200`). Add
`ManagedSourceOffer` and `ManagedSourceWithdrawn` to the client→server `DeviceMessage`
oneof; `StartReceive` remains the only command that authorizes an upload. The current
proto has no source-offer message (`proto/device.proto:22-35`) and only
`StartReceive`/`ListDirectory` server commands (`proto/device.proto:65-79`), so this is
an additive control-plane extension, not an autonomous ingest path.

Enrollment is the existing mechanism: the agent generates its key locally, presents a
CSR, and stores the returned certificate owner-only
(`~/sutra-agent/docs/architecture.md:86-104`). The US site uses an ingest-scoped,
long-lived managed-device enrollment with normal rotation/revocation. It does not use
the short-lived contributor grant previewed for Spec 2.

### 9.2 Workflow feed

The managed-site policy defines local source roots and one of two completion signals:

- **workflow spool (preferred):** the production workflow atomically renames a strict
  `setu-source-v1.json` descriptor into a local spool. It contains a locally confined
  source path, stable workflow id/idempotency key, label, and declared completion time.
- **watched ready directory:** the workflow atomically renames a completed file or
  directory into a configured ready root, or writes a sibling `.setu-ready` marker
  last. Mere “size unchanged for N seconds” is not sufficient for multi-TB sources.

The agent canonicalizes and confines the source under its configured root, plans it
with the existing payload planner, and emits an opaque offer (root id, relative path,
plan digest, planned bytes, workflow id)—never an absolute local path. The server
matches the offer to the managed-site policy, creates the normal receive intent, and
sends the existing `StartReceive`. The agent then follows the same journal,
`StartIntake`, `DataTransport`, commit, and confirmation path as a card. No watcher
writes directly to a landing directory or calls transport code.

Processed descriptors move atomically to `accepted/`, `complete/`, or `failed/`; they
are not deleted until the server terminal marker is reflected through the control
plane. Replayed workflow ids are idempotent. A source mutation after planning is the
same terminal safety error enforced by current before/after snapshots
(`~/sutra-agent/src/relay/intake.rs:1051-1147`).

An enrolled Bangalore site uses this same public endpoint, offer/control flow, and
transport selector. Its shorter path may need only one TCP lane or one QUIC connection,
as measurement and telemetry will show; it does not get a domestic-only ingest
implementation.

### 9.3 Strict scheduler/bandwidth policy

Setu site policy is a versioned JSON document with `#[serde(deny_unknown_fields)]` at
every object. Unknown keys, invalid time zones, overlapping contradictory windows,
nonpositive rates, memory sums above the host ceiling, or lane ceilings outside the
reviewed bounds are errors. The minimum shape is:

```json
{
  "schema": "setu-managed-site-policy-v1",
  "site_id": "us-managed-1",
  "timezone": "America/Los_Angeles",
  "roots": [{"root_id": "delivery", "path": "/srv/delivery", "completion": "ready-marker"}],
  "bandwidth": {
    "default_bps": 250000000,
    "windows": [
      {"days": ["mon", "tue", "wed", "thu", "fri"], "start": "07:00", "end": "22:00", "bps": 100000000}
    ]
  },
  "transport": {"quic_connections_max": 4, "tcp_lanes_max": 4},
  "memory": {"max_transport_bytes": 268435456},
  "queue": {"max_active_intakes": 1, "order": "fifo"}
}
```

Rates above are examples, not defaults to ship; Spec 0 and owner policy choose them.
The schedule uses the named IANA zone and records which side of a daylight-saving fold
was chosen. An already running transfer is paused by setting refill to zero, not
aborted; heartbeats, lease renewal, and control stay live. All active intakes on the
device share the one aggregate bucket. Server-issued policy is signed/bound to the
device and cached durably, but local hard ceilings may only reduce its rate/memory—not
increase server authorization.

## 10. Resume at intercontinental scale

### 10.1 Client journal v2

Replace `sutra-agent-inflight-v1` with strict `sutra-agent-inflight-v2`. In addition to
the current fields it stores `data_protocol`, `data_session_id`, `resume_epoch`,
negotiated chunk bytes, last server checkpoint sequence, selected transport/fallback
reason, source file identity snapshots, and last activity. It does **not** rewrite the
JSON for every chunk. Server checkpoints are coalesced; the client atomically persists
at most every 64 MiB or 2 seconds using the existing temp+fsync+replace+parent-fsync
pattern (`~/sutra-agent/src/relay/inflight.rs:216-238`).

On daemon restart:

1. rediscover and re-confine the source;
2. compare source plan and per-file identity/size/mtime;
3. call the idempotent existing `StartIntake` recovery path;
4. open/renew a data ticket and page through `GetDataResumePlan`;
5. sequentially rehash each partial source, submitting only missing server ranges;
6. complete/commit through the normal path.

The server range ledger is authoritative after either side crashes. A client checkpoint
ahead of the server is ignored; a server checkpoint ahead of the client saves the bytes
without special recovery.

### 10.2 Failure classification

Recoverable without abort: connection loss, QUIC path validation failure, TCP lane
failure, server restart/unavailable, client restart, sleep/network change, ticket
expiry while the control plane can renew, and timeout with a still-valid source. These
retain the journal, source lease where possible, partial landing, and server intake.

Terminal and aborting: explicit operator cancel, source mutation, source disappearance
beyond the owner-approved grace interval, revoked/unauthorized identity, intake owner
conflict, protocol/hash/path conflict, server-declared policy rejection, or the second
commit re-upload request. Disk-full pauses admission/transfer and alerts; it does not
delete partial bytes.

The current control reconnect loop already backs off 1 second to the configured maximum
(`~/sutra-agent/src/relay/control.rs:107-116`). Data reconnection uses jittered capped
backoff but never blocks heartbeat/control reconnect. `stall_timeout_seconds` measures
absence of durable progress, not time spent intentionally paused by a zero-rate schedule.

### 10.3 IP changes and multi-hour lifetime

Live IP/NAT change first tries QUIC migration (§5.5). TCP has no migration, so failed
lanes reconnect and resume from durable ranges. Tickets are short enough to limit theft
(15-minute cryptographic expiry) but renewable over the long-lived mTLS control stream;
an active multi-day transfer is not forced to restart. Independently of ticket expiry,
the new server activity lease rechecks enrollment/revocation no less often than every
20 seconds, matching the current default client heartbeat at
`~/sutra-agent/src/relay/config.rs:243-252`. Revocation stops renewal and terminates the
active data generation at the next activity checkpoint, so its designed propagation
bound is 20 seconds rather than the ticket lifetime.

### 10.4 One re-upload rule

Transport retransmission and missing-range recovery are not a commit “re-upload.” They
are idempotent delivery of one file before a receipt exists and may repeat until the
durable range set is complete. The existing one-re-upload rule begins only after every
file has a completed server receipt and `CommitIntake` reports a receipt/manifest
mismatch. For that requested relpath the server bumps its file generation, removes its
completed receipt and partial ledger under lock, and the client sends the whole file
once with `ForceUpload::Yes`. A second commit request for any re-upload remains the
current hard error (`~/sutra-agent/src/relay/intake.rs:680-706`).

## 11. Spec 0 measurement spike

Spec 0 answers *which* bottleneck exists before tuning Spec 1. It does not choose the
already-fixed engine. Run from the actual managed US source host to the actual akash
landing host, in both directions where practical, during at least one business-hour and
one overnight window. Use the same representative corpus for Setu spike and Signiant.

### 11.1 Prerequisites and evidence

Record:

- timestamp/time zone, host/kernel/NIC, ISP plan, wired/Wi-Fi, VPN, public IP path;
- sender source filesystem and receiver landing filesystem;
- `ping` RTT distribution and loss, `mtr` path/loss as observational evidence;
- sender disk sequential read and SHA-256 throughput; receiver create/write/fdatasync
  throughput with the same file/chunk/checkpoint sizes; CPU and memory headroom;
- iperf3 version and exact commands; QUIC spike git revision/config; and
- one real Signiant job log with useful bytes, start/finish, average/interval goodput,
  concurrency, retry/loss if exposed, and the same source corpus.

Do not infer WAN capacity from the ISP label. The tightest measured sender disk,
sender uplink, receiver downlink, receiver landing disk, CPU, or policy cap is the
candidate physical ceiling.

### 11.2 Matrix

Run each cell three times after a warm-up, retain interval JSON/CSV, and alternate order
to reduce time-of-day bias:

| Test | Parallelism | Direction/notes |
|---|---:|---|
| iperf3 TCP | `-P 1`, `-P 4`, `-P 16` | US→India and reverse; 120 s each |
| iperf3 UDP | paced sweep below/at/above observed TCP aggregate | loss, jitter, reachability; never flood blindly |
| Setu QUIC+BBR spike | 1, 4, 16 QUIC connections | 16 is diagnosis only; same chunk/reservoir |
| Setu parallel TCP spike | 1, 4, 16 independent TCP connections | shared work queue, not fixed partitions |
| real payload | disk→Setu→landing disk at 1/4/16 | hash/checkpoint enabled |
| Signiant | its actual production settings | same corpus, route, and time window |

For every interval record useful/durable goodput, wire rate, RTT, loss/retrans/PTO,
connection count, sender disk read, receiver disk write/fdatasync, CPU, reservoir
occupancy, cap state, and stalls. A memory-to-memory result is not a product result;
the real-payload row is mandatory.

Concrete operator sequence (replace bracketed values, preserve every emitted JSON
artifact):

```text
# 0. Agree a maintenance window. Temporarily allow the US test source only to the
#    dedicated iperf3 port; capture the rule before/after and remove it at the end.

# 1. Endpoint baselines on approved scratch, not an archive payload or root disk.
fio --name=setu-read  --filename=[64GiB-source-fixture] --rw=read  --bs=2M \
    --direct=1 --iodepth=1 --runtime=120 --time_based --output-format=json
fio --name=setu-write --filename=[landing-scratch]/setu-fio.tmp --rw=write --bs=2M \
    --direct=1 --iodepth=1 --size=64G --fsync=32 --output-format=json
/usr/bin/time -v sha256sum [64GiB-source-fixture]

# 2. On India: one maintenance-only server per run.
iperf3 -s --one-off -p [temporary-source-allowlisted-port] -J

# 3. On US, repeat P=1,4,16 three times, 10 s warm-up omitted from the result.
iperf3 -c [setu-dns] -p [port] -t 120 -O 10 -P 1  -J
iperf3 -c [setu-dns] -p [port] -t 120 -O 10 -P 4  -J
iperf3 -c [setu-dns] -p [port] -t 120 -O 10 -P 16 -J

# 4. UDP reachability/loss sweep. Rates come from the prior aggregate result;
#    use below/near/above points approved for the link, never an unbounded flood.
iperf3 -c [setu-dns] -p [port] -u -b [rate] -t 120 -O 10 -J

# 5. Run the pinned Spec 0 mover against the same hosts/corpus.
setu-bench send --transport quic-bbr   --connections [1|4|16] \
    --source [corpus] --json-out [artifact]
setu-bench send --transport parallel-tcp --connections [1|4|16] \
    --source [corpus] --json-out [artifact]

# 6. Run the real payload path with hashing, 2 MiB chunks, reservoir and grouped
#    durable checkpoints enabled; then run Signiant on the same corpus/window.
# 7. Remove the temporary iperf rule and capture the restored default-deny ruleset.
```

`fio --fsync=32` approximates one 64 MiB durability group at 2 MiB blocks; the real
Setu run remains authoritative because its atomic ledger/checkpoint work is not modeled
fully by fio. If a 64 GiB fixture is too small to leave cache effects behind on the
actual hosts, increase it beyond available RAM and record the chosen size. Reverse
iperf uses `-R` with the same matrix. The evidence packet includes stdout/stderr, exit
status, host telemetry, and exact commands rather than transcribed headline numbers.

### 11.3 Diagnosis readout

The report must end in one of these evidence-backed diagnoses (or a mixed one):

| Observation | Diagnosis | Spec 1 consequence |
|---|---|---|
| TCP 1 is 1–6 Mbps; TCP 4/16 rises strongly; one QUIC BBR connection approaches endpoint/link ceiling | loss/RTT-limited TCP | one QUIC connection; small TCP fallback pool |
| 4 materially beats 1, 16 is flat or worse in both engines | parallelism knee | production ceiling at the knee, never 16 |
| QUIC/TCP/iperf all plateau together below disk limits | aggregate last-mile/peering ceiling | transport cannot exceed it; cap/tune to ceiling |
| memory tests are fast but real payload tracks source read or landing write/fdatasync at high occupancy | endpoint disk/CPU bottleneck | fix disk/package/checkpoint path before more flows |
| QUIC handshake/progress fails while TCP is healthy | UDP block/throttle | validate automatic fallback; no POP inference yet |
| direct engines reach endpoint limits but both remain materially below Signiant on same corpus/time | route/peering advantage | Spec 3 relay experiment is justified |
| 4/16 improve QUIC as separate connections, but more streams inside one connection do not | per-five-tuple shaper | enable measured small QUIC connection pool |

The physics guard is explicit: aggregate throughput is
`min(sum of useful flow rates, sender disk/uplink, receiver downlink/disk, policy cap)`.
No report may call N× throughput a general law. Once BBR fills the bottleneck, added
connections merely re-slice it and can reduce goodput.

### 11.4 Spec 0 deliverable and gate

Produce one immutable packet: raw logs, commands/config, `setu-spec0-summary.json`, and a
short diagnosis. It proposes the Spec 1 chunk size, reservoir, QUIC connection ceiling,
TCP lane candidates, checkpoint batch, cap schedule, and parity target. The default
acceptance proposal is:

- Setu median durable goodput >= 90% of the same-window Signiant median; and
- Setu median durable goodput >= 85% of the diagnosed usable endpoint/link ceiling;
- zero integrity discrepancies, bounded configured memory, and successful resume/fallback.

The owner may set a different numeric target after seeing the data, but must set it
before Spec 1 performance acceptance. A POP is tested only when the last diagnosis row
applies; published 1.5–2.5x cloud-backbone gains are not used as a substitute for this
link's measurement.

## 12. Verification member and observability

### 12.1 Goodput proof

Useful goodput is **unique payload bytes durably checkpointed divided by active transfer
wall time**. It excludes retransmissions, duplicate chunks, protocol/TLS overhead,
scheduled zero-rate pauses, and time after the final durable byte. Report both active
and end-to-end workflow wall time so preprocessing and commit are not hidden.

The agent sends structured `TransferTelemetry` over the existing outbound
`DeviceService.Connect` stream; it opens no metrics listener, preserving the no-inbound
invariant enforced by the current architecture
(`~/sutra-agent/docs/architecture.md:4-10`). The server stores bounded interval samples
and exposes the dashboard through the existing authenticated operator UI. Per transfer,
show:

- transport and fallback reason; QUIC connections/streams or TCP lanes;
- unique durable goodput and wire rate; RTT, loss/PTO/retransmission;
- BBR congestion window/pacing estimate where `quinn` exposes it;
- cap rate and time cap-limited;
- sender/receiver reservoir occupancy and high-watermark time;
- source read, landing write/fdatasync, CPU, checkpoint latency;
- resumed bytes saved, duplicate/retransmitted bytes, and commit/re-upload state; and
- diagnosed current limiter (`cap`, `source_disk`, `network`, `landing_disk`, or
  `unknown`) with the inputs that led to the label.

The acceptance report overlays Setu and the real Signiant interval log for the same
corpus/link/window and evaluates the Spec 0 target. A single peak screenshot is not
parity evidence.

### 12.2 Hermetic `~/system` scenario

Add **Scenario SETU — WAN receive single funnel**, contract slug `setu-wan-receive`,
`hermetic_capable = true`, covering `sutradhara.device.relay`,
`sutradhara.intake.setu_transport`, `sutradhara.intake.receive`, and
`sutradhara.intake.accept`. This closes a real current gap: Scenario RDD invokes
`IntakeServicer.UploadFile` directly (`~/system/scenarios/scenario_receive_dedup.py:330-370`),
while Scenario IW.5 still says the real Rust agent streaming binding is retired/unwired
(`~/system/harness/seams/intake.py:200-240`). Existing RDD remains a funnel regression;
SETU crosses the actual transport seam.

The scenario uses loopback CA/enrollment, test-local SQLite/landing, the real Python
gRPC server, real Rust QUIC ingress, and a release/debug `sutra-agent`. A userspace UDP
fault proxy supplies delay/loss and can blackhole UDP without root/netem. Steps:

1. **SETU.1 identity/probe:** enroll the real agent, prove QUIC and TCP present the same
   device cert/fingerprint, and reject an unenrolled cert on both.
2. **SETU.2 QUIC funnel:** managed source offer → existing `StartReceive` → QUIC chunks
   → shared writer → existing commit/watch; assert one verified catalog intake and
   expected content hash.
3. **SETU.3 chunk resume:** kill the agent after a durable mid-file checkpoint, restart,
   assert only server-declared missing ranges cross the wire, and produce the identical
   final bag.
4. **SETU.4 UDP fallback:** blackhole UDP, assert the bounded probe selects TCP, at
   least two independently accepted TCP connections consume one work queue, and final
   bag/catalog output equals SETU.2.
5. **SETU.5 backpressure/cap:** slow the landing writer, assert high/low stop-start,
   measured buffer counters never exceed configured memory, control heartbeats remain
   live, and a deterministic fake clock enforces schedule/cap.
6. **SETU.6 fail closed:** corrupt a chunk, conflict an offset, revoke the cert, and
   attempt commit with active/uncheckpointed data; all fail without `intake.json` or a
   catalog row. Auth/protocol failure must not fall back.
7. **SETU.7 one funnel/parity:** compare QUIC and TCP BagIt/receipt fixtures byte for
   byte except declared transport telemetry, run the watcher, and assert the normal
   quarantine/dedup/policy behavior.

The scenario asserts a transport-neutral landing-writer call counter and forbids either
server leg from creating `intake.json`. Existing Scenario RDD already proves the real
servicer→watcher→catalog flow and is registered hermetically with receive/accept cover
(`~/system/scenarios/contracts.toml:465-476`); SETU extends rather than replaces that
evidence.

## 13. Security posture and future seams

### 13.1 Spec 1 public surface

- Both public sockets require TLS 1.3 mTLS with the existing enrolled cert/CA. The data
  ticket is bound to cert fingerprint, device, intake, expiry, protocol, and active
  transport generation.
- No data plane can mint an intake or select artifact class/policy. `StartIntake`
  currently requires an authorized receive intent and records owner/source metadata
  (`src/sutradhara/grpc/servicer.py:82-162`); that remains the gate.
- Paths are canonicalized and confined in the shared writer. Size, chunk count, range,
  session, concurrent-intake, disk-reserve, and memory limits are checked before
  allocation/write.
- QUIC 0-RTT is off. Tickets are random, stored hashed, short-lived, and never logged.
  Logs use intake/device ids and a redacted ticket prefix at most.
- Revocation/lease loss terminates the writer generation. The current server renews the
  device intake lease on upload activity and aborts after lease loss
  (`src/sutradhara/grpc/servicer.py:358-398`); QUIC checkpoint callbacks preserve that
  behavior.
- The control stream and telemetry stay responsive under bulk load by using separate
  sockets/tasks and memory budgets. A full data reservoir cannot consume its channel.
- Server/client policy documents reject unknown keys. This is especially important
  because the current general `RelayConfig` deserializer normalizes fields but does not
  reject unknown ones (`~/sutra-agent/src/relay/config.rs:113-205`); Setu policy is a
  separately strict contract.

### 13.2 Spec 2 preview — ad-hoc contributor

Spec 2 adds a `contributor_push` enrollment scope to the current explicit device-scope
registry (today only `ingest` and `restore` are valid:
`src/sutradhara/grpc/store.py:33-36`). It uses the same CSR/cert identity mechanism but
a short-lived, one-purpose grant. Server authorization forces:

- push-only; no browse, list-directory, restore, catalog query, or arbitrary artifact
  class;
- one intake/declared byte ceiling, server rate limit, and short expiry;
- a quarantine-only admission policy until the existing watcher validates it; and
- no reuse after terminal state.

The native contributor client can use the same `DataTransport`. A later browser upload
terminates at a portal service with no landing filesystem permission and submits
through a tightly scoped internal client of the same `IntakeLandingWriter`/commit
contract; it cannot write a loose object or publish `intake.json`. Spec 1's session,
writer, and policy binding therefore support Spec 2 without creating a trusted second
ingest path.

### 13.3 Spec 3 preview — relay POP

A relay is deferred until Spec 0 shows direct Setu below endpoint capacity and below
Signiant because of route/peering. The Spec 1 frame/session model separates
authorization/routing metadata from payload chunks, so a future relay can forward an
end-server-authenticated stream. QUIC TLS to a relay is only hop encryption; Spec 3 must
add object/frame-level end-to-end encryption whose keys are held only by the agent and
Coimbatore endpoint. The POP must see no plaintext and persist only opaque,
expiry-bounded ciphertext. Decryption still occurs before the same final
`IntakeLandingWriter`, and final whole-file SHA/BagIt verification remains in
Coimbatore. Spec 1 does not ship dormant relay code, flags, or cloud dependencies.

## 14. Dependency-ordered build order

Each row is intended to become one future Codex prompt. “Verification member” is part
of that prompt's done condition. Every production-code prompt runs the owning repo's
full tests; no runtime backout/compat flag is introduced.

| ID | Prompt-sized work item | Repository/repositories | Verification member |
|---|---|---|---|
| S0.1 | Build the disposable QUIC/BBR and parallel-TCP measurement binaries plus JSON telemetry; no intake integration | `~/sutra-agent` (sender spike), `~/sutradhara` (receiver spike) | loopback deterministic transfer/hash tests; exact-version lock; Spec 0 runbook dry run |
| S0.2 | Execute the §11 real-link matrix and freeze the diagnosis packet/targets | operational evidence in `~/system` (no production transport) | raw 1/4/16 iperf3/QUIC/TCP/Signiant logs and signed-off `setu-spec0-summary.json` |
| S1.1 | Define shared data-session proto, length-delimited framing, strict canonical fixtures, and regenerate Python/Rust agent stubs | `~/sutradhara` proto/fixtures; generated consumption in `~/sutra-agent` | proto unknown-version/fuzz/round-trip tests in both repos |
| S1.2 | Implement `IntakeLandingWriter` durable partial/range ledger in the dependency-light Rust receive crate and expose it through PyO3; no network code | `~/sutradhara/packages/sutradhara-receive` | Rust crash-point tests, Python binding parity, path/range/hash conflicts, bounded bitmap fixtures |
| S1.3 | Refactor current Python `UploadFile`/commit coordination onto the shared writer, replace process-local inflight state with durable lock/ledger, then remove the old production byte writer | `~/sutradhara` | existing gRPC tests + Scenario RDD; golden old/new completed-bag fixtures; crash between checkpoint/fsync/rename |
| S1.4 | Split `IntakeControlApi` from the exact `DataTransport` trait and refactor reader/reservoir/source hashing onto a fake transport, without adding QUIC/TCP yet | `~/sutra-agent` | fake fast/slow/failing transport tests; memory/watermark/poison; source mutation; existing commit one-reupload tests |
| S1.5 | Add `OpenDataSession`, hashed ticket DB rows, resume-plan pagination, transport-generation switch, lease callbacks, and private UDS authorization API | `~/sutradhara` | mTLS owner/revocation/expiry tests; cross-process lock; no payload over UDS; commit rejected while active |
| S1.6 | Implement `ParallelTcpTransport` with N genuinely independent tonic channels, bidi frames, one work queue, cap, and bounded lane tuner | `~/sutra-agent` client plus `~/sutradhara` `TransferChunks` server | multi-connection identity assertion, fast-lane work stealing, lane failure/resume, TCP completed-bag parity |
| S1.7 | Implement Rust QUIC ingress and `QuicBbrTransport`, same-CA mutual TLS, BBR config, framing, cap, migration/reconnect, and bounded connection tuner | `~/sutradhara` ingress binary plus `~/sutra-agent` client | TLS matrix, BBR selected in stats, loss/reorder/duplicate tests, QUIC completed-bag parity |
| S1.8 | Wire capability advertisement, hard/soft UDP-block detection, terminal-error classification, atomic QUIC→TCP switch, and journal v2 recovery | `~/sutra-agent` and `~/sutradhara` | UDP blackhole, auth-does-not-fallback, mid-file kill/restart, multi-hour fake-clock ticket renewal |
| S1.9 | Extend existing headless daemon with strict managed-site policy, ready-spool/watched-root offers, server intent matching, scheduler, and aggregate cap | `~/sutra-agent` agent/workflow plus `~/sutradhara` offer/admission control | confinement/idempotency/DST/fake-clock tests; no autonomous upload; existing no-inbound source scan |
| S1.10 | Add `sutra serve-setu`, exact-address listeners/supervision, dedicated-IP deployment, nft `public_guard`, service permissions, and runbook | `~/sutradhara` command/service; `~/system` akash deployment | config rejects wildcard, socket/PKI/permission checks; live nft/listener/negative-port evidence |
| S1.11 | Add telemetry storage/dashboard, A/B report generator, and full hermetic Scenario SETU | `~/sutra-agent`, `~/sutradhara`, and `~/system` | SETU.1–SETU.7 plus Spec 0 parity report against the frozen Signiant log |
| S1.12 | Performance acceptance and operational freeze using actual managed-site policy | evidence/runbook in `~/system`; no new engine branch | target in §11.4, 24-hour scheduled soak, restart/IP-change/fallback, disk-full pause, zero hash discrepancies |

S1.1–S1.5 establish the one-funnel correctness base. S1.6 deliberately lands TCP
first so the public control/data contract and resume writer can be proven without QUIC
loss variables. S1.7 adds the chosen primary engine; S1.8 makes selection automatic.
No prompt may shortcut S1.2 by letting either network leg write files itself.

Spec 2 begins only after S1.12 with contributor scope/grants, quarantine policy, native
portal client, then optional browser transport—all targeting the same open-session,
writer, commit, and watcher seams. Spec 3 begins only after a measured direct-route
failure and adds opaque frame relay plus end-to-end encryption; it does not alter the
landing or catalog contract.

## 15. Open questions for panel/owner

1. **Public address:** can akash receive a dedicated public IPv4 (and optionally IPv6)
   plus Setu DNS name so TCP/UDP 443 do not collide with the existing UI/HTTP3 surface?
2. **Spec 0 facts:** what are the actual US source host/site, disks, uplink, allowed
   test windows, representative corpus, and available Signiant log? These determine
   connection/lane ceilings and the acceptance target.
3. **BBR variant risk:** is `quinn`'s currently experimental BBR implementation accepted
   for Spec 1 after the spike, and which exact version is frozen? If the owner requires
   BBRv2/v3 semantics specifically, that is a separate dependency decision, not a
   tuning label.
4. **Managed workflow completion:** can the US workflow atomically emit a ready
   descriptor/marker, or must Setu integrate a named workflow API? Passive size
   stability is intentionally rejected.
5. **Bandwidth policy:** what business-hour/overnight rates, time zone, aggregate scope
   (one host vs all US agents), and fairness expectations should be encoded? Is zero-rate
   pause during office hours required?
6. **Partial retention/disk reserve:** how many days may a disconnected multi-TB intake
   retain partial landing data, and what minimum free-space reserve must stop new
   sessions?
7. **Service topology:** should `sutra serve-setu` supervise the Rust ingress subprocess,
   or should systemd supervise sibling Python/Rust units under one target? The wire,
   ticket, writer, and failure semantics are unchanged, but operations must choose one.
8. **Parity bar:** accept the proposed 90%-of-Signiant and 85%-of-diagnosed-ceiling
   thresholds, or set different values before S1.12?
9. **Spec 2 scope:** will every ad-hoc contributor install the native client, or must the
   later unaccelerated browser path be funded? What quarantine artifact class and byte
   ceiling apply?
10. **Spec 3 trigger:** is “direct Setu remains materially below Signiant after endpoint
    bottlenecks are removed” the accepted relay gate, and which cloud/region may be used
    for the measured relay experiment?

## 16. Final implementation invariants

1. Every received payload byte enters through
   `IntakeLandingWriter::accept_chunk`; no transport writes a completed payload or
   `intake.json`.
2. `CommitIntake` and existing BagIt assembly are the only network-intake handoff to the
   existing watcher; verify/dedup/policy/RAO/copy paths are not reimplemented.
3. QUIC and TCP present the same enrolled client certificate, verify the same enrolled
   CA/server name, and bind a data ticket to that fingerprint.
4. Resume acknowledges only fdatasync+ledger-checkpointed ranges. QUIC migration is an
   optimization, never the durable record.
5. One global byte reservoir and cap bound all lanes/connections; connection count
   cannot multiply memory or permitted bandwidth.
6. Chunk work is stolen from one queue. Parallelism is bounded, measured, and reduced
   after its knee; 16 is a diagnostic point unless explicitly approved from Spec 0.
7. Agent control remains outbound-only and responsive under data pressure.
8. Public exposure is exactly TCP/443 and UDP/443 on the dedicated Setu address, both
   mTLS, within live-audited default-deny nft rules.
9. No production backout flags, legacy upload transport, canary branch, or dormant POP
   path ships. Backout is the previous binaries via `git revert`.
10. Performance claims use unique durable goodput on the real corpus/link and are
    compared to the frozen Signiant evidence; integrity and bounded memory are part of
    parity, not secondary checks.
