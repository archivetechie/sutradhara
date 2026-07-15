# Design - Setu WAN receive / Signiant replacement (v0.4)

**Status:** technical verification **PASS**, 2026-07-15, after two panel rounds, an
independent Claude Fable 5 appellate review, and a fresh Codex gate. No blocker or
major remains. This document is still **not frozen and no implementation prompts may
be cut** until the owner answers the twelve business decisions in §15. The original
four-lens panel consolidated 22 issues; the first folded draft then failed independent
verification because several resolutions were not implementable or were contradicted
by the body. v0.4 incorporates that verification and a second failure/operations,
operator-journey, contract, and security review. The complete review record is
`docs/panel-setu-2026-07-14.md`. The complete shared wire contract is
`docs/contract-setu-data-session-v1.md` and is normative; this design references it
rather than repeating a second proto specification. Managed offers, policy, lifecycle,
operator actions, and status are normative in
`docs/contract-setu-managed-site-v1.md`.

**Purpose.** Setu moves very large completed packages from managed sites to the existing
Sutradhara intake in Coimbatore. It replaces Signiant's transfer role, not the archive
workflow around it. Think of Setu as a new long-distance truck and loading dock: it may
deliver bytes quickly and resume a broken journey, but it cannot decide what the cargo
is, publish it to the catalog, or send it to tape. Those decisions remain with the
existing intake, watcher, policy, and Remanence pipeline.

**Decision.** Retain `sutra-agent`'s outbound enrolled-device control plane and
Sutradhara's single admission-to-archive funnel. Replace only the bulk-transfer path
with a shared `DataTransport` contract implemented by bounded parallel TCP/443 and,
when Spec 0 proves it worthwhile on the real route, QUIC/UDP 443 using `quinn` BBR.
Both paths write partial state through the same trusted receive-core writer. TCP calls
it in-process; the QUIC edge forwards bounded protobuf frames over gRPC on a private
Unix socket and has no landing-filesystem access. A trusted finalizer verifies and
publishes completed payload files, and the existing idempotent commit path alone
publishes `intake.json`. Spec 1 is direct managed-site
transfer. Spec 2 adds ad-hoc contributors through the same authority boundary. Spec 3
adds an opaque relay only if measurements show that the direct route, rather than the
endpoints, is the limiting factor.

**Terms used below.** A *data session* is one resumable server record for an authorized
intake. A *transport generation* fences old QUIC or TCP connections after a switch. A
*file generation* fences an older copy after the one permitted commit-time re-upload.
A *durable range* is a chunk whose payload and range ledger have both reached stable
storage. The *partial store* is unpublished `.incoming/setu-v1` state. *Publication*
means the trusted atomic move into `data/`; *commit* means the later BagIt handoff that
writes `intake.json` last. These are separate operations.

## 0. Panel result and v0.4 changes

The first panel's core direction was sound, but its v0.3 fold used an impossible file
descriptor handoff over ordinary gRPC, overstated failure containment, assumed a TLS
floor that Python gRPC does not expose, and left important recovery and operator
contracts implicit. v0.4 makes the following changes directly in the body.

1. **Single-source wire contract.** `contract-setu-data-session-v1.md` defines every
   RPC, field number, direction, limit, typed error, close mode, transport/file
   generation, pagination rule, compatibility rule, and required crash test. The
   server owns transport selection through compare-and-swap; auth, ownership, policy,
   and protocol failures never trigger fallback.
2. **Implementable trust boundary.** The Rust public ingress has its own unprivileged
   uid and no landing-filesystem or database access. It does not receive file
   descriptors through gRPC. After QUIC authentication it forwards the same bounded
   protobuf data frames over a private gRPC Unix-socket stream; application credit on
   that stream propagates the trusted writer's backpressure to QUIC. A trusted
   Python/PyO3 writer owns partial files, and a separate finalizer independently verifies
   and publishes them. This deliberately pays one bounded local memory copy: moving an
   ingress-owned file would not revoke a hostile process's already-open descriptor, so
   it could not support the claimed publication boundary. One compromised ingress can
   disrupt or corrupt the frames of all active QUIC sessions, but it cannot alter a
   durable partial after the trusted writer has acknowledged it, reach inactive state,
   manufacture trusted file authority, or publish a receipt or sentinel.
3. **Recovery is a protocol, not a restart hope.** Normal `SIGTERM` quiesces, flushes,
   persists `SUSPENDED`, and retains state; only explicit cancel aborts. Commit uses a
   durable request plus idempotent assembly and startup reconciliation, including the
   crash windows before and after `intake.json`. Corrupt ledgers, receipts, or journals
   are quarantined and surfaced rather than guessed or deleted.
4. **A separate public service.** TCP/443 is a dedicated asynchronous Setu gRPC server
   with a service allowlist, bounded data-lane semaphores, and independently reserved
   control capacity. It does not reuse the current 16-worker private server and never
   registers Restore. Public startup requires an explicitly provisioned certificate
   whose configured Setu DNS name is present in the SAN; the current localhost
   auto-certificate path is forbidden. QUIC requires TLS 1.3. TCP uses mutually
   authenticated TLS 1.2 or newer because current `grpcio` cannot enforce a 1.3-only
   floor; choosing a 1.3-only TCP terminator would be a separate Rust architecture.
5. **Measured, bounded throughput.** One sender reservoir, token bucket, and work queue
   bound all lanes. Receiver budgets are disjoint per process and their checked sum is
   the host ceiling. QUIC stream credit is derived from reservoir capacity, not fixed
   at 64. A durability probe writes no payload but exercises the same ledger fsync.
   Spec 0 chooses `TCP_ONLY` or `QUIC_WITH_TCP_FALLBACK`; experimental BBR is not a
   production assumption.
6. **An operable product.** Managed roots bind root id to operator, artifact class,
   admission policy, label rules, and a named workflow-completion adapter. The first
   release includes a closed lifecycle/status model and pause, resume, and cancel
   controls; deep performance charts remain deferred. Schedule pause is an explicit
   mode, not an invalid zero rate. Queue position, reserved bytes, partial expiry,
   reclaim reason, and terminal reason are visible.
7. **Bounded rollout.** Spec 0 is a scripted, trap-cleaned, sequential-stopping runbook
   with three possible outcomes. Production runs as sibling systemd units under
   `setu.target`, with distinct users and fail-closed dependencies. Activation occurs
   only after v1 sessions drain. Once v2 durable state exists, recovery is forward-only
   until it is committed, cancelled, or purged; reverting to the old binary is not a
   safe rollback.

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
and can increase loss/queueing enough to lower goodput. That is why Setu measures BBR
against parallel TCP before selecting the production mode, uses one global work queue,
and treats 1/4/16 as a diagnosis matrix while production uses a small measured
connection/lane ceiling. Sender uplink, receiver downlink, source
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
unprivileged Rust ingress      dedicated Python Setu TransferChunks
   | bounded TransferChunks/gRPC    |
   +----------- UDS ---------------+
                  |
          PartialLandingWriter
       restricted partials + durable range ledger
                  |
                  v
 trusted IntakeLandingFinalizer (Python/PyO3 owner)
       independent hash -> atomic data/<relpath> + trusted receipt
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

**The one landing implementation is split into two authority levels.**
`PartialLandingWriter::accept_chunk` validates the session and generations, confines
the partial path, checks the chunk hash, writes only unpublished bytes, and checkpoints
the range ledger. Both public network legs use it. TCP invokes it directly; QUIC reaches
it through a bounded internal `TransferChunks` stream over gRPC on the private UDS. It
has no method that can rename into an intake's `data/`, append a trusted receipt, or
write a sentinel.
`IntakeLandingFinalizer::finish_file` is callable only by the trusted Python/PyO3
server owner. It reloads the authoritative file specification, reopens the partial,
independently hashes the complete byte sequence, fsyncs it, atomically renames it to
the confined `data/<relpath>`, fsyncs the parent, and appends the receipt. Only the
existing `assembly.assemble_committed_bag` may later publish `intake.json`.

The authoritative path, size, generation, force flag, and final expected SHA reach
that server owner through enrolled-mTLS `RegisterDataFile` and `FinalizeDataFile` RPCs,
even when QUIC carries the payload. The ingress sees routing metadata but cannot replace
the trusted copy. This is what makes finalization independent: a compromised ingress
cannot change both partial bytes and their expected hash.

Both types belong to `sutradhara_receive`; this keeps one path, hash, ledger, and
publication implementation without pretending that the public ingress has publication
authority. The package already owns canonical paths, hashing, BagIt encodings, resume
semantics, and server validation (`packages/sutradhara-receive/README.md:3-18`), and
its Rust/PyO3 build already serves Python and Rust consumers
(`packages/sutradhara-receive/pyproject.toml:1-31`). It remains dependency-light: it
contains filesystem/hash/ledger logic but no database, catalog, backend, network, or
Remanence dependency.

The Rust ingress is a small Sutradhara-owned public-edge process under a distinct
`sutradhara-setu-ingress` uid. That uid can connect only to the private bridge UDS; it
has no traversal permission to the landing root, `data/`, partials, sentinels, database,
or server secrets. The bridge registers only the data-session `TransferChunks` method,
not file registration, finalization, commit, abort, enrollment, or policy methods. It
accepts the same `DataPlaneFrame` values used on public TCP, revalidates ticket/session/
generation at the trusted owner, and returns durable acknowledgements only after the
trusted writer's checkpoint. Bounded application credit, not the socket's implicit
buffering, limits bytes between the QUIC parser and writer. No file descriptor crosses
gRPC.

This local hop is intentional. A directory rename or permission change cannot revoke a
file descriptor already held by a compromised process, so direct ingress writes would
leave a hash-to-rename race. Keeping every partial fd in the trusted writer makes the
durable acknowledgement and later final hash meaningful without a second multi-terabyte
disk copy. The Spec 0 QUIC spike measures the bridge's CPU, copy, and goodput cost; an
outcome whose bridge cannot clear the frozen endpoint target cannot select QUIC.

The design claims process-level, not per-intake, blast-radius containment: a compromised
ingress can drop, duplicate, reorder, or alter frames for all active QUIC sessions and
can force them to suspend or fall back. It cannot mutate bytes after the trusted writer
acknowledges them, supply the separately authenticated file specification or expected
hash, or reach TCP sessions, inactive state, receipts, the catalog, or publication.
On suspend, close, or ingress-authority failure, the trusted server fences the
generation and moves its owner-only partial state into `held/`. A repository test
rejects new direct writes to partials or `data/` outside the receive-core types, and
Scenario SETU proves the boundary with process uid, permission, symlink, stale-frame,
and cross-session tests.

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
    ) -> Result<CompletedDataReceipt, IntakeError>;

    async fn close(
        self: Arc<Self>,
        mode: DataSessionCloseMode,
    ) -> Result<CloseDataSessionResult, IntakeError>;
    fn snapshot(&self) -> DataTransportSnapshot;
}

pub(crate) struct DataFileSpec {
    pub file_id: [u8; 32],       // SHA-256 of canonical wire relpath
    pub file_generation: u64,
    pub relpath: String,
    pub size_bytes: u64,
    pub chunk_bytes: u32,
    pub force_reupload: bool,
}

pub(crate) struct PreparedChunk {
    pub file_id: [u8; 32],
    pub file_generation: u64,
    pub offset: u64,
    pub payload: bytes::Bytes,
    pub sha256: [u8; 32],
    pub reservoir_permit: OwnedSemaphorePermit,
}

pub(crate) struct FinishDataFile {
    pub file_id: [u8; 32],
    pub file_generation: u64,
    pub relpath: String,
    pub size_bytes: u64,
    pub sha256: [u8; 32],
}
```

`PreparedChunk` is an owned, fixed-size reservoir buffer. Its permit stays attached
across every retry/requeue and is released **only** on the durable ack or terminal
buffer destruction — never on requeue, whose bytes still occupy the reservoir (§7.1).
`QuicBbrTransport` and `ParallelTcpTransport` implement this trait and contain their
own lane tasks, but use the same bounded MPMC work queue, cap, progress counters, and
retry classification. `register_file` and `finish_file` always delegate metadata to
the trusted mTLS control API; only `submit_chunk` uses the selected bulk transport.
There is no `LegacyGrpcTransport`, runtime compatibility flag,
or “old upload” branch: this pre-production migration removes the old direct
`UploadFile` data path after fixtures have been captured. Deployment nevertheless has
a one-way state boundary: the old binary may be restored only before any v2 session is
accepted. Once v2 durable state exists, the new recovery code remains installed until
that state is committed, explicitly cancelled, or purged (§8.3).

### 3.2 Precise call-site replacement

Keep `upload_plan`'s bounded multi-file scheduling
(`~/sutra-agent/src/relay/intake.rs:737-753`), `prepare_unit`, package normalization,
source snapshots, and whole-file SHA-256. The current package path materializes a
normalized temporary tar before upload (`~/sutra-agent/src/relay/intake.rs:864-917`),
which can require a second copy of a multi-terabyte source. The managed-root contract
therefore identifies either an already normalized immutable artifact or a scratch root.
Before accepting an offer, the agent calculates and reserves peak scratch (planned tar
bytes, sender reservoir, and configured safety margin), reports `PREPARING`, and fails
the offer without opening a data session if that capacity is unavailable. Spec 0
measures preparation time and scratch occupancy. Streaming tar generation is deferred
because it would change restart and deterministic-package semantics, not treated as
free storage.

The immutable plan also supplies exact payload-file and unique-parent-directory counts
through `StartIntakeRequest.setu_admission`, as defined in data-contract §2.1. The
server binds them to the intent before opening a data session; registration cannot
exceed them. A legacy request without that message may finish only through the draining
`UploadFile` path and cannot negotiate Setu.

Replace `stream_file` at `~/sutra-agent/src/relay/intake.rs:975-1024` and the
`mpsc::Sender<FileChunk>` argument to `send_file_chunks` at
`~/sutra-agent/src/relay/intake.rs:1040-1148` with:

1. `transport.register_file(DataFileSpec)` calls trusted `RegisterDataFile` to persist
   path/size/generation/force authority, then pages `GetDataResumePlan` to obtain the
   complete durable missing-range set;
2. a sequential file reader that hashes every source byte in order, but allocates and
   submits only missing chunks;
3. multiple outstanding `submit_chunk` futures, bounded by the global byte reservoir;
4. `finish_file` calls trusted `FinalizeDataFile` with the expected whole-file hash
   after every missing chunk has a durable ack and the source before/after snapshot
   still matches; and
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
the crash-safe generation semantics in §10.4.

## 4. Negotiation, capability probe, and fallback

### 4.1 Shared contract

The current proto has only sequential whole-file `UploadFile` and no durable chunk ack
(`proto/intake.proto:5-12,28-41`). S1.1 replaces that production data method with the
RPCs and messages in **`docs/contract-setu-data-session-v1.md`**. That file is the only
normative schema. It includes open/resume, a probe and compare-and-swap switch,
trusted file registration/finalization, bidirectional TCP chunk transfer,
suspend/ready close, complete frame directions, field numbers, typed errors, and
pagination by both file and missing range. Prompts and code
comments must link to it rather than restating a subset that can drift.

The random data ticket augments existing mTLS; it never replaces it. The server stores
only a keyed ticket hash and binds the session to intake, enrolled device, certificate
fingerprint, required `ingest` scope, protocol version, negotiated ceilings, transport
generation, and expiry. QUIC and every TCP lane present the enrolled certificate. The
server resume ledger is authoritative; the client journal is a recovery pointer.

### 4.2 Probe state machine

After the existing authenticated `StartIntake`, the client calls
`OpenDataSession`. The server advertises both transports only when their listeners and
partial writer are healthy. Site policy selects one of two Spec 0 outcomes:

- `TCP_ONLY`: prepare a TCP probe and bind TCP directly from `UNBOUND`;
- `QUIC_WITH_TCP_FALLBACK`: prepare a QUIC probe and bind QUIC from `UNBOUND`, retaining
  TCP only as the fallback target.

A probe ticket authorizes a `DurabilityProbe` nonce and no file or payload frame. The
target listener authenticates mTLS and the probe ticket; the writer persists and
fsyncs the nonce in the session ledger; only then does it return
`DurabilityProbeAck`. This tests the network, auth, service, writer, and landing-disk
checkpoint path without transferring a fake chunk or allowing two payload transports.
The attempt budget is an operator-visible site-policy value; the proposed Spec 0
starting point is two attempts within three seconds, not a hidden constant.

Only reachability failures trigger connect-time fallback. Certificate failure, unknown
CA, hostname mismatch, revoked enrollment, expired/wrong ticket, protocol mismatch,
intake ownership error, or policy rejection is terminal. Falling back on those errors
would conceal a security/configuration fault.

During transfer, repeated QUIC PTOs are not by themselves a fallback signal; BBR must
be allowed to adapt. A soft fallback is considered only when server-reported
`received_unique_bytes` has not advanced for the site-policy stall interval while the
schedule is running and neither endpoint reports disk or server pause. The client then
runs the same non-payload TCP durability probe and requests the generation CAS. A rise
in received bytes with flat durable bytes is landing pressure, not a network failure.
This covers UDP that passes a handshake but is later blackholed without racing two
payload transports or switching on a guessed bandwidth ratio.

### 4.3 Durable transport state machine

The exact states and RPC rules are in the shared contract. In plain terms, either
transport may bind an unbound session; only QUIC may switch to TCP during an intake:

```text
UNBOUND -> ACTIVE_QUIC(n) -> SWITCHING(n+1) -> ACTIVE_TCP(n+1)
   |             |                                  |
   +-------> ACTIVE_TCP(n)                           +-> SUSPENDED
                 |                                         |
                 +--------------------------> SUSPENDED ----+
ACTIVE_* -> CLOSED_READY_TO_COMMIT
```

Binding and switching are database compare-and-swap operations. The `flock` represents
live authenticated data connections, not a long-lived server process: the serving
process acquires it for the first accepted connection of a generation, reference-counts
additional lanes/connections, and releases it after the last closes or its liveness
deadline expires. Deadline expiry cancels and joins every frame task before release; if
one cannot quiesce, the trusted serving process exits so the kernel releases the lock.
A process crash also releases it in the kernel.

To switch, the server persists `SWITCHING(n+1)`, atomically publishes a trusted
read-only generation guard that makes `n` stale at the writer, asks old connections to
close, and waits for their lock to release. It then activates TCP and returns the new
ticket without holding the connection lock; the first authenticated target lane
acquires it. A lost switch response therefore leaves active state with no live holder:
reopen fences it to `SUSPENDED` and repeats probe/CAS. A server crash during switching
also recovers to `SUSPENDED`. The trusted writer holds a per-session generation gate
across the final guard check and filesystem write/checkpoint; switch takes that gate
exclusively before publishing the new guard. A switch is not acknowledged until the
old lock is gone. Late old frames cannot write after an acknowledged switch.

Normal close chooses `SUSPEND` or `READY_TO_COMMIT`. Suspend fences the generation,
flushes or conservatively forgets uncheckpointed work, moves the partial into trusted
holding, and retains the journal. Ready-to-commit additionally runs the flush barrier
and requires one trusted receipt per registered file. Neither close mode deletes data;
only explicit `AbortIntake` is destructive.

### 4.4 Server symmetry and authority loss

| Concern | QUIC/BBR | Parallel TCP | Shared owner |
|---|---|---|---|
| Public listener | Rust `sutradhara-setu-ingress`, UDP/443 | dedicated Python `grpc.aio` Setu server, TCP/443 | sibling systemd units under `setu.target` |
| TLS | rustls TLS 1.3 mTLS | grpcio TLS 1.2+ mTLS | explicit Setu DNS certificate and enrolled CA |
| Public methods | data session only | allowlisted Device control + Intake control/data | no Restore or general server factory |
| Ticket authorization | gRPC-over-UDS call to DB owner | direct trusted-store check | device, `ingest` scope, intake, generation |
| Payload landing | bounded gRPC/UDS bridge to trusted writer | direct PyO3 partial writer | same receive-core ledger and validators |
| Publication | none | trusted PyO3 finalizer | independent full hash and atomic rename |
| Commit/handoff | none | idempotent `CommitIntake` | existing assembly/watcher |

The Rust ingress never implements `StartIntake`, file registration/finalization, commit,
abort, policy lookup, publication, catalog registration, or BagIt assembly. Ordinary
generated gRPC over a filesystem-permission-gated UDS carries authorization, renewal,
statistics, and a bounded `TransferChunks` stream containing the same protobuf frames
as public TCP. It carries payload bytes but never a path, expected hash, publication
command, or file descriptor. The socket is created by the trusted service, mode `0660`,
accessible only to the two systemd service identities, and never exposed through nft.
The trusted endpoint validates ticket, session, transport/file generation, frame limit,
and registered-file bounds again; an ingress acknowledgement is not durable until this
endpoint returns the writer's durable acknowledgement.

The ingress revalidates authorization on an independent timer no longer than 20
seconds, regardless of payload progress. If the UDS, DB owner, or revocation check is
unavailable beyond that window, it stops issuing durable acknowledgements, fences the
generation, closes public data lanes, and reports `SUSPENDED_AUTHORITY_UNAVAILABLE`.
The trusted service retains durable partials and alerts. Reconnection requires a fresh
server-authorized open; the ingress never continues from cached authority.

## 5. QUIC/BBR data plane

### 5.1 `quinn` and TLS configuration

If Spec 0 selects `QUIC_WITH_TCP_FALLBACK`, Spec 1 pins the exact `quinn` version
proven by the spike (the design baseline is 0.11.11) in both Rust lockfiles. Its BBR
module is documented as experimental, so the evidence packet records the selected
controller and the acceptance run repeats after every dependency update. Configuration
is explicit and reservoir-derived:

```rust
let mut transport = quinn::TransportConfig::default();
transport.congestion_controller_factory(Arc::new(
    quinn::congestion::BbrConfig::default()
));
let stream_bytes = u64::from(negotiated.chunk_bytes) + FRAME_OVERHEAD_MAX;
let stream_credit = (negotiated.max_inflight_bytes / stream_bytes)
    .min(negotiated.receiver_reservoir_bytes / stream_bytes)
    .clamp(1, HARD_STREAM_MAX);
transport.max_concurrent_uni_streams((stream_credit as u32).into());
// Bidi control streams, flow-control windows, idle timeout, and keepalive are also
// derived from negotiated limits and the measured BDP, then checked as one budget.
```

The client builds a rustls 0.23 `ClientConfig` from
`RelayConfig.client_cert`, `client_key`, and `ca_cert`; those are the same files read
by the current tonic channel (`~/sutra-agent/src/relay/transport.rs:85-99`). It verifies
the configured Setu server name. The server uses rustls
`WebPkiClientVerifier`, requires a client certificate, extracts its device CN and
SHA-256 fingerprint, and asks the existing enrollment store to authorize it. That is
the same resolution the current gRPC server performs from peer CN/fingerprint into an
enrolled device/operator (`src/sutradhara/grpc/ca.py:291-321` and
`src/sutradhara/grpc/store.py:549-561`). The current store binds enrolled device id and
fingerprint and records revocation (`src/sutradhara/grpc/store.py:121-143`); QUIC does
not create another registry.

The current `ensure_server_certificate` path is unsuitable for Setu: it defaults to
`localhost`/`127.0.0.1` SANs and returns an existing certificate without adding a new
name (`src/sutradhara/grpc/ca.py:93-105`). Deployment therefore provisions a distinct
CA-issued Setu server certificate from explicit configuration
`setu_server_name=<public DNS>`. Startup parses the certificate and fails unless the
private key matches, the certificate is current, its DNS SAN contains that exact name,
and both public services load the same fingerprint. It never silently calls the
localhost auto-certificate helper. The enrolled CA remains the trust anchor; a public
WebPKI certificate alone is not a substitute.

QUIC uses TLS 1.3, disables early data, and sets ALPN exactly
`sutradhara-setu/1`. A chunk receives no stream credit until both certificate and
ticket authentication succeed, so replayable 0-RTT cannot write payload. Stateless
address validation, handshake and idle deadlines, global and per-source connection
quotas, and pre-auth byte ceilings are required before the listener is public.

### 5.2 Connection and stream model

One QUIC connection is the default. It owns one bidi control stream and opens one
unidirectional stream per chunk. A chunk stream contains exactly one `DataChunk` header
and payload, then FIN. The ingress maps those frames onto one bounded bidirectional
`TransferChunks` stream over the private UDS; it does not acknowledge or retain a chunk
outside the trusted writer. The QUIC control stream carries the writer's durable
checkpoint acks, ticket renewal, errors, and telemetry back to the client; trusted file
registration and finalization remain direct TCP gRPC control calls. A fresh stream per
chunk avoids a lost
chunk head-of-line blocking unrelated chunks. The shared byte reservoir is the primary
memory bound and the derived stream-credit limit ensures that protocol buffering cannot
promise more simultaneous chunks than that reservoir can hold.

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

`RegisterDataFile` binds `file_id`, canonical relpath, total size, chunk size, resume
epoch, and generation through trusted control. `DataChunk` binds file id, generation,
offset, length, and SHA-256 of its payload. The server
rejects unaligned ranges, overlaps with a different digest, a path/file-id mismatch,
data beyond declared size, and any chunk hash mismatch. An identical duplicate is
idempotently acknowledged.

The partial writer uses positioned writes and checkpoints a compact range bitmap plus
per-chunk digests. At 2 MiB chunks a 2 TB file has 1,048,576 bits (128 KiB) in its
completion bitmap; digests live in a paged side ledger rather than RAM. Completion of
every range is only a readiness condition; it does not publish a file or create a
trusted receipt.

`FinalizeDataFile` persists the expected hash received over the enrolled-mTLS control
connection. The trusted finalizer reloads that specification from the DB owner and
reads the entire partial sequentially, regardless of the ingress ledger's claims. It verifies
exact length and whole-file SHA-256, fsyncs, atomically renames to the confined
`data/<relpath>`, fsyncs the parent, and appends the current completed-file receipt
shape. A full independent read is deliberate: an untrusted ingress-provided incremental
hash would collapse the boundary. This preserves the current server-side receipt proof
(`src/sutradhara/grpc/servicer.py:334-353`). Finalization has its own bounded read
semaphore and appears as a distinct `FINALIZING` lifecycle state so a multi-terabyte
read is visible rather than mistaken for a stalled network.

A finalizer hash mismatch creates no receipt and does not consume the commit-time
re-upload allowance, which begins only after a trusted receipt exists. It fences the
file as `RECOVERY_REQUIRED`, quarantines the suspect partial/ledger, and requires an
audited reset or cancellation; it never asks the same untrusted ingress to bless its
own corrupted bytes.

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

The agent monitors local route/address changes without opening an application listener.
`quinn::Endpoint::rebind` changes the UDP socket for **all** connections on that
endpoint, so the transport coordinates it once at the endpoint level rather than once
per connection; see the official
[`Endpoint::rebind`](https://docs.rs/quinn/0.11.11/quinn/struct.Endpoint.html#method.rebind)
contract. QUIC then validates each new path. Until validation succeeds, no chunk is
called durable merely because it was written to a stream. If validation fails or the
process restarts, the agent opens a new connection, presents the same mTLS identity and
refreshed ticket, asks for the server's durable ranges, and continues. No “migration
succeeded” event removes the client journal.

## 6. Parallel-TCP fallback

`ParallelTcpTransport` deliberately reuses the existing **Intake service contract** for
authentication, admission, session negotiation, lease renewal, and commit, but not the
current private server factory or its fixed 16-worker pool. A dedicated `grpc.aio`
Setu server binds the public TCP address, registers only Device and Intake services,
and never registers Restore. Long-lived `DeviceService.Connect` and data lanes therefore
do not consume synchronous worker threads. `TransferChunks` has global, per-device,
and per-ticket lane semaphores plus a bounded data executor for blocking receive-core
work; control/renew/close/commit do not acquire that semaphore and retain independent
event-loop and DB capacity. Saturating every permitted data lane must still pass the
control-latency acceptance bound.

The public TCP server uses the explicit Setu certificate from §5.1 and requires client
certificates. Python
[`grpc.ssl_server_credentials`](https://grpc.github.io/grpc/python/grpc.html#grpc.ssl_server_credentials)
does not expose a minimum-version setting, so v1 requires mutually authenticated TLS
1.2 or newer on TCP, consistent with the official gRPC HTTP/2
[security requirement](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#security),
and tests that plaintext, TLS 1.0/1.1, wrong CA, wrong SAN, and unenrolled clients fail.
QUIC remains TLS 1.3. A 1.3-only TCP requirement would move TCP termination into Rust
and must be reviewed as a new design, not asserted in configuration that cannot enforce
it.

The TCP path does not
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

TCP reserves one additional independent lane with `lane_id = 0` for session progress,
ticket renewal, errors, and orderly close. Negotiated `tcp_lanes_max` counts payload
lanes `1..N`; the reserved control lane never accepts `DataChunk` and does not compete
for the data-lane semaphore. Rotated tickets apply to newly opened lanes while already
accepted lanes remain bound to their authenticated session/generation for the bounded
renewal grace in the shared contract.

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
(`~/sutra-agent/src/relay/config.rs:259-268`). The loopback starting profile uses a
128 MiB payload reservoir with 96/64 MiB high/low watermarks; Spec 0 must replace that
with a measured site value before production. Its high watermark obeys
`target_payload_rate * (RTT_p95 + checkpoint_latency_p95)` and the checked total also
includes protocol, hash, queue, and kernel-buffer allowances. Every filling payload
buffer acquires byte credit **before** the disk read, including one buffer per
concurrent source file. There are no uncounted "currently filling" buffers. Startup
refuses a profile whose worst-case total exceeds `max_transport_memory_bytes`.

The sequence is: acquire reservoir credit → acquire a buffer → acquire token credit →
positioned/sequential read → update
whole-file and chunk SHA-256 → snapshot check point → enqueue → lane write → durable
ack → return buffer. A lane never reads the source itself. On poison/error, close the
work queue before joining lane and reader tasks, drain/requeue unacknowledged items,
and return every permit. A `PreparedChunk` retains its reservoir permit across every
retry or requeue and releases it only after durable ack or terminal destruction; no
bounded-credit cycle may leave a reader blocked forever or make queued bytes invisible
to accounting.

The cap is acquired after a free buffer but before enqueue. When the cap is low, the
reservoir therefore does not fill with data that cannot yet be sent. When disk is slow,
lanes naturally starve and telemetry says `source_disk_limited`; the tuner does not add
connections.

### 7.2 Receiver reservoir and durable checkpointing

The receiver profile defines one host payload ceiling and statically assigns disjoint
sub-budgets to Rust QUIC parsing/UDS forwarding and the trusted Python/PyO3
write/TCP process; startup proves their checked sum does not exceed the host ceiling.
Each process applies per-intake and per-device sub-limits inside its allocation. The
internal gRPC stream grants application credit only after the trusted process reserves
its side, so QUIC bytes counted in both processes are charged to both allocations rather
than disappearing between them. This is enforceable across processes without a live
cross-process byte broker and still permits different intakes to use QUIC and TCP
concurrently.
Spec 0 chooses the numeric ceilings and high/low watermarks from measured BDP,
checkpoint latency, available RAM, and control-plane headroom. Network handlers obtain
byte credit before decoding or retaining payload. At high watermark they stop granting
new QUIC stream credit or reading gRPC messages until the submitter drains below low;
transport flow control carries pressure to the sender.

There is one landing submitter per active intake, with at most one filesystem write
operation in flight, and a global disk semaphore bounds concurrent intakes. It consumes
validated chunk descriptors, performs positioned writes, and groups durability into a
checkpoint at the earlier of the Spec 0 byte or time threshold (the starting profile
is 64 MiB or 2 seconds). A `DurableChunkAck`
names only ranges covered by `fdatasync` plus an atomically written/fsynced ledger
checkpoint. Network receipt without that checkpoint is not durable; after a crash the
client may resend it. This batching avoids one fsync per 2 MiB while retaining a
bounded retransmit window.

The on-disk and database state separates public-edge partials from trusted metadata:

```text
<landing>/.setu/active/<data-session-id>/        # trusted owner only
  files/<file-id>.partial
  files/<file-id>.ranges                         # checkpoint ledger
  probe.log                                      # bounded fsynced probe nonces
<landing>/.setu/held/<data-session-id>/          # trusted owner only
<intake>/.incoming/setu-v1/receipts.jsonl        # trusted finalizer only
/run/sutradhara-setu/bridge.sock                  # ingress gRPC; no filesystem fds
/run/sutradhara-setu/locks/<data-session-id>     # server-created flock inode
/run/sutradhara-setu/guards/<data-session-id>    # atomic read-only generation/state
database data_session/file rows                  # owner, state, generations, limits
```

Every root is on the same filesystem as the intake so trusted publication can use
atomic rename. Paths are created through the receive package's canonical confinement;
the current server already confines before rename
(`src/sutradhara/grpc/servicer.py:292-297`). The server creates the lock inode in a
non-writable directory before activating a session and atomically maintains the
adjacent read-only generation guard. The trusted process reference-counts the lock for
direct TCP lanes and authorized QUIC bridge streams; the process holding `flock` owns
the currently live connections, while the database generation is the durable
authority. Switch order is §4.3, never release-old-then-bump.

`ListIntakeFiles` must stop deleting `.incoming`; the writer owns cleanup. Abort removes
the intake only after generations are fenced, ingress forwarding has stopped, and every
writer handle is closed. Suspend moves active partials to `held`; retention cleanup
uses explicit terminal/expiry state and never guesses from mtime. Commit obtains the
session lock, verifies `CLOSED_READY_TO_COMMIT`, requires trusted receipts, and then
executes the durable commit protocol below. This replaces the current process-local
`runtime.in_flight` gate (`src/sutradhara/grpc/servicer.py:197-220`) with a durable
cross-process gate.

### 7.3 Phase boundary into verify/RAO

After all files finish, `CommitIntake` durably inserts or reloads a commit request keyed
by `(intake_id, manifest_digest)`. The request moves through
`REQUESTED -> ASSEMBLING -> PUBLISHED -> OBSERVED`; retries return the stored result or
continue the next idempotent step. Before assembly it takes the session lock, verifies
`CLOSED_READY_TO_COMMIT`, runs the flush barrier, compares the manifest, and records any
one-time re-upload request with its bumped file generation. Assembly writes to a
staging name and keeps `intake.json` last, as today
(`src/sutradhara/grpc/assembly.py:97-105`).

Startup and a periodic reconciler examine both the commit row and filesystem:

- `REQUESTED/ASSEMBLING` with no `intake.json`: repeat idempotent assembly from trusted
  receipts; partial staging output is replaced, not trusted.
- `ASSEMBLING` with a valid matching `intake.json`: mark `PUBLISHED`; this closes the
  crash window after filesystem publication but before the DB transaction.
- `PUBLISHED` before watcher observation: leave the bag visible and retry observation;
  never rebuild or delete it.
- mismatched manifest, malformed receipt, corrupt ledger, or invalid `intake.json`:
  fence the intake as `RECOVERY_REQUIRED`, quarantine the suspect metadata, retain
  payload bytes, and raise an operator-visible alert. Recovery never fabricates ranges
  or silently starts over.

Only after publication can the watcher hash and register the bag. Verification,
RAO build, and copy work therefore remain behind the socket phase barrier and cannot
consume the receive reservoir. Landing capacity and archive backlog have separate
health alerts; combining their queues would blur ownership and make a network stall
indistinguishable from downstream archival work.

## 8. Public ingress and landing zone

### 8.1 Bounded public surface and supervision

Use a dedicated public Setu IP and DNS name on akash. The public profile opens:

1. **TCP/443** — mTLS gRPC carrying `DeviceService.Connect`, Intake control/commit,
   lifecycle/status, and `IntakeService.TransferChunks` for parallel TCP.
2. **UDP/443**, only for `QUIC_WITH_TCP_FALLBACK` — mTLS
   QUIC/`sutradhara-setu/1` carrying data-session frames.

These are separate protocol/socket rules even though both use port number 443. No public
50051, enrollment database, metrics listener, HTTP upload endpoint, UDS, SSH, or admin
port is added. Enrollment continues through its existing protected route; Setu data
connections require an already enrolled cert.

A dedicated IP is an implementation prerequisite because the existing system UI also
uses TCP/443 and may use HTTP/3/UDP/443. Sharing by ALPN is unsafe (web traffic can also
be HTTP/2), and sharing UDP/443 would collide with HTTP/3. The deployment prompt must
either allocate that IP or return to the owner; it must not silently insert a generic
TLS proxy in front of mTLS.

Production uses two sibling units under `setu.target`:

- `sutradhara-setu.service` runs the trusted Python `grpc.aio` server/DB owner as the
  existing Sutradhara service uid. It binds only the configured TCP address.
- `sutradhara-setu-ingress.service` runs Rust as `sutradhara-setu-ingress`, has
  `Requires=`/`After=` the trusted service, and binds only the configured UDP address.

Both are `PartOf=setu.target`, use `Restart=on-failure`, carry explicit CPU/memory/open
file ceilings, and publish structured readiness only after PKI, SAN, exact bind,
landing filesystem, DB schema, UDS, uid/permission, and writer self-checks pass. The
Rust unit stops when the trusted service or UDS disappears; the trusted service may
continue TCP when Rust fails, withdraws QUIC capability immediately, and marks active
QUIC sessions suspended for probed TCP fallback. `TCP_ONLY` does not start the Rust
unit. The current private server's public-bind rejection remains unchanged
(`src/sutradhara/grpc/server.py:94-108`).

Routine `SIGTERM` to either unit first disables new sessions, quiesces lanes, performs
the session flush/suspend protocol, and exits only after state is durable or the
shutdown deadline forces a conservative suspend. It never calls the current
destructive `abort_all` path. Only the authenticated operator cancel action invokes
`AbortIntake`.

### 8.2 `public_guard`

The `~/system` deployment prompt adds a destination-address-specific nft accept for
`SetuIP tcp dport 443` and, only in QUIC mode, `SetuIP udp dport 443` inside the existing `public_guard`
default-deny table, with established/related return traffic and rate-limited logging of
drops. IPv6 is either configured equivalently with a dedicated address and tested, or
has no Setu listener/accept rule; never bind IPv6 accidentally. Source-IP allowlisting
may be layered for the managed US site, but mTLS remains mandatory because ad-hoc Spec
2 cannot rely on stable source IPs.

Acceptance captures `nft list ruleset`, listening sockets, and negative probes to every
other host port from an off-tailnet machine. Since the actual akash rules are not in the
mounted repositories, this live audit is a hard deployment gate, not a paper claim.

### 8.3 Activation and rollback boundary

Setu has no dual-write or legacy compatibility mode. Activation is nevertheless
coordinated because the old agent and server do not understand v2 journals or the new
partial store:

1. Deploy code and schema with public Setu admission disabled.
2. Quiesce new v1 receives and wait for every active `UploadFile` stream to finish.
   An incomplete v1 intake is either completed on v1 or explicitly cancelled with
   owner evidence; it is never silently adopted as v2.
3. Run recovery/readiness checks, update the managed agent, enable TCP admission, and
   accept one canary intake. Enable UDP only if the frozen site profile selected QUIC.
4. Record the first accepted v2 session as the forward-only boundary.

Before step 4, disabling Setu and restoring the old deployment is safe. After step 4,
an old binary is not a rollback: it rejects the journal and its existing sweeper may
delete state it does not recognize. Operational fallback is to stop new admission,
force `TCP_ONLY`, keep the v2-capable trusted service running, and commit, cancel, or
purge every v2 session through the reviewed recovery path. Only then may an older
deployment be considered.

### 8.4 Direct landing

Spec 1 is direct US→Coimbatore. No cloud staging bucket or POP receives plaintext. The
server transactionally reserves declared payload bytes, exact planned file count, the
contract-derived inode allowance (including unique parent directories), and
finalization headroom on the intent/data-session row before issuing a ticket. It
checks existing reservations plus the configured landing reserve against current free
space; duplicate wire bytes and hash work also have per-device rate ceilings so a
client cannot exhaust CPU under a unique-byte reservation. Reservations are released
only on trusted publication, explicit cancellation, or audited expiry/reclaim.

When capacity is unavailable, the offer remains `QUEUED_CAPACITY` and exposes queue
position, requested bytes, current reserved bytes, and the blocking reserve threshold.
Stale partial cleanup uses durable lifecycle, explicit expiry, and last server-approved
lease, not directory mtime. It must not reuse the current blanket 24-hour sweep for a
legitimate multi-day transfer (`src/sutradhara/grpc/server.py:111-125`). The production
values for disconnected grace, partial TTL, minimum free-space reserve, maximum queued
bytes/intakes, and reclaim warning are owner inputs in §15; the implementation may not
invent them.

## 9. Managed site-to-site path (Spec 1)

### 9.1 Reuse the existing headless daemon and control stream

`sutra-agent serve` is already the headless entry point: it loads `RelayConfig`, takes
the daemon lock, secures identity files, installs rustls, and runs `ControlDaemon`
(`~/sutra-agent/src/main.rs:18-46`). Spec 1 extends that daemon; it does not introduce a
second “Setu agent.” The current long-lived outbound stream sends heartbeat/inventory
and accepts server commands (`~/sutra-agent/src/relay/control.rs:151-200`). Add
`ManagedSourceOffer`, `ManagedSourceWithdrawn`, `SetuTransferStatus`, and
`SetuTransferTelemetry` to the client→server `DeviceMessage` oneof. Add
`StartManagedReceive`, `ManageSetuReceive`, and `SetuPolicyUpdate` to `ServerCommand`.
The existing `StartReceive` remains the authorization for card receives;
`StartManagedReceive` is the only command that authorizes an accepted managed offer.
The current proto has no source-offer message (`proto/device.proto:22-35`) and only
`StartReceive`/`ListDirectory` server commands (`proto/device.proto:65-79`), so this is
an additive control-plane extension, not an autonomous ingest path. Exact fields and
tags live only in `contract-setu-managed-site-v1.md`.

The existing `CardSnapshot.capabilities` registry is the rollout gate. The agent
advertises exact `setu-data-session-v1` and `setu-managed-v1` strings only after the
corresponding handlers and durable stores are usable. The server checks the live
registry before accepting a Setu session or managed offer and **before** creating a
receive intent or dispatching `StartManagedReceive`. Missing capability remains an
operator-visible `OFFERED` item; it cannot become an unacknowledged in-progress command
on an old agent that decoded the new oneof member as unknown. Capability disappearance
after v2 state exists suspends forward-only recovery; it never falls back to
`UploadFile`.

Enrollment is the existing mechanism: the agent generates its key locally, presents a
CSR, and stores the returned certificate owner-only
(`~/sutra-agent/docs/architecture.md:86-104`). The US site uses an ingest-scoped,
long-lived managed-device enrollment with normal rotation/revocation. It does not use
the short-lived contributor grant previewed for Spec 2. Because enrollment is not on
the public Setu listener, the deployment runbook must provide the managed host a named
protected tailnet/admin route for initial enrollment and periodic rotation, then prove
rotation without losing resumable state.

### 9.2 Workflow feed

Each managed root is a complete authority mapping, not merely a filesystem path. It
binds `root_id` to the enrolled operator, the exact existing `artifactclass` value,
admission policy, label policy, preparation/scratch mode, and one named completion
adapter. This is required because current `StartIntakeRequest` requires
`artifactclass`, while the enrolled certificate supplies the operator
(`proto/intake.proto:14-20`; `src/sutradhara/grpc/store.py:41-64`). Neither value may
come from an untrusted descriptor.

The two v1 completion adapters are:

- **`spool-v1` (preferred):** the production workflow atomically renames a strict
  `setu-source-v1.json` descriptor into a local spool. It contains a locally confined
  source path, stable workflow id/idempotency key, policy-constrained label input, and
  declared completion time.
- **`ready-marker-v1`:** the workflow atomically renames a completed file or
  directory into a configured ready root, or writes a sibling `.setu-ready` marker
  last. Mere “size unchanged for N seconds” is not sufficient for multi-TB sources.

The owner must select and test the actual upstream adapter before S1.9. If neither
contract matches the production workflow, S1.9 adds a named adapter for its real API;
it does not infer completion from quiet files.

The agent canonicalizes and confines the source under its configured root, plans it
with the existing payload planner, and emits an opaque offer (root id, relative path,
plan digest, planned bytes/files/directories, workflow id, label input, completion
evidence, and scratch requirement), never an absolute local path or artifact class. The server
resolves operator/artifact class/label from the root policy, creates the normal receive
intent, and sends `StartManagedReceive`. The agent then follows the same journal,
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

The exact draft-2020-12 JSON Schema and semantic rules live in
`contract-setu-managed-site-v1.md` §5. The policy is strict, versioned, generation-
activated, and rejects unknown keys. It binds the root authorities in §9.2, transport
mode, measured memory/lane ceilings, bounded FIFO queue, and an IANA-zone bandwidth
schedule whose windows use explicit `rate` or `pause` modes.

The schedule rules are chosen for predictability: highest priority wins, equal-priority
overlap is invalid, both instances of a repeated fall-back hour receive the same mode,
and a transition in a spring gap applies at the first valid instant. A pause stops token
refill without aborting; heartbeat, renewal, and control remain live. All active
intakes on a device share one bucket. Server policy is bound to the device and cached
durably, while local safety limits may only reduce rate or memory.

### 9.4 Operator lifecycle and controls

The operator should not have to infer transfer state from byte counters. The closed
lifecycle, reason codes, status fields, protobuf additions, HTTP endpoints, permissions,
idempotency rules, and audit record are single-sourced in
`contract-setu-managed-site-v1.md` §§2-4. Unknown enum values render as `UNKNOWN` with
the raw value and disable mutation; they never fall back to a plausible-looking active
state.

The existing authenticated operator UI must support three audited commands before
acceptance: **Pause** performs durable `SUSPEND`; **Resume** rechecks enrollment,
policy, source identity, capacity, and expiry before opening a new generation; and
**Cancel** shows retained bytes and is the only destructive path to `AbortIntake`.
Schedule pause is labelled separately and cannot be manually resumed while policy
still says pause. Each command is idempotent, capability-checked, records actor/time/
old state/new state/reason, and shows acknowledgement or failure. Deep BBR and interval
performance charts are deferred; lifecycle visibility and safe controls are not.

## 10. Resume at intercontinental scale

### 10.1 Client journal v2

Replace `sutra-agent-inflight-v1` with strict `sutra-agent-inflight-v2`. In addition to
the current fields it stores `data_protocol`, `data_session_id`, `resume_epoch`,
transport and file generations, negotiated chunk bytes, last server checkpoint
sequence, selected transport/fallback reason, source file identity snapshots,
workflow idempotency key, and last activity. It never stores a data ticket. The strict
document carries a canonical content digest and is atomically replaced with parent
fsync. It does **not** rewrite for every chunk: checkpoint hints are persisted no more
frequently than every 64 MiB or 2 seconds using the existing durability pattern
(`~/sutra-agent/src/relay/inflight.rs:216-238`).

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

An unreadable, checksum-invalid, or unknown-version journal is renamed to a timestamped
owner-only quarantine file and reported as `LOCAL_RECOVERY_REQUIRED`; it never causes
`AbortIntake`. For managed sources, the unchanged workflow descriptor and stable
idempotency key reconstruct the source plan, the idempotent `StartIntake` call recovers
the intake id, and the server resume plan recovers durable ranges. If source identity
cannot be proven, the operator sees the retained server bytes and must cancel or supply
the original source; the client does not guess.

### 10.2 Failure classification

Recoverable without abort: connection loss, QUIC path validation failure, TCP lane
failure, server restart/unavailable, client restart, sleep/network change, ticket
expiry while the control plane can renew, and timeout with a still-valid source. These
retain the journal, source lease where possible, partial landing, and server intake.

Terminal and fenced, but not automatically destructive: source mutation, source
disappearance beyond the approved grace interval, revoked/unauthorized identity,
intake owner conflict, protocol/hash/path conflict, policy rejection, corrupt durable
metadata, or a second commit re-upload request. These enter `FAILED` or
`RECOVERY_REQUIRED`, retain bytes under the configured policy, and expose a reason.
Only explicit operator cancel or an audited expiry/reclaim action deletes partials.
Disk-full pauses admission/transfer and alerts; it never deletes bytes to make room
without that reclaim action.

The current control reconnect loop already backs off 1 second to the configured maximum
(`~/sutra-agent/src/relay/control.rs:107-116`). Data reconnection uses jittered capped
backoff but never blocks heartbeat/control reconnect. The network stall timer measures
absence of server-reported received progress and is stopped during schedule/operator
pause or a server-declared landing limiter; durable-progress delay is reported as disk
pressure instead of triggering fallback.

### 10.3 IP changes and multi-hour lifetime

Live IP/NAT change first tries QUIC migration (§5.5). TCP has no migration, so failed
lanes reconnect and resume from durable ranges. Tickets are short enough to limit theft
(the proposed lifetime is 15 minutes) and renewable on the active authenticated data
session control channel as specified in the shared contract. If that channel is gone,
the client reopens through mTLS rather than using cached authority; a multi-day transfer
does not restart its durable ranges. Independently of ticket expiry, the ingress
rechecks enrollment/revocation no less often than every 20 seconds, matching the
current default client heartbeat at
`~/sutra-agent/src/relay/config.rs:243-252`. Revocation stops renewal and terminates the
active generation on that timer even if no chunk completes, so the designed propagation
bound is 20 seconds rather than the ticket lifetime.

### 10.4 One re-upload rule

Transport retransmission and missing-range recovery are not a commit “re-upload.” They
are idempotent delivery of one file before a receipt exists and may repeat until the
durable range set is complete. The existing one-re-upload rule begins only after every
file has a completed server receipt and `CommitIntake` reports a receipt/manifest
mismatch. For that requested relpath the server bumps its file generation, removes its
completed receipt under lock, atomically moves the prior generation into trusted
quarantine, and persists `reupload_used = true` before reopening the data session. The
client sends the whole file with the new generation and `force_reupload = true`; old
generation frames remain fenced. The new file is independently finalized before the
quarantined generation is reclaimed. A crash at any point resumes from the persisted
generation and flag, so the one-use allowance cannot reset. A second commit mismatch
remains the current hard error (`~/sutra-agent/src/relay/intake.rs:680-706`).

### 10.5 Graceful process shutdown

The current agent's terminal supervisor path eventually aborts the intake and removes
its journal after bounded retries (`~/sutra-agent/src/relay/intake.rs:520-557`). Setu
must split process shutdown from operator cancellation. On `SIGTERM`, service stop,
upgrade, or host shutdown, the agent stops new offers, stops source reads, drains
already acknowledged work, requests `SUSPEND`, persists the returned checkpoint, and
keeps the journal. The server quiesces public admission, fences active generations,
runs checkpoint/holding transitions, and retains partials. A deadline expiry may lose
only unacknowledged chunks; restart obtains the server resume plan. `AbortIntake` is
never called by this path. Kill-at-each-step tests cover agent, Python server, Rust
ingress, finalizer, and commit reconciler.

## 11. Spec 0 measurement spike

Spec 0 answers which bottleneck exists and whether QUIC earns its implementation and
operational cost. Run from the actual managed US source host to akash during at least
one business-hour and one overnight window, using the same representative corpus and
same-window Signiant evidence. It ends in exactly one outcome:

- `ENDPOINT_REMEDIATE`: disk, preparation, CPU, or landing durability cannot meet the
  target, so fix that before drawing a WAN conclusion;
- `TCP_ONLY`: bounded parallel TCP meets the agreed parity/ceiling target, so S1.7 and
  the public UDP rule are skipped; or
- `QUIC_WITH_TCP_FALLBACK`: TCP misses the target, QUIC materially improves it and
  passes integrity/resource tests, so build QUIC with TCP fallback.

### 11.1 Prerequisites and evidence

Record:

- timestamp/time zone, host/kernel/NIC, ISP plan, wired/Wi-Fi, VPN, public IP path;
- sender source filesystem and receiver landing filesystem;
- `ping` RTT distribution and loss, `mtr` path/loss as observational evidence;
- sender disk sequential read and SHA-256 throughput; receiver create/write/fdatasync
  throughput with the same file/chunk/checkpoint sizes; CPU and memory headroom;
- iperf3 version and exact commands; QUIC spike git revision/config, including
  its bounded gRPC/UDS bridge CPU, copy, and memory profile; and
- one real Signiant job log with useful bytes, start/finish, average/interval goodput,
  concurrency, retry/loss if exposed, and the same source corpus.

Do not infer WAN capacity from the ISP label. The tightest measured sender disk,
sender uplink, receiver downlink, receiver landing disk, CPU, or policy cap is the
candidate physical ceiling.

### 11.2 Scripted, sequential-stopping matrix

Spec 0 ships a reviewed `setu-spec0 run <approved-config>` command before the real-link
run. The runner preflights exact hosts, scratch paths, free space, source corpus hash,
tool versions, time synchronization, Setu DNS, and approved source IP. It refuses an
archive payload, landing root, or root filesystem as scratch. It snapshots nft and
listener state, installs the narrow temporary iperf rule, and registers idempotent
`EXIT`, `INT`, and `TERM` cleanup before starting a listener. Cleanup stops all
transient servers, removes only the rule it created, removes scratch files, captures
the restored ruleset, and fails the run if post-state differs from pre-state.

The table is the maximum matrix, not a command to spend every cell. Retained cells run
three times after warm-up and alternate order where comparison needs it:

| Test | Parallelism | Direction/notes |
|---|---:|---|
| iperf3 TCP | `-P 1`, `-P 4`, `-P 16` | US→India and reverse; 120 s each |
| iperf3 UDP | paced sweep below/at/above observed TCP aggregate | loss, jitter, reachability; never flood blindly |
| Setu QUIC+BBR spike | 1, 4, 16 QUIC connections | 16 is diagnosis only; same chunk/reservoir |
| Setu parallel TCP spike | 1, 4, 16 independent TCP connections | shared work queue, not fixed partitions |
| real payload | disk→Setu→landing disk at 1/4/16 | hash/checkpoint enabled; QUIC includes the production-shape UDS bridge |
| Signiant | its actual production settings | same corpus, route, and time window |

The runner stops in this order:

1. Run endpoint disk/hash/preparation tests. If they cannot clear the parity target
   with headroom, emit `ENDPOINT_REMEDIATE` and do no WAN tuning.
2. Run TCP `P=1`, then `P=4`. Run `P=16` only if `P=4` improves durable goodput by at
   least the predeclared materiality threshold and has not reached endpoint/link/cap.
   Stop increasing after the first flat or worse step.
3. Run the real TCP payload path and same-window Signiant comparison. If TCP meets both
   frozen targets, emit `TCP_ONLY`; do not expose UDP or build the QUIC engine.
4. Only when TCP misses target without an endpoint explanation, open the temporary UDP
   rule, run bounded QUIC `1`, then `4/16` under the same stopping rule, and perform a
   paced UDP loss sweep at approved rates. Remove the rule immediately after this
   phase.
5. If QUIC clears the target and materially beats TCP, emit
   `QUIC_WITH_TCP_FALLBACK`. If neither transport does, retain the measured diagnosis
   and return to the owner before Spec 1; do not label an unproven engine production.

For every retained interval record useful/durable goodput, wire rate, RTT, loss/retrans/PTO,
connection count, sender disk read, receiver disk write/fdatasync, CPU, reservoir
occupancy, cap state, and stalls. A memory-to-memory result is not a product result;
the real-payload row is mandatory.

The operator invokes the runner, not a hand-copied firewall/fio sequence:

```text
setu-spec0 preflight /etc/setu/spec0-approved.toml
setu-spec0 run /etc/setu/spec0-approved.toml --evidence-dir /approved/evidence/run-id
setu-spec0 verify-cleanup /approved/evidence/run-id
setu-spec0 summarize /approved/evidence/run-id
```

`fio --fsync=32` approximates one 64 MiB durability group at 2 MiB blocks; the real
Setu run remains authoritative because its atomic ledger/checkpoint work is not modeled
fully by fio. If a 64 GiB fixture is too small to leave cache effects behind on the
actual hosts, the approved config increases it beyond available RAM. The evidence
packet includes runner revision, expanded commands, stdout/stderr, exit status, host
telemetry, cleanup proof, and exact configuration rather than transcribed headlines.

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

Produce one immutable, checksummed packet: raw logs, expanded commands/config, cleanup
proof, `setu-spec0-summary.json`, manifest, and a short diagnosis. The summary records
one outcome enum plus the Spec 1 chunk size, sender/receiver reservoir budgets, stream
credit, TCP lane ceiling, optional QUIC connection ceiling, checkpoint batch, cap
schedule, and parity target. The default acceptance proposal is:

- Setu median durable goodput >= 90% of the same-window Signiant median; and
- Setu median durable goodput >= 85% of the diagnosed usable endpoint/link ceiling;
- zero integrity discrepancies, bounded configured memory, and successful resume/fallback.

The owner may set different numeric targets, but must freeze them before interpreting
the comparative cells, not after seeing which engine wins. `TCP_ONLY` deletes S1.7 from
the implementation plan and keeps UDP closed. `QUIC_WITH_TCP_FALLBACK` retains S1.7
only when the production-shape local bridge also clears the endpoint target within its
checked CPU/memory budget.
`ENDPOINT_REMEDIATE` blocks both. A POP is tested only when direct engines reach their
endpoint limits yet remain materially below Signiant; published cloud-backbone claims
are never a substitute for this link's measurement.

## 12. Verification member and observability

### 12.1 Goodput proof

Useful goodput is **unique payload bytes durably checkpointed divided by active transfer
wall time**. It excludes retransmissions, duplicate chunks, protocol/TLS overhead,
scheduled zero-rate pauses, and time after the final durable byte. Report both active
and end-to-end workflow wall time so preprocessing and commit are not hidden.

The agent sends structured `SetuTransferTelemetry` over the existing outbound
`DeviceService.Connect` stream; it opens no metrics listener, preserving the no-inbound
invariant enforced by the current architecture
(`~/sutra-agent/docs/architecture.md:4-10`). Spec 1 writes structured interval JSON and
a bounded transfer summary; the parity script consumes those logs. The operator UI
shows the lifecycle and essential live fields in §9.4, not a new performance dashboard.
The evidence log records:

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

The existing Drishti Vector/VictoriaLogs path receives stable structured Setu
health/reason signals. It does **not** yet provide a level-triggered findings ledger;
that substrate exists only in the frozen Viveka design
(`~/drishti/docs/design-alerting-viveka.md`). S1.11 therefore owns signal emission and
the Setu status projection, then integrates those signals with Viveka once its findings
substrate is implemented. Production cutover S1.13 is gated on a real raise/clear test
through that substrate rather than treating logs as alerts.

Required signals cover public certificate expiry/SAN mismatch, listener or nft drift,
DB/UDS authority loss, control-latency breach under saturated data lanes, landing
reserve pressure, queue/reservation saturation, stalled received progress, stalled
durable progress, partial expiry within the warning window, corrupt journal/ledger/
receipt/sentinel, `RECOVERY_REQUIRED`, and commit-reconciler backlog. Each carries site,
device, intake/session where applicable, observed value, threshold, reason code, and
operator action. The Viveka adapter turns them into level-triggered findings with
first/last seen and clears only after observed health.

### 12.2 Hermetic `~/system` scenario

Add **Scenario SETU — WAN receive single funnel**, contract slug `setu-wan-receive`,
`hermetic_capable = true`, covering `sutradhara.device.relay`,
`sutradhara.intake.setu_transport`, `sutradhara.intake.receive`, and
`sutradhara.intake.accept`. This closes a real current gap: Scenario RDD invokes
`IntakeServicer.UploadFile` directly (`~/system/scenarios/scenario_receive_dedup.py:330-370`),
while Scenario IW.5 still says the real Rust agent streaming binding is retired/unwired
(`~/system/harness/seams/intake.py:200-240`). Existing RDD remains a funnel regression;
SETU crosses the actual transport seam.

The scenario uses loopback CA/enrollment, test-local SQLite/landing, the dedicated
Python Setu server, a release/debug `sutra-agent`, and the real Rust ingress when the
frozen profile includes QUIC. A userspace UDP fault proxy supplies delay/loss and can
blackhole UDP without root/netem. Injected clocks and named crash barriers replace
sleep-based assertions. Steps:

1. **SETU.1 identity/public boundary:** provision a DNS-SAN cert, enroll the real agent,
   prove every enabled transport presents the same fingerprint and `ingest` scope,
   reject wrong SAN/CA/unenrolled cert and unsupported TLS, and prove Restore/general
   private services are absent.
2. **SETU.2 TCP funnel:** managed source offer → policy-derived `StartManagedReceive` → at
   least two independent TCP connections → partial writer → trusted finalizer →
   idempotent commit/watch; assert one verified catalog intake and expected hash.
   First connect an old generated agent and an agent missing either Setu capability;
   both must leave the offer visible without creating an intent or dispatching a
   command. Only the fully capable agent may advance it.
3. **SETU.3 chunk resume:** kill the agent after a durable mid-file checkpoint, restart,
   then repeat with the trusted server; assert only server-declared missing ranges cross
   the wire and both produce the identical final bag.
4. **SETU.4 optional QUIC/fallback:** in QUIC mode, prove the same funnel, blackhole UDP,
   kill ingress, and cut the UDS/DB owner. The non-payload probe plus CAS must select
   TCP, stale frames must not write, authority loss must suspend within 20 seconds, and
   TCP output must equal SETU.2. This member is absent, not skipped green, in `TCP_ONLY`.
5. **SETU.5 backpressure/cap:** slow the landing writer, assert high/low stop-start,
   checked sender and both receiver-process budgets never exceed configuration, control
   status/renew/close latency stays within bound under lane saturation, and a fake clock
   enforces rate, explicit pause, generation/effective-at, and DST semantics.
6. **SETU.6 fail closed:** corrupt a chunk, conflict an offset, revoke the cert, and
   attempt commit with active/uncheckpointed data; all fail without `intake.json` or a
   catalog row. Auth/protocol failure must not fall back. Symlink, unauthorized UDS
   method, forged/stale frame, post-ack tamper, landing-path access, and
   direct-publication attempts prove the uid boundary.
7. **SETU.7 shutdown versus cancel:** send `SIGTERM` to agent, Python, and Rust at each
   named barrier; all retain durable state and resume. Only the audited cancel command
   removes partials and journal.
8. **SETU.8 commit/recovery:** kill before/after commit request, partial assembly,
   `intake.json` rename, DB publication, and watcher observation. Reconciliation must
   converge to one bag/catalog row. Corrupt journal, ledger, receipt, and sentinel each
   produce retained `RECOVERY_REQUIRED`, never deletion or fabricated completion.
9. **SETU.9 managed/operator journey:** exercise artifact class/operator/label mapping,
   scratch preflight, bounded capacity queue, lifecycle transitions, expiry warning,
   Pause/Resume/Cancel authorization and audit, and unknown-state fail-closed behavior.
10. **SETU.10 one funnel/parity:** compare every enabled transport's BagIt/receipt
    fixtures except declared telemetry, run the watcher, and assert normal quarantine,
    deduplication, policy, archive submission, and copy behavior.

The scenario asserts partial-writer and trusted-finalizer call counters and forbids
either public leg from creating a trusted receipt or `intake.json`. Scenario RDD's
helper migrates from direct `UploadFile` to the new shared writer when the old method is
removed; its watcher/catalog assertions remain registered
(`~/system/scenarios/contracts.toml:465-476`). SETU extends rather than strands that
evidence. The system-ui lifecycle/control prompt separately carries browser QA across
desktop and mobile, including queued, paused, recovery-required, destructive-confirm,
permission-denied, and unknown-state fixtures.

## 13. Security posture and future seams

### 13.1 Spec 1 public surface

- Every public socket requires mTLS with the enrolled CA and explicit Setu DNS
  certificate. QUIC enforces TLS 1.3; Python gRPC TCP accepts TLS 1.2 or newer. The data
  ticket is bound to fingerprint, device, `ingest` scope, intake, expiry, protocol,
  limits, and active transport generation.
- No data plane can mint an intake or select artifact class/policy. `StartIntake`
  currently requires an authorized receive intent and records owner/source metadata
  (`src/sutradhara/grpc/servicer.py:82-162`); that remains the gate.
- Paths are canonicalized and confined in the shared receive core. The ingress has no
  landing path or file descriptor: it can damage in-flight QUIC frames if compromised,
  but it cannot mutate a durable partial after acknowledgement, traverse active or held
  state, publish into `data/`, create a trusted receipt, reach the DB, or write
  `intake.json`. The trusted writer validates every forwarded frame and the finalizer
  rechecks the full hash; no ingress result is publication authority.
- Size, file/inode count, chunk/range, session, connection, pre-auth byte, duplicate
  work, disk-reserve, and memory limits are checked before allocation or write.
- QUIC 0-RTT is off. Tickets are random, stored hashed, short-lived, and never logged,
  even as a prefix. Logs use the separate non-secret `ticket_id`.
- Revocation fences the generation on the independent 20-second check. Transient lease
  or authority loss suspends and retains partials for the configured grace period; it
  does not preserve the current abort-on-lease-loss behavior
  (`src/sutradhara/grpc/servicer.py:358-398`). Expiry/reclaim is explicit and audited.
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
through a tightly scoped internal client of the same partial-writer/finalizer/commit
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
expiry-bounded ciphertext. Decryption still occurs before the same partial-writer and
trusted-finalizer boundary, and final whole-file SHA/BagIt verification remains in
Coimbatore. Spec 1 does not ship dormant relay code, flags, or cloud dependencies.

## 14. Dependency-ordered build order

Each row is intended to become one future Codex prompt. “Verification member” is part
of that prompt's done condition. Every production-code prompt runs the owning repo's
full tests; no runtime backout/compat flag is introduced.

| ID | Prompt-sized work item | Repository/repositories | Verification member |
|---|---|---|---|
| S0.1 | Build disposable TCP/QUIC movers and the trap-cleaned sequential `setu-spec0` runner; no intake integration | `~/sutra-agent`, `~/sutradhara`, deployment fixture in `~/system` | deterministic loopback hash/resource tests; exact dependency locks; firewall/listener cleanup under success, error, INT, TERM |
| S0.2 | Execute §11 on the real link and freeze targets plus one outcome | immutable evidence in `~/system` | checksummed raw logs/manifest, same-window Signiant evidence, signed `setu-spec0-summary.json` |
| S1.1 | Implement the normative shared data-session contract in proto and regenerate consumers without activating Setu | `~/sutradhara` proto/fixtures; `~/sutra-agent` consumption | merged current proto compiles while `UploadFile` still exists; staged production client leaves admission absent and sends the empty legacy commit session id; populated admission is fixture-only; generated-code/cross-language round trips, unknown/limit fixtures, schema fuzzing; no private schema copy |
| S1.2 | Implement trusted `PartialLandingWriter` and authority-split `IntakeLandingFinalizer` in the receive crate/PyO3 | `~/sutradhara/packages/sutradhara-receive` | crash points, GIL-release parallel test, openat2/symlink confinement, full independent hash, no partial-writer publication method |
| S1.3 | Add session/file/commit DB state, idempotent assembly/reconciliation, trusted receipts, corruption quarantine, and migrate RDD before removing `UploadFile` | `~/sutradhara` | existing gRPC/RDD tests; every §7.3 crash window; corrupt ledger/receipt/sentinel; exactly one bag/catalog row |
| S1.4 | Split `IntakeControlApi`/`DataTransport`; add one reservoir/work queue/token bucket, journal v2, scratch preflight, and graceful suspend | `~/sutra-agent` | fake fast/slow/failing transport; memory/permit poison; source mutation; journal corruption/rebuild; SIGTERM never aborts |
| S1.5 | Add open/resume/probe/switch/close authority, hashed tickets, durable reservations, generation CAS, and bounded gRPC-over-UDS bridge | `~/sutradhara` | scope/owner/revocation/expiry, pagination, stale frame, kill during switch, UDS/DB loss within 20 s, application-credit bound, data-only method allowlist, no fd/path/finalization over UDS |
| S1.6 | Build the dedicated public `grpc.aio` Setu server and independent-channel `ParallelTcpTransport`; activate the server first, then enable the agent's exact `setu-data-session-v1` advertisement | `~/sutradhara`, `~/sutra-agent` | server enforcement plus agent capability activation atomically changes the client from absent admission + `UploadFile` to declared admission + data session; pre-activation agent stays unadvertised; old-agent and mixed-version negatives; service allowlist/no Restore, SAN/TLS matrix, control capacity under lane saturation, work stealing, TCP final-bag parity |
| S1.7 | **Conditional on `QUIC_WITH_TCP_FALLBACK`:** build unprivileged Rust ingress, bounded UDS forwarding, and `QuicBbrTransport` | `~/sutradhara`, `~/sutra-agent` | no landing permission/fd, retry/pre-auth quotas, bridge CPU/copy/memory bound, BBR evidence, derived stream credit, loss/reorder/migration, QUIC final-bag parity |
| S1.8 | **Conditional with S1.7:** wire probes, progress-aware QUIC-to-TCP CAS, capability withdrawal, and ingress supervision | `~/sutradhara`, `~/sutra-agent`, `~/system` | UDP blackhole, ingress kill, late old frame, auth never falls back, TCP-only runtime fallback |
| S1.9 | Add managed-root authority mapping, selected workflow adapter, capability-gated bounded queue, scheduler, lifecycle/status, and audited controls API | `~/sutra-agent`, `~/sutradhara` | old/missing-capability agent creates no intent/command; artifactclass/operator/label binding, scratch/capacity, policy generation/DST, Pause/Resume/Cancel, unknown state |
| S1.10 | Provision explicit Setu PKI, protected enrollment/rotation route, sibling systemd units, exact-address binds, conditional nft rules, readiness, activation, and recovery runbook | `~/sutradhara`, `~/system` | clean-slate deployment; enroll/rotate without lost resume; SAN/key/uid/UDS checks; live nft/listener/negative-port evidence; v1 drain and pre-boundary rollback drill |
| S1.11 | Add lifecycle/control UI, structured telemetry/parity report, Drishti signal emission, and the Viveka findings adapter | `~/system-ui`, `~/sutradhara`, `~/sutra-agent`, `~/drishti` | API authorization/audit; browser QA states/actions; VictoriaLogs signal query; Viveka dependency declared and adapter raise/clear once substrate exists; frozen Signiant comparison |
| S1.12 | Add full hermetic Scenario SETU and freeze the selected profile | `~/system` with all owning repos | SETU.1–SETU.10, declared `covers`, clean-slate run, full repo tests |
| S1.13 | Shadow, cut over the real workflow, soak, and retire Signiant only after acceptance | operational evidence/runbook in `~/system` | no double archive admission; parity targets; scheduled pause, restart, IP change, capacity, recovery/cancel drills; live Viveka raise/clear; owner sign-off |

S1.1–S1.5 establish correctness and recovery before opening a public data service. S1.6
lands TCP first because it proves the contract and supplies a complete production mode
without experimental congestion control. S1.7/S1.8 exist only when Spec 0 pays for
them. No prompt may let a network leg publish a file or bypass the trusted finalizer.
The behavioral tests in data-contract §12 are acceptance requirements distributed to
the first row that implements each behavior and then composed in S1.12; S1.1 owns only
schema compilation, generated-code compatibility, limits, and serialization. It does
not claim server recovery behavior before S1.3-S1.8 exists. In particular, S1.1 keeps
`setu_admission` absent on the production legacy request. S1.6 is the first row allowed
to enable the agent's advertisement of `setu-data-session-v1`, after server-first
activation; capability visibility in `CardSnapshot`, admission enforcement, and the
client's switch away from `UploadFile` form one rollout boundary. There is no
server-advertised Setu capability.

Signiant cutover is an explicit operator journey. S1.13 first shadows offers without
creating a second archive intent, then transfers an approved representative set through
Setu while Signiant remains the recovery route, compares end-to-end outcomes, and moves
the upstream workflow trigger only after the soak passes. Signiant is decommissioned
after the owner-approved observation window and recovery drill, not merely after a
single fast transfer.

Spec 2 begins only after S1.13 with contributor scope/grants, quarantine policy, native
portal client, then optional browser transport—all targeting the same open-session,
writer, commit, and watcher seams. Spec 3 begins only after a measured direct-route
failure and adds opaque frame relay plus end-to-end encryption; it does not alter the
landing or catalog contract.

## 15. Open business decisions

1. **Public address:** can akash receive a dedicated public IPv4 (and optionally IPv6)
   plus Setu DNS name so TCP/UDP 443 do not collide with the existing UI/HTTP3 surface?
2. **Spec 0 facts:** what are the actual US source host/site, disks, uplink, allowed
   test windows, representative corpus, and available Signiant log? These determine
   connection/lane ceilings and the acceptance target.
3. **Engine gate:** may Spec 0 select `TCP_ONLY` and cancel QUIC work when TCP meets the
   frozen parity target? If QUIC is selected, is `quinn`'s experimental BBR risk
   acceptable after the pinned-version evidence?
4. **Managed workflow mapping:** which production API/marker proves completion, and what
   operator, existing artifact class, admission policy, label rule, normalized-artifact
   or scratch root apply to each managed source root?
5. **Bandwidth/queue policy:** what business-hour and overnight rates, time zone,
   explicit pause windows, aggregate scope, fairness, maximum active/queued intakes,
   and maximum queued bytes should be encoded?
6. **Retention/capacity policy:** how long may disconnected or failed multi-terabyte
   partials remain, how much warning precedes reclaim, and what landing reserve and
   finalization headroom stop admission?
7. **Parity bar:** accept the proposed 90%-of-Signiant and 85%-of-diagnosed-ceiling
   thresholds, or freeze different values before the comparative Spec 0 run?
8. **TCP TLS posture:** accept TLS 1.2+ mTLS on the Python gRPC leg, or fund a reviewed
   Rust TCP terminator to enforce TLS 1.3-only? The current Python API cannot honestly
   claim the latter.
9. **Signiant cutover:** what shadow corpus, observation/soak window, recovery drill,
   and final decommission approval are required before the workflow trigger moves?
10. **Spec 2 scope:** will every ad-hoc contributor install the native client, or must the
   later unaccelerated browser path be funded? What quarantine artifact class and byte
   ceiling apply?
11. **Spec 3 trigger:** is “direct Setu remains materially below Signiant after endpoint
    bottlenecks are removed” the accepted relay gate, and which cloud/region may be used
    for the measured relay experiment?
12. **Enrollment route:** which existing protected tailnet/admin path will the managed
    US host use for initial enrollment and rotation, and what rotation cadence and
    maintenance owner apply? Public Setu deliberately does not expose enrollment.

## 16. Final implementation invariants

1. Every public data frame reaches the one receive-core partial writer. No public
   transport can publish into `data/`, create a trusted receipt, or write `intake.json`.
2. Trusted file specification and expected SHA arrive through enrolled-mTLS control,
   never from ingress authority. Only the trusted finalizer may independently hash and
   atomically publish a payload. Only idempotent `CommitIntake`/BagIt assembly may
   publish `intake.json`; verify/dedup/policy/RAO/copy are not reimplemented.
3. Every enabled transport presents the same enrolled client certificate, verifies the
   explicit Setu DNS certificate against the enrolled CA, requires `ingest` scope, and
   binds the ticket to that identity and generation.
4. Resume acknowledges only payload plus ledger covered by durable checkpoint. The
   server ledger is authoritative; QUIC migration and client journal hints are not.
5. One sender reservoir, token bucket, and work queue bound all lanes. Disjoint receiver
   process budgets have a checked sum no greater than the host ceiling; connection or
   stream count cannot multiply memory or permitted bandwidth.
6. Parallelism is bounded, measured, and reduced after its knee. Sixteen is diagnostic,
   not a production default. QUIC ships only with a `QUIC_WITH_TCP_FALLBACK` outcome.
7. Control remains outbound-only at the agent and has reserved service capacity.
   Routine shutdown suspends and retains; only audited cancel is destructive.
8. Public exposure is TCP/443 and, only in QUIC mode, UDP/443 on the dedicated Setu
   address within live-audited default-deny nft rules. No Restore or metrics listener is
   registered there.
9. There is no legacy transport or dual-write flag. Before the first v2 session, the
   deployment can roll back; afterward recovery remains forward-only until all v2 state
   is committed, cancelled, or purged.
10. Corrupt or ambiguous journal, ledger, receipt, commit, or sentinel state is fenced,
    retained, and surfaced as `RECOVERY_REQUIRED`; it is never guessed or swept by age.
11. Managed roots bind operator, artifact class, admission, label, completion, and
    scratch policy. Lifecycle and Pause/Resume/Cancel semantics are shared by API, UI,
    logs, and audit.
12. Performance claims use unique durable goodput on the real corpus/link and compare
    with frozen Signiant evidence. Integrity, bounded resources, resume, controls, and
    recovery are part of parity, not secondary checks.
