# Prompt RM1.2a — authorized `OpenRestore` streaming + shared restore.proto (NO hdcache source-depth)

**Status:** implemented. Second RM1 milestone, part a. Builds on RM1.1 (landed
sutradhara main @ba04c6e) and RM0 (streaming restore, landed).
**Normative (read FIRST, binding — do NOT inline):**
`docs/design-restore-agent-protocol-v0.1.md` **§7** (the v0.2 fold) — esp **§7.5 RM1.2**, §3.2
(RestoreService), §3.5 (live gate), §3.6 (receiver-relative destination), §7.1-B6 (lease), §7.2
(concurrency / worker-starvation), §7.3/§7.4 (`manifest_end`, idempotency, mid-stream stance);
and `docs/design-restore-agent-v0.1.md` §12.1 (the RestoreFrame wire contract) + §5 (service/agent
split — the RPC drives the sutradhara plan, it is NOT a pass-through of remanence's read stream).
**Read the real code you extend (verify, cite file:line):**
`src/sutradhara/archive_restore.py` (RestorePlan ~152, `open_member_stream` ~195, `build_restore_plan`
~509 via `select_source_candidates` ~547, `PlannedMember` ~111; the AEAD extract-stream integration
from RM0.3b), `grpc/server.py` (~51 `ThreadPoolExecutor(max_workers=16)` shared with intake/device;
mTLS `ssl_server_credentials(require_client_auth=True)` ~67; servicer registration ~52/63),
`grpc/servicer.py` (IntakeServicer — the streaming precedent), `grpc/device_service.py`,
`grpc/ca.py` (`resolve_peer_identity` ~290), `grpc/store.py` (`resolve_device` ~98, the RM1.1
`GrpcLogicalDevice` ~74 with `scopes`, `restore_open_session` lease model, `grpc_device_destination_grant`),
`api/live_capabilities.py` (RM1.1 fail-closed `LiveCapabilityResolver`), `hdcache/manager.py`
(`admit_restore_request`, `serve_restore_item` ~703 — the server_local path stays; `_update_request_state`
~1937 now knows `sent`), `hdcache/models.py` (RestoreRequest/Item, `restore_item_checkpoint`,
`restore_open_session`, delivery_mode/receiver_device_id/final_rel_path), `proto/intake.proto` +
`proto/device.proto` (proto style + the codegen path — how _pb2 is built).

**Scope: the shared proto + the authorized OpenRestore streaming path over ARCHIVE-backed items via
the existing RM0 `build_restore_plan`. NOT the hdcache producer / cache-first source-selection depth
(that is RM1.2b). NOT durable commit / resume / lifecycle reconciliation (that is RM1.3).**

## 1. Author the shared `proto/restore.proto` (consumed by BOTH this server AND the RM2 Rust client)
Author the complete restore protocol now (RM1.3 implements CommitRestore's durable semantics; the
message SHAPES are authored here so the RM2 client can build against a stable contract). It MUST include:
- **`RestoreFrame`** = `oneof { manifest_head | manifest_entry | file_header | chunk | file_end |
  manifest_end | job_end | error }` — matching `design-restore-agent-v0.1.md` §12.1 exactly. Manifest is
  STREAMED as entries (never one giant message). `manifest_head{total_bytes, file_count,
  single_top_level, top_component}`; each `file_header` carries `content_sha256` (plaintext-layer, so the
  client can bound manifest memory + verify-on-receive) + `final_rel_path` + the RM1.1-B5 SYNTHETIC
  regular-file metadata (mode/uid/gid/mtime; document that xattrs/entry-types beyond regular files are
  out of v1); `chunk{bytes, offset?}`; `file_end`; `manifest_end`; `job_end`; `error{code, message}`.
- **`OpenRestore(OpenRestoreRequest{restore_request_item_id, lease_token?, resume_token?}) ->
  stream<RestoreFrame>`** — `lease_token` and `resume_token` carry the **open-session generation/lease**
  (§7.1-B6) so a resumed/retried open is arbitrated (RM1.2a implements lease ACQUIRE-on-open + duplicate
  rejection; RM1.3 implements resume re-drive). `resume_token = {restore_request_item_id,
  manifest_sha256, committed_index}` (defined now; consumed in RM1.3).
- **`CommitRestore(CommitRestoreRequest{restore_request_item_id, manifest_sha256, committed_index,
  durable_state: STAGED|REVEALED, lease_token}) -> CommitRestoreReply`** — message authored now; RM1.2a
  MAY register a minimal servicer method that returns UNIMPLEMENTED (or accepts + no-ops with a clear
  "RM1.3" status) — do NOT implement durable CAS here. State the choice explicitly.
- **`WatchAssignments(WatchRequest{device_id}) -> stream<Assignment{restore_request_item_id,
  manifest_sha256?, ...}>`** — the assignment channel: how a bound agent LEARNS which items are assigned
  to it. RM1.2a implements a working WatchAssignments that streams the device's currently-admitted
  agent-delivery items (bound via `receiver_device_id`) + new ones as they are admitted (a simple
  poll-and-stream or notify is fine; document the delivery/cadence semantics and that it is scoped to the
  authenticated peer device — a device sees ONLY its own assignments).
- Document in a header comment: `CommitRestore(REVEALED)` is terminal + idempotent regardless of
  intermediate STAGED; the **committed_index-divergence contract** (a lost STAGED ack ⇒ server MAY
  re-stream from an older index; the client verifies-on-receive, skips already-durable data, truncates
  the in-progress file) — normative for RM1.3 + RM2, authored here.
- Generate the `_pb2`/`_pb2_grpc` via the SAME codegen path as intake/device (update whatever
  build/generate step exists; commit generated stubs if the repo commits them, matching convention).

## 2. Register `RestoreService` (mTLS, shared server) — `grpc/server.py` + a new `grpc/restore_service.py`
- Register alongside Intake/Device with the SAME `ssl_server_credentials(require_client_auth=True)`; peer
  identity via `grpc_ca.resolve_peer_identity` → `store.resolve_device` (device_id + fingerprint).
- **Per-RPC ownership (§7.2 — binding):** each OpenRestore call constructs its OWN `RestorePlan` +
  backend instances + a SHORT-lived DB session; NEVER share a `RestorePlan`/session/`_bundle_temp`/
  `_bundle_paths` across streams (they are mutable, `archive_restore.py`). Add a **restore-specific
  concurrency semaphore** (bounded, < the 16 shared workers) so a few long restore streams cannot starve
  intake/device; when saturated, fail fast with RESOURCE_EXHAUSTED (do not block a worker indefinitely).
  Honor context cancellation (client disconnect / deadline) — stop the plan + release the lease promptly.

## 3. OpenRestore serves from the RM0 plan — WRAP, do not reimplement (single funnel)
- OpenRestore **frames the chunks produced by `build_restore_plan(...).open_member_stream(member)`** — the
  bounded, verified, plaintext (plaintext + AEAD via RM0.3b) iterator. The RPC does NOT re-implement
  copy-selection/decode/verify — it drives the existing RM0 plan and turns its output into frames.
  (RM1.2b deepens the SOURCE — hdcache-first + candidate/SUSPECT fallback. RM1.2a serves ARCHIVE-backed
  items via the existing `build_restore_plan` as-is. Do NOT add a second/parallel restore path.)
- **Freeze the server manifest digest BEFORE emitting any data frame.** Compute the plan's
  `manifest_sha256` (over the ordered per-file `content_sha256` + `final_rel_path` + sizes — define
  canonically, document it) and emit it in `manifest_head`/entries first; the same frozen digest is what
  CommitRestore/resume will match (RM1.3). No data frame precedes a frozen manifest.
- **Stop at `sent`, no local terminal:** the OpenRestore worker, on fully streaming the item, transitions
  the item to `sent` (NOT `done` — the terminal write is remote; `done` needs the agent's REVEALED commit
  in RM1.3). It must NEVER enter `serve_restore_item`'s local publish / `_serve_from_cache` local write.
  `server_local` items are untouched by this path.

## 4. The live open gate (§3.5 — fail closed) — the auth funnel
Before streaming any frame, an OpenRestore for an agent item must pass ALL, else abort with a precise
gRPC status (PERMISSION_DENIED / FAILED_PRECONDITION), streaming NOTHING:
- (a) the calling mTLS **device is still enrolled, not revoked, and holds the `restore` scope**
  (via the RM1.1 logical-device scopes) — an ingest-only or revoked device is refused;
- (b) it **is the `receiver_device_id`** bound to this request item (a device cannot pull another
  device's restore);
- (c) the **live operator holds `can_restore`** (+ the object's privacy tier `can_restore_p2/p3` if
  applicable) via the RM1.1 fail-closed `LiveCapabilityResolver` — unreachable authority ⇒ deny;
- (d) the item's `delivery_mode == "agent"` and it is in a streamable state (queued/sent per the RM1.1
  state model); a `server_local` item is never served over OpenRestore.
The frozen-admission caps (`validate_restore_item_admission`) stay for job dispatch; THIS is a second,
narrower LIVE gate for the agent open. Mid-stream revocation is out of scope for RM1 (documented risk,
§7.4) — a bounded stream + the open gate is the RM1 stance.

## 5. Lease acquire-on-open + duplicate rejection (§7.1-B6) — NOT the commit CAS (RM1.3)
- On OpenRestore, **acquire the open-session lease** (`restore_open_session`, RM1.1 table) bound to
  `(item_id, receiver_device_id, manifest_sha256)` via an **atomic conditional UPDATE (SQL CAS,
  row-count arbitrated)** with a generation + expiry. A second concurrent OpenRestore for the same item
  **is rejected** (or supersedes a demonstrably-expired one — pick reject-if-live, supersede-if-expired;
  document it). A zombie/partitioned holder's lease expires and the item becomes reopenable. Release the
  lease on stream end/cancel. (RM1.3 owns the COMMIT-side CAS + resume re-drive; RM1.2a owns
  acquire-on-open + duplicate rejection only.)

## Binding invariants
- **`server_local` restore is byte-for-byte UNCHANGED** (golden baseline — the existing restore suite is
  the oracle). The agent OpenRestore path NEVER enters the local writer / `canonicalize_restore_destination`
  / a server root. **Single funnel:** OpenRestore drives the RM0 `RestorePlan` — no parallel restore/decode
  path. Per-RPC plan+session (no shared mutable plan). Live gate fails CLOSED. No runtime compat flag.
  Author the complete proto; implement OpenRestore + WatchAssignments + lease-on-open; CommitRestore
  durable semantics + resume are RM1.3 (message authored, method stubbed/minimal).
- The proto is CONSUMED BY THE RM2 RUST CLIENT — every field the client needs (manifest_end,
  content_sha256, lease/resume tokens, WatchAssignments, committed_index-divergence doc) must be present
  and stable. Treat the proto as the cross-language contract.

## Tests (the verification member — REQUIRED; no `#[ignore]`/skip on these)
- **Real-socket OpenRestore round-trip:** a test gRPC client (mTLS) opens a restore for a bound
  restore-scoped device against an ARCHIVE-backed disk item (reuse the RM0.2 RestorePlan fixture pattern);
  asserts frames arrive in order (manifest_head → entries → manifest_end → per-file header/chunks/file_end
  → job_end), the reassembled plaintext SHA matches, and memory stays bounded (no whole-object buffer).
- **Encrypted item** streams too (RM0.3b AEAD) — same round-trip over an AEAD item.
- **Negative auth (each fails CLOSED, streams nothing):** ingest-only device refused; revoked device
  refused; wrong-receiver device refused; operator lacking `can_restore` refused (+ resolver-unreachable
  ⇒ deny); a `server_local` item refused over OpenRestore.
- **Duplicate open rejected:** a second concurrent OpenRestore for the same item is rejected while the
  first holds the lease; after lease expiry the item reopens.
- **`sent` transition:** a fully-streamed item ends in `sent` (NOT `done`); no Job/local write was created;
  the console request-state aggregation renders it in-progress (not `completed_with_errors`).
- **WatchAssignments:** a bound device sees its own admitted agent items and NOT another device's.
- **server_local unchanged:** the existing restore suite stays green.

## Definition of done (this repo's AGENTS.md)
`uv run pytest -q` green (paste tallies), `uv run ruff format --check`/`ruff check` + `uv run mypy` clean
on touched files (repo has pre-existing mypy debt elsewhere — don't regress yours; `_raise`-style
NoReturn helpers already exist). Regenerate proto stubs via the existing codegen path. Summary: files
touched, each test → the section it covers, the proto messages added, and an explicit statement that
(a) server_local is byte-for-byte unchanged, (b) OpenRestore drives the RM0 plan (no parallel path),
(c) the live gate fails closed, (d) no agent stream reaches a local write, (e) what is deferred to
RM1.2b (hdcache source-depth) and RM1.3 (durable commit/resume). Do NOT implement the hdcache producer,
cache-first selection, durable CommitRestore CAS, or resume re-drive here.
