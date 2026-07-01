# Codex prompt — device folder browse (sutradhara / server)

**Repo:** `~/sutradhara/repo` · **Status:** pending · **Server-first (do this one first).**
**Design:** `docs/design-device-folder-browse.md` (authoritative; read it).
**Companions (identical Shared contract):** `docs/prompt-device-folder-browse-sutra-agent.md`,
`~/system-ui/docs/prompt-device-folder-browse-system-ui.md`.

## Why
Add an on-demand directory listing so the operator GUI can browse a card and receive a
chosen folder (or the whole drive). This prompt is the **server** half: the proto, the
relay plumbing, the `/browse` endpoint, capability exposure, and the shared path validator
(applied to `/browse` **and** `/receive`).

---

## Shared contract (identical across all three prompts)

### Protocol — `proto/device.proto` (field numbers PINNED; committed stubs on both sides)
```proto
message CardSnapshot {
  repeated Card cards = 1;
  repeated string capabilities = 2;      // new helper sends ["browse"]; old helper sends none
}
message ListDirectory {
  string request_id = 1;
  string card_id    = 2;
  string rel_path   = 3;                  // fwd-slash relative, normalized, "" = card root
}
message DirectoryEntry {
  string name       = 1;
  bool   is_dir     = 2;
  int64  size_bytes = 3;                  // files only; 0 for dirs
  bool   is_package = 4;                  // dir matches PACKAGE_GLOBS
}
enum DirectoryStatus {                    // zero is *_UNSPECIFIED (repo convention)
  DIR_STATUS_UNSPECIFIED = 0;
  DIR_STATUS_OK = 1;
  DIR_STATUS_NOT_FOUND = 2;
  DIR_STATUS_NOT_A_DIRECTORY = 3;
  DIR_STATUS_PERMISSION_DENIED = 4;
  DIR_STATUS_CONFINEMENT_VIOLATION = 5;
  DIR_STATUS_CARD_UNAVAILABLE = 6;
  DIR_STATUS_IO_ERROR = 7;
}
message DirectoryListing {
  string request_id = 1;
  repeated DirectoryEntry entries = 2;
  bool truncated = 3;
  DirectoryStatus status = 4;
  string detail = 5;                      // display-only, sanitized relative-only
}
// ServerCommand.oneof payload  += ListDirectory list_directory = 2;   (start_receive = 1)
// DeviceMessage.oneof payload  += DirectoryListing directory_listing = 5;  (active_receives = 4)
```

### Path semantics
- `rel_path`/`source_ref`: forward-slash relative, normalized; `""` = card root; **root ≡ whole
  drive**. Null and `""` **canonicalize to `""`** — the same canonical form is used for
  validation, the idempotency hash, and dispatch (no null-vs-`""` conflict).
- Received file relpaths are relative to the selected folder (stream root); `DCIM/100MEDIA`
  stores `IMG001.JPG`. **Exception:** a package-boundary root yields `<name>.tar` (see Packages).

### Shared syntactic validator (server-side, `/browse` **and** `/receive`; before dispatch + idempotency claim)
Returns one canonical path; rejects with **400**: absolute paths, any `..` segment, backslashes,
drive-letter forms (`C:`), non-normalized input, length `> 1024`.

### Confinement guard (helper-side, authoritative) & Packages
`PACKAGE_GLOBS = *.fcpbundle, *.photoslibrary, *.imovielibrary, *.app`. The helper guard rejects
`..`/absolute/symlink-escape **and any non-final path segment matching `PACKAGE_GLOBS`** (no
descending into a package). Child symlinks are omitted from listings (`lstat`/no-follow). If the
resolved receive root itself matches `PACKAGE_GLOBS`, the planner packages it (`<name>.tar`).

### Caps / leakage
Folders in full (ceiling 5000); files capped 500 (after folders); `truncated=true` when hit.
Entries carry only relative names; `DirectoryListing.detail` and `CommandAck.reason` are
sanitized relative-only (never raw exception strings).

### HTTP
- `GET /api/devices/{id}/browse?card_id=&path=` — authz **`can_receive` + device-owner**; if the
  helper lacks `browse` → **409 `browse_unsupported`** (non-retryable); run the validator; relay
  a `ListDirectory`; await (bounded). Response
  `{ path, entries:[{name,isDir,sizeBytes,isPackage}], truncated }`. **Error map** (typed
  `status`): `NOT_FOUND`→404, `NOT_A_DIRECTORY`→422, `PERMISSION_DENIED`→403,
  `CONFINEMENT_VIOLATION`→400, `CARD_UNAVAILABLE`→409, `IO_ERROR`→502; timeout→**504**;
  mid-request stream close (`StreamClosed`)→**409 device_unavailable**; device/card not
  connected→404.
- `GET /api/devices` adds device-level **`"capabilities": ["browse", …]`** (empty for legacy).
- `POST /api/devices/{id}/receive` runs the same validator (canonicalized) **before the
  idempotency claim**; receive-time path failure → **409 `receive_rejected`** + sanitized reason.

### Registry
`PendingListing{request_id, generation, command, future, created_at}` sibling to `PendingCommand`;
dedicated `pending_listings` keyed by `request_id`; the outbound `command_queue` generalized to
`PendingCommand | PendingListing | None`; dispatch loop sends the matching `ServerCommand`.
Generation-guard drops replies on a superseded stream / after timeout. **Stream
close/revoke/supersede fails BOTH maps' futures** (CommandAck reject; listing → `StreamClosed`).

---

## Files
- `proto/device.proto`; regenerate **both** `src/sutradhara/_proto/` and
  `packages/sutra-agent/src/sutra_agent/_proto/` (protobuf 6.33.x / grpcio-tools 1.81.x).
- `src/sutradhara/api/paths.py` (new) — the shared syntactic validator.
- `src/sutradhara/grpc/registry.py` — `PendingListing`, `pending_listings`, generalized queue,
  `request_directory_listing`, generation guard, close-fails-both, `capabilities` on `DeviceView`.
- `src/sutradhara/grpc/device_service.py` — dispatch `ListDirectory`; route `DirectoryListing`.
- `src/sutradhara/api/routes_devices.py` — `GET …/browse`, `_device_payload` capabilities,
  `POST …/receive` validator wiring.
- Tests under `tests/` (match existing module layout).

## Milestones (TDD — test first, run red, implement, run green, commit each)
1. **Proto + stubs.** Add the messages/enum/fields with the pinned numbers; regenerate both
   `_proto` trees. Test: import `device_pb2` and construct `ListDirectory`/`DirectoryListing`.
2. **Shared validator (`paths.py`).** Unit tests: canonical `""` for null and `""`; reject
   absolute/`..`/backslash/`C:`/non-normalized/`>1024`; accept normalized relative paths.
3. **Registry.** `PendingListing` + `pending_listings` + generalized `command_queue` +
   `request_directory_listing(...) → Future` + generation guard. Tests: a `DirectoryListing`
   resolves the right future; a superseded-generation or post-timeout reply is dropped; stream
   close fails **both** pending maps.
4. **Device service dispatch.** Dispatch loop sends `ListDirectory` for a `PendingListing`; the
   reader routes `DirectoryListing` into `pending_listings`. Tests with a fake stream.
5. **Capabilities.** `CardSnapshot.capabilities` recorded on `DeviceView`; `_device_payload`
   emits `"capabilities"`. Test: a helper advertising `["browse"]` surfaces it in `GET /api/devices`.
6. **`GET …/browse`.** authz + capability gate (`409 browse_unsupported` for empty caps) +
   validator + relay + response shape (incl. `isPackage`) + the full typed-status→HTTP map +
   timeout→504 + mid-stream-close→409. Tests per branch.
7. **`POST …/receive` validator.** Apply the canonical validator before the idempotency claim.
   Tests: null vs `""` `source_ref` produce the **same** idempotency hash; a `..`/absolute
   `source_ref` → 400 (never dispatched/claimed).
8. **Full suite** `uv run pytest` green. Commit.

## Out of scope
- The helper's `ListDirectory` handler / confinement guard / capability advertisement / planner
  change (that's the sutra-agent prompt).
- The browser UI (system-ui prompt).

## DoD
Proto stubs regenerated both sides; `/browse` returns typed listings for a browse-capable helper
and `409 browse_unsupported` otherwise; `/receive` validates + canonicalizes `source_ref`;
registry relays and fails futures on close; full suite green.
