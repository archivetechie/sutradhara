# Codex prompt — device folder browse (sutra-agent / helper)

**Repo:** `~/sutradhara/repo` (package `packages/sutra-agent`, planner in
`packages/sutradhara-receive`) · **Status:** pending · **After the sutradhara prompt**
(consumes the regenerated `_proto` stubs).
**Design:** `docs/design-device-folder-browse.md` (authoritative; read it).

## Why
The **helper** half: advertise the `browse` capability, answer `ListDirectory` (the
authoritative confinement guard, folders-first, symlink-omit, `is_package`, caps), and fix the
receive handler so a selected folder is actually streamed as the root — including packaging a
package-boundary root.

---

## Shared contract (identical across all three prompts)

### Protocol — `proto/device.proto` (field numbers PINNED; committed stubs on both sides)
```proto
message CardSnapshot { repeated Card cards = 1; repeated string capabilities = 2; }
message ListDirectory { string request_id = 1; string card_id = 2; string rel_path = 3; }
message DirectoryEntry { string name = 1; bool is_dir = 2; int64 size_bytes = 3; bool is_package = 4; }
enum DirectoryStatus {   // zero is *_UNSPECIFIED
  DIR_STATUS_UNSPECIFIED = 0; DIR_STATUS_OK = 1; DIR_STATUS_NOT_FOUND = 2;
  DIR_STATUS_NOT_A_DIRECTORY = 3; DIR_STATUS_PERMISSION_DENIED = 4;
  DIR_STATUS_CONFINEMENT_VIOLATION = 5; DIR_STATUS_CARD_UNAVAILABLE = 6; DIR_STATUS_IO_ERROR = 7;
}
message DirectoryListing {
  string request_id = 1; repeated DirectoryEntry entries = 2; bool truncated = 3;
  DirectoryStatus status = 4; string detail = 5;
}
// ServerCommand.oneof += ListDirectory list_directory = 2;   (start_receive = 1)
// DeviceMessage.oneof += DirectoryListing directory_listing = 5;  (active_receives = 4)
```

### Path semantics
- `rel_path`/`source_ref`: forward-slash relative, normalized; `""` = card root; **root ≡ whole
  drive**. Null and `""` canonicalize to `""`.
- Received file relpaths are relative to the selected folder (stream root); `DCIM/100MEDIA`
  stores `IMG001.JPG`. **Exception:** a package-boundary root yields `<name>.tar`.

### Shared syntactic validator (server-side)
Server rejects (400) absolute/`..`/backslash/`C:`/non-normalized/`>1024`, returns canonical path.

### Confinement guard (helper-side, authoritative) & Packages
`PACKAGE_GLOBS = *.fcpbundle, *.photoslibrary, *.imovielibrary, *.app`. The helper guard resolves
`rel_path` within the card mount and rejects `..`/absolute/symlink-or-junction-escape **and any
non-final segment matching `PACKAGE_GLOBS`** (no descending into a package). Child symlinks are
omitted from listings (`lstat`/no-follow — matches the receive scanner's `RejectedEntry`
soft-skip). If the resolved receive root itself matches `PACKAGE_GLOBS`, the planner packages it
(`<name>.tar`).

### Caps / leakage
Folders in full (ceiling 5000); files capped 500 (after folders); `truncated=true` when hit.
Entries carry only relative names; `DirectoryListing.detail` (and `CommandAck.reason` for path
failures) sanitized relative-only — never raw exception strings.

### HTTP (server, for reference)
`GET …/browse` (authz `can_receive`+owner; capability gate → `409 browse_unsupported`; typed
`status`→HTTP map; timeout→504; mid-close→409). `GET /api/devices` adds device `capabilities`.
`POST …/receive` validates before dispatch; receive-time path failure → `409 receive_rejected`.

### Registry (server)
`PendingListing` sibling to `PendingCommand`; generalized outbound queue; generation guard;
stream close/revoke/supersede fails both maps' futures.

---

## Files
- `packages/sutra-agent/src/sutra_agent/_proto/` — regenerated stubs (from the sutradhara prompt).
- `packages/sutra-agent/src/sutra_agent/confine.py` (new, or into `mounts.py`) — the shared
  confinement/resolve guard.
- `packages/sutra-agent/src/sutra_agent/controld.py` — advertise `capabilities`; the
  `ListDirectory` handler; the receive-handler fix.
- `packages/sutradhara-receive/src/sutradhara_receive/core.py` — package a package-boundary
  **stream root** (today `_scan_source` only detects package boundaries among child dirs).
- Tests under each package's `tests/`.

## Milestones (TDD — test first each)
1. **Confinement guard.** `resolve(mount_path, rel_path) → abs path | error`. Unit tests: `""`→
   mount root; normal subpath ok; reject `..`/absolute/backslash; reject a path whose target is a
   symlink resolving outside the mount; **reject `A001.fcpbundle/Event`** (non-final package
   segment); allow `DCIM/A001.fcpbundle` (final package segment).
2. **Capability advertisement.** `CardSnapshot` includes `capabilities=["browse"]`. Test the
   snapshot builder emits it.
3. **`ListDirectory` handler.** List the confined dir; sort **folders first then files**; omit
   child symlinks; set `is_package` on entries matching `PACKAGE_GLOBS`; caps (folders full to
   5000, files 500, `truncated`); `size_bytes` files-only. Map failures to typed `status`
   (`NOT_FOUND`/`NOT_A_DIRECTORY`/`PERMISSION_DENIED`/`CONFINEMENT_VIOLATION`/`CARD_UNAVAILABLE`/
   `IO_ERROR`) with **relative-only** `detail`. Unit tests per behavior (folders-first, symlink
   omitted, package flagged, >500 files truncated, permission-denied → status, no absolute paths
   in any field).
4. **Planner packages a package-boundary root** (`sutradhara-receive`). Unit tests: streaming a
   root that is `A001.fcpbundle` yields a single `A001.fcpbundle.tar` payload unit (not exploded
   loose files); streaming a *parent* still packages child packages (regression).
5. **Receive-handler fix (`_handle_start_receive`).** Pass `confine(card.mount_path,
   command.source_ref or "")` as the `stream_source` **source** (currently `card.mount_path`);
   `source_ref` still rides `StartIntakeRequest` as metadata. Tests: selecting `DCIM/100MEDIA`
   receives `IMG001.JPG` (relpaths relative to selection); package root → `.tar`; a
   confinement/`..` failure rejects with a relative-only `CommandAck.reason`.
6. **Full suite** green across both packages. Commit.

## Out of scope
- Proto definition + server relay/registry/endpoints (sutradhara prompt — this consumes them).
- The browser UI (system-ui prompt).

## DoD
Helper advertises `browse`; `ListDirectory` returns confined, folders-first, symlink-free,
package-flagged, capped, relative-only listings; a path inside a package is rejected; the receive
handler streams the confined subpath and packages a package root; both packages' suites green.
