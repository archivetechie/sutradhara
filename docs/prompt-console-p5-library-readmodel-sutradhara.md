# codex prompt — console P5 (server): library read-models (disks / tapes / drives)

**Repo:** `~/sutradhara/repo` (work here only). **Normative source:**
`~/system-ui/docs/design-console-excellence.md` §4.4 (shapes verbatim incl. every note),
§4 conventions, §6-P5, §7-E/§7-H — read before writing. Reuse the shared console helpers
(`api/console.py`) and the admin-shaping pattern from P3/P4.
Status: implemented (2026-07-04; diff gate PASS; offsite canonical byte-match fix @b44c757).

## Tasks
1. **Contract doc `docs/contract-library-readmodel.md`** (copy §4.4 shapes + notes; register
   in `docs/INDEX.md`).
2. **`GET /api/ui/library/disks`** (`can_view`): **explicit whitelist projection — do NOT
   reuse `lifecycle._disk_payload`/`status()`** (they carry `mount`). `mount` NEVER crosses
   the API for anyone. Admin-only fields (omitted server-side for non-admin):
   `serial`,`wwn`,`fs_uuid`,`enclosure`,`slot`,`smart_status`. All-viewer fields: `disk_id`,
   `state`, `capacity_bytes`,`filled_bytes`,`capacity_state`,`last_walk_at`,
   `entry_count`/`lost_count` (CacheEntry aggregates). total/truncated envelope.
3. **`GET /api/ui/library/disks/{disk_id}`**: disk fields + drills — **reuse
   `repopulate.drill_status()`** (B9): map `started_at→lost_at`,
   `remaining+refilled→entries_lost`, `drill_id` from `lost_drill_id` grouping.
4. **`GET /api/ui/library/tapes`** (`can_view`): copies on tape backends grouped by media-id.
   Row key `tape_key` = opaque token for all viewers; `media_id` (voltag) admin-only,
   `null` otherwise. `library` provenance from the remanence catalog — document the
   **`media_id` byte-match normalization risk** in the contract (§7-H); `offsite_confirmed`
   via OffsiteConfirmation media_id match. total/truncated envelope.
5. **`GET /api/ui/library/drives`** (`can_view`): remanence LibraryService bridge (gRPC —
   reuse the existing remanence client plumbing). **Both VTLs** (`mainlib` changer revision
   `D.00`, `d2lib` `D2D0`) — enumerate + filter, never assume one. Drive `serial` and slot
   `voltag` admin-only (omitted for non-admin); all viewers get bay/status and slot
   address/full. Complete enumeration — no total/truncated. If the gRPC daemon is
   unreachable, return the standard error envelope (503-style code), never a partial fake.
6. **Tests**: whitelist invariant (adversarial: `mount` present on the model never appears in
   ANY payload, admin included); admin-vs-non-admin shaping on all three endpoints (non-admin
   payload has NO hardware identity — assert absence of every §7-E field); drill_status reuse
   mapping; tape grouping + tape_key opacity/stability + media_id null for non-admin;
   offsite_confirmed match; two-VTL filter test (both libraries present, revision filter);
   drives-unreachable error envelope; `can_view` gates; envelopes/caps.

## Constraints
No UI work; no schema migrations; no changes to other endpoints; don't touch remanence —
consume its existing gRPC surface only.

## Done
`uv run pytest -q` green (full suite). Commit to main:
`console-p5(server): library read-models (disks/tapes/drives)`. Print a 10-line summary.
