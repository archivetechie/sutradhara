# codex prompt — console P4 (server): intake + archive/catalog read-models

**Repo:** `~/sutradhara/repo` (work here only). **Normative source:**
`~/system-ui/docs/design-console-excellence.md` §4.2 + §4.3 (shapes normative, field lists
verbatim — including every B-item note under them), §4 conventions, §6-P4 — read before
writing. Reuse the shared console sanitizer + envelope helpers from P3 (`api/console.py`).
Status: pending.

## Tasks
1. **Contract docs** `docs/contract-intake-readmodel.md` + `docs/contract-archive-readmodel.md`
   (copy §4.2/§4.3 shapes + notes; register in `docs/INDEX.md`).
2. **`GET /api/ui/intakes?status=&days=&limit=`** (`can_view`): §4.2 fields verbatim;
   newest-first, cap 200, total/truncated. **`GET /api/ui/intakes/{id}`**: items expose
   `virtual_path`, NEVER `as_received_path` (§7-I); derivations are **item-id based, two
   joins** to project to hashes, and **edges may cross intakes** — the payload must not
   assume both ends in `{id}` (B9).
3. **`GET /api/ui/archive/bundles?artifactclass=&status=&limit=`** + **`/submissions`**
   (`can_view`): §4.3 shapes; `Bundle.status` vocabulary is
   `open|flushing|sealed|held|aborted` (frozen from `archive_bundle.py:170`); submissions
   `pending_archive|archived`.
4. **`GET /api/ui/archive/assets/{content_sha256}`** (`can_view`): copies MUST traverse
   `AssetLocator` (hash → copy_id/representation/native_locator) ⋈ Copy ⋈ Backend — **not** a
   direct asset→copy join (B1: archival copies are bundle copies under the XOR).
   `originating_intake_id` = latest `ingest_item` by `created_at` (B9). `locator_summary` is
   a display token (tape → voltag/media-id; cloud → blob-key digest; disk → pool + opaque
   key), recursively sanitized, and **admin-only — omitted server-side for non-admin** (B6b);
   non-admins still get backend_kind/health/tier per copy.
5. **`GET /api/ui/catalog/assets?q=&artifactclass=&limit=&offset=`** (`can_view`): each row an
   **(asset × artifactclass) pair** (B2) — mapping source `bundle_member→bundle.artifactclass`
   for archived, `ingest_item` for pre-bundle; classes can diverge per occurrence. `offset`
   paging (the one offset endpoint, B10); total/truncated. PoC search = hash-prefix /
   substring `LIKE` over asset + class rows (scale is §7-J, out of scope).
6. **Tests**: shape field-for-field vs contracts; the B1 traversal (fixture: bundled asset
   whose copies are reachable ONLY via asset_locator — a direct join must return nothing);
   asset×class duplication (one asset under two classes → two catalog rows, correct class
   each); originating-intake selection rule; virtual_path-only invariant (adversarial
   as_received_path present in DB, absent in payload); locator_summary admin/non-admin
   shaping + no host path even for admin; cross-intake derivation edge; offset paging
   determinism; can_view gates; cap+envelope.

## Constraints
No UI work; no schema migrations; no changes to jobs/restore/reconciliation endpoints.

## Done
`uv run pytest -q` green (full suite). Commit to main:
`console-p4(server): intake + archive/catalog read-models`. Print a 10-line summary.
