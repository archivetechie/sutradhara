# Codex prompt — P0.1: the fact-recording API (sutradhara engine)

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo`
> (single repo — no Shared-contract section).** Read `CLAUDE.md` + `AGENTS.md` first.
> Source: `docs/implementation-plan-ingest-v2.md` **item P0.1** (the first slice);
> the contract is `docs/design-arrangement-arc.md §2.7` (handlers record *facts* via a
> domain API, never raw catalog rows) and `docs/design-reconciliation-model.md §3.7`.
>
> **This is a seam refactor, not a feature.** No new tables, no Alembic migration, no
> behaviour change visible to scenario R. You are extracting the row-writes two handlers
> do today into one small, idempotent, transaction-safe API, then routing those two
> handlers through it. Done right, the diff is "handlers stop constructing ORM rows."

## Why (one paragraph, then build)
Today `transcode` and `pfr-index` reach straight into the ORM: `transcode` builds
`LogicalAsset` / `IngestItem` / `AssetDerivation` rows and sets `LogicalAsset.validity`;
`pfr-index` writes a sidecar pointer into `item.item_metadata` and sets `.validity`.
That couples every handler (and every future job kind, and the coming reconcilers) to the
schema. The fix is a thin **fact-recording API** — `record_derivation` / `record_index` /
`record_copy` / `record_validity` — that owns the get-or-create, idempotency, and edge
bookkeeping in one audited place. New job kinds and the P0.3 reconcilers then record facts
without knowing the tables.

## What already exists — BUILD ON IT, do not rebuild
- **Catalog tables** (`src/sutradhara/catalog/models.py`): `LogicalAsset`
  (content-addressed, PK = `content_sha256`), `IngestItem` (per-occurrence; UNIQUE
  `(intake_id, as_received_path)`; carries `item_metadata` JSON), `AssetDerivation`
  (provenance edge; UNIQUE `(derived_item_id, source_item_id, kind)`), `Copy`.
- **The copy funnel already exists.** `src/sutradhara/catalog/copies.py::add_copy` is
  documented as "the single, content-addressed funnel through which every Copy passes";
  `add_bundle_copy` is its bundle sibling. `cloud_blob` already writes copies *through*
  `add_bundle_copy` — it already conforms; do **not** rewire its behaviour.
- **Handler contract** (`src/sutradhara/jobs/registry.py`): `handle_x(ctx: JobContext)
  -> JobResult`; `JobContext{session, job, granted_leases}`.
- **Transaction boundary** (`src/sutradhara/jobs/engine.py::run_one`): the handler runs
  on `ctx.session`; immediately after, `run_one` sets `job.status` / `finished_at` /
  `step_state` **on the same session**; the **caller commits**. So a handler's facts and
  the job's terminal status are already committed atomically. **Therefore the fact API
  MUST NOT call `session.commit()` or `session.rollback()`** — it may `session.flush()`
  only (to allocate IDs / surface UNIQUE violations early), exactly as today's code does.
- **Shared hashing:** `src/sutradhara/rem_archive_cli.py::sha256_file` (already used by
  `cloud_blob`). Reuse it; do not add a third private `_sha256_file`.
- **Enums** (`catalog/types.py`): `AssetValidity{OK,SUSPECT,UNVALIDATED}`,
  `MediaKind{VIDEO,AUDIO,IMAGE,DOCUMENT,OTHER}`.

## The API to build — `src/sutradhara/catalog/facts.py` (new)
A new module beside `copies.py`. Module docstring states the two invariants: **(a) never
commit/rollback — the caller owns the transaction; (b) every function is idempotent —
calling it twice for the same fact is a no-op, never a duplicate row.**

### `record_derivation(...) -> IngestItem`
Move the body of `transcode._register_derived_item` here, verbatim in behaviour:
```python
def record_derivation(
    session: Session,
    *,
    source_item: IngestItem,
    output_path: Path,
    relpath: str,                 # the derived occurrence's as_received_path / virtual_path
    kind: str,                    # "mezz" | "preview" | ...
    artifactclass: str,           # the derived occurrence's class — CALLER-supplied for now
    media_kind: MediaKind,
    generated_by: str,            # provenance tag stored in item_metadata
) -> IngestItem: ...
```
- Require `output_path` exists and is non-empty (raise `ValueError` otherwise — current
  behaviour).
- `digest = sha256_file(output_path)`; get-or-create `LogicalAsset(content_sha256=digest,
  size_bytes=…, media_kind=…, media_info={"derived_from_item_id": source_item.id,
  "kind": kind}, validity=UNVALIDATED)`.
- Get-or-create the derived `IngestItem` by `(intake_id=source_item.intake_id,
  as_received_path=relpath)`; on re-run update hash/stat/artifactclass and **merge**
  `item_metadata` with `{"source_path": str(output_path), "generated_by": generated_by,
  "source_item_id": source_item.id, "kind": kind}` (do not clobber unrelated keys).
- Get-or-create the `AssetDerivation` edge `(derived_item_id, source_item_id, kind)`.
- `flush()` so the new item's `id` is available; return the derived `IngestItem`.
- **Idempotency note:** the natural keys + the two UNIQUE constraints already make this a
  no-op on re-run — preserve that; do not add `ON CONFLICT` cleverness.

> `artifactclass` stays a **caller parameter** in P0.1 — `transcode` keeps passing
> `proxy_artifactclass`. Assigning the class from a prepare profile / `output_class` is
> **P1.2's** job (the derivation reconciler), explicitly out of scope here.

### `record_index(...) -> None`
The fact for an index sidecar that produces **no Copy** (today: pfr-index):
```python
def record_index(
    session: Session,
    *,
    item: IngestItem,
    index_kind: str,              # "pfr-index-v1"
    sidecar_path: Path,
    metadata_key: str = "pfr_sidecar_path",
) -> None: ...
```
- Records that an index of `index_kind` exists at `sidecar_path` for `item` by **merging**
  `item.item_metadata[metadata_key] = str(sidecar_path)` (current behaviour). No
  `LogicalAsset`, no `Copy`. Idempotent (last-writer).
- **Forward-compat is the point:** the FACT is "an index of kind X exists at P," not the
  ffprobe fields. When P4.1 ships the real container-index sidecar (richer payload, byte
  offsets, GOP ranges), the handler calls the same `record_index` with `index_kind=
  "pfr-index-v2"` and a different sidecar — **no handler or schema churn.** Keep this
  function ignorant of the sidecar's *contents*.

### `record_validity(...) -> None`
```python
def record_validity(
    session: Session, *, asset: LogicalAsset, validity: AssetValidity, note: str | None = None,
) -> None: ...
```
Set `asset.validity` / `asset.validity_note`. Idempotent (last-writer). Both handlers use
this for their SUSPECT/OK transitions.

### `record_copy` — façade over the existing funnel
Re-export the copy funnel so handlers have **one** import surface for facts:
`from sutradhara.catalog.facts import record_copy, record_bundle_copy` →
thin aliases to `catalog.copies.add_copy` / `add_bundle_copy` (no logic change). `cloud_blob`
may switch its import to `facts` for consistency, but its **behaviour must not change**.

## Migration (the two handlers)
1. **`jobs/handlers/transcode.py`:** delete the local `_register_derived_item` (its body
   now lives in `record_derivation`) and call `record_derivation(...)` for `mezz` and
   `preview`. Replace both direct `source_asset.validity = …; source_asset.validity_note
   = …` sites (the SUSPECT decode-error path and the OK completion path) with
   `record_validity(...)`. Drop the now-unused private `_sha256_file` if nothing else uses
   it. `step_state` (mezz/preview item ids etc.) is unchanged.
2. **`jobs/handlers/pfr_index.py`:** replace the `item.item_metadata = {…
   "pfr_sidecar_path": …}` write with `record_index(ctx.session, item=item,
   index_kind="pfr-index-v1", sidecar_path=sidecar_path)`. Replace the
   `asset.validity = SUSPECT; asset.validity_note = …` (container-parse-error path) with
   `record_validity(...)`. The sidecar **file write** stays in the handler — the fact is
   the pointer.

## Tests
- New `tests/test_facts.py` (or extend `tests/test_ingest_handlers.py`):
  - **derivation idempotency:** run `transcode` twice (`SUTRADHARA_FAKE_TRANSCODE=1`) on
    one item → exactly one derived `IngestItem` per kind, one `AssetDerivation` edge per
    kind, no duplicate `LogicalAsset`, and the second run's returned ids equal the first.
  - **index idempotency:** run `pfr-index` twice (`SUTRADHARA_FAKE_FFPROBE=1`) → the
    `pfr_sidecar_path` pointer is stable and unrelated `item_metadata` keys survive.
  - **validity:** decode-error fixture flips `LogicalAsset.validity` to `suspect` via
    `record_validity` (same observable result as before).
  - **no-commit invariant:** a `record_*` call inside a transaction that the test then
    rolls back leaves no rows — i.e. the API didn't commit behind the caller's back.
- All existing tests stay green: `uv run pytest` (or the repo's runner).

## Acceptance
- `rg "AssetDerivation\(|LogicalAsset\(|\.validity\s*=" src/sutradhara/jobs/handlers/`
  returns **nothing** for `transcode.py` / `pfr_index.py` (they no longer construct rows
  or set validity directly; `session.get(LogicalAsset, …)` reads are fine).
- `src/sutradhara/catalog/facts.py` exists with the four verbs; its docstring states the
  no-commit + idempotency contract.
- `uv run pytest` green, including the new idempotency + no-commit tests.
- **Scenario R green from a clean slate** (`~/system`: `make reset && make up &&
  make scenario-r`) — the integration guard that behaviour is unchanged.
- `git diff --stat` shows **no** change under `migrations/` / no new model in
  `catalog/models.py` (zero schema change).

## Out of scope (do NOT do these here)
- **No new tables** (no typed `asset_index` table — the `item_metadata` pointer stays).
- **No reconciler, no `output_class` policy** — `record_derivation` takes `artifactclass`
  from the caller; profile-driven class assignment is P1.2.
- **No `cloud_blob` behaviour change** (it already uses the copy funnel).
- **No new job kinds, no PFR work, no restore work.**
- **No refactor of the engine / lease / claim logic.**
