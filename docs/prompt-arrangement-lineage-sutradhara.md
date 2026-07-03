# Codex prompt — arrangement revise-lineage column — sutradhara

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo` (single repo).**
> Read `CLAUDE.md` + `AGENTS.md` first.
> Findings source (the *why*): `~/system/docs/report-fable-review-hard-threads-2026-07-03.md`,
> Thread 3 ("Lineage: `create_from_arrangement` records no parent — add
> `Arrangement.cloned_from_arrangement_id` so the revise chain is reconstructible in 10 years").
>
> **What this is.** One small, surgical schema addition. Restore→submission→arrangement provenance
> is already sound; the missing edge is the **genealogy between arrangement revisions**. When an
> operator revises an archived program by cloning its arrangement, the clone today records nothing
> about its parent — so the revise chain can't be walked. Add a nullable self-referential FK and
> set it on clone. Nothing else.

## What already exists — BUILD ON IT
- `arrangement.create_from_arrangement(session, arrangement_id, *, label)`
  (`src/sutradhara/arrangement.py:139-168`) clones non-excluded members into a new `DRAFT`
  arrangement. It sets `label`, `intake_id`, `artifactclass`, `status` — but records **no parent**.
- The `Arrangement` model (`src/sutradhara/catalog/models.py:296-...`) already carries a
  `use_alter` self-consistent FK pattern (`submission_id` → `submission.id`,
  `ondelete="SET NULL"`, `models.py:319-329`) — mirror that style for the new column.
- Alembic head is **`4e6f8a1c2b3d`** (`add_restore_admission_inputs`). Chain the new revision
  from it. The column-add pattern for existing tables uses `op.batch_alter_table` for sqlite
  parity (see `alembic/versions/c4e9b7a2d6f8_add_virtual_arrangement.py:24-27`).
- `ArrangementSummary` (`arrangement.py:80-90`) + `summarize_arrangement` (`arrangement.py:246-258`)
  and the CLI `_arrangement_payload` (`src/sutradhara/cli/arrangement.py:175-192`) are the
  operator-facing summary/show surfaces to extend.

## Scope — exactly this
1. **Model.** Add `Arrangement.cloned_from_arrangement_id: Mapped[int | None]`, a nullable FK →
   `arrangement.id` with **`ondelete="SET NULL"`** (use `use_alter=True` like `submission_id`,
   since it is self-referential), indexed. Deleting a parent arrangement **nulls** the child's
   pointer — it must **never cascade-delete** the child.
2. **Migration.** One Alembic revision chained from `4e6f8a1c2b3d`, adding the column (and its FK
   + index) via `batch_alter_table`. `Base.metadata.create_all` and `alembic upgrade head` must
   agree — extend `tests/test_schema.py` accordingly.
3. **Set it on clone.** `create_from_arrangement` sets `cloned_from_arrangement_id = source.id`
   on the new clone (`arrangement.py:148-153`). No behavior change otherwise.
4. **Expose it.** Add `cloned_from_arrangement_id` to `ArrangementSummary` +
   `summarize_arrangement` and to the CLI `_arrangement_payload` so `sutra arrangement show`
   (and its `--json`) surface the parent. (If any other summary/show surface exists, include it;
   do not invent new ones.)

## Do NOT (explicit)
- **No other schema changes.** Only the one column + its migration.
- **Do not touch** `virtual_arrangement`, `restore`, retention, submit/freeze, or any reconciler.
- **No version/CAS column** anywhere (separate STANDING tripwire; out of scope).
- No provenance walker CLI/API beyond exposing the column — the chain being *walkable* is proven
  by tests, not by a new command.

## Tests — Definition of Done (`uv run pytest -q` green; extend `tests/test_schema.py` +
`tests/test_arrangement.py`)
- **Clone records parent:** `create_from_arrangement` sets `cloned_from_arrangement_id` to the
  source id; a freshly created (non-clone) arrangement has it `None`.
- **Parent delete nulls, never cascades:** deleting the parent arrangement row sets the child's
  `cloned_from_arrangement_id` to `NULL` and leaves the child (and its members) intact.
- **Provenance chain is walkable across two revisions:** A → clone B → clone C; walking
  `cloned_from_arrangement_id` from C reaches B reaches A.
- **Schema parity + migration:** the column exists after both `create_all` and `alembic upgrade
  head`; the new revision chains from `4e6f8a1c2b3d`; downgrade drops it cleanly.
- **Summary/show expose it:** `summarize_arrangement` and the CLI payload include the field.
- **Existing suites green:** `uv run pytest`, format + type-check clean.

## Note — the ~/system editable-dep trap
`~/system` scenarios run against **editable installs** of sutradhara; a half-landed schema change
(model updated but migration/tests not, or vice versa) silently regresses harness runs with
`ModuleNotFoundError`/schema-drift. **Land this complete on `main` in one green commit** — model +
migration + set-on-clone + summary/CLI + tests together. Direct-to-main, commit at the green
milestone.
