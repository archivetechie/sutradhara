# Quickstart

A working local tour of Sutradhara in about ten minutes: install, create a
scratch catalog, prove the rebuildable-index principle against a fixture
tape backend, and walk one receive through registration. Every command
here mirrors an invocation the test suite runs (`tests/test_cli.py`,
`tests/test_receive_front_door.py`), so it works on a machine with no
tape library, no Remanence build, and no ffmpeg.

<!-- code-anchor: pyproject.toml uv.lock @ 5c44b85 -->
## Install

Requires Python ≥ 3.11 and [`uv`](https://docs.astral.sh/uv/).

```sh
git clone <repo> sutradhara && cd sutradhara
uv sync            # installs the workspace, incl. packages/sutradhara-receive
uv run pytest -q   # hermetic; 842 passed, 1 skipped (live-MinIO) as of 5c44b85
```

The CLI lands in the virtualenv as `.venv/bin/sutra`. The examples below
assume `.venv/bin` is on your `PATH` or you prefix each command with
`uv run`.

<!-- code-anchor: src/sutradhara/catalog/session.py src/sutradhara/cli/db.py src/sutradhara/cli/admin.py @ 5c44b85 -->
## A scratch catalog

Without `SUTRADHARA_DB_URL`, sutra writes `./sutradhara.db` in whatever
directory you happen to be in. Set it explicitly, always:

```sh
export SUTRADHARA_DB_URL=sqlite:////tmp/sutradhara-quickstart.db
sutra db init        # create-all; production uses `alembic upgrade head`
sutra admin doctor   # readiness report; WARNs are fine for this tour
```

`doctor` will warn that the `rem` binary and the key registry are missing
unless you have Remanence built. Nothing in this quickstart needs them.

<!-- code-anchor: src/sutradhara/cli/backends.py src/sutradhara/cli/scrub.py tests/test_cli.py @ 5c44b85 -->
## Rebuild a catalog from a backend

This is the system's founding claim — the catalog is derived state,
rebuildable by enumerating backends — and you can demonstrate it against
a JSON fixture that stands in for a Remanence tape library:

```sh
sutra backends add tape1 --kind rem_tape --fixture tests/fixtures/remanence_objects.json
sutra backends list
sutra scrub --backend tape1
sutra list assets
```

`scrub` enumerates the "tape", finds objects the (empty) catalog has
never heard of, and inserts them as assets with copies. Run it twice:
the second pass changes nothing. Delete the database file, `db init`,
scrub again — the catalog comes back. That round trip is exactly
`test_scrub_against_empty_catalog_populates_everything` and
`test_second_scrub_is_idempotent`.

<!-- code-anchor: src/sutradhara/cli/receive.py src/sutradhara/cli/intake.py tests/test_receive_front_door.py @ 5c44b85 -->
## Receive and register an intake

Make a source folder and a landing share, then receive:

```sh
mkdir -p /tmp/qs-source /tmp/qs-landing
echo "hello archive" > /tmp/qs-source/clip.txt

sutra receive --fake-source /tmp/qs-source \
    --landing /tmp/qs-landing \
    --source-kind card --operator "$USER" \
    --confirm-timeout 0 --json
```

`--fake-source` substitutes a directory for a physical card reader; a
real invocation passes the mounted source as the positional argument
instead (`sutra receive /media/CARD01 --landing ... --source-kind card`).
The JSON output includes the new intake id, and the landing directory now
holds a complete BagIt bag whose `intake.json` sentinel was written last.
`--confirm-timeout 0` skips polling a server for release confirmation, so
the exit code is 3 (release not confirmed) — expected when no server is
running.

Now cross the acceptance boundary:

```sh
sutra intake inspect /tmp/qs-landing --json      # read-only validation
sutra intake register <INTAKE_ID> --landing-root /tmp/qs-landing
sutra list assets
sutra jobs list
```

`register` creates the logical asset and ingest item rows and enqueues a
`cloud-blob` job for the intake's cloud-temp disaster-recovery copy. On
this scratch setup that job will sit `pending` (and fail if run) because
no `cloud-temp` backend exists; that is honest behavior, not breakage.
The test suite runs it with `SUTRADHARA_FAKE_CLOUD_BLOB=1`.

## Where the full loop needs real infrastructure

The remaining lifecycle — `sutra arrangement` and `sutra archive
submission flush`, restore, retention — is wired end to end but needs the
Remanence `rem` CLI (set `REM_BIN`), registered pools with an applied
artifactclass policy, and for encrypted placements a key registry
directory. See [`reference-config.md`](reference-config.md) for those
knobs and [`architecture-overview.md`](architecture-overview.md) for the
flow. The maintainer's end-to-end truth lives in the separate `~/system`
scenario harness (per `CLAUDE.md`), which drives this repo as an editable
dependency.

<!-- code-anchor: src/sutradhara/cli/admin.py src/sutradhara/jobs/worker_lock.py src/sutradhara/resource_control.py src/sutradhara/jobs/reconcilers/conditions.py @ 5c44b85 -->
## Troubleshooting

**"Remanence CLI not found."** Archive/restore/seal paths resolve the
`rem` binary as: `--rem-bin` flag, then `REM_BIN`, then `rem` on `PATH`,
then `~/remanence/target/release/rem`. Set `REM_BIN` and re-check with
`sutra admin doctor`.

**A `sutradhara.db` file appeared somewhere unexpected.** That is the
default `SUTRADHARA_DB_URL` (`sqlite:///./sutradhara.db`, relative to the
working directory). Export the variable in every shell and unit file.

**`sutra worker` refuses to start a second instance.** By design: an
`flock`-based singleton lock enforces one worker per database. For a
SQLite file URL the lock is `<database>.worker.lock` next to the database
file; for other URLs it lives under `$SUTRADHARA_STATE_DIR/worker-locks`
(or `~/.local/state/sutradhara/worker-locks`). The kernel releases the
lock when the holding process dies, so crashed workers leave nothing to
clean up; the error message names the holder PID.

**Log line "resource enforcement degraded".** systemd transient scopes
were unavailable (no user manager, restricted container), so subprocesses
run without cgroup fairness — with best-effort `nice`/`ionice` only. It
is logged once per process. Set
`SUTRADHARA_RESOURCE_CONTROL_REQUIRE=1` to make this fatal instead, or
`SUTRADHARA_RESOURCE_CONTROL=off` to silence it deliberately.

**Jobs requiring `tape_drive` or `gpu` never run.** Default worker
capacities set both pools to zero. Start the worker with explicit
capacity: `sutra worker --pools tape_drive=1`.

**A reconciler condition is `blocked`.** After three failed attempts a
condition escalates from `backoff` to `blocked` and waits for a human.
Inspect with `sutra reconcile DOMAIN --list-blocked`, fix the cause, then
`sutra reconcile DOMAIN --reopen-blocked` (optionally `--reason` to
scope). A recorded tool-version bump reopens matching blocked conditions
automatically on the next cycle.

**An intake was quarantined.** Validation failed before acceptance; the
landing directory holds a terminal marker and the intake row records the
reason. `sutra intake inspect <path> --json` shows what the validator
sees. Quarantine never touches already-registered intakes.

**`sutra receive` exited 3.** The bag landed fine; the source could not
be confirmed safe to release (no server confirmation). Exit 4 is
different: destination verification failed — check
`sutra receive verify-pending --landing <root>`.
