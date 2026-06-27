# Codex prompt — `ssh_disk` backend (encrypted LAN backup copy) + cloud_blob retry fix

> Design by Claude + the owner; implementation by codex. **Repo: `~/sutradhara/repo` (single repo).**
> Read `CLAUDE.md` + `AGENTS.md` first.
>
> **Decision (the owner, 2026-06-27):** the temporary DR/backup copy made at ingest will **no longer go to
> the cloud** — cloud upload of footage-scale data is infeasible. Instead it goes to a **LAN file
> server over rsync/SSH** (no NFS/SMB mount — a hung mount must never wedge the durability host; a
> bounded SSH push fails cleanly and retries). It stays a **temporary copy** (expires once the durable
> tape copies land, via the existing retention/deletion gate) and stays **encrypted** (the payload is
> already RAO-AEAD; the LAN box only ever holds ciphertext, the key never leaves the host registry).
>
> Two deliverables: **(A)** a new `ssh_disk` storage backend, and **(B)** a small idempotency fix to the
> existing `cloud-blob` handler (found running the pilot). The temp-copy handler and lifecycle are
> otherwise **reused unchanged** — only the backend the temp pool maps to differs.
>
> **Codex review (2026-06-27) folded in:** `put` does mkdir-**before**-rsync then atomic in-dir rename;
> the locator carries object identity only (`key`/`sha256`/`size`), resolving `host`/`root` from the
> `Backend` row at runtime; a key-containment validator (no `..`/abs/control chars); `read_range`
> seeks the slice rather than loading the whole blob; `RsyncSshTransport` takes an injectable `runner`
> with a command-construction test; a handler-path integration test; and explicit "adapter only —
> deployment switch is separate" scope. **Round 2 (2026-06-27):** key containment is **lexical posix**
> (`posixpath.normpath`, no local `resolve()` of a remote root); enumerate/verify return **`ContentHash`
> bytes** (`content_hash(bytes.fromhex(...))`), not hex; `ssh_options` validated via `_optional_str_list`;
> the transport runs **argv lists with `shell=False`** + `rsync --protect-args` and a spaces/quotes test;
> the cloud_blob fix is `unlink(missing_ok=True)` (preferred over the temp-dir restructure).
> **Round 3 (2026-06-27):** `verify` is fully defensive (never raises on missing/malformed hashes —
> mirrors `s3.py`); `sha256`/`stat` distinguish proven-absent (`None`) from real errors (raise, not
> mask as "absent"); key validation applies to **every** locator key + each enumerated `relpath` (not
> just writes); the `metadata=({"pool": pool} if pool else {})` pseudocode is valid Python; acceptance
> pins `uv run pytest -q` + a `docs/INDEX.md` update (AGENTS.md).

## What already exists — build on it, do not rebuild
- **Backend contract** (`src/sutradhara/backend/port.py`): `StorageBackend` Protocol
  (`name`, `enumerate() -> Iterator[CopyRecord]`, `read_range(locator, ByteRange) -> bytes`,
  `verify(locator) -> VerifyResult`) and `DeletableStorageBackend` (`delete_object(locator)` —
  **must treat an already-absent object as success**). `CopyRecord{logical_id, native_locator,
  integrity_hash, size_bytes, metadata}`, `ByteRange` (`is_whole_object` when `[0,0)`), error types
  `BackendNotFoundError` / `BackendUnavailableError`.
- **The closest analog to mirror: `src/sutradhara/backend/s3.py`** (`S3Backend`). Copy its shape:
  `write_object(source, *, key, pool=None) -> CopyRecord` (the `KeyedObjectWriter` interface the
  `cloud-blob` handler calls), `write_object_to_pool`, `read_range`, `verify`, `delete_object`,
  `enumerate`, and the **injectable client** (`client=None`) that makes it testable without a live
  service. Your backend mirrors all of this with an injectable **transport** instead of a boto3 client.
- **Factory** (`src/sutradhara/backend/factory.py`): `backend_from_row(row)` dispatches on
  `row.kind`; reads `row.config` (JSON dict); `_optional_str` helper; raises `BackendNotConfigured`.
- **The handler is already backend-agnostic** (`src/sutradhara/jobs/handlers/cloud_blob.py`): it does
  `backend = factory.backend_from_row(backend_row)` then `backend.write_object(blob_path, key=…,
  pool=…)`. **So no handler change is needed for the backend swap** — only the §B fix.
- **Enum** (`src/sutradhara/catalog/types.py::BackendKind`): has `rem_disk`, `plain_disk` (reserved,
  unbuilt) etc. Add a sibling for this.

## A. The `ssh_disk` backend
### A1. Enum + factory
- Add `SSH_DISK = "ssh_disk"` to `BackendKind`.
- In `backend_from_row`, add the branch (lazy import like s3):
  ```python
  if row.kind == BackendKind.SSH_DISK:
      from sutradhara.backend.ssh_disk import SshDiskBackend
      host = _optional_str(cfg, "host"); root = _optional_str(cfg, "root")
      if not host or not root:
          raise BackendNotConfigured(f"backend {row.name!r} (kind=ssh_disk) needs config.host and config.root")
      return SshDiskBackend(row.name, host=host, root=root,
          user=_optional_str(cfg, "user"), identity_file=_optional_str(cfg, "identity_file"),
          ssh_options=_optional_str_list(cfg, "ssh_options"))   # [] if absent; BackendNotConfigured on non-list-of-str
  ```
  Fix the now-stale factory docstring (s3 is implemented; add ssh_disk). Add an `_optional_str_list`
  helper beside `_optional_str` — returns `[]` when absent, raises `BackendNotConfigured` if present but
  not a list of strings (so a bad string/dict/JSON value fails at config time, not at command build).

### A2. `src/sutradhara/backend/ssh_disk.py`
A filesystem-over-SSH object store rooted at `config.root` on `config.host`. **Transport is injectable**
(the load-bearing testability decision):

```python
class RemoteTransport(Protocol):
    def put(self, local: Path, relpath: str) -> None: ...     # atomic: stage <relpath>.partial then rename; mkdir -p parents
    def get(self, relpath: str, local: Path) -> None: ...      # raises FileNotFoundError if absent
    def sha256(self, relpath: str) -> str | None: ...          # hex; None if the file is absent (remote hashing — no download)
    def size(self, relpath: str) -> int | None: ...            # None if absent
    def remove(self, relpath: str) -> None: ...                # idempotent (missing is OK)
    def list_files(self) -> Iterator[str]: ...                 # relpaths under root
```

- **Default `RsyncSshTransport(host, root, user, identity_file, ssh_options, *, runner=None)`** — real
  subprocess (default `runner` shells `subprocess.run` with a timeout; **inject a fake `runner` in
  tests** to assert command construction without a live server):
  - `put` (order matters) = **(1)** `ssh … "mkdir -p <root>/<dir>"` — create the parent dir **FIRST**
    (for a key like `intakes/<id>.rao` the `intakes/` dir must exist before rsync writes into it);
    **(2)** `rsync -a --partial <local> [user@]host:<root>/<relpath>.partial` (with `-e "ssh -i …
    <ssh_options>"`); **(3)** `ssh … "mv -f <root>/<relpath>.partial <root>/<relpath>"` — atomic rename
    **within the same directory**, so a re-write is idempotent (no "already exists").
  - `get` = `rsync -a [user@]host:<root>/<relpath> <local>`.
  - `sha256` = `ssh … "sha256sum <root>/<relpath>"` → first field. **Distinguish absent from error:**
    only a *proven-missing* file → `None` (remote stderr "No such file", or gate on a `test -e` probe);
    any other nonzero is NOT "absent" — exit 255 → `BackendUnavailableError`, other nonzero → a backend
    error carrying stderr (so permission / missing-command / disk errors aren't masked as absence).
  - `size` = `ssh … "stat -c %s <root>/<relpath>"` — same absent-vs-error rule (proven-missing → `None`,
    else raise).
  - `remove` = `ssh … "rm -f <root>/<relpath>"`.
  - `list_files` = `ssh … "find <root> -type f ! -name '*.partial' -printf '%P\n'"`.
  - **Execution discipline:** run every command **locally as an argv list — `subprocess.run(...,
    shell=False)`** (no local shell). Only the *remote* command string handed to `ssh` (the
    `mkdir`/`mv`/`sha256sum`/`stat`/`rm`/`find` line the **remote** shell runs) is **`shlex.quote`d**;
    for `rsync`, use **`--protect-args` (`-s`)** so remote paths with spaces/quotes aren't re-split by
    the remote shell. Pass `-o ConnectTimeout=…` + `-o BatchMode=yes` + a `subprocess` timeout. Map an
    SSH/rsync connection failure (exit 255) to **`BackendUnavailableError`**, a genuinely-missing
    object to **`BackendNotFoundError`**.
- **`SshDiskBackend`** wraps a transport (default real, tests inject a fake):
  - **Key containment (security) — LEXICAL only (`root` is remote):** every `key` maps to a path under
    the *remote* `root`, so validate it with **`posixpath`/`PurePosixPath` lexical rules only — do NOT
    call `Path(root).resolve()` (root isn't on this machine). Reject: absolute keys / a leading `/`,
    backslashes, NUL & control chars, empty or `.`-only segments, and any key whose
    `posixpath.normpath(key)` is absolute or starts with `..` (escapes the root). Raise on an unsafe
    key; the transport then joins the validated relative key to the remote `root`. **Apply this
    validation to EVERY `key` that reaches the transport** — `write_object`'s `key`, `locator["key"]` in
    `read_range`/`verify`/`delete_object`, and each `relpath` from `list_files()` before yielding a
    `CopyRecord` (a corrupt locator or a hostile remote listing must not escape `root`). (The
    `cloud-blob` key `intakes/<intake>.rao` is controlled, but the backend must be safe for any key.)
  - `write_object(source, *, key, pool=None) -> CopyRecord`: validate `key` (above); `digest =
    content_hash(sha256_file(source))` (a `ContentHash`); `transport.put(source, key)`; return `CopyRecord(logical_id=digest,
    native_locator={"key": key, "sha256": digest.hex(), "size_bytes": source.stat().st_size},
    integrity_hash=digest, size_bytes=source.stat().st_size, metadata=({"pool": pool} if pool else {}))`.
    **The locator carries per-object identity ONLY (`key`/`sha256`/`size`) — NOT `host`/`root`** (those
    are backend config; `read_range`/`verify`/`delete_object` resolve them from the backend instance
    built from the current `Backend` row, so old locators survive a root move / row update). Idempotent
    (atomic-rename overwrite).
  - `write_object_to_pool(source, pool)`: `key = f"{pool.strip('/')}/{source.name}"` → `write_object`.
  - `read_range(locator, byte_range)`: validate `locator["key"]`; `transport.get(locator["key"], tmp)`; whole-object → return the
    file's bytes; for a non-whole range **`seek(start)` + `read(length)` from the temp file** (do NOT
    `read_bytes()` the whole file into memory then slice — footage-scale blobs are large). The fetch
    still downloads the whole object (a true `dd`-range over ssh is an optional later optimization), so
    only the requested slice is held in memory. Absent → `BackendNotFoundError`. Clean up the temp file.
  - `verify(locator)`: validate `locator["key"]`; `hex = transport.sha256(locator["key"])` (remote
    hashing — no download); absent → `VerifyResult(ok=False, detail="absent")`. **Defensive (mirror
    `s3.py`): never raise** — if `hex` or `locator["sha256"]` is missing / non-string / not valid hex,
    return `VerifyResult(ok=False, detail="invalid hash")` (wrap `bytes.fromhex` in try/except
    `ValueError`). Else `actual = content_hash(bytes.fromhex(hex))` (a **`ContentHash`** per `port.py`);
    compare to `content_hash(bytes.fromhex(locator["sha256"]))`; mismatch → `VerifyResult(ok=False,
    actual_hash=actual, detail=…)`. **Never raise on mismatch or malformed input** (contract).
  - `delete_object(locator)`: validate `locator["key"]`; `transport.remove(locator["key"])` (idempotent).
  - `enumerate()`: for each `relpath` in `transport.list_files()`, **skip any `relpath` that fails key
    validation** (don't trust the remote listing), then `hex = transport.sha256(relpath)`,
    `size = transport.size(relpath)`; skip if either is `None` or `hex` is not valid hex. Yield
    `CopyRecord(logical_id=content_hash(bytes.fromhex(hex)), integrity_hash=content_hash(bytes.fromhex(
    hex)), native_locator={"key": relpath, "sha256": hex, "size_bytes": size}, size_bytes=size)`.
    **`logical_id`/`integrity_hash` are `ContentHash` bytes (`content_hash(bytes.fromhex(...))`), never
    raw hex** — same as `s3.py`. (Scrub-time; per-file remote hash is acceptable.)

## B. Fix the `cloud-blob` retry idempotency bug
Found in the pilot: `cloud-blob` builds the RAO to a **persistent** path
`cache_root/intakes/<intake>/cloud/<intake>.rao` (`_build_cloud_blob(..., destination=blob_path)`),
then uploads. If the upload fails (e.g. a transient transport error) the `.rao` is left behind, and the
retry's `rem archive build --out <that path>` aborts with **`--out … already exists`** (real failure
trace from the pilot run). Fix in `src/sutradhara/jobs/handlers/cloud_blob.py`:
- Make the build idempotent on retry. **Preferred (simplest, no signature change):**
  `destination.unlink(missing_ok=True)` immediately before `run_rem_archive_build`, on **both** the real
  and the `SUTRADHARA_FAKE_CLOUD_BLOB` paths. *(The temp-dir alternative — build inside a per-attempt
  `tempfile.TemporaryDirectory` and upload from there — is cleaner about cache litter but requires
  changing `_build_cloud_blob` to return `(blob_path, digest)` from inside the temp context; only do that
  if you also want to stop leaving the blob in cache.)*

## Tests
- **`tests/test_ssh_disk_backend.py`** (mirror `tests/test_s3_backend.py`), using a
  **`LocalDirTransport(root: Path)`** fake (put/get/sha256/size/remove/list_files against a local dir,
  parents auto-created) injected into `SshDiskBackend` — so **no real SSH server is needed**:
  - write → read (whole-object) round-trips the exact bytes; `CopyRecord` has the right
    digest/size/locator.
  - **re-write is idempotent** (write the same key twice → no error, object intact).
  - `verify` ok on match; `ok=False` on a corrupted/absent object (no raise).
  - `read_range` of an absent key → `BackendNotFoundError`.
  - `delete_object` removes it; **deleting an absent object is success** (no raise).
  - `enumerate` yields one `CopyRecord` per stored file with the right digests.
  - factory: a `Backend(kind=ssh_disk, config={host,root})` row → `SshDiskBackend`; missing
    `host`/`root` → `BackendNotConfigured`.
- **`cloud-blob` retry idempotency** (extend `tests/test_ingest_handlers.py` or `test_jobs.py`):
  monkeypatch `run_rem_archive_build` with a fake that **raises if `output_path` already exists**
  (mimicking rem), pre-create the stale `<intake>.rao`, run the handler, and assert it **succeeds**
  (the fix avoided/removed the stale artifact) rather than raising "already exists".
- **Handler-path integration (proves the adapter actually serves the temp copy):** run the existing
  `cloud-blob` handler against a `Backend` row of **kind `ssh_disk`** (named `cloud-temp`) with the
  ssh_disk transport overridden to a `LocalDirTransport` (monkeypatch the `ssh_disk` module's default
  transport, or `factory.backend_from_row`) and `SUTRADHARA_FAKE_CLOUD_BLOB=1` to skip real rem —
  assert the blob lands in the local "remote" dir and a `Copy` row is recorded through the **unchanged**
  handler. Proves `factory.backend_from_row(kind=ssh_disk)` → `write_object` works end-to-end via the
  temp-copy path, without a live SSH server.
- **`RsyncSshTransport` command construction** (inject a fake `runner`): assert every call is an **argv
  list run with `shell=False`** (no local shell); **mkdir issues before rsync**; rsync uses `--partial`
  + `--protect-args` + `-e ssh …`; ssh carries `-o BatchMode=yes` + `-o ConnectTimeout=…`; the **remote**
  command string is `shlex.quote`d; `list_files` excludes `*.partial`; exit code 255 →
  `BackendUnavailableError` (a missing file on `get`/`sha256` → `None` / `BackendNotFoundError`). **Use a
  `key` and `root` containing spaces and a quote** so quoting/`--protect-args` is exercised, not
  theoretical. This guards the real transport that the `LocalDirTransport` tests don't exercise.
- **Key containment** — unsafe keys (`../x`, `/abs`, empty/`.` segments, control chars) are rejected
  before any transport call, **on writes AND on a hostile `locator["key"]`** passed to
  `read_range`/`verify`/`delete_object` (assert they reject rather than escape `root`).
- **`verify` is defensive** — a malformed/missing `locator["sha256"]` or bad remote hash returns
  `VerifyResult(ok=False, …)`, never raises.
- `uv run pytest -q` green; `uv run ruff check` clean (per AGENTS.md).

## Acceptance
- `src/sutradhara/backend/ssh_disk.py` implements `StorageBackend` + `DeletableStorageBackend` +
  `write_object`/`write_object_to_pool`, transport injected (default real rsync/ssh).
- `BackendKind.SSH_DISK` + factory branch land; `backend_from_row` builds it from `{host, root, …}`.
- `cloud-blob` build is idempotent on retry (no "--out already exists").
- New + existing tests green, ruff clean. **No schema/migration change** (it's a backend adapter +
  catalog config; the `Copy`/`Backend` tables already carry JSON locator/config).
- **Per AGENTS.md:** run `uv run pytest -q`, and **update `docs/INDEX.md`** to mark this prompt
  `implemented` when done.
- **Scope is explicit:** this lands the **generic `ssh_disk` adapter + the cloud_blob fix + a
  handler-path proof** — it does **not** switch any deployment from MinIO/S3 to LAN SSH. That switch is
  pointing the `cloud-temp` `Backend` row at kind `ssh_disk` (registration still defaults to
  backend/pool `cloud-temp`), a separate catalog/bringup step.

## Out of scope (do NOT do here)
- **No catalog/bringup wiring, and no deployment switch.** Switching the temp copy from MinIO/S3 to LAN
  SSH = pointing the existing `cloud-temp` `Backend` row at kind `ssh_disk` + config `{host, root, …}`
  (registration still defaults to backend/pool `cloud-temp` — `intake.py` `accept` + the enqueue
  params — so flipping that one row's kind+config is the whole switch). That catalog/bringup change is a
  separate deployment step, not this prompt. Keep the adapter generic.
- **No rename of the `cloud-blob` handler / `cloud-temp` pool** — functional swap only; the scenario
  harness keeps its MinIO `cloud-temp` (s3) for tests. A cosmetic rename (cloud→backup) is a later
  cleanup with test blast-radius.
- **No `plain_disk` (local-mount) adapter** — the owner chose SSH transfer over a mount; `plain_disk`
  stays reserved-unbuilt.
- **No true byte-range-over-ssh `read_range`** — whole-object fetch is correct for this DR copy.
- No retention/lifecycle change — the existing deletion gate already expires the temp copy via
  `delete_object`, which `ssh_disk` now satisfies.
