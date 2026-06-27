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
          ssh_options=cfg.get("ssh_options") or [])
  ```
  Fix the now-stale factory docstring (s3 is implemented; add ssh_disk).

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

- **Default `RsyncSshTransport(host, root, user, identity_file, ssh_options)`** — real subprocess:
  - `put` = `rsync -a --partial <local> [user@]host:<root>/<relpath>.partial` (with `-e "ssh -i …
    <ssh_options>"`), then `ssh … "mkdir -p <dir> && mv -f <root>/<relpath>.partial <root>/<relpath>"`
    (atomic overwrite — so a re-write is idempotent, no "already exists").
  - `get` = `rsync -a [user@]host:<root>/<relpath> <local>`.
  - `sha256` = `ssh … "sha256sum <root>/<relpath>"` → first field; absent file (nonzero) → `None`.
  - `size` = `ssh … "stat -c %s <root>/<relpath>"`; absent → `None`.
  - `remove` = `ssh … "rm -f <root>/<relpath>"`.
  - `list_files` = `ssh … "find <root> -type f ! -name '*.partial' -printf '%P\n'"`.
  - **Shell-quote every remote path** (`shlex.quote`), pass a connection **timeout**
    (`-o ConnectTimeout=…`, `-o BatchMode=yes`), and map an SSH connection failure
    (exit 255 / rsync 255) to **`BackendUnavailableError`**, a genuinely-missing object to
    **`BackendNotFoundError`**.
- **`SshDiskBackend`** wraps a transport (default real, tests inject a fake):
  - `write_object(source, *, key, pool=None) -> CopyRecord`: `digest = sha256(source)`;
    `transport.put(source, key)`; return `CopyRecord(logical_id=digest,
    native_locator={"host": host, "root": root, "key": key, "sha256": digest.hex()},
    integrity_hash=digest, size_bytes=source.stat().st_size, metadata={"pool": pool} if pool)`.
    Idempotent (atomic-rename overwrite).
  - `write_object_to_pool(source, pool)`: `key = f"{pool.strip('/')}/{source.name}"` → `write_object`.
  - `read_range(locator, byte_range)`: `transport.get(locator["key"], tmp)`; whole-object → return all
    bytes; else return `data[start:end]`. **Whole-object fetch is fine** — this DR copy is restored
    whole, never partially (a true `dd`-range over ssh is an optional later optimization). Absent →
    `BackendNotFoundError`. Clean up the temp file.
  - `verify(locator)`: `actual = transport.sha256(locator["key"])`; compare to `locator["sha256"]`
    (remote hashing — no download). Absent or mismatch → `VerifyResult(ok=False, …)`; **never raise on
    mismatch** (contract).
  - `delete_object(locator)`: `transport.remove(locator["key"])` (idempotent).
  - `enumerate()`: for each `relpath` in `transport.list_files()`, `digest = transport.sha256(relpath)`,
    `size = transport.size(relpath)` → yield `CopyRecord`. (Scrub-time; per-file remote hash is
    acceptable. Skip files whose sha/size come back `None`.)

## B. Fix the `cloud-blob` retry idempotency bug
Found in the pilot: `cloud-blob` builds the RAO to a **persistent** path
`cache_root/intakes/<intake>/cloud/<intake>.rao` (`_build_cloud_blob(..., destination=blob_path)`),
then uploads. If the upload fails (e.g. a transient transport error) the `.rao` is left behind, and the
retry's `rem archive build --out <that path>` aborts with **`--out … already exists`** (real failure
trace from the pilot run). Fix in `src/sutradhara/jobs/handlers/cloud_blob.py`:
- Make the build idempotent on retry — **either** build the blob inside a per-attempt
  `tempfile.TemporaryDirectory` (preferred — no stale cache artifact ever; upload from there), **or**
  `destination.unlink(missing_ok=True)` immediately before `run_rem_archive_build`. Apply to both the
  real and the `SUTRADHARA_FAKE_CLOUD_BLOB` paths.

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
- `uv run pytest` green; `uv run ruff check` clean.

## Acceptance
- `src/sutradhara/backend/ssh_disk.py` implements `StorageBackend` + `DeletableStorageBackend` +
  `write_object`/`write_object_to_pool`, transport injected (default real rsync/ssh).
- `BackendKind.SSH_DISK` + factory branch land; `backend_from_row` builds it from `{host, root, …}`.
- `cloud-blob` build is idempotent on retry (no "--out already exists").
- New + existing tests green, ruff clean. **No schema/migration change** (it's a backend adapter +
  catalog config; the `Copy`/`Backend` tables already carry JSON locator/config).

## Out of scope (do NOT do here)
- **No catalog/bringup wiring of an actual `lan-backup` backend+pool** — that's a deployment step
  (a `Backend(kind=ssh_disk, config={host,root})` row + a temp pool the register-time copy points at),
  done by the operator/bringup, not this prompt. Keep the adapter generic.
- **No rename of the `cloud-blob` handler / `cloud-temp` pool** — functional swap only; the scenario
  harness keeps its MinIO `cloud-temp` (s3) for tests. A cosmetic rename (cloud→backup) is a later
  cleanup with test blast-radius.
- **No `plain_disk` (local-mount) adapter** — the owner chose SSH transfer over a mount; `plain_disk`
  stays reserved-unbuilt.
- **No true byte-range-over-ssh `read_range`** — whole-object fetch is correct for this DR copy.
- No retention/lifecycle change — the existing deletion gate already expires the temp copy via
  `delete_object`, which `ssh_disk` now satisfies.
