# Prompt RM0.1 — resource-safe streaming read capability (sutradhara)

**Status:** pending (gpt-5.6-sol). First milestone of streaming restore (RM0). sutradhara-only.
**Normative (read FIRST, binding — do NOT inline):** `docs/design-streaming-restore.md` v0.2 — §3.1
(port capability), §3.5 (bounded-memory contract), §5-A (session lifetime), **§8-B2** (context-manager
+ cancel-before-close — the correctness core), **§8-M1** (fallback honesty), and RM0.1 in §8.1.
**Also read the real code you are changing:** `src/sutradhara/backend/port.py` (the `StorageBackend`
Protocol + the `DeletableStorageBackend` optional-capability pattern ~:127), `backend/remanence.py`
(`RemanenceReadSession` ~:275/562/596/604/608 — the `b"".join` and the `__exit__`/`close`),
`backend/s3.py:106-125` (`.read()` collapsing `iter_chunks`), `backend/d2tape.py`, `backend/ssh_disk.py`,
`backend/memory.py`.

## Scope — add a genuinely-lazy streaming read capability; do NOT change `read_range`
1. **Port capability (§3.1):** add a runtime-checkable Protocol `StreamingStorageBackend(StorageBackend)`
   with `open_range_chunks(locator, byte_range, *, chunk_bytes) -> ContextManager[Iterator[bytes]]`
   (NOT a bare iterator — see §8-B2). Follow the existing optional-capability pattern
   (`DeletableStorageBackend`, port.py:127). Add a capability descriptor enum
   `StreamKind = native_stream | scratch_stream | memory_buffered` a caller can query — NOT a Boolean
   that conflates lazy vs materialized.
2. **Remanence backend (the native, load-bearing one):** implement `open_range_chunks` that yields the
   `ReadObjectRange` gRPC stream's `chunk.data` lazily (undo `b"".join`, remanence.py:604) **as a
   context manager that owns the session**. Binding correctness (§8-B2):
   - Retain the gRPC **call object**, not just its Python iterator.
   - The context `__enter__` opens the read session + issues the ranged read; `__exit__`/`finally`
     **cancels the gRPC call FIRST** (so the server receiver drops and the blocking producer send gets
     `BrokenPipe`/RST) **THEN** sends `CloseReadSession`. Reversing this order can DEADLOCK: the bounded
     stream channel + blocking producer + `CloseReadSession` queued behind the ranged-read on the same
     drive actor means an abandoned-but-alive stream wedges the close.
   - Close is **unconditional** on success, error, exception, cancellation, or early consumer exit.
     Reports `StreamKind.native_stream`.
3. **S3 backend:** `open_range_chunks` yields `response["Body"].iter_chunks(chunk_bytes)` (undo the
   `.read()` at s3.py:117); real HTTP Range; `native_stream`; close the body on exit.
4. **Materialized fallback — explicit, NOT dressed up as streaming (§8-M1):** for d2tape / ssh_disk /
   memory, provide a SEPARATELY-NAMED helper `open_materialized_range_chunks(...)` that streams from the
   backend's existing temp/scratch file (kept alive for the context) — reports `scratch_stream` (d2tape,
   ssh_disk) or `memory_buffered` (memory). These are honestly whole-object-to-scratch-before-delivery;
   do NOT claim bounded/lazy for them. Do not add `StreamingStorageBackend` to these backends.

## Binding invariants
- `read_range(...) -> bytes` is UNCHANGED (all existing callers keep working; this is additive).
- Drain-or-close is **structural** (the context manager owns cleanup), never caller etiquette.
- No runtime/compat flag; git revert is the backout (pre-production). Wrap the existing gRPC/session
  machinery — do NOT fork a second read path.
- No stage buffers a whole object on the native path.

## Tests (§8.1 RM0.1 row)
- **lazy first-byte:** the first chunk is delivered before the whole object is read (assert via a
  fixture/mock that counts server-yielded chunks vs consumed).
- **bounded RSS:** consuming a large fixture holds ≤ a few chunks, not the whole object.
- **early close:** a consumer that abandons the stream after N chunks ⇒ the context closes cleanly,
  the gRPC call is cancelled, `CloseReadSession` completes, no session leak, no hang (bounded, not a
  timeout).
- **exception close:** an exception mid-consumption ⇒ same clean teardown.
- **server-side cancellation / no `CloseReadSession` wedge:** the cancel-before-close ordering actually
  unblocks a producer that was blocked on a full stream channel (this is the §8-B2 deadlock guard —
  make the test drive the abandoned-but-alive case and assert the close does not wedge).
- capability query returns the right `StreamKind` per backend; the materialized helper is not mistaken
  for native.

## Definition of done (this repo's AGENTS.md)
`uv run pytest` green + format + type-check. Summary: files touched, each test → the §8.1 behavior it
covers, and an explicit statement that `read_range` is unchanged and the remanence teardown cancels the
gRPC call before `CloseReadSession`. **Verification member:** the early-close / no-wedge test on a real
(or faithfully-mocked) bounded stream channel. Do NOT `#[ignore]`/skip the teardown tests.
