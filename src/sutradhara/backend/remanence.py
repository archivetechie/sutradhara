"""Remanence Layer 5 adapter.

Implements the `StorageBackend` Protocol against Remanence's Layer 5 gRPC
contract (proto/layer5.proto).

Fixture construction remains available for tests and dev-only scrubs. The live
constructor, `from_grpc()`, opens a Remanence daemon Catalog channel and maps
`ObjectRecord`/`ObjectCopy` messages into Sutradhara `CopyRecord`s. Object byte
reads use `ReadSessionService.ReadObjectRange`; Catalog metadata is never used
as a byte source.

Sutradhara's object-level model: one logical asset = one Remanence object
(content_sha256 == rem-tar manifest_sha256, per spec-v0.1.md §4.1 and
remanence/spec-v0.4.md §8.6.5). File-level (per-pax-entry) addressing
lands in a later slice; for day-1 every copy is whole-object.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
from collections.abc import Iterator
from types import TracebackType
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast

import grpc
from google.protobuf.empty_pb2 import Empty

from sutradhara._proto import layer5_pb2, layer5_pb2_grpc
from sutradhara.backend.port import (
    BackendError,
    BackendLocator,
    BackendNotFoundError,
    BackendSessionInvalidatedError,
    BackendTransientError,
    BackendUnavailableError,
    ByteRange,
    CopyRecord,
    VerifyResult,
)
from sutradhara.catalog.types import ContentHash, content_hash

_WRITE_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True)
class _RemanenceObjectCopy:
    """One Remanence ObjectCopy entry (proto/layer5.proto: ObjectCopy)."""

    tape_uuid: bytes
    tape_file_number: int
    first_body_lba: int
    pool_id: str
    health: str
    last_verified_at: str | None


@dataclass(frozen=True)
class _RemanenceObject:
    """One Remanence ObjectRecord (proto/layer5.proto: ObjectRecord).

    Only the fields Sutradhara's day-1 slice consumes — the proto has more,
    add them when needed.
    """

    object_id: bytes
    caller_object_id: str
    content_sha256: ContentHash
    size_bytes: int
    body_format: str
    caller_metadata: dict[str, str]
    copies: tuple[_RemanenceObjectCopy, ...]
    # Fixture-mode only: the actual bytes, so read_range can slice. In gRPC
    # mode this is None and reads go to Remanence.
    content: bytes | None = None


class _CatalogClient(Protocol):
    def EnumerateObjects(
        self, request: layer5_pb2.EnumerateObjectsRequest
    ) -> Iterator[layer5_pb2.ObjectRecord]: ...

    def GetObject(self, request: layer5_pb2.GetObjectRequest) -> layer5_pb2.ObjectRecord: ...

    def ListTapePools(
        self, request: layer5_pb2.ListTapePoolsRequest
    ) -> layer5_pb2.ListTapePoolsResponse: ...

    def GetFile(self, request: layer5_pb2.GetFileRequest) -> layer5_pb2.FileRecord: ...


class _WriteSessionClient(Protocol):
    def OpenWriteSession(
        self, request: layer5_pb2.OpenWriteSessionRequest
    ) -> layer5_pb2.WriteSession: ...

    def AppendObject(
        self, request_iterator: Iterator[layer5_pb2.AppendObjectMessage]
    ) -> layer5_pb2.ObjectRecord: ...

    def CloseWriteSession(
        self, request: layer5_pb2.CloseWriteSessionRequest
    ) -> layer5_pb2.WriteSession: ...

    def AbortWriteSession(
        self, request: layer5_pb2.AbortWriteSessionRequest
    ) -> layer5_pb2.WriteSession: ...


class _ReadSessionClient(Protocol):
    def OpenReadSession(
        self, request: layer5_pb2.OpenReadSessionRequest
    ) -> layer5_pb2.ReadSession: ...

    def ReadObjectRange(
        self, request: layer5_pb2.ReadObjectRangeRequest
    ) -> Iterator[layer5_pb2.BytesChunk]: ...

    def CloseReadSession(
        self, request: layer5_pb2.CloseReadSessionRequest
    ) -> layer5_pb2.ReadSession: ...


class RemanenceBackend:
    """Adapter implementing `StorageBackend` over a Remanence Layer 5 contract.

    Construct via `from_fixture_file()` (dev fixture), `from_objects()` (tests),
    or `from_grpc()` (live daemon Catalog).
    """

    def __init__(
        self,
        name: str,
        objects: list[_RemanenceObject] | None = None,
        *,
        endpoint: str | None = None,
        catalog: _CatalogClient | None = None,
        write_session: _WriteSessionClient | None = None,
        read_session: _ReadSessionClient | None = None,
        channel: grpc.Channel | None = None,
    ) -> None:
        self._name = name
        self._endpoint = endpoint
        self._catalog = catalog
        self._write_session = write_session
        self._read_session = read_session
        self._channel = channel
        self._objects = objects or []
        # Index by (tape_uuid, tape_file_number) for fast read_range lookup.
        # Live mode (endpoint set) holds no fixture objects, so the index is empty.
        self._by_locator: dict[tuple[bytes, int], _RemanenceObject] = {}
        for obj in self._objects:
            for cp in obj.copies:
                key = (cp.tape_uuid, cp.tape_file_number)
                if key in self._by_locator:
                    raise ValueError(
                        f"duplicate (tape_uuid, tape_file_number) in fixture: "
                        f"{cp.tape_uuid.hex()}/{cp.tape_file_number}"
                    )
                self._by_locator[key] = obj

    @property
    def name(self) -> str:
        return self._name

    @property
    def has_live_catalog(self) -> bool:
        """Return whether Catalog RPCs are available for live metadata checks."""

        return self._catalog is not None

    # --- constructors ----------------------------------------------------

    @classmethod
    def from_fixture_file(cls, name: str, path: Path | str) -> RemanenceBackend:
        """Load a JSON fixture of ObjectRecord-shaped dicts."""
        path = Path(path)
        raw = json.loads(path.read_text())
        return cls.from_object_dicts(name, raw)

    @classmethod
    def from_object_dicts(cls, name: str, dicts: list[dict[str, Any]]) -> RemanenceBackend:
        """Construct from in-memory dicts. Convenient for tests."""
        return cls(name, [_object_from_dict(d) for d in dicts])

    @classmethod
    def from_objects(cls, name: str, objects: list[_RemanenceObject]) -> RemanenceBackend:
        return cls(name, list(objects))

    @classmethod
    def from_grpc(cls, name: str, endpoint: str | None = None) -> RemanenceBackend:
        """Construct a live adapter targeting a Remanence daemon Catalog.

        Prefer `from_grpc(name, endpoint)`. `from_grpc(endpoint)` is accepted for
        callers that do not have an operator-visible backend name at hand.
        """
        if endpoint is None:
            endpoint = name
            name = "remanence"
        channel = grpc.insecure_channel(
            _grpc_target(endpoint),
            options=_grpc_channel_options(endpoint),
        )
        catalog = cast(
            _CatalogClient,
            layer5_pb2_grpc.CatalogStub(channel),  # type: ignore[no-untyped-call]
        )
        write_session = cast(
            _WriteSessionClient,
            layer5_pb2_grpc.WriteSessionServiceStub(channel),  # type: ignore[no-untyped-call]
        )
        read_session = cast(
            _ReadSessionClient,
            layer5_pb2_grpc.ReadSessionServiceStub(channel),  # type: ignore[no-untyped-call]
        )
        return cls(
            name,
            endpoint=endpoint,
            catalog=catalog,
            write_session=write_session,
            read_session=read_session,
            channel=channel,
        )

    # --- StorageBackend protocol -----------------------------------------

    def enumerate(self) -> Iterator[CopyRecord]:
        if self._catalog is not None:
            return self._enumerate_grpc()
        return self._enumerate_fixture()

    def _enumerate_grpc(self) -> Iterator[CopyRecord]:
        catalog = self._require_catalog()
        request = layer5_pb2.EnumerateObjectsRequest(all=Empty())
        try:
            for obj in catalog.EnumerateObjects(request):
                for cp in obj.copies:
                    yield _copy_record_from_proto(obj, cp)
        except grpc.RpcError as e:
            raise BackendUnavailableError(
                f"Remanence Catalog.EnumerateObjects at {self._endpoint!r} failed: "
                f"{_rpc_error_text(e)}"
            ) from e

    def _enumerate_fixture(self) -> Iterator[CopyRecord]:
        for obj in self._objects:
            for cp in obj.copies:
                yield CopyRecord(
                    logical_id=obj.content_sha256,
                    native_locator={
                        "tape_uuid": cp.tape_uuid.hex(),
                        "tape_file_number": cp.tape_file_number,
                        "first_body_lba": cp.first_body_lba,
                        "object_id": obj.object_id.hex(),
                        "caller_object_id": obj.caller_object_id,
                        "content_sha256": obj.content_sha256.hex(),
                        "pool_id": cp.pool_id,
                        "body_format": obj.body_format,
                    },
                    integrity_hash=obj.content_sha256,
                    size_bytes=obj.size_bytes,
                    metadata={
                        "body_format": obj.body_format,
                        "caller_object_id": obj.caller_object_id,
                        "health": cp.health,
                        "last_verified_at": cp.last_verified_at or "",
                        **{f"caller_meta:{k}": v for k, v in obj.caller_metadata.items()},
                    },
                )

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        if self._read_session is not None:
            with self.open_read_session(locator) as reader:
                return reader.read_range(byte_range)
        obj = self._object_for_locator(locator)
        if obj.content is None:
            raise BackendNotFoundError(
                "RemanenceBackend fixture mode has no bytes for this object; "
                "fixture is missing 'content_b64' / 'content_hex'"
            )
        if byte_range.is_whole_object:
            return obj.content
        if byte_range.end > len(obj.content):
            raise ValueError(
                f"byte range end {byte_range.end} exceeds object size {len(obj.content)}"
            )
        return obj.content[byte_range.start : byte_range.end]

    def open_read_session(self, locator: BackendLocator) -> RemanenceReadSession:
        """Open one reusable Remanence read session for a copy locator."""

        if self._read_session is None:
            return RemanenceReadSession.fixture(self, locator)
        client = self._require_read_session()
        tape_uuid = _uuid_bytes_from_locator(locator, "tape_uuid")
        object_id = _uuid_bytes_from_locator(locator, "object_id")
        try:
            session = client.OpenReadSession(
                layer5_pb2.OpenReadSessionRequest(
                    tape_target=layer5_pb2.TapeTarget(
                        tape_uuid=tape_uuid,
                        mount_if_needed=True,
                    )
                )
            )
        except grpc.RpcError as e:
            raise BackendUnavailableError(
                f"Remanence OpenReadSession at {self._endpoint!r} failed: {_rpc_error_text(e)}"
            ) from e
        return RemanenceReadSession.live(
            backend=self,
            client=client,
            session_id=session.session_id,
            object_id=object_id,
        )

    def get_file(self, locator: BackendLocator, *, path: str) -> layer5_pb2.FileRecord:
        """Return one Remanence file catalog row for a stored object path."""

        object_id = _uuid_bytes_from_locator(locator, "object_id")
        if self._catalog is not None:
            try:
                return self._catalog.GetFile(
                    layer5_pb2.GetFileRequest(
                        object_id=object_id,
                        path=path,
                    )
                )
            except grpc.RpcError as e:
                raise BackendUnavailableError(
                    f"Remanence Catalog.GetFile at {self._endpoint!r} failed: "
                    f"{_rpc_error_text(e)}"
                ) from e
        obj = self._object_for_locator(locator)
        if obj.content is None:
            raise BackendNotFoundError("fixture object has no bytes for file size cross-check")
        return layer5_pb2.FileRecord(
            object_id=obj.object_id,
            path=path,
            size_bytes=len(obj.content),
            first_chunk_body_lba=0,
            chunk_count=0,
        )

    def verify(self, locator: BackendLocator) -> VerifyResult:
        if self._read_session is not None:
            data = self.read_range(locator, ByteRange(0, 0))
            actual = content_hash(hashlib.sha256(data).digest())
            expected = _hash_from_locator_required(locator)
            if actual == expected:
                return VerifyResult(ok=True, actual_hash=actual)
            return VerifyResult(
                ok=False,
                actual_hash=actual,
                detail=f"expected {expected.hex()[:12]}…, got {actual.hex()[:12]}…",
            )
        obj = self._object_for_locator(locator)
        if obj.content is None:
            # Fixture without bytes: trust the declared hash.
            return VerifyResult(
                ok=True,
                actual_hash=obj.content_sha256,
                detail="fixture mode: bytes not available; declared hash assumed",
            )
        actual = content_hash(hashlib.sha256(obj.content).digest())
        if actual == obj.content_sha256:
            return VerifyResult(ok=True, actual_hash=actual)
        return VerifyResult(
            ok=False,
            actual_hash=actual,
            detail=f"expected {obj.content_sha256.hex()[:12]}…, got {actual.hex()[:12]}…",
        )

    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        """Write a local file into a tape pool via WriteSessionService.

        Returns the committed copy using the same proto-to-CopyRecord mapping as
        enumerate(), so a later scrub derives the same locator_key.
        """
        client = self._require_write_session()
        source = Path(source)

        try:
            session = client.OpenWriteSession(
                layer5_pb2.OpenWriteSessionRequest(
                    pool_target=layer5_pb2.TapePoolTarget(
                        pool_id=pool,
                        mount_if_needed=True,
                    )
                )
            )
        except grpc.RpcError as e:
            raise BackendUnavailableError(
                f"Remanence OpenWriteSession at {self._endpoint!r} failed: {_rpc_error_text(e)}"
            ) from e

        try:
            obj = client.AppendObject(self._append_messages(session.session_id, source))
            client.CloseWriteSession(
                layer5_pb2.CloseWriteSessionRequest(session_id=session.session_id)
            )
        except grpc.RpcError as e:
            self._safe_abort(client, session.session_id, _rpc_error_text(e))
            raise BackendUnavailableError(
                f"Remanence write session at {self._endpoint!r} failed: {_rpc_error_text(e)}"
            ) from e
        except Exception as e:
            self._safe_abort(client, session.session_id, str(e))
            raise

        return _copy_record_from_proto(obj, _select_written_copy(obj, session.tape_uuid))

    # --- helpers ---------------------------------------------------------

    def _require_catalog(self) -> _CatalogClient:
        if self._catalog is None:
            raise BackendUnavailableError(
                "RemanenceBackend is not configured with a live Catalog client"
            )
        return self._catalog

    def _require_write_session(self) -> _WriteSessionClient:
        if self._write_session is None:
            raise BackendError(
                "RemanenceBackend has no live WriteSession client; "
                "write_object_to_pool requires a live daemon (from_grpc)"
            )
        return self._write_session

    def _require_read_session(self) -> _ReadSessionClient:
        if self._read_session is None:
            raise BackendUnavailableError(
                "RemanenceBackend has no live ReadSession client; "
                "live read_range/verify require a live daemon (from_grpc)"
            )
        return self._read_session

    def _safe_close_read(
        self,
        client: _ReadSessionClient,
        session_id: bytes,
    ) -> None:
        try:
            client.CloseReadSession(layer5_pb2.CloseReadSessionRequest(session_id=session_id))
        except grpc.RpcError:
            return

    def _append_messages(
        self,
        session_id: bytes,
        source: Path,
    ) -> Iterator[layer5_pb2.AppendObjectMessage]:
        digest = hashlib.sha256()
        yield layer5_pb2.AppendObjectMessage(
            start=layer5_pb2.AppendObjectStart(
                session_id=session_id,
                caller_object_id=source.name,
                declared_size_bytes=source.stat().st_size,
            )
        )
        with source.open("rb") as f:
            while True:
                chunk = f.read(_WRITE_CHUNK_BYTES)
                if not chunk:
                    break
                digest.update(chunk)
                yield layer5_pb2.AppendObjectMessage(
                    chunk=layer5_pb2.AppendObjectChunk(
                        session_id=session_id,
                        data=chunk,
                    )
                )
        yield layer5_pb2.AppendObjectMessage(
            finish=layer5_pb2.AppendObjectFinish(
                session_id=session_id,
                expected_content_sha256=digest.digest(),
            )
        )

    def _safe_abort(
        self,
        client: _WriteSessionClient,
        session_id: bytes,
        reason: str,
    ) -> None:
        try:
            client.AbortWriteSession(
                layer5_pb2.AbortWriteSessionRequest(
                    session_id=session_id,
                    reason=reason,
                )
            )
        except Exception:
            return

    def _object_for_locator(self, locator: BackendLocator) -> _RemanenceObject:
        try:
            tape_uuid = bytes.fromhex(str(locator["tape_uuid"]))
            tape_file_number = int(locator["tape_file_number"])
        except (KeyError, ValueError, TypeError) as e:
            raise BackendNotFoundError(
                f"remanence locator must have 'tape_uuid' (hex) + "
                f"'tape_file_number' (int); got {locator!r}: {e}"
            ) from e
        try:
            return self._by_locator[(tape_uuid, tape_file_number)]
        except KeyError as e:
            raise BackendNotFoundError(
                f"no object at tape {tape_uuid.hex()[:12]}…, file {tape_file_number}"
            ) from e


class RemanenceReadSession:
    """Reusable read-session wrapper for several ranges from one Remanence object."""

    def __init__(
        self,
        *,
        backend: RemanenceBackend,
        client: _ReadSessionClient | None,
        session_id: bytes | None,
        object_id: bytes | None,
        fixture_locator: BackendLocator | None = None,
    ) -> None:
        self._backend = backend
        self._client = client
        self._session_id = session_id
        self._object_id = object_id
        self._fixture_locator = fixture_locator
        self._closed = False

    @classmethod
    def live(
        cls,
        *,
        backend: RemanenceBackend,
        client: _ReadSessionClient,
        session_id: bytes,
        object_id: bytes,
    ) -> RemanenceReadSession:
        return cls(
            backend=backend,
            client=client,
            session_id=session_id,
            object_id=object_id,
        )

    @classmethod
    def fixture(cls, backend: RemanenceBackend, locator: BackendLocator) -> RemanenceReadSession:
        return cls(
            backend=backend,
            client=None,
            session_id=None,
            object_id=None,
            fixture_locator=dict(locator),
        )

    def __enter__(self) -> RemanenceReadSession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def read_range(self, byte_range: ByteRange) -> bytes:
        if self._closed:
            raise BackendUnavailableError("Remanence read session is already closed")
        if self._client is None:
            if self._fixture_locator is None:
                raise BackendUnavailableError("fixture read session has no locator")
            obj = self._backend._object_for_locator(self._fixture_locator)
            if obj.content is None:
                raise BackendNotFoundError(
                    "RemanenceBackend fixture mode has no bytes for this object; "
                    "fixture is missing 'content_b64' / 'content_hex'"
                )
            if byte_range.is_whole_object:
                return obj.content
            if byte_range.end > len(obj.content):
                raise ValueError(
                    f"byte range end {byte_range.end} exceeds object size {len(obj.content)}"
                )
            return obj.content[byte_range.start : byte_range.end]

        assert self._session_id is not None
        assert self._object_id is not None
        try:
            stream = self._client.ReadObjectRange(
                layer5_pb2.ReadObjectRangeRequest(
                    session_id=self._session_id,
                    object_id=self._object_id,
                    start_byte=byte_range.start,
                    end_byte=byte_range.end,
                )
            )
            return b"".join(chunk.data for chunk in stream)
        except grpc.RpcError as e:
            raise _read_object_range_error(self._backend._endpoint, e) from e

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._client is None or self._session_id is None:
            return
        self._backend._safe_close_read(self._client, self._session_id)


# --- fixture decoding ----------------------------------------------------


def _object_from_dict(d: dict[str, Any]) -> _RemanenceObject:
    """Decode one ObjectRecord-shaped fixture dict."""
    content = _decode_content(d)
    return _RemanenceObject(
        object_id=_decode_uuid_field(d, "object_id"),
        caller_object_id=str(d.get("caller_object_id", "")),
        content_sha256=_decode_hash_field(d, "content_sha256"),
        size_bytes=int(d["logical_size_bytes"]),
        body_format=str(d.get("body_format", "rem-tar-v1")),
        caller_metadata={str(k): str(v) for k, v in d.get("caller_metadata", {}).items()},
        copies=tuple(_copy_from_dict(c) for c in d.get("copies", [])),
        content=content,
    )


def _copy_from_dict(d: dict[str, Any]) -> _RemanenceObjectCopy:
    return _RemanenceObjectCopy(
        tape_uuid=_decode_uuid_field(d, "tape_uuid"),
        tape_file_number=int(d["tape_file_number"]),
        first_body_lba=int(d.get("first_body_lba", 0)),
        pool_id=str(d.get("pool_id", "")),
        health=str(d.get("health", "ok")),
        last_verified_at=d.get("last_verified_at"),
    )


def _decode_hash_field(d: dict[str, Any], key: str) -> ContentHash:
    value = d[key]
    if isinstance(value, str):
        return content_hash(bytes.fromhex(value))
    if isinstance(value, bytes):
        return content_hash(value)
    raise ValueError(f"{key} must be hex string or bytes; got {type(value).__name__}")


def _decode_uuid_field(d: dict[str, Any], key: str) -> bytes:
    value = d[key]
    if isinstance(value, str):
        # Accept either 36-char hyphenated UUID or 32-char hex.
        decoded = bytes.fromhex(value.replace("-", ""))
    elif isinstance(value, bytes):
        decoded = value
    else:
        raise ValueError(f"{key} must be UUID string or bytes; got {type(value).__name__}")

    if len(decoded) != 16:
        raise ValueError(f"{key} must be a 16-byte UUID; got {len(decoded)} bytes")
    return decoded


def _decode_content(d: dict[str, Any]) -> bytes | None:
    """Optional content bytes from fixture."""
    if "content_b64" in d:
        return base64.b64decode(d["content_b64"])
    if "content_hex" in d:
        return bytes.fromhex(d["content_hex"])
    return None


# --- live proto decoding -------------------------------------------------


def _copy_record_from_proto(obj: layer5_pb2.ObjectRecord, cp: layer5_pb2.ObjectCopy) -> CopyRecord:
    digest = content_hash(obj.content_sha256)
    metadata: dict[str, Any] = {
        "body_format": obj.body_format,
        "caller_object_id": obj.caller_object_id,
        "health": _copy_health(cp.health),
        "last_verified_at": _timestamp_text(cp, "last_verified_at"),
        **{f"caller_meta:{k}": v for k, v in obj.caller_metadata.items()},
    }
    if obj.HasField("append_commit_info"):
        metadata["append_commit_info"] = _append_commit_info_from_proto(
            obj.append_commit_info
        )
    return CopyRecord(
        logical_id=digest,
        native_locator=_native_locator_from_proto(obj, cp),
        integrity_hash=digest,
        size_bytes=int(obj.logical_size_bytes),
        metadata=metadata,
    )


def _select_written_copy(
    obj: layer5_pb2.ObjectRecord,
    tape_uuid: bytes,
) -> layer5_pb2.ObjectCopy:
    """Pick the ObjectCopy committed by the write session."""
    matching = [cp for cp in obj.copies if cp.tape_uuid == tape_uuid]
    if len(matching) == 1:
        return matching[0]
    if not matching and len(obj.copies) == 1:
        return obj.copies[0]
    raise BackendError(
        f"Remanence AppendObject returned {len(obj.copies)} copies; cannot "
        f"identify the one written to tape {tape_uuid.hex()[:12]}…"
    )


def _native_locator_from_proto(
    obj: layer5_pb2.ObjectRecord, cp: layer5_pb2.ObjectCopy
) -> BackendLocator:
    """Canonical proto -> native_locator mapping.

    This shape must match the write path's canonical locator exactly, because
    Sutradhara keys copies by json.dumps(native_locator, sort_keys=True,
    separators=(",", ":")).
    """
    return {
        "tape_uuid": _hex_bytes(cp.tape_uuid, "tape_uuid", length=16),
        "tape_file_number": int(cp.tape_file_number),
        "first_body_lba": int(cp.first_body_lba),
        "object_id": _hex_bytes(obj.object_id, "object_id", length=16),
        "caller_object_id": obj.caller_object_id,
        "content_sha256": _hex_bytes(obj.content_sha256, "content_sha256", length=32),
        "pool_id": cp.pool_id,
        "body_format": obj.body_format,
    }


def _canonical_locator_from_mapping(locator: BackendLocator) -> BackendLocator:
    return {
        "tape_uuid": _uuid_bytes_from_locator(locator, "tape_uuid").hex(),
        "tape_file_number": int(locator["tape_file_number"]),
        "first_body_lba": int(locator["first_body_lba"]),
        "object_id": _uuid_bytes_from_locator(locator, "object_id").hex(),
        "caller_object_id": str(locator["caller_object_id"]),
        "content_sha256": _hash_from_locator_required(locator).hex(),
        "pool_id": str(locator["pool_id"]),
        "body_format": str(locator["body_format"]),
    }


def _copy_identity_from_locator(locator: BackendLocator) -> tuple[bytes, int, int]:
    try:
        return (
            _uuid_bytes_from_locator(locator, "tape_uuid"),
            int(locator["tape_file_number"]),
            int(locator["first_body_lba"]),
        )
    except (KeyError, ValueError, TypeError) as e:
        raise BackendNotFoundError(
            "remanence locator must have tape_uuid, tape_file_number, and "
            f"first_body_lba; got {locator!r}: {e}"
        ) from e


def _hash_from_locator(locator: BackendLocator) -> ContentHash | None:
    if "content_sha256" not in locator:
        return None
    return _hash_from_locator_required(locator)


def _hash_from_locator_required(locator: BackendLocator) -> ContentHash:
    try:
        value = locator["content_sha256"]
        if not isinstance(value, str):
            raise TypeError(f"content_sha256 must be a hex string, got {type(value).__name__}")
        return content_hash(bytes.fromhex(value))
    except (KeyError, ValueError, TypeError) as e:
        raise BackendNotFoundError(
            f"remanence locator must have content_sha256 hex; got {locator!r}: {e}"
        ) from e


def _uuid_bytes_from_locator(locator: BackendLocator, key: str) -> bytes:
    try:
        value = locator[key]
        if not isinstance(value, str):
            raise TypeError(f"{key} must be a UUID hex string, got {type(value).__name__}")
        decoded = bytes.fromhex(value.replace("-", ""))
    except (KeyError, ValueError, TypeError) as e:
        raise BackendNotFoundError(
            f"remanence locator must have {key!r} as UUID hex; got {locator!r}: {e}"
        ) from e
    if len(decoded) != 16:
        raise BackendNotFoundError(
            f"remanence locator {key!r} must be 16 bytes; got {len(decoded)}"
        )
    return decoded


def _hex_bytes(value: bytes, field: str, *, length: int) -> str:
    if len(value) != length:
        raise BackendUnavailableError(
            f"Remanence Catalog returned {field} with {len(value)} bytes; expected {length}"
        )
    return value.hex()


def _copy_health(value: int) -> str:
    health = int(value)
    labels: dict[int, str] = {
        int(layer5_pb2.ObjectCopy.OBJECT_COPY_HEALTH_UNSPECIFIED): "unspecified",
        int(layer5_pb2.ObjectCopy.OBJECT_COPY_HEALTH_OK): "ok",
        int(layer5_pb2.ObjectCopy.OBJECT_COPY_HEALTH_SUSPECT): "suspect",
        int(layer5_pb2.ObjectCopy.OBJECT_COPY_HEALTH_DEGRADED): "degraded",
        int(layer5_pb2.ObjectCopy.OBJECT_COPY_HEALTH_LOST): "lost",
    }
    return labels.get(health, f"unknown:{health}")


def _append_commit_info_from_proto(info: layer5_pb2.AppendCommitInfo) -> dict[str, Any]:
    return {
        "append_mode": _append_mode(info.append_mode),
        "tape_uuid": _hex_bytes(info.tape_uuid, "append_commit_info.tape_uuid", length=16),
        "voltag": info.voltag if info.HasField("voltag") else None,
        "tape_file_number": int(info.tape_file_number),
        "first_body_lba": int(info.first_body_lba),
        "position_before_lba": _optional_u64(info, "position_before_lba"),
        "position_after_lba": _optional_u64(info, "position_after_lba"),
        "journal_record_ordinal": _optional_u64(info, "journal_record_ordinal"),
        "estimated_remaining_bytes": _optional_u64(info, "estimated_remaining_bytes"),
        "sealed_after_write": info.sealed_after_write
        if info.HasField("sealed_after_write")
        else None,
    }


def _append_mode(value: int) -> str:
    labels: dict[int, str] = {
        int(layer5_pb2.APPEND_MODE_UNSPECIFIED): "unspecified",
        int(layer5_pb2.APPEND_MODE_FRESH): "fresh",
        int(layer5_pb2.APPEND_MODE_APPEND): "append",
        int(layer5_pb2.APPEND_MODE_RESUME_CONTROL): "resume_control",
        int(layer5_pb2.APPEND_MODE_SEAL): "seal",
    }
    return labels.get(int(value), f"unknown:{int(value)}")


def _optional_u64(message: Any, field: str) -> int | None:
    if not message.HasField(field):
        return None
    return int(getattr(message, field))


def _timestamp_text(message: Any, field: str) -> str:
    if not message.HasField(field):
        return ""
    value = getattr(message, field)
    return str(value.ToDatetime(tzinfo=dt.UTC).isoformat())


def _grpc_target(endpoint: str) -> str:
    if endpoint.startswith("http://"):
        return endpoint.removeprefix("http://")
    if endpoint.startswith("https://"):
        return endpoint.removeprefix("https://")
    return endpoint


def _grpc_channel_options(endpoint: str) -> tuple[tuple[str, str], ...]:
    if endpoint.startswith("unix:"):
        # tonic's UDS connector uses a dummy HTTP authority. grpcio otherwise
        # sends the socket path as :authority, which the daemon rejects.
        return (("grpc.default_authority", "127.0.0.1:50051"),)
    return ()


def _read_object_range_error(endpoint: str | None, error: grpc.RpcError) -> BackendError:
    text = (
        f"Remanence ReadObjectRange at {endpoint!r} failed: "
        f"{_rpc_error_text(error)}"
    )
    code = error.code()
    if code in {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.DATA_LOSS,
    }:
        return BackendTransientError(text)
    if code in {
        grpc.StatusCode.NOT_FOUND,
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.INVALID_ARGUMENT,
    }:
        return BackendSessionInvalidatedError(text)
    return BackendUnavailableError(text)


def _rpc_error_text(error: grpc.RpcError) -> str:
    details_raw = error.details()
    details = "" if details_raw is None else str(details_raw)
    code = error.code()
    code_name = str(getattr(code, "name", code))
    if details:
        return f"{code_name}: {details}"
    return code_name
