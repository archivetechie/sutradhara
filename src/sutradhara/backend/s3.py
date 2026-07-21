"""S3-compatible backend adapter."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from sutradhara.backend.port import (
    BackendLocator,
    BackendNotFoundError,
    ByteRange,
    CopyRecord,
    StreamKind,
    VerifyResult,
)
from sutradhara.catalog.types import content_hash


class S3Backend:
    """Thin boto3 adapter for S3-compatible object stores."""

    def __init__(
        self,
        name: str,
        *,
        bucket: str,
        prefix: str = "",
        endpoint_url: str | None = None,
        storage_class: str | None = None,
        client: Any | None = None,
    ) -> None:
        if not bucket:
            raise ValueError("S3Backend requires a bucket")
        self._name = name
        self._bucket = bucket
        self._prefix = prefix.strip("/")
        self._storage_class = storage_class
        self._client = client or _make_boto3_client(endpoint_url=endpoint_url)

    @property
    def name(self) -> str:
        return self._name

    @property
    def stream_kind(self) -> StreamKind:
        """S3 ranges are pulled lazily from the HTTP response body."""

        return StreamKind.native_stream

    def enumerate(self) -> Iterator[CopyRecord]:
        paginator = self._client.get_paginator("list_objects_v2")
        prefix = f"{self._prefix}/" if self._prefix else ""
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key")
                if not isinstance(key, str):
                    continue
                head = self._client.head_object(Bucket=self._bucket, Key=key)
                metadata = dict(head.get("Metadata") or {})
                digest_hex = metadata.get("logical_sha256") or metadata.get("sha256")
                if not isinstance(digest_hex, str):
                    continue
                try:
                    digest = content_hash(bytes.fromhex(digest_hex))
                except ValueError:
                    continue
                yield CopyRecord(
                    logical_id=digest,
                    native_locator={"bucket": self._bucket, "key": key, "sha256": digest.hex()},
                    integrity_hash=digest,
                    size_bytes=int(head.get("ContentLength") or obj.get("Size") or 0),
                    metadata=metadata,
                )

    def write_object_to_pool(self, source: Path | str, pool: str, *, caller_object_id: str | None = None) -> CopyRecord:
        source_path = Path(source)
        key = self._join_key(pool.strip("/"), source_path.name)
        return self.write_object(source_path, key=key, pool=pool)

    def write_object(self, source: Path | str, *, key: str, pool: str | None = None) -> CopyRecord:
        source_path = Path(source)
        digest = content_hash(_sha256_file(source_path))
        metadata = {
            "sha256": digest.hex(),
            "logical_sha256": digest.hex(),
        }
        if pool:
            metadata["pool"] = pool
        extra_args: dict[str, Any] = {"Metadata": metadata}
        if self._storage_class:
            extra_args["StorageClass"] = self._storage_class
        final_key = self._join_key(key)
        self._client.upload_file(
            str(source_path),
            self._bucket,
            final_key,
            ExtraArgs=extra_args,
        )
        return CopyRecord(
            logical_id=digest,
            native_locator={
                "bucket": self._bucket,
                "key": final_key,
                "sha256": digest.hex(),
            },
            integrity_hash=digest,
            size_bytes=source_path.stat().st_size,
            metadata=metadata,
        )

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        bucket, key = self._bucket_key(locator)
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if not byte_range.is_whole_object:
            kwargs["Range"] = f"bytes={byte_range.start}-{byte_range.end - 1}"
        try:
            response = self._client.get_object(**kwargs)
        except Exception as exc:
            raise BackendNotFoundError(f"S3 object not found: s3://{bucket}/{key}") from exc
        body = response["Body"]
        try:
            data = body.read()
            if not isinstance(data, bytes):
                raise TypeError("S3 body read did not return bytes")
            return data
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()

    @contextmanager
    def open_range_chunks(
        self,
        locator: BackendLocator,
        byte_range: ByteRange,
        *,
        chunk_bytes: int,
    ) -> Iterator[Iterator[bytes]]:
        """Open a ranged HTTP body and close it on every context exit path."""

        if chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be greater than zero")
        if not byte_range.is_whole_object and byte_range.length == 0:
            yield iter(())
            return
        bucket, key = self._bucket_key(locator)
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if not byte_range.is_whole_object:
            kwargs["Range"] = f"bytes={byte_range.start}-{byte_range.end - 1}"
        try:
            response = self._client.get_object(**kwargs)
        except Exception as exc:
            raise BackendNotFoundError(f"S3 object not found: s3://{bucket}/{key}") from exc
        body = response["Body"]
        try:
            yield body.iter_chunks(chunk_size=chunk_bytes)
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()

    def verify(self, locator: BackendLocator) -> VerifyResult:
        expected_hex = locator.get("sha256")
        data = self.read_range(locator, ByteRange(0, 0))
        actual = content_hash(hashlib.sha256(data).digest())
        if not isinstance(expected_hex, str):
            return VerifyResult(
                ok=True,
                measured=True,
                actual_hash=actual,
                detail="no expected hash in locator",
            )
        try:
            expected = content_hash(bytes.fromhex(expected_hex))
        except ValueError:
            return VerifyResult(
                ok=False,
                measured=True,
                actual_hash=actual,
                detail="invalid expected hash",
            )
        if actual == expected:
            return VerifyResult(ok=True, measured=True, actual_hash=actual)
        return VerifyResult(
            ok=False,
            measured=True,
            actual_hash=actual,
            detail=f"expected {expected.hex()[:12]}..., got {actual.hex()[:12]}...",
        )

    def delete_object(self, locator: BackendLocator) -> bool:
        """Delete an object, using S3's idempotent delete semantics."""
        bucket, key = self._bucket_key(locator)
        existed = True
        try:
            self._client.head_object(Bucket=bucket, Key=key)
        except KeyError:
            existed = False
        except Exception as exc:
            response = getattr(exc, "response", {})
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = response.get("Error", {}).get("Code")
            if status == 404 or code in ("404", "NoSuchKey", "NotFound"):
                existed = False
            else:
                raise
        self._client.delete_object(Bucket=bucket, Key=key)
        return existed

    def _join_key(self, *parts: str) -> str:
        key_parts = [self._prefix, *parts] if self._prefix else list(parts)
        return "/".join(part.strip("/") for part in key_parts if part.strip("/"))

    def _bucket_key(self, locator: BackendLocator) -> tuple[str, str]:
        bucket = locator.get("bucket", self._bucket)
        key = locator.get("key")
        if not isinstance(bucket, str) or not isinstance(key, str):
            raise BackendNotFoundError(f"S3 locator must contain bucket/key; got {locator!r}")
        return bucket, key


def _make_boto3_client(*, endpoint_url: str | None) -> Any:
    try:
        import boto3  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - depends on local env
        raise RuntimeError("boto3 is required for S3 backends") from exc
    return boto3.client("s3", endpoint_url=endpoint_url)


def _sha256_file(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()
