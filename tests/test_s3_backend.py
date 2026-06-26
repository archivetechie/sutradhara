"""S3 backend adapter tests."""

from __future__ import annotations

import hashlib
import io
import os
from pathlib import Path
from typing import Any

import pytest

from sutradhara.backend.port import ByteRange
from sutradhara.backend.s3 import S3Backend


def test_s3_backend_write_range_verify_and_enumerate(tmp_path: Path) -> None:
    client = _FakeS3Client()
    backend = S3Backend(
        "cloud-temp",
        bucket="bucket",
        prefix="sutra",
        storage_class="STANDARD_IA",
        client=client,
    )
    source = tmp_path / "object.rao"
    source.write_bytes(b"abcdef")

    record = backend.write_object(source, key="intakes/card-1.rao", pool="cloud-temp")

    assert record.native_locator["key"] == "sutra/intakes/card-1.rao"
    assert (
        client.extra_args[("bucket", "sutra/intakes/card-1.rao")]["StorageClass"] == "STANDARD_IA"
    )
    assert backend.read_range(record.native_locator, ByteRange(1, 4)) == b"bcd"
    assert backend.verify(record.native_locator).ok
    rows = list(backend.enumerate())
    assert len(rows) == 1
    assert rows[0].integrity_hash == hashlib.sha256(b"abcdef").digest()


def test_s3_minio_live_round_trip_skips_without_env(tmp_path: Path) -> None:
    endpoint = os.environ.get("SUTRADHARA_MINIO_ENDPOINT")
    bucket = os.environ.get("SUTRADHARA_MINIO_BUCKET")
    if not endpoint or not bucket:
        pytest.skip("set SUTRADHARA_MINIO_ENDPOINT and SUTRADHARA_MINIO_BUCKET for live MinIO")
    try:
        import boto3  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("boto3 is not installed")

    client = boto3.client("s3", endpoint_url=endpoint)
    backend = S3Backend("minio", bucket=bucket, prefix="sutradhara-test", client=client)
    source = tmp_path / "roundtrip.bin"
    source.write_bytes(b"minio round trip")
    record = backend.write_object(source, key="roundtrip.bin", pool="cloud-temp")

    assert backend.read_range(record.native_locator, ByteRange(0, 0)) == b"minio round trip"
    assert backend.verify(record.native_locator).ok


class _FakePaginator:
    def __init__(self, client: _FakeS3Client) -> None:
        self._client = client

    def paginate(self, *, Bucket: str, Prefix: str) -> list[dict[str, Any]]:
        contents = [
            {"Key": key, "Size": len(data)}
            for (bucket, key), data in self._client.objects.items()
            if bucket == Bucket and key.startswith(Prefix)
        ]
        return [{"Contents": contents}]


class _FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.metadata: dict[tuple[str, str], dict[str, str]] = {}
        self.extra_args: dict[tuple[str, str], dict[str, Any]] = {}

    def upload_file(
        self,
        filename: str,
        bucket: str,
        key: str,
        *,
        ExtraArgs: dict[str, Any] | None = None,
    ) -> None:
        data = Path(filename).read_bytes()
        object_key = (bucket, key)
        self.objects[object_key] = data
        self.extra_args[object_key] = ExtraArgs or {}
        self.metadata[object_key] = dict((ExtraArgs or {}).get("Metadata") or {})

    def get_object(self, *, Bucket: str, Key: str, Range: str | None = None) -> dict[str, Any]:
        data = self.objects[(Bucket, Key)]
        if Range:
            prefix, _, span = Range.partition("=")
            assert prefix == "bytes"
            start_raw, _, end_raw = span.partition("-")
            start = int(start_raw)
            end = int(end_raw)
            data = data[start : end + 1]
        return {"Body": io.BytesIO(data)}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        data = self.objects[(Bucket, Key)]
        return {
            "ContentLength": len(data),
            "Metadata": self.metadata.get((Bucket, Key), {}),
        }

    def get_paginator(self, name: str) -> _FakePaginator:
        assert name == "list_objects_v2"
        return _FakePaginator(self)
