"""Tests for the rsync/SSH disk backend adapter."""

from __future__ import annotations

import hashlib
import shutil
from collections.abc import Iterator, Sequence
from pathlib import Path
from subprocess import CompletedProcess

import pytest

from sutradhara.backend.factory import BackendNotConfigured, backend_from_row
from sutradhara.backend.port import (
    BackendError,
    BackendNotFoundError,
    BackendUnavailableError,
    ByteRange,
)
from sutradhara.backend.ssh_disk import RsyncSshTransport, SshDiskBackend
from sutradhara.catalog.models import Backend
from sutradhara.catalog.types import BackendKind, BackendTier, content_hash


def test_ssh_disk_backend_write_range_verify_enumerate_and_delete(tmp_path: Path) -> None:
    transport = _LocalDirTransport(tmp_path / "remote")
    backend = SshDiskBackend("lan", host="ignored", root="/ignored", transport=transport)
    source = tmp_path / "object.rao"
    source.write_bytes(b"abcdef")

    record = backend.write_object(source, key="intakes/card-1.rao", pool="cloud-temp")

    digest = content_hash(hashlib.sha256(b"abcdef").digest())
    assert record.logical_id == digest
    assert record.integrity_hash == digest
    assert record.native_locator == {
        "key": "intakes/card-1.rao",
        "sha256": digest.hex(),
        "size_bytes": 6,
    }
    assert record.metadata == {"pool": "cloud-temp"}
    assert backend.read_range(record.native_locator, ByteRange(0, 0)) == b"abcdef"
    assert backend.read_range(record.native_locator, ByteRange(1, 4)) == b"bcd"
    with backend.open_materialized_range_chunks(
        record.native_locator,
        ByteRange(1, 6),
        chunk_bytes=2,
    ) as chunks:
        assert list(chunks) == [b"bc", b"de", b"f"]
    assert backend.verify(record.native_locator).ok

    backend.write_object(source, key="intakes/card-1.rao", pool="cloud-temp")
    rows = list(backend.enumerate())
    assert len(rows) == 1
    assert rows[0].native_locator == record.native_locator
    assert rows[0].integrity_hash == digest

    (tmp_path / "remote" / "intakes" / "card-1.rao").write_bytes(b"corrupt")
    mismatch = backend.verify(record.native_locator)
    assert not mismatch.ok
    assert mismatch.actual_hash == content_hash(hashlib.sha256(b"corrupt").digest())

    backend.delete_object(record.native_locator)
    assert not backend.verify(record.native_locator).ok
    with pytest.raises(BackendNotFoundError):
        backend.read_range(record.native_locator, ByteRange(0, 0))
    backend.delete_object(record.native_locator)


def test_ssh_disk_backend_rejects_unsafe_keys_before_transport(tmp_path: Path) -> None:
    transport = _LocalDirTransport(tmp_path / "remote")
    backend = SshDiskBackend("lan", host="ignored", root="/ignored", transport=transport)
    source = tmp_path / "object.rao"
    source.write_bytes(b"payload")
    unsafe_keys = [
        "../x",
        "a/../b",
        "/abs",
        "a//b",
        ".",
        "a/./b",
        "bad\\key",
        "bad\x00key",
        "bad\nkey",
    ]

    for key in unsafe_keys:
        with pytest.raises(ValueError, match=r"unsafe|non-empty"):
            backend.write_object(source, key=key)
    assert transport.calls == []

    for key in unsafe_keys:
        locator = {"key": key, "sha256": "00" * 32}
        with pytest.raises(ValueError, match=r"unsafe|non-empty"):
            backend.read_range(locator, ByteRange(0, 0))
        with pytest.raises(ValueError, match=r"unsafe|non-empty"):
            backend.verify(locator)
        with pytest.raises(ValueError, match=r"unsafe|non-empty"):
            backend.delete_object(locator)
    assert transport.calls == []


def test_ssh_disk_verify_and_enumerate_are_defensive(tmp_path: Path) -> None:
    transport = _LocalDirTransport(tmp_path / "remote")
    backend = SshDiskBackend("lan", host="ignored", root="/ignored", transport=transport)
    source = tmp_path / "object.rao"
    source.write_bytes(b"payload")
    record = backend.write_object(source, key="intakes/card-1.rao")

    assert not backend.verify({"key": "intakes/card-1.rao"}).ok
    assert not backend.verify({"key": "intakes/card-1.rao", "sha256": "00"}).ok

    transport.hash_overrides["intakes/card-1.rao"] = "not-hex"
    assert not backend.verify(record.native_locator).ok
    transport.hash_overrides["intakes/card-1.rao"] = "00"
    assert not backend.verify(record.native_locator).ok

    transport.hash_overrides.pop("intakes/card-1.rao")
    (tmp_path / "remote" / "bad-hash.rao").write_bytes(b"bad")
    transport.hash_overrides["bad-hash.rao"] = "00"
    transport.extra_list_entries.extend(["a/../bad.rao", "missing.rao"])
    transport.missing_hashes.add("missing.rao")

    rows = list(backend.enumerate())
    assert [row.native_locator["key"] for row in rows] == ["intakes/card-1.rao"]


def test_backend_factory_builds_ssh_disk_and_validates_config() -> None:
    backend = backend_from_row(
        _ssh_disk_row(
            {
                "host": "backup.example",
                "root": "/srv/sutradhara",
                "user": "archive",
                "identity_file": "/run/keys/id_ed25519",
                "ssh_options": ["-p", "2222"],
            }
        )
    )

    assert isinstance(backend, SshDiskBackend)
    assert backend.name == "lan"

    with pytest.raises(BackendNotConfigured, match=r"host and config.root"):
        backend_from_row(_ssh_disk_row({"host": "backup.example"}))
    with pytest.raises(BackendNotConfigured, match="ssh_options"):
        backend_from_row(
            _ssh_disk_row(
                {"host": "backup.example", "root": "/srv/sutradhara", "ssh_options": "-p 2222"}
            )
        )
    with pytest.raises(BackendNotConfigured, match="ssh_options"):
        backend_from_row(
            _ssh_disk_row(
                {"host": "backup.example", "root": "/srv/sutradhara", "ssh_options": ["-p", 2222]}
            )
        )


def test_rsync_ssh_transport_constructs_safe_commands(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def runner(
        argv: Sequence[str],
        *,
        timeout: float,
        shell: bool,
    ) -> CompletedProcess[str]:
        assert timeout > 0
        assert shell is False
        calls.append(list(argv))
        stdout = "intakes/card 'one.rao\n" if argv[0] == "ssh" and "find " in argv[-1] else ""
        return _completed(argv, stdout=stdout)

    source = tmp_path / "clip 'one.rao"
    source.write_bytes(b"payload")
    transport = RsyncSshTransport(
        "backup.example",
        "/remote root/quote'root",
        user="archive",
        identity_file="/run/keys/id_ed25519",
        ssh_options=["-p", "2222"],
        runner=runner,
    )

    transport.put(source, "intakes/card 'one.rao")
    assert [call[0] for call in calls] == ["ssh", "rsync", "ssh"]
    assert "mkdir -p" in calls[0][-1]
    assert calls[1][:4] == ["rsync", "-a", "--partial", "--protect-args"]
    assert calls[1][4] == "-e"
    assert "BatchMode=yes" in calls[1][5]
    assert "ConnectTimeout=" in calls[1][5]
    assert calls[1][-1].endswith("/remote root/quote'root/intakes/card 'one.rao.partial")
    assert "mv -f" in calls[2][-1]
    assert "quote'\"'\"'root" in calls[0][-1]
    assert "card '\"'\"'one.rao" in calls[2][-1]

    calls.clear()
    assert list(transport.list_files()) == ["intakes/card 'one.rao"]
    assert calls[0][0] == "ssh"
    assert "! -name '*.partial'" in calls[0][-1]
    assert "-printf '%P\\n'" in calls[0][-1]


def test_rsync_ssh_transport_classifies_failures(tmp_path: Path) -> None:
    source = tmp_path / "object.rao"
    source.write_bytes(b"payload")

    absent = RsyncSshTransport("backup.example", "/root", runner=_runner_returning(42))
    assert absent.sha256("intakes/missing.rao") is None
    assert absent.size("intakes/missing.rao") is None
    with pytest.raises(BackendNotFoundError):
        absent.get("intakes/missing.rao", tmp_path / "out.rao")

    unavailable = RsyncSshTransport("backup.example", "/root", runner=_runner_returning(255))
    with pytest.raises(BackendUnavailableError):
        unavailable.sha256("intakes/object.rao")

    failed = RsyncSshTransport("backup.example", "/root", runner=_runner_returning(13))
    with pytest.raises(BackendError, match="permission denied"):
        failed.size("intakes/object.rao")


class _LocalDirTransport:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.calls: list[tuple[str, str]] = []
        self.hash_overrides: dict[str, str] = {}
        self.extra_list_entries: list[str] = []
        self.missing_hashes: set[str] = set()

    def put(self, local: Path, relpath: str) -> None:
        self.calls.append(("put", relpath))
        final = self.root / relpath
        partial = final.with_name(f"{final.name}.partial")
        partial.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local, partial)
        partial.replace(final)

    def get(self, relpath: str, local: Path) -> None:
        self.calls.append(("get", relpath))
        source = self.root / relpath
        if not source.exists():
            raise FileNotFoundError(relpath)
        local.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, local)

    def sha256(self, relpath: str) -> str | None:
        self.calls.append(("sha256", relpath))
        if relpath in self.missing_hashes:
            return None
        if relpath in self.hash_overrides:
            return self.hash_overrides[relpath]
        path = self.root / relpath
        if not path.exists():
            return None
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def size(self, relpath: str) -> int | None:
        self.calls.append(("size", relpath))
        path = self.root / relpath
        if not path.exists():
            return None
        return path.stat().st_size

    def remove(self, relpath: str) -> None:
        self.calls.append(("remove", relpath))
        (self.root / relpath).unlink(missing_ok=True)

    def list_files(self) -> Iterator[str]:
        entries = [
            path.relative_to(self.root).as_posix()
            for path in sorted(self.root.rglob("*"))
            if path.is_file() and not path.name.endswith(".partial")
        ]
        return iter([*entries, *self.extra_list_entries])


def _ssh_disk_row(config: dict[str, object] | None) -> Backend:
    return Backend(
        name="lan",
        kind=BackendKind.SSH_DISK,
        tier=BackendTier.CATALOG_AUTHORITATIVE,
        config=config,
    )


def _runner_returning(returncode: int):
    def runner(
        argv: Sequence[str],
        *,
        timeout: float,
        shell: bool,
    ) -> CompletedProcess[str]:
        return _completed(argv, returncode=returncode, stderr="permission denied")

    return runner


def _completed(
    argv: Sequence[str],
    *,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> CompletedProcess[str]:
    return CompletedProcess(list(argv), returncode, stdout=stdout, stderr=stderr)
