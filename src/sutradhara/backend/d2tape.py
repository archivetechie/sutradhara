"""d2tape storage backend adapter.

This adapter is Sutradhara's Scenario N bridge to the Java d2tape CLI: it
stages one plaintext file as a legacy d2 tar artifact, shells out to
``java -jar d2tape-cli``, verifies the written artifact before returning a
copy record, and keeps a small per-volume sidecar so Sutradhara can continue
appending statelessly between process runs.

The sidecar-backed ``enumerate()`` is an honest v1 limitation. Rebuilding the
catalog directly from a finalized on-tape volume index requires a d2tape
``enumerate`` reader, which is a later milestone; until then, these sidecars
are crash state and a local discovery source for copies this adapter wrote.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sutradhara.backend.port import (
    BackendError,
    BackendLocator,
    BackendNotFoundError,
    BackendUnavailableError,
    ByteRange,
    CopyRecord,
    VerifyResult,
)
from sutradhara.catalog.types import ContentHash, content_hash
from sutradhara.sealing.port import Representation

_DEFAULT_DEVICE_ENV = Path("/var/lib/replica/d2tape/device.env")
_DEFAULT_STATE_DIR = Path("/var/lib/replica/d2tape/volumes")
_DEFAULT_JAR_GLOB = (
    Path.home()
    / "d2tape"
    / "d2tape-cli"
    / "target"
    / "d2tape-cli-*-jar-with-dependencies.jar"
)
_DEFAULT_TIMEOUT_SECONDS = 300.0
_PAYLOAD_NAME = "payload.bin"
_STATE_VERSION = 1


@dataclass(frozen=True)
class _DeviceConfig:
    device: str
    barcode: str
    volume_blocksize: int
    archive_blocksize: int
    explicit_volume_uuid: str | None
    stinit_script: str | None


class D2TapeBackend:
    """Writable adapter for the d2tape CLI's legacy d2-tar format."""

    def __init__(
        self,
        name: str,
        *,
        jar_path: Path | str | None = None,
        java_home: Path | str | None = None,
        java_bin: Path | str | None = None,
        device_env_path: Path | str = _DEFAULT_DEVICE_ENV,
        state_dir: Path | str = _DEFAULT_STATE_DIR,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        file_backed: bool = False,
        temp_dir: Path | str | None = None,
        stinit_script: Path | str | None = None,
        volume_uuid: str | None = None,
    ) -> None:
        self._name = name
        self._jar_path = Path(jar_path) if jar_path is not None else _resolve_jar_path()
        self._java_bin = _resolve_java_bin(java_home=java_home, java_bin=java_bin)
        self._java_home_env = str(java_home) if java_home is not None else None
        self._device_env_path = Path(device_env_path)
        self._state_dir = Path(state_dir)
        self._timeout_seconds = timeout_seconds
        self._file_backed = file_backed
        self._temp_dir = Path(temp_dir) if temp_dir is not None else None
        self._stinit_script = str(stinit_script) if stinit_script is not None else None
        self._volume_uuid = volume_uuid

    @property
    def name(self) -> str:
        return self._name

    # --- StorageBackend protocol -----------------------------------------

    def enumerate(self) -> Iterator[CopyRecord]:
        if not self._state_dir.exists():
            return iter(())
        return self._enumerate_sidecars()

    def _enumerate_sidecars(self) -> Iterator[CopyRecord]:
        for path in sorted(self._state_dir.glob("*.json")):
            state = _load_json_object(path)
            artifacts = state.get("artifacts", [])
            if not isinstance(artifacts, list):
                raise BackendError(f"d2tape sidecar {path} has non-list artifacts")
            for artifact in artifacts:
                if not isinstance(artifact, dict):
                    raise BackendError(f"d2tape sidecar {path} has malformed artifact")
                if artifact.get("verified") is False:
                    continue
                yield _record_from_sidecar_artifact(artifact)

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        device = self._device_config()
        artifact_name = _required_str(locator, "artifact_name")
        start_block = _required_int(locator, "start_block")
        end_block = _required_int(locator, "end_block")
        volume_blocksize = _required_int(locator, "volume_blocksize")

        with tempfile.TemporaryDirectory(prefix="sutradhara-d2tape-restore-") as raw:
            dest = Path(raw)
            report = self._run_d2tape(
                [
                    "restore",
                    "--device",
                    device.device,
                    "--volume-blocksize",
                    str(volume_blocksize),
                    "--archive-blocksize",
                    str(device.archive_blocksize),
                    "--artifact-name",
                    artifact_name,
                    "--start-block",
                    str(start_block),
                    "--end-block",
                    str(end_block),
                    "--dest",
                    str(dest),
                ],
                device,
            )
            if report.get("ok") is not True:
                raise BackendError(f"d2tape restore reported ok=false for {artifact_name}")
            restored = dest / artifact_name / _PAYLOAD_NAME
            if not restored.is_file():
                raise BackendNotFoundError(
                    f"d2tape restore did not materialize expected payload {restored}"
                )
            data = restored.read_bytes()

        if byte_range.is_whole_object:
            return data
        if byte_range.end > len(data):
            raise ValueError(
                f"byte range end {byte_range.end} exceeds object size {len(data)}"
            )
        return data[byte_range.start : byte_range.end]

    def verify(self, locator: BackendLocator) -> VerifyResult:
        device = self._device_config()
        artifact = self._artifact_for_locator(locator)
        expected = content_hash(bytes.fromhex(_required_str(artifact, "integrity_hash")))
        with tempfile.TemporaryDirectory(prefix="sutradhara-d2tape-verify-") as raw:
            hashes_path = Path(raw) / "hashes.json"
            hashes_path.write_text(
                json.dumps(_required_dict(artifact, "hashes"), sort_keys=True) + "\n"
            )
            report = self._run_d2tape(
                [
                    "verify",
                    "--device",
                    device.device,
                    "--volume-blocksize",
                    str(_required_int(locator, "volume_blocksize")),
                    "--start-block",
                    str(_required_int(locator, "start_block")),
                    "--end-block",
                    str(_required_int(locator, "end_block")),
                    "--hashes",
                    str(hashes_path),
                ],
                device,
            )
        if report.get("ok") is True and _per_file_ok(report):
            return VerifyResult(ok=True, actual_hash=expected)
        return VerifyResult(
            ok=False,
            actual_hash=None,
            detail=f"d2tape verify reported failure for {_required_str(locator, 'artifact_name')}",
        )

    def write_object_to_pool(self, source: Path | str, pool: str) -> CopyRecord:
        """Write one plaintext file as one d2 tar artifact and verify it.

        d2tape finalization is intentionally not performed here: finalizing
        writes the volume index and is a tape-close lifecycle operation, not a
        per-copy write-path step.
        """
        source_path = Path(source)
        if not source_path.is_file():
            raise BackendNotFoundError(f"d2tape source file does not exist: {source_path}")

        device = self._device_config()
        digest = _sha256_file(source_path)
        artifact_name = f"n-{digest.hex()[:16]}"
        relpath = f"{artifact_name}/{_PAYLOAD_NAME}"
        volume_uuid = self._volume_uuid_for(device)
        state_path = self._state_path(device.barcode)
        state = self._load_state(device, volume_uuid)
        prev_end_block = state.get("last_end_block")
        if prev_end_block is not None and not isinstance(prev_end_block, int):
            raise BackendError(f"d2tape sidecar {state_path} has invalid last_end_block")

        with tempfile.TemporaryDirectory(prefix="sutradhara-d2tape-write-") as raw:
            temp_root = Path(raw)
            source_dir = temp_root / "source"
            artifact_dir = source_dir / artifact_name
            artifact_dir.mkdir(parents=True)
            staged = artifact_dir / _PAYLOAD_NAME
            shutil.copyfile(source_path, staged)
            hashes = {relpath: digest.hex()}
            hashes_path = temp_root / "hashes.json"
            hashes_path.write_text(json.dumps(hashes, sort_keys=True) + "\n")

            args = [
                "write",
                "--device",
                device.device,
                "--source-dir",
                str(source_dir),
                "--artifact-name",
                artifact_name,
                "--volume-blocksize",
                str(device.volume_blocksize),
                "--archive-blocksize",
                str(device.archive_blocksize),
                "--prev-end-block",
                "none" if prev_end_block is None else str(prev_end_block),
                "--junk-dir",
                ".dwara-ignored",
                "--hashes",
                str(hashes_path),
                "--barcode",
                device.barcode,
            ]
            if device.explicit_volume_uuid is not None:
                args.extend(["--volume-uuid", device.explicit_volume_uuid])
            write_report = self._run_d2tape(args, device)

            start_block = _required_int(write_report, "artifactStartVolumeBlock")
            end_block = _required_int(write_report, "artifactEndVolumeBlock")
            artifact = {
                "artifact_name": artifact_name,
                "barcode": device.barcode,
                "volume_uuid": volume_uuid,
                "start_block": start_block,
                "end_block": end_block,
                "volume_blocksize": device.volume_blocksize,
                "archive_blocksize": device.archive_blocksize,
                "pool_id": pool,
                "logical_id": digest.hex(),
                "integrity_hash": digest.hex(),
                "size_bytes": source_path.stat().st_size,
                "hashes": hashes,
                "relative_path": relpath,
                "verified": False,
            }
            _append_artifact_state(state, artifact)
            self._write_state(state_path, state)

            verify_report = self._run_d2tape(
                [
                    "verify",
                    "--device",
                    device.device,
                    "--volume-blocksize",
                    str(device.volume_blocksize),
                    "--start-block",
                    str(start_block),
                    "--end-block",
                    str(end_block),
                    "--hashes",
                    str(hashes_path),
                ],
                device,
            )
            if verify_report.get("ok") is not True or not _per_file_ok(verify_report):
                raise BackendError(f"d2tape verify reported failure for {artifact_name}")

        artifact["verified"] = True
        _replace_artifact_state(state, artifact)
        self._write_state(state_path, state)

        return CopyRecord(
            logical_id=digest,
            native_locator={
                "barcode": device.barcode,
                "volume_uuid": volume_uuid,
                "artifact_name": artifact_name,
                "start_block": start_block,
                "end_block": end_block,
                "volume_blocksize": device.volume_blocksize,
                "pool_id": pool,
            },
            integrity_hash=digest,
            size_bytes=source_path.stat().st_size,
            metadata={"representation": Representation.D2TAR_RAW.value},
        )

    # --- helpers ---------------------------------------------------------

    def _run_d2tape(self, args: list[str], device: _DeviceConfig) -> dict[str, Any]:
        cmd = [self._java_bin, "-jar", str(self._jar_path), *args]
        if self._file_backed:
            cmd.insert(4, "--file-backed")
        _append_common_cli_args(
            cmd,
            temp_dir=self._temp_dir,
            stinit_script=self._stinit_script or device.stinit_script,
        )
        env = os.environ.copy()
        if self._java_home_env is not None:
            env["JAVA_HOME"] = self._java_home_env
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=self._timeout_seconds,
                env=env,
            )
        except subprocess.TimeoutExpired as exc:
            raise BackendUnavailableError(
                f"d2tape {args[0]} timed out after {self._timeout_seconds:g}s"
            ) from exc

        if result.returncode != 0:
            raise BackendUnavailableError(
                f"d2tape {args[0]} failed (exit {result.returncode}): "
                f"stdout={_truncate(result.stdout)!r} stderr={_truncate(result.stderr)!r}"
            )

        stdout = result.stdout.strip()
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise BackendError(
                f"d2tape {args[0]} succeeded but emitted invalid JSON: "
                f"stdout={_truncate(result.stdout)!r}"
            ) from exc
        if not isinstance(parsed, dict):
            raise BackendError(f"d2tape {args[0]} JSON response is not an object")
        return parsed

    def _device_config(self) -> _DeviceConfig:
        values = _read_env_file(self._device_env_path)
        device = os.environ.get("D2TAPE_DEVICE") or values.get("D2TAPE_DEVICE")
        if not device:
            raise BackendUnavailableError(
                "d2tape device state not found; set D2TAPE_DEVICE or write "
                f"{self._device_env_path}"
            )
        barcode = os.environ.get("D2TAPE_BARCODE") or values.get("D2TAPE_BARCODE", "")
        if not barcode:
            raise BackendUnavailableError(
                "d2tape device state is missing D2TAPE_BARCODE"
            )
        return _DeviceConfig(
            device=device,
            barcode=barcode,
            volume_blocksize=int(
                os.environ.get("D2TAPE_VOLUME_BLOCKSIZE")
                or values.get("D2TAPE_VOLUME_BLOCKSIZE", "256000")
            ),
            archive_blocksize=int(
                os.environ.get("D2TAPE_ARCHIVE_BLOCKSIZE")
                or values.get("D2TAPE_ARCHIVE_BLOCKSIZE", "512")
            ),
            explicit_volume_uuid=(
                self._volume_uuid
                or os.environ.get("D2TAPE_VOLUME_UUID")
                or values.get("D2TAPE_VOLUME_UUID")
            ),
            stinit_script=(
                os.environ.get("D2TAPE_STINIT_SCRIPT")
                or values.get("D2TAPE_STINIT_SCRIPT")
            ),
        )

    def _volume_uuid_for(self, device: _DeviceConfig) -> str:
        if device.explicit_volume_uuid is not None:
            return device.explicit_volume_uuid
        state_path = self._state_path(device.barcode)
        if state_path.exists():
            state = _load_json_object(state_path)
            value = state.get("volume_uuid")
            if isinstance(value, str) and value:
                return value
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"sutradhara:d2tape:{device.barcode}"))

    def _state_path(self, barcode: str) -> Path:
        if "/" in barcode or barcode in {"", ".", ".."}:
            raise BackendError(f"unsafe d2tape barcode for sidecar path: {barcode!r}")
        return self._state_dir / f"{barcode}.json"

    def _load_state(self, device: _DeviceConfig, volume_uuid: str) -> dict[str, Any]:
        path = self._state_path(device.barcode)
        if path.exists():
            state = _load_json_object(path)
            if state.get("barcode") != device.barcode:
                raise BackendError(f"d2tape sidecar {path} barcode mismatch")
            return state
        return {
            "version": _STATE_VERSION,
            "barcode": device.barcode,
            "volume_uuid": volume_uuid,
            "volume_blocksize": device.volume_blocksize,
            "archive_blocksize": device.archive_blocksize,
            "last_end_block": None,
            "artifacts": [],
        }

    def _write_state(self, path: Path, state: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
        tmp.replace(path)

    def _artifact_for_locator(self, locator: BackendLocator) -> dict[str, Any]:
        barcode = _required_str(locator, "barcode")
        artifact_name = _required_str(locator, "artifact_name")
        start_block = _required_int(locator, "start_block")
        state_path = self._state_path(barcode)
        if not state_path.exists():
            raise BackendNotFoundError(f"d2tape sidecar not found for barcode {barcode}")
        state = _load_json_object(state_path)
        for artifact in state.get("artifacts", []):
            if (
                isinstance(artifact, dict)
                and artifact.get("artifact_name") == artifact_name
                and artifact.get("start_block") == start_block
            ):
                return artifact
        raise BackendNotFoundError(
            f"no d2tape sidecar entry for {barcode}/{artifact_name}@{start_block}"
        )


def _resolve_jar_path() -> Path:
    if value := os.environ.get("D2TAPE_JAR"):
        path = Path(value)
        if not path.is_file():
            raise FileNotFoundError(f"D2TAPE_JAR does not exist: {path}")
        return path
    candidates = sorted(_DEFAULT_JAR_GLOB.parent.glob(_DEFAULT_JAR_GLOB.name))
    if candidates:
        return candidates[-1]
    raise FileNotFoundError(
        "d2tape CLI fat jar not found. Expected $D2TAPE_JAR or "
        f"{_DEFAULT_JAR_GLOB}"
    )


def _resolve_java_bin(
    *,
    java_home: Path | str | None,
    java_bin: Path | str | None,
) -> str:
    if java_bin is not None:
        return str(java_bin)
    if java_home is not None:
        return str(Path(java_home) / "bin" / "java")
    return "java"


def _append_common_cli_args(
    cmd: list[str],
    *,
    temp_dir: Path | None,
    stinit_script: str | None,
) -> None:
    if temp_dir is not None:
        cmd.extend(["--temp-dir", str(temp_dir)])
    if stinit_script:
        cmd.extend(["--stinit-script", stinit_script])


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _sha256_file(path: Path) -> ContentHash:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return content_hash(digest.digest())


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise BackendError(f"d2tape sidecar {path} is invalid JSON") from exc
    if not isinstance(data, dict):
        raise BackendError(f"d2tape sidecar {path} JSON root is not an object")
    return data


def _append_artifact_state(state: dict[str, Any], artifact: dict[str, Any]) -> None:
    artifacts = state.get("artifacts")
    if not isinstance(artifacts, list):
        raise BackendError("d2tape sidecar has non-list artifacts")
    artifacts.append(dict(artifact))
    state["last_end_block"] = artifact["end_block"]


def _replace_artifact_state(state: dict[str, Any], artifact: dict[str, Any]) -> None:
    artifacts = state.get("artifacts")
    if not isinstance(artifacts, list):
        raise BackendError("d2tape sidecar has non-list artifacts")
    for index, existing in enumerate(artifacts):
        if (
            isinstance(existing, dict)
            and existing.get("artifact_name") == artifact["artifact_name"]
            and existing.get("start_block") == artifact["start_block"]
        ):
            artifacts[index] = dict(artifact)
            state["last_end_block"] = artifact["end_block"]
            return
    artifacts.append(dict(artifact))
    state["last_end_block"] = artifact["end_block"]


def _record_from_sidecar_artifact(artifact: dict[str, Any]) -> CopyRecord:
    logical_id = content_hash(bytes.fromhex(_required_str(artifact, "logical_id")))
    integrity_hash = content_hash(bytes.fromhex(_required_str(artifact, "integrity_hash")))
    return CopyRecord(
        logical_id=logical_id,
        native_locator={
            "barcode": _required_str(artifact, "barcode"),
            "volume_uuid": _required_str(artifact, "volume_uuid"),
            "artifact_name": _required_str(artifact, "artifact_name"),
            "start_block": _required_int(artifact, "start_block"),
            "end_block": _required_int(artifact, "end_block"),
            "volume_blocksize": _required_int(artifact, "volume_blocksize"),
            "pool_id": _required_str(artifact, "pool_id"),
        },
        integrity_hash=integrity_hash,
        size_bytes=_required_int(artifact, "size_bytes"),
        metadata={"representation": Representation.D2TAR_RAW.value},
    )


def _required_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise BackendNotFoundError(f"d2tape locator/sidecar must include string {key!r}")
    return value


def _required_int(data: dict[str, Any], key: str) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise BackendNotFoundError(f"d2tape locator/sidecar must include int {key!r}")
    return value


def _required_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise BackendNotFoundError(f"d2tape sidecar must include object {key!r}")
    return value


def _per_file_ok(report: dict[str, Any]) -> bool:
    per_file = report.get("perFile")
    if not isinstance(per_file, list):
        return True
    return all(isinstance(entry, dict) and entry.get("ok") is True for entry in per_file)


def _truncate(value: str, limit: int = 500) -> str:
    value = value.strip()
    if len(value) <= limit:
        return value
    return value[:limit] + "..."
