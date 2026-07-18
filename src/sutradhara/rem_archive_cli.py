"""Shared Remanence archive-build CLI adapter.

Sutradhara has multiple workers that need to create Remanence RAO archive
objects. This module owns the executable discovery, `rem archive build` flag
surface, subprocess error reporting, JSON report parsing, and stored-object
digest calculation so those workers do not grow separate command contracts.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import shutil
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sutradhara.keys import KEY_DOMAINS
from sutradhara.resource_control import run_managed


@dataclass(frozen=True)
class RemArchiveBuildResult:
    """Result of a successful `rem archive build` invocation."""

    artifact_path: Path
    stored_digest: bytes
    stdout_report: dict[str, Any]
    manifest_path: Path | None


def resolve_rem_bin(rem_bin: str | Path | None = None) -> str:
    """Resolve the Remanence CLI path used by Sutradhara archive workers."""

    if rem_bin is not None:
        return _resolve_candidate(str(rem_bin), source="rem_bin")

    env_value = os.environ.get("REM_BIN")
    if env_value:
        return _resolve_candidate(env_value, source="REM_BIN")

    path_match = shutil.which("rem")
    if path_match:
        return path_match

    fallback = _rem_bin_fallback()
    if _is_executable(fallback):
        return str(fallback)

    raise FileNotFoundError(
        "Remanence CLI not found. Set REM_BIN to the rem binary path, install rem "
        f"on PATH, or build {fallback}."
    )


def run_rem_archive_build(
    *,
    inputs: Sequence[Path | str] | None = None,
    ruleset: Path | str | None = None,
    map_path: Path | str | None = None,
    source_root: Path | str | None = None,
    map_sha256: str | None = None,
    output_path: Path,
    manifest_path: Path | None = None,
    rem_bin: str | Path | None = None,
    recipients: Sequence[Path | str] = (),
    chunk_size: int | None = None,
    object_id: str | None = None,
    caller_object_id: str | None = None,
    manifest_file_id: str | None = None,
    timestamp: str | None = None,
    failure_label: str = "rem archive build",
    resource_role: str = "medium",
    cpu_lease: int | None = None,
) -> RemArchiveBuildResult:
    """Run `rem archive build` with the current archive CLI contract."""

    input_paths = tuple(inputs or ())
    if map_path is None and not input_paths:
        raise ValueError("rem archive build requires at least one input")
    if map_path is not None and input_paths:
        raise ValueError("rem archive build --map cannot be combined with inputs")
    if map_path is not None and ruleset is not None:
        raise ValueError("rem archive build --map cannot be combined with ruleset")
    if map_path is not None and source_root is None:
        raise ValueError("rem archive build --map requires source_root")
    if map_path is None and source_root is not None:
        raise ValueError("source_root is only valid with rem archive build --map")
    if map_path is None and map_sha256 is not None:
        raise ValueError("map_sha256 is only valid with rem archive build --map")
    recipient_paths = tuple(Path(path) for path in recipients)
    if recipient_paths and not 2 <= len(recipient_paths) <= 8:
        raise ValueError("encrypted rem archive build requires 2 to 8 recipients")
    if len({path.resolve(strict=False) for path in recipient_paths}) != len(recipient_paths):
        raise ValueError("rem archive build recipients must be distinct")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if manifest_path is not None:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        resolve_rem_bin(rem_bin),
        "archive",
        "build",
    ]
    if map_path is not None:
        cmd.extend(["--map", str(map_path), "--source-root", str(source_root)])
        if map_sha256 is not None:
            cmd.extend(["--map-sha256", map_sha256])
    elif ruleset is not None:
        cmd.extend(["--rules", str(ruleset)])
    cmd.extend(["--out", str(output_path)])
    if manifest_path is not None:
        cmd.extend(["--manifest-out", str(manifest_path)])
    if chunk_size is not None:
        cmd.extend(["--chunk-size", str(chunk_size)])
    if object_id is not None:
        cmd.extend(["--object-id", object_id])
    if caller_object_id is not None:
        cmd.extend(["--caller-object-id", caller_object_id])
    if manifest_file_id is not None:
        cmd.extend(["--manifest-file-id", manifest_file_id])
    for recipient in recipient_paths:
        cmd.extend(["--recipient", str(recipient)])
    if timestamp is not None:
        cmd.extend(["--timestamp", timestamp])
    if map_path is None:
        cmd.extend(["--inputs", *[str(path) for path in input_paths]])

    completed = run_managed(
        cmd,
        role=resource_role,
        cpu_lease=cpu_lease,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{failure_label} failed (exit {completed.returncode}): "
            f"command={shlex.join(cmd)!r} "
            f"stdout={completed.stdout.strip()[:500]!r} "
            f"stderr={completed.stderr.strip()[:500]!r}"
        )
    if not output_path.exists():
        raise RuntimeError(f"{failure_label} did not produce expected output: {output_path}")
    return RemArchiveBuildResult(
        artifact_path=output_path,
        stored_digest=sha256_file(output_path),
        stdout_report=_json_report(completed, failure_label=failure_label),
        manifest_path=manifest_path,
    )


def run_rem_archive_scan(
    *,
    inputs: Sequence[Path | str],
    ruleset: Path | str | None,
    rem_bin: str | Path | None = None,
    failure_label: str = "rem archive scan",
    resource_role: str = "medium",
    cpu_lease: int | None = None,
) -> dict[str, Any]:
    """Run `rem archive build --scan-only` and return its JSON report."""

    if not inputs:
        raise ValueError("rem archive scan requires at least one input")

    cmd = [
        resolve_rem_bin(rem_bin),
        "archive",
        "build",
        "--scan-only",
    ]
    if ruleset is not None:
        cmd.extend(["--rules", str(ruleset)])
    cmd.extend(["--inputs", *[str(path) for path in inputs]])

    completed = run_managed(
        cmd,
        role=resource_role,
        cpu_lease=cpu_lease,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{failure_label} failed (exit {completed.returncode}): "
            f"command={shlex.join(cmd)!r} "
            f"stdout={completed.stdout.strip()[:500]!r} "
            f"stderr={completed.stderr.strip()[:500]!r}"
        )
    return _json_report(completed, failure_label=failure_label)


def sha256_file(path: Path) -> bytes:
    """Return the SHA-256 digest for a file without loading it into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()


def recipient_registry_ids(
    report: dict[str, Any],
    *,
    failure_label: str,
) -> tuple[str, ...]:
    """Parse canonical registry ids from a Remanence recipient report."""

    if report.get("format_version") != 2:
        raise RuntimeError(f"{failure_label} did not report format_version 2")
    raw = report.get("recipient_epochs")
    if not isinstance(raw, list) or not raw:
        raise RuntimeError(f"{failure_label} did not report recipient_epochs")
    labels: list[str] = []
    for index, value in enumerate(raw):
        if not isinstance(value, dict):
            raise RuntimeError(f"{failure_label} recipient_epochs[{index}] is not an object")
        epoch_id = value.get("epoch_id")
        label = value.get("label")
        if not isinstance(epoch_id, str) or len(epoch_id) != 32:
            raise RuntimeError(f"{failure_label} recipient_epochs[{index}] has invalid epoch_id")
        try:
            decoded = bytes.fromhex(epoch_id)
        except ValueError as exc:
            raise RuntimeError(
                f"{failure_label} recipient_epochs[{index}] epoch_id is not hex"
            ) from exc
        if len(decoded) != 16 or epoch_id != epoch_id.lower() or epoch_id == "0" * 32:
            raise RuntimeError(f"{failure_label} recipient_epochs[{index}] has invalid epoch_id")
        if not isinstance(label, str) or label not in KEY_DOMAINS:
            raise RuntimeError(f"{failure_label} recipient_epochs[{index}] has invalid label")
        labels.append(f"{label}-{epoch_id}")
    if len(set(labels)) != len(labels):
        raise RuntimeError(f"{failure_label} reported duplicate recipient epoch labels")
    return tuple(labels)


def _resolve_candidate(value: str, *, source: str) -> str:
    candidate = Path(value).expanduser()
    has_path_separator = os.sep in value or (os.altsep is not None and os.altsep in value)
    if has_path_separator or candidate.is_absolute():
        if _is_executable(candidate):
            return str(candidate)
        raise FileNotFoundError(
            f"{source} points to a non-executable Remanence CLI: {value!r}. "
            "Set REM_BIN to the rem binary path."
        )

    command_match = shutil.which(value)
    if command_match:
        return command_match

    fallback = _rem_bin_fallback()
    if value == "rem" and _is_executable(fallback):
        return str(fallback)

    raise FileNotFoundError(
        f"Remanence CLI {value!r} was not found. Set REM_BIN to the rem binary path."
    )


def _is_executable(path: Path) -> bool:
    return path.is_file() and os.access(path, os.X_OK)


def _rem_bin_fallback() -> Path:
    return Path.home() / "remanence" / "target" / "release" / "rem"


def _json_report(
    result: subprocess.CompletedProcess[str],
    *,
    failure_label: str,
) -> dict[str, Any]:
    for line in result.stdout.splitlines():
        line = line.strip()
        if not (line.startswith("{") and line.endswith("}")):
            continue
        data = json.loads(line)
        if isinstance(data, dict):
            return data
    raise RuntimeError(
        f"{failure_label} emitted no JSON object: "
        f"stdout={result.stdout.strip()[:500]!r} "
        f"stderr={result.stderr.strip()[:500]!r}"
    )
