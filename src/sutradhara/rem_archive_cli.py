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
    inputs: Sequence[Path | str],
    ruleset: Path | str | None,
    output_path: Path,
    manifest_path: Path | None = None,
    rem_bin: str | Path | None = None,
    encrypt: bool = False,
    key_id: str | None = None,
    key_file: Path | None = None,
    chunk_size: int | None = None,
    object_id: str | None = None,
    caller_object_id: str | None = None,
    manifest_file_id: str | None = None,
    timestamp: str | None = None,
    failure_label: str = "rem archive build",
) -> RemArchiveBuildResult:
    """Run `rem archive build` with the current archive CLI contract."""

    if not inputs:
        raise ValueError("rem archive build requires at least one input")
    if encrypt and (key_id is None or key_file is None):
        raise ValueError("encrypted rem archive build requires key_id and key_file")
    if not encrypt and (key_id is not None or key_file is not None):
        raise ValueError("key_id/key_file are only valid for encrypted rem archive builds")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if manifest_path is not None:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        resolve_rem_bin(rem_bin),
        "archive",
        "build",
    ]
    if ruleset is not None:
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
    if encrypt:
        cmd.append("--encrypt")
        cmd.extend(["--key-file", str(key_file), "--key-id", str(key_id)])
    if timestamp is not None:
        cmd.extend(["--timestamp", timestamp])
    cmd.extend(["--inputs", *[str(path) for path in inputs]])

    completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
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

    completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
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
