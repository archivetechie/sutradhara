"""Standalone edge CLI for Sutradhara's dependency-light receive package.

This module intentionally uses only the Python standard library plus
`sutradhara_receive.core`. It is the edge-installable command surface for
first-contact receive: hash-on-read copy, destination verification, BagIt tag
generation, resume, orphan sweep, and fail-safe server confirmation polling.
The full `sutradhara` server package may wrap these helpers, but the filesystem
contract lives here so edge installs do not need server/database/cloud
dependencies.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, TextIO

from sutradhara_receive.core import (
    ConfirmationResult,
    OrphanSweepResult,
    ReceiveError,
    ReceiveResult,
    receive_source,
    sweep_orphans,
    wait_for_server_confirmation,
)

SOURCE_KIND_CHOICES = ("card", "drive", "upload", "handoff", "download", "other")


class ReceiveCliUsageError(ValueError):
    """A receive command failed before runtime work because its arguments conflict."""


class ReceiveCliRuntimeError(RuntimeError):
    """A receive command failed while reading, writing, verifying, or polling."""


def default_operator() -> str:
    """Return the default operator name used by the edge receive command."""

    return os.environ.get("USER") or "operator"


def run_receive_command(
    source: Path | None,
    *,
    landing: Path,
    source_kind: str,
    operator: str,
    source_ref: str | None = None,
    artifactclass: str = "default",
    label: str | None = None,
    resume: str | None = None,
    fake_source: Path | None = None,
    confirm_timeout: float | None = None,
    confirm_interval: float = 1.0,
) -> tuple[ReceiveResult, ConfirmationResult | None]:
    """Run one receive operation and optional server-confirmation poll."""

    if fake_source is not None and source is not None:
        raise ReceiveCliUsageError("pass either SOURCE or --fake-source, not both")
    selected_source = fake_source if fake_source is not None else source
    if selected_source is None and resume is None:
        raise ReceiveCliUsageError("SOURCE is required unless --resume is used")

    try:
        result = receive_source(
            selected_source,
            landing=landing,
            source_kind=source_kind,
            operator=operator,
            source_ref=source_ref,
            artifactclass=artifactclass,
            label=label,
            resume=resume,
        )
        confirmation = (
            wait_for_server_confirmation(
                result.intake_dir,
                timeout_seconds=confirm_timeout,
                poll_interval_seconds=confirm_interval,
            )
            if confirm_timeout is not None
            else None
        )
    except (FileNotFoundError, ReceiveError, ValueError) as exc:
        raise ReceiveCliRuntimeError(str(exc)) from exc
    return result, confirmation


def run_sweep_command(
    landing: Path,
    *,
    older_than_hours: float = 24.0,
) -> OrphanSweepResult:
    """Remove stale sentinel-less receive directories from a landing root."""

    return sweep_orphans(
        landing,
        older_than=dt.timedelta(hours=older_than_hours),
    )


def receive_result_payload(
    result: ReceiveResult,
    confirmation: ConfirmationResult | None,
) -> dict[str, Any]:
    """Return the stable JSON payload for a completed receive command."""

    payload: dict[str, Any] = {
        "intake_id": result.intake_id,
        "intake_dir": str(result.intake_dir),
        "bag_profile": result.bag_profile,
        "manifest_path": str(result.manifest_path),
        "bag_info_path": str(result.bag_info_path),
        "tagmanifest_path": str(result.tagmanifest_path),
        "sentinel_path": str(result.sentinel_path),
        "file_count": result.file_count,
        "total_bytes": result.total_bytes,
        "skipped_count": result.skipped_count,
    }
    if confirmation is not None:
        payload["confirmation"] = {
            "release_ok": confirmation.release_ok,
            "status": confirmation.status,
            "marker_path": str(confirmation.marker_path) if confirmation.marker_path else None,
            "detail": confirmation.detail,
        }
    return payload


def receive_text_lines(
    result: ReceiveResult,
    confirmation: ConfirmationResult | None,
) -> tuple[list[str], list[str]]:
    """Return human-readable stdout/stderr lines for a receive result."""

    stdout_lines = [
        (
            f"{result.intake_id}: received {result.file_count} file(s), "
            f"{result.total_bytes} byte(s), skipped={result.skipped_count}"
        ),
        f"sentinel: {result.sentinel_path}",
        f"bag profile: {result.bag_profile}",
        f"manifest: {result.manifest_path}",
        f"tagmanifest: {result.tagmanifest_path}",
    ]
    stderr_lines: list[str] = []
    if confirmation is None:
        return stdout_lines, stderr_lines
    if confirmation.release_ok:
        stdout_lines.append("server confirmation: verified; source release allowed")
    elif confirmation.status == "quarantined":
        stderr_lines.append("server confirmation: quarantined; do not release source")
        if confirmation.detail is not None:
            stderr_lines.append(json.dumps(confirmation.detail, indent=2, sort_keys=True))
    elif confirmation.status == "discrepancy":
        stderr_lines.append("server confirmation: discrepancy; do not release source")
        if confirmation.detail is not None:
            stderr_lines.append(json.dumps(confirmation.detail, indent=2, sort_keys=True))
    elif confirmation.status == "pending":
        stderr_lines.append("server confirmation: pending; do not release source")
    else:
        stderr_lines.append("server confirmation: timeout; do not release source")
    return stdout_lines, stderr_lines


def sweep_result_payload(result: OrphanSweepResult) -> dict[str, Any]:
    """Return the stable JSON payload for an orphan sweep command."""

    return {"removed": [str(path) for path in result.removed]}


def sweep_text_lines(result: OrphanSweepResult) -> list[str]:
    """Return human-readable stdout lines for an orphan sweep command."""

    if not result.removed:
        return ["(no stale receives)"]
    return [f"removed {path}" for path in result.removed]


def build_parser() -> argparse.ArgumentParser:
    """Build the standalone `sutra-receive` argument parser."""

    parser = argparse.ArgumentParser(
        prog="sutra-receive",
        description="Receive source trees into Sutradhara landing intakes.",
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser(
        "run",
        help="receive a source tree into a landing intake",
        description="Receive a source tree into a landing intake.",
    )
    run_parser.add_argument("source", nargs="?", type=Path, metavar="SOURCE")
    run_parser.add_argument(
        "--landing",
        required=True,
        type=Path,
        help="Landing share where the completed intake directory will appear.",
    )
    run_parser.add_argument(
        "--source-kind",
        required=True,
        choices=SOURCE_KIND_CHOICES,
        help="Physical or transfer source category.",
    )
    run_parser.add_argument("--source-ref", default=None, help="Operator-visible source id.")
    run_parser.add_argument(
        "--artifactclass",
        default="default",
        help="Artifactclass for items. Defaults to %(default)s.",
    )
    run_parser.add_argument("--label", default=None, help="Human label for this intake.")
    run_parser.add_argument(
        "--operator",
        default=default_operator(),
        help="Operator name included in the intake id and sentinel. Defaults to $USER.",
    )
    run_parser.add_argument(
        "--resume", default=None, help="Resume a named sentinel-less intake id."
    )
    run_parser.add_argument(
        "--fake-source",
        type=Path,
        default=None,
        help="CI/harness source directory used instead of a device adapter.",
    )
    run_parser.add_argument(
        "--confirm-timeout",
        type=float,
        default=None,
        help="Poll for server confirmation before reporting source release as safe.",
    )
    run_parser.add_argument(
        "--confirm-interval",
        type=float,
        default=1.0,
        help="Seconds between server confirmation polls. Defaults to %(default)s.",
    )
    run_parser.add_argument("--json", dest="as_json", action="store_true", help="Emit JSON.")
    run_parser.set_defaults(handler=_handle_receive)

    sweep_parser = subparsers.add_parser(
        "sweep",
        aliases=["sweep-orphans"],
        help="remove stale sentinel-less receive directories",
        description="Remove stale sentinel-less receive directories.",
    )
    sweep_parser.add_argument(
        "--landing",
        required=True,
        type=Path,
        help="Landing share to scan for stale sentinel-less receives.",
    )
    sweep_parser.add_argument(
        "--older-than-hours",
        type=float,
        default=24.0,
        help="Remove `.receiving.json` intakes at least this old. Defaults to %(default)s.",
    )
    sweep_parser.add_argument("--json", dest="as_json", action="store_true", help="Emit JSON.")
    sweep_parser.set_defaults(handler=_handle_sweep)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the standalone `sutra-receive` command."""

    parser = build_parser()
    normalized = _normalize_argv(list(sys.argv[1:] if argv is None else argv))
    try:
        args = parser.parse_args(normalized)
        handler = getattr(args, "handler", None)
        if handler is None:
            parser.print_help(sys.stderr)
            return 2
        return int(handler(args))
    except ReceiveCliUsageError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except ReceiveCliRuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("error: interrupted", file=sys.stderr)
        return 130


def _normalize_argv(argv: list[str]) -> list[str]:
    if not argv:
        return ["run"]
    commands = {"run", "sweep", "sweep-orphans"}
    if argv[0] in commands or argv[0] in {"--help", "-h"}:
        return argv
    return ["run", *argv]


def _handle_receive(args: argparse.Namespace) -> int:
    result, confirmation = run_receive_command(
        args.source,
        landing=args.landing,
        source_kind=args.source_kind,
        operator=args.operator,
        source_ref=args.source_ref,
        artifactclass=args.artifactclass,
        label=args.label,
        resume=args.resume,
        fake_source=args.fake_source,
        confirm_timeout=args.confirm_timeout,
        confirm_interval=args.confirm_interval,
    )
    if args.as_json:
        _write_json(receive_result_payload(result, confirmation), stream=sys.stdout)
    else:
        stdout_lines, stderr_lines = receive_text_lines(result, confirmation)
        _write_lines(stdout_lines, stream=sys.stdout)
        _write_lines(stderr_lines, stream=sys.stderr)
    if confirmation is not None and not confirmation.release_ok:
        return 3
    return 0


def _handle_sweep(args: argparse.Namespace) -> int:
    result = run_sweep_command(args.landing, older_than_hours=args.older_than_hours)
    if args.as_json:
        _write_json(sweep_result_payload(result), stream=sys.stdout)
    else:
        _write_lines(sweep_text_lines(result), stream=sys.stdout)
    return 0


def _write_json(payload: dict[str, Any], *, stream: TextIO) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True), file=stream)


def _write_lines(lines: Sequence[str], *, stream: TextIO) -> None:
    for line in lines:
        print(line, file=stream)


if __name__ == "__main__":
    raise SystemExit(main())
