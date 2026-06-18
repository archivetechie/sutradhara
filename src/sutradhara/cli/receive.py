"""`sutra receive` edge-side front-door commands."""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

import click

from sutradhara.receive import (
    ConfirmationResult,
    ReceiveError,
    ReceiveResult,
    receive_source,
    sweep_orphans,
    wait_for_server_confirmation,
)

_SOURCE_KIND_CHOICES = ("card", "drive", "upload", "handoff", "download", "other")


class _ReceiveGroup(click.Group):
    """Dispatch `sutra receive SOURCE` to the hidden `run` subcommand."""

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        if args and args[0] not in self.commands and args[0] not in {"--help", "-h"}:
            args.insert(0, "run")
        return super().parse_args(ctx, args)


@click.group("receive", cls=_ReceiveGroup)
def receive_group() -> None:
    """Receive source trees into landing intakes."""


@receive_group.command("run", hidden=True)
@click.argument("source", required=False, type=click.Path(path_type=Path, file_okay=False))
@click.option(
    "--landing",
    required=True,
    type=click.Path(path_type=Path, file_okay=False),
    help="Landing share where the completed intake directory will appear.",
)
@click.option(
    "--source-kind",
    required=True,
    type=click.Choice(_SOURCE_KIND_CHOICES),
    help="Physical or transfer source category.",
)
@click.option("--source-ref", default=None, help="Operator-visible source identifier.")
@click.option(
    "--artifactclass", default="default", show_default=True, help="Artifactclass for items."
)
@click.option("--label", default=None, help="Human label for this intake.")
@click.option(
    "--operator",
    default=lambda: os.environ.get("USER") or "operator",
    show_default="$USER",
    help="Operator name included in the intake id and sentinel.",
)
@click.option("--resume", default=None, help="Resume a named sentinel-less intake id.")
@click.option(
    "--fake-source",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="CI/harness source directory used instead of a device adapter.",
)
@click.option(
    "--confirm-timeout",
    type=float,
    default=None,
    help="Poll for server confirmation before reporting source release as safe.",
)
@click.option(
    "--confirm-interval",
    type=float,
    default=1.0,
    show_default=True,
    help="Seconds between server confirmation polls.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def receive_run(
    source: Path | None,
    landing: Path,
    source_kind: str,
    source_ref: str | None,
    artifactclass: str,
    label: str | None,
    operator: str,
    resume: str | None,
    fake_source: Path | None,
    confirm_timeout: float | None,
    confirm_interval: float,
    as_json: bool,
) -> None:
    """Receive SOURCE into LANDING using a sentinel-last filesystem contract."""

    if fake_source is not None and source is not None:
        raise click.UsageError("pass either SOURCE or --fake-source, not both")
    selected_source = fake_source if fake_source is not None else source
    if selected_source is None and resume is None:
        raise click.UsageError("SOURCE is required unless --resume is used")

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
        raise click.ClickException(str(exc)) from exc

    if as_json:
        click.echo(json.dumps(_result_payload(result, confirmation), indent=2, sort_keys=True))
    else:
        _echo_result(result, confirmation)

    if confirmation is not None and not confirmation.release_ok:
        raise click.exceptions.Exit(3)


@receive_group.command("sweep-orphans")
@click.option(
    "--landing",
    required=True,
    type=click.Path(path_type=Path, file_okay=False),
    help="Landing share to scan for stale sentinel-less receives.",
)
@click.option(
    "--older-than-hours",
    type=float,
    default=24.0,
    show_default=True,
    help="Remove `.receiving.json` intakes at least this old.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def receive_sweep_orphans(landing: Path, older_than_hours: float, as_json: bool) -> None:
    """Remove stale sentinel-less receive directories."""

    result = sweep_orphans(
        landing,
        older_than=dt.timedelta(hours=older_than_hours),
    )
    payload = {"removed": [str(path) for path in result.removed]}
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    if not result.removed:
        click.echo("(no stale receives)")
        return
    for path in result.removed:
        click.echo(f"removed {path}")


receive_group.add_command(receive_sweep_orphans, "sweep")


def _result_payload(
    result: ReceiveResult,
    confirmation: ConfirmationResult | None,
) -> dict[str, Any]:
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


def _echo_result(result: ReceiveResult, confirmation: ConfirmationResult | None) -> None:
    click.echo(
        f"{result.intake_id}: received {result.file_count} file(s), "
        f"{result.total_bytes} byte(s), skipped={result.skipped_count}"
    )
    click.echo(f"sentinel: {result.sentinel_path}")
    click.echo(f"bag profile: {result.bag_profile}")
    click.echo(f"manifest: {result.manifest_path}")
    click.echo(f"tagmanifest: {result.tagmanifest_path}")
    if confirmation is None:
        return
    if confirmation.release_ok:
        click.echo("server confirmation: verified; source release allowed")
        return
    if confirmation.status == "quarantined":
        click.echo("server confirmation: quarantined; do not release source", err=True)
        if confirmation.detail is not None:
            click.echo(json.dumps(confirmation.detail, indent=2, sort_keys=True), err=True)
        return
    click.echo("server confirmation: timeout; do not release source", err=True)
