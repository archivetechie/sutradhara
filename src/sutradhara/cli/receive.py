"""`sutra receive` edge-side front-door commands."""

from __future__ import annotations

import json
from pathlib import Path

import click

from sutradhara_receive.cli import (
    SOURCE_KIND_CHOICES,
    ReceiveCliRuntimeError,
    ReceiveCliUsageError,
    ReceiveCliVerificationError,
    default_operator,
    receive_result_payload,
    receive_text_lines,
    run_receive_command,
    run_sweep_command,
    run_verify_pending_command,
    sweep_result_payload,
    sweep_text_lines,
    verify_pending_result_payload,
    verify_pending_text_lines,
)


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
    type=click.Choice(SOURCE_KIND_CHOICES),
    help="Physical or transfer source category.",
)
@click.option("--source-ref", default=None, help="Operator-visible source identifier.")
@click.option(
    "--artifactclass", default="default", show_default=True, help="Artifactclass for items."
)
@click.option("--label", default=None, help="Human label for this intake.")
@click.option(
    "--operator",
    default=default_operator,
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
@click.option(
    "--verify",
    type=click.Choice(["staged", "blocking"]),
    default="staged",
    show_default=True,
    help="Destination verification mode.",
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
    verify: str,
    as_json: bool,
) -> None:
    """Receive SOURCE into LANDING using a sentinel-last filesystem contract."""

    try:
        release_stream_err = as_json
        result, confirmation, _verify_result = run_receive_command(
            source,
            landing=landing,
            source_kind=source_kind,
            operator=operator,
            source_ref=source_ref,
            artifactclass=artifactclass,
            label=label,
            resume=resume,
            fake_source=fake_source,
            verify=verify,
            confirm_timeout=confirm_timeout,
            confirm_interval=confirm_interval,
            release_callback=lambda _result: click.echo(
                "CARD SAFE TO REMOVE — deep verify continuing"
                if verify == "staged"
                else "CARD SAFE TO REMOVE — deep verify complete",
                err=release_stream_err,
            ),
        )
    except ReceiveCliUsageError as exc:
        raise click.UsageError(str(exc)) from exc
    except ReceiveCliVerificationError as exc:
        click.echo(f"error: {exc}", err=True)
        raise click.exceptions.Exit(4) from exc
    except ReceiveCliRuntimeError as exc:
        raise click.ClickException(str(exc)) from exc

    if as_json:
        click.echo(
            json.dumps(receive_result_payload(result, confirmation), indent=2, sort_keys=True)
        )
    else:
        stdout_lines, stderr_lines = receive_text_lines(result, confirmation)
        for line in stdout_lines:
            click.echo(line)
        for line in stderr_lines:
            click.echo(line, err=True)

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

    result = run_sweep_command(landing, older_than_hours=older_than_hours)
    if as_json:
        click.echo(json.dumps(sweep_result_payload(result), indent=2, sort_keys=True))
        return
    for line in sweep_text_lines(result):
        click.echo(line)


@receive_group.command("verify-pending")
@click.option(
    "--landing",
    "landings",
    required=True,
    multiple=True,
    type=click.Path(path_type=Path, file_okay=False),
    help="Landing share to scan. Repeat for multiple roots.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON summary.")
def receive_verify_pending(landings: tuple[Path, ...], as_json: bool) -> None:
    """Verify completed bags with absent, transfer, or failed sidecars."""

    result = run_verify_pending_command(landings)
    if as_json:
        click.echo(json.dumps(verify_pending_result_payload(result), indent=2, sort_keys=True))
        for path in result.failed:
            click.echo(f"destination verification failed: {path}", err=True)
    else:
        stdout_lines, stderr_lines = verify_pending_text_lines(result)
        for line in stdout_lines:
            click.echo(line)
        for line in stderr_lines:
            click.echo(line, err=True)
    if result.failed:
        raise click.exceptions.Exit(4)


receive_group.add_command(receive_sweep_orphans, "sweep")
