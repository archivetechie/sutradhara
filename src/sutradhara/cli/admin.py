"""`sutra admin` — dangerous local maintenance commands."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import click

from sutradhara.catalog.session import database_url, make_engine, reset_all
from sutradhara.keys import KeyRegistry
from sutradhara.sealing.rao import resolve_rem_bin


@dataclass(frozen=True)
class _Diagnostic:
    name: str
    ok: bool
    detail: str


@click.group("admin")
def admin_group() -> None:
    """Dangerous local catalog maintenance."""


@admin_group.command("reset")
@click.option(
    "--i-mean-it",
    is_flag=True,
    default=False,
    help="Required confirmation: drops and recreates the catalog schema.",
)
@click.option(
    "--echo",
    is_flag=True,
    default=False,
    help="Echo SQL during DDL.",
)
def admin_reset(i_mean_it: bool, echo: bool) -> None:
    """Drop and recreate the configured catalog database schema."""
    if not i_mean_it:
        raise click.UsageError("admin reset requires --i-mean-it")

    url = database_url()
    click.echo(f"Resetting catalog schema at {url}")
    engine = make_engine(echo=echo)
    reset_all(engine)
    click.echo("OK.")


@admin_group.command("doctor")
@click.option(
    "--strict",
    is_flag=True,
    default=False,
    help="Exit non-zero if any diagnostic reports WARN.",
)
def admin_doctor(strict: bool) -> None:
    """Report local operational readiness for optional Scenario O seams."""
    diagnostics = [_rem_diagnostic(), _key_registry_diagnostic()]
    for diagnostic in diagnostics:
        status = "OK" if diagnostic.ok else "WARN"
        click.echo(f"{diagnostic.name}: {status} - {diagnostic.detail}")

    if strict and any(not diagnostic.ok for diagnostic in diagnostics):
        raise click.ClickException("one or more diagnostics reported WARN")


def _rem_diagnostic() -> _Diagnostic:
    try:
        resolved = resolve_rem_bin()
    except FileNotFoundError as exc:
        return _Diagnostic(
            "rem",
            False,
            f"{exc} Set REM_BIN or install rem in the default location.",
        )

    path = Path(resolved)
    if path.exists() and os.access(path, os.X_OK):
        return _Diagnostic("rem", True, f"using {resolved}")
    return _Diagnostic(
        "rem",
        False,
        f"resolved to {resolved}, but it is not an executable file",
    )


def _key_registry_diagnostic() -> _Diagnostic:
    registry_dir = KeyRegistry().registry_dir
    if registry_dir.exists():
        if registry_dir.is_dir() and os.access(registry_dir, os.R_OK | os.W_OK | os.X_OK):
            return _Diagnostic("key-registry", True, f"{registry_dir} is accessible")
        return _Diagnostic(
            "key-registry",
            False,
            f"{registry_dir} exists but is not a readable/writable/searchable directory",
        )

    parent = registry_dir.parent
    if parent.exists() and os.access(parent, os.W_OK | os.X_OK):
        return _Diagnostic(
            "key-registry",
            True,
            f"{registry_dir} does not exist yet; parent {parent} can create it",
        )
    return _Diagnostic(
        "key-registry",
        False,
        f"{registry_dir} does not exist and parent {parent} is not writable; "
        "create it with service ownership or set SUTRADHARA_KEY_REGISTRY_DIR",
    )
