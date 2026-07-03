"""`sutra backends` — register and list storage backends."""

from __future__ import annotations

import json
from typing import Any

import click
from sqlalchemy import select

from sutradhara.catalog.models import Backend
from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.catalog.types import BackendKind, BackendTier, implementation_family_for_kind
from sutradhara.pools import PoolError, set_pool_retired, set_pool_write_fence


@click.group("backends")
def backends_group() -> None:
    """Register and inspect storage backends."""


@backends_group.command("add")
@click.argument("name")
@click.option(
    "--kind",
    type=click.Choice([k.value for k in BackendKind]),
    required=True,
    help="Backend kind (memory|rem_tape|...).",
)
@click.option(
    "--tier",
    type=click.Choice([t.value for t in BackendTier]),
    default=BackendTier.SELF_DESCRIBING.value,
    show_default=True,
    help="Self-describing (rebuildable) vs catalog-authoritative.",
)
@click.option(
    "--fixture",
    "fixture_path",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Dev fixture file path for rem_tape tests/scrubs.",
)
@click.option(
    "--config",
    "config_pairs",
    multiple=True,
    help="key=value (repeatable). Values are JSON-decoded if possible, else strings.",
)
@click.option(
    "--library-uuid",
    default=None,
    help="Set config.library_uuid for a specific Remanence library.",
)
def backends_add(
    name: str,
    kind: str,
    tier: str,
    fixture_path: str | None,
    config_pairs: tuple[str, ...],
    library_uuid: str | None,
) -> None:
    """Register a new backend."""
    config = _parse_config(config_pairs)
    if fixture_path is not None:
        _put_config(config, "fixture_path", fixture_path, "--fixture")
    if library_uuid is not None:
        _put_config(config, "library_uuid", library_uuid, "--library-uuid")

    engine = make_engine()
    with session_scope(engine) as s:
        s.add(
            Backend(
                name=name,
                kind=BackendKind(kind),
                implementation_family=implementation_family_for_kind(kind),
                tier=BackendTier(tier),
                config=config or None,
            )
        )
    click.echo(f"Registered backend {name!r} (kind={kind}, tier={tier}).")


@backends_group.command("list")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit JSON (one record per line) instead of a table.",
)
def backends_list(as_json: bool) -> None:
    """List registered backends."""
    engine = make_engine()
    with session_scope(engine) as s:
        rows = list(s.scalars(select(Backend).order_by(Backend.name)))

    if not rows:
        click.echo("(no backends registered)")
        return

    if as_json:
        import json as _json

        for r in rows:
            click.echo(
                _json.dumps(
                    {
                        "name": r.name,
                        "kind": r.kind,
                        "implementation_family": r.implementation_family,
                        "tier": r.tier,
                        "config": r.config or {},
                        "added_at": r.added_at.isoformat(),
                    }
                )
            )
        return

    width = max(len(r.name) for r in rows)
    click.echo(f"{'NAME'.ljust(width)}  KIND        FAMILY    TIER")
    click.echo(f"{'-' * width}  ----------  --------  ----------")
    for r in rows:
        click.echo(f"{r.name.ljust(width)}  {r.kind:<10}  {r.implementation_family:<8}  {r.tier}")


@backends_group.command("set-pool-writes")
@click.argument("pool_id")
@click.option(
    "--accepts-writes/--no-accepts-writes",
    default=None,
    required=True,
    help="Enable or disable new writes to this pool.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Override durability-floor drain refusal and record an alarm.",
)
def set_pool_writes_cmd(
    pool_id: str,
    accepts_writes: bool,
    force: bool,
) -> None:
    """Set a pool's write fence with durability-floor validation."""

    engine = make_engine()
    try:
        with session_scope(engine) as s:
            pool = set_pool_write_fence(
                s,
                pool_id,
                accepts_writes=accepts_writes,
                force=force,
            )
            click.echo(f"Pool {pool.id!r}: accepts_writes={pool.accepts_writes}.")
    except PoolError as exc:
        raise click.ClickException(str(exc)) from exc


@backends_group.command("set-pool-retired")
@click.argument("pool_id")
@click.option(
    "--retired/--active",
    default=None,
    required=True,
    help="Set or clear the descriptive retired flag.",
)
def set_pool_retired_cmd(pool_id: str, retired: bool) -> None:
    """Set a pool's descriptive retired flag."""

    engine = make_engine()
    try:
        with session_scope(engine) as s:
            pool = set_pool_retired(s, pool_id, retired=retired)
            click.echo(f"Pool {pool.id!r}: retired={pool.retired}.")
    except PoolError as exc:
        raise click.ClickException(str(exc)) from exc


def _parse_config(pairs: tuple[str, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for pair in pairs:
        if "=" not in pair:
            raise click.UsageError(f"--config {pair!r} must be key=value")
        key, _, raw_value = pair.partition("=")
        try:
            value = json.loads(raw_value)
        except json.JSONDecodeError:
            value = raw_value
        _put_config(out, key, value, f"--config {pair!r}")
    return out


def _put_config(config: dict[str, Any], key: str, value: Any, source: str) -> None:
    if key in config:
        raise click.UsageError(
            f"{source} would overwrite config key {key!r}; pass each config key once"
        )
    config[key] = value
