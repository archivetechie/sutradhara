"""HD cache disk lifecycle commands."""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click
from sqlalchemy import Engine

from sutradhara.catalog.session import make_engine, session_scope
from sutradhara.hdcache.alarms import walker_event_alarm_sink
from sutradhara.hdcache.fill import (
    HdcacheFillPlan,
    enqueue_requested_fill,
    fill_config_from_env,
    top_up_lost_entries,
)
from sutradhara.hdcache.lifecycle import (
    BlockDeviceCandidate,
    HdcacheLifecycleManager,
    LifecycleError,
    add_result_payload,
    dead_result_payload,
    disk_payload,
    load_hmac_secret_from_env,
    status_payload,
)
from sutradhara.hdcache.models import CacheDisk
from sutradhara.hdcache.repopulate import drill_status
from sutradhara.hdcache.store import StoreError
from sutradhara.hdcache.walker import (
    HdcacheWalkerConfig,
    HdcacheWalkerEvent,
    rebuild_hdcache,
    walk_all_disks,
    walk_disk,
)

ManagerFactory = Callable[[], HdcacheLifecycleManager]
_MANAGER_FACTORY: ManagerFactory | None = None
CLI_ERRORS = (LifecycleError, OSError, StoreError)
DEFAULT_FILL_CONFIRM_THRESHOLD_BYTES = 100 * 1024**3


@click.group("hdcache")
def hdcache_group() -> None:
    """Manage the expendable HD cache disk tier."""


@hdcache_group.group("disk")
def disk_group() -> None:
    """Manage enrolled hdcache disks."""


@disk_group.command("add")
@click.argument("block_dev", required=False)
@click.option("--scan", "scan_mode", is_flag=True, default=False, help="Scan unenrolled disks.")
@click.option("--yes", is_flag=True, default=False, help="Confirm batch enrollment for --scan.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def disk_add_cmd(
    block_dev: str | None,
    scan_mode: bool,
    yes: bool,
    as_json: bool,
) -> None:
    """Enroll one block device or scan/enroll all candidates."""

    manager = _manager()
    try:
        if scan_mode:
            if not yes:
                candidates = manager.scan()
                _emit_scan(candidates, as_json=as_json)
                return
            results = manager.add_scan()
        else:
            if block_dev is None:
                raise click.UsageError("provide BLOCK_DEV or --scan")
            results = [manager.add_disk(block_dev)]
    except CLI_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    payload = [add_result_payload(result) for result in results]
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    if not payload:
        click.echo("no unenrolled disks found")
    for item in payload:
        click.echo(
            "enrolled "
            f"{item['disk_id']} serial={item['serial']} "
            f"slot={item['slot'] or '-'} mount={item['mount']}"
        )


@disk_group.command("list")
@click.option("--all", "include_dead", is_flag=True, default=False, help="Include dead disks.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def disk_list_cmd(include_dead: bool, as_json: bool) -> None:
    """List enrolled disks."""

    manager = _manager()
    try:
        rows = [disk_payload(row) for row in manager.disks(include_dead=include_dead)]
    except CLI_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    if as_json:
        click.echo(json.dumps({"disks": rows}, indent=2, sort_keys=True))
        return
    for row in rows:
        click.echo(
            f"{row['disk_id']} {row['state']} serial={row['serial']} "
            f"slot={row['slot'] or '-'} filled={row['filled_bytes']}/{row['capacity_bytes']} "
            f"smart={row['smart_status'] or '-'}"
        )


@disk_group.command("locate")
@click.argument("disk_id")
def disk_locate_cmd(disk_id: str) -> None:
    """Blink or identify a physical disk, best effort."""

    try:
        click.echo(_manager().locate(disk_id))
    except CLI_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc


@disk_group.command("retire")
@click.argument("disk_id")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def disk_retire_cmd(disk_id: str, as_json: bool) -> None:
    """Mark a disk retiring; entries stay present and servable."""

    try:
        row = _manager().retire(disk_id)
    except CLI_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    payload = disk_payload(row)
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(f"{disk_id}: state=retiring; done when entries migrate to other disks")


@disk_group.command("dead")
@click.argument("disk_id")
@click.option("--yes", is_flag=True, default=False, help="Confirm immediate loss marking.")
@click.option(
    "--confirm-mounted",
    is_flag=True,
    default=False,
    help="Confirm marking a disk dead even though it still appears mounted.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def disk_dead_cmd(disk_id: str, yes: bool, confirm_mounted: bool, as_json: bool) -> None:
    """Mark a disk gone now and flip entries to lost in bounded batches."""

    if not yes:
        click.echo(
            f"{disk_id}: would mark disk dead, drop its LUKS key-slot association, "
            "and mark entries lost; pass --yes to proceed"
        )
        return
    try:
        result = _manager().mark_dead(disk_id, confirm_mounted=confirm_mounted)
    except CLI_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    payload = dead_result_payload(result)
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    click.echo(
        f"{disk_id}: dead; entries_lost={result.entries_lost} "
        f"batches={result.batches}; done when repopulation clears lost backlog"
    )
    if result.luks_key_drop:
        click.echo(result.luks_key_drop)


@disk_group.command("forget")
@click.argument("disk_id")
def disk_forget_cmd(disk_id: str) -> None:
    """Validate that a dead disk has no cache entries and keep its id tombstoned."""

    try:
        _manager().forget(disk_id)
    except CLI_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"{disk_id}: forgotten for operations; disk id retained as a tombstone")


@hdcache_group.command("status")
@click.option("--disks", "show_disks", is_flag=True, default=False, help="Include disk rows.")
@click.option("--disk", "disk_id", default=None, help="Show one disk in detail.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def hdcache_status_cmd(show_disks: bool, disk_id: str | None, as_json: bool) -> None:
    """Show hdcache disk summary."""

    manager = _manager()
    try:
        if disk_id is not None:
            rows = [row for row in manager.disks(include_dead=True) if row.disk_id == disk_id]
            if not rows:
                raise click.ClickException(f"unknown cache disk: {disk_id}")
            payload: dict[str, Any] = {"disk": disk_payload(rows[0])}
        else:
            payload = {"summary": status_payload(manager.status())}
            if show_disks:
                payload["disks"] = [disk_payload(row) for row in manager.disks(include_dead=True)]
    except CLI_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    if "disk" in payload:
        row = payload["disk"]
        click.echo(
            f"{row['disk_id']} {row['state']} serial={row['serial']} "
            f"slot={row['slot'] or '-'} mount={row['mount']}"
        )
        return
    summary = payload["summary"]
    click.echo(
        f"disks={summary['disks_total']} capacity={summary['capacity_bytes']} "
        f"filled={summary['filled_bytes']} states={summary['by_state']}"
    )
    for row in summary["worst_disks"]:
        click.echo(f"{row['disk_id']} {row['state']} smart={row['smart_status'] or '-'}")


@hdcache_group.command("fill")
@click.argument("selector")
@click.option("--dry-run", is_flag=True, default=False, help="Only print count and bytes.")
@click.option("--yes", is_flag=True, default=False, help="Confirm large class fill requests.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
@click.option(
    "--confirm-threshold-bytes",
    type=int,
    default=DEFAULT_FILL_CONFIRM_THRESHOLD_BYTES,
    show_default=True,
    help="Require --yes above this planned byte count.",
)
def hdcache_fill_cmd(
    selector: str,
    dry_run: bool,
    yes: bool,
    as_json: bool,
    confirm_threshold_bytes: int,
) -> None:
    """Schedule hdcache fills for one sha256 or artifactclass."""

    engine = make_engine()
    config = fill_config_from_env()
    try:
        with session_scope(engine) as session:
            dry = enqueue_requested_fill(session, selector, config=config, dry_run=True)
            if dry_run:
                _emit_fill_plan(dry, selector=selector, dry_run=True, as_json=as_json)
                return
            if dry.bytes_total > confirm_threshold_bytes and not yes:
                raise click.ClickException(
                    f"{selector}: would fill {dry.count} asset(s), {dry.bytes_total} bytes; "
                    "pass --yes or use --dry-run"
                )
            plan = enqueue_requested_fill(session, selector, config=config, dry_run=False)
    except (ValueError, RuntimeError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    _emit_fill_plan(plan, selector=selector, dry_run=False, as_json=as_json)


@hdcache_group.group("drill")
def drill_group() -> None:
    """Inspect dead-disk repopulation drills."""


@drill_group.command("status")
@click.argument("disk_id", required=False)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def drill_status_cmd(disk_id: str | None, as_json: bool) -> None:
    """Show remaining/refilled counts and ETA for repopulation drills."""

    engine = make_engine()
    with session_scope(engine) as session:
        rows = drill_status(session, disk_id)
    payload = {"drills": [_drill_payload(row) for row in rows]}
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    if not rows:
        click.echo("no hdcache repopulation drills")
        return
    for row in payload["drills"]:
        eta = "-" if row["eta_seconds"] is None else f"{row['eta_seconds']:.0f}s"
        rate = "-" if row["bytes_per_hour"] is None else f"{row['bytes_per_hour']:.0f} B/hr"
        state = "complete" if row["completed"] else "active"
        click.echo(
            f"{row['disk_id']} {state} drill={row['drill_id']} "
            f"remaining={row['remaining_entries']}/{row['remaining_bytes']} "
            f"refilled={row['refilled_entries']}/{row['refilled_bytes']} "
            f"rate={rate} eta={eta}"
        )


@hdcache_group.command("walk")
@click.argument("disk_id", required=False)
@click.option("--read-only", is_flag=True, default=False, help="Do not delete or mark entries lost.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def hdcache_walk_cmd(disk_id: str | None, read_only: bool, as_json: bool) -> None:
    """Run the hdcache disk walker for one disk or all disks."""

    engine = make_engine()
    events: list[dict[str, object]] = []
    try:
        with session_scope(engine) as session:
            alarm_sink = walker_event_alarm_sink(session=session)

            def event_sink(event: HdcacheWalkerEvent) -> None:
                events.append(dataclasses_asdict(event))
                alarm_sink(event)

            config = HdcacheWalkerConfig(
                hmac_secret=load_hmac_secret_from_env(),
                event_sink=event_sink,
            )
            if disk_id is None:
                results = walk_all_disks(session, config=config, destructive=not read_only)
            else:
                disk = session.get(CacheDisk, disk_id)
                if disk is None:
                    raise click.ClickException(f"unknown cache disk: {disk_id}")
                results = [walk_disk(session, disk, config=config, destructive=not read_only)]
    except CLI_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    payload = {
        "results": [dataclasses_asdict(result) for result in results],
        "events": events,
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    for result in results:
        click.echo(
            f"{result.disk_id}: destructive={result.destructive} halted={result.halted} "
            f"lost={result.entries_lost} unknown_deleted={result.unknown_deleted} "
            f"tmp_deleted={result.tmp_deleted} filled={result.filled_bytes}"
        )
    for event in events:
        click.echo(f"event {event['code']} disk={event.get('disk_id') or '-'}")


@hdcache_group.command("rebuild")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit JSON.")
def hdcache_rebuild_cmd(as_json: bool) -> None:
    """Rebuild untrusted cache rows from self-describing disk filenames."""

    engine = make_engine()
    events: list[dict[str, object]] = []
    try:
        with session_scope(engine) as session:
            alarm_sink = walker_event_alarm_sink(session=session)

            def event_sink(event: HdcacheWalkerEvent) -> None:
                events.append(dataclasses_asdict(event))
                alarm_sink(event)

            config = HdcacheWalkerConfig(
                hmac_secret=load_hmac_secret_from_env(),
                event_sink=event_sink,
            )
            result = rebuild_hdcache(session, config=config)
    except CLI_ERRORS as exc:
        raise click.ClickException(str(exc)) from exc
    payload = {
        "entries": result.entries,
        "failures": [dataclasses_asdict(failure) for failure in result.failures],
        "disks": [dataclasses_asdict(disk) for disk in result.disks],
        "events": events,
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    for disk in result.disks:
        status = "rejected" if disk.rejected else "ok"
        click.echo(
            f"{disk.index}/{disk.total} {disk.disk_id}: {status}; "
            f"entries={disk.entries} elapsed={disk.elapsed_seconds:.2f}s"
        )
    if result.failures:
        click.echo(f"withheld={len(result.failures)}")


def set_manager_factory(factory: ManagerFactory | None) -> None:
    """Install a test-only manager factory."""

    global _MANAGER_FACTORY
    _MANAGER_FACTORY = factory


def _manager() -> HdcacheLifecycleManager:
    if _MANAGER_FACTORY is not None:
        return _MANAGER_FACTORY()
    try:
        engine = make_engine()
        return HdcacheLifecycleManager(
            engine,
            mount_root=Path("/srv/hdcache"),
            hmac_secret=load_hmac_secret_from_env(),
            on_entries_lost=lambda _disk_id, _count: _top_up_lost_entries(engine),
        )
    except (OSError, StoreError) as exc:
        raise click.ClickException(str(exc)) from exc


def _top_up_lost_entries(engine: Engine) -> None:
    with session_scope(engine) as session:
        top_up_lost_entries(session, config=fill_config_from_env())


def _emit_fill_plan(
    plan: HdcacheFillPlan,
    *,
    selector: str,
    dry_run: bool,
    as_json: bool,
) -> None:
    payload = {
        "selector": selector,
        "count": plan.count,
        "bytes": plan.bytes_total,
        "scheduled": plan.scheduled,
        "dry_run": dry_run,
        "live_job_cap": fill_config_from_env().live_job_cap,
    }
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    prefix = "would fill" if dry_run else "scheduled"
    click.echo(
        f"{selector}: {prefix} {payload['scheduled'] if not dry_run else payload['count']} "
        f"of {payload['count']} asset(s), bytes={payload['bytes']}"
    )


def _drill_payload(row: object) -> dict[str, object]:
    payload = dataclasses_asdict(row)
    started_at = payload.get("started_at")
    if hasattr(started_at, "isoformat"):
        payload["started_at"] = started_at.isoformat()
    return payload


def _emit_scan(candidates: list[BlockDeviceCandidate], *, as_json: bool) -> None:
    payload = [dataclasses_asdict(candidate) for candidate in candidates]
    if as_json:
        click.echo(json.dumps({"candidates": payload}, indent=2, sort_keys=True))
        return
    if not payload:
        click.echo("no unenrolled disks found")
        return
    for item in payload:
        click.echo(
            f"{item['block_dev']} serial={item['serial']} "
            f"slot={item['slot'] or '-'} capacity={item['capacity_bytes']}"
        )


def dataclasses_asdict(value: object) -> dict[str, object]:
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    return {key: getattr(value, key) for key in getattr(value, "__dataclass_fields__", {})}
