"""Command-line interface for the Sutradhara edge receive agent.

`sutra-agent` is the operator-facing wrapper around the shared receive engine. It
stores defaults and a local ledger, but it never accepts source release unless the
server-side intake marker proves the received bag was verified.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, TextIO

from sutra_agent import __version__
from sutra_agent.config import (
    DEFAULT_ARTIFACTCLASS,
    DEFAULT_CHUNK_BYTES,
    DEFAULT_CONFIRM_INTERVAL_SECONDS,
    DEFAULT_PARALLELISM,
    DEFAULT_SOURCE_KIND,
    AgentConfig,
    AgentConfigError,
    config_payload,
    default_config_path,
    default_ledger_path,
    resolve_config,
    write_config,
)
from sutra_agent.controld import ControlDaemon, ControlDaemonError
from sutra_agent.enroll_client import EnrollmentError, enroll_device
from sutra_agent.ledger import AgentLedgerError
from sutra_agent.receive import (
    AgentReceiveRuntimeError,
    AgentReceiveUsageError,
    AgentStatusOutcome,
    refresh_agent_status,
    release_message,
    run_agent_receive,
    run_agent_sweep,
)
from sutradhara_receive.cli import SOURCE_KIND_CHOICES


def build_parser() -> argparse.ArgumentParser:
    """Build the `sutra-agent` argument parser."""

    parser = argparse.ArgumentParser(
        prog="sutra-agent",
        description="Operator-facing Sutradhara edge receive agent.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command")

    config_parser = subparsers.add_parser("config", help="manage local agent config")
    config_subparsers = config_parser.add_subparsers(dest="config_command")
    init_parser = config_subparsers.add_parser("init", help="write local agent config")
    _add_config_path_option(init_parser)
    init_parser.add_argument(
        "--landing",
        type=Path,
        help="Landing share where completed receive intakes appear.",
    )
    init_parser.add_argument(
        "--operator",
        help="Default operator name recorded in receives.",
    )
    init_parser.add_argument("--server", dest="server_address", default=None, help="gRPC server address.")
    init_parser.add_argument("--client-cert", type=Path, default=None, help="Device certificate.")
    init_parser.add_argument("--client-key", type=Path, default=None, help="Device private key.")
    init_parser.add_argument("--ca-cert", type=Path, default=None, help="Server CA certificate.")
    init_parser.add_argument("--device-id", default=None, help="Device id; cert CN is authoritative.")
    init_parser.add_argument(
        "--source-kind",
        choices=SOURCE_KIND_CHOICES,
        default=DEFAULT_SOURCE_KIND,
        help=f"Default source kind. Defaults to {DEFAULT_SOURCE_KIND}.",
    )
    init_parser.add_argument(
        "--artifactclass",
        default=DEFAULT_ARTIFACTCLASS,
        help=f"Default artifactclass. Defaults to {DEFAULT_ARTIFACTCLASS}.",
    )
    init_parser.add_argument(
        "--ledger",
        type=Path,
        default=None,
        help=f"Ledger path. Defaults to {default_ledger_path()}.",
    )
    init_parser.add_argument(
        "--confirm-interval",
        type=float,
        default=DEFAULT_CONFIRM_INTERVAL_SECONDS,
        help="Seconds between server-confirmation polls.",
    )
    init_parser.add_argument("--parallelism", type=int, default=DEFAULT_PARALLELISM)
    init_parser.add_argument("--chunk-bytes", type=int, default=DEFAULT_CHUNK_BYTES)
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing config file.",
    )
    init_parser.add_argument("--json", dest="as_json", action="store_true", help="Emit JSON.")
    init_parser.set_defaults(handler=_handle_config_init)

    show_parser = config_subparsers.add_parser("show", help="show resolved local config")
    _add_config_path_option(show_parser)
    show_parser.add_argument("--json", dest="as_json", action="store_true", help="Emit JSON.")
    show_parser.set_defaults(handler=_handle_config_show)

    receive_parser = subparsers.add_parser("receive", help="start, resume, or sweep receives")
    receive_subparsers = receive_parser.add_subparsers(dest="receive_command")
    run_parser = receive_subparsers.add_parser("run", help="receive a source tree")
    run_parser.add_argument("source", nargs="?", type=Path, metavar="SOURCE")
    _add_runtime_config_options(run_parser)
    run_parser.add_argument("--source-ref", default=None, help="Operator-visible source id.")
    run_parser.add_argument("--label", default=None, help="Human label for this intake.")
    run_parser.add_argument("--resume", default=None, help="Resume a named partial intake id.")
    run_parser.add_argument(
        "--fake-source",
        type=Path,
        default=None,
        help="CI/harness source directory used instead of SOURCE.",
    )
    run_parser.add_argument(
        "--confirm-timeout",
        type=float,
        default=None,
        help="Wait this many seconds for server verification before returning.",
    )
    run_parser.add_argument(
        "--confirm-interval",
        type=float,
        default=None,
        help="Seconds between server-confirmation polls.",
    )
    run_parser.add_argument("--json", dest="as_json", action="store_true", help="Emit JSON.")
    run_parser.set_defaults(handler=_handle_receive_run)

    sweep_parser = receive_subparsers.add_parser("sweep", help="remove stale partial receives")
    _add_runtime_config_options(sweep_parser, include_receive_defaults=False)
    sweep_parser.add_argument(
        "--older-than-hours",
        type=float,
        default=24.0,
        help="Remove `.receiving.json` intakes at least this old.",
    )
    sweep_parser.add_argument("--json", dest="as_json", action="store_true", help="Emit JSON.")
    sweep_parser.set_defaults(handler=_handle_receive_sweep)

    status_parser = subparsers.add_parser("status", help="refresh and show receive status")
    status_parser.add_argument("intake_id", nargs="?")
    _add_runtime_config_options(status_parser, include_receive_defaults=False)
    status_parser.add_argument("--json", dest="as_json", action="store_true", help="Emit JSON.")
    status_parser.set_defaults(handler=_handle_status)

    serve_parser = subparsers.add_parser("serve", help="run the outbound control daemon")
    _add_runtime_config_options(serve_parser, include_receive_defaults=False)
    serve_parser.add_argument(
        "--status",
        action="store_true",
        help="Check local daemon configuration/status and exit.",
    )
    serve_parser.add_argument("--json", dest="as_json", action="store_true", help="Emit JSON.")
    serve_parser.set_defaults(handler=_handle_serve)

    enroll_parser = subparsers.add_parser("enroll", help="enroll this helper device")
    enroll_parser.add_argument("--server", required=True, help="HTTPS API base URL or /api/enroll/csr URL.")
    enroll_parser.add_argument("--device-id", required=True, help="Device id used as certificate CN.")
    enroll_parser.add_argument("--token", required=True, help="One-time enrollment token to submit.")
    enroll_parser.add_argument(
        "--ca-cert",
        type=Path,
        required=True,
        help="Pinned server CA certificate used for the enrollment request.",
    )
    enroll_parser.add_argument("--output-dir", type=Path, default=None, help="Directory for client.key and CSR.")
    enroll_parser.add_argument("--force", action="store_true", help="Overwrite existing key/CSR.")
    enroll_parser.add_argument("--json", dest="as_json", action="store_true", help="Emit JSON.")
    enroll_parser.set_defaults(handler=_handle_enroll)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the `sutra-agent` command."""

    normalized = _normalize_argv(list(argv) if argv is not None else sys.argv[1:])
    parser = build_parser()
    args = parser.parse_args(normalized)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help(sys.stderr)
        return 2
    try:
        return int(handler(args))
    except (AgentConfigError, AgentReceiveUsageError) as exc:
        print(f"usage error: {exc}", file=sys.stderr)
        return 2
    except (
        AgentLedgerError,
        AgentReceiveRuntimeError,
        ControlDaemonError,
        EnrollmentError,
        OSError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def _handle_config_init(args: argparse.Namespace) -> int:
    config = AgentConfig(
        landing=args.landing,
        operator=args.operator,
        source_kind=args.source_kind,
        artifactclass=args.artifactclass,
        ledger_path=args.ledger,
        confirm_interval_seconds=args.confirm_interval,
        server_address=args.server_address,
        client_cert=args.client_cert,
        client_key=args.client_key,
        ca_cert=args.ca_cert,
        device_id=args.device_id,
        parallelism=args.parallelism,
        chunk_bytes=args.chunk_bytes,
    )
    path = write_config(config, args.config, overwrite=args.force)
    payload = {"config_path": str(path), "config": config_payload(config)}
    if args.as_json:
        _write_json(payload, sys.stdout)
    else:
        print(f"wrote config: {path}")
        if config.streaming_enabled:
            print(f"server: {config.server_address}")
            print(f"device: {config.device_id}")
        else:
            print(f"landing: {config.landing}")
            print(f"operator: {config.operator}")
        print(f"ledger: {config.resolved_ledger_path()}")
    return 0


def _handle_config_show(args: argparse.Namespace) -> int:
    config = resolve_config(config_path=args.config)
    payload = {
        "config_path": str(args.config or default_config_path()),
        "config": config_payload(config),
        "ledger_path": str(config.resolved_ledger_path()),
    }
    if args.as_json:
        _write_json(payload, sys.stdout)
    else:
        print(f"config: {payload['config_path']}")
        if config.streaming_enabled:
            print(f"server: {config.server_address}")
            print(f"device: {config.device_id}")
        else:
            print(f"landing: {config.landing}")
            print(f"operator: {config.operator}")
        print(f"source kind: {config.source_kind}")
        print(f"artifactclass: {config.artifactclass}")
        print(f"ledger: {config.resolved_ledger_path()}")
    return 0


def _handle_receive_run(args: argparse.Namespace) -> int:
    config = _resolve_config_from_args(args, include_receive_defaults=True)
    outcome = run_agent_receive(
        args.source,
        config=config,
        source_ref=args.source_ref,
        label=args.label,
        resume=args.resume,
        fake_source=args.fake_source,
        confirm_timeout=args.confirm_timeout,
        confirm_interval=args.confirm_interval,
    )
    if args.as_json:
        _write_json(outcome.payload(), sys.stdout)
    else:
        record = outcome.record
        print(
            f"{record.intake_id}: received {record.file_count} file(s), "
            f"{record.total_bytes} byte(s), skipped={record.skipped_count}"
        )
        print(f"intake: {record.intake_dir}")
        print(f"ledger: {outcome.ledger_path}")
        print(release_message(record.confirmation))
    if args.confirm_timeout is not None and not outcome.record.confirmation.release_ok:
        return 3
    return 0


def _handle_receive_sweep(args: argparse.Namespace) -> int:
    config = _resolve_config_from_args(args, include_receive_defaults=False)
    outcome = run_agent_sweep(config=config, older_than_hours=args.older_than_hours)
    if args.as_json:
        _write_json(outcome.payload(), sys.stdout)
    elif not outcome.removed:
        print("(no stale receives)")
    else:
        for path in outcome.removed:
            print(f"removed {path}")
    return 0


def _handle_status(args: argparse.Namespace) -> int:
    config = _resolve_config_from_args(args, include_receive_defaults=False)
    if not config.resolved_ledger_path().exists():
        if args.intake_id is not None:
            raise AgentLedgerError(f"intake is not tracked in agent ledger: {args.intake_id}")
        records = ()
        outcome = AgentStatusOutcome(records=records, ledger_path=config.resolved_ledger_path())
    else:
        outcome = refresh_agent_status(config=config, intake_id=args.intake_id)
    if args.as_json:
        _write_json(outcome.payload(), sys.stdout)
    elif not outcome.records:
        print("(no tracked receives)")
    else:
        for record in outcome.records:
            print(
                f"{record.intake_id}: {record.confirmation.status}; "
                f"release_ok={str(record.confirmation.release_ok).lower()}"
            )
            print(f"intake: {record.intake_dir}")
            print(release_message(record.confirmation))
    return 0


def _handle_serve(args: argparse.Namespace) -> int:
    config = _resolve_config_from_args(args, include_receive_defaults=False)
    daemon = ControlDaemon(config)
    if args.status:
        payload = daemon.status_payload()
        if args.as_json:
            _write_json(payload, sys.stdout)
        else:
            print(f"server: {payload['server']}")
            print(f"device: {payload['device_id']}")
            print(f"active receives: {len(payload['active_receives'])}")
        return 0
    daemon.run_forever()
    return 0


def _handle_enroll(args: argparse.Namespace) -> int:
    output_dir = args.output_dir or default_config_path().parent
    result = enroll_device(
        server=args.server,
        token=args.token,
        device_id=args.device_id,
        output_dir=output_dir,
        ca_cert=args.ca_cert,
        overwrite=args.force,
    )
    payload = result.payload()
    payload["server"] = args.server
    if args.as_json:
        _write_json(payload, sys.stdout)
    else:
        print(f"device key: {result.client_key}")
        print(f"csr: {result.csr}")
        print(f"client cert: {result.client_cert}")
        print(f"ca cert: {result.ca_cert}")
    return 0


def _add_config_path_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help=f"Agent config path. Defaults to {default_config_path()}.",
    )


def _add_runtime_config_options(
    parser: argparse.ArgumentParser,
    *,
    include_receive_defaults: bool = True,
) -> None:
    _add_config_path_option(parser)
    parser.add_argument(
        "--landing",
        type=Path,
        default=None,
        help="Landing share override.",
    )
    parser.add_argument("--operator", default=None, help="Operator override.")
    parser.add_argument("--server", dest="server_address", default=None, help="gRPC server override.")
    parser.add_argument("--client-cert", type=Path, default=None, help="Device certificate override.")
    parser.add_argument("--client-key", type=Path, default=None, help="Device key override.")
    parser.add_argument("--ca-cert", type=Path, default=None, help="Server CA override.")
    parser.add_argument("--device-id", default=None, help="Device id override.")
    parser.add_argument("--ledger", type=Path, default=None, help="Ledger path override.")
    if include_receive_defaults:
        parser.add_argument(
            "--source-kind",
            choices=SOURCE_KIND_CHOICES,
            default=None,
            help="Source kind override.",
        )
        parser.add_argument(
            "--artifactclass",
            default=None,
            help="Artifactclass override.",
        )


def _resolve_config_from_args(
    args: argparse.Namespace,
    *,
    include_receive_defaults: bool,
) -> AgentConfig:
    return resolve_config(
        config_path=args.config,
        landing=args.landing,
        operator=args.operator,
        source_kind=args.source_kind if include_receive_defaults else None,
        artifactclass=args.artifactclass if include_receive_defaults else None,
        ledger_path=args.ledger,
        confirm_interval_seconds=getattr(args, "confirm_interval", None),
        server_address=args.server_address,
        client_cert=args.client_cert,
        client_key=args.client_key,
        ca_cert=args.ca_cert,
        device_id=args.device_id,
    )


def _normalize_argv(argv: list[str]) -> list[str]:
    if (
        len(argv) >= 1
        and argv[0] == "receive"
        and (len(argv) == 1 or argv[1] not in {"run", "sweep", "--help", "-h"})
    ):
        argv.insert(1, "run")
    return argv


def _write_json(payload: dict[str, Any], stream: TextIO) -> None:
    stream.write(json.dumps(payload, indent=2, sort_keys=True))
    stream.write("\n")


if __name__ == "__main__":
    raise SystemExit(main())
