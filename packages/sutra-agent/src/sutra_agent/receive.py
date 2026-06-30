"""Receive workflow facade for the Sutradhara edge agent.

The agent does not copy bytes or calculate manifests itself. It delegates that
contract-critical work to `sutradhara_receive`, then records local operator state
and maps server confirmation markers to the fail-safe source-release decision.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sutra_agent.config import AgentConfig
from sutra_agent.grpc_client import get_stream_status, stream_source
from sutra_agent.ledger import (
    ConfirmationSnapshot,
    ReceiveRunRecord,
    confirmation_from_wait_result,
    now_iso,
    record_from_receive_result,
    refresh_confirmation_records,
    upsert_receive_record,
)
from sutradhara_receive.cli import (
    ReceiveCliRuntimeError,
    ReceiveCliUsageError,
    run_receive_command,
    run_sweep_command,
)


class AgentReceiveUsageError(ValueError):
    """Raised when an agent receive request has invalid arguments."""


class AgentReceiveRuntimeError(RuntimeError):
    """Raised when an agent receive operation fails at runtime."""


@dataclass(frozen=True)
class AgentReceiveOutcome:
    """Completed agent receive operation and durable ledger record."""

    record: ReceiveRunRecord
    ledger_path: Path

    def payload(self) -> dict[str, Any]:
        """Return stable JSON for CLI output."""

        payload = self.record.payload()
        payload["ledger_path"] = str(self.ledger_path)
        payload["release_ok"] = self.record.confirmation.release_ok
        payload["confirmation_status"] = self.record.confirmation.status
        return payload


@dataclass(frozen=True)
class AgentStatusOutcome:
    """Current confirmation status for one or more tracked receives."""

    records: tuple[ReceiveRunRecord, ...]
    ledger_path: Path

    def payload(self) -> dict[str, Any]:
        """Return stable JSON for CLI output."""

        return {
            "ledger_path": str(self.ledger_path),
            "runs": [record.payload() for record in self.records],
        }


@dataclass(frozen=True)
class AgentSweepOutcome:
    """Stale receive sweep result."""

    removed: tuple[Path, ...]

    def payload(self) -> dict[str, Any]:
        """Return stable JSON for CLI output."""

        return {"removed": [str(path) for path in self.removed]}


def run_agent_receive(
    source: Path | None,
    *,
    config: AgentConfig,
    source_ref: str | None = None,
    label: str | None = None,
    resume: str | None = None,
    fake_source: Path | None = None,
    confirm_timeout: float | None = None,
    confirm_interval: float | None = None,
) -> AgentReceiveOutcome:
    """Run receive through the shared core and upsert the local agent ledger."""

    if config.streaming_enabled:
        selected_source = fake_source if fake_source is not None else source
        if selected_source is None:
            raise AgentReceiveUsageError("streaming receive requires SOURCE or --fake-source")
        try:
            result = stream_source(
                selected_source,
                config=config,
                source_ref=source_ref,
                label=label,
                idempotency_key=resume,
                confirm_timeout=confirm_timeout,
                confirm_interval=confirm_interval,
            )
        except Exception as exc:
            raise AgentReceiveRuntimeError(str(exc)) from exc
        timestamp = now_iso()
        record = ReceiveRunRecord(
            intake_id=result.intake_id,
            intake_dir=Path(result.intake_id),
            landing=Path(config.server_address or "grpc"),
            source=str(selected_source),
            source_kind=config.source_kind,
            artifactclass=config.artifactclass,
            operator="server-assigned",
            file_count=result.file_count,
            total_bytes=result.total_bytes,
            skipped_count=result.skipped_count,
            started_at=timestamp,
            updated_at=timestamp,
            confirmation=result.confirmation,
            resume_of=resume,
        )
        ledger_path = config.resolved_ledger_path()
        upsert_receive_record(ledger_path, record)
        return AgentReceiveOutcome(record=record, ledger_path=ledger_path)

    if config.landing is None or config.operator is None:
        raise AgentReceiveUsageError("legacy receive requires landing and operator")
    try:
        result, wait_result = run_receive_command(
            source,
            landing=config.landing,
            source_kind=config.source_kind,
            operator=config.operator,
            source_ref=source_ref,
            artifactclass=config.artifactclass,
            label=label,
            resume=resume,
            fake_source=fake_source,
            confirm_timeout=confirm_timeout,
            confirm_interval=confirm_interval or config.confirm_interval_seconds,
        )
    except ReceiveCliUsageError as exc:
        raise AgentReceiveUsageError(str(exc)) from exc
    except ReceiveCliRuntimeError as exc:
        raise AgentReceiveRuntimeError(str(exc)) from exc

    selected_source = fake_source if fake_source is not None else source
    confirmation = confirmation_from_wait_result(wait_result)
    record = record_from_receive_result(
        result,
        landing=config.landing,
        source=selected_source,
        source_kind=config.source_kind,
        artifactclass=config.artifactclass,
        operator=config.operator,
        confirmation=confirmation,
        resume_of=resume,
    )
    ledger_path = config.resolved_ledger_path()
    upsert_receive_record(ledger_path, record)
    return AgentReceiveOutcome(record=record, ledger_path=ledger_path)


def refresh_agent_status(
    *,
    config: AgentConfig,
    intake_id: str | None = None,
) -> AgentStatusOutcome:
    """Refresh tracked receive confirmation state from server marker files."""

    ledger_path = config.resolved_ledger_path()
    if config.streaming_enabled:
        from sutra_agent.ledger import ledger_records

        records = []
        now = now_iso()
        for record in ledger_records(ledger_path):
            if intake_id is not None and record.intake_id != intake_id:
                records.append(record)
                continue
            marker = get_stream_status(config, record.intake_id)
            records.append(
                ReceiveRunRecord(
                    intake_id=record.intake_id,
                    intake_dir=record.intake_dir,
                    landing=record.landing,
                    source=record.source,
                    source_kind=record.source_kind,
                    artifactclass=record.artifactclass,
                    operator=record.operator,
                    file_count=record.file_count,
                    total_bytes=record.total_bytes,
                    skipped_count=record.skipped_count,
                    started_at=record.started_at,
                    updated_at=now,
                    confirmation=marker,
                    resume_of=record.resume_of,
                )
            )
        if intake_id is not None and not any(record.intake_id == intake_id for record in records):
            from sutra_agent.ledger import AgentLedgerError

            raise AgentLedgerError(f"intake is not tracked in agent ledger: {intake_id}")
        for record in records:
            upsert_receive_record(ledger_path, record)
        return AgentStatusOutcome(
            records=tuple(record for record in records if intake_id is None or record.intake_id == intake_id),
            ledger_path=ledger_path,
        )
    records = tuple(refresh_confirmation_records(ledger_path, intake_id=intake_id))
    return AgentStatusOutcome(records=records, ledger_path=ledger_path)


def run_agent_sweep(
    *,
    config: AgentConfig,
    older_than_hours: float,
) -> AgentSweepOutcome:
    """Sweep stale sentinel-less receives from the configured landing root."""

    if config.landing is None:
        raise AgentReceiveUsageError("sweep is only available in legacy landing mode")
    result = run_sweep_command(config.landing, older_than_hours=older_than_hours)
    return AgentSweepOutcome(removed=result.removed)


def release_message(confirmation: ConfirmationSnapshot) -> str:
    """Return the operator-facing source-release message."""

    if confirmation.release_ok:
        return "source release: allowed; server verification marker is present"
    if confirmation.status == "quarantined":
        return "source release: blocked; server quarantined this intake"
    if confirmation.status == "discrepancy":
        return "source release: blocked; server reported an intake discrepancy"
    return "source release: blocked; waiting for server verification marker"
