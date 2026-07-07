"""Status-surface tests for online gRPC intakes."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from sutradhara.grpc.status import intake_status, release_safe_for_status
from sutradhara.grpc.store import GrpcIntake


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        # committed: CommitIntake has persisted the digest, so the server owns the
        # received bytes even before the watcher publishes a terminal marker.
        ("committed", True),
        # verifying: this is the public post-commit state while deep verification
        # runs against the server-side copy; eject is safe, but the card must not
        # be wiped until verification later passes.
        ("verifying", True),
        # verified: verification passed, so both eject and later card reuse are safe.
        ("verified", True),
        # discrepancy: verification failed; the operator must retain the original
        # and must not wipe the card.
        ("discrepancy", False),
        # quarantined: validation failed; the operator must retain the original
        # and must not wipe the card.
        ("quarantined", False),
    ],
)
def test_release_safe_status_matrix_for_online_card_intakes(
    tmp_path: Path,
    status: str,
    expected: bool,
) -> None:
    row = _row(tmp_path, source_kind="card")

    assert release_safe_for_status(row, status) is expected


def test_release_safe_is_not_set_for_non_card_sources(tmp_path: Path) -> None:
    row = _row(tmp_path, source_kind="handoff")

    assert release_safe_for_status(row, "verifying") is False
    assert release_safe_for_status(row, "verified") is False


def test_intake_status_marks_committed_card_receive_release_safe(tmp_path: Path) -> None:
    row = _row(tmp_path, state="committed", source_kind="card")

    view = intake_status(row)

    assert view.status == "verifying"
    assert view.errors == []
    assert view.release_safe is True


def test_intake_status_blocks_release_on_failed_terminal_markers(tmp_path: Path) -> None:
    row = _row(tmp_path, state="committed", source_kind="card")
    intake_dir = tmp_path / row.intake_id
    intake_dir.mkdir()
    (intake_dir / "intake.discrepancy.json").write_text(
        '{"details":{"errors":["hash mismatch"]}}',
        encoding="utf-8",
    )

    view = intake_status(row)

    assert view.status == "discrepancy"
    assert view.release_safe is False
    assert view.errors == ["errors: ['hash mismatch']"]


def _row(
    tmp_path: Path,
    *,
    state: str = "streaming",
    source_kind: str = "card",
) -> GrpcIntake:
    now = dt.datetime.now(dt.UTC)
    return GrpcIntake(
        intake_id="intake-1",
        operator="ada",
        device_id="mac-1",
        state=state,
        manifest_digest="a" * 64 if state == "committed" else None,
        card_id="card-1",
        idempotency_key="key-1",
        source_plan_digest="b" * 64,
        artifactclass="s-masters",
        source_kind=source_kind,
        source_ref="card-1",
        label="Card 1",
        landing_root=str(tmp_path),
        created_at=now,
        updated_at=now,
    )
