"""Phase-2 receive novelty, durability, estimate, and suppression tests."""

from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.api import store as api_store
from sutradhara.arrangement import create_from_intake
from sutradhara.backend.port import VerifyResult
from sutradhara.catalog.copies import add_copy
from sutradhara.catalog.models import (
    ArtifactClassPolicyRecord,
    ArtifactClassPool,
    Backend,
    Copy,
    IngestItem,
    Intake,
    Pool,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import (
    BackendKind,
    BackendTier,
    CopyHealth,
    CopySource,
    IngestDisposition,
)
from sutradhara.evidence_recorder import record_measured
from sutradhara.intake import register_intake
from sutradhara.receive_novelty import ListingEntry, estimate_listing_novelty, novelty_summary
from sutradhara.sealing.port import Representation


def test_registration_classifies_all_authoritative_dispositions_and_suppression(
    tmp_path: Path,
) -> None:
    engine = make_engine("sqlite:///:memory:")
    create_all(engine)
    payload = b"same server-hashed content"
    try:
        first_root = _write_intake(tmp_path, "first", {"a.mov": payload, "copy.mov": payload})
        under_root = _write_intake(tmp_path, "under", {"b.mov": payload})
        durable_root = _write_intake(tmp_path, "durable", {"c.mov": payload})
        with session_scope(engine) as session:
            session.add(
                ArtifactClassPolicyRecord(
                    artifactclass="masters",
                    ruleset="test.rules.v1",
                    expect="messy",
                    target_bytes=1024,
                    max_age_seconds=3600,
                    restore_preference=[],
                    min_copies=1,
                    min_impl_families=1,
                    staging_config={},
                    policy_sha256="a" * 64,
                )
            )
        with session_scope(engine) as session:
            first = register_intake(session, first_root, artifactclass="masters")
            first_items = list(
                session.scalars(
                    select(IngestItem)
                    .where(IngestItem.intake_id == "first")
                    .order_by(IngestItem.as_received_path)
                )
            )
            assert [item.disposition for item in first_items] == [
                IngestDisposition.NEW,
                IngestDisposition.REVERIFIED,
            ]
            assert all(item.disposition_evaluated_at is not None for item in first_items)
            assert all(item.disposition_policy_generation == "a" * 64 for item in first_items)
            assert first.jobs_submitted == 1

        with session_scope(engine) as session:
            under = register_intake(session, under_root, artifactclass="masters")
            item = session.scalars(
                select(IngestItem).where(IngestItem.intake_id == "under")
            ).one()
            assert item.disposition == IngestDisposition.KNOWN_UNDER_DURABLE
            assert item.prior_intake_id == "first"
            assert item.disposition_evidence["work_suppression_safe"] is False
            assert under.jobs_submitted == 1
            assert len(create_from_intake(session, "under", label="repair").members) == 1
            pool = _add_pool(session, "archive", "masters")
            copy, _ = add_copy(
                session,
                logical_asset_hash=item.logical_asset_hash,
                backend_id=pool.backend_id,
                pool_id=pool.id,
                native_locator={"object": "durable-copy"},
                integrity_hash=item.logical_asset_hash,
                source=CopySource.INGEST,
                health=CopyHealth.OK,
            )
            _qualify_fixture_copy(session, copy)
            assert copy.last_checked_at is not None

        with session_scope(engine) as session:
            durable = register_intake(session, durable_root, artifactclass="masters")
            item = session.scalars(
                select(IngestItem).where(IngestItem.intake_id == "durable")
            ).one()
            assert item.disposition == IngestDisposition.KNOWN_DURABLE
            assert item.prior_intake_id == "under"
            assert item.disposition_evidence["work_suppression_safe"] is True
            assert durable.jobs_submitted == 0
            assert len(create_from_intake(session, "durable", label="suppressed").members) == 0
            assert novelty_summary(session, "durable") == {
                "total": 1,
                "new": 0,
                "known_durable": 1,
                "known_under_durable": 0,
                "reverified": 0,
                "legacy_unknown": 0,
            }
            copy = session.scalars(select(Copy)).one()
            copy.health = CopyHealth.SUSPECT

        with session_scope(engine) as session:
            retried = register_intake(session, durable_root, artifactclass="masters")
            assert retried.jobs_submitted == 1
            assert len(create_from_intake(session, "durable", label="repair-again").members) == 1

        with session_scope(engine) as session:
            legacy = session.scalars(
                select(IngestItem).where(IngestItem.intake_id == "first").limit(1)
            ).one()
            legacy.disposition = IngestDisposition.LEGACY_UNKNOWN
            assert legacy.disposition == IngestDisposition.LEGACY_UNKNOWN
            legacy_summary = novelty_summary(session, "first")
            assert legacy_summary["legacy_unknown"] == 1
            assert legacy_summary["total"] == sum(
                count for bucket, count in legacy_summary.items() if bucket != "total"
            )
    finally:
        engine.dispose()


def test_estimate_and_nothing_new_handshake_are_content_based(tmp_path: Path) -> None:
    engine = make_engine(f"sqlite:///{tmp_path / 'estimate.db'}")
    create_all(engine)
    prior_root = _write_intake(tmp_path, "prior", {"known.mov": b"known"})
    try:
        with session_scope(engine) as session:
            session.add(
                ArtifactClassPolicyRecord(
                    artifactclass="masters",
                    ruleset="test.rules.v1",
                    expect="messy",
                    target_bytes=1024,
                    max_age_seconds=3600,
                    restore_preference=[],
                    min_copies=1,
                    min_impl_families=1,
                    staging_config={},
                    policy_sha256="b" * 64,
                )
            )
        with session_scope(engine) as session:
            register_intake(session, prior_root, artifactclass="masters")
            intake = session.get(Intake, "prior")
            assert intake is not None
            intake.card_id = "card-1"
            item = session.scalars(
                select(IngestItem).where(IngestItem.intake_id == "prior")
            ).one()
            pool = _add_pool(session, "archive", "masters")
            copy, _ = add_copy(
                session,
                logical_asset_hash=item.logical_asset_hash,
                backend_id=pool.backend_id,
                pool_id=pool.id,
                native_locator={"object": "durable-prior"},
                integrity_hash=item.logical_asset_hash,
                source=CopySource.INGEST,
                health=CopyHealth.OK,
            )
            _qualify_fixture_copy(session, copy)
            same = estimate_listing_novelty(
                session,
                card_identity="card-1",
                requester=intake.operator,
                listing=[ListingEntry("known.mov", 5)],
                listing_complete=True,
            )
            changed = estimate_listing_novelty(
                session,
                card_identity="card-1",
                requester=intake.operator,
                listing=[ListingEntry("known.mov", 5), ListingEntry("new.mov", 3)],
                listing_complete=True,
            )
            foreign = estimate_listing_novelty(
                session,
                card_identity="card-1",
                requester="other",
                listing=[ListingEntry("known.mov", 5)],
                listing_complete=True,
            )
            item.disposition = IngestDisposition.LEGACY_UNKNOWN
            legacy = estimate_listing_novelty(
                session,
                card_identity="card-1",
                requester=intake.operator,
                listing=[ListingEntry("known.mov", 5)],
                listing_complete=True,
            )
            item.disposition = IngestDisposition.NEW
        assert same["all_known_estimate"] is True
        assert changed["likely_new"] == 1
        assert changed["all_known_estimate"] is False
        assert foreign["visible"] is False
        assert "prior_intake_id" not in foreign
        assert legacy["all_known_estimate"] is False

        warned = api_store.begin_device_receive_intent(
            engine,
            operator_username="unknown",
            device_id="device-1",
            card_identity="card-1",
            card_label="Card 1",
            idempotency_key="all-known",
            request_hash="same",
            acknowledge_duplicate=False,
            current_listing=[("known.mov", 5)],
            listing_complete=True,
        )
        assert warned.state == "warned"
        assert warned.response_json is not None
        assert warned.response_json["error"] == "nothing_new"
        acknowledged = api_store.begin_device_receive_intent(
            engine,
            operator_username="unknown",
            device_id="device-1",
            card_identity="card-1",
            card_label="Card 1",
            idempotency_key="all-known",
            request_hash="same",
            acknowledge_duplicate=True,
            current_listing=[("known.mov", 5)],
            listing_complete=True,
        )
        assert acknowledged.state == "authorized"
    finally:
        engine.dispose()


def test_under_durable_prior_does_not_block_repair_receive(tmp_path: Path) -> None:
    """A path/size match is not known when its prior asset still needs repair."""

    engine = make_engine(f"sqlite:///{tmp_path / 'under-durable-estimate.db'}")
    create_all(engine)
    seed_root = _write_intake(tmp_path, "seed", {"seed.mov": b"repair me"})
    prior_root = _write_intake(tmp_path, "repair-prior", {"known.mov": b"repair me"})
    try:
        with session_scope(engine) as session:
            session.add(
                ArtifactClassPolicyRecord(
                    artifactclass="masters",
                    ruleset="test.rules.v1",
                    expect="messy",
                    target_bytes=1024,
                    max_age_seconds=3600,
                    restore_preference=[],
                    min_copies=1,
                    min_impl_families=1,
                    staging_config={},
                    policy_sha256="c" * 64,
                )
            )
            _add_pool(session, "repair-archive", "masters")
        with session_scope(engine) as session:
            register_intake(session, seed_root, artifactclass="masters")
            register_intake(session, prior_root, artifactclass="masters")
            prior = session.get(Intake, "repair-prior")
            assert prior is not None
            prior.card_id = "repair-card"
            item = session.scalars(
                select(IngestItem).where(IngestItem.intake_id == prior.intake_id)
            ).one()
            assert item.disposition == IngestDisposition.KNOWN_UNDER_DURABLE
            estimate = estimate_listing_novelty(
                session,
                card_identity="repair-card",
                requester=prior.operator,
                listing=[ListingEntry("known.mov", 9)],
                listing_complete=True,
            )

        assert estimate["match_prior"] == 0
        assert estimate["likely_new"] == 1
        assert estimate["all_known_estimate"] is False
        decision = api_store.begin_device_receive_intent(
            engine,
            operator_username="unknown",
            device_id="device-1",
            card_identity="repair-card",
            card_label="Repair Card",
            idempotency_key="repair-receive",
            request_hash="repair-listing",
            acknowledge_duplicate=False,
            current_listing=[("known.mov", 9)],
            listing_complete=True,
        )
        assert decision.state == "authorized"
    finally:
        engine.dispose()


def _qualify_fixture_copy(session: Session, copy: Copy) -> None:
    """Record explicit read-back evidence for a catalog-only fixture copy."""

    record_measured(
        session,
        copy,
        VerifyResult(ok=True, measured=True, actual_hash=copy.integrity_hash),
        source="fanout",
        execution_id=f"fixture-fanout:{copy.id}",
    )


def _write_intake(root: Path, intake_id: str, files: dict[str, bytes]) -> Path:
    intake_root = root / intake_id
    payload_root = intake_root / "payload"
    payload_root.mkdir(parents=True)
    for relpath, payload in files.items():
        destination = payload_root / relpath
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
    (intake_root / "intake.json").write_text(
        json.dumps(
            {
                "intake_id": intake_id,
                "operator": "unknown",
                "source_kind": "card",
            }
        ),
        encoding="utf-8",
    )
    return intake_root


def _add_pool(session: Session, pool_id: str, artifactclass: str) -> Pool:
    backend = Backend(
        name=f"backend-{pool_id}",
        kind=BackendKind.MEMORY,
        tier=BackendTier.CATALOG_AUTHORITATIVE,
    )
    session.add(backend)
    session.flush()
    pool = Pool(
        id=pool_id,
        backend_id=backend.id,
        representation=Representation.RAW_BYTES.value,
    )
    session.add(pool)
    session.add(ArtifactClassPool(artifactclass=artifactclass, pool_id=pool_id))
    session.flush()
    return pool
