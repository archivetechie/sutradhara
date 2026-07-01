"""Certificate enrollment tests for streaming gRPC intake."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import Engine

from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.grpc import ca, store


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def test_sign_resolve_and_revoke_device_certificate(engine: Engine, tmp_path) -> None:
    pki = tmp_path / "pki"
    material = ca.generate_device_csr(tmp_path / "device", device_id="mac-1")
    with session_scope(engine) as session:
        token = store.issue_enroll_token(session, operator="owner", device_id="mac-1")

    signed = ca.sign_device_csr(
        engine,
        pki_dir=pki,
        csr_path=material.csr_path,
        token=token,
    )

    assert signed.device_id == "mac-1"
    assert signed.cert_path.is_file()
    context = _FakeContext("mac-1", signed.cert_path.read_text(encoding="utf-8"))
    identity = ca.resolve_peer_identity(engine, context)
    assert identity.operator == "owner"
    assert identity.device_id == "mac-1"

    with session_scope(engine) as session:
        store.revoke_device(session, "mac-1")
    with pytest.raises(PermissionError):
        ca.resolve_peer_identity(engine, context)


def test_wrong_enrollment_token_refuses_signing(engine: Engine, tmp_path) -> None:
    material = ca.generate_device_csr(tmp_path / "device", device_id="mac-1")
    with pytest.raises(ValueError, match="unknown"):
        ca.sign_device_csr(
            engine,
            pki_dir=tmp_path / "pki",
            csr_path=material.csr_path,
            token="bad-token",
        )


def test_token_device_id_must_match_csr_common_name(engine: Engine, tmp_path) -> None:
    material = ca.generate_device_csr(tmp_path / "device", device_id="mac-1")
    with session_scope(engine) as session:
        token = store.issue_enroll_token(session, operator="owner", device_id="mac-2")

    with pytest.raises(ValueError, match="common name"):
        ca.sign_device_csr(
            engine,
            pki_dir=tmp_path / "pki",
            csr_path=material.csr_path,
            token=token,
        )


def test_signing_failure_releases_enrollment_token(engine: Engine, tmp_path, monkeypatch) -> None:
    material = ca.generate_device_csr(tmp_path / "device", device_id="mac-1")
    with session_scope(engine) as session:
        token = store.issue_enroll_token(session, operator="owner", device_id="mac-1")

    original_run_openssl = ca._run_openssl

    def fail_device_signing(args: list[str]) -> str:
        if args[:2] == ["x509", "-req"]:
            raise ca.CertificateError("signing failed")
        return original_run_openssl(args)

    monkeypatch.setattr(ca, "_run_openssl", fail_device_signing)
    with pytest.raises(ca.CertificateError, match="signing failed"):
        ca.sign_device_csr(
            engine,
            pki_dir=tmp_path / "pki",
            csr_path=material.csr_path,
            token=token,
        )

    with session_scope(engine) as session:
        row = session.get(store.GrpcEnrollToken, token)
        assert row is not None
        assert row.used_at is None


def test_signing_refuses_other_operator_and_releases_token(engine: Engine, tmp_path) -> None:
    material = ca.generate_device_csr(tmp_path / "device", device_id="mac-1")
    with session_scope(engine) as session:
        store.record_device_enrollment(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
            operator="owner",
        )
        token = store.issue_enroll_token(session, operator="other", device_id="mac-1")

    with pytest.raises(ca.CertificateError, match="different operator"):
        ca.sign_device_csr(
            engine,
            pki_dir=tmp_path / "pki",
            csr_path=material.csr_path,
            token=token,
        )

    with session_scope(engine) as session:
        row = session.get(store.GrpcEnrollToken, token)
        assert row is not None
        assert row.used_at is None
        assert store.resolve_device(
            session,
            device_id="mac-1",
            cert_fingerprint="AA" * 32,
        ).operator == "owner"
        with pytest.raises(PermissionError):
            store.resolve_device(session, device_id="mac-1", cert_fingerprint="BB" * 32)


class _FakeContext:
    def __init__(self, common_name: str, pem: str) -> None:
        self._common_name = common_name
        self._pem = pem

    def auth_context(self) -> dict[str, list[bytes]]:
        return {
            "x509_common_name": [self._common_name.encode("utf-8")],
            "x509_pem_cert": [self._pem.encode("utf-8")],
        }
