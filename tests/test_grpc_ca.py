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
        token = store.issue_enroll_token(session)

    signed = ca.sign_device_csr(
        engine,
        pki_dir=pki,
        csr_path=material.csr_path,
        token=token,
        operator="owner",
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
            operator="owner",
        )


class _FakeContext:
    def __init__(self, common_name: str, pem: str) -> None:
        self._common_name = common_name
        self._pem = pem

    def auth_context(self) -> dict[str, list[bytes]]:
        return {
            "x509_common_name": [self._common_name.encode("utf-8")],
            "x509_pem_cert": [self._pem.encode("utf-8")],
        }
