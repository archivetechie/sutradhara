"""OpenSSL-backed CA, enrollment, and peer-certificate resolution.

The streaming intake port authenticates workstations with mTLS. The client
certificate CN is the device id; this module maps that verified device
certificate fingerprint to the server-assigned operator stored in SQL.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import Engine

from sutradhara.catalog.session import make_session_factory
from sutradhara.grpc import store

DEFAULT_PKI_DIR = Path("/etc/sutradhara/pki")
CA_CERT_NAME = "ca.crt"
CA_KEY_NAME = "ca.key"
SERVER_CERT_NAME = "server.crt"
SERVER_KEY_NAME = "server.key"


class CertificateError(ValueError):
    """Raised when certificate generation, enrollment, or resolution fails."""


@dataclass(frozen=True)
class DeviceCertificate:
    """Signed device-certificate result."""

    device_id: str
    operator: str
    cert_path: Path
    fingerprint: str


@dataclass(frozen=True)
class LocalDeviceMaterial:
    """Locally generated device key and CSR paths."""

    key_path: Path
    csr_path: Path


def ensure_ca(pki_dir: Path | str = DEFAULT_PKI_DIR) -> tuple[Path, Path]:
    """Create the sutradhara CA if missing and return ``(cert, key)`` paths."""

    root = Path(pki_dir)
    root.mkdir(parents=True, exist_ok=True)
    ca_cert = root / CA_CERT_NAME
    ca_key = root / CA_KEY_NAME
    if ca_cert.exists() and ca_key.exists():
        return ca_cert, ca_key
    _require_openssl()
    _run_openssl(["genrsa", "-out", str(ca_key), "4096"])
    os.chmod(ca_key, 0o600)
    _run_openssl(
        [
            "req",
            "-x509",
            "-new",
            "-nodes",
            "-key",
            str(ca_key),
            "-sha256",
            "-days",
            "3650",
            "-subj",
            "/CN=Sutradhara Intake CA",
            "-out",
            str(ca_cert),
        ]
    )
    return ca_cert, ca_key


def ensure_server_certificate(
    pki_dir: Path | str = DEFAULT_PKI_DIR,
    *,
    common_name: str = "localhost",
) -> tuple[Path, Path, Path]:
    """Create a server key/cert signed by the sutradhara CA if missing."""

    root = Path(pki_dir)
    ca_cert, ca_key = ensure_ca(root)
    server_cert = root / SERVER_CERT_NAME
    server_key = root / SERVER_KEY_NAME
    if server_cert.exists() and server_key.exists():
        return ca_cert, server_cert, server_key
    _require_openssl()
    csr = root / "server.csr"
    san = _subject_alt_name(common_name)
    try:
        _run_openssl(["genrsa", "-out", str(server_key), "4096"])
        os.chmod(server_key, 0o600)
        _run_openssl(
            [
                "req",
                "-new",
                "-key",
                str(server_key),
                "-subj",
                f"/CN={common_name}",
                "-addext",
                f"subjectAltName={san}",
                "-out",
                str(csr),
            ]
        )
        _run_openssl(
            [
                "x509",
                "-req",
                "-in",
                str(csr),
                "-CA",
                str(ca_cert),
                "-CAkey",
                str(ca_key),
                "-CAcreateserial",
                "-out",
                str(server_cert),
                "-days",
                "825",
                "-sha256",
                "-copy_extensions",
                "copy",
            ]
        )
    finally:
        csr.unlink(missing_ok=True)
    return ca_cert, server_cert, server_key


def generate_device_csr(
    output_dir: Path | str,
    *,
    device_id: str,
    overwrite: bool = False,
) -> LocalDeviceMaterial:
    """Generate a local device private key and CSR with ``CN = device_id``."""

    root = Path(output_dir).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    key_path = root / "client.key"
    csr_path = root / f"{device_id}.csr"
    if not overwrite and (key_path.exists() or csr_path.exists()):
        raise CertificateError("device key/CSR already exists; pass overwrite=True to replace")
    _require_openssl()
    for path in (key_path, csr_path):
        path.unlink(missing_ok=True)
    _run_openssl(["genrsa", "-out", str(key_path), "4096"])
    os.chmod(key_path, 0o600)
    _run_openssl(
        [
            "req",
            "-new",
            "-key",
            str(key_path),
            "-subj",
            f"/CN={device_id}",
            "-out",
            str(csr_path),
        ]
    )
    return LocalDeviceMaterial(key_path=key_path, csr_path=csr_path)


def sign_device_csr(
    engine: Engine,
    *,
    pki_dir: Path | str,
    csr_path: Path | str,
    token: str,
    cert_path: Path | str | None = None,
) -> DeviceCertificate:
    """Validate a token-bound CSR, sign it, and record the token's operator mapping."""

    csr = Path(csr_path)
    device_id = csr_common_name(csr)
    output = Path(cert_path) if cert_path is not None else csr.with_suffix(".crt")
    factory = make_session_factory(engine)
    with factory.begin() as session:
        grant = store.consume_enroll_token(session, token, device_id=device_id)
    try:
        ca_cert, ca_key = ensure_ca(pki_dir)
        _run_openssl(
            [
                "x509",
                "-req",
                "-in",
                str(csr),
                "-CA",
                str(ca_cert),
                "-CAkey",
                str(ca_key),
                "-CAcreateserial",
                "-out",
                str(output),
                "-days",
                "825",
                "-sha256",
            ]
        )
    except Exception:
        with factory.begin() as session:
            store.release_enroll_token(session, token)
        raise
    fingerprint = cert_fingerprint(output)
    with factory.begin() as session:
        store.record_device_enrollment(
            session,
            device_id=device_id,
            cert_fingerprint=fingerprint,
            operator=grant.operator,
        )
    return DeviceCertificate(
        device_id=device_id,
        operator=grant.operator,
        cert_path=output,
        fingerprint=fingerprint,
    )


def cert_fingerprint(cert_path: Path | str) -> str:
    """Return the SHA-256 fingerprint for a PEM certificate."""

    output = _run_openssl(["x509", "-noout", "-fingerprint", "-sha256", "-in", str(cert_path)])
    value = output.strip().split("=", 1)[-1]
    return store.normalize_fingerprint(value)


def csr_common_name(csr_path: Path | str) -> str:
    """Return the subject CN from a CSR."""

    output = _run_openssl(["req", "-noout", "-subject", "-in", str(csr_path)])
    match = re.search(r"(?:^|[,/= ])CN\s*=\s*([^,/]+)", output)
    if match is None:
        raise CertificateError("CSR subject has no common name")
    return match.group(1).strip()


def resolve_peer_identity(engine: Engine, context: Any) -> store.DeviceIdentity:
    """Resolve a gRPC peer context to an enrolled device/operator identity."""

    device_id, fingerprint = peer_certificate_identity(context)
    factory = make_session_factory(engine)
    with factory() as session:
        return store.resolve_device(session, device_id=device_id, cert_fingerprint=fingerprint)


def peer_certificate_identity(context: Any) -> tuple[str, str]:
    """Extract ``(device_id, fingerprint)`` from a gRPC ServicerContext."""

    if hasattr(context, "device_id") and hasattr(context, "fingerprint"):
        return str(context.device_id), store.normalize_fingerprint(str(context.fingerprint))
    auth_context = context.auth_context()
    common_name = _first_auth_value(auth_context, "x509_common_name")
    if not common_name:
        raise PermissionError("peer certificate has no device common name")
    pem = _first_auth_value(auth_context, "x509_pem_cert")
    if not pem:
        raise PermissionError("peer certificate PEM is unavailable")
    with tempfile.NamedTemporaryFile("wb", suffix=".crt", delete=False) as handle:
        handle.write(pem.encode("utf-8") if isinstance(pem, str) else pem)
        temp_path = Path(handle.name)
    try:
        fingerprint = cert_fingerprint(temp_path)
    finally:
        temp_path.unlink(missing_ok=True)
    return common_name.decode("utf-8") if isinstance(common_name, bytes) else common_name, fingerprint


def load_server_credentials(pki_dir: Path | str) -> tuple[bytes, bytes, bytes]:
    """Return ``(ca_cert, server_cert, server_key)`` bytes for grpc credentials."""

    ca_cert, server_cert, server_key = ensure_server_certificate(pki_dir)
    return ca_cert.read_bytes(), server_cert.read_bytes(), server_key.read_bytes()


def _first_auth_value(auth_context: dict[str, Any], key: str) -> Any | None:
    values = auth_context.get(key)
    if not values:
        return None
    return values[0]


def _subject_alt_name(common_name: str) -> str:
    if common_name in {"localhost", "127.0.0.1"}:
        return "DNS:localhost,IP:127.0.0.1"
    if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", common_name):
        return f"IP:{common_name}"
    return f"DNS:{common_name}"


def _require_openssl() -> None:
    if shutil.which("openssl") is None:
        raise CertificateError("openssl command is required for gRPC PKI operations")


def _run_openssl(args: list[str]) -> str:
    try:
        result = subprocess.run(
            ["openssl", *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise CertificateError(exc.stderr.strip() or str(exc)) from exc
    return result.stdout
