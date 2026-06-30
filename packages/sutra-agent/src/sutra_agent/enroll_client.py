"""Enrollment client for CA-pinned operator-console helper certificates.

The helper generates its device key and CSR locally, then redeems an
operator-scoped token against the server's pre-cert enrollment endpoint. The
HTTP request is made with an explicit CA bundle; there is no trust-on-first-use
path.
"""

from __future__ import annotations

import json
import os
import ssl
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class EnrollmentError(RuntimeError):
    """Raised when device enrollment fails."""


@dataclass(frozen=True)
class DeviceMaterial:
    """Locally generated device key and CSR."""

    key_path: Path
    csr_path: Path


@dataclass(frozen=True)
class EnrollmentResult:
    """Files written by a successful helper enrollment."""

    device_id: str
    client_key: Path
    csr: Path
    client_cert: Path
    ca_cert: Path

    def payload(self) -> dict[str, str]:
        """Return stable JSON for CLI output."""

        return {
            "device_id": self.device_id,
            "client_key": str(self.client_key),
            "csr": str(self.csr),
            "client_cert": str(self.client_cert),
            "ca_cert": str(self.ca_cert),
        }


PostJson = Callable[[str, dict[str, str], Path], dict[str, str]]


def enroll_device(
    *,
    server: str,
    token: str,
    device_id: str,
    output_dir: Path,
    ca_cert: Path,
    overwrite: bool = False,
    post_json: PostJson | None = None,
) -> EnrollmentResult:
    """Generate a key/CSR, redeem it, and store the returned cert and CA."""

    output_dir.mkdir(parents=True, exist_ok=True)
    cert_path = output_dir / "client.crt"
    returned_ca_path = output_dir / "ca.crt"
    if not overwrite and (cert_path.exists() or returned_ca_path.exists()):
        raise EnrollmentError("client cert/CA already exists; pass --force to replace")
    material = generate_device_material(output_dir, device_id=device_id, overwrite=overwrite)
    csr_pem = material.csr_path.read_text(encoding="utf-8")
    post = post_json or post_enrollment_csr
    response = post(
        enroll_url(server),
        {"csr_pem": csr_pem, "token": token},
        ca_cert,
    )
    cert_pem = _required_response_string(response, "cert_pem")
    ca_pem = _required_response_string(response, "ca_pem")
    cert_path.write_text(cert_pem, encoding="utf-8")
    returned_ca_path.write_text(ca_pem, encoding="utf-8")
    return EnrollmentResult(
        device_id=device_id,
        client_key=material.key_path,
        csr=material.csr_path,
        client_cert=cert_path,
        ca_cert=returned_ca_path,
    )


def generate_device_material(
    output_dir: Path,
    *,
    device_id: str,
    overwrite: bool = False,
) -> DeviceMaterial:
    """Generate a device private key and CSR with ``CN = device_id``."""

    key_path = output_dir / "client.key"
    csr_path = output_dir / f"{device_id}.csr"
    if not overwrite and (key_path.exists() or csr_path.exists()):
        raise EnrollmentError("client key/CSR already exists; pass --force to replace")
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
    return DeviceMaterial(key_path=key_path, csr_path=csr_path)


def post_enrollment_csr(url: str, payload: dict[str, str], ca_cert: Path) -> dict[str, str]:
    """POST the CSR redemption payload using an explicit CA certificate."""

    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    context = ssl.create_default_context(cafile=str(ca_cert))
    try:
        with urllib.request.urlopen(request, context=context, timeout=30) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise EnrollmentError(_error_detail(detail) or f"enrollment failed: HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise EnrollmentError(f"enrollment request failed: {exc.reason}") from exc
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EnrollmentError("enrollment response was not valid JSON") from exc
    if not isinstance(decoded, dict):
        raise EnrollmentError("enrollment response must be a JSON object")
    if not all(isinstance(key, str) and isinstance(value, str) for key, value in decoded.items()):
        raise EnrollmentError("enrollment response fields must be strings")
    return decoded


def enroll_url(server: str) -> str:
    """Return the enrollment CSR URL for a server base URL or explicit endpoint."""

    if not server:
        raise EnrollmentError("--server is required for enrollment")
    base = server if "://" in server else f"https://{server}"
    parsed = urllib.parse.urlsplit(base)
    if parsed.scheme != "https":
        raise EnrollmentError("enrollment requires an https server URL")
    if base.rstrip("/").endswith("/api/enroll/csr"):
        return base.rstrip("/")
    return f"{base.rstrip('/')}/api/enroll/csr"


def _run_openssl(args: list[str]) -> None:
    try:
        subprocess.run(
            ["openssl", *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise EnrollmentError("openssl command is required for enroll") from exc
    except subprocess.CalledProcessError as exc:
        raise EnrollmentError(exc.stderr.strip() or str(exc)) from exc


def _required_response_string(payload: dict[str, str], key: str) -> str:
    value = payload.get(key)
    if not value:
        raise EnrollmentError(f"enrollment response missing {key}")
    return value


def _error_detail(raw: str) -> str | None:
    try:
        payload: Any = json.loads(raw)
    except json.JSONDecodeError:
        return raw.strip() or None
    if isinstance(payload, dict):
        detail = payload.get("detail")
        if isinstance(detail, dict):
            nested = detail.get("detail") or detail.get("error")
            return str(nested) if nested else None
        if isinstance(detail, str):
            return detail
    return None
