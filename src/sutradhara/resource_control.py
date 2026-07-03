"""Elastic resource-control wrapper for CPU-heavy child processes.

Sutradhara's job leases decide admission: how many jobs may run. This module
adds the enforcement side for subprocesses such as ffmpeg, ffprobe, and
Remanence archive commands by placing each child tree in a transient systemd
scope when the host supports it. The cgroup weight is a fairness/no-starvation
tool, not a hard CPU cap; optional CPUQuota properties are emitted only after
the capability probe proves that the local systemd manager accepts them.
"""

from __future__ import annotations

import itertools
import logging
import os
import re
import shutil
import signal
import subprocess
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

LOGGER = logging.getLogger(__name__)

IoniceClass = Literal["realtime", "best-effort", "idle"]
ManagerMode = Literal["user", "system"]
CapabilityMode = Literal["systemd", "degraded"]


@dataclass(frozen=True)
class ResourceProfile:
    """Execution profile for a CPU-heavy subprocess role."""

    cpu_weight: int
    io_weight: int
    nice: int
    ionice: tuple[IoniceClass, int | None] | None = None
    cpu_quota_pct: int | None = None


@dataclass(frozen=True)
class ResourceCapability:
    """Cached host capability for transient systemd scopes."""

    mode: CapabilityMode
    manager: ManagerMode
    properties: frozenset[str]
    reason: str | None = None


class ResourceControlUnavailable(RuntimeError):
    """Raised when resource-control enforcement is required but unavailable."""


RESOURCE_PROFILES: dict[str, ResourceProfile] = {
    "high": ResourceProfile(
        cpu_weight=1000,
        io_weight=1000,
        nice=0,
        ionice=("best-effort", 4),
    ),
    "medium": ResourceProfile(
        cpu_weight=100,
        io_weight=100,
        nice=0,
        ionice=("best-effort", 4),
    ),
    "low": ResourceProfile(
        cpu_weight=25,
        io_weight=10,
        nice=19,
        ionice=("idle", None),
    ),
}

ROLE_BY_JOB_KIND: dict[str, str] = {
    "restore": "high",
    "release-verify": "high",
    "transcode": "medium",
    "pfr-index": "medium",
    "cloud-blob": "medium",
    "archive-submission": "medium",
    "copy": "medium",
    "validate": "low",
    "verify": "low",
    "verify-freshness": "low",
    "re-derive": "low",
    "bulk": "low",
    "bulk-migration": "low",
}

_IONICE_CLASS_ARG: dict[IoniceClass, str] = {
    "realtime": "1",
    "best-effort": "2",
    "idle": "3",
}
_SYSTEMD_SETUP_PATTERNS = (
    "Failed to start transient scope",
    "Failed to set unit properties",
    "Failed to connect to bus",
    "System has not been booted with systemd",
)
_CAPABILITY_LOCK = threading.Lock()
_CAPABILITY_CACHE: ResourceCapability | None = None
_DEGRADED_LOGGED = False
_UNIT_COUNTER = itertools.count(1)
_SAFE_UNIT_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def clear_capability_cache() -> None:
    """Clear the cached probe result; intended for tests and config reloads."""

    global _CAPABILITY_CACHE, _DEGRADED_LOGGED
    with _CAPABILITY_LOCK:
        _CAPABILITY_CACHE = None
        _DEGRADED_LOGGED = False


def resource_role_for_job(kind: str, params: Mapping[str, Any] | None = None) -> str:
    """Resolve the execution role from job kind plus explicit params override."""

    override = (params or {}).get("resource_role")
    if override is not None:
        if not isinstance(override, str) or override not in RESOURCE_PROFILES:
            raise ValueError(f"unknown resource_role {override!r}")
        return override
    return ROLE_BY_JOB_KIND.get(kind, "medium")


def cpu_lease_from_job(
    granted_leases: Mapping[str, int],
    required_resources: Sequence[Mapping[str, Any]] | None,
) -> int | None:
    """Return the granted or requested CPU lease count for subprocess hints."""

    raw: object = granted_leases.get("cpu")
    if raw is None:
        for resource in required_resources or ():
            if resource.get("pool") == "cpu":
                raw = resource.get("count")
                break
    if raw is None:
        return None
    if not isinstance(raw, (int, str, bytes, bytearray)):
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return max(1, value)


def capability() -> ResourceCapability:
    """Probe and cache the systemd scope property subset that works here."""

    global _CAPABILITY_CACHE
    with _CAPABILITY_LOCK:
        if _CAPABILITY_CACHE is None:
            _CAPABILITY_CACHE = _probe_capability()
        return _CAPABILITY_CACHE


def run_managed(
    cmd: Sequence[str | os.PathLike[str]],
    *,
    role: str,
    cpu_lease: int | None = None,
    timeout: float | None = None,
    **popen_kw: Any,
) -> subprocess.CompletedProcess[Any]:
    """Run ``cmd`` under the role's cgroup/nice/ionice policy when available.

    ``cpu_lease`` is intentionally advisory. Callers still pass their own
    thread flags to codecs; the kernel-enforced behavior here comes from the
    profile's cgroup weight and optional quota.
    """

    profile = _profile_for_role(role)
    cap = capability()
    args = [os.fspath(part) for part in cmd]
    if cap.mode == "systemd":
        try:
            return _run_systemd(
                args,
                role=role,
                profile=profile,
                cap=cap,
                timeout=timeout,
                popen_kw=dict(popen_kw),
            )
        except OSError as exc:
            if not _is_systemd_launcher_error(exc):
                raise
            cap = _degrade_or_raise(cap, reason=f"systemd scope launch failed: {exc}")
    else:
        cap = _degrade_or_raise(cap)
    return _run_degraded(args, profile=profile, timeout=timeout, popen_kw=dict(popen_kw))


def _probe_capability() -> ResourceCapability:
    if _resource_control_disabled():
        return ResourceCapability(
            mode="degraded",
            manager=_manager_mode(),
            properties=frozenset(),
            reason="disabled by SUTRADHARA_RESOURCE_CONTROL",
        )
    if shutil.which("systemd-run") is None:
        return ResourceCapability(
            mode="degraded",
            manager=_manager_mode(),
            properties=frozenset(),
            reason="systemd-run not found",
        )

    manager = _manager_mode()
    wanted = _wanted_probe_properties()
    full = _run_probe(manager, wanted)
    if full.returncode == 0:
        return ResourceCapability(
            mode="systemd",
            manager=manager,
            properties=frozenset(wanted),
        )

    cpu_only = {"CPUWeight": wanted["CPUWeight"]}
    cpu_probe = _run_probe(manager, cpu_only)
    if cpu_probe.returncode != 0:
        return ResourceCapability(
            mode="degraded",
            manager=manager,
            properties=frozenset(),
            reason=_probe_reason(cpu_probe),
        )

    supported = {"CPUWeight"}
    for name in ("IOWeight", "CPUQuota"):
        value = wanted.get(name)
        if value is None:
            continue
        candidate = {"CPUWeight": wanted["CPUWeight"], name: value}
        result = _run_probe(manager, candidate)
        if result.returncode == 0:
            supported.add(name)
            continue
        LOGGER.warning(
            "systemd resource property %s unavailable; continuing without it: %s",
            name,
            _probe_reason(result),
        )
    return ResourceCapability(
        mode="systemd",
        manager=manager,
        properties=frozenset(supported),
    )


def _run_probe(
    manager: ManagerMode, properties: Mapping[str, str]
) -> subprocess.CompletedProcess[str]:
    cmd = [
        "systemd-run",
        *_manager_args(manager),
        "--scope",
        "--quiet",
        "--no-ask-password",
        *_property_args(properties),
        "--",
        "true",
    ]
    try:
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=_probe_timeout_seconds(),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr=str(exc))


def _run_systemd(
    args: list[str],
    *,
    role: str,
    profile: ResourceProfile,
    cap: ResourceCapability,
    timeout: float | None,
    popen_kw: dict[str, Any],
) -> subprocess.CompletedProcess[Any]:
    check = bool(popen_kw.pop("check", False))
    unit = _next_unit_name(role)
    managed_cmd = [
        "systemd-run",
        *_manager_args(cap.manager),
        "--scope",
        "--collect",
        "--quiet",
        "--no-ask-password",
        f"--unit={unit}",
        *_profile_property_args(profile, cap.properties),
        "--",
        *_systemd_child_prefix(profile),
        *args,
    ]
    try:
        completed = subprocess.run(
            managed_cmd,
            check=False,
            timeout=timeout,
            **popen_kw,
        )
    except subprocess.TimeoutExpired:
        _stop_scope(cap.manager, unit)
        raise

    if completed.returncode != 0 and _looks_like_setup_failure(completed):
        _degrade_or_raise(
            cap,
            reason=f"systemd scope setup failed: {_stderr(completed)}",
        )
        return _run_degraded(
            args, profile=profile, timeout=timeout, popen_kw={**popen_kw, "check": check}
        )
    _raise_if_requested(completed, check)
    return completed


def _run_degraded(
    args: list[str],
    *,
    profile: ResourceProfile,
    timeout: float | None,
    popen_kw: dict[str, Any],
) -> subprocess.CompletedProcess[Any]:
    check = bool(popen_kw.pop("check", False))
    command = [*_degraded_child_prefix(profile), *args]
    completed = _run_process_group(command, timeout=timeout, popen_kw=popen_kw)
    _raise_if_requested(completed, check)
    return completed


def _run_process_group(
    args: list[str],
    *,
    timeout: float | None,
    popen_kw: dict[str, Any],
) -> subprocess.CompletedProcess[Any]:
    input_data = popen_kw.pop("input", None)
    capture_output = bool(popen_kw.pop("capture_output", False))
    if capture_output:
        if popen_kw.get("stdout") is not None or popen_kw.get("stderr") is not None:
            raise ValueError("stdout and stderr arguments may not be used with capture_output")
        popen_kw["stdout"] = subprocess.PIPE
        popen_kw["stderr"] = subprocess.PIPE
    if input_data is not None:
        if popen_kw.get("stdin") is not None:
            raise ValueError("stdin and input arguments may not both be used")
        popen_kw["stdin"] = subprocess.PIPE
    popen_kw["start_new_session"] = True

    process = subprocess.Popen(args, **popen_kw)
    try:
        stdout, stderr = process.communicate(input=input_data, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        _kill_process_group(process.pid)
        stdout, stderr = process.communicate()
        exc.output = stdout
        exc.stderr = stderr
        raise
    return subprocess.CompletedProcess(args, process.returncode, stdout, stderr)


def _kill_process_group(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        return


def _stop_scope(manager: ManagerMode, unit: str) -> None:
    cmd = [
        "systemctl",
        *_manager_args(manager),
        "--no-ask-password",
        "stop",
        unit,
    ]
    try:
        subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=_probe_timeout_seconds(),
        )
    except (OSError, subprocess.TimeoutExpired):
        LOGGER.warning("failed to stop timed-out systemd scope %s", unit, exc_info=True)


def _profile_for_role(role: str) -> ResourceProfile:
    try:
        profile = RESOURCE_PROFILES[role]
    except KeyError as exc:
        raise ValueError(f"unknown resource role {role!r}") from exc
    _validate_profile(role, profile)
    return profile


def _validate_profile(role: str, profile: ResourceProfile) -> None:
    if not 1 <= profile.cpu_weight <= 10000:
        raise ValueError(f"resource profile {role!r} has invalid CPUWeight {profile.cpu_weight}")
    if not 1 <= profile.io_weight <= 10000:
        raise ValueError(f"resource profile {role!r} has invalid IOWeight {profile.io_weight}")
    if profile.nice < 0:
        raise ValueError(f"resource profile {role!r} uses unsupported negative nice {profile.nice}")
    if profile.cpu_quota_pct is not None and profile.cpu_quota_pct <= 0:
        raise ValueError(f"resource profile {role!r} has invalid CPUQuota {profile.cpu_quota_pct}")
    if profile.ionice is None:
        return
    ionice_class, level = profile.ionice
    if ionice_class == "idle" and level is not None:
        raise ValueError("idle ionice class must not carry a level")
    if ionice_class != "idle" and level is not None and not 0 <= level <= 7:
        raise ValueError(f"resource profile {role!r} has invalid ionice level {level}")


def _wanted_probe_properties() -> dict[str, str]:
    properties = {"CPUWeight": "100", "IOWeight": "100"}
    if any(profile.cpu_quota_pct is not None for profile in RESOURCE_PROFILES.values()):
        properties["CPUQuota"] = "50%"
    return properties


def _profile_property_args(profile: ResourceProfile, supported: frozenset[str]) -> list[str]:
    properties: dict[str, str] = {}
    if "CPUWeight" in supported:
        properties["CPUWeight"] = str(profile.cpu_weight)
    if "IOWeight" in supported:
        properties["IOWeight"] = str(profile.io_weight)
    if "CPUQuota" in supported and profile.cpu_quota_pct is not None:
        properties["CPUQuota"] = f"{profile.cpu_quota_pct}%"
    return _property_args(properties)


def _property_args(properties: Mapping[str, str]) -> list[str]:
    args: list[str] = []
    for name, value in properties.items():
        args.extend(["-p", f"{name}={value}"])
    return args


def _systemd_child_prefix(profile: ResourceProfile) -> list[str]:
    return [*_ionice_args(profile.ionice), "nice", "-n", str(profile.nice)]


def _degraded_child_prefix(profile: ResourceProfile) -> list[str]:
    prefix: list[str] = []
    nice = shutil.which("nice")
    if profile.nice != 0 and nice is not None:
        prefix.extend([nice, "-n", str(profile.nice)])
    ionice = shutil.which("ionice")
    if ionice is not None:
        prefix.extend(_ionice_args(profile.ionice, executable=ionice))
    return prefix


def _ionice_args(
    spec: tuple[IoniceClass, int | None] | None,
    *,
    executable: str = "ionice",
) -> list[str]:
    if spec is None:
        return []
    ionice_class, level = spec
    args = [executable, "-c", _IONICE_CLASS_ARG[ionice_class]]
    if ionice_class != "idle" and level is not None:
        args.extend(["-n", str(level)])
    return args


def _next_unit_name(role: str) -> str:
    safe_role = _SAFE_UNIT_RE.sub("-", role).strip(".-") or "job"
    return f"sutradhara-rc-{safe_role}-{os.getpid()}-{next(_UNIT_COUNTER)}.scope"


def _manager_mode() -> ManagerMode:
    raw = os.environ.get("SUTRADHARA_RESOURCE_CONTROL_SYSTEMD", "user").strip().lower()
    if raw in {"system", "systemd-system"}:
        return "system"
    if raw in {"user", "systemd-user", ""}:
        return "user"
    raise ValueError(f"SUTRADHARA_RESOURCE_CONTROL_SYSTEMD must be 'user' or 'system', got {raw!r}")


def _manager_args(manager: ManagerMode) -> list[str]:
    return ["--user"] if manager == "user" else ["--system"]


def _resource_control_disabled() -> bool:
    raw = os.environ.get("SUTRADHARA_RESOURCE_CONTROL", "auto").strip().lower()
    return raw in {"0", "off", "false", "disabled", "degraded"}


def _resource_control_required() -> bool:
    raw = os.environ.get("SUTRADHARA_RESOURCE_CONTROL_REQUIRE", "").strip().lower()
    return raw in {"1", "on", "true", "yes", "require", "required"}


def _degrade_or_raise(
    cap: ResourceCapability,
    *,
    reason: str | None = None,
) -> ResourceCapability:
    degraded = ResourceCapability(
        mode="degraded",
        manager=cap.manager,
        properties=frozenset(),
        reason=reason or cap.reason or "systemd scope unavailable",
    )
    if _resource_control_required():
        raise ResourceControlUnavailable(degraded.reason)
    _log_degraded_once(degraded.reason)
    return degraded


def _log_degraded_once(reason: str | None) -> None:
    global _DEGRADED_LOGGED
    with _CAPABILITY_LOCK:
        if _DEGRADED_LOGGED:
            return
        LOGGER.error("resource enforcement degraded: %s", reason or "systemd scope unavailable")
        _DEGRADED_LOGGED = True


def _probe_timeout_seconds() -> float:
    raw = os.environ.get("SUTRADHARA_RESOURCE_CONTROL_PROBE_TIMEOUT", "2.0")
    try:
        return max(0.1, float(raw))
    except ValueError:
        return 2.0


def _probe_reason(result: subprocess.CompletedProcess[str]) -> str:
    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()
    detail = stderr or stdout or f"exit {result.returncode}"
    return detail[:500]


def _looks_like_setup_failure(completed: subprocess.CompletedProcess[Any]) -> bool:
    stdout = _stream_text(completed.stdout)
    stderr = _stream_text(completed.stderr)
    if stdout not in {"", None}:
        return False
    if not stderr:
        return False
    return any(pattern in stderr for pattern in _SYSTEMD_SETUP_PATTERNS)


def _stream_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def _stderr(completed: subprocess.CompletedProcess[Any]) -> str:
    return (_stream_text(completed.stderr) or "").strip()[:500]


def _raise_if_requested(completed: subprocess.CompletedProcess[Any], check: bool) -> None:
    if check and completed.returncode:
        raise subprocess.CalledProcessError(
            completed.returncode,
            completed.args,
            output=completed.stdout,
            stderr=completed.stderr,
        )


def _is_systemd_launcher_error(exc: OSError) -> bool:
    filename = getattr(exc, "filename", None)
    return filename in {None, "systemd-run"} or str(filename).endswith("/systemd-run")
