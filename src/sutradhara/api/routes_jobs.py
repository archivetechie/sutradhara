"""Jobs and resource-pool read models for the operator console."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from fastapi import APIRouter, Request
from sqlalchemy import func, select

from sutradhara.api.console import (
    iso_utc,
    raise_console_error,
    require_view,
    sanitize_json,
    sanitize_text,
)
from sutradhara.api.identity import parse_identity
from sutradhara.catalog.session import session_scope
from sutradhara.jobs.config import WorkerConfig
from sutradhara.jobs.leases import normalize_required_resources
from sutradhara.jobs.models import Job, JobAttempt, JobStatus

router = APIRouter()

DEFAULT_JOBS_LIMIT = 50
MAX_JOBS_LIMIT = 200
JOB_STATUS_VALUES = frozenset(status.value for status in JobStatus)
PATHISH_PARAM_TOKENS = ("path", "dir", "root", "mount", "locator")


@router.get("/api/ui/jobs")
def get_jobs(
    request: Request,
    state: str | None = None,
    kind: str | None = None,
    limit: str | None = None,
) -> dict[str, object]:
    """Return job rows newest-first with a capped total/truncated envelope."""

    require_view(parse_identity(request.headers))
    state_filter = _optional_state(state)
    kind_filter = _optional_nonempty(kind)
    page_limit = _parse_limit(limit)
    with session_scope(request.app.state.engine) as session:
        total = func.count().over().label("total")
        query = select(Job, total).order_by(Job.created_at.desc(), Job.id.desc()).limit(page_limit)
        if state_filter is not None:
            query = query.where(Job.status == state_filter)
        if kind_filter is not None:
            query = query.where(Job.kind == kind_filter)
        rows = list(session.execute(query))
        jobs = [_job_payload(row[0], include_attempt_count=True) for row in rows]
        total_count = int(rows[0][1]) if rows else 0
    return {
        "total": total_count,
        "truncated": total_count > len(jobs),
        "jobs": jobs,
    }


@router.get("/api/ui/jobs/{job_id}")
def get_job(request: Request, job_id: int) -> dict[str, object]:
    """Return one job with its append-only attempt transcript."""

    require_view(parse_identity(request.headers))
    with session_scope(request.app.state.engine) as session:
        job = session.get(Job, job_id)
        if job is None:
            raise_console_error(404, "not_found", f"unknown job {job_id}")
        attempts = list(
            session.scalars(
                select(JobAttempt)
                .where(JobAttempt.job_id == job_id)
                .order_by(JobAttempt.attempt_number, JobAttempt.id)
            )
        )
        payload = _job_payload(job, include_attempt_count=False)
        payload["attempts"] = [_attempt_payload(attempt) for attempt in attempts]
        return payload


@router.get("/api/ui/resources")
def get_resources(request: Request) -> dict[str, object]:
    """Return configured worker resource pools with DB-derived occupancy."""

    require_view(parse_identity(request.headers))
    config = _worker_config(request)
    pools = {
        pool: {"pool": pool, "capacity": int(capacity), "in_use": 0, "waiting": 0}
        for pool, capacity in sorted(config.capacities.items())
    }
    with session_scope(request.app.state.engine) as session:
        rows = list(
            session.scalars(
                select(Job).where(Job.status.in_((JobStatus.RUNNING, JobStatus.PENDING)))
            )
        )
        for job in rows:
            required = normalize_required_resources(job.required_resources)
            for pool, count in required.items():
                if pool not in pools:
                    continue
                if _status_value(job.status) == JobStatus.RUNNING.value:
                    pools[pool]["in_use"] += count
                elif _status_value(job.status) == JobStatus.PENDING.value:
                    pools[pool]["waiting"] += 1
    return {"pools": list(pools.values())}


def _worker_config(request: Request) -> WorkerConfig:
    configured = getattr(request.app.state, "worker_config", None)
    if isinstance(configured, WorkerConfig):
        return configured
    return WorkerConfig.defaults()


def _optional_state(raw: str | None) -> str | None:
    value = _optional_nonempty(raw)
    if value is None:
        return None
    if value not in JOB_STATUS_VALUES:
        raise_console_error(400, "bad_request", f"unknown job state {value!r}")
    return value


def _optional_nonempty(raw: str | None) -> str | None:
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped or None


def _parse_limit(raw: str | None) -> int:
    value = _optional_nonempty(raw)
    if value is None:
        return DEFAULT_JOBS_LIMIT
    try:
        parsed = int(value)
    except ValueError:
        raise_console_error(400, "bad_request", "limit must be an integer")
    if parsed < 1:
        raise_console_error(400, "bad_request", "limit must be at least 1")
    return min(parsed, MAX_JOBS_LIMIT)


def _job_payload(job: Job, *, include_attempt_count: bool) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": job.id,
        "kind": sanitize_text(job.kind),
        "status": _status_value(job.status),
        "priority": job.priority,
        "target_summary": _target_summary(job),
        "created_at": iso_utc(job.created_at),
        "started_at": None if job.started_at is None else iso_utc(job.started_at),
        "finished_at": None if job.finished_at is None else iso_utc(job.finished_at),
        "not_before": iso_utc(job.not_before),
        "last_error": None if job.last_error is None else sanitize_text(job.last_error),
        "recon_domain": None if job.recon_domain is None else sanitize_text(job.recon_domain),
        "recon_target_key": (
            None if job.recon_target_key is None else sanitize_text(job.recon_target_key)
        ),
        "required_resources": _resource_list(job.required_resources),
        "dedupe_key": None if job.dedupe_key is None else sanitize_text(job.dedupe_key),
    }
    if include_attempt_count:
        payload["attempts"] = job.attempts
    return payload


def _attempt_payload(attempt: JobAttempt) -> dict[str, object]:
    detail = sanitize_json(attempt.detail or {})
    if not isinstance(detail, dict):
        detail = {"value": detail}
    return {
        "attempt_number": attempt.attempt_number,
        "outcome": _status_value(attempt.outcome),
        "error": None if attempt.error is None else sanitize_text(attempt.error),
        "started_at": iso_utc(attempt.started_at),
        "finished_at": iso_utc(attempt.finished_at),
        "worker_id": None if attempt.worker_id is None else sanitize_text(attempt.worker_id),
        "code_version": (
            None if attempt.code_version is None else sanitize_text(attempt.code_version)
        ),
        "granted_leases": _lease_payload(attempt.granted_leases),
        "detail": detail,
    }


def _target_summary(job: Job) -> str:
    if job.recon_target_key:
        return _summarize_recon_target(job.recon_target_key)
    return _summarize_params(job.kind, job.params)


def _summarize_recon_target(target_key: str) -> str:
    safe = sanitize_text(target_key)
    parts = [part for part in safe.split(":") if part]
    if len(parts) >= 3 and parts[0] == "asset":
        digest = parts[1]
        pool = parts[-1]
        return sanitize_text(f"asset {digest[:12]} pool {pool}")
    if len(parts) == 2:
        return sanitize_text(f"{parts[0].replace('-', ' ')} {parts[1]}")
    return safe or "reconciliation target"


def _summarize_params(kind: str, params: Any) -> str:
    if not isinstance(params, Mapping) or not params:
        return sanitize_text(kind)
    preferred = (
        ("restore_request_item_id", "restore item"),
        ("copy_id", "copy"),
        ("bundle_id", "bundle"),
        ("submission_id", "submission"),
        ("intake_id", "intake"),
        ("content_sha256", "asset"),
        ("pool_id", "pool"),
        ("artifactclass", "artifactclass"),
    )
    for key, label in preferred:
        if key in params:
            return sanitize_text(f"{label} {_summarize_value(params[key], key=key)}")
    pairs: list[str] = []
    for key in sorted(str(raw_key) for raw_key in params):
        value = params.get(key)
        pairs.append(f"{key}={_summarize_value(value, key=key)}")
        if len(pairs) >= 3:
            break
    if not pairs:
        return sanitize_text(f"{kind} parameters")
    return sanitize_text(", ".join(pairs))


def _summarize_value(value: Any, *, key: str) -> str:
    if _is_pathish_key(key):
        return "<redacted>"
    if isinstance(value, str):
        safe = sanitize_text(value)
    else:
        safe_json = sanitize_json(value)
        try:
            safe = json.dumps(safe_json, sort_keys=True, separators=(",", ":"))
        except TypeError:
            safe = sanitize_text(str(safe_json))
    if len(safe) > 80:
        return f"{safe[:77]}..."
    return safe


def _is_pathish_key(key: str) -> bool:
    lowered = key.lower()
    return any(token in lowered for token in PATHISH_PARAM_TOKENS)


def _resource_list(raw: list[dict[str, Any]] | None) -> list[dict[str, object]]:
    normalized = normalize_required_resources(raw)
    return [
        {"pool": sanitize_text(pool), "count": count}
        for pool, count in sorted(normalized.items())
    ]


def _lease_payload(raw: Mapping[str, Any] | None) -> dict[str, object]:
    if not isinstance(raw, Mapping):
        return {}
    return {sanitize_text(str(pool)): count for pool, count in raw.items()}


def _status_value(value: object) -> str:
    if isinstance(value, JobStatus):
        return value.value
    return str(value)
