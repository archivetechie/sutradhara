"""HTTP contract tests for the jobs/resources console read models."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy import Engine

from sutradhara.api.routes_jobs import MAX_JOBS_LIMIT
from sutradhara.catalog.session import session_scope
from sutradhara.jobs.config import WorkerConfig
from sutradhara.jobs.models import Job, JobAttempt, JobStatus
from tests.api.conftest import auth_headers, make_api_app

JOB_LIST_KEYS = {
    "id",
    "kind",
    "status",
    "priority",
    "attempts",
    "target_summary",
    "created_at",
    "started_at",
    "finished_at",
    "not_before",
    "last_error",
    "recon_domain",
    "recon_target_key",
    "required_resources",
    "dedupe_key",
}
ATTEMPT_KEYS = {
    "attempt_number",
    "outcome",
    "error",
    "started_at",
    "finished_at",
    "worker_id",
    "code_version",
    "granted_leases",
    "detail",
}


def test_jobs_list_contract_fields_and_six_state_passthrough(api_engine: Engine) -> None:
    statuses = [
        JobStatus.QUEUED,
        JobStatus.PENDING,
        JobStatus.RUNNING,
        JobStatus.SUCCEEDED,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    ]
    base = dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    with session_scope(api_engine) as session:
        for index, status in enumerate(statuses):
            _add_job(
                session,
                kind=f"kind-{status.value}",
                status=status,
                params={"copy_id": index + 1},
                created_at=base + dt.timedelta(minutes=index),
                attempts=index,
            )
    client = TestClient(make_api_app(api_engine))

    response = client.get("/api/ui/jobs?limit=10", headers=auth_headers("viewer"))

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"total", "truncated", "jobs"}
    assert body["total"] == 6
    assert body["truncated"] is False
    assert [row["status"] for row in body["jobs"]] == [
        status.value for status in reversed(statuses)
    ]
    for row in body["jobs"]:
        assert set(row) == JOB_LIST_KEYS
        assert isinstance(row["attempts"], int)
        assert row["status"] in {status.value for status in statuses}


def test_jobs_list_caps_at_200_with_total_and_truncated(api_engine: Engine) -> None:
    base = dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    with session_scope(api_engine) as session:
        for index in range(MAX_JOBS_LIMIT + 5):
            _add_job(
                session,
                kind="verify",
                status=JobStatus.PENDING,
                params={"copy_id": index},
                created_at=base + dt.timedelta(seconds=index),
            )
    client = TestClient(make_api_app(api_engine))

    response = client.get("/api/ui/jobs?limit=999", headers=auth_headers("viewer"))

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == MAX_JOBS_LIMIT + 5
    assert body["truncated"] is True
    assert len(body["jobs"]) == MAX_JOBS_LIMIT
    assert body["jobs"][0]["target_summary"] == f"copy {MAX_JOBS_LIMIT + 4}"
    assert body["jobs"][-1]["target_summary"] == "copy 5"


def test_jobs_target_summary_and_errors_never_expose_raw_paths(api_engine: Engine) -> None:
    with session_scope(api_engine) as session:
        _add_job(
            session,
            kind="restore",
            status=JobStatus.FAILED,
            params={
                "dest_path": "/var/lib/replica/private/export.mov",
                "nested": {"source_path": "/home/user/source.mov"},
            },
            last_error="restore failed under /var/lib/replica/private/export.mov",
        )
    client = TestClient(make_api_app(api_engine))

    response = client.get("/api/ui/jobs", headers=auth_headers("viewer"))

    assert response.status_code == 200
    payload_text = json.dumps(response.json())
    assert "/var/lib/replica" not in payload_text
    assert "/home/user" not in payload_text
    row = response.json()["jobs"][0]
    assert row["target_summary"] == 'dest_path=<redacted>, nested={"source_path":"<path>"}'
    assert row["last_error"] == "restore failed under <path>"


def test_job_detail_contract_fields_and_recursive_sanitizer(api_engine: Engine) -> None:
    started = dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    with session_scope(api_engine) as session:
        job = _add_job(
            session,
            kind="verify",
            status=JobStatus.FAILED,
            params={"copy_id": 7},
            attempts=1,
            last_error="failed reading /var/lib/replica/private/source.mov",
            created_at=started,
            started_at=started,
            finished_at=started + dt.timedelta(seconds=10),
        )
        session.add(
            JobAttempt(
                job_id=job.id,
                job_kind=job.kind,
                attempt_number=1,
                outcome=JobStatus.FAILED,
                error="worker opened /var/lib/replica/private/source.mov",
                started_at=started,
                finished_at=started + dt.timedelta(seconds=10),
                worker_id="worker-1",
                code_version="0.0.1",
                granted_leases={"io": 1},
                detail={
                    "outer": [
                        {
                            "inner": {
                                "path": "/var/lib/replica/private/source.mov",
                                "ok": "kept",
                            }
                        }
                    ]
                },
            )
        )
    client = TestClient(make_api_app(api_engine))

    response = client.get(f"/api/ui/jobs/{job.id}", headers=auth_headers("viewer"))

    assert response.status_code == 200
    body = response.json()
    assert set(body) == JOB_LIST_KEYS
    assert isinstance(body["attempts"], list)
    assert len(body["attempts"]) == 1
    attempt = body["attempts"][0]
    assert set(attempt) == ATTEMPT_KEYS
    assert attempt["error"] == "worker opened <path>"
    assert attempt["granted_leases"] == {"io": 1}
    assert attempt["detail"]["outer"][0]["inner"] == {"path": "<path>", "ok": "kept"}
    assert "/var/lib/replica" not in json.dumps(body)


def test_jobs_and_resources_require_can_view(api_engine: Engine) -> None:
    client = TestClient(make_api_app(api_engine))

    for path in ("/api/ui/jobs", "/api/ui/jobs/1", "/api/ui/resources"):
        response = client.get(path, headers=auth_headers("restore-p2"))
        assert response.status_code == 403
        assert response.json() == {
            "detail": {"error": "forbidden", "detail": "operator has no sutradhara role"}
        }


def test_resources_complete_pool_enumeration_from_worker_config(api_engine: Engine) -> None:
    app = make_api_app(api_engine)
    app.state.worker_config = WorkerConfig.defaults().with_pool_overrides(
        {"cpu": 4, "io": 2, "gpu": 1, "tape_drive": 0}
    )
    with session_scope(api_engine) as session:
        _add_job(
            session,
            kind="transcode",
            status=JobStatus.RUNNING,
            required_resources=[{"pool": "cpu", "count": 2}, {"pool": "io", "count": 1}],
        )
        _add_job(
            session,
            kind="restore",
            status=JobStatus.PENDING,
            required_resources=[{"pool": "io", "count": 1}, {"pool": "gpu", "count": 1}],
        )
        _add_job(
            session,
            kind="ignored-queued",
            status=JobStatus.QUEUED,
            required_resources=[{"pool": "cpu", "count": 4}],
        )
    client = TestClient(app)

    response = client.get("/api/ui/resources", headers=auth_headers("viewer"))

    assert response.status_code == 200
    assert response.json() == {
        "pools": [
            {"pool": "cpu", "capacity": 4, "in_use": 2, "waiting": 0},
            {"pool": "gpu", "capacity": 1, "in_use": 0, "waiting": 1},
            {"pool": "io", "capacity": 2, "in_use": 1, "waiting": 1},
            {"pool": "tape_drive", "capacity": 0, "in_use": 0, "waiting": 0},
        ]
    }


def _add_job(
    session: Any,
    *,
    kind: str,
    status: JobStatus,
    params: dict[str, Any] | None = None,
    required_resources: list[dict[str, Any]] | None = None,
    attempts: int = 0,
    priority: int = 0,
    last_error: str | None = None,
    created_at: dt.datetime | None = None,
    started_at: dt.datetime | None = None,
    finished_at: dt.datetime | None = None,
) -> Job:
    now = created_at or dt.datetime(2026, 7, 4, 8, 0, tzinfo=dt.UTC)
    job = Job(
        kind=kind,
        params=params or {},
        required_resources=required_resources or [],
        prerequisites=[],
        status=status,
        step_state={},
        attempts=attempts,
        not_before=now,
        priority=priority,
        last_error=last_error,
        created_at=now,
        started_at=started_at,
        finished_at=finished_at,
    )
    session.add(job)
    session.flush([job])
    return job
