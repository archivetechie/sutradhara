"""Job submission, atomic claim, and synchronous in-process execution.

The worker stores jobs in the Sutradhara catalog DB. SQLite is still the queue
substrate, but claims now use a guarded ``PENDING -> RUNNING`` update so the
future Postgres ``SKIP LOCKED`` implementation can be swapped in one place.
"""

from __future__ import annotations

import datetime as dt
import socket
import time
import traceback
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from copy import deepcopy
from functools import partial
from typing import Any, cast

from sqlalchemy import event, select, update
from sqlalchemy.engine import Connection, CursorResult
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from sutradhara.jobs.attempts import record_attempt
from sutradhara.jobs.components import touch_tape_locator
from sutradhara.jobs.config import WorkerConfig
from sutradhara.jobs.leases import LeaseManager, normalize_required_resources
from sutradhara.jobs.models import (
    LIVE_JOB_STATUS_VALUES,
    TERMINAL_STATUSES,
    Job,
    JobAttempt,
    JobStatus,
)
from sutradhara.jobs.reconcilers.conditions import (
    CONDITION_BACKOFF,
    CONDITION_BLOCKED,
    record_condition,
)
from sutradhara.jobs.registry import (
    CHECKPOINT_BATCH_STATE_KEY,
    ConditionProjection,
    HandlerNotRegistered,
    JobContext,
    JobHandler,
    JobResult,
    get_dispatch_gate,
    get_handler,
)
from sutradhara.jobs.runtime_observations import (
    bind_batch_observers,
    bind_session_open_observer,
    bind_tape_locator_observer,
)
from sutradhara.structured_logs import emit_structured_event


class AttemptSuperseded(Exception):
    """The job no longer belongs to the attempt that did the external work."""


class AttemptBoundaryError(Exception):
    """A handler violated the read/external-work/write transaction boundary."""


def _begin_immediate(connection: Connection, *, job_id: int, job_kind: str) -> None:
    """Wait for the writer slot without replaying the handler's external work."""
    started = time.monotonic()
    while True:
        try:
            connection.exec_driver_sql("BEGIN IMMEDIATE")
            return
        except OperationalError as error:
            if not is_sqlite_lock_error(error):
                raise
            emit_structured_event(
                "sutradhara.job.write_lock_wait",
                job_id=job_id,
                job_kind=job_kind,
                seconds_waited=time.monotonic() - started,
            )


class _AttemptBoundary:
    """Own the replaceable connection savepoint shared with handler dispatch."""

    def __init__(self, session: Session, handler_session: Session) -> None:
        self.session = session
        self.handler_session = handler_session
        self.connection = session.connection()
        self.savepoint = self.connection.begin_nested()

    @contextmanager
    def outside(self, job: Job) -> Iterator[bool]:
        session = self.session
        handler_session = self.handler_session
        connection = self.connection
        if "attempt_total_changes" not in session.info or connection.dialect.name != "sqlite":
            yield False
            return
        if session.info.get("attempt_boundary_used"):
            raise AttemptBoundaryError("attempt transaction boundary already used")
        dbapi = connection.connection.dbapi_connection
        if dbapi is None:
            raise AttemptBoundaryError("attempt connection is unavailable")
        if dbapi.total_changes != session.info["attempt_total_changes"] or any(
            s.new or s.dirty or s.deleted for s in (session, handler_session)
        ):
            raise AttemptBoundaryError("attempt transaction boundary requires no prior writes")
        job_id, job_kind, attempts = job.id, job.kind, job.attempts
        session.info["attempt_boundary_used"] = True
        handler_session.flush()
        Session.commit(handler_session)
        self.savepoint.commit()
        connection.exec_driver_sql("COMMIT")
        violation: AttemptBoundaryError | None = None

        def refuse_sql(
            conn: Any,
            cursor: Any,
            statement: str,
            parameters: Any,
            context: Any,
            executemany: Any,
        ) -> None:
            nonlocal violation
            # Reject even Session autobegin's SAVEPOINT: a rollback-to alone
            # would leave that outermost SQLite savepoint transaction open.
            violation = AttemptBoundaryError("SQL used inside attempt transaction boundary")
            raise violation

        def refuse_session(*args: Any) -> None:
            nonlocal violation
            violation = AttemptBoundaryError("session used inside attempt transaction boundary")
            raise violation

        event.listen(connection, "before_cursor_execute", refuse_sql)
        guarded_events = ("do_orm_execute", "before_flush", "before_attach", "before_commit")
        for guarded_session in (session, handler_session):
            for name in guarded_events:
                event.listen(guarded_session, name, refuse_session)
        try:
            yield True
        finally:
            event.remove(connection, "before_cursor_execute", refuse_sql)
            for guarded_session in (session, handler_session):
                for name in guarded_events:
                    event.remove(guarded_session, name, refuse_session)
            if any(s.new or s.dirty or s.deleted for s in (session, handler_session)):
                violation = AttemptBoundaryError(
                    "session mutated inside attempt transaction boundary"
                )
            handler_session.rollback()
            # The entry guard proved the engine session had no pending work.
            # Discard any in-memory misuse before the R4 query can autoflush it.
            for instance in list(session.new):
                session.expunge(instance)
            for instance in list(session.deleted):
                session.add(instance)
            if dbapi.in_transaction:
                connection.exec_driver_sql("ROLLBACK")
                violation = AttemptBoundaryError("attempt driver was not idle before re-opening")
            _begin_immediate(connection, job_id=job_id, job_kind=job_kind)
            self.savepoint = connection.begin_nested()
            session.expire_all()
            handler_session.expire_all()
            session.info["attempt_total_changes"] = dbapi.total_changes
            current = session.execute(
                select(Job.status, Job.attempts).where(Job.id == job_id)
            ).one_or_none()
            if current is None or current != (JobStatus.RUNNING, attempts):
                raise AttemptSuperseded(
                    f"superseded: job {job_id} changed from RUNNING attempt {attempts} to {current}"
                )
            if violation is not None:
                raise violation


@contextmanager
def _outside_transaction(ctx: JobContext) -> Iterator[bool]:
    """Delegate to the engine-owned boundary, or preserve a caller's transaction."""
    boundary = ctx.session.info.get("attempt_boundary")
    if boundary is None:
        yield False
    else:
        with boundary.outside(ctx.job) as active:
            yield active


def is_sqlite_lock_error(error: BaseException) -> bool:
    """Recognize only SQLite contention errors, leaving other failures unchanged."""
    return isinstance(error, OperationalError) and str(error.orig).lower().startswith(
        ("database is locked", "database table is locked")
    )


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


class _HandlerSession(Session):
    """Session whose handler-visible commits keep a joined transaction open."""

    def flush(self, objects: Sequence[Any] | None = None) -> None:
        retained = self.info.setdefault("handler_flushed_objects", set())
        retained.update(self.new)
        retained.update(self.dirty)
        super().flush(objects)

    def commit(self) -> None:
        super().commit()
        self.begin()


def submit(
    session: Session,
    kind: str,
    params: dict[str, Any] | None = None,
    *,
    required_resources: list[dict[str, Any]] | None = None,
    prerequisites: list[int] | None = None,
    not_before: dt.datetime | None = None,
    priority: int = 0,
    dedupe_key: str | None = None,
    recon_domain: str | None = None,
    recon_target_key: str | None = None,
) -> Job:
    """Create a PENDING job in the catalog. Returns the persisted Job.

    Caller is responsible for `session.commit()`.
    """
    if dedupe_key is not None:
        existing = _live_job_for_dedupe(session, dedupe_key)
        if existing is not None:
            return existing
    now = _utcnow()
    job = Job(
        kind=kind,
        params=params or {},
        required_resources=required_resources or [],
        prerequisites=prerequisites or [],
        status=JobStatus.PENDING,
        created_at=now,
        not_before=not_before or now,
        priority=priority,
        dedupe_key=dedupe_key,
        recon_domain=recon_domain,
        recon_target_key=recon_target_key,
    )
    if dedupe_key is None:
        session.add(job)
        session.flush()  # so caller can read job.id
        return job
    try:
        with session.begin_nested():
            session.add(job)
            session.flush()  # so caller can read job.id
    except IntegrityError:
        existing = _live_job_for_dedupe(session, dedupe_key)
        if existing is None:
            raise
        return existing
    return job


def run_one(
    session: Session,
    job_id: int,
    *,
    granted_leases: dict[str, int] | None = None,
) -> JobResult:
    """Run a specific job by id. Marks status, records outcome.

    Returns the `JobResult` from the handler (or a synthesized failure
    result if the handler raised). Caller is responsible for `commit()`.
    """
    job = session.get(Job, job_id)
    if job is None:
        raise ValueError(f"no job with id={job_id}")
    if job.status in TERMINAL_STATUSES:
        raise ValueError(f"job id={job_id} is in terminal status {job.status}; cannot re-run")
    if job.status == JobStatus.PENDING:
        job = _claim_job_by_id(session, job.id, now=_utcnow())
        if job is None:
            raise ValueError(f"job id={job_id} could not be claimed")
    elif job.status != JobStatus.RUNNING:
        raise ValueError(f"job id={job_id} is {job.status}; cannot run")

    ctx = job_context(session, job, granted_leases=granted_leases)
    job_kind = job.kind
    try:
        handler = get_handler(job.kind)
    except HandlerNotRegistered as e:
        job.status = JobStatus.FAILED
        job.finished_at = _utcnow()
        job.last_error = str(e)
        attempt = record_attempt(
            session,
            job,
            granted_leases=granted_leases,
            detail=attempt_detail(ctx),
        )
        _record_reconciler_condition(
            session,
            job,
            attempt,
            condition=CONDITION_BACKOFF,
            reason="handler-not-registered",
            message=str(e),
        )
        return JobResult(ok=False, detail=str(e))

    try:
        with (
            bind_session_open_observer(ctx.observe_session_open),
            bind_tape_locator_observer(partial(touch_tape_locator, ctx)),
            bind_batch_observers(
                ctx.record_batch_written,
                ctx.record_batch_checkpointed,
            ),
        ):
            result = _invoke_handler_in_savepoint(session, ctx, handler)
    except NotImplementedError as e:
        job.status = JobStatus.FAILED
        job.finished_at = _utcnow()
        job.last_error = f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"
        attempt = record_attempt(
            session,
            job,
            granted_leases=granted_leases,
            detail=attempt_detail(ctx),
        )
        _record_reconciler_condition(
            session,
            job,
            attempt,
            condition=CONDITION_BLOCKED,
            reason="not-implemented",
            message=str(e),
        )
        return JobResult(ok=False, detail=str(e))
    except Exception as e:
        if isinstance(e, AttemptSuperseded) or is_sqlite_lock_error(e):
            session.rollback()
            raise
        if not session.is_active:
            session.rollback()
            if session.info.get("attempt_boundary_used"):
                _begin_immediate(session.connection(), job_id=job_id, job_kind=job_kind)
            job = session.get(Job, job_id)
            if job is None:
                raise
        job.status = JobStatus.FAILED
        job.finished_at = _utcnow()
        job.last_error = f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"
        attempt = record_attempt(
            session,
            job,
            granted_leases=granted_leases,
            detail=attempt_detail(ctx),
        )
        _record_reconciler_condition(
            session,
            job,
            attempt,
            condition=CONDITION_BACKOFF,
            reason="handler-exception",
            message=str(e),
        )
        return JobResult(ok=False, detail=str(e))

    succeeded = result.ok and not result.superseded
    job.status = JobStatus.SUCCEEDED if succeeded else JobStatus.FAILED
    job.finished_at = _utcnow()
    job.last_error = None if succeeded else result.detail
    if result.step_state:
        # Merge over existing step_state so handlers can build up state
        # incrementally without clobbering.
        job.step_state = {**job.step_state, **result.step_state}
    attempt = record_attempt(
        session,
        job,
        granted_leases=granted_leases,
        detail=attempt_detail(ctx),
    )
    if result.superseded:
        if job.recon_domain is None:
            job.status = JobStatus.PENDING
            job.not_before = _utcnow() + dt.timedelta(seconds=30)
            job.started_at = None
            job.finished_at = None
        else:
            _record_reconciler_condition(
                session,
                job,
                attempt,
                condition=CONDITION_BACKOFF,
                reason="superseded",
                message=result.detail,
                auto_block=False,
            )
    elif result.condition is not None:
        _record_projection_condition(session, job, attempt, result.condition)
    elif result.ok:
        _record_reconciler_condition(session, job, attempt, condition=None)
    else:
        _record_reconciler_condition(
            session,
            job,
            attempt,
            condition=CONDITION_BACKOFF,
            reason="unclassified",
            message=result.detail,
        )
    return result


def _invoke_handler_in_savepoint(
    session: Session,
    ctx: JobContext,
    handler: JobHandler,
) -> JobResult:
    """Run a handler behind a savepoint while tolerating handler-local commits.

    The handler session joins the engine connection through its own short-lived
    savepoints. Its ``commit()`` calls can therefore release progress points
    without closing the outer savepoint owned here. An exception rolls that
    outer savepoint back before the engine records the failed attempt.
    """

    engine_job = ctx.job
    session.flush()
    handler_session = _HandlerSession(
        bind=session.connection(),
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    )
    boundary = _AttemptBoundary(session, handler_session)
    handler_session.info["attempt_boundary"] = boundary
    handler_job = handler_session.get(Job, engine_job.id)
    if handler_job is None:
        handler_session.close()
        boundary.savepoint.rollback()
        raise ValueError(f"job id={engine_job.id} disappeared before handler dispatch")
    ctx.session = handler_session
    ctx.job = handler_job
    try:
        result = handler(ctx)
        Session.commit(handler_session)
        handled_objects = list(handler_session.info.get("handler_flushed_objects", ()))
        boundary.savepoint.commit()
    except BaseException:
        batch_state_touched = bool(handler_session.info.get("checkpoint_batch_state_touched"))
        failed_batch_state = deepcopy(
            dict(handler_job.__dict__.get("step_state") or {}).get(CHECKPOINT_BATCH_STATE_KEY)
        )
        handler_session.rollback()
        if boundary.savepoint.is_active:
            boundary.savepoint.rollback()
        if batch_state_touched:
            session.expire(engine_job)
            prior = dict(engine_job.step_state or {})
            if failed_batch_state:
                prior[CHECKPOINT_BATCH_STATE_KEY] = failed_batch_state
            else:
                prior.pop(CHECKPOINT_BATCH_STATE_KEY, None)
            engine_job.step_state = prior
            session.flush([engine_job])
        raise
    else:
        session.expire_all()
        merged_objects: set[object] = set()
        for instance in handled_objects:
            merged_objects.add(session.merge(instance, load=False))
        session.info["handler_merged_objects"] = merged_objects
        return result
    finally:
        handler_session.close()
        ctx.session = session
        ctx.job = engine_job


def _record_projection_condition(
    session: Session,
    job: Job,
    attempt: JobAttempt,
    projection: ConditionProjection,
) -> None:
    _record_reconciler_condition(
        session,
        job,
        attempt,
        condition=projection.condition,
        reason=projection.reason,
        message=projection.message,
        next_eligible_at=projection.next_eligible_at,
        blocked_tool=projection.blocked_tool,
        auto_block=projection.auto_block,
    )


def _record_reconciler_condition(
    session: Session,
    job: Job,
    attempt: JobAttempt,
    *,
    condition: str | None,
    reason: str | None = None,
    message: str | None = None,
    next_eligible_at: dt.datetime | None = None,
    blocked_tool: tuple[str, str] | None = None,
    auto_block: bool = True,
) -> None:
    if job.recon_domain is None:
        return
    if job.recon_target_key is None:
        raise ValueError(f"reconciler job id={job.id} has recon_domain but no target key")
    record_condition(
        session,
        domain=job.recon_domain,
        target_key=job.recon_target_key,
        condition=condition,
        reason=reason,
        message=message,
        attempt=attempt,
        next_eligible_at=next_eligible_at,
        blocked_tool=blocked_tool,
        auto_block=auto_block,
    )


def claim_pending(
    session: Session,
    *,
    leases: LeaseManager | None = None,
    now: dt.datetime | None = None,
) -> Job | None:
    """Atomically claim the first eligible pending job that fits leases."""
    claim_time = now or _utcnow()
    manager = leases or LeaseManager(WorkerConfig.defaults().capacities)
    for job in _pending_candidates(session, now=claim_time):
        required = normalize_required_resources(job.required_resources)
        if not manager.fits(required):
            continue
        claimed = _claim_job_by_id(session, job.id, now=claim_time)
        if claimed is not None:
            return claimed
    return None


def run_pending(session: Session, *, limit: int = 1) -> list[tuple[int, JobResult]]:
    """Run up to `limit` PENDING jobs sequentially. Returns (job_id, result).

    `limit=0` runs everything currently pending.
    """
    results: list[tuple[int, JobResult]] = []
    remaining = limit if limit > 0 else None
    while remaining is None or remaining > 0:
        job = claim_pending(session)
        if job is None:
            break
        result = run_one(session, job.id)
        results.append((job.id, result))
        # Flush so the next claim sees the status transition.
        session.flush()
        if remaining is not None:
            remaining -= 1
    return results


def pending_candidates(session: Session, *, now: dt.datetime | None = None) -> list[Job]:
    """Return pending jobs whose delay and prerequisites allow dispatch."""
    return _pending_candidates(session, now=now or _utcnow())


def claim_job_by_id(
    session: Session,
    job_id: int,
    *,
    now: dt.datetime | None = None,
) -> Job | None:
    """Guarded ``PENDING -> RUNNING`` claim for a scheduler-selected job."""
    return _claim_job_by_id(session, job_id, now=now or _utcnow())


def reset_orphaned_running_jobs(session: Session) -> int:
    """Reset RUNNING jobs and retain the startup-observed crash gap as a fact."""

    now = _utcnow()
    rows = list(session.scalars(select(Job).where(Job.status == JobStatus.RUNNING)))
    marker = f"orphaned RUNNING at startup, reset to PENDING at {now.isoformat()}"
    for job in rows:
        prior = dict(job.step_state or {})
        engine_observations = list(prior.get("engine_observations") or [])
        engine_observations.append({"note": marker})
        job.step_state = {**prior, "engine_observations": engine_observations}
        job.last_error = marker
        job.status = JobStatus.PENDING
        job.started_at = None
        job.not_before = now
    session.flush(rows)
    return len(rows)


def job_context(
    session: Session,
    job: Job,
    *,
    granted_leases: dict[str, int] | None = None,
) -> JobContext:
    """Build and seed the context used by handlers and engine-side failures."""

    ctx = JobContext(
        session=session,
        job=job,
        granted_leases=dict(granted_leases or {}),
    )
    ctx.touch(f"job:{job.kind}")
    _touch_param_components(ctx, job.params or {})
    return ctx


def attempt_detail(ctx: JobContext) -> dict[str, Any]:
    """Build the single engine-owned attempt payload from a job context."""

    detail: dict[str, Any] = {
        "step_state": dict(ctx.job.step_state or {}),
        "components": list(dict.fromkeys(ctx.components)),
        "observations": [dict(observation) for observation in ctx.observations],
    }
    if ctx.component_parents:
        detail["component_parents"] = [dict(relation) for relation in ctx.component_parents]
    return detail


def _touch_param_components(ctx: JobContext, params: dict[str, Any]) -> None:
    """Tag exact component identities visible before handler dispatch."""

    for key in ("asset_hash", "content_sha256"):
        value = _component_value(params.get(key))
        if value is not None:
            suffix = value if value.startswith("sha256:") else f"sha256:{value}"
            ctx.touch(f"asset:{suffix}")
    for key in ("tape", "tape_id", "tape_uuid", "media_id"):
        value = _component_value(params.get(key))
        if value is not None:
            ctx.touch(f"tape:{value}")
    for key in ("drive", "drive_id", "drive_element_address"):
        value = _component_value(params.get(key))
        if value is not None:
            ctx.touch(f"drive:{value}")
    for key in ("library", "library_id", "library_uuid"):
        value = _component_value(params.get(key))
        if value is not None:
            ctx.touch(f"library:{value}")
    for key in ("backend", "backend_name", "target_backend"):
        value = _component_value(params.get(key))
        if value is not None:
            ctx.touch(f"backend:{value}")
    for key in ("dest_path", "destination", "output_path"):
        value = _component_value(params.get(key))
        if value is not None:
            destination = f"{socket.gethostname()}:{value}" if value.startswith("/") else value
            ctx.touch(f"dest:{destination}")


def _component_value(value: Any) -> str | None:
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, str) and value:
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return None


def apply_retry_policy(
    session: Session,
    job: Job,
    *,
    config: WorkerConfig,
    now: dt.datetime | None = None,
) -> None:
    """Re-enqueue a failed job when its retry policy has attempts remaining."""
    if job.status != JobStatus.FAILED:
        return
    retry = config.retry_for_kind(job.kind)
    if job.attempts >= retry.max_attempts:
        return
    base = now or _utcnow()
    job.status = JobStatus.PENDING
    job.not_before = base + dt.timedelta(
        seconds=retry.delay_seconds(job.attempts, max_seconds=config.max_backoff_seconds)
    )
    job.started_at = None
    job.finished_at = None
    session.flush()


def _pending_candidates(session: Session, *, now: dt.datetime) -> list[Job]:
    rows = list(
        session.scalars(
            select(Job)
            .where(
                Job.status == JobStatus.PENDING,
                Job.not_before <= now,
            )
            .order_by(Job.priority, Job.created_at, Job.id)
        )
    )
    return [
        job
        for job in rows
        if _prerequisites_succeeded(session, job) and _dispatch_gate_allows(session, job)
    ]


def _live_job_for_dedupe(session: Session, dedupe_key: str) -> Job | None:
    return session.scalars(
        select(Job)
        .where(
            Job.dedupe_key == dedupe_key,
            Job.status.in_(LIVE_JOB_STATUS_VALUES),
        )
        .order_by(Job.id)
        .limit(1)
    ).one_or_none()


def _dispatch_gate_allows(session: Session, job: Job) -> bool:
    """Consult the kind's registered release gate; a raising gate releases."""

    gate = get_dispatch_gate(job.kind)
    if gate is None:
        return True
    try:
        return gate(session, job)
    except Exception:
        traceback.print_exc()
        return True


def _prerequisites_succeeded(session: Session, job: Job) -> bool:
    for prereq_id in job.prerequisites or []:
        prereq = session.get(Job, prereq_id)
        if prereq is None or prereq.status != JobStatus.SUCCEEDED:
            return False
    return True


def _claim_job_by_id(session: Session, job_id: int, *, now: dt.datetime) -> Job | None:
    result = cast(
        CursorResult[Any],
        session.execute(
            update(Job)
            .where(Job.id == job_id, Job.status == JobStatus.PENDING)
            .values(
                status=JobStatus.RUNNING,
                started_at=now,
                finished_at=None,
                attempts=Job.attempts + 1,
            )
        ),
    )
    if result.rowcount != 1:
        return None
    session.flush()
    claimed = session.get(Job, job_id)
    if claimed is not None:
        session.refresh(claimed)
    return claimed
