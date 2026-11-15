"""Commit durable progress before backend work, then reopen the catalog step.

Job forms share the engine's handler savepoint; command forms own a step on
their caller's connection. Each window is a crash boundary: preceding writes
survive, while backend work cannot access either participating session.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from sqlalchemy import event, inspect, select
from sqlalchemy.orm import Session, make_transient, make_transient_to_detached

from sutradhara.jobs.models import Job, JobStatus
from sutradhara.jobs.runtime_observations import bind_batch_observers

if TYPE_CHECKING:
    from sutradhara.jobs.engine import _AttemptBoundary


class CommitPointMissing(RuntimeError):
    """A handler was dispatched without its engine-owned commit point."""


class CommitPoint:
    """Own the current rollback step and transaction-free backend windows."""

    def __init__(
        self,
        session: Session,
        *,
        boundary: _AttemptBoundary | None = None,
        job: Job | None = None,
    ) -> None:
        from sutradhara.jobs.engine import _begin_immediate

        self._session = session
        self._boundary = boundary
        self._connection = session.connection()
        self._sessions = (session,) if boundary is None else (session, boundary.handler_session)
        self._job_id = None if job is None else job.id
        self._job_kind = None if job is None else job.kind
        self._attempt = None if job is None else job.attempts
        if boundary is None:
            if not self._dbapi().in_transaction:
                _begin_immediate(self._connection)
            self._open_step()
        else:
            self._remember_step()
        for owned_session in self._sessions:
            event.listen(owned_session, "pending_to_persistent", self._track_added)
            event.listen(owned_session, "persistent_to_deleted", self._track_deleted)

    def _dbapi(self) -> Any:
        from sutradhara.jobs.engine import AttemptBoundaryError

        dbapi = self._connection.connection.dbapi_connection
        if dbapi is None:
            raise AttemptBoundaryError("commit point connection is unavailable")
        return dbapi

    def _remember_step(self) -> None:
        self._step_added: dict[Session, set[Any]] = {s: set() for s in self._sessions}
        self._step_deleted: dict[Session, set[Any]] = {s: set() for s in self._sessions}

    def _track_added(self, session: Session, instance: Any) -> None:
        self._step_added[session].add(instance)

    def _track_deleted(self, session: Session, instance: Any) -> None:
        self._step_deleted[session].add(instance)

    def _detach(self) -> None:
        """Stop tracking a finished handler on a possibly longer-lived outer session."""
        for session in self._sessions:
            event.remove(session, "pending_to_persistent", self._track_added)
            event.remove(session, "persistent_to_deleted", self._track_deleted)

    def _savepoint(self) -> Any:
        return self._step if self._boundary is None else self._boundary.savepoint

    def _open_step(self) -> None:
        if self._boundary is None:
            # Enlist the ORM in the step so a failed flush rolls back only this
            # savepoint, preserving BEGIN IMMEDIATE and the connection's writer slot.
            self._session.flush()
            self._step = self._session.begin_nested()
            self._session.connection()  # Materialize the savepoint before returning.
        else:
            self._boundary.savepoint = self._connection.begin_nested()
        self._remember_step()

    def rollback_to_step(self) -> None:
        """Discard this step's writes and ORM state without committing work."""
        if self._boundary is not None:
            self._boundary.handler_session.rollback()
        self._savepoint().rollback()
        for session in self._sessions:
            for instance in set(session.new) | self._step_added[session]:
                if instance in session:
                    session.expunge(instance)
            for instance in list(session.deleted):
                session.add(instance)
            for instance in self._step_deleted[session] - self._step_added[session]:
                if inspect(instance).was_deleted:
                    # A flushed DELETE has left the identity map, possibly
                    # detached by a handler-local commit. The Core rollback
                    # restored its row, so restore its identity without an INSERT.
                    make_transient(instance)
                    make_transient_to_detached(instance)
                    session.add(instance)
            session.expire_all()
        self._open_step()

    @contextmanager
    def window(self) -> Iterator[None]:
        """Commit progress, refuse catalog access in the block, and reopen."""
        from sutradhara.jobs.engine import (
            AttemptBoundaryError,
            AttemptSuperseded,
            _begin_immediate,
        )

        for session in self._sessions:
            session.flush()
        if self._boundary is not None:
            Session.commit(self._boundary.handler_session)
        self._savepoint().commit()
        dbapi = self._dbapi()
        if dbapi.in_transaction:
            self._connection.exec_driver_sql("COMMIT")
        self._session.info["commit_point_used"] = True
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
            violation = AttemptBoundaryError("SQL used inside commit point window")
            raise violation

        def refuse_session(*args: Any) -> None:
            nonlocal violation
            violation = AttemptBoundaryError("session used inside commit point window")
            raise violation

        guarded_events = ("do_orm_execute", "before_flush", "before_attach", "before_commit")
        event.listen(self._connection, "before_cursor_execute", refuse_sql)
        for session in self._sessions:
            for name in guarded_events:
                event.listen(session, name, refuse_session)
        try:
            with bind_batch_observers(None, None):
                yield
        finally:
            event.remove(self._connection, "before_cursor_execute", refuse_sql)
            for session in self._sessions:
                for name in guarded_events:
                    event.remove(session, name, refuse_session)
            if any(s.new or s.dirty or s.deleted for s in self._sessions):
                violation = AttemptBoundaryError("session mutated inside commit point window")
            if self._boundary is not None:
                self._boundary.handler_session.rollback()
            for session in self._sessions:
                for instance in list(session.new):
                    session.expunge(instance)
                for instance in list(session.deleted):
                    session.add(instance)
                if self._boundary is None:
                    # begin_nested() flushes unconditionally: discard guarded
                    # mutations before opening the command form's next step.
                    session.expire_all()
            if dbapi.in_transaction:
                self._connection.exec_driver_sql("ROLLBACK")
                violation = AttemptBoundaryError("commit point driver was not idle before reopen")
            _begin_immediate(self._connection, job_id=self._job_id, job_kind=self._job_kind)
            self._open_step()
            for session in self._sessions:
                session.expire_all()
            if self._job_id is not None:
                current = self._session.execute(
                    select(Job.status, Job.attempts).where(Job.id == self._job_id)
                ).one_or_none()
                if current is None or current != (JobStatus.RUNNING, self._attempt):
                    raise AttemptSuperseded(
                        f"superseded: job {self._job_id} changed from RUNNING "
                        f"attempt {self._attempt} to {current}"
                    )
            if violation is not None:
                raise violation


def commit_point_for(session: Session) -> CommitPoint:
    """Return the job form, or install a command form on a non-handler session.

    Installing the command form flushes pending changes before its first step
    opens, so rollback_to_step() does not undo them; the first window commits them.
    """
    from sutradhara.jobs.engine import _HandlerSession

    point = session.info.get("sutradhara_commit_point")
    if isinstance(point, CommitPoint):
        return point
    if isinstance(session, _HandlerSession):
        raise CommitPointMissing("handler session has no job commit point")
    point = CommitPoint(session)
    session.info["sutradhara_commit_point"] = point
    return point
