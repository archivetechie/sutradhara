"""Bind backend session-open facts to the currently executing job context.

Deep storage helpers can open a Remanence session several layers below a job
handler. A context-local callback lets them report the returned session fields
immediately without writing attempt rows or threading the context through every
storage helper API.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar

SessionOpenObserver = Callable[..., None]

_SESSION_OPEN_OBSERVER: ContextVar[SessionOpenObserver | None] = ContextVar(
    "sutradhara_job_session_open_observer",
    default=None,
)


@contextmanager
def bind_session_open_observer(observer: SessionOpenObserver) -> Iterator[None]:
    """Route session-open reports to one job context for a handler call."""

    token = _SESSION_OPEN_OBSERVER.set(observer)
    try:
        yield
    finally:
        _SESSION_OPEN_OBSERVER.reset(token)


def report_session_open(
    *,
    session_id: bytes | str,
    drive_element_address: int,
    tape_uuid: bytes | str | None = None,
    library: bytes | str | None = None,
) -> None:
    """Report only the factual fields returned when a tape session opens."""

    observer = _SESSION_OPEN_OBSERVER.get()
    if observer is None:
        return
    observer(
        session_id=session_id,
        drive_element_address=drive_element_address,
        tape_uuid=tape_uuid,
        library=library,
    )
