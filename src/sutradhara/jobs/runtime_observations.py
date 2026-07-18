"""Bind backend tape facts to the currently executing job context.

Deep storage helpers can open a Remanence session several layers below a job
handler or touch a D2 locator below its direct call stack. Context-local callbacks
let them report those facts immediately without writing attempt rows or threading
the context through every storage helper API.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

SessionOpenObserver = Callable[..., None]
TapeLocatorObserver = Callable[..., None]

_SESSION_OPEN_OBSERVER: ContextVar[SessionOpenObserver | None] = ContextVar(
    "sutradhara_job_session_open_observer",
    default=None,
)
_TAPE_LOCATOR_OBSERVER: ContextVar[TapeLocatorObserver | None] = ContextVar(
    "sutradhara_job_tape_locator_observer",
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


@contextmanager
def bind_tape_locator_observer(observer: TapeLocatorObserver) -> Iterator[None]:
    """Route tape locators touched below a handler to its job context."""

    token = _TAPE_LOCATOR_OBSERVER.set(observer)
    try:
        yield
    finally:
        _TAPE_LOCATOR_OBSERVER.reset(token)


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


def report_tape_locator(
    locator: Mapping[str, Any],
    *,
    library: bytes | str | None = None,
) -> None:
    """Report a tape locator at the point where storage I/O uses or returns it."""

    observer = _TAPE_LOCATOR_OBSERVER.get()
    if observer is None:
        return
    observer(locator, library=library)
