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
from pathlib import Path
from typing import Any

SessionOpenObserver = Callable[..., None]
TapeLocatorObserver = Callable[..., None]
BatchWrittenObserver = Callable[..., None]
BatchCheckpointedObserver = Callable[..., None]

_SESSION_OPEN_OBSERVER: ContextVar[SessionOpenObserver | None] = ContextVar(
    "sutradhara_job_session_open_observer",
    default=None,
)
_TAPE_LOCATOR_OBSERVER: ContextVar[TapeLocatorObserver | None] = ContextVar(
    "sutradhara_job_tape_locator_observer",
    default=None,
)
_BATCH_WRITTEN_OBSERVER: ContextVar[BatchWrittenObserver | None] = ContextVar(
    "sutradhara_job_batch_written_observer",
    default=None,
)
_BATCH_CHECKPOINTED_OBSERVER: ContextVar[BatchCheckpointedObserver | None] = ContextVar(
    "sutradhara_job_batch_checkpointed_observer",
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


@contextmanager
def bind_batch_observers(
    written: BatchWrittenObserver,
    checkpointed: BatchCheckpointedObserver,
) -> Iterator[None]:
    """Route provisional batch transitions into the current durable job state."""

    written_token = _BATCH_WRITTEN_OBSERVER.set(written)
    checkpointed_token = _BATCH_CHECKPOINTED_OBSERVER.set(checkpointed)
    try:
        yield
    finally:
        _BATCH_CHECKPOINTED_OBSERVER.reset(checkpointed_token)
        _BATCH_WRITTEN_OBSERVER.reset(written_token)


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


def report_batch_written(
    *,
    batch_id: str,
    provisional_ordinal: int,
    caller_object_id: str,
    source: Path | str,
) -> None:
    """Record that one caller object must be re-sent if its batch is lost."""

    observer = _BATCH_WRITTEN_OBSERVER.get()
    if observer is None:
        return
    observer(
        batch_id=batch_id,
        provisional_ordinal=provisional_ordinal,
        caller_object_id=caller_object_id,
        source=str(source),
    )


def report_batch_checkpointed(
    *,
    batch_id: str,
    caller_object_ids: tuple[str, ...],
) -> None:
    """Remove only objects proven durable by one checkpoint response."""

    observer = _BATCH_CHECKPOINTED_OBSERVER.get()
    if observer is None:
        return
    observer(batch_id=batch_id, caller_object_ids=caller_object_ids)
