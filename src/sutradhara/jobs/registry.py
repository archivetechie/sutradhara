"""Handler registry and dispatch.

A handler is a callable that takes a `JobContext` (session + job) and
returns a `JobResult`. Handlers are registered by `kind` string; the
engine looks up the handler at dispatch time.

Importing a handler module is the side-effect that registers it.
`sutradhara.jobs.handlers` imports each kind's module so all built-in
handlers are available after `import sutradhara.jobs.handlers`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.orm import Session

from sutradhara.jobs.models import Job


@dataclass
class JobContext:
    """What a handler sees when invoked."""

    session: Session
    job: Job
    granted_leases: dict[str, int] = field(default_factory=dict)


@dataclass
class JobResult:
    """What a handler returns.

    `ok=True` means "the job machinery worked end-to-end" — NOT that the
    underlying check succeeded. A verify job whose result reports
    integrity failure is still `ok=True`; the bad-integrity outcome shows
    up in `detail` and (separately) in catalog state (copy.health).
    """

    ok: bool
    detail: str = ""
    step_state: dict[str, Any] = field(default_factory=dict)


JobHandler = Callable[[JobContext], JobResult]


class HandlerNotRegistered(Exception):
    """No handler is registered for this `kind`."""


_HANDLERS: dict[str, JobHandler] = {}


def register_handler(kind: str) -> Callable[[JobHandler], JobHandler]:
    """Decorator to register a handler under a job `kind`."""

    def decorator(fn: JobHandler) -> JobHandler:
        if kind in _HANDLERS:
            raise ValueError(f"handler already registered for kind {kind!r}")
        _HANDLERS[kind] = fn
        return fn

    return decorator


def get_handler(kind: str) -> JobHandler:
    try:
        return _HANDLERS[kind]
    except KeyError as e:
        raise HandlerNotRegistered(
            f"no handler registered for kind {kind!r}; known kinds: {sorted(_HANDLERS)}"
        ) from e


def registered_kinds() -> Mapping[str, JobHandler]:
    """Snapshot of currently-registered kinds (test convenience)."""
    return dict(_HANDLERS)
