"""Handler registry and dispatch.

A handler is a callable that takes a `JobContext` (session + job) and
returns a `JobResult`. Handlers are registered by `kind` string; the
engine looks up the handler at dispatch time.

Importing a handler module is the side-effect that registers it.
`sutradhara.jobs.handlers` imports each kind's module so all built-in
handlers are available after `import sutradhara.jobs.handlers`.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from sutradhara.jobs.models import Job


@dataclass
class JobContext:
    """Mutable per-run state shared by a handler and the attempt recorder.

    Handlers append factual observations and component identities while work is
    in progress. The engine is the only writer that transfers these accumulators
    into the append-only ``JobAttempt`` row, including after handler exceptions.
    """

    session: Session
    job: Job
    granted_leases: dict[str, int] = field(default_factory=dict)
    observations: list[dict[str, Any]] = field(default_factory=list)
    components: list[str] = field(default_factory=list)
    component_parents: list[dict[str, str]] = field(default_factory=list)

    def observe(self, facts: Mapping[str, Any]) -> None:
        """Append one JSON-safe set of raw facts in observation order."""

        if not isinstance(facts, Mapping):
            raise TypeError("job observation must be a mapping")
        self.observations.append(
            {str(key): _json_fact_value(value) for key, value in facts.items()}
        )

    def touch(self, component: str, *, parent: str | None = None) -> None:
        """Record an exact component string and an optional parent relation."""

        if not isinstance(component, str) or not component:
            raise ValueError("job component must be a non-empty string")
        if component not in self.components:
            self.components.append(component)
        if parent is None:
            return
        if not isinstance(parent, str) or not parent:
            raise ValueError("job component parent must be a non-empty string")
        if parent not in self.components:
            self.components.append(parent)
        relation = {"component": component, "parent": parent}
        if relation not in self.component_parents:
            self.component_parents.append(relation)

    def observe_session_open(
        self,
        *,
        session_id: bytes | str,
        drive_element_address: int,
        tape_uuid: bytes | str | None = None,
        library: bytes | str | None = None,
    ) -> None:
        """Record fields returned by an opened Remanence tape session."""

        observation: dict[str, Any] = {
            "session_id": _identity_text(session_id),
            "drive_element_address": drive_element_address,
        }
        if tape_uuid is not None:
            observation["tape_uuid"] = _identity_text(tape_uuid)
        if library is not None:
            observation["library"] = _identity_text(library)
        self.observe(observation)
        drive = f"drive:{drive_element_address}"
        if library is None:
            self.touch(drive)
        else:
            self.touch(drive, parent=f"library:{_identity_text(library)}")


@dataclass(frozen=True)
class ConditionProjection:
    """Axis-B condition projection returned by reconciler-aware handlers.

    The projection intentionally contains no observed-state field: observed
    reality is owned by reconciler observation, not by job handlers.
    """

    condition: str
    reason: str | None = None
    message: str | None = None
    next_eligible_at: dt.datetime | None = None
    blocked_tool: tuple[str, str] | None = None
    auto_block: bool = True


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
    condition: ConditionProjection | None = None


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


def _identity_text(value: bytes | str) -> str:
    """Return a stable JSON/component spelling for an opaque identity."""

    return value.hex() if isinstance(value, bytes) else value


def _json_fact_value(value: Any) -> Any:
    """Normalize common factual values without adding interpretation."""

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return _json_fact_value(value.value)
    if isinstance(value, Mapping):
        return {str(key): _json_fact_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_fact_value(item) for item in value]
    raise TypeError(f"job observation value is not JSON-safe: {type(value).__name__}")
