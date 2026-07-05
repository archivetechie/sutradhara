"""Structured JSON-line events for the operator log pipeline."""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, TextIO

STRUCTURED_LOGGER_NAME = "sutradhara.structured"
_HANDLER_MARKER = "_sutradhara_structured_handler"

LOGGER = logging.getLogger(STRUCTURED_LOGGER_NAME)
LOGGER.addHandler(logging.NullHandler())
LOGGER.propagate = False


def configure_structured_stdout_logging(stream: TextIO | None = None) -> None:
    """Emit structured events as raw JSON lines on stdout.

    The worker service is journal-collected from stdout/stderr, so the formatter
    must leave the JSON payload untouched for the Drishti collector.
    """

    target = stream or sys.stdout
    for handler in list(LOGGER.handlers):
        if getattr(handler, _HANDLER_MARKER, False):
            LOGGER.removeHandler(handler)
    handler = logging.StreamHandler(target)
    handler.setFormatter(logging.Formatter("%(message)s"))
    setattr(handler, _HANDLER_MARKER, True)
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)


def emit_structured_event(event: str, **fields: Any) -> None:
    """Write one operational event if structured logging has been configured."""

    payload: dict[str, Any] = {
        "event": event,
        "content_tier": "operational",
        "redacted_fields": [],
    }
    payload.update(fields)
    LOGGER.info(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str))
