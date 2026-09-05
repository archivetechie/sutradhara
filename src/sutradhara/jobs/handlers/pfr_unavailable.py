"""Blocked job handler used when the optional PFR integration is not installed.

Derivation intent remains visible and level-triggered, but a base installation
must not crash its worker merely because ``format-anatomy`` is absent.
"""

from __future__ import annotations

from sutradhara.jobs.reconcilers.conditions import CONDITION_BLOCKED
from sutradhara.jobs.registry import ConditionProjection, JobContext, JobResult, register_handler
from sutradhara.jobs.tool_versions import current_tool_version


@register_handler("pfr-index")
def handle_pfr_unavailable(_ctx: JobContext) -> JobResult:
    """Block a PFR indexing job with an actionable optional-dependency result."""

    detected_version = current_tool_version("format-anatomy")
    missing = detected_version == "unknown"
    blocked_version = "missing" if missing else detected_version
    reason = "optional-dependency-missing" if missing else "optional-dependency-incompatible"
    detail = (
        "partial-file restore requires a compatible optional format-anatomy package; "
        "install or upgrade it in the worker environment"
    )
    return JobResult(
        # Missing an optional package is an operator-visible blocked condition,
        # not an execution failure that should leave a terminal failed job.
        ok=True,
        detail=detail,
        step_state={"pfr_index": {"kind": "blocked", "reason": reason}},
        condition=ConditionProjection(
            condition=CONDITION_BLOCKED,
            reason=reason,
            message=detail,
            blocked_tool=("format-anatomy", blocked_version),
        ),
    )
