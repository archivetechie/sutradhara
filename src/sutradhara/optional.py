"""Availability checks and clear errors for optional Sutradhara integrations.

The base orchestrator must remain importable from a standalone checkout.  This
module keeps feature probes independent of the optional implementation modules,
so importing the CLI or worker never imports a missing third-party package.
"""

from __future__ import annotations

from importlib.util import find_spec


class OptionalDependencyError(RuntimeError):
    """Raised when an explicitly requested feature lacks its integration package."""


def pfr_core_available() -> bool:
    """Return whether the separately distributed PFR integration can be imported."""

    try:
        return find_spec("pfr_core") is not None
    except (ImportError, ValueError):
        return False


def require_pfr_core() -> None:
    """Fail clearly when a caller requests PFR without ``format-anatomy`` installed."""

    if not pfr_core_available():
        raise OptionalDependencyError(
            "partial-file restore requires the optional format-anatomy package; "
            "install a compatible format-anatomy distribution in this environment"
        )
