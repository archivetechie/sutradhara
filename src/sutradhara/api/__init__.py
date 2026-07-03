"""HTTP API package for the operator console integration."""

from __future__ import annotations

__all__ = ["create_app"]


def __getattr__(name: str) -> object:
    if name == "create_app":
        from sutradhara.api.app import create_app

        return create_app
    raise AttributeError(name)
