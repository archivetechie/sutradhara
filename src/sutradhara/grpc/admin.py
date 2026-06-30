"""Administrative helpers for gRPC device enrollment.

These functions keep durable enrollment changes coupled to in-process relay
state when a caller has access to the live ``ConnectedDeviceRegistry``.
"""

from __future__ import annotations

from sqlalchemy import Engine

from sutradhara.catalog.session import make_session_factory
from sutradhara.grpc import store
from sutradhara.grpc.registry import ConnectedDeviceRegistry


def revoke_device(
    engine: Engine,
    device_id: str,
    *,
    registry: ConnectedDeviceRegistry | None = None,
) -> int:
    """Revoke every certificate for a device and evict its live stream."""

    factory = make_session_factory(engine)
    with factory.begin() as session:
        count = store.revoke_device(session, device_id)
    if registry is not None:
        registry.evict(device_id)
    return count
