"""Fail-closed live operator capability resolution from a synchronized SQL registry.

HTTP admission headers remain a trusted snapshot. Agent restore opens use this
separate authoritative source so an operator grant can be revoked after admission.
Provisioning/synchronization of the registry is deliberately outside RM1.1.
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Callable

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Engine,
    ForeignKey,
    String,
    UniqueConstraint,
    delete,
    select,
)
from sqlalchemy.orm import Mapped, Session, mapped_column

from sutradhara.api.identity import GROUP_CAPABILITIES
from sutradhara.catalog.models import Base
from sutradhara.catalog.session import make_session_factory

LOG = logging.getLogger(__name__)
KNOWN_CAPABILITIES = frozenset(cap for grants in GROUP_CAPABILITIES.values() for cap in grants)
CapabilitySource = Callable[[str], frozenset[str]]


class OperatorCapabilitySync(Base):
    """Freshness boundary for one authoritative synchronized capability snapshot."""

    __tablename__ = "operator_capability_sync"

    operator: Mapped[str] = mapped_column(String(256), primary_key=True)
    synchronized_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_until: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class OperatorLiveCapability(Base):
    """One synchronized, currently effective operator capability grant."""

    __tablename__ = "operator_live_capability"
    __table_args__ = (
        CheckConstraint(
            "capability IN ('can_view', 'can_receive', 'can_restore', 'can_logs', "
            "'can_admin', 'can_restore_p2', 'can_restore_p3')",
            name="ck_operator_live_capability_value",
        ),
        UniqueConstraint("operator", "capability", name="uq_operator_live_capability"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    operator: Mapped[str] = mapped_column(
        String(256),
        ForeignKey("operator_capability_sync.operator", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    capability: Mapped[str] = mapped_column(String(64), nullable=False)


class DatabaseCapabilitySource:
    """Read the deliberately synchronized capability registry in a fresh transaction."""

    def __init__(self, engine: Engine) -> None:
        self._factory = make_session_factory(engine)

    def __call__(self, operator: str) -> frozenset[str]:
        with self._factory() as session:
            sync = session.get(OperatorCapabilitySync, operator)
            if sync is None or _aware(sync.valid_until) <= dt.datetime.now(dt.UTC):
                return frozenset()
            values = session.scalars(
                select(OperatorLiveCapability.capability).where(
                    OperatorLiveCapability.operator == operator
                )
            ).all()
        if any(value not in KNOWN_CAPABILITIES for value in values):
            raise ValueError("live capability registry contains an unknown capability")
        return frozenset(values)


class LiveCapabilityResolver:
    """Resolve current grants and deny on source errors or malformed responses."""

    def __init__(self, source: CapabilitySource) -> None:
        self._source = source

    @classmethod
    def from_database(cls, engine: Engine) -> LiveCapabilityResolver:
        return cls(DatabaseCapabilitySource(engine))

    def capabilities_for(self, operator: str) -> frozenset[str]:
        try:
            capabilities = self._source(operator)
            if any(capability not in KNOWN_CAPABILITIES for capability in capabilities):
                raise ValueError("live capability source returned an unknown capability")
            return frozenset(capabilities)
        except Exception:
            LOG.warning("live capability resolution failed closed for operator=%s", operator)
            return frozenset()

    def has_capability(self, operator: str, capability: str) -> bool:
        if capability not in KNOWN_CAPABILITIES:
            return False
        return capability in self.capabilities_for(operator)


def replace_operator_capabilities(
    session: Session,
    *,
    operator: str,
    capabilities: frozenset[str],
    valid_for: dt.timedelta = dt.timedelta(minutes=5),
    now: dt.datetime | None = None,
) -> None:
    """Atomically replace one synchronized snapshot with a bounded freshness lease."""

    if not operator or valid_for <= dt.timedelta(0):
        raise ValueError("operator and a positive capability snapshot lifetime are required")
    if any(capability not in KNOWN_CAPABILITIES for capability in capabilities):
        raise ValueError("capability snapshot contains an unknown capability")
    timestamp = now or dt.datetime.now(dt.UTC)
    sync = session.get(OperatorCapabilitySync, operator)
    if sync is None:
        sync = OperatorCapabilitySync(operator=operator)
        session.add(sync)
    sync.synchronized_at = timestamp
    sync.valid_until = timestamp + valid_for
    session.execute(
        delete(OperatorLiveCapability).where(OperatorLiveCapability.operator == operator)
    )
    session.add_all(
        OperatorLiveCapability(operator=operator, capability=capability)
        for capability in sorted(capabilities)
    )


def _aware(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)
