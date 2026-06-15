"""Pool catalog mutation helpers.

Pools are the storage-policy surface. A pool owns its representation, so once a
copy has landed in the pool, changing that representation would silently retag
stored bytes. This module provides the explicit mutation API that enforces the
immutability rule.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from sutradhara.catalog.models import Copy, Pool
from sutradhara.sealing.port import Representation


class PoolError(Exception):
    """Base class for pool catalog errors."""


class UnknownPool(PoolError):
    """The requested pool id is not in the catalog."""


class PoolRepresentationImmutable(PoolError):
    """A pool with existing copies cannot change representation."""


def set_pool_representation(
    session: Session,
    pool_id: str,
    representation: Representation | str,
) -> Pool:
    """Set a pool representation, enforcing immutability after first copy."""
    pool = session.get(Pool, pool_id)
    if pool is None:
        raise UnknownPool(f"no Pool with id {pool_id!r}")
    new_value = (
        representation.value
        if isinstance(representation, Representation)
        else Representation(representation).value
    )
    if pool.representation == new_value:
        return pool
    copy_exists = session.scalars(select(Copy.id).where(Copy.pool_id == pool_id).limit(1)).first()
    if copy_exists is not None:
        raise PoolRepresentationImmutable(
            f"pool {pool_id!r} already contains copies; representation is immutable"
        )
    pool.representation = new_value
    session.flush()
    return pool
