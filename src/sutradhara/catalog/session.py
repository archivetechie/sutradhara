"""Engine and session factory for the catalog."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module
from typing import Any

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from sutradhara.catalog.models import Base

# Default DB URL: a SQLite file in the current working directory. Override
# with SUTRADHARA_DB_URL. SQLite is the day-1 default (docs/spec-v0.1.md §11);
# Postgres lands when concurrency demands.
DEFAULT_DB_URL = "sqlite:///./sutradhara.db"


def database_url() -> str:
    return os.environ.get("SUTRADHARA_DB_URL", DEFAULT_DB_URL)


def make_engine(url: str | None = None, *, echo: bool = False) -> Engine:
    """Create a SQLAlchemy Engine with sensible SQLite defaults.

    For SQLite: enables WAL mode and foreign key enforcement (the latter
    is off by default in SQLite; the catalog model relies on it).
    """
    final_url = url or database_url()
    connect_args: dict[str, object] = {}
    if final_url.startswith("sqlite"):
        # Busy timeout: the live services (sutra-serve worker, intake-watch)
        # and in-process harness/job engines share one SQLite file; without a
        # timeout a concurrent writer surfaces as an immediate
        # "database is locked" OperationalError instead of a short wait.
        connect_args["timeout"] = 30
    engine = create_engine(final_url, echo=echo, future=True, connect_args=connect_args)

    if final_url.startswith("sqlite"):
        _install_sqlite_pragmas(engine)

    return engine


def make_read_only_engine(url: str | None = None, *, echo: bool = False) -> Engine:
    """Create an engine whose database transactions cannot write.

    SQLite is opened through its ``mode=ro`` URI and deliberately receives no
    connection pragma hook, so inspection cannot create a WAL or alter the
    catalog's journal mode. Other SQL databases enforce read-only transactions
    when SQLAlchemy begins them.
    """

    final_url = url or database_url()
    parsed = make_url(final_url)
    if parsed.get_backend_name() == "sqlite":
        database = parsed.database
        if not database or database == ":memory:":
            raise ValueError("a read-only SQLite engine requires a file-backed database")
        uri_database = database if database.startswith("file:") else f"file:{database}"
        read_only_url = parsed.set(
            database=uri_database,
            query={**parsed.query, "mode": "ro", "uri": "true"},
        )
        return create_engine(read_only_url, echo=echo, future=True)

    engine = create_engine(parsed, echo=echo, future=True)

    @event.listens_for(engine, "begin")
    def _set_transaction_read_only(connection: Any) -> None:
        connection.exec_driver_sql("SET TRANSACTION READ ONLY")

    return engine


def _install_sqlite_pragmas(engine: Engine) -> None:
    @event.listens_for(engine, "connect")
    def _set_pragmas(dbapi_connection: Any, _conn_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.execute("PRAGMA synchronous = NORMAL")
        cursor.close()


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


def create_all(engine: Engine) -> None:
    """Create all tables. Convenience for tests and the day-1 CLI bootstrap.

    Production schema changes go through Alembic migrations; this helper
    exists so tests don't need to run Alembic on each fixture.
    """
    import_module("sutradhara.jobs.models")
    import_module("sutradhara.api.store")
    import_module("sutradhara.api.live_capabilities")
    import_module("sutradhara.grpc.store")
    import_module("sutradhara.hdcache.models")
    Base.metadata.create_all(engine)


def reset_all(engine: Engine) -> None:
    """Drop and recreate all catalog tables for clean-slate local development."""
    import_module("sutradhara.jobs.models")
    import_module("sutradhara.api.store")
    import_module("sutradhara.api.live_capabilities")
    import_module("sutradhara.grpc.store")
    import_module("sutradhara.hdcache.models")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


@contextmanager
def session_scope(engine: Engine) -> Iterator[Session]:
    """Yield a session, committing on success and rolling back on exception."""
    factory = make_session_factory(engine)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# Deterministic stringification of a native_locator dict for the UNIQUE
# constraint on (backend_id, native_locator_key). Sorted keys, compact
# separators, default JSON encoding — must match exactly across writers.
def locator_key(native_locator: dict[str, Any]) -> str:
    return json.dumps(native_locator, sort_keys=True, separators=(",", ":"))
