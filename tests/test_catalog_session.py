"""Catalog engine durability and connection-policy tests."""

from __future__ import annotations

from pathlib import Path

from sutradhara.catalog.session import make_engine


def test_sqlite_writer_uses_full_synchronous_mode(tmp_path: Path) -> None:
    """Non-rebuildable control records require power-loss-safe WAL commits."""

    engine = make_engine(f"sqlite:///{tmp_path / 'catalog.db'}")
    try:
        with engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA journal_mode").scalar_one() == "wal"
            assert connection.exec_driver_sql("PRAGMA synchronous").scalar_one() == 2
    finally:
        engine.dispose()


def test_sqlite_busy_timeout_on_every_connection(tmp_path: Path, monkeypatch) -> None:
    """Check both the DBAPI argument and pragma on fresh pooled connections."""
    from sutradhara.catalog import session

    original = session.create_engine
    captured = {}

    def capture(*args, **kwargs):
        captured.update(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(session, "create_engine", capture)
    engine = make_engine(f"sqlite:///{tmp_path / 'timeout.db'}")
    try:
        assert captured["connect_args"]["timeout"] == 30
        for _ in range(2):
            with engine.connect() as connection:
                assert connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one() == 30000
            engine.dispose()
    finally:
        engine.dispose()
