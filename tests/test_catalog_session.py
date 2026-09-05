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
