"""Reflection gates for the manifest-driven P1 schema conventions."""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy import CheckConstraint, Column

import sutradhara.api.live_capabilities
import sutradhara.api.store
import sutradhara.grpc.store
import sutradhara.hdcache.models
import sutradhara.jobs.models  # noqa: F401
from sutradhara.catalog.models import Base
from sutradhara.schema_conventions import (
    ALLOWED_ON_DELETE_BY_ROLE,
    CLOSED_VOCABULARY_COLUMNS,
    FOREIGN_KEYS,
    IDENTIFIER_CONVENTIONS,
    JSON_COLUMN_CONTRACTS,
    REGISTRY_TABLES,
    SEMANTIC_COLUMN_GROUPS,
    TIMESTAMP_CONVENTIONS,
    UPDATED_AT_COLUMNS,
    VOCABULARIES,
    vocabulary_check_sql,
)


def test_foreign_key_manifest_is_exhaustive_and_role_validated() -> None:
    """Every exact FK has one declared role and an allowed delete action."""

    reflected = {
        _foreign_key_key(constraint): constraint
        for table in Base.metadata.tables.values()
        for constraint in table.foreign_key_constraints
    }
    assert set(reflected) == set(FOREIGN_KEYS)
    for key, declaration in FOREIGN_KEYS.items():
        ondelete = reflected[key].ondelete
        assert ondelete is not None, key
        assert ondelete.upper() in ALLOWED_ON_DELETE_BY_ROLE[declaration.role], key


def test_closed_vocabularies_are_manifest_rendered_checks() -> None:
    """Every declared closed set is represented by its single canonical SQL."""

    assert set(CLOSED_VOCABULARY_COLUMNS.values()) == set(VOCABULARIES)
    reflected_simple_columns: set[str] = set()
    for table in Base.metadata.tables.values():
        for constraint in table.constraints:
            if not isinstance(constraint, CheckConstraint):
                continue
            match = re.fullmatch(
                r"(?:(\w+) IS NULL OR )?(\w+) IN \(.+\)",
                _normalize_sql(str(constraint.sqltext)),
            )
            if match is not None:
                nullable_column, column_name = match.groups()
                assert nullable_column in {None, column_name}, constraint.name
                reflected_simple_columns.add(f"{table.name}.{column_name}")
    assert reflected_simple_columns == set(CLOSED_VOCABULARY_COLUMNS)

    for qualified_column, vocabulary in CLOSED_VOCABULARY_COLUMNS.items():
        table_name, column_name = qualified_column.split(".", 1)
        table = Base.metadata.tables[table_name]
        expected = _normalize_sql(vocabulary_check_sql(column_name, vocabulary))
        matches = [
            constraint
            for constraint in table.constraints
            if isinstance(constraint, CheckConstraint)
            and _normalize_sql(str(constraint.sqltext)) == expected
        ]
        assert len(matches) == 1, qualified_column


def test_cross_cut_convention_members_reference_real_schema_objects() -> None:
    """Semantic, identifier, JSON, and timestamp manifest entries stay live."""

    grouped_columns: set[str] = set()
    for columns in SEMANTIC_COLUMN_GROUPS.values():
        assert grouped_columns.isdisjoint(columns)
        grouped_columns.update(columns)
        for qualified_column in columns:
            _manifest_column(qualified_column)

    reflected_identifiers = {
        table.name: tuple(column.name for column in table.primary_key.columns)
        for table in Base.metadata.tables.values()
    }
    assert reflected_identifiers == IDENTIFIER_CONVENTIONS

    for qualified_column in JSON_COLUMN_CONTRACTS:
        assert _manifest_column(qualified_column).type.python_type in {dict, list}

    for qualified_column in TIMESTAMP_CONVENTIONS:
        _manifest_column(qualified_column)


def test_artifactclass_registry_covers_every_artifactclass_column() -> None:
    """Every artifactclass value is checked by the policy-owned registry."""

    assert REGISTRY_TABLES == {"artifactclass": ("name",)}
    for table in Base.metadata.tables.values():
        column = table.c.get("artifactclass")
        if column is None:
            continue
        targets = {foreign_key.target_fullname for foreign_key in column.foreign_keys}
        assert "artifactclass.name" in targets, f"{table.name}.artifactclass"


def test_updated_at_manifest_is_exhaustive_and_has_onupdate() -> None:
    """Projection clocks are declared exhaustively and advance on mutation."""

    reflected = {
        f"{table.name}.updated_at": table.c.updated_at
        for table in Base.metadata.tables.values()
        if "updated_at" in table.c
    }
    assert set(reflected) == set(UPDATED_AT_COLUMNS)
    for key, column in reflected.items():
        assert column.onupdate is not None, key


def test_creation_clocks_never_gain_projection_onupdate_writers() -> None:
    """Creation timestamps remain immutable when their row later mutates."""

    for table in Base.metadata.tables.values():
        created_at = table.c.get("created_at")
        if created_at is not None:
            assert created_at.onupdate is None, f"{table.name}.created_at"


def _foreign_key_key(constraint: object) -> str:
    elements = constraint.elements
    source_table = elements[0].parent.table.name
    source_columns = ",".join(element.parent.name for element in elements)
    target_table = elements[0].column.table.name
    target_columns = ",".join(element.column.name for element in elements)
    return f"{source_table}.{source_columns}->{target_table}.{target_columns}"


def _normalize_sql(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def _manifest_column(qualified_column: str) -> Column[Any]:
    table_name, column_name = qualified_column.split(".", 1)
    table = Base.metadata.tables[table_name]
    return table.c[column_name]
