"""Catalog — content-addressed logical assets, copies, and backends.

See docs/spec-v0.1.md §4 (data model).
"""

from sutradhara.catalog.copies import (
    CatalogError,
    UnknownLogicalAsset,
    add_copy,
    lookup_by_hash,
)

__all__ = [
    "CatalogError",
    "UnknownLogicalAsset",
    "add_copy",
    "lookup_by_hash",
]
