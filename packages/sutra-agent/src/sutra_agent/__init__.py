"""Operator-facing edge agent for Sutradhara receive workflows.

The package is intentionally lightweight: it stores local operator config and a
receive ledger, then delegates all byte-moving and BagIt contract work to
`sutradhara_receive`.
"""

__version__ = "0.0.1"
