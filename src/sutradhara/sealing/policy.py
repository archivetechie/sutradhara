"""Representation policies for placement-tagged replication targets."""

from __future__ import annotations

from collections.abc import Mapping

from sutradhara.sealing.port import Representation

RepresentationPolicy = Mapping[tuple[str, str], str]
DEFAULT_POLICY: RepresentationPolicy = {}


def o_archive_policy() -> RepresentationPolicy:
    """Return the Scenario O policy for the two rem-hosted archive copies."""
    return {
        ("o-archive", "o-copy-1"): Representation.RAO_PLAIN_V1.value,
        ("o-archive", "o-copy-2"): Representation.RAO_AEAD_V1.value,
    }


def n_archive_policy() -> RepresentationPolicy:
    """Return the Scenario N three-copy policy across rem and d2tape."""
    return {
        ("n-archive", "copy-1"): Representation.RAO_PLAIN_V1.value,
        ("n-archive", "copy-2"): Representation.RAO_AEAD_V1.value,
        ("n-archive", "copy-3"): Representation.D2TAR_RAW.value,
    }
