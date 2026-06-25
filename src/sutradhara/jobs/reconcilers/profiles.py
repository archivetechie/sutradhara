"""Prepare-profile registry for derivation desired state.

Profiles are code-reviewed policy: a source artifact class plus a recorded
prepare profile selects derivation jobs, and each job advertises the facts it
must make for the derivation reconciler to observe completion.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from sutradhara.catalog.types import MediaKind

FactType = Literal["derivation", "index"]


@dataclass(frozen=True)
class FactSpec:
    """One fact a derivation profile entry is responsible for producing."""

    kind: str
    fact_type: FactType


@dataclass(frozen=True)
class DerivationEntry:
    """One derivation job target declared by a prepare profile."""

    job_kind: str
    input_media_kind: MediaKind | None
    produces: tuple[FactSpec, ...]
    output_class: str | None
    params: dict[str, object] = field(default_factory=dict)
    resources: tuple[dict[str, object], ...] = ()


PROFILES: dict[tuple[str, str], tuple[DerivationEntry, ...]] = {
    ("s-masters", "hd-review"): (
        DerivationEntry(
            job_kind="transcode",
            input_media_kind=MediaKind.VIDEO,
            produces=(
                FactSpec(kind="mezz", fact_type="derivation"),
                FactSpec(kind="preview", fact_type="derivation"),
            ),
            output_class="s-proxy",
            resources=({"pool": "cpu", "count": 8},),
        ),
        DerivationEntry(
            job_kind="pfr-index",
            input_media_kind=MediaKind.VIDEO,
            produces=(FactSpec(kind="pfr-index-v1", fact_type="index"),),
            output_class=None,
            resources=({"pool": "io", "count": 1}, {"pool": "cpu", "count": 1}),
        ),
    ),
    ("s-masters", "proxy-review"): (
        DerivationEntry(
            job_kind="transcode",
            input_media_kind=MediaKind.VIDEO,
            produces=(
                FactSpec(kind="mezz", fact_type="derivation"),
                FactSpec(kind="preview", fact_type="derivation"),
            ),
            output_class="s-proxy",
            resources=({"pool": "cpu", "count": 8},),
        ),
    ),
}


def known_profile_names(
    profiles: dict[tuple[str, str], tuple[DerivationEntry, ...]] | None = None,
) -> set[str]:
    """Return every profile name declared by the registry."""

    return {profile_name for _artifactclass, profile_name in (profiles or PROFILES)}


def entries_for(
    artifactclass: str,
    profile_name: str | None,
    media_kind: MediaKind,
    *,
    profiles: dict[tuple[str, str], tuple[DerivationEntry, ...]] | None = None,
) -> tuple[DerivationEntry, ...]:
    """Return matching entries after class/profile lookup and media filtering."""

    if profile_name is None:
        return ()
    candidates = (profiles or PROFILES).get((artifactclass, profile_name), ())
    filtered = tuple(
        entry
        for entry in candidates
        if entry.input_media_kind is None or entry.input_media_kind == media_kind
    )
    _validate_unique_job_kinds((artifactclass, profile_name), filtered)
    return filtered


def entry_for_job(
    artifactclass: str,
    profile_name: str | None,
    media_kind: MediaKind,
    job_kind: str,
) -> DerivationEntry | None:
    """Resolve the single matching profile entry for a target job kind."""

    for entry in entries_for(artifactclass, profile_name, media_kind):
        if entry.job_kind == job_kind:
            return entry
    return None


def validate_profiles(
    profiles: dict[tuple[str, str], tuple[DerivationEntry, ...]] | None = None,
) -> None:
    """Validate profile registry invariants at import time and in tests."""

    for key, entries in (profiles or PROFILES).items():
        if not key[0] or not key[1]:
            raise ValueError(f"profile key must be (artifactclass, profile_name); got {key!r}")
        _validate_unique_job_kinds(key, entries)
        for entry in entries:
            if not entry.job_kind:
                raise ValueError(f"profile {key!r} has entry with empty job_kind")
            if not entry.produces:
                raise ValueError(f"profile {key!r} entry {entry.job_kind!r} produces no facts")
            for fact in entry.produces:
                if not fact.kind:
                    raise ValueError(
                        f"profile {key!r} entry {entry.job_kind!r} has empty fact kind"
                    )
                if fact.fact_type not in ("derivation", "index"):
                    raise ValueError(
                        f"profile {key!r} entry {entry.job_kind!r} has invalid fact type "
                        f"{fact.fact_type!r}"
                    )


def _validate_unique_job_kinds(
    key: tuple[str, str],
    entries: tuple[DerivationEntry, ...],
) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for entry in entries:
        if entry.job_kind in seen:
            duplicates.add(entry.job_kind)
        seen.add(entry.job_kind)
    if duplicates:
        duplicate_list = ", ".join(sorted(duplicates))
        raise ValueError(f"profile {key!r} has duplicate derivation job_kind(s): {duplicate_list}")


validate_profiles()
