"""Flush-time catalog map: render, the map-only rem route, failure loop, preflight.

Every test names the failure it guards.

Two of these tests exist specifically because ``LocalArchiveBuilder`` ignores
``map_path`` — a map defect cannot fail a LocalArchiveBuilder test, and two
verify rounds flagged the map route as the one that ships green and fails on
iron. ``test_group_flush_builds_by_map_through_rem_builder`` and
``test_map_route_argv_carries_map_source_root_slash_and_no_inputs_or_rules``
therefore execute ``RemArchiveBuilder``'s own code path, mocking only the
process boundary.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, select

from sutradhara.archive_bundle import enqueue_artifact
from sutradhara.archive_fanout import (
    ArchiveFanoutError,
    BuildArtifact,
    FlushPreflightShort,
    LocalArchiveBuilder,
    RemArchiveBuilder,
    _member_input,
    _member_rows_for_flush,
    _render_flush_map,
    flush_bundle,
)
from sutradhara.artifactclass_policy import (
    ArtifactClassPolicy,
    BundlingPolicy,
    DurabilityPolicy,
    PlacementPolicy,
    apply_artifactclass_policy,
    get_artifactclass_policy,
)
from sutradhara.backend.port import (
    BackendLocator,
    ByteRange,
    CopyRecord,
    StreamKind,
    VerifyResult,
)
from sutradhara.catalog.models import (
    Arrangement,
    Backend,
    Bundle,
    BundleMember,
    Copy,
    IngestItem,
    Intake,
    LogicalAsset,
    Pool,
    StagingTransform,
    Submission,
    SubmissionMember,
)
from sutradhara.catalog.session import create_all, make_engine, session_scope
from sutradhara.catalog.types import (
    BackendKind,
    BackendTier,
    IntakeSourceKind,
    IntakeStatus,
    content_hash,
)
from sutradhara.rem_archive_cli import RemArchiveBuildResult
from sutradhara.sealing.port import Representation
from sutradhara.sealing.rao import RAO_CHUNK_SIZE
from tests.bundle_group_helpers import bundle_kwargs

MAP_HEADER = "archive_path\tsource_path\tsha256\tsize\tingest_item_id"


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = make_engine("sqlite:///:memory:")
    create_all(eng)
    yield eng
    eng.dispose()


def _digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


class _WriteBackend:
    """Minimal writable backend: keeps objects in memory, serves ranged reads."""

    def __init__(self, name: str = "rem") -> None:
        self._name = name
        self._objects: dict[str, bytes] = {}
        self._counter = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def stream_kind(self) -> StreamKind:
        return StreamKind.native_stream

    def write_object_to_pool(
        self,
        source: Path | str,
        pool: str,
        *,
        caller_object_id: str | None = None,
    ) -> CopyRecord:
        data = Path(source).read_bytes()
        digest = content_hash(hashlib.sha256(data).digest())
        self._counter += 1
        object_id = f"{self._name}-{self._counter}"
        self._objects[object_id] = data
        return CopyRecord(
            logical_id=digest,
            native_locator={
                "pool_id": pool,
                "object_id": object_id,
                "content_sha256": digest.hex(),
                "tape_uuid": f"{self._counter:032x}",
                "tape_file_number": self._counter,
            },
            integrity_hash=digest,
            size_bytes=len(data),
        )

    def enumerate(self) -> Iterator[CopyRecord]:
        return iter(())

    def read_range(self, locator: BackendLocator, byte_range: ByteRange) -> bytes:
        data = self._objects[str(locator["object_id"])]
        if byte_range.is_whole_object:
            return data
        return data[byte_range.start : byte_range.end]

    @contextmanager
    def open_range_chunks(
        self,
        locator: BackendLocator,
        byte_range: ByteRange,
        *,
        chunk_bytes: int,
    ) -> Iterator[Iterator[bytes]]:
        data = self._objects[str(locator["object_id"])]
        end = len(data) if byte_range.is_whole_object else byte_range.end

        def chunks() -> Iterator[bytes]:
            for cursor in range(byte_range.start, end, chunk_bytes):
                yield data[cursor : min(cursor + chunk_bytes, end)]

        yield chunks()

    def verify(self, locator: BackendLocator) -> VerifyResult:
        data = self.read_range(locator, ByteRange(0, 0))
        actual = content_hash(hashlib.sha256(data).digest())
        expected = content_hash(bytes.fromhex(str(locator["content_sha256"])))
        return VerifyResult(ok=actual == expected, measured=True, actual_hash=actual)


def _install_class(
    session: Any,
    artifactclass: str,
    *,
    pools: tuple[tuple[str, str], ...],
    target_gb: float = 0.000001,
) -> None:
    """Register backends/pools once, then apply one class policy over them."""
    for pool_id, representation in pools:
        if session.get(Pool, pool_id) is not None:
            continue
        kind = (
            BackendKind.D2_TAPE
            if representation == Representation.D2TAR_RAW.value
            else BackendKind.REM_TAPE
        )
        backend = session.scalars(select(Backend).where(Backend.name == pool_id)).first()
        if backend is None:
            backend = Backend(name=pool_id, kind=kind, tier=BackendTier.SELF_DESCRIBING)
            session.add(backend)
            session.flush()
        session.add(Pool(id=pool_id, backend_id=backend.id, representation=representation))
    session.flush()
    apply_artifactclass_policy(
        session,
        artifactclass,
        ArtifactClassPolicy(
            ruleset=f"rao.{artifactclass}.v1",
            placements=tuple(
                PlacementPolicy(pool_id, role="primary" if index == 0 else "shelf")
                for index, (pool_id, _) in enumerate(pools)
            ),
            bundling=BundlingPolicy(target_gb=target_gb, max_age_seconds=60),
            restore_preference=tuple(pool_id for pool_id, _ in pools),
            expect="messy",
            durability=DurabilityPolicy(min_copies=1, min_impl_families=1),
        ),
    )


def _backends_for(session: Any, backend: _WriteBackend) -> dict[int, _WriteBackend]:
    return {row.id: backend for row in session.scalars(select(Backend))}


def _enqueue(
    session: Any,
    *,
    artifactclass: str,
    source: Path,
    member_path: str,
) -> Bundle:
    data = source.read_bytes()
    asset_hash = _digest(data)
    if session.get(LogicalAsset, asset_hash) is None:
        session.add(LogicalAsset(content_sha256=asset_hash, size_bytes=len(data)))
        session.flush()
    policy = get_artifactclass_policy(session, artifactclass)
    bundle, _, _ = enqueue_artifact(
        session,
        artifactclass=artifactclass,
        policy=policy,
        logical_asset_hash=asset_hash,
        source_path=source,
        member_path=member_path,
    )
    return bundle


# --- map render ------------------------------------------------------------


def test_flush_map_render_is_sorted_tabbed_and_leaves_absent_lineage_empty(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """Golden map for a mixed-class bundle. Guards four separable defects:
    rows emitted in catalog/insertion order instead of sorted by archive path;
    a separator other than a single tab; a catalog-tagged member name being
    re-derived instead of copied verbatim; and a member with no submission
    lineage rendering the literal string "None" (the pre-change
    ``render_source_map`` did exactly that) instead of an empty column."""
    sources = {}
    for name, data in (
        ("photo.tif", b"photo bytes"),
        ("photo~1.tif", b"tagged photo bytes"),
        ("take.wav", b"audio bytes"),
    ):
        path = tmp_path / name
        path.write_bytes(data)
        sources[name] = path

    with session_scope(engine) as s:
        s.add(
            Bundle(
                id="bundle-map",
                **bundle_kwargs(seed="mixed"),
                status="open",
            )
        )
        # Deliberately inserted OUT of archive-path order: the renderer sorts.
        rows = [
            ("photos", "shoot/photo~1.tif", sources["photo~1.tif"]),
            ("audio", "audio/take.wav", sources["take.wav"]),
            ("photos", "shoot/photo.tif", sources["photo.tif"]),
        ]
        for artifactclass, member_path, source in rows:
            data = source.read_bytes()
            s.add(LogicalAsset(content_sha256=_digest(data), size_bytes=len(data)))
            s.flush()
            s.add(
                BundleMember(
                    bundle_id="bundle-map",
                    logical_asset_hash=_digest(data),
                    artifactclass=artifactclass,
                    member_path=member_path,
                    source_path=str(source),
                    size_bytes=len(data),
                    file_sha256=_digest(data),
                )
            )
        s.flush()

        # One member carries submission lineage; the other two do not.
        ingest_item_id = _add_submission_lineage(
            s,
            artifactclass="audio",
            archive_path="audio/take.wav",
            source_path=str(sources["take.wav"]),
            data=sources["take.wav"].read_bytes(),
        )

        bundle = s.get(Bundle, "bundle-map")
        assert bundle is not None
        members = [_member_input(row) for row in _member_rows_for_flush(s, bundle)]
        rendered = _render_flush_map(s, members)

    audio = sources["take.wav"].read_bytes()
    photo = sources["photo.tif"].read_bytes()
    tagged = sources["photo~1.tif"].read_bytes()
    assert rendered == (
        MAP_HEADER
        + "\n"
        + "\t".join(
            (
                "audio/take.wav",
                str(sources["take.wav"]),
                _digest(audio).hex(),
                str(len(audio)),
                str(ingest_item_id),
            )
        )
        + "\n"
        + "\t".join(
            (
                "shoot/photo.tif",
                str(sources["photo.tif"]),
                _digest(photo).hex(),
                str(len(photo)),
                "",
            )
        )
        + "\n"
        + "\t".join(
            (
                "shoot/photo~1.tif",
                str(sources["photo~1.tif"]),
                _digest(tagged).hex(),
                str(len(tagged)),
                "",
            )
        )
        + "\n"
    )
    assert "None" not in rendered
    assert rendered.endswith("\n")
    assert "\r" not in rendered


def _add_submission_lineage(
    session: Any,
    *,
    artifactclass: str,
    archive_path: str,
    source_path: str,
    data: bytes,
) -> int:
    """Create the intake→arrangement→submission chain for one member row."""
    session.add(
        Intake(
            intake_id="intake-lineage",
            operator="op",
            source_kind=IntakeSourceKind.CARD,
            artifactclass=artifactclass,
            status=IntakeStatus.REGISTERED,
        )
    )
    session.flush()
    item = IngestItem(
        intake_id="intake-lineage",
        logical_asset_hash=_digest(data),
        as_received_path=archive_path,
        virtual_path=archive_path,
        size_bytes=len(data),
        artifactclass=artifactclass,
    )
    session.add(item)
    arrangement = Arrangement(
        label="lineage",
        intake_id="intake-lineage",
        artifactclass=artifactclass,
        status="submitted",
    )
    session.add(arrangement)
    session.flush()
    session.add(
        Submission(
            id="submission-lineage",
            arrangement_id=arrangement.id,
            artifactclass=artifactclass,
            source_map_path="/tmp/lineage-source-map.tsv",
            manifest_digest="0" * 64,
            member_count=1,
            submitted_by="op",
        )
    )
    session.flush()
    session.add(
        SubmissionMember(
            submission_id="submission-lineage",
            ingest_item_id=item.id,
            archive_path=archive_path,
            source_path=source_path,
            sha256=_digest(data),
            size_bytes=len(data),
            ord=0,
        )
    )
    session.flush()
    return item.id


def test_flush_map_sha256_is_the_staged_digest_not_the_logical_hash(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """A transformed member's staged bytes differ from its logical asset. The
    map's digest column IS the pre-write content check rem's writer verifies
    against the streamed payload, so it must carry ``file_sha256`` (the staged
    digest). Emitting ``logical_asset_hash`` here would make every transformed
    member fail the build — or, worse on a lenient writer, ship unchecked."""
    original = b"original camera bytes"
    staged_bytes = b"transcoded bytes"
    staged = tmp_path / "clip.staged.mov"
    staged.write_bytes(staged_bytes)

    with session_scope(engine) as s:
        s.add(Bundle(id="bundle-xform", **bundle_kwargs(seed="video"), status="open"))
        s.add(LogicalAsset(content_sha256=_digest(original), size_bytes=len(original)))
        s.flush()
        s.add(
            BundleMember(
                bundle_id="bundle-xform",
                logical_asset_hash=_digest(original),
                artifactclass="video",
                member_path="clip.mov",
                source_path=str(staged),
                size_bytes=len(staged_bytes),
                file_sha256=_digest(staged_bytes),
            )
        )
        s.flush()
        bundle = s.get(Bundle, "bundle-xform")
        assert bundle is not None
        members = [_member_input(row) for row in _member_rows_for_flush(s, bundle)]
        rendered = _render_flush_map(s, members)

    [_, row] = rendered.strip("\n").split("\n")
    columns = row.split("\t")
    assert columns[2] == _digest(staged_bytes).hex()
    assert columns[2] != _digest(original).hex()
    assert columns[3] == str(len(staged_bytes))


def test_non_utf8_source_path_is_quarantined_and_the_rest_of_the_bundle_builds(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """A member whose source path is not valid UTF-8 carries its raw bytes in
    ``source_metadata['source_path_bytes_hex']`` (written by
    ``archive_bundle`` whenever the path is not encodable, and by
    ``archive_submission`` for every submission member), and
    ``_member_source_path`` surrogate-decodes them back.

    rem parses the map with ``std::str::from_utf8`` over the WHOLE file
    (``remanence-cli/src/archive_map.rs``), so one such row rejects every row,
    and that rejection names neither a map line nor an archive path —
    ``_identify_member_failure`` returns None and the failure propagates,
    letting one odd filename kill a whole multi-class bundle. Guards exactly
    that: the unmappable member must be quarantined by the ordinary hold
    split, the map rem actually receives must be strict UTF-8 and must not
    mention it, and its co-residents must reach media."""
    raw_name = b"caf\xe9.mov"  # latin-1 bytes, not valid UTF-8
    odd = tmp_path / os.fsdecode(raw_name)
    odd_data = b"movie bytes"
    odd.write_bytes(odd_data)

    bundle_id, backend = _two_member_bundle(engine, tmp_path)
    with session_scope(engine) as s:
        s.add(LogicalAsset(content_sha256=_digest(odd_data), size_bytes=len(odd_data)))
        s.flush()
        s.add(
            BundleMember(
                bundle_id=bundle_id,
                logical_asset_hash=_digest(odd_data),
                artifactclass="o-archive",
                member_path="cafe.mov",
                source_path=None,
                source_metadata={"source_path_bytes_hex": os.fsencode(odd).hex()},
                size_bytes=len(odd_data),
                file_sha256=_digest(odd_data),
            )
        )
        accumulator = s.get(Bundle, bundle_id)
        assert accumulator is not None
        accumulator.member_count += 1
        accumulator.total_bytes += len(odd_data)
        s.flush()

    builder = _FailingBuilder([])
    with session_scope(engine) as s:
        result = flush_bundle(
            s,
            bundle_id=bundle_id,
            backends=_backends_for(s, backend),
            builder=builder,
        )
        assert len(result.copy_ids) == 1

    # Attempt 1 never reached the builder (the render refused first); the
    # retry's map carries the two clean members and is plain UTF-8.
    assert len(builder.maps) == 1
    assert _map_archive_paths(builder.maps[0]) == ["a.bin", "nested/b.bin"]
    assert "cafe.mov" not in builder.maps[0]
    builder.maps[0].encode("utf-8")  # strict: no surrogates survived

    with session_scope(engine) as s:
        sealed = s.get(Bundle, bundle_id)
        assert sealed is not None
        assert sealed.status == "sealed"
        assert [row.member_path for row in _member_rows_for_flush(s, sealed)] == [
            "a.bin",
            "nested/b.bin",
        ]
        [quarantine] = list(s.scalars(select(Bundle).where(Bundle.status == "held")))
        assert [row.member_path for row in _member_rows_for_flush(s, quarantine)] == ["cafe.mov"]
        reason = quarantine.review_summary["quarantined_members"][0]["reason"]
        assert "not UTF-8" in reason


def test_repair_path_names_the_unmappable_member_instead_of_failing_opaquely(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """The bundle-repair rebuild renders its own map and has no quarantine loop
    to fall back on (finding 7 leaves its work dirs to a later prompt). It must
    still refuse by NAME rather than hand rem a map that cannot load and let
    the operator read ``source map ... is not UTF-8`` with nothing to act on."""
    odd = tmp_path / os.fsdecode(b"caf\xe9.mov")
    data = b"movie bytes"
    odd.write_bytes(data)

    with session_scope(engine) as s:
        s.add(Bundle(id="bundle-raw", **bundle_kwargs(seed="raw"), status="open"))
        s.add(LogicalAsset(content_sha256=_digest(data), size_bytes=len(data)))
        s.flush()
        s.add(
            BundleMember(
                bundle_id="bundle-raw",
                logical_asset_hash=_digest(data),
                artifactclass="video",
                member_path="cafe.mov",
                source_path=None,
                source_metadata={"source_path_bytes_hex": os.fsencode(odd).hex()},
                size_bytes=len(data),
                file_sha256=_digest(data),
            )
        )
        s.flush()
        bundle = s.get(Bundle, "bundle-raw")
        assert bundle is not None
        members = [_member_input(row) for row in _member_rows_for_flush(s, bundle)]
        with pytest.raises(ArchiveFanoutError, match="not UTF-8 encodable"):
            _render_flush_map(s, members)


# --- the map-only rem route ------------------------------------------------


def _fake_rao_build(
    kwargs: dict[str, Any],
) -> RemArchiveBuildResult:
    """Materialise a RAO-plain-shaped object from the map the caller handed in.

    Member bytes land at ``first_chunk_lba * RAO_CHUNK_SIZE`` so the flush's
    own ranged-read verification passes without a rem subprocess.
    """
    map_rows = [
        line.split("\t")
        for line in Path(kwargs["map_path"]).read_text(encoding="utf-8").strip("\n").split("\n")[1:]
    ]
    payload = bytearray()
    files: list[dict[str, Any]] = []
    for index, (archive_path, source_path, sha256, size, _ingest) in enumerate(map_rows):
        first_lba = index + 1
        start = first_lba * RAO_CHUNK_SIZE
        data = Path(source_path).read_bytes()
        if len(payload) < start + len(data):
            payload.extend(b"\0" * (start + len(data) - len(payload)))
        payload[start : start + len(data)] = data
        files.append(
            {
                "path": archive_path,
                "size_bytes": int(size),
                "sha256": sha256,
                "first_chunk_lba": first_lba,
            }
        )
    output_path = Path(kwargs["output_path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(bytes(payload))
    manifest_path = kwargs.get("manifest_path")
    if manifest_path is not None:
        Path(manifest_path).parent.mkdir(parents=True, exist_ok=True)
        Path(manifest_path).write_text(json.dumps({"files": files}), encoding="utf-8")
    return RemArchiveBuildResult(
        artifact_path=output_path,
        stored_digest=hashlib.sha256(bytes(payload)).digest(),
        stdout_report={"files": files, "chunk_size": RAO_CHUNK_SIZE},
        manifest_path=None if manifest_path is None else Path(manifest_path),
    )


def test_group_flush_builds_by_map_through_rem_builder(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ships-green hazard, pinned. LocalArchiveBuilder ignores map_path, so
    this test drives ``RemArchiveBuilder`` itself and mocks only the process
    boundary. It guards: a group flush falling back to the ``--inputs`` route
    (where rem derives member names from the filesystem and hard-errors on
    duplicate basenames); a ruleset leaking into a map build (``--map``
    conflicts with ``--rules``); a source_root other than ``/``; and the map
    digest handed to rem drifting from the map actually written."""
    calls: list[dict[str, Any]] = []

    def fake_build(**kwargs: Any) -> RemArchiveBuildResult:
        # The map lives in the flush's TemporaryDirectory; snapshot it here,
        # while it still exists.
        recorded = dict(kwargs)
        recorded["map_bytes"] = Path(kwargs["map_path"]).read_bytes()
        calls.append(recorded)
        return _fake_rao_build(kwargs)

    monkeypatch.setattr("sutradhara.archive_fanout.run_rem_archive_build", fake_build)

    a = tmp_path / "a.bin"
    a.write_bytes(b"alpha body")
    b = tmp_path / "b.bin"
    b.write_bytes(b"beta body")

    backend = _WriteBackend()
    with session_scope(engine) as s:
        # Two pools, one RAO and one legacy D2TAR_RAW: the RAO leg goes by map,
        # the D2 leg stays map-blind (_build_d2_tar), per design §4.
        _install_class(
            s,
            "o-archive",
            pools=(
                ("o-copy-1-pool", Representation.RAO_PLAIN_V1.value),
                ("d2-shelf-pool", Representation.D2TAR_RAW.value),
            ),
        )
        bundle = _enqueue(s, artifactclass="o-archive", source=a, member_path="a.bin")
        _enqueue(s, artifactclass="o-archive", source=b, member_path="nested/b.bin")
        bundle_id = bundle.id

    with session_scope(engine) as s:
        result = flush_bundle(
            s,
            bundle_id=bundle_id,
            backends=_backends_for(s, backend),
            builder=RemArchiveBuilder(),
        )
        assert len(result.copy_ids) == 2
        flushed = s.get(Bundle, bundle_id)
        assert flushed is not None
        assert flushed.status == "sealed"
        map_digest = flushed.scan_summary["map_sha256"]

    # Exactly one rem build: the RAO leg. The D2TAR_RAW leg never reaches rem.
    assert len(calls) == 1
    [call] = calls
    assert call["inputs"] is None
    assert call["ruleset"] is None
    assert call["map_path"] is not None
    assert call["source_root"] == Path("/")
    map_bytes = call["map_bytes"]
    assert call["map_sha256"] == hashlib.sha256(map_bytes).hexdigest()
    assert call["map_sha256"] == map_digest
    lines = map_bytes.decode("utf-8").strip("\n").split("\n")
    assert lines[0] == MAP_HEADER
    assert [line.split("\t")[0] for line in lines[1:]] == ["a.bin", "nested/b.bin"]


def test_map_route_argv_carries_map_source_root_slash_and_no_inputs_or_rules(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same route asserted at the true process boundary: the literal argv
    that would reach the rem binary. Guards an ``--inputs`` or ``--rules`` flag
    surviving on the map route (rem rejects the combination), a missing
    ``--source-root`` (``--map`` requires one), and a source root other
    than ``/``."""
    captured: list[list[str]] = []
    map_texts: list[str] = []

    rem_bin = tmp_path / "rem-stub"
    rem_bin.write_text("#!/bin/sh\nexit 0\n")
    rem_bin.chmod(rem_bin.stat().st_mode | stat.S_IXUSR)

    def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured.append(cmd)
        # Snapshot the map before the flush's TemporaryDirectory disappears.
        map_texts.append(Path(cmd[cmd.index("--map") + 1]).read_text(encoding="utf-8"))
        out = Path(cmd[cmd.index("--out") + 1])
        manifest_out = Path(cmd[cmd.index("--manifest-out") + 1])
        result = _fake_rao_build(
            {
                "map_path": Path(cmd[cmd.index("--map") + 1]),
                "output_path": out,
                "manifest_path": manifest_out,
            }
        )
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps(result.stdout_report) + "\n",
            stderr="",
        )

    monkeypatch.setattr("sutradhara.rem_archive_cli.run_managed", fake_run)

    source = tmp_path / "only.bin"
    source.write_bytes(b"only body")
    backend = _WriteBackend()
    with session_scope(engine) as s:
        _install_class(
            s,
            "o-archive",
            pools=(("o-copy-1-pool", Representation.RAO_PLAIN_V1.value),),
        )
        bundle_id = _enqueue(s, artifactclass="o-archive", source=source, member_path="only.bin").id

    with session_scope(engine) as s:
        flush_bundle(
            s,
            bundle_id=bundle_id,
            backends=_backends_for(s, backend),
            builder=RemArchiveBuilder(rem_bin=rem_bin),
        )

    [cmd] = captured
    [map_text] = map_texts
    assert cmd[:3] == [str(rem_bin), "archive", "build"]
    assert "--map" in cmd
    assert map_text.split("\n")[0] == MAP_HEADER
    assert "--source-root" in cmd
    assert cmd[cmd.index("--source-root") + 1] == "/"
    assert "--map-sha256" in cmd
    assert cmd[cmd.index("--map-sha256") + 1] == (
        hashlib.sha256(map_text.encode("utf-8")).hexdigest()
    )
    assert "--inputs" not in cmd
    assert "--rules" not in cmd
    assert "--scan-only" not in cmd


def test_rem_builder_refuses_the_retired_inputs_route(tmp_path: Path) -> None:
    """The segment-counting root derivation (``_rem_input_paths``) and its
    sibling-widening hazard are retired for group builds. A caller that still
    hands RemArchiveBuilder no map must fail loudly, not silently walk roots."""
    builder = RemArchiveBuilder()
    with pytest.raises(ArchiveFanoutError, match="map only"):
        builder.build(
            bundle=Bundle(id="bundle-x", **bundle_kwargs(seed="x"), status="open"),
            members=(),
            representation=Representation.RAO_PLAIN_V1,
            ruleset="rao.x.v1",
            key_epoch=None,
            work_dir=tmp_path,
        )


# --- work-dir hygiene ------------------------------------------------------


def test_same_representation_targets_get_separate_work_dirs(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """§8: two pool targets of the same representation used to collide on the
    work-dir artifact filename (``{bundle}-{representation}.rao``), so the
    second build overwrote the first target's artifact while its copy was
    still being verified. Each target must own a directory."""
    work_dirs: list[Path] = []

    class _RecordingBuilder(LocalArchiveBuilder):
        def build(self, **kwargs: Any) -> BuildArtifact:
            work_dirs.append(Path(kwargs["work_dir"]))
            return super().build(**kwargs)

    source = tmp_path / "only.bin"
    source.write_bytes(b"only body")
    backend = _WriteBackend()
    with session_scope(engine) as s:
        _install_class(
            s,
            "o-archive",
            pools=(
                ("rao-pool-a", Representation.RAO_PLAIN_V1.value),
                ("rao-pool-b", Representation.RAO_PLAIN_V1.value),
            ),
        )
        bundle_id = _enqueue(s, artifactclass="o-archive", source=source, member_path="only.bin").id

    with session_scope(engine) as s:
        result = flush_bundle(
            s,
            bundle_id=bundle_id,
            backends=_backends_for(s, backend),
            builder=_RecordingBuilder(),
        )
        assert len(result.copy_ids) == 2

    assert len(work_dirs) == 2
    assert work_dirs[0] != work_dirs[1]
    assert {path.name for path in work_dirs} == {
        "target-00-rao-pool-a",
        "target-01-rao-pool-b",
    }


# --- failure loop ----------------------------------------------------------


def _map_archive_paths(map_text: str) -> list[str]:
    """The archive_path column of a rendered map, header dropped."""
    rows = map_text.strip("\n").split("\n")
    assert rows[0] == MAP_HEADER
    return [row.split("\t")[0] for row in rows[1:]]


class _FailingBuilder(LocalArchiveBuilder):
    """LocalArchiveBuilder that fails the first N builds with a rem-shaped
    message, recording the map it was handed on every attempt."""

    def __init__(self, messages: list[str]) -> None:
        self.messages = list(messages)
        self.maps: list[str] = []

    def build(self, **kwargs: Any) -> BuildArtifact:
        map_path = kwargs.get("map_path")
        assert map_path is not None, "every build goes by map"
        self.maps.append(Path(map_path).read_text(encoding="utf-8"))
        if self.messages:
            raise RuntimeError(self.messages.pop(0))
        return super().build(**kwargs)


def _two_member_bundle(engine: Engine, tmp_path: Path) -> tuple[str, _WriteBackend]:
    a = tmp_path / "a.bin"
    a.write_bytes(b"alpha body")
    b = tmp_path / "b.bin"
    b.write_bytes(b"beta body")
    backend = _WriteBackend()
    with session_scope(engine) as s:
        _install_class(
            s,
            "o-archive",
            pools=(("o-copy-1-pool", Representation.RAO_PLAIN_V1.value),),
        )
        bundle = _enqueue(s, artifactclass="o-archive", source=a, member_path="a.bin")
        _enqueue(s, artifactclass="o-archive", source=b, member_path="nested/b.bin")
        return bundle.id, backend


def test_map_line_failure_quarantines_that_member_re_renders_and_retries(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """rem names the map LINE for a missing source or a size drift. The flush
    must map line N back to sorted row N-2 (line 1 is the header), quarantine
    exactly that member, decrement the source bundle's counters, re-render the
    map without it, and retry inside the existing claim. An off-by-one here
    quarantines an innocent member and re-flushes the broken one forever."""
    bundle_id, backend = _two_member_bundle(engine, tmp_path)
    # Line 3 = second data row = sorted index 1 = "nested/b.bin".
    builder = _FailingBuilder(["rem archive build failed: source map line 3 missing source"])

    with session_scope(engine) as s:
        result = flush_bundle(
            s,
            bundle_id=bundle_id,
            backends=_backends_for(s, backend),
            builder=builder,
        )
        assert len(result.copy_ids) == 1

    with session_scope(engine) as s:
        sealed = s.get(Bundle, bundle_id)
        assert sealed is not None
        assert sealed.status == "sealed"
        assert sealed.member_count == 1
        assert sealed.total_bytes == len(b"alpha body")
        assert [row.member_path for row in _member_rows_for_flush(s, sealed)] == ["a.bin"]

        held = list(s.scalars(select(Bundle).where(Bundle.status == "held")))
        assert len(held) == 1
        [quarantine] = held
        # Funnel-style mint: archive_id at creation, so it never sits open
        # under the group fingerprint and cannot be adopted.
        assert quarantine.archive_id is not None
        assert quarantine.bundle_group == sealed.bundle_group
        assert quarantine.member_count == 1
        assert quarantine.total_bytes == len(b"beta body")
        assert [row.member_path for row in _member_rows_for_flush(s, quarantine)] == [
            "nested/b.bin"
        ]
        assert quarantine.review_summary["quarantined_members"][0]["member_path"] == (
            "nested/b.bin"
        )
        # Only the surviving member got a copy.
        copies = list(s.scalars(select(Copy)))
        assert [copy.bundle_id for copy in copies] == [bundle_id]

    # Attempt 1 carried both members; the retry's map dropped the bad one.
    assert len(builder.maps) == 2
    assert _map_archive_paths(builder.maps[0]) == ["a.bin", "nested/b.bin"]
    assert _map_archive_paths(builder.maps[1]) == ["a.bin"]


def test_writer_digest_failure_quarantines_by_archive_path(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """The RAO writer's streamed-digest refusal names the ARCHIVE PATH, not a
    map line (remanence-format writer.rs). Guards a flush that only understands
    the line form and therefore lets a content mismatch propagate as an
    unidentified failure, poisoning the whole multi-class bundle."""
    bundle_id, backend = _two_member_bundle(engine, tmp_path)
    builder = _FailingBuilder(
        ["rem archive build failed: streamed data hash for a.bin does not match spec"]
    )

    with session_scope(engine) as s:
        result = flush_bundle(
            s,
            bundle_id=bundle_id,
            backends=_backends_for(s, backend),
            builder=builder,
        )
        assert len(result.copy_ids) == 1

    with session_scope(engine) as s:
        sealed = s.get(Bundle, bundle_id)
        assert sealed is not None
        assert [row.member_path for row in _member_rows_for_flush(s, sealed)] == ["nested/b.bin"]
        [quarantine] = list(s.scalars(select(Bundle).where(Bundle.status == "held")))
        assert [row.member_path for row in _member_rows_for_flush(s, quarantine)] == ["a.bin"]
    assert _map_archive_paths(builder.maps[0]) == ["a.bin", "nested/b.bin"]
    assert _map_archive_paths(builder.maps[1]) == ["nested/b.bin"]


# The two rem-shaped stderr lines, in rem's own wire format: `rem` writes
# `error: {message}` (remanence-cli run_with_mode), the map-line form comes
# from archive_map.rs::parse_source_map_row and the archive-path form from
# remanence-format writer.rs via FormatError::InvalidInput's Display.
_REM_SHAPED_STDERR = {
    "map-line": (
        "error: source map line 3 size mismatch for /srv/stage/b.bin: "
        "map says 9, filesystem says 12",
        "nested/b.bin",
    ),
    "archive-path": (
        "error: invalid REM-OBJECT input: streamed data hash for a.bin does not match spec",
        "a.bin",
    ),
}


@pytest.mark.parametrize("form", sorted(_REM_SHAPED_STDERR))
def test_rem_shaped_stderr_through_the_cli_wrapper_quarantines_the_named_member(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    form: str,
) -> None:
    """The failure loop parses ``str(exc)``, but in production that string is
    not rem's stderr — it is assembled by ``run_rem_archive_build``, which
    truncates stdout and stderr to 500 characters each and wraps them in
    ``repr()``. Every other failure test raises a hand-written RuntimeError and
    therefore never crosses that wrapper: the regexes could stop matching the
    real shape and the suite would stay green, which is the exact way this arc
    has shipped broken twice.

    This test lets a real non-zero ``rem`` process produce each of the two real
    stderr forms, so the string ``_identify_member_failure`` sees is the one
    production builds. It guards the quoting/truncation of the wrapper drifting
    away from ``_MAP_LINE_FAILURE``/``_WRITER_DIGEST_FAILURE`` — after which
    every member failure becomes an unidentified one and poisons the bundle."""
    stderr_text, expected_quarantined = _REM_SHAPED_STDERR[form]
    rem_bin = tmp_path / "rem-stub"
    rem_bin.write_text(f"#!/bin/sh\nprintf '%s\\n' '{stderr_text}' >&2\nexit 1\n")
    rem_bin.chmod(rem_bin.stat().st_mode | stat.S_IXUSR)

    calls: list[list[str]] = []

    def dispatch(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        if len(calls) == 1:
            # Really execute the fake rem: real exit code, real stderr bytes.
            return subprocess.run(cmd, capture_output=True, text=True, check=False)
        # The retry succeeds so the flush can finish and be asserted on.
        result = _fake_rao_build(
            {
                "map_path": Path(cmd[cmd.index("--map") + 1]),
                "output_path": Path(cmd[cmd.index("--out") + 1]),
                "manifest_path": Path(cmd[cmd.index("--manifest-out") + 1]),
            }
        )
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps(result.stdout_report) + "\n",
            stderr="",
        )

    monkeypatch.setattr("sutradhara.rem_archive_cli.run_managed", dispatch)

    bundle_id, backend = _two_member_bundle(engine, tmp_path)
    with session_scope(engine) as s:
        result = flush_bundle(
            s,
            bundle_id=bundle_id,
            backends=_backends_for(s, backend),
            builder=RemArchiveBuilder(rem_bin=rem_bin),
        )
        assert len(result.copy_ids) == 1

    assert len(calls) == 2
    with session_scope(engine) as s:
        sealed = s.get(Bundle, bundle_id)
        assert sealed is not None
        assert sealed.status == "sealed"
        [quarantine] = list(s.scalars(select(Bundle).where(Bundle.status == "held")))
        assert [row.member_path for row in _member_rows_for_flush(s, quarantine)] == [
            expected_quarantined
        ]
        assert [row.member_path for row in _member_rows_for_flush(s, sealed)] == [
            path for path in ("a.bin", "nested/b.bin") if path != expected_quarantined
        ]
        # The recorded reason is the wrapper's string, not rem's raw stderr:
        # proof the regex matched what production actually produces.
        reason = quarantine.review_summary["quarantined_members"][0]["reason"]
        assert reason.startswith("rem archive build failed (exit 1): command=")


def test_quarantine_moves_staging_transform_rows_with_the_member(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """``staging_transform.bundle_id`` is denormalized under a
    ``(bundle_id, stored_member_path, step_order)`` unique constraint. A member
    move that leaves its transform rows behind orphans the transform lineage
    against a sealed bundle the member no longer belongs to."""
    bundle_id, backend = _two_member_bundle(engine, tmp_path)
    with session_scope(engine) as s:
        bundle = s.get(Bundle, bundle_id)
        assert bundle is not None
        [target] = [row for row in _member_rows_for_flush(s, bundle) if row.member_path == "a.bin"]
        s.add(
            StagingTransform(
                bundle_id=bundle_id,
                bundle_member_id=target.id,
                logical_asset_hash=target.logical_asset_hash,
                artifactclass="o-archive",
                step_order=0,
                kind="test-transform",
                reversible=True,
                original_member_path="a.bin",
                stored_member_path="a.bin",
                original_size_bytes=target.size_bytes,
                stored_size_bytes=target.size_bytes,
                original_sha256=target.logical_asset_hash,
                stored_sha256=target.file_sha256,
            )
        )
        s.flush()

    builder = _FailingBuilder(
        ["rem archive build failed: streamed data hash for a.bin does not match spec"]
    )
    with session_scope(engine) as s:
        flush_bundle(
            s,
            bundle_id=bundle_id,
            backends=_backends_for(s, backend),
            builder=builder,
        )

    with session_scope(engine) as s:
        [quarantine] = list(s.scalars(select(Bundle).where(Bundle.status == "held")))
        [transform] = list(s.scalars(select(StagingTransform)))
        assert transform.bundle_id == quarantine.id


def test_non_member_build_failure_propagates(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """A rem failure that names no member (a broken binary, a full disk) is not
    a member verdict. Guards a quarantine loop that treats every failure as one
    bad member and silently strips a healthy bundle down to nothing."""
    bundle_id, backend = _two_member_bundle(engine, tmp_path)
    builder = _FailingBuilder(["rem archive build failed (exit 101): No space left on device"])

    with session_scope(engine) as s, pytest.raises(RuntimeError, match="No space left"):
        flush_bundle(
            s,
            bundle_id=bundle_id,
            backends=_backends_for(s, backend),
            builder=builder,
        )

    assert len(builder.maps) == 1
    with session_scope(engine) as s:
        assert list(s.scalars(select(Bundle).where(Bundle.status == "held"))) == []


def test_quarantine_loop_is_bounded_by_the_member_count(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """Every retry must remove one member, so the loop terminates in at most
    member_count attempts. Guards an unbounded retry against a rem that always
    blames the first map line."""
    bundle_id, backend = _two_member_bundle(engine, tmp_path)
    builder = _FailingBuilder(["rem archive build failed: source map line 2 missing source"] * 8)

    with session_scope(engine) as s, pytest.raises(ArchiveFanoutError, match="no members left"):
        flush_bundle(
            s,
            bundle_id=bundle_id,
            backends=_backends_for(s, backend),
            builder=builder,
        )

    # Two members, two build attempts, then the empty remainder stops the loop.
    assert len(builder.maps) == 2


# --- work-dir preflight ----------------------------------------------------


def test_flush_preflight_skips_with_an_alarm_and_touches_nothing(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """§8: the flush materialises the bundle once per pool target with no
    free-space check today. Guards a flush that starts, writes a partial copy,
    and dies mid-fan-out on ENOSPC — the preflight must refuse first, raise the
    alarm, and leave the bundle open and copy-less."""
    bundle_id, backend = _two_member_bundle(engine, tmp_path)
    events: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        "sutradhara.archive_fanout.emit_structured_event",
        lambda name, **fields: events.append((name, fields)),
    )

    real_statvfs = os.statvfs

    class _Short:
        f_bavail = 1
        f_frsize = 1

    monkeypatch.setattr(os, "statvfs", lambda path: _Short())

    builder = _FailingBuilder([])
    with session_scope(engine) as s, pytest.raises(FlushPreflightShort):
        flush_bundle(
            s,
            bundle_id=bundle_id,
            backends=_backends_for(s, backend),
            builder=builder,
        )

    monkeypatch.setattr(os, "statvfs", real_statvfs)
    assert builder.maps == []
    assert [name for name, _ in events] == ["bundle_flush_preflight_short"]
    [(_, fields)] = events
    assert fields["bundle_id"] == bundle_id
    assert fields["target_count"] == 1
    assert fields["available_bytes"] == 1
    assert fields["required_bytes"] == len(b"alpha body") + len(b"beta body")

    with session_scope(engine) as s:
        bundle = s.get(Bundle, bundle_id)
        assert bundle is not None
        assert bundle.status == "open"
        assert bundle.archive_id is None
        assert bundle.flushed_at is None
        assert list(s.scalars(select(Copy))) == []
