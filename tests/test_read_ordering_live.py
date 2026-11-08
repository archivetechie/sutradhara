"""Live wire proof: the regenerated stubs and field mapping against rem-daemon.

Spawns a **read-only rem-daemon** over a seeded temp state dir — the
scenario-RO seeding precedent: calibration state goes through the durable
stores the daemon itself replays (the calibration control journal before
start, the wrap-map SQLite projection after start), and catalog rows
(tapes / objects / object_copies / tape_files) are inserted directly into
the index, which the serve path re-opens read-only per request. No drive,
no mount, no tape motion — the planner is a pure function of the request
and the cached wrap map.

What this proves that the faked-client suite cannot:
- the regenerated `_proto` stubs speak the daemon's actual wire (no
  protoc-at-runtime anywhere);
- `Tape.written_extent_lba` optional-field mapping (absent != 0) through
  `RemanenceBackend.get_tape_facts`;
- `ObjectCopy.global_start_block/global_end_block` span mapping (present
  together / absent together) through `get_copy_read_span`;
- a real `PlanBatchRead` round trip through `plan_batch_read`, including
  the decoded `google.rpc.BadRequest` on INVALID_ARGUMENT.

Skips (not fails) when the rem-daemon binary is missing or predates
ReadPlanService: build with `cargo build --release --bin rem-daemon`.

covers: rem.plan.batch_read (live; sutradhara client funnel)
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import grpc
import pytest

from sutradhara._proto import layer5_pb2
from sutradhara.backend.remanence import RemanenceBackend
from sutradhara.hdcache.read_ordering import _bad_request_violations

TAPE_CAL = "5b" * 16
TAPE_UNCAL = "11" * 16
TAPE_NO_EXTENT = "22" * 16

WRITTEN_EXTENT = 4000
MAPPED_EXTENT = 3500
BLOCK_SIZE = 262_144
SEEDED_GENERATION = 1
MAX_TARGETS = 2730

OBJECT_ID = uuid.UUID("00000000-0000-4000-8000-000000000001")
SPAN_START = 1200
SPAN_BLOCKS = 300
UNSPANNED_OBJECT_ID = uuid.UUID("00000000-0000-4000-8000-000000000002")

FIXTURE_DESCRIPTORS = [
    {"partition": 0, "wrap_number": 0, "end_loi": 999},
    {"partition": 0, "wrap_number": 1, "end_loi": 1999},
    {"partition": 0, "wrap_number": 2, "end_loi": 2999},
    {"partition": 0, "wrap_number": 3, "end_loi": MAPPED_EXTENT},
]


def _daemon_binary() -> Path:
    override = os.environ.get("REM_DAEMON_BIN")
    if override:
        return Path(override)
    return Path.home() / "remanence" / "target" / "release" / "rem-daemon"


class FixtureDaemon:
    """A test-owned read-only rem-daemon over a seeded temp state dir."""

    def __init__(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="rro-s1-live-"))
        self.state = self.root / "state"
        self.socket = self.root / "rem.sock"
        self.sqlite_path = self.state / "index" / "rem-state.sqlite"
        self._proc: subprocess.Popen | None = None
        self._stderr_path = self.root / "daemon.stderr"

    @property
    def endpoint(self) -> str:
        return f"unix:{self.socket}"

    def start(self) -> None:
        binary = _daemon_binary()
        if not binary.exists():
            pytest.skip(f"rem-daemon binary missing: {binary}")
        blob = binary.read_bytes()
        if b"ReadPlanService" not in blob:
            pytest.skip("rem-daemon binary predates ReadPlanService; rebuild remanence")

        self.state.mkdir(parents=True, exist_ok=True)
        config = self.root / "config.toml"
        config.write_text(
            f"""\
[daemon]
state_dir = "{self.state}"
default_idle_timeout_seconds = 1800
read_only = true

[journal]
dir = "{self.state}/journals"
require_trusted_volume = false

[audit]
dir = "{self.state}/audit"
fsync = false

[index]
sqlite_path = "{self.sqlite_path}"

[cache]
tape_catalog_dir = "{self.state}/cache/tapes"
"""
        )
        self._seed_calibration_journal()
        stderr = self._stderr_path.open("wb")
        try:
            self._proc = subprocess.Popen(
                [str(binary), "--config", str(config), "--socket", str(self.socket)],
                stdout=subprocess.DEVNULL,
                stderr=stderr,
            )
        finally:
            stderr.close()
        try:
            self._wait_for_socket()
            self._seed_index_rows()
        except BaseException:
            # A failed start must not leave the daemon running: the module
            # fixture never reaches its yield, so stop() would not be called.
            self.stop()
            raise

    def _seed_calibration_journal(self) -> None:
        calibration_dir = self.state / "calibration"
        calibration_dir.mkdir(parents=True, exist_ok=True)
        record = {
            "kind": "harvest_succeeded",
            "tape_uuid_hex": TAPE_CAL,
            "write_epoch": 0,
            "state": "calibrated",
            "write_path_trust": "trusted",
            "calibration_generation": SEEDED_GENERATION,
        }
        (calibration_dir / "control.remcalibration").write_text(json.dumps(record) + "\n")

    def _wait_for_socket(self, timeout: float = 20.0) -> None:
        deadline = time.monotonic() + timeout
        while not self.socket.exists():
            proc = self._proc
            if proc is not None and proc.poll() is not None:
                raise RuntimeError(
                    f"rem-daemon exited with {proc.returncode} before binding: "
                    f"{self._stderr_tail()}"
                )
            if time.monotonic() > deadline:
                self.stop()
                raise RuntimeError(f"rem-daemon did not bind {self.socket} within {timeout}s")
            time.sleep(0.05)

    def _seed_index_rows(self) -> None:
        """Seed the projection rows the serve path reads (re-opened per request)."""

        conn = sqlite3.connect(self.sqlite_path)
        try:
            now = "2026-08-04T00:00:00Z"
            conn.execute(
                "insert into wrap_maps(tape_uuid, descriptors_json, mapped_extent_lba,"
                " write_epoch, calibration_generation, harvested_at_utc)"
                " values(?, ?, ?, ?, ?, ?)",
                (
                    bytes.fromhex(TAPE_CAL),
                    json.dumps(FIXTURE_DESCRIPTORS),
                    MAPPED_EXTENT,
                    0,
                    SEEDED_GENERATION,
                    now,
                ),
            )
            # The calibrated volume: voltag, block size, R1 written extent.
            conn.execute(
                "insert into tapes(tape_uuid, voltag, block_size, written_extent_lba,"
                " state, updated_at_utc) values(?, ?, ?, ?, 'ready', ?)",
                (
                    bytes.fromhex(TAPE_CAL),
                    "RO0001L8",
                    BLOCK_SIZE,
                    WRITTEN_EXTENT.to_bytes(8, "big"),
                    now,
                ),
            )
            # A volume with the R1 field absent: absent must map to None, not 0.
            conn.execute(
                "insert into tapes(tape_uuid, voltag, block_size, written_extent_lba,"
                " state, updated_at_utc) values(?, ?, ?, NULL, 'ready', ?)",
                (bytes.fromhex(TAPE_NO_EXTENT), "RO0002L8", BLOCK_SIZE, now),
            )
            # One object with a spanned copy (tape file carries the captured
            # start) and one whose tape file predates span capture.
            conn.execute(
                "insert into objects(object_id, caller_object_id, body_format,"
                " logical_size_bytes, content_hash, content_hash_algorithm,"
                " created_at_utc) values(?, ?, 'rem-object-v1', 9, ?, 'sha256', ?)",
                (str(OBJECT_ID), "caller-1", b"\x01" * 32, now),
            )
            conn.execute(
                "insert into object_copies(object_id, tape_uuid, tape_file_number,"
                " first_body_lba, status, representation, pool_id)"
                " values(?, ?, ?, ?, 'committed', 'plaintext', 'pool-a')",
                (
                    str(OBJECT_ID),
                    bytes.fromhex(TAPE_CAL),
                    (2).to_bytes(8, "big"),
                    (0).to_bytes(8, "big"),
                ),
            )
            conn.execute(
                "insert into tape_files(tape_uuid, tape_file_number, kind, block_count,"
                " physical_start_hint, object_id) values(?, ?, 'object', ?, ?, ?)",
                (
                    bytes.fromhex(TAPE_CAL),
                    (2).to_bytes(8, "big"),
                    SPAN_BLOCKS.to_bytes(8, "big"),
                    SPAN_START.to_bytes(8, "big"),
                    str(OBJECT_ID),
                ),
            )
            conn.execute(
                "insert into objects(object_id, caller_object_id, body_format,"
                " logical_size_bytes, content_hash, content_hash_algorithm,"
                " created_at_utc) values(?, ?, 'rem-object-v1', 9, ?, 'sha256', ?)",
                (str(UNSPANNED_OBJECT_ID), "caller-2", b"\x02" * 32, now),
            )
            conn.execute(
                "insert into object_copies(object_id, tape_uuid, tape_file_number,"
                " first_body_lba, status, representation, pool_id)"
                " values(?, ?, ?, ?, 'committed', 'plaintext', 'pool-a')",
                (
                    str(UNSPANNED_OBJECT_ID),
                    bytes.fromhex(TAPE_CAL),
                    (3).to_bytes(8, "big"),
                    (0).to_bytes(8, "big"),
                ),
            )
            conn.execute(
                "insert into tape_files(tape_uuid, tape_file_number, kind, block_count,"
                " physical_start_hint, object_id) values(?, ?, 'object', ?, NULL, ?)",
                (
                    bytes.fromhex(TAPE_CAL),
                    (3).to_bytes(8, "big"),
                    SPAN_BLOCKS.to_bytes(8, "big"),
                    str(UNSPANNED_OBJECT_ID),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _stderr_tail(self) -> str:
        try:
            return self._stderr_path.read_text()[-1000:]
        except OSError:
            return "<no stderr captured>"

    def stop(self) -> None:
        proc = self._proc
        self._proc = None
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
        shutil.rmtree(self.root, ignore_errors=True)


@pytest.fixture(scope="module")
def daemon() -> Iterator[FixtureDaemon]:
    fixture = FixtureDaemon()
    fixture.start()
    yield fixture
    fixture.stop()


@pytest.fixture(scope="module")
def backend(daemon: FixtureDaemon) -> RemanenceBackend:
    return RemanenceBackend.from_grpc("rem-live", daemon.endpoint)


def _locator(object_id: uuid.UUID, tape_file_number: int) -> dict:
    return {
        "tape_uuid": TAPE_CAL,
        "tape_file_number": tape_file_number,
        "object_id": object_id.hex,
    }


def test_live_planner_surface_is_exposed(backend: RemanenceBackend) -> None:
    assert backend.read_ordering_planner() is backend


def test_live_get_tape_facts_maps_written_extent_present_and_absent(
    backend: RemanenceBackend,
) -> None:
    """Guards: the R1 optional field defaulting to 0 (absent must be None)."""

    facts = backend.get_tape_facts(bytes.fromhex(TAPE_CAL))
    assert facts.voltag == "RO0001L8"
    assert facts.block_size_bytes == BLOCK_SIZE
    assert facts.written_extent_lba == WRITTEN_EXTENT

    absent = backend.get_tape_facts(bytes.fromhex(TAPE_NO_EXTENT))
    assert absent.written_extent_lba is None, "absent means unknown, never 0"


def test_live_copy_span_present_together_or_absent_together(
    backend: RemanenceBackend,
) -> None:
    """Guards: span fencepost drift and absent-span guessing on the wire."""

    span = backend.get_copy_read_span(_locator(OBJECT_ID, 2))
    assert span == (SPAN_START, SPAN_START + SPAN_BLOCKS), (
        "span is [start, start + block_count), exclusive"
    )
    assert backend.get_copy_read_span(_locator(UNSPANNED_OBJECT_ID, 3)) is None


def test_live_plan_batch_read_orders_a_real_n4_batch(backend: RemanenceBackend) -> None:
    """Guards: stub drift — a real daemon must accept our request and answer
    with a permutation of our targets."""

    pairs = [(100, 150), (2500, 2600), (1200, 1300), (900, 950)]
    request = layer5_pb2.PlanBatchReadRequest(
        cartridge=layer5_pb2.CartridgeFacts(
            cartridge_generation="LTO-8",
            recording_format="L8",
            block_size_bytes=BLOCK_SIZE,
            compression=layer5_pb2.COMPRESSION_DISABLED,
            written_extent_lba=WRITTEN_EXTENT,
        ),
        targets=[
            layer5_pb2.ReadTarget(
                partition=0,
                start_block=start,
                end_block=end,
                tag=b"\x00live-%d" % index,
            )
            for index, (start, end) in enumerate(pairs)
        ],
        objective=layer5_pb2.MIN_TOTAL_TIME,
        tape_uuid=bytes.fromhex(TAPE_CAL),
    )
    response = backend.plan_batch_read(request)
    assert response.status == layer5_pb2.OK, response.detail
    assert response.max_targets == MAX_TARGETS
    assert response.calibration_generation == SEEDED_GENERATION
    sent = {bytes(target.tag) for target in request.targets}
    returned = [bytes(hop.target.tag) for hop in response.hops]
    assert sorted(returned) == sorted(sent), "returned order must be a permutation"
    assert len(returned) == 4


def test_live_uncalibrated_volume_is_a_normal_response(backend: RemanenceBackend) -> None:
    """Guards: treating unavailability as an RPC error."""

    request = layer5_pb2.PlanBatchReadRequest(
        cartridge=layer5_pb2.CartridgeFacts(
            cartridge_generation="LTO-8",
            recording_format="L8",
            block_size_bytes=BLOCK_SIZE,
            compression=layer5_pb2.COMPRESSION_DISABLED,
            written_extent_lba=WRITTEN_EXTENT,
        ),
        targets=[layer5_pb2.ReadTarget(partition=0, start_block=10, end_block=20, tag=b"\x00u")],
        objective=layer5_pb2.MIN_TOTAL_TIME,
        tape_uuid=bytes.fromhex(TAPE_UNCAL),
    )
    response = backend.plan_batch_read(request)
    assert response.status == layer5_pb2.UNAVAILABLE_UNCALIBRATED
    assert not response.hops


def test_live_invalid_argument_carries_decodable_bad_request(
    backend: RemanenceBackend,
) -> None:
    """Guards: the vendored google/rpc stubs failing against real trailers."""

    request = layer5_pb2.PlanBatchReadRequest(
        cartridge=layer5_pb2.CartridgeFacts(
            cartridge_generation="LTO-8",
            recording_format="L8",
            block_size_bytes=BLOCK_SIZE,
            compression=layer5_pb2.COMPRESSION_DISABLED,
            written_extent_lba=WRITTEN_EXTENT,
        ),
        targets=[
            # end_block below start_block: malformed by contract.
            layer5_pb2.ReadTarget(partition=0, start_block=200, end_block=100, tag=b"\x00bad")
        ],
        objective=layer5_pb2.MIN_TOTAL_TIME,
        tape_uuid=bytes.fromhex(TAPE_CAL),
    )
    with pytest.raises(grpc.RpcError) as excinfo:
        backend.plan_batch_read(request)
    error = excinfo.value
    assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
    violations = _bad_request_violations(error)
    assert violations, "BadRequest details must decode through the vendored stubs"
    assert any("0" in violation["field"] for violation in violations), (
        "the offending target must be named by index (tags may be unprintable)"
    )


_FAKE_BINDS_AND_SLEEPS = (
    "#!/bin/sh\n"
    "# ReadPlanService\n"
    'while [ "$#" -gt 0 ]; do\n'
    '  if [ "$1" = "--socket" ]; then : > "$2"; fi\n'
    "  shift\n"
    "done\n"
    "exec sleep 60\n"
)
_FAKE_EXITS_AT_ONCE = "#!/bin/sh\n# ReadPlanService\nexit 3\n"
_FAKE_NEVER_BINDS = "#!/bin/sh\n# ReadPlanService\nexec sleep 60\n"


def _install_fake_daemon(tmp_path, monkeypatch, script: str) -> list[subprocess.Popen]:
    """Point the fixture at a fake daemon script; return the list of launched processes."""

    fake = tmp_path / "fake-rem-daemon"
    fake.write_text(script)
    fake.chmod(0o755)
    monkeypatch.setenv("REM_DAEMON_BIN", str(fake))

    launched: list[subprocess.Popen] = []
    real_popen = subprocess.Popen

    def recording_popen(*args, **kwargs):
        proc = real_popen(*args, **kwargs)
        launched.append(proc)
        return proc

    monkeypatch.setattr(subprocess, "Popen", recording_popen)
    return launched


def _assert_launched_daemon_stopped(launched: list[subprocess.Popen]) -> None:
    assert len(launched) == 1
    try:
        launched[0].wait(timeout=10)
    except subprocess.TimeoutExpired:
        pytest.fail("the launched daemon was left running after a failed start")


def _kill_leftovers(launched: list[subprocess.Popen], fixture: FixtureDaemon) -> None:
    for proc in launched:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
    shutil.rmtree(fixture.root, ignore_errors=True)


@pytest.mark.parametrize(
    "exc",
    [RuntimeError("seed failed"), KeyboardInterrupt()],
    ids=["runtime_error", "keyboard_interrupt"],
)
def test_fixture_start_failure_stops_the_daemon(tmp_path, monkeypatch, exc):
    """A seeding failure inside start() stops the daemon, cleans up and re-raises the original."""

    launched = _install_fake_daemon(tmp_path, monkeypatch, _FAKE_BINDS_AND_SLEEPS)

    def failing_seed(self) -> None:
        raise exc

    monkeypatch.setattr(FixtureDaemon, "_seed_index_rows", failing_seed)

    fixture = FixtureDaemon()
    try:
        with pytest.raises(type(exc)) as excinfo:
            fixture.start()
        assert excinfo.value is exc
        _assert_launched_daemon_stopped(launched)
        assert fixture._proc is None
        assert not fixture.root.exists()
    finally:
        _kill_leftovers(launched, fixture)


def test_fixture_start_early_daemon_exit_cleans_up(tmp_path, monkeypatch):
    """A daemon that exits before binding fails start() and leaves no temp root."""

    launched = _install_fake_daemon(tmp_path, monkeypatch, _FAKE_EXITS_AT_ONCE)

    fixture = FixtureDaemon()
    try:
        with pytest.raises(RuntimeError, match="before binding"):
            fixture.start()
        _assert_launched_daemon_stopped(launched)
        assert fixture._proc is None
        assert not fixture.root.exists()
    finally:
        _kill_leftovers(launched, fixture)


def test_fixture_start_interrupted_socket_wait_stops_the_daemon(tmp_path, monkeypatch):
    """An interrupt while waiting for the socket stops the daemon and cleans up."""

    launched = _install_fake_daemon(tmp_path, monkeypatch, _FAKE_NEVER_BINDS)

    real_sleep = time.sleep
    calls: list[float] = []

    def interrupted_sleep(seconds: float) -> None:
        # Only the first call (the socket wait) is interrupted: subprocess's
        # timed wait polls through time.sleep too, and must keep working.
        calls.append(seconds)
        if len(calls) == 1:
            raise KeyboardInterrupt
        real_sleep(seconds)

    monkeypatch.setattr(time, "sleep", interrupted_sleep)

    fixture = FixtureDaemon()
    try:
        with pytest.raises(KeyboardInterrupt):
            fixture.start()
        _assert_launched_daemon_stopped(launched)
        assert fixture._proc is None
        assert not fixture.root.exists()
    finally:
        _kill_leftovers(launched, fixture)
