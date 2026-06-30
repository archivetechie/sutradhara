"""Polling registrar for completed landing-root intakes.

The watcher treats the landing filesystem as a durable queue. It discovers
completed receive bags, waits for cheap filesystem stability, then invokes the
existing intake registration boundary in an isolated transaction per candidate.
Terminal marker files are published only after the caller's commit succeeds.
"""

from __future__ import annotations

import errno
import fcntl
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from sutradhara.catalog.session import make_engine, make_session_factory
from sutradhara.catalog.types import IntakeStatus
from sutradhara.intake import (
    IntakeDiscrepancyError,
    InspectReport,
    accept_intake,
    inspect_intake,
    publish_intake_marker,
    register_intake,
)
from sutradhara_receive import (
    BAG_INFO_NAME,
    DATA_DIR_NAME,
    MANIFEST_NAME,
    PACKAGE_INDEX_NAME,
    read_manifest_sha256,
    safe_payload_path,
)

BAGIT_NAME = "bagit.txt"
TAGMANIFEST_NAME = "tagmanifest-sha256.txt"
TERMINAL_MARKERS = (
    "intake.verified.json",
    "intake.quarantined.json",
    "intake.discrepancy.json",
)
ROUTE_REGISTER_STATUSES = {"ready", "already-registered", "quarantined"}

SleepFn = Callable[[float], None]
EventSink = Callable[["WatchEvent"], None]
StopPredicate = Callable[[], bool]


@dataclass(frozen=True)
class WatchEvent:
    """One operator-visible event emitted by the intake watcher."""

    event: str
    path: Path | None = None
    intake_id: str | None = None
    status: str | None = None
    reason: str | None = None
    item_count: int = 0
    jobs_submitted: int = 0
    marker_path: Path | None = None
    details: dict[str, Any] | None = None
    error: str | None = None

    def payload(self) -> dict[str, Any]:
        """Return a JSON-serializable event payload."""

        return {
            "event": self.event,
            "intake_id": self.intake_id,
            "path": str(self.path) if self.path is not None else None,
            "status": self.status,
            "reason": self.reason,
            "item_count": self.item_count,
            "jobs_submitted": self.jobs_submitted,
            "marker_path": str(self.marker_path) if self.marker_path is not None else None,
            "details": self.details,
            "error": self.error,
        }

    @property
    def is_bad_once_outcome(self) -> bool:
        """Return true when a one-shot run should surface a nonzero exit."""

        return self.event in {"intake-quarantined", "intake-discrepancy", "intake-error"}


@dataclass
class WatchState:
    """In-memory stability and error state for a running watcher process."""

    snapshots: dict[Path, tuple[tuple[tuple[str, str, int, int], ...], int]] = field(
        default_factory=dict
    )
    error_counts: dict[Path, int] = field(default_factory=dict)


def process_landing_once(
    landing_root: str | Path,
    *,
    engine: Engine | None = None,
    session_factory: sessionmaker[Session] | Callable[[], Session] | None = None,
    interval_seconds: float = 5.0,
    settle_seconds: float = 2.0,
    stable_polls: int = 2,
    validation_attempts: int = 2,
    artifactclass: str | None = None,
    prepare_profile: str | None = None,
    cache_root: str | Path | None = None,
    cloud_backend_name: str = "cloud-temp",
    cloud_pool_id: str = "cloud-temp",
    state: WatchState | None = None,
    sleep: SleepFn = time.sleep,
    use_lock: bool = True,
) -> list[WatchEvent]:
    """Run one watcher pass, sleeping internally only to satisfy stable polls."""

    root = Path(landing_root).resolve()
    final_cache_root = Path(cache_root).resolve() if cache_root else root / ".sutradhara-cache"
    current_state = state or WatchState()
    factory = _session_factory(engine=engine, session_factory=session_factory)

    def scan() -> list[WatchEvent]:
        return _run_scan(
            root,
            session_factory=factory,
            settle_seconds=settle_seconds,
            stable_polls=stable_polls,
            validation_attempts=validation_attempts,
            artifactclass=artifactclass,
            prepare_profile=prepare_profile,
            cache_root=final_cache_root,
            cloud_backend_name=cloud_backend_name,
            cloud_pool_id=cloud_pool_id,
            state=current_state,
        )

    if use_lock:
        with _WatchLock(final_cache_root / "intake-watch.lock") as lock:
            if not lock.acquired:
                return [
                    WatchEvent(
                        event="intake-skipped",
                        path=root,
                        status="skipped",
                        reason="locked",
                    )
                ]
            return _run_once_with_stability(scan, stable_polls=stable_polls, sleep=sleep, interval=interval_seconds)
    return _run_once_with_stability(scan, stable_polls=stable_polls, sleep=sleep, interval=interval_seconds)


def watch_landing(
    landing_root: str | Path,
    *,
    engine: Engine | None = None,
    session_factory: sessionmaker[Session] | Callable[[], Session] | None = None,
    interval_seconds: float = 5.0,
    settle_seconds: float = 2.0,
    stable_polls: int = 2,
    validation_attempts: int = 2,
    artifactclass: str | None = None,
    prepare_profile: str | None = None,
    cache_root: str | Path | None = None,
    cloud_backend_name: str = "cloud-temp",
    cloud_pool_id: str = "cloud-temp",
    on_event: EventSink | None = None,
    stop: StopPredicate | None = None,
    max_iterations: int | None = None,
    sleep: SleepFn = time.sleep,
    use_lock: bool = True,
) -> list[WatchEvent]:
    """Run the foreground polling loop until stopped."""

    root = Path(landing_root).resolve()
    final_cache_root = Path(cache_root).resolve() if cache_root else root / ".sutradhara-cache"
    current_state = WatchState()
    factory = _session_factory(engine=engine, session_factory=session_factory)
    events: list[WatchEvent] = []

    def emit(event: WatchEvent) -> None:
        events.append(event)
        if on_event is not None:
            on_event(event)

    lock_path = final_cache_root / "intake-watch.lock"
    lock_cm = _WatchLock(lock_path) if use_lock else _NullLock()
    with lock_cm as lock:
        if not lock.acquired:
            emit(WatchEvent(event="intake-skipped", path=root, status="skipped", reason="locked"))
            return events
        iteration = 0
        emit(
            WatchEvent(
                event="watch-start",
                path=root,
                details={"interval": interval_seconds, "settle_seconds": settle_seconds},
            )
        )
        while stop is None or not stop():
            for event in _run_scan(
                root,
                session_factory=factory,
                settle_seconds=settle_seconds,
                stable_polls=stable_polls,
                validation_attempts=validation_attempts,
                artifactclass=artifactclass,
                prepare_profile=prepare_profile,
                cache_root=final_cache_root,
                cloud_backend_name=cloud_backend_name,
                cloud_pool_id=cloud_pool_id,
                state=current_state,
            ):
                emit(event)
            iteration += 1
            if max_iterations is not None and iteration >= max_iterations:
                break
            sleep(interval_seconds)
        emit(WatchEvent(event="watch-stop", path=root))
    return events


def _run_once_with_stability(
    scan: Callable[[], list[WatchEvent]],
    *,
    stable_polls: int,
    sleep: SleepFn,
    interval: float,
) -> list[WatchEvent]:
    merged: dict[str, WatchEvent] = {}
    order: list[str] = []
    events = scan()
    _merge_scan_events(merged, order, events)
    attempts = max(1, stable_polls)
    for _ in range(1, attempts):
        if not any(event.reason == "not-stable" for event in events):
            break
        sleep(interval)
        events = scan()
        _merge_scan_events(merged, order, events)
    return [merged[key] for key in order]


def _merge_scan_events(
    merged: dict[str, WatchEvent],
    order: list[str],
    events: list[WatchEvent],
) -> None:
    for event in events:
        key = str(event.path) if event.path is not None else event.event
        previous = merged.get(key)
        if previous is not None and event.reason == "terminal-marker":
            continue
        if previous is not None and previous.reason != "not-stable" and event.reason == "not-stable":
            continue
        if key not in merged:
            order.append(key)
        merged[key] = event


def _run_scan(
    landing_root: Path,
    *,
    session_factory: sessionmaker[Session] | Callable[[], Session],
    settle_seconds: float,
    stable_polls: int,
    validation_attempts: int,
    artifactclass: str | None,
    prepare_profile: str | None,
    cache_root: Path,
    cloud_backend_name: str,
    cloud_pool_id: str,
    state: WatchState,
) -> list[WatchEvent]:
    events: list[WatchEvent] = []
    try:
        candidates = _candidate_dirs(landing_root)
    except Exception as exc:
        return [
            WatchEvent(
                event="intake-error",
                path=landing_root,
                status="error",
                reason=type(exc).__name__,
                error=str(exc),
            )
        ]
    for candidate in candidates:
        events.append(
            _process_candidate(
                candidate,
                session_factory=session_factory,
                settle_seconds=settle_seconds,
                stable_polls=stable_polls,
                validation_attempts=validation_attempts,
                artifactclass=artifactclass,
                prepare_profile=prepare_profile,
                cache_root=cache_root,
                cloud_backend_name=cloud_backend_name,
                cloud_pool_id=cloud_pool_id,
                state=state,
            )
        )
    return events


def _process_candidate(
    candidate: Path,
    *,
    session_factory: sessionmaker[Session] | Callable[[], Session],
    settle_seconds: float,
    stable_polls: int,
    validation_attempts: int,
    artifactclass: str | None,
    prepare_profile: str | None,
    cache_root: Path,
    cloud_backend_name: str,
    cloud_pool_id: str,
    state: WatchState,
) -> WatchEvent:
    if (candidate / ".receiving.json").exists():
        return WatchEvent(event="intake-skipped", path=candidate, status="skipped", reason="active-receive")
    if _has_terminal_marker(candidate):
        return WatchEvent(event="intake-skipped", path=candidate, status="skipped", reason="terminal-marker")
    sentinel = candidate / "intake.json"
    if _file_age_seconds(sentinel) < settle_seconds:
        return WatchEvent(event="intake-skipped", path=candidate, status="skipped", reason="not-settled")
    if not _observe_stable(candidate, state=state, stable_polls=stable_polls):
        return WatchEvent(event="intake-skipped", path=candidate, status="skipped", reason="not-stable")

    try:
        inspection = _inspect_with_retries(
            candidate,
            session_factory=session_factory,
            validation_attempts=validation_attempts,
        )
    except Exception as exc:
        _remember_error(candidate, state)
        return WatchEvent(
            event="intake-error",
            path=candidate,
            status="error",
            reason=type(exc).__name__,
            error=str(exc),
        )

    if inspection.status in ROUTE_REGISTER_STATUSES:
        return _register_candidate(
            candidate,
            session_factory=session_factory,
            artifactclass=artifactclass,
            prepare_profile=prepare_profile,
            cache_root=cache_root,
            cloud_backend_name=cloud_backend_name,
            cloud_pool_id=cloud_pool_id,
        )
    if inspection.status == "incomplete":
        return WatchEvent(
            event="intake-validation-retry",
            path=candidate,
            intake_id=inspection.intake_id,
            status=inspection.status,
            reason=inspection.reason or "bag-incomplete",
            details=inspection.details,
        )
    if inspection.status == "invalid":
        if _snapshot_is_unchanged(candidate, state):
            return _register_candidate(
                candidate,
                session_factory=session_factory,
                artifactclass=artifactclass,
                prepare_profile=prepare_profile,
                cache_root=cache_root,
                cloud_backend_name=cloud_backend_name,
                cloud_pool_id=cloud_pool_id,
            )
        return WatchEvent(
            event="intake-validation-retry",
            path=candidate,
            intake_id=inspection.intake_id,
            status=inspection.status,
            reason=inspection.reason or "bag-invalid",
            details=inspection.details,
        )
    return WatchEvent(
        event="intake-error",
        path=candidate,
        intake_id=inspection.intake_id,
        status="error",
        reason="unknown-inspect-status",
        details={"status": inspection.status},
    )


def _register_candidate(
    candidate: Path,
    *,
    session_factory: sessionmaker[Session] | Callable[[], Session],
    artifactclass: str | None,
    prepare_profile: str | None,
    cache_root: Path,
    cloud_backend_name: str,
    cloud_pool_id: str,
) -> WatchEvent:
    session = session_factory()
    try:
        if prepare_profile is None:
            outcome = register_intake(
                session,
                candidate,
                artifactclass=artifactclass,
                cache_root=cache_root,
                cloud_backend_name=cloud_backend_name,
                cloud_pool_id=cloud_pool_id,
            )
        else:
            outcome = accept_intake(
                session,
                candidate,
                artifactclass=artifactclass,
                prepare_profile=prepare_profile,
                cache_root=cache_root,
                cloud_backend_name=cloud_backend_name,
                cloud_pool_id=cloud_pool_id,
            )
        session.commit()
    except IntakeDiscrepancyError as exc:
        session.rollback()
        publish_intake_marker(exc.marker)
        return WatchEvent(
            event="intake-discrepancy",
            path=exc.path,
            intake_id=exc.intake_id,
            status="discrepancy",
            reason=exc.reason,
            marker_path=exc.marker.path,
            details=exc.details,
        )
    except Exception as exc:
        session.rollback()
        return WatchEvent(
            event="intake-error",
            path=candidate,
            status="error",
            reason=type(exc).__name__,
            error=str(exc),
        )
    finally:
        session.close()

    publish_intake_marker(outcome.marker)
    if outcome.status == IntakeStatus.QUARANTINED.value:
        event_name = "intake-quarantined"
    elif outcome.reason == "already-registered":
        event_name = "intake-already-registered"
    else:
        event_name = "intake-registered"
    return WatchEvent(
        event=event_name,
        path=outcome.path,
        intake_id=outcome.intake_id,
        status=outcome.status,
        reason=outcome.reason,
        item_count=outcome.item_count,
        jobs_submitted=outcome.jobs_submitted,
        marker_path=outcome.marker.path if outcome.marker else None,
        details=outcome.details,
    )


def _inspect_with_retries(
    candidate: Path,
    *,
    session_factory: sessionmaker[Session] | Callable[[], Session],
    validation_attempts: int,
) -> InspectReport:
    attempts = max(1, validation_attempts)
    report = _inspect_once(candidate, session_factory=session_factory)
    for _ in range(1, attempts):
        if report.status not in {"incomplete", "invalid"}:
            break
        report = _inspect_once(candidate, session_factory=session_factory)
    return report


def _inspect_once(
    candidate: Path,
    *,
    session_factory: sessionmaker[Session] | Callable[[], Session],
) -> InspectReport:
    session = session_factory()
    try:
        return inspect_intake(session, candidate)
    finally:
        session.close()


def _candidate_dirs(landing_root: Path) -> list[Path]:
    if not landing_root.exists():
        raise FileNotFoundError(landing_root)
    if (landing_root / "intake.json").exists():
        return [landing_root]
    return sorted(
        path
        for path in landing_root.iterdir()
        if path.is_dir() and not path.name.startswith(".") and (path / "intake.json").exists()
    )


def _has_terminal_marker(path: Path) -> bool:
    return any((path / name).exists() for name in TERMINAL_MARKERS)


def _file_age_seconds(path: Path) -> float:
    try:
        return max(0.0, time.time() - path.stat().st_mtime)
    except OSError:
        return 0.0


def _observe_stable(candidate: Path, *, state: WatchState, stable_polls: int) -> bool:
    snapshot = _stability_snapshot(candidate)
    previous = state.snapshots.get(candidate)
    if previous is None or previous[0] != snapshot:
        state.snapshots[candidate] = (snapshot, 1)
        return stable_polls <= 1
    count = previous[1] + 1
    state.snapshots[candidate] = (snapshot, count)
    return count >= max(1, stable_polls)


def _snapshot_is_unchanged(candidate: Path, state: WatchState) -> bool:
    previous = state.snapshots.get(candidate)
    current = _stability_snapshot(candidate)
    if previous is None:
        state.snapshots[candidate] = (current, 1)
        return False
    if previous[0] != current:
        state.snapshots[candidate] = (current, 1)
        return False
    return True


def _stability_snapshot(candidate: Path) -> tuple[tuple[str, str, int, int], ...]:
    entries: list[tuple[str, str, int, int]] = []
    for name in ("intake.json", BAGIT_NAME, BAG_INFO_NAME, MANIFEST_NAME, TAGMANIFEST_NAME, PACKAGE_INDEX_NAME):
        _append_path_snapshot(entries, candidate / name, name)
    data_root = candidate / DATA_DIR_NAME
    if data_root.exists():
        for path in sorted(data_root.rglob("*")):
            relpath = f"{DATA_DIR_NAME}/{path.relative_to(data_root).as_posix()}"
            _append_path_snapshot(entries, path, relpath)
    manifest_path = candidate / MANIFEST_NAME
    try:
        manifest = read_manifest_sha256(manifest_path)
    except Exception:
        if not data_root.exists():
            _append_absent(entries, DATA_DIR_NAME)
    else:
        for relpath in sorted(manifest):
            try:
                payload_path = safe_payload_path(data_root, relpath)
            except Exception:
                entries.append((f"{DATA_DIR_NAME}/{relpath}", "unsafe", 0, 0))
                continue
            if not payload_path.exists():
                _append_absent(entries, f"{DATA_DIR_NAME}/{relpath}")
    return tuple(sorted(entries))


def _append_path_snapshot(
    entries: list[tuple[str, str, int, int]],
    path: Path,
    relpath: str,
) -> None:
    try:
        stat_result = path.lstat()
    except OSError:
        return
    if path.is_dir():
        kind = "dir"
        size = 0
    elif path.is_file():
        kind = "file"
        size = stat_result.st_size
    else:
        kind = "other"
        size = stat_result.st_size
    entries.append((relpath, kind, size, stat_result.st_mtime_ns))


def _append_absent(entries: list[tuple[str, str, int, int]], relpath: str) -> None:
    entries.append((relpath, "absent", 0, 0))


def _remember_error(candidate: Path, state: WatchState) -> None:
    state.error_counts[candidate] = state.error_counts.get(candidate, 0) + 1


def _session_factory(
    *,
    engine: Engine | None,
    session_factory: sessionmaker[Session] | Callable[[], Session] | None,
) -> sessionmaker[Session] | Callable[[], Session]:
    if session_factory is not None:
        return session_factory
    return make_session_factory(engine or make_engine())


class _WatchLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle: Any | None = None
        self.acquired = False

    def __enter__(self) -> "_WatchLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+")
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                raise
            self._handle.close()
            self._handle = None
            self.acquired = False
            return self
        self.acquired = True
        return self

    def __exit__(self, *_exc: object) -> None:
        if self._handle is not None:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            self._handle.close()
        self._handle = None
        self.acquired = False


class _NullLock:
    acquired = True

    def __enter__(self) -> "_NullLock":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None
