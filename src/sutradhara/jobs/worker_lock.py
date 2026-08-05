"""Process-level singleton lock for the single-node job worker.

The lease scheduler keeps resource accounting in memory, so exactly one
``sutra worker`` process may own a database at a time. SQLite file URLs use a
neighboring ``<database>.worker.lock`` file. Non-file SQLite URLs and other
database URLs fall back to a deterministic lock under the state directory
(``SUTRADHARA_STATE_DIR`` when set, otherwise ``$XDG_STATE_HOME/sutradhara`` or
``~/.local/state/sutradhara``), keyed by a SHA-256 hash of the URL.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import os
import socket
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy.engine import Engine, make_url


class WorkerAlreadyRunning(RuntimeError):
    """Raised when another worker process holds the singleton lock."""

    def __init__(self, lockfile: Path, holder_pid: int | None) -> None:
        self.lockfile = lockfile
        self.holder_pid = holder_pid
        pid_text = "unknown" if holder_pid is None else str(holder_pid)
        super().__init__(f"worker already running; holder pid={pid_text}")


class ProcessLockHeld(RuntimeError):
    """Raised when an exclusive process lock is already owned."""

    def __init__(self, lockfile: Path, holder_pid: int | None, purpose: str) -> None:
        self.lockfile = lockfile
        self.holder_pid = holder_pid
        self.purpose = purpose
        pid_text = "unknown" if holder_pid is None else str(holder_pid)
        super().__init__(f"{purpose} already running; holder pid={pid_text}")


@contextmanager
def worker_lock(engine_or_url: Engine | str) -> Iterator[Path]:
    """Acquire the non-blocking worker singleton lock for a database URL."""

    lockfile = process_lockfile_for(engine_or_url, namespace="worker")
    try:
        with exclusive_process_lock(lockfile, purpose="worker"):
            yield lockfile
    except ProcessLockHeld as exc:
        raise WorkerAlreadyRunning(exc.lockfile, exc.holder_pid) from exc


@contextmanager
def exclusive_process_lock(lockfile: Path, *, purpose: str) -> Iterator[Path]:
    """Acquire the worker-pattern non-blocking flock for another singleton job."""

    lockfile.parent.mkdir(parents=True, exist_ok=True)
    with lockfile.open("a+", encoding="utf-8") as fh:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                raise
            fh.seek(0)
            raise ProcessLockHeld(lockfile, _holder_pid(fh.read()), purpose) from exc
        fh.seek(0)
        fh.truncate()
        pid = os.getpid()
        fh.write(f"{socket.gethostname()}:{pid}\npid={pid}\npurpose={purpose}\n")
        fh.flush()
        try:
            yield lockfile
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def held_process_lock_identity(lockfile: Path) -> str | None:
    """Return the ``hostname:pid`` identity of a *currently held* process lock.

    ``None`` means nobody holds it — a stale lockfile left behind by a dead
    worker reads as unheld, which is exactly what the bundle-claim reaper needs
    (design-bundle-groups §4: liveness checked against the worker-lock holder,
    not against a timeout). The recorded identity is the same string
    ``jobs/attempts.py::default_worker_id`` produces, so a bundle's
    ``claimed_by`` can be compared to it directly.

    **The probe never takes the lock.** Asking "is it held?" by trying to hold
    it makes the reader a writer: while the answer is being computed the lock
    is unavailable, and a worker starting inside that window is told another
    worker is already running and exits. ``LOCK_SH`` does not fix that — a
    shared lock still refuses the worker's ``LOCK_EX`` — so the holder is read
    out of ``/proc/locks``, which is a pure read. flock entries there carry the
    inode the lock is held on, and an entry means *some* open file description
    holds it, including one belonging to this process: the sweeper running
    inside the worker correctly sees its own claim as live.

    Where ``/proc/locks`` is unavailable the old acquire-probe stands in, with
    its window: an unsure answer must not read as "unheld", because for the
    reaper "unheld" means "reap", and reaping a live flush is the worst
    outcome available.
    """

    if not lockfile.exists():
        return None
    holders = _flock_holder_pids(lockfile)
    if holders is not None:
        if not holders:
            return None
        with lockfile.open("r", encoding="utf-8") as fh:
            return _holder_identity(fh.read())
    with lockfile.open("a+", encoding="utf-8") as fh:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                raise
            fh.seek(0)
            return _holder_identity(fh.read())
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        return None


_PROC_LOCKS = Path("/proc/locks")


def _flock_holder_pids(lockfile: Path) -> list[int] | None:
    """Return the pids holding an flock on ``lockfile``, or None if unknowable.

    ``/proc/locks`` prints one line per lock as
    ``<n>: FLOCK ADVISORY WRITE <pid> <major:minor:inode> 0 EOF``, with the
    device in hex and the inode in decimal — matched against the file's own
    ``st_dev``/``st_ino``. An empty list means nobody holds it; ``None`` means
    the file could not be read at all, and the caller falls back rather than
    guessing "unheld".
    """
    try:
        stat = lockfile.stat()
        content = _PROC_LOCKS.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    wanted = f"{os.major(stat.st_dev):02x}:{os.minor(stat.st_dev):02x}:{stat.st_ino}"
    pids: list[int] = []
    for line in content.splitlines():
        fields = line.split()
        # id:, type, ADVISORY/MANDATORY, mode, pid, dev:inode, start, end
        if len(fields) < 6 or fields[1] != "FLOCK" or fields[5] != wanted:
            continue
        pid = _parse_pid(fields[4])
        if pid is not None:
            pids.append(pid)
    return pids


def _holder_identity(content: str) -> str | None:
    for line in content.splitlines():
        stripped = line.strip()
        if ":" in stripped and not stripped.startswith(("pid=", "purpose=")):
            return stripped
    return None


def process_lockfile_for(engine_or_url: Engine | str, *, namespace: str) -> Path:
    """Return one database-scoped singleton lock path for a safe namespace."""

    if not namespace or any(
        char not in "abcdefghijklmnopqrstuvwxyz0123456789-" for char in namespace
    ):
        raise ValueError("lock namespace must contain only lowercase letters, digits, and hyphens")
    url = engine_or_url.url if isinstance(engine_or_url, Engine) else make_url(engine_or_url)
    if url.drivername.startswith("sqlite") and url.database not in {None, "", ":memory:"}:
        assert isinstance(url.database, str)
        db_path = Path(url.database).expanduser()
        if not db_path.is_absolute():
            db_path = Path.cwd() / db_path
        return db_path.resolve(strict=False).with_name(db_path.name + f".{namespace}.lock")
    rendered = url.render_as_string(hide_password=False)
    digest = hashlib.sha256(rendered.encode("utf-8")).hexdigest()[:32]
    return _state_dir() / f"{namespace}-{digest}.lock"


def _state_dir() -> Path:
    raw = os.environ.get("SUTRADHARA_STATE_DIR")
    if raw:
        return Path(raw).expanduser() / "worker-locks"
    xdg = os.environ.get("XDG_STATE_HOME")
    root = Path(xdg).expanduser() if xdg else Path.home() / ".local" / "state"
    return root / "sutradhara" / "worker-locks"


def _holder_pid(content: str) -> int | None:
    for line in content.splitlines():
        if line.startswith("pid="):
            return _parse_pid(line.partition("=")[2])
        if ":" in line:
            parsed = _parse_pid(line.rpartition(":")[2])
            if parsed is not None:
                return parsed
    return None


def _parse_pid(raw: str) -> int | None:
    try:
        pid = int(raw.strip())
    except ValueError:
        return None
    return pid if pid > 0 else None
