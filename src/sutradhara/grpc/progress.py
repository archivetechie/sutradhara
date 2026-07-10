"""In-memory progress snapshots for active streaming gRPC intakes.

The durable receive state remains in ``grpc_intake`` and the landing receipt
ledger. This registry is only for live operator feedback while ``sutra serve``
hosts both the gRPC upload path and HTTP console API in one process.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass


@dataclass(frozen=True)
class FileProgress:
    """Byte progress for one uploaded manifest relpath."""

    relpath: str
    bytes_received: int
    bytes_total: int


@dataclass(frozen=True)
class ReceiveProgressSnapshot:
    """Current aggregate byte progress for one active intake."""

    intake_id: str
    planned_bytes_total: int | None
    files: tuple[FileProgress, ...]

    @property
    def bytes_received(self) -> int:
        """Return received bytes across all files observed in this process."""

        return sum(item.bytes_received for item in self.files)

    @property
    def bytes_total(self) -> int | None:
        """Return the best known total bytes for this receive."""

        known_total = sum(item.bytes_total for item in self.files)
        if self.planned_bytes_total is None:
            return known_total if known_total > 0 else None
        return max(self.planned_bytes_total, known_total)


class ReceiveProgressRegistry:
    """Thread-safe live progress registry for gRPC upload workers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._planned_totals: dict[str, int | None] = {}
        self._files: dict[str, dict[str, FileProgress]] = {}

    def start(self, intake_id: str, *, planned_bytes_total: int | None = None) -> None:
        """Record the planned total for an intake before upload begins."""

        total = (
            planned_bytes_total
            if planned_bytes_total is not None and planned_bytes_total >= 0
            else None
        )
        with self._lock:
            self._planned_totals[intake_id] = total
            self._files.setdefault(intake_id, {})

    def update_file(
        self,
        intake_id: str,
        *,
        relpath: str,
        bytes_received: int,
        bytes_total: int,
    ) -> None:
        """Update one file's received bytes, clamped to the declared total."""

        total = max(0, int(bytes_total))
        received = max(0, min(int(bytes_received), total))
        with self._lock:
            self._files.setdefault(intake_id, {})[relpath] = FileProgress(
                relpath=relpath,
                bytes_received=received,
                bytes_total=total,
            )
            self._planned_totals.setdefault(intake_id, None)

    def discard(self, intake_id: str) -> None:
        """Forget live progress for an intake that no longer needs it."""

        with self._lock:
            self._planned_totals.pop(intake_id, None)
            self._files.pop(intake_id, None)

    def complete_file(self, intake_id: str, *, relpath: str) -> None:
        """Drop a file once its bytes are represented by the durable receipt summary."""

        with self._lock:
            self._files.get(intake_id, {}).pop(relpath, None)

    def snapshot(self, intake_id: str) -> ReceiveProgressSnapshot | None:
        """Return a stable snapshot for one intake, if this process has progress."""

        with self._lock:
            if intake_id not in self._planned_totals and intake_id not in self._files:
                return None
            planned = self._planned_totals.get(intake_id)
            files = tuple(sorted(self._files.get(intake_id, {}).values(), key=lambda item: item.relpath))
        return ReceiveProgressSnapshot(
            intake_id=intake_id,
            planned_bytes_total=planned,
            files=files,
        )
