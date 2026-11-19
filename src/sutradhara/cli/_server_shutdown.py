"""Latch serving-command stop signals without interrupting resource cleanup.

The CLI owns signals around startup and cleanup; uvicorn temporarily captures
them while serving and replays them into this gate when its shutdown returns.
"""

from __future__ import annotations

import signal
import threading
from collections.abc import Callable
from types import FrameType

import uvicorn


class ServerShutdown:
    """Request one stop, forwarding it to the attached HTTP server if present."""

    def __init__(self) -> None:
        self._stop_requested = False
        self._server: uvicorn.Server | None = None
        self._originals: dict[int, Callable[[int, FrameType | None], object] | int | None] = {}
        self._installed: list[int] = []

    def install(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("server shutdown signals require the main thread")
        self._originals = {
            signum: signal.getsignal(signum) for signum in (signal.SIGTERM, signal.SIGINT)
        }
        handler = self._handle_signal
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, handler)
            self._installed.append(signum)

    @property
    def stop_requested(self) -> bool:
        return self._stop_requested

    def check_pending(self) -> bool:
        return self._stop_requested

    def attach(self, server: uvicorn.Server) -> None:
        self._server = server
        if self._stop_requested:
            server.should_exit = True

    def restore(self) -> None:
        try:
            self._restore_signal(signal.SIGTERM)
        finally:
            self._restore_signal(signal.SIGINT)

    def _restore_signal(self, signum: int) -> None:
        if signum in self._installed:
            self._installed.remove(signum)
            signal.signal(signum, self._originals[signum])

    def _handle_signal(self, signum: int, frame: FrameType | None) -> None:
        if self._stop_requested:
            return
        self._stop_requested = True
        if self._server is not None:
            self._server.should_exit = True
