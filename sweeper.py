"""sweeper.py — leaf module: the background loop that runs over EVERY project.

DAH1 (initiative `daemon-audit-hardening`). `ChatSessionReaper` and
`QuotaProber` are unrelated in what they do and were identical in how they
did it: the same `_thread`/`_stop` lifecycle, the same `start()` guard, the
same `stop()`, and — the part that actually matters — the same FC-2
per-project binding dance:

    reg = getattr(self.daemon, "_registry", None)
    pids = [c.cluster.id for c in reg.built_contexts()] if reg else [None]
    for pid in pids:
        try:
            if pid is not None: self.daemon._set_req_project(pid)
            ...work...
        finally:
            if pid is not None: self.daemon._clear_req_project()

That block is easy to get subtly wrong (forget the `finally` and one
project's threadlocal leaks into the next project's sweep — every
`self.daemon.paths` afterwards resolves to the WRONG cluster). Having it
written twice meant two chances to get it wrong and two places to fix it.
Any future periodic sweeper should subclass this rather than re-type it.

Subclass contract: set `TICK_SECS` and implement `tick()`, which runs ONCE
per project per tick with that project already bound on this thread.
"""

from __future__ import annotations

import threading
from typing import Any, List, Optional

from utils import _log


class ProjectSweeper:
    """A daemon-thread loop that calls `tick()` once per registered project
    every `TICK_SECS`, with the project bound on the calling thread."""

    TICK_SECS = 30.0
    #: Shown in logs and used as the thread name. Subclasses override.
    NAME = "sweeper"

    def __init__(self, daemon: Any) -> None:
        self.daemon = daemon
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ── lifecycle ───────────────────────────────────────────────────────
    def start(self) -> None:
        if self._thread is not None:
            return
        self.on_start()
        self._thread = threading.Thread(target=self._loop, name=self.NAME, daemon=True)
        self._thread.start()
        _log(f"{self.NAME}: started (tick={self.TICK_SECS}s)")

    def stop(self) -> None:
        self._stop.set()

    def on_start(self) -> None:
        """Optional one-shot work before the loop thread starts (e.g. the
        chat reaper's boot sweep). Failures are logged, never fatal."""

    # ── the loop ────────────────────────────────────────────────────────
    def _project_ids(self) -> List[Optional[str]]:
        """Every project this daemon serves, or `[None]` (bind nothing → the
        default/boot project) when the registry isn't available yet."""
        reg = getattr(self.daemon, "_registry", None)
        if reg is None:
            return [None]
        try:
            return [c.cluster.id for c in reg.built_contexts()]
        except Exception as e:  # noqa: BLE001 — a registry hiccup must not stop the loop
            _log(f"{self.NAME}: project enumeration failed ({e})")
            return [None]

    def _loop(self) -> None:
        while not self._stop.wait(self.TICK_SECS):
            for pid in self._project_ids():
                if self._stop.is_set():
                    break
                try:
                    if pid is not None:
                        self.daemon._set_req_project(pid)
                    self.tick()
                except Exception as e:  # noqa: BLE001 — one project must not stop the rest
                    _log(f"{self.NAME}: tick failed ({e})")
                finally:
                    # Non-negotiable: leaving the threadlocal set would make the
                    # NEXT project's sweep resolve self.daemon.* to the previous
                    # cluster's paths/stores.
                    if pid is not None:
                        self.daemon._clear_req_project()

    def tick(self) -> None:
        """One unit of work for the currently-bound project. Subclass this."""
        raise NotImplementedError
