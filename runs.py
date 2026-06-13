"""Run history + timeline rotation — RunStore + TimelineRotator.

DM-modularize-3 (py-1.14.5): two self-contained background/storage
classes lifted verbatim from daemon.py. ``TimelineRotator`` gzips old
``.meshkore/timeline/*.jsonl`` files on a long cadence; ``RunStore`` is
the append-only ledger of agent run records the cockpit reads. Zero
daemon coupling — both are fed only a ``Paths``. daemon.py re-imports
``RunStore`` / ``TimelineRotator``.

Bundler note: imports shared helpers from utils/paths (stripped; resolved
via the flat namespace)."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from hub import Hub
from paths import Paths
from utils import _iso_now, _log


# ───────────────────────────────────────────────────────────────────────
# TimelineRotator (py-1.5.0)
#
# Compresses old jsonl files into .jsonl.gz to keep .meshkore/timeline/
# from growing unbounded over months / years. Files older than
# TIMELINE_ROTATE_AGE_DAYS get gzipped in place (or moved to an archive/
# subdir if configured). Cheap: runs in a background thread on a long
# cadence, only touches files modified before the threshold, never
# touches today's or yesterday's file.
#
# Readers (`_iter_timeline_files` + `_read_timeline_file`) already handle
# .gz transparently, so the cockpit and the agent's history block are
# unaffected by rotation.


TIMELINE_ROTATE_AGE_DAYS = 90
TIMELINE_ROTATE_SCAN_SEC = 3600.0  # once per hour


class TimelineRotator:
    """Background gzipper for old jsonl files in .meshkore/timeline/."""

    def __init__(self, paths: "Paths", age_days: int = TIMELINE_ROTATE_AGE_DAYS):
        self.paths = paths
        self.age_days = age_days
        self._stop = threading.Event()
        threading.Thread(target=self._loop, daemon=True).start()

    def shutdown(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        # Run once at boot (after a brief delay so we don't fight with
        # the cluster's first state.json rebuild), then every hour.
        if self._stop.wait(60.0):
            return
        while True:
            try:
                self.rotate_once()
            except Exception as e:
                _log(f"timeline rotator: {e}")
            if self._stop.wait(TIMELINE_ROTATE_SCAN_SEC):
                return

    def rotate_once(self) -> int:
        if not self.paths.timeline_dir.exists():
            return 0
        cutoff = time.time() - (self.age_days * 86400)
        archive_dir = self.paths.timeline_dir / "archive"
        rotated = 0
        for f in self.paths.timeline_dir.glob("*.jsonl"):
            try:
                st = f.stat()
            except OSError:
                continue
            if st.st_mtime > cutoff:
                continue  # too recent
            # Compress in place, move the .gz to archive/, delete the
            # original. Keep one log line per rotation so the operator
            # can audit it from the daemon's stderr.
            try:
                archive_dir.mkdir(parents=True, exist_ok=True)
                import gzip

                gz_path = archive_dir / (f.name + ".gz")
                if gz_path.exists():
                    # Already rotated — just delete the source.
                    f.unlink()
                    rotated += 1
                    continue
                with open(f, "rb") as src, gzip.open(gz_path, "wb") as dst:
                    while True:
                        chunk = src.read(64 * 1024)
                        if not chunk:
                            break
                        dst.write(chunk)
                f.unlink()
                _log(f"timeline rotator: {f.name} → archive/{gz_path.name}")
                rotated += 1
            except OSError as e:
                _log(f"timeline rotator: skipped {f.name}: {e}")
        return rotated


class RunStore:
    """Persistent registry of story runs (py-1.10.0).

    A "run" is the daemon-side first-class representation of "the
    operator clicked play on initiative X". Each run pins one conv +
    one agent_id + the ordered list of task ids it has to step
    through. Status moves running → cancelled|done|failed; the
    cursor advances per step.

    Storage: `.meshkore/.runtime/runs.json` (atomic tmp+rename).
    Why .runtime: it's per-machine and gitignored — runs are a
    coordinator artifact, not a roadmap artifact.

    Why this exists: the previous (V87) design lived in the cockpit's
    localStorage and the daemon had no concept of a "run". Symptom
    after reload: storyStore.run resurrected as paused and the UI
    treated it as active even though the daemon had finished/idled.
    With the run server-side, GET /runs returns ground truth + the
    `live` flag (= chat_sessions.has(conv)) so the cockpit always
    paints the real state.

    Cancellation propagation: chat_cancel(conv) calls
    `find_by_conv(conv)` and if a run owns the conv with status
    running/stopping, marks it cancelled + broadcasts run.cancelled.
    So either entry point — initiative card's ■ stop OR the chat
    panel's StopBar — converges to the same state.
    """

    STATUS_RUNNING = "running"
    STATUS_STOPPING = "stopping"
    STATUS_CANCELLED = "cancelled"
    STATUS_DONE = "done"
    STATUS_FAILED = "failed"

    ACTIVE_STATUSES = frozenset({STATUS_RUNNING, STATUS_STOPPING})

    def __init__(self, paths: "Paths", hub: "Hub"):
        self.paths = paths
        self.hub = hub
        self._lock = threading.Lock()
        # Schema: {"version": 1, "runs": [<run dict>, ...]}
        self._data: Dict[str, Any] = {"version": 1, "runs": []}
        self._load()

    # ── persistence ────────────────────────────────────────────────
    def _runs_path(self) -> Path:
        return self.paths.runtime / "runs.json"

    def _load(self) -> None:
        fp = self._runs_path()
        if not fp.exists():
            return
        try:
            data = json.loads(fp.read_text())
            if not isinstance(data, dict):
                return
            runs = data.get("runs")
            if isinstance(runs, list):
                # Filter shape-broken entries silently — better than crash.
                clean = [r for r in runs if isinstance(r, dict) and r.get("id")]
                self._data = {"version": 1, "runs": clean}
        except (OSError, ValueError) as e:
            _log(f"runs.json load failed: {e}")

    def _save(self) -> None:
        """Atomic write — tmp then rename — so partial writes don't
        corrupt the file. Called inside the lock."""
        fp = self._runs_path()
        fp.parent.mkdir(parents=True, exist_ok=True)
        tmp = fp.with_suffix(".json.tmp")
        try:
            tmp.write_text(json.dumps(self._data, indent=2, sort_keys=True))
            os.replace(tmp, fp)
        except OSError as e:
            _log(f"runs.json save failed: {e}")

    # ── mutations ──────────────────────────────────────────────────
    def create(
        self,
        *,
        initiative_id: str,
        initiative_title: str,
        conv: str,
        agent_id: str,
        agent_title: str,
        task_ids: List[str],
    ) -> Dict[str, Any]:
        run = {
            "id": f"run_{uuid.uuid4().hex[:12]}",
            "initiative_id": initiative_id,
            "initiative_title": initiative_title,
            "conv": conv,
            "agent_id": agent_id,
            "agent_title": agent_title,
            "task_ids": list(task_ids),
            "cursor": 0,
            "status": self.STATUS_RUNNING,
            "started_at": _iso_now(),
            "last_step_at": _iso_now(),
            "ended_at": None,
            "stream_id": None,
            "error": None,
        }
        with self._lock:
            self._data["runs"].append(run)
            self._save()
        self.hub.broadcast({"type": "run.started", "run": run})
        return run

    def cancel(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Mark cancelled. Returns the updated run, or None if unknown.
        Idempotent: cancelling an already-final run is a no-op."""
        with self._lock:
            run = self._find_locked(run_id)
            if not run:
                return None
            if run["status"] not in self.ACTIVE_STATUSES:
                return run
            run["status"] = self.STATUS_CANCELLED
            run["ended_at"] = _iso_now()
            self._save()
        self.hub.broadcast({"type": "run.cancelled", "run": run})
        return run

    def advance(
        self, run_id: str, cursor: int, stream_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        with self._lock:
            run = self._find_locked(run_id)
            if not run:
                return None
            if run["status"] not in self.ACTIVE_STATUSES:
                return run
            total = len(run["task_ids"])
            run["cursor"] = max(0, min(cursor, total))
            run["last_step_at"] = _iso_now()
            if stream_id is not None:
                run["stream_id"] = stream_id
            # Auto-finalise if cursor walked off the end.
            if run["cursor"] >= total:
                run["status"] = self.STATUS_DONE
                run["ended_at"] = _iso_now()
            self._save()
        ev_type = "run.done" if run["status"] == self.STATUS_DONE else "run.advanced"
        self.hub.broadcast({"type": ev_type, "run": run})
        return run

    def finish(
        self, run_id: str, status: str, error: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        if status not in (self.STATUS_DONE, self.STATUS_FAILED):
            return None
        with self._lock:
            run = self._find_locked(run_id)
            if not run:
                return None
            if run["status"] not in self.ACTIVE_STATUSES:
                return run
            run["status"] = status
            run["ended_at"] = _iso_now()
            if error is not None:
                run["error"] = str(error)
            self._save()
        ev_type = "run.done" if status == self.STATUS_DONE else "run.failed"
        self.hub.broadcast({"type": ev_type, "run": run})
        return run

    def set_stream(self, run_id: str, stream_id: str) -> Optional[Dict[str, Any]]:
        """Cockpit calls this after each /chat/dispatch so the run
        record carries the in-flight stream_id (debuggable trail)."""
        with self._lock:
            run = self._find_locked(run_id)
            if not run:
                return None
            run["stream_id"] = stream_id
            run["last_step_at"] = _iso_now()
            self._save()
        return run

    # ── reads ──────────────────────────────────────────────────────
    def _find_locked(self, run_id: str) -> Optional[Dict[str, Any]]:
        for r in self._data["runs"]:
            if r.get("id") == run_id:
                return r
        return None

    def get(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            r = self._find_locked(run_id)
            return dict(r) if r else None

    def find_by_conv(self, conv: str) -> Optional[Dict[str, Any]]:
        """Return the newest ACTIVE run bound to this conv, or None.
        Used by chat_cancel to propagate cancellation."""
        with self._lock:
            best: Optional[Dict[str, Any]] = None
            for r in self._data["runs"]:
                if r.get("conv") != conv:
                    continue
                if r.get("status") not in self.ACTIVE_STATUSES:
                    continue
                if best is None or (r.get("started_at") or "") > (
                    best.get("started_at") or ""
                ):
                    best = r
            return dict(best) if best else None

    def list_all(
        self, active_only: bool = False, limit: int = 200
    ) -> List[Dict[str, Any]]:
        with self._lock:
            out = list(self._data["runs"])
        # Newest first.
        out.sort(key=lambda r: r.get("started_at") or "", reverse=True)
        if active_only:
            out = [r for r in out if r.get("status") in self.ACTIVE_STATUSES]
        return out[:limit]
