"""cronsched.py — CronScheduler — 10s tick + due-job dispatch.

Extracted from cron.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

from datetime import datetime, timezone
from paths import Paths
from typing import Dict, List

import threading
from typing import TYPE_CHECKING, Any, Optional

from cron import CronRunner, _cron_next
from hub import Hub
from utils import _iso_now, _log

if TYPE_CHECKING:
    from cluster import Cluster


class CronScheduler:
    """Tick loop. Every TICK_SEC seconds: check each registered job,
    fire any whose `next_run` has arrived (only if this daemon is the
    coordinator), advance `next_run` to the next future slot."""

    TICK_SEC = 10  # operator decision 2026-05-19

    def __init__(self, paths: Paths, cluster: Cluster, hub: Hub, identity: str):
        self.paths = paths
        self.cluster = cluster
        self.hub = hub
        self.identity = identity
        self.runner = CronRunner(paths, cluster, hub, identity)
        self._jobs: Dict[str, Dict[str, Any]] = {}  # job_id → {job, next_run}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._timer: Optional[threading.Timer] = None

    # ── coordinator gate ─────────────────────────────────────────────
    def is_coordinator(self) -> bool:
        owner = self.cluster.crons_owner
        # If no owner is declared but crons exist, the first daemon to
        # boot owns them — pragmatic default for single-machine setups.
        if not owner:
            return bool(self.cluster.crons)
        return owner == self.identity

    # ── load/reload ─────────────────────────────────────────────────
    def reload_jobs(self) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            self._jobs = {}
            for job in self.cluster.crons:
                try:
                    next_run = _cron_next(job["schedule"], now)
                except ValueError as e:
                    _log(f"cron {job['id']}: cannot compute next_run: {e}")
                    continue
                self._jobs[job["id"]] = {"job": job, "next_run": next_run}

    # ── lifecycle ───────────────────────────────────────────────────
    def start(self) -> None:
        self.reload_jobs()
        n = len(self._jobs)
        if n == 0:
            _log("cron: no jobs registered (cluster.yaml has no `crons:` block)")
        else:
            owner_status = (
                "coordinator"
                if self.is_coordinator()
                else f"peer (owner={self.cluster.crons_owner})"
            )
            _log(
                f"cron: {n} job(s) registered, this daemon is {owner_status}, tick every {self.TICK_SEC}s"
            )
            for jid, state in self._jobs.items():
                _log(f"  - {jid}: next_run={state['next_run'].isoformat()}")
        self._schedule_next_tick()

    def stop(self) -> None:
        self._stop.set()
        if self._timer:
            self._timer.cancel()

    def _schedule_next_tick(self) -> None:
        if self._stop.is_set():
            return
        self._timer = threading.Timer(self.TICK_SEC, self._tick)
        self._timer.daemon = True
        self._timer.start()

    def _tick(self) -> None:
        try:
            self._do_tick()
        except Exception as e:
            _log(f"cron tick error: {e}")
        self._schedule_next_tick()

    def _do_tick(self) -> None:
        now = datetime.now(timezone.utc)
        is_coord = self.is_coordinator()
        fires = []
        with self._lock:
            for jid, state in self._jobs.items():
                job = state["job"]
                if not job.get("enabled", True):
                    continue
                if state["next_run"] > now:
                    continue
                fires.append((jid, job, state["next_run"]))
                # Advance — catch-up: skip missed windows, jump to next future
                try:
                    state["next_run"] = _cron_next(job["schedule"], now)
                except ValueError:
                    pass
        for jid, job, scheduled_for in fires:
            if is_coord:
                self.runner.spawn(job, reason="scheduled")
            else:
                self.hub.broadcast(
                    {
                        "type": "cron.would_have_fired",
                        "id": jid,
                        "scheduled_for": scheduled_for.isoformat(),
                        "reason": f"not coordinator (owner={self.cluster.crons_owner!r}, me={self.identity!r})",
                        "ts": _iso_now(),
                    }
                )

    # ── introspection ───────────────────────────────────────────────
    def list_jobs(self) -> List[Dict[str, Any]]:
        out = []
        with self._lock:
            for jid, state in self._jobs.items():
                out.append(
                    {
                        **state["job"],
                        "next_run": state["next_run"].isoformat(),
                        "running": self.runner.is_running(jid),
                    }
                )
        return out

    def trigger(self, job_id: str, reason: str = "manual") -> Optional[Dict[str, Any]]:
        with self._lock:
            state = self._jobs.get(job_id)
        if not state:
            return None
        return self.runner.spawn(state["job"], reason=reason)
