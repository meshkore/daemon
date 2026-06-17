"""cron.py — in-daemon POSIX cron (CronRunner + CronScheduler + helpers).

Extracted from daemon.py (DA-CRON-01, daemon-architecture-v2). Replaces
every external scheduler (LaunchAgent / cron-tab / GH Actions cron): the
daemon ticks every 10s, decides which `cluster.yaml.crons:` jobs are due,
and spawns a subprocess per due job. No daemon backref — constructed with
(paths, cluster, hub, identity); Cluster is a type-only reference."""

from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hub import Hub
from paths import Paths
from utils import _iso_now, _log

if TYPE_CHECKING:
    from cluster import Cluster  # noqa: F401 — type-only; bundler drops the whole TYPE_CHECKING block


def _parse_cron_field(field: str, lo: int, hi: int) -> set:
    """Parse one POSIX cron field (minute / hour / dom / month / dow)
    into the set of integers it matches. Supports: '*', 'A', 'A-B',
    'A,B,C', '*/N', 'A-B/N'. No L/W/# modifiers, no aliases."""
    out = set()
    for part in field.split(","):
        step = 1
        if "/" in part:
            base, step_s = part.split("/", 1)
            step = int(step_s)
        else:
            base = part
        if base == "*":
            start, end = lo, hi
        elif "-" in base:
            a, b = base.split("-", 1)
            start, end = int(a), int(b)
        else:
            n = int(base)
            start, end = n, n
        for v in range(start, end + 1, step):
            if lo <= v <= hi:
                out.add(v)
    return out


def _cron_next(expr: str, after: datetime) -> datetime:
    """Compute the next datetime > `after` that matches the 5-field
    POSIX cron expression. Walks forward minute-by-minute (bounded to
    ~4 years so a misconfigured expr fails loudly rather than spinning
    forever)."""
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"bad cron expression (need 5 fields): {expr!r}")
    minute_set = _parse_cron_field(parts[0], 0, 59)
    hour_set = _parse_cron_field(parts[1], 0, 23)
    dom_set = _parse_cron_field(parts[2], 1, 31)
    month_set = _parse_cron_field(parts[3], 1, 12)
    # Cron dow: Sunday=0..Saturday=6. Python's weekday(): Monday=0..Sunday=6.
    # Convert at match time with (py + 1) % 7.
    dow_set = _parse_cron_field(parts[4], 0, 6)
    t = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(60 * 24 * 366 * 4):
        if (
            t.minute in minute_set
            and t.hour in hour_set
            and t.month in month_set
            and t.day in dom_set
            and ((t.weekday() + 1) % 7) in dow_set
        ):
            return t
        t += timedelta(minutes=1)
    raise ValueError(f"no next match within 4 years for {expr!r}")


def _curated_path_entries() -> List[str]:
    """PATH entries we prepend to every cron child's env, so the cron
    can find `wrangler`, `flyctl`, `claude`, `node`, etc. regardless of
    how the daemon itself was launched. Solves the 2026-05-19 incident
    where the LaunchAgent's PATH didn't include nvm."""
    import glob as _glob

    out: List[str] = []
    candidates = [
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
    ]
    # Highest nvm Node version
    nvm = sorted(
        _glob.glob(os.path.expanduser("~/.nvm/versions/node/v*/bin")), reverse=True
    )
    if nvm:
        candidates.insert(0, nvm[0])
    for p in candidates:
        if os.path.isdir(p) and p not in out:
            out.append(p)
    return out


class CronRunner:
    """Spawns one subprocess per due job. Captures stdout+stderr to a
    per-run log file under `.meshkore/.runtime/logs/cron/<job_id>/<ts>.log`.
    Enforces `max_runtime_sec` with SIGTERM → 30 s → SIGKILL on the
    process group (so children of the spawned shell die too)."""

    def __init__(self, paths: Paths, cluster: Cluster, hub: Hub, identity: str):
        self.paths = paths
        self.cluster = cluster
        self.hub = hub
        self.identity = identity
        self.paths.crons_logs_dir.mkdir(parents=True, exist_ok=True)
        self._active: Dict[str, Any] = {}  # job_id → subprocess.Popen
        self._lock = threading.Lock()

    def is_running(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._active

    def spawn(
        self, job: Dict[str, Any], reason: str = "scheduled"
    ) -> Optional[Dict[str, Any]]:
        """Fire one run of `job`. Returns the started Run dict, or
        None if the job is already running (no concurrent fires)."""

        jid = job["id"]
        with self._lock:
            if jid in self._active:
                self.hub.broadcast(
                    {
                        "type": "cron.skipped",
                        "id": jid,
                        "reason": "already running",
                        "ts": _iso_now(),
                    }
                )
                return None
        env = self._resolve_env(job.get("env") or {})
        log_path = self._make_log_path(jid)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        ts = _iso_now()
        try:
            log_handle = open(log_path, "ab")
            proc = subprocess.Popen(
                job["cmd"],
                shell=True,
                cwd=str(self.paths.root),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except Exception as e:
            _log(f"cron spawn FAIL {jid}: {e}")
            self.hub.broadcast(
                {
                    "type": "cron.error",
                    "id": jid,
                    "error": str(e),
                    "ts": ts,
                }
            )
            return None
        with self._lock:
            self._active[jid] = proc
        self.hub.broadcast(
            {
                "type": "cron.fired",
                "id": jid,
                "reason": reason,
                "pid": proc.pid,
                "log": str(log_path.relative_to(self.paths.root)),
                "ts": ts,
            }
        )
        run = {
            "id": jid,
            "started_at": ts,
            "pid": proc.pid,
            "log_path": str(log_path),
            "status": "running",
        }
        threading.Thread(
            target=self._wait_for,
            args=(jid, proc, log_handle, job, log_path, ts),
            daemon=True,
        ).start()
        return run

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            proc = self._active.get(job_id)
        if not proc or proc.poll() is not None:
            return False
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            threading.Timer(30.0, lambda: self._sigkill(job_id)).start()
            return True
        except (OSError, ProcessLookupError):
            return False

    def _sigkill(self, job_id: str) -> None:
        with self._lock:
            proc = self._active.get(job_id)
        if not proc or proc.poll() is not None:
            return
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (OSError, ProcessLookupError):
            pass

    def _wait_for(
        self,
        jid: str,
        proc,
        log_handle,
        job: Dict[str, Any],
        log_path: Path,
        started_at: str,
    ) -> None:
        timeout = int(job.get("max_runtime_sec", 7200))
        t0 = time.monotonic()
        while proc.poll() is None and (time.monotonic() - t0) < timeout:
            time.sleep(1)
        timed_out = proc.poll() is None
        if timed_out:
            self.hub.broadcast({"type": "cron.timeout", "id": jid, "ts": _iso_now()})
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                time.sleep(30)
                if proc.poll() is None:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass
        exit_code = proc.wait()
        try:
            log_handle.close()
        except Exception:
            pass
        with self._lock:
            self._active.pop(jid, None)
        status = "timeout" if timed_out else ("ok" if exit_code == 0 else "failed")
        self.hub.broadcast(
            {
                "type": "cron.finished",
                "id": jid,
                "exit": exit_code,
                "status": status,
                "duration_sec": round(time.monotonic() - t0, 1),
                "log": str(log_path.relative_to(self.paths.root)),
                "ts": _iso_now(),
            }
        )

    def _resolve_env(self, job_env: Dict[str, str]) -> Dict[str, str]:
        env = dict(os.environ)
        curated = _curated_path_entries()
        if curated:
            env["PATH"] = ":".join(curated) + ":" + env.get("PATH", "")
        for k, v in job_env.items():
            if not isinstance(v, str) or not isinstance(k, str):
                continue
            if v.startswith("file:"):
                rel = v[len("file:") :]
                full = Path(rel) if os.path.isabs(rel) else (self.paths.root / rel)
                try:
                    env[k] = full.read_text().strip()
                except OSError as e:
                    _log(f"cron env: cannot read {full}: {e}")
            elif v.startswith("$"):
                env[k] = os.environ.get(v[1:], v)
            else:
                env[k] = os.path.expandvars(os.path.expanduser(v))
        return env

    def _make_log_path(self, job_id: str) -> Path:
        d = self.paths.crons_logs_dir / job_id
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return d / f"{ts}.log"


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
