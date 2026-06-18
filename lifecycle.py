"""lifecycle.py — extracted from daemon.py (daemon-architecture-v2 Phase 3d).

LifecycleMixin: methods moved VERBATIM out of Daemon; Daemon inherits both so
every self.* resolves on the combined instance -> byte-identical."""

from __future__ import annotations

import faulthandler
import os
import signal
import threading
import time
from typing import Optional

from chat import ChatSessionReaper
from constants import DAEMON_VERSION
from http_server import PoolHTTPServer, _build_tls_context
from quota import QuotaProber
from routes import make_handler
from selfupdate import VersionWatcher
from debuglog import DebugLog
from utils import (
    _debug_emit,
    _debug_enabled,
    _find_tls_bundle,
    _iso_now,
    _log,
    set_debug_log,
)


class LifecycleMixin:
    def serve_forever(self) -> None:
        self._write_runtime()
        # py-1.10.17 — Initialise the debug stream singleton FIRST so
        # boot-time `_log()` calls below already land in debug.jsonl.
        # py-1.10.21 — Honour `cluster.yaml.debug.enabled: false` for
        # downstream clusters that don't want the disk footprint.
        # Default is ON (this is MeshKore-native dogfooding).
        # DM7 — _DEBUG_LOG lives in utils.py. set_debug_log() wires it
        # so every sibling module's late-binding lookup finds the same
        # singleton. Works identically in source-tree dev and bundle.
        if _debug_enabled(self.cluster):
            set_debug_log(DebugLog(self.paths.runtime / "debug.jsonl"))
            _debug_emit(
                "boot",
                msg=f"daemon {DAEMON_VERSION} starting on port {self.port}",
                data={"identity": self.identity, "cluster": self.cluster.id},
            )
        else:
            set_debug_log(None)
            _log("debug stream: disabled by cluster.yaml.debug.enabled=false")
        handler = make_handler(self)
        # py-1.12.24 — Bounded worker pool. Cap configurable via
        # cluster.yaml.daemon.http.max_workers (default 64). Prevents
        # the unbounded thread spawn that caused the 2026-06-10 hang.
        d_block = (
            self.cluster.data.get("daemon")
            if isinstance(self.cluster.data, dict)
            else None
        )
        http_block = (d_block or {}).get("http") if isinstance(d_block, dict) else None
        max_workers = int((http_block or {}).get("max_workers") or 128)
        # py-1.14.3 — same-port re-exec support. When a self-update
        # handed off to us with MESHKORE_REEXEC_WAIT_PORT=1, the OLD
        # daemon is still releasing the listen socket on `self.port`.
        # Retry the bind for up to ~12 s (250 ms cadence) so we come up
        # on the SAME port — the cockpit's WS just reconnects to the
        # identical URL, no port hunting, no operator action. Without
        # the flag we bind once (fast-fail preserves the old behaviour
        # for a normal boot where a stale daemon means a real conflict).
        reexec_wait = os.environ.get("MESHKORE_REEXEC_WAIT_PORT", "").strip() in (
            "1",
            "true",
            "yes",
            "on",
        )
        if reexec_wait:
            deadline = time.monotonic() + 12.0
            last_err: Optional[Exception] = None
            self.server = None
            while time.monotonic() < deadline:
                try:
                    self.server = PoolHTTPServer(
                        ("127.0.0.1", self.port), handler, max_workers=max_workers
                    )
                    break
                except OSError as e:
                    last_err = e
                    time.sleep(0.25)
            if self.server is None:
                _log(
                    f"re-exec: port {self.port} never freed within 12s "
                    f"({last_err}); the old daemon may be stuck"
                )
                raise SystemExit(f"re-exec bind failed on port {self.port}: {last_err}")
        else:
            self.server = PoolHTTPServer(
                ("127.0.0.1", self.port), handler, max_workers=max_workers
            )
        # py-1.12.24 — SIGUSR1 → faulthandler dump. Operator sends
        # `kill -USR1 <pid>`; daemon appends every thread's stack to
        # `.meshkore/.runtime/threads.log`. Caught lock-contention bugs
        # (like 2026-06-10) leave actionable stacks for diagnosis.
        threads_log = open(self.paths.runtime / "threads.log", "a")
        faulthandler.register(
            signal.SIGUSR1, file=threads_log, all_threads=True, chain=False
        )
        self._threads_log_fp = threads_log  # keep ref so GC doesn't close
        # D-TLS-01 — wrap the socket with TLS when the bundle is
        # present. Cockpit uses https://daemon.meshkore.com:<port>
        # then, no mixed-content / LNA Issues.
        bundle = _find_tls_bundle()
        ctx = _build_tls_context(*bundle) if bundle else None
        self.tls_enabled = ctx is not None
        if ctx is not None:
            # py-1.15.2 — do_handshake_on_connect=False so accept() returns
            # an un-handshaked SSLSocket immediately; the handshake is then
            # completed on a pool worker (PoolHTTPServer.process_request_thread),
            # NOT in the single accept loop. Previously a slow/half-open
            # client (browsers open speculative connections; the cockpit
            # opens many to the actively-polled project) blocked the accept
            # loop mid-handshake and the kernel refused every other
            # connection → intermittent ERR_CONNECTION_REFUSED that
            # stranded cockpit hydration.
            self.server.socket = ctx.wrap_socket(
                self.server.socket, server_side=True, do_handshake_on_connect=False
            )
        scheme = "https" if self.tls_enabled else "http"
        _log(
            f"meshcore-py listening on {scheme}://127.0.0.1:{self.port} "
            f"(identity={self.identity}, cluster={self.cluster.id}, "
            f"tls={'on (daemon.meshkore.com)' if self.tls_enabled else 'off'})"
        )
        # D-CRON-02: start the scheduler. Ticks every 10s in a background
        # thread; cluster.yaml.crons jobs fire from here, no LaunchAgent.
        self.cron_scheduler.start()
        # py-1.10.27 — Quota prober. Wakes every 60s, probes paused
        # quota keys, unpauses (or extends pause) based on outcome.
        # Initiative `quota-aware-dispatch`.
        self.quota_prober = QuotaProber(self)
        self.quota_prober.start()
        # py-1.12.1 — Periodic CDN poll + idle-aware self-update. Honors
        # cluster.yaml.daemon.auto_update (opt-out) and
        # auto_update_check_interval_sec (default 30 min). Keeps fleets
        # of long-running daemons current without operator action.
        self.version_watcher = VersionWatcher(self)
        self.version_watcher.start()
        # py-1.12.16 — Chat-session reaper. Sweeps every 30 s for slots
        # whose subprocess exited without runner.done.set() (leaving the
        # conv stuck `live: true`) and for slots running past the
        # hard-timeout. Broadcasts conv.activity {live: false} on reap.
        # Initiative: stuck-live recovery (operator field report
        # 2026-06-10, IKA cluster).
        self.chat_session_reaper = ChatSessionReaper(self)
        self.chat_session_reaper.start()
        try:
            self.server.serve_forever(poll_interval=0.5)
        finally:
            try:
                self.cron_scheduler.stop()
            except Exception:
                pass
            try:
                if getattr(self, "quota_prober", None) is not None:
                    self.quota_prober.stop()
            except Exception:
                pass
            try:
                if getattr(self, "chat_session_reaper", None) is not None:
                    self.chat_session_reaper.stop()
            except Exception:
                pass
            self.cleanup()

    def request_shutdown(self) -> None:
        if self.stopping.is_set():
            return
        self.stopping.set()
        # py-1.12.16+: drain in-flight chat sessions BEFORE tearing down
        # the server. Without this, SIGTERM kills the daemon → propagates
        # to every claude-code subprocess → operator's mid-turn work is
        # lost (field report 2026-06-10: 4-minute-old subprocess died
        # mid-thinking when the daemon was killed to deploy py-1.12.16,
        # the user prompt msg_count went up but no assistant reply ever
        # came back).
        try:
            grace_cfg = (
                self.cluster.data.get("daemon")
                if isinstance(self.cluster.data, dict)
                else None
            ) or {}
            grace_secs = int(
                grace_cfg.get("shutdown_grace_secs", self.DEFAULT_SHUTDOWN_GRACE_SECS)
            )
        except Exception:
            grace_secs = self.DEFAULT_SHUTDOWN_GRACE_SECS
        try:
            in_flight = list(self.chat_sessions.list_active())
        except Exception:
            in_flight = []
        if in_flight and grace_secs > 0:
            _log(
                f"shutdown: draining {len(in_flight)} in-flight session(s) "
                f"(grace={grace_secs}s) — {in_flight}"
            )
            _debug_emit(
                "shutdown.drain.start",
                msg=f"draining {len(in_flight)} session(s) with {grace_secs}s grace",
                lvl="warn",
                data={"in_flight": in_flight, "grace_secs": grace_secs},
            )
            try:
                self.hub.broadcast(
                    {
                        "type": "daemon.shutting_down",
                        "ts": _iso_now(),
                        "in_flight": in_flight,
                        "grace_secs": grace_secs,
                    }
                )
            except Exception:
                pass
            deadline = time.time() + grace_secs
            while time.time() < deadline:
                try:
                    still = self.chat_sessions.list_active()
                except Exception:
                    still = []
                if not still:
                    _log("shutdown: all sessions drained, proceeding")
                    _debug_emit(
                        "shutdown.drain.done",
                        msg="all in-flight sessions finished cleanly",
                    )
                    break
                time.sleep(0.5)
            else:
                try:
                    still = self.chat_sessions.list_active()
                except Exception:
                    still = []
                if still:
                    _log(
                        f"shutdown: grace expired with {len(still)} session(s) "
                        f"still active — proceeding (subprocesses will die): {still}"
                    )
                    _debug_emit(
                        "shutdown.drain.timeout",
                        msg=f"{len(still)} session(s) still active after {grace_secs}s",
                        lvl="warn",
                        data={"still_active": still, "grace_secs": grace_secs},
                    )
        _log("shutdown requested — closing clients + server")
        try:
            self.hub.broadcast({"type": "daemon.shutdown", "ts": _iso_now()})
        except Exception:
            pass
        # Let the broadcast flush before tearing down
        time.sleep(0.2)
        self.hub.shutdown()
        self.state_manager.shutdown()
        if self.server is not None:
            threading.Thread(target=self.server.shutdown, daemon=True).start()

    def cleanup(self) -> None:
        try:
            if (
                self.paths.pid_file.exists()
                and self.paths.pid_file.read_text().strip() == str(os.getpid())
            ):
                self.paths.pid_file.unlink()
        except OSError:
            pass
        try:
            if (
                self.paths.port_file.exists()
                and self.paths.port_file.read_text().strip() == str(self.port)
            ):
                self.paths.port_file.unlink()
        except OSError:
            pass

    def _write_runtime(self) -> None:
        self.paths.runtime.mkdir(parents=True, exist_ok=True)
        self.paths.pid_file.write_text(str(os.getpid()))
        self.paths.port_file.write_text(str(self.port))
