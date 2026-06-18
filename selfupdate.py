"""selfupdate.py — periodic VersionWatcher + boot self-update + tls/version helpers.

Extracted from daemon.py (DA-SELFUPDATE-01, daemon-architecture-v2). The
CDN-poll auto-update path: VersionWatcher (background thread that self-
invokes /self-update when a newer DAEMON_VERSION is published) + the boot
self-update + the tls-bundle refresh + version-compare helpers. VersionWatcher
holds a daemon backref and calls daemon.self_update() (which STAYS a Daemon
method). DAEMON_VERSION comes from the constants leaf (no cycle).
"""

from __future__ import annotations

from bootupdate import _is_remote_newer  # noqa: E402

import re
import threading
import time
from typing import TYPE_CHECKING, Optional

from constants import DAEMON_VERSION
from utils import _debug_emit, _iso_now, _log

if TYPE_CHECKING:
    from daemon import Daemon  # noqa: F401 — backref type; bundler drops the TYPE_CHECKING block

# Boot self-update tunables (moved with the boot path, DA-SELFUPDATE-01).


class VersionWatcher:
    """py-1.12.1 — Periodic CDN poll + idle-aware self-update for the
    long-uptime case. See module-level docstring above."""

    DEFAULT_TICK_SECS = 1800  # 30 min
    MIN_TICK_SECS = 60
    MAX_TICK_SECS = 86400
    COOLDOWN_AFTER_ATTEMPT_SECS = 300  # 5 min between attempts

    def __init__(self, daemon: "Daemon") -> None:
        self.daemon = daemon
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_attempt_ts: float = 0.0

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        _log(f"version-watcher: started (tick={self._tick_secs()}s)")

    def stop(self) -> None:
        self._stop.set()

    def _tick_secs(self) -> int:
        try:
            d = (
                self.daemon.cluster.data.get("daemon")
                if isinstance(self.daemon.cluster.data, dict)
                else None
            )
            raw = (d or {}).get("auto_update_check_interval_sec")
            if raw is None:
                return self.DEFAULT_TICK_SECS
            n = int(raw)
            return max(self.MIN_TICK_SECS, min(self.MAX_TICK_SECS, n))
        except Exception:
            return self.DEFAULT_TICK_SECS

    def _enabled(self) -> bool:
        try:
            d = (
                self.daemon.cluster.data.get("daemon")
                if isinstance(self.daemon.cluster.data, dict)
                else None
            )
            return bool((d or {}).get("auto_update", True))
        except Exception:
            return True

    def _source_url(self) -> str:
        try:
            d = (
                self.daemon.cluster.data.get("daemon")
                if isinstance(self.daemon.cluster.data, dict)
                else None
            )
            u = (d or {}).get("auto_update_source")
            if isinstance(u, str) and u.strip():
                return u.strip()
        except Exception:
            pass
        return "https://architect.meshkore.com/reference/cluster/scripts/daemon.py"

    def _loop(self) -> None:
        # Initial small grace so we don't fight the boot self-update if
        # both happen to fire on the same first second.
        if self._stop.wait(60):
            return
        while True:
            # §17 (py-1.14.7) — keep the agent-CLI preamble fresh from the
            # canonical standard. Independent of the auto_update opt-out:
            # refreshing a doc is not a code upgrade. No-op when already
            # current; re-renders the per-CLI files on any change.
            try:
                r = getattr(self.daemon, "instructions_renderer", None)
                if r is not None:
                    r.refresh_from_remote()
                    # py-1.14.8 — detect+surface standard-version drift
                    # (does NOT auto-migrate; surfaced via /health + WS).
                    r.check_standard_drift()
            except Exception as e:
                _log(f"version-watcher: preamble refresh raised: {e}")
            try:
                if self._enabled():
                    self._check_once()
            except Exception as e:
                _log(f"version-watcher: tick raised: {e}")
            if self._stop.wait(self._tick_secs()):
                return

    def _check_once(self) -> None:
        # Cooldown gate.
        now = time.time()
        if now - self._last_attempt_ts < self.COOLDOWN_AFTER_ATTEMPT_SECS:
            return
        remote = self._fetch_remote_version()
        if not remote:
            return
        if not _is_remote_newer(local=DAEMON_VERSION, remote=remote):
            return
        # An upgrade is available. Are we idle?
        active = self.daemon.chat_sessions.list_active()
        if active:
            _log(
                f"version-watcher: upgrade {DAEMON_VERSION} → {remote} available "
                f"but {len(active)} chat session(s) live — deferring"
            )
            _debug_emit(
                "version-watcher.deferred",
                msg=f"upgrade {DAEMON_VERSION} → {remote} deferred ({len(active)} live)",
                lvl="info",
                data={"local": DAEMON_VERSION, "remote": remote, "live_convs": active},
            )
            try:
                self.daemon.hub.broadcast(
                    {
                        "type": "daemon.upgrade.deferred",
                        "local": DAEMON_VERSION,
                        "remote": remote,
                        "live_convs": active,
                        "ts": _iso_now(),
                    }
                )
            except Exception:
                pass
            return
        # Idle — call self_update directly. It re-checks the active set
        # so even if something raced into flight between the check above
        # and the swap, we get a clean 409 (no kill).
        self._last_attempt_ts = now
        _log(f"version-watcher: triggering self_update ({DAEMON_VERSION} → {remote})")
        _debug_emit(
            "version-watcher.upgrade.start",
            msg=f"auto self-update {DAEMON_VERSION} → {remote}",
            lvl="info",
            data={"local": DAEMON_VERSION, "remote": remote},
        )
        try:
            self.daemon.hub.broadcast(
                {
                    "type": "daemon.upgrade.starting",
                    "local": DAEMON_VERSION,
                    "remote": remote,
                    "ts": _iso_now(),
                }
            )
        except Exception:
            pass
        try:
            code, resp = self.daemon.self_update({})
            if code >= 400:
                _log(f"version-watcher: self_update returned {code}: {resp}")
                _debug_emit(
                    "version-watcher.upgrade.failed",
                    msg=f"self_update returned {code}",
                    lvl="warn",
                    data={"code": code, "resp": resp},
                )
        except Exception as e:
            _log(f"version-watcher: self_update raised: {e}")

    # The DAEMON_VERSION line lives ~line 69 of the canonical file —
    # past the module docstring + imports. 8 KB is enough to catch it
    # with room to spare; still <0.1% of the full ~400 KB daemon.py.
    _FETCH_BYTES = 8192

    def _fetch_remote_version(self) -> Optional[str]:
        """HTTP Range-request the head of the source URL and parse the
        DAEMON_VERSION line. Returns None on any failure (network,
        non-200, missing version marker)."""
        url = self._source_url()
        try:
            import urllib.request

            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": f"meshcore-py/{DAEMON_VERSION} version-watcher",
                    "Range": f"bytes=0-{self._FETCH_BYTES - 1}",
                },
            )
            with urllib.request.urlopen(req, timeout=8) as r:
                head = r.read(self._FETCH_BYTES).decode("utf-8", errors="replace")
        except Exception as e:
            _log(f"version-watcher: fetch failed {url}: {e}")
            return None
        m = re.search(r'^DAEMON_VERSION\s*=\s*"([^"]+)"', head, re.MULTILINE)
        if not m:
            return None
        return m.group(1).strip()
