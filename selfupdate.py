"""selfupdate.py — periodic VersionWatcher + boot self-update + tls/version helpers.

Extracted from daemon.py (DA-SELFUPDATE-01, daemon-architecture-v2). The
CDN-poll auto-update path: VersionWatcher (background thread that self-
invokes /self-update when a newer DAEMON_VERSION is published) + the boot
self-update + the tls-bundle refresh + version-compare helpers. VersionWatcher
holds a daemon backref and calls daemon.self_update() (which STAYS a Daemon
method). DAEMON_VERSION comes from the constants leaf (no cycle).
"""

from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from constants import DAEMON_VERSION
from paths import Paths
from utils import _debug_emit, _iso_now, _log, parse_simple_yaml

if TYPE_CHECKING:
    from daemon import Daemon  # noqa: F401 — backref type; bundler drops the TYPE_CHECKING block

# Boot self-update tunables (moved with the boot path, DA-SELFUPDATE-01).
_BOOT_PROBE_THROTTLE_SECS = 60  # don't hit the CDN more than 1×/min
_BOOT_PROBE_TIMEOUT_SECS = 4  # boot must never hang waiting on CDN
_BOOT_BACKUPS_TO_KEEP = 3  # daemon.py.bak, .bak.1, .bak.2


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
        return "https://meshkore.com/reference/cluster/scripts/daemon.py"

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


def _is_remote_newer(local: str, remote: str) -> bool:
    """Compare two `py-X.Y.Z` strings. Tolerates suffixes like
    `py-1.12.1-hotfix` — strips after the first non-numeric/dot char
    in the version body for comparison purposes."""

    def _tuple(v: str) -> Tuple[int, ...]:
        body = v[len("py-") :] if v.startswith("py-") else v
        # Stop at the first char that isn't digit or dot.
        clean = re.split(r"[^0-9.]", body, 1)[0]
        return tuple(int(p) for p in clean.split(".") if p.isdigit())

    try:
        return _tuple(remote) > _tuple(local)
    except Exception:
        return False


def _boot_self_update_if_needed(paths: Paths, args: Dict[str, Any]) -> None:
    """Probe `cluster.yaml.daemon.auto_update_source` at boot and replace
    ourselves before the listener opens if the CDN serves a newer
    `DAEMON_VERSION`. py-1.10.22, hardened py-1.10.23. Initiative
    `daemon-boot-self-update`.

    Hardening (py-1.10.23):
      • Throttle: a stamp file at `.meshkore/.runtime/last-boot-update-check`
        carries the wall-clock + outcome of the last probe. Restarting
        the daemon faster than `_BOOT_PROBE_THROTTLE_SECS` skips the
        probe — a crash-restart loop won't DDoS the CDN.
      • Always-log: every restart logs one `boot self-update: <verb>`
        line so the operator can read the boot log and see exactly
        what happened (skip-throttled, skip-disabled, no-update,
        updated, failed).
      • Backup rotation: previous `daemon.py.bak` shifts to `.bak.1`,
        `.bak.1` shifts to `.bak.2`, oldest is dropped. Three rollback
        points protects against "new version regresses but already on
        CDN" scenarios.
      • TLS bundle parallel refresh: when the auto_update_source URL
        ends with `/daemon.py`, we also try `<dir>/tls/{fullchain.pem,
        privkey.pem}` so a daemon that updates also gets the matching
        cert. Falls back gracefully if either file 404s.

    Opt-outs (unchanged):
      • `cluster.yaml.daemon.auto_update: false`         — no auto-update at all.
      • `cluster.yaml.daemon.auto_update_on_boot: false` — only the boot probe is skipped (HTTP /self-update still works).
      • env `MESHKORE_DAEMON_NO_BOOT_UPDATE=1`           — operator/script override.
      • env `MESHKORE_DAEMON_FORCE_BOOT_UPDATE=1`        — bypass the throttle (operator just published a fix and wants every restart to pick it up).
      • env `MESHKORE_DAEMON_SELF_UPDATED=1`             — set by the re-exec'd child to prevent infinite update loops.
    """
    if os.environ.get("MESHKORE_DAEMON_NO_BOOT_UPDATE") == "1":
        _log("boot self-update: skipped (MESHKORE_DAEMON_NO_BOOT_UPDATE=1)")
        return
    if os.environ.get("MESHKORE_DAEMON_SELF_UPDATED") == "1":
        # Post-update child. Confirm + clear the throttle for next time.
        _log(f"boot self-update: re-exec confirmed, now running {DAEMON_VERSION}")
        _boot_update_stamp(paths, outcome="re-exec-confirmed")
        return
    # Tolerant YAML — we don't need a full Cluster object yet.
    cfg: Dict[str, Any] = {}
    try:
        if paths.cluster_yaml.exists():
            cfg = parse_simple_yaml(paths.cluster_yaml.read_text())
    except Exception:
        cfg = {}
    d_block = cfg.get("daemon") if isinstance(cfg, dict) else None
    if not isinstance(d_block, dict):
        d_block = {}
    if d_block.get("auto_update") is False:
        _log("boot self-update: skipped (cluster.yaml.daemon.auto_update=false)")
        return
    if d_block.get("auto_update_on_boot") is False:
        _log(
            "boot self-update: skipped (cluster.yaml.daemon.auto_update_on_boot=false)"
        )
        return
    # Throttle check — protects the CDN from a crash-restart loop.
    force = os.environ.get("MESHKORE_DAEMON_FORCE_BOOT_UPDATE") == "1"
    if not force:
        recent = _boot_update_last_check_age(paths)
        if recent is not None and recent < _BOOT_PROBE_THROTTLE_SECS:
            _log(
                f"boot self-update: skipped (throttled, last check "
                f"{int(recent)}s ago < {_BOOT_PROBE_THROTTLE_SECS}s)"
            )
            return
    url = str(
        d_block.get("auto_update_source")
        or "https://meshkore.com/reference/cluster/scripts/daemon.py"
    ).strip()
    if not url:
        _log("boot self-update: skipped (empty auto_update_source)")
        return
    if not (url.startswith("https://") or url.startswith("http://localhost")):
        _log(f"boot self-update: skipped (rejected URL scheme: {url[:40]!r})")
        return
    import urllib.request
    import ast

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": f"meshcore-py/{DAEMON_VERSION} boot-self-update"},
        )
        with urllib.request.urlopen(req, timeout=_BOOT_PROBE_TIMEOUT_SECS) as r:
            payload = r.read()
    except Exception as e:
        _log(f"boot self-update: skipped (download failed: {e})")
        _boot_update_stamp(paths, outcome=f"download-failed: {e}"[:120])
        return
    if b"DAEMON_VERSION" not in payload:
        _log("boot self-update: skipped (payload lacks DAEMON_VERSION marker)")
        _boot_update_stamp(paths, outcome="no-version-marker")
        return
    try:
        ast.parse(payload)
    except SyntaxError as e:
        _log(f"boot self-update: skipped (syntax error in payload: {e})")
        _boot_update_stamp(paths, outcome=f"syntax-error: {e}"[:120])
        return
    m = re.search(rb'(?m)^DAEMON_VERSION\s*=\s*"([^"]+)"', payload)
    if not m:
        _log("boot self-update: skipped (cannot locate DAEMON_VERSION literal)")
        _boot_update_stamp(paths, outcome="version-literal-not-found")
        return
    new_version = m.group(1).decode("ascii", errors="replace")
    if not _version_is_newer(new_version, DAEMON_VERSION):
        _log(f"boot self-update: no update (CDN={new_version}, local={DAEMON_VERSION})")
        _boot_update_stamp(paths, outcome=f"no-update (cdn={new_version})")
        return
    _log(
        f"boot self-update: CDN serves {new_version}, we are "
        f"{DAEMON_VERSION} — swapping + re-exec"
    )
    scripts_dir = paths.scripts_dir
    scripts_dir.mkdir(parents=True, exist_ok=True)
    current = scripts_dir / "daemon.py"
    new_path = scripts_dir / "daemon.py.new"
    try:
        new_path.write_bytes(payload)
        if current.exists():
            _rotate_daemon_backups(scripts_dir, current)
        new_path.replace(current)
    except Exception as e:
        _log(f"boot self-update: swap failed ({e}) — keeping current version")
        try:
            new_path.unlink()
        except Exception:
            pass
        _boot_update_stamp(paths, outcome=f"swap-failed: {e}"[:120])
        return
    # Best-effort TLS bundle refresh — parity with the HTTP /self-update
    # path. If the CDN serves daemon.py at <base>/scripts/daemon.py we
    # also try <base>/scripts/tls/{fullchain.pem,privkey.pem}.
    if url.endswith("/daemon.py"):
        _refresh_tls_bundle_from_cdn(scripts_dir, url, new_version)
    _boot_update_stamp(paths, outcome=f"updated {DAEMON_VERSION}->{new_version}")
    env = dict(os.environ)
    env["MESHKORE_DAEMON_SELF_UPDATED"] = "1"
    _log(f"boot self-update: re-execing into {new_version}")
    try:
        os.execve(sys.executable, [sys.executable, str(current), *sys.argv[1:]], env)
    except Exception as e:
        _log(
            f"boot self-update: execve failed ({e}) — keep running old in-memory code; next restart picks up new file"
        )
        return


def _boot_update_stamp(paths: Paths, *, outcome: str) -> None:
    """Persist `{ts, outcome, version}` so the throttle check at the
    next boot has something to read. Quiet on I/O errors."""
    try:
        paths.runtime.mkdir(parents=True, exist_ok=True)
        stamp = paths.runtime / "last-boot-update-check"
        stamp.write_text(
            json.dumps(
                {
                    "ts": _iso_now(),
                    "epoch": int(time.time()),
                    "outcome": outcome,
                    "version": DAEMON_VERSION,
                },
                indent=2,
            )
        )
    except OSError:
        pass


def _boot_update_last_check_age(paths: Paths) -> Optional[float]:
    """Seconds since the last boot probe, or None if no stamp exists /
    is unreadable. Caller decides what to do with `None` (we treat it
    as 'no recent check, go ahead and probe')."""
    stamp = paths.runtime / "last-boot-update-check"
    try:
        if not stamp.exists():
            return None
        data = json.loads(stamp.read_text() or "{}")
        epoch = float(data.get("epoch") or 0)
        if epoch <= 0:
            return None
        age = time.time() - epoch
        return max(0.0, age)
    except (OSError, ValueError, TypeError):
        return None


def _rotate_daemon_backups(scripts_dir: "Path", current: "Path") -> None:
    """Shift daemon.py.bak.1 → .bak.2, daemon.py.bak → .bak.1, then
    write the current binary to .bak. Keeps `_BOOT_BACKUPS_TO_KEEP`
    rollback points; oldest gets dropped. Idempotent + tolerant — any
    missing intermediate just skips that shift."""
    import shutil

    for i in range(_BOOT_BACKUPS_TO_KEEP - 1, 0, -1):
        src = scripts_dir / (f"daemon.py.bak.{i - 1}" if i > 1 else "daemon.py.bak")
        dst = scripts_dir / f"daemon.py.bak.{i}"
        try:
            if src.exists():
                src.replace(dst)
        except OSError:
            pass
    try:
        shutil.copy2(current, scripts_dir / "daemon.py.bak")
    except OSError as e:
        _log(f"boot self-update: backup write failed ({e}) — proceeding anyway")


def _refresh_tls_bundle_from_cdn(
    scripts_dir: "Path", daemon_url: str, ver: str
) -> None:
    """Pull `<daemon-dir>/tls/{fullchain.pem,privkey.pem}` to keep the
    cert in lockstep with daemon.py. py-1.8.0 introduced this for the
    HTTP /self-update path; py-1.10.23 mirrors it on boot. Failures
    keep the existing on-disk bundle — never wedges the daemon."""
    import urllib.request

    tls_dir = scripts_dir / "tls"
    tls_dir.mkdir(parents=True, exist_ok=True)
    base_url = daemon_url[: -len("/daemon.py")] + "/tls"
    for fname, mode in (("fullchain.pem", 0o644), ("privkey.pem", 0o600)):
        try:
            treq = urllib.request.Request(
                f"{base_url}/{fname}",
                headers={"User-Agent": f"meshcore-py/{ver} boot-tls-refresh"},
            )
            with urllib.request.urlopen(treq, timeout=_BOOT_PROBE_TIMEOUT_SECS) as r:
                payload = r.read()
            if not payload.startswith(b"-----BEGIN"):
                _log(f"boot self-update: tls/{fname} skipped (not PEM)")
                continue
            target = tls_dir / fname
            target.write_bytes(payload)
            try:
                os.chmod(target, mode)
            except OSError:
                pass
            _log(f"boot self-update: refreshed tls/{fname}")
        except Exception as e:
            _log(f"boot self-update: tls/{fname} refresh skipped ({e})")


def _version_is_newer(a: str, b: str) -> bool:
    """True iff version `a` is strictly newer than `b`. Both look like
    `py-1.10.21`. Compares the dotted tuple after stripping the prefix;
    any non-numeric chunk sorts last (so `py-1.10.21-rc1` < `py-1.10.21`
    is intentional — release wins over pre-release)."""

    def parse(v: str) -> Tuple[int, ...]:
        core = v.strip()
        if core.startswith("py-"):
            core = core[3:]
        # Drop any trailing -suffix
        if "-" in core:
            core = core.split("-", 1)[0]
        out: List[int] = []
        for chunk in core.split("."):
            try:
                out.append(int(chunk))
            except ValueError:
                out.append(-1)  # unknown chunks rank last
        return tuple(out)

    try:
        return parse(a) > parse(b)
    except Exception:
        return False
