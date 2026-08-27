"""selfupdatesvc.py — extracted from daemon.py (daemon-architecture-v2 Phase 3d).

SelfUpdateMixin: methods moved VERBATIM out of Daemon; Daemon inherits both so
every self.* resolves on the combined instance -> byte-identical."""

from __future__ import annotations

import os
import threading
from typing import Any, Dict, Tuple

from pathlib import Path

from bootupdate import (
    _refresh_tls_bundle_from_cdn,
    _rotate_daemon_backups,
    verify_release_bundle,
)
from constants import DAEMON_VERSION
from nethttp import FetchError, fetch_bytes
from utils import _iso_now


class SelfUpdateMixin:
    def self_update(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        """Download a new daemon.py, validate it, swap it in, spawn the
        replacement on a free port, and schedule our own shutdown.
        The cockpit reconnects to the new port via re-discovery (same
        cluster_id, dedupe collapses the rail).

        Refused (409) while any chat turn is mid-stream — killing the
        daemon kills its claude-code children. The cockpit can cancel
        the conv first and retry.

        Network/syntax failures keep the running daemon untouched —
        the new download lands at daemon.py.new and is only swapped
        in after ast.parse() accepts it.
        """
        # 1. Refuse if any chat turn is active.
        active = self.chat_sessions.list_active()
        if active:
            return 409, {
                "error": "chat turn in progress",
                "convs": active,
                "hint": "POST /chat/cancel for each conv first, then retry.",
            }
        # 2. Resolve the download source. cluster.yaml takes precedence
        #    over the optional `url` in the body — operator config wins.
        cfg_src = None
        try:
            d = (
                self.cluster.data.get("daemon")
                if isinstance(self.cluster.data, dict)
                else None
            )
            if isinstance(d, dict):
                cfg_src = d.get("auto_update_source")
        except Exception:
            cfg_src = None
        url = (
            (isinstance(cfg_src, str) and cfg_src.strip())
            or str(body.get("url") or "").strip()
            or "https://architect.meshkore.com/reference/cluster/scripts/daemon.py"
        )
        if not (url.startswith("https://") or url.startswith("http://localhost")):
            return 400, {
                "error": "auto_update_source must be HTTPS (or http://localhost for testing)",
                "url": url,
            }
        # 3. Download to .new.
        import ast
        import sys
        import subprocess as _sp

        # DAH2 — resolve the daemon's OWN install directory, NOT the request
        # project's `.meshkore/scripts/`.
        #
        # This is a MACHINE-level operation: there is one daemon per machine
        # (Standard v28) and it must replace the binary IT IS RUNNING. But
        # `self.paths` is the DC-4 per-request accessor, resolved from the
        # `X-MeshKore-Project` header — so the update landed in whichever
        # project the caller happened to name. Observed live during the
        # py-1.34.0 release (2026-08-27): a `/self-update` sent with
        # `X-MeshKore-Project: meshkore-main` downloaded the new bundle into
        # `meshkore/.meshkore/scripts/`, backed up THAT project's stale
        # leftover (py-1.30.3) as the "rollback point" instead of the running
        # py-1.33.0, re-exec'd from there, and left the real install dir in
        # `meshkore-server/` orphaned one version behind. Three consequences:
        # the rollback path pointed at the wrong bytes, the daemon's install
        # location wandered between projects on every update, and a member
        # project acquired daemon code the centralized model says it must not
        # have.
        #
        # `sys.argv[0]` is the script this process was launched with, which is
        # exactly the file that must be swapped. `self.paths` stays correct for
        # the per-project bits (the response's relative path, the child cwd).
        scripts_dir = Path(sys.argv[0]).resolve().parent
        scripts_dir.mkdir(parents=True, exist_ok=True)
        new_path = scripts_dir / "daemon.py.new"
        try:
            payload = fetch_bytes(
                url, label="self-update", version=DAEMON_VERSION, timeout=10
            )
            new_path.write_bytes(payload)
        except (FetchError, OSError) as e:
            try:
                new_path.unlink()
            except Exception:
                pass
            return 500, {"error": "download failed", "url": url, "detail": str(e)}
        # 4. Syntax-check before swapping. Rejects HTML 404 pages,
        #    partial downloads, accidental binary content.
        try:
            ast.parse(payload)
        except SyntaxError as e:
            try:
                new_path.unlink()
            except Exception:
                pass
            return 500, {
                "error": "syntax check failed on downloaded daemon.py — running daemon untouched",
                "url": url,
                "detail": str(e),
            }
        # Quick sanity: must declare DAEMON_VERSION somewhere.
        if b"DAEMON_VERSION" not in payload:
            try:
                new_path.unlink()
            except Exception:
                pass
            return 500, {
                "error": "download does not look like a MeshKore daemon (no DAEMON_VERSION marker)",
                "url": url,
            }
        # 4.5. Cryptographic gate (py-1.27.5) — verify the detached Ed25519
        #      signature against the pinned release key before swapping. A
        #      CDN compromise / MITM that can't sign with the operator's
        #      off-CDN private key is refused here; the running daemon is
        #      left untouched.
        sig_err = verify_release_bundle(
            payload,
            url,
            timeout=10,
            ua=f"meshcore-py/{DAEMON_VERSION} self-update-sig",
        )
        if sig_err:
            try:
                new_path.unlink()
            except Exception:
                pass
            return 500, {
                "error": "release signature verification failed — running daemon untouched",
                "detail": sig_err,
                "url": url,
            }
        # 5. Backup current binary so the operator can roll back.
        #    DAH1 — this path used to keep exactly ONE rollback point while the
        #    boot path kept three (`_rotate_daemon_backups`). Two update routes
        #    to the same file had two different safety nets for no reason; both
        #    now rotate .bak → .bak.1 → .bak.2.
        current = Path(sys.argv[0]).resolve()
        backup = scripts_dir / "daemon.py.bak"  # newest rollback point
        try:
            if current.exists():
                _rotate_daemon_backups(scripts_dir, current)
        except Exception as e:
            return 500, {"error": "backup failed — refusing to swap", "detail": str(e)}
        # 6. Atomic rename .new → daemon.py.
        try:
            new_path.replace(current)
        except Exception as e:
            return 500, {"error": "rename failed", "detail": str(e)}
        # 6.5. py-1.8.0 — also refresh the bundled TLS cert if the
        #      published source serves one alongside daemon.py.
        #      Without this the new daemon comes up as plain HTTP
        #      while the cockpit still expects HTTPS, and the
        #      switch-to-new-port handshake fails. Best-effort: if
        #      either file 404s, we keep the existing tls/ bundle.
        #      DAH1 — was an inline duplicate of bootupdate's
        #      `_refresh_tls_bundle_from_cdn`; now the one implementation
        #      serves both update routes.
        if url.startswith("https://") and url.endswith("/daemon.py"):
            _refresh_tls_bundle_from_cdn(
                scripts_dir,
                url,
                DAEMON_VERSION,
                label="self-update",
                timeout=10,
            )
        # 7. Spawn the replacement on the SAME port (py-1.14.3).
        #    Previously we picked a NEW free port and let the cockpit
        #    re-discover the daemon — fragile (port hunting, WS fatal,
        #    operator-visible "taking longer than usual"). Now the new
        #    process is told to WAIT for OUR port to free
        #    (MESHKORE_REEXEC_WAIT_PORT=1 → serve_forever retries the
        #    bind for ~12 s). We release the socket by exiting promptly;
        #    the new daemon binds the identical port and the cockpit's
        #    WS just reconnects to the same URL — zero operator action,
        #    no port change, no front-end reload.
        new_port = self.port
        child_env = {**os.environ, "MESHKORE_REEXEC_WAIT_PORT": "1"}
        try:
            proc = _sp.Popen(
                [sys.executable, str(current), "--port", str(new_port)],
                cwd=str(self.paths.root),
                env=child_env,
                stdin=_sp.DEVNULL,
                stdout=_sp.DEVNULL,
                stderr=_sp.DEVNULL,
                start_new_session=True,  # detach from our process group
            )
        except Exception as e:
            return 500, {"error": "failed to spawn new daemon", "detail": str(e)}
        # 8. Release our socket + exit promptly so the child's bind-retry
        #    succeeds fast. A short delay lets the 202 response flush and
        #    the handoff broadcast reach connected cockpits first.
        SHUTDOWN_DELAY = 0.6

        def _self_kill():
            try:
                self.hub.broadcast(
                    {
                        "type": "daemon.self_update.handing_off",
                        "new_pid": proc.pid,
                        "new_port": new_port,
                        "same_port": True,
                        "ts": _iso_now(),
                    }
                )
            except Exception:
                pass
            # Close the listen socket explicitly before exit so the OS
            # frees the port immediately for the child's retry (don't
            # wait for os._exit's implicit FD reclaim under load).
            try:
                if self.server is not None:
                    self.server.server_close()
            except Exception:
                pass
            os._exit(0)

        threading.Timer(SHUTDOWN_DELAY, _self_kill).start()
        return 202, {
            "ok": True,
            "new_pid": proc.pid,
            "new_port": new_port,
            "same_port": True,
            "shutdown_in_sec": SHUTDOWN_DELAY,
            # Absolute: `backup` now lives in the daemon's install dir, which
            # is not necessarily under the request project's root.
            "old_backup": str(backup) if backup.exists() else None,
            "install_dir": str(scripts_dir),
            "old_version": DAEMON_VERSION,
            "source_url": url,
        }
