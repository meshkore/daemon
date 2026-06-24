"""bootupdate.py — boot-time self-update + version-compare helpers.

Extracted from selfupdate.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constants import DAEMON_VERSION, RELEASE_PUBKEY_HEX
from crypto_ed25519 import ed25519_verify
from paths import Paths
from utils import _iso_now, _log, parse_simple_yaml

_BOOT_PROBE_THROTTLE_SECS = 60  # don't hit the CDN more than 1×/min


def verify_release_bundle(
    payload: bytes, daemon_url: str, *, timeout: float, ua: str
) -> Optional[str]:
    """Verify a downloaded daemon bundle's detached Ed25519 signature against
    the pinned ``constants.RELEASE_PUBKEY_HEX`` (py-1.27.5). The signature is
    fetched from ``<daemon_url>.sig`` (base64). Returns None when the update
    may proceed (signature valid, OR enforcement disabled by an empty pinned
    key); returns a short reason string when the update MUST be refused
    (signature missing / malformed / not matching). This is what makes
    auto-update safe against a CDN compromise or MITM: an attacker who can't
    sign with the operator's off-CDN private key cannot get a build swapped in.

    Both update paths (boot + HTTP /self-update) call this BEFORE the swap, so
    a refusal simply keeps the current, already-trusted daemon running."""
    pub_hex = (RELEASE_PUBKEY_HEX or "").strip()
    if not pub_hex:
        return None  # enforcement disabled (empty pinned key)
    import base64
    import urllib.request

    sig_url = daemon_url + ".sig"
    try:
        req = urllib.request.Request(sig_url, headers={"User-Agent": ua})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            sig_raw = r.read(4096).decode("ascii", "replace").strip()
    except Exception as e:
        return f"signature unavailable at {sig_url} ({e})"
    try:
        sig = base64.b64decode(sig_raw)
        pub = bytes.fromhex(pub_hex)
    except Exception as e:
        return f"signature/pubkey decode failed ({e})"
    if not ed25519_verify(pub, payload, sig):
        return "signature does not verify against the pinned release key"
    return None


_BOOT_PROBE_TIMEOUT_SECS = 4  # boot must never hang waiting on CDN
_BOOT_BACKUPS_TO_KEEP = 3  # daemon.py.bak, .bak.1, .bak.2


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
        or "https://architect.meshkore.com/reference/cluster/scripts/daemon.py"
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
    # Cryptographic gate (py-1.27.5) — refuse to swap to an unsigned/forged
    # build even if it's "newer". A compromised CDN can't sign with the
    # operator's off-CDN key, so this keeps the trusted daemon running.
    sig_err = verify_release_bundle(
        payload,
        url,
        timeout=_BOOT_PROBE_TIMEOUT_SECS,
        ua=f"meshcore-py/{DAEMON_VERSION} boot-sig",
    )
    if sig_err:
        _log(
            f"boot self-update: REFUSED unsigned/invalid build — {sig_err} (keeping {DAEMON_VERSION})"
        )
        _boot_update_stamp(paths, outcome=f"sig-refused: {sig_err}"[:120])
        return
    _log(
        f"boot self-update: CDN serves {new_version} (signature OK), we are "
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
