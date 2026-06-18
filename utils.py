"""Shared utilities — time helpers, daemon logger, debug stream,
timeline-file iterators.

Imported by every sibling module that needs ``_log`` / ``_iso_now`` /
``_debug_emit``. Replaces the per-module shadow-stub pattern used in
DM3-DM6: now every module gets the REAL helpers from a single source,
and the bundle's late-binding global lookup keeps working unchanged
(``_log`` resolved from this module's globals, which become part of
the bundle's flat namespace).

Stdlib-only (constraint from ``python-daemon`` initiative). Depends on
``paths.py`` for ``_iter_timeline_files`` only; everything else is
self-contained."""

from __future__ import annotations

import socket  # noqa: F401 — re-exported for callers that prefer `utils.socket`
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

from paths import TLS_BUNDLE_NAME, TLS_CERT_FILENAME, TLS_KEY_FILENAME

# Re-exports so existing `from utils import X` call sites are unchanged after the
# Phase-3d split (time→timeutil, parse+_FM_RE→yamlparse, timeline→timeline). The
# leaves are import-light (timeutil/yamlparse self-contained; timeline→timeutil
# only), so utils→leaf is one-way — no cycle.
from timeutil import _iso_at, _iso_now  # noqa: E402,F401
from yamlparse import _FM_RE, parse_frontmatter, parse_simple_yaml  # noqa: E402,F401
from timeline import (  # noqa: E402,F401
    _append_timeline,
    _iter_timeline_files,
    _read_timeline_file,
)

if TYPE_CHECKING:  # only for annotations; DebugLog lives in debuglog.py
    from debuglog import DebugLog  # noqa: F401


# ── time helpers ──────────────────────────────────────────────────────


# ── debug stream singleton + daemon log ──────────────────────────────
# Module-level singleton so `_log()` and any free function can emit
# without threading a `daemon` ref through every call site. Set by
# Daemon.serve_forever during boot (see daemon.py).

_DEBUG_LOG: Optional["DebugLog"] = None


def set_debug_log(log: Optional["DebugLog"]) -> None:
    """Daemon boot calls this to wire the singleton. Sibling modules
    don't need to know which module owns the global — they just call
    ``debug_enabled()`` or ``_debug_emit(...)``."""
    global _DEBUG_LOG
    _DEBUG_LOG = log


def debug_enabled() -> bool:
    return _DEBUG_LOG is not None


def get_debug_log() -> Optional["DebugLog"]:
    """Live ref for callers that need to call methods on the singleton
    (``DebugLog.tail``, ``DebugLog.emit``). Returns ``None`` when the
    debug stream is disabled."""
    return _DEBUG_LOG


def _log(msg: str) -> None:
    print(f"[meshcore-py {_iso_now()}] {msg}", flush=True)
    # py-1.10.17 — mirror every daemon log line into the debug stream
    # so a single tail covers the unstructured prose + the structured
    # event hooks (architect-wake, chat-dispatch, …) below.
    if _DEBUG_LOG is not None:
        try:
            _DEBUG_LOG.emit(tag="log", lvl="info", msg=msg, src="daemon")
        except Exception:
            # Debug stream failures must never block the program.
            pass


_REDACT_KEYS = {
    "token",
    "authorization",
    "bearer",
    "api_key",
    "apikey",
    "secret",
    "password",
}


def _debug_redact(data: Any) -> Any:
    """Best-effort scrub of token-like values in arbitrary payloads."""
    if isinstance(data, dict):
        out: Dict[str, Any] = {}
        for k, v in data.items():
            if str(k).lower() in _REDACT_KEYS:
                out[k] = "<redacted>"
            else:
                out[k] = _debug_redact(v)
        return out
    if isinstance(data, list):
        return [_debug_redact(x) for x in data]
    if isinstance(data, str) and len(data) > 24 and data.startswith("Bearer "):
        return "Bearer <redacted>"
    return data


def _debug_emit(
    tag: str,
    *,
    msg: str = "",
    lvl: str = "info",
    src: str = "daemon",
    conv: Optional[str] = None,
    agent_id: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
) -> None:
    """Convenience: skip the `if _DEBUG_LOG is not None:` dance at
    every emit site. No-op when the daemon hasn't initialised yet OR
    when the operator opted out via `cluster.yaml.debug.enabled: false`
    (py-1.10.21)."""
    if _DEBUG_LOG is None:
        return
    try:
        _DEBUG_LOG.emit(
            tag=tag,
            msg=msg,
            lvl=lvl,
            src=src,
            conv=conv,
            agent_id=agent_id,
            data=data,
        )
    except Exception:
        pass


def _debug_enabled(cluster: Any) -> bool:
    """Read `cluster.yaml.debug.enabled` (default `True`). Falsy disables
    DebugLog initialisation entirely — no file written, /debug/tail
    returns empty, /debug/log accepts but drops. py-1.10.21. Note the
    default is ON for MeshKore native development; downstream clusters
    that want zero disk footprint flip it to false."""
    try:
        block = cluster.data.get("debug") if isinstance(cluster.data, dict) else None
        if not isinstance(block, dict):
            return True
        v = block.get("enabled")
        if v is None:
            return True
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() not in ("0", "false", "no", "off")
    except Exception:
        return True


# ───────────────────────────────────────────────────────────────────────
# Tiny YAML reader (stdlib has no yaml module — we only need flat scalars)
#
# DM-modularize-2 (py-1.14.4): relocated from daemon.py. parse_simple_yaml
# + parse_frontmatter are pure helpers used by Cluster, every frontmatter
# read, and the prompts module's StateIntegrityChecker. Living in utils
# keeps the layering top-down (prompts.py imports them from here instead
# of reaching back into daemon.py). daemon.py re-exports them so
# `daemon.parse_simple_yaml` stays a stable attribute.


# ───────────────────────────────────────────────────────────────────────
# Timeline append (DM-modularize-2: relocated from daemon.py). Shared by
# ChatRunner (runner.py) and the daemon's own chat/user event writers.


# ───────────────────────────────────────────────────────────────────────
# TLS bundle discovery + in-prompt base URL (DM-modularize-2: relocated
# from daemon.py). `_find_tls_bundle` resolves the (cert, key) sitting
# next to the running file via `Path(__file__).parent` — in the source
# tree that's `daemon/` (same parent as daemon.py), in the bundle it's
# `dist/` (everything inlined into one file). `_daemon_base_url` is used
# by the prompts module to bake endpoint URLs into agent briefings.


def _find_tls_bundle() -> Optional[Tuple[Path, Path]]:
    """Locate (cert, key) next to daemon.py. Returns None if either
    file is missing — daemon then falls back to plain HTTP, so older
    operators who don't have the bundle keep working unchanged."""
    here = Path(__file__).resolve().parent
    cert = here / TLS_BUNDLE_NAME / TLS_CERT_FILENAME
    key = here / TLS_BUNDLE_NAME / TLS_KEY_FILENAME
    if not cert.is_file() or not key.is_file():
        return None
    try:
        cert.read_bytes()
        key.read_bytes()
    except OSError as e:
        _log(f"tls: bundle exists but unreadable ({e}); falling back to HTTP")
        return None
    return cert, key


def _daemon_base_url(port: int) -> str:
    """Authoritative base URL for in-prompt daemon endpoints.

    py-1.10.14 — when the TLS bundle is present the listener wraps its
    socket and plain HTTP returns RST. Subprocess agents that the
    daemon spawns (architect, custom, deploy, …) get their endpoint
    URLs baked into the briefing string; previously those were always
    `http://localhost:<port>`, which silently broke the moment TLS was
    enabled. Now: prefer `https://daemon.meshkore.com:<port>` whenever
    the bundle exists, falling back to plain HTTP only when it's not.
    Same logic as `Daemon.health().endpoint`, kept in sync here because
    the briefing is composed off the request path (no daemon ref)."""
    if _find_tls_bundle() is not None:
        return f"https://daemon.meshkore.com:{port}"
    return f"http://localhost:{port}"
