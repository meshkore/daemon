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

import json
import os
import re
import socket  # noqa: F401 — re-exported for callers that prefer `utils.socket`
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from paths import TLS_BUNDLE_NAME, TLS_CERT_FILENAME, TLS_KEY_FILENAME, Paths


# ── time helpers ──────────────────────────────────────────────────────


def _iso_now() -> str:
    return (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.")
        + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"
    )


def _iso_at(epoch_secs: int) -> str:
    """ISO-8601 UTC for a given epoch — used for pause expiry stamps
    (py-1.10.26). Cheap; no ms component (we only care about the
    minute granularity for rate-limit cooldowns)."""
    return datetime.fromtimestamp(epoch_secs, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


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


class DebugLog:
    """Append-only JSONL debug stream.

    Path: `.meshkore/.runtime/debug.jsonl`. Each line is one event:
        {"ts": "...", "src": "daemon|cockpit|agent", "lvl": "...",
         "tag": "...", "conv"?: ..., "agent_id"?: ..., "msg": "...",
         "data"?: { ... }}

    Retention: when the file exceeds `MAX_BYTES`, the writer reads it
    back, keeps only events whose `ts` falls within `RETAIN_SECS` of
    the current instant, and atomically rewrites the file. Worst-case
    trim cost: O(file_size) but bounded by MAX_BYTES.

    Thread-safe. Failures never raise — the daemon must keep running
    even if the log disk is full or read-only."""

    MAX_BYTES = 5 * 1024 * 1024  # 5 MB
    RETAIN_SECS = 30 * 60  # 30 min
    TRIM_CHECK_EVERY = 50  # check size every N appends, not every time

    def __init__(self, path: "Path") -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._writes_since_check = 0
        # Touch the file so subsequent appends never hit ENOENT mid-write.
        if not self.path.exists():
            try:
                self.path.write_text("")
            except OSError:
                pass
        # py-1.10.21 — Trim once on boot. A long-running daemon that
        # writes < TRIM_CHECK_EVERY events between restarts (low-traffic
        # day) was leaving the file with stale head events that
        # predated the rolling window by hours. One trim at startup
        # gives the operator a clean window immediately.
        with self._lock:
            self._maybe_trim_locked()

    def emit(
        self,
        *,
        tag: str,
        msg: str = "",
        lvl: str = "info",
        src: str = "daemon",
        conv: Optional[str] = None,
        agent_id: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        rec: Dict[str, Any] = {
            "ts": _iso_now(),
            "src": src,
            "lvl": lvl,
            "tag": tag,
            "msg": msg,
        }
        if conv:
            rec["conv"] = conv
        if agent_id:
            rec["agent_id"] = agent_id
        if data:
            # Best-effort redaction. Token-like values get masked.
            rec["data"] = _debug_redact(data)
        line = json.dumps(rec, ensure_ascii=False) + "\n"
        with self._lock:
            try:
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write(line)
            except OSError:
                return
            self._writes_since_check += 1
            if self._writes_since_check >= self.TRIM_CHECK_EVERY:
                self._writes_since_check = 0
                self._maybe_trim_locked()

    def _maybe_trim_locked(self) -> None:
        # py-1.10.21 — Trim by EITHER size OR age. The original code
        # only checked size, so on low-traffic days the file kept
        # events from 2-3 hours ago even though the convention says
        # "30 min rolling window". We now also inspect the first line
        # cheaply: if it's older than RETAIN_SECS we know the head is
        # stale and read the full file to rewrite.
        try:
            size = self.path.stat().st_size
        except OSError:
            return
        cutoff = time.time() - self.RETAIN_SECS
        need_trim = size > self.MAX_BYTES
        if not need_trim:
            # Cheap age probe: read just the first non-empty line.
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                            ts_str = str(rec.get("ts") or "")
                            norm = (
                                ts_str[:-1] + "+00:00"
                                if ts_str.endswith("Z")
                                else ts_str
                            )
                            head_ts = datetime.fromisoformat(norm).timestamp()
                            if head_ts < cutoff:
                                need_trim = True
                        except (ValueError, TypeError):
                            pass
                        break
            except OSError:
                return
        if not need_trim:
            return
        try:
            raw = self.path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return
        kept: List[str] = []
        for line in raw.splitlines():
            if not line.strip():
                continue
            ts_str = ""
            try:
                rec = json.loads(line)
                ts_str = rec.get("ts") or ""
            except (ValueError, TypeError):
                continue
            try:
                # ts ends with Z; strptime via fromisoformat after Z→+00:00.
                norm = ts_str[:-1] + "+00:00" if ts_str.endswith("Z") else ts_str
                if datetime.fromisoformat(norm).timestamp() >= cutoff:
                    kept.append(line)
            except (ValueError, TypeError):
                continue
        new_blob = "\n".join(kept) + ("\n" if kept else "")
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            tmp.write_text(new_blob, encoding="utf-8")
            os.replace(tmp, self.path)
        except OSError:
            return

    def tail(
        self,
        *,
        last_secs: int = 300,
        tags: Optional[set[str]] = None,
        min_level: str = "debug",
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Return (events, retained_secs). `retained_secs` is the
        actual age of the oldest event still on disk — useful to detect
        when the operator asked for a window wider than retention."""
        levels = {"debug": 0, "info": 1, "warn": 2, "error": 3}
        min_rank = levels.get(min_level.lower(), 0)
        cutoff = time.time() - max(1, last_secs)
        out: List[Dict[str, Any]] = []
        oldest_ts: Optional[float] = None
        try:
            raw = self.path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return out, 0
        for line in raw.splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except (ValueError, TypeError):
                continue
            ts_str = str(rec.get("ts") or "")
            try:
                norm = ts_str[:-1] + "+00:00" if ts_str.endswith("Z") else ts_str
                ts = datetime.fromisoformat(norm).timestamp()
            except (ValueError, TypeError):
                continue
            if oldest_ts is None or ts < oldest_ts:
                oldest_ts = ts
            if ts < cutoff:
                continue
            if tags and rec.get("tag") not in tags:
                continue
            if levels.get(str(rec.get("lvl") or "info"), 1) < min_rank:
                continue
            out.append(rec)
        retained = int(time.time() - oldest_ts) if oldest_ts else 0
        return out, retained


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


def _iter_timeline_files(paths: "Paths") -> List[Any]:
    """All timeline files (jsonl + jsonl.gz from rotation)."""
    if not paths.timeline_dir.exists():
        return []
    files = list(paths.timeline_dir.glob("*.jsonl"))
    files.extend(paths.timeline_dir.glob("*.jsonl.gz"))
    # Also look in the archive subdir produced by rotation.
    archive_dir = paths.timeline_dir / "archive"
    if archive_dir.exists():
        files.extend(archive_dir.glob("*.jsonl"))
        files.extend(archive_dir.glob("*.jsonl.gz"))
    return files


def _read_timeline_file(path: Any) -> List[Dict[str, Any]]:
    """Parse one timeline file (jsonl or jsonl.gz) → list of events.
    Never raises; bad lines / unreadable files yield empty list."""
    try:
        if str(path).endswith(".gz"):
            import gzip

            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
                lines = fh.read().splitlines()
        else:
            lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return []
    out: List[Dict[str, Any]] = []
    for line in lines:
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


# ───────────────────────────────────────────────────────────────────────
# Tiny YAML reader (stdlib has no yaml module — we only need flat scalars)
#
# DM-modularize-2 (py-1.14.4): relocated from daemon.py. parse_simple_yaml
# + parse_frontmatter are pure helpers used by Cluster, every frontmatter
# read, and the prompts module's StateIntegrityChecker. Living in utils
# keeps the layering top-down (prompts.py imports them from here instead
# of reaching back into daemon.py). daemon.py re-exports them so
# `daemon.parse_simple_yaml` stays a stable attribute.


def parse_simple_yaml(text: str) -> Dict[str, Any]:
    """Parses a YAML subset sufficient for our cluster.yaml + frontmatter
    blocks. Supports scalars, dicts, lists, list-of-dicts, and inline
    list scalars (`tags: [a, b]`). NOT a general YAML parser — fail
    loudly for shapes we don't handle."""
    out: Dict[str, Any] = {}
    # Stack entry: (indent, container, key_in_parent, parent_ref_or_None)
    stack: List[Tuple[int, Any, str, Any]] = [(-1, out, "", None)]
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        raw = lines[i]
        line = raw.rstrip()
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            i += 1
            continue
        indent = len(line) - len(stripped)
        while stack and indent <= stack[-1][0] and len(stack) > 1:
            stack.pop()
        parent = stack[-1][1]

        if stripped.startswith("- "):
            value = stripped[2:].strip()
            # Promote: if the current container is an empty dict that was
            # just created as a nested holder for some key, convert it to
            # a list in the grandparent — we now know the value is a list.
            if isinstance(parent, dict) and not parent:
                key = stack[-1][2]
                gp = stack[-1][3]
                if key and isinstance(gp, dict) and gp.get(key) is parent:
                    new_list: List[Any] = []
                    gp[key] = new_list
                    stack[-1] = (stack[-1][0], new_list, key, gp)
                    parent = new_list
            if isinstance(parent, list):
                # Two shapes:
                #   "- value"               → scalar item
                #   "- key: val\n  key2: …" → dict item (continues below)
                if ":" in value:
                    item: Dict[str, Any] = {}
                    parent.append(item)
                    # Treat the inline "key: val" as the first dict entry
                    k2, _, v2 = value.partition(":")
                    k2 = k2.strip()
                    v2 = v2.strip()
                    if v2:
                        item[k2] = _coerce(_strip_inline_comment(v2))
                        stack.append((indent, item, "", parent))
                    else:
                        # Nested key with no value yet
                        nested: Dict[str, Any] = {}
                        item[k2] = nested
                        stack.append((indent, item, "", parent))
                        stack.append((indent + 2, nested, k2, item))
                else:
                    parent.append(
                        _coerce(_strip_inline_comment(value)) if value else None
                    )

        elif ":" in stripped:
            key, _, val = stripped.partition(":")
            key = key.strip()
            val = _strip_inline_comment(val.strip())
            if val == "":
                nxt: Dict[str, Any] = {}
                if isinstance(parent, dict):
                    parent[key] = nxt
                stack.append((indent, nxt, key, parent))
            elif val.startswith("[") and val.endswith("]"):
                # Inline list scalar: [a, b, "c d"]
                inner = val[1:-1].strip()
                items = (
                    [_coerce(x.strip()) for x in _split_top_level_commas(inner)]
                    if inner
                    else []
                )
                if isinstance(parent, dict):
                    parent[key] = items
            else:
                if isinstance(parent, dict):
                    parent[key] = _coerce(val)
        i += 1
    return out


def _strip_inline_comment(v: str) -> str:
    return re.sub(r"\s+#.*$", "", v)


def _split_top_level_commas(s: str) -> List[str]:
    out, buf, depth, in_str = [], "", 0, None
    for ch in s:
        if in_str:
            buf += ch
            if ch == in_str:
                in_str = None
            continue
        if ch in ('"', "'"):
            in_str = ch
            buf += ch
            continue
        if ch == "," and depth == 0:
            out.append(buf)
            buf = ""
            continue
        if ch in "[{":
            depth += 1
        elif ch in "]}":
            depth -= 1
        buf += ch
    if buf.strip():
        out.append(buf)
    return out


def _coerce(v: str) -> Any:
    s = v.strip()
    if not s:
        return ""
    if (s.startswith('"') and s.endswith('"')) or (
        s.startswith("'") and s.endswith("'")
    ):
        return s[1:-1]
    if s.lower() in ("true", "yes"):
        return True
    if s.lower() in ("false", "no"):
        return False
    if s.lower() in ("null", "~"):
        return None
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    return s


_FM_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


def parse_frontmatter(text: str) -> Dict[str, Any]:
    m = _FM_RE.match(text)
    if not m:
        return {}
    return parse_simple_yaml(m.group(1))


# ───────────────────────────────────────────────────────────────────────
# Timeline append (DM-modularize-2: relocated from daemon.py). Shared by
# ChatRunner (runner.py) and the daemon's own chat/user event writers.


def _append_timeline(paths: "Paths", event: Dict[str, Any]) -> Dict[str, Any]:
    """Append one JSON-line event to today's timeline file.
    Returns the event enriched with `ts` if it wasn't already set.

    py-1.5.0 — atomic append. The line is rendered fully in memory,
    then written + flushed + fsync'd in a single open/close cycle so
    a daemon crash mid-write can't leave a half-written line in the
    jsonl. We rely on the OS guarantee that `write()` is atomic for
    buffers < PIPE_BUF (~4KB on most systems); for larger events
    (very long assistant.final replies) we still get atomicity at the
    page-cache level under POSIX. The added fsync forces durability
    so we don't lose events on a power cut either."""
    paths.timeline_dir.mkdir(parents=True, exist_ok=True)
    if "ts" not in event:
        event = {**event, "ts": _iso_now()}
    date = event["ts"][:10]
    f = paths.timeline_dir / f"{date}.jsonl"
    payload = json.dumps(event, separators=(",", ":")) + "\n"
    encoded = payload.encode("utf-8")
    # Open with O_APPEND so concurrent writers (the StateManager poll
    # loop + ChatRunner reader threads) interleave at line boundaries
    # rather than overwrite each other. O_APPEND is atomic per write()
    # on POSIX for any size up to PIPE_BUF; for larger writes (a multi-
    # KB assistant.final) the worst case is interleaved bytes, but the
    # daemon's writers never race on the same line. Single line per
    # write() call preserves jsonl integrity.
    fd = os.open(f, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, encoded)
        try:
            os.fsync(fd)
        except OSError:
            pass  # best-effort durability
    finally:
        os.close(fd)
    return event


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
