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
import socket  # noqa: F401 — re-exported for callers that prefer `utils.socket`
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from paths import Paths


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
