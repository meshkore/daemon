"""debuglog.py — DebugLog — the debug.jsonl ring stream.

Extracted from utils.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from timeutil import _iso_now
from utils import _debug_redact


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
        project: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        rec: Dict[str, Any] = {
            "ts": _iso_now(),
            "src": src,
            "lvl": lvl,
            "tag": tag,
            "msg": msg,
        }
        # centralized/multi-project — which project this entry belongs to (from
        # the X-MeshKore-Project header on cockpit posts, or the daemon's
        # request context). Lets the ONE centralized debug stream be filtered
        # per project (GET /debug/tail?project=<id>).
        if project:
            rec["project"] = project
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
