"""chatqueue.py — ChatQueueManager — Standard v16 disk-backed chat-turn queue.

Extracted from storage.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

from fsatomic import atomic_write_json

import json
import secrets
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from paths import Paths
from utils import _iso_now, _log


class ChatQueueManager:
    """Standard v16 chat-turn queue — per-conv FIFO at
    `.meshkore/queues/<conv>.json`. Survives daemon restarts; auto-flushes
    the head item after each chat turn finishes.

    Five HTTP routes (GET / POST / edit / move-or-promote / DELETE), all
    serialised through a single lock so the disk view stays consistent
    with the in-memory view. Auto-flush is invoked by the daemon after
    `ChatRunner._read_stream` finalises — pops head + spawns next turn.

    Operator field report 2026-06-10: the cockpit was calling
    `POST /chat/conv/<id>/queue` for explicit enqueue and getting 404s
    because these endpoints were never actually shipped despite the
    py-1.12.12 release note. This class + the routes close that gap."""

    def __init__(self, paths: Paths, hub) -> None:
        self.paths = paths
        self.hub = hub
        self._lock = threading.Lock()

    def _path(self, conv: str) -> Path:
        # The queue file name uses the conv id verbatim; conv slugs are
        # ASCII-safe by convention. Sanitise just in case.
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in conv)
        return self.paths.queues_dir / f"{safe}.json"

    def _read(self, conv: str) -> List[Dict[str, Any]]:
        p = self._path(conv)
        if not p.exists():
            return []
        try:
            data = json.loads(p.read_text())
            items = data.get("items")
            return list(items) if isinstance(items, list) else []
        except (OSError, json.JSONDecodeError):
            return []

    def _write(self, conv: str, items: List[Dict[str, Any]]) -> None:
        p = self._path(conv)
        if items:
            self.paths.queues_dir.mkdir(parents=True, exist_ok=True)
            # py-1.16.0 (D-STORE-ATOMIC-01) — atomic tmp+replace, matching
            # ChatArchive/RunStore/conv_meta. A plain write_text could be
            # interrupted mid-write → a half-file that `_read` silently
            # parses to [] (queued turns vanished). os.replace is atomic.
            atomic_write_json(
                p,
                {"conv": conv, "items": items, "updated_at": _iso_now()},
                fsync=True,
            )
        else:
            try:
                p.unlink()
            except FileNotFoundError:
                pass

    def _broadcast(
        self, ev_type: str, conv: str, item: Optional[Dict[str, Any]]
    ) -> None:
        try:
            self.hub.broadcast(
                {
                    "type": ev_type,
                    "conv": conv,
                    "item": item,
                    "ts": _iso_now(),
                },
            )
        except Exception as e:
            _log(f"queue broadcast {ev_type} failed for {conv}: {e}")

    def list(self, conv: str) -> List[Dict[str, Any]]:
        with self._lock:
            return self._read(conv)

    def conv_ids(self) -> List[str]:
        """py-1.14.6 — Every conv id with a non-empty queue file on disk.
        Powers the daemon's idle-flush sweep (boot + reaper tick), which
        resumes queues stranded when no turn-completion fired the on_idle
        hook — e.g. after a self-update re-exec wiped the in-memory
        session + its _wait thread, or a session was abnormally reaped.
        Reads the canonical `conv` field from each file rather than
        un-sanitising the filename."""
        out: List[str] = []
        with self._lock:
            qdir = self.paths.queues_dir
            if not qdir.exists():
                return out
            for p in sorted(qdir.glob("*.json")):
                try:
                    data = json.loads(p.read_text())
                except (OSError, json.JSONDecodeError):
                    continue
                conv = data.get("conv")
                items = data.get("items")
                if isinstance(conv, str) and isinstance(items, list) and items:
                    out.append(conv)
        return out

    def enqueue(self, conv: str, text: str) -> Dict[str, Any]:
        text = (text or "").strip()
        if not text:
            raise ValueError("text required")
        with self._lock:
            items = self._read(conv)
            item = {
                "id": secrets.token_hex(6),
                "text": text,
                "position": len(items),
                "created_at": _iso_now(),
                "updated_at": _iso_now(),
            }
            items.append(item)
            self._write(conv, items)
        self._broadcast("queue.item.added", conv, item)
        return item

    def edit(self, conv: str, item_id: str, text: str) -> Optional[Dict[str, Any]]:
        text = (text or "").strip()
        if not text:
            return None
        with self._lock:
            items = self._read(conv)
            for it in items:
                if it.get("id") == item_id:
                    it["text"] = text
                    it["updated_at"] = _iso_now()
                    self._write(conv, items)
                    self._broadcast("queue.item.updated", conv, it)
                    return it
        return None

    def remove(self, conv: str, item_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            items = self._read(conv)
            for i, it in enumerate(items):
                if it.get("id") == item_id:
                    removed = items.pop(i)
                    for j, x in enumerate(items):
                        x["position"] = j
                    self._write(conv, items)
                    self._broadcast("queue.item.removed", conv, removed)
                    return removed
        return None

    def move(self, conv: str, item_id: str, position: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            items = self._read(conv)
            idx = next(
                (i for i, x in enumerate(items) if x.get("id") == item_id),
                -1,
            )
            if idx < 0:
                return None
            it = items.pop(idx)
            position = max(0, min(position, len(items)))
            items.insert(position, it)
            for j, x in enumerate(items):
                x["position"] = j
            self._write(conv, items)
        self._broadcast("queue.item.updated", conv, it)
        return it

    def promote(self, conv: str, item_id: str) -> Optional[Dict[str, Any]]:
        """Convenience — move the item to position 0 (head)."""
        return self.move(conv, item_id, 0)

    def pop_head(self, conv: str) -> Optional[Dict[str, Any]]:
        """Pop the head item and broadcast `queue.item.sent`. Used by the
        auto-flush hook after a turn finishes."""
        with self._lock:
            items = self._read(conv)
            if not items:
                return None
            head = items.pop(0)
            for j, x in enumerate(items):
                x["position"] = j
            self._write(conv, items)
        self._broadcast("queue.item.sent", conv, head)
        return head
