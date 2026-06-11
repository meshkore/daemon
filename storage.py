"""Storage layer — ChatArchive, StorageReport, UploadStore, ChatQueueManager.

All four classes own a slice of ``.meshkore/`` disk state:

* ChatArchive       — ``.runtime/archives.json`` (which convs are archived)
* StorageReport     — disk-usage breakdown of ``.meshkore/`` (5s cache)
* UploadStore       — ``.meshkore/uploads/YYYY-MM-DD/`` chat attachments
* ChatQueueManager  — ``.meshkore/queues/<conv>.json`` per-conv FIFOs

Each takes a ``Paths`` instance via constructor injection; ChatQueueManager
also takes a ``hub`` for ``queue.item.*`` broadcasts. None of these classes
spawn threads; the locks protect against concurrent HTTP request handlers
mutating the same on-disk file.

Bundler note: when ``daemon/bundle.py`` concatenates this module into
``dist/daemon.py``, this file's local ``_log`` / ``_iso_now`` are shadowed
by the daemon.py definitions that come later in the bundle — production
gets the full debug-stream-aware versions. Source-tree dev runs against
these local copies (simpler; no debug-stream visibility for storage ops,
which is fine).
"""

from __future__ import annotations

import base64
import json
import os
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from paths import Paths
from utils import _iso_now, _log  # DM7 — real helpers, no more shadow stubs


# ───────────────────────────────────────────────────────────────────────
# ChatArchive (py-1.5.0)
#
# Until v1.5.0 the cockpit kept the "is this conversation archived?" bit
# in localStorage. That meant a different browser (same operator, same
# project) saw all previously-archived convs as live again. The archive
# state is now daemon-side, persisted to `.meshkore/.runtime/archives.json`,
# so it's a single source of truth across cockpit instances on the same
# machine. The cockpit syncs from `/chat/archives` on boot and POSTs to
# `/chat/archive` / `/chat/unarchive` on toggle.
#
# Schema:
#   {
#     "version": 1,
#     "archived": {
#       "<conv-id>": {"archived_at": "<iso>", "by": "<author>"}
#     }
#   }


class ChatArchive:
    """Persistent registry of archived conv ids.
    Read on boot; mutated via /chat/archive + /chat/unarchive endpoints."""

    SCHEMA_VERSION = 1

    def __init__(self, paths: "Paths") -> None:
        self.paths = paths
        self._path = paths.runtime / "archives.json"
        self._lock = threading.Lock()
        self._data: Dict[str, Any] = {"version": self.SCHEMA_VERSION, "archived": {}}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text())
            if isinstance(raw, dict) and isinstance(raw.get("archived"), dict):
                self._data = raw
                self._data.setdefault("version", self.SCHEMA_VERSION)
        except Exception:
            # corrupted file → keep defaults; never crash the daemon
            pass

    def _save(self) -> None:
        # Atomic write — render to a temp file in the same dir, fsync,
        # then rename. Survives a daemon crash mid-write.
        self.paths.runtime.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".json.tmp")
        payload = json.dumps(self._data, indent=2).encode("utf-8")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            os.write(fd, payload)
            try:
                os.fsync(fd)
            except OSError:
                pass
        finally:
            os.close(fd)
        os.replace(tmp, self._path)

    def list(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {"conv": c, **meta}
                for c, meta in sorted(self._data["archived"].items())
            ]

    def is_archived(self, conv: str) -> bool:
        with self._lock:
            return conv in self._data["archived"]

    def archive(self, conv: str, by: str = "") -> Dict[str, Any]:
        with self._lock:
            entry = {"archived_at": _iso_now(), "by": by or "operator"}
            self._data["archived"][conv] = entry
            self._save()
            return {"conv": conv, **entry}

    def unarchive(self, conv: str) -> bool:
        with self._lock:
            if conv in self._data["archived"]:
                del self._data["archived"][conv]
                self._save()
                return True
            return False


class StorageReport:
    """py-1.12.22 — Disk usage report for `.meshkore/`.

    Walks the well-known top-level subtrees + sums their byte size and
    file count so the cockpit can render a storage panel. Cached for
    `CACHE_TTL_SECS` to avoid re-walking large trees on every poll.

    Buckets reported:
        log, snapshots, uploads, queues, timeline, agents, modules,
        docs, roadmap, .runtime, credentials.

    Each entry: `{path, bytes, files, exists, retention_days?}`.
    Unknown / empty subtrees report `bytes:0, files:0, exists:false`
    so the cockpit can render a stable row regardless of state.

    Initiative tracked in `.meshkore/log/<today>.md`. Standard v22+
    documents the endpoint as the canonical surface for operator-side
    capacity reporting."""

    CACHE_TTL_SECS = 5.0  # cheap-ish recompute; long enough that a
    # rapid poll cluster doesn't burn IO.

    # Each entry: (logical-name, attribute on Paths). The order is the
    # order the cockpit will render them in.
    _BUCKETS: List[Tuple[str, str]] = [
        ("log", "log_dir"),
        ("snapshots", "snapshots_dir"),
        ("uploads", "uploads_dir"),
        ("queues", "queues_dir"),
        ("timeline", "timeline_dir"),
        ("agents", "agents_dir"),
        ("modules", "modules_dir"),
        ("docs", "docs_dir"),
        ("roadmap", "roadmap_dir"),
        (".runtime", "runtime"),
        ("credentials", "credentials"),
    ]

    def __init__(self, paths: Paths, cluster: Any) -> None:
        self.paths = paths
        self.cluster = cluster
        self._lock = threading.Lock()
        self._cached_at: float = 0.0
        self._cache: Dict[str, Any] = {}

    def _retention_for(self, bucket: str) -> Optional[int]:
        """Pull the retention policy (in days) for a bucket from
        cluster.yaml, when one applies. Returns None when the bucket
        has no retention semantics."""
        try:
            data = self.cluster.data if isinstance(self.cluster.data, dict) else {}
        except Exception:
            return None
        cfg_key = {
            "snapshots": "snapshots",
            "uploads": "uploads",
        }.get(bucket)
        if cfg_key is None:
            return None
        try:
            cfg = data.get(cfg_key) if isinstance(data.get(cfg_key), dict) else None
            if cfg is None:
                # Fall through to the per-feature default.
                return 7 if bucket == "snapshots" else 30
            return int(cfg.get("retention_days", 7 if bucket == "snapshots" else 30))
        except Exception:
            return 7 if bucket == "snapshots" else 30

    def _walk_dir(self, root: Path) -> Tuple[int, int]:
        """Return (total_bytes, file_count) for everything under `root`.
        Missing directory returns (0, 0); permission errors are absorbed
        so one unreadable subtree doesn't poison the whole report."""
        if not root.exists() or not root.is_dir():
            return 0, 0
        total = 0
        files = 0
        try:
            for entry in root.rglob("*"):
                try:
                    if entry.is_file():
                        total += entry.stat().st_size
                        files += 1
                except (OSError, PermissionError):
                    continue
        except (OSError, PermissionError):
            pass
        return total, files

    def usage(self) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            if self._cache and now - self._cached_at < self.CACHE_TTL_SECS:
                return self._cache
        buckets: List[Dict[str, Any]] = []
        total = 0
        total_files = 0
        for name, attr in self._BUCKETS:
            root = getattr(self.paths, attr, None)
            if not isinstance(root, Path):
                buckets.append(
                    {"name": name, "bytes": 0, "files": 0, "exists": False},
                )
                continue
            b, f = self._walk_dir(root)
            entry: Dict[str, Any] = {
                "name": name,
                "bytes": b,
                "files": f,
                "exists": root.exists(),
            }
            ret = self._retention_for(name)
            if ret is not None:
                entry["retention_days"] = ret
            buckets.append(entry)
            total += b
            total_files += f
        report: Dict[str, Any] = {
            "root": ".meshkore/",
            "total_bytes": total,
            "total_files": total_files,
            "buckets": buckets,
            "generated_at": _iso_now(),
            "cache_ttl_secs": self.CACHE_TTL_SECS,
        }
        with self._lock:
            self._cache = report
            self._cached_at = time.time()
        return report


class UploadStore:
    """py-1.12.21 — chat attachment persistence.

    Stores image / binary attachments sent with /chat/dispatch under
    `.meshkore/uploads/<YYYY-MM-DD>/<filename>`. Returns a small
    manifest record that the daemon embeds in the matching chat.user
    timeline event, so the cockpit can render thumbnails on next
    hydrate. Retention is bounded by
    `cluster.yaml.uploads.retention_days` (default 30); a sweep runs
    opportunistically on every save.

    File-name shape:
        `<conv-slug>-<ms-ts>-<idx>-<rand4>.<ext>`
    Lexicographic ordering matches chronology + idx, and the random
    suffix avoids collisions when two uploads land in the same ms.
    """

    DEFAULT_RETENTION_DAYS = 30
    MAX_BYTES_PER_FILE = 8 * 1024 * 1024  # 8 MB, claude-code's friendly upper bound
    MAX_FILES_PER_DISPATCH = 12

    # Media-type → file extension. Anything outside this map gets
    # `.bin`, which the cockpit can still link / download but won't
    # render as <img>.
    _EXT_BY_MEDIA: Dict[str, str] = {
        "image/png": "png",
        "image/jpeg": "jpg",
        "image/jpg": "jpg",
        "image/gif": "gif",
        "image/webp": "webp",
        "image/svg+xml": "svg",
        "image/avif": "avif",
        "image/bmp": "bmp",
    }

    def __init__(self, paths: Paths, cluster: Any) -> None:
        self.paths = paths
        self.cluster = cluster

    def _retention_days(self) -> int:
        try:
            data = self.cluster.data if isinstance(self.cluster.data, dict) else {}
            cfg = data.get("uploads") if isinstance(data.get("uploads"), dict) else None
            if cfg is None:
                return self.DEFAULT_RETENTION_DAYS
            n = int(cfg.get("retention_days", self.DEFAULT_RETENTION_DAYS))
            return max(0, min(365, n))
        except Exception:
            return self.DEFAULT_RETENTION_DAYS

    def _safe_slug(self, s: str) -> str:
        out = []
        for c in s:
            if c.isalnum() or c in "-_":
                out.append(c)
            else:
                out.append("_")
        return ("".join(out) or "x")[:48]

    def save_dispatch(
        self,
        *,
        conv: str,
        images: Optional[List[Dict[str, Any]]],
        ts_iso: str,
    ) -> List[Dict[str, Any]]:
        """Persist the dispatch's images list. Returns a manifest list
        ready to be embedded in the chat.user event. Each entry:

            {
              "kind": "image",
              "media_type": "image/png",
              "url": "/chat/uploads/2026-06-10/<file>",
              "size_bytes": 12345,
              "filename": "<file>"
            }

        Silently skips entries that fail validation; never raises."""
        if not images:
            return []
        try:
            self._gc_old()
        except Exception as e:
            _log(f"upload gc failed: {e}")
        out: List[Dict[str, Any]] = []
        # Daily bucket — yyyy-mm-dd, gitignored.
        bucket = ts_iso[:10] if len(ts_iso) >= 10 else _iso_now()[:10]
        bucket_dir = self.paths.uploads_dir / bucket
        bucket_dir.mkdir(parents=True, exist_ok=True)
        # Single millisecond timestamp shared across this dispatch so
        # the operator's batch sorts together in `ls -lt`.
        ms = int(time.time() * 1000)
        conv_slug = self._safe_slug(conv)
        for idx, img in enumerate(images[: self.MAX_FILES_PER_DISPATCH]):
            if not isinstance(img, dict):
                continue
            media_type = str(img.get("media_type") or "image/png").lower()
            data_b64 = img.get("data")
            if not isinstance(data_b64, str) or not data_b64:
                continue
            try:
                blob = base64.b64decode(data_b64, validate=True)
            except Exception:
                continue
            if len(blob) > self.MAX_BYTES_PER_FILE or len(blob) == 0:
                continue
            ext = self._EXT_BY_MEDIA.get(media_type, "bin")
            rand4 = secrets.token_hex(2)
            fname = f"{conv_slug}-{ms}-{idx}-{rand4}.{ext}"
            path = bucket_dir / fname
            try:
                path.write_bytes(blob)
            except OSError as e:
                _log(f"upload save failed for {fname}: {e}")
                continue
            out.append(
                {
                    "kind": "image",
                    "media_type": media_type,
                    "url": f"/chat/uploads/{bucket}/{fname}",
                    "size_bytes": len(blob),
                    "filename": fname,
                }
            )
        return out

    def serve_path(self, bucket: str, filename: str) -> Optional[Path]:
        """Resolve `<uploads>/<bucket>/<filename>` if it's safe to
        serve. Returns None on traversal attempts or missing file."""
        if (
            not bucket
            or not filename
            or ".." in bucket
            or ".." in filename
            or "/" in filename
            or "\\" in filename
            or "/" in bucket
            or "\\" in bucket
        ):
            return None
        # bucket should be YYYY-MM-DD shaped.
        if len(bucket) != 10 or bucket[4] != "-" or bucket[7] != "-":
            return None
        path = (self.paths.uploads_dir / bucket / filename).resolve()
        try:
            path.relative_to(self.paths.uploads_dir.resolve())
        except ValueError:
            return None
        if not path.is_file():
            return None
        return path

    def _gc_old(self) -> None:
        days = self._retention_days()
        if days <= 0:
            return
        root = self.paths.uploads_dir
        if not root.is_dir():
            return
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
            "%Y-%m-%d",
        )
        for entry in root.iterdir():
            if not entry.is_dir():
                continue
            name = entry.name
            # Only sweep YYYY-MM-DD-shaped buckets.
            if len(name) != 10 or name[4] != "-" or name[7] != "-":
                continue
            if name < cutoff:
                try:
                    import shutil as _shutil

                    _shutil.rmtree(entry, ignore_errors=True)
                except Exception as e:
                    _log(f"upload gc rmtree({entry}) failed: {e}")


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
            p.write_text(
                json.dumps(
                    {"conv": conv, "items": items, "updated_at": _iso_now()},
                    indent=2,
                ),
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
