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

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from paths import Paths
from utils import _iso_now  # DM7 — real helpers, no more shadow stubs


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

    CACHE_TTL_SECS = 30.0  # py-1.16.1 (D-STOREREPORT-01) — was 5s; a
    # full-tree walk every 5s on a polling cockpit was wasteful. Storage
    # sizes change slowly; 30s is plenty fresh for a capacity panel.

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
        # py-1.16.1 (D-STOREREPORT-01) — os.scandir recursion instead of
        # rglob("*")+stat(): DirEntry carries size, so no extra stat()
        # syscall per file (~halves the IO of this poll-driven walk on
        # large clusters). Combined with the longer cache TTL below.
        total = 0
        files = 0
        stack = [root]
        while stack:
            d = stack.pop()
            try:
                with os.scandir(d) as it:
                    for entry in it:
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                stack.append(Path(entry.path))
                            elif entry.is_file(follow_symlinks=False):
                                total += entry.stat().st_size
                                files += 1
                        except OSError:
                            continue
            except OSError:
                continue
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
