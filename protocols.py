"""protocols.py — ProtocolsRegistry — .meshkore/protocols runbooks.

Extracted from registries.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

import hashlib
import struct
from hub import Hub

import threading
from typing import Any, Dict, List, Optional

from paths import Paths
import re

from registries import _split_frontmatter

_PROTOCOL_FILE_RE = re.compile(r"^P(\d+)-[a-z0-9-]+\.md$")
_PROTOCOL_LOG_RE = re.compile(r"^(P\d+)-(\d{4}-\d{2}-\d{2})-[a-z0-9-]+\.md$")


class ProtocolsRegistry:
    """Loads + watches .meshkore/protocols/; broadcasts on change."""

    POLL_SEC = 3.0

    def __init__(self, paths: Paths, hub: "Hub"):
        self.paths = paths
        self.hub = hub
        # Each entry: { id, title, scope, status, updated, file, log_count }
        self.protocols: List[Dict[str, Any]] = []
        self._sig: str = ""
        self._stop = threading.Event()
        self.reload(broadcast=False)
        threading.Thread(target=self._watch_loop, daemon=True).start()

    def _watch_loop(self) -> None:
        while not self._stop.wait(self.POLL_SEC):
            try:
                self.reload(broadcast=True)
            except Exception:
                pass

    def shutdown(self) -> None:
        self._stop.set()

    def reload(self, broadcast: bool = True) -> bool:
        sig = self._compute_sig()
        if sig == self._sig and self.protocols:
            return False
        self._sig = sig
        out: List[Dict[str, Any]] = []
        if self.paths.protocols_dir.exists():
            for fp in sorted(self.paths.protocols_dir.glob("P*-*.md")):
                m = _PROTOCOL_FILE_RE.match(fp.name)
                if not m:
                    continue
                try:
                    text = fp.read_text()
                except OSError:
                    continue
                fm, _body = _split_frontmatter(text)
                pid = str(fm.get("id") or f"P{m.group(1)}")
                entry = {
                    "id": pid,
                    "title": str(fm.get("title") or pid),
                    "scope": str(fm.get("scope") or "cluster"),
                    "status": str(fm.get("status") or "draft"),
                    "priority": str(fm.get("priority") or "medium"),
                    "owner": str(fm.get("owner") or ""),
                    "updated": str(fm.get("updated") or ""),
                    "tags": fm.get("tags") or [],
                    "file": fp.name,
                    "log_count": self._count_logs(pid),
                }
                out.append(entry)
        self.protocols = out
        if broadcast:
            self.hub.broadcast(
                {
                    "type": "protocols.updated",
                    "ids": [p["id"] for p in out],
                }
            )
        return True

    def _compute_sig(self) -> str:
        if not self.paths.protocols_dir.exists():
            return ""
        h = hashlib.sha1()
        for fp in sorted(self.paths.protocols_dir.glob("P*-*.md")):
            try:
                st = fp.stat()
                h.update(fp.name.encode())
                h.update(struct.pack(">dq", st.st_mtime, st.st_size))
            except OSError:
                pass
        return h.hexdigest()

    def list(self) -> List[Dict[str, Any]]:
        return list(self.protocols)

    def get(self, pid: str) -> Optional[Dict[str, Any]]:
        pid = pid.strip()
        for fp in sorted(self.paths.protocols_dir.glob(f"{pid}-*.md")):
            if not _PROTOCOL_FILE_RE.match(fp.name):
                continue
            try:
                text = fp.read_text()
            except OSError:
                return None
            fm, body = _split_frontmatter(text)
            return {
                "id": str(fm.get("id") or pid),
                "title": str(fm.get("title") or pid),
                "frontmatter": fm,
                "body": body,
                "file": fp.name,
            }
        return None

    def runs(self, pid: str, limit: int = 50) -> List[Dict[str, Any]]:
        pid = pid.strip()
        if not self.paths.protocols_log.exists():
            return []
        runs: List[Dict[str, Any]] = []
        for month_dir in sorted(self.paths.protocols_log.iterdir(), reverse=True):
            if not month_dir.is_dir():
                continue
            for fp in sorted(month_dir.iterdir(), reverse=True):
                m = _PROTOCOL_LOG_RE.match(fp.name)
                if not m or m.group(1) != pid:
                    continue
                try:
                    text = fp.read_text()
                except OSError:
                    continue
                fm, _ = _split_frontmatter(text)
                runs.append(
                    {
                        "protocol": pid,
                        "date": m.group(2),
                        "file": f"{month_dir.name}/{fp.name}",
                        "outcome": str(fm.get("outcome") or ""),
                        "operator": str(fm.get("operator") or ""),
                        "agent": str(fm.get("agent") or ""),
                        "commit": str(fm.get("commit") or ""),
                    }
                )
                if len(runs) >= limit:
                    return runs
        return runs

    def _count_logs(self, pid: str) -> int:
        if not self.paths.protocols_log.exists():
            return 0
        n = 0
        for month_dir in self.paths.protocols_log.iterdir():
            if not month_dir.is_dir():
                continue
            for fp in month_dir.iterdir():
                m = _PROTOCOL_LOG_RE.match(fp.name)
                if m and m.group(1) == pid:
                    n += 1
        return n
