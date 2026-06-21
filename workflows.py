"""workflows.py — WorkflowsRegistry — .meshkore/workflows/ runbooks.

Renamed from protocols.py (2026-06-21): "protocol" is reserved in MeshKore for
agent-COMMUNICATION standards (A2A / MCP / HTTP). An ordered, reviewable
operational runbook (deploy, audit, system-upgrade, PR-review) is a **workflow**
— the term the AI/dev community uses (CI / agentic workflows). See
`.meshkore/docs/conventions/workflows.md`.

Back-compat during the rename: reads the new `.meshkore/workflows/` dir AND the
legacy `.meshkore/protocols/` dir, accepts both `W<N>-` and legacy `P<N>-`
file/log prefixes, and broadcasts BOTH `workflows.updated` and (deprecated)
`protocols.updated` so an un-updated cockpit keeps working."""

from __future__ import annotations

import hashlib
import struct
from hub import Hub

import threading
from typing import Any, Dict, List, Optional

from paths import Paths
import re

from registries import _split_frontmatter

# Accept the new W-prefix and the legacy P-prefix during/after the rename.
_WF_FILE_RE = re.compile(r"^[WP](\d+)-[a-z0-9-]+\.md$")
# slug allows dots so version-tagged daemon-upgrade logs (…-py-1.10.0-…) match.
_WF_LOG_RE = re.compile(r"^([WP]\d+)-(\d{4}-\d{2}-\d{2})-[a-z0-9.-]+\.md$")


class WorkflowsRegistry:
    """Loads + watches .meshkore/workflows/ (legacy .meshkore/protocols/);
    broadcasts on change."""

    POLL_SEC = 3.0

    def __init__(self, paths: Paths, hub: "Hub"):
        self.paths = paths
        self.hub = hub
        # Each entry: { id, title, scope, status, updated, file, log_count }
        self.workflows: List[Dict[str, Any]] = []
        self._sig: str = ""
        self._stop = threading.Event()
        self.reload(broadcast=False)
        threading.Thread(target=self._watch_loop, daemon=True).start()

    # ── dir resolution (new workflows/, legacy protocols/ fallback) ──────
    def _dir(self):
        wf = self.paths.workflows_dir
        return wf if wf.exists() else self.paths.protocols_dir

    def _log_dir(self):
        wf = self.paths.workflows_log
        return wf if wf.exists() else self.paths.protocols_log

    def _watch_loop(self) -> None:
        while not self._stop.wait(self.POLL_SEC):
            try:
                self.reload(broadcast=True)
            except Exception:
                pass

    def shutdown(self) -> None:
        self._stop.set()

    def _glob_files(self):
        d = self._dir()
        if not d.exists():
            return []
        # both prefixes, de-duped + sorted by id
        return sorted(set(d.glob("W*-*.md")) | set(d.glob("P*-*.md")))

    def reload(self, broadcast: bool = True) -> bool:
        sig = self._compute_sig()
        if sig == self._sig and self.workflows:
            return False
        self._sig = sig
        out: List[Dict[str, Any]] = []
        for fp in self._glob_files():
            m = _WF_FILE_RE.match(fp.name)
            if not m:
                continue
            try:
                text = fp.read_text()
            except OSError:
                continue
            fm, _body = _split_frontmatter(text)
            wid = str(fm.get("id") or f"{fp.name[0]}{m.group(1)}")
            out.append(
                {
                    "id": wid,
                    "title": str(fm.get("title") or wid),
                    "scope": str(fm.get("scope") or "cluster"),
                    "status": str(fm.get("status") or "draft"),
                    "priority": str(fm.get("priority") or "medium"),
                    "owner": str(fm.get("owner") or ""),
                    "updated": str(fm.get("updated") or ""),
                    "tags": fm.get("tags") or [],
                    "file": fp.name,
                    "log_count": self._count_logs(wid),
                }
            )
        self.workflows = out
        if broadcast:
            ids = [w["id"] for w in out]
            # new event + deprecated alias (un-updated cockpit still listens).
            self.hub.broadcast({"type": "workflows.updated", "ids": ids})
            self.hub.broadcast({"type": "protocols.updated", "ids": ids})
        return True

    def _compute_sig(self) -> str:
        files = self._glob_files()
        if not files:
            return ""
        h = hashlib.sha1()
        for fp in files:
            try:
                st = fp.stat()
                h.update(fp.name.encode())
                h.update(struct.pack(">dq", st.st_mtime, st.st_size))
            except OSError:
                pass
        return h.hexdigest()

    def list(self) -> List[Dict[str, Any]]:
        return list(self.workflows)

    def get(self, wid: str) -> Optional[Dict[str, Any]]:
        wid = wid.strip()
        for fp in sorted(self._dir().glob(f"{wid}-*.md")):
            if not _WF_FILE_RE.match(fp.name):
                continue
            try:
                text = fp.read_text()
            except OSError:
                return None
            fm, body = _split_frontmatter(text)
            return {
                "id": str(fm.get("id") or wid),
                "title": str(fm.get("title") or wid),
                "frontmatter": fm,
                "body": body,
                "file": fp.name,
            }
        return None

    def runs(self, wid: str, limit: int = 50) -> List[Dict[str, Any]]:
        wid = wid.strip()
        log_dir = self._log_dir()
        if not log_dir.exists():
            return []
        runs: List[Dict[str, Any]] = []
        for month_dir in sorted(log_dir.iterdir(), reverse=True):
            if not month_dir.is_dir():
                continue
            for fp in sorted(month_dir.iterdir(), reverse=True):
                m = _WF_LOG_RE.match(fp.name)
                if not m or m.group(1) != wid:
                    continue
                try:
                    text = fp.read_text()
                except OSError:
                    continue
                fm, _ = _split_frontmatter(text)
                runs.append(
                    {
                        "workflow": wid,
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

    def _count_logs(self, wid: str) -> int:
        log_dir = self._log_dir()
        if not log_dir.exists():
            return 0
        n = 0
        for month_dir in log_dir.iterdir():
            if not month_dir.is_dir():
                continue
            for fp in month_dir.iterdir():
                m = _WF_LOG_RE.match(fp.name)
                if m and m.group(1) == wid:
                    n += 1
        return n


# Back-compat alias: some imports/tests may still reference the old name.
ProtocolsRegistry = WorkflowsRegistry
