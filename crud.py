"""crud.py — extracted from daemon.py (daemon-architecture-v2 Phase 2).

CrudMixin: methods moved VERBATIM; Daemon inherits it so every self.*
still resolves on the combined instance -> byte-identical."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from runs import RunStore
from utils import _append_timeline, _iso_now


class CrudMixin:
    def run_create(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        """Create a new story run. The cockpit decides which conv and
        agent_id to bind (it already manages those); the daemon just
        records the binding and emits run.started.
        """
        initiative_id = str(body.get("initiative_id") or "").strip()
        if not initiative_id:
            return 400, {"error": "initiative_id required"}
        conv = str(body.get("conv") or "").strip()
        if not conv:
            return 400, {"error": "conv required"}
        agent_id = str(body.get("agent_id") or "").strip()
        if not agent_id:
            return 400, {"error": "agent_id required"}
        task_ids_raw = body.get("task_ids") or []
        if not isinstance(task_ids_raw, list) or not task_ids_raw:
            return 400, {"error": "task_ids must be a non-empty list"}
        task_ids = [str(t) for t in task_ids_raw if t]
        run = self.runs.create(
            initiative_id=initiative_id,
            initiative_title=str(body.get("initiative_title") or initiative_id),
            conv=conv,
            agent_id=agent_id,
            agent_title=str(body.get("agent_title") or initiative_id),
            task_ids=task_ids,
        )
        return 201, {"ok": True, "run": run}

    def run_cancel(self, run_id: str) -> Tuple[int, Dict[str, Any]]:
        run = self.runs.get(run_id)
        if not run:
            return 404, {"error": f"unknown run {run_id!r}"}
        # Cancel the chat session (if live) AND mark the run cancelled.
        cancelled, dropped = self.chat_sessions.cancel(run["conv"])
        updated = self.runs.cancel(run_id)
        if cancelled:
            self.hub.broadcast(
                {
                    "type": "chat.cancelled",
                    "conv": run["conv"],
                    "ts": _iso_now(),
                    "dropped_pending": dropped,
                }
            )
        return 200, {
            "ok": True,
            "run": updated,
            "chat_cancelled": cancelled,
            "dropped_pending": dropped,
        }

    def run_advance(
        self, run_id: str, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        cursor = body.get("cursor")
        if not isinstance(cursor, int):
            return 400, {"error": "cursor (int) required"}
        stream_id = body.get("stream_id")
        if stream_id is not None and not isinstance(stream_id, str):
            return 400, {"error": "stream_id must be string"}
        updated = self.runs.advance(run_id, cursor, stream_id=stream_id)
        if updated is None:
            return 404, {"error": f"unknown run {run_id!r}"}
        return 200, {"ok": True, "run": updated}

    def run_finish(
        self, run_id: str, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        status = str(body.get("status") or "").strip()
        if status not in (RunStore.STATUS_DONE, RunStore.STATUS_FAILED):
            return 400, {"error": "status must be 'done' or 'failed'"}
        updated = self.runs.finish(run_id, status, error=body.get("error"))
        if updated is None:
            return 404, {"error": f"unknown run {run_id!r}"}
        return 200, {"ok": True, "run": updated}

    def run_set_stream(
        self, run_id: str, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        stream_id = str(body.get("stream_id") or "").strip()
        if not stream_id:
            return 400, {"error": "stream_id required"}
        updated = self.runs.set_stream(run_id, stream_id)
        if updated is None:
            return 404, {"error": f"unknown run {run_id!r}"}
        return 200, {"ok": True, "run": updated}

    def runs_list(self, active_only: bool = False) -> Tuple[int, Dict[str, Any]]:
        runs = self.runs.list_all(active_only=active_only)
        # Decorate each with a derived `live` flag — true when there's
        # a chat session in flight for the conv right now. Cockpit uses
        # it to decide play vs stop on the UI.
        for r in runs:
            r["live"] = self.chat_sessions.has(r["conv"])
        return 200, {"runs": runs, "count": len(runs)}

    def run_get(self, run_id: str) -> Tuple[int, Dict[str, Any]]:
        r = self.runs.get(run_id)
        if not r:
            return 404, {"error": f"unknown run {run_id!r}"}
        r["live"] = self.chat_sessions.has(r["conv"])
        return 200, {"run": r}

    def append_message(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        text = str(body.get("text") or "").strip()
        if not text:
            return 400, {"error": "text required"}
        author = str(body.get("author") or self.identity)
        conv = str(body.get("conv") or "general")
        ev = _append_timeline(
            self.paths,
            {
                "type": "message",
                "author": author,
                "text": text,
                "conv": conv,
            },
        )
        self.hub.broadcast(ev)
        return 201, ev

    def task_create(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        module = str(body.get("module") or "general").strip().replace("/", "")
        title = str(body.get("title") or "").strip()
        if not title:
            return 400, {"error": "title required"}
        status = str(body.get("status") or "next")
        priority = str(body.get("priority") or "medium")
        category = str(body.get("category") or module)
        tags = body.get("tags") or []
        depends_on = body.get("depends_on") or []
        body_md = str(body.get("body") or f"# {title}\n\n_New task — fill in._\n")
        # Pick the next id in the module.
        tasks_dir = self.paths.modules_dir / module / "tasks"
        tasks_dir.mkdir(parents=True, exist_ok=True)
        # Heuristic id: T{N} where N is the highest existing + 1.
        max_n = 0
        for f in tasks_dir.glob("T*.md"):
            m = re.match(r"T(\d+)", f.name)
            if m:
                try:
                    max_n = max(max_n, int(m.group(1)))
                except ValueError:
                    pass
        tid = f"T{max_n + 1:03d}"
        slug = re.sub(r"[^a-z0-9-]+", "-", title.lower())[:60].strip("-")
        fname = f"{tid}-{slug}.md" if slug else f"{tid}.md"
        target = tasks_dir / fname
        frontmatter = "\n".join(
            [
                "---",
                f"id: {tid}",
                f'title: "{title}"',
                f"status: {status}",
                f"priority: {priority}",
                f"category: {category}",
                f"owner: {self.identity}",
                f"created: {_iso_now()[:10]}",
                f"updated: {_iso_now()[:10]}",
                f"tags: {json.dumps(tags)}",
                f"depends_on: {json.dumps(depends_on)}",
                "---",
                "",
                body_md,
            ]
        )
        target.write_text(frontmatter)
        self.state_manager.rebuild(broadcast=True)
        return 201, {"id": tid, "path": str(target.relative_to(self.paths.root))}

    def task_transition(
        self, tid: str, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        to = str(body.get("to") or "").strip()
        valid = {"backlog", "next", "in_progress", "active", "blocked", "done"}
        if to not in valid:
            return 400, {"error": f"to must be one of {sorted(valid)}"}
        path = self._find_task(tid)
        if not path:
            return 404, {"error": f"task {tid} not found"}
        text = path.read_text()
        new = re.sub(r"^status:\s*\S+\s*$", f"status: {to}", text, count=1, flags=re.M)
        if new == text:
            new = re.sub(
                r"^---\s*$\n", f"---\nstatus: {to}\n", text, count=1, flags=re.M
            )
        path.write_text(new)
        self.state_manager.rebuild(broadcast=True)
        return 200, {
            "id": tid,
            "from": "?",
            "to": to,
            "path": str(path.relative_to(self.paths.root)),
        }

    def task_cancel(self, tid: str) -> Tuple[int, Dict[str, Any]]:
        # No active runner yet (dispatch is stubbed); this just transitions to blocked.
        return self.task_transition(tid, {"to": "blocked"})

    def _find_task(self, tid: str) -> Optional[Path]:
        for f in self.paths.modules_dir.rglob(f"{tid}*.md"):
            return f
        return None

    def agent_create(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        aid = str(body.get("id") or "").strip()
        if not re.match(r"^[a-z][a-z0-9-]{1,40}$", aid):
            return 400, {"error": "id must be lowercase kebab, 2-41 chars"}
        self.paths.agents_dir.mkdir(parents=True, exist_ok=True)
        target = self.paths.agents_dir / f"{aid}.yaml"
        if target.exists():
            return 409, {"error": f"agent {aid} already declared"}
        kind = str(body.get("kind") or "operator")
        permissions = str(body.get("permissions") or "edits")
        target.write_text(
            f"# Declared via POST /agents on {_iso_now()}\n"
            f"id: {aid}\n"
            f"kind: {kind}\n"
            f"permissions: {permissions}\n"
        )
        self.state_manager.rebuild(broadcast=True)
        return 201, {"id": aid, "path": str(target.relative_to(self.paths.root))}
