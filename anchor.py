"""anchor.py — AnchorMixin: the live-anchor-loop handler.

Phase 2 of daemon-architecture-v2 — methods MOVED VERBATIM off the Daemon
god-class into a mixin (Daemon inherits it). `self.*` still resolves on the
combined instance, so behaviour is byte-identical. Handles ⟦anchor⟧ markers:
create/resolve initiative+task, progress/rejected/missing, conv-activity
broadcast.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple

from utils import _debug_emit, _iso_now, _log


class AnchorMixin:
    @staticmethod
    def _normalize_init_ref(raw: Any) -> str:
        """py-1.14.11 (AS1) — identity-safe normalization of an initiative
        reference (`i` / `new_t.initiative`) before slug validation. A real
        slug is a lowercase file-stem with no `#`; agents routinely paste the
        `#`-prefixed DISPLAY id they're told to use in chat (`#I32`) or vary
        casing. Stripping a leading `#` + whitespace and lowercasing cannot
        change identity — it only recovers that common slip instead of hard-
        rejecting. NOTE: the display-id PREFIX itself (`I32-`) is deliberately
        NOT stripped — that would risk anchoring to the wrong initiative; the
        prompt (prompts.py) handles it by telling the agent to emit the slug."""
        return str(raw or "").strip().lstrip("#").lower()

    def _handle_anchor(self, conv: str, payload: Dict[str, Any], *, raw: str) -> None:
        """Resolve the anchor payload to (init_id, task_id), creating
        files on disk if `new_i` / `new_t` was specified, persist
        conv_meta, and broadcast `conv.anchored`."""
        if payload.get("info") is True:
            try:
                _debug_emit(
                    "anchor.info",
                    msg=f"info-only turn for conv {conv}",
                    conv=conv,
                )
            except Exception:
                pass
            return

        is_new_init = False
        is_new_task = False
        init_frontmatter: Optional[Dict[str, Any]] = None
        task_frontmatter: Optional[Dict[str, Any]] = None

        # --- Resolve initiative ---
        if "new_i" in payload:
            new_i = payload.get("new_i") or {}
            ok, err, init_id, init_frontmatter = self._anchor_create_init(new_i, conv)
            if not ok:
                self._handle_anchor_rejected(conv, err, raw=raw)
                return
            is_new_init = True
        elif "i" in payload:
            # py-1.14.11 (AS1) — identity-safe normalization before validation.
            init_id = self._normalize_init_ref(payload.get("i"))
            if not self._INIT_SLUG_RE.match(init_id):
                self._handle_anchor_rejected(
                    conv, f"invalid initiative slug: {init_id!r}", raw=raw
                )
                return
            if not (self.paths.initiatives / f"{init_id}.md").exists():
                self._handle_anchor_rejected(
                    conv, f"initiative #{init_id} not found on disk", raw=raw
                )
                return
        else:
            # payload had only `new_t` → look up initiative from new_t.initiative
            new_t = payload.get("new_t") or {}
            # py-1.14.11 (AS1) — same identity-safe normalization as the `i` branch.
            init_id = self._normalize_init_ref(new_t.get("initiative"))
            if not init_id:
                self._handle_anchor_rejected(
                    conv,
                    "no initiative — supply `i`, `new_i`, or `new_t.initiative`",
                    raw=raw,
                )
                return
            if not (self.paths.initiatives / f"{init_id}.md").exists():
                self._handle_anchor_rejected(
                    conv,
                    f"initiative #{init_id} (from new_t.initiative) not found",
                    raw=raw,
                )
                return

        # --- Resolve task ---
        if "new_t" in payload:
            new_t = payload.get("new_t") or {}
            ok, err, task_id, task_frontmatter = self._anchor_create_task(
                new_t, init_id, conv
            )
            if not ok:
                self._handle_anchor_rejected(conv, err, raw=raw)
                return
            is_new_task = True
        elif "t" in payload:
            task_id = str(payload.get("t") or "").strip()
            if not self._TASK_ID_RE.match(task_id):
                self._handle_anchor_rejected(
                    conv, f"invalid task id: {task_id!r}", raw=raw
                )
                return
            if not self._find_task(task_id):
                self._handle_anchor_rejected(
                    conv, f"task #{task_id} not found on disk", raw=raw
                )
                return
        else:
            self._handle_anchor_rejected(
                conv, "no task — supply `t` or `new_t`", raw=raw
            )
            return

        # --- Persist conv_meta + broadcast ---
        existing_meta = self._conv_meta_load().get(conv) or {}
        agent_type = existing_meta.get("agent_type") or "custom"
        agent_id = existing_meta.get("agent_id")
        parent_conv = existing_meta.get("parent_conv")
        self._conv_meta_set(
            conv,
            agent_type=agent_type,
            agent_id=agent_id,
            parent_conv=parent_conv,
            initiative_id=init_id,
            task_id=task_id,
        )

        evt = {
            "type": "conv.anchored",
            "conv": conv,
            "initiative_id": init_id,
            "task_id": task_id,
            "is_new_init": is_new_init,
            "is_new_task": is_new_task,
            "ts": _iso_now(),
        }
        if init_frontmatter is not None:
            evt["init_frontmatter"] = init_frontmatter
        if task_frontmatter is not None:
            evt["task_frontmatter"] = task_frontmatter
        try:
            self.hub.broadcast(evt)
        except Exception as e:
            _log(f"conv.anchored broadcast failed for {conv}: {e}")

        if is_new_init or is_new_task:
            try:
                self.state_manager.rebuild(broadcast=True)
            except Exception as e:
                _log(f"state rebuild after anchor failed: {e}")

    def _anchor_create_init(
        self, payload: Dict[str, Any], conv: str
    ) -> Tuple[bool, str, str, Dict[str, Any]]:
        """Validate + write `.meshkore/roadmap/initiatives/<id>.md`.
        Returns (ok, error_msg, id, frontmatter_dict)."""
        iid = str(payload.get("id") or "").strip()
        if not self._INIT_SLUG_RE.match(iid):
            return (
                False,
                f"new_i.id {iid!r} doesn't match {self._INIT_SLUG_RE.pattern}",
                "",
                {},
            )
        target = self.paths.initiatives / f"{iid}.md"
        if target.exists():
            return False, f"initiative #{iid} already exists", iid, {}
        title = str(payload.get("title") or iid).strip()
        oneliner = str(payload.get("oneliner") or "").strip()
        modules = payload.get("modules") or []
        if not isinstance(modules, list) or not modules:
            modules = ["general"]
        priority = str(payload.get("priority") or "medium")
        today = _iso_now()[:10]
        fm = {
            "id": iid,
            "title": title,
            "status": "active",
            "priority": priority,
            "oneliner": oneliner,
            "modules": list(modules),
            "created": today,
            "updated": today,
            "owner": self.identity,
            "created_by": "live-anchor-loop",
            "created_by_conv": conv,
        }
        body = (
            f"# {title}\n\n"
            f"{oneliner or '_New initiative created by an agent on first anchor._'}\n\n"
            "_Body will be filled in by the agent or operator in subsequent turns._\n"
        )
        self.paths.initiatives.mkdir(parents=True, exist_ok=True)
        target.write_text(self._fm_dump(fm) + "\n" + body)
        return True, "", iid, fm

    def _anchor_create_task(
        self, payload: Dict[str, Any], init_id: str, conv: str
    ) -> Tuple[bool, str, str, Dict[str, Any]]:
        """Validate + write `.meshkore/modules/<m>/tasks/<id>.md`.
        Returns (ok, error_msg, id, frontmatter_dict)."""
        tid = str(payload.get("id") or "").strip()
        if not self._TASK_ID_RE.match(tid):
            return (
                False,
                f"new_t.id {tid!r} doesn't match {self._TASK_ID_RE.pattern}",
                "",
                {},
            )
        if self._find_task(tid):
            return False, f"task #{tid} already exists", tid, {}
        category = str(payload.get("category") or "general").strip().replace("/", "")
        if not category:
            category = "general"
        title = str(payload.get("title") or tid).strip()
        depends_on = payload.get("depends_on") or []
        today = _iso_now()[:10]
        fm = {
            "id": tid,
            "title": title,
            "status": "active",
            "owner": self.identity,
            "category": category,
            "initiative": init_id,
            "depends_on": list(depends_on) if isinstance(depends_on, list) else [],
            "created": today,
            "updated": today,
            "created_by": "live-anchor-loop",
            "created_by_conv": conv,
        }
        body = (
            f"# {title}\n\n"
            "_New task — created by an agent on anchor._\n\n"
            "Detail will be filled in by the agent during execution.\n"
        )
        tasks_dir = self.paths.modules_dir / category / "tasks"
        tasks_dir.mkdir(parents=True, exist_ok=True)
        (tasks_dir / f"{tid}.md").write_text(self._fm_dump(fm) + "\n" + body)
        return True, "", tid, fm

    def _fm_dump(self, fm: Dict[str, Any]) -> str:
        """Render a frontmatter dict as the YAML subset our parser
        round-trips (see parse_simple_yaml). Strings quoted only when
        they contain colon, comma, hash, or leading whitespace."""
        out = ["---"]
        for k, v in fm.items():
            if isinstance(v, list):
                inner = ", ".join(
                    json.dumps(x) if isinstance(x, str) else str(x) for x in v
                )
                out.append(f"{k}: [{inner}]")
            elif isinstance(v, bool):
                out.append(f"{k}: {'true' if v else 'false'}")
            elif isinstance(v, (int, float)):
                out.append(f"{k}: {v}")
            else:
                s = str(v)
                if any(c in s for c in ":,#") or s != s.strip():
                    out.append(f"{k}: {json.dumps(s)}")
                else:
                    out.append(f"{k}: {s}")
        out.append("---")
        return "\n".join(out)
