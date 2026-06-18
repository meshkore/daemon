"""anchorprogress.py — extracted from anchor.py (daemon-architecture-v2 Phase 3d).

AnchorProgressMixin: methods moved VERBATIM out of AnchorMixin; Daemon inherits both so
every self.* resolves on the combined instance -> byte-identical."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

from prompts import _agent_type_from_conv_slug, _agent_type_normalised
from utils import _iso_now, _log


class AnchorProgressMixin:
    def _handle_anchor_progress(
        self, conv: str, payload: Dict[str, Any], *, raw: str
    ) -> None:
        """Mid-turn task transition. Writes `status: done` (or whatever
        the payload specifies) to the task .md and broadcasts."""
        tid = str(payload.get("t") or "").strip()
        new_status = str(payload.get("status") or "done").strip()
        if not self._TASK_ID_RE.match(tid):
            _log(f"anchor-progress: invalid task id {tid!r} from conv {conv}")
            return
        path = self._find_task(tid)
        if not path:
            _log(f"anchor-progress: task {tid} not found on disk (conv {conv})")
            return
        try:
            text = path.read_text()
            today = _iso_now()[:10]
            new = re.sub(
                r"^status:\s*\S+\s*$",
                f"status: {new_status}",
                text,
                count=1,
                flags=re.M,
            )
            if new == text:
                # No status line — insert after the opening ---
                new = re.sub(
                    r"^---\s*$\n",
                    f"---\nstatus: {new_status}\n",
                    text,
                    count=1,
                    flags=re.M,
                )
            new = re.sub(r"^updated:.*$", f"updated: {today}", new, count=1, flags=re.M)
            if new == text and "updated:" not in new:
                # No updated line — append within frontmatter
                new = re.sub(
                    r"(---\s*$)", f"updated: {today}\n\\1", new, count=1, flags=re.M
                )
            path.write_text(new)
        except Exception as e:
            _log(f"anchor-progress: write failed for task {tid}: {e}")
            return
        try:
            self.hub.broadcast(
                {
                    "type": "conv.task_completed",
                    "conv": conv,
                    "task_id": tid,
                    "new_status": new_status,
                    "ts": _iso_now(),
                }
            )
            self.state_manager.rebuild(broadcast=True)
        except Exception as e:
            _log(f"task_completed broadcast/rebuild failed: {e}")

    def _handle_anchor_rejected(self, conv: str, reason: str, *, raw: str) -> None:
        """LAL2 stub — broadcast warning + log. LAL3 keeps this shape."""
        try:
            self.hub.broadcast(
                {
                    "type": "conv.anchor_rejected",
                    "conv": conv,
                    "reason": reason,
                    "raw_payload": raw[:512],
                    "ts": _iso_now(),
                }
            )
        except Exception as e:
            _log(f"anchor.rejected broadcast failed for {conv}: {e}")
        _log(f"anchor rejected: conv={conv} reason={reason!r}")

    def _handle_anchor_missing(self, conv: str) -> None:
        """LAL2 stub — once per turn, broadcast that the agent skipped
        the marker. Cockpit can dim the 'anchored' affordance."""
        try:
            self.hub.broadcast(
                {
                    "type": "conv.anchor_missing",
                    "conv": conv,
                    "ts": _iso_now(),
                }
            )
        except Exception as e:
            _log(f"anchor.missing broadcast failed for {conv}: {e}")

    def _broadcast_conv_activity(
        self,
        conv: str,
        *,
        live_override: Optional[bool] = None,
    ) -> None:
        """Emit a `conv.activity` WS event for one conv so cockpits
        update their live/coordinating/waiting_on flags without a
        snapshot refetch. Cheap: computes the single entry inline.

        `live_override` lets the caller force the `live` flag when
        ChatSessions hasn't yet popped the conv from `_s` (the runner's
        emit-final path races with ChatSessions._wait's pop). Pass
        `False` from the wake hook when we know the child has just
        finalised; pass `None` (default) elsewhere to read the truth
        from `chat_sessions.list_active()`.

        Called from the points that change a conv's runtime state:
            • ChatRunner spawn (live=true)
            • Wake hook on child final (live=false override)
            • chat_cancel (live=false)
        Idempotent — duplicate fires are safe; the cockpit reducer
        dedupes on conv+live+coordinating identity."""
        try:
            all_meta = self._conv_meta_load()
            live = set(self.chat_sessions.list_active())
            if live_override is False:
                live.discard(conv)
            elif live_override is True:
                live.add(conv)
            kids = []
            for c in live:
                p = (all_meta.get(c) or {}).get("parent_conv")
                if p == conv:
                    kids.append(c)
            is_live = conv in live
            m = all_meta.get(conv) or {}
            self.hub.broadcast(
                {
                    "type": "conv.activity",
                    "conv": conv,
                    "agent_type": _agent_type_normalised(
                        _agent_type_from_conv_slug(conv) or m.get("agent_type")
                    ),
                    "agent_id": m.get("agent_id"),
                    "parent_conv": m.get("parent_conv"),
                    "initiative_id": m.get("initiative_id"),
                    "task_id": m.get("task_id"),
                    "live": is_live,
                    "coordinating": (not is_live) and bool(kids),
                    "waiting_on": sorted(kids),
                    "ts": _iso_now(),
                }
            )
        except Exception as e:
            _log(f"conv.activity broadcast failed for {conv}: {e}")
