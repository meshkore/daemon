"""chatread.py — extracted from readapi.py (daemon-architecture-v2 Phase 3).

ChatReadMixin: methods moved VERBATIM out of QueryMixin; Daemon inherits both so
every self.* still resolves on the combined instance -> byte-identical."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from constants import DAEMON_VERSION
from prompts import _agent_type_from_conv_slug, _agent_type_normalised
from utils import _iso_now, _iter_timeline_files, _read_timeline_file, debug_enabled


class ChatReadMixin:
    def chat_convs(self) -> List[Dict[str, Any]]:
        """Canonical list of every conv known to the daemon — union of
        conv_meta.json sidecar entries, live ChatRunner convs, and the
        ChatArchive registry. One source of truth so the cockpit no
        longer has to reconstruct the rail by walking the last 500
        timeline events.

        Per entry:
            conv               — conv id
            agent_type         — normalised role (slug-implied wins)
            agent_id           — A### if assigned
            parent_conv        — for subagents
            initiative_id      — work-* convs and the architect when known
            task_id            — work-* convs
            archived           — bool; archived_at + by when true
            live               — own ChatRunner is streaming RIGHT NOW
            coordinating       — has >=1 live child via parent_conv
            waiting_on         — list of child convs currently live
            created_at         — first-seen ts (from timeline; falls back to
                                  archive entry or "" if neither exists)
            last_activity_at   — most recent timeline event ts for this conv
            msg_count          — count of user/assistant events in timeline

        Note on cost: `_chat_msg_index()` walks all timeline files once
        per call to compute counts + ts boundaries. On small clusters
        (<10k events total) this is sub-millisecond; on big clusters we
        can later memoise on file mtimes, but YAGNI for the cavioca
        scale we're at today.
        """
        all_meta = self._conv_meta_load()
        live = set(self.chat_sessions.list_active())
        archived_list = self.chat_archive.list()  # [{conv, archived_at, by}, …]
        archived_by_conv: Dict[str, Dict[str, Any]] = {
            a["conv"]: a for a in archived_list
        }
        msg_index = self._chat_msg_index()

        # Build the union of all conv ids we know about.
        all_convs: set = set()
        all_convs.update(all_meta.keys())
        all_convs.update(live)
        all_convs.update(archived_by_conv.keys())
        all_convs.update(msg_index.keys())

        # Build parent → children map across the conv_meta entries that
        # name a parent, restricted to live children (the cockpit only
        # cares about "currently waiting on X").
        children_by_parent: Dict[str, List[str]] = {}
        for c in live:
            p = (all_meta.get(c) or {}).get("parent_conv")
            if p:
                children_by_parent.setdefault(str(p), []).append(c)

        entries: List[Dict[str, Any]] = []
        for conv in all_convs:
            meta = all_meta.get(conv) or {}
            arch = archived_by_conv.get(conv)
            idx = msg_index.get(conv) or {}
            is_live = conv in live
            kids = children_by_parent.get(conv) or []
            entry: Dict[str, Any] = {
                "conv": conv,
                "agent_type": _agent_type_normalised(
                    _agent_type_from_conv_slug(conv) or meta.get("agent_type")
                ),
                "agent_id": meta.get("agent_id"),
                "parent_conv": meta.get("parent_conv"),
                "initiative_id": meta.get("initiative_id"),
                "task_id": meta.get("task_id"),
                # MP1 (py-1.13.3) — surface the per-conv model preference
                # so the cockpit can show "running on opus" / etc. in
                # the scope strip alongside the agent role.
                "model": meta.get("model"),
                # MP3 (py-1.13.4) — per-conv effort (reasoning depth).
                "effort": meta.get("effort"),
                # DM-CLI-02 (multi-cli-clients) — per-conv CLI-client
                # preference; absent means claude-code.
                "client": meta.get("client"),
                # agent-team (ATM10) — the roster member this conv is bound
                # to. Was persisted to conv_meta but never surfaced here, so
                # the cockpit could never learn/heal a pre-existing conv's
                # member binding (only conv-creation-time local state), and
                # `member` was silently omitted from EVERY dispatch on such
                # a conv — defeating per-member model/client/provider
                # (operator field report 2026-07-13: switching a member's
                # provider had zero effect on its long-lived system conv).
                "member": meta.get("member"),
                # multi-provider-agents (MPV1) — surfaced for symmetry with
                # client/model so the cockpit can display/heal it too.
                "provider": meta.get("provider"),
                "archived": arch is not None,
                "archived_at": arch.get("archived_at") if arch else None,
                "archived_by": arch.get("by") if arch else None,
                "live": is_live,
                "coordinating": (not is_live) and bool(kids),
                "waiting_on": sorted(kids),
                "created_at": idx.get("first_ts")
                or (arch.get("archived_at") if arch else ""),
                "last_activity_at": idx.get("last_ts") or "",
                "msg_count": int(idx.get("count") or 0),
            }
            # CU1 (py-1.13.3) — cumulative token usage + cost for the
            # conv. None when no turn has finalised yet (the cockpit
            # hides the chip). Accumulated in ChatSessions; resets on
            # daemon restart (persisting is `usage-ledger` territory).
            usage = self.chat_sessions.usage_total(conv)
            if usage is not None:
                entry["usage"] = usage
            # SRL2 (py-1.13.1) — for live convs, attach `current_turn`
            # (partial_text + started_at + counters) and `queue` (the
            # in-memory ChatSessions.pending list). Lets a cockpit
            # that just connected rehydrate mid-turn UI without
            # waiting for the first WS delta. Both fields are
            # OPTIONAL — older cockpits ignore them. Cap: single
            # dict lookup + a 16 KB partial_text slice per live
            # conv, so cheap even with many active sessions.
            if is_live:
                snap = self.chat_sessions.turn_snapshot(conv)
                if snap is not None:
                    if snap.get("current_turn"):
                        entry["current_turn"] = snap["current_turn"]
                    if snap.get("queue"):
                        entry["queue"] = snap["queue"]
            entries.append(entry)

        # Order: live first, then idle, then archived. Inside each
        # bucket: newest activity first. Single sort with a composite
        # key — bucket ascending + activity-string-inverted so newest
        # ISO ts (which sort lexicographically) ends up on top.
        def _sort_key(e: Dict[str, Any]) -> Tuple[int, str]:
            bucket = 0 if e["live"] else (2 if e["archived"] else 1)
            # Invert the ISO ts per-char so lexicographic ASC == ts DESC.
            ts = e.get("last_activity_at") or ""
            inverted = "".join(chr(255 - ord(c)) for c in ts) if ts else "\xff"
            return (bucket, inverted)

        entries.sort(key=_sort_key)
        return entries

    def _chat_msg_index(self) -> Dict[str, Dict[str, Any]]:
        """Walk every timeline file once, return per-conv counts +
        first/last ts of chat.user / chat.assistant.final events.

        py-1.16.0 (D-CHAT-IDX-01) — memoised on the set of timeline-file
        (path, mtime, size). Previously EVERY call (incl. per-conv
        `/chat/conv/<id>/meta`, which the cockpit polls) re-read and
        DECOMPRESSED all timeline history — O(all events ever) — and the
        `.gz` files are never deleted, so the cost grew monotonically.
        Now we rebuild only when a timeline file actually changed (an
        append bumps mtime+size); otherwise we return the cached index."""
        out: Dict[str, Dict[str, Any]] = {}
        if not self.paths.timeline_dir.exists():
            return out
        files = list(_iter_timeline_files(self.paths))
        try:
            sig = tuple(
                sorted(
                    (str(f), st.st_mtime_ns, st.st_size)
                    for f in files
                    for st in (f.stat(),)
                )
            )
        except OSError:
            sig = None
        cache = getattr(self, "_chat_idx_cache", None)
        if sig is not None and cache is not None and cache[0] == sig:
            return cache[1]
        chat_types = ("chat.user", "chat.assistant", "chat.assistant.final")
        for f in files:
            for ev in _read_timeline_file(f):
                if ev.get("type") not in chat_types:
                    continue
                conv = ev.get("conv")
                if not conv:
                    continue
                ts = str(ev.get("ts") or "")
                slot = out.setdefault(conv, {"count": 0, "first_ts": "", "last_ts": ""})
                slot["count"] += 1
                if ts:
                    if not slot["first_ts"] or ts < slot["first_ts"]:
                        slot["first_ts"] = ts
                    if ts > slot["last_ts"]:
                        slot["last_ts"] = ts
        if sig is not None:
            self._chat_idx_cache = (sig, out)
        return out

    def chat_conv_meta(self, conv: str) -> Dict[str, Any]:
        """One conv's metadata sidecar, normalised. Used by the cockpit
        for deep-links and resync of individual entries without a full
        /chat/convs refetch."""
        all_meta = self._conv_meta_load()
        m = all_meta.get(conv) or {}
        idx = self._chat_msg_index().get(conv) or {}
        arch = self.chat_archive.is_archived(conv)
        return {
            "conv": conv,
            "agent_type": _agent_type_normalised(
                _agent_type_from_conv_slug(conv) or m.get("agent_type")
            ),
            "agent_id": m.get("agent_id"),
            "parent_conv": m.get("parent_conv"),
            "initiative_id": m.get("initiative_id"),
            "task_id": m.get("task_id"),
            "archived": arch,
            "live": self.chat_sessions.has(conv),
            "created_at": idx.get("first_ts") or "",
            "last_activity_at": idx.get("last_ts") or "",
            "msg_count": int(idx.get("count") or 0),
        }

    def chat_conv_messages(
        self,
        conv: str,
        *,
        before_ts: Optional[str] = None,
        limit: int = 200,
    ) -> Dict[str, Any]:
        """Paginated message read for one conv. Returns events of types
        chat.user / chat.assistant / chat.assistant.final / chat.cancelled
        whose ts < `before_ts` (when provided), newest-first, capped to
        `limit`. The cockpit reverses for display order.

        Pagination contract:
            • First page  → call with before_ts unset → newest `limit`.
            • Older page  → call with before_ts = oldest_ts of prior page.
            • has_more    → true iff a full `limit` came back, OR there
                              is at least one further event in the index.
            • oldest_ts   → the ts of the oldest event in the page
                              (cockpit feeds this back as `before_ts`).

        Cost is the same `_iter_timeline_files` walk as `_chat_msg_index`.
        For now we re-walk per request; the optimisation TODO (per-conv
        index files) is documented but unshipped — small clusters don't
        need it."""
        limit = max(1, min(2000, int(limit or 200)))
        wanted_types = (
            "chat.user",
            "chat.assistant",
            "chat.assistant.final",
            "chat.cancelled",
        )
        # Gather candidates across files in arbitrary order, then sort.
        all_events: List[Dict[str, Any]] = []
        if self.paths.timeline_dir.exists():
            for f in _iter_timeline_files(self.paths):
                for ev in _read_timeline_file(f):
                    if ev.get("conv") != conv:
                        continue
                    if ev.get("type") not in wanted_types:
                        continue
                    all_events.append(ev)
        all_events.sort(key=lambda e: str(e.get("ts") or ""))
        if before_ts:
            all_events = [e for e in all_events if str(e.get("ts") or "") < before_ts]
        # Newest-first cap, then re-reverse so the returned list is in
        # chronological order (the cockpit's reducer expects oldest→newest).
        page = all_events[-limit:]
        oldest_in_page = str(page[0].get("ts") or "") if page else ""
        # `has_more` = there exists at least one event older than the
        # oldest_in_page (we cut some off the front).
        has_more = len(all_events) > len(page)
        return {
            "conv": conv,
            "messages": page,
            "count": len(page),
            "has_more": has_more,
            "oldest_ts": oldest_in_page,
        }

    def chat_snapshot(self) -> Dict[str, Any]:
        """Boot consolidated payload. One round-trip on cockpit start
        instead of the old 3-call chain (/state for timeline replay,
        /chat/archives for archived set, /health for active convs).

        Shape kept narrow on purpose — cockpit consumes specific
        sub-keys; if we need more later, add a key. Never expose
        secrets here."""
        return {
            "convs": self.chat_convs(),
            "paused_agent_types": self._paused_agent_types_view(),
            "quota": self.quota.view(),
            "debug": {
                "enabled": debug_enabled(),
            },
            "version": DAEMON_VERSION,
            "generated_at": _iso_now(),
        }
