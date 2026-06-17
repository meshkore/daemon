"""readapi.py — read-mostly HTTP query surface (QueryMixin).

Extracted from daemon.py (DA-QUERY-01, daemon-architecture-v2 Phase 2) as a
MIXIN: the 16 read/serialize endpoints (health, chat_convs/meta/messages/
snapshot, info, _features, agents_listing, initiative_activity, context_tree,
log_listing, credentials CRUD) move VERBATIM into QueryMixin; Daemon inherits
it so every `self.*` still resolves on the combined instance → byte-identical.
The three credential-name validators (only used by the credential endpoints
here) come along, so readapi has no daemon.py backref → no import cycle."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constants import DAEMON_VERSION
from prompts import _agent_type_from_conv_slug, _agent_type_normalised
from registries import _split_frontmatter
from utils import (
    _iso_now,
    _iter_timeline_files,
    _log,
    _read_timeline_file,
    debug_enabled,
    parse_frontmatter,
    parse_simple_yaml,
)


_CREDENTIAL_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

# Protected names cannot be written or deleted via the API. portal-token
# is the daemon's own auth secret — letting the cockpit overwrite it
# would lock the cockpit out of the daemon on the very next request.
CREDENTIAL_PROTECTED_NAMES = frozenset({"portal-token"})


def _validate_credential_name(name: str) -> Optional[Tuple[int, Dict[str, Any]]]:
    """Returns None when the name is OK, or a (code, body) error tuple
    ready to ship back to the client. Used by every credential CRUD
    endpoint as the first gate."""
    if not isinstance(name, str) or not name:
        return 400, {"error": "credential name required"}
    if not _CREDENTIAL_NAME_RE.match(name):
        return 400, {
            "error": "invalid credential name; allowed: A-Za-z0-9._- (≤64 chars, must start with alnum)",
        }
    if "/" in name or ".." in name:
        return 400, {"error": "path separators not allowed in credential name"}
    return None


class QueryMixin:
    def health(self) -> Dict[str, Any]:
        # py-1.2.0 — Surface the cluster.yaml.daemon block (or its
        # defaults) so the cockpit knows whether to fire the silent
        # auto-update flow on a version mismatch.
        cfg = {}
        try:
            d = (
                self.cluster.data.get("daemon")
                if isinstance(self.cluster.data, dict)
                else None
            )
            if isinstance(d, dict):
                cfg = d
        except Exception:
            cfg = {}
        daemon_cfg = {
            "auto_update": bool(cfg.get("auto_update", True)),
            "auto_update_source": str(
                cfg.get("auto_update_source")
                or "https://meshkore.com/reference/cluster/scripts/daemon.py"
            ),
        }
        return {
            "ok": True,
            "identity": self.identity,
            "port": self.port,
            "mode": "server",
            "implementation": "python",
            "version": DAEMON_VERSION,
            "cluster_id": self.cluster.id,
            "cluster_name": self.cluster.name,
            "cluster_type": self.cluster.type,
            # D-TLS-01 — advertise the transport scheme so the cockpit
            # knows whether https://daemon.meshkore.com:<port> is
            # available or it must use http://localhost:<port>.
            "tls": self.tls_enabled,
            "endpoint": (
                f"https://daemon.meshkore.com:{self.port}"
                if self.tls_enabled
                else f"http://localhost:{self.port}"
            ),
            # U-DAEMON-01: capability advertisement.
            # During the Node→Python unification (initiative
            # `unified-python-daemon`), the cockpit reads this array
            # to route each call to the daemon that supports the
            # feature. Adding an endpoint here is part of the
            # acceptance criteria for that endpoint's port task.
            "features": self._features(),
            # py-1.2.0 — Standard v7 §10.4 (daemon self-update).
            "daemon": daemon_cfg,
            # py-1.14.8 — standard-version drift (detect + surface only).
            # `version` = the cluster's pinned STANDARD_VERSION; `latest`
            # = the published version (null until the first poll);
            # `drift` = latest > local. The cockpit can render a
            # "Standard vN available — review CHANGELOG / dispatch
            # catch-up" banner; the daemon never auto-migrates (§11).
            "standard": (
                {
                    "version": self.instructions_renderer.local_standard_version,
                    "latest": self.instructions_renderer.latest_standard_version,
                    "drift": self.instructions_renderer.standard_drift,
                }
                if getattr(self, "instructions_renderer", None) is not None
                else None
            ),
            # py-1.10.21 — Debug stream advertisement. `enabled` is the
            # operator-controlled flag (`cluster.yaml.debug.enabled`,
            # default true). Cockpit's debug-transport gates its POST
            # /debug/log buffer on this — when disabled it drains
            # silently instead of round-tripping.
            "debug": {
                "enabled": debug_enabled(),
                "path": (
                    str(self.paths.runtime / "debug.jsonl") if debug_enabled() else None
                ),
            },
            # py-1.10.26 — Agent-type pause state. Back-compat projection
            # from QuotaState (py-1.10.27+). Empty dict when no type is
            # paused. Cockpit's older banner reads from here.
            "paused_agent_types": self._paused_agent_types_view(),
            # py-1.10.27 — Full quota state keyed by `<platform>/<model>`
            # with probe history, last-success, consecutive-rate-limits.
            # New cockpit banner reads from here. Initiative
            # `quota-aware-dispatch`.
            "quota": self.quota.view(),
            "ts": _iso_now(),
        }

    # ── py-1.11.0: chat-state-rearchitecture. Canonical conv list +
    # paginated message reads + consolidated boot snapshot. The
    # daemon-authoritative chat surface — replaces the deleted
    # /state.timeline.recent_events + /health.chat_active_convs +
    # /health.chat_activity legacy channels.
    # ────────────────────────────────────────────────────────────────

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

    # LAL3 (py-1.13.0) — anchor protocol side-effects. The parser in
    # LAL2 (ChatRunner._resolve_anchor_head + _strip_anchor_progress)
    # extracts the marker and calls these handlers. THIS is the
    # closing of v23's loop — files get created, conv_meta gets
    # written, the cockpit gets WS events.

    _INIT_SLUG_RE = re.compile(r"^[a-z][a-z0-9-]{1,31}$")
    _TASK_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{1,31}$")

    def _features(self) -> List[str]:
        feats = [
            "health",
            "state",
            "state.subset",  # U-DAEMON-02
            "reload",
            # D-TLS-01 — only when the bundled cert actually loaded.
            *(["tls.loopback"] if self.tls_enabled else []),
            # D-TLS-02 — challenge-response auth for MITM defence.
            "auth.challenge",
            "agents",
            "agents.create",  # U-DAEMON-02 + 03
            "events",  # WS hub + chat.* + task.* + tool.*
            "files.docs",
            "files.modules",
            "files.tasks",  # U-DAEMON-02
            "files.log",  # py-1.9.0 — narrative day-logs for Diary tab
            "initiative.activity",  # py-1.9.3 — per-initiative git commits + files
            "runs.v1",  # py-1.10.0 — story-run coordinator
            "runs.cancel",  # POST /runs/<id>/cancel
            "runs.advance",  # POST /runs/<id>/advance
            "runs.finish",  # POST /runs/<id>/finish
            "agents.roadmap-architect",  # py-1.10.3 — coordinator agent type
            "agents.architect-consult.v1",  # py-1.10.8 — [architect-consult] addendum forces A001 to decide
            "agents.validation-gate.v1",  # py-1.10.9 — VALIDATION GREEN/RED first turn + batched questions
            "agents.architect-chain-first.v1",  # py-1.10.10 — chain-first prompt + wallet canonical example + length budgets
            "agents.validation-shortcuts.v1",  # py-1.10.11 — proceed/rework operator shortcuts + ROADMAP-REWORK trigger + chat-input UX
            "agents.slug-implied-type.v1",  # py-1.10.12 — slug-implied agent_type force heals stale conv_meta + drops the SOP-in-prompt lead-in
            "agents.roadmap-author.v1",  # py-1.10.13 — custom agent auto-triggers roadmap-author playbook (meshkore.com/reference/prompts/roadmap-author/v1/) on empty clusters
            "cluster.credentials.crud.v1",  # py-1.11.3 — GET/PUT/POST/DELETE /credentials/<name>; cockpit Config block reads/writes single-file secrets at .meshkore/credentials/ (chmod 600, protected names: portal-token)
            "agents.briefing-https.v1",  # py-1.10.14 — agent briefings emit https://daemon.meshkore.com:<port> URLs when TLS bundle present (architect no longer aborts on TLS RST against plain http://localhost)
            "roadmap.linked-list.v1",  # py-1.10.15 — state.initiatives[] ordered by linked-list walk + bucket sort (empty-at-bottom, done at end)
            "roadmap.auto-archive.v1",  # py-1.10.15 — initiatives with all-done child tasks get status/completed_at/commit_sha written by the daemon on every /state build
            "agents.architect-wake.v1",  # py-1.10.16 — subagent's chat.assistant.final triggers an automatic [architect-wake] dispatch to the parent_conv recorded in conv_meta; replaces architect-side polling
            "debug.stream.v1",  # py-1.10.17 — structured JSONL at .meshkore/.runtime/debug.jsonl, GET /debug/tail + POST /debug/log, 30-min rolling retention. Replaces ad-hoc screenshots as the cross-component observability channel.
            "rate-limit.auto-pause.v1",  # py-1.10.26 — subagent finals classified as rate-limited auto-pause their agent_type for 30 min; chat_dispatch returns 503 during cooldown; manual POST /agent-types/<t>/{pause,unpause} for operator override; /health.paused_agent_types advertises state.
            "quota.aware-dispatch.v1",  # py-1.10.27 — per-(platform,model) persistent QuotaState at .runtime/quota-state.json + QuotaProber thread that auto-clears expired pauses; /quota GET + /quota/<key>/{pause,unpause} endpoints.
            "chat.snapshot.v1",  # py-1.11.0+ — daemon-authoritative conv list. GET /chat/snapshot (boot), GET /chat/convs, GET /chat/conv/<id>/meta, GET /chat/conv/<id>/messages?before=&limit= (paginated history). WS events: conv.created, conv.meta_updated, conv.archived, conv.unarchived, conv.activity. py-1.11.1 Phase 2 deleted the legacy back-compat surfaces (/health.chat_active_convs, /health.chat_activity, /state.timeline.recent_events, chat.archived/chat.unarchived WS aliases). Initiative `chat-state-rearchitecture`.
            "diagnostics.sigusr1.v1",  # py-1.12.24 — `kill -USR1 <pid>` dumps every thread's stack to .meshkore/.runtime/threads.log via faulthandler.register. Designed for live diagnosis of lock-contention bugs like the 2026-06-10 ikamiro hang.
            "http.bounded-pool.v1",  # py-1.12.24 — ThreadingHTTPServer replaced with PoolHTTPServer (ThreadPoolExecutor with bounded max_workers; default 64, configurable via cluster.yaml.daemon.http.max_workers). Caps OS thread count regardless of request rate.
            "daemon.modular.layer-1.v1",  # py-1.12.25 DM3 — Paths + storage classes extracted to daemon/paths.py + daemon/storage.py. Bundler concatenates in dep order. Cockpit may use this feature to gate "view source layout" affordances in the future.
            "daemon.modular.layer-2.v1",  # py-1.12.26 DM4 — Hub + WSClient + HEARTBEAT_SEC extracted to daemon/hub.py. ws.broadcast contract unchanged; cockpit + tests unaffected.
            "daemon.modular.layer-3.v1",  # py-1.12.27 DM5 — ChatSessions + ChatSessionReaper extracted to daemon/chat.py. Lock invariant doc'd. ChatRunner deferred to a later task.
            "daemon.modular.layer-4.v1",  # py-1.12.28 DM6 step 1 — QuotaState + QuotaProber extracted to daemon/quota.py.
            "daemon.modular.layer-5.v1",  # py-1.12.29 DM6 step 2 — make_handler + WS read helpers extracted to daemon/routes.py.
            "daemon.modular.layer-6.v1",  # py-1.12.30 DM7 phase A — utils.py extracted. Sibling modules drop shadow stubs; single source of truth for _log/_iso_now/_debug_emit + DebugLog singleton wired via setter/getter functions.
            "anchor.v1",  # py-1.12.31 LAL1 — agent briefing teaches the ⟦anchor⟧ first-line marker protocol (4 shapes + ⟦anchor-progress⟧). Daemon-side parser + side-effects in LAL2/LAL3. Cockpit gates UI loaders behind this flag.
            "anchor.strip.v1",  # py-1.12.32 LAL2 — ChatRunner buffers + parses the head, strips the marker line from chat.assistant.delta broadcasts, calls _handle_anchor stubs. LAL3 makes the stubs do real file creation + conv_meta + WS events.
            "anchor.handler.v1",  # py-1.13.0 LAL3 — _handle_anchor resolves existing or new init/task, persists to conv_meta, broadcasts conv.anchored.
            "anchor.auto-create.v1",  # py-1.13.0 LAL3 — `new_i` / `new_t` payloads atomically create initiative + task .md files with frontmatter contract enforced.
            "anchor.progress.v1",  # py-1.13.0 LAL3 — `⟦anchor-progress⟧ {"t":...,"status":"done"}` writes status to the task .md and broadcasts conv.task_completed.
            "daemon.snapshot.turn_state.v1",  # py-1.13.1 SRL2 — `/chat/snapshot` carries `current_turn` (started_at + stream_id + partial_text + counters) + `queue` for live convs so the cockpit can rehydrate mid-turn UI after a browser refresh.
            "context.tree.v1",  # py-1.14.1 — Standard v14 §3.5 project context tree. GET /context returns the `.meshkore/context/` folder/file tree (per-file title/updated/status + word count + over_cap flag) with tree-level total_words/token_estimate/budget_tokens/over_budget/warnings; GET /context/<path> serves a single file body. Powers the cockpit's Context tab (ContextPanel.tsx, daemon-client.contextTree/contextFile). Fixes the 404 the cockpit logged on every Context-tab open prior to this version.
            "standard.drift.v1",  # py-1.14.8 — detect+surface standard-version drift. /health.standard = {version, latest, drift}; WS `standard.drift` {local, latest} on the transition into drift. Detect-only: never auto-bumps STANDARD_VERSION nor applies structural migrations (§11 stays LLM/operator).
            "agent_instructions.render.v1",  # py-1.14.7 ADI-01 — Standard §17 render loop. AgentInstructionsRenderer (render.py) boot-syncs + 3s-mtime-watches `.meshkore/public/AGENT_INSTRUCTIONS.md` → CLAUDE.md/AGENTS.md/GEMINI.md (+ .cursor/rules/meshkore.mdc + .clinerules when STANDARD_VERSION≥19), and refreshes the MESHKORE_PREAMBLE block from meshkore.com/standard/agent-instructions.md on the VersionWatcher tick (OPERATOR_CONTENT preserved). WS: agent_instructions.rendered / .preamble_refreshed. Closes the gap where the per-CLI files drifted because nothing re-rendered them.
            "credentials",  # U-DAEMON-02 (list-only)
            "info",
            "shutdown",
            # U-DAEMON-04 task lifecycle (dispatch is stubbed, marked separately)
            "tasks.create",
            "tasks.transition",
            "tasks.cancel",
            # U-DAEMON-05 + 06 chat coordinator
            "chat",
            "chat.cancel",
            # U-DAEMON-09 misc
            "messages",
            # py-1.2.0 — Standard v7 §10.4 daemon self-update.
            "self_update",
            "version_header",
            # py-1.5.0 — chat integrity bundle.
            "chat.tools_persisted",  # tool.use + tool.result in jsonl
            "chat.rolling_history",  # >12-turn summary in briefing
            "chat.atomic_writes",  # fsync + atomic append
            "chat.archives",  # /chat/archives + /chat/archive[+un]
            "timeline.rotation",  # gzip > 90d into archive/
            # py-1.6.0 → py-1.6.1 — session_resume opt-in only.
            # Set env MESHKORE_CLAUDE_SESSION_ID=1 to enable. Default
            # off after a production bug where claude-code exited
            # silently on resumed sessions.
        ]
        if os.environ.get("MESHKORE_CLAUDE_SESSION_ID", "").strip() in (
            "1",
            "true",
            "yes",
            "on",
        ):
            feats.append("chat.session_resume.optin")
        if hasattr(self.cluster, "crons"):
            feats.append("cron.schema")
        # D-CRON-02..05: scheduler is live, list + trigger + cancel + log endpoints.
        feats.extend(
            ["cron.tick", "cron.list", "cron.trigger", "cron.cancel", "cron.log"]
        )
        # Standard §13: deployment links registry.
        feats.extend(["links.read", "links.write"])
        # Standard §14: protocols registry (read-only this version).
        feats.extend(["protocols.read"])
        # Stubs — advertised separately so the cockpit can show
        # "not yet" badges without trying the endpoint.
        feats.extend(
            [
                "stub.workers",
                "stub.admission",
                "stub.tasks.dispatch",
                "stub.version.next",
            ]
        )
        return feats

    def info(self) -> Dict[str, Any]:
        h = self.health()
        h["version"] = DAEMON_VERSION
        h["paths"] = {
            "root": str(self.paths.root),
            "meshkore": str(self.paths.meshkore),
        }
        return h

    def agents_listing(self) -> List[Dict[str, Any]]:
        # U-DAEMON-02: matches Node's shape including pid + online so
        # the cockpit's Network tab works against either daemon.
        if not self.paths.agents_dir.exists():
            return []
        runtime_agents = self.paths.runtime / "agents"
        out = []
        for yml in sorted(self.paths.agents_dir.glob("*.yaml")):
            try:
                data = parse_simple_yaml(yml.read_text())
            except OSError:
                continue
            pid_file = runtime_agents / f"{yml.stem}.pid"
            pid: Optional[int] = None
            online = False
            if pid_file.exists():
                try:
                    pid = int(pid_file.read_text().strip())
                    # Crude liveness check — os.kill(pid, 0) raises if no such pid
                    os.kill(pid, 0)
                    online = True
                except (OSError, ValueError):
                    pid = None
            out.append(
                {
                    "id": yml.stem,
                    "identity": yml.stem,  # alias, matches Node
                    "pid": pid,
                    "online": online,
                    "data": data,
                }
            )
        return out

    def initiative_activity(self, initiative_id: str) -> Dict[str, Any]:
        """py-1.9.3 — Walk git log for commits referencing this initiative.
        Returns at most 50 of the most recent matching commits, each with
        the files it touched (`git diff-tree --no-commit-id --name-only -r`).
        Matching is plain substring on subject + body so operators can
        reference an initiative however they like ("[I-cron-dashboard]",
        "for cron-dashboard", etc.) — no rigid trailer schema.

        Bounded by 1000 commits scanned + a hard timeout per git call so
        a 50k-commit repo doesn't melt the daemon. Failures (no git, bad
        repo, timeout) degrade to an empty payload + an explanatory
        `error` field; the cockpit just shows "no activity yet".
        """
        out: Dict[str, Any] = {
            "initiative_id": initiative_id,
            "commits": [],
            "generated_at": _iso_now(),
        }
        if not isinstance(initiative_id, str) or not initiative_id.strip():
            out["error"] = "invalid initiative id"
            return out
        iid = initiative_id.strip()

        import subprocess as _sp

        root = self.paths.root

        # py-1.9.3 — Multi-repo workspaces (meshkore-style: webapp/,
        # architect/, .meshkore/ each a separate git repo at depth 1)
        # AND single-repo projects (typical ikamiro-style) both work.
        # Find every depth ≤ 1 directory that owns a `.git` and scan
        # each one. The commit row carries a `repo` field so the
        # cockpit can disambiguate when two repos both reference the
        # same initiative id.
        repo_dirs: List[Path] = []
        if (root / ".git").exists():
            repo_dirs.append(root)
        else:
            try:
                for child in sorted(root.iterdir()):
                    if not child.is_dir() or child.name.startswith("."):
                        continue
                    if (child / ".git").exists():
                        repo_dirs.append(child)
            except OSError:
                pass

        if not repo_dirs:
            out["error"] = "no git repos found at project root or depth-1"
            return out

        def git_in(cwd: Path, *args: str, timeout: float = 4.0) -> Optional[str]:
            try:
                r = _sp.run(
                    ["git", "-C", str(cwd), *args],
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                )
                if r.returncode != 0:
                    return None
                return r.stdout
            except (_sp.TimeoutExpired, FileNotFoundError, OSError):
                return None

        commits: List[Dict[str, Any]] = []
        for repo_dir in repo_dirs:
            repo_label = repo_dir.name if repo_dir != root else "(root)"
            raw = git_in(
                repo_dir,
                "log",
                "--max-count=1000",
                "--grep",
                iid,
                "-i",
                "--pretty=format:%H%x09%h%x09%aI%x09%an%x09%s",
                timeout=6.0,
            )
            if raw is None:
                continue
            for line in raw.splitlines():
                if not line.strip():
                    continue
                parts = line.split("\t", 4)
                if len(parts) != 5:
                    continue
                sha, short, ts, author, subject = parts
                files_raw = (
                    git_in(
                        repo_dir,
                        "diff-tree",
                        "--no-commit-id",
                        "--name-only",
                        "-r",
                        sha,
                        timeout=3.0,
                    )
                    or ""
                )
                files = [ln.strip() for ln in files_raw.splitlines() if ln.strip()]
                commits.append(
                    {
                        "repo": repo_label,
                        "sha": sha,
                        "short_sha": short,
                        "ts": ts,
                        "author": author,
                        "subject": subject,
                        "files": files[:200],
                        "files_truncated": len(files) > 200,
                    }
                )
                if len(commits) >= 50:
                    break
            if len(commits) >= 50:
                break

        # Newest first across repos (each repo's slice already comes
        # newest-first from git log, but interleaved across repos
        # needs an explicit ts sort).
        commits.sort(key=lambda c: c.get("ts") or "", reverse=True)
        out["commits"] = commits[:50]
        return out

    # ── Standard v14 §3.5 — project context tree ─────────────────────
    #
    # Per-file word caps from the brevity contract (§3.5 "Folder
    # layout"). A file over its cap is flagged `over_cap` so the
    # cockpit can paint a warning marker; the tree-level budget is the
    # 3000-word / 4500-token total.
    CONTEXT_WORD_CAPS: Dict[str, int] = {
        "overview.md": 200,
        "product.md": 200,
        "stack.md": 200,
        "architecture.md": 250,
        "constraints.md": 250,
        "glossary.md": 250,
    }
    # Files inside decisions/ and criteria/ each cap at 100 words
    # (README.md is an index — exempt from the per-entry cap).
    CONTEXT_FOLDER_ENTRY_CAP = 100
    CONTEXT_BUDGET_WORDS = 3000
    CONTEXT_BUDGET_TOKENS = 4500

    def context_tree(self) -> Dict[str, Any]:
        """py-1.14.1 — Standard v14 §3.5 project context tree.

        Walks `.meshkore/context/` and returns the nested folder/file
        shape the cockpit's Context tab renders: per-file `title`
        (frontmatter `title`, falling back to a humanized filename),
        `updated` + `status` (frontmatter), word count, and an
        `over_cap` flag against the §3.5 brevity caps. Tree-level the
        response carries `total_words`, `token_estimate` (~1.5 tokens /
        word), the 4500-token budget, an `over_budget` flag, and a
        `warnings` list (per-file over-cap notes + total-over-budget).

        File bodies are NOT inlined — the cockpit lazy-fetches each on
        selection via `/context/<path>`. Returns `exists: False` with
        an empty tree when no `.meshkore/context/` directory is present
        (e.g. a freshly bootstrapped cluster) so the cockpit can render
        its empty-state hint instead of an error.

        Path traversal is structurally impossible here — we only ever
        `iterdir()` inside `context_dir`; `path` values are relative to
        that root and consumed by `/context/<path>` which re-validates.
        """
        root = self.paths.context_dir
        warnings: List[str] = []

        def humanize(name: str) -> str:
            stem = name[:-3] if name.endswith(".md") else name
            return stem.replace("-", " ").replace("_", " ").strip().capitalize()

        def word_count(text: str) -> int:
            # Count words in the body only (frontmatter excluded) so the
            # cap reflects prose, not YAML keys.
            _fm, body = _split_frontmatter(text)
            return len(body.split())

        def build_file(fp: "Path", rel: str, cap: Optional[int]):
            title = humanize(fp.name)
            updated: Optional[str] = None
            status: Optional[str] = None
            words = 0
            try:
                text = fp.read_text(encoding="utf-8", errors="replace")
                fm = parse_frontmatter(text)
                if isinstance(fm.get("title"), str) and fm["title"].strip():
                    title = fm["title"].strip()
                if isinstance(fm.get("updated"), str):
                    updated = fm["updated"].strip()
                elif fm.get("updated") is not None:
                    updated = str(fm["updated"])
                if isinstance(fm.get("status"), str) and fm["status"].strip():
                    status = fm["status"].strip()
                words = word_count(text)
            except OSError:
                pass
            over_cap = cap is not None and words > cap
            if over_cap:
                warnings.append(f"{rel}: {words}w over the {cap}w cap")
            node: Dict[str, Any] = {
                "kind": "file",
                "name": fp.name,
                "path": rel,
                "title": title,
                "words": words,
                "over_cap": over_cap,
            }
            if updated:
                node["updated"] = updated
            if status:
                node["status"] = status
            return node, words

        def cap_for(rel: str, name: str, in_folder: bool) -> Optional[int]:
            if in_folder:
                # README.md is an index, exempt; other entries cap at 100.
                return None if name == "README.md" else self.CONTEXT_FOLDER_ENTRY_CAP
            return self.CONTEXT_WORD_CAPS.get(name)

        total_words = 0

        def build_dir(dp: "Path", rel_prefix: str, in_folder: bool):
            nonlocal total_words
            children: List[Dict[str, Any]] = []
            try:
                entries = sorted(dp.iterdir(), key=lambda e: e.name)
            except OSError:
                return children
            # Files first (alpha), then sub-dirs — but keep README.md at
            # the top of a folder so the cockpit's "click dir → README"
            # affordance lands on the index.
            files = [
                e
                for e in entries
                if e.is_file()
                and e.suffix.lower() == ".md"
                and not e.name.startswith(".")
            ]
            dirs = [e for e in entries if e.is_dir() and not e.name.startswith(".")]
            files.sort(key=lambda e: (e.name != "README.md", e.name))
            for f in files:
                rel = f"{rel_prefix}{f.name}"
                node, words = build_file(f, rel, cap_for(rel, f.name, in_folder))
                total_words += words
                children.append(node)
            for d in dirs:
                rel = f"{rel_prefix}{d.name}"
                sub = build_dir(d, f"{rel}/", in_folder=True)
                children.append(
                    {
                        "kind": "dir",
                        "name": d.name,
                        "path": rel,
                        "title": humanize(d.name),
                        "children": sub,
                    }
                )
            return children

        if not root.is_dir():
            return {
                "exists": False,
                "root": ".meshkore/context",
                "total_words": 0,
                "token_estimate": 0,
                "budget_tokens": self.CONTEXT_BUDGET_TOKENS,
                "over_budget": False,
                "warnings": [],
                "tree": [],
            }

        tree = build_dir(root, "", in_folder=False)
        token_estimate = int(round(total_words * 1.5))
        over_budget = token_estimate > self.CONTEXT_BUDGET_TOKENS
        if over_budget:
            warnings.append(
                f"context is {token_estimate} tokens — over the "
                f"{self.CONTEXT_BUDGET_TOKENS}-token budget (§3.5)"
            )
        return {
            "exists": True,
            "root": ".meshkore/context",
            "total_words": total_words,
            "token_estimate": token_estimate,
            "budget_tokens": self.CONTEXT_BUDGET_TOKENS,
            "over_budget": over_budget,
            "warnings": warnings,
            "tree": tree,
        }

    def log_listing(self) -> List[Dict[str, Any]]:
        """py-1.9.0 — Descending-by-date list of `.meshkore/log/*.md`
        narrative day-files. Just metadata (name, date, size, mtime);
        callers fetch the body via `/log/<filename>` for paged display
        in the cockpit Diary tab. Dotfiles + non-.md files are skipped.

        Returned shape:
            [{ "name": "2026-05-27.md", "date": "2026-05-27",
               "size": 12345, "mtime": "2026-05-27T21:00:00Z" }]
        """
        if not self.paths.log_dir.exists():
            return []
        out = []
        for f in self.paths.log_dir.iterdir():
            if not f.is_file() or f.name.startswith("."):
                continue
            if f.suffix.lower() != ".md":
                continue
            # Most filenames are `YYYY-MM-DD.md`. The few that aren't
            # (handoff notes etc.) get `date: null`.
            stem = f.stem
            date = (
                stem
                if (
                    len(stem) == 10
                    and stem[4] == "-"
                    and stem[7] == "-"
                    and stem[:4].isdigit()
                    and stem[5:7].isdigit()
                    and stem[8:10].isdigit()
                )
                else None
            )
            try:
                st = f.stat()
                size = st.st_size
                mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            except OSError:
                size = None
                mtime = None
            out.append(
                {
                    "name": f.name,
                    "date": date,
                    "size": size,
                    "mtime": mtime,
                }
            )
        # Dated entries descending (newest → oldest), then any extras
        # (handoff notes etc.) appended in stable filename order.
        dated = sorted(
            [e for e in out if e["date"]], key=lambda e: e["date"], reverse=True
        )
        extras = sorted([e for e in out if not e["date"]], key=lambda e: e["name"])
        return dated + extras

    def credentials_listing(self) -> List[Dict[str, Any]]:
        """Names + sizes of every file in .meshkore/credentials/.
        Never the contents — the cockpit only needs to know what
        exists, never what's in them. Same security stance as Node."""
        if not self.paths.credentials.exists():
            return []
        out = []
        for f in sorted(self.paths.credentials.iterdir()):
            if f.name.startswith("."):
                continue
            try:
                size = f.stat().st_size if f.is_file() else None
            except OSError:
                size = None
            out.append(
                {
                    "name": f.name,
                    "size": size,
                    "is_symlink": f.is_symlink(),
                    # py-1.11.3 — protected names are listable but the
                    # cockpit's CRUD blocks edit/delete on them. portal-token
                    # is the canonical example: rewriting it from the cockpit
                    # would lock the cockpit out of its own daemon.
                    "protected": f.name in CREDENTIAL_PROTECTED_NAMES,
                }
            )
        return out

    # py-1.11.3 — Single-credential CRUD helpers. All return (code, body)
    # tuples consumed by do_GET/do_PUT/do_DELETE. Auth handled by the
    # routing layer before these run.
    def credential_read(self, name: str) -> Tuple[int, Dict[str, Any]]:
        """Return the credential value for the operator-facing reveal
        action. The cockpit's CredentialsBlock keeps values masked by
        default and only fetches the raw via this endpoint when the
        operator clicks 'reveal'. Auth-required (handled upstream)."""
        valid = _validate_credential_name(name)
        if valid is not None:
            return valid
        path = self.paths.credentials / name
        if not path.exists() or not path.is_file():
            return 404, {"error": "credential not found", "name": name}
        try:
            value = path.read_text(encoding="utf-8")
        except OSError as e:
            return 500, {"error": f"read failed: {e}"}
        return 200, {
            "name": name,
            "value": value,
            "protected": name in CREDENTIAL_PROTECTED_NAMES,
        }

    def credential_write(self, name: str, value: str) -> Tuple[int, Dict[str, Any]]:
        """Create or overwrite a credential file under .meshkore/credentials/.
        Always chmod 600. Refuses protected names (portal-token) so the
        cockpit can't accidentally lock itself out of the daemon."""
        valid = _validate_credential_name(name)
        if valid is not None:
            return valid
        if name in CREDENTIAL_PROTECTED_NAMES:
            return 403, {
                "error": "protected credential — managed by daemon",
                "name": name,
            }
        if not isinstance(value, str):
            return 400, {"error": "value must be a string"}
        self.paths.credentials.mkdir(parents=True, exist_ok=True)
        path = self.paths.credentials / name
        try:
            path.write_text(value, encoding="utf-8")
            os.chmod(path, 0o600)
        except OSError as e:
            return 500, {"error": f"write failed: {e}"}
        _log(f"credential written: {name} ({len(value)} bytes)")
        return 200, {"name": name, "size": len(value.encode("utf-8"))}

    def credential_delete(self, name: str) -> Tuple[int, Dict[str, Any]]:
        valid = _validate_credential_name(name)
        if valid is not None:
            return valid
        if name in CREDENTIAL_PROTECTED_NAMES:
            return 403, {
                "error": "protected credential — managed by daemon",
                "name": name,
            }
        path = self.paths.credentials / name
        if not path.exists():
            return 404, {"error": "credential not found", "name": name}
        try:
            path.unlink()
        except OSError as e:
            return 500, {"error": f"delete failed: {e}"}
        _log(f"credential deleted: {name}")
        return 200, {"deleted": True, "name": name}
