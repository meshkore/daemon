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
from typing import Any, Dict, List, Optional

from constants import DAEMON_VERSION
from utils import (
    _iso_now,
    debug_enabled,
    parse_simple_yaml,
)


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
                or "https://architect.meshkore.com/reference/cluster/scripts/daemon.py"
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
            "initiative.walls.v1",  # py-1.20.0 — GET /initiative/walls + POST /initiative/reorder
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

    # py-1.11.3 — Single-credential CRUD helpers. All return (code, body)
    # tuples consumed by do_GET/do_PUT/do_DELETE. Auth handled by the
    # routing layer before these run.
