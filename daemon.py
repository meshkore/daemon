#!/usr/bin/env python3
"""
MeshKore daemon — pure-Python, stdlib only.

Runs in any folder that already has a `.meshkore/` tree. Binds the first
free port in 5570–5589, serves the architect (HTTP + WebSocket), and
rebuilds state.json from the markdown filesystem on demand or on file
change.

No pip, no venv, no Node. Designed for any Python ≥ 3.8 on macOS / Linux
/ Windows. Drop into `.meshkore/scripts/daemon.py` and run:

    python3 .meshkore/scripts/daemon.py

Distinguishing properties (vs the legacy meshcore binary):

- Stdlib only — works on locked-down corporate machines that block
  installable binaries but still allow scripts.
- Multi-instance safe — every running daemon picks a different port in
  the range; the architect lists them all in the Projects rail.
- Stoppable from the architect — `POST /shutdown` with the bearer token
  ends the process gracefully.
- Read-mostly today (state + reload + events). Heavy actions (agent
  dispatch, AI runners) belong to a richer Node daemon; this Python
  daemon is the canonical entry for L0–L3 read paths.

Endpoints:

    GET  /health                  no auth; basic identity
    GET  /state                   no auth (read-only); built from FS
    GET  /reload                  auth; rebuild + broadcast
    POST /shutdown                auth; graceful exit
    GET  /events                  WebSocket; heartbeats + state.rebuilt
    GET  /agents                  no auth; agents/*.yaml summary

The token lives in `.meshkore/credentials/portal-token`. If it doesn't
exist on first run we generate one (mode 0600).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import signal
import socket
import ssl
import struct
import subprocess
import sys
import threading
import faulthandler
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# DM3 — sibling-module imports. paths.py and storage.py live next to
# daemon.py in source; the bundler concatenates them into dist/daemon.py
# in dependency order, stripping these import lines from the bundled
# output. Source-tree runs hit the sibling files via sys.path[0].
from chat import ChatSessionReaper, ChatSessions  # noqa: E402
from hub import Hub  # noqa: E402
from paths import Paths  # noqa: E402
from prompts import (  # noqa: E402,F401 — F401: re-exported for callers/tests
    AGENT_PROMPTS,
    BriefingPipeline,
    _agent_manifest,
    _agent_type_from_conv_slug,
    _agent_type_normalised,
)
from quota import QuotaProber, QuotaState  # noqa: E402
from registries import (  # noqa: E402,F401 — F401: _split_frontmatter re-exported
    LinksRegistry,
    ProtocolsRegistry,
    _split_frontmatter,
)
from render import AgentInstructionsRenderer  # noqa: E402
from routes import make_handler  # noqa: E402
from runner import (  # noqa: E402,F401 — F401: _session_id_for_conv re-exported for tests
    ChatRunner,
    _session_id_for_conv,
)
from runs import RunStore, TimelineRotator  # noqa: E402
from storage import ChatArchive, ChatQueueManager, StorageReport, UploadStore  # noqa: E402
from utils import (  # noqa: E402
    DebugLog,
    _append_timeline,
    _FM_RE,
    _debug_emit,
    _debug_enabled,
    _find_tls_bundle,
    _iso_now,
    _iter_timeline_files,
    _log,
    _read_timeline_file,
    debug_enabled,
    parse_frontmatter,
    parse_simple_yaml,
    set_debug_log,
)

# ───────────────────────────────────────────────────────────────────────
# Configuration

PORT_RANGE = (5570, 5589)
# py-1.15.0 — machine-global sticky port registry (cluster_id → port).
# Lives outside any repo so every daemon on this box shares one source of
# truth and a cluster ALWAYS comes back up on the same port (no drift).
_PORT_REGISTRY_DIR = Path.home() / ".meshkore"
_PORT_REGISTRY_FILE = _PORT_REGISTRY_DIR / "ports.json"
FS_POLL_SEC = 1.5
DAEMON_VERSION = "py-1.15.1"  # 1.15.1 — listen-backlog fix (HTTP connection-refusal under cockpit boot burst). PoolHTTPServer(ThreadingHTTPServer) inherited socketserver's default request_queue_size=5, so the socket listen() backlog was 5. A cockpit boot fires a burst of ~30 concurrent connections (GET /state + GET /chat/snapshot + a GET per initiative body + the /events WS upgrade); with a backlog of 5 the kernel REFUSED the excess → intermittent ERR_CONNECTION_REFUSED (HTTP 000) that stranded the cockpit mid-hydration (BootingPanel stuck on "Snapshot del roadmap"/"Historial de conversaciones" forever) and is the same effect behind the test_pool_bounds_concurrent_requests 18/50 result. The class docstring's claim that excess requests "queue at the OS-accept layer (much higher than any sane workload)" was simply wrong — the accept backlog was the tiny default. Fix: `request_queue_size = 128` on PoolHTTPServer (socketserver passes it to socket.listen()). 128 absorbs any realistic single-cockpit burst. NO API/wire change. Pairs with cockpit V108 (switchProject re-entrancy guard + bounded fetch timeout) which removes the front-end's infinite switch loop and infinite-hang on a stalled fetch. 1.15.0 — STABLE per-cluster ports (anti-drift, initiative `daemon-port-stability`). ROOT CAUSE of the cockpit "doing weird things" with 3 projects on one box: `_pick_port` honoured a requested port then SILENTLY drifted to the first free port in 5570–5589 on any collision — so when several daemons (re)booted near-simultaneously they scrambled onto each other's ports (field 2026-06-14: ikamiro req 5573→bound 5571, meshkore req 5572→bound 5573, cavioca req 5570→bound 5572), and nobody pinned a stable port, so the cockpit's localStorage rail kept dialing the OLD port (ikamiro→5570, dead) and `switchToPort`-thrashed across the range. Fix: a machine-global sticky registry `~/.meshkore/ports.json` (cluster_id→port) is now the source of truth. `_pick_port(paths, cluster_id, cli_override, yaml_port)` resolves: (1) explicit `--port` wins + rewrites the sticky entry; (2) the cluster's sticky registry assignment; (3) a fresh seed from cluster.yaml `architect.port` / last `.runtime/port` / lowest-free, then claimed + persisted. The chosen port is then VALIDATED with an anti-steal guard: if busy and `/health` shows a DIFFERENT live cluster_id, we refuse to steal — reassign to a fresh free port and persist; if held by our OWN cluster_id (stale/dying instance or a self-update re-exec) we keep it and let the bind path reclaim. `_claim` re-reads+merges the registry so a sibling registering in the race window isn't clobbered, and `_lowest_free` skips ports reserved by other clusters AND live-checks `_port_free`, so even a simultaneous-boot race converges to distinct ports. New helpers `_last_runtime_port` / `_registry_read` / `_registry_write` / `_probe_cluster_id`; new consts `_PORT_REGISTRY_DIR`/`_PORT_REGISTRY_FILE`. Self-update re-exec is unaffected (it passes explicit `--port` = current port → step 1 returns it immediately, MESHKORE_REEXEC_WAIT_PORT bind-wait still reclaims the socket). NO wire/API change; cockpit needs no change — stable ports stop the thrash and its existing scan-by-cluster_id reconciles the stale localStorage entry on next probe. 1.14.11 — queue-flush user event (QF2) + anchor slug normalization (AS1). (QF2) The disk-queue flush path (`_maybe_flush_chat_queue`) called `_spawn_chat_turn` WITHOUT first persisting a `chat.user` timeline event — unlike `chat_dispatch` — so a flushed queued message was missing from the chat wall AND vanished from history on reload (only the agent's triggered response showed, orphaned). Extracted `_persist_user_event(conv, text, author, attachments)` as the single source for 'operator message enters the conversation'; `chat_dispatch` and the flush path now both call it. `_append_timeline` stamps ts → the user bubble sorts chronologically before the assistant final. Frontend unchanged (it already ingests `chat.user`; `queue.item.sent` drops the strip entry). Operator field report 2026-06-14 (IKA/circle: A113's deploy reply showed with no preceding user bubble). (AS1) Anchor `{"i":...}` resolution now normalizes identity-safe before validating — `strip().lstrip("#").lower()` — so the common slip of pasting the `#`-display id (`#I32` → `I32-...`) as a slug is recovered instead of hard-rejected with "invalid initiative slug". Same for `new_t.initiative`. The display-id PREFIX (`I32-`) is NOT stripped (would risk anchoring to the wrong initiative); that's handled prompt-side: prompts.py now states `i` is the lowercase file-stem slug, never the `#`-display id, with a ✗/✓ example. Anchor rejection remains harmless to chat history/log (text always persists). NO wire/API change. 1.14.10 — FIX anchor marker bracket fragility (LAL). The anchor wire protocol's canonical marker uses the mathematical white square brackets ⟦ ⟧ (U+27E6/U+27E7), but LLMs do NOT reliably emit rare Unicode glyphs — they routinely normalise them to ASCII `[anchor]` / `[[anchor]]` (or CJK 【anchor】). When that happened the strict `⟦anchor⟧`-only regex in ChatRunner matched NOTHING, so the marker was (a) never parsed → `_handle_anchor` never ran → no initiative/task creation, no conv_meta initiative_id/task_id persist, no `conv.anchor_*` WS event → the cockpit roadmap never lit the live spinner on the initiative/task being worked, AND (b) never stripped → the raw `[anchor] {...}` line leaked into the chat bubble (the very internal protocol data the operator said must stay hidden). Both symptoms, ONE root cause. Fix: ChatRunner `_ANCHOR_RE` / `_ANCHOR_PROGRESS_RE` now accept any common bracket rendering (⟦ 〚 【 [[ [ … ⟧ 〛 】 ]] ]) via shared `_ANCHOR_OPEN`/`_ANCHOR_CLOSE` classes; the `_strip_anchor_progress` + `_strip_all_anchor_markers` fast-path guards switched from `"⟦anchor…" in text` to bracket-agnostic substring checks. Briefing still teaches the canonical ⟦ ⟧ (prompts.py unchanged) — parser is liberal in what it accepts (Postel). The `-progress` variant can't be mis-eaten by the plain `anchor` regex because the close-bracket must follow `anchor` immediately (a `-` blocks it). Operator field report 2026-06-13 (ikamiro: `[anchor]` visible in chat + roadmap spinner never lit despite a valid anchor decision). NO wire/API change. ALSO 1.14.10 — FIX silent auto-update breakage since the DM3 modularization. VersionWatcher._fetch_remote_version HTTP Range-fetches only the FIRST 8 KB (`_FETCH_BYTES`) of the published bundle and parses `^DAEMON_VERSION`; but bundle.py inlines daemon.py LAST, so the canonical DAEMON_VERSION assignment sits ~334 KB deep — past the 8 KB window — so the fetch returned None and NO cluster auto-updated (published stuck at py-1.14.4 since 1.14.4/1.14.5's modularize-2/3, field-confirmed 2026-06-13). Fix is in bundle.py: it now echoes an EARLY `DAEMON_VERSION = "<ver>"` marker into the bundle header (right after `from __future__`), so the version-watcher's 8 KB fetch finds it. Heals detection for every already-deployed watcher (they read the first 8 KB of the new published file). The canonical assignment + changelog is still inlined from daemon.py below; Python reassigns the identical value (no behaviour change). 1.14.9 — FIX recurring ChatSessions._lock deadlock. ChatSessions._wait (the per-runner completion thread) invoked the on_idle / on_chain callbacks WHILE holding self._lock. Both re-enter ChatSessions (on_idle -> Daemon._maybe_flush_chat_queue -> has()/start(); on_chain -> _spawn_chat_turn -> start()) and broadcast via the Hub — self._lock is a plain non-reentrant threading.Lock, so the _wait thread self-deadlocked, kept the lock forever, and every list_active()/has()/reap_dead() (i.e. /chat/snapshot) hung. This is the intermittent ikamiro hang (cockpit stuck on "Historial de conversaciones"; agent rail flooded with stale localStorage convMeta because the snapshot never hydrated to prune/filter). py-1.14.6 made it FAR more frequent: the has()-guard it added at the top of _maybe_flush_chat_queue re-locks on EVERY turn-completion, not just when a disk queue item exists. Fix: capture idle/chain decision under the lock, then call the callbacks AFTER releasing it (slot already popped/cleared, so the re-entrant has()/start() see clean state; the 1.14.6 idempotency guard absorbs the dispatch race). No wire/API change. 1.14.8 — standard-version drift detection (detect + surface only). AgentInstructionsRenderer.check_standard_drift() fetches meshkore.com/standard/version on the VersionWatcher tick, compares to the cluster's pinned `.meshkore/STANDARD_VERSION`, and on the transition into drift logs + broadcasts a `standard.drift` {local, latest} WS event. `/health.standard` = {version, latest, drift} so the cockpit can render a "Standard vN available — review CHANGELOG / dispatch catch-up" banner (mirrors the daemon-outdated flow). Deliberately does NOT bump STANDARD_VERSION and does NOT apply the structural CHANGELOG catch-up (folder/schema migrations) — that stays LLM/operator work per Standard §11; a daemon rewriting the layout unattended is how clusters break. The §17 preamble/per-CLI instructions are ALREADY kept fresh unconditionally by refresh_from_remote() (py-1.14.7) regardless of the version number, so instructions never go stale even while a structural catch-up is pending. Feature flag `standard.drift.v1`. 1.14.7 — §17 agent-instructions render loop (ADI-01). New module render.py / AgentInstructionsRenderer (Paths+Hub, zero daemon coupling, same shape as the registries). Closes the long-standing gap where §17 mandated the daemon keep the per-CLI instruction files in sync but NO code did it — CLAUDE.md/AGENTS.md/GEMINI.md drifted behind the canonical preamble (the snapshots item §20 never reached them; field-confirmed 2026-06-13). Two idempotent jobs: (a) render_targets() mirrors `.meshkore/public/AGENT_INSTRUCTIONS.md` into the root per-CLI files each CLI auto-loads — boot-synced once (heals older-daemon drift) + driven by a 3 s mtime watch on the source so an operator edit fans out within seconds; the two v19+ targets (`.cursor/rules/meshkore.mdc`, `.clinerules`) are written only when the cluster STANDARD_VERSION ≥ 19. (b) refresh_from_remote() fetches the canonical preamble at meshkore.com/standard/agent-instructions.md and overwrites the MESHKORE_PREAMBLE block of AGENT_INSTRUCTIONS.md IN PLACE (OPERATOR_CONTENT preserved byte-for-byte), driven on the existing VersionWatcher 30-min tick INDEPENDENT of the auto_update opt-out (a doc refresh is not a code upgrade). Idempotent throughout (skips unchanged files → no disk churn / no WS spam). New WS events `agent_instructions.rendered` {files} + `agent_instructions.preamble_refreshed`. Feature flag `agent_instructions.render.v1`. bundle.py MODULES gains render.py (after registries.py). Reopens/closes ADI-01 which was marked done at the 2026-06-09 bootstrap but never shipped the maintenance loop. 1.14.6 — idle chat-queue flush (QF1). The disk chat-queue (ChatQueueManager, `.meshkore/queues/<conv>.json`) was drained ONLY by the on_idle hook that fires on turn-COMPLETION. So a conv that went idle with items still queued — after a daemon restart / py-1.14.3 self-update re-exec (in-memory ChatSessions + its _wait thread are gone), a session reaped abnormally (reap_dead pops the slot without firing on_idle), or an enqueue into an already-idle conv — left the queue stranded forever ("N WAITING · runs after the current turn" with no current turn to finish). New `Daemon._flush_idle_chat_queues()` sweeps every disk queue and flushes the head of any conv with NO turn in flight; flushing re-registers on_idle via `_spawn_chat_turn` (agent_type/id/model resolved from the persisted conv sidecar), so the rest drains turn-by-turn. Wired into `ChatSessionReaper._sweep` Phase 3 (boot sweep resumes update-stranded queues; 30s tick is the safety net). `_maybe_flush_chat_queue` gained an in-flight idempotency guard + bool return so the on_idle and reaper triggers can't double-spawn. New `ChatQueueManager.conv_ids()`. Operator field report 2026-06-13: IKA cluster queue stuck at 2 WAITING after a mid-session daemon update. NO wire/API change — cockpit unaffected. 1.14.5 — daemon-modularize-3. More pure code-movement, continuing the split toward small files. Three new modules: agent_prompts.py (the ~940-line AGENT_PROMPTS data dict, out of prompts.py so the briefing LOGIC stays small), registries.py (LinksRegistry + ProtocolsRegistry + their YAML/frontmatter helpers — zero daemon coupling), runs.py (RunStore + TimelineRotator — zero daemon coupling). daemon.py drops ~6280 → ~5600 LOC (and ~9360 → ~5600 across modularize-2+3). Bundle MODULES order now paths→utils→hub→registries→runs→storage→chat→agent_prompts→prompts→runner→quota→routes. quota.py/daemon.py keep importing AGENT_PROMPTS from prompts (prompts re-exports it). STOPPED before extracting cron + version-watcher/self-update: cron is interleaved across 3 regions with the credentials validator, and the self-update path (py-1.14.3 same-port re-exec) has near-zero test coverage — extracting either risks the no-behaviour-change guarantee on under-tested code. The `Daemon` god-class (~3400 LOC) is the remaining lever and needs a redesign (shared self-state), not a move. NO behaviour change — 97/97 tests green incl. bundle parity + golden-master characterization. 1.14.4 — daemon-modularize-2. Pure code-movement refactor extracting the two pieces the first modularize pass (py-1.13.x) deferred: ChatRunner (~760 LOC, the claude-code subprocess driver) → runner.py, and the prompt machinery (AGENT_PROMPTS registry + _agent_manifest + _agent_type_* + ProjectState + StateIntegrityChecker + _conversation_history + BriefingPipeline) → prompts.py. daemon.py drops 9359 → ~6280 LOC. Shared pure helpers also relocated to utils.py (parse_simple_yaml/parse_frontmatter family, _append_timeline, _find_tls_bundle, _daemon_base_url) so the extracted modules import top-down (utils → prompts → runner) with no `from daemon import` at module scope. quota.py's _agent_manifest/AGENT_PROMPTS shadow-stubs replaced by a real `from prompts import`. The architect SOP's `MeshKore: <ver>` trailer is now substituted at BriefingPipeline.build() (placeholder __MESHKORE_VERSION__) instead of f-string-interpolated at dict-load, because the prompts section loads before DAEMON_VERSION is defined in the bundle. bundle.py MODULES gains prompts.py + runner.py (order: …chat → prompts → runner → quota → routes) and now strips `from daemon import` lines (the build() version import resolves via the flat namespace). NO behaviour change — 97/97 tests green incl. bundle parity + a new golden-master characterization suite (briefing SHA per agent type, manifest snapshot, anchor-strip, --model/--effort argv). 1.14.3 — same-port self-update re-exec. self_update() previously spawned the replacement on a NEW free port and let the cockpit re-discover it (fragile: port hunting, WS-fatal, operator-visible "taking longer than usual"). Now the new process re-execs on the SAME port: the old daemon spawns the child with MESHKORE_REEXEC_WAIT_PORT=1, explicitly server_close()s its listen socket, and os._exit(0)s after a 0.6s flush delay; the child's serve_forever retries the bind for ~12s (250ms cadence) until the port frees, then comes up on the identical port. The cockpit's WS simply reconnects to the same URL — no port change, no port-recovery scan, no front-end reload. Falls back to SystemExit if the port never frees in 12s (old daemon stuck). 1.14.2 — MP3 effort pass-through + full model ids. `/chat/dispatch` now accepts `effort` (low/medium/high/xhigh/max), persisted in conv_meta, resolved by `_conv_meta_get_effort`, and injected as `claude-code --effort <level>` by ChatRunner.spawn (skipped on the 'default' sentinel). This is claude-code's reasoning-depth dial — the cockpit's "thinking" control. `_conv_meta_set` no longer lowercases the model so pinned ids (claude-opus-4-8) survive verbatim alongside the opus/sonnet/haiku aliases. conv.created/meta_updated broadcasts + /chat/snapshot.convs[] now carry both `model` and `effort`. Pairs with cockpit NewAgentWizard which exposes a versioned model catalog (aliases + pinned 4.x) + an effort picker. 1.14.1 — context tree endpoint (Standard v14 §3.5). New `context_tree()` method walks `.meshkore/context/` and `GET /context` serves the folder/file tree (per-file title/updated/status from frontmatter + word count + §3.5 over_cap flag) with tree-level total_words/token_estimate/budget_tokens/over_budget/warnings; `GET /context/<path>` serves a single file body (reuses `_serve_meshkore_file` rooted at context_dir, same path-traversal defence). Fixes the cockpit's Context tab which logged `GET /context 404` on every open (ContextPanel.tsx → daemon-client.contextTree, shipped V107.34 against a daemon endpoint that never existed). Feature flag `context.tree.v1`. New `paths.context_dir`. 1.14.0 — universal Output Contract (OC1). The single weak "Reply concisely" line in `_section_core_rules` is replaced by a prominent `## Output contract` section that EVERY agent type inherits every turn (previously only the architect had a LENGTH BUDGETS table; custom/audit/deploy/db/docs had no length guidance, so an audit-style turn dumped ~50 lines). The contract mandates: lead with a ≤8-line summary (problem + files touched + N-step plan), put ALL detail inside native HTML `<details>` blocks (one per file/topic, blank line after `</summary>` so inner markdown renders), no detail-prose at the top level, no process narration. Pairs with cockpit V107.36 which renders `<details>` natively + stops auto-expanding fresh finals (the auto-expand was un-clamping the very 50-line walls the operator complained about). Operator field report 2026-06-12: agent A108 audit reply was unreadable. Convention: `.meshkore/docs/conventions/output-contract.md`. 1.13.3 — model pass-through (MP1) + chat usage broadcast (CU1). Two initiatives shipped together. (a) The cockpit's NewAgentWizard model picker (auto/opus/sonnet/haiku) now wires end-to-end: `/chat/dispatch` accepts `model`, stored in conv_meta, ChatRunner.spawn injects `--model <id>` into claude-code argv (skipped when `auto`/None — lets the CLI use its default). Chained turns inherit. (b) ChatRunner captures `usage` + `total_cost_usd` from the SDK's terminal `result` event, ChatSessions accumulates cumulative-per-conv totals (input/output/cache_read/cache_creation/cost_usd/turns), `chat.usage` WS event fires after each turn final, and `/chat/snapshot.convs[].usage` exposes the cumulative dict on every snapshot. Cockpit can render `12.3k in · 4.5k out · $0.15` per agent. 1.13.2 — anchor-strip-final fix. The Claude SDK `result` event was bypassing the per-delta anchor stripper (which only runs on `_cumulative_text`). When the SDK emitted a final `result` block, daemon preferred that text and the leading `⟦anchor⟧ {...}` line + any `⟦anchor-progress⟧ {...}` lines leaked into the persisted timeline + the broadcast `chat.assistant.final`. New `_strip_all_anchor_markers` sweep applied to `final_text` before persisting. Pure scrubbing — the side-effects already ran during streaming. Operator field report 2026-06-12: agent A108 anchor marker visible in chat bubble. 1.13.1 — SRL2 state-recovery-loop snapshot expansion. `/chat/snapshot` (and `/chat/convs`) now carry, for each live conv, a `current_turn` dict (started_at + stream_id + partial_text up to 16 KB + tool_calls_count + deltas_seen) and a `queue` list (the in-memory ChatSessions.pending). Lets a cockpit that just connected mid-turn rehydrate the assistant bubble exactly where it was — restores "Reviewing the roadmap…" output + QUEUED user bubbles + the "preparing" indicator after a browser refresh. Both fields are OPTIONAL on the wire — older cockpits ignore them. New feature: daemon.snapshot.turn_state.v1. SRL1 (e647746) added ChatRunner.started_at / deltas_seen / tool_calls_count attrs that SRL2 reads via getattr. 1.13.0 — LAL3 live-anchor-loop side-effects. The single weak "Reply concisely" line in `_section_core_rules` is replaced by a prominent `## Output contract` section that EVERY agent type inherits every turn (previously only the architect had a LENGTH BUDGETS table; custom/audit/deploy/db/docs had no length guidance, so an audit-style turn dumped ~50 lines). The contract mandates: lead with a ≤8-line summary (problem + files touched + N-step plan), put ALL detail inside native HTML `<details>` blocks (one per file/topic, blank line after `</summary>` so inner markdown renders), no detail-prose at the top level, no process narration. Pairs with cockpit V107.36 which renders `<details>` natively + stops auto-expanding fresh finals (the auto-expand was un-clamping the very 50-line walls the operator complained about). Operator field report 2026-06-12: agent A108 audit reply was unreadable. Convention: `.meshkore/docs/conventions/output-contract.md`. 1.13.3 — model pass-through (MP1) + chat usage broadcast (CU1). Two initiatives shipped together. (a) The cockpit's NewAgentWizard model picker (auto/opus/sonnet/haiku) now wires end-to-end: `/chat/dispatch` accepts `model`, stored in conv_meta, ChatRunner.spawn injects `--model <id>` into claude-code argv (skipped when `auto`/None — lets the CLI use its default). Chained turns inherit. (b) ChatRunner captures `usage` + `total_cost_usd` from the SDK's terminal `result` event, ChatSessions accumulates cumulative-per-conv totals (input/output/cache_read/cache_creation/cost_usd/turns), `chat.usage` WS event fires after each turn final, and `/chat/snapshot.convs[].usage` exposes the cumulative dict on every snapshot. Cockpit can render `12.3k in · 4.5k out · $0.15` per agent. 1.13.2 — anchor-strip-final fix. The Claude SDK `result` event was bypassing the per-delta anchor stripper (which only runs on `_cumulative_text`). When the SDK emitted a final `result` block, daemon preferred that text and the leading `⟦anchor⟧ {...}` line + any `⟦anchor-progress⟧ {...}` lines leaked into the persisted timeline + the broadcast `chat.assistant.final`. New `_strip_all_anchor_markers` sweep applied to `final_text` before persisting. Pure scrubbing — the side-effects already ran during streaming. Operator field report 2026-06-12: agent A108 anchor marker visible in chat bubble. 1.13.1 — SRL2 state-recovery-loop snapshot expansion. `/chat/snapshot` (and `/chat/convs`) now carry, for each live conv, a `current_turn` dict (started_at + stream_id + partial_text up to 16 KB + tool_calls_count + deltas_seen) and a `queue` list (the in-memory ChatSessions.pending). Lets a cockpit that just connected mid-turn rehydrate the assistant bubble exactly where it was — restores "Reviewing the roadmap…" output + QUEUED user bubbles + the "preparing" indicator after a browser refresh. Both fields are OPTIONAL on the wire — older cockpits ignore them. New feature: daemon.snapshot.turn_state.v1. SRL1 (e647746) added ChatRunner.started_at / deltas_seen / tool_calls_count attrs that SRL2 reads via getattr. 1.13.0 — LAL3 live-anchor-loop side-effects.
# 1.12.8 — architect curation-vs-execution rule. Operator field report 2026-06-02: after asking the architect to "review the roadmap", tasks the architect curated (trimmed body, fixed frontmatter cosmetic fields) ended up with `status: active` and stayed yellow/blinking in the cockpit, with no agent alive on them. Added explicit FORBIDDEN rule: setting `status: active` on a task purely to claim it for editing/curation is forbidden. `active` means a coder subagent is dispatched against this task RIGHT NOW (`activeTaskIds().has(task.id)`). Curating the body / fixing tags / trimming verbose intros is curation — leave `status` untouched. Pairs with TaskCard.tsx fix that removed the pulse animation from `status: active` alone — pulse is now reserved for the live-agent branch.
# 1.12.7 — architect no-disguised-no-ops rule. Operator field report 2026-06-02: a 2-min Run-all pass closed 3 initiatives looking like real work — architect had only touched mtimes (re-wrote 21 files with identical content) to kick the daemon's stale in-memory `serverStore` view. Disk + HEAD both already said `status: done` for everything; the rewrite was cosmetic. Added explicit FORBIDDEN rule + correct behaviour spec (cite SHA, recommend /reload, no fake diary entry). 1.12.4 initiative status consistency guard preserved.
# 1.12.3 — deploy escalation boundary. Added to architect's DECISION MATRIX 3 dedicated rows for handling `deploy` agent `✗` returns: (a) build/code error in app source → dispatch focused custom coder + re-dispatch deploy; (b) infra-only issue → re-dispatch deploy with edit-authorisation; (c) post-deploy verification mismatch → diagnose propagation, then `blocked: deploy-unverified` after 2 attempts. The `deploy` agent prompt gained an explicit BOUNDARY section listing files it CAN edit (wrangler.toml, fly.toml, links.yaml, deploy scripts, READMEs) vs files it CANNOT edit (apps/*/src, packages/*/src, business logic, tests, migrations). Closes the operator field-report bug where the deploy agent silently failed on a Next.js edge-incompat import and reported `✓ deploy done` while cavioca.com served the previous version for 13h.
# 1.12.2 — agent honesty pass. Two prompt fixes from operator field report 2026-05-31:
#   (a) `deploy` agent prompt completely rewritten — mandatory read of `.meshkore/links.yaml` + `.meshkore/modules/<id>/README.md` + `.meshkore/credentials/` BEFORE acting; mandatory post-deploy verification via provider CLI OR curl-against-prod.url with version match; explicit "deploy isn't done until verified" rule. Closes the bug where the agent shipped a `partial-pass` smoke + a `web-build-failed` component and still reported `✓ deploy done` on the top line.
#   (b) Commit cadence in the architect prompt now mandates VERIFY-BEFORE-CLAIMING-DONE for ALL agent types (code → build exit 0, deploy → curl/CLI version match, db → SELECT read-back, testing → actual test run) + HONEST REPORTING with `✓` vs `✗` as the first character. Stops the false-positive success pattern across the whole fleet.
# Periodic VersionWatcher (py-1.12.1) + 4 dispatch invariants (py-1.12.0) preserved.
# 1.12.1 — periodic VersionWatcher thread polls the CDN for upgrades every cluster.yaml.daemon.auto_update_check_interval_sec (default 1800s / 30min). When a newer DAEMON_VERSION is published AND no chat session is in flight AND cluster.yaml.daemon.auto_update is true, the watcher self-invokes /self-update so the cluster stays current without operator action. Designed for fleet-scale operation: 100 daemons keep themselves fresh on the same cadence the CDN ships. The 4 safety nets from 1.12.0 still apply. Architect prompt strengthened with explicit phase-order (foundation→build→test→ship) + depends_on reading instruction (operator field report 2026-05-31: architect picked tasks in apparent random order).
# 1.12.0 — roadmap safety net. 4 NEW invariants on top of the 1.10.25/.28 set, all enforced server-side at chat_dispatch time:
#   Invariant 4 — Wave cap. At most WAVE_CAP (default 3, cluster.yaml.architect.wave_cap) work-* subagents alive at once per parent_conv. Bounds quota burn during a wave + prevents architect prompt bugs from spawning 7 parallel.
#   Invariant 5 — Required join keys. work-* conv dispatch MUST carry both initiative_id AND task_id. Closes the bypass where dispatch without these fields skipped Invariants 2+3.
#   Invariant 6 — Depends-on gate. Task being dispatched must have its `depends_on:` frontmatter satisfied (every referenced task is `done`). Refuses 409 with the missing list. Prevents the architect from racing a downstream task before its upstream finishes.
#   Invariant 7 — Claimed-commit verification. The wake hook classifier now runs `git cat-file -e <sha>` on every commit hash the subagent claimed. If the sha doesn't exist in the repo, the verdict is downgraded from 'success' to 'no-commit' so the architect doesn't credit phantom work. Catches subagents that hallucinate commit SHAs.
# Together: tighter token spend (wave cap), no ghost commits accepted as done (verification), no impossible dispatches accepted (depends_on), no bypasses of the linear-init policy (required join keys). py-1.11.3 credentials CRUD preserved.

# ── TLS bundle (D-TLS-01) ─────────────────────────────────────────────
# Wildcard cert for *.daemon.meshkore.com (public CF A record → 127.0.0.1)
# so the cockpit at architect.meshkore.com can talk to localhost over
# HTTPS+WSS without mixed-content / Chrome Local Network Access Issues.
# Bundled cert + key are intentionally "public" (only useful for
# impersonating daemon.meshkore.com on the attacker's own loopback,
# a no-op). The daemon falls back to plain HTTP if the bundle is
# missing — backwards-compatible with operators who haven't pulled
# the tls/ directory.
# DM3 — Paths + TLS constants live in daemon/paths.py. ChatArchive,
# StorageReport, UploadStore, ChatQueueManager live in daemon/storage.py.
# Sibling imports moved to the top of the file; the bundler strips
# them and inlines the modules in dependency order.

# Max number of timeline events to surface in /state.timeline.recent_events.
# The architect needs these to rebuild chat history + task lifecycle on
# every reload — without them, conv history vanishes from the cockpit
# even though the JSONL files on disk are intact. Bound to keep state.json
# small enough to serve cheaply; everything older is still readable from
# the per-day JSONL files in .meshkore/timeline/.
TIMELINE_RECENT_LIMIT = 500
MAX_BODY_BYTES = 4 * 1024 * 1024  # 4 MB — protect against runaway POSTs
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# Paths — moved to daemon/paths.py (DM3, py-1.12.25)


# ───────────────────────────────────────────────────────────────────────
# Cron scheduler — schema (D-CRON-01)
#
# Job definitions live in `cluster.yaml.crons:` (committed, travels with
# the repo). Runtime state lives in `.meshkore/.runtime/crons.json`
# (gitignored, per-machine). Only the daemon whose `device_id` matches
# `cluster.yaml.crons_owner` fires jobs; peers tick + emit
# `cron.would_have_fired` events. See
# `.meshkore/docs/conventions/cluster-yaml-crons.md` for the full
# schema reference and `.meshkore/docs/architecture/daemon.md` for the
# tick-loop diagram.

# Allowed values — typed as plain string sets so we keep stdlib-only.
_CRON_RUN_STATUSES = frozenset(
    {
        "pending",
        "running",
        "ok",
        "failed",
        "interrupted",
        "timeout",
    }
)
_CRON_RESTART_POLICIES = frozenset({"never", "on-failure", "always"})

# Defaults applied when a `crons:` entry omits the field.
_CRON_DEFAULTS = {
    "enabled": True,
    "max_runtime_sec": 7200,  # 2h
    "restart_policy": "never",
    "retention_runs": 30,
    "destructive": False,
}


# py-1.11.3 — Credentials CRUD constants.
#
# Names must be filesystem-safe and reasonably short. Pattern lets the
# operator use kebab/snake/dot conventions (cloudflare-token,
# openrouter.env, fly_org_id) without ever escaping the credentials
# directory.
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


def _validate_cron_expr(expr: str) -> Optional[str]:
    """Lightweight validation. Full parsing lands in D-CRON-02. Here we
    only need to reject obviously malformed values at config load so the
    daemon doesn't carry junk into the scheduler later.

    Returns None on OK, or a short error message string on reject.
    Accepts 5 space-separated fields. Each field is non-empty and
    consists of characters from [0-9*/,\\-]. Quartz (6 fields with
    seconds), `@daily`-style aliases, and the `L/W/#` modifiers are
    explicitly NOT supported in v1.
    """
    if not isinstance(expr, str) or not expr.strip():
        return "schedule must be a non-empty string"
    fields = expr.strip().split()
    if len(fields) != 5:
        return f"schedule must have 5 space-separated fields, got {len(fields)}"
    allowed = set("0123456789*/,-")
    for i, f in enumerate(fields):
        if not f:
            return f"schedule field {i} is empty"
        if not set(f).issubset(allowed):
            return f"schedule field {i} ({f!r}) contains unsupported characters"
    return None


def _validate_crons_block(
    data: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Validates the `crons:` section of cluster.yaml in isolation.
    Returns (cleaned_jobs, errors). Bad entries are skipped (not raised)
    so a single broken job doesn't disable the entire scheduler.

    Each returned job has defaults filled in and the schema's shape
    enforced. Invariants:
      - id is a non-empty kebab-case string, unique within the list
      - cmd is non-empty string
      - schedule passes _validate_cron_expr
      - restart_policy is in _CRON_RESTART_POLICIES
      - env values are strings
    """
    raw = data.get("crons") or []
    if not isinstance(raw, list):
        return [], ["crons: must be a list"]
    out: List[Dict[str, Any]] = []
    errors: List[str] = []
    seen_ids: set = set()
    for idx, entry in enumerate(raw):
        if not isinstance(entry, dict):
            errors.append(f"crons[{idx}] is not a dict — skipped")
            continue
        cid = entry.get("id")
        if not isinstance(cid, str) or not cid.strip():
            errors.append(f"crons[{idx}] missing id — skipped")
            continue
        cid = cid.strip()
        if cid in seen_ids:
            errors.append(f"crons[{idx}] duplicate id {cid!r} — skipped")
            continue
        cmd = entry.get("cmd")
        if not isinstance(cmd, str) or not cmd.strip():
            errors.append(f"crons[{cid}] missing cmd — skipped")
            continue
        sched = entry.get("schedule")
        sched_err = (
            _validate_cron_expr(sched) if isinstance(sched, str) else "schedule missing"
        )
        if sched_err:
            errors.append(f"crons[{cid}] {sched_err} — skipped")
            continue
        policy = entry.get("restart_policy", _CRON_DEFAULTS["restart_policy"])
        if policy not in _CRON_RESTART_POLICIES:
            errors.append(
                f"crons[{cid}] restart_policy={policy!r} not in "
                f"{sorted(_CRON_RESTART_POLICIES)} — defaulting to 'never'"
            )
            policy = "never"
        env = entry.get("env") or {}
        if not isinstance(env, dict):
            errors.append(f"crons[{cid}] env must be a dict — replaced with empty")
            env = {}
        env_clean: Dict[str, str] = {}
        for k, v in env.items():
            if not isinstance(k, str) or not isinstance(v, str):
                errors.append(
                    f"crons[{cid}] env {k!r}: values must be strings — dropped"
                )
                continue
            env_clean[k] = v

        cleaned = {
            "id": cid,
            "name": str(entry.get("name") or cid),
            "schedule": sched.strip(),
            "cmd": cmd.strip(),
            "cwd": entry.get("cwd"),
            "env": env_clean,
            "enabled": bool(entry.get("enabled", _CRON_DEFAULTS["enabled"])),
            "max_runtime_sec": int(
                entry.get("max_runtime_sec", _CRON_DEFAULTS["max_runtime_sec"])
            ),
            "restart_policy": policy,
            "retention_runs": int(
                entry.get("retention_runs", _CRON_DEFAULTS["retention_runs"])
            ),
            "destructive": bool(
                entry.get("destructive", _CRON_DEFAULTS["destructive"])
            ),
        }
        out.append(cleaned)
        seen_ids.add(cid)
    return out, errors


# ───────────────────────────────────────────────────────────────────────
# Tiny YAML reader + frontmatter parser — relocated to utils.py
# (DM-modularize-2). `parse_simple_yaml` / `parse_frontmatter` are
# re-imported from utils above so `daemon.parse_simple_yaml` stays a
# stable attribute for callers and tests.


# ───────────────────────────────────────────────────────────────────────
# Cluster + state


class Cluster:
    def __init__(self, paths: Paths):
        self.paths = paths
        self.data: Dict[str, Any] = {}
        # Cron scheduler (D-CRON-01): validated job set + ownership.
        # Populated by reload(); empty + None until a `crons:` block
        # appears in cluster.yaml.
        self.crons: List[Dict[str, Any]] = []
        self.crons_owner: Optional[str] = None
        self.reload()

    def reload(self) -> None:
        if not self.paths.cluster_yaml.exists():
            raise SystemExit(
                f"\n .meshkore/public/cluster.yaml not found at {self.paths.cluster_yaml}."
                "\n   Run `meshcore init` (or hand-author cluster.yaml from"
                "\n   https://meshkore.com/reference/cluster/templates/) and re-run.\n"
            )
        self.data = parse_simple_yaml(self.paths.cluster_yaml.read_text())
        # Validate the cron block last so a bad config logs warnings but
        # never blocks the daemon's other features.
        self.crons, errs = _validate_crons_block(self.data)
        for e in errs:
            _log(f"cluster.yaml crons: {e}")
        owner = self.data.get("crons_owner")
        self.crons_owner = (
            owner.strip() if isinstance(owner, str) and owner.strip() else None
        )
        if self.crons and not self.crons_owner:
            _log(
                "cluster.yaml has crons: but no crons_owner — scheduler will tick but never fire"
            )

    @property
    def id(self) -> str:
        return str(self.data.get("id") or "unknown")

    @property
    def name(self) -> str:
        return str(self.data.get("name") or self.id)

    @property
    def type(self) -> str:
        return str(self.data.get("type") or "dev")

    @property
    def architect_port(self) -> Optional[int]:
        # cluster.yaml.architect.port (preferred) → fall back to legacy portal.port
        for key in ("architect", "portal"):
            sec = self.data.get(key)
            if isinstance(sec, dict) and "port" in sec:
                try:
                    return int(sec["port"])
                except (TypeError, ValueError):
                    pass
        return None

    @property
    def modules(self) -> List[Dict[str, Any]]:
        m = self.data.get("modules") or []
        return m if isinstance(m, list) else []


# ───────────────────────────────────────────────────────────────────────
# Links + Protocols registries relocated to registries.py (DM-modularize-3).
# daemon.py re-imports LinksRegistry / ProtocolsRegistry / _split_frontmatter
# near the top.


def build_state(paths: Paths, cluster: Cluster) -> Dict[str, Any]:
    """Walk the FS and produce a state.json equivalent — the same shape
    the architect's renderInitiativesPanel + renderTasksList expect."""
    tasks: List[Dict[str, Any]] = []
    docs: List[Dict[str, Any]] = []
    initiatives: List[Dict[str, Any]] = []
    by_module: Dict[str, List[str]] = {}
    stats = {
        "backlog": 0,
        "next": 0,
        "in_progress": 0,
        "active": 0,
        "blocked": 0,
        "done": 0,
        "total": 0,
    }

    # Tasks live at .meshkore/modules/<id>/tasks/*.md (+ archived under log/)
    if paths.modules_dir.exists():
        for mdir in paths.modules_dir.iterdir():
            if not mdir.is_dir():
                continue
            mid = mdir.name
            by_module.setdefault(mid, [])
            for tasks_dir in (mdir / "tasks", mdir / "log"):
                if not tasks_dir.exists():
                    continue
                for md in tasks_dir.rglob("*.md"):
                    if md.name.startswith("_"):
                        continue
                    try:
                        text = md.read_text(errors="replace")
                    except OSError:
                        continue
                    fm = parse_frontmatter(text)
                    if not fm.get("id"):
                        continue
                    t = {
                        "id": str(fm.get("id")),
                        "title": str(fm.get("title") or fm["id"]),
                        "status": normalize_status(fm.get("status")),
                        "priority": str(fm.get("priority") or "medium"),
                        "owner": str(fm.get("owner") or "unknown"),
                        "category": str(fm.get("category") or mid),
                        "created": str(fm.get("created") or ""),
                        "updated": str(fm.get("updated") or ""),
                        "tags": fm.get("tags")
                        if isinstance(fm.get("tags"), list)
                        else [],
                        "depends_on": fm.get("depends_on")
                        if isinstance(fm.get("depends_on"), list)
                        else [],
                        "initiative": str(fm.get("initiative") or "") or None,
                        "path": str(md.relative_to(paths.root)),
                    }
                    tasks.append(t)
                    by_module[t["category"]] = by_module.get(t["category"], []) + [
                        t["id"]
                    ]
                    stats[t["status"]] = stats.get(t["status"], 0) + 1
                    stats["total"] += 1

    # Docs
    if paths.docs_dir.exists():
        for md in paths.docs_dir.rglob("*.md"):
            if md.name in ("INDEX.md", "README.md"):
                continue
            try:
                text = md.read_text(errors="replace")
            except OSError:
                continue
            fm = parse_frontmatter(text)
            if not fm:
                continue
            docs.append(
                {
                    "title": str(fm.get("title") or md.stem),
                    "category": str(fm.get("category") or ""),
                    "tags": fm.get("tags") if isinstance(fm.get("tags"), list) else [],
                    "updated": str(fm.get("updated") or ""),
                    "owner": str(fm.get("owner") or ""),
                    "status": str(fm.get("status") or "draft"),
                    "path": str(md.relative_to(paths.root)),
                }
            )

    # Initiatives
    if paths.initiatives.exists():
        for md in paths.initiatives.glob("*.md"):
            try:
                text = md.read_text(errors="replace")
            except OSError:
                continue
            fm = parse_frontmatter(text)
            if not fm.get("id"):
                continue
            child_ids = [
                t["id"] for t in tasks if (t.get("initiative") or "") == fm["id"]
            ]
            initiatives.append(
                {
                    "id": str(fm["id"]),
                    "title": str(fm.get("title") or fm["id"]),
                    "status": str(fm.get("status") or "backlog"),
                    "priority": str(fm.get("priority") or "medium"),
                    "oneliner": str(fm.get("oneliner") or ""),
                    "modules": fm.get("modules")
                    if isinstance(fm.get("modules"), list)
                    else [],
                    "target": str(fm.get("target") or ""),
                    "owner": str(fm.get("owner") or ""),
                    "created": str(fm.get("created") or ""),
                    "updated": str(fm.get("updated") or ""),
                    "child_task_ids": child_ids,
                    "task_total": len(child_ids),
                    "path": str(md.relative_to(paths.root)),
                    # py-1.10.15 — Roadmap ordering (initiative
                    # `roadmap-ordering-archive`). The operator curates
                    # order via a linked-list pointer in each .md
                    # frontmatter; absent/dangling pointers degrade to
                    # bucket-sort below. `completed_at` + `commit_sha`
                    # populate when the daemon auto-archives the
                    # initiative (D-RM-ARCHIVE-02).
                    "next": (str(fm.get("next")) if fm.get("next") else None),
                    "completed_at": str(fm.get("completed_at") or "") or None,
                    "commit_sha": str(fm.get("commit_sha") or "") or None,
                }
            )

    # py-1.10.15 — Auto-archive reconcile pass (D-RM-ARCHIVE-02).
    # MUST run before the linked-list sort so newly-archived items
    # land in the `done` bucket (the bottom of the active section).
    _reconcile_initiative_archive(initiatives, tasks, paths)

    # py-1.10.15 — Linked-list ordering (D-RM-LINKED-01). Walks the
    # operator-curated `next:` chain, then bucket-sorts by:
    #   0 = active/next with task_total > 0
    #   1 = active/next with task_total == 0 (empty-at-bottom)
    #   2 = backlog
    #   3 = done (archived view filters from here)
    initiatives = _order_initiatives(initiatives)

    # py-1.11.1 — `timeline.recent_events` removed from /state. The
    # cockpit lazy-loads per-conv history via GET /chat/conv/<id>/messages
    # when the operator focuses the conv; the boot snapshot
    # (/chat/snapshot) carries enough conv metadata to render the rail
    # immediately without replaying any events.
    return {
        "$schema": "https://meshkore.com/standard.json",
        "cluster": {
            "id": cluster.id,
            "name": cluster.name,
            "type": cluster.type,
        },
        "modules": cluster.modules,
        "roadmap": {
            "tasks": tasks,
            "stats": stats,
        },
        "docs": docs,
        "initiatives": initiatives,
        "generated_at": _iso_now(),
        "generator": {"name": "meshcore-py", "version": DAEMON_VERSION},
    }


# ── Roadmap ordering (initiative `roadmap-ordering-archive`) ──────────
# Operator curates initiative order via a `next: <id>` pointer in each
# `.meshkore/roadmap/initiatives/<id>.md` frontmatter. The daemon walks
# the chain, then bucket-sorts so:
#   • initiatives without tasks NEVER appear as the head of the list
#     (they can't be acted on — Run All can't dispatch them);
#   • done initiatives drop to the bottom of the payload, where the
#     cockpit's `vis=archived` filter picks them up chronologically;
#   • broken / missing `next:` degrades gracefully (orphans fall to end
#     of their bucket, sorted by `updated`).
# No central index file: the operator's directive is "modifica las
# historias relacionadas". Three writes max to move one item.

_ORDER_BUCKET_ACTIVE_WITH_TASKS = 0
_ORDER_BUCKET_ACTIVE_NO_TASKS = 1
_ORDER_BUCKET_BACKLOG = 2
_ORDER_BUCKET_DONE = 3


def _order_initiatives(initiatives: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Linked-list walk + bucket sort. Deterministic + stable."""
    if not initiatives:
        return initiatives
    by_id = {it["id"]: it for it in initiatives}
    edges: Dict[str, str] = {}
    for it in initiatives:
        nxt = it.get("next")
        if nxt and nxt in by_id and nxt != it["id"]:
            edges[it["id"]] = nxt

    pointed_to = set(edges.values())
    # Heads = initiatives no one points to. Stable order: by `updated` desc
    # then id asc, so the most recently-touched chain leads when there
    # are several disconnected lists.
    heads = sorted(
        [i for i in by_id.keys() if i not in pointed_to],
        key=lambda i: (-_sortable_ts(by_id[i].get("updated")), i),
    )

    visited: set[str] = set()
    walked: List[str] = []
    for h in heads:
        cur: Optional[str] = h
        while cur is not None and cur not in visited:
            visited.add(cur)
            walked.append(cur)
            cur = edges.get(cur)
    # Orphans (everything not visited — i.e. members of a pure cycle):
    # append in `updated` order so they don't randomly shuffle.
    orphans = sorted(
        [i for i in by_id.keys() if i not in visited],
        key=lambda i: (-_sortable_ts(by_id[i].get("updated")), i),
    )
    flat_ids = walked + orphans

    def bucket(it: Dict[str, Any]) -> int:
        status = normalize_status(it.get("status"))
        if status == "done":
            return _ORDER_BUCKET_DONE
        if status == "backlog":
            return _ORDER_BUCKET_BACKLOG
        # active / next / in_progress / blocked
        if int(it.get("task_total") or 0) > 0:
            return _ORDER_BUCKET_ACTIVE_WITH_TASKS
        return _ORDER_BUCKET_ACTIVE_NO_TASKS

    # Stable sort: Python's `sorted` preserves the linked-list order
    # within each bucket.
    ordered_items = sorted(
        [by_id[i] for i in flat_ids],
        key=bucket,
    )
    return ordered_items


def _sortable_ts(v: Any) -> float:
    """Best-effort ISO/YYYY-MM-DD → epoch seconds. 0 on parse failure
    so items without `updated` sort last (because we negate the key)."""
    s = str(v or "").strip()
    if not s:
        return 0.0
    try:
        # Strip trailing Z if present (Python 3.10 fromisoformat doesn't
        # accept it on older versions; 3.11+ does — be conservative).
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).timestamp()
    except (ValueError, TypeError):
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").timestamp()
        except (ValueError, TypeError):
            return 0.0


def _reconcile_initiative_archive(
    initiatives: List[Dict[str, Any]],
    tasks: List[Dict[str, Any]],
    paths: "Paths",
) -> None:
    """Bidirectional reconcile between initiative status + child tasks.

    py-1.10.15 (forward): active → done when ALL tasks done.
    py-1.12.4 (BACKWARD): done → active when ANY task is not done.
      Closes the bug where the architect prematurely set
      `status: done` on a partial initiative (operator field report
      2026-05-31: Visual identity v2 archived with 5/7 done). The
      cockpit was showing the initiative as DONE in the archived
      view despite 2 tasks still pending. The architect's prompt
      already forbids this, but a server-side guard is the only
      reliable defense.

    For both directions: idempotent — the second pass sees the
    consistent state and skips.
    """
    # tasks_by_initiative reused so we don't iterate N×M times.
    children: Dict[str, List[Dict[str, Any]]] = {}
    for t in tasks:
        iid = t.get("initiative")
        if iid:
            children.setdefault(iid, []).append(t)

    head_sha: Optional[str] = None
    head_sha_attempted = False
    iso_now = _iso_now()

    for it in initiatives:
        status = normalize_status(it.get("status"))
        if status == "backlog":
            continue
        kids = children.get(it["id"], [])
        if not kids:
            continue

        all_done = all(normalize_status(k.get("status")) == "done" for k in kids)

        # ── Forward path: active/next/in_progress → done ────────────
        if status != "done" and all_done:
            if not head_sha_attempted:
                head_sha_attempted = True
                head_sha = _git_head_sha(paths.root)
            new_fields = {
                "status": "done",
                "completed_at": iso_now,
            }
            if head_sha:
                new_fields["commit_sha"] = head_sha
            try:
                fp = paths.root / it["path"]
                if _patch_frontmatter(fp, new_fields):
                    it["status"] = "done"
                    it["completed_at"] = iso_now
                    if head_sha:
                        it["commit_sha"] = head_sha
                    _log(
                        f"roadmap: auto-archived initiative {it['id']} "
                        f"({len(kids)} tasks done, commit={head_sha or 'none'})"
                    )
                    _debug_emit(
                        "init-archive",
                        msg=f"initiative {it['id']} auto-archived",
                        data={
                            "initiative_id": it["id"],
                            "tasks_done": len(kids),
                            "commit_sha": head_sha,
                            "completed_at": iso_now,
                        },
                    )
            except OSError as e:
                _log(f"roadmap: archive write failed for {it['id']}: {e}")
            continue

        # ── Backward path (py-1.12.4): done → active when partial ───
        if status == "done" and not all_done:
            pending = [
                k.get("id") for k in kids if normalize_status(k.get("status")) != "done"
            ]
            new_fields = {"status": "active"}
            # Wipe the completion markers — they're lying.
            for stale in ("completed_at", "commit_sha"):
                if it.get(stale):
                    new_fields[stale] = None  # _patch_frontmatter removes nulls
            try:
                fp = paths.root / it["path"]
                if _patch_frontmatter(fp, new_fields):
                    it["status"] = "active"
                    it.pop("completed_at", None)
                    it.pop("commit_sha", None)
                    _log(
                        f"roadmap: REVERTED initiative {it['id']} from done → active "
                        f"({len(pending)} task(s) still pending: {pending[:5]}"
                        f"{', …' if len(pending) > 5 else ''})"
                    )
                    _debug_emit(
                        "init-archive.reverted",
                        msg=f"initiative {it['id']} reverted: pending tasks remain",
                        lvl="warn",
                        data={
                            "initiative_id": it["id"],
                            "pending_task_ids": pending,
                            "total_tasks": len(kids),
                        },
                    )
            except OSError as e:
                _log(f"roadmap: revert write failed for {it['id']}: {e}")


def _git_head_sha(root: "Path") -> Optional[str]:
    """`git rev-parse HEAD` in `root`. Returns None if the cluster isn't
    a git repo or git is unavailable — auto-archive still proceeds with
    `commit_sha: null`."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode == 0:
            sha = proc.stdout.strip()
            return sha or None
    except (OSError, subprocess.SubprocessError):
        return None
    return None


def _patch_frontmatter(fp: "Path", patch: Dict[str, Any]) -> bool:
    """Idempotent frontmatter merge. Writes only the fields in `patch`
    that differ from current. Preserves field order: known fields keep
    their position, new fields append in `patch` order.

    py-1.12.4 — a `None` value in the patch REMOVES that key from the
    frontmatter (used by the bidirectional reconcile to wipe stale
    `completed_at` / `commit_sha` when a partially-done initiative is
    reverted from done → active).

    Returns True iff the file was actually rewritten."""
    text = fp.read_text(errors="replace")
    m = _FM_RE.match(text)
    if not m:
        # No frontmatter to patch — refuse rather than corrupt.
        return False
    fm_block = m.group(1)
    rest = text[m.end() :]
    current = parse_simple_yaml(fm_block)
    # Detect any actual change. A None patch entry counts as a change
    # iff the key currently exists.
    changed = False
    for k, v in patch.items():
        if v is None:
            if k in current and current.get(k) not in (None, ""):
                changed = True
                break
        else:
            if str(current.get(k) or "") != str(v):
                changed = True
                break
    if not changed:
        return False
    lines = fm_block.splitlines()
    handled: set[str] = set()
    new_lines: List[str] = []
    for line in lines:
        if ":" in line and not line.startswith((" ", "\t", "-", "#")):
            key = line.split(":", 1)[0].strip()
            if key in patch:
                handled.add(key)
                if patch[key] is None:
                    # Skip the line — that's the removal.
                    continue
                new_lines.append(f"{key}: {patch[key]}")
                continue
        new_lines.append(line)
    for k, v in patch.items():
        if k in handled or v is None:
            continue
        new_lines.append(f"{k}: {v}")
    new_fm = "\n".join(new_lines)
    if not new_fm.endswith("\n"):
        new_fm += "\n"
    new_text = "---\n" + new_fm + "---\n" + rest.lstrip("\n")
    tmp = fp.with_suffix(fp.suffix + ".tmp")
    tmp.write_text(new_text)
    os.replace(tmp, fp)
    return True


def normalize_status(s: Any) -> str:
    s = str(s or "backlog").lower()
    if s in ("in_progress", "in-progress"):
        return "active"
    if s in ("backlog", "next", "active", "blocked", "done"):
        return s
    return "backlog"


# ───────────────────────────────────────────────────────────────────────
# Chat coordinator runner (U-DAEMON-05 + 06)
#
# Replaces the Node spawnCoordinatorChat + chatSessions pair from
# `daemon/src/server.ts`. Same protocol on the wire — the cockpit's
# `daemon-client.ts` is unchanged. Differences from the Node port:
# explicit, no worker pool yet (sessions don't carry --session-id /
# --resume across turns yet; that lands with U-DAEMON-07 worker pool
# port). Conversation history is rebuilt from the timeline file on
# each turn so context survives daemon restarts.


# py-1.6.0 — Stable namespace for deterministic per-conv claude session
# ids. uuid5(NAMESPACE, conv_id) yields a valid UUID that's the same
# across daemon restarts → claude resumes the same session across turns
# (memory + prompt cache). Same conv id in two different MeshKore
# clusters will collide on UUID — fine, claude isolates sessions per
# project (cwd-scoped).
# _session_id_for_conv + _find_claude + _CLAUDE_SESSION_NAMESPACE relocated
# to runner.py (DM-modularize-2) — only ChatRunner used them.
# _session_id_for_conv is re-imported from runner above for callers/tests.


# _conversation_history relocated to prompts.py (DM-modularize-2) —
# only the briefing pipeline consumed it; re-imported there from utils.


# py-1.11.1 — `_recent_timeline_events` removed (it powered the boot
# replay channel /state.timeline.recent_events, deleted in Phase 2).
# Per-conv message reads now go through `Daemon.chat_conv_messages`
# which filters the same JSONL files by conv id with pagination.


# _append_timeline relocated to utils.py (DM-modularize-2) — shared by
# ChatRunner (runner.py) + the daemon's chat/user event writers; re-imported
# from utils above.


# ───────────────────────────────────────────────────────────────────────
# Briefing pipeline + AGENT_PROMPTS registry + ProjectState /
# StateIntegrityChecker relocated to prompts.py (DM-modularize-2).
# daemon.py re-imports the public names (AGENT_PROMPTS, _agent_manifest,
# _agent_type_normalised, _agent_type_from_conv_slug, BriefingPipeline)
# via `from prompts import ...` near the top so `daemon.X` stays stable.


# ───────────────────────────────────────────────────────────────────────
# ChatRunner relocated to runner.py (DM-modularize-2). daemon.py
# re-imports it via `from runner import ChatRunner` near the top so
# `daemon.ChatRunner` stays stable; Daemon._spawn_chat_turn constructs
# it with `daemon=self` (the intentional Daemon<->ChatRunner back-ref).


# ───────────────────────────────────────────────────────────────────────
# TimelineRotator + RunStore relocated to runs.py (DM-modularize-3).
# daemon.py re-imports them near the top.


# ───────────────────────────────────────────────────────────────────────
# Cron scheduler (D-CRON-02..05)
#
# Replaces every external scheduler (LaunchAgent, cron-tab, GH Actions
# cron). The Python daemon ticks every 10 s, decides which jobs are
# due based on `cluster.yaml.crons:` (validated by Cluster.reload —
# see D-CRON-01), and spawns a subprocess per due job via CronRunner.
# Only the daemon whose `device_id` matches `cluster.crons_owner`
# actually fires; peers emit `cron.would_have_fired` events.


def _parse_cron_field(field: str, lo: int, hi: int) -> set:
    """Parse one POSIX cron field (minute / hour / dom / month / dow)
    into the set of integers it matches. Supports: '*', 'A', 'A-B',
    'A,B,C', '*/N', 'A-B/N'. No L/W/# modifiers, no aliases."""
    out = set()
    for part in field.split(","):
        step = 1
        if "/" in part:
            base, step_s = part.split("/", 1)
            step = int(step_s)
        else:
            base = part
        if base == "*":
            start, end = lo, hi
        elif "-" in base:
            a, b = base.split("-", 1)
            start, end = int(a), int(b)
        else:
            n = int(base)
            start, end = n, n
        for v in range(start, end + 1, step):
            if lo <= v <= hi:
                out.add(v)
    return out


def _cron_next(expr: str, after: datetime) -> datetime:
    """Compute the next datetime > `after` that matches the 5-field
    POSIX cron expression. Walks forward minute-by-minute (bounded to
    ~4 years so a misconfigured expr fails loudly rather than spinning
    forever)."""
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"bad cron expression (need 5 fields): {expr!r}")
    minute_set = _parse_cron_field(parts[0], 0, 59)
    hour_set = _parse_cron_field(parts[1], 0, 23)
    dom_set = _parse_cron_field(parts[2], 1, 31)
    month_set = _parse_cron_field(parts[3], 1, 12)
    # Cron dow: Sunday=0..Saturday=6. Python's weekday(): Monday=0..Sunday=6.
    # Convert at match time with (py + 1) % 7.
    dow_set = _parse_cron_field(parts[4], 0, 6)
    t = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(60 * 24 * 366 * 4):
        if (
            t.minute in minute_set
            and t.hour in hour_set
            and t.month in month_set
            and t.day in dom_set
            and ((t.weekday() + 1) % 7) in dow_set
        ):
            return t
        t += timedelta(minutes=1)
    raise ValueError(f"no next match within 4 years for {expr!r}")


def _curated_path_entries() -> List[str]:
    """PATH entries we prepend to every cron child's env, so the cron
    can find `wrangler`, `flyctl`, `claude`, `node`, etc. regardless of
    how the daemon itself was launched. Solves the 2026-05-19 incident
    where the LaunchAgent's PATH didn't include nvm."""
    import glob as _glob

    out: List[str] = []
    candidates = [
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
    ]
    # Highest nvm Node version
    nvm = sorted(
        _glob.glob(os.path.expanduser("~/.nvm/versions/node/v*/bin")), reverse=True
    )
    if nvm:
        candidates.insert(0, nvm[0])
    for p in candidates:
        if os.path.isdir(p) and p not in out:
            out.append(p)
    return out


class CronRunner:
    """Spawns one subprocess per due job. Captures stdout+stderr to a
    per-run log file under `.meshkore/.runtime/logs/cron/<job_id>/<ts>.log`.
    Enforces `max_runtime_sec` with SIGTERM → 30 s → SIGKILL on the
    process group (so children of the spawned shell die too)."""

    def __init__(self, paths: Paths, cluster: Cluster, hub: Hub, identity: str):
        self.paths = paths
        self.cluster = cluster
        self.hub = hub
        self.identity = identity
        self.paths.crons_logs_dir.mkdir(parents=True, exist_ok=True)
        self._active: Dict[str, Any] = {}  # job_id → subprocess.Popen
        self._lock = threading.Lock()

    def is_running(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._active

    def spawn(
        self, job: Dict[str, Any], reason: str = "scheduled"
    ) -> Optional[Dict[str, Any]]:
        """Fire one run of `job`. Returns the started Run dict, or
        None if the job is already running (no concurrent fires)."""
        import subprocess

        jid = job["id"]
        with self._lock:
            if jid in self._active:
                self.hub.broadcast(
                    {
                        "type": "cron.skipped",
                        "id": jid,
                        "reason": "already running",
                        "ts": _iso_now(),
                    }
                )
                return None
        env = self._resolve_env(job.get("env") or {})
        log_path = self._make_log_path(jid)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        ts = _iso_now()
        try:
            log_handle = open(log_path, "ab")
            proc = subprocess.Popen(
                job["cmd"],
                shell=True,
                cwd=str(self.paths.root),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except Exception as e:
            _log(f"cron spawn FAIL {jid}: {e}")
            self.hub.broadcast(
                {
                    "type": "cron.error",
                    "id": jid,
                    "error": str(e),
                    "ts": ts,
                }
            )
            return None
        with self._lock:
            self._active[jid] = proc
        self.hub.broadcast(
            {
                "type": "cron.fired",
                "id": jid,
                "reason": reason,
                "pid": proc.pid,
                "log": str(log_path.relative_to(self.paths.root)),
                "ts": ts,
            }
        )
        run = {
            "id": jid,
            "started_at": ts,
            "pid": proc.pid,
            "log_path": str(log_path),
            "status": "running",
        }
        threading.Thread(
            target=self._wait_for,
            args=(jid, proc, log_handle, job, log_path, ts),
            daemon=True,
        ).start()
        return run

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            proc = self._active.get(job_id)
        if not proc or proc.poll() is not None:
            return False
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            threading.Timer(30.0, lambda: self._sigkill(job_id)).start()
            return True
        except (OSError, ProcessLookupError):
            return False

    def _sigkill(self, job_id: str) -> None:
        with self._lock:
            proc = self._active.get(job_id)
        if not proc or proc.poll() is not None:
            return
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (OSError, ProcessLookupError):
            pass

    def _wait_for(
        self,
        jid: str,
        proc,
        log_handle,
        job: Dict[str, Any],
        log_path: Path,
        started_at: str,
    ) -> None:
        timeout = int(job.get("max_runtime_sec", 7200))
        t0 = time.monotonic()
        while proc.poll() is None and (time.monotonic() - t0) < timeout:
            time.sleep(1)
        timed_out = proc.poll() is None
        if timed_out:
            self.hub.broadcast({"type": "cron.timeout", "id": jid, "ts": _iso_now()})
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                time.sleep(30)
                if proc.poll() is None:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass
        exit_code = proc.wait()
        try:
            log_handle.close()
        except Exception:
            pass
        with self._lock:
            self._active.pop(jid, None)
        status = "timeout" if timed_out else ("ok" if exit_code == 0 else "failed")
        self.hub.broadcast(
            {
                "type": "cron.finished",
                "id": jid,
                "exit": exit_code,
                "status": status,
                "duration_sec": round(time.monotonic() - t0, 1),
                "log": str(log_path.relative_to(self.paths.root)),
                "ts": _iso_now(),
            }
        )

    def _resolve_env(self, job_env: Dict[str, str]) -> Dict[str, str]:
        env = dict(os.environ)
        curated = _curated_path_entries()
        if curated:
            env["PATH"] = ":".join(curated) + ":" + env.get("PATH", "")
        for k, v in job_env.items():
            if not isinstance(v, str) or not isinstance(k, str):
                continue
            if v.startswith("file:"):
                rel = v[len("file:") :]
                full = Path(rel) if os.path.isabs(rel) else (self.paths.root / rel)
                try:
                    env[k] = full.read_text().strip()
                except OSError as e:
                    _log(f"cron env: cannot read {full}: {e}")
            elif v.startswith("$"):
                env[k] = os.environ.get(v[1:], v)
            else:
                env[k] = os.path.expandvars(os.path.expanduser(v))
        return env

    def _make_log_path(self, job_id: str) -> Path:
        d = self.paths.crons_logs_dir / job_id
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return d / f"{ts}.log"


class CronScheduler:
    """Tick loop. Every TICK_SEC seconds: check each registered job,
    fire any whose `next_run` has arrived (only if this daemon is the
    coordinator), advance `next_run` to the next future slot."""

    TICK_SEC = 10  # operator decision 2026-05-19

    def __init__(self, paths: Paths, cluster: Cluster, hub: Hub, identity: str):
        self.paths = paths
        self.cluster = cluster
        self.hub = hub
        self.identity = identity
        self.runner = CronRunner(paths, cluster, hub, identity)
        self._jobs: Dict[str, Dict[str, Any]] = {}  # job_id → {job, next_run}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._timer: Optional[threading.Timer] = None

    # ── coordinator gate ─────────────────────────────────────────────
    def is_coordinator(self) -> bool:
        owner = self.cluster.crons_owner
        # If no owner is declared but crons exist, the first daemon to
        # boot owns them — pragmatic default for single-machine setups.
        if not owner:
            return bool(self.cluster.crons)
        return owner == self.identity

    # ── load/reload ─────────────────────────────────────────────────
    def reload_jobs(self) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            self._jobs = {}
            for job in self.cluster.crons:
                try:
                    next_run = _cron_next(job["schedule"], now)
                except ValueError as e:
                    _log(f"cron {job['id']}: cannot compute next_run: {e}")
                    continue
                self._jobs[job["id"]] = {"job": job, "next_run": next_run}

    # ── lifecycle ───────────────────────────────────────────────────
    def start(self) -> None:
        self.reload_jobs()
        n = len(self._jobs)
        if n == 0:
            _log("cron: no jobs registered (cluster.yaml has no `crons:` block)")
        else:
            owner_status = (
                "coordinator"
                if self.is_coordinator()
                else f"peer (owner={self.cluster.crons_owner})"
            )
            _log(
                f"cron: {n} job(s) registered, this daemon is {owner_status}, tick every {self.TICK_SEC}s"
            )
            for jid, state in self._jobs.items():
                _log(f"  - {jid}: next_run={state['next_run'].isoformat()}")
        self._schedule_next_tick()

    def stop(self) -> None:
        self._stop.set()
        if self._timer:
            self._timer.cancel()

    def _schedule_next_tick(self) -> None:
        if self._stop.is_set():
            return
        self._timer = threading.Timer(self.TICK_SEC, self._tick)
        self._timer.daemon = True
        self._timer.start()

    def _tick(self) -> None:
        try:
            self._do_tick()
        except Exception as e:
            _log(f"cron tick error: {e}")
        self._schedule_next_tick()

    def _do_tick(self) -> None:
        now = datetime.now(timezone.utc)
        is_coord = self.is_coordinator()
        fires = []
        with self._lock:
            for jid, state in self._jobs.items():
                job = state["job"]
                if not job.get("enabled", True):
                    continue
                if state["next_run"] > now:
                    continue
                fires.append((jid, job, state["next_run"]))
                # Advance — catch-up: skip missed windows, jump to next future
                try:
                    state["next_run"] = _cron_next(job["schedule"], now)
                except ValueError:
                    pass
        for jid, job, scheduled_for in fires:
            if is_coord:
                self.runner.spawn(job, reason="scheduled")
            else:
                self.hub.broadcast(
                    {
                        "type": "cron.would_have_fired",
                        "id": jid,
                        "scheduled_for": scheduled_for.isoformat(),
                        "reason": f"not coordinator (owner={self.cluster.crons_owner!r}, me={self.identity!r})",
                        "ts": _iso_now(),
                    }
                )

    # ── introspection ───────────────────────────────────────────────
    def list_jobs(self) -> List[Dict[str, Any]]:
        out = []
        with self._lock:
            for jid, state in self._jobs.items():
                out.append(
                    {
                        **state["job"],
                        "next_run": state["next_run"].isoformat(),
                        "running": self.runner.is_running(jid),
                    }
                )
        return out

    def trigger(self, job_id: str, reason: str = "manual") -> Optional[Dict[str, Any]]:
        with self._lock:
            state = self._jobs.get(job_id)
        if not state:
            return None
        return self.runner.spawn(state["job"], reason=reason)


# ───────────────────────────────────────────────────────────────────────
# State manager — caches state + polls FS for changes


class StateManager:
    def __init__(self, paths: Paths, cluster: Cluster, hub: Hub):
        self.paths = paths
        self.cluster = cluster
        self.hub = hub
        self._state: Dict[str, Any] = {}
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._fs_signature = ""
        # Backref set by Daemon.__init__. Currently unused after the
        # py-1.11.1 chat-state cleanup (the `state()` method no longer
        # joins live chat data — that lives on /chat/snapshot now), but
        # kept around for future cross-system reads that may need a
        # daemon handle without a global lookup.
        self._daemon: Optional["Daemon"] = None
        self.rebuild()
        threading.Thread(target=self._poll_loop, daemon=True).start()

    def bind_daemon(self, daemon: "Daemon") -> None:
        self._daemon = daemon

    def state(self) -> Dict[str, Any]:
        # py-1.11.1 — `timeline.recent_events` and `chat_activity`
        # removed from /state. Chat lives on its own surface:
        # `/chat/snapshot` (boot conv list with live/coordinating/
        # waiting_on flags), `/chat/conv/<id>/messages` (paginated
        # history), `conv.*` WS events (live deltas). /state is now
        # purely cluster + modules + roadmap + docs.
        with self._lock:
            return dict(self._state)

    def rebuild(self, broadcast: bool = True) -> None:
        self.cluster.reload()
        with self._lock:
            self._state = build_state(self.paths, self.cluster)
            self._fs_signature = self._compute_signature()
        # Persist state.json so the legacy Node tooling can also read it.
        try:
            self.paths.roadmap_dir.mkdir(parents=True, exist_ok=True)
            self.paths.state_json.write_text(json.dumps(self._state, indent=2))
        except OSError:
            pass
        if broadcast:
            self.hub.broadcast({"type": "state.rebuilt", "ts": _iso_now()})

    def shutdown(self) -> None:
        self._stop.set()

    def _poll_loop(self) -> None:
        while not self._stop.wait(FS_POLL_SEC):
            try:
                sig = self._compute_signature()
                if sig != self._fs_signature:
                    self.rebuild(broadcast=True)
            except Exception:  # pragma: no cover — best-effort
                pass

    def _compute_signature(self) -> str:
        h = hashlib.sha1()
        for root in (
            self.paths.modules_dir,
            self.paths.docs_dir,
            self.paths.initiatives,
            self.paths.public,
        ):
            if not root.exists():
                continue
            for md in sorted(root.rglob("*")):
                if not md.is_file():
                    continue
                try:
                    st = md.stat()
                    h.update(str(md).encode())
                    h.update(struct.pack(">dq", st.st_mtime, st.st_size))
                except OSError:
                    pass
        return h.hexdigest()


# ───────────────────────────────────────────────────────────────────────
# HTTP / WebSocket server


class PoolHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer with a bounded worker pool (py-1.12.24+).

    The stdlib default spawns a fresh thread per request and never
    recycles. On a long-running daemon the OS thread count grows
    unboundedly; the 2026-06-10 ikamiro incident reached 18 000+ before
    the daemon was killed. With a pool of ``max_workers`` the count
    stays bounded; excess requests queue at the OS-accept layer (which
    has its own limits, much higher than any sane workload).
    ``cluster.yaml.daemon.http.max_workers`` overrides; default 64."""

    # py-1.15.1 — listen backlog. socketserver's default request_queue_size
    # is 5; the docstring's old claim that excess requests "queue at the
    # OS-accept layer (much higher than any sane workload)" was wrong — the
    # accept backlog WAS 5, so a cockpit boot burst (~30 concurrent fetches:
    # /state + /chat/snapshot + every initiative body + the WS upgrade)
    # overflowed it and the kernel REFUSED the excess connections. That is
    # the intermittent ERR_CONNECTION_REFUSED the cockpit hit mid-hydration
    # (stranding the boot panel) and the `test_pool_bounds` 18/50 result.
    # 128 absorbs any realistic single-cockpit burst.
    request_queue_size = 128

    def __init__(self, *args, max_workers: int = 64, **kw) -> None:
        super().__init__(*args, **kw)
        self.daemon_threads = True
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="http"
        )

    def process_request(self, request, client_address):  # type: ignore[override]
        self._pool.submit(self.process_request_thread, request, client_address)

    def server_close(self) -> None:  # type: ignore[override]
        try:
            self._pool.shutdown(wait=False, cancel_futures=True)
        finally:
            super().server_close()


# ───────────────────────────────────────────────────────────────────────
# Quota state (py-1.10.27 — initiative `quota-aware-dispatch`)
#
# Persistent per-(platform, model) rate-limit ledger. Tracks which
# upstream LLM pools are currently exhausted, with the exact expiry
# instant + history of probe attempts. Survives daemon restart at
# `.meshkore/.runtime/quota-state.json` so a quick relaunch doesn't
# lose the "Claude Pro window doesn't reset until 06:23 UTC" datum
# and waste tokens re-discovering it.
#
# Replaces the py-1.10.26 in-memory `_agent_type_pauses` dict.
# `/health.paused_agent_types` is kept as a back-compat projection so
# the existing cockpit banner keeps working without changes.


# ───────────────────────────────────────────────────────────────────────
# ChatSessionReaper (py-1.12.16)
#
# Background thread that periodically sweeps `ChatSessions` for slots
# whose subprocess has exited (or never spawned) but whose `done` event
# was never set — which would leave the conv marked `live: true` and
# every subsequent /chat/dispatch silently queued. The reaper:
#
#   1. Calls ChatSessions.reap_dead() — pops the orphan slots.
#   2. Broadcasts conv.activity {live: false} so cockpits drop the
#      stale "STOP" UI immediately.
#   3. Emits a `chat-session.reaped` debug event with the reason.
#
# It also runs once on daemon boot to clear any anomalies left from
# a forced shutdown (kill -9). On a normal boot ChatSessions is empty
# in memory, so the sweep is a no-op — defense in depth.
#
# Field-reported 2026-06-10 (IKA cluster, py-1.12.10): master conv had
# been stuck `live: true` for 2.5+ days because a subprocess ended
# without the runner's done.set() being reached. Operator: "el daemon
# debería gestionar eso, los usuarios no sabrán hacerlo ni deberían."


# ───────────────────────────────────────────────────────────────────────
# VersionWatcher (py-1.12.1)
#
# Background thread that periodically polls the CDN for newer
# daemon.py versions and self-invokes /self-update when the cluster
# is idle. Designed for fleet operation: an operator with 100 clients
# shouldn't need to log into each one to push an upgrade — the
# daemon sees the new version on CDN and rolls itself forward.
#
# Coexists with the BOOT self-update (`_boot_self_update_if_needed`)
# which only fires when the daemon starts. Long-running daemons (days
# of uptime, no restart) would never upgrade without this thread.
#
# Behavior
# ────────
#   • Tick interval: `cluster.yaml.daemon.auto_update_check_interval_sec`
#     (default 1800 = 30 min). Clamped 60-86400.
#   • Skips entirely when `cluster.yaml.daemon.auto_update: false`.
#   • Each tick:
#       1. Fetch the first ~1 KB of `auto_update_source` to read its
#          DAEMON_VERSION line. Cheap — single Range request.
#       2. Parse local + remote versions. If remote ≤ local, sleep.
#       3. If `chat_sessions.list_active()` non-empty → defer (log
#          "deferred until idle", emit `daemon.upgrade.deferred` WS).
#       4. Otherwise call `self.daemon.self_update({})` directly. The
#          method spawns the new daemon on a fresh port and schedules
#          this process's shutdown. Cockpits reconnect via the daemon
#          dedup-by-cluster_id path.
#   • Cooldown: 5 min after any attempt (successful or not) to avoid
#     hammering a misconfigured CDN or looping if the upgrade fails.


class VersionWatcher:
    """py-1.12.1 — Periodic CDN poll + idle-aware self-update for the
    long-uptime case. See module-level docstring above."""

    DEFAULT_TICK_SECS = 1800  # 30 min
    MIN_TICK_SECS = 60
    MAX_TICK_SECS = 86400
    COOLDOWN_AFTER_ATTEMPT_SECS = 300  # 5 min between attempts

    def __init__(self, daemon: "Daemon") -> None:
        self.daemon = daemon
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_attempt_ts: float = 0.0

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        _log(f"version-watcher: started (tick={self._tick_secs()}s)")

    def stop(self) -> None:
        self._stop.set()

    def _tick_secs(self) -> int:
        try:
            d = (
                self.daemon.cluster.data.get("daemon")
                if isinstance(self.daemon.cluster.data, dict)
                else None
            )
            raw = (d or {}).get("auto_update_check_interval_sec")
            if raw is None:
                return self.DEFAULT_TICK_SECS
            n = int(raw)
            return max(self.MIN_TICK_SECS, min(self.MAX_TICK_SECS, n))
        except Exception:
            return self.DEFAULT_TICK_SECS

    def _enabled(self) -> bool:
        try:
            d = (
                self.daemon.cluster.data.get("daemon")
                if isinstance(self.daemon.cluster.data, dict)
                else None
            )
            return bool((d or {}).get("auto_update", True))
        except Exception:
            return True

    def _source_url(self) -> str:
        try:
            d = (
                self.daemon.cluster.data.get("daemon")
                if isinstance(self.daemon.cluster.data, dict)
                else None
            )
            u = (d or {}).get("auto_update_source")
            if isinstance(u, str) and u.strip():
                return u.strip()
        except Exception:
            pass
        return "https://meshkore.com/reference/cluster/scripts/daemon.py"

    def _loop(self) -> None:
        # Initial small grace so we don't fight the boot self-update if
        # both happen to fire on the same first second.
        if self._stop.wait(60):
            return
        while True:
            # §17 (py-1.14.7) — keep the agent-CLI preamble fresh from the
            # canonical standard. Independent of the auto_update opt-out:
            # refreshing a doc is not a code upgrade. No-op when already
            # current; re-renders the per-CLI files on any change.
            try:
                r = getattr(self.daemon, "instructions_renderer", None)
                if r is not None:
                    r.refresh_from_remote()
                    # py-1.14.8 — detect+surface standard-version drift
                    # (does NOT auto-migrate; surfaced via /health + WS).
                    r.check_standard_drift()
            except Exception as e:
                _log(f"version-watcher: preamble refresh raised: {e}")
            try:
                if self._enabled():
                    self._check_once()
            except Exception as e:
                _log(f"version-watcher: tick raised: {e}")
            if self._stop.wait(self._tick_secs()):
                return

    def _check_once(self) -> None:
        # Cooldown gate.
        now = time.time()
        if now - self._last_attempt_ts < self.COOLDOWN_AFTER_ATTEMPT_SECS:
            return
        remote = self._fetch_remote_version()
        if not remote:
            return
        if not _is_remote_newer(local=DAEMON_VERSION, remote=remote):
            return
        # An upgrade is available. Are we idle?
        active = self.daemon.chat_sessions.list_active()
        if active:
            _log(
                f"version-watcher: upgrade {DAEMON_VERSION} → {remote} available "
                f"but {len(active)} chat session(s) live — deferring"
            )
            _debug_emit(
                "version-watcher.deferred",
                msg=f"upgrade {DAEMON_VERSION} → {remote} deferred ({len(active)} live)",
                lvl="info",
                data={"local": DAEMON_VERSION, "remote": remote, "live_convs": active},
            )
            try:
                self.daemon.hub.broadcast(
                    {
                        "type": "daemon.upgrade.deferred",
                        "local": DAEMON_VERSION,
                        "remote": remote,
                        "live_convs": active,
                        "ts": _iso_now(),
                    }
                )
            except Exception:
                pass
            return
        # Idle — call self_update directly. It re-checks the active set
        # so even if something raced into flight between the check above
        # and the swap, we get a clean 409 (no kill).
        self._last_attempt_ts = now
        _log(f"version-watcher: triggering self_update ({DAEMON_VERSION} → {remote})")
        _debug_emit(
            "version-watcher.upgrade.start",
            msg=f"auto self-update {DAEMON_VERSION} → {remote}",
            lvl="info",
            data={"local": DAEMON_VERSION, "remote": remote},
        )
        try:
            self.daemon.hub.broadcast(
                {
                    "type": "daemon.upgrade.starting",
                    "local": DAEMON_VERSION,
                    "remote": remote,
                    "ts": _iso_now(),
                }
            )
        except Exception:
            pass
        try:
            code, resp = self.daemon.self_update({})
            if code >= 400:
                _log(f"version-watcher: self_update returned {code}: {resp}")
                _debug_emit(
                    "version-watcher.upgrade.failed",
                    msg=f"self_update returned {code}",
                    lvl="warn",
                    data={"code": code, "resp": resp},
                )
        except Exception as e:
            _log(f"version-watcher: self_update raised: {e}")

    # The DAEMON_VERSION line lives ~line 69 of the canonical file —
    # past the module docstring + imports. 8 KB is enough to catch it
    # with room to spare; still <0.1% of the full ~400 KB daemon.py.
    _FETCH_BYTES = 8192

    def _fetch_remote_version(self) -> Optional[str]:
        """HTTP Range-request the head of the source URL and parse the
        DAEMON_VERSION line. Returns None on any failure (network,
        non-200, missing version marker)."""
        url = self._source_url()
        try:
            import urllib.request

            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": f"meshcore-py/{DAEMON_VERSION} version-watcher",
                    "Range": f"bytes=0-{self._FETCH_BYTES - 1}",
                },
            )
            with urllib.request.urlopen(req, timeout=8) as r:
                head = r.read(self._FETCH_BYTES).decode("utf-8", errors="replace")
        except Exception as e:
            _log(f"version-watcher: fetch failed {url}: {e}")
            return None
        m = re.search(r'^DAEMON_VERSION\s*=\s*"([^"]+)"', head, re.MULTILINE)
        if not m:
            return None
        return m.group(1).strip()


def _is_remote_newer(local: str, remote: str) -> bool:
    """Compare two `py-X.Y.Z` strings. Tolerates suffixes like
    `py-1.12.1-hotfix` — strips after the first non-numeric/dot char
    in the version body for comparison purposes."""

    def _tuple(v: str) -> Tuple[int, ...]:
        body = v[len("py-") :] if v.startswith("py-") else v
        # Stop at the first char that isn't digit or dot.
        clean = re.split(r"[^0-9.]", body, 1)[0]
        return tuple(int(p) for p in clean.split(".") if p.isdigit())

    try:
        return _tuple(remote) > _tuple(local)
    except Exception:
        return False


# ───────────────────────────────────────────────────────────────────────
# Daemon orchestrator


class Daemon:
    def __init__(
        self, paths: Paths, identity: Optional[str], requested_port: Optional[int]
    ):
        self.paths = paths
        # DM6 step 2 — instance-bound version so routes.py (and any other
        # extracted module) reads from `daemon.daemon_version` instead of
        # the module-level DAEMON_VERSION (which in source-tree dev only
        # exists in daemon.py's namespace, not the sibling module's).
        self.daemon_version = DAEMON_VERSION
        self.cluster = Cluster(paths)
        # py-1.2.0 — Standard v7 migration: write a default `daemon:`
        # block into cluster.yaml if it's missing. Idempotent; quiet
        # on success, no-op when the operator has already opted out
        # by setting auto_update: false.
        try:
            _migrate_cluster_daemon_block(paths)
            # Re-parse so self.cluster.data reflects the migration we
            # just wrote.
            self.cluster.reload()
        except Exception as e:
            _log(f"daemon-block migration skipped: {e}")
        self.identity = identity or _detect_identity(paths) or _hostname_default()
        self.token = _ensure_token(paths)
        self.port = _pick_port(
            paths,
            cluster_id=self.cluster.id,
            cli_override=requested_port,
            yaml_port=self.cluster.architect_port,
        )
        self.hub = Hub()
        self.state_manager = StateManager(paths, self.cluster, self.hub)
        # StateManager keeps a daemon backref for future cross-system
        # reads. Currently unused after the py-1.11.1 chat-state cleanup
        # (chat data is no longer joined into /state). Bound here, after
        # both objects exist.
        self.state_manager.bind_daemon(self)
        self.chat_sessions = ChatSessions()
        # py-1.12.19 — Standard v16 chat-turn queue. Disk-backed FIFO
        # per conv. Auto-flushed after each turn via
        # `_maybe_flush_queue` invoked from ChatRunner's end-of-stream.
        self.chat_queue_manager = ChatQueueManager(self.paths, self.hub)
        # py-1.12.21 — chat attachment persistence + retention GC.
        self.upload_store = UploadStore(self.paths, self.cluster)
        # py-1.12.22 — Standard v22 storage reporting. Cached walk of
        # the well-known .meshkore/ subtrees so the cockpit can render
        # a capacity panel without re-`du`-ing on every poll.
        self.storage_report = StorageReport(self.paths, self.cluster)
        # py-1.10.27 — Persistent quota state. Replaces the in-memory
        # `_agent_type_pauses` dict from py-1.10.26. State is keyed by
        # `<platform>/<model>` (the "quota_key" from _agent_manifest)
        # and survives daemon restart at .meshkore/.runtime/quota-state.json.
        # Multiple agent_types that share platform+model share the pool.
        self.quota = QuotaState(self.paths.runtime / "quota-state.json")
        # py-1.10.0 — server-side story-run coordinator. Owns the
        # initiative ↔ conv ↔ agent ↔ task-list binding so play/stop
        # has unambiguous identity and survives cockpit reload.
        self.runs = RunStore(paths, self.hub)
        # py-1.5.0 — persistent archive state (was cockpit-localStorage-only).
        self.chat_archive = ChatArchive(paths)
        # py-1.5.0 — background gzipper for .meshkore/timeline/*.jsonl
        # older than 90 days. Keeps disk footprint bounded on long-running
        # clusters; transparent to readers (gzip-aware).
        self.timeline_rotator = TimelineRotator(paths)
        # Standard §13 — deployment links registry. Quiet no-op when
        # .meshkore/public/links.yaml is absent.
        self.links_registry = LinksRegistry(paths, self.hub)
        # Standard §14 — protocols registry. Quiet no-op when
        # .meshkore/protocols/ is absent.
        self.protocols_registry = ProtocolsRegistry(paths, self.hub)
        # Standard §17 (ADI-01, py-1.14.7) — renders AGENT_INSTRUCTIONS.md
        # into CLAUDE.md/AGENTS.md/GEMINI.md (+ v19 Cursor/Cline targets).
        # Boot-syncs the per-CLI files + watches the source for edits; the
        # preamble itself is refreshed from the standard on the
        # VersionWatcher tick (see VersionWatcher._loop).
        self.instructions_renderer = AgentInstructionsRenderer(paths, self.hub)
        # D-CRON-02..05: tick loop + runner; started in serve_forever()
        self.cron_scheduler = CronScheduler(
            paths, self.cluster, self.hub, self.identity
        )
        self.stopping = threading.Event()
        self.server: Optional[ThreadingHTTPServer] = None
        # D-TLS-01 — set by serve_forever once it knows whether the
        # bundle loaded. /health reports this; cockpit decides URL scheme.
        self.tls_enabled: bool = False

    # ── U-DAEMON-06: chat coordinator ──────────────────────────────────
    def _spawn_chat_turn(
        self,
        conv: str,
        prompt: str,
        *,
        context_docs: Optional[List[Dict[str, Any]]] = None,
        agent_type: Optional[str] = None,
        agent_id: Optional[str] = None,
        parent_conv: Optional[str] = None,
        initiative_id: Optional[str] = None,
        task_id: Optional[str] = None,
        model: Optional[str] = None,
        effort: Optional[str] = None,
    ) -> ChatRunner:
        """Start one chat turn. Wires the chain so a buffered next
        prompt re-spawns automatically when the current turn finishes.
        Context docs (py-1.4.0) flow into the BriefingPipeline."""
        # py-1.7.0 — Resolve agent_type/id from caller args, falling
        # back to the persisted conv sidecar so chained turns and
        # cockpit reconnects don't lose specialisation.
        resolved_type, resolved_id = self._conv_meta_get(conv)
        if agent_type:
            resolved_type = agent_type
        if agent_id:
            resolved_id = agent_id
        # py-1.10.12 — Slug-implied type wins. The conv slug
        # (roadmap-architect-<N>) is unforgeable signal of intent.
        # If the body/sidecar disagree, the slug is right and they
        # are drift. Heals stale ikamiro-style conv_meta where the
        # body field never carried the agent_type.
        slug_implied = _agent_type_from_conv_slug(conv)
        if slug_implied and resolved_type != slug_implied:
            _log(
                f"conv {conv}: slug implies agent_type={slug_implied!r} "
                f"but resolved={resolved_type!r}; forcing slug-implied"
            )
            resolved_type = slug_implied
        # Persist whatever we end up with so subsequent turns inherit it.
        # `parent_conv` / `initiative_id` / `task_id` only overwrite the
        # sidecar when explicitly provided — silent updates (chained
        # re-spawns) reuse whatever was written on the first dispatch.
        self._conv_meta_set(
            conv,
            resolved_type,
            resolved_id,
            parent_conv=parent_conv,
            initiative_id=initiative_id,
            task_id=task_id,
            model=model,
            effort=effort,
        )
        # MP1 (py-1.13.3) / MP3 (py-1.13.4) — Resolve model + effort
        # AFTER the sidecar write so chained turns inherit even when the
        # dispatch body omitted them. Each returns None when the
        # preference is the "auto"/"default" sentinel — ChatRunner.spawn
        # skips the matching CLI flag in that case.
        resolved_model = self._conv_meta_get_model(conv)
        resolved_effort = self._conv_meta_get_effort(conv)
        runner = ChatRunner(
            paths=self.paths,
            cluster=self.cluster,
            hub=self.hub,
            identity=self.identity,
            conv=conv,
            prompt=prompt,
            context_docs=context_docs or [],
            agent_type=resolved_type,
            agent_id=resolved_id,
            model=resolved_model,
            effort=resolved_effort,
            daemon=self,
        )
        runner.spawn()
        # Chained turns (auto-spawn when a queued prompt lands) inherit
        # the current turn's context_docs + agent metadata.
        chain_ctx = list(context_docs or [])
        chain_type = resolved_type
        chain_id = resolved_id
        self.chat_sessions.start(
            conv,
            runner,
            on_chain=lambda c, p: self._spawn_chat_turn(
                c,
                p,
                context_docs=chain_ctx,
                agent_type=chain_type,
                agent_id=chain_id,
            ),
            # py-1.12.19 — Standard v16 auto-flush. After a turn finishes
            # with no in-memory pending, check the disk queue for the
            # conv. If a queued item exists, pop the head and dispatch
            # it as the next turn — operator's accumulated instructions
            # land seamlessly. Carries the same context_docs / agent_type
            # / agent_id as the just-finished turn (chain inheritance).
            on_idle=lambda c: self._maybe_flush_chat_queue(
                c,
                context_docs=chain_ctx,
                agent_type=chain_type,
                agent_id=chain_id,
            ),
        )
        # py-1.11.0 — snapshot.v1 contract: emit conv.activity AFTER
        # ChatSessions.start() registers the conv so the broadcast's
        # `live` flag is true (matches what /chat/convs would return).
        # Also emit for the parent so its `coordinating` + `waiting_on`
        # flip in one round-trip instead of waiting for state.rebuilt.
        self._broadcast_conv_activity(conv)
        if parent_conv:
            self._broadcast_conv_activity(parent_conv)
        return runner

    # py-1.7.0 — conv → (agent_type, agent_id) sidecar. Lets the daemon
    # remember the specialisation across turns even if the cockpit
    # forgets to re-send it (and gives offline/migrated clusters a stable
    # store outside the cockpit's localStorage).
    def _conv_meta_path(self) -> Any:
        return self.paths.runtime / "conv_meta.json"

    def _conv_meta_load(self) -> Dict[str, Dict[str, str]]:
        p = self._conv_meta_path()
        try:
            if not p.exists():
                return {}
            return json.loads(p.read_text() or "{}") or {}
        except Exception:
            return {}

    def _conv_meta_get(self, conv: str) -> Tuple[str, Optional[str]]:
        meta = self._conv_meta_load().get(conv) or {}
        # py-1.10.12 — Slug-implied type wins on read too. Heals any
        # historic sidecar entry written before py-1.10.12 that has
        # the wrong agent_type (e.g. ikamiro had several
        # roadmap-architect-* convs persisted as 'custom').
        slug_implied = _agent_type_from_conv_slug(conv)
        recorded = _agent_type_normalised(meta.get("agent_type"))
        return (
            slug_implied if slug_implied else recorded,
            (meta.get("agent_id") or None),
        )

    def _conv_meta_get_model(self, conv: str) -> Optional[str]:
        """MP1 (py-1.13.3) — Read the per-conv model preference stored
        by the cockpit's NewAgentWizard. Returns None / 'auto' when no
        override is set; otherwise one of 'opus' / 'sonnet' / 'haiku'
        (or any string claude-code accepts, incl. pinned ids like
        'claude-opus-4-8'). Used by ChatRunner.spawn to inject
        `--model <id>` into the CLI argv."""
        meta = self._conv_meta_load().get(conv) or {}
        m = str(meta.get("model") or "").strip()
        if not m or m.lower() == "auto":
            return None
        return m

    def _conv_meta_get_effort(self, conv: str) -> Optional[str]:
        """MP3 (py-1.13.4) — Read the per-conv effort (reasoning-depth)
        preference. Returns None / 'default' when unset; otherwise one
        of low/medium/high/xhigh/max. Used by ChatRunner.spawn to inject
        `--effort <level>` into the CLI argv. This is claude-code's
        thinking dial — there is no separate thinking flag."""
        meta = self._conv_meta_load().get(conv) or {}
        e = str(meta.get("effort") or "").strip().lower()
        if not e or e == "default":
            return None
        if e not in ("low", "medium", "high", "xhigh", "max"):
            return None
        return e

    def _conv_meta_set(
        self,
        conv: str,
        agent_type: str,
        agent_id: Optional[str],
        parent_conv: Optional[str] = None,
        initiative_id: Optional[str] = None,
        task_id: Optional[str] = None,
        model: Optional[str] = None,
        effort: Optional[str] = None,
    ) -> None:
        try:
            all_meta = self._conv_meta_load()
            existed_before = conv in all_meta
            before = dict(all_meta.get(conv) or {})
            entry = all_meta.get(conv) or {}
            entry["agent_type"] = _agent_type_normalised(agent_type)
            if agent_id:
                entry["agent_id"] = agent_id
            # py-1.10.16 — Parent-child conv linkage for the architect
            # wake protocol (initiative `architect-wake-on-subagent`).
            # The architect dispatches a subagent with `parent_conv: <me>`
            # so that when the subagent's final fires, the daemon can
            # post a `[architect-wake]` turn back to the architect's
            # conv. Persisted so a daemon restart preserves the linkage.
            if parent_conv:
                entry["parent_conv"] = parent_conv
            # py-1.10.19 — Initiative + task linkage. Drives the
            # cockpit's per-initiative working spinner + per-task
            # blink in the roadmap (initiative `agent-activity-surface`).
            # Stored alongside parent_conv so a daemon restart preserves
            # the full join, and the architect wake hook can quote them
            # back to the parent ("subagent A101 on I1/D-DBG-01 finished").
            if initiative_id:
                entry["initiative_id"] = initiative_id
            if task_id:
                entry["task_id"] = task_id
            # MP1 (py-1.13.3) — Per-conv model preference. Normalised to
            # lowercase; 'auto' is stored explicitly so chained turns
            # don't pick up a stale value. Empty / None means "no
            # override".
            if model is not None:
                # Preserve case for pinned ids (claude-opus-4-8); only
                # the aliases are conventionally lowercase anyway.
                m_norm = str(model).strip()
                if m_norm:
                    entry["model"] = m_norm
                elif "model" in entry:
                    del entry["model"]
            # MP3 (py-1.13.4) — per-conv effort (reasoning depth).
            if effort is not None:
                e_norm = str(effort).strip().lower()
                if e_norm:
                    entry["effort"] = e_norm
                elif "effort" in entry:
                    del entry["effort"]
            all_meta[conv] = entry
            p = self._conv_meta_path()
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(all_meta, indent=2, sort_keys=True))
            tmp.replace(p)
            # py-1.11.0 — Broadcast conv.created (first-time) or
            # conv.meta_updated (subsequent) so cockpits update the rail
            # WITHOUT waiting for a state.rebuilt + refetch. The hub may
            # not be wired yet during boot — guard with hasattr.
            if getattr(self, "hub", None) is not None:
                try:
                    payload = {
                        "conv": conv,
                        "agent_type": entry.get("agent_type"),
                        "agent_id": entry.get("agent_id"),
                        "parent_conv": entry.get("parent_conv"),
                        "initiative_id": entry.get("initiative_id"),
                        "task_id": entry.get("task_id"),
                        "model": entry.get("model"),
                        "effort": entry.get("effort"),
                        "ts": _iso_now(),
                    }
                    if not existed_before:
                        self.hub.broadcast({"type": "conv.created", **payload})
                    elif before != entry:
                        self.hub.broadcast({"type": "conv.meta_updated", **payload})
                except Exception as bx:
                    _log(f"conv meta broadcast failed: {bx}")
        except Exception as e:
            _log(f"conv_meta write failed: {e}")

    def _conv_meta_parent(self, conv: str) -> Optional[str]:
        """Return the parent conv id recorded for `conv`, if any."""
        meta = self._conv_meta_load().get(conv) or {}
        p = meta.get("parent_conv")
        return str(p) if p else None

    def _dispatch_mutex_check(
        self,
        *,
        conv: str,
        agent_type: Optional[str],
        parent_conv: Optional[str],
        task_id: Optional[str],
        initiative_id: Optional[str] = None,
    ) -> Optional[Tuple[int, Dict[str, Any]]]:
        """py-1.10.25 — server-side enforcement of dispatch invariants
        the architect prompt claims but the LLM sometimes ignores.
        Returns `None` to allow the dispatch, or `(409, body)` to reject.

        Invariants enforced (both observed broken in cavioca 2026-05-30):

        1. **Single live roadmap-architect.** At most one
           `roadmap-architect-*` conv may have a live ChatRunner. A
           wake to the SAME conv is allowed (it's just the next turn
           on the existing architect); a dispatch to a DIFFERENT
           `roadmap-architect-*` while one is alive is refused.

        2. **No parallel dispatch on the same (parent_conv, task_id).**
           If the architect already dispatched task `T` and that conv
           is still streaming, a second dispatch on the same (parent,
           task) pair is refused. Prevents two subagents racing on
           the same file commits.

        The architect catches 409s on its bash tool and (per the
        prompt addendum below) should treat them as "wait for the
        wake, don't retry". Cockpit reads the `hint` and surfaces
        a soft notice.
        """
        # py-1.10.26 — Pause check FIRST. If the agent_type is in
        # cool-down because of a recent rate-limit hit, refuse 503
        # with a hint that names the ETA. Architect prompt update
        # below tells it to NOT retry — wait, or switch type.
        # `roadmap-architect` itself is exempted (we don't want to
        # lock the coordinator out of its own conv just because a
        # subagent hit a wall). The architect can still narrate +
        # dispatch other types or different convs.
        norm_target = _agent_type_normalised(agent_type)
        if norm_target != "roadmap-architect":
            pause = self._agent_type_is_paused(norm_target)
            if pause is not None:
                return 503, {
                    "error": "agent-type-paused",
                    "agent_type": norm_target,
                    "reason": pause.get("reason"),
                    "expires_at": pause.get("expires_at"),
                    "expires_epoch": pause.get("expires_epoch"),
                    "hint": (
                        f"Agent type `{norm_target}` is paused until "
                        f"{pause.get('expires_at')} (rate-limit cooldown). "
                        "Wait for the window to reset, switch to a "
                        "different agent_type, or `POST /agent-types/"
                        f"{norm_target}/unpause` to override."
                    ),
                }

        is_architect_target = (agent_type == "roadmap-architect") or conv.startswith(
            "roadmap-architect-"
        )
        live = self.chat_sessions.list_active()

        # Invariant 1: single live roadmap-architect.
        # Match BOTH by slug AND by stored agent_type in conv_meta —
        # the slug is the canonical signal for cockpit-spawned convs
        # but custom-named convs can also carry the agent_type via
        # /chat/dispatch body, and we must catch both.
        if is_architect_target:
            all_meta = self._conv_meta_load()
            others: List[str] = []
            for c in live:
                if c == conv:
                    continue
                slug_arch = c.startswith("roadmap-architect-")
                meta_arch = (
                    _agent_type_normalised((all_meta.get(c) or {}).get("agent_type"))
                    == "roadmap-architect"
                )
                if slug_arch or meta_arch:
                    others.append(c)
            if others:
                return 409, {
                    "error": "roadmap-architect-already-live",
                    "hint": (
                        "Another roadmap-architect conv is already running. "
                        "Stop it first (POST /chat/cancel) before spawning a new one."
                    ),
                    "existing_convs": others,
                    "requested_conv": conv,
                }

        # Invariants 2 + 3 both need the conv_meta sidecar.
        if parent_conv:
            all_meta = self._conv_meta_load()

            # Invariant 2: no parallel dispatch on same (parent_conv, task_id).
            # Only meaningful when both parent_conv and task_id are set —
            # i.e., the architect dispatching a subagent.
            if task_id:
                for live_conv in live:
                    if live_conv == conv:
                        continue
                    m = all_meta.get(live_conv) or {}
                    if (
                        m.get("parent_conv") == parent_conv
                        and m.get("task_id") == task_id
                    ):
                        return 409, {
                            "error": "task-already-dispatched",
                            "hint": (
                                f"Task `{task_id}` (parent `{parent_conv}`) "
                                f"already has a live dispatch: `{live_conv}`. "
                                "Wait for the [architect-wake] on its final; "
                                "do not retry while it's still running."
                            ),
                            "existing_conv": live_conv,
                            "parent_conv": parent_conv,
                            "task_id": task_id,
                        }

            # Invariant 3 (py-1.10.28): single initiative in-flight per
            # architect. Operator's product decision (2026-05-31): "una
            # iniciativa a la vez, tareas en paralelo DENTRO pero no
            # mezclando entre iniciativas". The architect is allowed
            # to dispatch parallel tasks within initiative I, but
            # cannot start I+1 while ANY task on I still has a live
            # subagent. The 409 hint names the live initiative(s) so
            # the architect knows what it's waiting on. Linear-roadmap
            # mode prevents half-finished initiatives + reduces quota
            # burn on speculative parallel work.
            if initiative_id:
                live_initiatives: set = set()
                for live_conv in live:
                    if live_conv == conv:
                        continue
                    m = all_meta.get(live_conv) or {}
                    if m.get("parent_conv") != parent_conv:
                        continue
                    other = m.get("initiative_id")
                    if other:
                        live_initiatives.add(other)
                if live_initiatives and initiative_id not in live_initiatives:
                    return 409, {
                        "error": "initiative-already-in-flight",
                        "hint": (
                            "Linear-roadmap mode: another initiative still "
                            f"has live subagents (`{', '.join(sorted(live_initiatives))}`). "
                            f"Wait for ALL its tasks to finish (or mark them "
                            f"blocked) before dispatching into "
                            f"`{initiative_id}`. Parallel work is allowed "
                            "INSIDE a single initiative, never across."
                        ),
                        "live_initiatives": sorted(live_initiatives),
                        "requested_initiative": initiative_id,
                        "parent_conv": parent_conv,
                    }

        # py-1.12.0 — Worker-dispatch invariants. Only fire when this
        # dispatch is creating/touching a `work-*` subagent slot. The
        # architect's own dispatches (roadmap-architect-*) and the
        # operator's free-form custom convs sidestep these checks —
        # they're not "worker dispatches", they're conversation starts.
        is_worker_dispatch = conv.startswith("work-")
        if is_worker_dispatch:
            # Invariant 5: required join keys. work-* dispatches MUST
            # carry both `initiative_id` AND `task_id` so that
            # Invariants 2+3 actually fire. Pre-py-1.12.0 a dispatch
            # missing either field would silently slip past the
            # mutex (line 6325 was guarded by `if task_id:`, line 6354
            # by `if initiative_id:`). The architect prompt already
            # requires both fields; this turns "should send" into
            # "must send" with a clear 400 if it forgets.
            if not initiative_id or not task_id:
                missing = []
                if not initiative_id:
                    missing.append("initiative_id")
                if not task_id:
                    missing.append("task_id")
                return 400, {
                    "error": "worker-dispatch-missing-join-keys",
                    "missing": missing,
                    "hint": (
                        f"`{conv}` is a work-* subagent dispatch — it MUST "
                        f"include both `initiative_id` AND `task_id` in the "
                        f"POST body so the daemon can enforce linear-init + "
                        f"depends_on. Missing: {', '.join(missing)}. Re-read "
                        f"the SOP `EXECUTION LOOP — LINEAR INITIATIVES` block."
                    ),
                }

            # Invariant 4: wave cap. The architect prompt promises
            # "max 3 parallel"; enforce it here so a runaway loop or a
            # confused turn can't spawn 7 workers and 5x the quota burn.
            # Cap is configurable via cluster.yaml.architect.wave_cap;
            # default 3 (matches the prompt). Per-parent_conv so two
            # operators on the same cluster (different architect convs)
            # each get their own wave budget.
            cap = self._wave_cap()
            if parent_conv:
                same_wave = 0
                all_meta_w = self._conv_meta_load()
                for live_conv in live:
                    if live_conv == conv:
                        continue
                    if not live_conv.startswith("work-"):
                        continue
                    m = all_meta_w.get(live_conv) or {}
                    if m.get("parent_conv") == parent_conv:
                        same_wave += 1
                if same_wave >= cap:
                    return 429, {
                        "error": "wave-cap-reached",
                        "wave_cap": cap,
                        "current_wave_size": same_wave,
                        "parent_conv": parent_conv,
                        "hint": (
                            f"This architect already has {same_wave} work-* "
                            f"subagent(s) in flight (cap={cap}). Wait for a "
                            f"slot to free up via [architect-wake] before "
                            f"dispatching the next task. Operator can raise "
                            f"the cap via `cluster.yaml.architect.wave_cap` "
                            f"(higher = faster, more quota burn + more "
                            f"chance of git-race)."
                        ),
                    }

            # Invariant 6: depends-on gate. Refuse the dispatch if the
            # target task's `depends_on:` frontmatter lists upstream
            # tasks that are NOT marked `done`. The architect should
            # already serialise via depends_on at the prompt level —
            # this is the server-side belt to the prompt's braces.
            # Cheap: reads one task .md file + checks the upstream
            # statuses we already cache in `_state['roadmap']['tasks']`.
            missing_deps = self._unfinished_dependencies(task_id, initiative_id)
            if missing_deps:
                return 409, {
                    "error": "task-dependencies-not-done",
                    "task_id": task_id,
                    "initiative_id": initiative_id,
                    "missing": missing_deps,
                    "hint": (
                        f"Task `{task_id}` declares `depends_on: "
                        f"{missing_deps}` in its frontmatter but those "
                        f"upstream task(s) are not `done` yet. Finish "
                        f"them first (or remove the dependency if it's "
                        f"stale). Do NOT retry this dispatch until then."
                    ),
                }

        return None

    def _wave_cap(self) -> int:
        """Return the per-architect parallel-worker cap. Read from
        cluster.yaml.architect.wave_cap; default 3 (matches the
        roadmap-architect prompt's stated bound). Operator can widen
        for throughput or narrow for cost."""
        try:
            raw = (self.cluster.data.get("architect") or {}).get("wave_cap")
            if raw is None:
                return 3
            n = int(raw)
            return max(1, min(10, n))  # clamp to a sane range
        except Exception:
            return 3

    def _unfinished_dependencies(
        self,
        task_id: Optional[str],
        initiative_id: Optional[str],
    ) -> List[str]:
        """Read the target task's frontmatter and return the subset of
        `depends_on:` references whose current status is NOT `done`.
        Empty list = green light. Returns [] silently on any IO error
        (we don't want a missing file or a bad parse to deadlock the
        architect — the dispatch proceeds and the subagent will hit
        the real problem with a clearer error)."""
        if not task_id or not initiative_id:
            return []
        try:
            # Locate the task .md file under .meshkore/roadmap/initiatives/<init>/<task>.md
            # OR the legacy flat layout. Honour either.
            candidates = [
                self.paths.roadmap_dir
                / "initiatives"
                / initiative_id
                / f"{task_id}.md",
                self.paths.roadmap_dir / "tasks" / f"{task_id}.md",
            ]
            task_path: Optional[Path] = None
            for c in candidates:
                if c.exists():
                    task_path = c
                    break
            if task_path is None:
                return []
            raw = task_path.read_text(errors="replace")
            front = parse_frontmatter(raw)
            deps_raw = front.get("depends_on") if isinstance(front, dict) else None
            if not deps_raw:
                return []
            # Accept either a YAML list or a comma-separated string.
            if isinstance(deps_raw, str):
                deps = [s.strip() for s in deps_raw.split(",") if s.strip()]
            elif isinstance(deps_raw, list):
                deps = [str(s).strip() for s in deps_raw if str(s).strip()]
            else:
                return []
            if not deps:
                return []
            # Read current statuses from the state cache. Build a quick
            # task_id → status map; default to "unknown" (treated as
            # not-done, conservative).
            state = self.state_manager.state()
            status_by_id: Dict[str, str] = {}
            for t in (state.get("roadmap") or {}).get("tasks") or []:
                tid = t.get("id")
                if tid:
                    status_by_id[str(tid)] = str(t.get("status") or "unknown")
            missing: List[str] = []
            for dep in deps:
                if status_by_id.get(dep, "unknown") != "done":
                    missing.append(dep)
            return missing
        except Exception as e:
            _log(f"_unfinished_dependencies({task_id}) raised: {e}")
            return []

    def _persist_user_event(
        self,
        conv: str,
        text: str,
        *,
        author: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """py-1.14.11 — Single source for 'the operator's message enters
        the conversation': append a `chat.user` timeline event and
        broadcast it. Reused by `chat_dispatch` AND the queue-flush path
        (`_maybe_flush_chat_queue`) so a flushed queued message lands in
        the timeline (chat history) and on the wall in chronological
        order — exactly like a live dispatch. Before this, the flush
        path called `_spawn_chat_turn` directly with no user event, so
        the queued message was missing from the wall AND vanished from
        history on reload. `_append_timeline` stamps `ts`, so the user
        event sorts before the assistant final the turn produces."""
        user_ev: Dict[str, Any] = {
            "type": "chat.user",
            "author": author or self.identity,
            "text": text,
            "conv": conv,
        }
        if attachments:
            user_ev["attachments"] = attachments
        ev = _append_timeline(self.paths, user_ev)
        self.hub.broadcast(ev)
        return ev

    def _maybe_flush_chat_queue(
        self,
        conv: str,
        *,
        context_docs: Optional[List[Dict[str, Any]]] = None,
        agent_type: Optional[str] = None,
        agent_id: Optional[str] = None,
    ) -> bool:
        """Standard v16 auto-flush hook. Called by ChatSessions when a
        conv has just gone idle (in-memory `pending` was empty / cancelled
        + slot popped) AND by the idle-flush sweep (`_flush_idle_chat_queues`).
        If the disk queue has items, pop the head and dispatch it as the
        next turn. The just-popped item gets `queue.item.sent` broadcast by
        `ChatQueueManager.pop_head`. Returns True iff a turn was spawned.

        py-1.14.6 — idempotency guard: refuse if a turn is already in
        flight for this conv. The on_idle path pops the session slot
        BEFORE calling here (so has() is False — proceeds normally), but
        the reaper-driven sweep can race a turn that just started; the
        guard keeps the two triggers from double-spawning. When omitted,
        agent_type/agent_id/context fall back to the persisted conv
        sidecar inside `_spawn_chat_turn` — so a boot/idle flush with no
        in-flight turn to inherit from still resolves the right agent."""
        if self.chat_sessions.has(conv):
            return False
        try:
            head = self.chat_queue_manager.pop_head(conv)
        except Exception as e:
            _log(f"queue auto-flush pop_head failed for {conv}: {e}")
            return False
        if head is None:
            return False
        text = str(head.get("text") or "").strip()
        if not text:
            return False
        _debug_emit(
            "queue.auto-flush",
            msg=f"flushing queue head into conv={conv}",
            conv=conv,
            data={"item_id": head.get("id"), "text_preview": text[:200]},
        )
        try:
            # py-1.14.11 — persist the user event BEFORE spawning, exactly
            # like chat_dispatch, so the flushed queued message appears on
            # the wall (and in history) chronologically before the agent's
            # response. pop_head already broadcast queue.item.sent (cockpit
            # drops it from the queue strip); this is what makes it a real
            # user bubble. `head` has no stored author → defaults to self.identity.
            self._persist_user_event(conv, text)
            self._spawn_chat_turn(
                conv,
                text,
                context_docs=context_docs,
                agent_type=agent_type,
                agent_id=agent_id,
            )
            return True
        except Exception as e:
            _log(f"queue auto-flush spawn failed for {conv}: {e}")
            return False

    def _flush_idle_chat_queues(self) -> int:
        """py-1.14.6 — Sweep every disk queue and flush the head of any
        whose conv has NO turn in flight. The on_idle hook only drains a
        queue on turn-COMPLETION; a conv can go idle with items still
        queued and never fire it — after a daemon restart / self-update
        re-exec (in-memory ChatSessions + its _wait thread are gone), an
        abnormally-reaped session (reap_dead pops the slot without firing
        on_idle), or an enqueue into an already-idle conv. Those queues
        would sit forever showing 'N WAITING · runs after the current
        turn' with no current turn. Flushing one head re-registers
        on_idle (via _spawn_chat_turn), so the rest of the queue drains
        normally turn-by-turn. Returns the count of convs flushed.

        Called from ChatSessionReaper._sweep (boot + every 30s tick)."""
        flushed = 0
        try:
            conv_ids = self.chat_queue_manager.conv_ids()
        except Exception as e:
            _log(f"_flush_idle_chat_queues: conv_ids failed: {e}")
            return 0
        for conv in conv_ids:
            if self.chat_sessions.has(conv):
                continue  # a turn is in flight — on_idle will drain it
            try:
                if self._maybe_flush_chat_queue(conv):
                    flushed += 1
            except Exception as e:
                _log(f"_flush_idle_chat_queues: flush {conv} failed: {e}")
        return flushed

    def chat_dispatch(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        text = str(body.get("text") or "").strip()
        # py-1.12.18 — image-only / docs-only dispatch is valid. The
        # operator field-reported 2026-06-10 that attaching images to
        # the architect without typing a question produced
        # `400 text required` — the message disappeared into thin air.
        # Now we accept the dispatch and synthesize a minimal text so
        # the model gets a coherent turn (claude-code expects a text
        # part). Reject only when EVERYTHING is empty.
        has_images = isinstance(body.get("images"), list) and len(body["images"]) > 0
        has_docs = (
            isinstance(body.get("context_docs"), list) and len(body["context_docs"]) > 0
        )
        if not text and not has_images and not has_docs:
            return 400, {
                "error": "empty dispatch — provide text, images, or context_docs",
            }
        if not text:
            # Synthesize a neutral placeholder so the briefing pipeline
            # has something to render as the user's turn. The
            # attachments themselves carry the operator's intent.
            if has_images and has_docs:
                text = "(see attached images and documents)"
            elif has_images:
                text = (
                    "(see attached image)"
                    if len(body["images"]) == 1
                    else "(see attached images)"
                )
            else:
                text = (
                    "(see attached document)"
                    if len(body["context_docs"]) == 1
                    else "(see attached documents)"
                )
        author = str(body.get("author") or self.identity)
        conv = str(
            body.get("conv")
            or f"chat-{_iso_now()[:16].replace(':', '-').replace('T', '-').lower()}"
        )
        # py-1.7.0 — agent specialisation from cockpit. Both fields are
        # optional; missing → 'custom' (General coder). When present,
        # persisted to the conv_meta sidecar so chained turns and
        # cockpit reconnects keep the same role.
        agent_type = body.get("agent_type")
        agent_id = body.get("agent_id")
        # py-1.10.16 — `parent_conv` (initiative `architect-wake-on-subagent`).
        # When the architect dispatches a subagent, it passes its own
        # conv id so the daemon can re-dispatch a wake turn the moment
        # the subagent's `chat.assistant.final` fires. Optional;
        # missing = the conv has no parent (cockpit-initiated chat).
        parent_conv = body.get("parent_conv")
        if parent_conv is not None:
            parent_conv = str(parent_conv).strip() or None
        # py-1.10.19 — `initiative_id` + `task_id` (initiative
        # `agent-activity-surface`). Both already flow on the wire
        # (architect prompt + story-runner cockpit dispatch); now
        # they're persisted so /state can join them and the cockpit
        # can render per-initiative / per-task working state without
        # heuristics on the conv slug.
        initiative_id = body.get("initiative_id")
        if initiative_id is not None:
            initiative_id = str(initiative_id).strip() or None
        task_id = body.get("task_id")
        if task_id is not None:
            task_id = str(task_id).strip() or None
        # MP1 (py-1.13.3) — per-conv model preference from the
        # NewAgentWizard (cockpit). The wizard already collects
        # `auto`/`opus`/`sonnet`/`haiku`; previously the value died in
        # convMeta. Now it flows through to `_conv_meta_set` and
        # ChatRunner.spawn, which injects `--model <id>` into claude-code.
        model_pref = body.get("model")
        if model_pref is not None:
            model_pref = str(model_pref).strip() or None
        # MP3 (py-1.13.4) — per-conv effort (reasoning depth) from the
        # NewAgentWizard. Forwarded to claude-code as `--effort <level>`.
        effort_pref = body.get("effort")
        if effort_pref is not None:
            effort_pref = str(effort_pref).strip() or None
        # py-1.10.25 — Daemon-side dispatch mutex. Enforces invariants
        # the architect prompt already claims but the LLM intermittently
        # violates (observed in cavioca 2026-05-30: same task got 4
        # parallel dispatches, two roadmap-architect convs running
        # simultaneously, etc.). Rejected requests return 409 with a
        # `hint` field naming the existing conv so the caller can
        # decide what to do (architect: wait for the wake; cockpit:
        # surface the conflict).
        mutex_err = self._dispatch_mutex_check(
            conv=conv,
            agent_type=agent_type,
            parent_conv=parent_conv,
            task_id=task_id,
            initiative_id=initiative_id,
        )
        if mutex_err is not None:
            code_err, body_err = mutex_err
            _debug_emit(
                "chat-dispatch.refused",
                msg=body_err.get("error", "refused"),
                lvl="warn",
                conv=conv,
                data=body_err,
            )
            return code_err, body_err
        # py-1.4.0 — Accept cockpit-attached context as part of the
        # briefing pipeline. Previously this field was silently
        # dropped, which broke V46/V78b onboarding (the cockpit
        # thought it was sending a bootstrap brief but the agent
        # never saw it).
        raw_docs = body.get("context_docs")
        context_docs: List[Dict[str, Any]] = []
        if isinstance(raw_docs, list):
            for d in raw_docs:
                if isinstance(d, dict) and (d.get("content") or "").strip():
                    context_docs.append(
                        {
                            "filename": str(d.get("filename") or "doc.md"),
                            "content": str(d.get("content") or ""),
                        }
                    )
        # py-1.12.21 — persist any image attachments to
        # `.meshkore/uploads/<bucket>/<file>` and embed a small
        # manifest in the chat.user event so the cockpit can render
        # thumbnails on hydrate. Failures are silently absorbed —
        # the dispatch still proceeds with text-only.
        attachments: List[Dict[str, Any]] = []
        if has_images:
            try:
                attachments = self.upload_store.save_dispatch(
                    conv=conv,
                    images=body.get("images")
                    if isinstance(body.get("images"), list)
                    else None,
                    ts_iso=_iso_now(),
                )
            except Exception as e:
                _log(f"upload save_dispatch failed: {e}")
                attachments = []
        # 1) Emit + persist the user event right away.
        self._persist_user_event(conv, text, author=author, attachments=attachments)
        # 2) Queue if a turn is already running for this conv.
        if self.chat_sessions.has(conv):
            pending = self.chat_sessions.queue(conv, text)
            return 202, {
                "queued": True,
                "conv": conv,
                "pending": pending,
                "message": "turn in progress — your prompt will be merged into the next turn",
            }
        # 3) New turn.
        _debug_emit(
            "chat-dispatch",
            msg=f"new turn (conv={conv}, type={agent_type or 'custom'})",
            conv=conv,
            agent_id=agent_id,
            data={
                "agent_type": agent_type,
                "parent_conv": parent_conv,
                "initiative_id": initiative_id,
                "task_id": task_id,
                "text_len": len(text),
                "text_preview": text[:200],
                "context_docs": len(context_docs),
                "author": author,
            },
        )
        try:
            runner = self._spawn_chat_turn(
                conv,
                text,
                context_docs=context_docs,
                agent_type=agent_type,
                agent_id=agent_id,
                parent_conv=parent_conv,
                initiative_id=initiative_id,
                task_id=task_id,
                model=model_pref,
                effort=effort_pref,
            )
        except Exception as e:
            return 400, {"error": str(e)}
        return 202, {
            "conv": conv,
            "runner": "claude-code",
            "identity": self.identity,
            "pid": runner.pid,
            "stream_id": runner.stream_id,
            "agent_type": _agent_type_normalised(agent_type),
        }

    # py-1.10.24 — Per-task unproductive-final counter (cavioca incident:
    # API2 went into plan-mode 3 times, architect kept retrying instead of
    # following matrix rule "blocked after 2 failures"). When the wake
    # hook detects a subagent final with NO commit hash AND NO success
    # marker, it bumps this counter and surfaces the count in the wake
    # message so the architect can't pretend it doesn't know.
    # Reset on Daemon restart — Run All sessions are bounded.
    _COMMIT_PATTERNS = (
        re.compile(r"\bcommit[:\s]+([0-9a-f]{6,40})\b", re.IGNORECASE),
        re.compile(r"^\s*✓\s+task\s+\S+\s+done\b", re.IGNORECASE | re.MULTILINE),
    )
    # py-1.10.26 — Rate-limit signatures emitted by the upstream CLIs
    # (Claude Code most commonly; Codex / DeepSeek would have their own
    # phrasing once integrated). The patterns are intentionally broad
    # so a phrasing change in a future CLI build still triggers — we'd
    # rather over-pause than spin on a quota-exhausted subagent forever.
    _RATE_LIMIT_PATTERNS = (
        re.compile(r"Claude AI usage limit reached", re.IGNORECASE),
        re.compile(r"\busage limit (reached|exceeded)\b", re.IGNORECASE),
        re.compile(r"\brate[- ]?limit(ed|ing)?\b", re.IGNORECASE),
        re.compile(r"\bquota (exceeded|reached|exhausted)\b", re.IGNORECASE),
        re.compile(r"\b5[- ]hour (limit|window)\b", re.IGNORECASE),
        re.compile(r"\bHTTP[\s/]+429\b"),
        re.compile(r"\btoo many requests\b", re.IGNORECASE),
        re.compile(r"Anthropic API .*\b(limit|quota)\b", re.IGNORECASE | re.DOTALL),
    )

    def _classify_subagent_final(self, preview: str, exit_code: Optional[int]) -> str:
        """Return one of:
            'success'     — committed work landed AND `git cat-file -e` confirms the sha
            'no-commit'   — turn ended with no commit hash in preview, OR the
                             claimed hash doesn't exist in the repo (py-1.12.0
                             Invariant 7 — ghost commit detection)
            'error'       — non-zero exit (CLI crashed or was killed)
            'rate-limited' — upstream CLI told us the quota is out
        Rate-limit detection runs first because some CLIs report the
        condition with exit=0 + a polite "try again later" message,
        which would otherwise look like a normal `no-commit`."""
        text = preview or ""
        if any(p.search(text) for p in self._RATE_LIMIT_PATTERNS):
            return "rate-limited"
        if exit_code not in (None, 0):
            return "error"
        commit_match = False
        for pat in self._COMMIT_PATTERNS:
            m = pat.search(text)
            if not m:
                continue
            # py-1.12.0 Invariant 7 — verify the claimed sha exists in
            # the repo. Subagents occasionally hallucinate commit
            # hashes; without this check the architect would mark the
            # task `done` and move on, leaving the work undone forever.
            # If the pattern doesn't capture a sha (e.g. the ✓-line
            # pattern) we still trust it — the prompt mandates the
            # commit line too, and we don't want false negatives from
            # a pattern that's intentionally permissive.
            sha = m.group(1) if m.lastindex and m.lastindex >= 1 else None
            if sha and not self._git_commit_exists(sha):
                _log(
                    f"classify: subagent claimed commit {sha} but it doesn't exist in repo — demoting to no-commit"
                )
                _debug_emit(
                    "subagent-final.ghost-commit",
                    msg=f"claimed commit {sha} does not exist",
                    lvl="warn",
                    data={"claimed_sha": sha, "preview_head": text[:200]},
                )
                continue
            commit_match = True
            break
        return "success" if commit_match else "no-commit"

    def _git_commit_exists(self, sha: str) -> bool:
        """Run `git cat-file -e <sha>` from the project root. Returns
        True iff the sha is a valid object in the repo. Silently False
        on any error (no git binary, not a repo, etc.) — the architect
        will get the 'no-commit' verdict and the task fail-counter will
        bump, which is the correct safe default."""
        if not re.match(r"^[0-9a-f]{6,40}$", sha):
            return False
        try:
            import subprocess

            r = subprocess.run(
                ["git", "cat-file", "-e", sha],
                cwd=str(self.paths.root),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
            )
            return r.returncode == 0
        except Exception:
            return False

    def _roadmap_pass_complete(self) -> bool:
        """py-1.10.25 — True iff no active/next initiative has any
        non-terminal task left. Used by the architect-wake hook to
        flip the message into "emit the summary and stop" mode when
        the pass has run out of actionable work.

        Terminal task statuses (count as 'done for the pass'):
        `done`, `blocked`, `cancelled`. Anything else (`next`,
        `active`, `in_progress`, `pending-operator`, …) still needs
        an agent.

        Falls back to False on any error — better to keep retrying
        than to falsely terminate a live pass."""
        try:
            snap = self.state_manager.state()
            inits = snap.get("initiatives") or []
            tasks_by_init: Dict[str, List[Dict[str, Any]]] = {}
            for t in (snap.get("roadmap") or {}).get("tasks") or []:
                iid = t.get("initiative")
                if iid:
                    tasks_by_init.setdefault(iid, []).append(t)
            terminal = {"done", "blocked", "cancelled"}
            for it in inits:
                status = normalize_status(it.get("status"))
                if status in ("done", "backlog"):
                    continue
                # active/next/in_progress — does it have actionable tasks?
                kids = tasks_by_init.get(it.get("id"), [])
                if any(normalize_status(k.get("status")) not in terminal for k in kids):
                    return False
            return True
        except Exception:
            return False

    # ── Agent-type pause state (py-1.10.27 — backed by QuotaState) ─────
    # The per-agent_type API is preserved as a thin wrapper over
    # QuotaState so existing callers (HTTP endpoints, wake hook) keep
    # working without contortion. Under the hood every lookup goes
    # through the (platform, model) quota_key derived from the
    # agent manifest.

    def _pause_agent_type(
        self,
        agent_type: str,
        *,
        reason: str,
        duration_secs: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Pause the quota pool that `agent_type` belongs to. Multiple
        types sharing a platform+model pause together (that's the
        point — they share the same upstream account)."""
        if not agent_type:
            return {}
        m = _agent_manifest(agent_type)
        entry = self.quota.pause(
            m["quota_key"],
            reason=reason,
            duration_secs=duration_secs,
            platform=m["platform"],
            model=m["model"],
        )
        # Back-compat shape for existing cockpit reader (until V108 lands).
        return {
            "since": entry.get("paused_at"),
            "epoch": entry.get("paused_until_epoch", 0)
            - (entry.get("paused_until_epoch", 0) - int(time.time())),
            "expires_at": entry.get("paused_until"),
            "expires_epoch": entry.get("paused_until_epoch"),
            "reason": entry.get("reason"),
            "duration_secs": duration_secs,
            "quota_key": m["quota_key"],
            "platform": m["platform"],
            "model": m["model"],
        }

    def _unpause_agent_type(self, agent_type: str) -> bool:
        if not agent_type:
            return False
        m = _agent_manifest(agent_type)
        return self.quota.unpause(m["quota_key"])

    def _agent_type_is_paused(
        self, agent_type: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        if not agent_type:
            return None
        m = _agent_manifest(agent_type)
        if not self.quota.is_paused(m["quota_key"]):
            return None
        entry = self.quota.get(m["quota_key"]) or {}
        return {
            "expires_at": entry.get("paused_until"),
            "expires_epoch": entry.get("paused_until_epoch"),
            "reason": entry.get("reason"),
            "quota_key": m["quota_key"],
            "platform": m["platform"],
            "model": m["model"],
        }

    def _paused_agent_types_view(self) -> Dict[str, Dict[str, Any]]:
        """Back-compat projection: map every agent_type whose
        quota_key is paused onto the legacy /health field shape.
        Built by walking AGENT_PROMPTS so multiple types sharing a
        pool all appear paused together (correct — they actually are)."""
        paused = self.quota.paused_view()
        out: Dict[str, Dict[str, Any]] = {}
        for t in AGENT_PROMPTS.keys():
            m = _agent_manifest(t)
            entry = paused.get(m["quota_key"])
            if entry:
                out[t] = {
                    "since": entry.get("paused_at"),
                    "expires_at": entry.get("paused_until"),
                    "expires_epoch": entry.get("paused_until_epoch"),
                    "reason": entry.get("reason"),
                    "quota_key": m["quota_key"],
                    "platform": entry.get("platform"),
                    "model": entry.get("model"),
                    "consecutive_rate_limits": entry.get("consecutive_rate_limits", 0),
                }
        return out

    def _bump_task_failure(self, task_id: Optional[str]) -> int:
        """Increment + return the cumulative unproductive-final count
        for `task_id` since daemon boot. Returns 0 when task_id is
        missing (untrackable)."""
        if not task_id:
            return 0
        if not hasattr(self, "_task_failures"):
            self._task_failures: Dict[str, int] = {}
        self._task_failures[task_id] = self._task_failures.get(task_id, 0) + 1
        return self._task_failures[task_id]

    def _maybe_wake_parent_architect(
        self,
        *,
        child_conv: str,
        child_agent_id: Optional[str],
        child_final_text: str,
        child_exit: Optional[int],
    ) -> None:
        """Architect-wake hook (initiative `architect-wake-on-subagent`).

        When a child conv emits `chat.assistant.final`, look up its
        recorded `parent_conv`. If the parent is a roadmap-architect
        conv, post a `[architect-wake]` user turn back to it so the
        pass resumes automatically. If the architect is mid-turn the
        wake is merged into its pending queue (chat_sessions.queue);
        if it has already exited the wake spawns a fresh turn. Both
        paths are correct and converge on the same outcome.

        py-1.10.24 — Wake message now annotates the outcome explicitly
        ('success' / 'no-commit' / 'error') + the cumulative unproductive
        count per task_id so the architect cannot ignore the
        DECISION MATRIX rule "Sub-agent failed twice → mark blocked".

        No-op when: no parent recorded; parent isn't a roadmap-architect
        conv; parent conv has been archived/cancelled. Quiet failures —
        a missing wake never blocks the child's final from being
        broadcast.
        """
        parent_conv = self._conv_meta_parent(child_conv)
        if not parent_conv:
            _debug_emit(
                "architect-wake.skipped",
                msg=f"no parent_conv recorded for {child_conv}",
                lvl="debug",
                conv=child_conv,
                agent_id=child_agent_id,
            )
            return
        parent_type = _agent_type_from_conv_slug(parent_conv)
        if parent_type != "roadmap-architect":
            # Wake hook is roadmap-architect-only for now. Generalising
            # to any parent type is on roadmap (would let custom agents
            # spawn worker children with auto-resume too) but needs
            # cycle-protection design first.
            _debug_emit(
                "architect-wake.skipped",
                msg=f"parent {parent_conv} is not a roadmap-architect conv",
                lvl="debug",
                conv=child_conv,
                data={"parent_conv": parent_conv, "parent_type": parent_type},
            )
            return
        # Build a compact wake message. Architect needs the child id +
        # a preview of the answer to know whether the task succeeded.
        preview = (child_final_text or "").strip()
        if len(preview) > 800:
            preview = preview[:800].rstrip() + " …(truncated)"
        agent_tag = f" ({child_agent_id})" if child_agent_id else ""
        exit_tag = f" exit={child_exit}" if child_exit not in (None, 0) else ""
        # py-1.10.24 — Classify the outcome + count failures per task.
        outcome = self._classify_subagent_final(preview, child_exit)
        # Pull the task_id the child was working on so we can name it
        # in the wake AND bump the counter.
        child_meta = self._conv_meta_load().get(child_conv) or {}
        task_id = child_meta.get("task_id") or None
        initiative_id = child_meta.get("initiative_id") or None
        child_agent_type = _agent_type_normalised(child_meta.get("agent_type"))
        fail_count = 0
        verdict_line = ""
        if outcome == "success":
            verdict_line = "VERDICT: ✓ success (commit detected in preview)"
        elif outcome == "rate-limited":
            # py-1.10.26 — Quota exhausted on the upstream CLI. Pause
            # the whole agent_type so the architect doesn't keep
            # throwing dispatches at a wall. Verdict tells it WHY
            # AND how long the cooldown lasts; matrix rule forces
            # mark-blocked-and-move-on (different from a normal fail
            # because no retry helps here).
            pause = self._pause_agent_type(
                child_agent_type,
                reason=f"rate-limited final from {child_conv}",
            )
            verdict_line = (
                f"VERDICT: ⏸ rate-limited — task `{task_id or '?'}` hit the "
                f"`{child_agent_type}` CLI quota. Agent type **paused until "
                f"{pause.get('expires_at')}**; further dispatches of this "
                f"type will return 503. **MATRIX RULE: mark this task "
                f"`blocked: rate-limited` and DO NOT retry — retrying does "
                f"not help until the quota window resets. You CAN dispatch "
                f"a DIFFERENT agent_type (deploy / db / testing / docs / "
                f"review) on other tasks while we wait.**"
            )
        else:
            fail_count = self._bump_task_failure(task_id)
            kind = (
                "no-commit (subagent didn't ship)"
                if outcome == "no-commit"
                else f"error (exit={child_exit})"
            )
            if fail_count >= 2:
                verdict_line = (
                    f"VERDICT: ✗ {kind} — task `{task_id or '?'}` has now "
                    f"failed {fail_count}× this session. **MATRIX RULE: "
                    f"sub-agent failed twice → mark this task `blocked` "
                    f"with the reason and MOVE ON. Do NOT retry a third "
                    f"time.**"
                )
            else:
                verdict_line = (
                    f"VERDICT: ✗ {kind} — task `{task_id or '?'}` fail #{fail_count}. "
                    f"One retry allowed by matrix; after that mark blocked."
                )
        task_tag = f" (init={initiative_id}, task={task_id})" if task_id else ""
        # py-1.10.25 — Pass-complete detection. When no active/next
        # initiative has any task left in {next, active, in_progress},
        # the architect is done and the wake forces the 4-bucket
        # end-of-pass summary instead of allowing more dispatches.
        pass_complete = self._roadmap_pass_complete()
        if pass_complete:
            continuation = (
                "**END-OF-PASS DETECTED.** The roadmap has NO remaining "
                "actionable tasks (every active/next initiative is either "
                "fully shipped or fully blocked). DO NOT dispatch more "
                "subagents. Emit the 4-bucket summary NOW (shipped / "
                "stubs-in-place / deferred-ops / decisions, + "
                "spec-needs-clarification if any), then end your turn. "
                "The pass is closed."
            )
        else:
            continuation = (
                "Continue the roadmap pass: apply the verdict, mark "
                "the originating task done/blocked accordingly, then dispatch "
                "the next wave (or emit the end-of-pass summary if everything "
                "actionable is shipped or blocked)."
            )
        wake_text = (
            f"[architect-wake] Subagent `{child_conv}`{agent_tag}{task_tag} finished{exit_tag}.\n\n"
            f"{verdict_line}\n\n"
            f"Result preview:\n{preview}\n\n"
            f"{continuation}"
        )
        _debug_emit(
            "architect-wake",
            msg=f"waking {parent_conv} on {outcome} of {child_conv}"
            + (f" (task {task_id} fail#{fail_count})" if fail_count else ""),
            conv=parent_conv,
            agent_id=child_agent_id,
            lvl=("warn" if outcome != "success" and fail_count >= 2 else "info"),
            data={
                "child_conv": child_conv,
                "child_exit": child_exit,
                "outcome": outcome,
                "task_id": task_id,
                "initiative_id": initiative_id,
                "task_fail_count": fail_count,
                "preview_len": len(preview),
                "preview_head": preview[:200],
            },
        )
        try:
            code, resp = self.chat_dispatch(
                {
                    "conv": parent_conv,
                    "text": wake_text,
                    "author": "architect-wake",
                    "agent_type": "roadmap-architect",
                }
            )
            if code >= 400:
                _log(
                    f"architect-wake dispatch to {parent_conv} returned {code}: {resp}"
                )
                _debug_emit(
                    "architect-wake.failed",
                    msg=f"chat_dispatch returned {code}",
                    lvl="warn",
                    conv=parent_conv,
                    data={"code": code, "resp": resp},
                )
        except Exception as e:
            _log(f"architect-wake dispatch raised for {parent_conv}: {e}")
            _debug_emit(
                "architect-wake.failed",
                msg=f"chat_dispatch raised: {e}",
                lvl="error",
                conv=parent_conv,
            )
        # py-1.11.0 — Re-broadcast the PARENT's activity. The wake just
        # re-dispatched the architect (live=true again) or queued it
        # (still live with pending merged). Child broadcast + child
        # auto-archive happen directly from the runner's emit-final
        # path so they fire even when there's no parent to wake.
        self._broadcast_conv_activity(parent_conv)

    def chat_cancel(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        conv = str(body.get("conv") or "").strip()
        if not conv:
            return 400, {"error": "conv required"}
        cancelled, dropped = self.chat_sessions.cancel(conv)
        # py-1.10.0 — propagate to runs. If the cancelled conv belongs
        # to an active run (started via /runs), mark it cancelled too
        # and emit run.cancelled. Operator hitting the chat's StopBar
        # converges with hitting ■ on the initiative card.
        run = self.runs.find_by_conv(conv)
        if run is not None:
            self.runs.cancel(run["id"])
        if not cancelled:
            return 200, {
                "ok": True,
                "cancelled": False,
                "reason": "no active turn for that conv",
                "run_cancelled": run["id"] if run else None,
            }
        self.hub.broadcast(
            {
                "type": "chat.cancelled",
                "conv": conv,
                "ts": _iso_now(),
                "dropped_pending": dropped,
            }
        )
        # py-1.11.0 — conv.activity flip. The conv is no longer live;
        # if a parent was coordinating it, the parent's waiting_on
        # shrinks (and may go empty → coordinating=false).
        parent = self._conv_meta_parent(conv)
        self._broadcast_conv_activity(conv)
        if parent:
            self._broadcast_conv_activity(parent)
        return 200, {
            "ok": True,
            "cancelled": True,
            "dropped_pending": dropped,
            "run_cancelled": run["id"] if run else None,
        }

    # ── py-1.10.0: story-run coordinator ────────────────────────────
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

    # ── py-1.5.0: daemon-side archive lifecycle ───────────────────────
    def chat_archive_set(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        conv = str(body.get("conv") or "").strip()
        if not conv:
            return 400, {"error": "conv required"}
        by = str(body.get("author") or "").strip()
        entry = self.chat_archive.archive(conv, by=by)
        # py-1.11.1 — Single broadcast on the snapshot.v1 contract. The
        # legacy `chat.archived` alias was retired in Phase 2.
        self.hub.broadcast(
            {
                "type": "conv.archived",
                "conv": conv,
                "archived_at": entry.get("archived_at"),
                "by": entry.get("by"),
                "ts": entry.get("archived_at"),
            }
        )
        return 200, {"ok": True, "archived": entry}

    def chat_archive_clear(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        conv = str(body.get("conv") or "").strip()
        if not conv:
            return 400, {"error": "conv required"}
        was_archived = self.chat_archive.unarchive(conv)
        if was_archived:
            self.hub.broadcast(
                {
                    "type": "conv.unarchived",
                    "conv": conv,
                    "ts": _iso_now(),
                }
            )
        return 200, {"ok": True, "unarchived": was_archived, "conv": conv}

    # ── py-1.2.0: self-update (standard v7 §10.4) ──────────────────────
    def self_update(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        """Download a new daemon.py, validate it, swap it in, spawn the
        replacement on a free port, and schedule our own shutdown.
        The cockpit reconnects to the new port via re-discovery (same
        cluster_id, dedupe collapses the rail).

        Refused (409) while any chat turn is mid-stream — killing the
        daemon kills its claude-code children. The cockpit can cancel
        the conv first and retry.

        Network/syntax failures keep the running daemon untouched —
        the new download lands at daemon.py.new and is only swapped
        in after ast.parse() accepts it.
        """
        # 1. Refuse if any chat turn is active.
        active = self.chat_sessions.list_active()
        if active:
            return 409, {
                "error": "chat turn in progress",
                "convs": active,
                "hint": "POST /chat/cancel for each conv first, then retry.",
            }
        # 2. Resolve the download source. cluster.yaml takes precedence
        #    over the optional `url` in the body — operator config wins.
        cfg_src = None
        try:
            d = (
                self.cluster.data.get("daemon")
                if isinstance(self.cluster.data, dict)
                else None
            )
            if isinstance(d, dict):
                cfg_src = d.get("auto_update_source")
        except Exception:
            cfg_src = None
        url = (
            (isinstance(cfg_src, str) and cfg_src.strip())
            or str(body.get("url") or "").strip()
            or "https://meshkore.com/reference/cluster/scripts/daemon.py"
        )
        if not (url.startswith("https://") or url.startswith("http://localhost")):
            return 400, {
                "error": "auto_update_source must be HTTPS (or http://localhost for testing)",
                "url": url,
            }
        # 3. Download to .new.
        import urllib.request
        import ast
        import shutil
        import sys
        import subprocess as _sp

        scripts_dir = self.paths.scripts_dir
        scripts_dir.mkdir(parents=True, exist_ok=True)
        new_path = scripts_dir / "daemon.py.new"
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": f"meshcore-py/{DAEMON_VERSION} self-update"}
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                payload = r.read()
            new_path.write_bytes(payload)
        except Exception as e:
            try:
                new_path.unlink()
            except Exception:
                pass
            return 500, {"error": "download failed", "url": url, "detail": str(e)}
        # 4. Syntax-check before swapping. Rejects HTML 404 pages,
        #    partial downloads, accidental binary content.
        try:
            ast.parse(payload)
        except SyntaxError as e:
            try:
                new_path.unlink()
            except Exception:
                pass
            return 500, {
                "error": "syntax check failed on downloaded daemon.py — running daemon untouched",
                "url": url,
                "detail": str(e),
            }
        # Quick sanity: must declare DAEMON_VERSION somewhere.
        if b"DAEMON_VERSION" not in payload:
            try:
                new_path.unlink()
            except Exception:
                pass
            return 500, {
                "error": "download does not look like a MeshKore daemon (no DAEMON_VERSION marker)",
                "url": url,
            }
        # 5. Backup current binary so the operator can roll back.
        current = scripts_dir / "daemon.py"
        backup = scripts_dir / "daemon.py.bak"
        try:
            if current.exists():
                shutil.copy2(current, backup)
        except Exception as e:
            return 500, {"error": "backup failed — refusing to swap", "detail": str(e)}
        # 6. Atomic rename .new → daemon.py.
        try:
            new_path.replace(current)
        except Exception as e:
            return 500, {"error": "rename failed", "detail": str(e)}
        # 6.5. py-1.8.0 — also refresh the bundled TLS cert if the
        #      published source serves one alongside daemon.py.
        #      Without this the new daemon comes up as plain HTTP
        #      while the cockpit still expects HTTPS, and the
        #      switch-to-new-port handshake fails. Best-effort: if
        #      either file 404s, we keep the existing tls/ bundle.
        if url.startswith("https://") and url.endswith("/daemon.py"):
            tls_dir = scripts_dir / "tls"
            tls_dir.mkdir(parents=True, exist_ok=True)
            base_url = url[: -len("/daemon.py")] + "/tls"
            for fname, mode in (("fullchain.pem", 0o644), ("privkey.pem", 0o600)):
                try:
                    treq = urllib.request.Request(
                        f"{base_url}/{fname}",
                        headers={
                            "User-Agent": f"meshcore-py/{DAEMON_VERSION} self-update"
                        },
                    )
                    with urllib.request.urlopen(treq, timeout=10) as tr:
                        tls_payload = tr.read()
                    if not tls_payload.startswith(b"-----BEGIN"):
                        _log(f"self-update: skipped tls/{fname} — not a PEM payload")
                        continue
                    target = tls_dir / fname
                    target.write_bytes(tls_payload)
                    try:
                        os.chmod(target, mode)
                    except Exception:
                        pass
                except Exception as e:
                    # 404 / network / TLS error — keep whatever bundle
                    # the operator already had on disk. The new daemon
                    # will fall back to plain HTTP if neither lands.
                    _log(f"self-update: tls/{fname} refresh skipped ({e})")
        # 7. Spawn the replacement on the SAME port (py-1.14.3).
        #    Previously we picked a NEW free port and let the cockpit
        #    re-discover the daemon — fragile (port hunting, WS fatal,
        #    operator-visible "taking longer than usual"). Now the new
        #    process is told to WAIT for OUR port to free
        #    (MESHKORE_REEXEC_WAIT_PORT=1 → serve_forever retries the
        #    bind for ~12 s). We release the socket by exiting promptly;
        #    the new daemon binds the identical port and the cockpit's
        #    WS just reconnects to the same URL — zero operator action,
        #    no port change, no front-end reload.
        new_port = self.port
        child_env = {**os.environ, "MESHKORE_REEXEC_WAIT_PORT": "1"}
        try:
            proc = _sp.Popen(
                [sys.executable, str(current), "--port", str(new_port)],
                cwd=str(self.paths.root),
                env=child_env,
                stdin=_sp.DEVNULL,
                stdout=_sp.DEVNULL,
                stderr=_sp.DEVNULL,
                start_new_session=True,  # detach from our process group
            )
        except Exception as e:
            return 500, {"error": "failed to spawn new daemon", "detail": str(e)}
        # 8. Release our socket + exit promptly so the child's bind-retry
        #    succeeds fast. A short delay lets the 202 response flush and
        #    the handoff broadcast reach connected cockpits first.
        SHUTDOWN_DELAY = 0.6

        def _self_kill():
            try:
                self.hub.broadcast(
                    {
                        "type": "daemon.self_update.handing_off",
                        "new_pid": proc.pid,
                        "new_port": new_port,
                        "same_port": True,
                        "ts": _iso_now(),
                    }
                )
            except Exception:
                pass
            # Close the listen socket explicitly before exit so the OS
            # frees the port immediately for the child's retry (don't
            # wait for os._exit's implicit FD reclaim under load).
            try:
                if self.server is not None:
                    self.server.server_close()
            except Exception:
                pass
            os._exit(0)

        threading.Timer(SHUTDOWN_DELAY, _self_kill).start()
        return 202, {
            "ok": True,
            "new_pid": proc.pid,
            "new_port": new_port,
            "same_port": True,
            "shutdown_in_sec": SHUTDOWN_DELAY,
            "old_backup": str(backup.relative_to(self.paths.root))
            if backup.exists()
            else None,
            "old_version": DAEMON_VERSION,
            "source_url": url,
        }

    # ── U-DAEMON-09: message append + version stubs ────────────────────
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

    # ── U-DAEMON-04: task lifecycle ────────────────────────────────────
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

    # ── U-DAEMON-03 finish: declare a new agent identity ───────────────
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

    # ── HTTP body for /health and /info ────────────────────────────────
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

        Cheap for cluster sizes we ship today (≤ a few hundred K events
        across all jsonl + gz files combined → low-ms reads). If this
        becomes a hot spot we'd memoise on file mtimes or maintain an
        incremental `.runtime/conv-index/` cache; not needed yet."""
        out: Dict[str, Dict[str, Any]] = {}
        if not self.paths.timeline_dir.exists():
            return out
        chat_types = ("chat.user", "chat.assistant", "chat.assistant.final")
        for f in _iter_timeline_files(self.paths):
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

    # ── lifecycle ──────────────────────────────────────────────────────
    def serve_forever(self) -> None:
        self._write_runtime()
        # py-1.10.17 — Initialise the debug stream singleton FIRST so
        # boot-time `_log()` calls below already land in debug.jsonl.
        # py-1.10.21 — Honour `cluster.yaml.debug.enabled: false` for
        # downstream clusters that don't want the disk footprint.
        # Default is ON (this is MeshKore-native dogfooding).
        # DM7 — _DEBUG_LOG lives in utils.py. set_debug_log() wires it
        # so every sibling module's late-binding lookup finds the same
        # singleton. Works identically in source-tree dev and bundle.
        if _debug_enabled(self.cluster):
            set_debug_log(DebugLog(self.paths.runtime / "debug.jsonl"))
            _debug_emit(
                "boot",
                msg=f"daemon {DAEMON_VERSION} starting on port {self.port}",
                data={"identity": self.identity, "cluster": self.cluster.id},
            )
        else:
            set_debug_log(None)
            _log("debug stream: disabled by cluster.yaml.debug.enabled=false")
        handler = make_handler(self)
        # py-1.12.24 — Bounded worker pool. Cap configurable via
        # cluster.yaml.daemon.http.max_workers (default 64). Prevents
        # the unbounded thread spawn that caused the 2026-06-10 hang.
        d_block = (
            self.cluster.data.get("daemon")
            if isinstance(self.cluster.data, dict)
            else None
        )
        http_block = (d_block or {}).get("http") if isinstance(d_block, dict) else None
        max_workers = int((http_block or {}).get("max_workers") or 64)
        # py-1.14.3 — same-port re-exec support. When a self-update
        # handed off to us with MESHKORE_REEXEC_WAIT_PORT=1, the OLD
        # daemon is still releasing the listen socket on `self.port`.
        # Retry the bind for up to ~12 s (250 ms cadence) so we come up
        # on the SAME port — the cockpit's WS just reconnects to the
        # identical URL, no port hunting, no operator action. Without
        # the flag we bind once (fast-fail preserves the old behaviour
        # for a normal boot where a stale daemon means a real conflict).
        reexec_wait = os.environ.get("MESHKORE_REEXEC_WAIT_PORT", "").strip() in (
            "1",
            "true",
            "yes",
            "on",
        )
        if reexec_wait:
            deadline = time.monotonic() + 12.0
            last_err: Optional[Exception] = None
            self.server = None
            while time.monotonic() < deadline:
                try:
                    self.server = PoolHTTPServer(
                        ("127.0.0.1", self.port), handler, max_workers=max_workers
                    )
                    break
                except OSError as e:
                    last_err = e
                    time.sleep(0.25)
            if self.server is None:
                _log(
                    f"re-exec: port {self.port} never freed within 12s "
                    f"({last_err}); the old daemon may be stuck"
                )
                raise SystemExit(f"re-exec bind failed on port {self.port}: {last_err}")
        else:
            self.server = PoolHTTPServer(
                ("127.0.0.1", self.port), handler, max_workers=max_workers
            )
        # py-1.12.24 — SIGUSR1 → faulthandler dump. Operator sends
        # `kill -USR1 <pid>`; daemon appends every thread's stack to
        # `.meshkore/.runtime/threads.log`. Caught lock-contention bugs
        # (like 2026-06-10) leave actionable stacks for diagnosis.
        threads_log = open(self.paths.runtime / "threads.log", "a")
        faulthandler.register(
            signal.SIGUSR1, file=threads_log, all_threads=True, chain=False
        )
        self._threads_log_fp = threads_log  # keep ref so GC doesn't close
        # D-TLS-01 — wrap the socket with TLS when the bundle is
        # present. Cockpit uses https://daemon.meshkore.com:<port>
        # then, no mixed-content / LNA Issues.
        bundle = _find_tls_bundle()
        ctx = _build_tls_context(*bundle) if bundle else None
        self.tls_enabled = ctx is not None
        if ctx is not None:
            self.server.socket = ctx.wrap_socket(self.server.socket, server_side=True)
        scheme = "https" if self.tls_enabled else "http"
        _log(
            f"meshcore-py listening on {scheme}://127.0.0.1:{self.port} "
            f"(identity={self.identity}, cluster={self.cluster.id}, "
            f"tls={'on (daemon.meshkore.com)' if self.tls_enabled else 'off'})"
        )
        # D-CRON-02: start the scheduler. Ticks every 10s in a background
        # thread; cluster.yaml.crons jobs fire from here, no LaunchAgent.
        self.cron_scheduler.start()
        # py-1.10.27 — Quota prober. Wakes every 60s, probes paused
        # quota keys, unpauses (or extends pause) based on outcome.
        # Initiative `quota-aware-dispatch`.
        self.quota_prober = QuotaProber(self)
        self.quota_prober.start()
        # py-1.12.1 — Periodic CDN poll + idle-aware self-update. Honors
        # cluster.yaml.daemon.auto_update (opt-out) and
        # auto_update_check_interval_sec (default 30 min). Keeps fleets
        # of long-running daemons current without operator action.
        self.version_watcher = VersionWatcher(self)
        self.version_watcher.start()
        # py-1.12.16 — Chat-session reaper. Sweeps every 30 s for slots
        # whose subprocess exited without runner.done.set() (leaving the
        # conv stuck `live: true`) and for slots running past the
        # hard-timeout. Broadcasts conv.activity {live: false} on reap.
        # Initiative: stuck-live recovery (operator field report
        # 2026-06-10, IKA cluster).
        self.chat_session_reaper = ChatSessionReaper(self)
        self.chat_session_reaper.start()
        try:
            self.server.serve_forever(poll_interval=0.5)
        finally:
            try:
                self.cron_scheduler.stop()
            except Exception:
                pass
            try:
                if getattr(self, "quota_prober", None) is not None:
                    self.quota_prober.stop()
            except Exception:
                pass
            try:
                if getattr(self, "chat_session_reaper", None) is not None:
                    self.chat_session_reaper.stop()
            except Exception:
                pass
            self.cleanup()

    # py-1.12.16+: graceful-drain default. Configurable via
    # `cluster.yaml.daemon.shutdown_grace_secs` (int, 0 = no drain).
    DEFAULT_SHUTDOWN_GRACE_SECS = 30

    def request_shutdown(self) -> None:
        if self.stopping.is_set():
            return
        self.stopping.set()
        # py-1.12.16+: drain in-flight chat sessions BEFORE tearing down
        # the server. Without this, SIGTERM kills the daemon → propagates
        # to every claude-code subprocess → operator's mid-turn work is
        # lost (field report 2026-06-10: 4-minute-old subprocess died
        # mid-thinking when the daemon was killed to deploy py-1.12.16,
        # the user prompt msg_count went up but no assistant reply ever
        # came back).
        try:
            grace_cfg = (
                self.cluster.data.get("daemon")
                if isinstance(self.cluster.data, dict)
                else None
            ) or {}
            grace_secs = int(
                grace_cfg.get("shutdown_grace_secs", self.DEFAULT_SHUTDOWN_GRACE_SECS)
            )
        except Exception:
            grace_secs = self.DEFAULT_SHUTDOWN_GRACE_SECS
        try:
            in_flight = list(self.chat_sessions.list_active())
        except Exception:
            in_flight = []
        if in_flight and grace_secs > 0:
            _log(
                f"shutdown: draining {len(in_flight)} in-flight session(s) "
                f"(grace={grace_secs}s) — {in_flight}"
            )
            _debug_emit(
                "shutdown.drain.start",
                msg=f"draining {len(in_flight)} session(s) with {grace_secs}s grace",
                lvl="warn",
                data={"in_flight": in_flight, "grace_secs": grace_secs},
            )
            try:
                self.hub.broadcast(
                    {
                        "type": "daemon.shutting_down",
                        "ts": _iso_now(),
                        "in_flight": in_flight,
                        "grace_secs": grace_secs,
                    }
                )
            except Exception:
                pass
            deadline = time.time() + grace_secs
            while time.time() < deadline:
                try:
                    still = self.chat_sessions.list_active()
                except Exception:
                    still = []
                if not still:
                    _log("shutdown: all sessions drained, proceeding")
                    _debug_emit(
                        "shutdown.drain.done",
                        msg="all in-flight sessions finished cleanly",
                    )
                    break
                time.sleep(0.5)
            else:
                try:
                    still = self.chat_sessions.list_active()
                except Exception:
                    still = []
                if still:
                    _log(
                        f"shutdown: grace expired with {len(still)} session(s) "
                        f"still active — proceeding (subprocesses will die): {still}"
                    )
                    _debug_emit(
                        "shutdown.drain.timeout",
                        msg=f"{len(still)} session(s) still active after {grace_secs}s",
                        lvl="warn",
                        data={"still_active": still, "grace_secs": grace_secs},
                    )
        _log("shutdown requested — closing clients + server")
        try:
            self.hub.broadcast({"type": "daemon.shutdown", "ts": _iso_now()})
        except Exception:
            pass
        # Let the broadcast flush before tearing down
        time.sleep(0.2)
        self.hub.shutdown()
        self.state_manager.shutdown()
        if self.server is not None:
            threading.Thread(target=self.server.shutdown, daemon=True).start()

    def cleanup(self) -> None:
        try:
            if (
                self.paths.pid_file.exists()
                and self.paths.pid_file.read_text().strip() == str(os.getpid())
            ):
                self.paths.pid_file.unlink()
        except OSError:
            pass
        try:
            if (
                self.paths.port_file.exists()
                and self.paths.port_file.read_text().strip() == str(self.port)
            ):
                self.paths.port_file.unlink()
        except OSError:
            pass

    # ── runtime files ─────────────────────────────────────────────────
    def _write_runtime(self) -> None:
        self.paths.runtime.mkdir(parents=True, exist_ok=True)
        self.paths.pid_file.write_text(str(os.getpid()))
        self.paths.port_file.write_text(str(self.port))


# ───────────────────────────────────────────────────────────────────────
# Helpers


def _hostname_default() -> str:
    return f"{socket.gethostname().split('.')[0]}-py"


def _detect_identity(paths: Paths) -> Optional[str]:
    if not paths.agents_dir.exists():
        return None
    for yml in sorted(paths.agents_dir.glob("*.yaml")):
        return yml.stem
    return None


def _ensure_token(paths: Paths) -> str:
    """Read or freshly mint the architect bearer token."""
    paths.credentials.mkdir(parents=True, exist_ok=True)
    if paths.token_file.exists():
        tok = paths.token_file.read_text().strip()
        if tok:
            return tok
    tok = secrets.token_urlsafe(32)
    paths.token_file.write_text(tok)
    try:
        os.chmod(paths.token_file, 0o600)
    except OSError:
        pass
    _log(f"minted new architect token at {paths.token_file}")
    return tok


# ───────────────────────────────────────────────────────────────────────
# TLS — loopback subdomain (D-TLS-01)


# _daemon_base_url + _find_tls_bundle relocated to utils.py
# (DM-modularize-2). _find_tls_bundle is re-imported from utils above
# (daemon's TLS setup + health endpoint use it); _daemon_base_url is
# consumed by the prompts module directly from utils.


def _build_tls_context(cert_path: Path, key_path: Path) -> Optional[ssl.SSLContext]:
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
        return ctx
    except (ssl.SSLError, OSError) as e:
        _log(f"tls: failed to load cert ({e}); falling back to HTTP")
        return None


def _port_free(port: int) -> bool:
    # py-1.10.18 — Use SO_REUSEADDR for the probe bind too. Without it,
    # a port still in kernel TIME_WAIT (from a daemon that exited
    # seconds ago) reads as busy and the daemon migrates to the next
    # port. ThreadingHTTPServer enables reuse on the real listener, so
    # the actual bind succeeds — the test bind just lied.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _migrate_cluster_daemon_block(paths: Paths) -> None:
    """Standard v7 §10.4 migration — ensure cluster.yaml has a `daemon:`
    block with auto_update defaulted to true. Existing clusters scaffolded
    under v6 pick up the new behaviour the first time they boot a v7+
    daemon. No-op when the block (or just the field) is already there,
    so operators who set `auto_update: false` keep their preference."""
    yml = paths.cluster_yaml
    if not yml.exists():
        return
    text = yml.read_text()
    # Crude detection — we don't want to round-trip parse + reserialise
    # because our YAML parser is a tiny subset and would drop comments
    # the operator may have added. Just append a block at EOF if absent.
    has_block = re.search(r"(?m)^daemon\s*:", text) is not None
    if has_block:
        # The block exists; check for auto_update inside it. If the
        # operator wrote `daemon:` with sub-keys but not auto_update,
        # leave it alone — they're aware of the section and chose not
        # to set the field, so default applies at read time.
        return
    block = (
        "\n# Standard v7 §10.4 — Daemon self-update. Written automatically by\n"
        "# the v7+ daemon on first boot. Set auto_update: false to require\n"
        "# explicit confirmation for every version update via the V47 modal.\n"
        "daemon:\n"
        "  auto_update: true\n"
        "  auto_update_source: https://meshkore.com/reference/cluster/scripts/daemon.py\n"
    )
    # Ensure there's exactly one newline between the existing tail and our
    # appended block so YAML stays valid (no trailing whitespace gymnastics).
    if not text.endswith("\n"):
        text += "\n"
    yml.write_text(text + block)


def _last_runtime_port(paths: Paths) -> Optional[int]:
    """The port this cluster bound on its previous boot, if recorded."""
    try:
        return int(paths.port_file.read_text().strip())
    except Exception:
        return None


def _registry_read() -> Dict[str, int]:
    """Read the machine-global cluster_id → port map. Missing or corrupt
    file → {} (we'll re-derive a stable assignment from scratch)."""
    try:
        data = json.loads(_PORT_REGISTRY_FILE.read_text())
    except FileNotFoundError:
        return {}
    except Exception as e:
        _log(f"port-registry read failed ({e}); treating as empty")
        return {}
    out: Dict[str, int] = {}
    if isinstance(data, dict):
        for k, v in data.items():
            try:
                out[str(k)] = int(v)
            except (TypeError, ValueError):
                continue
    return out


def _registry_write(mapping: Dict[str, int]) -> None:
    """Persist the map atomically. Best-effort: a write failure just means
    the next boot re-derives the same assignment, so it is never fatal."""
    try:
        _PORT_REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
        tmp = _PORT_REGISTRY_FILE.with_name(_PORT_REGISTRY_FILE.name + ".tmp")
        tmp.write_text(json.dumps(mapping, indent=2, sort_keys=True) + "\n")
        tmp.replace(_PORT_REGISTRY_FILE)
    except Exception as e:
        _log(f"port-registry write failed ({e}); assignment not persisted")


def _probe_cluster_id(port: int) -> Optional[str]:
    """Best-effort identity of whoever holds `port`. Returns the served
    `cluster_id`, or "" when a socket is held but no reachable meshkore
    daemon answers /health. Used so we NEVER silently steal a sibling's
    live port — only ports owned by a *different* cluster trigger a
    reassignment."""
    import ssl as _ssl
    import urllib.request as _u

    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    for scheme in ("https", "http"):
        try:
            req = _u.Request(
                f"{scheme}://127.0.0.1:{port}/health",
                headers={"accept": "application/json"},
            )
            with _u.urlopen(req, timeout=1.5, context=ctx) as r:
                body = json.loads(r.read().decode("utf-8", "replace"))
                return str(body.get("cluster_id") or "")
        except Exception:
            continue
    return ""


def _pick_port(
    paths: Paths,
    cluster_id: str,
    cli_override: Optional[int],
    yaml_port: Optional[int],
) -> int:
    """Assign this cluster a STABLE port (py-1.15.0 — anti-drift).

    Resolution order, highest priority first:
      1. explicit ``--port`` from the operator — honoured hard, and it
         rewrites the sticky registry entry so the choice persists.
      2. the sticky registry assignment for this ``cluster_id``
         (``~/.meshkore/ports.json``).
      3. a fresh assignment, seeded from ``cluster.yaml`` ``architect.port``
         or the last ``.meshkore/.runtime/port`` when free, else the
         lowest free port in the range — then claimed + persisted.

    The chosen port is validated before returning: if it's busy AND held
    by a *different* live cluster we refuse to steal it — we reassign this
    cluster to a fresh free port and persist that. If it's held by our OWN
    ``cluster_id`` (a stale/dying instance or a self-update re-exec) we
    return it unchanged and let the bind path reclaim it. This is what
    makes drift impossible: a cluster's port only ever moves when it would
    otherwise collide with a genuinely different live cluster."""
    registry = _registry_read()
    taken_by_others = {p for cid, p in registry.items() if cid != cluster_id}

    def _claim(port: int) -> int:
        # Re-read + merge so a sibling that registered between our read and
        # now isn't clobbered (its key survives; we only set our own).
        latest = _registry_read()
        latest[cluster_id] = port
        _registry_write(latest)
        registry[cluster_id] = port
        return port

    def _lowest_free(avoid: Optional[int] = None) -> int:
        for p in range(PORT_RANGE[0], PORT_RANGE[1] + 1):
            if p == avoid or p in taken_by_others:
                continue
            if _port_free(p):
                return p
        raise SystemExit(
            f"all ports in {PORT_RANGE[0]}-{PORT_RANGE[1]} are busy or "
            f"reserved by sibling clusters; stop a sibling daemon first "
            f"or override with --port"
        )

    def _valid(p: Optional[int]) -> bool:
        return bool(p) and 1024 <= int(p) <= 65535

    # 1. operator override always wins (becomes the new sticky value)
    if _valid(cli_override):
        return _claim(int(cli_override))

    # 2/3. sticky assignment, or a fresh seed
    chosen = registry.get(cluster_id)
    if chosen is None:
        seed: Optional[int] = None
        for cand in (yaml_port, _last_runtime_port(paths)):
            if _valid(cand) and cand not in taken_by_others and _port_free(int(cand)):
                seed = int(cand)
                break
        chosen = _claim(seed if seed is not None else _lowest_free())

    # 4. anti-steal validation — never silently land on another cluster
    if not _port_free(chosen):
        holder = _probe_cluster_id(chosen)
        if holder and holder != cluster_id:
            fresh = _lowest_free(avoid=chosen)
            _log(
                f"port {chosen} held by cluster '{holder}'; reassigning "
                f"'{cluster_id}' → {fresh} (anti-drift, py-1.15.0)"
            )
            chosen = _claim(fresh)
        # else: held by our own stale/re-exec instance, or a non-meshkore
        # listener → keep the sticky port; the bind path (re-exec wait /
        # fast-fail) decides what to do next.
    return chosen


# ───────────────────────────────────────────────────────────────────────
# CLI


def _parse_args(argv: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"identity": None, "port": None, "root": None}
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("-h", "--help"):
            print(__doc__)
            raise SystemExit(0)
        if a == "--version":
            print(f"meshcore-py {DAEMON_VERSION}")
            raise SystemExit(0)
        if a == "--identity":
            out["identity"] = argv[i + 1]
            i += 2
            continue
        if a == "--port":
            out["port"] = int(argv[i + 1])
            i += 2
            continue
        if a == "--root":
            out["root"] = Path(argv[i + 1])
            i += 2
            continue
        # Positional default = root
        if not out["root"]:
            out["root"] = Path(a)
            i += 1
            continue
        print(f"unknown arg: {a}", file=sys.stderr)
        raise SystemExit(2)
    if not out["root"]:
        out["root"] = Path.cwd()
    return out


# Boot self-update tunables. Module-level so opt-out / restart-loop
# back-off behaviour is auditable from one place.
_BOOT_PROBE_THROTTLE_SECS = 60  # don't hit the CDN more than 1×/min
_BOOT_PROBE_TIMEOUT_SECS = 4  # boot must never hang waiting on CDN
_BOOT_BACKUPS_TO_KEEP = 3  # daemon.py.bak, .bak.1, .bak.2


def _boot_self_update_if_needed(paths: Paths, args: Dict[str, Any]) -> None:
    """Probe `cluster.yaml.daemon.auto_update_source` at boot and replace
    ourselves before the listener opens if the CDN serves a newer
    `DAEMON_VERSION`. py-1.10.22, hardened py-1.10.23. Initiative
    `daemon-boot-self-update`.

    Hardening (py-1.10.23):
      • Throttle: a stamp file at `.meshkore/.runtime/last-boot-update-check`
        carries the wall-clock + outcome of the last probe. Restarting
        the daemon faster than `_BOOT_PROBE_THROTTLE_SECS` skips the
        probe — a crash-restart loop won't DDoS the CDN.
      • Always-log: every restart logs one `boot self-update: <verb>`
        line so the operator can read the boot log and see exactly
        what happened (skip-throttled, skip-disabled, no-update,
        updated, failed).
      • Backup rotation: previous `daemon.py.bak` shifts to `.bak.1`,
        `.bak.1` shifts to `.bak.2`, oldest is dropped. Three rollback
        points protects against "new version regresses but already on
        CDN" scenarios.
      • TLS bundle parallel refresh: when the auto_update_source URL
        ends with `/daemon.py`, we also try `<dir>/tls/{fullchain.pem,
        privkey.pem}` so a daemon that updates also gets the matching
        cert. Falls back gracefully if either file 404s.

    Opt-outs (unchanged):
      • `cluster.yaml.daemon.auto_update: false`         — no auto-update at all.
      • `cluster.yaml.daemon.auto_update_on_boot: false` — only the boot probe is skipped (HTTP /self-update still works).
      • env `MESHKORE_DAEMON_NO_BOOT_UPDATE=1`           — operator/script override.
      • env `MESHKORE_DAEMON_FORCE_BOOT_UPDATE=1`        — bypass the throttle (operator just published a fix and wants every restart to pick it up).
      • env `MESHKORE_DAEMON_SELF_UPDATED=1`             — set by the re-exec'd child to prevent infinite update loops.
    """
    if os.environ.get("MESHKORE_DAEMON_NO_BOOT_UPDATE") == "1":
        _log("boot self-update: skipped (MESHKORE_DAEMON_NO_BOOT_UPDATE=1)")
        return
    if os.environ.get("MESHKORE_DAEMON_SELF_UPDATED") == "1":
        # Post-update child. Confirm + clear the throttle for next time.
        _log(f"boot self-update: re-exec confirmed, now running {DAEMON_VERSION}")
        _boot_update_stamp(paths, outcome="re-exec-confirmed")
        return
    # Tolerant YAML — we don't need a full Cluster object yet.
    cfg: Dict[str, Any] = {}
    try:
        if paths.cluster_yaml.exists():
            cfg = parse_simple_yaml(paths.cluster_yaml.read_text())
    except Exception:
        cfg = {}
    d_block = cfg.get("daemon") if isinstance(cfg, dict) else None
    if not isinstance(d_block, dict):
        d_block = {}
    if d_block.get("auto_update") is False:
        _log("boot self-update: skipped (cluster.yaml.daemon.auto_update=false)")
        return
    if d_block.get("auto_update_on_boot") is False:
        _log(
            "boot self-update: skipped (cluster.yaml.daemon.auto_update_on_boot=false)"
        )
        return
    # Throttle check — protects the CDN from a crash-restart loop.
    force = os.environ.get("MESHKORE_DAEMON_FORCE_BOOT_UPDATE") == "1"
    if not force:
        recent = _boot_update_last_check_age(paths)
        if recent is not None and recent < _BOOT_PROBE_THROTTLE_SECS:
            _log(
                f"boot self-update: skipped (throttled, last check "
                f"{int(recent)}s ago < {_BOOT_PROBE_THROTTLE_SECS}s)"
            )
            return
    url = str(
        d_block.get("auto_update_source")
        or "https://meshkore.com/reference/cluster/scripts/daemon.py"
    ).strip()
    if not url:
        _log("boot self-update: skipped (empty auto_update_source)")
        return
    if not (url.startswith("https://") or url.startswith("http://localhost")):
        _log(f"boot self-update: skipped (rejected URL scheme: {url[:40]!r})")
        return
    import urllib.request
    import ast

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": f"meshcore-py/{DAEMON_VERSION} boot-self-update"},
        )
        with urllib.request.urlopen(req, timeout=_BOOT_PROBE_TIMEOUT_SECS) as r:
            payload = r.read()
    except Exception as e:
        _log(f"boot self-update: skipped (download failed: {e})")
        _boot_update_stamp(paths, outcome=f"download-failed: {e}"[:120])
        return
    if b"DAEMON_VERSION" not in payload:
        _log("boot self-update: skipped (payload lacks DAEMON_VERSION marker)")
        _boot_update_stamp(paths, outcome="no-version-marker")
        return
    try:
        ast.parse(payload)
    except SyntaxError as e:
        _log(f"boot self-update: skipped (syntax error in payload: {e})")
        _boot_update_stamp(paths, outcome=f"syntax-error: {e}"[:120])
        return
    m = re.search(rb'(?m)^DAEMON_VERSION\s*=\s*"([^"]+)"', payload)
    if not m:
        _log("boot self-update: skipped (cannot locate DAEMON_VERSION literal)")
        _boot_update_stamp(paths, outcome="version-literal-not-found")
        return
    new_version = m.group(1).decode("ascii", errors="replace")
    if not _version_is_newer(new_version, DAEMON_VERSION):
        _log(f"boot self-update: no update (CDN={new_version}, local={DAEMON_VERSION})")
        _boot_update_stamp(paths, outcome=f"no-update (cdn={new_version})")
        return
    _log(
        f"boot self-update: CDN serves {new_version}, we are "
        f"{DAEMON_VERSION} — swapping + re-exec"
    )
    scripts_dir = paths.scripts_dir
    scripts_dir.mkdir(parents=True, exist_ok=True)
    current = scripts_dir / "daemon.py"
    new_path = scripts_dir / "daemon.py.new"
    try:
        new_path.write_bytes(payload)
        if current.exists():
            _rotate_daemon_backups(scripts_dir, current)
        new_path.replace(current)
    except Exception as e:
        _log(f"boot self-update: swap failed ({e}) — keeping current version")
        try:
            new_path.unlink()
        except Exception:
            pass
        _boot_update_stamp(paths, outcome=f"swap-failed: {e}"[:120])
        return
    # Best-effort TLS bundle refresh — parity with the HTTP /self-update
    # path. If the CDN serves daemon.py at <base>/scripts/daemon.py we
    # also try <base>/scripts/tls/{fullchain.pem,privkey.pem}.
    if url.endswith("/daemon.py"):
        _refresh_tls_bundle_from_cdn(scripts_dir, url, new_version)
    _boot_update_stamp(paths, outcome=f"updated {DAEMON_VERSION}->{new_version}")
    env = dict(os.environ)
    env["MESHKORE_DAEMON_SELF_UPDATED"] = "1"
    _log(f"boot self-update: re-execing into {new_version}")
    try:
        os.execve(sys.executable, [sys.executable, str(current), *sys.argv[1:]], env)
    except Exception as e:
        _log(
            f"boot self-update: execve failed ({e}) — keep running old in-memory code; next restart picks up new file"
        )
        return


def _boot_update_stamp(paths: Paths, *, outcome: str) -> None:
    """Persist `{ts, outcome, version}` so the throttle check at the
    next boot has something to read. Quiet on I/O errors."""
    try:
        paths.runtime.mkdir(parents=True, exist_ok=True)
        stamp = paths.runtime / "last-boot-update-check"
        stamp.write_text(
            json.dumps(
                {
                    "ts": _iso_now(),
                    "epoch": int(time.time()),
                    "outcome": outcome,
                    "version": DAEMON_VERSION,
                },
                indent=2,
            )
        )
    except OSError:
        pass


def _boot_update_last_check_age(paths: Paths) -> Optional[float]:
    """Seconds since the last boot probe, or None if no stamp exists /
    is unreadable. Caller decides what to do with `None` (we treat it
    as 'no recent check, go ahead and probe')."""
    stamp = paths.runtime / "last-boot-update-check"
    try:
        if not stamp.exists():
            return None
        data = json.loads(stamp.read_text() or "{}")
        epoch = float(data.get("epoch") or 0)
        if epoch <= 0:
            return None
        age = time.time() - epoch
        return max(0.0, age)
    except (OSError, ValueError, TypeError):
        return None


def _rotate_daemon_backups(scripts_dir: "Path", current: "Path") -> None:
    """Shift daemon.py.bak.1 → .bak.2, daemon.py.bak → .bak.1, then
    write the current binary to .bak. Keeps `_BOOT_BACKUPS_TO_KEEP`
    rollback points; oldest gets dropped. Idempotent + tolerant — any
    missing intermediate just skips that shift."""
    import shutil

    for i in range(_BOOT_BACKUPS_TO_KEEP - 1, 0, -1):
        src = scripts_dir / (f"daemon.py.bak.{i - 1}" if i > 1 else "daemon.py.bak")
        dst = scripts_dir / f"daemon.py.bak.{i}"
        try:
            if src.exists():
                src.replace(dst)
        except OSError:
            pass
    try:
        shutil.copy2(current, scripts_dir / "daemon.py.bak")
    except OSError as e:
        _log(f"boot self-update: backup write failed ({e}) — proceeding anyway")


def _refresh_tls_bundle_from_cdn(
    scripts_dir: "Path", daemon_url: str, ver: str
) -> None:
    """Pull `<daemon-dir>/tls/{fullchain.pem,privkey.pem}` to keep the
    cert in lockstep with daemon.py. py-1.8.0 introduced this for the
    HTTP /self-update path; py-1.10.23 mirrors it on boot. Failures
    keep the existing on-disk bundle — never wedges the daemon."""
    import urllib.request

    tls_dir = scripts_dir / "tls"
    tls_dir.mkdir(parents=True, exist_ok=True)
    base_url = daemon_url[: -len("/daemon.py")] + "/tls"
    for fname, mode in (("fullchain.pem", 0o644), ("privkey.pem", 0o600)):
        try:
            treq = urllib.request.Request(
                f"{base_url}/{fname}",
                headers={"User-Agent": f"meshcore-py/{ver} boot-tls-refresh"},
            )
            with urllib.request.urlopen(treq, timeout=_BOOT_PROBE_TIMEOUT_SECS) as r:
                payload = r.read()
            if not payload.startswith(b"-----BEGIN"):
                _log(f"boot self-update: tls/{fname} skipped (not PEM)")
                continue
            target = tls_dir / fname
            target.write_bytes(payload)
            try:
                os.chmod(target, mode)
            except OSError:
                pass
            _log(f"boot self-update: refreshed tls/{fname}")
        except Exception as e:
            _log(f"boot self-update: tls/{fname} refresh skipped ({e})")


def _version_is_newer(a: str, b: str) -> bool:
    """True iff version `a` is strictly newer than `b`. Both look like
    `py-1.10.21`. Compares the dotted tuple after stripping the prefix;
    any non-numeric chunk sorts last (so `py-1.10.21-rc1` < `py-1.10.21`
    is intentional — release wins over pre-release)."""

    def parse(v: str) -> Tuple[int, ...]:
        core = v.strip()
        if core.startswith("py-"):
            core = core[3:]
        # Drop any trailing -suffix
        if "-" in core:
            core = core.split("-", 1)[0]
        out: List[int] = []
        for chunk in core.split("."):
            try:
                out.append(int(chunk))
            except ValueError:
                out.append(-1)  # unknown chunks rank last
        return tuple(out)

    try:
        return parse(a) > parse(b)
    except Exception:
        return False


def main() -> None:
    args = _parse_args(sys.argv[1:])
    paths = Paths(args["root"])
    if not paths.meshkore.exists():
        raise SystemExit(
            f"\n .meshkore/ not found at {paths.meshkore}."
            "\n   Run this script from a repo that already has a .meshkore/ tree,"
            "\n   or pass --root <path>. See https://meshkore.com/standard for"
            "\n   the canonical layout.\n"
        )
    # py-1.10.22 — Boot self-update. Pulls auto_update_source from the
    # CDN before the listener opens; if the CDN serves a newer
    # DAEMON_VERSION, atomic-swaps daemon.py and re-execs us. This is
    # what prevents the "stale daemon silently breaks Run All" failure
    # mode where an operator-spawned cluster keeps running py-1.10.13
    # forever (architect-wake hook absent → architect stuck idle).
    # Opt-out per-cluster via `cluster.yaml.daemon.auto_update_on_boot: false`.
    _boot_self_update_if_needed(paths, args)
    daemon = Daemon(paths, identity=args["identity"], requested_port=args["port"])

    # Graceful shutdown on signal
    def _on_signal(signum, _frame):
        _log(f"signal {signum} received")
        daemon.request_shutdown()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _on_signal)
        except ValueError:
            pass  # Windows main-thread quirk; ignore

    daemon.serve_forever()
    _log("daemon stopped cleanly")


if __name__ == "__main__":
    main()
