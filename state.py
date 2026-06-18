"""state.py — the StateManager background poller.

Phase 2 lifted StateManager out of daemon.py (DA-STATE-01); Phase 3d moved the
pure FS→state projection (build_state + ordering/reconcile/git helpers) on to
statebuild.py, which StateManager imports. StateManager keeps its runtime daemon
backref via bind_daemon() (set after construction) so there is no import cycle —
state.py imports only leaf modules + statebuild."""

from __future__ import annotations

import hashlib
import json
import os
import struct
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from cluster import Cluster
from constants import FS_POLL_SEC
from hub import Hub
from paths import Paths
from statebuild import build_state
from utils import _iso_now

if TYPE_CHECKING:
    from daemon import Daemon


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
        # py-1.16.0 (D-FSPOLL-01) — os.scandir recursion instead of
        # rglob("*") + per-file stat(). scandir's DirEntry carries the
        # stat result, so is_dir/is_file/stat resolve without an extra
        # syscall per file — roughly halves the idle IO of this 1.5s
        # change-detector on a large roadmap tree. Identical file set and
        # signature inputs (sorted path + mtime + size), so detection is
        # unchanged (still catches content edits via st_mtime).
        h = hashlib.sha1()
        stack = [
            r
            for r in (
                self.paths.modules_dir,
                self.paths.docs_dir,
                self.paths.initiatives,
                self.paths.public,
            )
            if r.exists()
        ]
        files: List[Tuple[str, float, int]] = []
        while stack:
            d = stack.pop()
            try:
                with os.scandir(d) as it:
                    for entry in it:
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                stack.append(Path(entry.path))
                            elif entry.is_file(follow_symlinks=False):
                                st = entry.stat()
                                files.append((entry.path, st.st_mtime, st.st_size))
                        except OSError:
                            pass
            except OSError:
                pass
        for path, mtime, size in sorted(files):
            h.update(path.encode())
            h.update(struct.pack(">dq", mtime, size))
        return h.hexdigest()


# ───────────────────────────────────────────────────────────────────────
# HTTP / WebSocket server


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


# ───────────────────────────────────────────────────────────────────────
# Daemon orchestrator
