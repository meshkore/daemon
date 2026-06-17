"""state.py — FS-to-state projection + the StateManager poll loop.

Extracted from daemon.py (DA-STATE-01, daemon-architecture-v2 Phase 2).
build_state() + its ordering/reconcile/git helpers + the StateManager
background poller move VERBATIM. StateManager keeps its runtime daemon
backref via bind_daemon() (set after construction) so there is no import
cycle — state.py imports only leaf modules."""

from __future__ import annotations

import hashlib
import json
import os
import struct
import subprocess
import threading
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from cluster import Cluster, _patch_frontmatter, normalize_status
from constants import DAEMON_VERSION, FS_POLL_SEC
from hub import Hub
from paths import Paths
from utils import _debug_emit, _iso_now, _log, parse_frontmatter

if TYPE_CHECKING:
    from daemon import Daemon


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
