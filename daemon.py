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
import signal
import struct
import subprocess
import sys
import threading
import faulthandler
import time
from datetime import datetime
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# DM3 — sibling-module imports. paths.py and storage.py live next to
# daemon.py in source; the bundler concatenates them into dist/daemon.py
# in dependency order, stripping these import lines from the bundled
# output. Source-tree runs hit the sibling files via sys.path[0].
from anchor import AnchorMixin  # noqa: E402
from chatsvc import ChatMixin  # noqa: E402
from coordination import CoordinationMixin  # noqa: E402
from readapi import QueryMixin  # noqa: E402
from bootstrap import (  # noqa: E402,F401 — re-exported for main()/Daemon + tests
    _detect_identity,
    _ensure_token,
    _hostname_default,
    _last_runtime_port,
    _migrate_cluster_daemon_block,
    _pick_port,
    _probe_cluster_id,
    _registry_read,
    _registry_write,
)
from chat import ChatSessionReaper, ChatSessions  # noqa: E402
from cluster import Cluster, _patch_frontmatter, normalize_status  # noqa: E402
from constants import (  # noqa: E402,F401 — leaf consts; re-exported for callers/tests
    DAEMON_VERSION,
    FS_POLL_SEC,
    PORT_RANGE,
    _PORT_REGISTRY_DIR,
    _PORT_REGISTRY_FILE,
)
from cron import CronRunner, CronScheduler  # noqa: E402,F401
from hub import Hub  # noqa: E402
from http_server import (  # noqa: E402
    PoolHTTPServer,
    _build_tls_context,
)
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
from selfupdate import (  # noqa: E402,F401 — re-exported for serve_forever/main + tests
    VersionWatcher,
    _boot_self_update_if_needed,
)
from storage import ChatArchive, ChatQueueManager, StorageReport, UploadStore  # noqa: E402
from utils import (  # noqa: E402
    DebugLog,
    _append_timeline,
    _debug_emit,
    _debug_enabled,
    _find_tls_bundle,
    _iso_now,
    _log,
    parse_frontmatter,
    parse_simple_yaml,  # noqa: F401 — re-exported for test_refactor_characterization
    set_debug_log,
)

# ───────────────────────────────────────────────────────────────────────
# Configuration

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

# Defaults applied when a `crons:` entry omits the field.


# py-1.11.3 — Credentials CRUD constants.
#
# Names must be filesystem-safe and reasonably short. Pattern lets the
# operator use kebab/snake/dot conventions (cloudflare-token,
# openrouter.env, fly_org_id) without ever escaping the credentials
# directory.


# ───────────────────────────────────────────────────────────────────────
# Tiny YAML reader + frontmatter parser — relocated to utils.py
# (DM-modularize-2). `parse_simple_yaml` / `parse_frontmatter` are
# re-imported from utils above so `daemon.parse_simple_yaml` stays a
# stable attribute for callers and tests.


# ───────────────────────────────────────────────────────────────────────
# Cluster + state


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


class Daemon(AnchorMixin, ChatMixin, CoordinationMixin, QueryMixin):
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
        # py-1.16.1 (D-STORE-RETENTION-01) — opt-in archive retention.
        # cluster.yaml `storage.retention_days` (int) deletes archived
        # timeline .gz that many days after rotation; absent/0 = keep
        # forever (no surprise history deletion).
        _storage_cfg = (
            self.cluster.data.get("storage")
            if isinstance(self.cluster.data, dict)
            else None
        )
        try:
            _retention_days = int((_storage_cfg or {}).get("retention_days") or 0)
        except (TypeError, ValueError):
            _retention_days = 0
        self.timeline_rotator = TimelineRotator(paths, delete_days=_retention_days)
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

    # py-1.7.0 — conv → (agent_type, agent_id) sidecar. Lets the daemon
    # remember the specialisation across turns even if the cockpit
    # forgets to re-send it (and gives offline/migrated clusters a stable
    # store outside the cockpit's localStorage).

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

    # ── Agent-type pause state (py-1.10.27 — backed by QuotaState) ─────
    # The per-agent_type API is preserved as a thin wrapper over
    # QuotaState so existing callers (HTTP endpoints, wake hook) keep
    # working without contortion. Under the hood every lookup goes
    # through the (platform, model) quota_key derived from the
    # agent manifest.

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
        max_workers = int((http_block or {}).get("max_workers") or 128)
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
            # py-1.15.2 — do_handshake_on_connect=False so accept() returns
            # an un-handshaked SSLSocket immediately; the handshake is then
            # completed on a pool worker (PoolHTTPServer.process_request_thread),
            # NOT in the single accept loop. Previously a slow/half-open
            # client (browsers open speculative connections; the cockpit
            # opens many to the actively-polled project) blocked the accept
            # loop mid-handshake and the kernel refused every other
            # connection → intermittent ERR_CONNECTION_REFUSED that
            # stranded cockpit hydration.
            self.server.socket = ctx.wrap_socket(
                self.server.socket, server_side=True, do_handshake_on_connect=False
            )
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


# ───────────────────────────────────────────────────────────────────────
# TLS — loopback subdomain (D-TLS-01)


# _daemon_base_url + _find_tls_bundle relocated to utils.py
# (DM-modularize-2). _find_tls_bundle is re-imported from utils above
# (daemon's TLS setup + health endpoint use it); _daemon_base_url is
# consumed by the prompts module directly from utils.


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
