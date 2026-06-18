"""statebuild.py — FS→state projection (build_state + ordering/reconcile/git helpers).

Extracted from state.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from cluster import Cluster, _patch_frontmatter, normalize_status
from constants import DAEMON_VERSION
from paths import Paths
from utils import _debug_emit, _iso_now, _log, parse_frontmatter


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
                    # py-1.20.0 — roadmap wall ordering; the cockpit reads
                    # this to paint each wall in order. Optional → None.
                    "wall_order": (
                        int(fm["wall_order"])
                        if isinstance(fm.get("wall_order"), int)
                        and not isinstance(fm.get("wall_order"), bool)
                        else None
                    ),
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
        "$schema": "https://api.meshkore.com/v1/standard.json",
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
