"""walls.py — initiative wall ordering (WallsMixin).

The cockpit's roadmap UI arranges initiatives into four walls — active / next /
backlog / archived — and persists each one's position within its wall via a
`wall_order: <int>` frontmatter field. This mixin owns:

  GET  /initiative/walls    → { active:[id…], next:[…], backlog:[…], archived:[…] }
  POST /initiative/reorder  → move an initiative to (wall, order), recompact

The status⇄wall mapping: active→active, next→next, done→archived, everything
else (backlog/blocked/unknown)→backlog. `wall_order` is OPTIONAL — initiatives
without it sort LAST in their wall by filename, so every pre-existing initiative
keeps working (back-compat). Coexists with the legacy linked-list `next:`
ordering; the UI reads `wall_order` only.

Daemon facet (mixin): uses self.paths / self.hub / self.state_manager.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from cluster import _patch_frontmatter, normalize_status
from utils import _iso_now, parse_frontmatter

_WALLS = ("active", "next", "backlog", "archived")
# wall → the status frontmatter value written when an initiative moves there.
_WALL_STATUS = {
    "active": "active",
    "next": "next",
    "backlog": "backlog",
    "archived": "done",
}
_FAR = 10**9  # sort key for initiatives with no wall_order → end of the wall


def _wall_of(fm: Dict[str, Any]) -> str:
    """Which wall an initiative belongs to. The `archived` flag wins — it's an
    explicit operator action that build_state's archive-reconcile never reverts
    (the reconcile only flips `status`/completed_at, e.g. done→active when child
    tasks are still open). Otherwise the wall follows status."""
    if fm.get("archived"):
        return "archived"
    s = normalize_status(fm.get("status"))
    if s == "active":
        return "active"
    if s == "next":
        return "next"
    if s == "done":
        return "archived"
    return "backlog"  # backlog, blocked, unknown


def _wall_order_of(fm: Dict[str, Any]) -> Optional[int]:
    """Read `wall_order` as an int, or None when absent/invalid. `bool` is an
    int subclass — reject it so `wall_order: true` never reads as 1."""
    v = fm.get("wall_order")
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip().lstrip("-").isdigit():
        return int(v)
    return None


class WallsMixin:
    def _initiative_fms(self) -> List[tuple]:
        """[(filename_stem, id, frontmatter)] for every initiative .md,
        filename-sorted. Skips files without an `id`."""
        out: List[tuple] = []
        d = self.paths.initiatives
        if not d.exists():
            return out
        for p in sorted(d.glob("*.md")):
            try:
                fm = parse_frontmatter(p.read_text(errors="replace"))
            except OSError:
                continue
            iid = fm.get("id")
            if iid:
                out.append((p.stem, str(iid), fm))
        return out

    def initiative_walls(self) -> Dict[str, List[str]]:
        """The four walls, each a list of initiative ids ordered by wall_order
        asc (missing → last), ties by filename."""
        buckets: Dict[str, List[tuple]] = {w: [] for w in _WALLS}
        for stem, iid, fm in self._initiative_fms():
            buckets[_wall_of(fm)].append((_wall_order_of(fm), stem, iid))
        out: Dict[str, List[str]] = {}
        for w in _WALLS:
            items = sorted(
                buckets[w], key=lambda t: (t[0] if t[0] is not None else _FAR, t[1])
            )
            out[w] = [iid for _, _, iid in items]
        return out

    def initiative_reorder(self, body: Dict[str, Any]):
        iid = str(body.get("id") or "").strip()
        wall = str(body.get("wall") or "").strip()
        order = body.get("order")
        if not iid:
            return 400, {"error": "id required"}
        if wall not in _WALLS:
            return 400, {"error": f"wall must be one of {list(_WALLS)}"}
        if not isinstance(order, int) or isinstance(order, bool):
            return 400, {"error": "order (int) required"}
        path = self.paths.initiatives / f"{iid}.md"
        if not path.exists():
            return 404, {"error": "unknown initiative", "id": iid}
        try:
            fm = parse_frontmatter(path.read_text(errors="replace"))
        except OSError as e:
            return 500, {"error": f"read failed: {e}"}

        # Update status/archived first so the moved initiative is a member of
        # the destination wall BEFORE we recompact it.
        #  • → archived: status:done + archived:true. The `archived` flag is what
        #    holds the wall even if the archive-reconcile reverts status→active
        #    (open child tasks); completed_at is left untouched.
        #  • → any other wall: status:<wall>, and CLEAR a stale `archived` flag
        #    (else _wall_of would keep it in archived).
        if _wall_of(fm) != wall:
            patch: Dict[str, Any] = {"status": _WALL_STATUS[wall]}
            if wall == "archived":
                patch["archived"] = True
            elif fm.get("archived"):
                patch["archived"] = None  # _patch_frontmatter removes None keys
            _patch_frontmatter(path, patch)

        self._recompact_wall(wall, moved_id=iid, target_order=order)
        self.hub.broadcast(
            {"type": "initiative.reordered", "id": iid, "wall": wall, "ts": _iso_now()}
        )
        # Rebuild so /state emits the new wall_order values immediately.
        self.state_manager.rebuild(broadcast=True)
        return 200, {"ok": True, "id": iid, "wall": wall, "order": order}

    def _recompact_wall(self, wall: str, *, moved_id: str, target_order: int) -> None:
        """Renumber every initiative in `wall` to wall_order 0,1,2,… (no gaps),
        with `moved_id` inserted at `target_order`. Writes only the files whose
        wall_order actually changes (_patch_frontmatter is a no-op otherwise)."""
        members = [
            (_wall_order_of(fm), stem, iid)
            for stem, iid, fm in self._initiative_fms()
            if _wall_of(fm) == wall
        ]
        ordered = [
            iid
            for _, _, iid in sorted(
                (m for m in members if m[2] != moved_id),
                key=lambda t: (t[0] if t[0] is not None else _FAR, t[1]),
            )
        ]
        idx = max(0, min(int(target_order), len(ordered)))
        ordered.insert(idx, moved_id)
        for pos, mid in enumerate(ordered):
            _patch_frontmatter(
                self.paths.initiatives / f"{mid}.md", {"wall_order": pos}
            )
