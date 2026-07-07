#!/usr/bin/env python3
"""
projectsapi.py — ProjectsMixin: the GLOBAL /projects API (DC-5).

These endpoints are machine-level, NOT project-scoped (no X-MeshKore-Project
header needed): they manage WHICH projects the single daemon serves.

    GET    /projects            list registered projects (id, name, path, …)
    POST   /projects {path,name?}  register (scaffolding .meshkore/ if absent)
    DELETE /projects/<id>       unregister (does NOT delete the project's ledger)

The registry of projects is persisted to the GlobalLedger's projects.json so
the daemon re-hydrates its projects on restart (see Daemon boot rehydrate).

Depends on `self._registry` (ProjectRegistry) and `self.global_ledger`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

from scaffold import ScaffoldError, scaffold_cluster, slugify_id
from paths import Paths
from utils import _log

# ── HARDCODED server-home backstop (FC-2 / operator-directed 2026-07-07) ─────
# The central daemon's OWN home (its `.meshkore` is the machine-global store:
# ideas, the projects registry, external creds) is NEVER a project. The
# structural test `_is_home_context` detects it by comparing the context root
# against the global-ledger root — but that comparison DEPENDS ON HOW THE DAEMON
# WAS LAUNCHED (if `MESHKORE_GLOBAL_ROOT` doesn't resolve to the home's
# `.meshkore`, the structural test returns False and the home leaks into
# /projects as if it were a project). It "kept coming back" for exactly this
# reason. This id denylist is the env-independent backstop: a cluster id in it
# is NEVER a project, regardless of launch flags, projects.json contents, or the
# structural test. `meshkore-server` is this machine's home; extend the set per
# machine via `MESHKORE_HOME_IDS` (os.pathsep- or comma-joined). Harmless on any
# machine that has no cluster by that id.
_DEFAULT_HOME_IDS = frozenset({"meshkore-server"})


class ProjectsMixin:
    # ── server-home identity (the central store, never a project) ────────
    def _home_ids(self) -> frozenset:
        """The set of cluster ids that ARE the server home / global store and
        must never be treated as projects. Hardcoded default + per-machine env
        override (`MESHKORE_HOME_IDS`, comma/os.pathsep-separated)."""
        extra = os.environ.get("MESHKORE_HOME_IDS") or ""
        ids = set(_DEFAULT_HOME_IDS)
        for chunk in extra.replace(os.pathsep, ",").split(","):
            chunk = chunk.strip()
            if chunk:
                ids.add(chunk)
        return frozenset(ids)

    def is_home(self, pid: str, root: Any = None) -> bool:
        """True if `pid` is the server home — by the hardcoded id denylist OR by
        the structural global-ledger-root test. EITHER signal is sufficient, so
        the home is excluded even when the daemon was launched such that the
        structural test can't see it. This is the single gate every /projects
        path goes through."""
        if pid and pid in self._home_ids():
            return True
        if root is None:
            root = self._registry.root_of(pid)
        return self._is_home_context(root)

    # ── persistence helpers ──────────────────────────────────────────────
    def _projects_meta(self) -> Dict[str, Dict[str, Any]]:
        data = self.global_ledger.load_projects()
        return {p["id"]: p for p in data.get("projects", []) if p.get("id")}

    def _is_home_context(self, root: Any) -> bool:
        """True if this context is the SERVER'S OWN home — its `.meshkore` IS the
        machine-global ledger (ideas, the projects registry, external creds /
        agents). The home is the central store, NOT a project: it must never
        appear in /projects, never be persisted to projects.json, and never be
        offered to the cockpit as a selectable project. Detection is automatic —
        in a per-project daemon the boot cluster's `.meshkore` is NOT the global
        ledger, so that boot project is (correctly) treated as a real project."""
        if not root:
            return False
        try:
            return (
                Path(root).resolve() / ".meshkore"
            ) == self.global_ledger.root.resolve()
        except Exception:
            return False

    def _real_project_ids(self) -> List[str]:
        """Registry ids EXCLUDING the server home — i.e. only real projects.
        Uses the combined `is_home` gate (id denylist OR structural test) so the
        home is dropped even when launched such that the structural test fails."""
        return [
            pid
            for pid in self._registry.ids()
            if not self.is_home(pid, self._registry.root_of(pid))
        ]

    def _persist_projects(self) -> None:
        """Write the real projects (id, name, path) to projects.json — NEVER the
        server home (it's the global store, not a project)."""
        meta = self._projects_meta()
        rows: List[Dict[str, Any]] = []
        for pid in self._real_project_ids():
            root = self._registry.root_of(pid)
            rows.append(
                {
                    "id": pid,
                    "name": (meta.get(pid) or {}).get("name") or pid,
                    "path": str(root) if root else "",
                }
            )
        self.global_ledger.save_projects(rows)

    # ── boot rehydrate ────────────────────────────────────────────────────
    def rehydrate_projects(self) -> None:
        """On boot, lazily register every additional project recorded in
        projects.json, then SELF-HEAL: prune any server-home entry that a past
        bad write left in projects.json. Called from __init__ after the boot
        project is registered."""
        home = self._home_ids()
        for p in self.global_ledger.load_projects().get("projects", []):
            pid, path = p.get("id"), p.get("path")
            if not pid or not path:
                continue
            # NEVER re-register the server home from a stale projects.json entry.
            if pid in home or self._is_home_context(path):
                continue
            if self._registry.has(pid):
                continue  # already registered (e.g. the boot project)
            if not Path(path).exists():
                _log(f"projects: skip {pid!r} — path gone: {path}")
                continue
            self._registry.add_path(pid, Path(path))
        self._prune_home_from_projects()

    def _prune_home_from_projects(self) -> None:
        """Rewrite projects.json without any server-home row. No-op (no write)
        when the file is empty or already clean — so a fresh machine never gets
        a machine-global ledger created by this. Idempotent self-heal for the
        recurring 'home shows up as a project' bug."""
        try:
            rows = self.global_ledger.load_projects().get("projects", [])
        except Exception:  # noqa: BLE001
            return
        home = self._home_ids()
        kept = [
            p
            for p in rows
            if p.get("id") not in home and not self._is_home_context(p.get("path"))
        ]
        if len(kept) != len(rows):
            self.global_ledger.save_projects(kept)
            _log(
                f"projects: pruned {len(rows) - len(kept)} server-home entry "
                "from projects.json (never a project)"
            )

    # ── endpoints ────────────────────────────────────────────────────────
    def projects_list(self) -> Tuple[int, Dict[str, Any]]:
        # ONLY real projects — the server home (central store: ideas, registry,
        # creds) is never a project and is excluded here.
        meta = self._projects_meta()
        real = self._real_project_ids()
        out: List[Dict[str, Any]] = []
        for pid in real:
            root = self._registry.root_of(pid)
            out.append(
                {
                    "id": pid,
                    "name": (meta.get(pid) or {}).get("name") or pid,
                    "path": str(root) if root else "",
                    "default": pid == self._registry.default_project_id,
                    "built": self._registry.is_built(pid),
                }
            )
        # The cockpit's "default project to land on" is the first REAL project
        # (never the home); None when no real project is registered yet.
        default = real[0] if real else None
        return 200, {"projects": out, "default": default}

    # ── CPL-2 (master-copilot): create-from-scratch guards ───────────────
    @staticmethod
    def _within(path: Path, ancestor: Path) -> bool:
        """True if `path` is `ancestor` itself or a descendant of it."""
        try:
            path.relative_to(ancestor)
            return True
        except ValueError:
            return False

    def _allowed_project_parents(self) -> List[Path]:
        """Operator-approved roots under which a BRAND-NEW project folder may be
        scaffolded (create-from-scratch). Default = the parent dir of every
        already-registered real project (the operator's existing project home(s),
        nowhere else) so a stray order can't scaffold anywhere on disk. Extend
        via `MESHKORE_PROJECT_ROOTS` (os.pathsep-joined absolute dirs). Adopting
        an EXISTING dir is NOT gated by this — only mkdir of a new one is."""
        parents: set = set()
        for pid in self._real_project_ids():
            root = self._registry.root_of(pid)
            if not root:
                continue
            try:
                parents.add(Path(root).expanduser().resolve().parent)
            except Exception:  # noqa: BLE001 — a bad path never widens the allowlist
                continue
        extra = os.environ.get("MESHKORE_PROJECT_ROOTS") or ""
        for chunk in extra.split(os.pathsep):
            chunk = chunk.strip()
            if not chunk:
                continue
            try:
                parents.add(Path(chunk).expanduser().resolve())
            except Exception:  # noqa: BLE001
                continue
        return sorted(parents)

    def project_register(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        # Target resolution: an explicit `path`, or `parent` + `name` (the
        # create-from-scratch "crea un proyecto nuevo" voice flow, CPL-2).
        raw = str(body.get("path") or "").strip()
        parent = str(body.get("parent") or "").strip()
        name_in = str(body.get("name") or "").strip()
        if raw:
            root = Path(raw).expanduser()
        elif parent and name_in:
            root = Path(parent).expanduser() / slugify_id(name_in)
        else:
            return 400, {"error": "path OR (parent + name) required"}

        creating = not root.exists()
        if root.exists() and not root.is_dir():
            return 400, {
                "error": "path exists but is not a directory",
                "path": str(root),
            }
        # CPL-2 — a NEW folder may only be created under an allowlisted parent.
        # Adopting an EXISTING dir stays unrestricted (the operator already put a
        # project there). `mkdir -p` then falls through to scaffold + register.
        if creating:
            try:
                target_parent = root.expanduser().resolve().parent
            except Exception:  # noqa: BLE001
                target_parent = root.parent
            allowed = self._allowed_project_parents()
            if not any(self._within(target_parent, a) for a in allowed):
                return 403, {
                    "error": "parent not allowlisted for create-from-scratch",
                    "parent": str(target_parent),
                    "allowed": [str(a) for a in allowed],
                }
            try:
                root.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                return 500, {
                    "error": f"could not create folder: {e}",
                    "path": str(root),
                }
        name = name_in or root.name
        # Scaffold the cluster ledger if this folder has none yet (the daemon
        # owns the schema — same path the `init` CLI uses).
        paths = Paths(root)
        scaffolded = False
        if not paths.cluster_yaml.exists():
            try:
                scaffold_cluster(root, name)
                scaffolded = True
            except ScaffoldError as e:
                _log(f"projects: scaffold raced/failed for {root}: {e}")
        # Build + register; project_id is the cluster id (stable, portable).
        try:
            pid = self._registry.register_root(root)
        except Exception as e:  # noqa: BLE001 — surface a clean 400 to the cockpit
            return 500, {"error": f"could not register project: {e}"}
        # The server home (central store) can never be adopted as a project.
        # Refuse to persist it to projects.json — regardless of the id denylist
        # or the structural test. (Leave the registry as-is: the home may be the
        # boot/default cluster.)
        if self.is_home(pid, root):
            return 409, {
                "error": "the server home is the central store, not a project",
                "id": pid,
                "path": str(root),
            }
        # Record the operator-facing name (registry only knows ids/paths). Persist
        # the REAL projects only (never the home) — reuse the single writer.
        meta = self._projects_meta()
        meta[pid] = {"id": pid, "name": name, "path": str(root)}
        self.global_ledger.save_projects(
            [row for row in meta.values() if not self.is_home(row.get("id", ""))]
        )
        return 201, {
            "id": pid,
            "name": name,
            "path": str(root),
            "scaffolded": scaffolded,
        }

    def project_unregister(self, project_id: str) -> Tuple[int, Dict[str, Any]]:
        if not self._registry.has(project_id):
            return 404, {"error": "unknown project", "id": project_id}
        if project_id == self._registry.default_project_id:
            return 409, {"error": "cannot unregister the default (boot) project"}
        self._registry.unregister(project_id)
        self._persist_projects()
        return 200, {"id": project_id, "unregistered": True}
