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

from pathlib import Path
from typing import Any, Dict, List, Tuple

from scaffold import ScaffoldError, scaffold_cluster
from paths import Paths
from utils import _log


class ProjectsMixin:
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
        """Registry ids EXCLUDING the server home — i.e. only real projects."""
        return [
            pid
            for pid in self._registry.ids()
            if not self._is_home_context(self._registry.root_of(pid))
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

    # ── boot rehydrate (READ-ONLY) ───────────────────────────────────────
    def rehydrate_projects(self) -> None:
        """On boot, lazily register every additional project recorded in
        projects.json. READ-ONLY: writes nothing (so a daemon/test boot never
        creates a machine-global ledger). projects.json is only ever WRITTEN by
        an explicit POST /projects. Called from __init__ after the boot project
        is registered."""
        for p in self.global_ledger.load_projects().get("projects", []):
            pid, path = p.get("id"), p.get("path")
            if not pid or not path:
                continue
            if self._registry.has(pid):
                continue  # already registered (e.g. the boot project)
            if not Path(path).exists():
                _log(f"projects: skip {pid!r} — path gone: {path}")
                continue
            self._registry.add_path(pid, Path(path))

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

    def project_register(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        raw = str(body.get("path") or "").strip()
        if not raw:
            return 400, {"error": "path required"}
        root = Path(raw).expanduser()
        if not root.exists() or not root.is_dir():
            return 400, {
                "error": "path is not an existing directory",
                "path": str(root),
            }
        name = str(body.get("name") or root.name).strip()
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
        # Record the operator-facing name (registry only knows ids/paths).
        meta = self._projects_meta()
        meta[pid] = {"id": pid, "name": name, "path": str(root)}
        self.global_ledger.save_projects(list(meta.values()))
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
