#!/usr/bin/env python3
"""
registry.py — ProjectRegistry: the Daemon's map of project_id → ProjectContext.

DC-2 of `daemon-centralized`. The seam from DC-1 (one held ProjectContext)
becomes a registry. Today the Daemon registers exactly ONE project (the boot
cluster) and keeps `self._ctx` pointing at it, so behaviour is identical to
DC-1 — but the holder is now multi-project capable. DC-5 adds the `/projects`
API that registers more; DC-4 makes request dispatch resolve the right context
per request.

Contexts are LAZY: a project registered by path builds its ProjectContext on
first `get()`. The boot project is registered already-built (it's constructed
once in Daemon.__init__, same as before).

GLOBAL services (hub, identity, daemon backref) are injected once and shared
across every ProjectContext the registry builds.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from projectctx import ProjectContext
from paths import Paths
from utils import _log


class ProjectRegistry:
    def __init__(self, *, hub: Any, identity: str, daemon: Any) -> None:
        self._hub = hub
        self._identity = identity
        self._daemon = daemon
        self._roots: Dict[str, Path] = {}
        self._ctxs: Dict[str, ProjectContext] = {}
        self._lock = threading.RLock()
        self.default_project_id: Optional[str] = None

    # ── registration ────────────────────────────────────────────────────
    def add_built(
        self, project_id: str, ctx: ProjectContext, *, default: bool = False
    ) -> str:
        """Insert an already-constructed context (the boot project)."""
        with self._lock:
            self._roots[project_id] = ctx.paths.root
            self._ctxs[project_id] = ctx
            if default or self.default_project_id is None:
                self.default_project_id = project_id
        return project_id

    def add_path(self, project_id: str, root: Path, *, default: bool = False) -> str:
        """Register a project by path; its context is built lazily on get()."""
        with self._lock:
            self._roots[project_id] = Path(root)
            if default or self.default_project_id is None:
                self.default_project_id = project_id
        return project_id

    # ── lookup ───────────────────────────────────────────────────────────
    def get(self, project_id: Optional[str] = None) -> Optional[ProjectContext]:
        """Resolve a context, building it lazily. None → the default project."""
        with self._lock:
            pid = project_id or self.default_project_id
            if pid is None:
                return None
            ctx = self._ctxs.get(pid)
            if ctx is not None:
                return ctx
            root = self._roots.get(pid)
            if root is None:
                return None
            # Build inside the lock: ProjectContext.__init__ only reads the FS
            # (cluster.yaml etc.) and starts NO threads — cheap + safe to hold.
            _log(f"registry: building context for project {pid!r} at {root}")
            ctx = ProjectContext(
                Paths(root),
                hub=self._hub,
                identity=self._identity,
                daemon=self._daemon,
            )
            self._ctxs[pid] = ctx
            return ctx

    def has(self, project_id: str) -> bool:
        with self._lock:
            return project_id in self._roots

    def unregister(self, project_id: str) -> bool:
        with self._lock:
            existed = project_id in self._roots
            self._roots.pop(project_id, None)
            self._ctxs.pop(project_id, None)
            if self.default_project_id == project_id:
                self.default_project_id = next(iter(self._roots), None)
        return existed

    # ── introspection ────────────────────────────────────────────────────
    def ids(self) -> List[str]:
        with self._lock:
            return list(self._roots.keys())

    def built_contexts(self) -> List[ProjectContext]:
        with self._lock:
            return list(self._ctxs.values())
