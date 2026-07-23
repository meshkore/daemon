#!/usr/bin/env python3
"""
globalledger.py — GlobalLedger: the machine-level (cross-project) data store.

DC-3 of `daemon-centralized`. Everything that is NOT tied to a single project
lives here, separate from any cluster's `.meshkore/`:

    <root>/                       ← the global ledger root
    ├── ideas/                    ← Ideas feature (converges here from the
    │                               interim ~/.meshkore/ideas store)
    ├── projects.json             ← the project registry (id → path, name…)
    ├── credentials/              ← creds for EXTERNAL clusters this machine
    │                               connects to
    ├── agents/                   ← config of EXTERNAL agents (mesh / buses)
    ├── config.yaml               ← global daemon config
    ├── .runtime/                 ← global runtime (port/token/version state)
    └── log/                      ← the daemon's own operational log

Root resolution (first hit wins):
  1. explicit `root=` arg (OC-1 will pass `meshkore-server/.meshkore`)
  2. env `MESHKORE_GLOBAL_ROOT`
  3. `~/.meshkore`  (machine-global default; works today while the daemon
     still boots inside a project, and is where the interim Ideas store
     already lives)

LAZY: the constructor resolves the root but creates NOTHING on disk. Dirs are
made on first write only — so spinning a Daemon in tests never scatters a
`~/.meshkore/` tree. NOT git-tracked (credentials never in git).
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from fsatomic import atomic_write_json
from utils import _log


def resolve_global_root(explicit: Optional[Path] = None) -> Path:
    if explicit is not None:
        return Path(explicit)
    env = os.environ.get("MESHKORE_GLOBAL_ROOT")
    if env:
        return Path(env)
    return Path.home() / ".meshkore"


class GlobalLedger:
    def __init__(self, root: Optional[Path] = None) -> None:
        self.root = resolve_global_root(root)
        self._lock = threading.RLock()

    # ── path accessors (no mkdir here — caller ensures on write) ─────────
    @property
    def ideas_dir(self) -> Path:
        return self.root / "ideas"

    @property
    def projects_file(self) -> Path:
        return self.root / "projects.json"

    @property
    def credentials_dir(self) -> Path:
        return self.root / "credentials"

    @property
    def agents_dir(self) -> Path:
        return self.root / "agents"

    @property
    def config_file(self) -> Path:
        return self.root / "config.yaml"

    @property
    def runtime_dir(self) -> Path:
        return self.root / ".runtime"

    @property
    def log_dir(self) -> Path:
        return self.root / "log"

    def ensure(self, path: Path) -> Path:
        """mkdir -p the directory that will hold `path` (or `path` itself if a
        dir). Called lazily right before a write."""
        target = path if path.suffix == "" else path.parent
        target.mkdir(parents=True, exist_ok=True)
        return path

    # ── project registry persistence (used by DC-5 /projects API) ────────
    def load_projects(self) -> Dict[str, Any]:
        """Read projects.json → {version, projects: [{id, path, name, ...}]}.
        Returns an empty skeleton when absent."""
        with self._lock:
            f = self.projects_file
            if not f.exists():
                return {"version": 1, "projects": []}
            try:
                import json

                return json.loads(f.read_text(encoding="utf-8"))
            except (OSError, ValueError) as e:
                _log(f"GlobalLedger: projects.json unreadable ({e}); empty")
                return {"version": 1, "projects": []}

    def save_projects(self, projects: List[Dict[str, Any]]) -> None:
        with self._lock:
            self.ensure(self.projects_file)
            atomic_write_json(self.projects_file, {"version": 1, "projects": projects})

    # ── clients/providers config (multi-provider-agents, MPV1) ───────────
    # NON-secret machine-global config: which CLI clients are enabled and,
    # per provider, {enabled, base_url, small_fast_model}. Stored as JSON
    # (`clients-config.json`) — consistent with projects.json and needs no
    # YAML writer. The API KEYS never live here; they're chmod-600 files in
    # `credentials_dir` (see providersvc.ProviderKeyStore).
    @property
    def clients_config_file(self) -> Path:
        return self.root / "clients-config.json"

    def load_clients_config(self) -> Dict[str, Any]:
        """Read clients-config.json → {version, clients, providers}. Returns
        an empty skeleton when absent (callers merge over their defaults)."""
        with self._lock:
            f = self.clients_config_file
            if not f.exists():
                return {"version": 1, "clients": {}, "providers": {}}
            try:
                import json

                data = json.loads(f.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    return {"version": 1, "clients": {}, "providers": {}}
                data.setdefault("clients", {})
                data.setdefault("providers", {})
                return data
            except (OSError, ValueError) as e:
                _log(f"GlobalLedger: clients-config.json unreadable ({e}); empty")
                return {"version": 1, "clients": {}, "providers": {}}

    def save_clients_config(self, config: Dict[str, Any]) -> None:
        with self._lock:
            self.ensure(self.clients_config_file)
            payload = {
                "version": 1,
                "clients": config.get("clients") or {},
                "providers": config.get("providers") or {},
            }
            atomic_write_json(self.clients_config_file, payload)
