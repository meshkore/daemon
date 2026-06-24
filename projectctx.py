#!/usr/bin/env python3
"""
projectctx.py — ProjectContext: all PER-PROJECT (per-cluster) state.

DC-1 (initiative `daemon-centralized`). This is the seam for the move from
"one daemon per project" to "one daemon per machine, many projects". It
holds everything rooted in ONE project's `.meshkore/` folder.

Boundary:
- GLOBAL (stays on `Daemon`, NOT here): hub, identity, token, port, the
  HTTP server, VersionWatcher, daemon_version, the stop/serve runtime.
- PER-PROJECT (here): paths, cluster, state_manager, chat_sessions,
  chat_queue_manager, upload_store, storage_report, quota, runs,
  chat_archive, timeline_rotator, links_registry, workflows_registry
  (+ protocols_registry alias), instructions_renderer, cron_scheduler.

DC-1 keeps exactly ONE context on the Daemon and aliases its attributes
(`self.paths = ctx.paths`, …) so every mixin keeps reading `self.<attr>`
unchanged — zero behaviour change, parity-protected. DC-2 turns this into
a registry keyed by project_id; DC-4 switches handlers to `ctx.<attr>` and
drops the aliases.

Depends on GLOBAL services passed in by the Daemon: `hub`, `identity`, and
a `daemon` backref (only for `state_manager.bind_daemon`).
"""

from __future__ import annotations

from typing import Any

from cluster import Cluster
from hub import ProjectHub
from state import StateManager
from chat import ChatSessions
from chatqueue import ChatQueueManager
from uploads import UploadStore
from storage import ChatArchive, StorageReport
from quota import QuotaState
from runs import RunStore
from runrotator import TimelineRotator
from registries import LinksRegistry
from workflows import WorkflowsRegistry
from render import AgentInstructionsRenderer
from cronsched import CronScheduler
from bootstrap import _migrate_cluster_daemon_block
from paths import Paths
from utils import _log


class ProjectContext:
    """All state bound to ONE project's `.meshkore/` tree.

    Construction order mirrors the legacy `Daemon.__init__` exactly (cluster
    + daemon-block migration first, then the stores that depend on it), so
    the golden-master / parity tests stay byte-identical.
    """

    def __init__(self, paths: Paths, *, hub: Any, identity: str, daemon: Any) -> None:
        self.paths = paths
        self.cluster = Cluster(paths)
        # Standard v7 migration: ensure a `daemon:` block in cluster.yaml.
        # Idempotent; re-parse so cluster.data reflects the write.
        try:
            _migrate_cluster_daemon_block(paths)
            self.cluster.reload()
        except Exception as e:  # noqa: BLE001 — migration is best-effort
            _log(f"daemon-block migration skipped: {e}")

        # DC-6 — every per-project component broadcasts through this proxy, so
        # each event it emits is tagged with this project's id. The cockpit's
        # single WS connection then carries (and routes) events for all
        # projects. project_id = cluster.id (stable, portable).
        self.project_hub = ProjectHub(hub, self.cluster.id)
        phub = self.project_hub

        self.state_manager = StateManager(paths, self.cluster, phub)
        # Backref for future cross-system reads (bound after both exist).
        self.state_manager.bind_daemon(daemon)

        self.chat_sessions = ChatSessions()
        self.chat_queue_manager = ChatQueueManager(paths, phub)
        self.upload_store = UploadStore(paths, self.cluster)
        self.storage_report = StorageReport(paths, self.cluster)
        self.quota = QuotaState(paths.runtime / "quota-state.json")
        self.runs = RunStore(paths, phub)
        self.chat_archive = ChatArchive(paths)

        # Archive retention: cluster.yaml `storage.retention_days` (0/absent
        # = keep forever).
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

        self.links_registry = LinksRegistry(paths, phub)
        self.workflows_registry = WorkflowsRegistry(paths, phub)
        # Back-compat alias for callers still using the pre-2026-06-21 name.
        self.protocols_registry = self.workflows_registry
        self.instructions_renderer = AgentInstructionsRenderer(paths, phub)
        self.cron_scheduler = CronScheduler(paths, self.cluster, phub, identity)
