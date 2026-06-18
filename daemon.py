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

import re
import signal
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

# DM3 — sibling-module imports. paths.py and storage.py live next to
# daemon.py in source; the bundler concatenates them into dist/daemon.py
# in dependency order, stripping these import lines from the bundled
# output. Source-tree runs hit the sibling files via sys.path[0].
from anchor import AnchorMixin  # noqa: E402
from anchorprogress import AnchorProgressMixin  # noqa: E402
from chatread import ChatReadMixin  # noqa: E402
from fsread import FsReadMixin  # noqa: E402
from chatsvc import ChatMixin  # noqa: E402
from chatspawn import ChatSpawnMixin  # noqa: E402
from convmeta import ConvMetaMixin  # noqa: E402
from crud import CrudMixin  # noqa: E402
from coordination import CoordinationMixin  # noqa: E402
from coordwake import WakeMixin  # noqa: E402
from pausemgr import PauseMixin  # noqa: E402
from credapi import CredMixin  # noqa: E402
from readapi import QueryMixin  # noqa: E402
from lifecycle import LifecycleMixin  # noqa: E402
from selfupdatesvc import SelfUpdateMixin  # noqa: E402
from state import StateManager  # noqa: E402
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
from chat import ChatSessions  # noqa: E402
from cluster import Cluster  # noqa: E402
from constants import (  # noqa: E402,F401 — leaf consts; re-exported for callers/tests
    DAEMON_VERSION,
    FS_POLL_SEC,
    PORT_RANGE,
    _PORT_REGISTRY_DIR,
    _PORT_REGISTRY_FILE,
)
from cron import CronRunner  # noqa: E402,F401
from cronsched import CronScheduler  # noqa: E402,F401
from hub import Hub  # noqa: E402
from paths import Paths  # noqa: E402
from prompts import (  # noqa: E402,F401 — F401: re-exported for callers/tests
    AGENT_PROMPTS,
    BriefingPipeline,
    _agent_manifest,
    _agent_type_from_conv_slug,
    _agent_type_normalised,
)
from quota import QuotaState  # noqa: E402
from protocols import ProtocolsRegistry  # noqa: E402
from registries import (  # noqa: E402,F401 — F401: _split_frontmatter re-exported
    LinksRegistry,
    _split_frontmatter,
)
from render import AgentInstructionsRenderer  # noqa: E402
from runner import ChatRunner  # noqa: E402,F401 — re-exported as daemon.ChatRunner
from runnerutil import (  # noqa: E402,F401 — _session_id_for_conv re-exported for tests
    _session_id_for_conv,
)
from runrotator import TimelineRotator  # noqa: E402
from runs import RunStore  # noqa: E402
from bootupdate import _boot_self_update_if_needed  # noqa: E402,F401
from selfupdate import VersionWatcher  # noqa: E402,F401
from chatqueue import ChatQueueManager  # noqa: E402
from storage import ChatArchive, StorageReport  # noqa: E402
from uploads import UploadStore  # noqa: E402
from utils import (  # noqa: E402
    _iso_now,  # noqa: F401 — re-exported as daemon._iso_now for test_prompts
    _log,
    parse_frontmatter,  # noqa: F401 — re-exported for test_refactor_characterization
    parse_simple_yaml,  # noqa: F401 — re-exported for test_refactor_characterization
)

# ───────────────────────────────────────────────────────────────────────
# Configuration


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


class Daemon(
    AnchorMixin,
    AnchorProgressMixin,
    ChatMixin,
    ChatSpawnMixin,
    ConvMetaMixin,
    ChatReadMixin,
    CoordinationMixin,
    PauseMixin,
    WakeMixin,
    CredMixin,
    CrudMixin,
    FsReadMixin,
    LifecycleMixin,
    QueryMixin,
    SelfUpdateMixin,
):
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

    # ── py-1.5.0: daemon-side archive lifecycle ───────────────────────

    # ── py-1.2.0: self-update (standard v7 §10.4) ──────────────────────

    # ── U-DAEMON-09: message append + version stubs ────────────────────

    # ── U-DAEMON-04: task lifecycle ────────────────────────────────────

    # ── U-DAEMON-03 finish: declare a new agent identity ───────────────

    # ── HTTP body for /health and /info ────────────────────────────────

    # ── lifecycle ──────────────────────────────────────────────────────

    # py-1.12.16+: graceful-drain default. Configurable via
    # `cluster.yaml.daemon.shutdown_grace_secs` (int, 0 = no drain).
    DEFAULT_SHUTDOWN_GRACE_SECS = 30

    # ── runtime files ─────────────────────────────────────────────────


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
