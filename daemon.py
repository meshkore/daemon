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

To scaffold a brand-new cluster authoritatively (folder layout +
cluster.yaml + AGENT_INSTRUCTIONS + per-CLI files + starter task +
.gitignore, all matching the standard version this daemon targets), run
once before the first launch:

    python3 .meshkore/scripts/daemon.py init --name "My Project" [--desc "…"] [--id slug] [--force]

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
from walls import WallsMixin  # noqa: E402
from lifecycle import LifecycleMixin  # noqa: E402
from selfupdatesvc import SelfUpdateMixin  # noqa: E402
from verifysvc import VerifyMixin  # noqa: E402
from projectctx import ProjectContext  # noqa: E402 — DC-1: per-project state
from registry import ProjectRegistry  # noqa: E402 — DC-2: project registry
from globalledger import GlobalLedger  # noqa: E402 — DC-3: machine-global ledger
from projectsapi import ProjectsMixin  # noqa: E402 — DC-5: /projects API
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
from registries import (  # noqa: E402,F401 — F401: _split_frontmatter re-exported
    _split_frontmatter,
)
from runner import ChatRunner  # noqa: E402,F401 — re-exported as daemon.ChatRunner
from runnerutil import (  # noqa: E402,F401 — _session_id_for_conv re-exported for tests
    _session_id_for_conv,
)
from scaffold import ScaffoldError, scaffold_cluster  # noqa: E402
from bootupdate import _boot_self_update_if_needed  # noqa: E402,F401
from selfupdate import VersionWatcher  # noqa: E402,F401
from utils import (  # noqa: E402
    _iso_now,  # noqa: F401 — re-exported as daemon._iso_now for test_prompts
    _log,
    parse_frontmatter,  # noqa: F401 — re-exported for test_refactor_characterization
    parse_simple_yaml,  # noqa: F401 — re-exported for test_refactor_characterization
)

# DC-1 — these per-project classes are now CONSTRUCTED in projectctx.py, but
# daemon.py keeps re-exporting them so `daemon.Cluster`, `daemon.RunStore`, …
# stay valid attributes. Tests (`import daemon as d` → `d.Cluster`) and the
# "re-exports keep call sites stable" rule (ARCHITECTURE.md) depend on this.
from cluster import Cluster  # noqa: E402,F401 — re-exported (built in projectctx)
from chat import ChatSessions  # noqa: E402,F401 — re-exported (built in projectctx)
from state import StateManager  # noqa: E402,F401 — re-exported (built in projectctx)
from quota import QuotaState  # noqa: E402,F401 — re-exported (built in projectctx)
from runs import RunStore  # noqa: E402,F401 — re-exported (built in projectctx)
from runrotator import TimelineRotator  # noqa: E402,F401 — re-exported (projectctx)
from chatqueue import ChatQueueManager  # noqa: E402,F401 — re-exported (projectctx)
from storage import (  # noqa: E402,F401 — re-exported (built in projectctx)
    ChatArchive,
    StorageReport,
)
from uploads import UploadStore  # noqa: E402,F401 — re-exported (built in projectctx)
from workflows import WorkflowsRegistry  # noqa: E402,F401 — re-exported (projectctx)
from render import (  # noqa: E402,F401 — re-exported (built in projectctx)
    AgentInstructionsRenderer,
)
from registries import LinksRegistry  # noqa: E402,F401 — re-exported (projectctx)

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
    VerifyMixin,
    WallsMixin,
    ProjectsMixin,
):
    def __init__(
        self, paths: Paths, identity: Optional[str], requested_port: Optional[int]
    ):
        # DM6 step 2 — instance-bound version so routes.py (and any other
        # extracted module) reads from `daemon.daemon_version` instead of
        # the module-level DAEMON_VERSION (which in source-tree dev only
        # exists in daemon.py's namespace, not the sibling module's).
        self.daemon_version = DAEMON_VERSION
        # DC-4 — per-request project selector. The HTTP dispatcher sets this
        # from the `X-MeshKore-Project` header (per worker thread); the
        # per-project @property accessors read it to resolve the right
        # ProjectContext. Unset → the default/boot project (background threads,
        # tests, single-project clusters).
        self._req_local = threading.local()

        # ── GLOBAL services (one per machine; NOT per-project) ──────────
        self.hub = Hub()
        self.identity = identity or _detect_identity(paths) or _hostname_default()
        self.token = _ensure_token(paths)
        # DC-3 — machine-global ledger (ideas, projects.json, external creds /
        # agents). Lazy: resolves its root (~/.meshkore by default) but touches
        # no disk until a write. NOT per-project.
        self.global_ledger = GlobalLedger()

        # ── PER-PROJECT state (DC-1/DC-2, initiative `daemon-centralized`) ─
        # Per-cluster stores live in ProjectContext; the Daemon keeps a
        # ProjectRegistry of them keyed by project_id. Today exactly ONE
        # project is registered (the boot cluster) and `self._ctx` points at
        # it, so behaviour is identical — but the holder is multi-project
        # capable (DC-5 registers more; DC-4 resolves per request). The
        # aliases below keep every mixin reading `self.<attr>` unchanged.
        self._registry = ProjectRegistry(
            hub=self.hub, identity=self.identity, daemon=self
        )
        boot_ctx = ProjectContext(
            paths, hub=self.hub, identity=self.identity, daemon=self
        )
        # Key the boot project by its cluster id (its stable project_id).
        self._registry.add_built(boot_ctx.cluster.id, boot_ctx, default=True)
        self._ctx = boot_ctx
        # DC-5 — lazily register any ADDITIONAL projects from the global
        # projects.json (read-only; writes nothing on boot).
        self.rehydrate_projects()

        # Port depends on the (migrated + reloaded) cluster inside the ctx.
        self.port = _pick_port(
            paths,
            cluster_id=self._ctx.cluster.id,
            cli_override=requested_port,
            yaml_port=self._ctx.cluster.architect_port,
        )

        # DC-4 — the per-project attributes (paths, cluster, runs, chat_sessions,
        # …) are now @property on the class; each resolves the ProjectContext
        # for the CURRENT request (set from the X-MeshKore-Project header), or
        # the default/boot project when unset. No alias assignments here — see
        # the property block below __init__.

        # ── runtime (global) ────────────────────────────────────────────
        self.stopping = threading.Event()
        self.server: Optional[ThreadingHTTPServer] = None
        # D-TLS-01 — set by serve_forever once it knows whether the
        # bundle loaded. /health reports this; cockpit decides URL scheme.
        self.tls_enabled: bool = False

    # ── DC-4: per-request project resolution ───────────────────────────
    # The HTTP dispatcher calls _set_req_project() from the
    # `X-MeshKore-Project` header before handling, and _clear_req_project()
    # after. The per-project @property accessors below read the current
    # context via _resolve_ctx(); unset / unknown id falls back to the
    # default (boot) project, so single-project clusters, background threads
    # and the existing test-suite behave exactly as before.
    def _set_req_project(self, project_id: Optional[str]) -> None:
        self._req_local.project_id = project_id

    def _clear_req_project(self) -> None:
        self._req_local.project_id = None

    def _resolve_ctx(self) -> ProjectContext:
        pid = getattr(self._req_local, "project_id", None)
        ctx = self._registry.get(pid)
        return ctx if ctx is not None else self._ctx

    @property
    def paths(self):
        return self._resolve_ctx().paths

    @property
    def cluster(self):
        return self._resolve_ctx().cluster

    @property
    def state_manager(self):
        return self._resolve_ctx().state_manager

    @property
    def chat_sessions(self):
        return self._resolve_ctx().chat_sessions

    @property
    def chat_queue_manager(self):
        return self._resolve_ctx().chat_queue_manager

    @property
    def upload_store(self):
        return self._resolve_ctx().upload_store

    @property
    def storage_report(self):
        return self._resolve_ctx().storage_report

    @property
    def quota(self):
        return self._resolve_ctx().quota

    @property
    def runs(self):
        return self._resolve_ctx().runs

    @property
    def chat_archive(self):
        return self._resolve_ctx().chat_archive

    @property
    def timeline_rotator(self):
        return self._resolve_ctx().timeline_rotator

    @property
    def links_registry(self):
        return self._resolve_ctx().links_registry

    @property
    def workflows_registry(self):
        return self._resolve_ctx().workflows_registry

    @property
    def protocols_registry(self):
        return self._resolve_ctx().protocols_registry

    @property
    def instructions_renderer(self):
        return self._resolve_ctx().instructions_renderer

    @property
    def cron_scheduler(self):
        return self._resolve_ctx().cron_scheduler

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
    out: Dict[str, Any] = {
        "cmd": "serve",
        "identity": None,
        "port": None,
        "root": None,
        "name": None,
        "id": None,
        "desc": None,
        "force": False,
    }
    # Leading `init` subcommand — authoritative first-boot scaffolder.
    if argv and argv[0] == "init":
        out["cmd"] = "init"
        argv = argv[1:]
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
        # `init`-only flags
        if a == "--name":
            out["name"] = argv[i + 1]
            i += 2
            continue
        if a == "--id":
            out["id"] = argv[i + 1]
            i += 2
            continue
        if a == "--desc":
            out["desc"] = argv[i + 1]
            i += 2
            continue
        if a == "--force":
            out["force"] = True
            i += 1
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

    # `init` — authoritative first-boot scaffolder. Runs BEFORE the
    # `.meshkore/` existence guard (the whole point is the tree may not
    # exist yet) and exits without starting the listener. The operator's
    # launch command boots the daemon normally afterwards.
    if args["cmd"] == "init":
        name = args["name"] or args["root"].resolve().name
        try:
            paths = scaffold_cluster(
                args["root"],
                name,
                description=args["desc"],
                cluster_id=args["id"],
                force=args["force"],
            )
        except ScaffoldError as e:
            # Already scaffolded + no --force → GRACEFUL no-op (exit 0), so the
            # operator's "init … ; launch" one-liner is safe to paste twice and
            # a chained `&&` launch isn't broken by a non-zero exit. This is
            # what kills the old "do you want --force?" question.
            print(f"\n✓ Cluster already scaffolded — skipping init ({e}).\n")
            raise SystemExit(0)
        print(
            f"\n✓ MeshKore cluster scaffolded at {paths.meshkore}"
            f"\n  Launch the daemon from the repo root:"
            f"\n    python3 .meshkore/scripts/daemon.py\n"
        )
        raise SystemExit(0)

    paths = Paths(args["root"])
    # Auto-scaffold on first boot (py-1.27.2). A project where the coding
    # agent DOWNLOADED the daemon (`.meshkore/scripts/`) but never ran `init`
    # has a `.meshkore/` dir yet no `cluster.yaml`. Rather than error, finish
    # the scaffold here so the operator's SINGLE launch command bootstraps
    # everything — the agent only ever runs `curl` (no executing downloaded
    # code → no agent safety prompt). The display name defaults to the folder
    # name; the operator can refine it in cluster.yaml. If `.meshkore/` does
    # not exist AT ALL, we're being run from the wrong directory → refuse
    # (don't scatter a cluster into a random folder).
    if not paths.cluster_yaml.exists():
        if paths.meshkore.exists():
            _log("first boot: cluster.yaml absent — auto-scaffolding cluster")
            try:
                scaffold_cluster(args["root"], args["root"].resolve().name)
            except ScaffoldError:
                pass  # raced with another writer; cluster.yaml now present
        else:
            raise SystemExit(
                f"\n .meshkore/ not found at {paths.meshkore}."
                "\n   Run this script from a repo that already has a .meshkore/ tree"
                "\n   (or where the daemon was downloaded to .meshkore/scripts/),"
                "\n   or pass --root <path>. See https://meshkore.com/standard.\n"
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
