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

import os
import re
import signal
import sys
import threading
import faulthandler
import time
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# DM3 — sibling-module imports. paths.py and storage.py live next to
# daemon.py in source; the bundler concatenates them into dist/daemon.py
# in dependency order, stripping these import lines from the bundled
# output. Source-tree runs hit the sibling files via sys.path[0].
from anchor import AnchorMixin  # noqa: E402
from chatsvc import ChatMixin  # noqa: E402
from crud import CrudMixin  # noqa: E402
from coordination import CoordinationMixin  # noqa: E402
from readapi import QueryMixin  # noqa: E402
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
from chat import ChatSessionReaper, ChatSessions  # noqa: E402
from cluster import Cluster  # noqa: E402
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
    _debug_emit,
    _debug_enabled,
    _find_tls_bundle,
    _iso_now,
    _log,
    parse_frontmatter,  # noqa: F401 — re-exported for test_refactor_characterization
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


class Daemon(AnchorMixin, ChatMixin, CoordinationMixin, CrudMixin, QueryMixin):
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

    # ── U-DAEMON-04: task lifecycle ────────────────────────────────────

    # ── U-DAEMON-03 finish: declare a new agent identity ───────────────

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
