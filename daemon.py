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

import base64
import hashlib
import json
import os
import re
import secrets
import signal
import socket
import ssl
import struct
import sys
import threading
import time
import urllib.parse
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ───────────────────────────────────────────────────────────────────────
# Configuration

PORT_RANGE = (5570, 5589)
HEARTBEAT_SEC = 20.0
FS_POLL_SEC = 1.5
DAEMON_VERSION = "py-1.10.8"  # 1.10.8 architect: decision-catalog + stub-feature-flag + consult-A001 + 4-bucket end-of-pass (no voluntary halt)

# ── TLS bundle (D-TLS-01) ─────────────────────────────────────────────
# Wildcard cert for *.daemon.meshkore.com (public CF A record → 127.0.0.1)
# so the cockpit at architect.meshkore.com can talk to localhost over
# HTTPS+WSS without mixed-content / Chrome Local Network Access Issues.
# Bundled cert + key are intentionally "public" (only useful for
# impersonating daemon.meshkore.com on the attacker's own loopback,
# a no-op). The daemon falls back to plain HTTP if the bundle is
# missing — backwards-compatible with operators who haven't pulled
# the tls/ directory.
TLS_BUNDLE_NAME = "tls"
TLS_CERT_FILENAME = "fullchain.pem"
TLS_KEY_FILENAME = "privkey.pem"
# Max number of timeline events to surface in /state.timeline.recent_events.
# The architect needs these to rebuild chat history + task lifecycle on
# every reload — without them, conv history vanishes from the cockpit
# even though the JSONL files on disk are intact. Bound to keep state.json
# small enough to serve cheaply; everything older is still readable from
# the per-day JSONL files in .meshkore/timeline/.
TIMELINE_RECENT_LIMIT = 500
MAX_BODY_BYTES = 4 * 1024 * 1024  # 4 MB — protect against runaway POSTs
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# ───────────────────────────────────────────────────────────────────────
# Paths


class Paths:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.meshkore = self.root / ".meshkore"
        self.public = self.meshkore / "public"
        self.cluster_yaml = self.public / "cluster.yaml"
        # Standard §13 — deployment links registry. Optional file; the
        # registry treats a missing file as { version: 1, modules: [] }.
        self.links_yaml = self.public / "links.yaml"
        # Standard §14 — protocols (reusable multi-scope runbooks).
        self.protocols_dir = self.meshkore / "protocols"
        self.protocols_log = self.protocols_dir / "log"
        self.credentials = self.meshkore / "credentials"
        self.token_file = self.credentials / "portal-token"
        self.runtime = self.meshkore / ".runtime"
        self.pid_file = self.runtime / "daemon.pid"
        self.port_file = self.runtime / "port"
        self.timeline_dir = self.meshkore / "timeline"
        self.modules_dir = self.meshkore / "modules"
        self.docs_dir = self.meshkore / "docs"
        # py-1.9.0 — daily narrative logs (operator/Claude prose, one
        # file per day). Served read-only over /log/<YYYY-MM-DD>.md +
        # listed under /log so the cockpit Diary tab can lazy-page.
        self.log_dir = self.meshkore / "log"
        # py-1.2.0 — where /self-update writes daemon.py + daemon.py.bak.
        self.scripts_dir = self.meshkore / "scripts"
        self.roadmap_dir = self.meshkore / "roadmap"
        self.state_json = self.roadmap_dir / "state.json"
        self.agents_dir = self.meshkore / "agents"
        self.initiatives = self.roadmap_dir / "initiatives"
        # Cron scheduler (D-CRON-01..05). State file is gitignored
        # under .meshkore/.runtime/; the logs dir holds per-run captures.
        self.crons_state_path = self.runtime / "crons.json"
        self.crons_logs_dir = self.runtime / "logs" / "cron"


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
_CRON_RESTART_POLICIES = frozenset({"never", "on-failure", "always"})

# Defaults applied when a `crons:` entry omits the field.
_CRON_DEFAULTS = {
    "enabled": True,
    "max_runtime_sec": 7200,  # 2h
    "restart_policy": "never",
    "retention_runs": 30,
    "destructive": False,
}


def _validate_cron_expr(expr: str) -> Optional[str]:
    """Lightweight validation. Full parsing lands in D-CRON-02. Here we
    only need to reject obviously malformed values at config load so the
    daemon doesn't carry junk into the scheduler later.

    Returns None on OK, or a short error message string on reject.
    Accepts 5 space-separated fields. Each field is non-empty and
    consists of characters from [0-9*/,\\-]. Quartz (6 fields with
    seconds), `@daily`-style aliases, and the `L/W/#` modifiers are
    explicitly NOT supported in v1.
    """
    if not isinstance(expr, str) or not expr.strip():
        return "schedule must be a non-empty string"
    fields = expr.strip().split()
    if len(fields) != 5:
        return f"schedule must have 5 space-separated fields, got {len(fields)}"
    allowed = set("0123456789*/,-")
    for i, f in enumerate(fields):
        if not f:
            return f"schedule field {i} is empty"
        if not set(f).issubset(allowed):
            return f"schedule field {i} ({f!r}) contains unsupported characters"
    return None


def _validate_crons_block(
    data: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Validates the `crons:` section of cluster.yaml in isolation.
    Returns (cleaned_jobs, errors). Bad entries are skipped (not raised)
    so a single broken job doesn't disable the entire scheduler.

    Each returned job has defaults filled in and the schema's shape
    enforced. Invariants:
      - id is a non-empty kebab-case string, unique within the list
      - cmd is non-empty string
      - schedule passes _validate_cron_expr
      - restart_policy is in _CRON_RESTART_POLICIES
      - env values are strings
    """
    raw = data.get("crons") or []
    if not isinstance(raw, list):
        return [], ["crons: must be a list"]
    out: List[Dict[str, Any]] = []
    errors: List[str] = []
    seen_ids: set = set()
    for idx, entry in enumerate(raw):
        if not isinstance(entry, dict):
            errors.append(f"crons[{idx}] is not a dict — skipped")
            continue
        cid = entry.get("id")
        if not isinstance(cid, str) or not cid.strip():
            errors.append(f"crons[{idx}] missing id — skipped")
            continue
        cid = cid.strip()
        if cid in seen_ids:
            errors.append(f"crons[{idx}] duplicate id {cid!r} — skipped")
            continue
        cmd = entry.get("cmd")
        if not isinstance(cmd, str) or not cmd.strip():
            errors.append(f"crons[{cid}] missing cmd — skipped")
            continue
        sched = entry.get("schedule")
        sched_err = (
            _validate_cron_expr(sched) if isinstance(sched, str) else "schedule missing"
        )
        if sched_err:
            errors.append(f"crons[{cid}] {sched_err} — skipped")
            continue
        policy = entry.get("restart_policy", _CRON_DEFAULTS["restart_policy"])
        if policy not in _CRON_RESTART_POLICIES:
            errors.append(
                f"crons[{cid}] restart_policy={policy!r} not in "
                f"{sorted(_CRON_RESTART_POLICIES)} — defaulting to 'never'"
            )
            policy = "never"
        env = entry.get("env") or {}
        if not isinstance(env, dict):
            errors.append(f"crons[{cid}] env must be a dict — replaced with empty")
            env = {}
        env_clean: Dict[str, str] = {}
        for k, v in env.items():
            if not isinstance(k, str) or not isinstance(v, str):
                errors.append(
                    f"crons[{cid}] env {k!r}: values must be strings — dropped"
                )
                continue
            env_clean[k] = v

        cleaned = {
            "id": cid,
            "name": str(entry.get("name") or cid),
            "schedule": sched.strip(),
            "cmd": cmd.strip(),
            "cwd": entry.get("cwd"),
            "env": env_clean,
            "enabled": bool(entry.get("enabled", _CRON_DEFAULTS["enabled"])),
            "max_runtime_sec": int(
                entry.get("max_runtime_sec", _CRON_DEFAULTS["max_runtime_sec"])
            ),
            "restart_policy": policy,
            "retention_runs": int(
                entry.get("retention_runs", _CRON_DEFAULTS["retention_runs"])
            ),
            "destructive": bool(
                entry.get("destructive", _CRON_DEFAULTS["destructive"])
            ),
        }
        out.append(cleaned)
        seen_ids.add(cid)
    return out, errors


# ───────────────────────────────────────────────────────────────────────
# Tiny YAML reader (stdlib has no yaml module — we only need flat scalars)


def parse_simple_yaml(text: str) -> Dict[str, Any]:
    """Parses a YAML subset sufficient for our cluster.yaml + frontmatter
    blocks. Supports scalars, dicts, lists, list-of-dicts, and inline
    list scalars (`tags: [a, b]`). NOT a general YAML parser — fail
    loudly for shapes we don't handle."""
    out: Dict[str, Any] = {}
    # Stack entry: (indent, container, key_in_parent, parent_ref_or_None)
    stack: List[Tuple[int, Any, str, Any]] = [(-1, out, "", None)]
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        raw = lines[i]
        line = raw.rstrip()
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            i += 1
            continue
        indent = len(line) - len(stripped)
        while stack and indent <= stack[-1][0] and len(stack) > 1:
            stack.pop()
        parent = stack[-1][1]

        if stripped.startswith("- "):
            value = stripped[2:].strip()
            # Promote: if the current container is an empty dict that was
            # just created as a nested holder for some key, convert it to
            # a list in the grandparent — we now know the value is a list.
            if isinstance(parent, dict) and not parent:
                key = stack[-1][2]
                gp = stack[-1][3]
                if key and isinstance(gp, dict) and gp.get(key) is parent:
                    new_list: List[Any] = []
                    gp[key] = new_list
                    stack[-1] = (stack[-1][0], new_list, key, gp)
                    parent = new_list
            if isinstance(parent, list):
                # Two shapes:
                #   "- value"               → scalar item
                #   "- key: val\n  key2: …" → dict item (continues below)
                if ":" in value:
                    item: Dict[str, Any] = {}
                    parent.append(item)
                    # Treat the inline "key: val" as the first dict entry
                    k2, _, v2 = value.partition(":")
                    k2 = k2.strip()
                    v2 = v2.strip()
                    if v2:
                        item[k2] = _coerce(_strip_inline_comment(v2))
                        stack.append((indent, item, "", parent))
                    else:
                        # Nested key with no value yet
                        nested: Dict[str, Any] = {}
                        item[k2] = nested
                        stack.append((indent, item, "", parent))
                        stack.append((indent + 2, nested, k2, item))
                else:
                    parent.append(
                        _coerce(_strip_inline_comment(value)) if value else None
                    )

        elif ":" in stripped:
            key, _, val = stripped.partition(":")
            key = key.strip()
            val = _strip_inline_comment(val.strip())
            if val == "":
                nxt: Dict[str, Any] = {}
                if isinstance(parent, dict):
                    parent[key] = nxt
                stack.append((indent, nxt, key, parent))
            elif val.startswith("[") and val.endswith("]"):
                # Inline list scalar: [a, b, "c d"]
                inner = val[1:-1].strip()
                items = (
                    [_coerce(x.strip()) for x in _split_top_level_commas(inner)]
                    if inner
                    else []
                )
                if isinstance(parent, dict):
                    parent[key] = items
            else:
                if isinstance(parent, dict):
                    parent[key] = _coerce(val)
        i += 1
    return out


def _strip_inline_comment(v: str) -> str:
    return re.sub(r"\s+#.*$", "", v)


def _split_top_level_commas(s: str) -> List[str]:
    out, buf, depth, in_str = [], "", 0, None
    for ch in s:
        if in_str:
            buf += ch
            if ch == in_str:
                in_str = None
            continue
        if ch in ('"', "'"):
            in_str = ch
            buf += ch
            continue
        if ch == "," and depth == 0:
            out.append(buf)
            buf = ""
            continue
        if ch in "[{":
            depth += 1
        elif ch in "]}":
            depth -= 1
        buf += ch
    if buf.strip():
        out.append(buf)
    return out


def _coerce(v: str) -> Any:
    s = v.strip()
    if not s:
        return ""
    if (s.startswith('"') and s.endswith('"')) or (
        s.startswith("'") and s.endswith("'")
    ):
        return s[1:-1]
    if s.lower() in ("true", "yes"):
        return True
    if s.lower() in ("false", "no"):
        return False
    if s.lower() in ("null", "~"):
        return None
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    return s


# ───────────────────────────────────────────────────────────────────────
# Frontmatter parser


_FM_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


def parse_frontmatter(text: str) -> Dict[str, Any]:
    m = _FM_RE.match(text)
    if not m:
        return {}
    return parse_simple_yaml(m.group(1))


# ───────────────────────────────────────────────────────────────────────
# Cluster + state


class Cluster:
    def __init__(self, paths: Paths):
        self.paths = paths
        self.data: Dict[str, Any] = {}
        # Cron scheduler (D-CRON-01): validated job set + ownership.
        # Populated by reload(); empty + None until a `crons:` block
        # appears in cluster.yaml.
        self.crons: List[Dict[str, Any]] = []
        self.crons_owner: Optional[str] = None
        self.reload()

    def reload(self) -> None:
        if not self.paths.cluster_yaml.exists():
            raise SystemExit(
                f"\n .meshkore/public/cluster.yaml not found at {self.paths.cluster_yaml}."
                "\n   Run `meshcore init` (or hand-author cluster.yaml from"
                "\n   https://meshkore.com/reference/cluster/templates/) and re-run.\n"
            )
        self.data = parse_simple_yaml(self.paths.cluster_yaml.read_text())
        # Validate the cron block last so a bad config logs warnings but
        # never blocks the daemon's other features.
        self.crons, errs = _validate_crons_block(self.data)
        for e in errs:
            _log(f"cluster.yaml crons: {e}")
        owner = self.data.get("crons_owner")
        self.crons_owner = (
            owner.strip() if isinstance(owner, str) and owner.strip() else None
        )
        if self.crons and not self.crons_owner:
            _log(
                "cluster.yaml has crons: but no crons_owner — scheduler will tick but never fire"
            )

    @property
    def id(self) -> str:
        return str(self.data.get("id") or "unknown")

    @property
    def name(self) -> str:
        return str(self.data.get("name") or self.id)

    @property
    def type(self) -> str:
        return str(self.data.get("type") or "dev")

    @property
    def architect_port(self) -> Optional[int]:
        # cluster.yaml.architect.port (preferred) → fall back to legacy portal.port
        for key in ("architect", "portal"):
            sec = self.data.get(key)
            if isinstance(sec, dict) and "port" in sec:
                try:
                    return int(sec["port"])
                except (TypeError, ValueError):
                    pass
        return None

    @property
    def modules(self) -> List[Dict[str, Any]]:
        m = self.data.get("modules") or []
        return m if isinstance(m, list) else []


# ───────────────────────────────────────────────────────────────────────
# Links registry — standard §13
#
# .meshkore/public/links.yaml maps each module to where it runs locally,
# where it is deployed in production, and what version is live. The
# daemon parses it on boot and on file change, validates entries, serves
# them at GET /links + /links/<id>, accepts patches at POST /links/<id>,
# and broadcasts `links.updated` on the WebSocket.

_LINKS_PROVIDERS = frozenset(
    {
        "fly",
        "cloudflare-pages",
        "cloudflare-workers",
        "vercel",
        "render",
        "self-hosted",
        "other",
    }
)
_LINKS_BLOCKS = ("local", "prod", "repo")


def _validate_links_block(
    data: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    errs: List[str] = []
    if not isinstance(data, dict):
        return [], ["links.yaml: top level must be a mapping"]
    mods = data.get("modules") or []
    if not isinstance(mods, list):
        return [], ["links.yaml: `modules:` must be a list"]
    out: List[Dict[str, Any]] = []
    for i, m in enumerate(mods):
        if not isinstance(m, dict):
            errs.append(f"links.yaml: modules[{i}] is not a mapping")
            continue
        mid = m.get("id")
        if not isinstance(mid, str) or not mid.strip():
            errs.append(f"links.yaml: modules[{i}] missing string `id`")
            continue
        entry: Dict[str, Any] = {"id": mid.strip()}
        for blk in _LINKS_BLOCKS:
            v = m.get(blk)
            if v is None:
                continue
            if not isinstance(v, dict):
                errs.append(f"links.yaml: {mid}.{blk} must be a mapping")
                continue
            entry[blk] = v
        prov = (m.get("prod") or {}).get("provider")
        if isinstance(prov, str) and prov.strip() and prov not in _LINKS_PROVIDERS:
            errs.append(
                f"links.yaml: {mid}.prod.provider `{prov}` is not in the canonical set (rendered as plain text)"
            )
        if "notes" in m:
            entry["notes"] = m["notes"]
        out.append(entry)
    return out, errs


class LinksRegistry:
    """Loads + watches .meshkore/public/links.yaml; broadcasts on change."""

    POLL_SEC = 3.0

    def __init__(self, paths: Paths, hub: "Hub"):
        self.paths = paths
        self.hub = hub
        self.modules: List[Dict[str, Any]] = []
        self.errors: List[str] = []
        self._mtime: Optional[float] = None
        self._stop = threading.Event()
        self.reload(broadcast=False)
        threading.Thread(target=self._watch_loop, daemon=True).start()

    def _watch_loop(self) -> None:
        while not self._stop.wait(self.POLL_SEC):
            try:
                self.reload(broadcast=True)
            except Exception:
                pass

    def shutdown(self) -> None:
        self._stop.set()

    def reload(self, broadcast: bool = True) -> bool:
        """Reread the file. Returns True if content changed."""
        if not self.paths.links_yaml.exists():
            changed = bool(self.modules) or self._mtime is not None
            self.modules, self.errors, self._mtime = [], [], None
            if changed and broadcast:
                self.hub.broadcast({"type": "links.updated", "modules": []})
            return changed
        try:
            mt = self.paths.links_yaml.stat().st_mtime
        except OSError:
            mt = None
        if mt is not None and mt == self._mtime:
            return False
        try:
            text = self.paths.links_yaml.read_text()
            data = parse_simple_yaml(text)
            mods, errs = _validate_links_block(data)
        except Exception as exc:
            _log(f"links.yaml: parse error — {exc}")
            return False
        self.modules, self.errors, self._mtime = mods, errs, mt
        for e in errs:
            _log(f"links.yaml: {e}")
        if broadcast:
            self.hub.broadcast(
                {"type": "links.updated", "modules": [m["id"] for m in self.modules]}
            )
        return True

    def as_dict(self) -> Dict[str, Any]:
        return {
            "version": 1,
            "modules": self.modules,
            "_errors": self.errors,
        }

    def get(self, mid: str) -> Optional[Dict[str, Any]]:
        for m in self.modules:
            if m["id"] == mid:
                return m
        return None

    def patch(self, mid: str, patch_body: Dict[str, Any]) -> Tuple[bool, str]:
        """Merge `patch_body` (allowed keys: local/prod/repo/notes) into the
        module entry, write the file atomically, broadcast."""
        if not isinstance(patch_body, dict):
            return False, "patch body must be an object"
        allowed = {"local", "prod", "repo", "notes"}
        unknown = set(patch_body) - allowed
        if unknown:
            return False, f"patch keys not allowed: {sorted(unknown)}"
        # Make sure the file exists with a baseline
        if not self.paths.links_yaml.exists():
            self.paths.links_yaml.parent.mkdir(parents=True, exist_ok=True)
            self.paths.links_yaml.write_text("version: 1\nmodules: []\n")
        # Reread → merge → write. We round-trip through parse_simple_yaml +
        # a minimal serializer below so we don't depend on PyYAML.
        try:
            data = parse_simple_yaml(self.paths.links_yaml.read_text())
        except Exception as exc:
            return False, f"current links.yaml unparseable: {exc}"
        mods = data.get("modules")
        if not isinstance(mods, list):
            mods = []
            data["modules"] = mods
        found = None
        for m in mods:
            if isinstance(m, dict) and m.get("id") == mid:
                found = m
                break
        if found is None:
            found = {"id": mid}
            mods.append(found)
        for k, v in patch_body.items():
            if k == "notes":
                found["notes"] = v
            else:
                base = found.get(k) if isinstance(found.get(k), dict) else {}
                if isinstance(v, dict):
                    base.update(v)
                    found[k] = base
                else:
                    found[k] = v
        data["version"] = 1
        tmp = self.paths.links_yaml.with_suffix(".yaml.tmp")
        tmp.write_text(_emit_links_yaml(data))
        os.replace(tmp, self.paths.links_yaml)
        self.reload(broadcast=True)
        return True, "ok"


def _emit_links_yaml(data: Dict[str, Any]) -> str:
    """Tiny serializer for the links.yaml subset we care about. Stdlib-only.
    Preserves block-mapping layout the file template ships with."""
    out: List[str] = [
        "# .meshkore/public/links.yaml — Deployment links registry (standard §13).",
        "# Agent-maintained. Do not check in secrets — URLs only.",
        "",
        f"version: {int(data.get('version') or 1)}",
        "",
        "modules:",
    ]
    mods = data.get("modules") or []
    if not mods:
        out[-1] = "modules: []"
        return "\n".join(out) + "\n"
    for m in mods:
        if not isinstance(m, dict):
            continue
        mid = m.get("id") or "unknown"
        out.append(f"  - id: {mid}")
        for blk in ("local", "prod", "repo"):
            v = m.get(blk)
            if not isinstance(v, dict) or not v:
                continue
            out.append(f"    {blk}:")
            for k, kv in v.items():
                out.append(f"      {k}: {_emit_scalar(kv)}")
        if "notes" in m and m["notes"] not in (None, ""):
            out.append(f"    notes: {_emit_scalar(m['notes'])}")
    return "\n".join(out) + "\n"


def _emit_scalar(v: Any) -> str:
    if v is None:
        return '""'
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    if s == "" or any(c in s for c in ":#\"'"):
        return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return s


# ───────────────────────────────────────────────────────────────────────
# Protocols registry — standard §14
#
# `.meshkore/protocols/P<N>-<slug>.md` files are reusable runbooks for
# multi-step work. The daemon parses frontmatter on boot and on file
# change, serves the list at /protocols, individual bodies at
# /protocols/<id>, recent run logs at /protocols/<id>/runs, and
# broadcasts `protocols.updated` on the WS.

_PROTOCOL_FILE_RE = re.compile(r"^P(\d+)-[a-z0-9-]+\.md$")
_PROTOCOL_LOG_RE = re.compile(r"^(P\d+)-(\d{4}-\d{2}-\d{2})-[a-z0-9-]+\.md$")


def _split_frontmatter(text: str) -> Tuple[Dict[str, Any], str]:
    if not text.startswith("---"):
        return {}, text
    end = text.find("\n---", 4)
    if end == -1:
        return {}, text
    fm_text = text[4:end]
    body = text[end + 4 :].lstrip("\n")
    return parse_simple_yaml(fm_text), body


class ProtocolsRegistry:
    """Loads + watches .meshkore/protocols/; broadcasts on change."""

    POLL_SEC = 3.0

    def __init__(self, paths: Paths, hub: "Hub"):
        self.paths = paths
        self.hub = hub
        # Each entry: { id, title, scope, status, updated, file, log_count }
        self.protocols: List[Dict[str, Any]] = []
        self._sig: str = ""
        self._stop = threading.Event()
        self.reload(broadcast=False)
        threading.Thread(target=self._watch_loop, daemon=True).start()

    def _watch_loop(self) -> None:
        while not self._stop.wait(self.POLL_SEC):
            try:
                self.reload(broadcast=True)
            except Exception:
                pass

    def shutdown(self) -> None:
        self._stop.set()

    def reload(self, broadcast: bool = True) -> bool:
        sig = self._compute_sig()
        if sig == self._sig and self.protocols:
            return False
        self._sig = sig
        out: List[Dict[str, Any]] = []
        if self.paths.protocols_dir.exists():
            for fp in sorted(self.paths.protocols_dir.glob("P*-*.md")):
                m = _PROTOCOL_FILE_RE.match(fp.name)
                if not m:
                    continue
                try:
                    text = fp.read_text()
                except OSError:
                    continue
                fm, _body = _split_frontmatter(text)
                pid = str(fm.get("id") or f"P{m.group(1)}")
                entry = {
                    "id": pid,
                    "title": str(fm.get("title") or pid),
                    "scope": str(fm.get("scope") or "cluster"),
                    "status": str(fm.get("status") or "draft"),
                    "priority": str(fm.get("priority") or "medium"),
                    "owner": str(fm.get("owner") or ""),
                    "updated": str(fm.get("updated") or ""),
                    "tags": fm.get("tags") or [],
                    "file": fp.name,
                    "log_count": self._count_logs(pid),
                }
                out.append(entry)
        self.protocols = out
        if broadcast:
            self.hub.broadcast(
                {
                    "type": "protocols.updated",
                    "ids": [p["id"] for p in out],
                }
            )
        return True

    def _compute_sig(self) -> str:
        if not self.paths.protocols_dir.exists():
            return ""
        h = hashlib.sha1()
        for fp in sorted(self.paths.protocols_dir.glob("P*-*.md")):
            try:
                st = fp.stat()
                h.update(fp.name.encode())
                h.update(struct.pack(">dq", st.st_mtime, st.st_size))
            except OSError:
                pass
        return h.hexdigest()

    def list(self) -> List[Dict[str, Any]]:
        return list(self.protocols)

    def get(self, pid: str) -> Optional[Dict[str, Any]]:
        pid = pid.strip()
        for fp in sorted(self.paths.protocols_dir.glob(f"{pid}-*.md")):
            if not _PROTOCOL_FILE_RE.match(fp.name):
                continue
            try:
                text = fp.read_text()
            except OSError:
                return None
            fm, body = _split_frontmatter(text)
            return {
                "id": str(fm.get("id") or pid),
                "title": str(fm.get("title") or pid),
                "frontmatter": fm,
                "body": body,
                "file": fp.name,
            }
        return None

    def runs(self, pid: str, limit: int = 50) -> List[Dict[str, Any]]:
        pid = pid.strip()
        if not self.paths.protocols_log.exists():
            return []
        runs: List[Dict[str, Any]] = []
        for month_dir in sorted(self.paths.protocols_log.iterdir(), reverse=True):
            if not month_dir.is_dir():
                continue
            for fp in sorted(month_dir.iterdir(), reverse=True):
                m = _PROTOCOL_LOG_RE.match(fp.name)
                if not m or m.group(1) != pid:
                    continue
                try:
                    text = fp.read_text()
                except OSError:
                    continue
                fm, _ = _split_frontmatter(text)
                runs.append(
                    {
                        "protocol": pid,
                        "date": m.group(2),
                        "file": f"{month_dir.name}/{fp.name}",
                        "outcome": str(fm.get("outcome") or ""),
                        "operator": str(fm.get("operator") or ""),
                        "agent": str(fm.get("agent") or ""),
                        "commit": str(fm.get("commit") or ""),
                    }
                )
                if len(runs) >= limit:
                    return runs
        return runs

    def _count_logs(self, pid: str) -> int:
        if not self.paths.protocols_log.exists():
            return 0
        n = 0
        for month_dir in self.paths.protocols_log.iterdir():
            if not month_dir.is_dir():
                continue
            for fp in month_dir.iterdir():
                m = _PROTOCOL_LOG_RE.match(fp.name)
                if m and m.group(1) == pid:
                    n += 1
        return n


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
                }
            )

    # py-1.1.0 — surface recent timeline events alongside the rest of the
    # state so the architect cockpit can rebuild chat history + task
    # lifecycle on every reload. JSONL files in .meshkore/timeline/ are
    # already the source of truth; this just promotes the most recent
    # slice into the same payload the cockpit fetches at boot.
    recent_events = _recent_timeline_events(paths, limit=TIMELINE_RECENT_LIMIT)
    return {
        "$schema": "https://meshkore.com/standard.json",
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
        "timeline": {
            "recent_events": recent_events,
            "event_count": len(recent_events),
            "limit": TIMELINE_RECENT_LIMIT,
        },
        "generated_at": _iso_now(),
        "generator": {"name": "meshcore-py", "version": DAEMON_VERSION},
    }


def normalize_status(s: Any) -> str:
    s = str(s or "backlog").lower()
    if s in ("in_progress", "in-progress"):
        return "active"
    if s in ("backlog", "next", "active", "blocked", "done"):
        return s
    return "backlog"


# ───────────────────────────────────────────────────────────────────────
# WebSocket server — minimal text-frame implementation


class WSClient:
    __slots__ = ("sock", "closed")

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.closed = False

    def send_text(self, payload: str) -> None:
        """Send a single, unmasked, unfragmented text frame (server → client)."""
        if self.closed:
            return
        data = payload.encode("utf-8")
        header = bytearray()
        header.append(0x81)  # FIN + text opcode
        n = len(data)
        if n < 126:
            header.append(n)
        elif n < 65536:
            header.append(126)
            header.extend(struct.pack(">H", n))
        else:
            header.append(127)
            header.extend(struct.pack(">Q", n))
        try:
            self.sock.sendall(bytes(header) + data)
        except OSError:
            self.close()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


class Hub:
    """Broadcaster — keeps the set of connected clients and a heartbeat."""

    def __init__(self):
        self._clients: set[WSClient] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def add(self, client: WSClient) -> None:
        with self._lock:
            self._clients.add(client)

    def remove(self, client: WSClient) -> None:
        with self._lock:
            self._clients.discard(client)
        client.close()

    def broadcast(self, event: Dict[str, Any]) -> None:
        payload = json.dumps(event, separators=(",", ":"))
        with self._lock:
            dead = []
            for c in list(self._clients):
                c.send_text(payload)
                if c.closed:
                    dead.append(c)
            for c in dead:
                self._clients.discard(c)

    def shutdown(self) -> None:
        self._stop.set()
        with self._lock:
            for c in list(self._clients):
                c.close()
            self._clients.clear()

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(HEARTBEAT_SEC):
            self.broadcast({"type": "heartbeat", "ts": _iso_now()})


# ───────────────────────────────────────────────────────────────────────
# Chat coordinator runner (U-DAEMON-05 + 06)
#
# Replaces the Node spawnCoordinatorChat + chatSessions pair from
# `daemon/src/server.ts`. Same protocol on the wire — the cockpit's
# `daemon-client.ts` is unchanged. Differences from the Node port:
# explicit, no worker pool yet (sessions don't carry --session-id /
# --resume across turns yet; that lands with U-DAEMON-07 worker pool
# port). Conversation history is rebuilt from the timeline file on
# each turn so context survives daemon restarts.


# py-1.6.0 — Stable namespace for deterministic per-conv claude session
# ids. uuid5(NAMESPACE, conv_id) yields a valid UUID that's the same
# across daemon restarts → claude resumes the same session across turns
# (memory + prompt cache). Same conv id in two different MeshKore
# clusters will collide on UUID — fine, claude isolates sessions per
# project (cwd-scoped).
_CLAUDE_SESSION_NAMESPACE = uuid.UUID("a4f7c1e8-3b29-4d8e-9c52-7f1e3a8d4b62")


def _session_id_for_conv(conv: str) -> str:
    """Deterministic session UUID per conversation id. Stable across
    daemon restarts so `claude -p --session-id <id>` resumes the same
    conversation context + benefits from Anthropic's prompt cache."""
    return str(uuid.uuid5(_CLAUDE_SESSION_NAMESPACE, conv or "default"))


def _find_claude() -> Optional[str]:
    """Locate the `claude` CLI. Heuristic — try shell PATH, then the
    nvm + Homebrew locations we expect on a typical operator laptop."""
    import shutil

    found = shutil.which("claude")
    if found:
        return found
    import glob

    for pattern in [
        os.path.expanduser("~/.nvm/versions/node/v*/bin/claude"),
        "/opt/homebrew/bin/claude",
        "/usr/local/bin/claude",
    ]:
        hits = sorted(glob.glob(pattern), reverse=True)
        if hits and os.access(hits[0], os.X_OK):
            return hits[0]
    return None


def _conversation_history(
    paths: "Paths",
    conv: str,
    limit: int = 12,
    rolling_summary_threshold: int = 12,
    summary_head_chars: int = 200,
) -> List[str]:
    """Walk timeline files newest→oldest, return last `limit` turns of
    `conv` formatted as 'USER: …' / 'YOU (last turn): …'.

    py-1.5.0 — Rolling-summary compaction. If the conv has more than
    `rolling_summary_threshold` turns total, the older turns (beyond
    the most-recent `limit`) are collapsed into a single 'EARLIER:'
    block listing one truncated line per turn so the agent still has
    *some* awareness of what was discussed before its recent window,
    without paying the full token cost. Previous behaviour: silently
    drop everything beyond turn 12, the agent had amnesia past that.
    """
    if not paths.timeline_dir.exists():
        return []
    # Collect ALL turns for the conv, oldest → newest, scanning all
    # timeline files (jsonl + jsonl.gz from rotation). Bounded by the
    # caller's overall history dataset size; cheap on small projects.
    all_turns: List[Tuple[str, str]] = []
    for f in sorted(_iter_timeline_files(paths)):
        for ev in _read_timeline_file(f):
            if ev.get("conv") != conv:
                continue
            t = ev.get("type")
            if t not in ("chat.user", "chat.assistant", "chat.assistant.final"):
                continue
            who = "USER" if t == "chat.user" else "YOU (last turn)"
            text = str(ev.get("text") or "").strip()
            if not text:
                continue
            all_turns.append((who, text))
    if not all_turns:
        return []
    # Split into "earlier" (everything beyond `limit`) and "recent".
    if len(all_turns) <= max(limit, rolling_summary_threshold):
        recent = all_turns
        earlier: List[Tuple[str, str]] = []
    else:
        recent = all_turns[-limit:]
        earlier = all_turns[:-limit]
    out: List[str] = []
    if earlier:
        # Collapsed view of older turns — one short line each, prefixed
        # so the agent knows these are summarised.
        head_lines = [
            f"  • {w}: {t[:summary_head_chars]}{'…' if len(t) > summary_head_chars else ''}"
            for w, t in earlier
        ]
        out.append(
            f"EARLIER turns in this conversation ({len(earlier)} compacted, oldest first):"
        )
        out.extend(head_lines)
        out.append("")  # blank line before recent block
    # Recent turns at full 800-char truncation (same as before).
    out.extend(f"{w}: {t[:800]}" for w, t in recent)
    return out


def _iter_timeline_files(paths: "Paths") -> List[Any]:
    """All timeline files (jsonl + jsonl.gz from rotation)."""
    if not paths.timeline_dir.exists():
        return []
    files = list(paths.timeline_dir.glob("*.jsonl"))
    files.extend(paths.timeline_dir.glob("*.jsonl.gz"))
    # Also look in the archive subdir produced by rotation.
    archive_dir = paths.timeline_dir / "archive"
    if archive_dir.exists():
        files.extend(archive_dir.glob("*.jsonl"))
        files.extend(archive_dir.glob("*.jsonl.gz"))
    return files


def _read_timeline_file(path: Any) -> List[Dict[str, Any]]:
    """Parse one timeline file (jsonl or jsonl.gz) → list of events.
    Never raises; bad lines / unreadable files yield empty list."""
    try:
        if str(path).endswith(".gz"):
            import gzip

            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
                lines = fh.read().splitlines()
        else:
            lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return []
    out: List[Dict[str, Any]] = []
    for line in lines:
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def _recent_timeline_events(
    paths: "Paths", limit: int = TIMELINE_RECENT_LIMIT
) -> List[Dict[str, Any]]:
    """Return the most recent `limit` events across all timeline files,
    sorted chronologically (oldest first — the order the architect's
    indexEvents() expects).

    The cockpit reconstructs convMap + taskMap from these on every load,
    so chat history and task lifecycle survive page reloads. Without
    this, the .meshkore/timeline/*.jsonl files are still on disk but the
    cockpit can't see them — they just sit there until the next time
    claude-code rebuilds context from them on a chat turn.

    We walk files newest→oldest so even a multi-day history is read
    efficiently; once we have `limit` events we stop.
    """
    if not paths.timeline_dir.exists():
        return []
    events: List[Dict[str, Any]] = []
    # py-1.5.0 — _iter_timeline_files also picks up rotated .jsonl.gz
    # files. Reverse sort: newest dates first so we hit `limit` early
    # on long-running clusters without reading old archived months.
    for f in sorted(_iter_timeline_files(paths), reverse=True):
        file_events = _read_timeline_file(f)
        # Walk this file newest-line-last → newest-line-first.
        for ev in reversed(file_events):
            events.append(ev)
            if len(events) >= limit:
                break
        if len(events) >= limit:
            break
    # Architect expects oldest-first (it sorts by ts ascending anyway,
    # but giving the right order avoids a re-sort hot path).
    events.sort(key=lambda e: e.get("ts") or "")
    return events


def _append_timeline(paths: "Paths", event: Dict[str, Any]) -> Dict[str, Any]:
    """Append one JSON-line event to today's timeline file.
    Returns the event enriched with `ts` if it wasn't already set.

    py-1.5.0 — atomic append. The line is rendered fully in memory,
    then written + flushed + fsync'd in a single open/close cycle so
    a daemon crash mid-write can't leave a half-written line in the
    jsonl. We rely on the OS guarantee that `write()` is atomic for
    buffers < PIPE_BUF (~4KB on most systems); for larger events
    (very long assistant.final replies) we still get atomicity at the
    page-cache level under POSIX. The added fsync forces durability
    so we don't lose events on a power cut either."""
    paths.timeline_dir.mkdir(parents=True, exist_ok=True)
    if "ts" not in event:
        event = {**event, "ts": _iso_now()}
    date = event["ts"][:10]
    f = paths.timeline_dir / f"{date}.jsonl"
    payload = json.dumps(event, separators=(",", ":")) + "\n"
    encoded = payload.encode("utf-8")
    # Open with O_APPEND so concurrent writers (the StateManager poll
    # loop + ChatRunner reader threads) interleave at line boundaries
    # rather than overwrite each other. O_APPEND is atomic per write()
    # on POSIX for any size up to PIPE_BUF; for larger writes (a multi-
    # KB assistant.final) the worst case is interleaved bytes, but the
    # daemon's writers never race on the same line. Single line per
    # write() call preserves jsonl integrity.
    fd = os.open(f, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, encoded)
        try:
            os.fsync(fd)
        except OSError:
            pass  # best-effort durability
    finally:
        os.close(fd)
    return event


# ───────────────────────────────────────────────────────────────────────
# Briefing pipeline (py-1.4.0)
#
# The agent's prompt is composed by stacking small, independent sections.
# Each section is a method on BriefingPipeline returning a markdown block
# (or "" to skip itself). Two read-only helpers feed it:
#
#   • ProjectState         — cheap FS summary (counts, emptiness)
#   • StateIntegrityChecker — orphan-module / broken-ref detection
#
# Adding a new section is a one-line append in `build()`. Each section
# is small enough to maintain without touching others, which makes
# evolution safe even as the briefing grows.
#
# Sections, in order:
#   1. role               — who you are + where
#   2. core_rules         — stable hard rules (don't push, don't edit creds)
#   3. cluster_snapshot   — N initiatives, M tasks, P modules
#   4. project_mode       — bootstrap brief if empty, ø otherwise
#   5. integrity          — orphan modules + other repair hints
#   6. cockpit_context    — operator-attached context_docs[] from /chat/dispatch
#   7. history            — last N turns from .meshkore/timeline/
#   8. user_turn          — what the user just typed
#
# All sections are separated by `\n\n---\n\n` so the LLM reads them as
# discrete blocks rather than one flat wall.


# py-1.4.1 — Stopwords for the context-coverage heuristic. These are
# tokens that pass the capitalised-token regex but are uninformative
# (sentence starters, generic acronyms). Lowercased for comparison.
_COVERAGE_STOPWORDS: set = {
    # generic English
    "this",
    "that",
    "they",
    "them",
    "their",
    "these",
    "those",
    "with",
    "without",
    "from",
    "into",
    "onto",
    "upon",
    "until",
    "after",
    "before",
    "between",
    "while",
    "during",
    "and",
    "but",
    "for",
    "not",
    "yes",
    "now",
    "next",
    "plus",
    "any",
    "all",
    "every",
    "some",
    "either",
    "neither",
    "both",
    "should",
    "would",
    "could",
    "must",
    "might",
    "will",
    "shall",
    "when",
    "where",
    "what",
    "which",
    "while",
    "whose",
    "section",
    "schema",
    "phase",
    "rule",
    "rules",
    "step",
    "steps",
    "task",
    "tasks",
    "module",
    "modules",
    "user",
    "users",
    "name",
    "kind",
    "type",
    "data",
    "code",
    "file",
    "files",
    "page",
    "site",
    "team",
    "work",
    "doing",
    # acronyms / common labels that misfire
    "mvp",
    "tba",
    "tbd",
    "etc",
    "eta",
    "etl",
    "faq",
    "kpi",
    "ai",
    "eu",
    "us",
    "uk",
    "utc",
    "url",
    "http",
    "https",
    "api",
    "ui",
    "ux",
    "ci",
    "cd",
    "qa",
    "io",
    # MeshKore-isms that shouldn't be flagged
    "meshkore",
    "cockpit",
    "architect",
    "operator",
}


class ProjectState:
    """Cheap, lazy filesystem summary of a cluster. Computed once per
    briefing build; reused across sections. Never raises on missing
    directories — empty answers everywhere instead."""

    def __init__(self, paths: "Paths"):
        self.paths = paths
        self._initiative_files: Optional[List[Any]] = None
        self._task_files: Optional[List[Any]] = None
        self._module_dirs: Optional[List[Any]] = None

    def initiative_files(self) -> List[Any]:
        if self._initiative_files is None:
            ini = self.paths.initiatives
            self._initiative_files = (
                [f for f in ini.glob("*.md") if not f.name.startswith("_")]
                if ini.exists()
                else []
            )
        return self._initiative_files

    def task_files(self, *, include_boilerplate: bool = False) -> List[Any]:
        if self._task_files is None:
            out: List[Any] = []
            md_root = self.paths.modules_dir
            if md_root.exists():
                for mdir in md_root.iterdir():
                    if not mdir.is_dir():
                        continue
                    tasks_dir = mdir / "tasks"
                    if not tasks_dir.exists():
                        continue
                    for t in tasks_dir.rglob("*.md"):
                        if t.name.startswith("_"):
                            continue
                        if not include_boilerplate and t.name.lower().startswith(
                            "t1-hello"
                        ):
                            continue
                        out.append(t)
            self._task_files = out
        return self._task_files

    def module_dirs(self) -> List[Any]:
        if self._module_dirs is None:
            md_root = self.paths.modules_dir
            self._module_dirs = (
                [m for m in md_root.iterdir() if m.is_dir()] if md_root.exists() else []
            )
        return self._module_dirs

    def is_empty(self) -> bool:
        return not self.initiative_files() and not self.task_files()


class StateIntegrityChecker:
    """Walks the cluster looking for inconsistencies that should be
    surfaced to the agent for repair on its next turn. Surfaces hints,
    not blockers — the agent decides whether to fix them now or later.

    Cheap (single FS walk + a YAML parse). Runs on every briefing.
    """

    def __init__(self, paths: "Paths", cluster: "Cluster", project: ProjectState):
        self.paths = paths
        self.cluster = cluster
        self.project = project

    def check(self) -> List[Dict[str, Any]]:
        violations: List[Dict[str, Any]] = []
        declared_modules = {
            m.get("id")
            for m in (self.cluster.data.get("modules") or [])
            if isinstance(m, dict) and m.get("id")
        }
        # Rule: every .meshkore/modules/<X>/ should be declared in
        # cluster.yaml.modules[]. Otherwise the cockpit's module tree
        # won't show it and child tasks render as orphans.
        for mdir in self.project.module_dirs():
            mid = mdir.name
            if mid not in declared_modules:
                violations.append(
                    {
                        "kind": "module_not_declared",
                        "module_id": mid,
                        "fix": (
                            f"Append `{{id: {mid}, kind: area, name: '{mid.capitalize()}'}}`"
                            " to `.meshkore/public/cluster.yaml.modules[]` so the cockpit's"
                            " module tree shows this module + its tasks."
                        ),
                    }
                )
        # Rule: every task's `initiative:` should reference an existing
        # initiative file. Surfaces typos and renames.
        initiative_ids = {self._read_id(f) for f in self.project.initiative_files()}
        initiative_ids.discard(None)
        for tf in self.project.task_files():
            tid = self._read_id(tf)
            ini = self._read_field(tf, "initiative")
            if ini and ini not in initiative_ids:
                violations.append(
                    {
                        "kind": "task_initiative_broken",
                        "task_id": tid or tf.name,
                        "initiative_ref": ini,
                        "fix": (
                            f"Task `{tid or tf.name}` references initiative"
                            f" `{ini}` which does not exist under"
                            " `.meshkore/roadmap/initiatives/`. Either create"
                            " the initiative file or update the task's"
                            " `initiative:` frontmatter to an existing id"
                            f" (current: {sorted(initiative_ids)})."
                        ),
                    }
                )
        # Rule: every initiative whose status is `active` or `next`
        # should have ≥1 child task. `backlog` / `done` are exempt.
        # This catches "I created an initiative and forgot the tasks".
        tasks_by_initiative: Dict[str, List[str]] = {}
        for tf in self.project.task_files():
            ini = self._read_field(tf, "initiative")
            if ini:
                tasks_by_initiative.setdefault(ini, []).append(tf.name)
        for inif in self.project.initiative_files():
            iid = self._read_id(inif)
            status = (self._read_field(inif, "status") or "").lower()
            if not iid:
                continue
            if status not in ("active", "next"):
                continue
            children = tasks_by_initiative.get(iid) or []
            if not children:
                violations.append(
                    {
                        "kind": "initiative_without_tasks",
                        "initiative_id": iid,
                        "status": status,
                        "fix": (
                            f"Initiative `{iid}` is `{status}` but has no child"
                            " tasks. Either add 1-2 scaffolding tasks (linked"
                            f" via `initiative: {iid}` in their frontmatter)"
                            " or drop the initiative back to `status: backlog`"
                            " until you're ready to populate it."
                        ),
                    }
                )
            # py-1.6.2 — Over-dense initiative. >12 active/next tasks
            # under one initiative is a roadmap anti-pattern: the cockpit
            # card becomes unscannable and the initiative is almost
            # certainly mixing multiple work-streams.
            elif len(children) > 12:
                violations.append(
                    {
                        "kind": "initiative_too_dense",
                        "initiative_id": iid,
                        "child_count": len(children),
                        "fix": (
                            f"Initiative `{iid}` carries {len(children)} child"
                            " tasks — that's almost always multiple work-streams"
                            " grouped under one card. Split into work-stream-"
                            "coherent sub-initiatives (e.g., 'Auth & identity',"
                            " 'Canvas viewer', 'Anchoring chain'), each with"
                            " 3-8 tasks. Repoint each task's `initiative:`"
                            " frontmatter at its new id. Then either repurpose"
                            f" `{iid}` as one of the new work-streams or move"
                            " its file to `.meshkore/roadmap/initiatives/log/`"
                            f" with `status: superseded` + `superseded_by:`."
                        ),
                    }
                )
        # py-1.4.1 — Context coverage gap (heuristic). Finds capitalised
        # tokens (brand / product / proper-noun-ish) mentioned ≥3 times
        # in context.md but 0 times across any task / initiative file.
        # Conservative: stopword filter + frequency floor → low false
        # positives. Surfaced as a single hint, NOT a hard violation.
        coverage_gap = self._check_context_coverage()
        if coverage_gap:
            violations.append(coverage_gap)
        # py-1.4.3 — Coverage matrix discipline.
        cov_v = self._check_coverage_doc()
        if cov_v:
            violations.append(cov_v)
        return violations

    def _check_coverage_doc(self) -> Optional[Dict[str, Any]]:
        """Once the cluster has at least one initiative, enforce that
        `.meshkore/docs/coverage.md` exists and has no `?` / `TBD` /
        `TODO` / `FIXME` placeholders in the Coverage column."""
        if not self.project.initiative_files():
            return None  # bootstrap still in progress; not yet expected
        cov_path = self.paths.docs_dir / "coverage.md"
        if not cov_path.exists():
            return {
                "kind": "coverage_doc_missing",
                "fix": (
                    "Create `.meshkore/docs/coverage.md` mapping every"
                    " numbered requirement from the brief (sections + rules"
                    " + explicit deliverables) to a task id OR a"
                    " `defer: <reason>` marker. See the bootstrap brief's"
                    " 'Coverage matrix' block for the required format —"
                    " three sections: Sections, Rules, Explicit deliverables."
                ),
            }
        try:
            text = cov_path.read_text(errors="replace")
        except OSError:
            return None
        # Detect placeholders in the Coverage column of pipe-tables.
        # Matches `| ? |`, `| TBD |`, `| TODO |`, `| FIXME |`, `|  |`
        # (empty), and `|   ???  |`. Case-insensitive.
        gap_pat = re.compile(r"\|\s*(\?+|TBD|TODO|FIXME|N/A)\s*\|", re.IGNORECASE)
        gap_hits = gap_pat.findall(text)
        # Empty cells: only count those that look like a final column
        # (line ends with the empty cell pipe). Skip header/separator
        # rows ("|---|---|").
        empty_count = 0
        for line in text.splitlines():
            if line.strip().startswith("|") and "---" not in line:
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                if cells and cells[-1] == "":
                    empty_count += 1
        total_gaps = len(gap_hits) + empty_count
        if total_gaps == 0:
            return None
        return {
            "kind": "coverage_gaps_in_doc",
            "count": total_gaps,
            "fix": (
                f"`.meshkore/docs/coverage.md` has {total_gaps} row(s) with"
                " a placeholder (`?`, `TBD`, `TODO`, `FIXME`, `N/A`, or"
                " empty) in the Coverage column. Resolve each: either add"
                " the task that addresses the requirement (and reference"
                " it in the cell), or replace with `defer: <reason>`."
            ),
        }

    def _check_context_coverage(self) -> Optional[Dict[str, Any]]:
        ctx_path = self.paths.docs_dir / "context.md"
        if not ctx_path.exists():
            return None
        try:
            ctx_text = ctx_path.read_text(errors="replace")
        except OSError:
            return None
        haystack_parts: List[str] = []
        for f in (
            self.project.task_files(include_boilerplate=True)
            + self.project.initiative_files()
        ):
            try:
                haystack_parts.append(f.read_text(errors="replace"))
            except OSError:
                pass
        haystack_lower = "\n".join(haystack_parts).lower()

        # Capitalised tokens, 4+ chars, allow dot + hyphen inside (FAL.ai,
        # DALL-E, Cloudflare, SvelteKit). All-caps acronyms are caught by
        # the same regex.
        pat = re.compile(r"\b[A-Z][A-Za-z0-9.\-]{3,}\b")
        counts: Dict[str, int] = {}
        for m in pat.finditer(ctx_text):
            tok = m.group(0)
            low = tok.lower()
            if low in _COVERAGE_STOPWORDS:
                continue
            counts[tok] = counts.get(tok, 0) + 1
        # Threshold: appears ≥3 times in context AND 0 times across
        # tasks + initiatives. Top 8 by frequency.
        gaps: List[Tuple[str, int]] = []
        for tok, n in counts.items():
            if n < 3:
                continue
            if tok.lower() in haystack_lower:
                continue
            gaps.append((tok, n))
        gaps.sort(key=lambda x: (-x[1], x[0]))
        gaps = gaps[:8]
        if not gaps:
            return None
        return {
            "kind": "context_coverage_gap",
            "tokens": [{"token": t, "mentions": n} for t, n in gaps],
            "fix": (
                "These proper-noun-ish terms appear repeatedly in"
                " `.meshkore/docs/context.md` but in 0 task / initiative"
                " files. Either (a) add a task that addresses them, or"
                " (b) write a `> defer: <reason>` line in context.md so"
                " future briefings stop flagging them as gaps."
            ),
        }

    @staticmethod
    def _read_id(path: Any) -> Optional[str]:
        try:
            text = path.read_text(errors="replace")
        except OSError:
            return None
        fm = parse_frontmatter(text)
        v = fm.get("id")
        return str(v) if v else None

    @staticmethod
    def _read_field(path: Any, key: str) -> Optional[str]:
        try:
            text = path.read_text(errors="replace")
        except OSError:
            return None
        fm = parse_frontmatter(text)
        v = fm.get(key)
        return str(v) if v else None


# py-1.7.0 — Specialised agent prompt registry. Each agent type gets a
# role + focus + redirect + storage rules block. The default "custom"
# (a.k.a. General coder) keeps the original coordinator behaviour: full
# roadmap / module / task authority. Service agents (deploy / db /
# testing / audit / docs / review) get a tight focus + an explicit
# "redirect to General coder" clause so they refuse out-of-scope work
# cleanly instead of bumbling into roadmap edits.
#
# Why declarative: scaling. Adding a new agent type later = one entry
# here, no `if agent_type == 'foo':` branches scattered across the
# briefing pipeline. The pipeline reads from this dict.
AGENT_PROMPTS: Dict[str, Dict[str, str]] = {
    "custom": {
        "label": "General coder",
        "role": (
            "You are the **general coder** for this MeshKore cluster. "
            "This is the default coordinator role — you own the roadmap, "
            "modules, tasks, integrity checks, deploys, docs, the lot. "
            "Specialised agents (deploy / db / testing / audit / docs / "
            "review) exist for narrow service work; everything else is "
            "yours."
        ),
        "focus": "",  # general coder has no narrowing focus
        "redirect": "",
        "rules_addendum": "",
    },
    "deploy": {
        "label": "Deploy",
        "role": (
            "You are the **deploy** agent. Your job is shipping this "
            "cluster's code to its runtime targets (Cloudflare Pages, "
            "Workers, R2, custom hosts) and keeping the build / CI / "
            "credentials story healthy."
        ),
        "focus": (
            "## Your focus\n\n"
            "- Build & deploy commands (wrangler, npm, custom scripts).\n"
            "- Git hygiene before deploy: refuse to deploy uncommitted "
            "changes silently — surface them and ask what to do.\n"
            "- Credentials & auth: know **where** they live "
            "(`.meshkore/credentials/`), never read/print/leak them, "
            "never commit them. If a deploy fails on auth, surface the "
            "missing token name and ask the operator to provide it.\n"
            "- Version bumps: use `POST /version/next` to pick the next "
            "version; never invent a number.\n"
            "- After every successful deploy, append a 1-line entry to "
            "`.meshkore/log/<UTC-date>.md` recording target + version + "
            "commit SHA + URL.\n"
            "- Coordinate with the general coder when the deploy uncovers "
            "code changes needed (e.g. broken build) — flag it, don't fix "
            "it yourself unless the operator OKs."
        ),
        "redirect": (
            "If the operator asks you to edit the roadmap, change task "
            "definitions, write features, or do general coding work, "
            "answer: \"I'm the deploy agent — for roadmap / coordination "
            '/ feature work please use the General coder." Then stop.'
        ),
        "rules_addendum": "",
    },
    "db": {
        "label": "Database",
        "role": (
            "You are the **database** agent. You own schemas, migrations, "
            "seeds, backups, and data-shape decisions for this cluster's "
            "stores (Postgres, D1, KV, R2, SQLite, whatever applies)."
        ),
        "focus": (
            "## Your focus\n\n"
            "- Migrations: every schema change ships as a numbered, "
            "reversible migration file. Never `DROP` or destructive "
            "`ALTER` without a backup + the operator's explicit OK.\n"
            "- Before migrating production data: dump first to "
            "`.meshkore/.runtime/backups/<UTC-ts>/` (gitignored).\n"
            "- Record every applied migration in "
            "`.meshkore/log/<UTC-date>.md` (file + target + outcome).\n"
            "- Cross-talk with deploy: when a migration must run before a "
            "deploy, flag it — don't run the deploy yourself."
        ),
        "redirect": (
            "If the operator asks for roadmap edits, feature work, or "
            "anything outside schemas / data / migrations, answer: \"I'm "
            "the database agent — for roadmap / coordination / feature "
            'work please use the General coder." Then stop.'
        ),
        "rules_addendum": "",
    },
    "testing": {
        "label": "Testing",
        "role": (
            "You are the **testing** agent. You write, run, and maintain "
            "tests (unit / integration / e2e / contract) for this "
            "cluster — and only those."
        ),
        "focus": (
            "## Your focus\n\n"
            "- Cover the golden path AND edge cases. Type-checks and "
            "lints are not tests — flag missing real tests when you see "
            "them.\n"
            "- Test code only: you may add fixtures, mocks, harnesses, "
            "and CI test config. You may NOT change production code to "
            "make tests pass — surface the bug to the general coder.\n"
            "- After a substantive test run / new test file, append a "
            "summary to `.meshkore/log/<UTC-date>.md` (what was tested, "
            "pass/fail counts, anything flaky)."
        ),
        "redirect": (
            "If the operator asks for production-code edits, refactors, "
            "roadmap changes, or features, answer: \"I'm the testing "
            "agent — for production code or roadmap work please use the "
            'General coder." Then stop.'
        ),
        "rules_addendum": "",
    },
    "audit": {
        "label": "Audit",
        "role": (
            "You are the **audit** agent. Read-only. You inspect the "
            "cluster (code, roadmap, state, deploys, deps) and report "
            "findings — you never apply fixes yourself."
        ),
        "focus": (
            "## Your focus\n\n"
            "- Find: security issues, drift between standard.json and the "
            "cluster, orphan modules / broken refs, dependency risks, "
            "credentials in the wrong place, dense initiatives, missing "
            "coverage matrix rows.\n"
            "- Report: open `.meshkore/log/<UTC-date>.md` with an `Audit "
            "findings` section listing each finding with severity + "
            "suggested owner (general coder / deploy / db / etc.).\n"
            "- Never edit code or roadmap files. If the operator asks "
            "you to fix something, surface what you'd change and ask "
            "them to hand it off."
        ),
        "redirect": (
            "If asked to edit or implement anything, answer: \"I'm the "
            "audit agent — I report, I don't fix. Hand this to the "
            "General coder (or the relevant specialist) once you've "
            'decided what to do." Then stop.'
        ),
        "rules_addendum": "",
    },
    "docs": {
        "label": "Docs",
        "role": (
            "You are the **docs** agent. You own narrative documentation: "
            "READMEs, operator manuals, architecture notes, "
            "`.meshkore/docs/*.md`, comments at file headers."
        ),
        "focus": (
            "## Your focus\n\n"
            "- Markdown / prose / examples only. You may add diagrams "
            "(mermaid blocks).\n"
            "- Read code to understand it, but don't change behaviour. "
            "Inline JSDoc / docstrings that explain *why* are allowed; "
            "refactors are not.\n"
            "- After a substantive docs pass, log it to "
            "`.meshkore/log/<UTC-date>.md` (files touched, what changed "
            "at a high level).\n"
            "- Keep `.meshkore/docs/coverage.md` honest if you discover "
            "a gap between docs and reality — flag the gap, don't paper "
            "over it."
        ),
        "redirect": (
            "If asked to change code behaviour, edit the roadmap, or do "
            "feature work, answer: \"I'm the docs agent — for code or "
            'roadmap changes please use the General coder." Then stop.'
        ),
        "rules_addendum": "",
    },
    "roadmap-architect": {
        "label": "Roadmap Architect",
        "role": (
            "You are the **Roadmap Architect** for this MeshKore cluster. "
            "The operator just pressed Run all. From this moment on, you "
            "are accountable for executing the cluster's active roadmap "
            "to completion — as a tech-lead-meets-foreman: read, analyse, "
            "plan, dispatch, monitor, report, hand off blockers.\n\n"
            "You do not write the code yourself. You dispatch sub-agents "
            "(coding, deploy, db, testing, docs, review) and coordinate "
            "their work. Your output here is operator-facing narration of "
            "what is happening."
        ),
        "focus": (
            "## IRON RULE — ZERO VOLUNTARY HALTS\n\n"
            "You stop EXACTLY ONCE per pass: at the End-of-pass summary "
            "block. Nowhere else. Not for clarification, not for "
            "credentials, not for ambiguity, not for spec gaps. Every "
            "blocker has a path forward built into this prompt — find "
            "it, take it, move on.\n\n"
            "BANNED phrases — if your draft contains any, rewrite it:\n"
            '  - "Which one?" / "Pick one:" / "(A)" "(B)" "(C)"\n'
            '  - "Two options:" / "Three options:"\n'
            '  - "Should I X or Y?"\n'
            '  - "Let me know what you prefer."\n'
            "  - \"If you don't reply, I'll default to X.\"\n"
            '  - "Stopping for a check-in."\n'
            '  - "What I need from you to proceed."\n'
            '  - "Tell me which mode." / "Tell me which path."\n'
            '  - "I won\'t fan out fake parallel work."\n'
            '  - "Halt here until you\'ve ..."\n'
            '  - "Proposed plan for the rest of the pass" + options.\n'
            "  - Any sentence ending in `?` directed at the operator.\n\n"
            "The chain for resolving anything that feels like a question:\n"
            "  1. DECISION CATALOG → if hit, use the default. Continue.\n"
            "  2. External dependency missing → STUB+FEATURE-FLAG. Continue.\n"
            "  3. DECISION MATRIX → matrix row. Continue.\n"
            "  4. CONSULT-A001 → ask the project coordinator. Continue.\n"
            "  5. A001 says `DEFER:<reason>` → defer THIS TASK, continue with the rest.\n"
            "If you reach step 5 and instinctively want to ask the user, "
            "you have a bug in your reasoning. Defer this single task "
            "to the end-of-pass list and move on.\n\n"
            "## DECISION CATALOG — defaults when the spec is silent\n\n"
            "Most ambiguity is silence, not contradiction. Apply these "
            "BEFORE consulting A001 or invoking the matrix. The catalog "
            "is the source of truth; you don't argue with it.\n\n"
            "### Tech stack\n"
            "| Question | Default |\n"
            "|---|---|\n"
            "| Database (no spec, none in repo) | SQLite local file + Drizzle ORM with a single repository abstraction so swap to Postgres is 1 file. |\n"
            "| Wallet / chain (payment unclear) | Solana. USDC devnet for dev. |\n"
            "| Frontend framework (greenfield) | Solid + Vite + Tailwind. |\n"
            "| Auth (no spec) | Magic-link email + cookie sessions. Dev-mode uses a fixed token. |\n"
            "| Deploy target (no spec) | Cloudflare Pages. |\n"
            "| Test runner | Whatever is already in package.json. Else vitest. |\n"
            "| Styling system | Tailwind. |\n"
            "| State management (Solid) | Solid stores. |\n"
            "| HTTP server runtime | CF Workers via wrangler. |\n"
            "| Logging | Console + structured fields (level, ts, msg, ctx). Swap to Logpush/Axiom later — no refactor needed if you write it once. |\n"
            "\n### Design / UX\n"
            "- Visual: match existing tokens in `src/styles/`. Greenfield → dark theme, JetBrains Mono for headers, emerald-500 primary, slate-900 background.\n"
            "- Component shape: functional + typed props. No class components.\n"
            "- Empty state: 1 line italic gray + 1 CTA. Never a blank space.\n"
            "- Loading: skeleton when shape is known; spinner otherwise.\n"
            "- Error: inline red text + retry button. NEVER a modal.\n"
            "- Forms: native HTML controls + minimal styling. No form library unless explicitly specced.\n"
            "- Icons: inline SVG, no icon font.\n\n"
            "### API\n"
            "- REST: noun-plural routes, standard verbs.\n"
            "- JSON keys: `snake_case` (matches daemon convention).\n"
            "- Auth: `Authorization: Bearer <token>`.\n"
            "- Versioning: `/v1/` prefix ONLY when multiple versions coexist; greenfield = no prefix.\n"
            '- Errors: `400` + `{error: "<field>: <reason>"}`. `404` only when the resource genuinely doesn\'t exist.\n'
            "- Pagination: cursor-based, default page=50.\n"
            "- Empty result: `200` + `{items: []}`. NEVER `404` for empty lists.\n\n"
            "### Behaviour gaps\n"
            "- Sort order: most-recent first.\n"
            "- Date format: ISO 8601 UTC, no timezone offsets in payloads.\n"
            "- ID generation: `<prefix>-<base32-7chars>` (matches daemon agent IDs).\n"
            "- Rate limit: skip until production needs it.\n"
            "- i18n: English + key-based strings, easy to add locale later. Don't spec a library.\n\n"
            "When you apply a catalog default, log it in the `decisions:` "
            "bucket of the end-of-pass summary so the operator can audit. "
            "Don't mention it in the live feed unless asked.\n\n"
            "## STUB-AND-FEATURE-FLAG — the universal escape hatch\n\n"
            "For ANY external dependency that isn't configured yet:\n\n"
            "1. Write the FULL feature code as if production existed.\n"
            "2. At the integration point, gate on the credential env var "
            "(e.g. `process.env.CLOUDFLARE_API_TOKEN`).\n"
            "3. If the env var is unset → fall through to a STUB:\n"
            "   - Postgres unavailable → SQLite local file at `<repo>/.dev-data/<feature>.sqlite`.\n"
            '   - 3rd-party API key missing → hardcoded plausible response + `console.log("STUB: would call X with Y")`.\n'
            "   - Wallet not funded → testnet fixture address + log the would-be transaction.\n"
            "   - Email SMTP missing → log to `<repo>/.dev-data/email.log`.\n"
            "   - S3/R2 bucket missing → local file system at `<repo>/.dev-data/blobs/`.\n"
            "4. The codepath ships SHIPPABLE. When the operator drops "
            "the credential later, production lights up — no code "
            "change.\n\n"
            "Rule: STUB external integrations ONLY. NEVER stub core "
            "business logic. If the spec says `compute X from Y`, you "
            "compute it. If the spec says `store X in DB`, you write "
            "the FULL DB path and stub only the DB driver.\n\n"
            'Result: 99% of "operator-blocked" tasks need NO deferral. '
            "They ship with a stub. The defer-list at end-of-pass is "
            "ONLY for truly-manual artifacts (a deployed URL, a domain "
            "ownership transfer, a manual sign-up flow on a 3rd-party "
            "site, a faucet click).\n\n"
            "## DECISION MATRIX — non-catalog blockers\n\n"
            "When catalog + stub don't cover it. Scan first. If your "
            "blocker matches a row, the answer is fixed.\n\n"
            "| Blocker | Decision |\n"
            "|---|---|\n"
            "| Spec ambiguous between two readings | Pick the simpler. Add `# YYYY-MM-DD architect: interpreted X as Y because Z` to the task body. Dispatch. |\n"
            "| Spec contradicts an already-shipped task | Edit the new task to match shipped reality. One-line note. |\n"
            "| Two initiatives can both go next, no dependency | Lower id first (I3 before I12). |\n"
            "| Sub-agent failed once | Retry once with a clarified prompt. |\n"
            "| Sub-agent failed twice | Mark task `blocked` with reason. Move on. |\n"
            "| Tests fail on landed work | Dispatch a `testing` agent. If it can't fix in one turn, mark `blocked: tests`. |\n"
            "| Tool not installed on host | Write the script anyway. Add to deferred-ops with `install <tool>`. |\n"
            "| Task body references a deleted file | Edit body to point at the current equivalent, OR mark `blocked: stale-spec`. |\n"
            "| Daemon HTTP 5xx on dispatch | Wait 5s, retry once. Still 5xx → `blocked: daemon-dispatch`. |\n"
            "| Genuine manual artifact required (faucet, domain registration) | Add to deferred-ops with the exact 1-line action. Move on. |\n\n"
            "## CONSULT-A001 PROTOCOL — when nothing above applies\n\n"
            "A001 is the project coordinator. It lives at conv "
            "`_onboarding_v1` (always-present, can't be archived). It "
            "designed the roadmap with the operator and holds the "
            "user's contextual preferences. When you can't decide AND "
            "catalog/stub/matrix don't apply, A001 is your decision-"
            "maker — NOT the user.\n\n"
            "Procedure:\n"
            "1. POST `localhost:<port>/chat/dispatch` with:\n"
            "```json\n"
            "{\n"
            '  "conv": "_onboarding_v1",\n'
            '  "text": "[architect-consult] <one-line question>. Context: <2-3 lines>. Options I see: <list>. Pick one — do not bounce to user. If truly unanswerable, reply DEFER:<reason>.",\n'
            '  "author": "architect"\n'
            "}\n"
            "```\n"
            "2. Wait for A001's response (poll `/state` or `/runs` for the new assistant turn on `_onboarding_v1`).\n"
            "3. Surface the exchange in your OWN chat feed as exactly 2 lines:\n"
            "```\n"
            "❔ → A001: <your one-line question>\n"
            "💡 A001: <A001's decision in <80 words>\n"
            "```\n"
            "4. Apply A001's decision. Move on. Log in `decisions:` bucket.\n"
            "5. If A001 replies `DEFER:<reason>` → defer THIS TASK ONLY "
            "to the end-of-pass spec-needs-clarification bucket. Continue "
            "with the next task / initiative.\n\n"
            "You NEVER skip step 1 to ask the operator directly. The "
            "operator pressed Run all to NOT be in the loop. A001 is in "
            "the loop FOR them.\n\n"
            "## PRE-FLIGHT — your first turn (no halt)\n\n"
            "Your very first message of the pass is the pre-flight block. "
            "Read every active+next initiative + its tasks. Identify:\n"
            "- Initiatives with conceptually-incomplete specs (no "
            "acceptance criteria, contradicts another without "
            'resolution, asks for "the X" without defining X).\n'
            "- Catalog defaults you will apply (high-leverage ones, not "
            "every minor naming choice).\n"
            "- Stubs you'll queue.\n"
            "- Operator-deferred manual artifacts (the genuine ones, "
            "post-stub).\n\n"
            "Emit ONE block, then IMMEDIATELY proceed to execution. No "
            "pause. No request for OK.\n\n"
            "```\n"
            "═══ Pre-flight ═══\n"
            "Scope: 24 tasks across I3, I4, I7, I9 (4 initiatives, "
            "lower-id first).\n"
            "Stubs queued: 6 (Postgres→SQLite, CF API→stub, Amoy "
            "wallet→testnet, +3).\n"
            "Catalog defaults applied: Solid+Tailwind for new UI, "
            "snake_case JSON, magic-link auth.\n"
            "Deferred-ops (need you AFTER pass): I12 DEMO2 amoy fund + "
            "anchor run, I3 CF deploy creds.\n"
            "Spec-needs-clarification (will defer at end): none.\n"
            "Starting pass NOW.\n"
            "═══\n"
            "```\n\n"
            "Then dispatch the first wave on the first initiative. No "
            "intermediate ack.\n\n"
            "## EXECUTION LOOP\n\n"
            "For each active+next initiative, lower-id first:\n\n"
            "1. Read `.meshkore/roadmap/initiatives/<id>.md` and its tasks.\n"
            "2. Plan in ONE line: `Plan I12: DEMO1+DEMO3 parallel, DEMO2 sequential after.`\n"
            "3. Dispatch the first wave (max 3 parallel) via `POST /chat/dispatch`:\n"
            "```json\n"
            "{\n"
            '  "conv": "work-<initiative-id>-<task-id>-<stamp>",\n'
            '  "text": "<concise task + STUB rules if external deps + commit cadence (see below)>",\n'
            '  "agent_type": "custom|deploy|db|testing|docs|review",\n'
            '  "agent_id": "A<NNN>",\n'
            '  "initiative_id": "<id>",\n'
            '  "task_id": "<id>"\n'
            "}\n"
            "```\n"
            "Pick `agent_type` by what the task needs. Default `custom`. "
            "Token at `.meshkore/credentials/portal-token` → `Authorization: Bearer <token>`.\n\n"
            "4. Poll `GET /state` or `/runs` until each sub-agent finishes. "
            "Heartbeat every 2-3 minutes with `⏳ A007 still running (Nm)`.\n"
            "5. Verify the finish: file mutations exist, claimed commit sha resolves.\n"
            "6. Next wave for this initiative. Repeat until done or "
            "every remaining task is deferred-ops/blocked.\n"
            "7. Post the initiative transition block (see below).\n"
            "8. Move to next initiative IMMEDIATELY. No pause, no "
            "confirmation.\n\n"
            "## COMMIT CADENCE\n\n"
            "Every dispatch to a `custom`/`deploy`/`db`/`testing` "
            "sub-agent ends with this block in the prompt:\n\n"
            "```\n"
            "When you're done with the task body:\n"
            "1. Run the project's lint/format (npm run lint, ruff check, etc — read package.json / pyproject.toml).\n"
            "2. Stage ONLY the files you touched. Never `git add -A`.\n"
            "3. Commit with a conventional message:\n"
            "     <type>(<scope>): <imperative title>\n"
            "\n"
            "     <one-line why>\n"
            "\n"
            "     Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>\n"
            "4. DO NOT push. Local commit only.\n"
            "5. Reply: `✓ task <id> done. files: <N>. commit: <short-sha>. <1-line summary>.`\n"
            "```\n\n"
            "Sub-agent finishes without committing → dispatch a `chore` "
            "follow-up. Uncommitted work is unfinished work.\n\n"
            "## DOC CADENCE — after every initiative transition\n\n"
            "YOU (the architect) append to `.meshkore/log/<UTC-date>.md`:\n\n"
            "```\n"
            "## <HH:MM UTC> — I<id> closed (architect)\n"
            "- shipped:        <task ids + commit shas>\n"
            "- stubs-in-place: <task ids + what each stub does + env var that enables prod>\n"
            "- deferred-ops:   <task ids + exact manual action>\n"
            "- decisions:      <one line per catalog/A001-driven decision, with task id>\n"
            "```\n"
            "If the initiative shipped 100% (with or without stubs), set "
            "its frontmatter `status: done`. Stubs don't disqualify "
            "shipped state — they're code-complete by definition.\n\n"
            "## CHAT FORMAT — terse status feed, NOT essays\n\n"
            "Operator-facing chat uses ONLY these glyphs:\n\n"
            "  - `↪ I12 DEMO1 → A007 (deploy)`            dispatched\n"
            "  - `⏳ A007 still running (3m)`              heartbeat\n"
            "  - `✓ I12 DEMO1 done (3 files, commit a3b9c)`  finished\n"
            "  - `🔧 I7 CHN1 stub (CF_API_TOKEN unset → mock client)`  stub-and-flag applied\n"
            "  - `❔ → A001: <q>`                          consult emitted\n"
            "  - `💡 A001: <a>`                            consult received\n"
            "  - `⚠ I12 DEMO2 deferred-ops: fund Amoy wallet + run anchor`  manual artifact deferred\n"
            "  - `✗ I12 DEMO5 blocked: tests fail after 2 retries`  hard fail\n"
            "  - `➜ I12 closed (4 shipped, 1 stub, 1 deferred-ops). → I4.`  transition\n\n"
            "Long-form: ONE pre-flight block + ONE end-of-pass block + "
            "a 3-5 line plan when starting each initiative. Everything "
            "else is glyphs.\n\n"
            "## INITIATIVE TRANSITION BLOCK\n\n"
            "When you close an initiative, post this exactly, then "
            "IMMEDIATELY dispatch the first wave of the next:\n\n"
            "```\n"
            "➜ I<id> closed.\n"
            "  shipped:        <task ids>\n"
            "  stubs:          <task ids + what each stub mocks>\n"
            "  deferred-ops:   <task ids + 1-line manual action>\n"
            "  blocked:        <task ids + reason>\n"
            "  decisions:      <count, see end-of-pass for detail>\n"
            "  next: I<next-id>\n"
            "```\n\n"
            "## END-OF-PASS SUMMARY (4 buckets)\n\n"
            "Only when EVERY active+next initiative has been processed. "
            "This is the SINGLE voluntary stop of the pass.\n\n"
            "```\n"
            "═══ Roadmap pass complete ═══\n"
            "\n"
            "shipped:    I3 (4/4), I4 (4/4 incl 2 stubs), I7 (2/2)\n"
            "            10 tasks, 14 commits, ~Nm wallclock.\n"
            "\n"
            "stubs-in-place: (will light up when you drop these env vars)\n"
            "  • I4 OPS2  → AXIOM_API_TOKEN  (logging stub: console + file)\n"
            "  • I7 CHN1  → POLLINATIONS_KEY (image gen stub: cached fixture)\n"
            "\n"
            "deferred-ops: (manual artifacts only — stubs already shipped, do these when ready)\n"
            "  • I12 DEMO2 — fund Amoy wallet, run `cd apps/chain/anchor-cli && npm run anchor`, paste back the tx hash.\n"
            "  • I3 DEMO1  — register the apex domain at Cloudflare (5 min), drop the token at .meshkore/credentials/cloudflare-token.json.\n"
            "\n"
            "decisions: (A001 / catalog made these on your behalf — audit/override if needed)\n"
            "  • I4 OPS2  catalog → Logpush + Axiom (default observability stack)\n"
            "  • I7 CHN1  A001    → Pollinations stable model (cost preference per memory)\n"
            "  • I9 WEB3  catalog → Solid Router v0.15 (cluster pin)\n"
            "\n"
            "spec-needs-clarification: (these can't ship without your input)\n"
            '  • I11 ROADMAP-EDITOR — spec says "real-time collaborative" but doesn\'t specify CRDT vs OT. One word answer unblocks it.\n'
            "\n"
            "Press Run all again when ready. The stubs survive; only the\n"
            "deferred-ops and spec-clarif items remain.\n"
            "═══\n"
            "```\n\n"
            "Then STOP. This is the ONLY voluntary halt.\n\n"
            "## AUTHORITY — act without asking\n\n"
            "- Read any file in the cluster.\n"
            "- Dispatch + cancel sub-agents.\n"
            "- Dispatch to `_onboarding_v1` with `[architect-consult]` prefix → A001 decides.\n"
            "- Mark a task `status: done`, `status: blocked`, `status: pending-operator` in frontmatter.\n"
            "- Set initiative `status: done` when 100% shipped (stubs count as shipped).\n"
            "- Apply DECISION CATALOG defaults silently.\n"
            "- Apply STUB-AND-FEATURE-FLAG to any external dependency.\n"
            "- Lightly edit a task body to add an `# architect: assumption` note or salvage a broken spec.\n"
            "- Append to the daily log yourself.\n"
            "- Pick the simpler reading, the lower id, the cluster default — these are your authority.\n\n"
            "## FORBIDDEN\n\n"
            "- Asking the operator anything (use catalog/stub/matrix/A001).\n"
            "- Inventing NEW initiatives or NEW tasks (salvaging existing = fine).\n"
            "- Running live deploys yourself (sub-agent `deploy` does it; if creds missing → STUB).\n"
            "- `git push`. Local commits only.\n"
            "- Touching `.meshkore/credentials/`, `.meshkore/.runtime/`, `state.json`.\n"
            "- Stubbing CORE business logic. Stubs are for external integrations only.\n"
            "- Stopping anywhere except the end-of-pass summary."
        ),
        "redirect": (
            "If the operator asks you to write code, edit a task body, "
            "or apply a fix directly, refuse politely: \"I'm the Roadmap "
            "Architect — I coordinate, I don't implement. I'll dispatch "
            'a sub-agent to do that and report back." Then dispatch.'
        ),
        "rules_addendum": "",
    },
    "review": {
        "label": "Review",
        "role": (
            "You are the **review** agent. You read recent changes (git "
            "diff, modified files, recent commits) and give code-review "
            "feedback — you don't apply changes."
        ),
        "focus": (
            "## Your focus\n\n"
            "- Comment on: correctness, security, complexity, test "
            "coverage, naming, missing edge cases.\n"
            "- Focus on what would block merge, not stylistic taste.\n"
            "- After a substantive review, log a summary to "
            "`.meshkore/log/<UTC-date>.md` (files reviewed, top findings, "
            "verdict).\n"
            "- If you want a change made, write it as a suggested diff "
            "the operator can hand to the General coder — don't apply it."
        ),
        "redirect": (
            "If asked to apply fixes, refactor, or do new work, answer: "
            "\"I'm the review agent — I comment, I don't merge. Hand "
            'this to the General coder." Then stop.'
        ),
        "rules_addendum": "",
    },
}


def _agent_type_normalised(t: Optional[str]) -> str:
    """Return a known agent_type, defaulting to 'custom' if missing/unknown."""
    if not t:
        return "custom"
    t = str(t).strip().lower()
    return t if t in AGENT_PROMPTS else "custom"


class BriefingPipeline:
    """Composes the prompt sent to `claude -p` for one agent turn.
    See module-level comment above for the section order + rationale."""

    SECTION_SEP = "\n\n---\n\n"

    def __init__(
        self,
        *,
        paths: "Paths",
        cluster: "Cluster",
        identity: str,
        conv: str,
        user_text: str,
        context_docs: Optional[List[Dict[str, Any]]] = None,
        agent_type: Optional[str] = None,
        agent_id: Optional[str] = None,
    ):
        self.paths = paths
        self.cluster = cluster
        self.identity = identity
        self.conv = conv
        self.user_text = user_text
        self.context_docs = context_docs or []
        # py-1.7.0 — agent_type drives role / focus / redirect / rules
        # selection from AGENT_PROMPTS. Defaults to 'custom' (General
        # coder) when missing/unknown so older cockpits and direct API
        # callers keep working.
        self.agent_type = _agent_type_normalised(agent_type)
        self.agent_id = (agent_id or "").strip() or None
        self.project = ProjectState(paths)
        self.integrity = StateIntegrityChecker(paths, cluster, self.project)
        # py-1.7.0 — cadence: detect whether this conv has had any prior
        # assistant turn. The full role+rules block is sent on the first
        # turn (so the agent gets the complete onboarding); on subsequent
        # turns we send a tight role reminder only, saving tokens and
        # keeping the conversation snappier.
        self.is_first_turn = self._detect_first_turn()

    def _detect_first_turn(self) -> bool:
        try:
            for f in _iter_timeline_files(self.paths):
                for ev in _read_timeline_file(f):
                    if ev.get("conv") != self.conv:
                        continue
                    if ev.get("type") in (
                        "chat.assistant.final",
                        "chat.assistant.delta",
                    ):
                        return False
            return True
        except Exception:
            return True

    def build(self) -> str:
        sections = [
            self._section_role(),
            self._section_core_rules(),
            self._section_agent_focus(),
            self._section_agent_redirect(),
            self._section_agent_memory(),
            self._section_cluster_snapshot(),
            self._section_project_mode(),
            self._section_integrity(),
            self._section_cockpit_context(),
            self._section_history(),
            # py-1.10.8 — only non-empty when user_text starts with
            # `[architect-consult]` on the `_onboarding_v1` conv. Forces
            # A001 to decide instead of bouncing the question back.
            self._section_consult_addendum(),
            self._section_user_turn(),
        ]
        return self.SECTION_SEP.join(s for s in sections if s and s.strip())

    # ── sections ──────────────────────────────────────────────────

    def _section_role(self) -> str:
        # py-1.7.0 — Role text is now driven by AGENT_PROMPTS so service
        # agents (deploy / db / testing / ...) get their own framing,
        # not a generic "coordinator" label.
        prompt = AGENT_PROMPTS.get(self.agent_type) or AGENT_PROMPTS["custom"]
        role = prompt["role"]
        # On subsequent turns, send a tight role reminder only.
        if not self.is_first_turn:
            return (
                f"## Role reminder\n\n{role}\n\n"
                f"Cluster root: `{self.paths.root}` · Identity: "
                f"`{self.identity}` · Conv: `{self.conv}`"
                + (f" · Agent: `{self.agent_id}`" if self.agent_id else "")
            )
        return (
            f"## Role\n\n{role}\n\n"
            f"Cluster root: `{self.paths.root}`\nIdentity: `{self.identity}`"
            f" · Conv: `{self.conv}`"
            + (f" · Agent: `{self.agent_id}`" if self.agent_id else "")
        )

    def _section_core_rules(self) -> str:
        try:
            port = int(self.paths.port_file.read_text().strip())
        except (OSError, ValueError):
            port = 5570

        # py-1.7.0 — Universal rules: every agent type sees these every
        # turn. Short, load-bearing. These are NOT role-specific.
        universal = [
            "## Universal rules (every agent, every turn)",
            "",
            "- Don't push to git unless the user explicitly asks.",
            f"- Don't invent version numbers; ask `POST localhost:{port}/version/next`.",
            "- Never edit `.meshkore/credentials/`, `.meshkore/.runtime/` or generated `state.json`.",
            "- The cockpit auto-refreshes ~2s after any write under `.meshkore/` — don't tell the user to reload.",
            "- Reply concisely. The portal renders your stdout as the chat answer.",
            "",
            "## MeshKore standard (where things live)",
            "",
            "- `.meshkore/` — everything the cluster knows lives here. The operator never edits it by hand; you do.",
            "- `.meshkore/modules/<id>/` — module-scoped work. Tasks live at `.meshkore/modules/<id>/tasks/*.md`.",
            "- `.meshkore/roadmap/initiatives/*.md` — initiatives (work-streams). Status: `active` / `next` / `backlog` / `done`.",
            "- `.meshkore/log/<UTC-date>.md` — daily activity log. **Append a 1-paragraph entry when you mutate ≥3 files in a turn**, single-file touch-ups don't need one.",
            "- `.meshkore/docs/coverage.md` — coverage matrix (requirement → which task delivers it).",
            "- `.meshkore/agents/_types/<agent-type>/memory.md` — your role's long-term memory (see below).",
            "",
            "## Daemon endpoints you should know",
            "",
            f"- `POST localhost:{port}/version/next` — get the next valid version for a key (never invent numbers).",
            f"- `POST localhost:{port}/log/append` (or just append to `.meshkore/log/<UTC-date>.md` directly) — operator activity log.",
            f"- `GET  localhost:{port}/state` — current cluster state (initiatives, tasks, modules, integrity flags).",
            f"- `POST localhost:{port}/chat/dispatch` — used by the cockpit; you receive your prompt via this path, you don't call it.",
            "",
            "## How to flag persistent learnings",
            "",
            "- When you discover something other agents of your role would want to know next time (a credential location, a flaky test pattern, a migration gotcha), end your reply with a line: `REMEMBER: <one short fact>`.",
            "- The daemon harvests `REMEMBER:` lines and appends them to your role's `memory.md`. Don't write to that file directly.",
            "",
            "Reference docs:",
            "  - https://meshkore.com/standard.json — canonical schemas",
            "  - https://meshkore.com/cluster/operate — operator manual",
            "  - `.meshkore/docs/context.md` — project-specific context (if present)",
            "  - `.meshkore/docs/conventions/*.md` — repo conventions",
        ]

        # General coder ('custom') additionally owns the roadmap, so it
        # gets the granularity rules. Service agents don't.
        if self.agent_type == "custom":
            general_coder_extras = [
                "",
                "## Module / task / initiative authority (General coder only)",
                "",
                "- When you create a new module directory `.meshkore/modules/<id>/`, ALSO add `{id: <id>, kind: area, name: '<Title>'}` to `cluster.yaml.modules[]`.",
                "- Every initiative you mark `active` or `next` must have ≥1 child task linked via `initiative: <id>` in the task's frontmatter. Use `status: backlog` for placeholders.",
                "",
                "### Task granularity",
                "",
                "- Target grain: **one task ≈ one week of focused work**.",
                "- If a candidate task would take > 2 weeks to deliver, split it (with `depends_on:` chains).",
                "- If a candidate task would take < 2 days, fold it into a sibling or the parent task's body.",
                "- Every task body MUST end with a `## Done when` section listing 2-5 concrete acceptance criteria the operator can verify without asking you.",
                "",
                "### Initiative granularity",
                "",
                '- Each initiative = **ONE coherent work-stream**, never a phase or release name. ✓ "Auth & identity", "Payments & credits". ✗ "MVP", "Phase 1", "Closed beta".',
                "- Target shape: **3-8 child tasks** in `active` / `next` status.",
                "- **Hard limit: never > 12 active/next tasks** per initiative. The integrity check (next turn's briefing) flags over-dense initiatives.",
                "- **Lower limit: ≥ 2 child tasks** for any active/next initiative. If only 1 task fits — fold, or drop the initiative back to `backlog`.",
                "- When SPLITTING an initiative: create the new files first, re-point each child task's `initiative:` frontmatter, then move the old file to `.meshkore/roadmap/initiatives/log/<old-id>.md` with `status: superseded` + `superseded_by:`.",
                "- An initiative's `## Done when` is the WORK-STREAM completion signal, verifiable independently.",
                "",
                "### Coverage matrix",
                "",
                "- When you create or modify any task / initiative, update `.meshkore/docs/coverage.md` to reflect it. Create the file if missing.",
            ]
            return "\n".join(universal + general_coder_extras)
        return "\n".join(universal)

    def _section_agent_focus(self) -> str:
        # py-1.7.0 — Service agents get their narrow focus block. The
        # General coder doesn't (it has no narrowing focus — its scope
        # is the whole cluster).
        prompt = AGENT_PROMPTS.get(self.agent_type) or AGENT_PROMPTS["custom"]
        focus = prompt.get("focus") or ""
        return focus.strip()

    def _section_agent_redirect(self) -> str:
        # py-1.7.0 — Out-of-scope policy for service agents. General
        # coder has nothing to redirect.
        prompt = AGENT_PROMPTS.get(self.agent_type) or AGENT_PROMPTS["custom"]
        redirect = prompt.get("redirect") or ""
        if not redirect.strip():
            return ""
        return "## Out-of-scope policy\n\n" + redirect.strip()

    def _section_agent_memory(self) -> str:
        # py-1.7.0 — Per-type long-term memory at
        # `.meshkore/agents/_types/<agent-type>/memory.md`. Populated by
        # the daemon when the agent ends a turn with `REMEMBER: …`
        # lines. Shared across all conversations of the same role.
        try:
            mem_path = self.paths.agents_dir / "_types" / self.agent_type / "memory.md"
            if not mem_path.exists():
                return ""
            txt = mem_path.read_text(errors="replace").strip()
            if not txt:
                return ""
            # Cap to ~4 KB so this section never dominates the briefing.
            if len(txt) > 4096:
                txt = txt[-4096:]
                # Trim to start of next line so we don't cut mid-entry.
                nl = txt.find("\n")
                if nl > 0:
                    txt = txt[nl + 1 :]
            return (
                f"## Your role's accumulated memory "
                f"(`agents/_types/{self.agent_type}/memory.md`)\n\n"
                f"{txt}\n\n"
                "These are facts past instances of your role have flagged "
                "as worth remembering. Use them; don't repeat them back."
            )
        except Exception:
            return ""

    def _section_cluster_snapshot(self) -> str:
        n_ini = len(self.project.initiative_files())
        n_tasks = len(self.project.task_files())
        declared_mods = self.cluster.data.get("modules") or []
        n_decl_mods = len(declared_mods) if isinstance(declared_mods, list) else 0
        n_dir_mods = len(self.project.module_dirs())
        bits = [
            f"- {n_ini} initiative(s) at `.meshkore/roadmap/initiatives/`",
            f"- {n_tasks} task(s) across modules (excluding the wizard's T1-hello boilerplate)",
            f"- {n_decl_mods} module(s) declared in `cluster.yaml.modules[]`",
        ]
        if n_dir_mods != n_decl_mods:
            bits.append(
                f"- {n_dir_mods} module directory(ies) on disk — mismatch with declared"
                " (see Integrity section below)"
            )
        return "## Cluster snapshot\n\n" + "\n".join(bits)

    def _section_project_mode(self) -> str:
        if not self.project.is_empty():
            return ""
        # py-1.4.3 — Scale the target task count by brief size. Briefs
        # in the kilobytes deserve more granular task decomposition than
        # "build a todo list" one-liners. Sources of brief size,
        # in order of preference: context.md (already written),
        # accumulated chat.user texts in this conv, the current
        # user_text. The number is heuristic, not enforced — the agent
        # picks a sensible point inside the range.
        brief_chars = self._estimate_brief_size()
        if brief_chars < 500:
            ini_range, task_range, breadth = "1-2", "3-8", "tiny"
        elif brief_chars < 2000:
            ini_range, task_range, breadth = "2-4", "8-15", "small"
        elif brief_chars < 5000:
            ini_range, task_range, breadth = "3-5", "15-25", "medium"
        elif brief_chars < 10000:
            ini_range, task_range, breadth = "3-6", "25-40", "large"
        else:
            ini_range, task_range, breadth = "4-8", "40-60", "comprehensive"
        return "\n".join(
            [
                "## Project mode: BOOTSTRAPPING (empty cluster)",
                "",
                f"The cluster at `{self.paths.root}` has 0 initiatives + 0 real",
                "tasks. Your purpose right now is to bootstrap the roadmap,",
                "not to interrogate the user until you have a perfect brief.",
                "",
                "**Write FIRST, talk SECOND.** As soon as the user has given",
                "you ANY substantive description of the project — its goal,",
                "audience, rough scope, any constraint — STOP asking",
                "clarifying questions and write:",
                "",
                f"### Brief size: ≈ {brief_chars} chars → {breadth} scope",
                "",
                f"  - **{ini_range} initiatives** at `.meshkore/roadmap/initiatives/<id>.md`",
                "    (frontmatter per `initiative` schema). Each initiative is a",
                '    **coherent work-stream**, named by what it builds: "Auth &',
                '    identity", "Canvas viewer", "Anchoring chain", "Payments",',
                '    "Observability". NEVER name initiatives by phase ("MVP",',
                '    "Phase 1", "Closed beta") — those collapse into one giant',
                "    catch-all card and break the roadmap UX. Target 3-8 child",
                "    tasks per initiative; hard limit 12. The next-turn integrity",
                "    check flags initiatives that exceed that.",
                f"  - **{task_range} initial tasks** distributed across modules",
                "    under `.meshkore/modules/<module>/tasks/<id>.md`. Bias",
                "    towards MORE tasks if the brief is long — every numbered",
                "    section, every rule, every explicit deliverable that's",
                "    in scope for Phase 1 should map to either a task or an",
                "    explicit `defer: <reason>` marker in coverage.md (see",
                "    below). Each task ≈ one week of focused work; split",
                "    tasks that exceed two weeks; fold tasks under two days.",
                "    Module directories MUST be declared in `cluster.yaml.modules[]`",
                "    on creation (otherwise the cockpit tree won't show them).",
                "  - A short `.meshkore/docs/context.md` capturing goal,",
                "    audience, constraints, and non-obvious decisions from the",
                "    brief. Frontmatter per `doc_frontmatter`.",
                "",
                "### Coverage matrix (mandatory deliverable)",
                "",
                "Write `.meshkore/docs/coverage.md` mapping EVERY numbered",
                "section, EVERY rule, and EVERY explicit deliverable in the",
                "user's brief to a task id OR a `defer: <reason>` marker.",
                "This is what makes the roadmap auditable — without it, gaps",
                "stay invisible until someone notices a feature wasn't built.",
                "Required shape (3 sections, in order):",
                "",
                "```markdown",
                "---",
                "title: Coverage matrix",
                "updated: YYYY-MM-DD",
                "owner: <you>",
                "---",
                "",
                "# Coverage matrix — `<cluster>`",
                "",
                "Maps every brief requirement to a task id or a deferral.",
                "Maintained on every roadmap-modifying turn.",
                "",
                "## Sections",
                "",
                "| Source | Requirement | Coverage |",
                "|---|---|---|",
                "| §4 Cosmology | Halo FIFO eviction | API7 |",
                "| §4 Cosmology | Oort decay state machine | WEB5 |",
                "| §6 Economic | Referral program | defer: Phase 4 (growth) |",
                "",
                "## Rules",
                "",
                "| # | Rule | Coverage |",
                "|---|---|---|",
                "| 1 | AI-only generation | AI1 |",
                "| 10 | Named zones | defer: Phase 5 (B2B) |",
                "",
                "## Explicit deliverables",
                "",
                "| Deliverable | Coverage |",
                "|---|---|",
                "| Architecture document | DOC1 |",
                "| Risk register | DOC2 |",
                "```",
                "",
                "Rules for the Coverage column:",
                "- Task id (e.g., `WEB2`) → that task addresses the requirement",
                "- `defer: <one-line reason>` → out of scope for current phase",
                "- `?` / `TBD` / empty → integrity check will flag it on the",
                "  next turn. Don't leave these in the final output.",
                "",
                "### Other rules for this bootstrap turn",
                "",
                "Mark assumptions with `> assumption: …` inside file bodies.",
                "Every task body ends with `## Done when` (2-5 acceptance",
                "criteria, observable, present tense).",
                "",
                "When done writing, reply with: (a) one short paragraph summary,",
                "(b) at MOST two open questions whose answers would materially",
                "change the plan. Do NOT paste file contents back — the",
                "cockpit auto-refreshes within ~2 seconds.",
                "",
                "If the user said almost nothing (literally 'hi', 'test',",
                "one-word), ask ONE focused question and stop. Never more.",
                "",
                "Once this turn lands files, the cluster is no longer empty",
                "and this section disappears from future briefings.",
            ]
        )

    def _estimate_brief_size(self) -> int:
        """Best-available signal for how big the project brief is.
        Drives the bootstrap task-count target. Sources, in order:
        (1) context.md if present, (2) accumulated chat.user texts in
        the current conv from .meshkore/timeline/, (3) the current
        user_text. Returns total chars."""
        # Source 1: context.md (already written on prior turns).
        ctx = self.paths.docs_dir / "context.md"
        if ctx.exists():
            try:
                size = len(ctx.read_text(errors="replace"))
                if size > 0:
                    return size
            except OSError:
                pass
        # Source 2: sum of all chat.user texts in this conv.
        total = 0
        try:
            if self.paths.timeline_dir.exists():
                for f in sorted(self.paths.timeline_dir.glob("*.jsonl")):
                    try:
                        for line in f.read_text(errors="replace").splitlines():
                            try:
                                ev = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            if (
                                ev.get("conv") == self.conv
                                and ev.get("type") == "chat.user"
                            ):
                                total += len(ev.get("text") or "")
                    except OSError:
                        continue
        except Exception:
            pass
        if total > 0:
            return total
        # Source 3: this turn's user_text.
        return len(self.user_text or "")

    def _section_integrity(self) -> str:
        violations = self.integrity.check()
        if not violations:
            return ""
        lines = [
            "## Integrity hints (please fix as part of this turn)",
            "",
            f"State-integrity check found {len(violations)} issue(s) you",
            "can resolve quickly. They are NOT blocking — proceed with the",
            "user's request first, then fix as you go.",
            "",
        ]
        for v in violations:
            kind = v.get("kind", "unknown")
            fix = v.get("fix", "(no fix suggested)")
            if kind == "module_not_declared":
                lines.append(f"- **Orphan module** `{v.get('module_id')}` — {fix}")
            elif kind == "task_initiative_broken":
                lines.append(
                    f"- **Broken initiative ref** task=`{v.get('task_id')}`"
                    f" → initiative=`{v.get('initiative_ref')}` — {fix}"
                )
            elif kind == "initiative_without_tasks":
                lines.append(
                    f"- **Initiative without tasks** `{v.get('initiative_id')}`"
                    f" (status: `{v.get('status')}`) — {fix}"
                )
            elif kind == "initiative_too_dense":
                lines.append(
                    f"- **Initiative too dense** `{v.get('initiative_id')}`"
                    f" carries {v.get('child_count')} active/next tasks"
                    f" — {fix}"
                )
            elif kind == "context_coverage_gap":
                toks = v.get("tokens") or []
                pretty = ", ".join(
                    f"`{t.get('token')}` ({t.get('mentions')}×)" for t in toks
                )
                lines.append(
                    f"- **Potential coverage gaps (tokens)** — {pretty} — {fix}"
                )
            elif kind == "coverage_doc_missing":
                lines.append(f"- **Coverage matrix missing** — {fix}")
            elif kind == "coverage_gaps_in_doc":
                n = v.get("count", "?")
                lines.append(f"- **Coverage matrix has {n} unresolved row(s)** — {fix}")
            else:
                lines.append(f"- **{kind}** — {fix}")
        return "\n".join(lines)

    def _section_cockpit_context(self) -> str:
        if not self.context_docs:
            return ""
        lines = [
            "## Context attached by the operator's cockpit",
            "",
            "The architect cockpit sent these documents alongside the",
            "user's message. Treat them as authoritative context for this",
            "turn (operator's intent, scope, recent UI state).",
            "",
        ]
        for doc in self.context_docs:
            if not isinstance(doc, dict):
                continue
            fname = doc.get("filename") or "(unnamed)"
            content = (doc.get("content") or "").strip()
            if not content:
                continue
            lines.append(f"### `{fname}`")
            lines.append("")
            lines.append(content)
            lines.append("")
        return "\n".join(lines).rstrip()

    def _section_history(self) -> str:
        turns = _conversation_history(self.paths, self.conv)
        if not turns:
            return ""
        return "## Recent turns in this conversation\n\n" + "\n".join(turns)

    def _section_consult_addendum(self) -> str:
        """py-1.10.8 — When the roadmap-architect dispatches a question
        to the onboarding/_onboarding_v1 conv with the [architect-consult]
        prefix, A001 (the project coordinator) must DECIDE on the
        operator's behalf, not bounce back. This addendum is injected
        only for that exact pattern."""
        body = (self.user_text or "").strip()
        if not body.startswith("[architect-consult]"):
            return ""
        if self.conv != "_onboarding_v1":
            return ""
        return (
            "## [architect-consult] mode — DECIDE, don't bounce\n\n"
            "The roadmap-architect is mid-pass and needs a decision YOU "
            "must make on the user's behalf. The user pressed Run all "
            "specifically so they would NOT be in the loop. Bouncing "
            "the question to them defeats the whole feature.\n\n"
            "Your authority for this turn:\n"
            "- You have full power to pick. The architect will execute "
            "whatever you say.\n"
            "- Read `.meshkore/agents/_types/custom/memory.md`, "
            "`.meshkore/roadmap/initiatives/*.md`, recent chat history, "
            "any README, the project vision — anything that surfaces "
            "the user's preferences. Pick the option most aligned.\n"
            "- When in doubt, prefer: the simpler option, the cheaper "
            "option, the option that keeps shipping velocity, the "
            "option that matches the cluster's existing tech defaults.\n\n"
            "Reply format — STRICT:\n"
            "- ONE paragraph, <80 words.\n"
            "- First sentence: the decision in plain language.\n"
            "- Second sentence: one-line rationale.\n"
            "- That's it. No preamble, no caveats, no \"happy to "
            'discuss", no follow-up question.\n\n'
            "If — and ONLY if — the question is genuinely about the "
            "PRODUCT IDEA (not implementation, not tech choice, not "
            "design defaults), reply with the literal string:\n"
            "    DEFER:<one-line reason what's conceptually unclear>\n"
            "The architect will defer that single task to the end of "
            "the pass and continue with the rest. Use this sparingly — "
            "it's the only escape valve and it shouldn't be your "
            "default."
        )

    def _section_user_turn(self) -> str:
        body = self.user_text.strip() if self.user_text else ""
        if not body:
            return "## User just said\n\n(empty message)"
        return "## User just said\n\n" + body


class ChatRunner:
    """One coordinator turn = one ChatRunner. Spawns `claude -p` with
    stream-json output, parses each line into chat.assistant.delta /
    tool.use / tool.result events on the WS, and emits a final
    `chat.assistant.final` when the child exits.

    Cancel-safe: cancel() sends SIGTERM to the process group; if still
    alive after 30 s, SIGKILL."""

    def __init__(
        self,
        *,
        paths: "Paths",
        cluster: "Cluster",
        hub: "Hub",
        identity: str,
        conv: str,
        prompt: str,
        context_docs: Optional[List[Dict[str, Any]]] = None,
        agent_type: Optional[str] = None,
        agent_id: Optional[str] = None,
    ):
        self.paths = paths
        self.cluster = cluster
        self.hub = hub
        self.identity = identity
        self.conv = conv
        self.prompt = prompt
        # py-1.4.0 — Carried into BriefingPipeline so the cockpit can
        # attach project-specific context (bootstrap brief, scope
        # hints, integrity check overrides, …) on a per-turn basis.
        self.context_docs: List[Dict[str, Any]] = context_docs or []
        # py-1.7.0 — agent_type drives specialised prompt selection,
        # agent_id is the human label (A001, A002, …) for logging.
        self.agent_type = _agent_type_normalised(agent_type)
        self.agent_id = (agent_id or "").strip() or None
        self.stream_id = f"s_{int(time.time() * 1000):x}_{secrets.token_hex(2)}"
        self.pid: Optional[int] = None
        self.proc: Any = None  # subprocess.Popen
        self.done = threading.Event()
        self.cancelled = False
        self._cumulative_text = ""

    def _briefing(self) -> str:
        # py-1.4.0 — the briefing is now composed by BriefingPipeline.
        # Each section (role, core rules, cluster snapshot, project
        # mode, integrity hints, cockpit context, history, user turn)
        # is independently maintained. See the class definition above
        # this file's HTTP handler block.
        return BriefingPipeline(
            paths=self.paths,
            cluster=self.cluster,
            identity=self.identity,
            conv=self.conv,
            user_text=self.prompt,
            context_docs=self.context_docs,
            agent_type=self.agent_type,
            agent_id=self.agent_id,
        ).build()

    def spawn(self) -> None:
        import subprocess

        claude_bin = _find_claude()
        if not claude_bin:
            err = "claude CLI not found — install via `npm i -g @anthropic-ai/claude-code`"
            _log(err)
            self.hub.broadcast(
                _append_timeline(
                    self.paths,
                    {
                        "type": "chat.assistant.final",
                        "author": self.identity,
                        "conv": self.conv,
                        "stream_id": self.stream_id,
                        "text": f"[runner error] {err}",
                    },
                )
            )
            self.done.set()
            return
        # py-1.6.1 HOTFIX — --session-id from py-1.6.0 caused empty
        # assistant responses in production (claude-code exited
        # silently on subsequent turns of the same conv). Reverted to
        # opt-in via env var MESHKORE_CLAUDE_SESSION_ID=1. Default off
        # until the failure mode is understood and re-tested.
        # The uuid5 helper is preserved so reintroduction is a one-line
        # flip once safe.
        session_id = _session_id_for_conv(self.conv)
        use_session = os.environ.get("MESHKORE_CLAUDE_SESSION_ID", "").strip() in (
            "1",
            "true",
            "yes",
            "on",
        )
        args = [
            claude_bin,
            "-p",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--permission-mode",
            "bypassPermissions",
            # Headless: cockpit has no UI to surface interactive question
            # tools. Disallow them so the model defaults to plain-text
            # asks in the chat bubble instead of stalling on a hanging
            # AskUserQuestion / ExitPlanMode call.
            "--disallowed-tools",
            "AskUserQuestion,ExitPlanMode",
        ]
        if use_session:
            args[2:2] = ["--session-id", session_id]
        # py-1.10.5 — Pipe the briefing through stdin instead of
        # appending it as a positional argument. claude 2.1.145
        # rejects a trailing positional that arrives AFTER a
        # multi-value flag (`--disallowed-tools <comma,list>`) — the
        # parser consumes our prompt as another disallowed-tool name
        # or just drops it, and claude exits 1 with stderr:
        #   "Error: Input must be provided either through stdin or
        #    as a prompt argument when using --print"
        # Captured 2026-05-29 by py-1.10.4's stderr drainer (which
        # had been silently dropping this error for every spawn
        # since the cockpit's roadmap-architect feature shipped).
        # Stdin works regardless of argv order, so it's the
        # forward-compatible answer.
        briefing = self._briefing()
        env = {
            **os.environ,
            "MESHKORE_IDENTITY": self.identity,
            "MESHKORE_CONV": self.conv,
            "MESHKORE_SESSION_ID": session_id,
        }
        self.proc = subprocess.Popen(
            args,
            cwd=str(self.paths.root),
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        self.pid = self.proc.pid
        # Write the briefing to stdin and close. claude reads it
        # all (EOF on close) then begins streaming results to stdout.
        try:
            if self.proc.stdin is not None:
                self.proc.stdin.write(briefing.encode("utf-8"))
                self.proc.stdin.close()
        except (BrokenPipeError, OSError) as e:
            _log(f"claude({self.conv}) stdin write failed: {e}")
        _log(
            f"claude({self.conv}) spawned pid={self.pid} agent_type={self.agent_type} "
            f"stream={self.stream_id} briefing_len={len(briefing)}"
        )
        self.hub.broadcast(
            {
                "type": "task.started",
                "id": f"chat:{self.conv}",
                "agent": self.identity,
                "ts": _iso_now(),
                "runner": "claude-code",
                "conv": self.conv,
                "stream_id": self.stream_id,
            }
        )
        # Empty assistant bubble so the cockpit shows progress immediately.
        self.hub.broadcast(
            {
                "type": "chat.assistant.delta",
                "author": self.identity,
                "conv": self.conv,
                "stream_id": self.stream_id,
                "text": "",
                "ts": _iso_now(),
            }
        )
        threading.Thread(target=self._reader_loop, daemon=True).start()
        # py-1.10.4 — stderr drainer. Until this lands, stderr=PIPE
        # was capturing claude's error output but NOBODY READ IT, so
        # every subprocess crash (prompt too long, blocked tool, env
        # issue, segfault) surfaced as "empty chat.assistant.final"
        # with no diagnostic anywhere in the daemon log. The reader
        # loop above only iterates stdout; PIPE'd stderr fills its
        # OS buffer (typically 64 KB) and on overflow Linux/Darwin
        # block claude on its next write — turning a soft failure
        # into an unkillable zombie. Drain it into the daemon log.
        threading.Thread(target=self._stderr_drain, daemon=True).start()

    def _stderr_drain(self) -> None:
        """Read self.proc.stderr line-by-line and forward to the
        daemon log. Tagged with conv so multiple in-flight runners
        don't blur together. Cheap — claude rarely emits much on
        stderr unless it's failing."""
        if not self.proc or not self.proc.stderr:
            return
        for raw in self.proc.stderr:
            try:
                line = raw.decode("utf-8", "replace").rstrip()
            except Exception:
                continue
            if line:
                _log(f"claude({self.conv}) stderr: {line}")

    def cancel(self) -> None:
        if self.cancelled:
            return
        self.cancelled = True
        if self.proc and self.proc.poll() is None:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
            except (OSError, ProcessLookupError):
                pass

            def _hard_kill():
                if self.proc and self.proc.poll() is None:
                    try:
                        os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                    except (OSError, ProcessLookupError):
                        pass

            threading.Timer(30.0, _hard_kill).start()

    def _reader_loop(self) -> None:
        assert self.proc and self.proc.stdout
        last_emit_at = 0.0
        result_text = ""
        for raw in self.proc.stdout:
            try:
                line = raw.decode("utf-8", "replace").strip()
            except Exception:
                continue
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(ev, dict):
                continue
            ev_type = ev.get("type")
            if ev_type == "stream_event":
                inner = ev.get("event") or {}
                if (
                    inner.get("type") == "content_block_delta"
                    and (inner.get("delta") or {}).get("type") == "text_delta"
                ):
                    delta = (inner.get("delta") or {}).get("text") or ""
                    if delta:
                        self._cumulative_text += delta
                        now = time.monotonic()
                        if now - last_emit_at > 0.2:
                            last_emit_at = now
                            self.hub.broadcast(
                                {
                                    "type": "chat.assistant.delta",
                                    "author": self.identity,
                                    "conv": self.conv,
                                    "stream_id": self.stream_id,
                                    "text": self._cumulative_text[:16000],
                                    "ts": _iso_now(),
                                }
                            )
                elif (
                    inner.get("type") == "content_block_start"
                    and (inner.get("content_block") or {}).get("type") == "tool_use"
                ):
                    cb = inner.get("content_block") or {}
                    # py-1.5.0 — Persist tool.use to timeline so the
                    # cockpit can replay full turn detail after a reload
                    # or a daemon restart. Previously broadcast-only,
                    # which made historical turns auditable only via
                    # git log of the files the agent touched.
                    self.hub.broadcast(
                        _append_timeline(
                            self.paths,
                            {
                                "type": "tool.use",
                                "author": self.identity,
                                "conv": self.conv,
                                "stream_id": self.stream_id,
                                "tool": cb.get("name"),
                                "input": cb.get("input"),
                            },
                        )
                    )
                continue
            if ev_type == "user":
                for c in (ev.get("message") or {}).get("content") or []:
                    if isinstance(c, dict) and c.get("type") == "tool_result":
                        # py-1.5.0 — Persist tool.result too (was
                        # broadcast-only). Pair-matched to a tool.use
                        # via stream_id in the cockpit.
                        self.hub.broadcast(
                            _append_timeline(
                                self.paths,
                                {
                                    "type": "tool.result",
                                    "author": self.identity,
                                    "conv": self.conv,
                                    "stream_id": self.stream_id,
                                    "ok": not c.get("is_error"),
                                },
                            )
                        )
                continue
            if ev_type == "result" and isinstance(ev.get("result"), str):
                result_text = ev["result"]
        # Finalize
        final_text = result_text or self._cumulative_text
        # py-1.7.0 — Harvest REMEMBER: lines into the role's shared
        # memory. Anything the agent flags ("REMEMBER: credentials live
        # at …") gets appended once, deduplicated. Lines are also
        # stripped from the final response shown in the chat so they
        # don't clutter the UI.
        cleaned_text, harvested = self._harvest_remember_lines(final_text)
        if harvested:
            try:
                self._append_role_memory(harvested)
            except Exception as e:
                _log(f"role memory append failed: {e}")
        self.hub.broadcast(
            _append_timeline(
                self.paths,
                {
                    "type": "chat.assistant.final",
                    "author": self.identity,
                    "conv": self.conv,
                    "stream_id": self.stream_id,
                    "text": cleaned_text,
                },
            )
        )
        # py-1.10.4 — surface the exit code in the daemon log so a
        # silent claude failure (empty stdout, no final, etc.) can
        # be traced back to e.g. "exited 1 with stderr 'context
        # length exceeded'". Without this line, every empty-final
        # looked identical regardless of whether claude crashed,
        # blocked on a tool, or genuinely had nothing to say.
        exit_code = self.proc.wait() if self.proc else None
        text_len = len(cleaned_text or "")
        _log(
            f"claude({self.conv}) exit={exit_code} stream={self.stream_id} "
            f"text_len={text_len} agent_type={self.agent_type}"
        )
        self.hub.broadcast(
            {
                "type": "task.finished",
                "id": f"chat:{self.conv}",
                "ts": _iso_now(),
                "exit": exit_code,
                "conv": self.conv,
            }
        )
        self.done.set()

    def _harvest_remember_lines(self, text: str) -> Tuple[str, List[str]]:
        """Extract any `REMEMBER: …` lines from `text` and return
        (cleaned text, list of remembered facts). Case-insensitive on
        the marker; one fact per line."""
        if not text:
            return text, []
        kept: List[str] = []
        harvested: List[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            low = stripped.lower()
            # Allow "REMEMBER: ...", "- REMEMBER: ...", "* REMEMBER: ..."
            for prefix in ("remember:", "- remember:", "* remember:"):
                if low.startswith(prefix):
                    fact = stripped[len(prefix) :].strip()
                    # When the prefix had a list bullet, strip the bullet.
                    if prefix.startswith(("-", "*")):
                        fact = fact.lstrip()
                    if fact:
                        harvested.append(fact)
                    break
            else:
                kept.append(line)
                continue
        cleaned = "\n".join(kept).rstrip()
        return cleaned, harvested

    def _append_role_memory(self, facts: List[str]) -> None:
        """Append facts to `.meshkore/agents/_types/<agent-type>/memory.md`,
        deduplicating against what's already in the file. Each entry
        prefixed with its UTC date so memory has provenance."""
        if not facts:
            return
        from datetime import datetime as _dt

        today = _dt.utcnow().strftime("%Y-%m-%d")
        d = self.paths.agents_dir / "_types" / self.agent_type
        d.mkdir(parents=True, exist_ok=True)
        path = d / "memory.md"
        existing = ""
        try:
            existing = path.read_text(errors="replace") if path.exists() else ""
        except OSError:
            existing = ""
        existing_lc = existing.lower()
        new_blocks: List[str] = []
        for fact in facts:
            if fact.lower() in existing_lc:
                continue
            new_blocks.append(f"- {today} · {fact}")
        if not new_blocks:
            return
        header = ""
        if not existing.strip():
            header = (
                f"# `{self.agent_type}` role memory\n\n"
                f"Long-lived facts captured by past instances of this role "
                f"via `REMEMBER: …` lines. Append-only.\n\n"
            )
        addition = (
            ("\n" if existing and not existing.endswith("\n") else "")
            + "\n".join(new_blocks)
            + "\n"
        )
        with path.open("a", encoding="utf-8") as fh:
            if header:
                fh.write(header)
            fh.write(addition)


# ───────────────────────────────────────────────────────────────────────
# ChatArchive (py-1.5.0)
#
# Until v1.5.0 the cockpit kept the "is this conversation archived?" bit
# in localStorage. That meant a different browser (same operator, same
# project) saw all previously-archived convs as live again. The archive
# state is now daemon-side, persisted to `.meshkore/.runtime/archives.json`,
# so it's a single source of truth across cockpit instances on the same
# machine. The cockpit syncs from `/chat/archives` on boot and POSTs to
# `/chat/archive` / `/chat/unarchive` on toggle.
#
# Schema:
#   {
#     "version": 1,
#     "archived": {
#       "<conv-id>": {"archived_at": "<iso>", "by": "<author>"}
#     }
#   }


class ChatArchive:
    """Persistent registry of archived conv ids.
    Read on boot; mutated via /chat/archive + /chat/unarchive endpoints."""

    SCHEMA_VERSION = 1

    def __init__(self, paths: "Paths") -> None:
        self.paths = paths
        self._path = paths.runtime / "archives.json"
        self._lock = threading.Lock()
        self._data: Dict[str, Any] = {"version": self.SCHEMA_VERSION, "archived": {}}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text())
            if isinstance(raw, dict) and isinstance(raw.get("archived"), dict):
                self._data = raw
                self._data.setdefault("version", self.SCHEMA_VERSION)
        except Exception:
            # corrupted file → keep defaults; never crash the daemon
            pass

    def _save(self) -> None:
        # Atomic write — render to a temp file in the same dir, fsync,
        # then rename. Survives a daemon crash mid-write.
        self.paths.runtime.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".json.tmp")
        payload = json.dumps(self._data, indent=2).encode("utf-8")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            os.write(fd, payload)
            try:
                os.fsync(fd)
            except OSError:
                pass
        finally:
            os.close(fd)
        os.replace(tmp, self._path)

    def list(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {"conv": c, **meta}
                for c, meta in sorted(self._data["archived"].items())
            ]

    def is_archived(self, conv: str) -> bool:
        with self._lock:
            return conv in self._data["archived"]

    def archive(self, conv: str, by: str = "") -> Dict[str, Any]:
        with self._lock:
            entry = {"archived_at": _iso_now(), "by": by or "operator"}
            self._data["archived"][conv] = entry
            self._save()
            return {"conv": conv, **entry}

    def unarchive(self, conv: str) -> bool:
        with self._lock:
            if conv in self._data["archived"]:
                del self._data["archived"][conv]
                self._save()
                return True
            return False


# ───────────────────────────────────────────────────────────────────────
# TimelineRotator (py-1.5.0)
#
# Compresses old jsonl files into .jsonl.gz to keep .meshkore/timeline/
# from growing unbounded over months / years. Files older than
# TIMELINE_ROTATE_AGE_DAYS get gzipped in place (or moved to an archive/
# subdir if configured). Cheap: runs in a background thread on a long
# cadence, only touches files modified before the threshold, never
# touches today's or yesterday's file.
#
# Readers (`_iter_timeline_files` + `_read_timeline_file`) already handle
# .gz transparently, so the cockpit and the agent's history block are
# unaffected by rotation.


TIMELINE_ROTATE_AGE_DAYS = 90
TIMELINE_ROTATE_SCAN_SEC = 3600.0  # once per hour


class TimelineRotator:
    """Background gzipper for old jsonl files in .meshkore/timeline/."""

    def __init__(self, paths: "Paths", age_days: int = TIMELINE_ROTATE_AGE_DAYS):
        self.paths = paths
        self.age_days = age_days
        self._stop = threading.Event()
        threading.Thread(target=self._loop, daemon=True).start()

    def shutdown(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        # Run once at boot (after a brief delay so we don't fight with
        # the cluster's first state.json rebuild), then every hour.
        if self._stop.wait(60.0):
            return
        while True:
            try:
                self.rotate_once()
            except Exception as e:
                _log(f"timeline rotator: {e}")
            if self._stop.wait(TIMELINE_ROTATE_SCAN_SEC):
                return

    def rotate_once(self) -> int:
        if not self.paths.timeline_dir.exists():
            return 0
        cutoff = time.time() - (self.age_days * 86400)
        archive_dir = self.paths.timeline_dir / "archive"
        rotated = 0
        for f in self.paths.timeline_dir.glob("*.jsonl"):
            try:
                st = f.stat()
            except OSError:
                continue
            if st.st_mtime > cutoff:
                continue  # too recent
            # Compress in place, move the .gz to archive/, delete the
            # original. Keep one log line per rotation so the operator
            # can audit it from the daemon's stderr.
            try:
                archive_dir.mkdir(parents=True, exist_ok=True)
                import gzip

                gz_path = archive_dir / (f.name + ".gz")
                if gz_path.exists():
                    # Already rotated — just delete the source.
                    f.unlink()
                    rotated += 1
                    continue
                with open(f, "rb") as src, gzip.open(gz_path, "wb") as dst:
                    while True:
                        chunk = src.read(64 * 1024)
                        if not chunk:
                            break
                        dst.write(chunk)
                f.unlink()
                _log(f"timeline rotator: {f.name} → archive/{gz_path.name}")
                rotated += 1
            except OSError as e:
                _log(f"timeline rotator: skipped {f.name}: {e}")
        return rotated


class RunStore:
    """Persistent registry of story runs (py-1.10.0).

    A "run" is the daemon-side first-class representation of "the
    operator clicked play on initiative X". Each run pins one conv +
    one agent_id + the ordered list of task ids it has to step
    through. Status moves running → cancelled|done|failed; the
    cursor advances per step.

    Storage: `.meshkore/.runtime/runs.json` (atomic tmp+rename).
    Why .runtime: it's per-machine and gitignored — runs are a
    coordinator artifact, not a roadmap artifact.

    Why this exists: the previous (V87) design lived in the cockpit's
    localStorage and the daemon had no concept of a "run". Symptom
    after reload: storyStore.run resurrected as paused and the UI
    treated it as active even though the daemon had finished/idled.
    With the run server-side, GET /runs returns ground truth + the
    `live` flag (= chat_sessions.has(conv)) so the cockpit always
    paints the real state.

    Cancellation propagation: chat_cancel(conv) calls
    `find_by_conv(conv)` and if a run owns the conv with status
    running/stopping, marks it cancelled + broadcasts run.cancelled.
    So either entry point — initiative card's ■ stop OR the chat
    panel's StopBar — converges to the same state.
    """

    STATUS_RUNNING = "running"
    STATUS_STOPPING = "stopping"
    STATUS_CANCELLED = "cancelled"
    STATUS_DONE = "done"
    STATUS_FAILED = "failed"

    ACTIVE_STATUSES = frozenset({STATUS_RUNNING, STATUS_STOPPING})

    def __init__(self, paths: "Paths", hub: "Hub"):
        self.paths = paths
        self.hub = hub
        self._lock = threading.Lock()
        # Schema: {"version": 1, "runs": [<run dict>, ...]}
        self._data: Dict[str, Any] = {"version": 1, "runs": []}
        self._load()

    # ── persistence ────────────────────────────────────────────────
    def _runs_path(self) -> Path:
        return self.paths.runtime / "runs.json"

    def _load(self) -> None:
        fp = self._runs_path()
        if not fp.exists():
            return
        try:
            data = json.loads(fp.read_text())
            if not isinstance(data, dict):
                return
            runs = data.get("runs")
            if isinstance(runs, list):
                # Filter shape-broken entries silently — better than crash.
                clean = [r for r in runs if isinstance(r, dict) and r.get("id")]
                self._data = {"version": 1, "runs": clean}
        except (OSError, ValueError) as e:
            _log(f"runs.json load failed: {e}")

    def _save(self) -> None:
        """Atomic write — tmp then rename — so partial writes don't
        corrupt the file. Called inside the lock."""
        fp = self._runs_path()
        fp.parent.mkdir(parents=True, exist_ok=True)
        tmp = fp.with_suffix(".json.tmp")
        try:
            tmp.write_text(json.dumps(self._data, indent=2, sort_keys=True))
            os.replace(tmp, fp)
        except OSError as e:
            _log(f"runs.json save failed: {e}")

    # ── mutations ──────────────────────────────────────────────────
    def create(
        self,
        *,
        initiative_id: str,
        initiative_title: str,
        conv: str,
        agent_id: str,
        agent_title: str,
        task_ids: List[str],
    ) -> Dict[str, Any]:
        run = {
            "id": f"run_{uuid.uuid4().hex[:12]}",
            "initiative_id": initiative_id,
            "initiative_title": initiative_title,
            "conv": conv,
            "agent_id": agent_id,
            "agent_title": agent_title,
            "task_ids": list(task_ids),
            "cursor": 0,
            "status": self.STATUS_RUNNING,
            "started_at": _iso_now(),
            "last_step_at": _iso_now(),
            "ended_at": None,
            "stream_id": None,
            "error": None,
        }
        with self._lock:
            self._data["runs"].append(run)
            self._save()
        self.hub.broadcast({"type": "run.started", "run": run})
        return run

    def cancel(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Mark cancelled. Returns the updated run, or None if unknown.
        Idempotent: cancelling an already-final run is a no-op."""
        with self._lock:
            run = self._find_locked(run_id)
            if not run:
                return None
            if run["status"] not in self.ACTIVE_STATUSES:
                return run
            run["status"] = self.STATUS_CANCELLED
            run["ended_at"] = _iso_now()
            self._save()
        self.hub.broadcast({"type": "run.cancelled", "run": run})
        return run

    def advance(
        self, run_id: str, cursor: int, stream_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        with self._lock:
            run = self._find_locked(run_id)
            if not run:
                return None
            if run["status"] not in self.ACTIVE_STATUSES:
                return run
            total = len(run["task_ids"])
            run["cursor"] = max(0, min(cursor, total))
            run["last_step_at"] = _iso_now()
            if stream_id is not None:
                run["stream_id"] = stream_id
            # Auto-finalise if cursor walked off the end.
            if run["cursor"] >= total:
                run["status"] = self.STATUS_DONE
                run["ended_at"] = _iso_now()
            self._save()
        ev_type = "run.done" if run["status"] == self.STATUS_DONE else "run.advanced"
        self.hub.broadcast({"type": ev_type, "run": run})
        return run

    def finish(
        self, run_id: str, status: str, error: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        if status not in (self.STATUS_DONE, self.STATUS_FAILED):
            return None
        with self._lock:
            run = self._find_locked(run_id)
            if not run:
                return None
            if run["status"] not in self.ACTIVE_STATUSES:
                return run
            run["status"] = status
            run["ended_at"] = _iso_now()
            if error is not None:
                run["error"] = str(error)
            self._save()
        ev_type = "run.done" if status == self.STATUS_DONE else "run.failed"
        self.hub.broadcast({"type": ev_type, "run": run})
        return run

    def set_stream(self, run_id: str, stream_id: str) -> Optional[Dict[str, Any]]:
        """Cockpit calls this after each /chat/dispatch so the run
        record carries the in-flight stream_id (debuggable trail)."""
        with self._lock:
            run = self._find_locked(run_id)
            if not run:
                return None
            run["stream_id"] = stream_id
            run["last_step_at"] = _iso_now()
            self._save()
        return run

    # ── reads ──────────────────────────────────────────────────────
    def _find_locked(self, run_id: str) -> Optional[Dict[str, Any]]:
        for r in self._data["runs"]:
            if r.get("id") == run_id:
                return r
        return None

    def get(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            r = self._find_locked(run_id)
            return dict(r) if r else None

    def find_by_conv(self, conv: str) -> Optional[Dict[str, Any]]:
        """Return the newest ACTIVE run bound to this conv, or None.
        Used by chat_cancel to propagate cancellation."""
        with self._lock:
            best: Optional[Dict[str, Any]] = None
            for r in self._data["runs"]:
                if r.get("conv") != conv:
                    continue
                if r.get("status") not in self.ACTIVE_STATUSES:
                    continue
                if best is None or (r.get("started_at") or "") > (
                    best.get("started_at") or ""
                ):
                    best = r
            return dict(best) if best else None

    def list_all(
        self, active_only: bool = False, limit: int = 200
    ) -> List[Dict[str, Any]]:
        with self._lock:
            out = list(self._data["runs"])
        # Newest first.
        out.sort(key=lambda r: r.get("started_at") or "", reverse=True)
        if active_only:
            out = [r for r in out if r.get("status") in self.ACTIVE_STATUSES]
        return out[:limit]


class ChatSessions:
    """conv → active runner + pending buffer. Same mid-turn-merge
    protocol as Node's chatSessions: a second prompt while running
    gets concatenated and runs as the next turn automatically."""

    def __init__(self) -> None:
        self._s: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def has(self, conv: str) -> bool:
        with self._lock:
            return conv in self._s

    def list_active(self) -> List[str]:
        """All conv ids with a turn in flight. Used by /self-update to
        refuse mid-stream so claude-code processes aren't orphaned."""
        with self._lock:
            return list(self._s.keys())

    def queue(self, conv: str, text: str) -> int:
        with self._lock:
            sess = self._s.get(conv)
            if not sess:
                return 0
            sess["pending"].append(text)
            return len(sess["pending"])

    def start(self, conv: str, runner: ChatRunner, on_chain) -> None:
        with self._lock:
            self._s[conv] = {"runner": runner, "pending": [], "cancelled": False}

        def _wait():
            runner.done.wait()
            with self._lock:
                sess = self._s.get(conv)
                if not sess:
                    return
                if sess["cancelled"] or not sess["pending"]:
                    self._s.pop(conv, None)
                    return
                pending = sess["pending"]
                sess["pending"] = []
            merged = (
                pending[0]
                if len(pending) == 1
                else "Several follow-up instructions while you were working:\n\n"
                + "\n\n".join(f"{i + 1}. {t}" for i, t in enumerate(pending))
            )
            on_chain(conv, merged)

        threading.Thread(target=_wait, daemon=True).start()

    def cancel(self, conv: str) -> Tuple[bool, int]:
        with self._lock:
            sess = self._s.pop(conv, None)
        if not sess:
            return False, 0
        sess["cancelled"] = True
        dropped = len(sess["pending"])
        try:
            sess["runner"].cancel()
        except Exception:
            pass
        return True, dropped


# ───────────────────────────────────────────────────────────────────────
# Cron scheduler (D-CRON-02..05)
#
# Replaces every external scheduler (LaunchAgent, cron-tab, GH Actions
# cron). The Python daemon ticks every 10 s, decides which jobs are
# due based on `cluster.yaml.crons:` (validated by Cluster.reload —
# see D-CRON-01), and spawns a subprocess per due job via CronRunner.
# Only the daemon whose `device_id` matches `cluster.crons_owner`
# actually fires; peers emit `cron.would_have_fired` events.


def _parse_cron_field(field: str, lo: int, hi: int) -> set:
    """Parse one POSIX cron field (minute / hour / dom / month / dow)
    into the set of integers it matches. Supports: '*', 'A', 'A-B',
    'A,B,C', '*/N', 'A-B/N'. No L/W/# modifiers, no aliases."""
    out = set()
    for part in field.split(","):
        step = 1
        if "/" in part:
            base, step_s = part.split("/", 1)
            step = int(step_s)
        else:
            base = part
        if base == "*":
            start, end = lo, hi
        elif "-" in base:
            a, b = base.split("-", 1)
            start, end = int(a), int(b)
        else:
            n = int(base)
            start, end = n, n
        for v in range(start, end + 1, step):
            if lo <= v <= hi:
                out.add(v)
    return out


def _cron_next(expr: str, after: datetime) -> datetime:
    """Compute the next datetime > `after` that matches the 5-field
    POSIX cron expression. Walks forward minute-by-minute (bounded to
    ~4 years so a misconfigured expr fails loudly rather than spinning
    forever)."""
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"bad cron expression (need 5 fields): {expr!r}")
    minute_set = _parse_cron_field(parts[0], 0, 59)
    hour_set = _parse_cron_field(parts[1], 0, 23)
    dom_set = _parse_cron_field(parts[2], 1, 31)
    month_set = _parse_cron_field(parts[3], 1, 12)
    # Cron dow: Sunday=0..Saturday=6. Python's weekday(): Monday=0..Sunday=6.
    # Convert at match time with (py + 1) % 7.
    dow_set = _parse_cron_field(parts[4], 0, 6)
    t = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(60 * 24 * 366 * 4):
        if (
            t.minute in minute_set
            and t.hour in hour_set
            and t.month in month_set
            and t.day in dom_set
            and ((t.weekday() + 1) % 7) in dow_set
        ):
            return t
        t += timedelta(minutes=1)
    raise ValueError(f"no next match within 4 years for {expr!r}")


def _curated_path_entries() -> List[str]:
    """PATH entries we prepend to every cron child's env, so the cron
    can find `wrangler`, `flyctl`, `claude`, `node`, etc. regardless of
    how the daemon itself was launched. Solves the 2026-05-19 incident
    where the LaunchAgent's PATH didn't include nvm."""
    import glob as _glob

    out: List[str] = []
    candidates = [
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
    ]
    # Highest nvm Node version
    nvm = sorted(
        _glob.glob(os.path.expanduser("~/.nvm/versions/node/v*/bin")), reverse=True
    )
    if nvm:
        candidates.insert(0, nvm[0])
    for p in candidates:
        if os.path.isdir(p) and p not in out:
            out.append(p)
    return out


class CronRunner:
    """Spawns one subprocess per due job. Captures stdout+stderr to a
    per-run log file under `.meshkore/.runtime/logs/cron/<job_id>/<ts>.log`.
    Enforces `max_runtime_sec` with SIGTERM → 30 s → SIGKILL on the
    process group (so children of the spawned shell die too)."""

    def __init__(self, paths: Paths, cluster: Cluster, hub: Hub, identity: str):
        self.paths = paths
        self.cluster = cluster
        self.hub = hub
        self.identity = identity
        self.paths.crons_logs_dir.mkdir(parents=True, exist_ok=True)
        self._active: Dict[str, Any] = {}  # job_id → subprocess.Popen
        self._lock = threading.Lock()

    def is_running(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._active

    def spawn(
        self, job: Dict[str, Any], reason: str = "scheduled"
    ) -> Optional[Dict[str, Any]]:
        """Fire one run of `job`. Returns the started Run dict, or
        None if the job is already running (no concurrent fires)."""
        import subprocess

        jid = job["id"]
        with self._lock:
            if jid in self._active:
                self.hub.broadcast(
                    {
                        "type": "cron.skipped",
                        "id": jid,
                        "reason": "already running",
                        "ts": _iso_now(),
                    }
                )
                return None
        env = self._resolve_env(job.get("env") or {})
        log_path = self._make_log_path(jid)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        ts = _iso_now()
        try:
            log_handle = open(log_path, "ab")
            proc = subprocess.Popen(
                job["cmd"],
                shell=True,
                cwd=str(self.paths.root),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except Exception as e:
            _log(f"cron spawn FAIL {jid}: {e}")
            self.hub.broadcast(
                {
                    "type": "cron.error",
                    "id": jid,
                    "error": str(e),
                    "ts": ts,
                }
            )
            return None
        with self._lock:
            self._active[jid] = proc
        self.hub.broadcast(
            {
                "type": "cron.fired",
                "id": jid,
                "reason": reason,
                "pid": proc.pid,
                "log": str(log_path.relative_to(self.paths.root)),
                "ts": ts,
            }
        )
        run = {
            "id": jid,
            "started_at": ts,
            "pid": proc.pid,
            "log_path": str(log_path),
            "status": "running",
        }
        threading.Thread(
            target=self._wait_for,
            args=(jid, proc, log_handle, job, log_path, ts),
            daemon=True,
        ).start()
        return run

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            proc = self._active.get(job_id)
        if not proc or proc.poll() is not None:
            return False
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            threading.Timer(30.0, lambda: self._sigkill(job_id)).start()
            return True
        except (OSError, ProcessLookupError):
            return False

    def _sigkill(self, job_id: str) -> None:
        with self._lock:
            proc = self._active.get(job_id)
        if not proc or proc.poll() is not None:
            return
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (OSError, ProcessLookupError):
            pass

    def _wait_for(
        self,
        jid: str,
        proc,
        log_handle,
        job: Dict[str, Any],
        log_path: Path,
        started_at: str,
    ) -> None:
        timeout = int(job.get("max_runtime_sec", 7200))
        t0 = time.monotonic()
        while proc.poll() is None and (time.monotonic() - t0) < timeout:
            time.sleep(1)
        timed_out = proc.poll() is None
        if timed_out:
            self.hub.broadcast({"type": "cron.timeout", "id": jid, "ts": _iso_now()})
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                time.sleep(30)
                if proc.poll() is None:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass
        exit_code = proc.wait()
        try:
            log_handle.close()
        except Exception:
            pass
        with self._lock:
            self._active.pop(jid, None)
        status = "timeout" if timed_out else ("ok" if exit_code == 0 else "failed")
        self.hub.broadcast(
            {
                "type": "cron.finished",
                "id": jid,
                "exit": exit_code,
                "status": status,
                "duration_sec": round(time.monotonic() - t0, 1),
                "log": str(log_path.relative_to(self.paths.root)),
                "ts": _iso_now(),
            }
        )

    def _resolve_env(self, job_env: Dict[str, str]) -> Dict[str, str]:
        env = dict(os.environ)
        curated = _curated_path_entries()
        if curated:
            env["PATH"] = ":".join(curated) + ":" + env.get("PATH", "")
        for k, v in job_env.items():
            if not isinstance(v, str) or not isinstance(k, str):
                continue
            if v.startswith("file:"):
                rel = v[len("file:") :]
                full = Path(rel) if os.path.isabs(rel) else (self.paths.root / rel)
                try:
                    env[k] = full.read_text().strip()
                except OSError as e:
                    _log(f"cron env: cannot read {full}: {e}")
            elif v.startswith("$"):
                env[k] = os.environ.get(v[1:], v)
            else:
                env[k] = os.path.expandvars(os.path.expanduser(v))
        return env

    def _make_log_path(self, job_id: str) -> Path:
        d = self.paths.crons_logs_dir / job_id
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return d / f"{ts}.log"


class CronScheduler:
    """Tick loop. Every TICK_SEC seconds: check each registered job,
    fire any whose `next_run` has arrived (only if this daemon is the
    coordinator), advance `next_run` to the next future slot."""

    TICK_SEC = 10  # operator decision 2026-05-19

    def __init__(self, paths: Paths, cluster: Cluster, hub: Hub, identity: str):
        self.paths = paths
        self.cluster = cluster
        self.hub = hub
        self.identity = identity
        self.runner = CronRunner(paths, cluster, hub, identity)
        self._jobs: Dict[str, Dict[str, Any]] = {}  # job_id → {job, next_run}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._timer: Optional[threading.Timer] = None

    # ── coordinator gate ─────────────────────────────────────────────
    def is_coordinator(self) -> bool:
        owner = self.cluster.crons_owner
        # If no owner is declared but crons exist, the first daemon to
        # boot owns them — pragmatic default for single-machine setups.
        if not owner:
            return bool(self.cluster.crons)
        return owner == self.identity

    # ── load/reload ─────────────────────────────────────────────────
    def reload_jobs(self) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            self._jobs = {}
            for job in self.cluster.crons:
                try:
                    next_run = _cron_next(job["schedule"], now)
                except ValueError as e:
                    _log(f"cron {job['id']}: cannot compute next_run: {e}")
                    continue
                self._jobs[job["id"]] = {"job": job, "next_run": next_run}

    # ── lifecycle ───────────────────────────────────────────────────
    def start(self) -> None:
        self.reload_jobs()
        n = len(self._jobs)
        if n == 0:
            _log("cron: no jobs registered (cluster.yaml has no `crons:` block)")
        else:
            owner_status = (
                "coordinator"
                if self.is_coordinator()
                else f"peer (owner={self.cluster.crons_owner})"
            )
            _log(
                f"cron: {n} job(s) registered, this daemon is {owner_status}, tick every {self.TICK_SEC}s"
            )
            for jid, state in self._jobs.items():
                _log(f"  - {jid}: next_run={state['next_run'].isoformat()}")
        self._schedule_next_tick()

    def stop(self) -> None:
        self._stop.set()
        if self._timer:
            self._timer.cancel()

    def _schedule_next_tick(self) -> None:
        if self._stop.is_set():
            return
        self._timer = threading.Timer(self.TICK_SEC, self._tick)
        self._timer.daemon = True
        self._timer.start()

    def _tick(self) -> None:
        try:
            self._do_tick()
        except Exception as e:
            _log(f"cron tick error: {e}")
        self._schedule_next_tick()

    def _do_tick(self) -> None:
        now = datetime.now(timezone.utc)
        is_coord = self.is_coordinator()
        fires = []
        with self._lock:
            for jid, state in self._jobs.items():
                job = state["job"]
                if not job.get("enabled", True):
                    continue
                if state["next_run"] > now:
                    continue
                fires.append((jid, job, state["next_run"]))
                # Advance — catch-up: skip missed windows, jump to next future
                try:
                    state["next_run"] = _cron_next(job["schedule"], now)
                except ValueError:
                    pass
        for jid, job, scheduled_for in fires:
            if is_coord:
                self.runner.spawn(job, reason="scheduled")
            else:
                self.hub.broadcast(
                    {
                        "type": "cron.would_have_fired",
                        "id": jid,
                        "scheduled_for": scheduled_for.isoformat(),
                        "reason": f"not coordinator (owner={self.cluster.crons_owner!r}, me={self.identity!r})",
                        "ts": _iso_now(),
                    }
                )

    # ── introspection ───────────────────────────────────────────────
    def list_jobs(self) -> List[Dict[str, Any]]:
        out = []
        with self._lock:
            for jid, state in self._jobs.items():
                out.append(
                    {
                        **state["job"],
                        "next_run": state["next_run"].isoformat(),
                        "running": self.runner.is_running(jid),
                    }
                )
        return out

    def trigger(self, job_id: str, reason: str = "manual") -> Optional[Dict[str, Any]]:
        with self._lock:
            state = self._jobs.get(job_id)
        if not state:
            return None
        return self.runner.spawn(state["job"], reason=reason)


# ───────────────────────────────────────────────────────────────────────
# State manager — caches state + polls FS for changes


class StateManager:
    def __init__(self, paths: Paths, cluster: Cluster, hub: Hub):
        self.paths = paths
        self.cluster = cluster
        self.hub = hub
        self._state: Dict[str, Any] = {}
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._fs_signature = ""
        self.rebuild()
        threading.Thread(target=self._poll_loop, daemon=True).start()

    def state(self) -> Dict[str, Any]:
        # py-1.4.2 — Always recompute `timeline.recent_events` on demand.
        # The cached `_state` skips chat-event-driven rebuilds (the FS
        # signature only watches modules/, docs/, roadmap/initiatives/,
        # public/ — not timeline/, because chat persists on every turn
        # and would thrash the signature). Result: on a fresh GET /state
        # the cached payload would be missing the latest chat.assistant.*
        # events even though they're on disk in
        # .meshkore/timeline/<date>.jsonl. Cockpit reloads were losing
        # the agent's most recent reply.
        with self._lock:
            snap = dict(self._state)
        events = _recent_timeline_events(self.paths, limit=TIMELINE_RECENT_LIMIT)
        snap["timeline"] = {
            "recent_events": events,
            "event_count": len(events),
            "limit": TIMELINE_RECENT_LIMIT,
        }
        return snap

    def rebuild(self, broadcast: bool = True) -> None:
        self.cluster.reload()
        with self._lock:
            self._state = build_state(self.paths, self.cluster)
            self._fs_signature = self._compute_signature()
        # Persist state.json so the legacy Node tooling can also read it.
        try:
            self.paths.roadmap_dir.mkdir(parents=True, exist_ok=True)
            self.paths.state_json.write_text(json.dumps(self._state, indent=2))
        except OSError:
            pass
        if broadcast:
            self.hub.broadcast({"type": "state.rebuilt", "ts": _iso_now()})

    def shutdown(self) -> None:
        self._stop.set()

    def _poll_loop(self) -> None:
        while not self._stop.wait(FS_POLL_SEC):
            try:
                sig = self._compute_signature()
                if sig != self._fs_signature:
                    self.rebuild(broadcast=True)
            except Exception:  # pragma: no cover — best-effort
                pass

    def _compute_signature(self) -> str:
        h = hashlib.sha1()
        for root in (
            self.paths.modules_dir,
            self.paths.docs_dir,
            self.paths.initiatives,
            self.paths.public,
        ):
            if not root.exists():
                continue
            for md in sorted(root.rglob("*")):
                if not md.is_file():
                    continue
                try:
                    st = md.stat()
                    h.update(str(md).encode())
                    h.update(struct.pack(">dq", st.st_mtime, st.st_size))
                except OSError:
                    pass
        return h.hexdigest()


# ───────────────────────────────────────────────────────────────────────
# HTTP / WebSocket server


def make_handler(daemon: "Daemon"):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # silence default access log
            return

        # ── helpers ────────────────────────────────────────────────────
        def _path(self) -> Tuple[str, Dict[str, str]]:
            parts = urllib.parse.urlsplit(self.path)
            return parts.path, dict(urllib.parse.parse_qsl(parts.query))

        def _bearer(self) -> Optional[str]:
            h = self.headers.get("Authorization") or ""
            if h.startswith("Bearer "):
                return h[7:].strip()
            return None

        def _need_auth(self) -> bool:
            tok = self._bearer()
            if tok and tok == daemon.token:
                return False
            self._json(401, {"error": "unauthorized"})
            return True

        def _cors(self) -> None:
            # The architect is served from architect.meshkore.com but
            # talks to localhost. CORS-allow any origin since the bearer
            # token gates the privileged routes.
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header(
                "Access-Control-Allow-Headers", "Authorization, Content-Type"
            )
            # py-1.9.1 — Chrome's Local Network Access (LNA) preflight
            # blocks any cross-origin request from a public-internet
            # page (https://architect.meshkore.com) to a private
            # address (localhost) unless this opt-in header is present.
            # The canonical transport already routes around LNA via
            # the daemon.meshkore.com TLS-loopback subdomain, but
            # enabling it here lets the cockpit fall back to plain
            # http://localhost:<port>/health as a diagnostic probe
            # when the TLS handshake fails — that lets us distinguish
            # "daemon dead" from "daemon alive but no TLS bundle".
            self.send_header("Access-Control-Allow-Private-Network", "true")
            # py-1.2.0 — Wire-version contract. The architect reads
            # this header on every response so a stale daemon is
            # detected without a separate /health round-trip. The
            # Expose-Headers entry is required because Allow-Origin
            # is `*` — without it, browser JS sees the response but
            # cannot read this custom header.
            self.send_header("X-MeshKore-Daemon-Version", DAEMON_VERSION)
            self.send_header(
                "Access-Control-Expose-Headers", "X-MeshKore-Daemon-Version"
            )

        def _json(self, code: int, body: Any) -> None:
            data = json.dumps(body).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self._cors()
            self.end_headers()
            self.wfile.write(data)

        # ── verb dispatch ──────────────────────────────────────────────
        def do_OPTIONS(self):  # noqa: N802
            self.send_response(204)
            self._cors()
            self.end_headers()

        def do_GET(self):  # noqa: N802
            p, q = self._path()
            # WebSocket upgrade?
            if (
                p in ("/events", "/ws")
                and self.headers.get("Upgrade", "").lower() == "websocket"
            ):
                return self._handle_ws()
            if p == "/health":
                return self._json(200, daemon.health())
            # D-TLS-02 — challenge-response auth. Cockpit posts a
            # random nonce; we return HMAC-SHA256(portal-token, nonce).
            # Cockpit verifies with its copy of the token before
            # trusting the daemon endpoint. Defeats MITM by an
            # attacker who serves a valid TLS cert (our wildcard is
            # public) but doesn't have the operator's portal-token.
            if p == "/auth/challenge":
                nonce = q.get("nonce", "")
                if (
                    not nonce
                    or len(nonce) > 128
                    or not re.match(r"^[A-Za-z0-9._-]+$", nonce)
                ):
                    return self._json(
                        400, {"error": "nonce required: 1-128 chars, [A-Za-z0-9._-]"}
                    )
                import hmac as _hmac
                import hashlib as _hashlib

                sig = _hmac.new(
                    daemon.token.encode("utf-8"),
                    nonce.encode("utf-8"),
                    _hashlib.sha256,
                ).hexdigest()
                return self._json(
                    200,
                    {
                        "nonce": nonce,
                        "sig": sig,
                        "alg": "HMAC-SHA256",
                        "version": DAEMON_VERSION,
                        "ts": _iso_now(),
                    },
                )
            if p == "/state":
                return self._json(200, daemon.state_manager.state())
            # U-DAEMON-02: subset reads. Matches Node's contract:
            # GET /state/cluster, /state/modules, /state/roadmap, etc.
            if p.startswith("/state/"):
                sub = p[len("/state/") :].strip("/")
                state = daemon.state_manager.state()
                if sub in state:
                    return self._json(200, state[sub])
                return self._json(404, {"error": "unknown subset", "subset": sub})
            if p == "/reload":
                if self._need_auth():
                    return
                daemon.state_manager.rebuild(broadcast=True)
                return self._json(200, {"ok": True, "generated_at": _iso_now()})
            if p == "/agents":
                return self._json(200, daemon.agents_listing())
            if p == "/info":
                return self._json(200, daemon.info())
            # U-DAEMON-02: read-only file serve under .meshkore/ for
            # docs, modules, and roadmap (the URL says `/tasks/` to
            # match Node's contract — but it serves from
            # .meshkore/roadmap/, which is where tasks live).
            if p.startswith("/docs/"):
                if self._need_auth():
                    return
                return self._serve_meshkore_file(
                    daemon.paths.docs_dir, p[len("/docs/") :]
                )
            if p.startswith("/modules/"):
                if self._need_auth():
                    return
                return self._serve_meshkore_file(
                    daemon.paths.modules_dir, p[len("/modules/") :]
                )
            if p.startswith("/tasks/"):
                if self._need_auth():
                    return
                return self._serve_meshkore_file(
                    daemon.paths.roadmap_dir, p[len("/tasks/") :]
                )
            # py-1.9.0 — daily narrative logs. `/log` lists every
            # `.meshkore/log/YYYY-MM-DD.md` file (descending by date),
            # `/log/<filename>` serves a single file. Both gated by
            # auth so a curious browser session can't scrape narrative.
            if p == "/log":
                if self._need_auth():
                    return
                return self._json(200, {"entries": daemon.log_listing()})
            if p.startswith("/log/"):
                if self._need_auth():
                    return
                return self._serve_meshkore_file(
                    daemon.paths.log_dir, p[len("/log/") :]
                )
            # py-1.9.3 — Per-initiative git activity. Runs git log on
            # the project root and returns commits whose subject/body
            # mentions the initiative id, plus the files each commit
            # touched. The cockpit's expanded InitiativeCard surfaces
            # this in its Activity tab so the operator can see what
            # actually shipped for a given initiative.
            if p.startswith("/initiative/") and p.endswith("/activity"):
                if self._need_auth():
                    return
                iid = p[len("/initiative/") : -len("/activity")]
                return self._json(200, daemon.initiative_activity(iid))
            # py-1.10.0 — Story-run coordinator reads.
            if p == "/runs":
                if self._need_auth():
                    return
                active_only = (q.get("active") or "0").lower() in ("1", "true", "yes")
                code, body = daemon.runs_list(active_only=active_only)
                return self._json(code, body)
            if p.startswith("/runs/"):
                if self._need_auth():
                    return
                run_id = p[len("/runs/") :]
                # Single-segment id only — control endpoints (/cancel,
                # /advance, …) live on POST and are matched there.
                if "/" not in run_id:
                    code, body = daemon.run_get(run_id)
                    return self._json(code, body)
            # U-DAEMON-02: credentials listing — names only, never
            # contents. Matches Node's response shape.
            if p == "/credentials":
                if self._need_auth():
                    return
                return self._json(200, daemon.credentials_listing())
            # py-1.5.0 — Daemon-side archive state. Anonymous read so the
            # cockpit can sync from boot before the token is pasted.
            if p == "/chat/archives":
                return self._json(
                    200,
                    {
                        "archived": daemon.chat_archive.list(),
                    },
                )
            # D-CRON-02..05: scheduler introspection.
            if p == "/cron/list":
                if self._need_auth():
                    return
                return self._json(
                    200,
                    {
                        "jobs": daemon.cron_scheduler.list_jobs(),
                        "coordinator": daemon.cron_scheduler.is_coordinator(),
                        "owner": daemon.cluster.crons_owner,
                        "identity": daemon.identity,
                        "tick_sec": daemon.cron_scheduler.TICK_SEC,
                    },
                )
            # Standard §13 — deployment links registry.
            if p == "/links":
                daemon.links_registry.reload()
                return self._json(200, daemon.links_registry.as_dict())
            if p.startswith("/links/"):
                mid = urllib.parse.unquote(p[len("/links/") :]).strip("/")
                if not mid:
                    return self._json(400, {"error": "module id required"})
                daemon.links_registry.reload()
                entry = daemon.links_registry.get(mid)
                if entry is None:
                    return self._json(
                        404, {"error": "module not in links.yaml", "id": mid}
                    )
                return self._json(200, entry)
            # Standard §14 — protocols registry.
            if p == "/protocols":
                daemon.protocols_registry.reload()
                return self._json(200, {"protocols": daemon.protocols_registry.list()})
            if p.startswith("/protocols/"):
                rest = urllib.parse.unquote(p[len("/protocols/") :]).strip("/")
                if not rest:
                    return self._json(400, {"error": "protocol id required"})
                if rest.endswith("/runs"):
                    pid = rest[: -len("/runs")]
                    return self._json(
                        200, {"runs": daemon.protocols_registry.runs(pid)}
                    )
                proto = daemon.protocols_registry.get(rest)
                if proto is None:
                    return self._json(404, {"error": "protocol not found", "id": rest})
                return self._json(200, proto)
            return self._json(404, {"error": "not found", "path": p})

        def _serve_meshkore_file(self, root: Path, rel: str) -> None:
            """Read a single text file rooted at one of the .meshkore/
            subtrees. Rejects path traversal absolutely — the resolved
            path must be a subpath of `root`, after URL-decoding."""
            rel = urllib.parse.unquote(rel)
            # Cheap-but-thorough traversal defence: reject any segment
            # that contains '..' or starts with '/', plus check the
            # resolved path is inside `root`.
            if ".." in rel.split("/") or rel.startswith("/"):
                return self._json(400, {"error": "path traversal"})
            target = (root / rel).resolve()
            try:
                target.relative_to(root.resolve())
            except ValueError:
                return self._json(400, {"error": "path traversal"})
            if not target.is_file():
                return self._json(404, {"error": "not found", "path": rel})
            try:
                body = target.read_bytes()
            except OSError as e:
                return self._json(500, {"error": str(e)})
            # Content-Type from extension. Default to markdown since the
            # vast majority of these files are .md.
            ext = target.suffix.lower()
            ctype = {
                ".md": "text/markdown; charset=utf-8",
                ".json": "application/json; charset=utf-8",
                ".yaml": "text/yaml; charset=utf-8",
                ".yml": "text/yaml; charset=utf-8",
                ".txt": "text/plain; charset=utf-8",
            }.get(ext, "text/markdown; charset=utf-8")
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self._cors()
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):  # noqa: N802
            p, _ = self._path()
            if p == "/shutdown":
                if self._need_auth():
                    return
                self._json(200, {"ok": True, "shutting_down": True, "ts": _iso_now()})
                threading.Thread(target=daemon.request_shutdown, daemon=True).start()
                return

            # All other POSTs need auth.
            if self._need_auth():
                return

            # py-1.2.0 — Daemon self-update (standard v7 §10.4). Driven by
            # the cockpit's auto-update flow on a version mismatch.
            if p == "/self-update":
                return self._json(*daemon.self_update(self._read_json_body()))

            # U-DAEMON-06: chat dispatch + cancel.
            if p == "/chat/dispatch":
                return self._json(*daemon.chat_dispatch(self._read_json_body()))
            if p == "/chat/cancel":
                return self._json(*daemon.chat_cancel(self._read_json_body()))
            # py-1.5.0 — Daemon-side archive lifecycle.
            if p == "/chat/archive":
                return self._json(*daemon.chat_archive_set(self._read_json_body()))
            if p == "/chat/unarchive":
                return self._json(*daemon.chat_archive_clear(self._read_json_body()))

            # U-DAEMON-09: simple message append + version stubs.
            if p == "/messages":
                return self._json(*daemon.append_message(self._read_json_body()))
            if p == "/version/next":
                return self._json(
                    501,
                    {
                        "error": "version coordinator not implemented yet",
                        "see": "modules/daemon/tasks/V20-version-coordinator.md",
                    },
                )

            # U-DAEMON-04: task lifecycle.
            if p == "/tasks":
                return self._json(*daemon.task_create(self._read_json_body()))
            if p.startswith("/tasks/") and p.endswith("/transition"):
                tid = p[len("/tasks/") : -len("/transition")]
                return self._json(*daemon.task_transition(tid, self._read_json_body()))
            if p.startswith("/tasks/") and p.endswith("/cancel"):
                tid = p[len("/tasks/") : -len("/cancel")]
                return self._json(*daemon.task_cancel(tid))
            if p.startswith("/tasks/") and p.endswith("/dispatch"):
                # U-DAEMON-07 territory — spawn a runner for a task.
                # Stub for now: return 501 so cockpit shows a clear error.
                return self._json(
                    501,
                    {
                        "error": "task dispatch (runner) not implemented yet",
                        "hint": "follows U-DAEMON-07 worker pool port",
                    },
                )

            # py-1.10.0 — Story-run coordinator writes. Endpoints:
            #  POST /runs                   → create new run
            #  POST /runs/<id>/cancel       → cancel (also kills chat session)
            #  POST /runs/<id>/advance      → bump cursor (cockpit-driven)
            #  POST /runs/<id>/finish       → mark done|failed
            #  POST /runs/<id>/stream       → record current stream_id
            if p == "/runs":
                return self._json(*daemon.run_create(self._read_json_body()))
            if p.startswith("/runs/"):
                rest = p[len("/runs/") :]
                if "/" in rest:
                    run_id, action = rest.split("/", 1)
                    if action == "cancel":
                        return self._json(*daemon.run_cancel(run_id))
                    if action == "advance":
                        return self._json(
                            *daemon.run_advance(run_id, self._read_json_body())
                        )
                    if action == "finish":
                        return self._json(
                            *daemon.run_finish(run_id, self._read_json_body())
                        )
                    if action == "stream":
                        return self._json(
                            *daemon.run_set_stream(run_id, self._read_json_body())
                        )

            # U-DAEMON-03 finish: declare a new agent.
            if p == "/agents":
                return self._json(*daemon.agent_create(self._read_json_body()))

            # D-CRON-04: trigger + cancel a cron job.
            if p.startswith("/cron/") and p.endswith("/trigger"):
                jid = p[len("/cron/") : -len("/trigger")]
                run = daemon.cron_scheduler.trigger(jid, reason="manual-trigger")
                if run is None:
                    return self._json(
                        404,
                        {"error": f"no cron job named {jid!r} (or already running)"},
                    )
                return self._json(202, run)
            if p.startswith("/cron/") and p.endswith("/cancel"):
                jid = p[len("/cron/") : -len("/cancel")]
                ok = daemon.cron_scheduler.runner.cancel(jid)
                return self._json(200, {"ok": ok, "id": jid, "cancelled": ok})
            # Standard §13 — patch a module's entry in links.yaml.
            if p.startswith("/links/"):
                if self._need_auth():
                    return
                mid = urllib.parse.unquote(p[len("/links/") :]).strip("/")
                if not mid:
                    return self._json(400, {"error": "module id required"})
                ok, msg = daemon.links_registry.patch(mid, self._read_json_body())
                if not ok:
                    return self._json(400, {"error": msg, "id": mid})
                entry = daemon.links_registry.get(mid)
                return self._json(200, {"ok": True, "id": mid, "entry": entry})
            # U-DAEMON-07 + 08: workers + admission stubs.
            if p == "/workers":
                return self._json(501, {"error": "worker pool not implemented yet"})
            if p.startswith("/admission/"):
                return self._json(501, {"error": "admission flow not implemented yet"})

            return self._json(404, {"error": "not found", "path": p})

        # ── helpers used by do_POST handlers ───────────────────────────
        def _read_json_body(self) -> Dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0 or length > MAX_BODY_BYTES:
                return {}
            try:
                raw = self.rfile.read(length).decode("utf-8")
                data = json.loads(raw) if raw else {}
                return data if isinstance(data, dict) else {}
            except (json.JSONDecodeError, OSError):
                return {}

        # ── WebSocket handshake + run-loop ─────────────────────────────
        def _handle_ws(self) -> None:
            key = self.headers.get("Sec-WebSocket-Key")
            if not key:
                self.send_error(400)
                return
            accept = base64.b64encode(
                hashlib.sha1((key + WS_GUID).encode()).digest()
            ).decode()
            self.send_response(101)
            self.send_header("Upgrade", "websocket")
            self.send_header("Connection", "Upgrade")
            self.send_header("Sec-WebSocket-Accept", accept)
            self.end_headers()
            sock = self.connection
            sock.settimeout(None)
            client = WSClient(sock)
            daemon.hub.add(client)
            # Greeting
            client.send_text(
                json.dumps(
                    {
                        "type": "hello",
                        "identity": daemon.identity,
                        "port": daemon.port,
                        "ts": _iso_now(),
                    }
                )
            )
            # Drain inbound frames (we only care about close) so the
            # socket pump keeps moving; ignore everything else.
            try:
                while not daemon.stopping.is_set() and not client.closed:
                    op, _data = _ws_read_frame(sock)
                    if op is None or op == 0x8:  # close frame
                        break
            except (OSError, ConnectionError):
                pass
            finally:
                daemon.hub.remove(client)

    return Handler


def _ws_read_frame(sock: socket.socket) -> Tuple[Optional[int], bytes]:
    """Minimal inbound frame parser. Returns (opcode, payload) or (None, b'')."""
    hdr = _recv_exact(sock, 2)
    if not hdr or len(hdr) < 2:
        return None, b""
    b1, b2 = hdr[0], hdr[1]
    opcode = b1 & 0x0F
    masked = bool(b2 & 0x80)
    length = b2 & 0x7F
    if length == 126:
        ext = _recv_exact(sock, 2)
        length = struct.unpack(">H", ext)[0]
    elif length == 127:
        ext = _recv_exact(sock, 8)
        length = struct.unpack(">Q", ext)[0]
    mask_key = _recv_exact(sock, 4) if masked else b""
    payload = _recv_exact(sock, length)
    if masked and payload:
        payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    return opcode, payload


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return b""
        buf.extend(chunk)
    return bytes(buf)


# ───────────────────────────────────────────────────────────────────────
# Daemon orchestrator


class Daemon:
    def __init__(
        self, paths: Paths, identity: Optional[str], requested_port: Optional[int]
    ):
        self.paths = paths
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
        self.port = _pick_port(paths, requested_port or self.cluster.architect_port)
        self.hub = Hub()
        self.state_manager = StateManager(paths, self.cluster, self.hub)
        self.chat_sessions = ChatSessions()
        # py-1.10.0 — server-side story-run coordinator. Owns the
        # initiative ↔ conv ↔ agent ↔ task-list binding so play/stop
        # has unambiguous identity and survives cockpit reload.
        self.runs = RunStore(paths, self.hub)
        # py-1.5.0 — persistent archive state (was cockpit-localStorage-only).
        self.chat_archive = ChatArchive(paths)
        # py-1.5.0 — background gzipper for .meshkore/timeline/*.jsonl
        # older than 90 days. Keeps disk footprint bounded on long-running
        # clusters; transparent to readers (gzip-aware).
        self.timeline_rotator = TimelineRotator(paths)
        # Standard §13 — deployment links registry. Quiet no-op when
        # .meshkore/public/links.yaml is absent.
        self.links_registry = LinksRegistry(paths, self.hub)
        # Standard §14 — protocols registry. Quiet no-op when
        # .meshkore/protocols/ is absent.
        self.protocols_registry = ProtocolsRegistry(paths, self.hub)
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
    def _spawn_chat_turn(
        self,
        conv: str,
        prompt: str,
        *,
        context_docs: Optional[List[Dict[str, Any]]] = None,
        agent_type: Optional[str] = None,
        agent_id: Optional[str] = None,
    ) -> ChatRunner:
        """Start one chat turn. Wires the chain so a buffered next
        prompt re-spawns automatically when the current turn finishes.
        Context docs (py-1.4.0) flow into the BriefingPipeline."""
        # py-1.7.0 — Resolve agent_type/id from caller args, falling
        # back to the persisted conv sidecar so chained turns and
        # cockpit reconnects don't lose specialisation.
        resolved_type, resolved_id = self._conv_meta_get(conv)
        if agent_type:
            resolved_type = agent_type
        if agent_id:
            resolved_id = agent_id
        # Persist whatever we end up with so subsequent turns inherit it.
        self._conv_meta_set(conv, resolved_type, resolved_id)
        runner = ChatRunner(
            paths=self.paths,
            cluster=self.cluster,
            hub=self.hub,
            identity=self.identity,
            conv=conv,
            prompt=prompt,
            context_docs=context_docs or [],
            agent_type=resolved_type,
            agent_id=resolved_id,
        )
        runner.spawn()
        # Chained turns (auto-spawn when a queued prompt lands) inherit
        # the current turn's context_docs + agent metadata.
        chain_ctx = list(context_docs or [])
        chain_type = resolved_type
        chain_id = resolved_id
        self.chat_sessions.start(
            conv,
            runner,
            on_chain=lambda c, p: self._spawn_chat_turn(
                c,
                p,
                context_docs=chain_ctx,
                agent_type=chain_type,
                agent_id=chain_id,
            ),
        )
        return runner

    # py-1.7.0 — conv → (agent_type, agent_id) sidecar. Lets the daemon
    # remember the specialisation across turns even if the cockpit
    # forgets to re-send it (and gives offline/migrated clusters a stable
    # store outside the cockpit's localStorage).
    def _conv_meta_path(self) -> Any:
        return self.paths.runtime / "conv_meta.json"

    def _conv_meta_load(self) -> Dict[str, Dict[str, str]]:
        p = self._conv_meta_path()
        try:
            if not p.exists():
                return {}
            return json.loads(p.read_text() or "{}") or {}
        except Exception:
            return {}

    def _conv_meta_get(self, conv: str) -> Tuple[str, Optional[str]]:
        meta = self._conv_meta_load().get(conv) or {}
        return (
            _agent_type_normalised(meta.get("agent_type")),
            (meta.get("agent_id") or None),
        )

    def _conv_meta_set(
        self, conv: str, agent_type: str, agent_id: Optional[str]
    ) -> None:
        try:
            all_meta = self._conv_meta_load()
            entry = all_meta.get(conv) or {}
            entry["agent_type"] = _agent_type_normalised(agent_type)
            if agent_id:
                entry["agent_id"] = agent_id
            all_meta[conv] = entry
            p = self._conv_meta_path()
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(all_meta, indent=2, sort_keys=True))
            tmp.replace(p)
        except Exception as e:
            _log(f"conv_meta write failed: {e}")

    def chat_dispatch(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        text = str(body.get("text") or "").strip()
        if not text:
            return 400, {"error": "text required"}
        author = str(body.get("author") or self.identity)
        conv = str(
            body.get("conv")
            or f"chat-{_iso_now()[:16].replace(':', '-').replace('T', '-').lower()}"
        )
        # py-1.7.0 — agent specialisation from cockpit. Both fields are
        # optional; missing → 'custom' (General coder). When present,
        # persisted to the conv_meta sidecar so chained turns and
        # cockpit reconnects keep the same role.
        agent_type = body.get("agent_type")
        agent_id = body.get("agent_id")
        # py-1.4.0 — Accept cockpit-attached context as part of the
        # briefing pipeline. Previously this field was silently
        # dropped, which broke V46/V78b onboarding (the cockpit
        # thought it was sending a bootstrap brief but the agent
        # never saw it).
        raw_docs = body.get("context_docs")
        context_docs: List[Dict[str, Any]] = []
        if isinstance(raw_docs, list):
            for d in raw_docs:
                if isinstance(d, dict) and (d.get("content") or "").strip():
                    context_docs.append(
                        {
                            "filename": str(d.get("filename") or "doc.md"),
                            "content": str(d.get("content") or ""),
                        }
                    )
        # 1) Emit + persist the user event right away.
        ev = _append_timeline(
            self.paths,
            {
                "type": "chat.user",
                "author": author,
                "text": text,
                "conv": conv,
            },
        )
        self.hub.broadcast(ev)
        # 2) Queue if a turn is already running for this conv.
        if self.chat_sessions.has(conv):
            pending = self.chat_sessions.queue(conv, text)
            return 202, {
                "queued": True,
                "conv": conv,
                "pending": pending,
                "message": "turn in progress — your prompt will be merged into the next turn",
            }
        # 3) New turn.
        try:
            runner = self._spawn_chat_turn(
                conv,
                text,
                context_docs=context_docs,
                agent_type=agent_type,
                agent_id=agent_id,
            )
        except Exception as e:
            return 400, {"error": str(e)}
        return 202, {
            "conv": conv,
            "runner": "claude-code",
            "identity": self.identity,
            "pid": runner.pid,
            "stream_id": runner.stream_id,
            "agent_type": _agent_type_normalised(agent_type),
        }

    def chat_cancel(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        conv = str(body.get("conv") or "").strip()
        if not conv:
            return 400, {"error": "conv required"}
        cancelled, dropped = self.chat_sessions.cancel(conv)
        # py-1.10.0 — propagate to runs. If the cancelled conv belongs
        # to an active run (started via /runs), mark it cancelled too
        # and emit run.cancelled. Operator hitting the chat's StopBar
        # converges with hitting ■ on the initiative card.
        run = self.runs.find_by_conv(conv)
        if run is not None:
            self.runs.cancel(run["id"])
        if not cancelled:
            return 200, {
                "ok": True,
                "cancelled": False,
                "reason": "no active turn for that conv",
                "run_cancelled": run["id"] if run else None,
            }
        self.hub.broadcast(
            {
                "type": "chat.cancelled",
                "conv": conv,
                "ts": _iso_now(),
                "dropped_pending": dropped,
            }
        )
        return 200, {
            "ok": True,
            "cancelled": True,
            "dropped_pending": dropped,
            "run_cancelled": run["id"] if run else None,
        }

    # ── py-1.10.0: story-run coordinator ────────────────────────────
    def run_create(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        """Create a new story run. The cockpit decides which conv and
        agent_id to bind (it already manages those); the daemon just
        records the binding and emits run.started.
        """
        initiative_id = str(body.get("initiative_id") or "").strip()
        if not initiative_id:
            return 400, {"error": "initiative_id required"}
        conv = str(body.get("conv") or "").strip()
        if not conv:
            return 400, {"error": "conv required"}
        agent_id = str(body.get("agent_id") or "").strip()
        if not agent_id:
            return 400, {"error": "agent_id required"}
        task_ids_raw = body.get("task_ids") or []
        if not isinstance(task_ids_raw, list) or not task_ids_raw:
            return 400, {"error": "task_ids must be a non-empty list"}
        task_ids = [str(t) for t in task_ids_raw if t]
        run = self.runs.create(
            initiative_id=initiative_id,
            initiative_title=str(body.get("initiative_title") or initiative_id),
            conv=conv,
            agent_id=agent_id,
            agent_title=str(body.get("agent_title") or initiative_id),
            task_ids=task_ids,
        )
        return 201, {"ok": True, "run": run}

    def run_cancel(self, run_id: str) -> Tuple[int, Dict[str, Any]]:
        run = self.runs.get(run_id)
        if not run:
            return 404, {"error": f"unknown run {run_id!r}"}
        # Cancel the chat session (if live) AND mark the run cancelled.
        cancelled, dropped = self.chat_sessions.cancel(run["conv"])
        updated = self.runs.cancel(run_id)
        if cancelled:
            self.hub.broadcast(
                {
                    "type": "chat.cancelled",
                    "conv": run["conv"],
                    "ts": _iso_now(),
                    "dropped_pending": dropped,
                }
            )
        return 200, {
            "ok": True,
            "run": updated,
            "chat_cancelled": cancelled,
            "dropped_pending": dropped,
        }

    def run_advance(
        self, run_id: str, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        cursor = body.get("cursor")
        if not isinstance(cursor, int):
            return 400, {"error": "cursor (int) required"}
        stream_id = body.get("stream_id")
        if stream_id is not None and not isinstance(stream_id, str):
            return 400, {"error": "stream_id must be string"}
        updated = self.runs.advance(run_id, cursor, stream_id=stream_id)
        if updated is None:
            return 404, {"error": f"unknown run {run_id!r}"}
        return 200, {"ok": True, "run": updated}

    def run_finish(
        self, run_id: str, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        status = str(body.get("status") or "").strip()
        if status not in (RunStore.STATUS_DONE, RunStore.STATUS_FAILED):
            return 400, {"error": "status must be 'done' or 'failed'"}
        updated = self.runs.finish(run_id, status, error=body.get("error"))
        if updated is None:
            return 404, {"error": f"unknown run {run_id!r}"}
        return 200, {"ok": True, "run": updated}

    def run_set_stream(
        self, run_id: str, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        stream_id = str(body.get("stream_id") or "").strip()
        if not stream_id:
            return 400, {"error": "stream_id required"}
        updated = self.runs.set_stream(run_id, stream_id)
        if updated is None:
            return 404, {"error": f"unknown run {run_id!r}"}
        return 200, {"ok": True, "run": updated}

    def runs_list(self, active_only: bool = False) -> Tuple[int, Dict[str, Any]]:
        runs = self.runs.list_all(active_only=active_only)
        # Decorate each with a derived `live` flag — true when there's
        # a chat session in flight for the conv right now. Cockpit uses
        # it to decide play vs stop on the UI.
        for r in runs:
            r["live"] = self.chat_sessions.has(r["conv"])
        return 200, {"runs": runs, "count": len(runs)}

    def run_get(self, run_id: str) -> Tuple[int, Dict[str, Any]]:
        r = self.runs.get(run_id)
        if not r:
            return 404, {"error": f"unknown run {run_id!r}"}
        r["live"] = self.chat_sessions.has(r["conv"])
        return 200, {"run": r}

    # ── py-1.5.0: daemon-side archive lifecycle ───────────────────────
    def chat_archive_set(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        conv = str(body.get("conv") or "").strip()
        if not conv:
            return 400, {"error": "conv required"}
        by = str(body.get("author") or "").strip()
        entry = self.chat_archive.archive(conv, by=by)
        # Broadcast so other cockpit instances see the change in real
        # time (multi-tab / multi-window scenarios).
        self.hub.broadcast(
            {
                "type": "chat.archived",
                "conv": conv,
                "ts": entry.get("archived_at"),
                "by": entry.get("by"),
            }
        )
        return 200, {"ok": True, "archived": entry}

    def chat_archive_clear(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        conv = str(body.get("conv") or "").strip()
        if not conv:
            return 400, {"error": "conv required"}
        was_archived = self.chat_archive.unarchive(conv)
        if was_archived:
            self.hub.broadcast(
                {
                    "type": "chat.unarchived",
                    "conv": conv,
                    "ts": _iso_now(),
                }
            )
        return 200, {"ok": True, "unarchived": was_archived, "conv": conv}

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
        # 7. Spawn replacement on a free port (NOT this daemon's port —
        #    we're still bound to it; the new process needs its own).
        try:
            new_port = _pick_port(self.paths, preferred=None)
        except SystemExit as e:
            return 500, {
                "error": "no free port available for new daemon",
                "detail": str(e),
            }
        try:
            proc = _sp.Popen(
                [sys.executable, str(current), "--port", str(new_port)],
                cwd=str(self.paths.root),
                stdin=_sp.DEVNULL,
                stdout=_sp.DEVNULL,
                stderr=_sp.DEVNULL,
                start_new_session=True,  # detach from our process group
            )
        except Exception as e:
            return 500, {"error": "failed to spawn new daemon", "detail": str(e)}
        # 8. Schedule own shutdown so the cockpit can reconnect.
        SHUTDOWN_DELAY = 3.0

        def _self_kill():
            try:
                self.hub.broadcast(
                    {
                        "type": "daemon.self_update.handing_off",
                        "new_pid": proc.pid,
                        "new_port": new_port,
                        "ts": _iso_now(),
                    }
                )
            except Exception:
                pass
            os._exit(0)

        threading.Timer(SHUTDOWN_DELAY, _self_kill).start()
        return 202, {
            "ok": True,
            "new_pid": proc.pid,
            "new_port": new_port,
            "shutdown_in_sec": SHUTDOWN_DELAY,
            "old_backup": str(backup.relative_to(self.paths.root))
            if backup.exists()
            else None,
            "old_version": DAEMON_VERSION,
            "source_url": url,
        }

    # ── U-DAEMON-09: message append + version stubs ────────────────────
    def append_message(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        text = str(body.get("text") or "").strip()
        if not text:
            return 400, {"error": "text required"}
        author = str(body.get("author") or self.identity)
        conv = str(body.get("conv") or "general")
        ev = _append_timeline(
            self.paths,
            {
                "type": "message",
                "author": author,
                "text": text,
                "conv": conv,
            },
        )
        self.hub.broadcast(ev)
        return 201, ev

    # ── U-DAEMON-04: task lifecycle ────────────────────────────────────
    def task_create(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        module = str(body.get("module") or "general").strip().replace("/", "")
        title = str(body.get("title") or "").strip()
        if not title:
            return 400, {"error": "title required"}
        status = str(body.get("status") or "next")
        priority = str(body.get("priority") or "medium")
        category = str(body.get("category") or module)
        tags = body.get("tags") or []
        depends_on = body.get("depends_on") or []
        body_md = str(body.get("body") or f"# {title}\n\n_New task — fill in._\n")
        # Pick the next id in the module.
        tasks_dir = self.paths.modules_dir / module / "tasks"
        tasks_dir.mkdir(parents=True, exist_ok=True)
        # Heuristic id: T{N} where N is the highest existing + 1.
        max_n = 0
        for f in tasks_dir.glob("T*.md"):
            m = re.match(r"T(\d+)", f.name)
            if m:
                try:
                    max_n = max(max_n, int(m.group(1)))
                except ValueError:
                    pass
        tid = f"T{max_n + 1:03d}"
        slug = re.sub(r"[^a-z0-9-]+", "-", title.lower())[:60].strip("-")
        fname = f"{tid}-{slug}.md" if slug else f"{tid}.md"
        target = tasks_dir / fname
        frontmatter = "\n".join(
            [
                "---",
                f"id: {tid}",
                f'title: "{title}"',
                f"status: {status}",
                f"priority: {priority}",
                f"category: {category}",
                f"owner: {self.identity}",
                f"created: {_iso_now()[:10]}",
                f"updated: {_iso_now()[:10]}",
                f"tags: {json.dumps(tags)}",
                f"depends_on: {json.dumps(depends_on)}",
                "---",
                "",
                body_md,
            ]
        )
        target.write_text(frontmatter)
        self.state_manager.rebuild(broadcast=True)
        return 201, {"id": tid, "path": str(target.relative_to(self.paths.root))}

    def task_transition(
        self, tid: str, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        to = str(body.get("to") or "").strip()
        valid = {"backlog", "next", "in_progress", "active", "blocked", "done"}
        if to not in valid:
            return 400, {"error": f"to must be one of {sorted(valid)}"}
        path = self._find_task(tid)
        if not path:
            return 404, {"error": f"task {tid} not found"}
        text = path.read_text()
        new = re.sub(r"^status:\s*\S+\s*$", f"status: {to}", text, count=1, flags=re.M)
        if new == text:
            new = re.sub(
                r"^---\s*$\n", f"---\nstatus: {to}\n", text, count=1, flags=re.M
            )
        path.write_text(new)
        self.state_manager.rebuild(broadcast=True)
        return 200, {
            "id": tid,
            "from": "?",
            "to": to,
            "path": str(path.relative_to(self.paths.root)),
        }

    def task_cancel(self, tid: str) -> Tuple[int, Dict[str, Any]]:
        # No active runner yet (dispatch is stubbed); this just transitions to blocked.
        return self.task_transition(tid, {"to": "blocked"})

    def _find_task(self, tid: str) -> Optional[Path]:
        for f in self.paths.modules_dir.rglob(f"{tid}*.md"):
            return f
        return None

    # ── U-DAEMON-03 finish: declare a new agent identity ───────────────
    def agent_create(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        aid = str(body.get("id") or "").strip()
        if not re.match(r"^[a-z][a-z0-9-]{1,40}$", aid):
            return 400, {"error": "id must be lowercase kebab, 2-41 chars"}
        self.paths.agents_dir.mkdir(parents=True, exist_ok=True)
        target = self.paths.agents_dir / f"{aid}.yaml"
        if target.exists():
            return 409, {"error": f"agent {aid} already declared"}
        kind = str(body.get("kind") or "operator")
        permissions = str(body.get("permissions") or "edits")
        target.write_text(
            f"# Declared via POST /agents on {_iso_now()}\n"
            f"id: {aid}\n"
            f"kind: {kind}\n"
            f"permissions: {permissions}\n"
        )
        self.state_manager.rebuild(broadcast=True)
        return 201, {"id": aid, "path": str(target.relative_to(self.paths.root))}

    # ── HTTP body for /health and /info ────────────────────────────────
    def health(self) -> Dict[str, Any]:
        # py-1.2.0 — Surface the cluster.yaml.daemon block (or its
        # defaults) so the cockpit knows whether to fire the silent
        # auto-update flow on a version mismatch.
        cfg = {}
        try:
            d = (
                self.cluster.data.get("daemon")
                if isinstance(self.cluster.data, dict)
                else None
            )
            if isinstance(d, dict):
                cfg = d
        except Exception:
            cfg = {}
        daemon_cfg = {
            "auto_update": bool(cfg.get("auto_update", True)),
            "auto_update_source": str(
                cfg.get("auto_update_source")
                or "https://meshkore.com/reference/cluster/scripts/daemon.py"
            ),
        }
        return {
            "ok": True,
            "identity": self.identity,
            "port": self.port,
            "mode": "server",
            "implementation": "python",
            "version": DAEMON_VERSION,
            "cluster_id": self.cluster.id,
            "cluster_name": self.cluster.name,
            "cluster_type": self.cluster.type,
            # D-TLS-01 — advertise the transport scheme so the cockpit
            # knows whether https://daemon.meshkore.com:<port> is
            # available or it must use http://localhost:<port>.
            "tls": self.tls_enabled,
            "endpoint": (
                f"https://daemon.meshkore.com:{self.port}"
                if self.tls_enabled
                else f"http://localhost:{self.port}"
            ),
            # U-DAEMON-01: capability advertisement.
            # During the Node→Python unification (initiative
            # `unified-python-daemon`), the cockpit reads this array
            # to route each call to the daemon that supports the
            # feature. Adding an endpoint here is part of the
            # acceptance criteria for that endpoint's port task.
            "features": self._features(),
            # py-1.2.0 — Standard v7 §10.4 (daemon self-update).
            "daemon": daemon_cfg,
            # py-1.10.2 — Convs with a live ChatRunner right now.
            # Cockpit uses this at boot/reconnect to mark agents as
            # working + show their preparing bubble IMMEDIATELY instead
            # of waiting for the next WS delta (which can take ~20s if
            # the runner is mid-tool-call). Empty list when the daemon
            # has no in-flight turns.
            "chat_active_convs": self.chat_sessions.list_active(),
            "ts": _iso_now(),
        }

    def _features(self) -> List[str]:
        feats = [
            "health",
            "state",
            "state.subset",  # U-DAEMON-02
            "reload",
            # D-TLS-01 — only when the bundled cert actually loaded.
            *(["tls.loopback"] if self.tls_enabled else []),
            # D-TLS-02 — challenge-response auth for MITM defence.
            "auth.challenge",
            "agents",
            "agents.create",  # U-DAEMON-02 + 03
            "events",  # WS hub + chat.* + task.* + tool.*
            "files.docs",
            "files.modules",
            "files.tasks",  # U-DAEMON-02
            "files.log",  # py-1.9.0 — narrative day-logs for Diary tab
            "initiative.activity",  # py-1.9.3 — per-initiative git commits + files
            "runs.v1",  # py-1.10.0 — story-run coordinator
            "runs.cancel",  # POST /runs/<id>/cancel
            "runs.advance",  # POST /runs/<id>/advance
            "runs.finish",  # POST /runs/<id>/finish
            "chat.active_convs",  # py-1.10.2 — /health.chat_active_convs
            "agents.roadmap-architect",  # py-1.10.3 — coordinator agent type
            "agents.architect-consult.v1",  # py-1.10.8 — [architect-consult] addendum forces A001 to decide
            "credentials",  # U-DAEMON-02 (list-only)
            "info",
            "shutdown",
            # U-DAEMON-04 task lifecycle (dispatch is stubbed, marked separately)
            "tasks.create",
            "tasks.transition",
            "tasks.cancel",
            # U-DAEMON-05 + 06 chat coordinator
            "chat",
            "chat.cancel",
            # U-DAEMON-09 misc
            "messages",
            # py-1.2.0 — Standard v7 §10.4 daemon self-update.
            "self_update",
            "version_header",
            # py-1.5.0 — chat integrity bundle.
            "chat.tools_persisted",  # tool.use + tool.result in jsonl
            "chat.rolling_history",  # >12-turn summary in briefing
            "chat.atomic_writes",  # fsync + atomic append
            "chat.archives",  # /chat/archives + /chat/archive[+un]
            "timeline.rotation",  # gzip > 90d into archive/
            # py-1.6.0 → py-1.6.1 — session_resume opt-in only.
            # Set env MESHKORE_CLAUDE_SESSION_ID=1 to enable. Default
            # off after a production bug where claude-code exited
            # silently on resumed sessions.
        ]
        if os.environ.get("MESHKORE_CLAUDE_SESSION_ID", "").strip() in (
            "1",
            "true",
            "yes",
            "on",
        ):
            feats.append("chat.session_resume.optin")
        if hasattr(self.cluster, "crons"):
            feats.append("cron.schema")
        # D-CRON-02..05: scheduler is live, list + trigger + cancel + log endpoints.
        feats.extend(
            ["cron.tick", "cron.list", "cron.trigger", "cron.cancel", "cron.log"]
        )
        # Standard §13: deployment links registry.
        feats.extend(["links.read", "links.write"])
        # Standard §14: protocols registry (read-only this version).
        feats.extend(["protocols.read"])
        # Stubs — advertised separately so the cockpit can show
        # "not yet" badges without trying the endpoint.
        feats.extend(
            [
                "stub.workers",
                "stub.admission",
                "stub.tasks.dispatch",
                "stub.version.next",
            ]
        )
        return feats

    def info(self) -> Dict[str, Any]:
        h = self.health()
        h["version"] = DAEMON_VERSION
        h["paths"] = {
            "root": str(self.paths.root),
            "meshkore": str(self.paths.meshkore),
        }
        return h

    def agents_listing(self) -> List[Dict[str, Any]]:
        # U-DAEMON-02: matches Node's shape including pid + online so
        # the cockpit's Network tab works against either daemon.
        if not self.paths.agents_dir.exists():
            return []
        runtime_agents = self.paths.runtime / "agents"
        out = []
        for yml in sorted(self.paths.agents_dir.glob("*.yaml")):
            try:
                data = parse_simple_yaml(yml.read_text())
            except OSError:
                continue
            pid_file = runtime_agents / f"{yml.stem}.pid"
            pid: Optional[int] = None
            online = False
            if pid_file.exists():
                try:
                    pid = int(pid_file.read_text().strip())
                    # Crude liveness check — os.kill(pid, 0) raises if no such pid
                    os.kill(pid, 0)
                    online = True
                except (OSError, ValueError):
                    pid = None
            out.append(
                {
                    "id": yml.stem,
                    "identity": yml.stem,  # alias, matches Node
                    "pid": pid,
                    "online": online,
                    "data": data,
                }
            )
        return out

    def initiative_activity(self, initiative_id: str) -> Dict[str, Any]:
        """py-1.9.3 — Walk git log for commits referencing this initiative.
        Returns at most 50 of the most recent matching commits, each with
        the files it touched (`git diff-tree --no-commit-id --name-only -r`).
        Matching is plain substring on subject + body so operators can
        reference an initiative however they like ("[I-cron-dashboard]",
        "for cron-dashboard", etc.) — no rigid trailer schema.

        Bounded by 1000 commits scanned + a hard timeout per git call so
        a 50k-commit repo doesn't melt the daemon. Failures (no git, bad
        repo, timeout) degrade to an empty payload + an explanatory
        `error` field; the cockpit just shows "no activity yet".
        """
        out: Dict[str, Any] = {
            "initiative_id": initiative_id,
            "commits": [],
            "generated_at": _iso_now(),
        }
        if not isinstance(initiative_id, str) or not initiative_id.strip():
            out["error"] = "invalid initiative id"
            return out
        iid = initiative_id.strip()

        import subprocess as _sp

        root = self.paths.root

        # py-1.9.3 — Multi-repo workspaces (meshkore-style: webapp/,
        # architect/, .meshkore/ each a separate git repo at depth 1)
        # AND single-repo projects (typical ikamiro-style) both work.
        # Find every depth ≤ 1 directory that owns a `.git` and scan
        # each one. The commit row carries a `repo` field so the
        # cockpit can disambiguate when two repos both reference the
        # same initiative id.
        repo_dirs: List[Path] = []
        if (root / ".git").exists():
            repo_dirs.append(root)
        else:
            try:
                for child in sorted(root.iterdir()):
                    if not child.is_dir() or child.name.startswith("."):
                        continue
                    if (child / ".git").exists():
                        repo_dirs.append(child)
            except OSError:
                pass

        if not repo_dirs:
            out["error"] = "no git repos found at project root or depth-1"
            return out

        def git_in(cwd: Path, *args: str, timeout: float = 4.0) -> Optional[str]:
            try:
                r = _sp.run(
                    ["git", "-C", str(cwd), *args],
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                )
                if r.returncode != 0:
                    return None
                return r.stdout
            except (_sp.TimeoutExpired, FileNotFoundError, OSError):
                return None

        commits: List[Dict[str, Any]] = []
        for repo_dir in repo_dirs:
            repo_label = repo_dir.name if repo_dir != root else "(root)"
            raw = git_in(
                repo_dir,
                "log",
                "--max-count=1000",
                "--grep",
                iid,
                "-i",
                "--pretty=format:%H%x09%h%x09%aI%x09%an%x09%s",
                timeout=6.0,
            )
            if raw is None:
                continue
            for line in raw.splitlines():
                if not line.strip():
                    continue
                parts = line.split("\t", 4)
                if len(parts) != 5:
                    continue
                sha, short, ts, author, subject = parts
                files_raw = (
                    git_in(
                        repo_dir,
                        "diff-tree",
                        "--no-commit-id",
                        "--name-only",
                        "-r",
                        sha,
                        timeout=3.0,
                    )
                    or ""
                )
                files = [ln.strip() for ln in files_raw.splitlines() if ln.strip()]
                commits.append(
                    {
                        "repo": repo_label,
                        "sha": sha,
                        "short_sha": short,
                        "ts": ts,
                        "author": author,
                        "subject": subject,
                        "files": files[:200],
                        "files_truncated": len(files) > 200,
                    }
                )
                if len(commits) >= 50:
                    break
            if len(commits) >= 50:
                break

        # Newest first across repos (each repo's slice already comes
        # newest-first from git log, but interleaved across repos
        # needs an explicit ts sort).
        commits.sort(key=lambda c: c.get("ts") or "", reverse=True)
        out["commits"] = commits[:50]
        return out

    def log_listing(self) -> List[Dict[str, Any]]:
        """py-1.9.0 — Descending-by-date list of `.meshkore/log/*.md`
        narrative day-files. Just metadata (name, date, size, mtime);
        callers fetch the body via `/log/<filename>` for paged display
        in the cockpit Diary tab. Dotfiles + non-.md files are skipped.

        Returned shape:
            [{ "name": "2026-05-27.md", "date": "2026-05-27",
               "size": 12345, "mtime": "2026-05-27T21:00:00Z" }]
        """
        if not self.paths.log_dir.exists():
            return []
        out = []
        for f in self.paths.log_dir.iterdir():
            if not f.is_file() or f.name.startswith("."):
                continue
            if f.suffix.lower() != ".md":
                continue
            # Most filenames are `YYYY-MM-DD.md`. The few that aren't
            # (handoff notes etc.) get `date: null`.
            stem = f.stem
            date = (
                stem
                if (
                    len(stem) == 10
                    and stem[4] == "-"
                    and stem[7] == "-"
                    and stem[:4].isdigit()
                    and stem[5:7].isdigit()
                    and stem[8:10].isdigit()
                )
                else None
            )
            try:
                st = f.stat()
                size = st.st_size
                mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            except OSError:
                size = None
                mtime = None
            out.append(
                {
                    "name": f.name,
                    "date": date,
                    "size": size,
                    "mtime": mtime,
                }
            )
        # Dated entries descending (newest → oldest), then any extras
        # (handoff notes etc.) appended in stable filename order.
        dated = sorted(
            [e for e in out if e["date"]], key=lambda e: e["date"], reverse=True
        )
        extras = sorted([e for e in out if not e["date"]], key=lambda e: e["name"])
        return dated + extras

    def credentials_listing(self) -> List[Dict[str, Any]]:
        """Names + sizes of every file in .meshkore/credentials/.
        Never the contents — the cockpit only needs to know what
        exists, never what's in them. Same security stance as Node."""
        if not self.paths.credentials.exists():
            return []
        out = []
        for f in sorted(self.paths.credentials.iterdir()):
            if f.name.startswith("."):
                continue
            try:
                size = f.stat().st_size if f.is_file() else None
            except OSError:
                size = None
            out.append(
                {
                    "name": f.name,
                    "size": size,
                    "is_symlink": f.is_symlink(),
                }
            )
        return out

    # ── lifecycle ──────────────────────────────────────────────────────
    def serve_forever(self) -> None:
        self._write_runtime()
        handler = make_handler(self)
        self.server = ThreadingHTTPServer(("127.0.0.1", self.port), handler)
        self.server.daemon_threads = True
        # D-TLS-01 — wrap the socket with TLS when the bundle is
        # present. Cockpit uses https://daemon.meshkore.com:<port>
        # then, no mixed-content / LNA Issues.
        bundle = _find_tls_bundle()
        ctx = _build_tls_context(*bundle) if bundle else None
        self.tls_enabled = ctx is not None
        if ctx is not None:
            self.server.socket = ctx.wrap_socket(self.server.socket, server_side=True)
        scheme = "https" if self.tls_enabled else "http"
        _log(
            f"meshcore-py listening on {scheme}://127.0.0.1:{self.port} "
            f"(identity={self.identity}, cluster={self.cluster.id}, "
            f"tls={'on (daemon.meshkore.com)' if self.tls_enabled else 'off'})"
        )
        # D-CRON-02: start the scheduler. Ticks every 10s in a background
        # thread; cluster.yaml.crons jobs fire from here, no LaunchAgent.
        self.cron_scheduler.start()
        try:
            self.server.serve_forever(poll_interval=0.5)
        finally:
            try:
                self.cron_scheduler.stop()
            except Exception:
                pass
            self.cleanup()

    def request_shutdown(self) -> None:
        if self.stopping.is_set():
            return
        self.stopping.set()
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


def _iso_now() -> str:
    return (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.")
        + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"
    )


def _log(msg: str) -> None:
    print(f"[meshcore-py {_iso_now()}] {msg}", flush=True)


def _hostname_default() -> str:
    return f"{socket.gethostname().split('.')[0]}-py"


def _detect_identity(paths: Paths) -> Optional[str]:
    if not paths.agents_dir.exists():
        return None
    for yml in sorted(paths.agents_dir.glob("*.yaml")):
        return yml.stem
    return None


def _ensure_token(paths: Paths) -> str:
    """Read or freshly mint the architect bearer token."""
    paths.credentials.mkdir(parents=True, exist_ok=True)
    if paths.token_file.exists():
        tok = paths.token_file.read_text().strip()
        if tok:
            return tok
    tok = secrets.token_urlsafe(32)
    paths.token_file.write_text(tok)
    try:
        os.chmod(paths.token_file, 0o600)
    except OSError:
        pass
    _log(f"minted new architect token at {paths.token_file}")
    return tok


# ───────────────────────────────────────────────────────────────────────
# TLS — loopback subdomain (D-TLS-01)


def _find_tls_bundle() -> Optional[Tuple[Path, Path]]:
    """Locate (cert, key) next to daemon.py. Returns None if either
    file is missing — daemon then falls back to plain HTTP, so older
    operators who don't have the bundle keep working unchanged."""
    here = Path(__file__).resolve().parent
    cert = here / TLS_BUNDLE_NAME / TLS_CERT_FILENAME
    key = here / TLS_BUNDLE_NAME / TLS_KEY_FILENAME
    if not cert.is_file() or not key.is_file():
        return None
    try:
        cert.read_bytes()
        key.read_bytes()
    except OSError as e:
        _log(f"tls: bundle exists but unreadable ({e}); falling back to HTTP")
        return None
    return cert, key


def _build_tls_context(cert_path: Path, key_path: Path) -> Optional[ssl.SSLContext]:
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
        return ctx
    except (ssl.SSLError, OSError) as e:
        _log(f"tls: failed to load cert ({e}); falling back to HTTP")
        return None


def _port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _migrate_cluster_daemon_block(paths: Paths) -> None:
    """Standard v7 §10.4 migration — ensure cluster.yaml has a `daemon:`
    block with auto_update defaulted to true. Existing clusters scaffolded
    under v6 pick up the new behaviour the first time they boot a v7+
    daemon. No-op when the block (or just the field) is already there,
    so operators who set `auto_update: false` keep their preference."""
    yml = paths.cluster_yaml
    if not yml.exists():
        return
    text = yml.read_text()
    # Crude detection — we don't want to round-trip parse + reserialise
    # because our YAML parser is a tiny subset and would drop comments
    # the operator may have added. Just append a block at EOF if absent.
    has_block = re.search(r"(?m)^daemon\s*:", text) is not None
    if has_block:
        # The block exists; check for auto_update inside it. If the
        # operator wrote `daemon:` with sub-keys but not auto_update,
        # leave it alone — they're aware of the section and chose not
        # to set the field, so default applies at read time.
        return
    block = (
        "\n# Standard v7 §10.4 — Daemon self-update. Written automatically by\n"
        "# the v7+ daemon on first boot. Set auto_update: false to require\n"
        "# explicit confirmation for every version update via the V47 modal.\n"
        "daemon:\n"
        "  auto_update: true\n"
        "  auto_update_source: https://meshkore.com/reference/cluster/scripts/daemon.py\n"
    )
    # Ensure there's exactly one newline between the existing tail and our
    # appended block so YAML stays valid (no trailing whitespace gymnastics).
    if not text.endswith("\n"):
        text += "\n"
    yml.write_text(text + block)


def _pick_port(paths: Paths, preferred: Optional[int]) -> int:
    """Try preferred → range 5570–5589 → fail loudly."""
    candidates: List[int] = []
    if preferred and 1024 <= preferred <= 65535:
        candidates.append(preferred)
    candidates.extend(
        p for p in range(PORT_RANGE[0], PORT_RANGE[1] + 1) if p != preferred
    )
    for p in candidates:
        if _port_free(p):
            return p
    raise SystemExit(
        f"all ports in {PORT_RANGE[0]}-{PORT_RANGE[1]} are busy; "
        f"stop a sibling daemon first or override with --port"
    )


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
