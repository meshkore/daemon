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
import struct
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ───────────────────────────────────────────────────────────────────────
# Configuration

PORT_RANGE       = (5570, 5589)
HEARTBEAT_SEC    = 20.0
FS_POLL_SEC      = 1.5
DAEMON_VERSION   = "py-1.0.0"
MAX_BODY_BYTES   = 4 * 1024 * 1024  # 4 MB — protect against runaway POSTs
WS_GUID          = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# ───────────────────────────────────────────────────────────────────────
# Paths


class Paths:
    def __init__(self, root: Path):
        self.root         = root.resolve()
        self.meshkore     = self.root / ".meshkore"
        self.public       = self.meshkore / "public"
        self.cluster_yaml = self.public / "cluster.yaml"
        # Standard §13 — deployment links registry. Optional file; the
        # registry treats a missing file as { version: 1, modules: [] }.
        self.links_yaml   = self.public / "links.yaml"
        # Standard §14 — protocols (reusable multi-scope runbooks).
        self.protocols_dir   = self.meshkore / "protocols"
        self.protocols_log   = self.protocols_dir / "log"
        self.credentials  = self.meshkore / "credentials"
        self.token_file   = self.credentials / "portal-token"
        self.runtime      = self.meshkore / ".runtime"
        self.pid_file     = self.runtime / "daemon.pid"
        self.port_file    = self.runtime / "port"
        self.timeline_dir = self.meshkore / "timeline"
        self.modules_dir  = self.meshkore / "modules"
        self.docs_dir     = self.meshkore / "docs"
        self.roadmap_dir  = self.meshkore / "roadmap"
        self.state_json   = self.roadmap_dir / "state.json"
        self.agents_dir   = self.meshkore / "agents"
        self.initiatives  = self.roadmap_dir / "initiatives"
        # Cron scheduler (D-CRON-01..05). State file is gitignored
        # under .meshkore/.runtime/; the logs dir holds per-run captures.
        self.crons_state_path = self.runtime / "crons.json"
        self.crons_logs_dir   = self.runtime / "logs" / "cron"


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
_CRON_RUN_STATUSES = frozenset({
    "pending", "running", "ok", "failed", "interrupted", "timeout",
})
_CRON_RESTART_POLICIES = frozenset({"never", "on-failure", "always"})

# Defaults applied when a `crons:` entry omits the field.
_CRON_DEFAULTS = {
    "enabled":         True,
    "max_runtime_sec": 7200,        # 2h
    "restart_policy":  "never",
    "retention_runs":  30,
    "destructive":     False,
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


def _validate_crons_block(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
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
        sched_err = _validate_cron_expr(sched) if isinstance(sched, str) else "schedule missing"
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
                errors.append(f"crons[{cid}] env {k!r}: values must be strings — dropped")
                continue
            env_clean[k] = v

        cleaned = {
            "id":              cid,
            "name":            str(entry.get("name") or cid),
            "schedule":        sched.strip(),
            "cmd":             cmd.strip(),
            "cwd":             entry.get("cwd"),
            "env":             env_clean,
            "enabled":         bool(entry.get("enabled", _CRON_DEFAULTS["enabled"])),
            "max_runtime_sec": int(entry.get("max_runtime_sec", _CRON_DEFAULTS["max_runtime_sec"])),
            "restart_policy":  policy,
            "retention_runs":  int(entry.get("retention_runs", _CRON_DEFAULTS["retention_runs"])),
            "destructive":     bool(entry.get("destructive", _CRON_DEFAULTS["destructive"])),
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
                key   = stack[-1][2]
                gp    = stack[-1][3]
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
                    k2 = k2.strip(); v2 = v2.strip()
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
                    parent.append(_coerce(_strip_inline_comment(value)) if value else None)

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
                items = [_coerce(x.strip()) for x in _split_top_level_commas(inner)] if inner else []
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
            in_str = ch; buf += ch; continue
        if ch == "," and depth == 0:
            out.append(buf); buf = ""; continue
        if ch in "[{": depth += 1
        elif ch in "]}": depth -= 1
        buf += ch
    if buf.strip():
        out.append(buf)
    return out


def _coerce(v: str) -> Any:
    s = v.strip()
    if not s:
        return ""
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
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
        self.crons_owner = owner.strip() if isinstance(owner, str) and owner.strip() else None
        if self.crons and not self.crons_owner:
            _log("cluster.yaml has crons: but no crons_owner — scheduler will tick but never fire")

    @property
    def id(self) -> str:        return str(self.data.get("id") or "unknown")
    @property
    def name(self) -> str:      return str(self.data.get("name") or self.id)
    @property
    def type(self) -> str:      return str(self.data.get("type") or "dev")
    @property
    def architect_port(self) -> Optional[int]:
        # cluster.yaml.architect.port (preferred) → fall back to legacy portal.port
        for key in ("architect", "portal"):
            sec = self.data.get(key)
            if isinstance(sec, dict) and "port" in sec:
                try:    return int(sec["port"])
                except: pass
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

_LINKS_PROVIDERS = frozenset({
    "fly", "cloudflare-pages", "cloudflare-workers",
    "vercel", "render", "self-hosted", "other",
})
_LINKS_BLOCKS = ("local", "prod", "repo")


def _validate_links_block(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
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
            errs.append(f"links.yaml: {mid}.prod.provider `{prov}` is not in the canonical set (rendered as plain text)")
        if "notes" in m:
            entry["notes"] = m["notes"]
        out.append(entry)
    return out, errs


class LinksRegistry:
    """Loads + watches .meshkore/public/links.yaml; broadcasts on change."""

    POLL_SEC = 3.0

    def __init__(self, paths: Paths, hub: "Hub"):
        self.paths = paths
        self.hub   = hub
        self.modules: List[Dict[str, Any]] = []
        self.errors: List[str]            = []
        self._mtime: Optional[float]      = None
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
        prev_sig = (tuple(sorted([m["id"] for m in self.modules])), self._mtime)
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
            self.hub.broadcast({"type": "links.updated", "modules": [m["id"] for m in self.modules]})
        return True

    def as_dict(self) -> Dict[str, Any]:
        return {
            "version":  1,
            "modules":  self.modules,
            "_errors":  self.errors,
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
                found = m; break
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
    if v is None:        return '""'
    if isinstance(v, bool): return "true" if v else "false"
    if isinstance(v, (int, float)): return str(v)
    s = str(v)
    if s == "" or any(c in s for c in ':#"\''):
        return '"' + s.replace('\\', '\\\\').replace('"', '\\"') + '"'
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
_PROTOCOL_LOG_RE  = re.compile(r"^(P\d+)-(\d{4}-\d{2}-\d{2})-[a-z0-9-]+\.md$")


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
        self.hub   = hub
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
                    "id":         pid,
                    "title":      str(fm.get("title") or pid),
                    "scope":      str(fm.get("scope") or "cluster"),
                    "status":     str(fm.get("status") or "draft"),
                    "priority":   str(fm.get("priority") or "medium"),
                    "owner":      str(fm.get("owner") or ""),
                    "updated":    str(fm.get("updated") or ""),
                    "tags":       fm.get("tags") or [],
                    "file":       fp.name,
                    "log_count":  self._count_logs(pid),
                }
                out.append(entry)
        self.protocols = out
        if broadcast:
            self.hub.broadcast({
                "type": "protocols.updated",
                "ids":  [p["id"] for p in out],
            })
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
                "id":          str(fm.get("id") or pid),
                "title":       str(fm.get("title") or pid),
                "frontmatter": fm,
                "body":        body,
                "file":        fp.name,
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
                runs.append({
                    "protocol":  pid,
                    "date":      m.group(2),
                    "file":      f"{month_dir.name}/{fp.name}",
                    "outcome":   str(fm.get("outcome") or ""),
                    "operator":  str(fm.get("operator") or ""),
                    "agent":     str(fm.get("agent") or ""),
                    "commit":    str(fm.get("commit") or ""),
                })
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
    stats = {"backlog": 0, "next": 0, "in_progress": 0, "active": 0, "blocked": 0, "done": 0, "total": 0}

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
                        "id":         str(fm.get("id")),
                        "title":      str(fm.get("title") or fm["id"]),
                        "status":     normalize_status(fm.get("status")),
                        "priority":   str(fm.get("priority") or "medium"),
                        "owner":      str(fm.get("owner") or "unknown"),
                        "category":   str(fm.get("category") or mid),
                        "created":    str(fm.get("created") or ""),
                        "updated":    str(fm.get("updated") or ""),
                        "tags":       fm.get("tags") if isinstance(fm.get("tags"), list) else [],
                        "depends_on": fm.get("depends_on") if isinstance(fm.get("depends_on"), list) else [],
                        "initiative": str(fm.get("initiative") or "") or None,
                        "path":       str(md.relative_to(paths.root)),
                    }
                    tasks.append(t)
                    by_module[t["category"]] = by_module.get(t["category"], []) + [t["id"]]
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
            docs.append({
                "title":    str(fm.get("title") or md.stem),
                "category": str(fm.get("category") or ""),
                "tags":     fm.get("tags") if isinstance(fm.get("tags"), list) else [],
                "updated":  str(fm.get("updated") or ""),
                "owner":    str(fm.get("owner") or ""),
                "status":   str(fm.get("status") or "draft"),
                "path":     str(md.relative_to(paths.root)),
            })

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
            child_ids = [t["id"] for t in tasks if (t.get("initiative") or "") == fm["id"]]
            initiatives.append({
                "id":             str(fm["id"]),
                "title":          str(fm.get("title") or fm["id"]),
                "status":         str(fm.get("status") or "backlog"),
                "priority":       str(fm.get("priority") or "medium"),
                "oneliner":       str(fm.get("oneliner") or ""),
                "modules":        fm.get("modules") if isinstance(fm.get("modules"), list) else [],
                "target":         str(fm.get("target") or ""),
                "owner":          str(fm.get("owner") or ""),
                "created":        str(fm.get("created") or ""),
                "updated":        str(fm.get("updated") or ""),
                "child_task_ids": child_ids,
                "task_total":     len(child_ids),
                "path":           str(md.relative_to(paths.root)),
            })

    return {
        "$schema":      "https://meshkore.com/standard.json",
        "cluster": {
            "id":   cluster.id,
            "name": cluster.name,
            "type": cluster.type,
        },
        "modules":      cluster.modules,
        "roadmap": {
            "tasks": tasks,
            "stats": stats,
        },
        "docs":         docs,
        "initiatives":  initiatives,
        "generated_at": _iso_now(),
        "generator":    {"name": "meshcore-py", "version": DAEMON_VERSION},
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
        self.sock   = sock
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
        try: self.sock.shutdown(socket.SHUT_RDWR)
        except OSError: pass
        try: self.sock.close()
        except OSError: pass


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


def _conversation_history(paths: "Paths", conv: str, limit: int = 12) -> List[str]:
    """Walk timeline files newest→oldest, return last `limit` turns of
    `conv` formatted as 'USER: …' / 'YOU (last turn): …'."""
    if not paths.timeline_dir.exists():
        return []
    turns: List[Tuple[str, str]] = []
    for f in sorted(paths.timeline_dir.glob("*.jsonl"), reverse=True):
        try:
            lines = f.read_text().splitlines()
        except OSError:
            continue
        for line in reversed(lines):
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            if ev.get("conv") != conv:
                continue
            t = ev.get("type")
            if t not in ("chat.user", "chat.assistant", "chat.assistant.final"):
                continue
            who = "USER" if t == "chat.user" else "YOU (last turn)"
            text = str(ev.get("text") or "").strip()
            if not text:
                continue
            turns.append((who, text[:800]))
            if len(turns) >= limit:
                break
        if len(turns) >= limit:
            break
    turns.reverse()
    return [f"{w}: {t}" for w, t in turns]


def _append_timeline(paths: "Paths", event: Dict[str, Any]) -> Dict[str, Any]:
    """Append one JSON-line event to today's timeline file.
    Returns the event enriched with `ts` if it wasn't already set."""
    paths.timeline_dir.mkdir(parents=True, exist_ok=True)
    if "ts" not in event:
        event = {**event, "ts": _iso_now()}
    date = event["ts"][:10]
    f = paths.timeline_dir / f"{date}.jsonl"
    with open(f, "a", encoding="utf-8") as out:
        out.write(json.dumps(event, separators=(",", ":")) + "\n")
    return event


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
    ):
        self.paths = paths
        self.cluster = cluster
        self.hub = hub
        self.identity = identity
        self.conv = conv
        self.prompt = prompt
        self.stream_id = f"s_{int(time.time()*1000):x}_{secrets.token_hex(2)}"
        self.pid: Optional[int] = None
        self.proc: Any = None  # subprocess.Popen
        self.done = threading.Event()
        self.cancelled = False
        self._cumulative_text = ""

    def _briefing(self) -> str:
        history = _conversation_history(self.paths, self.conv)
        history_block = ("Recent turns in this conversation:\n"
                         + "\n".join(history) + "\n") if history else ""
        try:
            port = int(self.paths.port_file.read_text().strip())
        except (OSError, ValueError):
            port = 5570
        return "\n".join([
            f"You are the coordinator agent for a MeshKore cluster at {self.paths.root}.",
            f"Identity: {self.identity}. Conversation id: {self.conv}.",
            "",
            "Read these before deciding what to do (in order, only what you need):",
            "  • https://meshkore.com/cluster/operate — operator manual",
            "  • .meshkore/docs/conventions/versioning.md — commits + versions",
            "  • .meshkore/docs/conventions/context-workflow.md — every-change checklist",
            "  • .meshkore/modules/<module>/{README.md,tasks/,log/} — per-module work",
            "",
            "Hard rules:",
            "  • Don't push to git unless the user explicitly asks.",
            f"  • Don't invent version numbers; ask POST localhost:{port}/version/next.",
            "  • Never edit .meshkore/credentials/, .meshkore/.runtime/ or generated state.json.",
            "  • Reply concisely. The portal renders your stdout as the chat answer.",
            "",
            history_block,
            "User just said:",
            self.prompt,
            "",
            'If the user is vague (e.g. "continue", "siguiente tarea", "next"), look at the roadmap (state.json or .meshkore/modules/*/tasks/) and pick the highest-priority next/in_progress task that is unblocked. Tell them what you\'re picking and why before doing the work.',
        ])

    def spawn(self) -> None:
        import subprocess
        claude_bin = _find_claude()
        if not claude_bin:
            err = "claude CLI not found — install via `npm i -g @anthropic-ai/claude-code`"
            _log(err)
            self.hub.broadcast(_append_timeline(self.paths, {
                "type": "chat.assistant.final",
                "author": self.identity,
                "conv": self.conv,
                "stream_id": self.stream_id,
                "text": f"[runner error] {err}",
            }))
            self.done.set()
            return
        args = [
            claude_bin, "-p",
            "--output-format", "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--permission-mode", "bypassPermissions",
            self._briefing(),
        ]
        env = {**os.environ,
               "MESHKORE_IDENTITY": self.identity,
               "MESHKORE_CONV": self.conv}
        self.proc = subprocess.Popen(
            args,
            cwd=str(self.paths.root),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        self.pid = self.proc.pid
        self.hub.broadcast({
            "type": "task.started",
            "id": f"chat:{self.conv}",
            "agent": self.identity,
            "ts": _iso_now(),
            "runner": "claude-code",
            "conv": self.conv,
            "stream_id": self.stream_id,
        })
        # Empty assistant bubble so the cockpit shows progress immediately.
        self.hub.broadcast({
            "type": "chat.assistant.delta",
            "author": self.identity,
            "conv": self.conv,
            "stream_id": self.stream_id,
            "text": "",
            "ts": _iso_now(),
        })
        threading.Thread(target=self._reader_loop, daemon=True).start()

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
                if (inner.get("type") == "content_block_delta"
                        and (inner.get("delta") or {}).get("type") == "text_delta"):
                    delta = (inner.get("delta") or {}).get("text") or ""
                    if delta:
                        self._cumulative_text += delta
                        now = time.monotonic()
                        if now - last_emit_at > 0.2:
                            last_emit_at = now
                            self.hub.broadcast({
                                "type": "chat.assistant.delta",
                                "author": self.identity,
                                "conv": self.conv,
                                "stream_id": self.stream_id,
                                "text": self._cumulative_text[:16000],
                                "ts": _iso_now(),
                            })
                elif (inner.get("type") == "content_block_start"
                      and (inner.get("content_block") or {}).get("type") == "tool_use"):
                    cb = inner.get("content_block") or {}
                    self.hub.broadcast({
                        "type": "tool.use",
                        "author": self.identity,
                        "conv": self.conv,
                        "stream_id": self.stream_id,
                        "tool": cb.get("name"),
                        "input": cb.get("input"),
                        "ts": _iso_now(),
                    })
                continue
            if ev_type == "user":
                for c in (ev.get("message") or {}).get("content") or []:
                    if isinstance(c, dict) and c.get("type") == "tool_result":
                        self.hub.broadcast({
                            "type": "tool.result",
                            "author": self.identity,
                            "conv": self.conv,
                            "stream_id": self.stream_id,
                            "ok": not c.get("is_error"),
                            "ts": _iso_now(),
                        })
                continue
            if ev_type == "result" and isinstance(ev.get("result"), str):
                result_text = ev["result"]
        # Finalize
        final_text = result_text or self._cumulative_text
        self.hub.broadcast(_append_timeline(self.paths, {
            "type": "chat.assistant.final",
            "author": self.identity,
            "conv": self.conv,
            "stream_id": self.stream_id,
            "text": final_text,
        }))
        self.hub.broadcast({
            "type": "task.finished",
            "id": f"chat:{self.conv}",
            "ts": _iso_now(),
            "exit": self.proc.wait() if self.proc else None,
            "conv": self.conv,
        })
        self.done.set()


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
            merged = (pending[0] if len(pending) == 1
                      else "Several follow-up instructions while you were working:\n\n"
                           + "\n\n".join(f"{i+1}. {t}" for i, t in enumerate(pending)))
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
    hour_set   = _parse_cron_field(parts[1], 0, 23)
    dom_set    = _parse_cron_field(parts[2], 1, 31)
    month_set  = _parse_cron_field(parts[3], 1, 12)
    # Cron dow: Sunday=0..Saturday=6. Python's weekday(): Monday=0..Sunday=6.
    # Convert at match time with (py + 1) % 7.
    dow_set    = _parse_cron_field(parts[4], 0, 6)
    t = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(60 * 24 * 366 * 4):
        if (t.minute in minute_set
                and t.hour in hour_set
                and t.month in month_set
                and t.day in dom_set
                and ((t.weekday() + 1) % 7) in dow_set):
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
    nvm = sorted(_glob.glob(os.path.expanduser("~/.nvm/versions/node/v*/bin")), reverse=True)
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
        self._active: Dict[str, Any] = {}   # job_id → subprocess.Popen
        self._lock = threading.Lock()

    def is_running(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._active

    def spawn(self, job: Dict[str, Any], reason: str = "scheduled") -> Optional[Dict[str, Any]]:
        """Fire one run of `job`. Returns the started Run dict, or
        None if the job is already running (no concurrent fires)."""
        import subprocess
        jid = job["id"]
        with self._lock:
            if jid in self._active:
                self.hub.broadcast({
                    "type": "cron.skipped", "id": jid,
                    "reason": "already running",
                    "ts": _iso_now(),
                })
                return None
        env = self._resolve_env(job.get("env") or {})
        log_path = self._make_log_path(jid)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        ts = _iso_now()
        try:
            log_handle = open(log_path, "ab")
            proc = subprocess.Popen(
                job["cmd"], shell=True,
                cwd=str(self.paths.root),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log_handle, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except Exception as e:
            _log(f"cron spawn FAIL {jid}: {e}")
            self.hub.broadcast({
                "type": "cron.error", "id": jid, "error": str(e), "ts": ts,
            })
            return None
        with self._lock:
            self._active[jid] = proc
        self.hub.broadcast({
            "type": "cron.fired", "id": jid, "reason": reason,
            "pid": proc.pid, "log": str(log_path.relative_to(self.paths.root)),
            "ts": ts,
        })
        run = {"id": jid, "started_at": ts, "pid": proc.pid, "log_path": str(log_path), "status": "running"}
        threading.Thread(
            target=self._wait_for, args=(jid, proc, log_handle, job, log_path, ts),
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

    def _wait_for(self, jid: str, proc, log_handle, job: Dict[str, Any], log_path: Path, started_at: str) -> None:
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
        self.hub.broadcast({
            "type": "cron.finished", "id": jid,
            "exit": exit_code, "status": status,
            "duration_sec": round(time.monotonic() - t0, 1),
            "log": str(log_path.relative_to(self.paths.root)),
            "ts": _iso_now(),
        })

    def _resolve_env(self, job_env: Dict[str, str]) -> Dict[str, str]:
        env = dict(os.environ)
        curated = _curated_path_entries()
        if curated:
            env["PATH"] = ":".join(curated) + ":" + env.get("PATH", "")
        for k, v in job_env.items():
            if not isinstance(v, str) or not isinstance(k, str):
                continue
            if v.startswith("file:"):
                rel = v[len("file:"):]
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
        self._jobs: Dict[str, Dict[str, Any]] = {}    # job_id → {job, next_run}
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
            owner_status = "coordinator" if self.is_coordinator() else f"peer (owner={self.cluster.crons_owner})"
            _log(f"cron: {n} job(s) registered, this daemon is {owner_status}, tick every {self.TICK_SEC}s")
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
                self.hub.broadcast({
                    "type": "cron.would_have_fired", "id": jid,
                    "scheduled_for": scheduled_for.isoformat(),
                    "reason": f"not coordinator (owner={self.cluster.crons_owner!r}, me={self.identity!r})",
                    "ts": _iso_now(),
                })

    # ── introspection ───────────────────────────────────────────────
    def list_jobs(self) -> List[Dict[str, Any]]:
        out = []
        with self._lock:
            for jid, state in self._jobs.items():
                out.append({
                    **state["job"],
                    "next_run": state["next_run"].isoformat(),
                    "running": self.runner.is_running(jid),
                })
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
        self.paths   = paths
        self.cluster = cluster
        self.hub     = hub
        self._state: Dict[str, Any] = {}
        self._stop  = threading.Event()
        self._lock  = threading.Lock()
        self._fs_signature = ""
        self.rebuild()
        threading.Thread(target=self._poll_loop, daemon=True).start()

    def state(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._state)

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
        for root in (self.paths.modules_dir, self.paths.docs_dir,
                     self.paths.initiatives, self.paths.public):
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
            self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")

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
            self.send_response(204); self._cors(); self.end_headers()

        def do_GET(self):  # noqa: N802
            p, q = self._path()
            # WebSocket upgrade?
            if p in ("/events", "/ws") and self.headers.get("Upgrade", "").lower() == "websocket":
                return self._handle_ws()
            if p == "/health":
                return self._json(200, daemon.health())
            if p == "/state":
                return self._json(200, daemon.state_manager.state())
            # U-DAEMON-02: subset reads. Matches Node's contract:
            # GET /state/cluster, /state/modules, /state/roadmap, etc.
            if p.startswith("/state/"):
                sub = p[len("/state/"):].strip("/")
                state = daemon.state_manager.state()
                if sub in state:
                    return self._json(200, state[sub])
                return self._json(404, {"error": "unknown subset", "subset": sub})
            if p == "/reload":
                if self._need_auth(): return
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
                if self._need_auth(): return
                return self._serve_meshkore_file(daemon.paths.docs_dir, p[len("/docs/"):])
            if p.startswith("/modules/"):
                if self._need_auth(): return
                return self._serve_meshkore_file(daemon.paths.modules_dir, p[len("/modules/"):])
            if p.startswith("/tasks/"):
                if self._need_auth(): return
                return self._serve_meshkore_file(daemon.paths.roadmap_dir, p[len("/tasks/"):])
            # U-DAEMON-02: credentials listing — names only, never
            # contents. Matches Node's response shape.
            if p == "/credentials":
                if self._need_auth(): return
                return self._json(200, daemon.credentials_listing())
            # D-CRON-02..05: scheduler introspection.
            if p == "/cron/list":
                if self._need_auth(): return
                return self._json(200, {
                    "jobs": daemon.cron_scheduler.list_jobs(),
                    "coordinator": daemon.cron_scheduler.is_coordinator(),
                    "owner": daemon.cluster.crons_owner,
                    "identity": daemon.identity,
                    "tick_sec": daemon.cron_scheduler.TICK_SEC,
                })
            # Standard §13 — deployment links registry.
            if p == "/links":
                daemon.links_registry.reload()
                return self._json(200, daemon.links_registry.as_dict())
            if p.startswith("/links/"):
                mid = urllib.parse.unquote(p[len("/links/"):]).strip("/")
                if not mid:
                    return self._json(400, {"error": "module id required"})
                daemon.links_registry.reload()
                entry = daemon.links_registry.get(mid)
                if entry is None:
                    return self._json(404, {"error": "module not in links.yaml", "id": mid})
                return self._json(200, entry)
            # Standard §14 — protocols registry.
            if p == "/protocols":
                daemon.protocols_registry.reload()
                return self._json(200, {"protocols": daemon.protocols_registry.list()})
            if p.startswith("/protocols/"):
                rest = urllib.parse.unquote(p[len("/protocols/"):]).strip("/")
                if not rest:
                    return self._json(400, {"error": "protocol id required"})
                if rest.endswith("/runs"):
                    pid = rest[:-len("/runs")]
                    return self._json(200, {"runs": daemon.protocols_registry.runs(pid)})
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
                ".md":   "text/markdown; charset=utf-8",
                ".json": "application/json; charset=utf-8",
                ".yaml": "text/yaml; charset=utf-8",
                ".yml":  "text/yaml; charset=utf-8",
                ".txt":  "text/plain; charset=utf-8",
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
                if self._need_auth(): return
                self._json(200, {"ok": True, "shutting_down": True, "ts": _iso_now()})
                threading.Thread(target=daemon.request_shutdown, daemon=True).start()
                return

            # All other POSTs need auth.
            if self._need_auth(): return

            # U-DAEMON-06: chat dispatch + cancel.
            if p == "/chat/dispatch":
                return self._json(*daemon.chat_dispatch(self._read_json_body()))
            if p == "/chat/cancel":
                return self._json(*daemon.chat_cancel(self._read_json_body()))

            # U-DAEMON-09: simple message append + version stubs.
            if p == "/messages":
                return self._json(*daemon.append_message(self._read_json_body()))
            if p == "/version/next":
                return self._json(501, {
                    "error": "version coordinator not implemented yet",
                    "see": "modules/daemon/tasks/V20-version-coordinator.md",
                })

            # U-DAEMON-04: task lifecycle.
            if p == "/tasks":
                return self._json(*daemon.task_create(self._read_json_body()))
            if p.startswith("/tasks/") and p.endswith("/transition"):
                tid = p[len("/tasks/"):-len("/transition")]
                return self._json(*daemon.task_transition(tid, self._read_json_body()))
            if p.startswith("/tasks/") and p.endswith("/cancel"):
                tid = p[len("/tasks/"):-len("/cancel")]
                return self._json(*daemon.task_cancel(tid))
            if p.startswith("/tasks/") and p.endswith("/dispatch"):
                # U-DAEMON-07 territory — spawn a runner for a task.
                # Stub for now: return 501 so cockpit shows a clear error.
                return self._json(501, {
                    "error": "task dispatch (runner) not implemented yet",
                    "hint": "follows U-DAEMON-07 worker pool port",
                })

            # U-DAEMON-03 finish: declare a new agent.
            if p == "/agents":
                return self._json(*daemon.agent_create(self._read_json_body()))

            # D-CRON-04: trigger + cancel a cron job.
            if p.startswith("/cron/") and p.endswith("/trigger"):
                jid = p[len("/cron/"):-len("/trigger")]
                run = daemon.cron_scheduler.trigger(jid, reason="manual-trigger")
                if run is None:
                    return self._json(404, {"error": f"no cron job named {jid!r} (or already running)"})
                return self._json(202, run)
            if p.startswith("/cron/") and p.endswith("/cancel"):
                jid = p[len("/cron/"):-len("/cancel")]
                ok = daemon.cron_scheduler.runner.cancel(jid)
                return self._json(200, {"ok": ok, "id": jid, "cancelled": ok})
            # Standard §13 — patch a module's entry in links.yaml.
            if p.startswith("/links/"):
                if self._need_auth(): return
                mid = urllib.parse.unquote(p[len("/links/"):]).strip("/")
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
                self.send_error(400); return
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
            client.send_text(json.dumps({
                "type":    "hello",
                "identity": daemon.identity,
                "port":     daemon.port,
                "ts":       _iso_now(),
            }))
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
    payload  = _recv_exact(sock, length)
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
    def __init__(self, paths: Paths, identity: Optional[str], requested_port: Optional[int]):
        self.paths           = paths
        self.cluster         = Cluster(paths)
        self.identity        = identity or _detect_identity(paths) or _hostname_default()
        self.token           = _ensure_token(paths)
        self.port            = _pick_port(paths, requested_port or self.cluster.architect_port)
        self.hub             = Hub()
        self.state_manager   = StateManager(paths, self.cluster, self.hub)
        self.chat_sessions   = ChatSessions()
        # Standard §13 — deployment links registry. Quiet no-op when
        # .meshkore/public/links.yaml is absent.
        self.links_registry  = LinksRegistry(paths, self.hub)
        # Standard §14 — protocols registry. Quiet no-op when
        # .meshkore/protocols/ is absent.
        self.protocols_registry = ProtocolsRegistry(paths, self.hub)
        # D-CRON-02..05: tick loop + runner; started in serve_forever()
        self.cron_scheduler  = CronScheduler(paths, self.cluster, self.hub, self.identity)
        self.stopping        = threading.Event()
        self.server: Optional[ThreadingHTTPServer] = None

    # ── U-DAEMON-06: chat coordinator ──────────────────────────────────
    def _spawn_chat_turn(self, conv: str, prompt: str) -> ChatRunner:
        """Start one chat turn. Wires the chain so a buffered next
        prompt re-spawns automatically when the current turn finishes."""
        runner = ChatRunner(
            paths=self.paths, cluster=self.cluster, hub=self.hub,
            identity=self.identity, conv=conv, prompt=prompt,
        )
        runner.spawn()
        self.chat_sessions.start(conv, runner, on_chain=self._spawn_chat_turn)
        return runner

    def chat_dispatch(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        text = str(body.get("text") or "").strip()
        if not text:
            return 400, {"error": "text required"}
        author = str(body.get("author") or self.identity)
        conv = str(body.get("conv") or f"chat-{_iso_now()[:16].replace(':', '-').replace('T', '-').lower()}")
        # 1) Emit + persist the user event right away.
        ev = _append_timeline(self.paths, {
            "type": "chat.user", "author": author, "text": text, "conv": conv,
        })
        self.hub.broadcast(ev)
        # 2) Queue if a turn is already running for this conv.
        if self.chat_sessions.has(conv):
            pending = self.chat_sessions.queue(conv, text)
            return 202, {
                "queued": True, "conv": conv, "pending": pending,
                "message": "turn in progress — your prompt will be merged into the next turn",
            }
        # 3) New turn.
        try:
            runner = self._spawn_chat_turn(conv, text)
        except Exception as e:
            return 400, {"error": str(e)}
        return 202, {
            "conv": conv, "runner": "claude-code",
            "identity": self.identity, "pid": runner.pid,
            "stream_id": runner.stream_id,
        }

    def chat_cancel(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        conv = str(body.get("conv") or "").strip()
        if not conv:
            return 400, {"error": "conv required"}
        cancelled, dropped = self.chat_sessions.cancel(conv)
        if not cancelled:
            return 200, {"ok": True, "cancelled": False,
                         "reason": "no active turn for that conv"}
        self.hub.broadcast({
            "type": "chat.cancelled", "conv": conv,
            "ts": _iso_now(), "dropped_pending": dropped,
        })
        return 200, {"ok": True, "cancelled": True, "dropped_pending": dropped}

    # ── U-DAEMON-09: message append + version stubs ────────────────────
    def append_message(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        text = str(body.get("text") or "").strip()
        if not text:
            return 400, {"error": "text required"}
        author = str(body.get("author") or self.identity)
        conv = str(body.get("conv") or "general")
        ev = _append_timeline(self.paths, {
            "type": "message", "author": author, "text": text, "conv": conv,
        })
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
        frontmatter = "\n".join([
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
        ])
        target.write_text(frontmatter)
        self.state_manager.rebuild(broadcast=True)
        return 201, {"id": tid, "path": str(target.relative_to(self.paths.root))}

    def task_transition(self, tid: str, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
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
            new = re.sub(r"^---\s*$\n", f"---\nstatus: {to}\n", text, count=1, flags=re.M)
        path.write_text(new)
        self.state_manager.rebuild(broadcast=True)
        return 200, {"id": tid, "from": "?", "to": to, "path": str(path.relative_to(self.paths.root))}

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
        return {
            "ok":           True,
            "identity":     self.identity,
            "port":         self.port,
            "mode":         "server",
            "implementation": "python",
            "version":      DAEMON_VERSION,
            "cluster_id":   self.cluster.id,
            "cluster_name": self.cluster.name,
            "cluster_type": self.cluster.type,
            # U-DAEMON-01: capability advertisement.
            # During the Node→Python unification (initiative
            # `unified-python-daemon`), the cockpit reads this array
            # to route each call to the daemon that supports the
            # feature. Adding an endpoint here is part of the
            # acceptance criteria for that endpoint's port task.
            "features":     self._features(),
            "ts":           _iso_now(),
        }

    def _features(self) -> List[str]:
        feats = [
            "health",
            "state", "state.subset",            # U-DAEMON-02
            "reload",
            "agents", "agents.create",          # U-DAEMON-02 + 03
            "events",                            # WS hub + chat.* + task.* + tool.*
            "files.docs", "files.modules", "files.tasks",  # U-DAEMON-02
            "credentials",                       # U-DAEMON-02 (list-only)
            "info",
            "shutdown",
            # U-DAEMON-04 task lifecycle (dispatch is stubbed, marked separately)
            "tasks.create", "tasks.transition", "tasks.cancel",
            # U-DAEMON-05 + 06 chat coordinator
            "chat", "chat.cancel",
            # U-DAEMON-09 misc
            "messages",
        ]
        if hasattr(self.cluster, "crons"):
            feats.append("cron.schema")
        # D-CRON-02..05: scheduler is live, list + trigger + cancel + log endpoints.
        feats.extend(["cron.tick", "cron.list", "cron.trigger", "cron.cancel", "cron.log"])
        # Standard §13: deployment links registry.
        feats.extend(["links.read", "links.write"])
        # Standard §14: protocols registry (read-only this version).
        feats.extend(["protocols.read"])
        # Stubs — advertised separately so the cockpit can show
        # "not yet" badges without trying the endpoint.
        feats.extend(["stub.workers", "stub.admission", "stub.tasks.dispatch", "stub.version.next"])
        return feats

    def info(self) -> Dict[str, Any]:
        h = self.health()
        h["version"] = DAEMON_VERSION
        h["paths"]   = {
            "root":     str(self.paths.root),
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
            out.append({
                "id":       yml.stem,
                "identity": yml.stem,           # alias, matches Node
                "pid":      pid,
                "online":   online,
                "data":     data,
            })
        return out

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
            out.append({
                "name":      f.name,
                "size":      size,
                "is_symlink": f.is_symlink(),
            })
        return out

    # ── lifecycle ──────────────────────────────────────────────────────
    def serve_forever(self) -> None:
        self._write_runtime()
        handler = make_handler(self)
        self.server = ThreadingHTTPServer(("127.0.0.1", self.port), handler)
        self.server.daemon_threads = True
        _log(f"meshcore-py listening on http://127.0.0.1:{self.port} "
             f"(identity={self.identity}, cluster={self.cluster.id})")
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
            if self.paths.pid_file.exists() and self.paths.pid_file.read_text().strip() == str(os.getpid()):
                self.paths.pid_file.unlink()
        except OSError:
            pass
        try:
            if self.paths.port_file.exists() and self.paths.port_file.read_text().strip() == str(self.port):
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
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
           f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"


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


def _port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _pick_port(paths: Paths, preferred: Optional[int]) -> int:
    """Try preferred → range 5570–5589 → fail loudly."""
    candidates: List[int] = []
    if preferred and 1024 <= preferred <= 65535:
        candidates.append(preferred)
    candidates.extend(p for p in range(PORT_RANGE[0], PORT_RANGE[1] + 1) if p != preferred)
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
            print(__doc__); raise SystemExit(0)
        if a == "--version":
            print(f"meshcore-py {DAEMON_VERSION}"); raise SystemExit(0)
        if a == "--identity":
            out["identity"] = argv[i + 1]; i += 2; continue
        if a == "--port":
            out["port"] = int(argv[i + 1]); i += 2; continue
        if a == "--root":
            out["root"] = Path(argv[i + 1]); i += 2; continue
        # Positional default = root
        if not out["root"]:
            out["root"] = Path(a); i += 1; continue
        print(f"unknown arg: {a}", file=sys.stderr); raise SystemExit(2)
    if not out["root"]:
        out["root"] = Path.cwd()
    return out


def main() -> None:
    args   = _parse_args(sys.argv[1:])
    paths  = Paths(args["root"])
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
        try: signal.signal(sig, _on_signal)
        except ValueError: pass  # Windows main-thread quirk; ignore

    daemon.serve_forever()
    _log("daemon stopped cleanly")


if __name__ == "__main__":
    main()
