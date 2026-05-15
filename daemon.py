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
from datetime import datetime, timezone
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
        self.reload()

    def reload(self) -> None:
        if not self.paths.cluster_yaml.exists():
            raise SystemExit(
                f"\n .meshkore/public/cluster.yaml not found at {self.paths.cluster_yaml}."
                "\n   Run `meshcore init` (or hand-author cluster.yaml from"
                "\n   https://meshkore.com/reference/cluster/templates/) and re-run.\n"
            )
        self.data = parse_simple_yaml(self.paths.cluster_yaml.read_text())

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
            if p == "/reload":
                if self._need_auth(): return
                daemon.state_manager.rebuild(broadcast=True)
                return self._json(200, {"ok": True, "generated_at": _iso_now()})
            if p == "/agents":
                return self._json(200, daemon.agents_listing())
            if p == "/info":
                return self._json(200, daemon.info())
            return self._json(404, {"error": "not found", "path": p})

        def do_POST(self):  # noqa: N802
            p, _ = self._path()
            if p == "/shutdown":
                if self._need_auth(): return
                self._json(200, {"ok": True, "shutting_down": True, "ts": _iso_now()})
                threading.Thread(target=daemon.request_shutdown, daemon=True).start()
                return
            return self._json(404, {"error": "not found", "path": p})

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
        self.stopping        = threading.Event()
        self.server: Optional[ThreadingHTTPServer] = None

    # ── HTTP body for /health and /info ────────────────────────────────
    def health(self) -> Dict[str, Any]:
        return {
            "ok":           True,
            "identity":     self.identity,
            "port":         self.port,
            "mode":         "server",
            "implementation": "python",
            "cluster_id":   self.cluster.id,
            "cluster_name": self.cluster.name,
            "cluster_type": self.cluster.type,
            "ts":           _iso_now(),
        }

    def info(self) -> Dict[str, Any]:
        h = self.health()
        h["version"] = DAEMON_VERSION
        h["paths"]   = {
            "root":     str(self.paths.root),
            "meshkore": str(self.paths.meshkore),
        }
        return h

    def agents_listing(self) -> List[Dict[str, Any]]:
        if not self.paths.agents_dir.exists():
            return []
        out = []
        for yml in sorted(self.paths.agents_dir.glob("*.yaml")):
            try:
                data = parse_simple_yaml(yml.read_text())
            except OSError:
                continue
            out.append({
                "id":     yml.stem,
                "data":   data,
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
        try:
            self.server.serve_forever(poll_interval=0.5)
        finally:
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
