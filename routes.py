"""HTTP + WebSocket request handler — the daemon's wire surface.

``make_handler(daemon)`` returns a ``BaseHTTPRequestHandler`` subclass
that closes over the daemon instance. ``do_GET`` / ``do_POST`` dispatch
to per-route handlers; the WS upgrade path uses ``_handle_ws`` →
``_ws_read_frame`` / ``_recv_exact``.

Coupling: this module is dense in daemon-method calls
(``daemon.health()``, ``daemon.chat_snapshot()``,
``daemon.state_manager.state()``, ``daemon.cluster``, ``daemon.paths``,
``daemon.hub``, …). That coupling is by design — the handler IS the
daemon's wire surface and would have nothing to do without the daemon
reference. Constructor-style "all deps explicit" would mean 30+
parameters; the closure is cleaner.

Bundler note: ``BaseHTTPRequestHandler``, ``_log``, ``_iso_now``,
``_debug_emit``, ``_DEBUG_LOG`` are looked up at call time. Local
stubs at the top are shadowed by daemon.py's real definitions in
``dist/daemon.py``. The daemon version string is reached via
``daemon.daemon_version`` (instance attribute) — single source of
truth in daemon.py, no per-module duplication."""

from __future__ import annotations

import base64
import hashlib
import json
import socket
import struct
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from hub import WSClient
import traceback  # py-1.16.0 (D-HTTP-500-01) — handler exception guard

from routes_get import route_get
from routes_post import route_post
from utils import _debug_emit, _iso_now  # DM7

MAX_BODY_BYTES = 4 * 1024 * 1024
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def make_handler(daemon: Any):
    class Handler(BaseHTTPRequestHandler):
        # py-1.16.2 — HTTP keep-alive. The default is HTTP/1.0, so the
        # cockpit opened a NEW TLS connection for EVERY fetch (/state,
        # per-conv /chat queues, health polls). Each closed into TIME_WAIT;
        # under a busy cockpit that exhausted the OS ephemeral-port table
        # (30k+ TIME_WAIT observed) and NEW connections — including the
        # cockpit's own /state — stalled in SYN_SENT and timed out. THIS is
        # the chronic "connection refused / timeout / blank chats" we kept
        # chasing through 1.15.x. HTTP/1.1 lets the browser + httpx reuse
        # one connection for many requests → connection count collapses, no
        # TIME_WAIT storm. Safe: every body response sends Content-Length;
        # 204 (OPTIONS) and 101 (WS upgrade) are bodyless.
        protocol_version = "HTTP/1.1"
        # A kept-alive connection blocks a pool worker in handle() waiting
        # for the next request; this read timeout closes truly-idle ones so
        # the worker is freed (handle_one_request treats the timeout as a
        # clean close).
        timeout = 30

        def log_message(self, fmt, *args):  # silence default access log
            return

        def setup(self):
            # Capture a connection-open stamp; the real per-request stamp
            # is set in handle_one_request below.
            self._http_t0 = time.time()
            super().setup()

        def handle_one_request(self):
            # py-1.17.1 — stamp the request-start time PER REQUEST. Under
            # HTTP/1.1 keep-alive (py-1.16.2) one Handler instance serves
            # MANY requests, so `setup()` runs once per CONNECTION, not per
            # request. The old code left `_http_t0` at connection-open, so
            # log_request reported the CONNECTION AGE (e.g. "17838 ms" for
            # an instant OPTIONS on an old kept-alive socket) instead of the
            # request latency — badly misleading the debug/http stream.
            self._http_t0 = time.time()
            super().handle_one_request()

        def log_request(self, code="-", size="-"):  # noqa: D401
            # py-1.10.19 — emit one structured `http` event per response.
            # Mutes `/health` and `/state` (polled every ~2 s by the
            # cockpit; would drown the stream). `send_response` calls
            # this for both `_json()` and `send_error()` paths, so it's
            # the single funnel for every wire-level reply.
            try:
                path_only = urllib.parse.urlsplit(self.path or "").path
                if path_only in ("/health", "/state"):
                    return
                if path_only.startswith("/state/"):
                    return
                try:
                    code_int = int(code)
                except (TypeError, ValueError):
                    code_int = 0
                dur_ms = int(
                    (time.time() - getattr(self, "_http_t0", time.time())) * 1000
                )
                lvl = "warn" if code_int >= 400 else "info"
                _debug_emit(
                    "http",
                    msg=f"{self.command} {path_only} → {code_int} ({dur_ms} ms)",
                    lvl=lvl,
                    data={
                        "method": self.command,
                        "path": path_only,
                        "status": code_int,
                        "duration_ms": dur_ms,
                    },
                )
            except Exception:
                pass

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

        def _allowed_origin(self) -> Optional[str]:
            """Reflect the request Origin only if it's a MeshKore cockpit
            surface or a loopback dev server. Returns the origin to echo,
            or None (→ omit Allow-Origin so the browser blocks the
            cross-origin read). py-1.27.4 — replaces the blanket `*`."""
            origin = self.headers.get("Origin")
            if not origin:
                return None
            try:
                host = (urllib.parse.urlsplit(origin).hostname or "").lower()
            except Exception:
                return None
            if (
                host == "meshkore.com"
                or host.endswith(".meshkore.com")  # architect., www., etc.
                or host.endswith(".pages.dev")  # Cloudflare Pages previews
                or host in ("localhost", "127.0.0.1", "::1")  # dev / diagnostic
            ):
                return origin
            return None

        def _cors(self) -> None:
            # py-1.27.4 — reflect an ALLOWLISTED origin instead of `*`. The
            # bearer token gates writes, but the OPEN read routes (/state,
            # /agents, /info, /storage/usage) would otherwise be cross-origin
            # readable by ANY website the operator visits while the daemon
            # runs (project state / agent roster / storage leak). We now
            # reflect only the cockpit origins (*.meshkore.com + CF previews)
            # and loopback; everything else gets NO Allow-Origin, so the
            # browser blocks the read. Same-origin / non-browser callers
            # (no Origin header) are unaffected — local CLI tools still work.
            allowed = self._allowed_origin()
            if allowed:
                self.send_header("Access-Control-Allow-Origin", allowed)
                self.send_header("Vary", "Origin")
                self.send_header(
                    "Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS"
                )
                self.send_header(
                    "Access-Control-Allow-Headers",
                    "Authorization, Content-Type, X-MeshKore-Project",
                )
                # py-1.9.1 — Chrome Local Network Access preflight opt-in, so
                # the cockpit's plain-http://localhost diagnostic probe works.
                self.send_header("Access-Control-Allow-Private-Network", "true")
            # Wire-version contract (py-1.2.0) — always present so a stale
            # daemon is detected without a separate /health round-trip.
            self.send_header("X-MeshKore-Daemon-Version", daemon.daemon_version)
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

        def _guard(self, fn, verb: str) -> None:
            # py-1.16.0 (D-HTTP-500-01) — turn an unexpected handler error
            # into a 500 JSON instead of letting it propagate to
            # BaseHTTPRequestHandler, which closes the socket with NO
            # response → the cockpit sees a bare connection reset (000),
            # indistinguishable from "daemon dead". A client abort is
            # normal and stays quiet.
            #
            # DC-4 (daemon-centralized) — resolve the request's project from the
            # `X-MeshKore-Project` header BEFORE dispatch so the per-project
            # property accessors (daemon.paths/cluster/runs/…) hit the right
            # ProjectContext, and clear it after. The same try/except is the
            # per-request isolation boundary: one project's handler error is a
            # 500 for that request, never a crash of the shared daemon or the
            # other projects.
            # Drain + cache the request body UP-FRONT so an early-reject handler
            # (401 unauth, 400 bad-input, 503 not-ready) never leaves it unread.
            # On an HTTP/1.1 keep-alive socket an unread body makes the NEXT
            # request parse the leftover bytes as its request line → "400 Bad
            # request syntax" → the browser reports it as a CORS failure. The
            # cockpit's debug-transport POSTs /debug/log before it holds a token
            # (→401), so this bit hard the moment one daemon served the HTTPS
            # cockpit. _read_json_body() now parses this cache. (daemon-centralized)
            try:
                _clen = int(self.headers.get("Content-Length") or 0)
                self._raw_body = (
                    self.rfile.read(_clen) if 0 < _clen <= MAX_BODY_BYTES else b""
                )
            except Exception:
                self._raw_body = b""
            # Project resolution: prefer the X-MeshKore-Project header, but fall
            # back to a ?project=<id> query param. The browser's <img> loader (and
            # any raw URL: download links, anchor hrefs) CANNOT send a custom
            # header, so chat-upload images would otherwise resolve against the
            # default project and 404. (daemon-centralized FC-2)
            _proj = self.headers.get("X-MeshKore-Project")
            if not _proj:
                try:
                    _qs = urllib.parse.urlparse(self.path).query
                    _proj = urllib.parse.parse_qs(_qs).get("project", [None])[0]
                except Exception:
                    _proj = None
            daemon._set_req_project(_proj)
            try:
                fn()
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception:
                traceback.print_exc()  # → stderr → daemon.log
                try:
                    self._json(500, {"error": "internal daemon error", "verb": verb})
                except Exception:
                    pass  # response already (partly) sent — nothing safe to do
            finally:
                daemon._clear_req_project()

        def do_GET(self):  # noqa: N802
            self._guard(self._do_GET, "GET")

        def do_POST(self):  # noqa: N802
            self._guard(self._do_POST, "POST")

        def _do_GET(self):  # noqa: N802
            return route_get(self, daemon)

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

        def _do_POST(self):  # noqa: N802
            return route_post(self, daemon)

        def do_PUT(self):  # noqa: N802
            self._guard(self._do_PUT, "PUT")

        def _do_PUT(self):  # noqa: N802
            p, _ = self._path()
            if self._need_auth():
                return
            if p.startswith("/credentials/"):
                name = p[len("/credentials/") :]
                body = self._read_json_body()
                value = body.get("value") if isinstance(body, dict) else None
                code, resp = daemon.credential_write(
                    name, value if isinstance(value, str) else ""
                )
                return self._json(code, resp)
            return self._json(404, {"error": "not found", "path": p})

        def do_DELETE(self):  # noqa: N802
            self._guard(self._do_DELETE, "DELETE")

        def _do_DELETE(self):  # noqa: N802
            p, _ = self._path()
            if self._need_auth():
                return
            if p.startswith("/credentials/"):
                name = p[len("/credentials/") :]
                code, resp = daemon.credential_delete(name)
                return self._json(code, resp)
            # DC-5 (daemon-centralized) — GLOBAL: unregister a project (does
            # NOT delete its ledger on disk).
            if p.startswith("/projects/"):
                pid = p[len("/projects/") :]
                return self._json(*daemon.project_unregister(pid))
            # py-1.12.19 — Standard v16 queue: remove one item.
            #   DELETE /chat/conv/<id>/queue/<itemId>
            if p.startswith("/chat/conv/"):
                rest = p[len("/chat/conv/") :]
                if "/queue/" in rest:
                    cid_part, _, item_id_part = rest.partition("/queue/")
                    cid = urllib.parse.unquote(cid_part)
                    item_id = urllib.parse.unquote(item_id_part)
                    if not cid or not item_id:
                        return self._json(400, {"error": "conv + item id required"})
                    removed = daemon.chat_queue_manager.remove(cid, item_id)
                    if removed is None:
                        return self._json(404, {"error": "item not found"})
                    return self._json(200, {"conv": cid, "item": removed})
            return self._json(404, {"error": "not found", "path": p})

        # ── helpers used by do_POST handlers ───────────────────────────
        def _read_json_body(self) -> Dict[str, Any]:
            # Parse the body that _guard already drained + cached on self._raw_body
            # (NOT a fresh self.rfile.read — that would block, the bytes are gone).
            raw = getattr(self, "_raw_body", b"")
            if not raw:
                return {}
            try:
                data = json.loads(raw.decode("utf-8"))
                return data if isinstance(data, dict) else {}
            except (json.JSONDecodeError, UnicodeDecodeError):
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
            # Flush the 101 onto the wire BEFORE the ws-pump starts
            # sending frames (else the client sees a frame before the
            # upgrade response → protocol error).
            try:
                self.wfile.flush()
            except OSError:
                return
            sock = self.connection
            sock.settimeout(None)
            # py-1.16.0 (D-WS-01) — hand the lifelong inbound read loop to
            # a DEDICATED thread instead of blocking this PoolHTTPServer
            # worker for the WS's whole life. Pinning bounded workers
            # (max_workers=64) on long-lived WS could starve HTTP
            # (/state, /health, /chat) of workers. We register the socket
            # as hijacked so the server's shutdown_request does NOT close
            # it when this (now-freed) worker returns; the ws-pump thread
            # owns and closes it. `close_connection=True` exits the
            # keep-alive loop so the worker returns immediately.
            hijacked = False
            srv = getattr(self, "server", None)
            reg = getattr(srv, "_ws_hijacked", None)
            if reg is not None:
                reg.add(id(sock))
                hijacked = True
            self.close_connection = True
            client = WSClient(sock)
            daemon.hub.add(client)
            if hijacked:
                threading.Thread(
                    target=_ws_pump,
                    args=(daemon, client, sock, srv),
                    name="ws-pump",
                    daemon=True,
                ).start()
            else:
                # Fallback (server without the hijack registry): keep the
                # old in-worker behaviour so WS still functions.
                _ws_pump(daemon, client, sock, None)

    return Handler


def _ws_pump(daemon, client, sock, server) -> None:
    """Drive one WebSocket: greet, then drain inbound frames until close.
    Runs on a dedicated thread (D-WS-01) so it never holds a request-pool
    worker. Owns the socket: closes it (via hub.remove) on exit and
    deregisters the hijack so the fd can be reclaimed."""
    try:
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
        while not daemon.stopping.is_set() and not client.closed:
            op, _data = _ws_read_frame(sock)
            if op is None or op == 0x8:  # close frame
                break
    except (OSError, ConnectionError):
        pass
    finally:
        daemon.hub.remove(client)  # closes the socket
        reg = getattr(server, "_ws_hijacked", None)
        if reg is not None:
            reg.discard(id(sock))


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
# Quota state (py-1.10.27 — initiative `quota-aware-dispatch`)
#
# Persistent per-(platform, model) rate-limit ledger. Tracks which
# upstream LLM pools are currently exhausted, with the exact expiry
# instant + history of probe attempts. Survives daemon restart at
# `.meshkore/.runtime/quota-state.json` so a quick relaunch doesn't
# lose the "Claude Pro window doesn't reset until 06:23 UTC" datum
# and waste tokens re-discovering it.
#
# Replaces the py-1.10.26 in-memory `_agent_type_pauses` dict.
# `/health.paused_agent_types` is kept as a back-compat projection so
# the existing cockpit banner keeps working without changes.


# ───────────────────────────────────────────────────────────────────────
# ChatSessionReaper (py-1.12.16)
#
# Background thread that periodically sweeps `ChatSessions` for slots
# whose subprocess has exited (or never spawned) but whose `done` event
# was never set — which would leave the conv marked `live: true` and
# every subsequent /chat/dispatch silently queued. The reaper:
#
#   1. Calls ChatSessions.reap_dead() — pops the orphan slots.
#   2. Broadcasts conv.activity {live: false} so cockpits drop the
#      stale "STOP" UI immediately.
#   3. Emits a `chat-session.reaped` debug event with the reason.
#
# It also runs once on daemon boot to clear any anomalies left from
# a forced shutdown (kill -9). On a normal boot ChatSessions is empty
# in memory, so the sweep is a no-op — defense in depth.
#
# Field-reported 2026-06-10 (IKA cluster, py-1.12.10): master conv had
# been stuck `live: true` for 2.5+ days because a subprocess ended
# without the runner's done.set() being reached. Operator: "el daemon
# debería gestionar eso, los usuarios no sabrán hacerlo ni deberían."


# ───────────────────────────────────────────────────────────────────────
# VersionWatcher (py-1.12.1)
#
# Background thread that periodically polls the CDN for newer
# daemon.py versions and self-invokes /self-update when the cluster
# is idle. Designed for fleet operation: an operator with 100 clients
# shouldn't need to log into each one to push an upgrade — the
# daemon sees the new version on CDN and rolls itself forward.
#
# Coexists with the BOOT self-update (`_boot_self_update_if_needed`)
# which only fires when the daemon starts. Long-running daemons (days
# of uptime, no restart) would never upgrade without this thread.
#
# Behavior
# ────────
#   • Tick interval: `cluster.yaml.daemon.auto_update_check_interval_sec`
#     (default 1800 = 30 min). Clamped 60-86400.
#   • Skips entirely when `cluster.yaml.daemon.auto_update: false`.
#   • Each tick:
#       1. Fetch the first ~1 KB of `auto_update_source` to read its
#          DAEMON_VERSION line. Cheap — single Range request.
#       2. Parse local + remote versions. If remote ≤ local, sleep.
#       3. If `chat_sessions.list_active()` non-empty → defer (log
#          "deferred until idle", emit `daemon.upgrade.deferred` WS).
#       4. Otherwise call `self.daemon.self_update({})` directly. The
#          method spawns the new daemon on a fresh port and schedules
#          this process's shutdown. Cockpits reconnect via the daemon
#          dedup-by-cluster_id path.
#   • Cooldown: 5 min after any attempt (successful or not) to avoid
#     hammering a misconfigured CDN or looping if the upgrade fails.
