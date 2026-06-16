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
import re
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

from utils import _debug_emit, _iso_now, debug_enabled, get_debug_log  # DM7

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

        def _cors(self) -> None:
            # The architect is served from architect.meshkore.com but
            # talks to localhost. CORS-allow any origin since the bearer
            # token gates the privileged routes.
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header(
                "Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS"
            )
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

        def do_GET(self):  # noqa: N802
            self._guard(self._do_GET, "GET")

        def do_POST(self):  # noqa: N802
            self._guard(self._do_POST, "POST")

        def _do_GET(self):  # noqa: N802
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
                        "version": daemon.daemon_version,
                        "ts": _iso_now(),
                    },
                )
            if p == "/state":
                return self._json(200, daemon.state_manager.state())
            # py-1.10.27 — Quota state read endpoint. Full per-key
            # ledger including probe history; richer than /health.quota
            # (which is just a snapshot). Auth-required because probe
            # history exposes conv ids.
            if p == "/quota":
                if self._need_auth():
                    return
                return self._json(
                    200,
                    {
                        "by_key": daemon.quota.view(),
                        "generated_at": _iso_now(),
                    },
                )
            # py-1.10.17 — debug stream tail. Auth required because the
            # stream contains conv ids, agent ids, and prompt previews
            # that aren't meant for the public internet.
            if p == "/debug/tail":
                if self._need_auth():
                    return
                if not debug_enabled():
                    return self._json(200, {"events": [], "retained_secs": 0})
                try:
                    last_secs = int(q.get("last") or "300")
                except ValueError:
                    last_secs = 300
                tag_csv = (q.get("tag") or "").strip()
                tags = set(t for t in tag_csv.split(",") if t) or None
                lvl = (q.get("level") or "debug").lower()
                events, retained = get_debug_log().tail(
                    last_secs=last_secs,
                    tags=tags,
                    min_level=lvl,
                )
                return self._json(
                    200,
                    {
                        "events": events,
                        "retained_secs": retained,
                        "window_secs": last_secs,
                        "generated_at": _iso_now(),
                    },
                )
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
            # py-1.12.22 / Standard v22 — `.meshkore/` capacity report
            # for the operator's storage panel. Cached server-side
            # (CACHE_TTL_SECS) so polling is cheap. No auth required —
            # bytes per bucket is metadata, not contents.
            if p == "/storage/usage":
                return self._json(200, daemon.storage_report.usage())
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
            # py-1.14.1 — Standard v14 §3.5 project context. `GET
            # /context` returns the `.meshkore/context/` folder/file
            # tree (per-file title/updated/status + word count +
            # over_cap flag, tree-level budget + warnings). `GET
            # /context/<path>` serves a single file's raw markdown body
            # (lazy-fetched by the cockpit on node selection). Auth-
            # gated like the other .meshkore/ reads — context can name
            # internal decisions not meant for an anonymous browser.
            # NB: the exact-match `/context` MUST precede the
            # `/context/` prefix so the tree endpoint isn't shadowed.
            if p == "/context":
                if self._need_auth():
                    return
                return self._json(200, daemon.context_tree())
            if p.startswith("/context/"):
                if self._need_auth():
                    return
                return self._serve_meshkore_file(
                    daemon.paths.context_dir, p[len("/context/") :]
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
            # py-1.11.3 — Single-credential read. Cockpit only fetches
            # the value when the operator clicks "reveal". Auth required.
            if p.startswith("/credentials/"):
                if self._need_auth():
                    return
                name = p[len("/credentials/") :]
                code, body = daemon.credential_read(name)
                return self._json(code, body)
            # py-1.5.0 — Daemon-side archive state. Anonymous read so the
            # cockpit can sync from boot before the token is pasted.
            if p == "/chat/archives":
                return self._json(
                    200,
                    {
                        "archived": daemon.chat_archive.list(),
                    },
                )
            # py-1.11.0 — chat-state-rearchitecture (initiative
            # `chat-state-rearchitecture`). Canonical conv list +
            # boot snapshot + per-conv meta + paginated history.
            # Anonymous reads to mirror /chat/archives — the cockpit
            # consumes them before the token is pasted, and conv ids
            # are not secrets (they appear in the timeline events that
            # /state already serves anonymously).
            if p == "/chat/snapshot":
                return self._json(200, daemon.chat_snapshot())
            if p == "/chat/convs":
                return self._json(
                    200,
                    {
                        "convs": daemon.chat_convs(),
                        "generated_at": _iso_now(),
                    },
                )
            # Path-prefixed routes for one conv: /chat/conv/<id>/meta
            # and /chat/conv/<id>/messages. URL-encode the id when it
            # contains chars outside [A-Za-z0-9_-] (rare; conv ids are
            # ASCII-clean by convention but the architect's slugs can
            # carry hyphens that are already safe).
            if p.startswith("/chat/conv/"):
                rest = p[len("/chat/conv/") :]
                if rest.endswith("/meta"):
                    cid = urllib.parse.unquote(rest[: -len("/meta")])
                    if not cid:
                        return self._json(400, {"error": "conv id required"})
                    return self._json(200, daemon.chat_conv_meta(cid))
                if rest.endswith("/messages"):
                    cid = urllib.parse.unquote(rest[: -len("/messages")])
                    if not cid:
                        return self._json(400, {"error": "conv id required"})
                    before = q.get("before") or None
                    try:
                        limit = int(q.get("limit") or "200")
                    except ValueError:
                        limit = 200
                    return self._json(
                        200,
                        daemon.chat_conv_messages(
                            cid,
                            before_ts=before,
                            limit=limit,
                        ),
                    )
                # py-1.12.19 — Standard v16 chat-turn queue. GET lists
                # the items for one conv. If the conv has no queue file
                # we return 200 with empty items (NOT 404) so the
                # cockpit's hydrate path doesn't log false negatives.
                if rest.endswith("/queue"):
                    cid = urllib.parse.unquote(rest[: -len("/queue")])
                    if not cid:
                        return self._json(400, {"error": "conv id required"})
                    items = daemon.chat_queue_manager.list(cid)
                    return self._json(
                        200,
                        {"conv": cid, "items": items, "generated_at": _iso_now()},
                    )
            # py-1.12.21 — serve persisted chat uploads.
            #   GET /chat/uploads/<YYYY-MM-DD>/<filename>
            # Returns the file with its inferred content-type so the
            # cockpit's <img src=…> just works. No auth required for
            # the file body itself — the URL is opaque (random suffix
            # in the filename), the bucket+file pair is hard to guess,
            # and the privileged endpoints that produce these URLs
            # already gate on the portal-token at write time.
            if p.startswith("/chat/uploads/"):
                rest = p[len("/chat/uploads/") :]
                parts = rest.split("/", 1)
                if len(parts) != 2:
                    return self._json(400, {"error": "bucket + filename required"})
                bucket, filename = parts[0], urllib.parse.unquote(parts[1])
                path = daemon.upload_store.serve_path(bucket, filename)
                if path is None:
                    return self._json(404, {"error": "not found"})
                try:
                    body_bytes = path.read_bytes()
                except OSError:
                    return self._json(404, {"error": "not found"})
                # Infer content-type from extension; default to octet-stream.
                ext = path.suffix.lower().lstrip(".")
                ctype = {
                    "png": "image/png",
                    "jpg": "image/jpeg",
                    "jpeg": "image/jpeg",
                    "gif": "image/gif",
                    "webp": "image/webp",
                    "svg": "image/svg+xml",
                    "avif": "image/avif",
                    "bmp": "image/bmp",
                }.get(ext, "application/octet-stream")
                self.send_response(200)
                self._cors()
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body_bytes)))
                # Cache for 1h — the filename has a 4-hex rand suffix so
                # it's effectively immutable; a longer max-age is safe.
                self.send_header("Cache-Control", "private, max-age=3600")
                self.end_headers()
                try:
                    self.wfile.write(body_bytes)
                except Exception:
                    pass
                return
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

        def _do_POST(self):  # noqa: N802
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

            # py-1.10.17 — cockpit log ingestion for the debug stream.
            # Body: one event `{tag, msg?, lvl?, conv?, agent_id?, data?}`
            # or `{events: [...]}`. `src` is always overwritten to
            # `cockpit` so a forged `src: "daemon"` from the wire is
            # impossible.
            if p == "/debug/log":
                if not debug_enabled():
                    return self._json(503, {"error": "debug stream not ready"})
                body = self._read_json_body()
                events = body.get("events") if isinstance(body, dict) else None
                if not isinstance(events, list):
                    events = [body] if isinstance(body, dict) else []
                accepted = 0
                for ev in events:
                    if not isinstance(ev, dict):
                        continue
                    tag = str(ev.get("tag") or "log")[:64]
                    msg = str(ev.get("msg") or "")[:4000]
                    lvl = str(ev.get("lvl") or "info")
                    conv = ev.get("conv")
                    agent_id = ev.get("agent_id")
                    data = ev.get("data") if isinstance(ev.get("data"), dict) else None
                    get_debug_log().emit(
                        tag=tag,
                        msg=msg,
                        lvl=lvl,
                        src="cockpit",
                        conv=(str(conv) if conv else None),
                        agent_id=(str(agent_id) if agent_id else None),
                        data=data,
                    )
                    accepted += 1
                return self._json(200, {"accepted": accepted})

            # py-1.10.26 — Manual agent-type pause / unpause. Used by
            # the operator when they know they're about to hit the
            # 5-hour wall (preventive pause) or when they've manually
            # cleared a rate-limit and want to resume early.
            #   POST /agent-types/<type>/pause       body: {duration_secs?, reason?}
            #   POST /agent-types/<type>/unpause     body: {}
            if p.startswith("/agent-types/") and p.endswith("/pause"):
                t = p[len("/agent-types/") : -len("/pause")]
                body = self._read_json_body() or {}
                entry = daemon._pause_agent_type(
                    t,
                    reason=str(body.get("reason") or "operator-paused"),
                    duration_secs=body.get("duration_secs"),
                )
                _debug_emit(
                    "agent-type.pause",
                    msg=f"operator paused {t} until {entry.get('expires_at')}",
                    lvl="warn",
                    data={"agent_type": t, **entry},
                )
                return self._json(200, {"ok": True, "agent_type": t, **entry})
            if p.startswith("/agent-types/") and p.endswith("/unpause"):
                t = p[len("/agent-types/") : -len("/unpause")]
                cleared = daemon._unpause_agent_type(t)
                _debug_emit(
                    "agent-type.unpause",
                    msg=f"operator unpaused {t}",
                    lvl="info",
                    data={"agent_type": t, "was_paused": cleared},
                )
                return self._json(
                    200, {"ok": True, "agent_type": t, "was_paused": cleared}
                )

            # py-1.10.27 — Direct quota-key control. More precise than
            # /agent-types/<t>/{pause,unpause} because it targets the
            # (platform, model) pool directly — useful when multiple
            # types share a pool and the operator wants explicit
            # confirmation about what's being paused.
            #   POST /quota/<key>/pause     body: {duration_secs?, reason?}
            #   POST /quota/<key>/unpause   body: {}
            # NOTE: `<key>` contains a `/` so the URL is /quota/claude-code/auto/pause.
            if p.startswith("/quota/") and (
                p.endswith("/pause") or p.endswith("/unpause")
            ):
                tail = p[len("/quota/") :]
                if tail.endswith("/pause"):
                    key = tail[: -len("/pause")]
                    body = self._read_json_body() or {}
                    entry = daemon.quota.pause(
                        key,
                        reason=str(body.get("reason") or "operator-paused"),
                        duration_secs=body.get("duration_secs"),
                    )
                    _debug_emit(
                        "quota.pause",
                        msg=f"operator paused {key} until {entry.get('paused_until')}",
                        lvl="warn",
                        data={"quota_key": key, **entry},
                    )
                    return self._json(
                        200, {"ok": True, "quota_key": key, "entry": entry}
                    )
                else:
                    key = tail[: -len("/unpause")]
                    cleared = daemon.quota.unpause(key)
                    _debug_emit(
                        "quota.unpause",
                        msg=f"operator unpaused {key}",
                        lvl="info",
                        data={"quota_key": key, "was_paused": cleared},
                    )
                    return self._json(
                        200, {"ok": True, "quota_key": key, "was_paused": cleared}
                    )

            # U-DAEMON-06: chat dispatch + cancel.
            if p == "/chat/dispatch":
                return self._json(*daemon.chat_dispatch(self._read_json_body()))
            if p == "/chat/cancel":
                return self._json(*daemon.chat_cancel(self._read_json_body()))

            # py-1.12.19 — Standard v16 chat-turn queue mutations.
            #   POST /chat/conv/<id>/queue                  {text}      → add
            #   POST /chat/conv/<id>/queue/<itemId>/edit    {text}      → edit
            #   POST /chat/conv/<id>/queue/<itemId>/move    {position}  → reorder
            #   POST /chat/conv/<id>/queue/<itemId>/promote             → head
            # The matching DELETE (remove) is handled in do_DELETE below.
            if p.startswith("/chat/conv/"):
                rest = p[len("/chat/conv/") :]
                if rest.endswith("/queue"):
                    cid = urllib.parse.unquote(rest[: -len("/queue")])
                    if not cid:
                        return self._json(400, {"error": "conv id required"})
                    body = self._read_json_body()
                    text = str((body or {}).get("text") or "").strip()
                    if not text:
                        return self._json(400, {"error": "text required"})
                    try:
                        item = daemon.chat_queue_manager.enqueue(cid, text)
                    except ValueError as e:
                        return self._json(400, {"error": str(e)})
                    return self._json(200, {"conv": cid, "item": item})
                if "/queue/" in rest:
                    cid_part, _, sub = rest.partition("/queue/")
                    cid = urllib.parse.unquote(cid_part)
                    if not cid:
                        return self._json(400, {"error": "conv id required"})
                    if sub.endswith("/edit"):
                        item_id = urllib.parse.unquote(sub[: -len("/edit")])
                        body = self._read_json_body()
                        text = str((body or {}).get("text") or "").strip()
                        if not text:
                            return self._json(400, {"error": "text required"})
                        it = daemon.chat_queue_manager.edit(cid, item_id, text)
                        if it is None:
                            return self._json(404, {"error": "item not found"})
                        return self._json(200, {"conv": cid, "item": it})
                    if sub.endswith("/move"):
                        item_id = urllib.parse.unquote(sub[: -len("/move")])
                        body = self._read_json_body()
                        try:
                            pos = int((body or {}).get("position"))
                        except (TypeError, ValueError):
                            return self._json(400, {"error": "position required (int)"})
                        it = daemon.chat_queue_manager.move(cid, item_id, pos)
                        if it is None:
                            return self._json(404, {"error": "item not found"})
                        return self._json(200, {"conv": cid, "item": it})
                    if sub.endswith("/promote"):
                        item_id = urllib.parse.unquote(sub[: -len("/promote")])
                        it = daemon.chat_queue_manager.promote(cid, item_id)
                        if it is None:
                            return self._json(404, {"error": "item not found"})
                        return self._json(200, {"conv": cid, "item": it})
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

            # py-1.11.3 — POST /credentials/<name> is treated as
            # write-or-create (alias of PUT). Some HTTP clients can't
            # send PUT; routing both verbs to the same handler keeps
            # the cockpit's `chatDispatch`-shaped fetch usable.
            if p.startswith("/credentials/"):
                name = p[len("/credentials/") :]
                body = self._read_json_body()
                value = body.get("value") if isinstance(body, dict) else None
                code, resp = daemon.credential_write(
                    name, value if isinstance(value, str) else ""
                )
                return self._json(code, resp)

            return self._json(404, {"error": "not found", "path": p})

        def do_PUT(self):  # noqa: N802
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
            p, _ = self._path()
            if self._need_auth():
                return
            if p.startswith("/credentials/"):
                name = p[len("/credentials/") :]
                code, resp = daemon.credential_delete(name)
                return self._json(code, resp)
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
