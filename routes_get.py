"""routes_get.py — route_get, the HTTP GET route table.

Extracted from routes.py make_handler (daemon-architecture-v2 Phase 3c).
Was the Handler._do_GET method; lifted to a free function taking
(self=the live BaseHTTPRequestHandler, daemon=the Daemon). Body is VERBATIM
(only the signature + 8-space dedent changed); Handler._do_* now delegates
here, so dispatch is byte-for-byte identical."""

from __future__ import annotations

import re
import urllib.parse

from utils import _iso_now, debug_enabled, get_debug_log


def route_get(self, daemon):  # noqa: N802
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
        if not nonce or len(nonce) > 128 or not re.match(r"^[A-Za-z0-9._-]+$", nonce):
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
        return self._serve_meshkore_file(daemon.paths.docs_dir, p[len("/docs/") :])
    if p.startswith("/modules/"):
        if self._need_auth():
            return
        return self._serve_meshkore_file(
            daemon.paths.modules_dir, p[len("/modules/") :]
        )
    if p.startswith("/tasks/"):
        if self._need_auth():
            return
        return self._serve_meshkore_file(daemon.paths.roadmap_dir, p[len("/tasks/") :])
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
        return self._serve_meshkore_file(daemon.paths.log_dir, p[len("/log/") :])
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
    # knowledge-tree-unified KT4 — the unified knowledge tree. `GET
    # /knowledge` returns the manifest-driven concept tree (an overlay
    # over context/ + docs/ + modules/ defined in context/_index.yaml;
    # per-node load policy + spawn-token budget). `GET /knowledge/<id>`
    # returns a single node's processed body (lazy-fetched by the
    # cockpit + by agents on demand). Exact-match before the prefix.
    if p == "/knowledge":
        if self._need_auth():
            return
        return self._json(200, daemon.knowledge_tree())
    if p.startswith("/knowledge/"):
        if self._need_auth():
            return
        node_id = urllib.parse.unquote(p[len("/knowledge/") :]).strip("/")
        return self._json(200, daemon.knowledge_node(node_id))
    # py-1.9.3 — Per-initiative git activity. Runs git log on
    # the project root and returns commits whose subject/body
    # mentions the initiative id, plus the files each commit
    # touched. The cockpit's expanded InitiativeCard surfaces
    # this in its Activity tab so the operator can see what
    # actually shipped for a given initiative.
    # py-1.20.0 — roadmap wall ordering. The cockpit reads the four
    # walls (active/next/backlog/archived) ordered by `wall_order`.
    if p == "/initiative/walls":
        if self._need_auth():
            return
        return self._json(200, daemon.initiative_walls())
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
            return self._json(404, {"error": "module not in links.yaml", "id": mid})
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
            return self._json(200, {"runs": daemon.protocols_registry.runs(pid)})
        proto = daemon.protocols_registry.get(rest)
        if proto is None:
            return self._json(404, {"error": "protocol not found", "id": rest})
        return self._json(200, proto)
    return self._json(404, {"error": "unknown route", "path": p})
