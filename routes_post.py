"""routes_post.py — route_post, the HTTP POST route table.

Extracted from routes.py make_handler (daemon-architecture-v2 Phase 3c).
Was the Handler._do_POST method; lifted to a free function taking
(self=the live BaseHTTPRequestHandler, daemon=the Daemon). Body is VERBATIM
(only the signature + 8-space dedent changed); Handler._do_* now delegates
here, so dispatch is byte-for-byte identical."""

from __future__ import annotations

import threading
import urllib.parse

from utils import _debug_emit, _iso_now, debug_enabled, get_debug_log


def route_post(self, daemon):  # noqa: N802
    p, _ = self._path()
    if p == "/shutdown":
        if self._need_auth():
            return
        self._json(200, {"ok": True, "shutting_down": True, "ts": _iso_now()})
        threading.Thread(target=daemon.request_shutdown, daemon=True).start()
        return

    # TEG-2 — external ask. Matched BEFORE the global portal-token gate:
    # the caller authenticates with the MEMBER's bearer token (validated in
    # the handler; 401/403/404/429 semantics live there). The member token
    # authorizes ONLY this route + /team/requests/* it created — every
    # other route still compares against the portal token and 401s.
    if p.startswith("/team/") and p.endswith("/ask"):
        mid = urllib.parse.unquote(p[len("/team/") : -len("/ask")]).strip("/")
        if not mid:
            return self._json(400, {"error": "team member id required"})
        return self._json(
            *daemon.team_ask_http(
                mid, bearer=self._bearer(), body=self._read_json_body()
            )
        )

    # All other POSTs need auth.
    if self._need_auth():
        return

    # TEG-1 — rotate an exposed member's bearer token (portal-token gated,
    # NOT the member token). The old token dies with the write.
    if p.startswith("/team/") and p.endswith("/token/rotate"):
        mid = urllib.parse.unquote(p[len("/team/") : -len("/token/rotate")]).strip("/")
        if not mid:
            return self._json(400, {"error": "team member id required"})
        return self._json(*daemon.team_token_rotate_http(mid))

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
            # Tag with the project: the event may name it, else the request's
            # X-MeshKore-Project header (centralized multi-project debug).
            project = ev.get("project") or self.headers.get("X-MeshKore-Project")
            get_debug_log().emit(
                tag=tag,
                msg=msg,
                lvl=lvl,
                src="cockpit",
                conv=(str(conv) if conv else None),
                agent_id=(str(agent_id) if agent_id else None),
                project=(str(project) if project else None),
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
        return self._json(200, {"ok": True, "agent_type": t, "was_paused": cleared})

    # py-1.10.27 — Direct quota-key control. More precise than
    # /agent-types/<t>/{pause,unpause} because it targets the
    # (platform, model) pool directly — useful when multiple
    # types share a pool and the operator wants explicit
    # confirmation about what's being paused.
    #   POST /quota/<key>/pause     body: {duration_secs?, reason?}
    #   POST /quota/<key>/unpause   body: {}
    # NOTE: `<key>` contains a `/` so the URL is /quota/claude-code/auto/pause.
    if p.startswith("/quota/") and (p.endswith("/pause") or p.endswith("/unpause")):
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
            return self._json(200, {"ok": True, "quota_key": key, "entry": entry})
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

    # py-1.20.0 — roadmap wall reorder. Body {id, wall, order}; sets
    # wall_order (+ status when the wall changes) and recompacts the wall.
    if p == "/initiative/reorder":
        return self._json(*daemon.initiative_reorder(self._read_json_body()))

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
    # DC-5 (daemon-centralized) — GLOBAL: register a project by path
    # (scaffolds .meshkore/ if absent). Auth-gated at the top of route_post.
    if p == "/projects":
        return self._json(*daemon.project_register(self._read_json_body()))
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
                return self._json(*daemon.run_advance(run_id, self._read_json_body()))
            if action == "finish":
                return self._json(*daemon.run_finish(run_id, self._read_json_body()))
            if action == "stream":
                return self._json(
                    *daemon.run_set_stream(run_id, self._read_json_body())
                )

    # U-DAEMON-03 finish: declare a new agent.
    if p == "/agents":
        return self._json(*daemon.agent_create(self._read_json_body()))

    # Initiative `agent-team` — ATM5: free text → structured member draft
    # (read-only, LLM-backed). Checked BEFORE /team create so the more
    # specific path wins.
    if p == "/team/draft":
        return self._json(*daemon.team_draft(self._read_json_body()))
    # ATM9: create a team member.
    if p == "/team":
        return self._json(*daemon.team_create_http(self._read_json_body()))

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
    # Cluster admission (joining a cluster from ANOTHER device) is a RESERVED
    # FUTURE capability. Today the daemon orchestrates LOCAL agents only; no
    # cross-device membership is wired. Kept as a placeholder so the route
    # contract exists for when off-device orchestration lands. The facilitated
    # cross-device channel work lives in initiative `private-clusters`, which
    # is intentionally NOT routed through the daemon.
    if p.startswith("/admission/"):
        return self._json(
            501,
            {
                "error": "cluster admission is local-only today; "
                "off-device join is a reserved future capability"
            },
        )

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

    # MeshKore Verify (VRF2) — run the local visual+functional verifier
    # against a public/preview URL and return evidence (shots + flows +
    # console/network + verdict). Body is a verify spec.
    if p == "/verify":
        return self._json(*daemon.verify_request(self._read_json_body()))

    return self._json(404, {"error": "unknown route", "path": p})
