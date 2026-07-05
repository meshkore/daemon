"""teamext.py — Team External Gateway (initiative `team-external-gateway`).

TEG-1/TEG-2/TEG-4. Lets software OUTSIDE the cockpit consume a team member
as a service. A member whose frontmatter says `exposure: external` gets:

- a per-member NON-EXPIRING bearer token (`TeamTokenStore`) stored at
  `.meshkore/credentials/team-tokens.yaml` — credentials/ is §2.2
  deny-listed, so the secret never travels with the repo. No TTL, no
  scheduled rotation: lifecycle is operator-driven (rotate / revoke).
- an async ask/poll HTTP surface (`TeamExtMixin`):
    POST /team/<id>/ask            {text, session?, context_docs?} → 202
    GET  /team/requests/<rid>      → {status, result_text?, error?, …}
  Both gated by the MEMBER token (NOT the portal token); the member token
  authorizes NOTHING else. The ask is a thin façade over the existing
  chat_dispatch(member=<id>) path, so external convs are ordinary
  instances: `ext-<member>-<session|stamp>` slugs in the chat rail, init
  prompt on turn 1, member model/effort, singleton rules — all for free.
- an A2A Public Card (TEG-4, `build_member_card` — PURE on purpose:
  Phase 2 `exposure: mesh` republishes the same card to the hub, only
  WHERE it's published changes):
    GET /team/<id>/.well-known/agent.json   (no auth; loopback = perimeter)

The requests index lives at `.meshkore/.runtime/team-requests.json`
(gitignored runtime state); entries are GC'd after ~24 h. Completion is
observed by a small watcher thread per request that polls the live
ChatSessions + the conv's timeline for the turn's `chat.assistant.final`,
then broadcasts `team.request.done|error {member, request_id, ts}`.
"""

from __future__ import annotations

import hmac
import os
import re
import secrets
import threading
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from fsatomic import atomic_write_json, atomic_write_text
from team import TeamError
from utils import (
    _debug_emit,
    _iso_now,
    _iter_timeline_files,
    _log,
    _read_timeline_file,
)
from yamlparse import parse_simple_yaml

# Per-member concurrent-ask cap (TEG-2 guard-rail). Loopback binding is the
# perimeter; no rate limiting beyond this in v1. Config knob comes later.
EXT_ASK_CONCURRENT_CAP = 2
# Requests older than this are GC'd from the runtime index.
EXT_REQUEST_TTL_HOURS = 24
# Watcher poll cadence + overall turn budget. Agent turns take 30 s – minutes;
# the chat runner owns the real limits — this is only the observer's ceiling.
_WATCH_POLL_SECS = 2.0
_WATCH_MAX_SECS = 60 * 60
# Grace: how long a conv may sit idle (no live runner) with no final on the
# timeline before the watcher declares the request errored (spawn died, turn
# cancelled, …).
_IDLE_GRACE_SECS = 20.0

_SESSION_SAFE_RE = re.compile(r"[^a-z0-9-]+")


class TeamTokenStore:
    """`.meshkore/credentials/team-tokens.yaml` — `{<member-id>: <token>}`.

    SECRETS live here and ONLY here (never in `.meshkore/team/`). Tokens do
    NOT expire — no TTL anywhere; mint/rotate/revoke are the only writes.
    File is chmod 600 and sits in the §2.2 deny-listed credentials/ dir.
    Cheap to construct per call; every write is atomic (tmp+rename)."""

    FILENAME = "team-tokens.yaml"

    def __init__(self, paths: Any) -> None:
        self.path = paths.credentials / self.FILENAME

    def _load(self) -> Dict[str, str]:
        try:
            if not self.path.is_file():
                return {}
            data = parse_simple_yaml(self.path.read_text(encoding="utf-8"))
        except OSError:
            return {}
        return {
            str(k): str(v)
            for k, v in (data or {}).items()
            if isinstance(v, (str, int, float)) and str(v).strip()
        }

    def _save(self, data: Dict[str, str]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lines = ["# MeshKore team-member bearer tokens (TEG-1). SECRET — never commit."]
        for mid in sorted(data):
            lines.append(f"{mid}: {data[mid]}")
        atomic_write_text(self.path, "\n".join(lines) + "\n", fsync=True)
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    def get(self, mid: str) -> Optional[str]:
        return self._load().get(mid)

    def mint(self, mid: str) -> str:
        """Mint a FRESH token for `mid` (rotate semantics: any previous
        value dies with this write). 32 urlsafe random bytes (43 chars)."""
        token = secrets.token_urlsafe(32)
        data = self._load()
        data[mid] = token
        self._save(data)
        return token

    def ensure(self, mid: str) -> str:
        """Return the existing token, minting one only when absent
        (seed-time idempotency: re-boots never rotate silently)."""
        existing = self.get(mid)
        return existing if existing else self.mint(mid)

    def delete(self, mid: str) -> bool:
        data = self._load()
        if mid not in data:
            return False
        del data[mid]
        self._save(data)
        return True

    def matches(self, mid: str, presented: str) -> bool:
        """Constant-time comparison of a presented bearer against the
        member's stored token. False when the member has no token."""
        stored = self.get(mid)
        if not stored or not presented:
            return False
        return hmac.compare_digest(stored, presented)

    def ensure_for_external(self, team_store: Any) -> int:
        """Mint tokens for every `exposure: external` member that has none
        (seed-time + boot repair). Never rotates an existing token; never
        touches internal members. Returns the count minted."""
        minted = 0
        try:
            members = team_store.team_list()
        except Exception:
            return 0
        for fm in members:
            mid = str(fm.get("id") or "").strip()
            if not mid:
                continue
            if str(fm.get("exposure") or "internal").strip().lower() != "external":
                continue
            if self.get(mid) is None:
                self.mint(mid)
                minted += 1
        return minted


# ── TEG-4: A2A Public Card (pure builder) ───────────────────────────────


def _first_paragraph(body: str) -> str:
    """First prose paragraph of the init prompt: skip markdown headings and
    blank lines, collect until the next blank line, join into one string."""
    para: List[str] = []
    for raw in (body or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            if para:
                break
            continue
        para.append(line)
    return " ".join(para)


def build_member_card(
    fm: Dict[str, Any],
    body: str,
    *,
    base_url: str,
    project_id: str,
) -> Dict[str, Any]:
    """A2A Public Card for one EXPOSED member (TEG-4). PURE — no I/O, no
    daemon state — so Phase 2 (`exposure: mesh`) can republish the exact
    same card to hub.meshkore.com by only changing where it's sent.

    `url` is the ask endpoint; the required headers + the poll contract are
    documented in the skill description, `metadata.required_headers` and the
    example, so a stranger A2A client can self-configure from this one URL."""
    mid = str(fm.get("id") or "")
    name = str(fm.get("name") or mid)
    ask_url = f"{base_url}/team/{mid}/ask"
    poll_url = f"{base_url}/team/requests/{{request_id}}"
    example = (
        f"curl -sk -X POST {ask_url} "
        f"-H 'Authorization: Bearer <member-token>' "
        f"-H 'X-MeshKore-Project: {project_id}' "
        f"-H 'content-type: application/json' "
        '-d \'{"text": "What is this project and what does it do?"}\''
    )
    return {
        "protocolVersion": "0.3.0",
        "name": name,
        "description": _first_paragraph(body),
        "url": ask_url,
        "preferredTransport": "HTTP+JSON",
        "version": "1.0.0",
        "capabilities": {"streaming": False, "pushNotifications": False},
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "securitySchemes": {
            "bearer": {
                "type": "http",
                "scheme": "bearer",
                "description": (
                    "Per-member token issued by the cluster operator "
                    "(Team UI / GET /team/<id> with the portal token)."
                ),
            }
        },
        "security": [{"bearer": []}],
        "skills": [
            {
                "id": "ask",
                "name": f"Ask {name}",
                "description": (
                    f"Async ask/poll. POST {ask_url} with headers "
                    f"'Authorization: Bearer <member-token>' and "
                    f"'X-MeshKore-Project: {project_id}', body "
                    "{text, session?, context_docs?} -> 202 {request_id, conv}. "
                    f"Poll GET {poll_url} (same bearer + project header) until "
                    "status is 'done' (read result_text) or 'error'. Passing the "
                    "same `session` keeps one conversation (the member remembers "
                    "the thread)."
                ),
                "tags": ["ask", "consulting", "meshkore-team"],
                "examples": [example],
                "inputModes": ["text/plain"],
                "outputModes": ["text/plain"],
            }
        ],
        "metadata": {
            "id": mid,
            "kind": fm.get("kind"),
            "model": fm.get("model"),
            "exposure": "external",
            "project": project_id,
            "required_headers": {
                "Authorization": "Bearer <member-token>",
                "X-MeshKore-Project": project_id,
            },
            "poll_url": poll_url,
        },
    }


# ── TEG-2: ask/poll surface ─────────────────────────────────────────────


class TeamExtMixin:
    """External ask/poll + token lifecycle + A2A card HTTP handlers.
    Inherited by the Daemon (like TeamMixin) so self.* resolves per-project
    via the DC-4 property accessors (team_store, chat_sessions, paths, …)."""

    # One lock guards every requests-index read-modify-write (all projects;
    # the file op inside is per-project and cheap).
    _teamext_lock = threading.Lock()

    # ── requests index (runtime, not committed) ────────────────────────
    def _teamext_requests_path(self) -> Any:
        return self.paths.runtime / "team-requests.json"

    def _teamext_load(self) -> Dict[str, Dict[str, Any]]:
        p = self._teamext_requests_path()
        try:
            if not p.is_file():
                return {}
            import json as _json

            data = _json.loads(p.read_text(encoding="utf-8") or "{}") or {}
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _teamext_save(self, data: Dict[str, Dict[str, Any]]) -> None:
        p = self._teamext_requests_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(p, data, sort_keys=True)

    @staticmethod
    def _teamext_gc(data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Drop entries older than EXT_REQUEST_TTL_HOURS (ISO ts compare)."""
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=EXT_REQUEST_TTL_HOURS)
        ).strftime("%Y-%m-%dT%H:%M:%S")
        return {
            rid: e
            for rid, e in data.items()
            if str((e or {}).get("started_at") or "") >= cutoff
        }

    def _teamext_update(self, rid: str, **fields: Any) -> Optional[Dict[str, Any]]:
        with self._teamext_lock:
            data = self._teamext_load()
            entry = data.get(rid)
            if entry is None:
                return None
            entry.update(fields)
            self._teamext_save(data)
            return dict(entry)

    def _teamext_broadcast(self, event: str, member: str, rid: str) -> None:
        """team.request.created|done|error {member, request_id, ts} on the
        chat.* WS bus so the cockpit can surface external activity."""
        try:
            if getattr(self, "hub", None) is not None:
                self.hub.broadcast(
                    {
                        "type": event,
                        "member": member,
                        "request_id": rid,
                        "ts": _iso_now(),
                    }
                )
        except Exception as e:  # noqa: BLE001 — broadcast failure is non-fatal
            _log(f"teamext broadcast {event} failed: {e}")

    # ── CPL-1: resolve a singleton's bound conv ─────────────────────────
    def _member_bound_conv(self, mid: str) -> Tuple[Optional[str], bool]:
        """Return (conv, archived) for the conv this member is bound to, or
        (None, False) when none exists. A NON-archived binding wins; an
        archived one is returned only when there is no live binding (the
        caller then unarchives it — the singleton's history IS the value).
        Used by the singleton ask path so talking to a singleton always
        lands in ITS one conversation."""
        try:
            meta = self._conv_meta_load()
        except Exception:
            return None, False
        archived_conv: Optional[str] = None
        for conv, entry in (meta or {}).items():
            if not isinstance(entry, dict):
                continue
            if str(entry.get("member") or "").strip() != mid:
                continue
            try:
                is_arch = self.chat_archive.is_archived(conv)
            except Exception:
                is_arch = False
            if not is_arch:
                return conv, False
            archived_conv = conv
        if archived_conv is not None:
            return archived_conv, True
        return None, False

    # ── POST /team/<id>/ask ─────────────────────────────────────────────
    def team_ask_http(
        self,
        mid: str,
        *,
        bearer: Optional[str],
        body: Dict[str, Any],
        remote: bool = False,
    ) -> Tuple[int, Dict[str, Any]]:
        """Member-token gated (NOT the portal token). Error order per TEG-2:
        401 bad/missing token · 403 member internal · 404 unknown member ·
        429 over the per-member concurrent cap.

        CPL-2 (master-copilot): when `remote` is set the caller presented the
        machine remote-control token (the operator's hand). That path is
        MASTER-ONLY — asks to any member other than `architect-master` → 403 —
        and it BYPASSES the exposure check + the per-member token match (the
        remote token IS the operator; `exposure: external` + member tokens stay
        the third-party TEG surface, untouched)."""
        if not bearer:
            return 401, {
                "error": "unauthorized",
                "hint": "Authorization: Bearer <member-token> required",
            }
        if remote and mid != "architect-master":
            # The remote token authorizes ONLY the operator's master, per
            # project. Any other member id is the TEG member-token surface.
            return 403, {
                "error": "remote_token_master_only",
                "member": mid,
                "hint": "the remote-control token can only ask architect-master; "
                "use a member token for other members",
            }
        try:
            member = self.team_store.team_get(mid)
        except TeamError:
            return 404, {"error": f"unknown team member {mid!r}", "member": mid}
        fm = member.get("frontmatter") or {}
        if not remote:
            exposure = str(fm.get("exposure") or "internal").strip().lower()
            if exposure != "external":
                # Revoke semantics: PATCH {exposure: internal} deleted the token,
                # so any in-flight caller lands here — 403, access is gone.
                return 403, {
                    "error": "member_not_exposed",
                    "member": mid,
                    "hint": "this member has exposure: internal — ask the operator to expose it",
                }
            tokens = TeamTokenStore(self.paths)
            if not tokens.matches(mid, bearer):
                return 401, {"error": "unauthorized", "member": mid}
        if not isinstance(body, dict):
            return 400, {"error": "JSON object body required"}
        text = str(body.get("text") or "").strip()
        if not text:
            return 400, {"error": "text required"}

        # Per-member concurrent cap (queued|running requests).
        with self._teamext_lock:
            data = self._teamext_gc(self._teamext_load())
            active = sum(
                1
                for e in data.values()
                if e.get("member") == mid and e.get("status") in ("queued", "running")
            )
            if active >= EXT_ASK_CONCURRENT_CAP:
                self._teamext_save(data)
                return 429, {
                    "error": "too_many_requests",
                    "member": mid,
                    "cap": EXT_ASK_CONCURRENT_CAP,
                    "hint": "poll your in-flight requests before asking again",
                }
            self._teamext_save(data)

        # Conv slug resolution.
        # CPL-1 — a `kind: singleton` member has exactly ONE conversation:
        # talking to it means talking to THAT thread. Resolve its bound conv
        # (live → use it; archived → unarchive + use it; none → create + bind,
        # letting the member's init prompt inject on turn 1). `session` is
        # meaningless for a singleton (one thread by definition) and ignored;
        # the 202 returns the real conv. Serialization is the existing dispatch
        # queue: a second ask while a turn runs is queued into the same conv
        # (chat_dispatch returns queued=True), never a duplicate conv.
        # Profiles keep today's behaviour EXACTLY.
        kind = str(fm.get("kind") or "").strip().lower()
        if kind == "singleton":
            conv, archived = self._member_bound_conv(mid)
            if conv is None:
                # A stable single-thread slug for this singleton; conv_meta
                # binds `member` on turn 1 (dispatch_body below carries it).
                conv = mid
            elif archived:
                self.chat_archive.unarchive(conv)
                try:
                    if getattr(self, "hub", None) is not None:
                        self.hub.broadcast(
                            {"type": "conv.unarchived", "conv": conv, "ts": _iso_now()}
                        )
                except Exception:  # noqa: BLE001 — broadcast failure is non-fatal
                    pass
        else:
            # Profile: session → continuity (same session = same conv = the
            # member remembers the thread); no session → one-shot stamp conv.
            session = str(body.get("session") or "").strip().lower()
            session = _SESSION_SAFE_RE.sub("-", session).strip("-")[:48]
            if session:
                conv = f"ext-{mid}-{session}"
            else:
                stamp = (
                    _iso_now()[:19].replace(":", "").replace("-", "").replace("T", "-")
                )
                conv = f"ext-{mid}-{stamp}"

        dispatch_body: Dict[str, Any] = {
            "text": text,
            "conv": conv,
            "member": mid,
            "author": f"{'remote' if remote else 'ext'}:{mid}",
        }
        if isinstance(body.get("context_docs"), list):
            dispatch_body["context_docs"] = body["context_docs"]

        started_at = _iso_now()
        # Reuse the EXISTING dispatch path — init prompt on turn 1, member
        # model/effort, rail visibility, singleton rules all come for free.
        code, resp = self.chat_dispatch(dispatch_body)
        if code != 202:
            return code, resp

        rid = "req-" + secrets.token_urlsafe(9)
        status = "queued" if resp.get("queued") else "running"
        entry = {
            "request_id": rid,
            "member": mid,
            "conv": conv,
            "status": status,
            "started_at": started_at,
        }
        with self._teamext_lock:
            data = self._teamext_gc(self._teamext_load())
            data[rid] = entry
            self._teamext_save(data)
        self._teamext_broadcast("team.request.created", mid, rid)
        _debug_emit(
            "team.ask",
            msg=f"external ask → {mid} (conv={conv}, request={rid})",
            conv=conv,
            data={"member": mid, "request_id": rid, "status": status},
        )
        # Watcher observes the turn to completion (final → done, else error).
        project_id = self._current_project_id()
        threading.Thread(
            target=self._teamext_watch,
            args=(project_id, rid, mid, conv, started_at),
            name=f"teamext-{rid}",
            daemon=True,
        ).start()
        return 202, {"request_id": rid, "conv": conv}

    # ── GET /team/requests/<rid> ────────────────────────────────────────
    def team_request_get_http(
        self, rid: str, *, bearer: Optional[str], remote: bool = False
    ) -> Tuple[int, Dict[str, Any]]:
        """Same bearer auth as the ask; a member token can ONLY read the
        requests created for its own member (token identity = member).

        CPL-2: the machine remote-control token additionally reads any
        `architect-master` request (the operator polls the asks it made)."""
        if not bearer:
            return 401, {"error": "unauthorized"}
        with self._teamext_lock:
            entry = self._teamext_load().get(rid)
        if entry is None:
            return 404, {"error": f"unknown request {rid!r}"}
        mid = str(entry.get("member") or "")
        tokens = TeamTokenStore(self.paths)
        authorized = tokens.matches(mid, bearer) or (
            remote and mid == "architect-master"
        )
        if not authorized:
            # Wrong member's token, revoked member, or a stale token after a
            # rotate — in every case the presented bearer no longer names the
            # member this request belongs to.
            return 401, {"error": "unauthorized"}
        out = {
            "request_id": rid,
            "status": entry.get("status"),
            "conv": entry.get("conv"),
            "started_at": entry.get("started_at"),
        }
        if entry.get("finished_at"):
            out["finished_at"] = entry["finished_at"]
        if entry.get("status") == "done":
            out["result_text"] = entry.get("result_text") or ""
        if entry.get("status") == "error":
            out["error"] = entry.get("error") or "unknown error"
        return 200, out

    # ── POST /team/<id>/token/rotate (portal-token gated at the route) ──
    def team_token_rotate_http(self, mid: str) -> Tuple[int, Dict[str, Any]]:
        try:
            member = self.team_store.team_get(mid)
        except TeamError as e:
            return self._team_err(e)
        fm = member.get("frontmatter") or {}
        if str(fm.get("exposure") or "internal").strip().lower() != "external":
            return 409, {
                "error": "member is not external — nothing to rotate",
                "member": mid,
            }
        token = TeamTokenStore(self.paths).mint(mid)  # old token dies here
        self._team_broadcast("team.updated", mid)
        _debug_emit(
            "team.token.rotate",
            msg=f"token rotated for member {mid}",
            data={"member": mid},
        )
        return 200, {"id": mid, "token": token, "rotated_at": _iso_now()}

    # ── GET /team/<id>/.well-known/agent.json (no auth) ─────────────────
    def team_agent_card_http(self, mid: str) -> Tuple[int, Dict[str, Any]]:
        try:
            member = self.team_store.team_get(mid)
        except TeamError:
            return 404, {"error": f"unknown team member {mid!r}"}
        fm = member.get("frontmatter") or {}
        if str(fm.get("exposure") or "internal").strip().lower() != "external":
            # Internal members have no public card — indistinguishable from
            # absent on purpose (don't leak the roster shape).
            return 404, {"error": f"no public card for {mid!r}"}
        scheme = "https" if getattr(self, "tls_enabled", True) else "http"
        base_url = f"{scheme}://127.0.0.1:{getattr(self, 'port', 0)}"
        card = build_member_card(
            fm,
            member.get("body") or "",
            base_url=base_url,
            project_id=str(self.cluster.id),
        )
        return 200, card

    # ── watcher: observe the turn to completion ─────────────────────────
    def _teamext_latest_final(self, conv: str, after_ts: str) -> Optional[str]:
        """Text of the newest `chat.assistant.final` for `conv` with
        ts >= after_ts, or None. Timeline walk mirrors chatread.py."""
        best_ts, best_text = "", None
        try:
            if not self.paths.timeline_dir.exists():
                return None
            for f in _iter_timeline_files(self.paths):
                for ev in _read_timeline_file(f):
                    if ev.get("conv") != conv:
                        continue
                    if ev.get("type") != "chat.assistant.final":
                        continue
                    ts = str(ev.get("ts") or "")
                    if ts >= after_ts and ts >= best_ts:
                        best_ts, best_text = ts, str(ev.get("text") or "")
        except Exception:
            return None
        return best_text

    def _teamext_watch(
        self, project_id: Optional[str], rid: str, mid: str, conv: str, after_ts: str
    ) -> None:
        """Background observer for one external request. Re-binds the
        originating project on THIS thread (FC-2 pattern — the request
        threadlocal died with the 202) so every self.* property resolves to
        the right ProjectContext. Terminal states: done (final found on the
        timeline) or error (turn ended finalless / observer budget spent)."""
        try:
            self._set_req_project(project_id)
        except Exception:
            pass
        deadline = time.time() + _WATCH_MAX_SECS
        idle_since: Optional[float] = None
        marked_running = False
        try:
            while time.time() < deadline and not self.stopping.is_set():
                live = False
                try:
                    live = self.chat_sessions.has(conv)
                except Exception:
                    pass
                if live:
                    idle_since = None
                    if not marked_running:
                        # Flip queued → running ONCE (don't rewrite the
                        # index every poll). None = entry GC'd; stop.
                        if self._teamext_update(rid, status="running") is None:
                            return
                        marked_running = True
                else:
                    final = self._teamext_latest_final(conv, after_ts)
                    if final is not None:
                        self._teamext_update(
                            rid,
                            status="done",
                            result_text=final,
                            finished_at=_iso_now(),
                        )
                        self._teamext_broadcast("team.request.done", mid, rid)
                        return
                    # No runner and no final (yet): allow a short grace for
                    # the timeline append / spawn hand-off, then declare error.
                    if idle_since is None:
                        idle_since = time.time()
                    elif time.time() - idle_since > _IDLE_GRACE_SECS:
                        self._teamext_update(
                            rid,
                            status="error",
                            error="turn ended without an assistant final "
                            "(spawn failure, cancel, or crash)",
                            finished_at=_iso_now(),
                        )
                        self._teamext_broadcast("team.request.error", mid, rid)
                        return
                time.sleep(_WATCH_POLL_SECS)
            self._teamext_update(
                rid,
                status="error",
                error=f"request watcher timed out after {_WATCH_MAX_SECS}s",
                finished_at=_iso_now(),
            )
            self._teamext_broadcast("team.request.error", mid, rid)
        except Exception as e:  # noqa: BLE001 — watcher must never take the daemon down
            _log(f"teamext watcher {rid} failed: {e}")
        finally:
            try:
                self._clear_req_project()
            except Exception:
                pass


# Re-exported for route modules that only need the path helper.
def member_id_from_path(p: str, prefix: str, suffix: str) -> str:
    """Extract `<id>` from `<prefix><id><suffix>` URL paths, unquoted."""
    return urllib.parse.unquote(p[len(prefix) : len(p) - len(suffix)]).strip("/")
