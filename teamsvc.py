"""teamsvc.py — the /team HTTP surface + team.* WS events + member dispatch.

Initiative `agent-team` (ATM9, ATM10, ATM5). TeamMixin is inherited by the
Daemon so every `self.*` (team_store, hub, chat_archive, conv_meta helpers)
resolves on the combined instance — mirrors crud.py / chatsvc.py.

The in-process data layer (schema, validation, CRUD, seed) lives in team.py
(`TeamStore`, ATM2); this module is the WIRE + orchestration layer:

- ATM9: GET/POST/PATCH/DELETE /team[/<id>] mapped to TeamStore, each
  mutation broadcasting `team.created|updated|deleted {id, ts}` on the
  chat.* WS bus; GET /team decorates each member with a live `instances`
  count (non-archived convs whose conv_meta.member == id).
- ATM10: `_member_dispatch_prep` resolves a `member` on /chat/dispatch into
  (agent_type, client, model, effort) + enforces freeze-after-first-message
  and the singleton one-live-instance rule. `client` added DM-CLI-02
  (multi-cli-clients) — same override rule as model/effort.
- ATM5: POST /team/draft — free text → structured member draft via the
  project's Anthropic key (read-only; 503 when no key).
"""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple

from team import (
    STRONGEST_MODEL_ALIAS,
    TeamError,
    _ID_RE,
)
from teamext import TeamTokenStore
from utils import _iso_now, _log

# The concrete Anthropic model id used for the /team/draft normaliser call.
# The DRAFT the operator gets back always carries the strongest ALIAS
# ("opus") in its `model` field — this is only the wire model for the
# single normalisation request.
_DRAFT_API_MODEL = "claude-opus-4-8"
_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_ANTHROPIC_VERSION = "2023-06-01"


class TeamMixin:
    # ── ATM9: /team HTTP CRUD ──────────────────────────────────────────
    def _team_broadcast(self, event: str, mid: str) -> None:
        """Emit team.created|updated|deleted {id, ts} on the chat.* bus."""
        try:
            if getattr(self, "hub", None) is not None:
                self.hub.broadcast({"type": event, "id": mid, "ts": _iso_now()})
        except Exception as e:  # noqa: BLE001 — a broadcast failure is non-fatal
            _log(f"team broadcast {event} failed: {e}")

    def _team_instance_counts(self) -> Dict[str, int]:
        """Per-member count of LIVE instances = non-archived convs whose
        conv_meta.member == <id>. Joins the conv_meta sidecar (ATM10) with
        the archive store, exactly like /team's `instances` field."""
        counts: Dict[str, int] = {}
        try:
            meta = self._conv_meta_load()
        except Exception:
            return counts
        for conv, entry in (meta or {}).items():
            if not isinstance(entry, dict):
                continue
            member = str(entry.get("member") or "").strip()
            if not member:
                continue
            try:
                if self.chat_archive.is_archived(conv):
                    continue
            except Exception:
                pass
            counts[member] = counts.get(member, 0) + 1
        return counts

    @staticmethod
    def _member_exposure(fm: Dict[str, Any]) -> str:
        """TEG-1 — exposure with the schema default (absent = internal)."""
        return str(fm.get("exposure") or "internal").strip().lower()

    def team_list_http(self) -> Tuple[int, Dict[str, Any]]:
        members = self.team_store.team_list()
        counts = self._team_instance_counts()
        for m in members:
            m["instances"] = counts.get(str(m.get("id") or ""), 0)
            # TEG-1 — normalise the default so pre-v1.30 member files read
            # back as internal. The public list NEVER includes tokens.
            m["exposure"] = self._member_exposure(m)
        return 200, {"members": members, "count": len(members)}

    def team_get_http(
        self, mid: str, *, include_token: bool = False
    ) -> Tuple[int, Dict[str, Any]]:
        try:
            member = self.team_store.team_get(mid)
        except TeamError as e:
            return self._team_err(e)
        counts = self._team_instance_counts()
        member["instances"] = counts.get(mid, 0)
        fm = member.get("frontmatter") or {}
        fm["exposure"] = self._member_exposure(fm)
        # TEG-1 — the cockpit (portal-token caller) additionally gets the
        # member's bearer token when it is external, so the Team UI can show
        # + copy it. Anonymous/loopback reads never see it.
        if include_token and fm["exposure"] == "external":
            tok = TeamTokenStore(self.paths).get(mid)
            if tok:
                member["token"] = tok
        return 200, member

    def team_create_http(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        if not isinstance(body, dict):
            return 400, {"error": "JSON object body required"}
        try:
            member = self.team_store.team_create(body, today=_iso_now()[:10])
        except TeamError as e:
            return self._team_err(e)
        mid = str(member["frontmatter"].get("id"))
        # TEG-1 — a member BORN external gets its token minted right away
        # (same rule as the internal→external PATCH transition).
        if self._member_exposure(member.get("frontmatter") or {}) == "external":
            member["token"] = TeamTokenStore(self.paths).ensure(mid)
        self._team_broadcast("team.created", mid)
        return 201, member

    def team_update_http(
        self, mid: str, patch: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        if not isinstance(patch, dict):
            return 400, {"error": "JSON object body required"}
        # TEG-1 — capture the PRE-patch exposure so the token lifecycle can
        # key on the transition (mint on →external, revoke on →internal).
        try:
            before = self.team_store.team_get(mid)
        except TeamError as e:
            return self._team_err(e)
        old_exposure = self._member_exposure(before.get("frontmatter") or {})
        try:
            member = self.team_store.team_update(mid, patch, today=_iso_now()[:10])
        except TeamError as e:
            return self._team_err(e)
        new_exposure = self._member_exposure(member.get("frontmatter") or {})
        tokens = TeamTokenStore(self.paths)
        if new_exposure == "external":
            # Mint on internal→external (idempotent: an already-external
            # member keeps its token — rotation is an explicit endpoint).
            member["token"] = tokens.ensure(mid)
        elif old_exposure == "external":
            # Revoke: exposure flipped back to internal → the token entry is
            # deleted atomically; any in-flight external caller 403s from
            # its next request. (The Team UI's "Revoke access" is this PATCH.)
            tokens.delete(mid)
            _log(f"team: revoked external token for member {mid}")
        self._team_broadcast("team.updated", mid)
        return 200, member

    def team_delete_http(self, mid: str) -> Tuple[int, Dict[str, Any]]:
        try:
            self.team_store.team_delete(mid)
        except TeamError as e:
            return self._team_err(e)
        self._team_broadcast("team.deleted", mid)
        return 200, {"ok": True, "deleted": mid}

    @staticmethod
    def _team_err(e: TeamError) -> Tuple[int, Dict[str, Any]]:
        body: Dict[str, Any] = {"error": e.message}
        if getattr(e, "extra", None):
            body.update(e.extra)
        return int(getattr(e, "code", 400)), body

    # ── ATM10: member → dispatch resolution ────────────────────────────
    def _member_dispatch_prep(
        self,
        conv: str,
        member: str,
        *,
        body_agent_type: Optional[str],
        body_model: Optional[str],
        body_effort: Optional[str],
        body_client: Optional[str] = None,
    ) -> Tuple[
        Optional[Tuple[int, Dict[str, Any]]],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
    ]:
        """Resolve a `member` binding for a /chat/dispatch turn.

        Returns (err | None, agent_type, client, model, effort). On any
        rule violation the first element is a ready (code, body) HTTP
        error and the rest are None:
          - unknown member                → 400
          - rebind after ≥1 message       → 409
          - 2nd live singleton instance   → 409 singleton_instance_exists

        Resolution (when OK): agent_type ← member.agent_type; client/
        model/effort ← member values UNLESS the dispatch body explicitly
        overrides them (overrides win on ANY turn). The caller passes the
        resolved values through to _spawn_chat_turn / conv_meta so they
        persist.
        """
        try:
            m = self.team_store.team_get(member)
        except TeamError:
            return (
                (400, {"error": f"unknown team member {member!r}", "member": member}),
                None,
                None,
                None,
                None,
            )
        fm = m.get("frontmatter") or {}

        # Freeze: a conv already bound to a DIFFERENT member that has spoken
        # (≥1 message) cannot be rebound.
        existing = self._conv_meta_get_member(conv)
        if existing and existing != member and self._conv_has_message(conv):
            return (
                (
                    409,
                    {
                        "error": "member binding is frozen after the first message",
                        "conv": conv,
                        "bound_member": existing,
                        "requested_member": member,
                    },
                ),
                None,
                None,
                None,
                None,
            )

        # Singleton: only one live (non-archived) instance across convs.
        if str(fm.get("kind")) == "singleton":
            other = self._singleton_live_conv(member, exclude_conv=conv)
            if other is not None:
                return (
                    (
                        409,
                        {
                            "error": "singleton_instance_exists",
                            "member": member,
                            "conv": other,
                        },
                    ),
                    None,
                    None,
                    None,
                    None,
                )

        resolved_type = str(fm.get("agent_type") or "custom").strip() or "custom"
        # body overrides win on any turn; member fills the gaps.
        resolved_client = (
            body_client
            if body_client
            else (str(fm.get("client") or "").strip().lower() or None)
        )
        resolved_model = (
            body_model if body_model else (str(fm.get("model") or "").strip() or None)
        )
        resolved_effort = (
            body_effort
            if body_effort
            else (str(fm.get("effort") or "").strip().lower() or None)
        )
        # 'default' effort is a no-op sentinel — treat as no override.
        if resolved_effort in ("default", ""):
            resolved_effort = None
        # body agent_type does NOT override the member's baseline (ATM10:
        # "agent_type ← member's agent_type"). Ignore body_agent_type here.
        _ = body_agent_type
        return None, resolved_type, resolved_client, resolved_model, resolved_effort

    def _conv_has_message(self, conv: str) -> bool:
        """True iff this conv already has ≥1 assistant/user turn on disk —
        used to freeze the member binding after the first message."""
        try:
            from prompts import _iter_timeline_files, _read_timeline_file

            for f in _iter_timeline_files(self.paths):
                for ev in _read_timeline_file(f):
                    if ev.get("conv") != conv:
                        continue
                    if ev.get("type") in (
                        "chat.user",
                        "chat.assistant.final",
                        "chat.assistant.delta",
                    ):
                        return True
        except Exception:
            return False
        return False

    def _singleton_live_conv(self, member: str, *, exclude_conv: str) -> Optional[str]:
        """Return the conv id of an existing LIVE (non-archived) instance of
        `member`, or None. Used to enforce kind:singleton."""
        try:
            meta = self._conv_meta_load()
        except Exception:
            return None
        for conv, entry in (meta or {}).items():
            if conv == exclude_conv or not isinstance(entry, dict):
                continue
            if str(entry.get("member") or "").strip() != member:
                continue
            try:
                if self.chat_archive.is_archived(conv):
                    continue
            except Exception:
                pass
            return conv
        return None

    def _team_backfill_onboarding(self) -> None:
        """Bind the coordinator conv (_onboarding_v1) to architect-master on
        boot if it has no member yet (ATM10 'master conv & first boot')."""
        try:
            conv = "_onboarding_v1"
            # Only backfill an EXISTING coordinator conv — never mint one. On a
            # fresh cluster the conv doesn't exist yet; creating a conv_meta
            # entry here would surface a phantom live conv in /chat/snapshot.
            meta = self._conv_meta_load()
            entry = meta.get(conv)
            if not isinstance(entry, dict):
                return
            if str(entry.get("member") or "").strip():
                return
            if not self.team_store.exists("architect-master"):
                return
            agent_type, agent_id = self._conv_meta_get(conv)
            self._conv_meta_set(conv, agent_type, agent_id, member="architect-master")
            _log("team: backfilled _onboarding_v1 → member=architect-master")
        except Exception as e:  # noqa: BLE001 — backfill is best-effort
            _log(f"team onboarding backfill skipped: {e}")

    # ── ATM5: /team/draft — free text → structured member draft ────────
    def team_draft(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        if not isinstance(body, dict):
            return 400, {"error": "JSON object body required"}
        raw_text = str(body.get("raw_text") or body.get("text") or "").strip()
        if not raw_text:
            return 400, {"error": "raw_text required"}
        name = str(body.get("name") or "").strip()
        emoji = str(body.get("emoji") or "").strip()

        key = self._anthropic_key()
        if not key:
            return 503, {
                "error": "llm_unavailable",
                "hint": (
                    "no Anthropic API key found in .meshkore/credentials/ "
                    "(anthropic.key / anthropic.env / ANTHROPIC_API_KEY / "
                    "claude-code.env) — set one, or fill the member fields by hand."
                ),
            }

        # Slug: snake-lowercase of name, dedup against existing ids. If the
        # base slug already exists, 409 with a free suggestion (ATM5).
        base = self._slugify(name or raw_text[:24])
        if not base:
            base = "member"
        if self.team_store.exists(base):
            return 409, {
                "error": "slug_exists",
                "id": base,
                "suggested": self._free_slug(base),
            }

        try:
            draft = self._llm_member_draft(raw_text, name=name, emoji=emoji, key=key)
        except _LlmError as e:
            return 503, {"error": "llm_unavailable", "hint": str(e)}

        draft["id"] = base
        if name:
            draft["name"] = name
        if emoji:
            draft["emoji"] = emoji
        draft.setdefault("name", name or base)
        draft.setdefault("emoji", "🤖")
        # Policy: strongest alias, tune with effort; drafts are always profiles.
        draft["model"] = STRONGEST_MODEL_ALIAS
        draft["effort"] = "default"
        draft["kind"] = "profile"
        draft["required"] = False
        return 200, draft

    def _anthropic_key(self) -> Optional[str]:
        """Resolve an Anthropic API key from env or .meshkore/credentials/.
        Never logs the value. Returns None when absent."""
        env = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
        if env:
            return env
        creds = self.paths.credentials
        candidates = (
            "anthropic.key",
            "anthropic-api.key",
            "anthropic.env",
            "claude-code.env",
        )
        for name in candidates:
            p = creds / name
            try:
                if not p.is_file():
                    continue
                text = p.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            k = self._extract_key(text)
            if k:
                return k
        return None

    @staticmethod
    def _extract_key(text: str) -> Optional[str]:
        # `.key` files may be the bare token; `.env` files are KEY=VALUE lines.
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                _, _, val = line.partition("=")
                val = val.strip().strip('"').strip("'")
            else:
                val = line
            if val.startswith("sk-ant"):
                return val
        stripped = text.strip()
        if stripped.startswith("sk-ant"):
            return stripped
        return None

    def _slugify(self, s: str) -> str:
        s = (s or "").strip().lower()
        s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
        s = s[:32].strip("-")
        if s and not re.match(r"^[a-z]", s):
            s = "m-" + s
            s = s[:32].strip("-")
        return s if _ID_RE.match(s or "") else (s or "")

    def _free_slug(self, base: str) -> str:
        for n in range(2, 100):
            cand = f"{base[:30]}-{n}"
            if not self.team_store.exists(cand):
                return cand
        return f"{base[:28]}-{_iso_now()[-6:-1]}"

    def _llm_member_draft(
        self, raw_text: str, *, name: str, emoji: str, key: str
    ) -> Dict[str, Any]:
        """Single short Messages-API call → parsed member draft dict.
        Raises _LlmError on any transport/parse failure."""
        system = (
            "You normalise an operator's free-text description of an AI team "
            "member into a structured JSON draft. Reply with ONE json object and "
            "NOTHING else. Keys: name (string), emoji (one glyph), refs (array of "
            "path-like strings you find VERBATIM in the text — .meshkore/..., "
            "apps/..., webapp/..., file paths/urls; never invent), "
            "credentials_hint (string; '(none — read-only role)' if the text says "
            "no credentials), prompt (a markdown system prompt for the member: "
            "mission, responsibilities, attributions & limits, reference docs — "
            "match the language the operator wrote in). Do NOT include id, model, "
            "effort or kind — those are set by policy."
        )
        user = raw_text
        if name:
            user = f"name: {name}\n{user}"
        if emoji:
            user = f"emoji: {emoji}\n{user}"
        payload = json.dumps(
            {
                "model": _DRAFT_API_MODEL,
                "max_tokens": 1500,
                "system": system,
                "messages": [{"role": "user", "content": user}],
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            _ANTHROPIC_URL,
            data=payload,
            method="POST",
            headers={
                "content-type": "application/json",
                "x-api-key": key,
                "anthropic-version": _ANTHROPIC_VERSION,
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8")[:300]
            except Exception:
                pass
            raise _LlmError(f"Anthropic API {e.code}: {detail}") from e
        except (urllib.error.URLError, OSError) as e:
            raise _LlmError(f"Anthropic API unreachable: {e}") from e
        try:
            data = json.loads(raw)
            parts = data.get("content") or []
            text = "".join(
                p.get("text", "") for p in parts if isinstance(p, dict)
            ).strip()
        except Exception as e:
            raise _LlmError(f"unexpected API response: {e}") from e
        draft = self._parse_json_object(text)
        if not isinstance(draft, dict):
            raise _LlmError("model did not return a JSON object")
        # Keep only known keys; coerce refs to a list of strings.
        out: Dict[str, Any] = {}
        if draft.get("name"):
            out["name"] = str(draft["name"]).strip()
        if draft.get("emoji"):
            out["emoji"] = str(draft["emoji"]).strip()
        refs = draft.get("refs")
        out["refs"] = (
            [str(r).strip() for r in refs if str(r).strip()]
            if isinstance(refs, list)
            else []
        )
        if draft.get("credentials_hint"):
            out["credentials_hint"] = str(draft["credentials_hint"]).strip()
        out["prompt"] = str(draft.get("prompt") or "").strip()
        return out

    @staticmethod
    def _parse_json_object(text: str) -> Any:
        text = text.strip()
        # Strip ```json fences if the model wrapped its reply.
        if text.startswith("```"):
            text = re.sub(r"^```[a-zA-Z]*\n", "", text)
            text = re.sub(r"\n```$", "", text).strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Salvage the first {...} block.
            start = text.find("{")
            end = text.rfind("}")
            if 0 <= start < end:
                try:
                    return json.loads(text[start : end + 1])
                except json.JSONDecodeError:
                    return None
        return None


class _LlmError(Exception):
    """Internal — any failure talking to the draft LLM. Surfaced as 503."""
