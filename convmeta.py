"""convmeta.py — extracted from chatsvc.py (daemon-architecture-v2 Phase 3d).

ConvMetaMixin: methods moved VERBATIM out of ChatMixin; Daemon inherits both so
every self.* resolves on the combined instance -> byte-identical."""

from __future__ import annotations

from fsatomic import atomic_write_json

import json
from typing import Any, Dict, Optional, Tuple

from prompts import _agent_type_from_conv_slug, _agent_type_normalised
from utils import _iso_now, _log


class ConvMetaMixin:
    def _conv_meta_path(self) -> Any:
        return self.paths.runtime / "conv_meta.json"

    def _conv_meta_load(self) -> Dict[str, Dict[str, str]]:
        # py-1.16.0 (D-STORE-RETENTION-01) — mtime-keyed cache. This is
        # called on hot paths (_get_model/_get_effort/_parent, often 4+×
        # per spawn) and previously re-read + JSON-parsed the whole
        # sidecar from disk every time. Reparse only when the file mtime
        # changes (every write here uses tmp+rename → mtime bumps).
        p = self._conv_meta_path()
        try:
            if not p.exists():
                self._conv_meta_cache = (None, {})
                return {}
            mtime = p.stat().st_mtime_ns
        except OSError:
            mtime = None
        cache = getattr(self, "_conv_meta_cache", None)
        if mtime is not None and cache is not None and cache[0] == mtime:
            return cache[1]
        try:
            data = json.loads(p.read_text() or "{}") or {}
        except Exception:
            return {}
        if mtime is not None:
            self._conv_meta_cache = (mtime, data)
        return data

    def _conv_meta_get(self, conv: str) -> Tuple[str, Optional[str]]:
        meta = self._conv_meta_load().get(conv) or {}
        # py-1.10.12 — Slug-implied type wins on read too. Heals any
        # historic sidecar entry written before py-1.10.12 that has
        # the wrong agent_type (e.g. ikamiro had several
        # roadmap-architect-* convs persisted as 'custom').
        slug_implied = _agent_type_from_conv_slug(conv)
        recorded = _agent_type_normalised(meta.get("agent_type"))
        return (
            slug_implied if slug_implied else recorded,
            (meta.get("agent_id") or None),
        )

    def _conv_meta_get_model(self, conv: str) -> Optional[str]:
        """MP1 (py-1.13.3) — Read the per-conv model preference stored
        by the cockpit's NewAgentWizard. Returns None / 'auto' when no
        override is set; otherwise one of 'opus' / 'sonnet' / 'haiku'
        (or any string claude-code accepts, incl. pinned ids like
        'claude-opus-4-8'). Used by ChatRunner.spawn to inject
        `--model <id>` into the CLI argv."""
        meta = self._conv_meta_load().get(conv) or {}
        m = str(meta.get("model") or "").strip()
        if not m or m.lower() == "auto":
            return None
        return m

    def _conv_meta_get_effort(self, conv: str) -> Optional[str]:
        """MP3 (py-1.13.4) — Read the per-conv effort (reasoning-depth)
        preference. Returns None / 'default' when unset; otherwise one
        of low/medium/high/xhigh/max. Used by ChatRunner.spawn to inject
        `--effort <level>` into the CLI argv. This is claude-code's
        thinking dial — there is no separate thinking flag."""
        meta = self._conv_meta_load().get(conv) or {}
        e = str(meta.get("effort") or "").strip().lower()
        if not e or e == "default":
            return None
        if e not in ("low", "medium", "high", "xhigh", "max"):
            return None
        return e

    def _conv_meta_get_client(self, conv: str) -> Optional[str]:
        """DM-CLI-02 (multi-cli-clients) — read the per-conv CLI-client
        preference. Returns None when unset (mirrors
        `_conv_meta_get_model`/`_conv_meta_get_effort`); the caller
        falls back to `clidrivers.driver_for(None)` -> claude-code."""
        meta = self._conv_meta_load().get(conv) or {}
        c = str(meta.get("client") or "").strip().lower()
        return c or None

    def _conv_meta_get_member(self, conv: str) -> Optional[str]:
        """ATM10 (agent-team) — Read the team member this conv is an INSTANCE
        of. Returns the member id (e.g. 'api-developer') or None when the
        conv is not bound to any member. The binding is frozen after the
        first message; see teamsvc._member_dispatch_prep."""
        meta = self._conv_meta_load().get(conv) or {}
        m = str(meta.get("member") or "").strip()
        return m or None

    def _conv_meta_set(
        self,
        conv: str,
        agent_type: str,
        agent_id: Optional[str],
        parent_conv: Optional[str] = None,
        initiative_id: Optional[str] = None,
        task_id: Optional[str] = None,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        client: Optional[str] = None,
        member: Optional[str] = None,
    ) -> None:
        try:
            # py-1.16.0 (D-STORE-RETENTION-01) — drop the read cache so the
            # load below returns a FRESH dict to mutate (not the shared
            # cached object). The tmp+rename write bumps mtime, so the
            # next reader reloads anyway.
            self._conv_meta_cache = None
            all_meta = self._conv_meta_load()
            existed_before = conv in all_meta
            before = dict(all_meta.get(conv) or {})
            entry = all_meta.get(conv) or {}
            entry["agent_type"] = _agent_type_normalised(agent_type)
            if agent_id:
                entry["agent_id"] = agent_id
            # py-1.10.16 — Parent-child conv linkage for the architect
            # wake protocol (initiative `architect-wake-on-subagent`).
            # The architect dispatches a subagent with `parent_conv: <me>`
            # so that when the subagent's final fires, the daemon can
            # post a `[architect-wake]` turn back to the architect's
            # conv. Persisted so a daemon restart preserves the linkage.
            if parent_conv:
                entry["parent_conv"] = parent_conv
            # py-1.10.19 — Initiative + task linkage. Drives the
            # cockpit's per-initiative working spinner + per-task
            # blink in the roadmap (initiative `agent-activity-surface`).
            # Stored alongside parent_conv so a daemon restart preserves
            # the full join, and the architect wake hook can quote them
            # back to the parent ("subagent A101 on I1/D-DBG-01 finished").
            if initiative_id:
                entry["initiative_id"] = initiative_id
            if task_id:
                entry["task_id"] = task_id
            # MP1 (py-1.13.3) — Per-conv model preference. Normalised to
            # lowercase; 'auto' is stored explicitly so chained turns
            # don't pick up a stale value. Empty / None means "no
            # override".
            if model is not None:
                # Preserve case for pinned ids (claude-opus-4-8); only
                # the aliases are conventionally lowercase anyway.
                m_norm = str(model).strip()
                if m_norm:
                    entry["model"] = m_norm
                elif "model" in entry:
                    del entry["model"]
            # MP3 (py-1.13.4) — per-conv effort (reasoning depth).
            if effort is not None:
                e_norm = str(effort).strip().lower()
                if e_norm:
                    entry["effort"] = e_norm
                elif "effort" in entry:
                    del entry["effort"]
            # DM-CLI-02 (multi-cli-clients) — per-conv CLI-client
            # preference, same "unset means no override" shape as
            # model/effort above.
            if client is not None:
                c_norm = str(client).strip().lower()
                if c_norm:
                    entry["client"] = c_norm
                elif "client" in entry:
                    del entry["client"]
            # ATM10 (agent-team) — the team member this conv is an INSTANCE of.
            # Persisted beside type/model/effort so the binding survives daemon
            # restarts, drives the /team `instances` join, and is frozen after
            # the first message. Written once (on the first dispatch that
            # carries `member`); chained/silent re-spawns pass member=None and
            # inherit the stored value.
            if member is not None:
                m_norm = str(member).strip()
                if m_norm:
                    entry["member"] = m_norm
                elif "member" in entry:
                    del entry["member"]
            all_meta[conv] = entry
            p = self._conv_meta_path()
            p.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_json(p, all_meta, sort_keys=True)
            # py-1.11.0 — Broadcast conv.created (first-time) or
            # conv.meta_updated (subsequent) so cockpits update the rail
            # WITHOUT waiting for a state.rebuilt + refetch. The hub may
            # not be wired yet during boot — guard with hasattr.
            if getattr(self, "hub", None) is not None:
                try:
                    payload = {
                        "conv": conv,
                        "agent_type": entry.get("agent_type"),
                        "agent_id": entry.get("agent_id"),
                        "parent_conv": entry.get("parent_conv"),
                        "initiative_id": entry.get("initiative_id"),
                        "task_id": entry.get("task_id"),
                        "model": entry.get("model"),
                        "effort": entry.get("effort"),
                        "client": entry.get("client"),
                        "member": entry.get("member"),
                        "ts": _iso_now(),
                    }
                    if not existed_before:
                        self.hub.broadcast({"type": "conv.created", **payload})
                    elif before != entry:
                        self.hub.broadcast({"type": "conv.meta_updated", **payload})
                except Exception as bx:
                    _log(f"conv meta broadcast failed: {bx}")
        except Exception as e:
            _log(f"conv_meta write failed: {e}")

    def _conv_meta_parent(self, conv: str) -> Optional[str]:
        """Return the parent conv id recorded for `conv`, if any."""
        meta = self._conv_meta_load().get(conv) or {}
        p = meta.get("parent_conv")
        return str(p) if p else None
