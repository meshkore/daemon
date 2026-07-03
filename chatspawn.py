"""chatspawn.py — extracted from chatsvc.py (daemon-architecture-v2 Phase 3d).

ChatSpawnMixin: methods moved VERBATIM out of ChatMixin; Daemon inherits both so
every self.* resolves on the combined instance -> byte-identical."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from prompts import _agent_type_from_conv_slug
from runner import ChatRunner
from utils import _append_timeline, _debug_emit, _log


class ChatSpawnMixin:
    def _spawn_chat_turn(
        self,
        conv: str,
        prompt: str,
        *,
        context_docs: Optional[List[Dict[str, Any]]] = None,
        agent_type: Optional[str] = None,
        agent_id: Optional[str] = None,
        parent_conv: Optional[str] = None,
        initiative_id: Optional[str] = None,
        task_id: Optional[str] = None,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        member: Optional[str] = None,
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
        # py-1.10.12 — Slug-implied type wins. The conv slug
        # (roadmap-architect-<N>) is unforgeable signal of intent.
        # If the body/sidecar disagree, the slug is right and they
        # are drift. Heals stale ikamiro-style conv_meta where the
        # body field never carried the agent_type.
        slug_implied = _agent_type_from_conv_slug(conv)
        if slug_implied and resolved_type != slug_implied:
            _log(
                f"conv {conv}: slug implies agent_type={slug_implied!r} "
                f"but resolved={resolved_type!r}; forcing slug-implied"
            )
            resolved_type = slug_implied
        # Persist whatever we end up with so subsequent turns inherit it.
        # `parent_conv` / `initiative_id` / `task_id` only overwrite the
        # sidecar when explicitly provided — silent updates (chained
        # re-spawns) reuse whatever was written on the first dispatch.
        self._conv_meta_set(
            conv,
            resolved_type,
            resolved_id,
            parent_conv=parent_conv,
            initiative_id=initiative_id,
            task_id=task_id,
            model=model,
            effort=effort,
            member=member,
        )
        # ATM10 (agent-team) — if this conv is an INSTANCE of a team member,
        # resolve the member's init-prompt BODY so BriefingPipeline can inject
        # it into the FIRST turn's system prompt (verbatim). Read from the
        # sidecar so chained turns inherit the binding even when the dispatch
        # body omitted `member`. Only the body is threaded through; the
        # pipeline itself decides to emit it on turn 1 only.
        member_body: Optional[str] = None
        try:
            bound_member = self._conv_meta_get_member(conv)
            if bound_member:
                member_body = (
                    self.team_store.team_get(bound_member).get("body") or ""
                ).strip() or None
        except Exception:
            member_body = None
        # MP1 (py-1.13.3) / MP3 (py-1.13.4) — Resolve model + effort
        # AFTER the sidecar write so chained turns inherit even when the
        # dispatch body omitted them. Each returns None when the
        # preference is the "auto"/"default" sentinel — ChatRunner.spawn
        # skips the matching CLI flag in that case.
        resolved_model = self._conv_meta_get_model(conv)
        resolved_effort = self._conv_meta_get_effort(conv)
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
            model=resolved_model,
            effort=resolved_effort,
            member_body=member_body,
            daemon=self,
            # FC-2 (daemon-centralized) — capture the dispatch's project so the
            # runner's BACKGROUND thread re-binds it before any self.daemon.*
            # callback (anchor / conv_meta / architect-wake / task-resolution /
            # usage / archive). Without this they persist into the DEFAULT
            # project (the request threadlocal is cleared the instant POST
            # /chat/dispatch returns 202).
            project_id=self._current_project_id(),
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
            # py-1.12.19 — Standard v16 auto-flush. After a turn finishes
            # with no in-memory pending, check the disk queue for the
            # conv. If a queued item exists, pop the head and dispatch
            # it as the next turn — operator's accumulated instructions
            # land seamlessly. Carries the same context_docs / agent_type
            # / agent_id as the just-finished turn (chain inheritance).
            on_idle=lambda c: self._maybe_flush_chat_queue(
                c,
                context_docs=chain_ctx,
                agent_type=chain_type,
                agent_id=chain_id,
            ),
        )
        # py-1.11.0 — snapshot.v1 contract: emit conv.activity AFTER
        # ChatSessions.start() registers the conv so the broadcast's
        # `live` flag is true (matches what /chat/convs would return).
        # Also emit for the parent so its `coordinating` + `waiting_on`
        # flip in one round-trip instead of waiting for state.rebuilt.
        self._broadcast_conv_activity(conv)
        if parent_conv:
            self._broadcast_conv_activity(parent_conv)
        return runner

    def _persist_user_event(
        self,
        conv: str,
        text: str,
        *,
        author: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """py-1.14.11 — Single source for 'the operator's message enters
        the conversation': append a `chat.user` timeline event and
        broadcast it. Reused by `chat_dispatch` AND the queue-flush path
        (`_maybe_flush_chat_queue`) so a flushed queued message lands in
        the timeline (chat history) and on the wall in chronological
        order — exactly like a live dispatch. Before this, the flush
        path called `_spawn_chat_turn` directly with no user event, so
        the queued message was missing from the wall AND vanished from
        history on reload. `_append_timeline` stamps `ts`, so the user
        event sorts before the assistant final the turn produces."""
        user_ev: Dict[str, Any] = {
            "type": "chat.user",
            "author": author or self.identity,
            "text": text,
            "conv": conv,
        }
        if attachments:
            user_ev["attachments"] = attachments
        ev = _append_timeline(self.paths, user_ev)
        self.hub.broadcast(ev)
        return ev

    def _maybe_flush_chat_queue(
        self,
        conv: str,
        *,
        context_docs: Optional[List[Dict[str, Any]]] = None,
        agent_type: Optional[str] = None,
        agent_id: Optional[str] = None,
    ) -> bool:
        """Standard v16 auto-flush hook. Called by ChatSessions when a
        conv has just gone idle (in-memory `pending` was empty / cancelled
        + slot popped) AND by the idle-flush sweep (`_flush_idle_chat_queues`).
        If the disk queue has items, pop the head and dispatch it as the
        next turn. The just-popped item gets `queue.item.sent` broadcast by
        `ChatQueueManager.pop_head`. Returns True iff a turn was spawned.

        py-1.14.6 — idempotency guard: refuse if a turn is already in
        flight for this conv. The on_idle path pops the session slot
        BEFORE calling here (so has() is False — proceeds normally), but
        the reaper-driven sweep can race a turn that just started; the
        guard keeps the two triggers from double-spawning. When omitted,
        agent_type/agent_id/context fall back to the persisted conv
        sidecar inside `_spawn_chat_turn` — so a boot/idle flush with no
        in-flight turn to inherit from still resolves the right agent."""
        if self.chat_sessions.has(conv):
            return False
        try:
            head = self.chat_queue_manager.pop_head(conv)
        except Exception as e:
            _log(f"queue auto-flush pop_head failed for {conv}: {e}")
            return False
        if head is None:
            return False
        text = str(head.get("text") or "").strip()
        if not text:
            return False
        _debug_emit(
            "queue.auto-flush",
            msg=f"flushing queue head into conv={conv}",
            conv=conv,
            data={"item_id": head.get("id"), "text_preview": text[:200]},
        )
        try:
            # py-1.14.11 — persist the user event BEFORE spawning, exactly
            # like chat_dispatch, so the flushed queued message appears on
            # the wall (and in history) chronologically before the agent's
            # response. pop_head already broadcast queue.item.sent (cockpit
            # drops it from the queue strip); this is what makes it a real
            # user bubble. `head` has no stored author → defaults to self.identity.
            self._persist_user_event(conv, text)
            self._spawn_chat_turn(
                conv,
                text,
                context_docs=context_docs,
                agent_type=agent_type,
                agent_id=agent_id,
            )
            return True
        except Exception as e:
            _log(f"queue auto-flush spawn failed for {conv}: {e}")
            return False

    def _flush_idle_chat_queues(self) -> int:
        """py-1.14.6 — Sweep every disk queue and flush the head of any
        whose conv has NO turn in flight. The on_idle hook only drains a
        queue on turn-COMPLETION; a conv can go idle with items still
        queued and never fire it — after a daemon restart / self-update
        re-exec (in-memory ChatSessions + its _wait thread are gone), an
        abnormally-reaped session (reap_dead pops the slot without firing
        on_idle), or an enqueue into an already-idle conv. Those queues
        would sit forever showing 'N WAITING · runs after the current
        turn' with no current turn. Flushing one head re-registers
        on_idle (via _spawn_chat_turn), so the rest of the queue drains
        normally turn-by-turn. Returns the count of convs flushed.

        Called from ChatSessionReaper._sweep (boot + every 30s tick)."""
        flushed = 0
        try:
            conv_ids = self.chat_queue_manager.conv_ids()
        except Exception as e:
            _log(f"_flush_idle_chat_queues: conv_ids failed: {e}")
            return 0
        for conv in conv_ids:
            if self.chat_sessions.has(conv):
                continue  # a turn is in flight — on_idle will drain it
            try:
                if self._maybe_flush_chat_queue(conv):
                    flushed += 1
            except Exception as e:
                _log(f"_flush_idle_chat_queues: flush {conv} failed: {e}")
        return flushed
