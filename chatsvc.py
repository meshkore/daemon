"""chatsvc.py — extracted from daemon.py (daemon-architecture-v2 Phase 2).

ChatMixin: methods moved VERBATIM; Daemon inherits it so every self.*
still resolves on the combined instance -> byte-identical."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from prompts import _agent_type_normalised
from utils import _debug_emit, _iso_now, _log


class ChatMixin:
    def chat_dispatch(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        text = str(body.get("text") or "").strip()
        # py-1.12.18 — image-only / docs-only dispatch is valid. The
        # operator field-reported 2026-06-10 that attaching images to
        # the architect without typing a question produced
        # `400 text required` — the message disappeared into thin air.
        # Now we accept the dispatch and synthesize a minimal text so
        # the model gets a coherent turn (claude-code expects a text
        # part). Reject only when EVERYTHING is empty.
        has_images = isinstance(body.get("images"), list) and len(body["images"]) > 0
        has_docs = (
            isinstance(body.get("context_docs"), list) and len(body["context_docs"]) > 0
        )
        if not text and not has_images and not has_docs:
            return 400, {
                "error": "empty dispatch — provide text, images, or context_docs",
            }
        if not text:
            # Synthesize a neutral placeholder so the briefing pipeline
            # has something to render as the user's turn. The
            # attachments themselves carry the operator's intent.
            if has_images and has_docs:
                text = "(see attached images and documents)"
            elif has_images:
                text = (
                    "(see attached image)"
                    if len(body["images"]) == 1
                    else "(see attached images)"
                )
            else:
                text = (
                    "(see attached document)"
                    if len(body["context_docs"]) == 1
                    else "(see attached documents)"
                )
        author = str(body.get("author") or self.identity)
        conv = str(
            body.get("conv")
            or f"chat-{_iso_now()[:16].replace(':', '-').replace('T', '-').lower()}"
        )
        # py-1.7.0 — agent specialisation from cockpit. Both fields are
        # optional; missing → 'custom' (General coder). When present,
        # persisted to the conv_meta sidecar so chained turns and
        # cockpit reconnects keep the same role.
        agent_type = body.get("agent_type")
        agent_id = body.get("agent_id")
        # py-1.10.16 — `parent_conv` (initiative `architect-wake-on-subagent`).
        # When the architect dispatches a subagent, it passes its own
        # conv id so the daemon can re-dispatch a wake turn the moment
        # the subagent's `chat.assistant.final` fires. Optional;
        # missing = the conv has no parent (cockpit-initiated chat).
        parent_conv = body.get("parent_conv")
        if parent_conv is not None:
            parent_conv = str(parent_conv).strip() or None
        # py-1.10.19 — `initiative_id` + `task_id` (initiative
        # `agent-activity-surface`). Both already flow on the wire
        # (architect prompt + story-runner cockpit dispatch); now
        # they're persisted so /state can join them and the cockpit
        # can render per-initiative / per-task working state without
        # heuristics on the conv slug.
        initiative_id = body.get("initiative_id")
        if initiative_id is not None:
            initiative_id = str(initiative_id).strip() or None
        task_id = body.get("task_id")
        if task_id is not None:
            task_id = str(task_id).strip() or None
        # MP1 (py-1.13.3) — per-conv model preference from the
        # NewAgentWizard (cockpit). The wizard already collects
        # `auto`/`opus`/`sonnet`/`haiku`; previously the value died in
        # convMeta. Now it flows through to `_conv_meta_set` and
        # ChatRunner.spawn, which injects `--model <id>` into claude-code.
        model_pref = body.get("model")
        if model_pref is not None:
            model_pref = str(model_pref).strip() or None
        # MP3 (py-1.13.4) — per-conv effort (reasoning depth) from the
        # NewAgentWizard. Forwarded to claude-code as `--effort <level>`.
        effort_pref = body.get("effort")
        if effort_pref is not None:
            effort_pref = str(effort_pref).strip() or None
        # DM-CLI-02 (multi-cli-clients) — optional per-conv CLI-client
        # override. None (every caller today) resolves to claude-code.
        client_pref = body.get("client")
        if client_pref is not None:
            client_pref = str(client_pref).strip().lower() or None
        # ATM10 (agent-team) — optional `member`: the team-member PROFILE this
        # conv is an INSTANCE of. When set we resolve agent_type from the
        # member and fill model/effort from it UNLESS the body overrode them
        # (overrides win on any turn). The binding is frozen after the first
        # message and singletons allow only one live instance — both enforced
        # in _member_dispatch_prep, which returns a ready (code, body) error.
        member = body.get("member")
        if member is not None:
            member = str(member).strip() or None
        if member:
            err, r_type, r_client, r_model, r_effort = self._member_dispatch_prep(
                conv,
                member,
                body_agent_type=agent_type,
                body_model=model_pref,
                body_effort=effort_pref,
                body_client=client_pref,
            )
            if err is not None:
                code_err, body_err = err
                _debug_emit(
                    "chat-dispatch.refused",
                    msg=body_err.get("error", "member refused"),
                    lvl="warn",
                    conv=conv,
                    data=body_err,
                )
                return code_err, body_err
            agent_type = r_type
            client_pref = r_client
            model_pref = r_model
            effort_pref = r_effort
        # py-1.10.25 — Daemon-side dispatch mutex. Enforces invariants
        # the architect prompt already claims but the LLM intermittently
        # violates (observed in cavioca 2026-05-30: same task got 4
        # parallel dispatches, two roadmap-architect convs running
        # simultaneously, etc.). Rejected requests return 409 with a
        # `hint` field naming the existing conv so the caller can
        # decide what to do (architect: wait for the wake; cockpit:
        # surface the conflict).
        mutex_err = self._dispatch_mutex_check(
            conv=conv,
            agent_type=agent_type,
            parent_conv=parent_conv,
            task_id=task_id,
            initiative_id=initiative_id,
        )
        if mutex_err is not None:
            code_err, body_err = mutex_err
            _debug_emit(
                "chat-dispatch.refused",
                msg=body_err.get("error", "refused"),
                lvl="warn",
                conv=conv,
                data=body_err,
            )
            return code_err, body_err
        # py-1.4.0 — Accept cockpit-attached context as part of the
        # briefing pipeline. Previously this field was silently
        # dropped, which broke V46/V78b onboarding (the cockpit
        # thought it was sending a bootstrap brief but the agent
        # never saw it).
        raw_docs = body.get("context_docs")
        context_docs: List[Dict[str, Any]] = []
        if isinstance(raw_docs, list):
            for d in raw_docs:
                if isinstance(d, dict) and (d.get("content") or "").strip():
                    context_docs.append(
                        {
                            "filename": str(d.get("filename") or "doc.md"),
                            "content": str(d.get("content") or ""),
                        }
                    )
        # py-1.12.21 — persist any image attachments to
        # `.meshkore/uploads/<bucket>/<file>` and embed a small
        # manifest in the chat.user event so the cockpit can render
        # thumbnails on hydrate. Failures are silently absorbed —
        # the dispatch still proceeds with text-only.
        attachments: List[Dict[str, Any]] = []
        skipped_uploads: List[Dict[str, Any]] = []
        if has_images:
            try:
                attachments = self.upload_store.save_dispatch(
                    conv=conv,
                    images=body.get("images")
                    if isinstance(body.get("images"), list)
                    else None,
                    ts_iso=_iso_now(),
                    skipped=skipped_uploads,
                )
            except Exception as e:
                _log(f"upload save_dispatch failed: {e}")
                attachments = []
        # 1) Emit + persist the user event right away.
        self._persist_user_event(conv, text, author=author, attachments=attachments)
        # 2) Queue if a turn is already running for this conv.
        if self.chat_sessions.has(conv):
            pending = self.chat_sessions.queue(conv, text)
            return 202, {
                "queued": True,
                "conv": conv,
                "pending": pending,
                "message": "turn in progress — your prompt will be merged into the next turn",
                **({"skipped_uploads": skipped_uploads} if skipped_uploads else {}),
            }
        # 3) New turn.
        _debug_emit(
            "chat-dispatch",
            msg=f"new turn (conv={conv}, type={agent_type or 'custom'})",
            conv=conv,
            agent_id=agent_id,
            data={
                "agent_type": agent_type,
                "parent_conv": parent_conv,
                "initiative_id": initiative_id,
                "task_id": task_id,
                "text_len": len(text),
                "text_preview": text[:200],
                "context_docs": len(context_docs),
                "author": author,
            },
        )
        try:
            runner = self._spawn_chat_turn(
                conv,
                text,
                context_docs=context_docs,
                agent_type=agent_type,
                agent_id=agent_id,
                parent_conv=parent_conv,
                initiative_id=initiative_id,
                task_id=task_id,
                model=model_pref,
                effort=effort_pref,
                client=client_pref,
                member=member,
            )
        except Exception as e:
            return 400, {"error": str(e)}
        return 202, {
            "conv": conv,
            "runner": getattr(runner, "_driver_id", None) or "claude-code",
            "identity": self.identity,
            "pid": runner.pid,
            "stream_id": runner.stream_id,
            "agent_type": _agent_type_normalised(agent_type),
            **({"skipped_uploads": skipped_uploads} if skipped_uploads else {}),
        }

    def chat_cancel(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        conv = str(body.get("conv") or "").strip()
        if not conv:
            return 400, {"error": "conv required"}
        cancelled, dropped = self.chat_sessions.cancel(conv)
        # py-1.10.0 — propagate to runs. If the cancelled conv belongs
        # to an active run (started via /runs), mark it cancelled too
        # and emit run.cancelled. Operator hitting the chat's StopBar
        # converges with hitting ■ on the initiative card.
        run = self.runs.find_by_conv(conv)
        if run is not None:
            self.runs.cancel(run["id"])
        if not cancelled:
            return 200, {
                "ok": True,
                "cancelled": False,
                "reason": "no active turn for that conv",
                "run_cancelled": run["id"] if run else None,
            }
        self.hub.broadcast(
            {
                "type": "chat.cancelled",
                "conv": conv,
                "ts": _iso_now(),
                "dropped_pending": dropped,
            }
        )
        # py-1.11.0 — conv.activity flip. The conv is no longer live;
        # if a parent was coordinating it, the parent's waiting_on
        # shrinks (and may go empty → coordinating=false).
        parent = self._conv_meta_parent(conv)
        self._broadcast_conv_activity(conv)
        if parent:
            self._broadcast_conv_activity(parent)
        return 200, {
            "ok": True,
            "cancelled": True,
            "dropped_pending": dropped,
            "run_cancelled": run["id"] if run else None,
        }

    def chat_archive_set(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        conv = str(body.get("conv") or "").strip()
        if not conv:
            return 400, {"error": "conv required"}
        by = str(body.get("author") or "").strip()
        entry = self.chat_archive.archive(conv, by=by)
        # py-1.11.1 — Single broadcast on the snapshot.v1 contract. The
        # legacy `chat.archived` alias was retired in Phase 2.
        self.hub.broadcast(
            {
                "type": "conv.archived",
                "conv": conv,
                "archived_at": entry.get("archived_at"),
                "by": entry.get("by"),
                "ts": entry.get("archived_at"),
            }
        )
        return 200, {"ok": True, "archived": entry}

    def chat_archive_clear(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        conv = str(body.get("conv") or "").strip()
        if not conv:
            return 400, {"error": "conv required"}
        was_archived = self.chat_archive.unarchive(conv)
        if was_archived:
            self.hub.broadcast(
                {
                    "type": "conv.unarchived",
                    "conv": conv,
                    "ts": _iso_now(),
                }
            )
        return 200, {"ok": True, "unarchived": was_archived, "conv": conv}
