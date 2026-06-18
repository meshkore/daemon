"""runnerloop.py — extracted from runner.py (daemon-architecture-v2 Phase 3d).

RunnerLoopMixin: methods moved VERBATIM out of ChatRunner; Daemon inherits both so
every self.* resolves on the combined instance -> byte-identical."""

from __future__ import annotations

import json
import re
import secrets
import time
from typing import List, Tuple

from utils import _append_timeline, _debug_emit, _iso_now, _log


class RunnerLoopMixin:
    def _reader_loop(self) -> None:
        assert self.proc and self.proc.stdout
        last_emit_at = 0.0
        result_text = ""
        for raw in self.proc.stdout:
            try:
                line = raw.decode("utf-8", "replace").strip()
            except Exception:
                continue
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(ev, dict):
                continue
            ev_type = ev.get("type")
            if ev_type == "stream_event":
                inner = ev.get("event") or {}
                if (
                    inner.get("type") == "content_block_delta"
                    and (inner.get("delta") or {}).get("type") == "text_delta"
                ):
                    delta = (inner.get("delta") or {}).get("text") or ""
                    if delta:
                        self.deltas_seen += 1
                        # LAL2 — Anchor protocol head buffering. Until the
                        # first newline (or 4 KB) the delta is held in
                        # `_head_buffer`; once we can decide whether it
                        # opens with `⟦anchor⟧ {...}` we strip the marker
                        # and forward the rest. After the head is resolved
                        # subsequent deltas just pass through the
                        # `⟦anchor-progress⟧` stripper.
                        if not self._anchor_head_resolved:
                            visible = self._resolve_anchor_head(delta)
                            if not self._anchor_head_resolved:
                                # Still buffering; nothing to broadcast yet.
                                continue
                            self._cumulative_text += visible
                        else:
                            self._cumulative_text += self._strip_anchor_progress(delta)
                        now = time.monotonic()
                        if now - last_emit_at > 0.2:
                            last_emit_at = now
                            self.hub.broadcast(
                                {
                                    "type": "chat.assistant.delta",
                                    "author": self.identity,
                                    "conv": self.conv,
                                    "stream_id": self.stream_id,
                                    "text": self._cumulative_text[:16000],
                                    "ts": _iso_now(),
                                }
                            )
                elif (
                    inner.get("type") == "content_block_start"
                    and (inner.get("content_block") or {}).get("type") == "tool_use"
                ):
                    self.tool_calls_count += 1
                    cb = inner.get("content_block") or {}
                    # py-1.5.0 — Persist tool.use to timeline so the
                    # cockpit can replay full turn detail after a reload
                    # or a daemon restart. Previously broadcast-only,
                    # which made historical turns auditable only via
                    # git log of the files the agent touched.
                    self.hub.broadcast(
                        _append_timeline(
                            self.paths,
                            {
                                "type": "tool.use",
                                "author": self.identity,
                                "conv": self.conv,
                                "stream_id": self.stream_id,
                                "tool": cb.get("name"),
                                "input": cb.get("input"),
                            },
                        )
                    )
                continue
            if ev_type == "user":
                for c in (ev.get("message") or {}).get("content") or []:
                    if isinstance(c, dict) and c.get("type") == "tool_result":
                        # py-1.5.0 — Persist tool.result too (was
                        # broadcast-only). Pair-matched to a tool.use
                        # via stream_id in the cockpit.
                        self.hub.broadcast(
                            _append_timeline(
                                self.paths,
                                {
                                    "type": "tool.result",
                                    "author": self.identity,
                                    "conv": self.conv,
                                    "stream_id": self.stream_id,
                                    "ok": not c.get("is_error"),
                                },
                            )
                        )
                continue
            if ev_type == "result" and isinstance(ev.get("result"), str):
                result_text = ev["result"]
                # CU1 (py-1.13.3) — Capture token usage + cost from the
                # SDK's terminal event. claude-code emits e.g.
                #   {"type":"result","result":"…","usage":{
                #       "input_tokens":N,"output_tokens":N,
                #       "cache_read_input_tokens":N,
                #       "cache_creation_input_tokens":N},
                #    "total_cost_usd":N,"num_turns":N}
                # Daemon previously ignored both fields. Stored on the
                # runner so `_finalize_usage` can broadcast + accumulate
                # after the loop exits.
                usage = ev.get("usage")
                if isinstance(usage, dict):
                    self.last_turn_usage = {
                        "input_tokens": int(usage.get("input_tokens") or 0),
                        "output_tokens": int(usage.get("output_tokens") or 0),
                        "cache_read_input_tokens": int(
                            usage.get("cache_read_input_tokens") or 0
                        ),
                        "cache_creation_input_tokens": int(
                            usage.get("cache_creation_input_tokens") or 0
                        ),
                    }
                cost = ev.get("total_cost_usd")
                if isinstance(cost, (int, float)):
                    self.last_turn_cost_usd = float(cost)
        # Finalize. py-1.13.2 — `result_text` (from the Claude SDK
        # `result` event) was bypassing the anchor stripper because the
        # stripper runs delta-by-delta on `_cumulative_text`. Sweep both
        # marker kinds from the final text before persisting/broadcasting.
        final_text = self._strip_all_anchor_markers(
            result_text or self._cumulative_text
        )
        # py-1.21.1 — TRANSIENT API-ERROR RETRY SHIELD (TR1). claude-code
        # 2.1.145 occasionally fails a long interleaved-thinking + multi-
        # tool turn with `API Error: 400 … thinking/redacted_thinking
        # blocks in the latest assistant message cannot be modified` — a
        # CLI bug reconstructing the multi-block assistant message for the
        # tool-loop continuation (the daemon never touches that array), so
        # a fresh spawn rebuilds a clean array and the same turn succeeds.
        # Siblings: transient 5xx / overloaded / rate-limit. These are
        # TRANSPORT failures, NOT task outcomes. Before this shield the
        # error string got persisted as the turn's chat.assistant.final —
        # poisoning EVERY future briefing via `_section_history` — AND woke
        # the parent architect with a bogus task-failure verdict (field
        # 2026-06-18: `_onboarding_v1` exit=1 at content.12 → false
        # one-retry-then-blocked on the roadmap). Re-spawn the SAME turn up
        # to `_MAX_TRANSIENT_RETRIES`; the error is surfaced normally only
        # after the budget is spent. We branch on `result_text` (the raw
        # SDK `result`) so anchor-stripping can't mask the signature.
        if self._maybe_retry_transient(result_text or self._cumulative_text):
            return
        # py-1.7.0 — Harvest REMEMBER: lines into the role's shared
        # memory. Anything the agent flags ("REMEMBER: credentials live
        # at …") gets appended once, deduplicated. Lines are also
        # stripped from the final response shown in the chat so they
        # don't clutter the UI.
        cleaned_text, harvested = self._harvest_remember_lines(final_text)
        if harvested:
            try:
                self._append_role_memory(harvested)
            except Exception as e:
                _log(f"role memory append failed: {e}")
        self.hub.broadcast(
            _append_timeline(
                self.paths,
                {
                    "type": "chat.assistant.final",
                    "author": self.identity,
                    "conv": self.conv,
                    "stream_id": self.stream_id,
                    "text": cleaned_text,
                },
            )
        )
        # CU1 (py-1.13.3) — Broadcast token usage + cost AFTER the
        # final lands. Cockpit ingests via `chat.usage` and updates
        # `chatStore.state.convs[conv].usage` so the operator sees
        # `12.3k in · 4.5k out · $0.15` in the agent's scope strip.
        if self.last_turn_usage is not None and self.daemon is not None:
            try:
                cumulative = self.daemon.chat_sessions.record_usage(
                    self.conv,
                    self.last_turn_usage,
                    self.last_turn_cost_usd,
                )
                self.hub.broadcast(
                    {
                        "type": "chat.usage",
                        "conv": self.conv,
                        "stream_id": self.stream_id,
                        "turn": {
                            **self.last_turn_usage,
                            "cost_usd": self.last_turn_cost_usd,
                        },
                        "total": cumulative,
                        "model": self.model,
                        "ts": _iso_now(),
                    }
                )
            except Exception as e:
                _log(f"chat.usage broadcast failed for {self.conv}: {e}")
        # py-1.10.4 — surface the exit code in the daemon log so a
        # silent claude failure (empty stdout, no final, etc.) can
        # be traced back to e.g. "exited 1 with stderr 'context
        # length exceeded'". Without this line, every empty-final
        # looked identical regardless of whether claude crashed,
        # blocked on a tool, or genuinely had nothing to say.
        exit_code = self.proc.wait() if self.proc else None
        text_len = len(cleaned_text or "")
        _log(
            f"claude({self.conv}) exit={exit_code} stream={self.stream_id} "
            f"text_len={text_len} agent_type={self.agent_type}"
        )
        _debug_emit(
            "subagent-final",
            msg=f"{self.conv} exit={exit_code} text_len={text_len}",
            lvl=("warn" if exit_code not in (None, 0) else "info"),
            conv=self.conv,
            agent_id=self.agent_id,
            data={
                "agent_type": self.agent_type,
                "exit": exit_code,
                "text_len": text_len,
                "stream_id": self.stream_id,
                "preview": (cleaned_text or "")[:200],
            },
        )
        self.hub.broadcast(
            {
                "type": "task.finished",
                "id": f"chat:{self.conv}",
                "ts": _iso_now(),
                "exit": exit_code,
                "conv": self.conv,
            }
        )
        # py-1.10.16 — Architect wake hook. If this conv was dispatched
        # by a roadmap-architect (parent_conv recorded in conv_meta),
        # post a `[architect-wake]` turn back to the parent so the
        # pass resumes the moment the subagent finishes. Without this,
        # the architect would have to poll inside its own turn (burns
        # tokens) or rely on the operator to nudge it.
        if self.daemon is not None:
            try:
                self.daemon._maybe_wake_parent_architect(
                    child_conv=self.conv,
                    child_agent_id=self.agent_id,
                    child_final_text=cleaned_text,
                    child_exit=exit_code,
                )
            except Exception as e:
                _log(f"architect wake hook failed for {self.conv}: {e}")
            # py-1.11.0 — Broadcast conv.activity for this conv with
            # live=false override. Fires before ChatSessions._wait pops
            # us from `_s`; the override ensures the cockpit sees the
            # right state regardless of the race.
            try:
                self.daemon._broadcast_conv_activity(self.conv, live_override=False)
            except Exception as e:
                _log(f"conv.activity broadcast on final failed for {self.conv}: {e}")
            # py-1.12.9 — Auto-archive any finished SUBAGENT conv.
            # Criterion broadened from "work-* prefix" (py-1.11.2) to
            # "has parent_conv in meta OR matches `work-*` slug". A
            # subagent is anything the architect dispatched — workers
            # (work-*), deploy, db, testing, and ad-hoc customs all
            # carry `parent_conv` in conv_meta. The new rule catches
            # them uniformly.
            #
            # NOT auto-archived (operator-owned, multi-turn):
            #   - Master `_onboarding_v1` (the Coordinator)
            #   - `roadmap-architect-*` (carries the pass summary)
            #   - Any conv WITHOUT parent_conv and not prefixed work-
            #     (= the operator opened it manually, keep it open)
            #
            # Operator field report 2026-06-06: "garantizar que cuando
            # se lanzan agentes que hacen tareas se cierran. Si el
            # usuario quiere abrir tres a mano y dejarlos ahí no hay
            # problema." This matches the rule exactly: dispatched →
            # auto-archive; operator-opened → leave alone.
            should_auto_archive = False
            if not self.daemon.chat_archive.is_archived(self.conv):
                if self.conv.startswith("work-"):
                    should_auto_archive = True
                elif self.conv == "_onboarding_v1":
                    should_auto_archive = False
                elif self.conv.startswith("roadmap-architect-"):
                    should_auto_archive = False
                else:
                    # Look up parent_conv from meta sidecar.
                    try:
                        meta = self.daemon._conv_meta_load().get(self.conv) or {}
                        if meta.get("parent_conv"):
                            should_auto_archive = True
                    except Exception as e:
                        _log(f"auto-archive meta check failed for {self.conv}: {e}")
            if should_auto_archive:
                try:
                    entry = self.daemon.chat_archive.archive(
                        self.conv,
                        by="auto-subagent-finish",
                    )
                    self.hub.broadcast(
                        {
                            "type": "conv.archived",
                            "conv": self.conv,
                            "archived_at": entry.get("archived_at"),
                            "by": entry.get("by"),
                            "ts": entry.get("archived_at"),
                        }
                    )
                except Exception as e:
                    _log(f"auto-archive of {self.conv} failed: {e}")
        self.done.set()

    # TR1 (py-1.21.1) — re-spawn budget for transport-class API errors.
    # 2 retries = 3 attempts total. Past this we surface the error so a
    # genuinely stuck turn can't loop forever.
    _MAX_TRANSIENT_RETRIES = 2
    # Status codes claude-code surfaces as "API Error: <code> …" that a
    # fresh spawn can clear (overload / upstream / gateway). 400/401/403/
    # 404/413 are deliberately EXCLUDED — those are request-shape problems
    # (bad model id, auth, prompt-too-long) a retry won't fix; the one 400
    # we DO retry is matched by signature below, not by code.
    _TRANSIENT_STATUS_RE = re.compile(r"api error:\s*(?:429|5\d\d)\b")

    def _is_transient_api_error(self, text: str) -> bool:
        """True iff `text` is a claude-code API error that a fresh spawn
        can plausibly clear — a TRANSPORT failure, not a task outcome.
        Conservative: only the explicit allowlist below qualifies."""
        if not text:
            return False
        low = text.strip().lower()
        if not low.startswith("api error"):
            return False
        # NEVER retry request-shape failures — a re-spawn rebuilds the same
        # oversized/invalid request and burns the budget for nothing.
        if "too long" in low or "prompt is too" in low:
            return False
        # The specific 400 we DO retry: the thinking/redacted_thinking
        # "cannot be modified" CLI reconstruction bug — a fresh message
        # array makes it disappear.
        if "cannot be modified" in low or "redacted_thinking" in low:
            return True
        # Transient upstream conditions.
        markers = (
            "overloaded",
            "rate limit",
            "rate_limit",
            "internal server error",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
            "timed out",
            "timeout",
        )
        if any(m in low for m in markers):
            return True
        return bool(self._TRANSIENT_STATUS_RE.search(low))

    def _maybe_retry_transient(self, result_text: str) -> bool:
        """If the turn died on a transient API error and we still have
        budget, reap the dead child, reset per-turn streaming state, and
        re-spawn the SAME turn on a fresh `claude -p` process. Returns
        True when a re-spawn was launched (the caller must `return`
        immediately so the failed run does NOT broadcast a final / wake
        the architect / set `done`). Returns False to let the normal
        finalize path run (success, cancel, non-transient error, or
        budget exhausted)."""
        if self.cancelled:
            return False
        if not self._is_transient_api_error(result_text):
            return False
        # Reap the dead child + confirm it actually failed. A clean exit
        # whose text merely *discusses* an API error must not be retried.
        exit_code = None
        try:
            if self.proc is not None:
                exit_code = self.proc.wait(timeout=5)
        except Exception:
            exit_code = None
        if exit_code in (None, 0):
            return False
        attempt = getattr(self, "_transient_attempt", 0)
        if attempt >= self._MAX_TRANSIENT_RETRIES:
            _log(
                f"claude({self.conv}) transient API error — retry budget "
                f"({self._MAX_TRANSIENT_RETRIES}) exhausted; surfacing error"
            )
            return False
        self._transient_attempt = attempt + 1
        preview = (result_text or "").strip()[:200]
        _log(
            f"claude({self.conv}) transient API error "
            f"(attempt {self._transient_attempt}/{self._MAX_TRANSIENT_RETRIES}) "
            f"— re-spawning. err={preview!r}"
        )
        _debug_emit(
            "transient-retry",
            msg=(
                f"{self.conv} transient API error — re-spawn "
                f"{self._transient_attempt}/{self._MAX_TRANSIENT_RETRIES}"
            ),
            lvl="warn",
            conv=self.conv,
            agent_id=self.agent_id,
            data={
                "attempt": self._transient_attempt,
                "agent_type": self.agent_type,
                "preview": preview,
            },
        )
        # Reset per-turn streaming + anchor + usage state so the fresh run
        # starts from a clean slate (a NEW stream_id so the cockpit treats
        # it as a brand-new bubble rather than appending to the dead one).
        self._cumulative_text = ""
        self._head_buffer = ""
        self._anchor_head_resolved = False
        self.deltas_seen = 0
        self.tool_calls_count = 0
        self.last_turn_usage = None
        self.last_turn_cost_usd = None
        self.stream_id = f"s_{int(time.time() * 1000):x}_{secrets.token_hex(2)}"
        # Small backoff — helps 5xx/overloaded settle; harmless for the
        # thinking-block 400 (which is instant-clearable).
        time.sleep(min(2**self._transient_attempt, 8))
        if self.cancelled:
            return False
        # Fresh spawn → new _reader_loop thread; THIS loop returns. The
        # failed turn's error was never persisted, so `_briefing()` rebuilds
        # clean history (the user turn was persisted at dispatch).
        self.spawn()
        return True

    def _harvest_remember_lines(self, text: str) -> Tuple[str, List[str]]:
        """Extract any `REMEMBER: …` lines from `text` and return
        (cleaned text, list of remembered facts). Case-insensitive on
        the marker; one fact per line."""
        if not text:
            return text, []
        kept: List[str] = []
        harvested: List[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            low = stripped.lower()
            # Allow "REMEMBER: ...", "- REMEMBER: ...", "* REMEMBER: ..."
            for prefix in ("remember:", "- remember:", "* remember:"):
                if low.startswith(prefix):
                    fact = stripped[len(prefix) :].strip()
                    # When the prefix had a list bullet, strip the bullet.
                    if prefix.startswith(("-", "*")):
                        fact = fact.lstrip()
                    if fact:
                        harvested.append(fact)
                    break
            else:
                kept.append(line)
                continue
        cleaned = "\n".join(kept).rstrip()
        return cleaned, harvested

    def _append_role_memory(self, facts: List[str]) -> None:
        """Append facts to `.meshkore/agents/_types/<agent-type>/memory.md`,
        deduplicating against what's already in the file. Each entry
        prefixed with its UTC date so memory has provenance."""
        if not facts:
            return
        from datetime import datetime as _dt

        today = _dt.utcnow().strftime("%Y-%m-%d")
        d = self.paths.agents_dir / "_types" / self.agent_type
        d.mkdir(parents=True, exist_ok=True)
        path = d / "memory.md"
        existing = ""
        try:
            existing = path.read_text(errors="replace") if path.exists() else ""
        except OSError:
            existing = ""
        existing_lc = existing.lower()
        new_blocks: List[str] = []
        for fact in facts:
            if fact.lower() in existing_lc:
                continue
            new_blocks.append(f"- {today} · {fact}")
        if not new_blocks:
            return
        header = ""
        if not existing.strip():
            header = (
                f"# `{self.agent_type}` role memory\n\n"
                f"Long-lived facts captured by past instances of this role "
                f"via `REMEMBER: …` lines. Append-only.\n\n"
            )
        addition = (
            ("\n" if existing and not existing.endswith("\n") else "")
            + "\n".join(new_blocks)
            + "\n"
        )
        with path.open("a", encoding="utf-8") as fh:
            if header:
                fh.write(header)
            fh.write(addition)
