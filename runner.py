"""ChatRunner — the claude-code subprocess driver for one agent turn.

DM-modularize-2 (py-1.14.4): lifted verbatim from daemon.py — the piece
the first modularize pass deferred because of its coupling. One turn =
one ChatRunner: it spawns `claude -p --output-format stream-json`, parses
each streamed line into chat.assistant.delta / tool.use / tool.result WS
events, enforces the `⟦anchor⟧` / `⟦anchor-progress⟧` wire protocol (parse
+ strip so markers never reach the chat bubble), injects `--model` /
`--effort` (MP1/MP3) into the argv, and emits `chat.assistant.final` +
`chat.usage` when the child exits.

The Daemon↔ChatRunner cycle is intentional and preserved: the runner
holds an optional ``self.daemon`` back-ref and calls back into it for
anchor handling, parent-architect wake, usage accumulation, and auto-
archive. Those calls are all None-guarded so the runner is still
constructable in tests without a full daemon.

Helpers that ONLY the runner used travel with it: ``_session_id_for_conv``
(deterministic per-conv claude session id) + ``_find_claude`` (CLI path
discovery) + ``_CLAUDE_SESSION_NAMESPACE``.

Bundler note: imports shared helpers from utils + the briefing pipeline
from prompts (both stripped by bundle.py; resolved via the flat global
namespace). The daemon-defined ``Cluster`` / ``Daemon`` types can't be
imported here (daemon.py loads last) so they're annotated ``Any`` — the
same convention storage.py/chat.py use. daemon.py re-exports ``ChatRunner``
(+ ``_session_id_for_conv``) so ``daemon.ChatRunner`` stays stable for
callers and tests."""

from __future__ import annotations

import json
import os
import re
import secrets
import signal
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from paths import Paths
from prompts import BriefingPipeline, _agent_type_normalised
from utils import _append_timeline, _debug_emit, _iso_now, _log


_CLAUDE_SESSION_NAMESPACE = uuid.UUID("a4f7c1e8-3b29-4d8e-9c52-7f1e3a8d4b62")


def _session_id_for_conv(conv: str) -> str:
    """Deterministic session UUID per conversation id. Stable across
    daemon restarts so `claude -p --session-id <id>` resumes the same
    conversation context + benefits from Anthropic's prompt cache."""
    return str(uuid.uuid5(_CLAUDE_SESSION_NAMESPACE, conv or "default"))


def _find_claude() -> Optional[str]:
    """Locate the `claude` CLI. Heuristic — try shell PATH, then the
    nvm + Homebrew locations we expect on a typical operator laptop."""
    import shutil

    found = shutil.which("claude")
    if found:
        return found
    import glob

    for pattern in [
        os.path.expanduser("~/.nvm/versions/node/v*/bin/claude"),
        "/opt/homebrew/bin/claude",
        "/usr/local/bin/claude",
    ]:
        hits = sorted(glob.glob(pattern), reverse=True)
        if hits and os.access(hits[0], os.X_OK):
            return hits[0]
    return None


class ChatRunner:
    """One coordinator turn = one ChatRunner. Spawns `claude -p` with
    stream-json output, parses each line into chat.assistant.delta /
    tool.use / tool.result events on the WS, and emits a final
    `chat.assistant.final` when the child exits.

    Cancel-safe: cancel() sends SIGTERM to the process group; if still
    alive after 30 s, SIGKILL."""

    def __init__(
        self,
        *,
        paths: "Paths",
        cluster: Any,
        hub: Any,
        identity: str,
        conv: str,
        prompt: str,
        context_docs: Optional[List[Dict[str, Any]]] = None,
        agent_type: Optional[str] = None,
        agent_id: Optional[str] = None,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        daemon: Optional[Any] = None,
    ):
        self.paths = paths
        self.cluster = cluster
        # MP1 (py-1.13.3) — Per-turn model preference. None / 'auto' →
        # claude-code uses its default; otherwise spawn injects
        # `--model <id>` into the argv.
        self.model = model
        # MP3 (py-1.13.4) — Per-turn effort (reasoning depth). None /
        # 'default' → no flag; otherwise spawn injects
        # `--effort <level>` (low/medium/high/xhigh/max).
        self.effort = effort
        # CU1 (py-1.13.3) — Per-turn usage payload populated when the
        # SDK emits its terminal `result` event with `usage` +
        # `total_cost_usd`. Used in finalize to broadcast `chat.usage`
        # and to accumulate cumulative-per-conv counters.
        self.last_turn_usage: Optional[Dict[str, Any]] = None
        self.last_turn_cost_usd: Optional[float] = None
        self.hub = hub
        self.identity = identity
        self.conv = conv
        self.prompt = prompt
        # py-1.4.0 — Carried into BriefingPipeline so the cockpit can
        # attach project-specific context (bootstrap brief, scope
        # hints, integrity check overrides, …) on a per-turn basis.
        self.context_docs: List[Dict[str, Any]] = context_docs or []
        # py-1.7.0 — agent_type drives specialised prompt selection,
        # agent_id is the human label (A001, A002, …) for logging.
        self.agent_type = _agent_type_normalised(agent_type)
        self.agent_id = (agent_id or "").strip() or None
        self.stream_id = f"s_{int(time.time() * 1000):x}_{secrets.token_hex(2)}"
        self.pid: Optional[int] = None
        self.proc: Any = None  # subprocess.Popen
        self.done = threading.Event()
        self.cancelled = False
        self._cumulative_text = ""
        # SRL1 (py-1.13.1) — instance attrs the snapshot reader (SRL2)
        # pulls via getattr. `started_at` is the ISO timestamp of turn
        # spawn; `deltas_seen` + `tool_calls_count` are running
        # counters incremented in `_read_stream`. Together they let a
        # cockpit that just connected know how long the turn has been
        # running and how much work it's done, even if no delta has
        # arrived yet via WS.
        self.started_at = _iso_now()
        self.deltas_seen = 0
        self.tool_calls_count = 0
        # LAL2 (py-1.12.32) — anchor protocol head buffering. The
        # subprocess's first delta is held in `_head_buffer` until we
        # either see a newline (decide if it's an anchor marker line)
        # or exceed 4 KB. After the head is resolved, subsequent deltas
        # also get scanned for `⟦anchor-progress⟧` lines mid-turn.
        self._head_buffer = ""
        self._anchor_head_resolved = False
        # py-1.10.16 — Back-reference for the architect-wake hook
        # (initiative `architect-wake-on-subagent`). When the
        # subprocess emits `chat.assistant.final`, the runner calls
        # `daemon._maybe_wake_parent_architect(...)` so the architect
        # is automatically re-dispatched as each subagent completes.
        # Optional so tests / standalone uses don't need a daemon.
        self.daemon = daemon

    # LAL2 — anchor protocol helpers. The agent's reply is expected to
    # OPEN with one of:
    #   ⟦anchor⟧ {"i":"...","t":"..."}        — anchor to existing
    #   ⟦anchor⟧ {"new_i":{...},"new_t":{...}} — create both
    #   ⟦anchor⟧ {"new_t":{...,"initiative":"..."}}  — task in existing init
    #   ⟦anchor⟧ {"info":true}                — informational turn
    # Mid-turn: ⟦anchor-progress⟧ {"t":"...","status":"done"}.
    # The daemon strips these from the visible chat thread and acts on
    # them in LAL3's _handle_anchor / _handle_anchor_progress.

    _ANCHOR_RE = re.compile(
        r"⟦anchor⟧\s*(\{[^\n]*\})\s*(?:\n|$)",
    )
    _ANCHOR_PROGRESS_RE = re.compile(
        r"⟦anchor-progress⟧\s*(\{[^\n]*\})\s*(?:\n|$)",
    )

    def _resolve_anchor_head(self, more_text: str) -> str:
        """Buffer the head of the reply until we can decide whether
        it opens with an anchor marker. Returns the text that's safe
        to forward downstream (i.e. the head minus any marker line).

        Called for every delta until `_anchor_head_resolved` is True."""
        self._head_buffer += more_text
        # Wait for at least one full line OR a hard 4 KB cap before
        # deciding — protects against fragmented first deltas.
        if "\n" not in self._head_buffer and len(self._head_buffer) < 4096:
            return ""  # hold the entire buffer for now
        m = self._ANCHOR_RE.match(self._head_buffer.lstrip())
        # Strip leading whitespace; re-anchor the offset.
        lstripped_len = len(self._head_buffer) - len(self._head_buffer.lstrip())
        if m:
            raw_payload = m.group(1)
            try:
                payload = json.loads(raw_payload)
            except json.JSONDecodeError as e:
                if self.daemon is not None:
                    try:
                        self.daemon._handle_anchor_rejected(
                            self.conv, f"malformed JSON: {e}", raw=raw_payload
                        )
                    except Exception:
                        pass
                else:
                    _log(f"anchor: malformed JSON in conv {self.conv}: {e}")
                payload = None
            if payload is not None and self.daemon is not None:
                try:
                    self.daemon._handle_anchor(self.conv, payload, raw=raw_payload)
                except Exception as e:
                    _log(f"anchor: handler raised for conv {self.conv}: {e}")
            elif payload is not None:
                # No daemon ref (standalone) — log and continue.
                _log(f"anchor (no daemon ref): conv={self.conv} payload={payload!r}")
            # Consume up to + including the matched marker line.
            consumed_end = lstripped_len + m.end()
            visible = self._head_buffer[consumed_end:]
        else:
            # No marker → operator-visible from the start. Notify the
            # daemon so it can broadcast `conv.anchor_missing` once.
            if self.daemon is not None:
                try:
                    self.daemon._handle_anchor_missing(self.conv)
                except Exception:
                    pass
            visible = self._head_buffer
        self._head_buffer = ""
        self._anchor_head_resolved = True
        return self._strip_anchor_progress(visible)

    def _strip_anchor_progress(self, text: str) -> str:
        """Detect `⟦anchor-progress⟧ {...}` lines anywhere in `text`,
        notify the daemon, and remove them so they don't reach the
        chat thread."""
        if "⟦anchor-progress⟧" not in text:
            return text
        out_parts: List[str] = []
        last = 0
        for m in self._ANCHOR_PROGRESS_RE.finditer(text):
            out_parts.append(text[last : m.start()])
            raw_payload = m.group(1)
            try:
                payload = json.loads(raw_payload)
            except json.JSONDecodeError:
                payload = None
            if payload is not None and self.daemon is not None:
                try:
                    self.daemon._handle_anchor_progress(
                        self.conv, payload, raw=raw_payload
                    )
                except Exception as e:
                    _log(f"anchor-progress: handler raised: {e}")
            last = m.end()
        out_parts.append(text[last:])
        return "".join(out_parts)

    def _strip_all_anchor_markers(self, text: str) -> str:
        """py-1.13.2 (anchor-strip-final fix, 2026-06-12). Belt-and-
        suspenders strip for the FINAL assistant message. The Claude SDK
        emits a `result` event with the entire reply in one piece; when
        present, daemon prefers `result_text` over `_cumulative_text`
        (the latter was already stripped delta-by-delta in
        `_resolve_anchor_head` + `_strip_anchor_progress`). That bypass
        meant `⟦anchor⟧ {...}` and `⟦anchor-progress⟧ {...}` lines
        leaked into the persisted timeline AND the broadcast chat
        message — operator saw the wire-protocol marker in chat.

        This helper sweeps both marker shapes from the final text. Pure
        text scrubbing — the side-effects (anchor handler, init/task
        file creation, conv_meta persist) already ran during the
        streaming pass. We only need to redact for display."""
        if "⟦anchor" not in text:
            return text
        cleaned = self._ANCHOR_RE.sub("", text, count=1)
        cleaned = self._ANCHOR_PROGRESS_RE.sub("", cleaned)
        return cleaned.lstrip("\n")

    def _briefing(self) -> str:
        # py-1.4.0 — the briefing is now composed by BriefingPipeline.
        # Each section (role, core rules, cluster snapshot, project
        # mode, integrity hints, cockpit context, history, user turn)
        # is independently maintained. See the class definition above
        # this file's HTTP handler block.
        return BriefingPipeline(
            paths=self.paths,
            cluster=self.cluster,
            identity=self.identity,
            conv=self.conv,
            user_text=self.prompt,
            context_docs=self.context_docs,
            agent_type=self.agent_type,
            agent_id=self.agent_id,
        ).build()

    def spawn(self) -> None:
        import subprocess

        claude_bin = _find_claude()
        if not claude_bin:
            err = "claude CLI not found — install via `npm i -g @anthropic-ai/claude-code`"
            _log(err)
            self.hub.broadcast(
                _append_timeline(
                    self.paths,
                    {
                        "type": "chat.assistant.final",
                        "author": self.identity,
                        "conv": self.conv,
                        "stream_id": self.stream_id,
                        "text": f"[runner error] {err}",
                    },
                )
            )
            self.done.set()
            return
        # py-1.6.1 HOTFIX — --session-id from py-1.6.0 caused empty
        # assistant responses in production (claude-code exited
        # silently on subsequent turns of the same conv). Reverted to
        # opt-in via env var MESHKORE_CLAUDE_SESSION_ID=1. Default off
        # until the failure mode is understood and re-tested.
        # The uuid5 helper is preserved so reintroduction is a one-line
        # flip once safe.
        session_id = _session_id_for_conv(self.conv)
        use_session = os.environ.get("MESHKORE_CLAUDE_SESSION_ID", "").strip() in (
            "1",
            "true",
            "yes",
            "on",
        )
        args = [
            claude_bin,
            "-p",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--permission-mode",
            "bypassPermissions",
            # Headless: cockpit has no UI to surface interactive question
            # tools. Disallow them so the model defaults to plain-text
            # asks in the chat bubble instead of stalling on a hanging
            # AskUserQuestion / ExitPlanMode call.
            "--disallowed-tools",
            "AskUserQuestion,ExitPlanMode",
        ]
        # MP1 (py-1.13.3) — Per-conv model override. `--model` accepts
        # one of `opus` / `sonnet` / `haiku` or an explicit model id
        # (claude-opus-4-7, etc.). When unset (`auto` / None), we omit
        # the flag entirely and let claude-code pick its default.
        if self.model:
            args.extend(["--model", self.model])
        # MP3 (py-1.13.4) — reasoning-depth dial. Omitted when None
        # ('default' sentinel) so claude-code uses its own default.
        if self.effort:
            args.extend(["--effort", self.effort])
        if use_session:
            args[2:2] = ["--session-id", session_id]
        # py-1.10.5 — Pipe the briefing through stdin instead of
        # appending it as a positional argument. claude 2.1.145
        # rejects a trailing positional that arrives AFTER a
        # multi-value flag (`--disallowed-tools <comma,list>`) — the
        # parser consumes our prompt as another disallowed-tool name
        # or just drops it, and claude exits 1 with stderr:
        #   "Error: Input must be provided either through stdin or
        #    as a prompt argument when using --print"
        # Captured 2026-05-29 by py-1.10.4's stderr drainer (which
        # had been silently dropping this error for every spawn
        # since the cockpit's roadmap-architect feature shipped).
        # Stdin works regardless of argv order, so it's the
        # forward-compatible answer.
        briefing = self._briefing()
        env = {
            **os.environ,
            "MESHKORE_IDENTITY": self.identity,
            "MESHKORE_CONV": self.conv,
            "MESHKORE_SESSION_ID": session_id,
        }
        # Stamped so ChatSessionReaper can apply the hard-timeout check
        # (any runner whose runtime exceeds the reaper's threshold gets
        # force-cancelled). Set BEFORE Popen so even a subprocess that
        # hangs in the OS spawn path gets the timestamp.
        self._started_at = time.time()
        self.proc = subprocess.Popen(
            args,
            cwd=str(self.paths.root),
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        self.pid = self.proc.pid
        # Write the briefing to stdin and close. claude reads it
        # all (EOF on close) then begins streaming results to stdout.
        try:
            if self.proc.stdin is not None:
                self.proc.stdin.write(briefing.encode("utf-8"))
                self.proc.stdin.close()
        except (BrokenPipeError, OSError) as e:
            _log(f"claude({self.conv}) stdin write failed: {e}")
        _log(
            f"claude({self.conv}) spawned pid={self.pid} agent_type={self.agent_type} "
            f"stream={self.stream_id} briefing_len={len(briefing)}"
        )
        self.hub.broadcast(
            {
                "type": "task.started",
                "id": f"chat:{self.conv}",
                "agent": self.identity,
                "ts": _iso_now(),
                "runner": "claude-code",
                "conv": self.conv,
                "stream_id": self.stream_id,
            }
        )
        # Empty assistant bubble so the cockpit shows progress immediately.
        self.hub.broadcast(
            {
                "type": "chat.assistant.delta",
                "author": self.identity,
                "conv": self.conv,
                "stream_id": self.stream_id,
                "text": "",
                "ts": _iso_now(),
            }
        )
        threading.Thread(target=self._reader_loop, daemon=True).start()
        # py-1.10.4 — stderr drainer. Until this lands, stderr=PIPE
        # was capturing claude's error output but NOBODY READ IT, so
        # every subprocess crash (prompt too long, blocked tool, env
        # issue, segfault) surfaced as "empty chat.assistant.final"
        # with no diagnostic anywhere in the daemon log. The reader
        # loop above only iterates stdout; PIPE'd stderr fills its
        # OS buffer (typically 64 KB) and on overflow Linux/Darwin
        # block claude on its next write — turning a soft failure
        # into an unkillable zombie. Drain it into the daemon log.
        threading.Thread(target=self._stderr_drain, daemon=True).start()

    def _stderr_drain(self) -> None:
        """Read self.proc.stderr line-by-line and forward to the
        daemon log. Tagged with conv so multiple in-flight runners
        don't blur together. Cheap — claude rarely emits much on
        stderr unless it's failing."""
        if not self.proc or not self.proc.stderr:
            return
        for raw in self.proc.stderr:
            try:
                line = raw.decode("utf-8", "replace").rstrip()
            except Exception:
                continue
            if line:
                _log(f"claude({self.conv}) stderr: {line}")

    def cancel(self) -> None:
        if self.cancelled:
            return
        self.cancelled = True
        if self.proc and self.proc.poll() is None:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
            except (OSError, ProcessLookupError):
                pass

            def _hard_kill():
                if self.proc and self.proc.poll() is None:
                    try:
                        os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                    except (OSError, ProcessLookupError):
                        pass

            threading.Timer(30.0, _hard_kill).start()

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
