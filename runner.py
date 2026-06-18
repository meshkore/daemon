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

import os
import re
import secrets
import signal
import threading
import time
from typing import Any, Dict, List, Optional

from paths import Paths
from prompts import BriefingPipeline, _agent_type_normalised
from runneranchor import RunnerAnchorMixin
from runnerloop import RunnerLoopMixin
from runnerspawn import RunnerSpawnMixin
from utils import _iso_now


class ChatRunner(RunnerAnchorMixin, RunnerLoopMixin, RunnerSpawnMixin):
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

    # py-1.14.10 — TOLERANT bracket matching (Postel's law). The canonical
    # marker uses the mathematical white square brackets ⟦ ⟧ (U+27E6/U+27E7),
    # but LLMs do NOT reliably reproduce rare Unicode glyphs — they routinely
    # normalise them to ASCII `[anchor]` / `[[anchor]]` (or CJK 【anchor】).
    # When that happened the strict `⟦anchor⟧`-only regex matched NOTHING, so
    # the marker (a) was never parsed → no init/task creation, no conv_meta,
    # no live roadmap painting, AND (b) was never stripped → the raw
    # `[anchor] {...}` line leaked into the chat bubble. Both symptoms, one
    # cause. Operator field report 2026-06-13 (ikamiro: `[anchor]` visible in
    # chat + spinner never lit on the roadmap). We keep instructing the
    # canonical ⟦ ⟧ in the briefing but accept every common rendering here.
    _ANCHOR_OPEN = r"(?:⟦|〚|【|\[\[?)"  # ⟦  〚  【  [[  [
    _ANCHOR_CLOSE = r"(?:⟧|〛|】|\]\]?)"  # ⟧  〛  】  ]]  ]
    _ANCHOR_RE = re.compile(
        _ANCHOR_OPEN + r"anchor" + _ANCHOR_CLOSE + r"\s*(\{[^\n]*\})\s*(?:\n|$)",
    )
    _ANCHOR_PROGRESS_RE = re.compile(
        _ANCHOR_OPEN
        + r"anchor-progress"
        + _ANCHOR_CLOSE
        + r"\s*(\{[^\n]*\})\s*(?:\n|$)",
    )

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
