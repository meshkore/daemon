"""clidrivers/claudecode.py — ClaudeCodeDriver.

DM-CLI-01 (multi-cli-clients). Verbatim extraction of the logic that
used to live inline in runnerspawn.py/runnerloop.py — the argv-build
block, `claude` binary discovery, and the `stream-json` line-parsing
branch. This is a pure code move: every comment explaining WHY a given
flag/behavior exists (py-1.6.1 hotfix, MP1/MP3, the stdin-piping
workaround, CU1 usage capture) travels with the code, unchanged."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional

from .base import ClientDriver, Final, TextDelta, ToolResult, ToolUse


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


class ClaudeCodeDriver(ClientDriver):
    id = "claude-code"
    label = "Claude Code"

    def find_binary(self) -> Optional[str]:
        return _find_claude()

    def install_hint(self) -> str:
        return "install via `npm i -g @anthropic-ai/claude-code`"

    def auth_configured(self) -> Optional[bool]:
        # claude-code manages its own login state (`claude /login`) outside
        # any single env var MeshKore can check non-interactively — a
        # missing ANTHROPIC_API_KEY does NOT mean unauthenticated (it may
        # be using a Claude subscription login instead). Unknown, not a
        # false negative.
        return None

    def models_catalog(self) -> List[Dict[str, Any]]:
        # Mirrors architect/src/lib/models.ts MODEL_CATALOG (the frontend's
        # existing hardcoded source of truth) until DM-CLI-06/07 make this
        # driver-owned list the one both sides read from.
        return [
            {"id": "auto", "label": "Auto"},
            {"id": "opus", "label": "Opus"},
            {"id": "sonnet", "label": "Sonnet"},
            {"id": "haiku", "label": "Haiku"},
        ]

    def efforts_catalog(self) -> List[Dict[str, Any]]:
        return [
            {"id": "default", "label": "Default"},
            {"id": "low", "label": "Low"},
            {"id": "medium", "label": "Medium"},
            {"id": "high", "label": "High"},
            {"id": "xhigh", "label": "XHigh"},
            {"id": "max", "label": "Max"},
        ]

    def build_args(
        self,
        binary: str,
        *,
        prompt: str,
        model: Optional[str],
        effort: Optional[str],
        session_id: str,
        use_session: bool,
    ) -> List[str]:
        # claude-code takes the briefing via stdin (write_prompt's
        # default) — see the py-1.10.5 note below for why.
        _ = prompt
        args = [
            binary,
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
        if model:
            args.extend(["--model", model])
        # MP3 (py-1.13.4) — reasoning-depth dial. Omitted when None
        # ('default' sentinel) so claude-code uses its own default.
        if effort:
            args.extend(["--effort", effort])
        # py-1.6.1 HOTFIX — --session-id from py-1.6.0 caused empty
        # assistant responses in production (claude-code exited
        # silently on subsequent turns of the same conv). Reverted to
        # opt-in via env var MESHKORE_CLAUDE_SESSION_ID=1. Default off
        # until the failure mode is understood and re-tested. The
        # uuid5 helper (`_session_id_for_conv`, runnerutil.py) is
        # preserved so reintroduction is a one-line flip once safe.
        if use_session:
            args[2:2] = ["--session-id", session_id]
        return args

    # py-1.10.5 — Pipe the briefing through stdin instead of appending it
    # as a positional argument (base class default already does this —
    # claude 2.1.145 rejects a trailing positional that arrives AFTER a
    # multi-value flag like `--disallowed-tools <comma,list>`; the parser
    # consumes our prompt as another disallowed-tool name or drops it,
    # exiting 1 with "Input must be provided either through stdin or as a
    # prompt argument when using --print". Stdin works regardless of argv
    # order, so it's the forward-compatible answer — no override needed
    # here, `ClientDriver.write_prompt`'s default IS this behavior.)

    def parse_stream_line(self, line: str) -> List[Any]:
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            return []
        if not isinstance(ev, dict):
            return []
        out: List[Any] = []
        ev_type = ev.get("type")
        if ev_type == "stream_event":
            inner = ev.get("event") or {}
            if (
                inner.get("type") == "content_block_delta"
                and (inner.get("delta") or {}).get("type") == "text_delta"
            ):
                delta = (inner.get("delta") or {}).get("text") or ""
                if delta:
                    out.append(TextDelta(text=delta))
            elif (
                inner.get("type") == "content_block_start"
                and (inner.get("content_block") or {}).get("type") == "tool_use"
            ):
                cb = inner.get("content_block") or {}
                out.append(ToolUse(name=cb.get("name"), input=cb.get("input")))
            return out
        if ev_type == "user":
            for c in (ev.get("message") or {}).get("content") or []:
                if isinstance(c, dict) and c.get("type") == "tool_result":
                    out.append(ToolResult(ok=not c.get("is_error")))
            return out
        if ev_type == "result" and isinstance(ev.get("result"), str):
            # CU1 (py-1.13.3) — Capture token usage + cost from the
            # SDK's terminal event. claude-code emits e.g.
            #   {"type":"result","result":"…","usage":{
            #       "input_tokens":N,"output_tokens":N,
            #       "cache_read_input_tokens":N,
            #       "cache_creation_input_tokens":N},
            #    "total_cost_usd":N,"num_turns":N}
            usage_raw = ev.get("usage")
            usage: Optional[Dict[str, int]] = None
            if isinstance(usage_raw, dict):
                usage = {
                    "input_tokens": int(usage_raw.get("input_tokens") or 0),
                    "output_tokens": int(usage_raw.get("output_tokens") or 0),
                    "cache_read_input_tokens": int(
                        usage_raw.get("cache_read_input_tokens") or 0
                    ),
                    "cache_creation_input_tokens": int(
                        usage_raw.get("cache_creation_input_tokens") or 0
                    ),
                }
            cost_raw = ev.get("total_cost_usd")
            cost = float(cost_raw) if isinstance(cost_raw, (int, float)) else None
            out.append(Final(text=ev["result"], usage=usage, cost_usd=cost))
        return out

    # py-1.21.1 — TRANSIENT API-ERROR RETRY SHIELD (TR1). claude-code
    # 2.1.145 occasionally fails a long interleaved-thinking + multi-
    # tool turn with `API Error: 400 … thinking/redacted_thinking
    # blocks in the latest assistant message cannot be modified` — a
    # CLI bug reconstructing the multi-block assistant message for the
    # tool-loop continuation, so a fresh spawn rebuilds a clean array
    # and the same turn succeeds. Siblings: transient 5xx / overloaded /
    # rate-limit. These are TRANSPORT failures, not task outcomes.
    _TRANSIENT_STATUS_RE = re.compile(r"api error:\s*(?:429|5\d\d)\b")

    def is_transient_error(self, text: str) -> bool:
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
