"""clidrivers/codex.py — CodexDriver.

DM-CLI-05 (multi-cli-clients). Built after DM-CLI-03's spawn-safety
spike PASSED on the operator's machine (codex-cli 0.137.0): three
spawns with the daemon's EXACT Popen shape (piped stdin/stdout/stderr,
no TTY, start_new_session=True) all returned cleanly — no hang, no
crash. See `.meshkore/modules/daemon/tasks/DM-CLI-03-...md` for the
transcript.

Verified live against the real CLI (not assumed from docs):
- `codex exec --json -` reads the prompt from stdin (closing stdin ends
  input) and emits NDJSON events on stdout.
- Codex does NOT stream token-level deltas — one `item.completed` per
  finished item (whole message at once). A Codex-driven chat bubble
  therefore "pops in" complete rather than streaming char-by-char like
  Claude; this is a real, observed limitation, not a gap in this
  driver. The hard requirement (Final always carries real text) holds.
- Tool-call visibility is GOOD: shell tool calls surface as
  `item.started`/`item.completed` with `type: "command_execution"`,
  `command`, `exit_code`, `aggregated_output` — a clean map to
  ToolUse/ToolResult, better than initially assumed.
- `--dangerously-bypass-approvals-and-sandbox` is required for headless
  use (no interactive approval prompts to block on); `--skip-git-repo-
  check` avoids a hard failure when `paths.root` isn't a git repo.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from .base import ClientDriver, Final, TextDelta, ToolResult, ToolUse


class CodexDriver(ClientDriver):
    id = "codex"
    label = "Codex CLI"

    def find_binary(self) -> Optional[str]:
        import shutil

        found = shutil.which("codex")
        if found:
            return found
        import glob

        for pattern in [
            os.path.expanduser("~/.nvm/versions/node/v*/bin/codex"),
            "/opt/homebrew/bin/codex",
            "/usr/local/bin/codex",
        ]:
            hits = sorted(glob.glob(pattern), reverse=True)
            if hits and os.access(hits[0], os.X_OK):
                return hits[0]
        return None

    def install_hint(self) -> str:
        return "install via `npm i -g @openai/codex`"

    def auth_configured(self) -> Optional[bool]:
        if os.environ.get("OPENAI_API_KEY"):
            return True
        # codex-cli's default login flow (`codex login`) writes here —
        # presence is a reasonable signal even without an API-key env
        # var (ChatGPT/ Codex account login, not just API-key auth).
        return os.path.exists(os.path.expanduser("~/.codex/auth.json"))

    def models_catalog(self) -> List[Dict[str, Any]]:
        # PROVISIONAL — verify against `codex exec --help` / the
        # installed CLI's actual model list before relying on this for
        # anything beyond a starting default; OpenAI's model lineup
        # moves fast and this list will drift.
        return [
            {"id": "", "label": "Default (CLI config)"},
        ]

    def efforts_catalog(self) -> List[Dict[str, Any]]:
        # codex-cli's `model_reasoning_effort` config key accepts these
        # (verified accepted without error via `-c
        # model_reasoning_effort=...` on the installed CLI). "default"
        # here means "omit the override, let codex use its own config
        # default" — mirrors every other driver's "default" sentinel.
        return [
            {"id": "default", "label": "Default"},
            {"id": "minimal", "label": "Minimal"},
            {"id": "low", "label": "Low"},
            {"id": "medium", "label": "Medium"},
            {"id": "high", "label": "High"},
            {"id": "xhigh", "label": "XHigh"},
        ]

    # MeshKore's shared effort vocabulary includes "max", which codex
    # doesn't have — map it down to codex's top tier rather than
    # rejecting the turn outright.
    _EFFORT_MAP = {"max": "xhigh"}

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
        # codex reads the prompt from stdin (write_prompt's default) —
        # confirmed live via the DM-CLI-03 spike.
        _ = prompt
        args = [
            binary,
            "exec",
            "--json",
            # Headless: no interactive approval prompts to block on
            # (mirrors claude-code's --permission-mode bypassPermissions).
            "--dangerously-bypass-approvals-and-sandbox",
            # paths.root isn't guaranteed to be a git repo (e.g. a
            # freshly scaffolded project before the first commit).
            "--skip-git-repo-check",
        ]
        if model:
            args.extend(["-m", model])
        if effort and effort != "default":
            level = self._EFFORT_MAP.get(effort, effort)
            args.extend(["-c", f'model_reasoning_effort="{level}"'])
        # No native session/resume in v1 (see module docstring + the
        # multi-cli-clients initiative's explicit scope boundary) — every
        # turn is a fresh `codex exec`, matching how MeshKore already
        # treats claude-code (the daemon owns cross-turn history via the
        # briefing, not the CLI's own session mechanism).
        _ = session_id, use_session
        # Trailing `-` — read the prompt from stdin (verified: omitting
        # the positional entirely also works, but `-` is documented and
        # explicit).
        args.append("-")
        return args

    def parse_stream_line(self, line: str) -> List[Any]:
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            return []
        if not isinstance(ev, dict):
            return []
        ev_type = ev.get("type")
        out: List[Any] = []
        if ev_type == "item.started":
            item = ev.get("item") or {}
            if item.get("type") == "command_execution":
                out.append(
                    ToolUse(name="shell", input={"command": item.get("command")})
                )
            return out
        if ev_type == "item.completed":
            item = ev.get("item") or {}
            item_type = item.get("type")
            if item_type == "command_execution":
                out.append(ToolResult(ok=item.get("exit_code") == 0))
                return out
            if item_type == "agent_message":
                text = item.get("text") or ""
                # No token-level streaming from this CLI (see module
                # docstring) — emit the complete text as a single delta
                # so it still renders in the live chat bubble, THEN...
                if text:
                    out.append(TextDelta(text=text))
                return out
            return out
        if ev_type == "turn.completed":
            usage_raw = ev.get("usage")
            usage: Optional[Dict[str, int]] = None
            if isinstance(usage_raw, dict):
                # codex's usage keys differ from claude's — normalise to
                # the same shape chat.usage already broadcasts.
                usage = {
                    "input_tokens": int(usage_raw.get("input_tokens") or 0),
                    "output_tokens": int(usage_raw.get("output_tokens") or 0),
                    "cache_read_input_tokens": int(
                        usage_raw.get("cached_input_tokens") or 0
                    ),
                    "cache_creation_input_tokens": 0,
                }
            # ...the terminal event carries the SAME text again as Final
            # (codex's turn.completed doesn't repeat the message text,
            # so reuse whatever the last agent_message delta produced —
            # tracked by the caller via cumulative text, same as every
            # other driver's Final; here we simply signal "turn done"
            # with no additional text of its own).
            out.append(Final(text="", usage=usage, cost_usd=None))
            return out
        return []

    def is_transient_error(self, text: str) -> bool:
        if not text:
            return False
        low = text.strip().lower()
        markers = (
            "rate limit",
            "rate_limit",
            "429",
            "overloaded",
            "internal server error",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
            "timed out",
            "timeout",
        )
        return any(m in low for m in markers)
