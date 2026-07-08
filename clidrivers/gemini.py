"""clidrivers/gemini.py — GeminiDriver.

DM-CLI-04 (multi-cli-clients). Partially verified: flags below were
confirmed against a REAL local install (`gemini --version` → 0.49.0,
installed via `npm i -g @google/gemini-cli`, `gemini --help`) and a
real spawn using the daemon's exact Popen shape (piped stdin/stdout/
stderr, no TTY, start_new_session=True) — it returns cleanly (no hang,
no crash) in under 2s, including on the failure path (see below).

**What's NOT verified**: this machine has no `GEMINI_API_KEY`/ADC
configured, so every real spawn attempt failed at the auth step before
producing any model output. The exact `--output-format stream-json`
JSON-line schema below is therefore a BEST-EFFORT guess based on the
stable, well-documented core Gemini API response shape
(`candidates[].content.parts[].text`), not something observed from
this CLI's actual output. `parse_stream_line()` is written defensively
(try several plausible shapes, never raise, degrade to "no event"
rather than crash the reader loop) specifically because of this gap.
**A live smoke turn with a real `GEMINI_API_KEY` is required before
this driver can be considered fully done** — see DM-CLI-04's task file.

Confirmed-real facts (via `gemini --help`, this install):
- Headless mode is triggered by `-p/--prompt <text>` (NOT a separate
  `--non-interactive` flag — that flag doesn't exist in 0.49.0).
- `-o/--output-format` accepts `text | json | stream-json`.
- `-y/--yolo` (or `--approval-mode yolo`) auto-approves all tool calls,
  the equivalent of claude-code's `--permission-mode bypassPermissions`.
- `--skip-trust` avoids an interactive workspace-trust prompt.
- `-m/--model <id>` selects the model.
- There is NO CLI flag for reasoning effort/thinking budget in 0.49.0
  (no `--thinking-level`, no `--thinking-budget`, nothing in `--help`)
  — earlier research suggesting one exists was NOT confirmed against
  the real CLI and is not trusted here. `efforts_catalog()` reflects
  this honestly (a single no-op "default").
- On missing auth, the CLI exits fast (~1s) with a clear stderr
  message rather than hanging — a real, observed failure path this
  driver's `find_binary()`/`auth_configured()` split is designed to
  surface as an install/auth problem, not a mysterious empty response.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from .base import ClientDriver, Final, TextDelta, ToolResult, ToolUse


class GeminiDriver(ClientDriver):
    id = "gemini"
    label = "Gemini CLI"

    def find_binary(self) -> Optional[str]:
        import shutil

        found = shutil.which("gemini")
        if found:
            return found
        import glob

        for pattern in [
            os.path.expanduser("~/.nvm/versions/node/v*/bin/gemini"),
            "/opt/homebrew/bin/gemini",
            "/usr/local/bin/gemini",
        ]:
            hits = sorted(glob.glob(pattern), reverse=True)
            if hits and os.access(hits[0], os.X_OK):
                return hits[0]
        return None

    def install_hint(self) -> str:
        return "install via `npm i -g @google/gemini-cli`"

    def auth_configured(self) -> Optional[bool]:
        if os.environ.get("GEMINI_API_KEY"):
            return True
        if os.environ.get("GOOGLE_GENAI_USE_VERTEXAI") or os.environ.get(
            "GOOGLE_GENAI_USE_GCA"
        ):
            return True
        # Confirmed via a real failed spawn on this machine: the CLI
        # also reads `~/.gemini/settings.json` for an auth method.
        return os.path.exists(os.path.expanduser("~/.gemini/settings.json")) or None

    def models_catalog(self) -> List[Dict[str, Any]]:
        # PROVISIONAL — verify against the installed CLI / Google's
        # current model docs before relying on this; not observed from
        # a real successful call (no auth on this machine).
        return [
            {"id": "", "label": "Default (CLI config)"},
            {"id": "gemini-2.5-pro", "label": "Gemini 2.5 Pro"},
            {"id": "gemini-2.5-flash", "label": "Gemini 2.5 Flash"},
        ]

    def efforts_catalog(self) -> List[Dict[str, Any]]:
        # Honest reflection of `gemini --help` (0.49.0): there is no
        # reasoning-effort/thinking-budget CLI flag at all. A single
        # no-op entry rather than inventing levels that don't exist.
        return [{"id": "default", "label": "Default"}]

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
        # `-p/--prompt` takes the prompt as a FLAG VALUE (confirmed via
        # `gemini --help`: "Appended to input on stdin (if any)" — i.e.
        # stdin is an ADDITION to -p, not how the primary prompt
        # arrives). Unlike claude-code/codex, this driver puts the
        # prompt directly into argv; `write_prompt()` below is a no-op.
        args = [
            binary,
            "-p",
            prompt,
            "-o",
            "stream-json",
            "-y",  # auto-approve tool calls — headless, no one to ask
            "--skip-trust",
        ]
        if model:
            args.extend(["-m", model])
        # No effort flag exists on this CLI (see module docstring) —
        # `effort` is accepted for interface uniformity and silently
        # ignored.
        _ = effort
        # No native session/resume in v1 (explicit initiative scope
        # boundary, same as claude-code/codex) — every turn is a fresh
        # `gemini` process; the daemon owns cross-turn history.
        _ = session_id, use_session
        return args

    def write_prompt(self, proc: Any, prompt: str) -> None:
        # Prompt already went into argv via build_args() above — just
        # close stdin (no separate input to append) so the process
        # doesn't block waiting for EOF on a pipe nothing will write to.
        _ = prompt
        try:
            if proc.stdin is not None:
                proc.stdin.close()
        except (BrokenPipeError, OSError):
            pass

    def parse_stream_line(self, line: str) -> List[Any]:
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            return []
        if not isinstance(ev, dict):
            return []
        out: List[Any] = []
        # BEST-EFFORT, UNVERIFIED (see module docstring) — try the
        # stable core Gemini API shape first (candidates[].content.
        # parts[].text), then a couple of plausible CLI-wrapper
        # shortcuts, so a real run has the best chance of surfacing
        # text even if the exact wrapper shape differs from this guess.
        text = self._extract_text(ev)
        if text:
            out.append(TextDelta(text=text))
        tool_name = self._extract_tool_name(ev)
        if tool_name is not None:
            out.append(ToolUse(name=tool_name, input=ev.get("input") or ev.get("args")))
        if "error" in ev or ev.get("is_error"):
            out.append(ToolResult(ok=False))
        if ev.get("type") in ("result", "response", "final") or ev.get("done") is True:
            out.append(Final(text=text or "", usage=None, cost_usd=None))
        return out

    @staticmethod
    def _extract_text(ev: Dict[str, Any]) -> str:
        # Core Gemini API shape.
        try:
            candidates = ev.get("candidates")
            if isinstance(candidates, list) and candidates:
                parts = ((candidates[0] or {}).get("content") or {}).get("parts")
                if isinstance(parts, list):
                    joined = "".join(
                        p.get("text", "") for p in parts if isinstance(p, dict)
                    )
                    if joined:
                        return joined
        except Exception:
            pass
        for key in ("text", "delta", "content", "message"):
            v = ev.get(key)
            if isinstance(v, str) and v:
                return v
        return ""

    @staticmethod
    def _extract_tool_name(ev: Dict[str, Any]) -> Optional[str]:
        for key in ("tool_call", "toolCall", "function_call", "functionCall"):
            v = ev.get(key)
            if isinstance(v, dict):
                return v.get("name")
        return None

    def is_transient_error(self, text: str) -> bool:
        if not text:
            return False
        low = text.strip().lower()
        markers = (
            "rate limit",
            "429",
            "resource_exhausted",
            "overloaded",
            "unavailable",
            "internal error",
            "deadline exceeded",
            "timed out",
            "timeout",
        )
        return any(m in low for m in markers)
