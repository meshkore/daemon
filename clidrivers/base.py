"""clidrivers/base.py — ClientDriver interface + normalized stream events.

DM-CLI-01 (multi-cli-clients). Every CLI a team member can be dispatched
through (claude-code, gemini, codex, ...) implements ClientDriver so
runnerspawn.py/runnerloop.py can spawn + parse a turn without knowing
which binary is actually running.

`parse_stream_line()` is the one method every driver MUST get right: it
translates one raw stdout line from the client's own wire format into
zero or more of the four NormalizedEvent shapes below. Everything
downstream of that call — anchor-marker buffering, tool timeline
persistence, WS broadcast, usage/cost capture, frontend rendering — is
untouched by which driver produced the events; it was written once
against these four shapes and stays that way."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class TextDelta:
    """One incremental chunk of assistant text (not cumulative)."""

    text: str


@dataclass
class ToolUse:
    name: Optional[str]
    input: Any


@dataclass
class ToolResult:
    ok: bool


@dataclass
class Final:
    """Terminal event for the turn. `usage`/`cost_usd` are None when the
    client doesn't report them (not every CLI will)."""

    text: str
    usage: Optional[Dict[str, int]] = None
    cost_usd: Optional[float] = None


NormalizedEvent = Any  # TextDelta | ToolUse | ToolResult | Final


class ClientDriver:
    """Base class. `id`/`label` identify the driver in `team.py`'s
    `client` field and the `GET /clients` catalog. Concrete drivers
    override the methods they need; the defaults here keep a minimal
    driver (e.g. one with no catalog yet) safe to register."""

    id: str = "base"
    label: str = "Base"

    def find_binary(self) -> Optional[str]:
        """Locate the CLI binary on this machine, or None if absent.
        Cheap/local only (shutil.which + known install-path globs) —
        never a network call."""
        raise NotImplementedError

    def install_hint(self) -> str:
        return f"install the {self.label} CLI"

    def auth_configured(self) -> Optional[bool]:
        """Best-effort local check (env var / credentials file
        presence). None means "can't tell" — never claim a false
        positive/negative when the driver genuinely doesn't know."""
        return None

    def models_catalog(self) -> List[Dict[str, Any]]:
        """[{id, label, ...}] — this driver's current model options."""
        return []

    def efforts_catalog(self) -> List[Dict[str, Any]]:
        """[{id, label, ...}] — this driver's current effort/reasoning
        options."""
        return []

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
        """Full argv (binary as argv[0]) for a one-shot non-interactive
        turn. `prompt` (the built briefing) is passed in so a driver
        whose CLI wants it as an argv flag (not stdin) can include it
        here; a driver that delivers it via stdin instead (the default
        — see `write_prompt()`) simply ignores this parameter."""
        raise NotImplementedError

    def write_prompt(self, proc: Any, prompt: str) -> None:
        """Deliver the briefing to the already-spawned process. Default:
        pipe via stdin then close (works for claude-code and codex —
        override to a no-op, closing stdin without writing, when
        `build_args()` already put the prompt into argv instead, e.g.
        Gemini's `-p <text>`)."""
        try:
            if proc.stdin is not None:
                proc.stdin.write(prompt.encode("utf-8"))
                proc.stdin.close()
        except (BrokenPipeError, OSError):
            pass

    def parse_stream_line(self, line: str) -> List[NormalizedEvent]:
        """Translate one decoded, non-empty stdout line into zero or
        more NormalizedEvents. Must swallow malformed/non-JSON lines
        (return []) rather than raising — a client's banner/noise
        output must never crash the reader loop."""
        raise NotImplementedError

    def is_transient_error(self, text: str) -> bool:
        """True iff `text` is a TRANSPORT-class failure (rate limit,
        upstream 5xx, timeout) that a fresh spawn of the SAME turn can
        plausibly clear — as opposed to a task outcome or a
        request-shape error a retry won't fix. Conservative default:
        never retry unless a driver explicitly knows its own error
        vocabulary."""
        return False
