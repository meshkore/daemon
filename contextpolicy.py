#!/usr/bin/env python3
"""contextpolicy.py — per-runtime context-window policy (CTX1).

WHY THIS EXISTS
---------------
The daemon dispatches every chat turn to an AI CLI/runtime named by the agent
type's ``platform`` field (see ``agent_types._agent_manifest``). Today that is
almost always ``claude-code``, but the design is explicitly multi-runtime:
DeepSeek / Codex / a direct Anthropic-API agent can declare their own platform.

Those runtimes differ in TWO ways that matter for context management:

  1. **Do they self-compact?** Claude Code manages its own context window
     *within a turn* (it auto-compacts tool output, file reads, etc.) and does
     it well — we must NOT fight it. A raw-API or thin headless agent may have
     NO compaction at all, so the daemon would have to manage context itself.

  2. **What is the window size?** Needed to turn raw token counts into a
     "how full is the context" fill ratio (the cockpit gauge — the little
     circle the operator watches).

So context handling MUST be bound to the PLATFORM, never hard-coded to
claude-code. This module is the single place that knows, per platform:
  • the model→window-size map,
  • whether the platform supports compaction,
  • the fill ratio at which we consider the context "degrading" and want to
    compact (the threshold; default 50% per the operator — never sit past the
    point where a too-large context hurts more than it helps).

ARCHITECTURE NOTE — MeshKore's headless turn model
--------------------------------------------------
MeshKore does NOT keep a persistent Claude session across turns (``--session-id``
is opt-in and off by default since the py-1.6.1 hotfix). Each turn is a fresh
``claude -p`` fed a daemon-built briefing via stdin. Therefore:
  • WITHIN a turn → Claude Code owns the window and self-compacts. We only read
    the resulting ``usage``.
  • ACROSS turns → the DAEMON owns context (the briefing's rolling history).
The fill ratio here measures what the model actually read THIS turn
(``input_tokens`` + both cache buckets) against its window, so the cockpit can
show how close a turn ran to the edge, and a future trigger (CTX2) can delegate
a real summarisation to the model before the next turn when it crosses.

PURITY
------
Pure + stdlib-only (bundle-safe): no sockets, no subprocess, no sibling imports
beyond plain constants. Inlined early in the bundle (see bundle.py MODULES).
"""

from __future__ import annotations

from typing import Any, Dict, Optional

# ── model → context-window size (tokens) ────────────────────────────────────
# Conservative, current as of the assistant knowledge cutoff. Unknown Claude
# models fall back to _DEFAULT_CLAUDE_WINDOW via family-prefix match; unknown
# platforms get a None window (gauge hidden — we don't guess for runtimes we
# don't model yet). The ``[1m]`` suffix is the 1M-context variant id.
_DEFAULT_CLAUDE_WINDOW = 200_000
_CLAUDE_WINDOWS: Dict[str, int] = {
    # short aliases the cockpit / --model flag accept
    "opus": _DEFAULT_CLAUDE_WINDOW,
    "sonnet": _DEFAULT_CLAUDE_WINDOW,
    "haiku": _DEFAULT_CLAUDE_WINDOW,
    # explicit ids
    "claude-opus-4-8": _DEFAULT_CLAUDE_WINDOW,
    "claude-opus-4-7": _DEFAULT_CLAUDE_WINDOW,
    "claude-opus-4-6": _DEFAULT_CLAUDE_WINDOW,
    "claude-sonnet-5": _DEFAULT_CLAUDE_WINDOW,
    "claude-sonnet-4-6": _DEFAULT_CLAUDE_WINDOW,
    "claude-haiku-4-5": _DEFAULT_CLAUDE_WINDOW,
    # Claude 5 family — Fable 5's 1M window is NATIVE (the API's default and
    # maximum are both 1M; there is no 200k tier), so both the bare id and the
    # [1m]-suffixed variant map to 1M. Without the explicit bare-id entry the
    # longest-prefix fallback made "claude-fable-5[1m]" inherit 200k → the
    # cockpit context gauge read 5× too full.
    "claude-fable-5": 1_000_000,
    "claude-fable-5[1m]": 1_000_000,
    # 1M-context variants (opt-in [1m] tier on Opus/Sonnet)
    "claude-opus-4-8[1m]": 1_000_000,
    "claude-opus-4-7[1m]": 1_000_000,
    "claude-opus-4-6[1m]": 1_000_000,
    "claude-sonnet-5[1m]": 1_000_000,
    "claude-sonnet-4-6[1m]": 1_000_000,
}

# Default fill ratio at which we flag "compact now". The operator's rule:
# never run past ~50% — that's the band where a too-large context starts to
# hurt more than help. Per-platform overridable; later cluster.yaml-tunable.
_DEFAULT_COMPACTION_THRESHOLD = 0.50


class ContextPolicy:
    """Base / generic policy — used for any platform we don't model yet.

    A generic runtime: unknown window (gauge hidden) and NO compaction support
    (the daemon won't claim it can compact a runtime it knows nothing about).
    Subclass per platform to declare a window map + compaction behaviour."""

    platform: str = "generic"
    supports_compaction: bool = False
    compaction_threshold: float = _DEFAULT_COMPACTION_THRESHOLD

    def context_window(self, model: Optional[str]) -> Optional[int]:
        """The model's context window in tokens, or None when unknown."""
        return None

    @staticmethod
    def prompt_tokens(usage: Optional[Dict[str, Any]]) -> int:
        """Tokens the model actually READ this turn = fresh input + both cache
        buckets. (Output is what it *wrote*, not part of the prompt window, so
        it's excluded from the fill numerator.)"""
        if not isinstance(usage, dict):
            return 0
        return (
            int(usage.get("input_tokens") or 0)
            + int(usage.get("cache_read_input_tokens") or 0)
            + int(usage.get("cache_creation_input_tokens") or 0)
        )

    def fill_ratio(
        self, usage: Optional[Dict[str, Any]], model: Optional[str]
    ) -> Optional[float]:
        """prompt_tokens / window, clamped to [0,1]; None when the window is
        unknown (caller hides the gauge rather than show a wrong number)."""
        window = self.context_window(model)
        if not window or window <= 0:
            return None
        return min(1.0, self.prompt_tokens(usage) / window)

    def should_compact(
        self, usage: Optional[Dict[str, Any]], model: Optional[str]
    ) -> bool:
        """True only when the platform supports compaction AND the fill ratio
        has crossed the threshold. A platform that can't compact never says
        yes (the daemon would have to handle it another way)."""
        if not self.supports_compaction:
            return False
        ratio = self.fill_ratio(usage, model)
        return ratio is not None and ratio >= self.compaction_threshold

    def describe(
        self, usage: Optional[Dict[str, Any]], model: Optional[str]
    ) -> Dict[str, Any]:
        """The ``context`` block attached to a ``chat.usage`` event so the
        cockpit can paint the fill gauge + a 'will compact' hint. Always a
        plain JSON-safe dict; fields are None when not computable."""
        window = self.context_window(model)
        ratio = self.fill_ratio(usage, model)
        return {
            "platform": self.platform,
            "window": window,
            "prompt_tokens": self.prompt_tokens(usage),
            "fill_ratio": round(ratio, 4) if ratio is not None else None,
            "supports_compaction": self.supports_compaction,
            "threshold": self.compaction_threshold
            if self.supports_compaction
            else None,
            "should_compact": self.should_compact(usage, model),
        }


class ClaudeCodePolicy(ContextPolicy):
    """Claude Code — knows Claude window sizes and self-compacts within a turn.

    For an ``auto`` / unset model we can't know which Claude the CLI picked, so
    we assume the conservative default window (the gauge then fills a touch
    early — the safe direction). Explicit ids resolve exactly, with a
    family-prefix fallback for ids we haven't enumerated."""

    platform = "claude-code"
    supports_compaction = True
    compaction_threshold = _DEFAULT_COMPACTION_THRESHOLD

    def context_window(self, model: Optional[str]) -> Optional[int]:
        if not model or str(model).strip().lower() in ("auto", ""):
            # Model unknown (CLI default) — assume the conservative window so
            # the operator still gets a (slightly pessimistic) gauge.
            return _DEFAULT_CLAUDE_WINDOW
        m = str(model).strip()
        if m in _CLAUDE_WINDOWS:
            return _CLAUDE_WINDOWS[m]
        # Family fallback: longest matching known prefix wins (so
        # "claude-opus-4-8[1m]" beats "claude-opus-4-8" when both match).
        best: Optional[int] = None
        best_len = -1
        for known, size in _CLAUDE_WINDOWS.items():
            if m.startswith(known) and len(known) > best_len:
                best, best_len = size, len(known)
        return best if best is not None else _DEFAULT_CLAUDE_WINDOW


# ── registry ─────────────────────────────────────────────────────────────────
_GENERIC = ContextPolicy()
_POLICIES: Dict[str, ContextPolicy] = {
    "claude-code": ClaudeCodePolicy(),
}


def policy_for(platform: Optional[str]) -> ContextPolicy:
    """Resolve the policy for a platform string (from ``_agent_manifest``).
    Unknown / None platforms get the generic policy — window unknown, no
    compaction — so the daemon never assumes claude-code behaviour for a
    runtime it doesn't model."""
    return _POLICIES.get((platform or "").strip(), _GENERIC)
