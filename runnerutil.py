"""runnerutil.py — per-conv claude session-id helper.

Extracted from runner.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used.

DM-CLI-01 (multi-cli-clients) — CLI binary discovery (`_find_claude`)
moved to `clidrivers/claudecode.py:ClaudeCodeDriver.find_binary()`,
since binary discovery is now per-driver. `_session_id_for_conv` stays
here: it's a pure conv→uuid hash, not tied to any one client's CLI."""

from __future__ import annotations

import uuid


_CLAUDE_SESSION_NAMESPACE = uuid.UUID("a4f7c1e8-3b29-4d8e-9c52-7f1e3a8d4b62")


def _session_id_for_conv(conv: str) -> str:
    """Deterministic session UUID per conversation id. Stable across
    daemon restarts so `claude -p --session-id <id>` resumes the same
    conversation context + benefits from Anthropic's prompt cache."""
    return str(uuid.uuid5(_CLAUDE_SESSION_NAMESPACE, conv or "default"))
