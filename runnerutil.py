"""runnerutil.py — per-conv claude session-id + CLI discovery helpers.

Extracted from runner.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

import os
import uuid
from typing import Optional


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
