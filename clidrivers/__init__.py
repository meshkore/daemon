"""clidrivers — pluggable CLI-client drivers (multi-cli-clients initiative).

`driver_for(client_id)` is the one entry point the rest of the daemon
should use. Unknown/absent/None ids resolve to ClaudeCodeDriver — a
member file with no `client` key (every existing one, pre DM-CLI-02),
or a `client` value from a future daemon version this one doesn't know
about, degrades safely to today's behavior instead of crashing a spawn.

Bundler note: this is a package-FOLDER entry in bundle.py's MODULES
(like agent_prompts) — `_expand_module()` inlines every non-dunder
`*.py` here (sorted) then this `__init__.py` last, so `DRIVERS` below
can reference names assembled from the fragment files above it."""

from __future__ import annotations

from typing import Dict

from .base import ClientDriver
from .claudecode import ClaudeCodeDriver
from .codex import CodexDriver
from .gemini import GeminiDriver

DRIVERS: Dict[str, ClientDriver] = {
    "claude-code": ClaudeCodeDriver(),
    "gemini": GeminiDriver(),
    "codex": CodexDriver(),
}

_DEFAULT = DRIVERS["claude-code"]


def driver_for(client_id) -> ClientDriver:
    if not client_id:
        return _DEFAULT
    return DRIVERS.get(str(client_id).strip().lower(), _DEFAULT)


def known_ids() -> list:
    return sorted(DRIVERS.keys())
