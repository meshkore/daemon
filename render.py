"""Agent-CLI instructions renderer (Standard §17, v18+).

py-1.14.7 (ADI-01) — closes the gap where §17 mandated the daemon keep
the per-CLI instruction files in sync but no code actually did it, so
`CLAUDE.md` / `AGENTS.md` / `GEMINI.md` drifted behind the canonical
preamble (the snapshots item §20 never reached them; field-confirmed
2026-06-13).

Two responsibilities, both idempotent:

1. **Preamble refresh** (`refresh_from_remote`). Fetches the canonical
   preamble at ``https://meshkore.com/standard/agent-instructions.md``
   and overwrites the ``MESHKORE_PREAMBLE`` block of
   ``.meshkore/public/AGENT_INSTRUCTIONS.md`` IN PLACE — the
   operator's ``OPERATOR_CONTENT`` block is preserved byte-for-byte.
   Driven on the existing VersionWatcher tick (30 min), independent of
   the ``auto_update`` opt-out (preamble freshness is not a code
   upgrade).

2. **Per-CLI render** (`render_targets`). Mirrors
   ``AGENT_INSTRUCTIONS.md`` into the well-known root files each CLI
   auto-loads. Driven by a 3 s mtime watch on the source (so an
   operator edit fans out within seconds) and called once at boot
   (heals any drift left by an older daemon). The two v19+ targets
   (`.cursor/rules/meshkore.mdc`, `.clinerules`) are written only when
   the cluster's ``STANDARD_VERSION`` is ≥ 19.

Zero daemon coupling beyond ``Paths`` + ``Hub`` — same shape as the
registries in ``registries.py``. daemon.py re-imports
``AgentInstructionsRenderer``."""

from __future__ import annotations

import threading
from typing import List, Optional, Tuple

from hub import Hub
from paths import Paths
from utils import _log

CANONICAL_PREAMBLE_URL = "https://meshkore.com/standard/agent-instructions.md"

_PREAMBLE_BEGIN = (
    "<!-- MESHKORE_PREAMBLE_BEGIN"  # marker PREFIX (line carries an em-dash note)
)
_PREAMBLE_END = "<!-- MESHKORE_PREAMBLE_END -->"

# (filename, audience label, min STANDARD_VERSION that mandates it).
# Standard §17.2 render-targets table. Paths are repo-root-relative.
_TARGETS: Tuple[Tuple[str, str, int], ...] = (
    ("CLAUDE.md", "Claude Code (Anthropic).", 18),
    ("AGENTS.md", "Codex / Aider / general convention.", 18),
    ("GEMINI.md", "Gemini CLI (Google).", 18),
    (".cursor/rules/meshkore.mdc", "Cursor (Anysphere).", 19),
    (".clinerules", "Cline (VSCode extension).", 19),
)


def _render_header(audience: str) -> str:
    return (
        "<!-- Auto-rendered from .meshkore/public/AGENT_INSTRUCTIONS.md per\n"
        "     MeshKore standard §17 (v18+). Edit the source, not this file.\n"
        f"     Audience: {audience} -->\n\n"
    )


class AgentInstructionsRenderer:
    """Keeps the §17 per-CLI files in sync with AGENT_INSTRUCTIONS.md,
    and the AGENT_INSTRUCTIONS.md preamble in sync with the standard."""

    POLL_SEC = 3.0
    _FETCH_TIMEOUT = 8

    def __init__(self, paths: Paths, hub: "Hub") -> None:
        self.paths = paths
        self.hub = hub
        self._stop = threading.Event()
        self._mtime: Optional[float] = None
        # Boot sync: bring the per-CLI files up to date with whatever
        # AGENT_INSTRUCTIONS.md currently says (heals drift left by an
        # older daemon that lacked this loop). Remote preamble refresh
        # happens on the first VersionWatcher tick.
        try:
            self.render_targets(broadcast=False)
        except Exception as e:
            _log(f"instructions-renderer: boot render failed: {e}")
        threading.Thread(target=self._watch_loop, daemon=True).start()

    # ── source / paths ──────────────────────────────────────────────
    @property
    def _source(self):
        return self.paths.public / "AGENT_INSTRUCTIONS.md"

    def _standard_version(self) -> int:
        try:
            return int((self.paths.meshkore / "STANDARD_VERSION").read_text().strip())
        except Exception:
            return 0

    def shutdown(self) -> None:
        self._stop.set()

    # ── local render (source → per-CLI files) ───────────────────────
    def _watch_loop(self) -> None:
        while not self._stop.wait(self.POLL_SEC):
            try:
                mt = self._source.stat().st_mtime
            except OSError:
                continue
            if mt == self._mtime:
                continue
            try:
                self.render_targets(broadcast=True)
            except Exception as e:
                _log(f"instructions-renderer: render failed: {e}")

    def render_targets(self, broadcast: bool = True) -> List[str]:
        """Write the per-CLI files from AGENT_INSTRUCTIONS.md. Idempotent:
        a target whose content already matches is skipped (no disk churn,
        no WS spam). Returns the list of files actually (re)written."""
        try:
            body = self._source.read_text()
            self._mtime = self._source.stat().st_mtime
        except OSError:
            return []
        std_ver = self._standard_version()
        written: List[str] = []
        for rel, audience, min_ver in _TARGETS:
            if std_ver and std_ver < min_ver:
                continue  # not yet mandated for this cluster's standard version
            content = _render_header(audience) + body
            dest = self.paths.root / rel
            try:
                if dest.exists() and dest.read_text() == content:
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_text(content)
                written.append(rel)
            except OSError as e:
                _log(f"instructions-renderer: write {rel} failed: {e}")
        if written:
            _log(f"instructions-renderer: rendered {', '.join(written)}")
            if broadcast:
                try:
                    self.hub.broadcast(
                        {"type": "agent_instructions.rendered", "files": written}
                    )
                except Exception:
                    pass
        return written

    # ── remote refresh (standard → source preamble block) ───────────
    def refresh_from_remote(self) -> bool:
        """Fetch the canonical preamble and overwrite the
        MESHKORE_PREAMBLE block of AGENT_INSTRUCTIONS.md in place,
        preserving OPERATOR_CONTENT. Returns True if the source changed
        (a render is triggered by the mtime watch / inline call)."""
        remote = self._fetch_canonical()
        if not remote:
            return False
        try:
            text = self._source.read_text()
        except OSError:
            return False
        b = text.find(_PREAMBLE_BEGIN)
        e = text.find(_PREAMBLE_END)
        if b < 0 or e < 0 or e < b:
            _log(
                "instructions-renderer: AGENT_INSTRUCTIONS.md missing preamble markers — skip"
            )
            return False
        begin_line_end = text.find("\n", b)
        if begin_line_end < 0:
            return False
        begin_marker_line = text[
            b:begin_line_end
        ]  # keep the exact marker (em-dash note)
        end_line_end = text.find("\n", e)
        if end_line_end < 0:
            end_line_end = len(text)
        suffix = text[
            end_line_end:
        ]  # from END marker's newline onward (operator block)
        new_block = f"{begin_marker_line}\n\n{remote.strip(chr(10))}\n\n{_PREAMBLE_END}"
        new_text = text[:b] + new_block + suffix
        if new_text == text:
            return False
        try:
            self._source.write_text(new_text)
        except OSError as e:
            _log(f"instructions-renderer: preamble write failed: {e}")
            return False
        _log("instructions-renderer: preamble refreshed from standard")
        try:
            self.hub.broadcast({"type": "agent_instructions.preamble_refreshed"})
        except Exception:
            pass
        # Render now so the per-CLI files don't wait for the next 3 s poll.
        try:
            self.render_targets(broadcast=True)
        except Exception:
            pass
        return True

    def _fetch_canonical(self) -> Optional[str]:
        try:
            import urllib.request

            req = urllib.request.Request(
                CANONICAL_PREAMBLE_URL,
                headers={"User-Agent": "meshcore-py instructions-renderer"},
            )
            with urllib.request.urlopen(req, timeout=self._FETCH_TIMEOUT) as r:
                return r.read().decode("utf-8", errors="replace")
        except Exception as e:
            _log(f"instructions-renderer: canonical fetch failed: {e}")
            return None
