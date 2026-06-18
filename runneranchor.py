"""runneranchor.py — extracted from runner.py (daemon-architecture-v2 Phase 3d).

RunnerAnchorMixin: methods moved VERBATIM out of ChatRunner; Daemon inherits both so
every self.* resolves on the combined instance -> byte-identical."""

from __future__ import annotations

import json
from typing import List

from utils import _log


class RunnerAnchorMixin:
    def _resolve_anchor_head(self, more_text: str) -> str:
        """Buffer the head of the reply until we can decide whether
        it opens with an anchor marker. Returns the text that's safe
        to forward downstream (i.e. the head minus any marker line).

        Called for every delta until `_anchor_head_resolved` is True."""
        self._head_buffer += more_text
        # Wait for at least one full line OR a hard 4 KB cap before
        # deciding — protects against fragmented first deltas.
        if "\n" not in self._head_buffer and len(self._head_buffer) < 4096:
            return ""  # hold the entire buffer for now
        m = self._ANCHOR_RE.match(self._head_buffer.lstrip())
        # Strip leading whitespace; re-anchor the offset.
        lstripped_len = len(self._head_buffer) - len(self._head_buffer.lstrip())
        if m:
            raw_payload = m.group(1)
            try:
                payload = json.loads(raw_payload)
            except json.JSONDecodeError as e:
                if self.daemon is not None:
                    try:
                        self.daemon._handle_anchor_rejected(
                            self.conv, f"malformed JSON: {e}", raw=raw_payload
                        )
                    except Exception:
                        pass
                else:
                    _log(f"anchor: malformed JSON in conv {self.conv}: {e}")
                payload = None
            if payload is not None and self.daemon is not None:
                try:
                    self.daemon._handle_anchor(self.conv, payload, raw=raw_payload)
                except Exception as e:
                    _log(f"anchor: handler raised for conv {self.conv}: {e}")
            elif payload is not None:
                # No daemon ref (standalone) — log and continue.
                _log(f"anchor (no daemon ref): conv={self.conv} payload={payload!r}")
            # Consume up to + including the matched marker line.
            consumed_end = lstripped_len + m.end()
            visible = self._head_buffer[consumed_end:]
        else:
            # No marker → operator-visible from the start. Notify the
            # daemon so it can broadcast `conv.anchor_missing` once.
            if self.daemon is not None:
                try:
                    self.daemon._handle_anchor_missing(self.conv)
                except Exception:
                    pass
            visible = self._head_buffer
        self._head_buffer = ""
        self._anchor_head_resolved = True
        return self._strip_anchor_progress(visible)

    def _strip_anchor_progress(self, text: str) -> str:
        """Detect `⟦anchor-progress⟧ {...}` lines anywhere in `text`,
        notify the daemon, and remove them so they don't reach the
        chat thread."""
        # py-1.14.10 — bracket-agnostic fast-path guard (see _ANCHOR_RE).
        if "anchor-progress" not in text:
            return text
        out_parts: List[str] = []
        last = 0
        for m in self._ANCHOR_PROGRESS_RE.finditer(text):
            out_parts.append(text[last : m.start()])
            raw_payload = m.group(1)
            try:
                payload = json.loads(raw_payload)
            except json.JSONDecodeError:
                payload = None
            if payload is not None and self.daemon is not None:
                try:
                    self.daemon._handle_anchor_progress(
                        self.conv, payload, raw=raw_payload
                    )
                except Exception as e:
                    _log(f"anchor-progress: handler raised: {e}")
            last = m.end()
        out_parts.append(text[last:])
        return "".join(out_parts)

    def _strip_all_anchor_markers(self, text: str) -> str:
        """py-1.13.2 (anchor-strip-final fix, 2026-06-12). Belt-and-
        suspenders strip for the FINAL assistant message. The Claude SDK
        emits a `result` event with the entire reply in one piece; when
        present, daemon prefers `result_text` over `_cumulative_text`
        (the latter was already stripped delta-by-delta in
        `_resolve_anchor_head` + `_strip_anchor_progress`). That bypass
        meant `⟦anchor⟧ {...}` and `⟦anchor-progress⟧ {...}` lines
        leaked into the persisted timeline AND the broadcast chat
        message — operator saw the wire-protocol marker in chat.

        This helper sweeps both marker shapes from the final text. Pure
        text scrubbing — the side-effects (anchor handler, init/task
        file creation, conv_meta persist) already ran during the
        streaming pass. We only need to redact for display."""
        # py-1.14.10 — bracket-agnostic fast-path guard (see _ANCHOR_RE).
        # "anchor" in prose is harmless: the regexes still require a
        # bracket-wrapped marker + JSON body, so a bare word never matches.
        if "anchor" not in text:
            return text
        cleaned = self._ANCHOR_RE.sub("", text, count=1)
        cleaned = self._ANCHOR_PROGRESS_RE.sub("", cleaned)
        return cleaned.lstrip("\n")
