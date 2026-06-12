"""Chat coordinator state — ChatSessions + ChatSessionReaper.

The two classes that own the per-conv slot lifecycle:

* ``ChatSessions``       — conv → active runner + pending buffer.
* ``ChatSessionReaper``  — background sweeper that pops slots whose
                            subprocess has exited without setting
                            ``done`` (defence against the failure mode
                            that pinned the IKA master conv live for
                            2.5 days, 2026-06-10).

These are the lock-heavy pieces. ``ChatSessions._lock`` is the only
lock these classes acquire; the broadcast hub is hit via
``self.daemon._broadcast_conv_activity(...)`` and must NOT be called
while the lock is held (would nest into Hub's lock; the 2026-06-10
incident was exactly this kind of nested-lock hazard waiting to bite).

ChatRunner (the claude-code subprocess driver, ~800 LOC) stays in
daemon.py for now — it has many module-level helper dependencies
that warrant a deliberate later extraction. Reduces DM5 risk to
near-zero while still isolating the lock-prone code.

Bundler note: local ``_log`` / ``_debug_emit`` stubs at the top are
shadowed in ``dist/daemon.py`` by daemon.py's real definitions
(daemon.py is appended last so its module-level names win)."""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from utils import _debug_emit, _log  # DM7 — real helpers, no more shadow stubs


class ChatSessions:
    """conv → active runner + pending buffer. Same mid-turn-merge
    protocol as Node's chatSessions: a second prompt while running
    gets concatenated and runs as the next turn automatically."""

    def __init__(self) -> None:
        self._s: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        # CU1 (py-1.13.3) — cumulative usage per conv. Populated by
        # ChatRunner via `record_usage()` after each `result` event.
        # Survives the runner slot's lifetime (which only spans ONE
        # turn) so chained turns + cockpit reconnects see the full
        # consumption history. In-memory only; daemon restart resets.
        # Persisting belongs to the broader `usage-ledger` initiative.
        self._usage_total: Dict[str, Dict[str, Any]] = {}

    # CU1 — accumulate usage by conv (called per-turn-final).
    def record_usage(
        self,
        conv: str,
        turn_usage: Dict[str, int],
        turn_cost_usd: Optional[float],
    ) -> Dict[str, Any]:
        """Add this turn's tokens / cost to the conv's running total
        and return the cumulative dict shaped as:
            {input_tokens, output_tokens, cache_read_input_tokens,
             cache_creation_input_tokens, cost_usd, turns}"""
        with self._lock:
            tot = self._usage_total.get(conv) or {
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 0,
                "cost_usd": 0.0,
                "turns": 0,
            }
            for k in (
                "input_tokens",
                "output_tokens",
                "cache_read_input_tokens",
                "cache_creation_input_tokens",
            ):
                tot[k] = int(tot.get(k, 0)) + int(turn_usage.get(k, 0) or 0)
            if turn_cost_usd is not None:
                tot["cost_usd"] = float(tot.get("cost_usd", 0.0)) + float(turn_cost_usd)
            tot["turns"] = int(tot.get("turns", 0)) + 1
            self._usage_total[conv] = tot
            # Return a defensive copy so callers can broadcast it
            # without holding the lock.
            return dict(tot)

    def usage_total(self, conv: str) -> Optional[Dict[str, Any]]:
        """Read-only accessor for the cumulative usage dict. Returns
        None when the conv has no recorded turns yet (cockpit hides
        the chip)."""
        with self._lock:
            tot = self._usage_total.get(conv)
            return dict(tot) if tot else None

    def has(self, conv: str) -> bool:
        with self._lock:
            return conv in self._s

    def list_active(self) -> List[str]:
        """All conv ids with a turn in flight. Used by /self-update to
        refuse mid-stream so claude-code processes aren't orphaned."""
        with self._lock:
            return list(self._s.keys())

    def turn_snapshot(self, conv: str) -> Optional[Dict[str, Any]]:
        """SRL2 (py-1.13.1) — for a live conv, return the data a
        cockpit needs to rehydrate mid-turn after a browser refresh:

            {
              "current_turn": {
                "started_at": "<iso ts when runner spawned>",
                "stream_id":  "<runner.stream_id>",
                "partial_text": "<runner._cumulative_text, capped 16 KB>",
                "tool_calls_count": <int>,
                "deltas_seen": <int>,
              },
              "queue": [{"text", "id", "queued_at"}, …],
            }

        Returns ``None`` if the conv has no live session. Single dict
        lookup + a partial_text slice; safe to call on every
        chat_convs() build."""
        with self._lock:
            sess = self._s.get(conv)
            if not sess:
                return None
            runner = sess.get("runner")
            pending = sess.get("pending") or []
            queued_at = sess.get("queued_at") or ""
        current_turn = None
        if runner is not None:
            current_turn = {
                "started_at": getattr(runner, "started_at", None),
                "stream_id": getattr(runner, "stream_id", None),
                "partial_text": (getattr(runner, "_cumulative_text", "") or "")[:16000],
                "tool_calls_count": int(getattr(runner, "tool_calls_count", 0) or 0),
                "deltas_seen": int(getattr(runner, "deltas_seen", 0) or 0),
            }
        queue: List[Dict[str, Any]] = [
            {"text": t, "id": f"q_{i}", "queued_at": queued_at}
            for i, t in enumerate(pending)
        ]
        return {"current_turn": current_turn, "queue": queue}

    def queue(self, conv: str, text: str) -> int:
        with self._lock:
            sess = self._s.get(conv)
            if not sess:
                return 0
            # py-1.12.20 — merge-on-arrival. Instead of accumulating
            # separate pending texts that `_wait` then merges with the
            # awkward "Several follow-up instructions while you were
            # working: 1. … 2. …" prefix, we concatenate into a single
            # pending text with `\n\n` separators. The agent sees one
            # clean continuation; the cockpit collapses the matching
            # QUEUED user bubbles into a single growing bubble.
            # Operator clarification 2026-06-10 case 1: "si mandamos
            # otro mientras hay uno en espera, añadimos el texto, con
            # una linea en medio, para que se vea que es otro párrafo."
            if sess["pending"]:
                sess["pending"][0] = sess["pending"][0] + "\n\n" + text
            else:
                sess["pending"].append(text)
            return 1

    def start(self, conv: str, runner: Any, on_chain, on_idle=None) -> None:
        with self._lock:
            self._s[conv] = {"runner": runner, "pending": [], "cancelled": False}

        def _wait():
            runner.done.wait()
            with self._lock:
                sess = self._s.get(conv)
                if not sess:
                    return
                cancelled = sess["cancelled"]
                pending = sess["pending"]
                if cancelled or not pending:
                    self._s.pop(conv, None)
                    # py-1.12.19 — Notify the on_idle hook BEFORE returning,
                    # AFTER the slot is popped. The Daemon wires this to
                    # the disk-queue auto-flush: if a queued item exists
                    # we'll spawn the next turn (and ChatSessions.start
                    # will re-occupy the slot cleanly).
                    if on_idle is not None:
                        try:
                            on_idle(conv)
                        except Exception as e:
                            _log(f"on_idle hook failed for {conv}: {e}")
                    return
                sess["pending"] = []
            merged = (
                pending[0]
                if len(pending) == 1
                else "Several follow-up instructions while you were working:\n\n"
                + "\n\n".join(f"{i + 1}. {t}" for i, t in enumerate(pending))
            )
            on_chain(conv, merged)

        threading.Thread(target=_wait, daemon=True).start()

    def cancel(self, conv: str) -> Tuple[bool, int]:
        with self._lock:
            sess = self._s.pop(conv, None)
        if not sess:
            return False, 0
        sess["cancelled"] = True
        dropped = len(sess["pending"])
        try:
            sess["runner"].cancel()
        except Exception:
            pass
        return True, dropped

    def reap_dead(self) -> List[Tuple[str, str]]:
        """Sweep every active session and force-clean any whose subprocess
        is dead but whose `done` event was never set. Returns a list of
        (conv, reason) tuples that were reaped — caller broadcasts.

        Defense-in-depth against the failure mode where ChatRunner's
        end-of-stream code (between proc.wait() and self.done.set()) raises
        an uncaught exception or otherwise skips the done.set(). Without
        this sweep the slot would stay forever — the conv would show
        `live: true`, every subsequent /chat/dispatch would silently
        queue, and the operator's chat would look dead. Field-reported
        2026-06-10 (IKA cluster: master conv stuck live for >2 days)."""
        reaped: List[Tuple[str, str]] = []
        with self._lock:
            convs = list(self._s.keys())
        for conv in convs:
            with self._lock:
                sess = self._s.get(conv)
                if sess is None:
                    continue
                runner = sess.get("runner")
            if runner is None:
                with self._lock:
                    self._s.pop(conv, None)
                reaped.append((conv, "runner-missing"))
                continue
            proc = getattr(runner, "proc", None)
            done_set = (
                getattr(runner, "done", None) is not None and runner.done.is_set()
            )
            if done_set:
                # done is set but slot wasn't popped — _wait raced or
                # threw. Pop it now so a future dispatch isn't blocked.
                with self._lock:
                    self._s.pop(conv, None)
                reaped.append((conv, "done-set-but-slot-held"))
                continue
            # poll() returns None while the process is alive, the exit
            # code (incl. 0) once it has exited. Treat "exited without
            # done.set()" as a dead session.
            exited = False
            if proc is not None:
                try:
                    exited = proc.poll() is not None
                except Exception:
                    # Can't poll → treat as dead to be safe.
                    exited = True
            else:
                # No proc attached yet (spawn raced) → don't reap mid-spawn.
                continue
            if exited:
                try:
                    runner.done.set()
                except Exception:
                    pass
                with self._lock:
                    self._s.pop(conv, None)
                reaped.append((conv, "subprocess-exited"))
        return reaped


class ChatSessionReaper:
    TICK_SECS = 30
    # Grace before we declare a session stuck even if the subprocess
    # is still alive. Protects against a legitimately long-running
    # subagent. Tuned to "no claude turn should ever take this long";
    # if a real turn does, the reaper won't kill it — alive-pid check
    # comes first.
    HARD_TIMEOUT_SECS = 60 * 30  # 30 minutes

    def __init__(self, daemon: Any) -> None:
        self.daemon = daemon
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        # Initial sweep — covers the boot path where a previous kill -9
        # left state inconsistent (shouldn't happen with in-memory
        # ChatSessions, but defense in depth).
        try:
            self._sweep("boot")
        except Exception as e:
            _log(f"chat-reaper: boot sweep failed ({e})")
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        _log(f"chat-reaper: started (tick={self.TICK_SECS}s)")

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.wait(self.TICK_SECS):
            try:
                self._sweep("tick")
            except Exception as e:
                _log(f"chat-reaper: tick failed ({e})")

    def _sweep(self, source: str) -> None:
        # Phase 1: subprocess-died-without-done sweep.
        reaped = self.daemon.chat_sessions.reap_dead()
        for conv, reason in reaped:
            _log(f"chat-reaper: reaped conv={conv} reason={reason} source={source}")
            _debug_emit(
                "chat-session.reaped",
                msg=f"reaped {conv} ({reason})",
                lvl="warn",
                conv=conv,
                data={"reason": reason, "source": source},
            )
            # Tell every connected cockpit the conv is no longer live.
            # Uses the same broadcast helper as the normal final path.
            try:
                self.daemon._broadcast_conv_activity(conv, live_override=False)
            except Exception as e:
                _log(f"chat-reaper: conv.activity broadcast failed for {conv}: {e}")
        # Phase 2: hard-timeout sweep — if a session has been running
        # for HARD_TIMEOUT_SECS straight without exiting, treat it as
        # stuck. The subprocess is still alive but going nowhere; this
        # catches deadlocks in claude-code that wouldn't trip the
        # "subprocess exited" check.
        now = time.time()
        with self.daemon.chat_sessions._lock:
            entries = list(self.daemon.chat_sessions._s.items())
        for conv, sess in entries:
            runner = sess.get("runner")
            if runner is None:
                continue
            started_at = getattr(runner, "_started_at", None)
            if started_at is None:
                continue
            if now - started_at < self.HARD_TIMEOUT_SECS:
                continue
            _log(
                f"chat-reaper: hard-timeout conv={conv} "
                f"runtime={int(now - started_at)}s — cancelling"
            )
            _debug_emit(
                "chat-session.reaped",
                msg=f"hard-timeout {conv} after {int(now - started_at)}s",
                lvl="warn",
                conv=conv,
                data={
                    "reason": "hard-timeout",
                    "source": source,
                    "runtime_secs": int(now - started_at),
                },
            )
            try:
                self.daemon.chat_sessions.cancel(conv)
            except Exception as e:
                _log(f"chat-reaper: cancel failed for {conv}: {e}")
            try:
                self.daemon._broadcast_conv_activity(conv, live_override=False)
            except Exception as e:
                _log(f"chat-reaper: hard-timeout broadcast failed for {conv}: {e}")
