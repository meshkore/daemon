"""chatreaper.py — ChatSessionReaper — stuck-live session sweeper.

Extracted from chat.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations


import time
from typing import TYPE_CHECKING

from sweeper import ProjectSweeper
from utils import _debug_emit, _log

if TYPE_CHECKING:
    pass


class ChatSessionReaper(ProjectSweeper):
    NAME = "chat-reaper"
    TICK_SECS = 30
    # Grace before we declare a session stuck even if the subprocess
    # is still alive. Protects against a legitimately long-running
    # subagent. Tuned to "no claude turn should ever take this long";
    # if a real turn does, the reaper won't kill it — alive-pid check
    # comes first.
    HARD_TIMEOUT_SECS = 60 * 30  # 30 minutes

    def on_start(self) -> None:
        # Initial sweep — covers the boot path where a previous kill -9
        # left state inconsistent (shouldn't happen with in-memory
        # ChatSessions, but defense in depth).
        try:
            self._sweep("boot")
        except Exception as e:
            _log(f"chat-reaper: boot sweep failed ({e})")

    def tick(self) -> None:
        # ProjectSweeper has already bound this project on this thread, so
        # self.daemon.chat_sessions / _flush_idle_chat_queues /
        # _broadcast_conv_activity resolve to it (FC-2: without the binding a
        # stuck session in a non-default project is never reaped and its queue
        # never idle-flushed, so the conv looks dead forever).
        self._sweep("tick")

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
        # Phase 3 (py-1.14.6) — idle chat-queue flush. The disk queue
        # (ChatQueueManager) is normally drained by the on_idle hook on
        # turn-COMPLETION. But a conv can hold queued items while idle
        # with no on_idle ever firing — daemon restart / self-update
        # re-exec (in-memory ChatSessions + its _wait thread gone), a
        # session reaped in Phase 1 above (pops the slot without firing
        # on_idle), or an enqueue into an already-idle conv. Those queues
        # sit forever ("N WAITING · runs after the current turn" with no
        # current turn). Flushing a head re-registers on_idle, so the
        # chain resumes. Runs on the boot sweep (resumes queues stranded
        # by the update) and every tick (safety net). Operator field
        # report 2026-06-13 (IKA cluster): queue stuck at 2 WAITING after
        # the daemon was updated mid-session.
        try:
            flushed = self.daemon._flush_idle_chat_queues()
            if flushed:
                _log(f"chat-reaper: flushed {flushed} idle queue(s) source={source}")
        except Exception as e:
            _log(f"chat-reaper: idle-queue flush failed ({e})")
