"""quotaprober.py — QuotaProber — wakes paused quota keys.

Extracted from quota.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

import threading
import time
from typing import Any, Optional

from agent_prompts import AGENT_PROMPTS
from agent_types import _agent_manifest
from utils import _debug_emit, _log


class QuotaProber:
    """py-1.10.27 — Background thread that probes paused quota keys.

    Every TICK_SECS, scans `QuotaState.keys_due_for_probe()` and
    dispatches a minimal Claude Code subprocess against each. Reads
    the final, classifies, and either un-pauses the key (probe
    succeeded → quota is back) or extends the pause by another
    DEFAULT cooldown (probe still rate-limited → still locked).

    The probe runs INSIDE the daemon, NOT through `/chat/dispatch`,
    so it bypasses the dispatch mutex (the pause itself is what we're
    testing against). It does NOT touch the timeline (we don't want
    `chat.user` events for `probe-…` cluttering the cockpit).

    Cost: one ~10-token Claude Code invocation per paused key per
    hour. Negligible vs the cost of looping into a wall."""

    TICK_SECS = 60
    PROBE_PROMPT = (
        "This is an automated quota probe from the meshcore daemon. "
        "Reply with exactly the single word `pong` and nothing else. "
        "Do not use tools. Do not commit. End your turn immediately."
    )
    PROBE_PROMPT_TIMEOUT_SECS = 90  # subprocess wall-clock cap

    def __init__(self, daemon: Any) -> None:
        self.daemon = daemon
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        _log(f"quota-prober: started (tick={self.TICK_SECS}s)")

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.wait(self.TICK_SECS):
            try:
                due = self.daemon.quota.keys_due_for_probe()
                for key in due:
                    if self._stop.is_set():
                        break
                    self._probe_one(key)
            except Exception as e:
                _log(f"quota-prober: tick failed ({e})")

    def _probe_one(self, key: str) -> None:
        # Resolve an agent_type for this key (any type whose manifest
        # matches; we use the first hit since they share a quota pool).
        agent_type = self._agent_type_for_key(key)
        probe_conv = f"probe-{key.replace('/', '-')}-{int(time.time())}"
        _log(f"quota-prober: probing {key} via {agent_type} (conv={probe_conv})")
        _debug_emit(
            "quota.probe.start",
            msg=f"probing {key}",
            conv=probe_conv,
            data={"quota_key": key, "agent_type": agent_type},
        )
        try:
            runner = self.daemon._spawn_chat_turn(
                probe_conv,
                self.PROBE_PROMPT,
                agent_type=agent_type,
                # No parent_conv on purpose — keeps the wake hook out.
            )
        except Exception as e:
            _log(f"quota-prober: spawn failed for {key}: {e}")
            self.daemon.quota.record_probe(key, probe_conv, "spawn-failed")
            return
        # Block until the subprocess ends or our timeout fires.
        runner.done.wait(timeout=self.PROBE_PROMPT_TIMEOUT_SECS)
        if not runner.done.is_set():
            _log(
                f"quota-prober: {key} timed out after {self.PROBE_PROMPT_TIMEOUT_SECS}s — cancelling"
            )
            try:
                runner.cancel()
            except Exception:
                pass
            self.daemon.quota.record_probe(key, probe_conv, "timeout")
            return
        # Classify the final.
        preview = (runner._cumulative_text or "").strip()
        exit_code = runner.proc.returncode if runner.proc else None
        outcome = self.daemon._classify_subagent_final(preview, exit_code)
        _debug_emit(
            "quota.probe.result",
            msg=f"probe {key} → {outcome}",
            conv=probe_conv,
            lvl=("info" if outcome == "success" else "warn"),
            data={
                "quota_key": key,
                "outcome": outcome,
                "exit": exit_code,
                "preview_head": preview[:200],
            },
        )
        # success → unpause (quota window has reset)
        # rate-limited → keep pause, bump consecutive counter (auto-extends)
        # no-commit / error → treat as inconclusive: leave pause as-is,
        #                     record probe so the operator can see history.
        if outcome == "success":
            self.daemon.quota.record_probe(key, probe_conv, "success")
            _log(f"quota-prober: {key} CLEARED (probe succeeded)")
        elif outcome == "rate-limited":
            # Extend the pause by another default cooldown.
            self.daemon.quota.pause(
                key,
                reason=f"probe still rate-limited ({probe_conv})",
            )
            self.daemon.quota.record_probe(key, probe_conv, "rate-limited")
            _log(f"quota-prober: {key} STILL LOCKED (probe rate-limited again)")
        else:
            self.daemon.quota.record_probe(key, probe_conv, outcome)
            _log(f"quota-prober: {key} probe inconclusive ({outcome})")

    def _agent_type_for_key(self, quota_key: str) -> str:
        """Pick any agent_type whose manifest maps to this quota_key.
        Falls back to 'custom' which is always present."""
        for t in AGENT_PROMPTS.keys():
            if _agent_manifest(t)["quota_key"] == quota_key:
                return t
        return "custom"
