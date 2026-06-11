"""Per-(platform, model) quota state + background prober.

Auto-pause logic for rate-limited LLM keys. The daemon classifies a
subagent's final output as ``success`` / ``rate-limited`` / other; on
``rate-limited`` it pauses the corresponding quota key. ``QuotaProber``
wakes every ``TICK_SECS`` and re-probes paused keys with a minimal
claude turn; on ``success`` it clears the pause.

Both classes are constructor-injected the daemon ref so they can call
``self.daemon._spawn_chat_turn(...)``, ``self.daemon._classify_subagent_final(...)``,
``self.daemon._broadcast_conv_activity(...)``. Coupling is intentional —
the prober's whole job is to talk to subagents the daemon manages.

Bundler note: local stubs at the top (``_log``, ``_iso_now``,
``_iso_at``, ``_debug_emit``, ``_agent_manifest``) are shadowed in
``dist/daemon.py`` by daemon.py's later definitions. Source-tree dev
runs ``QuotaState`` correctly; ``QuotaProber.probe_one`` requires the
full daemon and is exercised only in production / integration runs."""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


from utils import _debug_emit, _iso_at, _iso_now, _log  # DM7 — real helpers


# Shadowed in bundle — `_agent_manifest` + `AGENT_PROMPTS` live in daemon.py
# (they reference the full prompt catalog). At call time the bundle's
# late-binding global lookup resolves to the real ones.
def _agent_manifest(agent_type: str) -> Dict[str, Any]:  # stub
    return {"quota_key": "claude-code/auto"}


AGENT_PROMPTS: Dict[str, Any] = {}


class QuotaState:
    """Append-only ledger of (platform, model) → pause-state.

    File format (`.meshkore/.runtime/quota-state.json`):

        {
          "version": 1,
          "updated_at": "...",
          "by_key": {
            "claude-code/auto": {
              "platform": "claude-code",
              "model": "auto",
              "paused": true,
              "paused_at": "...",
              "paused_until": "...",
              "paused_until_epoch": 1780202625,
              "reason": "Claude AI usage limit reached (work-…)",
              "first_rate_limit_at": "...",
              "consecutive_rate_limits": 2,
              "probes": [
                {"at": "...", "conv": "probe-…", "outcome": "rate-limited"}
              ],
              "last_success_at": "...",
              "last_success_conv": "work-…"
            }
          }
        }

    All writes go through `_persist_locked` (tmp + atomic rename).
    All reads return defensive copies — callers must NOT mutate the
    returned dicts directly.
    """

    # Conservative default: claude-code Pro/Max rolling window is 5h
    # but the practical sliding window resets in chunks. 60 min is the
    # smallest useful probe interval that still lets a long block clear
    # in under an hour. Operator can extend via the pause endpoint.
    DEFAULT_PAUSE_SECS = 60 * 60
    MAX_PROBE_HISTORY = 20

    def __init__(self, path: "Path") -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._data = self._load()

    def _load(self) -> Dict[str, Any]:
        try:
            if self.path.exists():
                raw = json.loads(self.path.read_text() or "{}")
                if isinstance(raw, dict) and "by_key" in raw:
                    return raw
        except Exception as e:
            _log(f"quota-state: load failed ({e}) — starting empty")
        return {"version": 1, "by_key": {}, "updated_at": _iso_now()}

    def _persist_locked(self) -> None:
        self._data["updated_at"] = _iso_now()
        try:
            tmp = self.path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(self._data, indent=2, sort_keys=True))
            tmp.replace(self.path)
        except OSError as e:
            _log(f"quota-state: persist failed ({e})")

    def _entry(self, key: str) -> Dict[str, Any]:
        return self._data.setdefault("by_key", {}).setdefault(
            key,
            {
                "platform": key.split("/", 1)[0] if "/" in key else key,
                "model": key.split("/", 1)[1] if "/" in key else "auto",
                "paused": False,
                "probes": [],
                "consecutive_rate_limits": 0,
            },
        )

    def is_paused(self, key: str) -> bool:
        """True iff `key` has an unexpired pause. Reaps stale entries."""
        with self._lock:
            e = self._data.get("by_key", {}).get(key)
            if not e or not e.get("paused"):
                return False
            until = int(e.get("paused_until_epoch") or 0)
            if until <= int(time.time()):
                e["paused"] = False
                self._persist_locked()
                return False
            return True

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            e = self._data.get("by_key", {}).get(key)
            return dict(e) if e else None

    def pause(
        self,
        key: str,
        *,
        reason: str,
        duration_secs: Optional[int] = None,
        platform: Optional[str] = None,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        secs = max(60, int(duration_secs or self.DEFAULT_PAUSE_SECS))
        now = int(time.time())
        until = now + secs
        with self._lock:
            e = self._entry(key)
            if platform:
                e["platform"] = platform
            if model:
                e["model"] = model
            # Track consecutive lockouts so the prober can escalate
            # the cooldown if the same key keeps coming back locked.
            if e.get("paused") and (e.get("paused_until_epoch") or 0) > now:
                e["consecutive_rate_limits"] = (
                    int(e.get("consecutive_rate_limits") or 0) + 1
                )
            else:
                # First lockout in this streak — note the start time.
                e["consecutive_rate_limits"] = (
                    int(e.get("consecutive_rate_limits") or 0) + 1
                )
                e["first_rate_limit_at"] = _iso_now()
            e["paused"] = True
            e["paused_at"] = _iso_now()
            e["paused_until"] = _iso_at(until)
            e["paused_until_epoch"] = until
            e["reason"] = (reason or "")[:240]
            self._persist_locked()
            return dict(e)

    def unpause(self, key: str) -> bool:
        with self._lock:
            e = self._data.get("by_key", {}).get(key)
            if not e or not e.get("paused"):
                return False
            e["paused"] = False
            e["consecutive_rate_limits"] = 0
            self._persist_locked()
            return True

    def record_probe(
        self,
        key: str,
        conv: str,
        outcome: str,
    ) -> None:
        """Append a probe outcome to the per-key history. Trims to
        MAX_PROBE_HISTORY so the file doesn't grow unbounded."""
        with self._lock:
            e = self._entry(key)
            probes = e.setdefault("probes", [])
            probes.append({"at": _iso_now(), "conv": conv, "outcome": outcome})
            if len(probes) > self.MAX_PROBE_HISTORY:
                del probes[: len(probes) - self.MAX_PROBE_HISTORY]
            if outcome == "success":
                e["last_success_at"] = _iso_now()
                e["last_success_conv"] = conv
                e["paused"] = False
                e["consecutive_rate_limits"] = 0
            self._persist_locked()

    def record_success(self, key: str, conv: str) -> None:
        """Mark a non-probe success — resets the consecutive-fail
        counter so a transient rate-limit doesn't escalate forever."""
        with self._lock:
            e = self._entry(key)
            e["last_success_at"] = _iso_now()
            e["last_success_conv"] = conv
            e["consecutive_rate_limits"] = 0
            self._persist_locked()

    def keys_due_for_probe(self, *, max_age_secs: int = 60) -> List[str]:
        """Paused keys whose `paused_until` elapsed at least
        `max_age_secs` seconds ago AND haven't been probed in the
        last 60s. Used by QuotaProber to pick what to probe."""
        out: List[str] = []
        now = int(time.time())
        with self._lock:
            for key, e in (self._data.get("by_key") or {}).items():
                if not e.get("paused"):
                    continue
                until = int(e.get("paused_until_epoch") or 0)
                if now - until < max_age_secs:
                    continue
                # Throttle probe rate per key — never more than 1/min.
                last_probe = (e.get("probes") or [])[-1:]
                if last_probe:
                    try:
                        probe_ts = datetime.fromisoformat(
                            str(last_probe[0].get("at") or "").replace("Z", "+00:00")
                        ).timestamp()
                        if now - probe_ts < 60:
                            continue
                    except (ValueError, TypeError):
                        pass
                out.append(key)
        return out

    def view(self) -> Dict[str, Dict[str, Any]]:
        """Snapshot of all entries, with stale 'paused' flags reaped.
        Read-only for /health + /quota consumers."""
        now = int(time.time())
        out: Dict[str, Dict[str, Any]] = {}
        with self._lock:
            for key, e in (self._data.get("by_key") or {}).items():
                e2 = dict(e)
                if e2.get("paused") and int(e2.get("paused_until_epoch") or 0) <= now:
                    e2["paused"] = False
                out[key] = e2
        return out

    def paused_view(self) -> Dict[str, Dict[str, Any]]:
        """Subset of view() including only currently-paused keys."""
        return {k: v for k, v in self.view().items() if v.get("paused")}


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
