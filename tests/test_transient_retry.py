"""test_transient_retry.py — TR1 (py-1.21.1) transient API-error retry shield.

ChatRunner._reader_loop must, on a TRANSPORT-class API error (the
claude-code thinking-block 400 reconstruction bug, or a transient
429/5xx/overloaded), re-spawn the SAME turn instead of persisting the
error string as the turn's final (which poisons every future briefing
via _section_history) and waking the architect with a bogus task
failure. These tests lock in the classifier allowlist + the re-spawn /
budget / guard behaviour.
"""

from __future__ import annotations

import sys
import time as _time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import daemon as d  # type: ignore[import-not-found]  # noqa: E402


class _FakeHub:
    def broadcast(self, *_a: Any, **_k: Any) -> None:
        pass


class _DeadProc:
    """Stands in for a claude-code child that already exited non-zero."""

    def __init__(self, code: int = 1) -> None:
        self._code = code

    def wait(self, timeout: Any = None) -> int:
        return self._code

    def poll(self) -> int:
        return self._code


def _runner() -> Any:
    return d.ChatRunner(
        paths=d.Paths(Path("/tmp")),
        cluster=object.__new__(d.Cluster),  # unused by the methods under test
        hub=_FakeHub(),
        identity="i",
        conv="c",
        prompt="p",
        daemon=None,
    )


# ── classifier ─────────────────────────────────────────────────────────

THINKING_400 = (
    "API Error: 400 messages.1.content.12: `thinking` or `redacted_thinking` "
    "blocks in the latest assistant message cannot be modified. These blocks "
    "must remain as they were in the original response."
)


def test_classifier_matches_thinking_block_400() -> None:
    assert _runner()._is_transient_api_error(THINKING_400) is True


def test_classifier_matches_transient_upstream() -> None:
    r = _runner()
    for txt in (
        "API Error: 529 Overloaded",
        "API Error: 503 Service Unavailable",
        "API Error: 502 Bad Gateway",
        "API Error: 429 rate limit exceeded",
        "API Error: 500 Internal Server Error",
        "API Error: Request timed out",
    ):
        assert r._is_transient_api_error(txt) is True, txt


def test_classifier_rejects_request_shape_and_success() -> None:
    r = _runner()
    for txt in (
        # request-shape 4xx — a re-spawn rebuilds the same bad request
        "API Error: 400 Prompt is too long",
        "API Error: 401 authentication_error: invalid x-api-key",
        "API Error: 404 model: claude-bogus not found",
        # not an API error at all
        "Here is my summary of the API error handling in the codebase.",
        "",
    ):
        assert r._is_transient_api_error(txt) is False, txt


# ── re-spawn behaviour ───────────────────────────────────────────────────


def test_retry_respawns_and_resets_state(monkeypatch: Any) -> None:
    r = _runner()
    r.proc = _DeadProc(code=1)
    # Dirty per-turn state from the failed run — must be reset before respawn.
    r._cumulative_text = "partial junk"
    r.deltas_seen = 9
    r.tool_calls_count = 5
    first_stream = r.stream_id

    spawned: list[int] = []
    monkeypatch.setattr(r, "spawn", lambda: spawned.append(1))
    monkeypatch.setattr(_time, "sleep", lambda *_a: None)

    assert r._maybe_retry_transient(THINKING_400) is True
    assert spawned == [1]
    assert r._transient_attempt == 1
    assert r._cumulative_text == ""
    assert r.deltas_seen == 0
    assert r.tool_calls_count == 0
    assert r.stream_id != first_stream  # fresh bubble


def test_retry_budget_exhausts(monkeypatch: Any) -> None:
    r = _runner()
    r.proc = _DeadProc(code=1)
    monkeypatch.setattr(r, "spawn", lambda: None)
    monkeypatch.setattr(_time, "sleep", lambda *_a: None)

    # 2 retries allowed, then the shield gives up and lets finalize surface it.
    assert r._maybe_retry_transient(THINKING_400) is True
    assert r._maybe_retry_transient(THINKING_400) is True
    assert r._maybe_retry_transient(THINKING_400) is False
    assert r._transient_attempt == d.ChatRunner._MAX_TRANSIENT_RETRIES


def test_no_retry_on_clean_exit_or_cancel(monkeypatch: Any) -> None:
    monkeypatch.setattr(_time, "sleep", lambda *_a: None)
    # clean exit 0 → not a failure, never retry (even if text mentions an error)
    r = _runner()
    r.proc = _DeadProc(code=0)
    monkeypatch.setattr(
        r, "spawn", lambda: (_ for _ in ()).throw(AssertionError("spawned"))
    )
    assert r._maybe_retry_transient(THINKING_400) is False
    # cancelled turn → never retry
    r2 = _runner()
    r2.proc = _DeadProc(code=1)
    r2.cancelled = True
    monkeypatch.setattr(
        r2, "spawn", lambda: (_ for _ in ()).throw(AssertionError("spawned"))
    )
    assert r2._maybe_retry_transient(THINKING_400) is False


def test_no_retry_on_non_transient_error(monkeypatch: Any) -> None:
    r = _runner()
    r.proc = _DeadProc(code=1)
    monkeypatch.setattr(
        r, "spawn", lambda: (_ for _ in ()).throw(AssertionError("spawned"))
    )
    monkeypatch.setattr(_time, "sleep", lambda *_a: None)
    assert r._maybe_retry_transient("API Error: 400 Prompt is too long") is False
