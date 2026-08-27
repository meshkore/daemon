"""DAH6 — the per-member concurrent cap must hold under concurrent asks.

`team_ask_http` used to check the cap under `_teamext_lock`, RELEASE it,
dispatch, and only then insert the request under a second acquisition. Two
asks arriving together at `active == cap - 1` both counted cap-1, both passed
the check, and both inserted: the member ran cap+1 turns.

That was written off in the 2026-08-26 audit as harmless — cap 6, one local
operator. It stops being harmless the moment the external gateway is used for
what it exists for: a third-party endpoint where concurrent asks are the
normal shape, and where this cap is the ONLY rate limit in TEG-2 (loopback is
the perimeter; there is no other throttle).

The fix reserves the slot inside the same lock that counts it. These tests
drive `team_ask_http` from many threads at once, with a `chat_dispatch` that
parks on a barrier so every caller is genuinely in flight together — the
interleaving the old code needed, forced rather than hoped for.
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path
from typing import Any, Dict, Tuple

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from teamext import (  # type: ignore[import-not-found]  # noqa: E402
    EXT_ASK_CONCURRENT_CAP,
    TeamExtMixin,
)

MEMBER = "architect-master"  # the remote-token path: no member-token store needed


class _Paths:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.runtime = root / ".runtime"
        self.runtime.mkdir(parents=True, exist_ok=True)


class _TeamStore:
    def team_get(self, mid: str) -> Dict[str, Any]:
        return {"id": mid, "frontmatter": {"exposure": "external", "kind": "profile"}}


class _Daemon(TeamExtMixin):
    """The smallest daemon `team_ask_http` will accept. Driven through the
    CPL-2 REMOTE path (`remote=True`, member `architect-master`), which skips
    the exposure check and the member-token match — leaving the cap as the
    only gate under test, which is the point."""

    def __init__(self, root: Path, gate: threading.Event) -> None:
        self.paths = _Paths(root)
        self.team_store = _TeamStore()
        self.gate = gate
        self.dispatched = 0
        self._dispatch_lock = threading.Lock()

    # every ask parks here until the test opens the gate, so all callers sit
    # between "cap checked" and "request recorded" simultaneously
    def chat_dispatch(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        self.gate.wait(timeout=10)
        with self._dispatch_lock:
            self.dispatched += 1
        return 202, {"queued": False, "stream_id": f"stream-{self.dispatched}"}

    def _current_project_id(self):
        return None

    def _teamext_watch(self, *a: Any, **kw: Any) -> None:
        return None


@pytest.fixture()
def daemon(tmp_path: Path):
    gate = threading.Event()
    return _Daemon(tmp_path, gate)


def _ask_many(daemon: _Daemon, n: int) -> list[int]:
    codes: list[int] = [0] * n
    barrier = threading.Barrier(n)

    def one(i: int) -> None:
        barrier.wait(timeout=10)  # all threads enter team_ask_http together
        code, _ = daemon.team_ask_http(
            MEMBER, bearer="tok", body={"text": f"ask {i}"}, remote=True
        )
        codes[i] = code

    threads = [threading.Thread(target=one, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    # let the racers pile up inside chat_dispatch, then release them
    threading.Timer(0.25, daemon.gate.set).start()
    for t in threads:
        t.join(timeout=15)
    return codes


def test_cap_holds_when_every_ask_arrives_at_once(daemon: _Daemon) -> None:
    """The regression. `cap + 4` simultaneous asks must yield EXACTLY `cap`
    acceptances — never cap+1, which is what the check-then-insert window
    produced."""
    n = EXT_ASK_CONCURRENT_CAP + 4
    codes = _ask_many(daemon, n)
    assert codes.count(202) == EXT_ASK_CONCURRENT_CAP
    assert codes.count(429) == n - EXT_ASK_CONCURRENT_CAP


def test_accepted_requests_are_all_recorded(daemon: _Daemon) -> None:
    """Every 202 leaves exactly one live entry — the reservation is filled in,
    not duplicated alongside the real record."""
    _ask_many(daemon, EXT_ASK_CONCURRENT_CAP + 2)
    data = daemon._teamext_load()
    live = [e for e in data.values() if e.get("status") in ("queued", "running")]
    assert len(live) == EXT_ASK_CONCURRENT_CAP
    assert all(e.get("conv") for e in live), "a filled-in entry carries its conv"
    assert len({e["request_id"] for e in live}) == EXT_ASK_CONCURRENT_CAP


def test_a_failed_dispatch_returns_its_slot(tmp_path: Path) -> None:
    """A dispatch that never starts must not cost the member a slot for 24 h.
    The reservation is released on the failure path."""

    class _Failing(_Daemon):
        def chat_dispatch(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
            return 503, {"error": "no runner"}

    d = _Failing(tmp_path, threading.Event())
    for _ in range(EXT_ASK_CONCURRENT_CAP + 3):
        code, _ = d.team_ask_http(MEMBER, bearer="tok", body={"text": "x"}, remote=True)
        assert code == 503, "every attempt must reach dispatch, never 429"
    assert d._teamext_load() == {}, "no reservation survives a failed dispatch"
