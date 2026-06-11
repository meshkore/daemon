"""SRL5 — state-recovery-loop snapshot tests.

Verifies the SRL2 contract: live conv entries in `/chat/snapshot`
carry `current_turn` (started_at + stream_id + partial_text +
counters) and `queue` (pending messages). After SRL5, a future
fake-claude harness can extend these into full end-to-end smoke;
for now we exercise the in-process surface."""

from __future__ import annotations

import sys
from pathlib import Path

# Make the daemon package importable from the source tree.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import daemon as d  # type: ignore[import-not-found]  # noqa: E402

from conftest import Daemon  # noqa: E402


# ── In-process — ChatSessions.turn_snapshot() shape ─────────────────


def test_turn_snapshot_returns_none_for_idle_conv() -> None:
    """No live session → None. Cheap branch; safe to call on every
    chat_convs() build per conv id."""
    cs = d.ChatSessions()
    assert cs.turn_snapshot("never-started") is None


def test_turn_snapshot_returns_current_turn_for_live_conv() -> None:
    """Mock a ChatSessions slot with a fake runner exposing the four
    SRL1 attrs. turn_snapshot should expose them via the
    current_turn dict and an empty queue."""
    cs = d.ChatSessions()

    class FakeRunner:
        started_at = "2026-06-11T22:46:12.341Z"
        stream_id = "s_test_001"
        _cumulative_text = "Voy a empezar revisando el roadmap…"
        tool_calls_count = 2
        deltas_seen = 47

    # Mirror ChatSessions.start's slot shape.
    cs._s["live-conv"] = {
        "runner": FakeRunner(),
        "pending": [],
        "cancelled": False,
    }
    snap = cs.turn_snapshot("live-conv")
    assert snap is not None
    ct = snap["current_turn"]
    assert ct["started_at"] == "2026-06-11T22:46:12.341Z"
    assert ct["stream_id"] == "s_test_001"
    assert ct["partial_text"] == "Voy a empezar revisando el roadmap…"
    assert ct["tool_calls_count"] == 2
    assert ct["deltas_seen"] == 47
    assert snap["queue"] == []


def test_turn_snapshot_caps_partial_text_at_16kb() -> None:
    """Long-running turns shouldn't blow up the snapshot payload.
    The cap is 16 000 chars; anything past is sliced off."""
    cs = d.ChatSessions()

    class FakeRunner:
        started_at = "2026-06-11T22:46:12.341Z"
        stream_id = "s_test_002"
        _cumulative_text = "x" * 50000
        tool_calls_count = 0
        deltas_seen = 0

    cs._s["live-big"] = {
        "runner": FakeRunner(),
        "pending": [],
        "cancelled": False,
    }
    snap = cs.turn_snapshot("live-big")
    assert snap is not None
    assert len(snap["current_turn"]["partial_text"]) == 16000


def test_turn_snapshot_exposes_queue() -> None:
    """Pending messages flow into the snapshot as `queue` entries with
    a stable shape (id + text + queued_at)."""
    cs = d.ChatSessions()

    class FakeRunner:
        started_at = "2026-06-11T22:46:12.341Z"
        stream_id = "s_test_003"
        _cumulative_text = "running"
        tool_calls_count = 0
        deltas_seen = 1

    cs._s["live-queue"] = {
        "runner": FakeRunner(),
        "pending": ["luego mira el bug del scroll", "y al final commit"],
        "cancelled": False,
        "queued_at": "2026-06-11T22:47:00.000Z",
    }
    snap = cs.turn_snapshot("live-queue")
    assert snap is not None
    q = snap["queue"]
    assert len(q) == 2
    assert q[0]["text"] == "luego mira el bug del scroll"
    assert q[0]["id"] == "q_0"
    assert q[0]["queued_at"] == "2026-06-11T22:47:00.000Z"
    assert q[1]["text"] == "y al final commit"
    assert q[1]["id"] == "q_1"


def test_turn_snapshot_handles_missing_attrs_gracefully() -> None:
    """If a runner is missing some attrs (older daemon hot-reloaded,
    test mock, etc.), turn_snapshot returns sane defaults instead of
    raising AttributeError. Belt-and-braces — SRL1 ensured the attrs
    always exist, but this guards against regression."""
    cs = d.ChatSessions()

    class BarebonesRunner:
        # Only the bare minimum from older versions.
        stream_id = "s_bare"
        _cumulative_text = "hello"

    cs._s["bare-conv"] = {
        "runner": BarebonesRunner(),
        "pending": [],
        "cancelled": False,
    }
    snap = cs.turn_snapshot("bare-conv")
    assert snap is not None
    assert snap["current_turn"]["stream_id"] == "s_bare"
    assert snap["current_turn"]["started_at"] is None
    assert snap["current_turn"]["tool_calls_count"] == 0
    assert snap["current_turn"]["deltas_seen"] == 0


# ── HTTP — the snapshot endpoint advertises the new feature ─────────


def test_health_advertises_turn_state_feature(daemon: Daemon) -> None:
    """py-1.13.1+ ships `daemon.snapshot.turn_state.v1`. Cockpit gates
    its rehydration paths on this flag."""
    r = daemon.get("/health")
    assert r.status_code == 200
    assert "daemon.snapshot.turn_state.v1" in r.json()["features"]


def test_snapshot_no_current_turn_for_idle_convs(daemon: Daemon) -> None:
    """Idle (non-live) convs in /chat/snapshot do NOT carry
    current_turn or queue. Keeps the payload small for the common
    case (most convs are idle)."""
    r = daemon.get("/chat/snapshot")
    assert r.status_code == 200
    for c in r.json()["convs"]:
        if not c.get("live"):
            assert "current_turn" not in c, (
                f"idle conv {c.get('conv')} has current_turn"
            )
            assert "queue" not in c, f"idle conv {c.get('conv')} has queue"
