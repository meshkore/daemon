"""_teamext_reap_stale — self-heal requests orphaned by a daemon restart
mid-turn (2026-07-08 incident: a port-restart killed an in-flight
external-ask watcher; the request sat `running` forever, silently eating
an EXT_ASK_CONCURRENT_CAP slot until the 24h GC finally erased it).

Pure-logic tests — no daemon boot, no HTTP. `TeamExtMixin` needs no other
daemon state for this method (`_teamext_broadcast` no-ops without a `hub`
attribute).
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from teamext import TeamExtMixin, _WATCH_MAX_SECS  # type: ignore[import-not-found]  # noqa: E402


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def test_stale_running_request_flipped_to_error():
    old_start = _iso(
        datetime.now(timezone.utc) - timedelta(seconds=_WATCH_MAX_SECS + 60)
    )
    data = {
        "req-old": {
            "request_id": "req-old",
            "member": "consultant",
            "conv": "ext-consultant-x",
            "status": "running",
            "started_at": old_start,
        }
    }
    out = TeamExtMixin()._teamext_reap_stale(data)
    assert out["req-old"]["status"] == "error"
    assert "orphaned" in out["req-old"]["error"]
    assert out["req-old"]["finished_at"]


def test_fresh_running_request_left_alone():
    fresh_start = _iso(datetime.now(timezone.utc) - timedelta(seconds=5))
    data = {
        "req-new": {
            "request_id": "req-new",
            "member": "consultant",
            "conv": "ext-consultant-y",
            "status": "running",
            "started_at": fresh_start,
        }
    }
    out = TeamExtMixin()._teamext_reap_stale(data)
    assert out["req-new"]["status"] == "running"
    assert "error" not in out["req-new"]


def test_terminal_states_never_touched():
    old_start = _iso(
        datetime.now(timezone.utc) - timedelta(seconds=_WATCH_MAX_SECS + 60)
    )
    data = {
        "req-done": {
            "request_id": "req-done",
            "member": "consultant",
            "status": "done",
            "started_at": old_start,
            "result_text": "already finished",
        }
    }
    out = TeamExtMixin()._teamext_reap_stale(data)
    assert out["req-done"]["status"] == "done"
    assert out["req-done"]["result_text"] == "already finished"


def test_cap_raised_from_2():
    from teamext import EXT_ASK_CONCURRENT_CAP  # noqa: E402

    assert EXT_ASK_CONCURRENT_CAP > 2
