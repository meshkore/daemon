"""DAH1 — an external ask must resolve to ITS OWN turn's final.

The bug (initiative `daemon-audit-hardening`): `_teamext_latest_final` matched
on `(conv, ts >= started_at)` alone. That is wrong precisely in the shape the
external gateway hits most often — an ask that arrives while another turn is
already running on the conv, which is the normal case for a `kind: singleton`
member like `architect-master` (the one the machine remote-control token talks
to). `chat_dispatch` returns `queued: True`; the turn that was ALREADY running
then finishes with a ts newer than our `started_at`; the old predicate handed
that turn's answer to the external caller as the reply to its own question.

Every `chat.assistant.final` emitter stamps `stream_id`, so turn identity was
already on the wire — the watcher just never used it.

Pure-logic tests: `_teamext_latest_final` only needs `self.paths` to point at a
tree with a timeline dir, so a tiny stub stands in for the Daemon.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from teamext import TeamExtMixin  # type: ignore[import-not-found]  # noqa: E402


class _Paths:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.timeline_dir = root / "timeline"


class _Stub(TeamExtMixin):
    def __init__(self, root: Path) -> None:
        self.paths = _Paths(root)


def _write_timeline(root: Path, events) -> None:
    d = root / "timeline"
    d.mkdir(parents=True, exist_ok=True)
    with open(d / "2026-08-26.jsonl", "w", encoding="utf-8") as fh:
        for ev in events:
            fh.write(json.dumps(ev) + "\n")


FINAL = "chat.assistant.final"
CONV = "architect-master"
T0 = "2026-08-26T10:00:00.000Z"  # our ask lands here


@pytest.fixture()
def tree(tmp_path: Path) -> Path:
    _write_timeline(
        tmp_path,
        [
            # The operator's turn — already running when we asked, finishes
            # AFTER our started_at. This is the impostor.
            {
                "type": FINAL,
                "conv": CONV,
                "stream_id": "stream-operator",
                "ts": "2026-08-26T10:00:30.000Z",
                "text": "answer to the OPERATOR's question",
            },
            # Our own turn, spawned from the queue once the first one ended.
            {
                "type": FINAL,
                "conv": CONV,
                "stream_id": "stream-ours",
                "ts": "2026-08-26T10:02:00.000Z",
                "text": "answer to the EXTERNAL caller's question",
            },
        ],
    )
    return tmp_path


def test_exact_stream_id_wins(tree: Path) -> None:
    """Given our own stream_id, only our turn's final is eligible."""
    got = _Stub(tree)._teamext_latest_final(CONV, T0, stream_id="stream-ours")
    assert got == "answer to the EXTERNAL caller's question"


def test_neighbouring_turn_is_never_our_answer(tree: Path) -> None:
    """The regression itself: while queued we have no stream_id of our own,
    only the id of the turn we queued BEHIND. That turn's final must not be
    reported as our result — the caller would receive someone else's reply."""
    got = _Stub(tree)._teamext_latest_final(CONV, T0, exclude_stream="stream-operator")
    assert got == "answer to the EXTERNAL caller's question"


def test_excluded_stream_alone_yields_nothing(tree: Path) -> None:
    """If the ONLY final present belongs to the turn we queued behind, the
    watcher must keep waiting rather than resolve with the wrong text."""
    _write_timeline(
        tree,
        [
            {
                "type": FINAL,
                "conv": CONV,
                "stream_id": "stream-operator",
                "ts": "2026-08-26T10:00:30.000Z",
                "text": "answer to the OPERATOR's question",
            }
        ],
    )
    assert (
        _Stub(tree)._teamext_latest_final(CONV, T0, exclude_stream="stream-operator")
        is None
    )


def test_legacy_events_without_stream_id_still_match(tree: Path) -> None:
    """Timelines written before py-1.13 carry no `stream_id`. Those events stay
    eligible under the ts predicate — an old cluster must not regress into
    polling until the 1 h watcher timeout."""
    _write_timeline(
        tree,
        [
            {
                "type": FINAL,
                "conv": CONV,
                "ts": "2026-08-26T10:01:00.000Z",
                "text": "pre-1.13 final, no stream_id",
            }
        ],
    )
    got = _Stub(tree)._teamext_latest_final(CONV, T0, stream_id="stream-ours")
    assert got == "pre-1.13 final, no stream_id"


def test_finals_before_the_ask_are_ignored(tree: Path) -> None:
    """The ts floor still applies — a final from an earlier turn on the same
    stream must not be picked up."""
    _write_timeline(
        tree,
        [
            {
                "type": FINAL,
                "conv": CONV,
                "stream_id": "stream-ours",
                "ts": "2026-08-26T09:59:00.000Z",
                "text": "yesterday's news",
            }
        ],
    )
    assert _Stub(tree)._teamext_latest_final(CONV, T0, stream_id="stream-ours") is None


def test_other_convs_never_leak(tree: Path) -> None:
    got = _Stub(tree)._teamext_latest_final(
        "some-other-conv", T0, exclude_stream="stream-operator"
    )
    assert got is None
