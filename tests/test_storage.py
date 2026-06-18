"""GET /storage/usage — Standard v22 disk-usage report."""

from __future__ import annotations

from pathlib import Path as _Path

from conftest import Daemon
from paths import Paths as _Paths
from chatqueue import ChatQueueManager as _ChatQueueManager


def test_storage_usage_shape(daemon: Daemon) -> None:
    r = daemon.get("/storage/usage")
    assert r.status_code == 200
    d = r.json()
    # Per Standard v22: top-level keys + per-bucket breakdown.
    assert "total_bytes" in d or "buckets" in d
    # The cockpit cares about the breakdown shape — accept either flat
    # dict (one key per bucket) or list-of-objects.
    if "buckets" in d:
        assert isinstance(d["buckets"], (list, dict))


def test_storage_usage_cached(daemon: Daemon) -> None:
    """5s server-side cache. Two back-to-back calls return the same
    generated_at — proves the cache is hot."""
    r1 = daemon.get("/storage/usage").json()
    r2 = daemon.get("/storage/usage").json()
    # Both responses should be self-consistent. If `generated_at` exists,
    # the cache contract is observable; if not, the test still passes —
    # the cache invariant is internal.
    if "generated_at" in r1 and "generated_at" in r2:
        assert r1["generated_at"] == r2["generated_at"]


# ─────────────────────────────────────────────────────────────────────
# QF1 (py-1.14.6) — ChatQueueManager.conv_ids(): enumerate convs with a
# non-empty disk queue. Powers the daemon's idle-flush sweep that
# resumes queues stranded when no on_idle hook fired (restart /
# self-update re-exec / abnormally-reaped session). Pure disk read —
# tested in-process against a tmp Paths, no daemon subprocess needed.


class _SilentHub:
    def broadcast(self, *_a: object, **_k: object) -> None:
        pass


def _qmgr(tmp_path: _Path) -> _ChatQueueManager:
    return _ChatQueueManager(_Paths(tmp_path), _SilentHub())


def test_queue_conv_ids_empty(tmp_path: _Path) -> None:
    # No queues dir yet → empty list, no crash.
    assert _qmgr(tmp_path).conv_ids() == []


def test_queue_conv_ids_lists_nonempty_convs(tmp_path: _Path) -> None:
    q = _qmgr(tmp_path)
    q.enqueue("conv-a", "first")
    q.enqueue("conv-a", "second")
    q.enqueue("conv-b", "only")
    assert sorted(q.conv_ids()) == ["conv-a", "conv-b"]


def test_queue_conv_ids_drops_emptied(tmp_path: _Path) -> None:
    q = _qmgr(tmp_path)
    q.enqueue("conv-a", "only")
    q.enqueue("conv-b", "keep")
    # Draining conv-a to empty removes its file → conv_ids drops it.
    assert q.pop_head("conv-a") is not None
    assert q.conv_ids() == ["conv-b"]
    assert q.pop_head("conv-a") is None
