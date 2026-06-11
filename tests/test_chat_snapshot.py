"""GET /chat/snapshot, /chat/convs, /chat/conv/<id>/meta, /chat/archives.

These are the endpoints behind the architect's rail. Anonymous reads
(conv ids are not secrets — they appear in timeline events served by
/state). The 2026-06-10 incident hung exactly here, so we hammer it
with the heavy-archive cluster too."""

from __future__ import annotations

import pytest

from conftest import Daemon


def test_snapshot_shape(daemon: Daemon) -> None:
    r = daemon.get("/chat/snapshot")
    assert r.status_code == 200
    d = r.json()
    for k in ("convs", "paused_agent_types", "quota", "version", "generated_at"):
        assert k in d
    assert isinstance(d["convs"], list)


def test_snapshot_archived_flag(daemon: Daemon) -> None:
    """conv-b is in archives.json — daemon must mark it archived=True
    even though conv_meta.json doesn't carry the flag (the archive
    registry is the authority)."""
    convs = {c["conv"]: c for c in daemon.get("/chat/snapshot").json()["convs"]}
    assert "conv-a" in convs and convs["conv-a"]["archived"] is False
    assert "conv-b" in convs and convs["conv-b"]["archived"] is True


def test_snapshot_msg_count(daemon: Daemon) -> None:
    """msg_count is computed from timeline files. conv-a has 2 chat events
    (user + assistant.final), conv-b has 1."""
    convs = {c["conv"]: c for c in daemon.get("/chat/snapshot").json()["convs"]}
    assert convs["conv-a"]["msg_count"] == 2
    assert convs["conv-b"]["msg_count"] == 1


def test_convs_endpoint(daemon: Daemon) -> None:
    r = daemon.get("/chat/convs")
    assert r.status_code == 200
    d = r.json()
    assert "convs" in d and "generated_at" in d


def test_archives_endpoint(daemon: Daemon) -> None:
    r = daemon.get("/chat/archives")
    assert r.status_code == 200
    d = r.json()
    assert "archived" in d
    # Daemon serialises archives as a list of {conv, archived_at, by}.
    archived_ids = {entry["conv"] for entry in d["archived"]}
    assert "conv-b" in archived_ids


def test_conv_meta(daemon: Daemon) -> None:
    r = daemon.get("/chat/conv/conv-a/meta")
    assert r.status_code == 200
    d = r.json()
    assert d["conv"] == "conv-a"
    assert d["archived"] is False


@pytest.mark.cluster("heavy_archive")
def test_snapshot_under_heavy_archive(daemon: Daemon) -> None:
    """100 archived convs + 5 live. Must complete in well under a second
    — the bug we are guarding against is the 2026-06-10 indefinite hang."""
    import time

    start = time.time()
    r = daemon.get("/chat/snapshot", timeout=10.0)
    elapsed = time.time() - start
    assert r.status_code == 200
    assert elapsed < 5.0, (
        f"snapshot took {elapsed:.2f}s under heavy archive — regression"
    )
    convs = r.json()["convs"]
    archived = sum(1 for c in convs if c["archived"])
    live = sum(1 for c in convs if not c["archived"])
    assert archived == 100
    assert live == 5
