"""GET /health — version, identity, feature list. Anonymous."""

from __future__ import annotations

import pytest

from conftest import Daemon


def test_health_shape(daemon: Daemon) -> None:
    r = daemon.get("/health")
    assert r.status_code == 200
    d = r.json()
    assert d["ok"] is True
    assert d["version"].startswith("py-")
    assert d["cluster_id"] == "populated"
    assert d["implementation"] == "python"
    assert d["mode"] == "server"
    assert isinstance(d["features"], list)
    # Sentinel features that EVERY daemon must report — if one of these
    # ever drops, the cockpit's feature-gap detector lights up.
    for must_have in ("health", "state", "chat", "chat.snapshot.v1"):
        assert must_have in d["features"], f"missing feature: {must_have}"


def test_health_version_header(daemon: Daemon) -> None:
    """py-1.12.x feature `version_header` — every response carries the
    daemon version in `X-MeshKore-Daemon-Version`. The cockpit reads
    this to detect a version drift mid-session."""
    r = daemon.get("/health")
    assert "X-MeshKore-Daemon-Version" in r.headers or "x-meshkore-daemon-version" in {
        k.lower() for k in r.headers
    }


@pytest.mark.cluster("empty")
def test_health_on_empty_cluster(daemon: Daemon) -> None:
    """Bootstrap path: an empty cluster still answers /health."""
    r = daemon.get("/health")
    assert r.status_code == 200
    assert r.json()["cluster_id"] == "empty"
