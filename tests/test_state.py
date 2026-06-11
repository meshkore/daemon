"""GET /state, /state/<subset>, /reload — read-only snapshot endpoints."""

from __future__ import annotations

from conftest import Daemon


def test_state_full(daemon: Daemon) -> None:
    r = daemon.get("/state")
    assert r.status_code == 200
    d = r.json()
    # Top-level keys the cockpit reads at boot. The architect's
    # daemon-client reads these directly.
    for k in ("cluster", "generated_at"):
        assert k in d, f"/state missing key: {k}"
    # Cluster identity matches the fixture.
    assert d["cluster"]["id"] == "populated"


def test_state_subset(daemon: Daemon) -> None:
    """State subsets keep the cockpit cheap — fetch only what it needs."""
    r = daemon.get("/state/initiatives")
    assert r.status_code == 200
    # Either the subset returns a list directly or wraps it — accept both
    # for parity (the daemon's done both historically).
    d = r.json()
    assert isinstance(d, (list, dict))


def test_state_subset_unknown_is_404(daemon: Daemon) -> None:
    r = daemon.get("/state/this-key-does-not-exist")
    assert r.status_code == 404


def test_reload_requires_auth(daemon: Daemon) -> None:
    r = daemon.get("/reload")
    assert r.status_code == 401


def test_reload_with_token(daemon: Daemon) -> None:
    r = daemon.get("/reload", headers=daemon.auth)
    assert r.status_code == 200
    assert r.json().get("ok") is True
