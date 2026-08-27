"""Auth contract: anonymous reads vs token-gated routes.

If a route's auth gate flips during the modularization, the cockpit
breaks silently (401 instead of data, or — worse — secrets leak to
anonymous clients). This file pins the gate per route.

Hand-curated spot-check. The EXHAUSTIVE version — every route the endpoint
warranty knows about, probed live, plus the structural fail-closed guards for
both verb tables — is `test_auth_matrix.py` (DAH1/DAH2)."""

from __future__ import annotations

import pytest

from conftest import Daemon


ANONYMOUS_GET = [
    "/health",
    "/state",
    "/info",
    "/agents",
    "/auth/challenge",
    "/storage/usage",
    "/chat/archives",
]

TOKEN_REQUIRED_GET = [
    # DAH2(b) — these three served conversation CONTENT anonymously until
    # py-1.34.0. `/chat/conv/<id>/messages` returns message BODIES; the
    # snapshot/convs pair reaches the same content by another door.
    "/chat/snapshot",
    "/chat/convs",
    "/chat/conv/conv-a/messages",
    "/reload",
    "/quota",
    "/runs",
    "/credentials",
    "/debug/tail",
    "/log",
    "/tasks/initiatives/alpha.md",
    "/modules/daemon/tasks/T1.md",
]


@pytest.mark.parametrize("path", ANONYMOUS_GET)
def test_anonymous_routes_open(daemon: Daemon, path: str) -> None:
    """200 (or 204) — never 401. 404 is acceptable if the route requires
    a populated entity that doesn't exist; this fixture has every entity
    the path needs."""
    r = daemon.get(path)
    assert r.status_code != 401, f"{path} unexpectedly gated"


@pytest.mark.parametrize("path", TOKEN_REQUIRED_GET)
def test_token_routes_reject_anonymous(daemon: Daemon, path: str) -> None:
    r = daemon.get(path)
    assert r.status_code == 401, f"{path} should require auth, got {r.status_code}"


@pytest.mark.parametrize("path", TOKEN_REQUIRED_GET)
def test_token_routes_accept_token(daemon: Daemon, path: str) -> None:
    r = daemon.get(path, headers=daemon.auth)
    assert r.status_code != 401, f"{path} rejected a valid token"
