"""Every route the cockpit calls must have a handler — no silent 404
regressions during the modularization.

The route list is captured from architect/src/lib/daemon-client.ts as of
2026-06-11. Add an entry here whenever a new client route ships."""

from __future__ import annotations

import pytest

from conftest import Daemon


# Routes the cockpit hits at boot / during normal use. Some are
# anonymous, some require auth — we send the token to all of them so we
# only test "handler exists", not the auth gate (auth is its own file).
ROUTES_GET = [
    "/health",
    "/state",
    "/info",
    "/agents",
    "/storage/usage",
    "/chat/snapshot",
    "/chat/convs",
    "/chat/archives",
    "/runs",
    "/quota",
    "/credentials",
    "/log",
    "/context",
]


@pytest.mark.parametrize("path", ROUTES_GET)
def test_handler_exists(daemon: Daemon, path: str) -> None:
    """We accept anything except 404. 200/204/400/401 — all proof of life."""
    r = daemon.get(path, headers=daemon.auth)
    assert r.status_code != 404, f"{path} has no handler"


def test_dispatch_route_exists(daemon: Daemon) -> None:
    """POST /chat/dispatch with a token + empty body returns a 4xx (bad
    request), not 404 — proving the route is wired even if the body is
    rejected. Full happy-path needs a fake claude subprocess — out of
    scope for DM0."""
    r = daemon.post("/chat/dispatch", headers=daemon.auth, json={})
    assert r.status_code != 404
