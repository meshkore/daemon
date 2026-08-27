"""DAH1 — the CORS allowlist must name OUR origins, not a hosting domain.

`_allowed_origin` decides which `Origin` gets reflected in
`Access-Control-Allow-Origin`. It used to end with:

    or host.endswith(".pages.dev")   # Cloudflare Pages previews

which is not an allowlist — `*.pages.dev` is a public hosting domain where any
third party can deploy a page in a minute. Combined with the GET routes that
carry no auth (`/state`, `/chat/snapshot`, `/chat/conv/<id>/messages`), that let
ANY pages.dev site the operator happened to visit read this project's state and
chat transcripts cross-origin. The loopback bind is not a perimeter against
this: the operator's browser is itself on loopback.

The project already knew the shape of the problem — `/auth/local-token` was
deliberately gated to exact cockpit origins, "NOT the broad CORS allowlist (no
*.pages.dev)". This narrows the data routes to the same standard.

Narrowed to our own Pages project (`meshkore-portal`, per
`architect/wrangler.toml` + the `deploy:preview`/`deploy:prod` scripts), so every
legitimate cockpit preview keeps working: the production alias
`meshkore-portal.pages.dev` and the branch/deployment previews
`<branch-or-hash>.meshkore-portal.pages.dev`.
"""

from __future__ import annotations

import pytest

from conftest import Daemon

ALLOWED = [
    "https://meshkore.com",
    "https://architect.meshkore.com",
    "https://www.meshkore.com",
    "https://meshkore-portal.pages.dev",  # our Pages production alias
    "https://preview-solid.meshkore-portal.pages.dev",  # our branch preview
    "https://a1b2c3d4.meshkore-portal.pages.dev",  # our deployment preview
    "http://localhost:3000",
    "http://127.0.0.1:5173",
]

REFUSED = [
    # The regression: someone else's Cloudflare Pages site.
    "https://totally-unrelated.pages.dev",
    "https://evil.pages.dev",
    # A near-miss that must not pass a naive substring/suffix check.
    "https://meshkore-portal.pages.dev.evil.com",
    "https://notmeshkore.com",
    "https://meshkore.com.evil.com",
    "https://evil.com",
]


@pytest.mark.parametrize("origin", ALLOWED)
def test_cockpit_origins_are_reflected(daemon: Daemon, origin: str) -> None:
    r = daemon.get("/health", headers={"Origin": origin})
    assert r.headers.get("access-control-allow-origin") == origin


@pytest.mark.parametrize("origin", REFUSED)
def test_third_party_origins_get_no_allow_origin(daemon: Daemon, origin: str) -> None:
    """No `Access-Control-Allow-Origin` header at all → the browser blocks the
    cross-origin read. The daemon still answers (a non-browser client is
    unaffected); it just refuses to authorise the page to see the body."""
    r = daemon.get("/health", headers={"Origin": origin})
    assert "access-control-allow-origin" not in r.headers, (
        f"{origin} was authorised to read this daemon cross-origin"
    )


def test_preflight_follows_the_same_rule(daemon: Daemon) -> None:
    """OPTIONS shares `_cors()`, so the preflight must refuse the same origins
    the actual request would — otherwise the browser is told the call is
    allowed and only discovers otherwise on the real request."""
    ok = daemon.client.options(
        daemon.base + "/state", headers={"Origin": "https://architect.meshkore.com"}
    )
    assert ok.headers.get("access-control-allow-origin") == (
        "https://architect.meshkore.com"
    )
    bad = daemon.client.options(
        daemon.base + "/state", headers={"Origin": "https://evil.pages.dev"}
    )
    assert "access-control-allow-origin" not in bad.headers


def test_no_origin_header_is_untouched(daemon: Daemon) -> None:
    """Non-browser callers (curl, the test client, a local CLI tool) send no
    Origin and must keep working exactly as before."""
    r = daemon.get("/health")
    assert r.status_code == 200
    assert "access-control-allow-origin" not in r.headers


def test_version_header_is_always_present(daemon: Daemon) -> None:
    """The wire-version contract rides on `_cors()` too — it must NOT be
    collateral damage of a refused origin, or a stale-daemon check would
    silently stop working for exactly the callers it matters to."""
    r = daemon.get("/health", headers={"Origin": "https://evil.pages.dev"})
    assert r.headers.get("x-meshkore-daemon-version", "").startswith("py-")
