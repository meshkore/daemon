"""CORS origin allowlist (py-1.27.4). The open read routes (/state, /health,
/agents …) must NOT be cross-origin-readable by an arbitrary website — the
daemon reflects an Origin only when it's a MeshKore cockpit surface
(*.meshkore.com / *.pages.dev) or loopback; everything else gets no
Access-Control-Allow-Origin header so the browser blocks the read."""

from __future__ import annotations

from conftest import Daemon


def _aco(daemon: Daemon, origin: str | None) -> str | None:
    headers = {"Origin": origin} if origin else {}
    r = daemon.get("/state", headers=headers)
    assert r.status_code == 200
    return r.headers.get("access-control-allow-origin")


def test_cockpit_origin_reflected(daemon: Daemon) -> None:
    assert (
        _aco(daemon, "https://architect.meshkore.com")
        == "https://architect.meshkore.com"
    )


def test_meshkore_subdomain_and_apex_allowed(daemon: Daemon) -> None:
    assert _aco(daemon, "https://meshkore.com") == "https://meshkore.com"
    assert _aco(daemon, "https://abc123.meshkore-portal.pages.dev") == (
        "https://abc123.meshkore-portal.pages.dev"
    )


def test_loopback_allowed(daemon: Daemon) -> None:
    assert _aco(daemon, "http://localhost:4173") == "http://localhost:4173"


def test_evil_origin_not_reflected(daemon: Daemon) -> None:
    # The whole point: a random site cannot read /state cross-origin.
    assert _aco(daemon, "https://evil.example.com") is None
    # A look-alike that merely CONTAINS the brand must not pass either.
    assert _aco(daemon, "https://meshkore.com.evil.example") is None


def test_no_origin_no_acao(daemon: Daemon) -> None:
    # Non-browser / same-origin callers send no Origin → no ACAO header,
    # and the route still works (local CLI tools unaffected).
    assert _aco(daemon, None) is None


def test_version_header_always_present(daemon: Daemon) -> None:
    # The wire-version contract header is independent of CORS.
    r = daemon.get("/health")
    assert r.headers.get("x-meshkore-daemon-version")
