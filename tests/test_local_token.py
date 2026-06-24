"""GET /auth/local-token (py-1.27.6) — local auto-unlock. Hands the bearer
token to the SAME-ORIGIN cockpit on this machine so a local project never
prompts. Must be gated to EXACT cockpit/loopback origins only, and opt-out-able."""

from __future__ import annotations

from conftest import Daemon, TOKEN


def _get(daemon: Daemon, origin: str | None):
    headers = {"Origin": origin} if origin else {}
    return daemon.get("/auth/local-token", headers=headers)


def test_cockpit_origin_gets_token(daemon: Daemon) -> None:
    r = _get(daemon, "https://architect.meshkore.com")
    assert r.status_code == 200
    assert r.json().get("token") == TOKEN


def test_loopback_origins_get_token(daemon: Daemon) -> None:
    assert _get(daemon, "http://localhost:4173").json().get("token") == TOKEN
    assert _get(daemon, "http://127.0.0.1:5599").json().get("token") == TOKEN


def test_foreign_origin_refused(daemon: Daemon) -> None:
    # A malicious website must NOT obtain the token.
    r = _get(daemon, "https://evil.example.com")
    assert r.status_code == 403
    assert "token" not in r.json()
    # A pages.dev preview is in the general CORS allowlist but NOT trusted
    # with the shell-exec token.
    assert _get(daemon, "https://x.meshkore-portal.pages.dev").status_code == 403
    # Brand look-alike must not pass.
    assert (
        _get(daemon, "https://architect.meshkore.com.evil.example").status_code == 403
    )
