"""DAH1 — oversized request bodies must be refused AND drained.

`_guard` drains the request body up-front so an early-reject handler (401, 400,
503) never leaves it unread: on an HTTP/1.1 keep-alive socket an unread body
makes the NEXT request parse the leftover bytes as its request line, producing
"400 Bad request syntax" that the browser surfaces as a CORS failure.

The bug this file pins: the drain was written as

    self._raw_body = self.rfile.read(_clen) if 0 < _clen <= MAX_BODY_BYTES else b""

so an OVERSIZED body took the `else` branch — the one path that skips the read
entirely and leaves the bytes on the socket, i.e. exactly the desync the drain
exists to prevent. It also let the handler run against a silently empty body
instead of saying no.
"""

from __future__ import annotations

from conftest import Daemon

# Must match constants.MAX_BODY_BYTES.
MAX_BODY_BYTES = 4 * 1024 * 1024


def test_oversized_body_gets_413(daemon: Daemon) -> None:
    payload = b"x" * (MAX_BODY_BYTES + 1024)
    r = daemon.client.post(
        daemon.base + "/messages",
        content=payload,
        headers={**daemon.auth, "Content-Type": "application/json"},
    )
    assert r.status_code == 413
    assert r.json()["max_bytes"] == MAX_BODY_BYTES


def test_connection_still_usable_after_an_oversized_body(daemon: Daemon) -> None:
    """The regression itself. `daemon.client` is a keep-alive httpx client, so
    the follow-up request rides the SAME connection — if the refused body were
    still sitting in the socket buffer, this would come back as a bad-request
    parse error rather than a healthy /health."""
    daemon.client.post(
        daemon.base + "/messages",
        content=b"y" * (MAX_BODY_BYTES + 1024),
        headers={**daemon.auth, "Content-Type": "application/json"},
    )
    for _ in range(3):
        follow_up = daemon.get("/health")
        assert follow_up.status_code == 200
        assert follow_up.json()["ok"] is True


def test_body_at_the_limit_is_still_accepted(daemon: Daemon) -> None:
    """The ceiling is inclusive — exactly MAX_BODY_BYTES must NOT 413. The
    payload is nonsense JSON, so any non-413 answer proves the body was read
    and handed to the route."""
    r = daemon.client.post(
        daemon.base + "/messages",
        content=b" " * MAX_BODY_BYTES,
        headers={**daemon.auth, "Content-Type": "application/json"},
    )
    assert r.status_code != 413
