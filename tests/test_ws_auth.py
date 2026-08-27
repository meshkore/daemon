"""DAH4 — `GET /events` must not hand the event bus to an anonymous caller.

The hole: `route_get` resolved the WebSocket upgrade before the HTTP auth
gate, on the strength of a comment saying `_handle_ws` authenticated itself.
It did not. Any caller that could open a socket got a 101 and then every
`hub.broadcast` — chat deltas and finals, timeline events, anchors, verify
results, for ALL projects on the machine, since `ProjectHub` tags the event
and delegates to the one global Hub.

Reproduced against the live py-1.34.1 daemon before the fix: no token, and
`Origin: https://evil.example.com`, produced `101 Switching Protocols`
followed by the `hello` frame and live `*.updated` events.

The loopback bind is not a defence. WebSockets are exempt from the
same-origin policy and get no CORS preflight, so any page in any tab of the
operator's browser can dial the daemon; the server has to check `Origin`
itself, and `_handle_ws` wrote its 101 by hand without ever passing through
`_cors()`.

These tests speak raw TLS + RFC-6455 rather than going through httpx, because
what is under test IS the handshake.
"""

from __future__ import annotations

import base64
import os
import socket
import ssl

from conftest import Daemon


def _handshake(
    port: int,
    *,
    path: str = "/events",
    origin: str | None = None,
    bearer: str | None = None,
) -> tuple[bytes, ssl.SSLSocket]:
    """Send a WS upgrade and return (status line, socket)."""
    raw = socket.create_connection(("127.0.0.1", port), timeout=5)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    s = ctx.wrap_socket(raw, server_hostname="daemon.meshkore.com")
    key = base64.b64encode(os.urandom(16)).decode()
    lines = [
        f"GET {path} HTTP/1.1",
        "Host: daemon.meshkore.com",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
    ]
    if origin:
        lines.append(f"Origin: {origin}")
    if bearer:
        lines.append(f"Authorization: Bearer {bearer}")
    s.sendall(("\r\n".join(lines) + "\r\n\r\n").encode())
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = s.recv(4096)
        if not chunk:
            break
        buf += chunk
    return buf.split(b"\r\n", 1)[0], s


def _status(port: int, **kw) -> bytes:
    line, s = _handshake(port, **kw)
    try:
        s.close()
    except OSError:
        pass
    return line


# ── the regression itself ───────────────────────────────────────────────


def test_anonymous_upgrade_is_refused(daemon: Daemon) -> None:
    """No credential at all. This is the exact request that used to be
    answered with 101 and the full event stream."""
    assert b"401" in _status(daemon.port)


def test_hostile_origin_is_refused_even_with_a_token(daemon: Daemon) -> None:
    """Defence in depth: a page that somehow obtained the portal token still
    cannot subscribe, because a browser cannot forge `Origin`."""
    assert b"401" in _status(
        daemon.port, origin="https://evil.example.com", bearer=daemon.token
    )


def test_hostile_origin_without_token_is_refused(daemon: Daemon) -> None:
    assert b"401" in _status(daemon.port, origin="https://evil.pages.dev")


def test_wrong_token_is_refused(daemon: Daemon) -> None:
    assert b"401" in _status(daemon.port, bearer="not-the-portal-token")


def test_empty_token_param_is_refused(daemon: Daemon) -> None:
    """`?token=` with nothing after it must not read as "authenticated"."""
    assert b"401" in _status(daemon.port, path="/events?token=")


# ── the paths that must keep working ────────────────────────────────────


def test_query_token_upgrades(daemon: Daemon) -> None:
    """The cockpit's channel: browsers cannot set a header on a
    `new WebSocket(...)`, so `?token=` is the only one it has. `lib/ws.ts` has
    always sent it — the daemon simply never read it."""
    assert b"101" in _status(daemon.port, path=f"/events?token={daemon.token}")


def test_bearer_header_upgrades(daemon: Daemon) -> None:
    """The CLI channel — no Origin, so only the token is judged."""
    assert b"101" in _status(daemon.port, bearer=daemon.token)


def test_cockpit_origin_upgrades(daemon: Daemon) -> None:
    assert b"101" in _status(
        daemon.port,
        path=f"/events?token={daemon.token}",
        origin="https://architect.meshkore.com",
    )


def test_ws_alias_is_gated_too(daemon: Daemon) -> None:
    """`/ws` is the second alias for the same upgrade — both go through
    `_handle_ws`, so both must be gated."""
    assert b"401" in _status(daemon.port, path="/ws")
    assert b"101" in _status(daemon.port, path=f"/ws?token={daemon.token}")


def test_authorized_client_still_receives_the_hello_frame(daemon: Daemon) -> None:
    """The gate must not have broken the greeting handshake itself."""
    line, s = _handshake(daemon.port, path=f"/events?token={daemon.token}")
    try:
        assert b"101" in line
        head = s.recv(2)
        assert head[0] & 0x0F == 0x1, "hello must be a text frame"
        payload = s.recv(head[1] & 0x7F)
        assert b"hello" in payload
    finally:
        try:
            s.close()
        except OSError:
            pass
