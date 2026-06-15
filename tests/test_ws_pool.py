"""WebSocket lifecycle (py-1.16.0, initiative `comms-hardening`).

D-WS-01: a live WS must NOT pin a request-pool worker — HTTP stays
responsive while many WS are held open. D-WS-02 (broadcast outside the
lock) is exercised indirectly: the hello frame is a broadcast-path send.

Raw-socket WS client over TLS (verify off) — the test daemon runs HTTPS.
"""

from __future__ import annotations

import base64
import os
import socket
import ssl
import struct

from conftest import Daemon


def _ws_open(port: int) -> ssl.SSLSocket:
    raw = socket.create_connection(("127.0.0.1", port), timeout=5)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    s = ctx.wrap_socket(raw, server_hostname="daemon.meshkore.com")
    key = base64.b64encode(os.urandom(16)).decode()
    req = (
        "GET /events HTTP/1.1\r\n"
        "Host: daemon.meshkore.com\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n"
    )
    s.sendall(req.encode())
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = s.recv(1024)
        if not chunk:
            break
        buf += chunk
    status = buf.split(b"\r\n", 1)[0]
    assert b"101" in status, f"expected 101 upgrade, got {status!r}"
    return s


def _read_frame(s: ssl.SSLSocket) -> tuple[int, bytes]:
    h = s.recv(2)
    assert len(h) == 2
    opcode = h[0] & 0x0F
    length = h[1] & 0x7F
    if length == 126:
        length = struct.unpack(">H", s.recv(2))[0]
    elif length == 127:
        length = struct.unpack(">Q", s.recv(8))[0]
    payload = b""
    while len(payload) < length:
        payload += s.recv(length - len(payload))
    return opcode, payload


def test_ws_does_not_starve_http(daemon: Daemon) -> None:
    """Hold several WS open; HTTP must still answer (workers not pinned)."""
    socks: list[ssl.SSLSocket] = []
    try:
        for _ in range(5):
            s = _ws_open(daemon.port)
            op, payload = _read_frame(s)  # greeting
            assert op == 0x1, "hello should be a text frame"
            assert b"hello" in payload
            socks.append(s)
        # With 5 WS held open, plain HTTP must still be served promptly —
        # proves the WS read loops aren't holding request-pool workers.
        r = daemon.get("/health")
        assert r.status_code == 200
        # And a second round, to be sure the pool isn't draining.
        assert daemon.get("/health").status_code == 200
    finally:
        for s in socks:
            try:
                s.close()
            except OSError:
                pass


def test_ws_heartbeat_or_clean_close(daemon: Daemon) -> None:
    """A WS that the client closes is reaped without wedging the daemon."""
    s = _ws_open(daemon.port)
    op, _ = _read_frame(s)
    assert op == 0x1
    s.close()
    # daemon must still serve HTTP right after a WS disconnect
    assert daemon.get("/health").status_code == 200
