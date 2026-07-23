"""WebSocket Hub — subscriber registry + broadcast fan-out + heartbeat.

A daemon instance has exactly one ``Hub``. Every component that needs
to push events to connected cockpits / mesh peers / SSE listeners
gets the hub via constructor injection and calls ``hub.broadcast({…})``.

The broadcast loop holds ``_lock`` for the snapshot of the subscriber
set, NOT during the per-client send — a slow / dead client never
blocks the broadcast (dead clients are detected by the
``OSError`` on send and discarded next iteration).

The heartbeat thread keeps an idle ws open through aggressive proxies
(every ``HEARTBEAT_SEC`` seconds) and tells the cockpit the daemon
is still alive even when no real events fire.

Bundler note (see storage.py for the full pattern): the local
``_iso_now`` is shadowed in ``dist/daemon.py`` by daemon.py's
debug-stream-aware version because daemon.py is appended last."""

from __future__ import annotations

import json
import socket
import struct
import threading
from datetime import datetime, timezone
from typing import Any, Dict

HEARTBEAT_SEC = 20.0
SEND_TIMEOUT_SEC = 5.0  # py-1.16.0 (D-WS-02) — bound a send so a stalled client can't wedge a broadcaster


def _iso_now() -> str:
    return (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.")
        + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"
    )


class WSClient:
    __slots__ = ("sock", "closed", "_send_lock")

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.closed = False
        # py-1.31.4 (daemon-centralized) — SERIALIZE every write to this
        # socket. `broadcast()` is called from MANY threads (heartbeat, agent
        # runners, anchor, state-rebuild) AND the ws handler thread itself
        # sends frames — all onto the SAME `SSLSocket`. Two concurrent
        # `sendall`s become two concurrent `SSL_write`s on ONE OpenSSL
        # connection object, which is undefined behavior → native heap
        # corruption ("BUG IN CLIENT OF LIBMALLOC" in `tls_setup_write_buffer`,
        # 5 hard crashes 2026-07-05→09). The GIL does NOT prevent it: `SSL_write`
        # releases the GIL for the duration of the I/O. One lock per connection
        # guarantees at most one writer at a time (reads stay lock-free — OpenSSL
        # allows one concurrent reader + one writer). Version-independent fix.
        # RLock (reentrant): `send_text` calls `close()` on its own OSError while
        # still holding the lock — a plain Lock would self-deadlock there.
        self._send_lock = threading.RLock()
        # py-1.16.0 (D-WS-02) — bound sends so a client with a full TCP
        # send buffer (suspended laptop, paused devtools) can't block a
        # broadcaster forever. SO_SNDTIMEO affects ONLY send, not the
        # ws-pump's recv. Best-effort: the timeval struct layout is
        # platform-specific, so fall back to untimed sends on failure
        # (the broadcast-outside-the-lock change still prevents the
        # whole-Hub stall; this just bounds the per-client cost).
        try:
            sock.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_SNDTIMEO,
                struct.pack("ll", int(SEND_TIMEOUT_SEC), 0),
            )
        except (OSError, struct.error):
            pass

    def send_text(self, payload: str) -> None:
        """Send a single, unmasked, unfragmented text frame (server → client)."""
        if self.closed:
            return
        data = payload.encode("utf-8")
        header = bytearray()
        header.append(0x81)  # FIN + text opcode
        n = len(data)
        if n < 126:
            header.append(n)
        elif n < 65536:
            header.append(126)
            header.extend(struct.pack(">H", n))
        else:
            header.append(127)
            header.extend(struct.pack(">Q", n))
        frame = bytes(header) + data
        # Hold the per-connection lock across the WHOLE sendall so no other
        # thread can interleave an SSL_write on this socket (see __init__).
        with self._send_lock:
            if self.closed:
                return
            try:
                self.sock.sendall(frame)
            except OSError:
                self.close()

    def close(self) -> None:
        # Serialize teardown against in-flight sends (same per-connection lock)
        # so we never shutdown/close the socket mid-`SSL_write`. Reentrant, so
        # `send_text`'s own error-path `close()` (already holding the lock) is
        # fine; an external `remove()` waits for the bounded send to finish.
        with self._send_lock:
            if self.closed:
                return
            self.closed = True
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.sock.close()
            except OSError:
                pass


class Hub:
    """Broadcaster — keeps the set of connected clients and a heartbeat."""

    def __init__(self):
        self._clients: set[WSClient] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def add(self, client: WSClient) -> None:
        with self._lock:
            self._clients.add(client)

    def remove(self, client: WSClient) -> None:
        with self._lock:
            self._clients.discard(client)
        client.close()

    def broadcast(self, event: Dict[str, Any]) -> None:
        # py-1.16.0 (D-WS-02) — snapshot the client set UNDER the lock,
        # then send OUTSIDE it. Previously the per-client blocking
        # `sendall` ran while holding `_lock`, so one stalled client
        # froze every broadcaster (chat deltas, run.* events, heartbeat)
        # — the docstring claimed sends were outside the lock but the
        # code did the opposite. Sends are now bounded by SO_SNDTIMEO
        # (see WSClient), so a wedged client is dropped within the
        # timeout instead of blocking the fan-out.
        payload = json.dumps(event, separators=(",", ":"))
        with self._lock:
            clients = list(self._clients)
        dead = []
        for c in clients:
            c.send_text(payload)
            if c.closed:
                dead.append(c)
        if dead:
            with self._lock:
                for c in dead:
                    self._clients.discard(c)

    def shutdown(self) -> None:
        self._stop.set()
        with self._lock:
            for c in list(self._clients):
                c.close()
            self._clients.clear()

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(HEARTBEAT_SEC):
            self.broadcast({"type": "heartbeat", "ts": _iso_now()})


class ProjectHub:
    """A per-project view of the one global Hub (DC-6, daemon-centralized).

    Each per-project component (state_manager, runs, cron, the registries, the
    ChatRunner …) broadcasts through one of these instead of the raw Hub, so
    every event is auto-tagged with its ``project_id``. The single WS
    connection then carries events for all projects and the cockpit routes them
    (its event-bus already filters by cluster). Any non-broadcast attribute
    (add / remove / shutdown / _clients …) delegates straight to the real Hub.
    """

    __slots__ = ("_hub", "_project_id")

    def __init__(self, hub: "Hub", project_id: str) -> None:
        self._hub = hub
        self._project_id = project_id

    def broadcast(self, event: Dict[str, Any]) -> None:
        if isinstance(event, dict) and event.get("project_id") is None:
            event = {**event, "project_id": self._project_id}
        self._hub.broadcast(event)

    def __getattr__(self, name: str) -> Any:
        # Everything that isn't an explicit slot/method here → the real Hub.
        return getattr(self._hub, name)
