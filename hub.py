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


def _iso_now() -> str:
    return (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.")
        + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"
    )


class WSClient:
    __slots__ = ("sock", "closed")

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.closed = False

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
        try:
            self.sock.sendall(bytes(header) + data)
        except OSError:
            self.close()

    def close(self) -> None:
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
        payload = json.dumps(event, separators=(",", ":"))
        with self._lock:
            dead = []
            for c in list(self._clients):
                c.send_text(payload)
                if c.closed:
                    dead.append(c)
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
