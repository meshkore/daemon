"""http_server.py — the bounded-pool HTTPS server + TLS/port helpers.

Extracted from daemon.py (DA-HTTPSERVER-01, daemon-architecture-v2). Pure
infrastructure: PoolHTTPServer (ThreadingHTTPServer with a bounded worker
pool, deferred TLS handshake, WS-hijack-aware shutdown, _pool-guarded
close), plus `_build_tls_context` and `_port_free`. No daemon coupling
beyond `utils._log` (which the bundle resolves to daemon.py's canonical
debug-stream-aware `_log` via the flat namespace).
"""

from __future__ import annotations

import socket
import ssl
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Optional

from utils import _log


class PoolHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer with a bounded worker pool (py-1.12.24+).

    The stdlib default spawns a fresh thread per request and never
    recycles. On a long-running daemon the OS thread count grows
    unboundedly; the 2026-06-10 ikamiro incident reached 18 000+ before
    the daemon was killed. With a pool of ``max_workers`` the count
    stays bounded; excess requests queue at the OS-accept layer.
    ``cluster.yaml.daemon.http.max_workers`` overrides; default 128
    (py-1.16.2 — raised from 64 with HTTP/1.1 keep-alive: a kept-alive
    connection holds a worker between requests, so headroom matters)."""

    # py-1.15.1 — listen backlog. socketserver's default request_queue_size
    # is 5; the docstring's old claim that excess requests "queue at the
    # OS-accept layer (much higher than any sane workload)" was wrong — the
    # accept backlog WAS 5, so a cockpit boot burst (~30 concurrent fetches:
    # /state + /chat/snapshot + every initiative body + the WS upgrade)
    # overflowed it and the kernel REFUSED the excess connections. That is
    # the intermittent ERR_CONNECTION_REFUSED the cockpit hit mid-hydration
    # (stranding the boot panel) and the `test_pool_bounds` 18/50 result.
    # 128 absorbs any realistic single-cockpit burst.
    request_queue_size = 128

    def __init__(self, *args, max_workers: int = 128, **kw) -> None:
        super().__init__(*args, **kw)
        self.daemon_threads = True
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="http"
        )
        # py-1.16.0 (D-WS-01) — fds of WebSocket connections whose
        # lifelong read loop was handed to a dedicated ws-pump thread.
        # shutdown_request must NOT close these when the (freed) pool
        # worker returns; the ws-pump owns + closes them on disconnect.
        self._ws_hijacked: set[int] = set()

    def process_request(self, request, client_address):  # type: ignore[override]
        self._pool.submit(self.process_request_thread, request, client_address)

    def shutdown_request(self, request):  # type: ignore[override]
        # Skip closing sockets a WS upgrade hijacked onto the ws-pump
        # thread (D-WS-01); the pump closes them itself.
        if id(request) in self._ws_hijacked:
            return
        super().shutdown_request(request)  # type: ignore[misc]

    def process_request_thread(self, request, client_address):  # type: ignore[override]
        # py-1.15.2 — complete the deferred TLS handshake HERE, on the
        # pool worker. The listening socket is wrapped with
        # do_handshake_on_connect=False, so a slow/half-open handshake
        # ties up one worker (of max_workers) instead of stalling the
        # single accept loop and refusing every other connection.
        if isinstance(request, ssl.SSLSocket):
            try:
                request.settimeout(10.0)
                request.do_handshake()
                request.settimeout(None)
            except OSError:
                try:
                    self.shutdown_request(request)  # type: ignore[attr-defined]
                except Exception:
                    pass
                return
        super().process_request_thread(request, client_address)  # type: ignore[misc]

    def server_close(self) -> None:  # type: ignore[override]
        # py-1.17.2 — guard `_pool`. `super().__init__()` binds the socket
        # FIRST; if the bind fails (port still held by a dying sibling/old
        # instance during a restart or self-update re-exec) it raises BEFORE
        # `self._pool` is assigned, then cleanup calls server_close() →
        # `self._pool.shutdown` raised AttributeError, MASKING the real bind
        # error and crashing the new daemon instead of letting it retry.
        # Field 2026-06-16: a restart race left two clusters dead this way.
        pool = getattr(self, "_pool", None)
        if pool is not None:
            try:
                pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
        super().server_close()


def _build_tls_context(cert_path: Path, key_path: Path) -> Optional[ssl.SSLContext]:
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
        return ctx
    except (ssl.SSLError, OSError) as e:
        _log(f"tls: failed to load cert ({e}); falling back to HTTP")
        return None


def _port_free(port: int) -> bool:
    # py-1.10.18 — Use SO_REUSEADDR for the probe bind too. Without it,
    # a port still in kernel TIME_WAIT (from a daemon that exited
    # seconds ago) reads as busy and the daemon migrates to the next
    # port. ThreadingHTTPServer enables reuse on the real listener, so
    # the actual bind succeeds — the test bind just lied.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False
