"""WSClient send serialization (py-1.31.4, initiative `daemon-centralized`).

Regression guard for the recurring native crash: `Hub.broadcast()` is called
from many threads AND the ws handler sends frames — all onto ONE `SSLSocket`.
Two concurrent `sendall`s → two concurrent `SSL_write`s on one OpenSSL object →
heap corruption (5 hard crashes 2026-07-05→09). The fix is a per-connection
lock so at most one thread is ever inside `sendall`. This test proves the
serialization with a fake socket that FAILS if two threads overlap in `sendall`
— no TLS/daemon needed, fully deterministic.
"""

from __future__ import annotations

import threading
import time

from hub import WSClient


class _OverlapDetectingSock:
    """Fake socket whose sendall FAILS the test if two threads are inside it
    at the same time (exactly the concurrent-SSL_write condition)."""

    def __init__(self) -> None:
        self.in_send = 0
        self.overlaps = 0
        self.calls = 0
        self._probe = threading.Lock()  # guards the counters ONLY, not sendall

    def setsockopt(self, *a, **k) -> None:  # WSClient.__init__ calls this
        pass

    def sendall(self, data: bytes) -> None:
        with self._probe:
            self.calls += 1
            self.in_send += 1
            overlapping = self.in_send > 1
            if overlapping:
                self.overlaps += 1
        # Hold the "send" open a beat so any unsynchronized caller would collide.
        time.sleep(0.001)
        with self._probe:
            self.in_send -= 1


def test_send_text_is_serialized_across_threads() -> None:
    sock = _OverlapDetectingSock()
    client = WSClient(sock)  # type: ignore[arg-type]

    N_THREADS = 16
    PER_THREAD = 25
    barrier = threading.Barrier(N_THREADS)

    def hammer() -> None:
        barrier.wait()  # maximize contention — all start together
        for i in range(PER_THREAD):
            client.send_text(f"event-{i}")

    threads = [threading.Thread(target=hammer) for _ in range(N_THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sock.calls == N_THREADS * PER_THREAD, sock.calls
    assert sock.overlaps == 0, f"{sock.overlaps} overlapping sendall(s) — lock failed"


def test_close_does_not_deadlock_from_send_error_path() -> None:
    """send_text calls self.close() on OSError WHILE holding the lock; the
    reentrant lock must let that through (a plain Lock would self-deadlock)."""

    class _RaisingSock:
        def setsockopt(self, *a, **k) -> None:
            pass

        def sendall(self, data: bytes) -> None:
            raise OSError("broken pipe")

        def shutdown(self, *a) -> None:
            pass

        def close(self) -> None:
            pass

    client = WSClient(_RaisingSock())  # type: ignore[arg-type]
    done = threading.Event()

    def run() -> None:
        client.send_text("boom")  # OSError → close() while holding the lock
        done.set()

    t = threading.Thread(target=run)
    t.start()
    t.join(timeout=5)
    assert done.is_set(), "send_text→close() deadlocked (lock not reentrant)"
    assert client.closed is True
