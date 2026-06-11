"""DM2 — SIGUSR1 thread dump + bounded HTTP pool.

These tests exist because the 2026-06-10 ikamiro lock-contention bug
had two failure modes the daemon couldn't show us live:
* no thread stacks → we guessed for hours which lock was held
* unbounded thread spawn → 18 000+ threads at the time of kill

DM2 adds the diagnostics; these tests pin them so a future
refactor can't quietly drop the safety net."""

from __future__ import annotations

import os
import signal
import time

from conftest import Daemon


def test_sigusr1_dumps_thread_stacks(daemon: Daemon) -> None:
    """`kill -USR1 <pid>` appends every thread's stack to
    .meshkore/.runtime/threads.log via faulthandler.register."""
    threads_log = daemon.root / ".meshkore" / ".runtime" / "threads.log"
    # File may or may not exist before the signal — both states are OK.
    before = threads_log.stat().st_size if threads_log.exists() else 0

    os.kill(daemon.proc.pid, signal.SIGUSR1)
    # faulthandler writes synchronously but the kernel queues the
    # signal — give it a brief moment to land.
    deadline = time.time() + 2.0
    while time.time() < deadline:
        if threads_log.exists() and threads_log.stat().st_size > before:
            break
        time.sleep(0.05)

    assert threads_log.exists(), "threads.log not created on SIGUSR1"
    body = threads_log.read_text()
    assert "Thread" in body, "thread dump missing the expected header"
    # The signal handler dumps ALL threads — a daemon at idle has
    # several (main, heartbeat, poll, hub broadcast, reaper). At
    # minimum we should see more than one stack frame.
    assert body.count("Thread 0x") >= 2, "expected ≥2 threads in dump"


def test_pool_bounds_concurrent_requests(daemon: Daemon) -> None:
    """Bounded pool handles 50 concurrent /health hits without spawning
    a thread per request. We don't measure the pool size from outside
    (daemon doesn't expose it on /health), but we DO assert the daemon
    keeps answering under load — i.e. requests queue and serve cleanly."""
    import threading

    ok = [0]

    def hit() -> None:
        try:
            if daemon.get("/health").status_code == 200:
                ok[0] += 1
        except Exception:  # pragma: no cover - timing flake guard
            pass

    threads = [threading.Thread(target=hit) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    # At least 90% should succeed — the bounded pool queues, doesn't drop.
    assert ok[0] >= 45, f"only {ok[0]}/50 requests succeeded under burst"
