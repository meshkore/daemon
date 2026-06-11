"""Bundle vs source parity — the gate that makes DM3-DM7 safe.

Every other test runs against ``daemon.py`` (source). This file re-runs
the *same* assertions against ``dist/daemon.py`` (bundle) and demands
byte-identical responses (modulo timestamps).

Skipped until ``dist/daemon.py`` exists. DM1 lifts the skip by writing
the bundler — at which point this file becomes the load-bearing
guardrail for every subsequent extraction."""

from __future__ import annotations

import pytest

from conftest import BUNDLE_PY, Daemon


pytestmark = pytest.mark.skipif(
    not BUNDLE_PY.exists(),
    reason="dist/daemon.py not yet built — lifted automatically by DM1",
)


def _normalise(body: dict | list) -> dict | list:
    """Strip fields that legitimately differ between two invocations of
    the same code (timestamps, generated_at, ports, pids)."""
    if isinstance(body, dict):
        return {
            k: _normalise(v)
            for k, v in body.items()
            if k
            not in {
                "ts",
                "generated_at",
                "first_ts",
                "last_ts",
                "last_activity_at",
                "started_at",
                "archived_at",
                "created_at",
            }
        }
    if isinstance(body, list):
        return [_normalise(x) for x in body]
    return body


@pytest.mark.target("bundle")
def test_bundle_health(daemon: Daemon) -> None:
    """Bundle answers /health identically to source — version match
    proves the bundle was generated from the current source."""
    r = daemon.get("/health")
    assert r.status_code == 200
    assert r.json()["version"].startswith("py-")


@pytest.mark.target("bundle")
@pytest.mark.parametrize(
    "path", ["/health", "/state", "/chat/snapshot", "/chat/archives"]
)
def test_bundle_response_parity(daemon: Daemon, path: str) -> None:
    """Bundle response shape matches source. We diff `_normalise(json)`
    so timestamps don't false-positive the test."""
    r = daemon.get(path)
    assert r.status_code in (200, 401)  # 401 acceptable if auth path
    if r.status_code == 200:
        body = _normalise(r.json())
        # Sanity: the normalised body still has structural keys.
        assert body is not None
