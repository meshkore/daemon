"""Endpoint warranty — every route the daemon dispatches is (a) acknowledged
and (b) provably wired.

This is the safety net that makes the daemon-architecture-v2 route split
(routes.py → routes_get.py / routes_post.py) trustworthy, and a standing
guard against silent 404 regressions as new endpoints ship.

Three layers:

1. **Static extraction** — parse the three dispatch files (routes_get.py,
   routes_post.py, routes.py) and collect EVERY route pattern: exact
   matches (``p == "/x"``) and prefix/suffix matches
   (``p.startswith`` / ``p.endswith``). This is the source of truth.

2. **Completeness guard** (`test_no_unacknowledged_routes` /
   `test_no_stale_acknowledgements`) — the set of patterns each EXERCISE
   entry declares it covers, plus the GUARD_ONLY set, must EQUAL the
   statically-extracted set. Add a route to the daemon without covering
   it here → red. Delete a route and forget to drop its test → red.

3. **Live exercise** (`test_route_wired`) — spin up a real daemon and hit
   each route with a concrete request, asserting it is NOT the no-handler
   fall-through (``404 {"error": "not found"}``). A resource-missing 404
   (e.g. ``{"error": "unknown run"}``) still proves the handler ran.

GUARD_ONLY holds the two routes that are unsafe to invoke live:
``/shutdown`` (would stop the test daemon — we prove its own auth gate
instead) and ``/self-update`` (would fetch + swap the bundle over the
network). Both are still required to exist in source by the guard.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Set, Tuple

import pytest

from conftest import Daemon

DAEMON_DIR = Path(__file__).resolve().parents[1]
ROUTE_FILES = ["routes_get.py", "routes_post.py", "routes.py"]

Pattern = Tuple[str, str]  # (kind, literal) — kind ∈ {"==","startswith","endswith"}


def _extract_routes(path: Path) -> Set[Pattern]:
    """Every route literal compared against the local var ``p`` in one file."""
    tree = ast.parse(path.read_text())
    out: Set[Pattern] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            parts = [node.left, *node.comparators]
            names = {p.id for p in parts if isinstance(p, ast.Name)}
            if "p" in names:
                for c in parts:
                    if isinstance(c, ast.Constant) and isinstance(c.value, str):
                        out.add(("==", c.value))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            tgt = node.func.value
            if (
                isinstance(tgt, ast.Name)
                and tgt.id == "p"
                and node.func.attr in ("startswith", "endswith")
            ):
                for a in node.args:
                    if isinstance(a, ast.Constant) and isinstance(a.value, str):
                        out.add((node.func.attr, a.value))
    return out


def _all_source_routes() -> Set[Pattern]:
    routes: Set[Pattern] = set()
    for fn in ROUTE_FILES:
        routes |= _extract_routes(DAEMON_DIR / fn)
    return routes


ALL_SOURCE_ROUTES = _all_source_routes()


# ── Live exercise table ──────────────────────────────────────────────────
# Each entry: (method, path, send_auth, covers). `covers` is the set of
# source patterns this single request proves wired — one request can cover
# a startswith prefix AND an endswith suffix (e.g. POST /agent-types/x/pause
# satisfies both ("startswith","/agent-types/") and ("endswith","/pause")).
EXERCISE = [
    # ── GET, exact ──
    ("GET", "/health", False, {("==", "/health")}),
    ("GET", "/state", False, {("==", "/state")}),
    ("GET", "/info", False, {("==", "/info")}),
    ("GET", "/agents", False, {("==", "/agents")}),
    ("GET", "/storage/usage", False, {("==", "/storage/usage")}),
    ("GET", "/chat/snapshot", True, {("==", "/chat/snapshot")}),
    ("GET", "/chat/convs", True, {("==", "/chat/convs")}),
    ("GET", "/chat/archives", True, {("==", "/chat/archives")}),
    ("GET", "/runs", True, {("==", "/runs")}),
    ("GET", "/quota", True, {("==", "/quota")}),
    ("GET", "/credentials", True, {("==", "/credentials")}),
    ("GET", "/log", True, {("==", "/log")}),
    ("GET", "/context", True, {("==", "/context")}),
    ("GET", "/links", True, {("==", "/links")}),
    ("GET", "/protocols", True, {("==", "/protocols")}),
    ("GET", "/cron/list", True, {("==", "/cron/list")}),
    ("GET", "/debug/tail", True, {("==", "/debug/tail")}),
    ("GET", "/reload", True, {("==", "/reload")}),
    ("GET", "/auth/challenge?nonce=probe", False, {("==", "/auth/challenge")}),
    # ── GET, prefix / suffix ──
    ("GET", "/state/cluster", True, {("startswith", "/state/")}),
    ("GET", "/docs/INDEX.md", True, {("startswith", "/docs/")}),
    ("GET", "/modules/daemon/README.md", True, {("startswith", "/modules/")}),
    ("GET", "/tasks/T1.md", True, {("startswith", "/tasks/")}),
    ("GET", "/log/2099-01-01.md", True, {("startswith", "/log/")}),
    ("GET", "/context/overview.md", True, {("startswith", "/context/")}),
    (
        "GET",
        "/initiative/alpha/activity",
        True,
        {("startswith", "/initiative/"), ("endswith", "/activity")},
    ),
    ("GET", "/runs/__probe__", True, {("startswith", "/runs/")}),
    ("GET", "/credentials/portal-token", True, {("startswith", "/credentials/")}),
    ("GET", "/chat/conv/general/meta", True, {("startswith", "/chat/conv/")}),
    ("GET", "/chat/uploads/2099-01-01/x.png", True, {("startswith", "/chat/uploads/")}),
    ("GET", "/links/__probe__", True, {("startswith", "/links/")}),
    ("GET", "/protocols/__probe__", True, {("startswith", "/protocols/")}),
    # ── POST, exact (auth → reaches the route table past the global gate) ──
    ("POST", "/chat/dispatch", True, {("==", "/chat/dispatch")}),
    ("POST", "/chat/cancel", True, {("==", "/chat/cancel")}),
    ("POST", "/chat/archive", True, {("==", "/chat/archive")}),
    ("POST", "/chat/unarchive", True, {("==", "/chat/unarchive")}),
    ("POST", "/debug/log", True, {("==", "/debug/log")}),
    ("POST", "/messages", True, {("==", "/messages")}),
    ("POST", "/runs", True, {("==", "/runs")}),
    ("POST", "/tasks", True, {("==", "/tasks")}),
    ("POST", "/agents", True, {("==", "/agents")}),
    ("POST", "/workers", True, {("==", "/workers")}),
    ("POST", "/version/next", True, {("==", "/version/next")}),
    # ── POST, prefix / suffix ──
    (
        "POST",
        "/agent-types/custom/pause",
        True,
        {("startswith", "/agent-types/"), ("endswith", "/pause")},
    ),
    ("POST", "/agent-types/custom/unpause", True, {("endswith", "/unpause")}),
    ("POST", "/quota/claude-code/auto/pause", True, {("startswith", "/quota/")}),
    (
        "POST",
        "/tasks/T1/transition",
        True,
        {("endswith", "/transition")},
    ),
    ("POST", "/tasks/T1/dispatch", True, {("endswith", "/dispatch")}),
    ("POST", "/tasks/T1/cancel", True, {("endswith", "/cancel")}),
    (
        "POST",
        "/cron/__probe__/trigger",
        True,
        {("startswith", "/cron/"), ("endswith", "/trigger")},
    ),
    ("POST", "/chat/conv/general/queue", True, {("startswith", "/chat/conv/")}),
    ("POST", "/runs/__probe__/cancel", True, {("startswith", "/runs/")}),
    ("POST", "/links/__probe__", True, {("startswith", "/links/")}),
    ("POST", "/admission/__probe__", True, {("startswith", "/admission/")}),
    ("POST", "/credentials/probecred", True, {("startswith", "/credentials/")}),
]

# Routes that exist in source but must NOT be invoked live (destructive).
# Still required to be present by the completeness guard.
GUARD_ONLY = {
    ("==", "/shutdown"): "would stop the test daemon — auth gate proven separately",
    ("==", "/self-update"): "would fetch + swap the bundle over the network",
}

_EXERCISED = set().union(*(c for _, _, _, c in EXERCISE)) if EXERCISE else set()
_COVERED = _EXERCISED | set(GUARD_ONLY)


# ── Layer 2: completeness guard ─────────────────────────────────────────


def test_no_unacknowledged_routes() -> None:
    """Every route the daemon dispatches must be covered here. A new
    ``p == "/foo"`` in routes_*.py with no EXERCISE/GUARD_ONLY entry → red."""
    missing = ALL_SOURCE_ROUTES - _COVERED
    assert not missing, (
        "These routes exist in source but are not exercised or guard-listed in "
        f"test_route_coverage.py: {sorted(missing)}"
    )


def test_no_stale_acknowledgements() -> None:
    """Every covered pattern must still exist in source — delete a route,
    and its now-orphaned test entry trips this."""
    stale = _COVERED - ALL_SOURCE_ROUTES
    assert not stale, (
        "These patterns are covered in test_route_coverage.py but no longer "
        f"exist in source: {sorted(stale)}"
    )


# ── Layer 3: live exercise ──────────────────────────────────────────────


def _is_no_handler(r) -> bool:
    """True only for the dispatch fall-through ``404 {"error":"unknown route"}``.
    A resource-missing 404 (unknown run / protocol / credential / missing file,
    all ``{"error":"not found"}``) is NOT a no-handler — the route ran, it just
    had nothing to return. The two were disambiguated in the route split so this
    test can tell "route absent" from "resource absent"."""
    if r.status_code != 404:
        return False
    try:
        return r.json().get("error") == "unknown route"
    except Exception:
        return False


# Force a fresh connection per request: some handlers reject early (e.g. a
# 400 before reading the JSON body), leaving the body undrained — on a reused
# keep-alive socket that would misframe the NEXT request. The cockpit always
# sends bodies its handlers read, so this only bites synthetic probes.
_NO_KEEPALIVE = {"Connection": "close"}


@pytest.mark.parametrize(
    "method,path",
    [(m, p) for (m, p, _a, _c) in EXERCISE],
    ids=[f"{m} {p}" for (m, p, _a, _c) in EXERCISE],
)
def test_route_wired(daemon: Daemon, method: str, path: str) -> None:
    """Each route dispatches to a handler (not the no-handler fall-through)."""
    send_auth = next(a for (m, p, a, _c) in EXERCISE if m == method and p == path)
    headers = {**_NO_KEEPALIVE, **(daemon.auth if send_auth else {})}
    body = {} if method == "POST" else None
    r = daemon.client.request(
        method, daemon.base + path, headers=headers, json=body, timeout=10.0
    )
    assert not _is_no_handler(r), (
        f"{method} {path} hit the no-handler fall-through (404 not found) — "
        "the route is not wired"
    )


def test_shutdown_route_is_gated_not_invoked(daemon: Daemon) -> None:
    """/shutdown is matched before the global POST auth gate, so an
    unauthenticated POST proves the route exists (its own auth check fires →
    401) WITHOUT actually stopping the daemon."""
    r = daemon.client.post(
        daemon.base + "/shutdown", headers=_NO_KEEPALIVE, json={}, timeout=10.0
    )
    assert r.status_code == 401, (
        f"expected 401 from unauth /shutdown, got {r.status_code}"
    )
    # The route exists (its own auth gate fired) without reaching shutdown
    # logic — the daemon process must still be alive and answering.
    assert daemon.proc.poll() is None, "daemon exited after unauth /shutdown probe"
    assert daemon.get("/health").status_code == 200
