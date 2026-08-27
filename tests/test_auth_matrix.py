"""The auth matrix — every route the daemon dispatches, and whether it is
reachable WITHOUT a token. Probed live, not read off the source.

Why this file exists (DAH1, initiative `daemon-audit-hardening`): the two verb
tables used to run OPPOSITE default policies.

- `route_post` has ONE gate near the top, so every POST below it is protected
  by default. Fail-closed.
- `route_get` had no such gate. Each route opted in with its own
  `if self._need_auth(): return`, so a GET added without that line was PUBLIC.
  Fail-open — and nothing failed when it happened.

**DAH2(a) closed that** (py-1.34.0): `route_get` now has a single gate too,
driven by the `PUBLIC_GET_EXACT` / `PUBLIC_GET_PREFIXES` tables at the top of
`routes_get.py`. A new GET is private unless someone deliberately adds it
there. This file remains the runtime proof that the declared table and the
observed behaviour agree — a table entry that does not match reality (a typo
in a prefix, a route that authenticates inside its handler) still goes red
here.

`test_routes_auth.py` pins 18 routes by hand out of ~60. That is a spot-check,
not a warranty: it can only catch a regression on a route someone remembered to
list. This file closes the gap by driving the SAME static route table the
endpoint warranty extracts (`test_route_coverage.EXERCISE`) and asserting the
observed anonymous-reachability of every entry against ANONYMOUS_OK below.

Read the assertion failure as a question, not a verdict: a new route showing up
here means "is this one supposed to be public?" — answer it by adding the route
to ANONYMOUS_OK (with the reason) or by adding its `_need_auth()` line.

**DAH2(b) closed the content leak** (py-1.34.0). `/chat/snapshot`,
`/chat/convs` and everything under `/chat/conv/` — including
`/chat/conv/<id>/messages`, which returns message BODIES, i.e. the operator's
full transcripts with the agents — were anonymous. They are now gated.

That took a coordinated two-repo change in a mandatory order, because the
cockpit fetched them with `requireAuth: false`, a flag that SUPPRESSES the
Authorization header even when a token is in hand. Cockpit first
(architect@bdc8dd4 — starts sending the token, a no-op against an ungated
daemon), daemon second. Reversed, every cockpit still on the old bundle would
have lost its chat history until the Pages deploy landed.

Note on coverage: the exercise table probes ONE path per route pattern, so
`/chat/conv/<id>/meta` stands in for the whole `startswith("/chat/conv/")`
prefix. The sub-routes under that prefix differ sharply in how sensitive their
payload is — `/meta` is a sidecar of ids, `/messages` is the transcript — so
EXTRA_PROBES pins each of them separately.
"""

from __future__ import annotations

import pytest

from conftest import Daemon
from test_route_coverage import EXERCISE, GUARD_ONLY  # noqa: F401


# ── The recorded state ───────────────────────────────────────────────────
# (method, path) pairs that answer WITHOUT an Authorization header. Every
# entry carries the reason it is public; anything not listed must 401.
ANONYMOUS_OK = {
    # Paths are spelled EXACTLY as test_route_coverage.EXERCISE spells them,
    # so the two tables can never drift apart.
    #
    # ── boot discovery + wire-version handshake — no project content ──
    ("GET", "/health"),
    ("GET", "/info"),
    ("GET", "/auth/challenge?nonce=probe"),
    # (GET /auth/local-token hands the LOCAL cockpit its token, py-1.27.6. It
    # is gated on EXACT cockpit origins inside the handler — stricter than
    # _need_auth, not weaker — and is not in the exercise table.)
    #
    # ── project state + roadmap ──
    # Public since the Node era; the cockpit paints the board before the
    # operator pastes a token.
    ("GET", "/state"),
    ("GET", "/state/cluster"),
    ("GET", "/roadmap/live"),
    ("GET", "/agents"),
    ("GET", "/clients"),
    ("GET", "/storage/usage"),
    ("GET", "/projects"),
    #
    # ── registries ──
    # Deployment links + workflow runbooks. Repo content that travels with the
    # repo anyway (§2.2 commits public/, docs/, modules/).
    ("GET", "/links"),
    ("GET", "/links/__probe__"),
    ("GET", "/workflows"),
    ("GET", "/workflows/__probe__"),
    ("GET", "/protocols"),
    ("GET", "/protocols/__probe__"),
    #
    # ── team ──
    # Roster frontmatter only: the member TOKEN is added to the payload solely
    # when the caller presents the portal token (TEG-1). The A2A card is public
    # metadata by definition and 404s for internal/unknown members.
    ("GET", "/team"),
    ("GET", "/team/__probe__"),
    ("GET", "/team/__probe__/.well-known/agent.json"),
    # (GET /team/requests/<rid> is NOT here: it is MEMBER-token gated inside
    # the handler, so an anonymous caller gets 401 from there.)
    #
    # ── chat ──
    ("GET", "/chat/archives"),  # archive flags: ids + booleans, no bodies
    # NOTE: /chat/snapshot, /chat/convs and everything under /chat/conv/ are
    # NOT here any more — DAH2(b) gated them, see the module docstring.
    #
    # ── opaque-URL reads ──
    # The filename carries a random suffix and every write path that mints one
    # is portal-gated, so the URL itself is the capability. The verify shot is
    # confined to .meshkore/.runtime/verify/ and requires the exact ?path=.
    ("GET", "/chat/uploads/2099-01-01/x.png"),
    ("GET", "/verify/shot"),
}


# Route patterns whose SUB-paths differ in sensitivity, so one probe per
# pattern is not enough. Each is pinned on its own.
EXTRA_PROBES = [
    # Message BODIES — the operator's full transcript with the agents. Probed
    # separately from `/meta` because one exercise entry stands in for the
    # whole `/chat/conv/` prefix, and these two are the sensitive members of it.
    ("GET", "/chat/conv/general/messages"),
    ("GET", "/chat/conv/general/queue"),  # the operator's typed pending prompts
]


def _probe(daemon: Daemon, method: str, path: str):
    if method == "GET":
        return daemon.get(path)
    if method == "POST":
        return daemon.post(path, json={})
    return daemon.client.request(method, daemon.base + path)


def _matrix_entries():
    """One (method, path) per EXERCISE row, minus the routes that are unsafe
    to invoke (/shutdown would stop the test daemon, /self-update would fetch
    and swap the bundle over the network)."""
    seen = []
    for method, path, _auth, _covers in EXERCISE:
        if path in ("/shutdown", "/self-update"):
            continue
        seen.append((method, path))
    seen.extend(EXTRA_PROBES)
    return sorted(set(seen))


MATRIX = _matrix_entries()


@pytest.mark.parametrize("method,path", MATRIX)
def test_anonymous_reachability_matches_the_record(
    daemon: Daemon, method: str, path: str
) -> None:
    """Drives the endpoint warranty's own route table, so a route can no
    longer become publicly readable without this file going red."""
    r = _probe(daemon, method, path)
    reachable = r.status_code != 401
    expected = (method, path) in ANONYMOUS_OK
    if reachable and not expected:
        pytest.fail(
            f"{method} {path} answered {r.status_code} to an ANONYMOUS caller "
            "but is not in ANONYMOUS_OK. Either add its "
            "`if self._need_auth(): return` line, or add it to ANONYMOUS_OK "
            "with the reason it is safe to publish."
        )
    if expected and not reachable:
        pytest.fail(
            f"{method} {path} is recorded as anonymous but 401'd. If the gate "
            "was added deliberately, drop it from ANONYMOUS_OK — and check the "
            "cockpit sends a token for it (daemon-client.ts `requireAuth`)."
        )


def test_every_anonymous_entry_is_a_real_route() -> None:
    """Stale entries rot the record. Every ANONYMOUS_OK pair must still be a
    route the daemon dispatches."""
    live = set(MATRIX)
    stale = {e for e in ANONYMOUS_OK if e not in live}
    assert not stale, (
        f"ANONYMOUS_OK lists routes that no longer exist (or are no longer "
        f"exercised): {sorted(stale)}"
    )


def test_post_is_fail_closed() -> None:
    """The structural invariant behind the POST table: exactly one global
    `_need_auth()` gate, and the handful of routes matched BEFORE it are the
    ones with their own deliberate auth (the TEG member token, the CPL-2
    remote token, and /shutdown's own gate)."""
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "routes_post.py").read_text()
    head, sep, _tail = src.partition(
        "    # All other POSTs need auth.\n    if self._need_auth():"
    )
    assert sep, "the global POST auth gate moved or was renamed — check why"
    # Only these may be routed before the gate.
    pre_gate_routes = {
        line.split('"')[1]
        for line in head.splitlines()
        if ('if p == "' in line or 'p.startswith("' in line)
    }
    assert pre_gate_routes <= {
        "/shutdown",  # gates itself on the next line
        "/team/",  # TEG-2 ask — member token, validated in the handler
        "/projects",  # CPL-2 — portal token OR machine remote token
    }, (
        "a POST route is matched BEFORE the global auth gate: "
        f"{sorted(pre_gate_routes)}. Anything there is UNAUTHENTICATED unless "
        "its handler does its own check."
    )


def test_get_is_fail_closed() -> None:
    """DAH2(a) — the structural invariant for GET, mirroring
    `test_post_is_fail_closed`.

    Two things must hold in `routes_get.py`:

    1. There is exactly ONE `_need_auth()` call — the global gate. A second one
       means a route grew its own opt-in again, which is the pattern that made
       the surface fail-open in the first place (23 of them had accumulated).
    2. Every route the endpoint warranty knows about is either matched by the
       declared public tables or it is gated. There is no third state.
    """
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "routes_get.py").read_text()
    calls = src.count("if self._need_auth():")
    assert calls == 1, (
        f"routes_get.py has {calls} `_need_auth()` gates; there must be exactly "
        "one (the global gate). A per-route opt-in re-introduces the fail-open "
        "policy DAH2(a) removed — add the route to PUBLIC_GET_* instead."
    )

    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from routes_get import _get_is_public  # type: ignore[import-not-found]

    for method, path in MATRIX:
        if method != "GET":
            continue
        bare = path.split("?", 1)[0]
        declared_public = _get_is_public(bare)
        recorded_public = (method, path) in ANONYMOUS_OK
        # `/team/requests/<rid>` is the one route the table publishes while the
        # HANDLER authenticates (member token / CPL-2 remote token), so it is
        # declared-public but observed-401. That asymmetry is intentional.
        if bare.startswith("/team/requests/"):
            continue
        assert declared_public == recorded_public, (
            f"{path}: PUBLIC_GET_* says public={declared_public} but the "
            f"observed record says public={recorded_public}. The declared "
            "table and reality must agree."
        )
