"""The auth matrix — every route the daemon dispatches, and whether it is
reachable WITHOUT a token. Probed live, not read off the source.

Why this file exists (DAH1, initiative `daemon-audit-hardening`): the two verb
tables use OPPOSITE default policies.

- `route_post` has ONE gate near the top (`if self._need_auth(): return`), so
  every POST added below it is protected by default. Fail-closed.
- `route_get` has no such gate. Each route opts in with its own
  `if self._need_auth(): return` line, so a GET added without that line is
  PUBLIC. Fail-open — and nothing failed when it happened.

`test_routes_auth.py` pins 18 routes by hand out of ~60. That is a spot-check,
not a warranty: it can only catch a regression on a route someone remembered to
list. This file closes the gap by driving the SAME static route table the
endpoint warranty extracts (`test_route_coverage.EXERCISE`) and asserting the
observed anonymous-reachability of every entry against ANONYMOUS_OK below.

Read the assertion failure as a question, not a verdict: a new route showing up
here means "is this one supposed to be public?" — answer it by adding the route
to ANONYMOUS_OK (with the reason) or by adding its `_need_auth()` line.

The listed anonymous routes are the CURRENT state, faithfully recorded —
including the ones the audit flagged as questionable. Those are marked
`# REVIEW` rather than quietly blessed, and are tracked as task DAH2:

  /chat/conv/<id>/messages   serves full transcript BODIES, not just ids
  /chat/snapshot             boot hydrate, same content by another door
  /chat/convs                conv list + previews

The cockpit fetches all of them with `requireAuth: false` (see
`architect/src/lib/daemon-client.ts`), so gating them daemon-side is a
COORDINATED two-repo change — cockpit first (start sending the token, which is
harmless against an ungated daemon), daemon second. That ordering hazard is why
DAH1 did not flip them unilaterally.

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
    # REVIEW (task DAH2) — the three below serve conversation CONTENT to an
    # anonymous caller. /chat/conv/<id>/messages returns message BODIES, i.e.
    # full transcripts of the operator's conversations with the agents; the
    # snapshot/convs pair reaches the same content by another door. The
    # in-code justification ("conv ids are not secrets") holds for the ids and
    # not for the bodies.
    ("GET", "/chat/snapshot"),
    ("GET", "/chat/convs"),
    ("GET", "/chat/conv/general/meta"),
    ("GET", "/chat/conv/general/messages"),
    ("GET", "/chat/conv/general/queue"),
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
    # REVIEW (DAH2) — message BODIES, i.e. the full transcript of the
    # operator's conversations with the agents, to an anonymous caller.
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
