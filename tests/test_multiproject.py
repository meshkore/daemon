"""TC-1 (initiative `daemon-centralized`) — one daemon serving MANY projects.

End-to-end against a real spawned daemon (the conftest `daemon` fixture):
register a SECOND project via POST /projects, prove per-request routing by
X-MeshKore-Project isolates the two, and unregister cleanly. This is the live
warranty that DC-2 (registry) + DC-4 (routing) + DC-5 (/projects API) actually
work over HTTP, not just in unit construction.

The daemon's boot project is the fixture's 'populated' cluster (id "populated").
The global ledger + ports file are workdir-isolated (conftest), so this test
never touches the operator's ~/.meshkore.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.cluster("populated")
def test_register_isolate_unregister_second_project(daemon, tmp_path: Path) -> None:
    # ── boot project (A) is the default ──────────────────────────────────
    r = daemon.get("/projects")
    assert r.status_code == 200, r.text
    listing = r.json()
    a_id = listing["default"]
    assert a_id == "populated", listing
    assert a_id in {p["id"] for p in listing["projects"]}

    # ── register a SECOND project (B) by path ────────────────────────────
    # Distinct dir; the daemon scaffolds .meshkore/ then registers by cluster id.
    b_root = tmp_path / "project-b"
    b_root.mkdir(parents=True, exist_ok=True)
    r = daemon.post(
        "/projects",
        headers=daemon.auth,
        json={"path": str(b_root), "name": "Project B"},
    )
    assert r.status_code == 201, r.text
    b_id = r.json()["id"]
    assert b_id and b_id != a_id
    assert r.json()["scaffolded"] is True

    # ── both now listed ──────────────────────────────────────────────────
    ids = {p["id"] for p in daemon.get("/projects").json()["projects"]}
    assert {a_id, b_id} <= ids

    # ── isolation: the X-MeshKore-Project header routes to the right ctx ──
    # /health reports self.cluster.id, which is a per-project property (DC-4).
    ha = daemon.get("/health", headers={"X-MeshKore-Project": a_id}).json()
    hb = daemon.get("/health", headers={"X-MeshKore-Project": b_id}).json()
    assert ha["cluster_id"] == a_id
    assert hb["cluster_id"] == b_id

    # Unknown / absent header → daemon falls back to the default (boot) project
    # (DC-4 design: registry.get(None|unknown) → default).
    assert daemon.get("/health").json()["cluster_id"] == a_id
    assert (
        daemon.get("/health", headers={"X-MeshKore-Project": "no-such"}).json()[
            "cluster_id"
        ]
        == a_id
    )

    # ── unregister B (ledger stays on disk) ──────────────────────────────
    r = daemon.client.request(
        "DELETE", daemon.base + f"/projects/{b_id}", headers=daemon.auth
    )
    assert r.status_code == 200, r.text
    ids_after = {p["id"] for p in daemon.get("/projects").json()["projects"]}
    assert b_id not in ids_after and a_id in ids_after
    assert (b_root / ".meshkore" / "public" / "cluster.yaml").exists()

    # ── the default (boot) project cannot be unregistered ────────────────
    r = daemon.client.request(
        "DELETE", daemon.base + f"/projects/{a_id}", headers=daemon.auth
    )
    assert r.status_code == 409, r.text


@pytest.mark.cluster("populated")
def test_debug_stream_is_project_tagged(daemon) -> None:
    """Centralized multi-project debug: a cockpit-style POST /debug/log with the
    X-MeshKore-Project header is tagged with that project and GET
    /debug/tail?project=<id> slices the one stream per project."""
    a_id = daemon.get("/projects").json()["default"]
    r = daemon.post(
        "/debug/log",
        headers={**daemon.auth, "X-MeshKore-Project": a_id},
        json={"events": [{"tag": "ws", "msg": "hello-from-cockpit", "lvl": "info"}]},
    )
    assert r.status_code == 200, r.text
    # Tail filtered by THIS project shows the tagged entry.
    r = daemon.get(
        "/debug/tail",
        headers=daemon.auth,
        params={"project": a_id, "last": "120", "tag": "ws"},
    )
    assert r.status_code == 200, r.text
    evs = r.json()["events"]
    assert any(
        e.get("project") == a_id and e.get("msg") == "hello-from-cockpit" for e in evs
    ), evs
    # Filtering by a DIFFERENT project excludes it.
    r2 = daemon.get(
        "/debug/tail",
        headers=daemon.auth,
        params={"project": "other-proj", "last": "120"},
    )
    assert not any(e.get("msg") == "hello-from-cockpit" for e in r2.json()["events"])


_FAKE_CLAUDE = (
    "#!/usr/bin/env python3\n"
    "import sys, json\n"
    "sys.stdin.read()\n"
    'print(json.dumps({"type": "result", "result": "pong",\n'
    '  "usage": {"input_tokens": 1, "output_tokens": 1,\n'
    '            "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0}}),\n'
    "  flush=True)\n"
)


@pytest.mark.cluster("populated")
def test_chat_dispatch_routes_to_its_project(daemon, tmp_path: Path) -> None:
    """FC-2 regression — a chat dispatched with X-MeshKore-Project=B runs its
    turn AND finalises into B, and does NOT leak into the default project A.
    Exercises the dispatch → reader-loop (background thread) → finalise path
    with a project header end-to-end (nothing else does)."""
    binp = daemon.work / "bin" / "claude"
    binp.write_text(_FAKE_CLAUDE)
    binp.chmod(0o755)
    b_root = tmp_path / "proj-b-chat"
    b_root.mkdir(parents=True, exist_ok=True)
    b_id = daemon.post(
        "/projects", headers=daemon.auth, json={"path": str(b_root)}
    ).json()["id"]
    a_id = daemon.get("/projects").json()["default"]
    assert b_id != a_id
    conv = "route-b"
    hb = {**daemon.auth, "X-MeshKore-Project": b_id}
    ha = {**daemon.auth, "X-MeshKore-Project": a_id}

    r = daemon.post("/chat/dispatch", headers=hb, json={"conv": conv, "text": "ping"})
    assert r.status_code in (200, 202), r.text

    import time as _t

    final = None
    for _ in range(80):
        msgs = (
            daemon.get(f"/chat/conv/{conv}/messages", headers=hb)
            .json()
            .get("messages", [])
        )
        final = next((m for m in msgs if m.get("type") == "chat.assistant.final"), None)
        if final:
            break
        _t.sleep(0.25)
    assert final, "no chat.assistant.final landed in project B"
    # The conv must NOT exist in the default project A (no cross-project leak).
    a_msgs = (
        daemon.get(f"/chat/conv/{conv}/messages", headers=ha).json().get("messages", [])
    )
    assert not any(m.get("type") == "chat.assistant.final" for m in a_msgs), (
        "conv leaked into the default project"
    )


@pytest.mark.cluster("populated")
def test_state_is_project_isolated(daemon, tmp_path: Path) -> None:
    """GET /state routes by header — A (populated) has initiatives, a freshly
    registered B (empty scaffold) does not."""
    b_root = tmp_path / "proj-b-state"
    b_root.mkdir(parents=True, exist_ok=True)
    b_id = daemon.post(
        "/projects", headers=daemon.auth, json={"path": str(b_root)}
    ).json()["id"]
    a_id = daemon.get("/projects").json()["default"]
    sa = daemon.get(
        "/state", headers={**daemon.auth, "X-MeshKore-Project": a_id}
    ).json()
    sb = daemon.get(
        "/state", headers={**daemon.auth, "X-MeshKore-Project": b_id}
    ).json()
    a_inits = len(
        (sa.get("initiatives") or sa.get("roadmap", {}).get("initiatives") or [])
    )
    b_inits = len(
        (sb.get("initiatives") or sb.get("roadmap", {}).get("initiatives") or [])
    )
    assert a_inits >= 1, f"default project A should have initiatives: {a_inits}"
    assert b_inits == 0, f"fresh project B should have no initiatives: {b_inits}"


@pytest.mark.cluster("populated")
def test_cors_allows_project_header(daemon) -> None:
    """The cockpit sends X-MeshKore-Project on every request; if CORS doesn't
    allow it, the browser preflight blocks EVERY cross-origin call (the
    architect.meshkore.com → daemon failure seen 2026-06-25). A preflight from
    an allowlisted origin must echo the header in Access-Control-Allow-Headers."""
    r = daemon.client.request(
        "OPTIONS",
        daemon.base + "/state",
        headers={
            "Origin": "https://architect.meshkore.com",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "x-meshkore-project",
            "Connection": "close",
        },
    )
    allow = r.headers.get("access-control-allow-headers", "")
    assert "x-meshkore-project" in allow.lower(), f"allow-headers={allow!r}"


@pytest.mark.cluster("populated")
@pytest.mark.server_home
def test_server_home_never_appears_as_a_project(daemon, tmp_path: Path) -> None:
    """FC-2 — when the boot cluster's .meshkore IS the global ledger (the
    central-server topology), that cluster is the HOME (ideas, projects
    registry, external creds), NOT a project. It must NEVER appear in /projects
    nor be the landing default; /health flags it with server_home=True. Only
    real projects (registered by path) show up."""
    # /health reports the home flag (so the cockpit can avoid landing on it).
    assert daemon.get("/health").json()["server_home"] is True

    # No real projects yet → empty list, no default to land on.
    listing = daemon.get("/projects").json()
    assert listing["projects"] == [], listing
    assert listing["default"] is None, listing

    # Register a REAL project by path.
    b_root = tmp_path / "real-proj"
    b_root.mkdir(parents=True, exist_ok=True)
    b_id = daemon.post(
        "/projects", headers=daemon.auth, json={"path": str(b_root)}
    ).json()["id"]

    # Now ONLY the real project shows; the home cluster id is never listed,
    # and it becomes the landing default.
    listing = daemon.get("/projects").json()
    ids = {p["id"] for p in listing["projects"]}
    assert ids == {b_id}, listing
    assert "populated" not in ids, "the server home leaked into /projects"
    assert listing["default"] == b_id, listing


@pytest.mark.cluster("populated")
def test_uploads_route_by_query_project(daemon, tmp_path: Path) -> None:
    """FC-2 — the browser's <img> loader can't send X-MeshKore-Project, so
    GET /chat/uploads honours ?project=<id>. An image under project B is served
    with ?project=B and NOT found under the default project A (no header)."""
    b_root = tmp_path / "proj-b-upload"
    b_root.mkdir(parents=True, exist_ok=True)
    b_id = daemon.post(
        "/projects", headers=daemon.auth, json={"path": str(b_root)}
    ).json()["id"]
    a_id = daemon.get("/projects").json()["default"]
    assert b_id != a_id

    bucket = "2026-06-26"  # serve_path requires a YYYY-MM-DD bucket
    updir = b_root / ".meshkore" / "uploads" / bucket
    updir.mkdir(parents=True, exist_ok=True)
    fname = "route-b-1-0-abcd.png"
    blob = b"\x89PNG\r\n\x1a\nDATA"
    (updir / fname).write_bytes(blob)
    url = f"/chat/uploads/{bucket}/{fname}"

    # ?project=B (no header — like an <img>) → served from B.
    r = daemon.get(url, params={"project": b_id})
    assert r.status_code == 200, r.text
    assert r.content == blob
    # ?project=A → resolves against A, which has no such upload → 404.
    r = daemon.get(url, params={"project": a_id})
    assert r.status_code == 404, r.text
    # The header path still works (the cockpit's authed requests use it).
    r = daemon.get(url, headers={"X-MeshKore-Project": b_id})
    assert r.status_code == 200, r.text


@pytest.mark.cluster("populated")
def test_register_requires_auth_and_valid_path(daemon, tmp_path: Path) -> None:
    # `Connection: close` on the early-reject probes: the auth gate returns 401
    # BEFORE draining the JSON body, which would misframe the next request on a
    # reused keep-alive socket (same quirk test_route_coverage guards with
    # _NO_KEEPALIVE; the cockpit never hits it — it always auths).
    close = {"Connection": "close"}
    # No auth → 401 (POST /projects portal-OR-remote gate).
    r = daemon.post("/projects", json={"path": str(tmp_path)}, headers=close)
    assert r.status_code == 401, r.text
    # Auth but neither path nor parent+name → 400.
    r = daemon.post("/projects", headers={**daemon.auth, **close}, json={})
    assert r.status_code == 400, r.text
    # CPL-2 create-from-scratch: a non-existent path UNDER an allowlisted parent
    # (the boot project's own parent) is mkdir'd + scaffolded + registered (201),
    # replacing the old adopt-only 400. The folder now holds a real cluster.
    r = daemon.post(
        "/projects",
        headers={**daemon.auth, **close},
        json={"path": str(tmp_path / "nope")},
    )
    assert r.status_code == 201, r.text
    assert r.json().get("scaffolded") is True, r.text
    assert (tmp_path / "nope" / ".meshkore" / "public" / "cluster.yaml").exists()


@pytest.mark.cluster("populated")
def test_register_create_from_scratch_parent_name_and_allowlist(
    daemon, tmp_path: Path
) -> None:
    close = {"Connection": "close"}
    # {parent, name} under an allowlisted parent (the boot project's parent) →
    # the folder is created from the slugified name, scaffolded, registered.
    r = daemon.post(
        "/projects",
        headers={**daemon.auth, **close},
        json={"parent": str(tmp_path), "name": "CPL Smoke!"},
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body.get("scaffolded") is True, body
    assert (tmp_path / "cpl-smoke" / ".meshkore" / "public" / "cluster.yaml").exists()
    # A parent OUTSIDE the allowlist never scaffolds anywhere → 403.
    r = daemon.post(
        "/projects",
        headers={**daemon.auth, **close},
        json={"parent": "/opt/cpl-not-allowed-xyz", "name": "nope"},
    )
    assert r.status_code == 403, r.text
    assert not Path("/opt/cpl-not-allowed-xyz").exists()
