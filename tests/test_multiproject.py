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


@pytest.mark.cluster("populated")
def test_register_requires_auth_and_valid_path(daemon, tmp_path: Path) -> None:
    # `Connection: close` on the early-reject probes: the global auth gate
    # returns 401 BEFORE draining the JSON body, which would misframe the next
    # request on a reused keep-alive socket (same quirk test_route_coverage
    # guards with _NO_KEEPALIVE; the cockpit never hits it — it always auths).
    close = {"Connection": "close"}
    # No auth → 401 (global POST gate).
    r = daemon.post("/projects", json={"path": str(tmp_path)}, headers=close)
    assert r.status_code == 401, r.text
    # Auth but missing path → 400.
    r = daemon.post("/projects", headers={**daemon.auth, **close}, json={})
    assert r.status_code == 400, r.text
    # Auth but non-existent path → 400.
    r = daemon.post(
        "/projects",
        headers={**daemon.auth, **close},
        json={"path": str(tmp_path / "nope")},
    )
    assert r.status_code == 400, r.text
