"""Initiative wall ordering (py-1.20.0) — GET /initiative/walls + POST
/initiative/reorder. The populated fixture has alpha (status active) + beta
(status next), neither with a wall_order, so it exercises both the back-compat
path (no wall_order → filename order) and the reorder/recompact/status-flip."""

from __future__ import annotations

import pytest

from conftest import Daemon

INI = ".meshkore/roadmap/initiatives"


@pytest.mark.cluster("populated")
def test_walls_initial_back_compat(daemon: Daemon) -> None:
    w = daemon.get("/initiative/walls", headers=daemon.auth).json()
    assert set(w.keys()) == {"active", "next", "backlog", "archived"}
    assert "alpha" in w["active"]  # status: active
    assert "beta" in w["next"]  # status: next
    # no wall_order yet — every wall is still a (possibly empty) list
    assert all(isinstance(v, list) for v in w.values())


@pytest.mark.cluster("populated")
def test_reorder_moves_wall_sets_status_and_recompacts(daemon: Daemon) -> None:
    r = daemon.post(
        "/initiative/reorder",
        headers=daemon.auth,
        json={"id": "beta", "wall": "active", "order": 0},
    )
    assert r.status_code == 200, r.text

    w = daemon.get("/initiative/walls", headers=daemon.auth).json()
    assert w["active"][0] == "beta"  # placed at order 0
    assert "alpha" in w["active"]
    assert "beta" not in w["next"]  # left its old wall

    # /state carries the recompacted wall_order (beta=0, alpha=1)
    inits = {i["id"]: i for i in daemon.get("/state").json()["initiatives"]}
    assert inits["beta"]["wall_order"] == 0
    assert inits["alpha"]["wall_order"] == 1
    # status flipped on disk
    assert "status: active" in (daemon.root / INI / "beta.md").read_text()


@pytest.mark.cluster("populated")
def test_reorder_archive_holds_via_archived_flag(daemon: Daemon) -> None:
    # alpha has open child tasks (T1/T2), so build_state's archive-reconcile
    # reverts status done→active. The `archived: true` flag is what keeps it in
    # the archived wall regardless — that's the contract the walls UI relies on.
    r = daemon.post(
        "/initiative/reorder",
        headers=daemon.auth,
        json={"id": "alpha", "wall": "archived", "order": 0},
    )
    assert r.status_code == 200, r.text
    w = daemon.get("/initiative/walls", headers=daemon.auth).json()
    assert "alpha" in w["archived"], "archived flag must hold the wall"
    assert "alpha" not in w["active"]
    txt = (daemon.root / INI / "alpha.md").read_text()
    assert "archived: True" in txt or "archived: true" in txt

    # Moving back OUT of archived clears the flag → returns to a live wall.
    daemon.post(
        "/initiative/reorder",
        headers=daemon.auth,
        json={"id": "alpha", "wall": "next", "order": 0},
    )
    w2 = daemon.get("/initiative/walls", headers=daemon.auth).json()
    assert "alpha" in w2["next"]
    assert "alpha" not in w2["archived"]


@pytest.mark.cluster("populated")
def test_reorder_within_wall_reorders(daemon: Daemon) -> None:
    # Put both in active: beta@0, alpha@1. Then move alpha to 0 → alpha,beta.
    daemon.post(
        "/initiative/reorder",
        headers=daemon.auth,
        json={"id": "beta", "wall": "active", "order": 1},
    )
    daemon.post(
        "/initiative/reorder",
        headers=daemon.auth,
        json={"id": "alpha", "wall": "active", "order": 0},
    )
    w = daemon.get("/initiative/walls", headers=daemon.auth).json()
    assert w["active"] == ["alpha", "beta"]


@pytest.mark.cluster("populated")
@pytest.mark.parametrize(
    "body,code",
    [
        ({"wall": "active", "order": 0}, 400),  # no id
        ({"id": "alpha", "wall": "bogus", "order": 0}, 400),  # bad wall
        ({"id": "alpha", "wall": "active", "order": "x"}, 400),  # bad order
        ({"id": "nope", "wall": "active", "order": 0}, 404),  # unknown initiative
    ],
)
def test_reorder_validation(daemon: Daemon, body, code) -> None:
    r = daemon.post("/initiative/reorder", headers=daemon.auth, json=body)
    assert r.status_code == code, f"{body} → {r.status_code} {r.text}"
