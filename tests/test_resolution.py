"""Standard v26 — task resolution record + Queue (`next` wall) contract.

Covers the two pieces shipped 2026-06-19:
  • `_upsert_body_section` — the pure body-writer the daemon uses to write
    `## Resolution` (idempotent; preserves the original spec body).
  • /state serves the new frontmatter pointers (`completed_at`,
    `resolved_by`, `resolved_by_conv`, `commit_shas`).
  • the Queue contract: moving an initiative to the `next` wall persists
    `status: next` + `wall_order`, which a CLI agent reads back.
"""

from __future__ import annotations

import pytest

from conftest import Daemon
from anchorprogress import _upsert_body_section

INI = ".meshkore/roadmap/initiatives"
T1 = ".meshkore/modules/daemon/tasks/T1.md"


# ── pure helper ──────────────────────────────────────────────────────────


def test_upsert_appends_when_absent() -> None:
    src = "---\nid: T1\nstatus: done\n---\n# Body\n\nDo the thing.\n"
    out = _upsert_body_section(src, "Resolution", "Shipped it.")
    assert "# Body" in out and "Do the thing." in out  # spec preserved
    assert "## Resolution" in out and "Shipped it." in out


def test_upsert_replaces_when_present_and_preserves_spec() -> None:
    src = "---\nid: T1\n---\n# Body\n\nspec.\n\n## Resolution\n\nold summary.\n"
    out = _upsert_body_section(src, "Resolution", "new summary.")
    assert "new summary." in out
    assert "old summary." not in out
    assert out.count("## Resolution") == 1  # not duplicated
    assert "spec." in out  # original spec body untouched


def test_upsert_keeps_sections_after_resolution() -> None:
    src = "---\nid: T1\n---\nspec\n\n## Resolution\n\nold\n\n## Notes\n\nkeep me\n"
    out = _upsert_body_section(src, "Resolution", "fresh")
    assert "fresh" in out and "old" not in out
    assert "## Notes" in out and "keep me" in out


# ── /state serves the resolution pointers ────────────────────────────────


@pytest.mark.cluster("populated")
def test_state_serves_resolution_fields(daemon: Daemon) -> None:
    # Every task carries the keys (null until resolved) so the cockpit can
    # paint the wall without a body read.
    tasks = {t["id"]: t for t in daemon.get("/state").json()["roadmap"]["tasks"]}
    assert "T1" in tasks
    for k in ("completed_at", "resolved_by", "resolved_by_conv", "commit_shas"):
        assert k in tasks["T1"], f"/state task missing {k}"

    # Simulate a resolved task on disk + reload → fields surface.
    p = daemon.root / T1
    p.write_text(
        "---\nid: T1\ntitle: T1\nstatus: done\ncategory: daemon\n"
        "initiative: alpha\ncompleted_at: 2026-06-19T15:42:30Z\n"
        "resolved_by: A042\nresolved_by_conv: work-coder-T1-001\n---\n"
        "# T1\n\n## Resolution\n\nDid the work. 3 tests pass.\n"
    )
    daemon.get("/reload", headers=daemon.auth)
    t1 = {t["id"]: t for t in daemon.get("/state").json()["roadmap"]["tasks"]}["T1"]
    assert t1["completed_at"] == "2026-06-19T15:42:30Z"
    assert t1["resolved_by"] == "A042"
    assert t1["resolved_by_conv"] == "work-coder-T1-001"
    assert t1["status"] == "done"


# ── Queue contract — the `next` wall is the execution queue ───────────────


@pytest.mark.cluster("populated")
def test_queue_stage_persists_status_next_and_order(daemon: Daemon) -> None:
    # Stage `alpha` (currently active) into the queue = move to `next` wall.
    r = daemon.post(
        "/initiative/reorder",
        headers=daemon.auth,
        json={"id": "alpha", "wall": "next", "order": 0},
    )
    assert r.status_code == 200, r.text
    # On disk: a CLI agent reads `status: next` + `wall_order`.
    disk = (daemon.root / INI / "alpha.md").read_text()
    assert "status: next" in disk
    assert "wall_order:" in disk
    # /state agrees and the walls endpoint lists it in `next`.
    inits = {i["id"]: i for i in daemon.get("/state").json()["initiatives"]}
    assert inits["alpha"]["status"] == "next"
    assert (
        daemon.get("/initiative/walls", headers=daemon.auth).json()["next"][0]
        == "alpha"
    )
