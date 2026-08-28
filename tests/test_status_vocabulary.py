"""RSV1 — the roadmap's `status` vocabulary, and what the cockpit is told.

Two separate bugs lived here, and the second one was invisible because the
first hid it.

1. `normalize_status` recognised five values and returned `backlog` for
   everything else, silently. This repo's roadmap files use 17. So an
   initiative marked `shipped` and one marked `archived` read as pending
   work, and `cancelled` / `dropped` tasks counted as outstanding.

2. Task status was normalised before publication; INITIATIVE status was not —
   it went out as the raw frontmatter literal. The cockpit's `InitiativeCard`
   tests for `done` / `backlog` / `next` and treats anything else as ACTIVE,
   so 36 `draft` / `planned` / `ready` initiatives rendered as live work.
   Measured against the live daemon: 48 initiatives shown ACTIVE, only 12 of
   them genuinely active.

The alias table is the fix for both. These tests pin the mapping and the
publication shape.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cluster import (  # noqa: E402
    CANONICAL_STATUSES,
    STATUS_ALIASES,
    is_resolved_status,
    normalize_status,
)


# ── the aliases that were actually wrong in this repo ───────────────────


@pytest.mark.parametrize("raw", ["shipped", "archived", "completed", "complete"])
def test_finished_work_normalises_to_done(raw: str) -> None:
    """`cluster-admin-lifecycle` (shipped) and `openclaw-marketplace`
    (archived) are delivered. They must not read as anything else."""
    assert normalize_status(raw) == "done"


@pytest.mark.parametrize("raw", ["draft", "planned", "ready", "pending", "todo", "new"])
def test_not_started_work_normalises_to_backlog(raw: str) -> None:
    """A sketch is backlog. Before RSV1 these reached the cockpit raw and its
    done/backlog/next test fell through to the ACTIVE branch."""
    assert normalize_status(raw) == "backlog"


@pytest.mark.parametrize(
    "raw", ["cancelled", "canceled", "dropped", "wontfix", "superseded", "obsolete"]
)
def test_abandoned_work_normalises_to_cancelled(raw: str) -> None:
    assert normalize_status(raw) == "cancelled"


@pytest.mark.parametrize("raw", ["in_progress", "in-progress", "doing", "wip"])
def test_in_flight_work_normalises_to_active(raw: str) -> None:
    """`doing` is used in this repo's own task files and was reading as
    backlog — an in-flight task showing as untouched."""
    assert normalize_status(raw) == "active"


# ── invariants of the table itself ──────────────────────────────────────


def test_every_canonical_value_is_a_fixed_point() -> None:
    for s in CANONICAL_STATUSES:
        assert normalize_status(s) == s


def test_every_alias_lands_on_a_canonical_value() -> None:
    for raw, mapped in STATUS_ALIASES.items():
        assert mapped in CANONICAL_STATUSES, f"{raw} → {mapped} is not canonical"


def test_no_alias_shadows_a_canonical_value() -> None:
    """An alias for a canonical word would be unreachable — the fixed-point
    branch wins — so it is dead config and a sign of a mistake."""
    assert not (set(STATUS_ALIASES) & set(CANONICAL_STATUSES))


def test_unknown_status_fails_toward_pending() -> None:
    """A typo must never read as finished. `backlog` overstates nothing; the
    integrity checker is what makes it visible."""
    assert normalize_status("in-review") == "backlog"
    assert normalize_status("") == "backlog"
    assert normalize_status(None) == "backlog"
    assert not is_resolved_status("in-review")


def test_case_and_whitespace_are_tolerated() -> None:
    assert normalize_status("  SHIPPED ") == "done"
    assert normalize_status("Draft") == "backlog"


# ── what actually goes on the wire ──────────────────────────────────────


def _build(tmp_path: Path):
    from cluster import Cluster
    from paths import Paths
    from statebuild import build_state

    root = tmp_path
    (root / ".meshkore" / "roadmap" / "initiatives").mkdir(parents=True)
    (root / ".meshkore" / "modules" / "daemon" / "tasks").mkdir(parents=True)
    (root / ".meshkore" / "public").mkdir(parents=True, exist_ok=True)
    (root / ".meshkore" / "public" / "cluster.yaml").write_text(
        "id: t\nname: t\n", encoding="utf-8"
    )
    (root / ".meshkore" / "roadmap" / "initiatives" / "sketch.md").write_text(
        "---\nid: sketch\ntitle: A sketch\nstatus: draft\n---\n", encoding="utf-8"
    )
    (root / ".meshkore" / "roadmap" / "initiatives" / "old.md").write_text(
        "---\nid: old\ntitle: Delivered\nstatus: shipped\n---\n", encoding="utf-8"
    )
    (root / ".meshkore" / "modules" / "daemon" / "tasks" / "T1.md").write_text(
        "---\nid: T1\ntitle: t\nstatus: cancelled\ncategory: daemon\n"
        "initiative: sketch\n---\n",
        encoding="utf-8",
    )
    paths = Paths(root)
    return build_state(paths, Cluster(paths))


def test_initiative_status_is_normalised_on_the_wire(tmp_path: Path) -> None:
    """THE second bug. `draft` used to be published verbatim and the cockpit
    rendered it ACTIVE."""
    state = _build(tmp_path)
    by_id = {i["id"]: i for i in state["initiatives"]}
    assert by_id["sketch"]["status"] == "backlog"
    assert by_id["old"]["status"] == "done"


def test_the_literal_survives_alongside_it(tmp_path: Path) -> None:
    """Normalising must not destroy what the file said — a future `draft`
    column, and any audit of the roadmap, needs the original word."""
    state = _build(tmp_path)
    by_id = {i["id"]: i for i in state["initiatives"]}
    assert by_id["sketch"]["status_raw"] == "draft"
    assert by_id["old"]["status_raw"] == "shipped"


def test_cancelled_reaches_the_cockpit_as_cancelled(tmp_path: Path) -> None:
    """It used to arrive as `backlog`, which is why the cockpit's two
    `!== 'cancelled'` guards were dead code."""
    state = _build(tmp_path)
    t1 = next(t for t in state["roadmap"]["tasks"] if t["id"] == "T1")
    assert t1["status"] == "cancelled"
    assert t1["status_raw"] == "cancelled"
